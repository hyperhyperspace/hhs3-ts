// The schema diff: the migration rules that take a schema's working tables
// (its parents' state after upgrade-manual.sql) to the tables target-catalog.sql
// describes.
//
// A column whose definition changed, or a kept table whose identity provider
// changed, can't be migrated in place and is refused (the reset goes in
// upgrade-manual.sql). Everything else becomes rules in five phases, each sorted
// by table and then column:
//
//   1. add-column
//   2. add-table, a new table after the new tables it references (by a local
//      FK or EXISTS); a table on a reference cycle is added without its FKs
//      and restrictions, which phase 3 sets
//   3. set-fks, set-restrictions, set-concurrent-deletes where the effective
//      value differs, plus what phase 2 deferred; dropped tables that
//      reference each other in a cycle have those references cleared
//   4. drop-table, referencing tables first
//   5. drop-column
//
// Everything is added before anything refers to it, and after phase 3 nothing
// that stays refers to what phases 4 and 5 remove.

import { collectExistsAtoms, splitTableRef, type MigrationRule, type TableDef } from "@hyper-hyper-space/hhs3_rdb";

import {
    effectiveConcurrentDeletes, effectiveFks, sameColumn, sameConcurrentDeletes, sameFks, sameIdProvider, sameRestrictions,
} from "./model.js";
import { UPGRADE_MANUAL_FILE } from "./project.js";
import type { Where } from "./source.js";

export type Refusal = { message: string; hint?: string; where?: Where };

export type SchemaDiff = { rules: MigrationRule[]; refusals: Refusal[] };

export type SchemaLocator = {
    table(table: string): Where | undefined;
    column(table: string, column: string): Where | undefined;
};

const byName = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);

// The local tables a table's FKs and EXISTS atoms name.
export function localReferences(def: TableDef): Set<string> {
    const out = new Set<string>();
    for (const target of Object.values(def.fks ?? {})) {
        const [group, table] = splitTableRef(target);
        if (group === undefined) out.add(table);
    }
    for (const restriction of def.restrictions ?? []) {
        for (const atom of collectExistsAtoms(restriction.rule)) {
            const [group, table] = splitTableRef(atom.table);
            if (group === undefined) out.add(table);
        }
    }
    return out;
}

// The tables among `names` that lie on a reference cycle (self references
// don't count: a table may name itself).
function onCycles(names: string[], refs: (name: string) => Set<string>): Set<string> {
    const within = new Set(names);
    const edges = new Map(names.map((n) => [n, [...refs(n)].filter((r) => r !== n && within.has(r)).sort(byName)]));
    const out = new Set<string>();
    // Tarjan's strongly connected components.
    let counter = 0;
    const index = new Map<string, number>();
    const low = new Map<string, number>();
    const stack: string[] = [];
    const onStack = new Set<string>();
    const visit = (v: string) => {
        index.set(v, counter);
        low.set(v, counter);
        counter += 1;
        stack.push(v);
        onStack.add(v);
        for (const w of edges.get(v)!) {
            if (!index.has(w)) {
                visit(w);
                low.set(v, Math.min(low.get(v)!, low.get(w)!));
            } else if (onStack.has(w)) {
                low.set(v, Math.min(low.get(v)!, index.get(w)!));
            }
        }
        if (low.get(v) === index.get(v)) {
            const component: string[] = [];
            let w: string;
            do {
                w = stack.pop()!;
                onStack.delete(w);
                component.push(w);
            } while (w !== v);
            if (component.length > 1) for (const c of component) out.add(c);
        }
    };
    for (const name of [...names].sort(byName)) if (!index.has(name)) visit(name);
    return out;
}

// `names` ordered so a name comes after the ones it `needs` (among `names`),
// ties by name. The graph must be acyclic.
function orderAfter(names: string[], needs: (name: string) => Set<string>): string[] {
    const pending = [...names].sort(byName);
    const placed = new Set<string>();
    const out: string[] = [];
    while (pending.length > 0) {
        const i = pending.findIndex((n) => [...needs(n)].every((m) => m === n || placed.has(m) || !pending.includes(m)));
        const [next] = pending.splice(i === -1 ? 0 : i, 1);
        placed.add(next);
        out.push(next);
    }
    return out;
}

function withoutReferences(def: TableDef): TableDef {
    const { fks: _fks, restrictions: _restrictions, ...rest } = def;
    return rest;
}

export function diffTables(
    schema: string,
    working: Map<string, TableDef>,
    desired: Map<string, TableDef>,
    locate?: SchemaLocator,
): SchemaDiff {
    const kept = [...desired.keys()].filter((t) => working.has(t)).sort(byName);
    const added = [...desired.keys()].filter((t) => !working.has(t)).sort(byName);
    const dropped = [...working.keys()].filter((t) => !desired.has(t)).sort(byName);

    const refusals: Refusal[] = [];
    for (const table of kept) {
        const w = working.get(table)!;
        const d = desired.get(table)!;
        if (!sameIdProvider(w, d)) {
            refusals.push({
                message: `${schema} ${table}: its identity provider changes, which a table can't do in place`,
                hint: `to reset the table, add to ${UPGRADE_MANUAL_FILE}:  ALTER SCHEMA ${schema} AS (DROP TABLE ${table});`,
                ...where(locate?.table(table)),
            });
        }
        for (const column of Object.keys(d.columns).sort(byName)) {
            const before = w.columns[column];
            const after = d.columns[column];
            if (before !== undefined && !sameColumn(before, after)) {
                refusals.push({
                    message: `${schema} ${table}.${column}: its definition changes, and a column can't be redefined in place`,
                    hint: `to reset the column, add to ${UPGRADE_MANUAL_FILE}:  ALTER SCHEMA ${schema} AS (DROP COLUMN ${table}.${column});`,
                    ...where(locate?.column(table, column)),
                });
            }
            if (before === undefined && !(after.nullable ?? false) && after.default === undefined) {
                refusals.push({
                    message: `${schema} ${table}.${column}: a new NOT NULL column needs a DEFAULT for the rows already there`,
                    hint: 'give it a DEFAULT, or make it NULL',
                    ...where(locate?.column(table, column)),
                });
            }
        }
    }
    if (refusals.length > 0) return { rules: [], refusals };

    const rules: MigrationRule[] = [];

    // 1. add-column
    for (const table of kept) {
        const w = working.get(table)!;
        const d = desired.get(table)!;
        for (const column of Object.keys(d.columns).sort(byName)) {
            if (w.columns[column] === undefined) rules.push({ rule: 'add-column', table, column, def: d.columns[column] });
        }
    }

    // 2. add-table
    const newRefs = (t: string) => localReferences(desired.get(t)!);
    const cyclic = onCycles(added, newRefs);
    for (const table of [...cyclic].sort(byName)) rules.push({ rule: 'add-table', def: withoutReferences(desired.get(table)!) });
    const acyclic = added.filter((t) => !cyclic.has(t));
    for (const table of orderAfter(acyclic, newRefs)) rules.push({ rule: 'add-table', def: desired.get(table)! });

    // 3. settings
    const setAll = (table: string, def: TableDef, from: TableDef) => {
        if (!sameFks(from, def)) rules.push({ rule: 'set-fks', table, fks: effectiveFks(def) });
        if (!sameRestrictions(from, def)) rules.push({ rule: 'set-restrictions', table, restrictions: def.restrictions ?? [] });
        if (!sameConcurrentDeletes(from, def)) rules.push({ rule: 'set-concurrent-deletes', table, value: effectiveConcurrentDeletes(def) });
    };
    const settingsOf = new Map<string, () => void>();
    for (const table of kept) settingsOf.set(table, () => setAll(table, desired.get(table)!, working.get(table)!));
    for (const table of cyclic) settingsOf.set(table, () => setAll(table, desired.get(table)!, withoutReferences(desired.get(table)!)));
    const droppedRefs = (t: string) => localReferences(working.get(t)!);
    const droppedCycles = onCycles(dropped, droppedRefs);
    for (const table of droppedCycles) {
        settingsOf.set(table, () => {
            const def = working.get(table)!;
            if (Object.keys(effectiveFks(def)).length > 0) rules.push({ rule: 'set-fks', table, fks: {} });
            if ((def.restrictions ?? []).length > 0) rules.push({ rule: 'set-restrictions', table, restrictions: [] });
        });
    }
    for (const table of [...settingsOf.keys()].sort(byName)) settingsOf.get(table)!();

    // 4. drop-table: a table goes once no remaining dropped table names it
    const remaining = new Set(dropped);
    const namedBy = (t: string) => new Set(dropped.filter((o) => o !== t && remaining.has(o) && !droppedCycles.has(o) && droppedRefs(o).has(t)));
    while (remaining.size > 0) {
        const next = [...remaining].sort(byName).find((t) => namedBy(t).size === 0) ?? [...remaining].sort(byName)[0];
        remaining.delete(next);
        rules.push({ rule: 'drop-table', table: next });
    }

    // 5. drop-column
    for (const table of kept) {
        const w = working.get(table)!;
        const d = desired.get(table)!;
        for (const column of Object.keys(w.columns).sort(byName)) {
            if (d.columns[column] === undefined) rules.push({ rule: 'drop-column', table, column });
        }
    }

    return { rules, refusals: [] };
}

function where(w: Where | undefined): { where?: Where } {
    return w === undefined ? {} : { where: w };
}

// Rules that can remove data or change who may write, for the major-bump
// warning.
export function isBreaking(rule: MigrationRule): boolean {
    return rule.rule === 'drop-table' || rule.rule === 'drop-column' || rule.rule.startsWith('set-');
}
