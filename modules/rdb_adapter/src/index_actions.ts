// Pure planner for projection-local indexes: validate an IndexSpec, resolve its
// declarations against one group's schema view (rdb names -> target names),
// and diff the desired indexes against what a target has materialized.
//
// One diff serves both callers:
//   - apply time (projectGroupTo): desired = the INSTALLED spec resolved at the
//     delta's end view; the dying set comes from the delta's schema actions, so
//     an index is dropped before its table/column goes and rebuilt on the new
//     incarnation in the same apply;
//   - reconcile (index_reconcile.ts): desired = a NEW spec resolved at the
//     checkpoint view, with nothing dying.
//
// A declaration whose table or columns are not (yet) projected is PENDING, not
// an error: it becomes complete when a later delta adds what it needs.

import { json } from "@hyper-hyper-space/hhs3_json";
import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { isValidName, type RSchemaView } from "@hyper-hyper-space/hhs3_rdb";

import {
    AdapterConfig, AUTHOR_INDEX_COLUMN, IndexDecl, IndexSpec, ResolvedIndex, SchemaAction,
} from "./types.js";
import { authorColumn, targetTableName } from "./names.js";
import { reshapeColumn } from "./schema_actions.js";

export const PUB_INDEX_PREFIX = 'pub__';

export type PendingIndex = { name: string; group: string; table: string; missing: string[] };

// Structural validation. `options` is opaque here (each target checks its own
// format in validateIndexOptions). Returns a reason, or undefined when valid.
export function validateIndexSpec(spec: IndexSpec): string | undefined {
    if (!Number.isInteger(spec.version) || spec.version < 0) {
        return `index spec version must be a non-negative integer, got ${spec.version}`;
    }
    if (!Array.isArray(spec.indexes)) return 'index spec must carry an `indexes` array';
    const seen = new Set<string>();
    for (const decl of spec.indexes) {
        const label = `index '${decl.group}.${decl.name}'`;
        if (typeof decl.group !== 'string' || decl.group.length === 0) {
            return `index '${decl.name}' must name its group`;
        }
        if (typeof decl.name !== 'string' || !isValidName(decl.name)) {
            return `index name '${decl.name}' in group '${decl.group}' is not a valid identifier`;
        }
        if (decl.name.startsWith(PUB_INDEX_PREFIX)) {
            return `${label}: the '${PUB_INDEX_PREFIX}' prefix is reserved for indexPub`;
        }
        const key = decl.group + '\u0000' + decl.name;
        if (seen.has(key)) return `${label} is declared twice`;
        seen.add(key);
        if (typeof decl.table !== 'string' || !isValidName(decl.table)) {
            return `${label}: table '${decl.table}' is not a valid identifier`;
        }
        if (!Array.isArray(decl.columns) || decl.columns.length === 0) {
            return `${label} must list at least one column`;
        }
        const cols = new Set<string>();
        for (const name of decl.columns as unknown[]) {
            if (typeof name !== 'string') {
                return `${label}: column entries must be rdb column names; `
                    + "per-column settings (e.g. descending order) belong in the target's options";
            }
            if (name !== AUTHOR_INDEX_COLUMN && !isValidName(name)) {
                return `${label}: column '${name}' is not a valid identifier or '${AUTHOR_INDEX_COLUMN}'`;
            }
            if (cols.has(name)) return `${label} lists column '${name}' twice`;
            cols.add(name);
        }
    }
    return undefined;
}

// Canonical JSON of a whole spec (the installed-spec compare-and-set token).
export function indexSpecFingerprint(spec: IndexSpec): string {
    const out: json.LiteralMap = {
        version: spec.version,
        indexes: spec.indexes.map((d) => {
            const m: json.LiteralMap = {
                name: d.name, group: d.group, table: d.table, columns: [...d.columns],
            };
            if (d.options !== undefined) m.options = d.options;
            return m;
        }),
        indexPub: spec.indexPub === true,
    };
    return json.toStringNormalized(out);
}

function indexFingerprint(index: Omit<ResolvedIndex, 'fingerprint' | 'groupId'>): string {
    const out: json.LiteralMap = {
        name: index.name,
        table: index.table,
        columns: index.columns.map((c) => ({ rdb: c.rdb, target: c.target })),
    };
    if (index.options !== undefined) out.options = index.options;
    return json.toStringNormalized(out);
}

// The declarations that apply to one group: its explicit ones, plus (with
// indexPub) one `pub__<column>` index per pub column of each table in `view`.
// An explicit declaration never collides with a generated one (the prefix is
// reserved by validateIndexSpec).
export function groupIndexDecls(spec: IndexSpec | undefined, groupName: string, view: RSchemaView): IndexDecl[] {
    if (spec === undefined) return [];
    const decls = spec.indexes.filter((d) => d.group === groupName);
    if (spec.indexPub === true) {
        for (const table of view.getTableNames()) {
            const def = view.getTable(table);
            if (def === undefined) continue;
            for (const [column, colDef] of Object.entries(def.columns)) {
                if (colDef.pub === true) {
                    decls.push({ name: PUB_INDEX_PREFIX + column, group: groupName, table, columns: [column] });
                }
            }
        }
    }
    return decls;
}

// Resolve declarations (already filtered to one group) against that group's
// schema view: target table via targetTableName (group-qualified in a shared
// projection), target columns via the mapper's own reshapeColumn (FK
// companions, `<col>_key_id`, provider `key_id`), '@author' via the config's
// author column. Anything missing makes the declaration pending.
export function resolveIndexes(
    decls: IndexDecl[], groupId: B64Hash, view: RSchemaView, config: AdapterConfig,
): { resolved: ResolvedIndex[]; pending: PendingIndex[] } {
    const resolved: ResolvedIndex[] = [];
    const pending: PendingIndex[] = [];

    for (const decl of decls) {
        const def = view.getTable(decl.table);
        if (def === undefined) {
            pending.push({ name: decl.name, group: decl.group, table: decl.table, missing: [`table '${decl.table}'`] });
            continue;
        }
        const fks = view.getFKs(decl.table);
        const provider = view.getIdProvider?.(decl.table);

        const missing: string[] = [];
        const columns: ResolvedIndex['columns'] = [];
        for (const rdb of decl.columns) {
            let target: string | undefined;
            if (rdb === AUTHOR_INDEX_COLUMN) {
                target = authorColumn(config);
                if (target === undefined) missing.push(`${AUTHOR_INDEX_COLUMN} (author column disabled)`);
            } else {
                const colDef = def.columns[rdb];
                if (colDef === undefined) {
                    missing.push(`column '${rdb}'`);
                } else {
                    target = reshapeColumn(config, decl.table, rdb, colDef, fks, provider)?.name;
                    if (target === undefined) missing.push(`column '${rdb}' (not projected)`);
                }
            }
            if (target !== undefined) columns.push({ rdb, target });
        }
        if (missing.length > 0) {
            pending.push({ name: decl.name, group: decl.group, table: decl.table, missing });
            continue;
        }

        const base: Omit<ResolvedIndex, 'fingerprint' | 'groupId'> = {
            name: decl.name, table: targetTableName(config, decl.table), columns,
        };
        if (decl.options !== undefined) base.options = decl.options;
        resolved.push({ ...base, groupId, fingerprint: indexFingerprint(base) });
    }

    return { resolved, pending };
}

function indexKey(table: string, name: string): string {
    return table + '\u0000' + name;
}

// Diff desired against materialized, given the schema actions this apply will
// run. A materialized index is dropped when its table is dropped or recreated,
// when any of its columns is dropped (column reincarnations and FK flips are
// drop+add), when it is no longer desired, or when its resolved form changed.
// A desired index is ensured unless an identical one survives.
export function planIndexActions(
    desired: ResolvedIndex[], materialized: ResolvedIndex[], schemaActions: SchemaAction[],
): { drops: SchemaAction[]; ensures: SchemaAction[] } {
    const dyingTables = new Set<string>();
    const dyingColumns = new Set<string>();
    for (const a of schemaActions) {
        if (a.kind === 'drop-table' || a.kind === 'create-table') dyingTables.add(a.table);
        else if (a.kind === 'drop-column') dyingColumns.add(indexKey(a.table, a.column));
    }

    const desiredByKey = new Map<string, ResolvedIndex>();
    for (const d of desired) desiredByKey.set(indexKey(d.table, d.name), d);

    const drops: SchemaAction[] = [];
    const surviving = new Map<string, ResolvedIndex>();
    for (const m of materialized) {
        const key = indexKey(m.table, m.name);
        const dying = dyingTables.has(m.table)
            || m.columns.some((c) => dyingColumns.has(indexKey(m.table, c.target)));
        const want = desiredByKey.get(key);
        if (dying || want === undefined || want.fingerprint !== m.fingerprint) {
            drops.push({ kind: 'drop-index', table: m.table, name: m.name });
        } else {
            surviving.set(key, m);
        }
    }

    const ensures: SchemaAction[] = [];
    for (const [key, d] of desiredByKey) {
        if (!surviving.has(key)) ensures.push({ kind: 'ensure-index', index: d });
    }
    return { drops, ensures };
}

// The full schema channel for one apply: index drops first (SQLite refuses to
// drop an indexed column), then the table/column actions, then index ensures.
export function withIndexActions(
    schemaActions: SchemaAction[], plan: { drops: SchemaAction[]; ensures: SchemaAction[] },
): SchemaAction[] {
    if (plan.drops.length === 0 && plan.ensures.length === 0) return schemaActions;
    return [...plan.drops, ...schemaActions, ...plan.ensures];
}
