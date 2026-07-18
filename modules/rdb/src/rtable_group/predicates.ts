// Predicate evaluation for restrictions and the canDeploy gate.
//
// Row restrictions are evaluated twice with this same machinery. Validation is
// a hard gate over the parent frontier `(at, at)`: authorizing rows must
// causally precede the op. View-time voiding re-evaluates at the op's own
// position, with the evaluating view's `from` horizon, so a witness row
// barrier-deleted CONCURRENTLY with the use voids it, while a causally-later
// delete does not (use-before-revoke) — the anchoring does the work, no special
// casing here.
//
// A `group.table` exists target resolves through the bound foreign group at
// the foreign version observed at the op's position (getForeignTableView); a
// missing reference (unbound name or absent foreign table) makes the atom
// false, same as an empty local result.
//
// Identity terms resolve from the op's author (already signature-verified at
// validation when the group has a provider, so op.author is trusted here). The
// subject row's insert author is exposed as the readonly system column
// `rowAuthor`.

import { json } from "@hyper-hyper-space/hhs3_json";
import type { KeyId, B64Hash } from "@hyper-hyper-space/hhs3_crypto";

import type { RSchemaView } from "../rschema/interfaces.js";
import type { ColumnType, Predicate, PredicateContext, WhereValue, Operand } from "../rschema/payload.js";
import { splitTableRef, parseRowFieldTerm } from "../rschema/payload.js";
import { evalOperand, compareOperands, resolveCmpType } from "../rschema/expr.js";
import type { RTableView, RowValues } from "../rtable/interfaces.js";
import type { RowOpPayload } from "../rtable/payload.js";

export type PredicateEnv = {
    // Anchored table-view supplier: views at the op's position, observed from
    // the evaluating view's horizon.
    getTableView: (table: string) => Promise<RTableView>;
    // Anchored cross-group table-view supplier for `group.table` exists
    // targets: resolves through the bound foreign group at the foreign version
    // observed at the op's position. undefined = unbound name or table absent
    // at that version (a missing reference: the exists atom is false).
    getForeignTableView: (group: string, table: string) => Promise<RTableView | undefined>;
    author?: KeyId;       // op author (signature-verified at validation; trusted here)
    // subject row's readonly-resolved values ($row.<col> source); 'row' context
    // only. Inserts: op values + schema defaults; updates/deletes: the live
    // row's values overlaid with the op's writes (post-image). $row refs are
    // readonly-only, so a missing entry makes the referencing atom false.
    subjectRow?: RowValues;
    context: PredicateContext;
    // Column-type lookup for the subject table (rowAuthor -> 'string'), used to
    // resolve cmp ordering by numeric value for bigint/decimal columns. Absent
    // for callers that only use eq/ne (comparison then falls back to normalized
    // string equality, which is correct for canonical carriers).
    typeOf?: (column: string) => ColumnType | undefined;
};

function resolveIdTerm(term: '$author', env: PredicateEnv): KeyId | undefined {
    return env.author;
}

// Resolve a `where` value: a $-term ($author / $row.<col>) to its
// concrete value, or a plain literal as-is. An unresolvable term yields
// undefined (positive logic: the atom cannot be proven, so it is false).
function resolveWhereTerm(value: WhereValue, env: PredicateEnv): json.Literal | undefined {
    if (typeof value === 'string' && value.startsWith('$')) {
        if (value === '$author') return resolveIdTerm(value, env);
        const col = parseRowFieldTerm(value);
        if (col !== undefined) return env.subjectRow?.[col];
        return undefined;
    }
    return value;
}

// The subject-row column lookup behind `{col}` operands ($row.<col>): the
// readonly-resolved subject row, or undefined when absent (the atom is false).
function subjectLookup(env: PredicateEnv): (column: string) => json.Literal | undefined {
    return (column) => env.subjectRow?.[column];
}

function evalPredicateOperand(op: Operand, env: PredicateEnv): json.Literal | undefined {
    if ('lit' in op && op.lit === '$author') return env.author;
    return evalOperand(op, subjectLookup(env));
}

export async function evaluatePredicate(pred: Predicate, env: PredicateEnv): Promise<boolean> {
    switch (pred.p) {
        case 'true':
            return true;
        case 'false':
            return false;

        case 'exists': {
            const [group, table] = splitTableRef(pred.table);

            // resolve $-terms ($author / $row.<col>) in where
            // values; an unresolvable term makes the atom unprovable
            const where: { [field: string]: json.Literal } = {};
            for (const field of Object.keys(pred.where ?? {})) {
                const resolved = resolveWhereTerm(pred.where![field], env);
                if (resolved === undefined) return false;
                where[field] = resolved;
            }

            // local target resolves on a sibling; a `group.table` target
            // resolves through the bound foreign group. A missing reference
            // (unbound name or absent foreign table) makes the atom false.
            const view = group !== undefined
                ? await env.getForeignTableView(group, table)
                : await env.getTableView(table);
            if (view === undefined) return false;

            const rowIds = await view.findRowIds(where);
            return rowIds.length > 0;
        }

        case 'cmp': {
            const l = evalPredicateOperand(pred.left, env);
            const r = evalPredicateOperand(pred.right, env);
            if (l === undefined || r === undefined) return false;
            const type = env.typeOf !== undefined ? resolveCmpType(pred.left, pred.right, env.typeOf) : undefined;
            return compareOperands(pred.cmp, l, r, type);
        }

        case 'str': {
            const v = evalPredicateOperand(pred.value, env);
            const s = evalPredicateOperand(pred.sub, env);
            if (typeof v !== 'string' || typeof s !== 'string') return false;
            switch (pred.str) {
                case 'prefix': return v.startsWith(s);
                case 'suffix': return v.endsWith(s);
                case 'contains': return v.includes(s);
            }
        }

        case 'and': {
            for (const arg of pred.args) {
                if (!await evaluatePredicate(arg, env)) return false;
            }
            return true;
        }

        case 'or': {
            for (const arg of pred.args) {
                if (await evaluatePredicate(arg, env)) return true;
            }
            return false;
        }
    }
}

// Evaluate the restriction gating one row op (the and-combination of declared
// restrictions matching the op's action, or the default rule). The caller
// decides the horizon by supplying anchored views: validation passes `(at, at)`,
// while view-time voiding passes the op position observed from `from`. The
// subject row's author comes from the insert op author for inserts, and from
// the live row for updates/deletes.
export async function evaluateRowOpRestriction(
    op: RowOpPayload,
    table: string,
    schemaView: RSchemaView,
    getTableView: (table: string) => Promise<RTableView>,
    getForeignTableView: (group: string, table: string) => Promise<RTableView | undefined>,
): Promise<boolean> {
    if (!schemaView.hasTable(table)) return false;
    const rule = schemaView.getRestriction(table, op.action);

    let subjectRow: RowValues | undefined;
    if (op.action === 'insert') {
        // op values plus schema defaults (the row's resolved state at insert)
        const def = schemaView.getTable(table);
        subjectRow = op.author === undefined ? {} : { rowAuthor: op.author };
        for (const [column, cdef] of Object.entries(def?.columns ?? {})) {
            const value = op.values[column] ?? cdef.default;
            if (value !== undefined) subjectRow[column] = value;
        }
    } else {
        // post-image: the live row's resolved values overlaid with the op's
        // writes (none for delete). A not-live row leaves subjectRow undefined,
        // so $row refs fail (positive logic).
        const view = await getTableView(table);
        const row = await view.getRow(op.rowId);
        if (row !== undefined) {
            subjectRow = op.action === 'update' ? { ...row.values, ...op.values } : row.values;
            if (row.author !== undefined) subjectRow = { ...subjectRow, rowAuthor: row.author };
        }
    }

    const def = schemaView.getTable(table);
    const typeOf = (column: string): ColumnType | undefined => column === 'rowAuthor' ? 'string' : def?.columns[column]?.type;

    return evaluatePredicate(rule, {
        getTableView, getForeignTableView, author: op.author, subjectRow, context: 'row', typeOf,
    });
}

export type RowOpRestrictionFailure = {
    table: string;
    action: RowOpPayload['action'];
    rowId: B64Hash;
    rule: Predicate;
};

export async function explainRowOpRestriction(
    op: RowOpPayload,
    table: string,
    schemaView: RSchemaView,
    getTableView: (table: string) => Promise<RTableView>,
    getForeignTableView: (group: string, table: string) => Promise<RTableView | undefined>,
): Promise<RowOpRestrictionFailure | undefined> {
    if (!schemaView.hasTable(table)) {
        return { table, action: op.action, rowId: op.rowId, rule: { p: 'false' } };
    }
    const rule = schemaView.getRestriction(table, op.action);
    const ok = await evaluateRowOpRestriction(op, table, schemaView, getTableView, getForeignTableView);
    return ok ? undefined : { table, action: op.action, rowId: op.rowId, rule };
}

export type RowOpFKFailure = {
    table: string;
    action: RowOpPayload['action'];
    rowId: B64Hash;
    column: string;
    targetRef: string;
    targetRowId: B64Hash;
};

// At-use FK reach: every FK column the op writes (or inherits as a schema
// default) must name a target row that is LIVE at the op's OWN position,
// observed from the evaluating view's `from`. This folds FK enforcement into
// op-voiding — a dangling write voids the op on the same view-time path as a
// restriction recheck: a voided insert never lives, a voided FK-update
// contributes no value (LWW reverts to the prior write). At-use anchoring means
// a target deleted
// CONCURRENTLY with the use voids the write (merge stability), while a
// causally-later delete does not (use-before-revoke) — and a causally-later
// add-fk / drop-fk never revises an old write (the FK set is read from the
// schema at the op's position). Deletes carry no FK obligation. A missing
// reference (unbound name / absent foreign table) or a non-string value voids
// the op. The supplied views are anchored at the op's position, so the FK
// recursion is guarded by the group's least-fixpoint void guard: a reference
// cycle resolves to DENY (treated as not-live), unlike the former
// greatest-fixpoint assume-live behavior.
//
// `localTargetProvided` is the entry's own sequential-cut overlay (bundles):
// a LOCAL target inserted by a sibling op of the SAME entry is live (true) and
// one deleted by a sibling op is dead (false) WITHOUT re-checking the view —
// the entry is all-or-nothing and was order-validated at write time, and
// recursing into the view would re-enter this very entry's void computation
// (a false self-cycle). undefined = not touched by the entry: check the view.
export async function evaluateRowOpFKReach(
    op: RowOpPayload,
    table: string,
    schemaView: RSchemaView,
    getTableView: (table: string) => Promise<RTableView>,
    getForeignTableView: (group: string, table: string) => Promise<RTableView | undefined>,
    localTargetProvided?: (table: string, rowId: B64Hash) => boolean | undefined,
): Promise<boolean> {
    if (op.action === 'delete') return true;

    const fks = schemaView.getFKs(table);
    const def = schemaView.getTable(table);

    for (const column of Object.keys(fks)) {
        const value = op.values[column] ?? def?.columns[column]?.default;
        if (value === undefined) continue;             // absent (nullable): unconstrained
        if (typeof value !== 'string') return false;

        const [group, targetTable] = splitTableRef(fks[column]);
        if (group !== undefined) {
            const foreign = await getForeignTableView(group, targetTable);
            if (foreign === undefined || !await foreign.hasRow(value)) return false;
        } else {
            const provided = localTargetProvided?.(targetTable, value);
            if (provided === false) return false;
            if (provided === undefined && !await (await getTableView(targetTable)).hasRow(value)) return false;
        }
    }

    return true;
}

export async function explainRowOpFKReach(
    op: RowOpPayload,
    table: string,
    schemaView: RSchemaView,
    getTableView: (table: string) => Promise<RTableView>,
    getForeignTableView: (group: string, table: string) => Promise<RTableView | undefined>,
    localTargetProvided?: (table: string, rowId: B64Hash) => boolean | undefined,
): Promise<RowOpFKFailure | undefined> {
    if (op.action === 'delete') return undefined;

    const fks = schemaView.getFKs(table);
    const def = schemaView.getTable(table);

    for (const column of Object.keys(fks)) {
        const value = op.values[column] ?? def?.columns[column]?.default;
        if (value === undefined) continue;
        const targetRef = fks[column];
        if (typeof value !== 'string') {
            return { table, action: op.action, rowId: op.rowId, column, targetRef, targetRowId: '' as B64Hash };
        }

        const [group, targetTable] = splitTableRef(targetRef);
        if (group !== undefined) {
            const foreign = await getForeignTableView(group, targetTable);
            if (foreign === undefined || !await foreign.hasRow(value)) {
                return { table, action: op.action, rowId: op.rowId, column, targetRef, targetRowId: value };
            }
        } else {
            const provided = localTargetProvided?.(targetTable, value);
            if (provided === false) {
                return { table, action: op.action, rowId: op.rowId, column, targetRef, targetRowId: value };
            }
            if (provided === undefined && !await (await getTableView(targetTable)).hasRow(value)) {
                return { table, action: op.action, rowId: op.rowId, column, targetRef, targetRowId: value };
            }
        }
    }

    return undefined;
}
