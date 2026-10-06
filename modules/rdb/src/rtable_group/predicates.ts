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
// Identity terms resolve from the op's author (admission rejects any author
// that does not verify, so op.author is trusted here; an anonymous op leaves
// them unresolved). The subject row's insert author is exposed as the readonly
// system column `rowAuthor`.
//
// Evaluation is three-valued (Kleene) over the views' three-valued reads
// (../rtable/view.ts), so a rule re-checked inside a verdict evaluation is
// undecided only when the entries it rests on leave it so (VOID_SEMANTICS.md
// section 4). `and` / `or` stop at the first deciding argument; the grammar has
// no negation, so every rule is monotone in its reads. An atom over the subject
// row ($row.<col>) is `and(the row is live, the atom)`. A cmp / like / exists
// atom whose subject value is undecided is undecided: no value is enumerated.
// The boolean entry points evaluate over top-level views, whose reads are
// always decided.

import { json } from "@hyper-hyper-space/hhs3_json";
import type { KeyId, B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { TRUE, FALSE, truthOf, allTruth, everyTruth, someTruth } from "@hyper-hyper-space/hhs3_mvt";
import type { Truth } from "@hyper-hyper-space/hhs3_mvt";

import type { RSchemaView } from "../rschema/interfaces.js";
import type { ColumnType, Predicate, PredicateContext, Operand, TableDef } from "../rschema/payload.js";
import { splitTableRef, parseRowFieldTerm } from "../rschema/payload.js";
import { evalOperand, compareOperands, resolveCmpType, likeMatch } from "../rschema/expr.js";
import type { RTableView, RowValues } from "../rtable/interfaces.js";
import type { RowOpPayload } from "../rtable/payload.js";
import { RTableViewImpl } from "../rtable/view.js";
import type { ColumnValue, TruthTableView } from "../rtable/view.js";

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

// The subject row behind $row.<col> terms, read lazily: whether the row is
// live for the op (always, for an insert) and its readonly-resolved column
// values (undefined when absent).
export type SubjectRow = {
    live(): Promise<Truth>;
    column(column: string): Promise<ColumnValue>;
};

export type TruthPredicateEnv = {
    getTableView: (table: string) => Promise<TruthTableView>;
    getForeignTableView: (group: string, table: string) => Promise<TruthTableView | undefined>;
    author?: KeyId;
    subject?: SubjectRow;
    context: PredicateContext;
    typeOf?: (column: string) => ColumnType | undefined;
};

function decided(truth: Truth, what: string): boolean {
    if (truth.status === 'undecided') throw new Error(`${what} is undecided outside the component being solved`);
    return truth.status === 'true';
}

export async function evaluatePredicate(pred: Predicate, env: PredicateEnv): Promise<boolean> {
    const subjectRow = env.subjectRow;
    return decided(await evaluatePredicateTruth(pred, {
        getTableView: async (table) => truthView(await env.getTableView(table)),
        getForeignTableView: async (group, table) => {
            const view = await env.getForeignTableView(group, table);
            return view === undefined ? undefined : truthView(view);
        },
        author: env.author,
        subject: subjectRow === undefined ? undefined : {
            live: async () => TRUE,
            column: async (column) => ({ status: 'decided', value: subjectRow[column] }),
        },
        context: env.context,
        typeOf: env.typeOf,
    }), 'a predicate');
}

// A view's three-valued reads. Views of this module have them; any other view
// answers through its public reads.
function truthView(view: RTableView): TruthTableView {
    if (view instanceof RTableViewImpl) return view;
    return {
        rowLiveness: async (rowId) => ({ truth: truthOf(await view.hasRow(rowId)), author: await view.getAuthor(rowId) }),
        columnValue: async (rowId, column) => ({ status: 'decided', value: (await view.getRow(rowId))?.values[column] }),
        existsMatching: async (where) => truthOf((await view.findRowIds(where)).length > 0),
    };
}

export async function evaluatePredicateTruth(pred: Predicate, env: TruthPredicateEnv): Promise<Truth> {
    switch (pred.p) {
        case 'true':
            return TRUE;
        case 'false':
            return FALSE;

        case 'exists':
            return existsTruth(pred, env);

        case 'cmp':
            return valueAtomTruth([pred.left, pred.right], env, (value) => {
                const l = value(pred.left);
                const r = value(pred.right);
                if (l === undefined || r === undefined) return false;
                const type = env.typeOf !== undefined ? resolveCmpType(pred.left, pred.right, env.typeOf) : undefined;
                return compareOperands(pred.cmp, l, r, type);
            });

        case 'like':
            return valueAtomTruth([pred.value, pred.pattern], env, (value) => {
                const v = value(pred.value);
                const pattern = value(pred.pattern);
                return typeof v === 'string' && typeof pattern === 'string' && likeMatch(v, pattern);
            });

        case 'and':
            return everyTruth(pred.args, (arg) => evaluatePredicateTruth(arg, env));

        case 'or':
            return someTruth(pred.args, (arg) => evaluatePredicateTruth(arg, env));
    }
}

async function existsTruth(pred: Extract<Predicate, { p: 'exists' }>, env: TruthPredicateEnv): Promise<Truth> {
    const [group, table] = splitTableRef(pred.table);
    const terms = pred.where ?? {};

    // resolve $-terms ($author / $row.<col>) in where values; an unresolvable
    // term makes the atom unprovable
    const where: { [field: string]: json.Literal } = {};
    const rowTerms = new Map<string, string>();
    for (const field of Object.keys(terms)) {
        const value = terms[field];
        if (typeof value !== 'string' || !value.startsWith('$')) {
            where[field] = value;
        } else if (value === '$author') {
            if (env.author === undefined) return FALSE;
            where[field] = env.author;
        } else {
            const column = parseRowFieldTerm(value);
            if (column === undefined) return FALSE;
            rowTerms.set(field, column);
        }
    }

    const subject = await readSubject([...new Set(rowTerms.values())], env);
    if ('status' in subject) return subject;
    for (const [field, column] of rowTerms) where[field] = subject.values.get(column)!;

    // local target resolves on a sibling; a `group.table` target resolves
    // through the bound foreign group. A missing reference (unbound name or
    // absent foreign table) makes the atom false.
    const view = group !== undefined
        ? await env.getForeignTableView(group, table)
        : await env.getTableView(table);
    if (view === undefined) return FALSE;

    return allTruth([subject.live, await view.existsMatching(where)]);
}

function evalPredicateOperand(op: Operand, env: TruthPredicateEnv, lookup: (column: string) => json.Literal | undefined): json.Literal | undefined {
    if ('lit' in op && op.lit === '$author') return env.author;
    return evalOperand(op, lookup);
}

async function valueAtomTruth(
    operands: Operand[], env: TruthPredicateEnv,
    atom: (value: (op: Operand) => json.Literal | undefined) => boolean,
): Promise<Truth> {
    const columns = new Set<string>();
    for (const op of operands) collectColumns(op, columns);

    const subject = await readSubject([...columns], env);
    if ('status' in subject) return subject;

    const lookup = (column: string) => subject.values.get(column);
    return allTruth([subject.live, truthOf(atom((op) => evalPredicateOperand(op, env, lookup)))]);
}

function collectColumns(op: Operand, into: Set<string>): void {
    if ('col' in op) into.add(op.col);
    if ('fn' in op) for (const arg of op.args) collectColumns(arg, into);
}

// The subject row's values for `columns` and whether it is live, or the truth
// that already settles an atom over them: false with no subject row, a row
// surely not live, or a missing value (every operand over it is unresolved);
// undecided when a value is.
async function readSubject(
    columns: string[], env: TruthPredicateEnv,
): Promise<{ live: Truth; values: Map<string, json.Literal> } | Truth> {
    const values = new Map<string, json.Literal>();
    if (columns.length === 0) return { live: TRUE, values };
    if (env.subject === undefined) return FALSE;

    const live = await env.subject.live();
    if (live.status === 'false') return FALSE;

    const undecided: Truth[] = [];
    for (const column of columns) {
        const read = await env.subject.column(column);
        if (read.status === 'undecided') {
            undecided.push(read);
        } else if (read.value === undefined) {
            return FALSE;
        } else {
            values.set(column, read.value);
        }
    }
    if (undecided.length > 0) return allTruth([live, ...undecided]);
    return { live, values };
}

// The subject row of a row op. Inserts: the op's values plus schema defaults
// (the row's resolved state at insert). Updates / deletes: the post-image, the
// row's resolved values overlaid with the op's writes (none for delete); a row
// that is not live fails every $row atom (positive logic).
function opSubject(
    op: RowOpPayload, table: string, def: TableDef | undefined,
    getTableView: (table: string) => Promise<TruthTableView>,
): SubjectRow {
    const fallback = (column: string) => def?.columns[column]?.default;

    if (op.action === 'insert') {
        return {
            live: async () => TRUE,
            column: async (column) => ({
                status: 'decided',
                value: column === 'rowAuthor'
                    ? op.author
                    : def?.columns[column] === undefined ? undefined : op.values[column] ?? fallback(column),
            }),
        };
    }

    let liveness: Promise<{ truth: Truth; author?: KeyId }> | undefined;
    const row = () => liveness ??= getTableView(table).then((view) => view.rowLiveness(op.rowId));
    return {
        live: async () => (await row()).truth,
        column: async (column) => {
            if (column === 'rowAuthor') return { status: 'decided', value: (await row()).author };
            if (op.action === 'update' && op.values[column] !== undefined) return { status: 'decided', value: op.values[column] };
            const read = await (await getTableView(table)).columnValue(op.rowId, column);
            return read.status === 'undecided' ? read : { status: 'decided', value: read.value ?? fallback(column) };
        },
    };
}

// Evaluate the restriction gating one row op (the and-combination of declared
// restrictions matching the op's action, or the default rule). The caller
// decides the horizon by supplying anchored views: validation passes `(at, at)`,
// while view-time voiding passes the op position observed from `from`. The
// subject row's author comes from the insert op author for inserts, and from
// the row's insert for updates/deletes. `authenticated` (the group has an
// idProvider) selects the default rule for an op the table declares none for.
export async function evaluateRowOpRestriction(
    op: RowOpPayload,
    table: string,
    schemaView: RSchemaView,
    authenticated: boolean,
    getTableView: (table: string) => Promise<RTableView>,
    getForeignTableView: (group: string, table: string) => Promise<RTableView | undefined>,
): Promise<boolean> {
    const { truth } = await explainRowOpRestriction(op, table, schemaView, authenticated,
        async (name) => truthView(await getTableView(name)),
        async (group, name) => {
            const view = await getForeignTableView(group, name);
            return view === undefined ? undefined : truthView(view);
        });
    return decided(truth, 'a row restriction');
}

export type RowOpRestrictionFailure = {
    table: string;
    action: RowOpPayload['action'];
    rowId: B64Hash;
    rule: Predicate;
};

// The restriction's truth, with the failure when it is false.
export async function explainRowOpRestriction(
    op: RowOpPayload,
    table: string,
    schemaView: RSchemaView,
    authenticated: boolean,
    getTableView: (table: string) => Promise<TruthTableView>,
    getForeignTableView: (group: string, table: string) => Promise<TruthTableView | undefined>,
): Promise<{ truth: Truth; failure?: RowOpRestrictionFailure }> {
    if (!schemaView.hasTable(table)) {
        return { truth: FALSE, failure: { table, action: op.action, rowId: op.rowId, rule: { p: 'false' } } };
    }
    const rule = schemaView.getRestriction(table, op.action, authenticated);
    const def = schemaView.getTable(table);
    const typeOf = (column: string): ColumnType | undefined => column === 'rowAuthor' ? 'string' : def?.columns[column]?.type;

    const truth = await evaluatePredicateTruth(rule, {
        getTableView, getForeignTableView, author: op.author,
        subject: opSubject(op, table, def, getTableView), context: 'row', typeOf,
    });
    return truth.status === 'false' ? { truth, failure: { table, action: op.action, rowId: op.rowId, rule } } : { truth };
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
// a target deleted CONCURRENTLY with the use voids the write (merge
// stability), while a causally-later delete does not (use-before-revoke) — and
// a causally-later add-fk / drop-fk never revises an old write (the FK set is
// read from the schema at the op's position). Deletes carry no FK obligation.
// A missing reference (unbound name / absent foreign table) or a non-string
// value voids the op. The supplied views are anchored at the op's position, so
// the FK recursion runs through the group's verdict evaluation: a reference
// cycle that nothing outside it supports is void.
//
// The result is the `and` over the FK columns, stopping at the first that
// fails, with that failure.
//
// `localTargetProvided` is the entry's own sequential-cut overlay (bundles):
// a LOCAL target inserted by a sibling op of the SAME entry is live (true) and
// one deleted by a sibling op is dead (false) WITHOUT re-checking the view —
// the entry is all-or-nothing and was order-validated at write time, and the
// views diagnosing an entry read that entry as void. undefined = not touched
// by the entry: check the view.
export async function explainRowOpFKReach(
    op: RowOpPayload,
    table: string,
    schemaView: RSchemaView,
    getTableView: (table: string) => Promise<TruthTableView>,
    getForeignTableView: (group: string, table: string) => Promise<TruthTableView | undefined>,
    localTargetProvided?: (table: string, rowId: B64Hash) => boolean | undefined,
): Promise<{ truth: Truth; failure?: RowOpFKFailure }> {
    if (op.action === 'delete') return { truth: TRUE };

    const fks = schemaView.getFKs(table);
    const def = schemaView.getTable(table);

    const undecided: Truth[] = [];
    for (const column of Object.keys(fks)) {
        const value = op.values[column] ?? def?.columns[column]?.default;
        if (value === undefined) continue;             // absent (nullable): unconstrained
        const targetRef = fks[column];
        const failure: RowOpFKFailure = {
            table, action: op.action, rowId: op.rowId, column, targetRef,
            targetRowId: typeof value === 'string' ? value : '' as B64Hash,
        };
        if (typeof value !== 'string') return { truth: FALSE, failure };

        const [group, targetTable] = splitTableRef(targetRef);
        let reach: Truth;
        if (group !== undefined) {
            const foreign = await getForeignTableView(group, targetTable);
            reach = foreign === undefined ? FALSE : (await foreign.rowLiveness(value)).truth;
        } else {
            const provided = localTargetProvided?.(targetTable, value);
            reach = provided !== undefined
                ? truthOf(provided)
                : (await (await getTableView(targetTable)).rowLiveness(value)).truth;
        }
        if (reach.status === 'false') return { truth: FALSE, failure };
        if (reach.status === 'undecided') undecided.push(reach);
    }

    return { truth: allTruth(undecided) };
}
