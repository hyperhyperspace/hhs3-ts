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
// no negation, so every rule is monotone in its reads. A cmp / like atom over
// the subject row ($row.<col>) is `and(the row is live, the atom)`, undecided
// when a subject value it names is: no value is enumerated. An exists atom
// reads its `where` tree as written and has the value of the tree's
// distributed form, the `or` of one single-leaf atom per way of satisfying the
// tree (see existsTruth). The boolean entry points evaluate over top-level
// views, whose reads are always decided.

import { json } from "@hyper-hyper-space/hhs3_json";
import type { KeyId, B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { TRUE, FALSE, truthOf, allTruth, anyTruth, everyTruth, someTruth } from "@hyper-hyper-space/hhs3_mvt";
import type { Truth, Undecided } from "@hyper-hyper-space/hhs3_mvt";

import type { RSchemaView } from "../rschema/interfaces.js";
import type { ColumnType, Predicate, PredicateContext, RuleOperand, TableDef, Where } from "../rschema/payload.js";
import { splitTableRef, parseParamTerm, parseRowFieldTerm, whereGroup } from "../rschema/payload.js";
import { evalOperand, compareOperands, resolveCmpType, likeMatch } from "../rschema/expr.js";
import type { RTableView, RowValues } from "../rtable/interfaces.js";
import type { RowOpPayload } from "../rtable/payload.js";
import { RTableViewImpl, readLeaves } from "../rtable/view.js";
import type { ColumnValue, ExistsReads, ReadLeaf, ReadWhere, TruthTableView } from "../rtable/view.js";

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
    params?: { [name: string]: json.Literal };
    context: PredicateContext;
    // Column-type lookup for the subject table (rowAuthor -> 'string'), used to
    // resolve cmp ordering by numeric value for bigint/decimal columns. Absent
    // for callers that only use eq/ne (comparison then falls back to normalized
    // string equality, which is correct for canonical carriers).
    typeOf?: (column: string) => ColumnType | undefined;
    paramTypeOf?: (param: string) => ColumnType | undefined;
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
    params?: { [name: string]: json.Literal };
    context: PredicateContext;
    typeOf?: (column: string) => ColumnType | undefined;
    paramTypeOf?: (param: string) => ColumnType | undefined;
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
        params: env.params,
        context: env.context,
        typeOf: env.typeOf,
        paramTypeOf: env.paramTypeOf,
    }), 'a predicate');
}

// A view's three-valued reads. Views of this module have them; any other view
// answers through its public reads: a `where` tree is evaluated over the sets
// of rows findRowIds returns per leaf, and a leaf whose search throws (a field
// that is not a pub column) matches no row, as it does in RTableViewImpl.
function truthView(view: RTableView): TruthTableView {
    if (view instanceof RTableViewImpl) return view;
    return {
        rowLiveness: async (rowId) => ({ truth: truthOf(await view.hasRow(rowId)), author: await view.getAuthor(rowId) }),
        columnValue: async (rowId, column) => ({ status: 'decided', value: (await view.getRow(rowId))?.values[column] }),
        existsMatching: async (where) => {
            const found = new Map<ReadLeaf, Set<B64Hash>>();
            const rowsOf = async (leaf: ReadLeaf): Promise<Set<B64Hash>> => {
                let rows = found.get(leaf);
                if (rows === undefined) {
                    try {
                        rows = leaf.aside !== undefined ? new Set() : new Set(await view.findRowIds(leaf.fields));
                    } catch {
                        rows = new Set();
                    }
                    found.set(leaf, rows);
                }
                return rows;
            };
            const intersect = (sets: Set<B64Hash>[]): Set<B64Hash> =>
                new Set([...sets[0]].filter((rowId) => sets.every((set) => set.has(rowId))));
            const unite = (sets: Set<B64Hash>[]): Set<B64Hash> => new Set(sets.flatMap((set) => [...set]));

            const matching = async (node: ReadWhere, plain: boolean): Promise<Set<B64Hash>> => {
                if ('fields' in node) return plain && node.row ? new Set() : rowsOf(node);
                const sets: Set<B64Hash>[] = [];
                for (const member of 'and' in node ? node.and : node.or) sets.push(await matching(member, plain));
                return 'and' in node ? intersect(sets) : unite(sets);
            };
            const throughRow = async (node: ReadWhere): Promise<Set<B64Hash>> => {
                if ('fields' in node) return node.row ? rowsOf(node) : new Set();
                if ('or' in node) {
                    const sets: Set<B64Hash>[] = [];
                    for (const member of node.or) sets.push(await throughRow(member));
                    return unite(sets);
                }
                const whole: Set<B64Hash>[] = [];
                for (const member of node.and) whole.push(await matching(member, false));
                const sets: Set<B64Hash>[] = [];
                for (const [i, member] of node.and.entries()) {
                    sets.push(intersect([await throughRow(member), ...whole.filter((_, k) => k !== i)]));
                }
                return unite(sets);
            };
            return {
                touched: new Map(),
                read: async (which) => truthOf((which === 'row' ? await throughRow(where) : await matching(where, which === 'plain')).size > 0),
            };
        },
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
                const type = env.typeOf !== undefined ? resolveCmpType(pred.left, pred.right, env.typeOf, env.paramTypeOf) : undefined;
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

// A leaf while its terms are resolved: `rowTerms` are the fields still waiting
// for a subject value.
type TermLeaf = ReadLeaf & { rowTerms: [field: string, column: string][] };

// The tree with the leaves `isFalse` names taken as false: an `and` with a
// false member is false, and an `or` loses its false members. undefined when
// nothing is left: the tree is false. Every leaf of the result lies on some
// way of satisfying it.
export function simplifyWhere(where: ReadWhere, isFalse: (leaf: ReadLeaf) => boolean): ReadWhere | undefined {
    if ('fields' in where) return isFalse(where) ? undefined : where;
    if ('and' in where) {
        const members: ReadWhere[] = [];
        for (const member of where.and) {
            const kept = simplifyWhere(member, isFalse);
            if (kept === undefined) return undefined;
            members.push(kept);
        }
        return { and: members };
    }
    const members = where.or.map((member) => simplifyWhere(member, isFalse)).filter((member) => member !== undefined) as ReadWhere[];
    return members.length === 0 ? undefined : members.length === 1 ? members[0] : { or: members };
}

// The three arms below are computed from a tree's structure, in one pass each,
// and read nothing. They take a tree already simplified for the leaves with no
// value, and `touched`, the leaves a pending observe touches.

// X: the ways through a leaf set aside. Undecided on the supports of the
// undecided subject values, when such a leaf is left.
export function asideArm(where: ReadWhere): Truth {
    return anyTruth(readLeaves(where).flatMap((leaf) => leaf.aside ?? []));
}

// T0: the ways through a touched leaf and no leaf that names the subject row.
// With every such leaf false, undecided on the observes that touch the touched
// leaves left.
export function touchedArm(where: ReadWhere, touched: ReadonlyMap<ReadLeaf, Undecided>): Truth {
    const plain = simplifyWhere(where, (leaf) => leaf.row || leaf.aside !== undefined);
    if (plain === undefined) return FALSE;
    return anyTruth(readLeaves(plain).flatMap((leaf) => touched.get(leaf) ?? []));
}

// T$: the ways through a touched leaf and a leaf that names the subject row,
// and no leaf set aside. With every leaf set aside false, a touched leaf
// counts when it names the subject row itself, or when an `and` above it has
// another member that holds a leaf that does. Undecided on the observes that
// touch the leaves that count.
export function touchedRowArm(where: ReadWhere, touched: ReadonlyMap<ReadLeaf, Undecided>): Truth {
    const kept = simplifyWhere(where, (leaf) => leaf.aside !== undefined);
    if (kept === undefined) return FALSE;

    const counted: Undecided[] = [];
    const holdsRow = (node: ReadWhere): boolean => readLeaves(node).some((leaf) => leaf.row);
    const walk = (node: ReadWhere, joined: boolean): void => {
        if ('fields' in node) {
            const touching = touched.get(node);
            if (touching !== undefined && (joined || node.row)) counted.push(touching);
        } else if ('or' in node) {
            for (const member of node.or) walk(member, joined);
        } else {
            for (const [i, member] of node.and.entries()) {
                walk(member, joined || node.and.some((other, k) => k !== i && holdsRow(other)));
            }
        }
    };
    walk(kept, false);
    return anyTruth(counted);
}

// The `where` tree with its $author and $param terms resolved, or undefined
// when it is false: a leaf with a term that has no value is false ($author of
// an anonymous op, an absent parameter).
function resolveWhere(where: Where, env: TruthPredicateEnv): ReadWhere | undefined {
    const group = whereGroup(where);
    if (group !== undefined) {
        const members: ReadWhere[] = [];
        for (const member of group.members) {
            const resolved = resolveWhere(member, env);
            if (resolved !== undefined) {
                members.push(resolved);
            } else if (group.op === '$and') {
                return undefined;
            }
        }
        if (group.op === '$and') return { and: members };
        return members.length === 0 ? undefined : members.length === 1 ? members[0] : { or: members };
    }

    const leaf: TermLeaf = { fields: {}, row: false, rowTerms: [] };
    for (const [field, value] of Object.entries(where)) {
        if (typeof value !== 'string' || !value.startsWith('$')) {
            leaf.fields[field] = value;
        } else if (value === '$author') {
            if (env.author === undefined) return undefined;
            leaf.fields[field] = env.author;
        } else {
            const param = parseParamTerm(value);
            if (param !== undefined) {
                const paramValue = env.params?.[param];
                if (paramValue === undefined) return undefined;
                leaf.fields[field] = paramValue;
                continue;
            }
            const column = parseRowFieldTerm(value);
            if (column === undefined) return undefined;
            leaf.rowTerms.push([field, column]);
            leaf.row = true;
        }
    }
    return leaf;
}

// An exists atom: whether some live row of the target table matches `where`.
//
// A WAY of satisfying the tree picks one member of every `$or` on its path, so
// it is a conjunction of leaves, and a single-leaf atom over it fixes its
// value at the first of these that applies:
//
//   1. a term with no value: false;
//   2. a subject row that is not live, when the way names it: false;
//   3. an undecided subject value: `and(S, undecided)` on that value's
//      support, S being the subject row's liveness;
//   4. a slot the read depends on that a pending observe writes: undecided on
//      the touching observes, inside `and(S, ...)` when the way names the
//      subject row;
//   5. otherwise the read over the candidate rows, inside `and(S, ...)` when
//      the way names the subject row.
//
// The atom has the value of the `or` of those atoms over every way, without
// listing them. The ways fall into five classes, one arm each:
//
//   X    through a leaf set aside (3);
//   T$   through a touched leaf and a leaf naming the subject row (4);
//   T0   through a touched leaf and no leaf naming the subject row (4);
//   R$   through a leaf naming the subject row, none touched or set aside (5);
//   R0   through plain leaves only (5).
//
// X, T$ and T0 come from the tree's structure (asideArm, touchedRowArm,
// touchedArm). R0 and R$ are reads of the view (ExistsReads 'plain' and
// 'row'), and R, the read of the whole tree ('all'), stands in for
// `or(R0, R$)` where S is live. They combine by the subject row's state:
//
//   not read (no leaf names it)   or(R, T0)
//   not live                      or(R0, T0)
//   live                          or(R, X, T$, T0)
//   undecided                     or(R0, T0, and(S, or(R$, X, T$)))
//
// With one leaf this is the single-leaf atom, in value and in support.
async function existsTruth(pred: Extract<Predicate, { p: 'exists' }>, env: TruthPredicateEnv): Promise<Truth> {
    const [group, table] = splitTableRef(pred.table);

    // $author and $param terms: a leaf with a term that has no value is false
    let tree = resolveWhere(pred.where ?? {}, env);
    if (tree === undefined) return FALSE;

    // The subject row, when a leaf left names it: its liveness first, then
    // the values the leaves name. A leaf naming a missing value is false, and
    // one naming an undecided value is set aside.
    let live: Truth = TRUE;
    const rowLeaves = readLeaves(tree).filter((leaf) => leaf.row) as TermLeaf[];
    if (rowLeaves.length > 0) {
        const subject = env.subject;
        live = subject === undefined ? FALSE : await subject.live();
        if (subject === undefined || live.status === 'false') {
            tree = simplifyWhere(tree, (leaf) => leaf.row);
        } else {
            const columns = new Map<string, ColumnValue>();
            const noValue = new Set<ReadLeaf>();
            for (const leaf of rowLeaves) {
                const undecided: Undecided[] = [];
                for (const [field, column] of leaf.rowTerms) {
                    let read = columns.get(column);
                    if (read === undefined) {
                        read = await subject.column(column);
                        columns.set(column, read);
                    }
                    if (read.status === 'undecided') {
                        undecided.push(read);
                    } else if (read.value === undefined) {
                        noValue.add(leaf);
                        break;
                    } else {
                        leaf.fields[field] = read.value;
                    }
                }
                if (!noValue.has(leaf) && undecided.length > 0) leaf.aside = anyTruth(undecided) as Undecided;
            }
            tree = simplifyWhere(tree, (leaf) => noValue.has(leaf));
        }
        if (tree === undefined) return FALSE;
    }

    const leaves = readLeaves(tree);
    const namesSubject = leaves.some((leaf) => leaf.row);
    const aside = asideArm(tree);

    // local target resolves on a sibling; a `group.table` target resolves
    // through the bound foreign group. With a missing reference (unbound name
    // or absent foreign table) no row matches and no leaf is touched; the
    // leaves set aside still count. A tree whose leaves are all set aside
    // reads nothing of the target.
    let reads: ExistsReads | undefined;
    if (leaves.some((leaf) => leaf.aside === undefined)) {
        const view = group !== undefined
            ? await env.getForeignTableView(group, table)
            : await env.getTableView(table);
        if (view !== undefined) reads = await view.existsMatching(tree);
    }
    const read = async (which: 'all' | 'plain' | 'row'): Promise<Truth> => reads === undefined ? FALSE : reads.read(which);
    const touched = reads?.touched ?? new Map<ReadLeaf, Undecided>();
    const touchedPlain = touchedArm(tree, touched);

    if (!namesSubject) return anyTruth([await read('all'), touchedPlain]);

    const touchedRow = touchedRowArm(tree, touched);
    if (live.status === 'true') return anyTruth([await read('all'), aside, touchedRow, touchedPlain]);

    const plain = await read('plain');
    if (plain.status === 'true') return TRUE;
    return anyTruth([plain, touchedPlain, allTruth([live, anyTruth([await read('row'), aside, touchedRow])])]);
}

// An exists atom over one view, for a `where` whose values are all literals:
// the read of the tree, undecided where a pending observe touches it.
export async function existsOnView(view: TruthTableView, where: Where): Promise<Truth> {
    return existsTruth({ p: 'exists', table: 'target', where }, {
        getTableView: async () => view,
        getForeignTableView: async () => undefined,
        context: 'object',
    });
}

function evalPredicateOperand(op: RuleOperand, env: TruthPredicateEnv, lookup: (column: string) => json.Literal | undefined): json.Literal | undefined {
    if ('lit' in op && op.lit === '$author') return env.author;
    return evalOperand(op, lookup, (param) => env.params?.[param]);
}

async function valueAtomTruth(
    operands: RuleOperand[], env: TruthPredicateEnv,
    atom: (value: (op: RuleOperand) => json.Literal | undefined) => boolean,
): Promise<Truth> {
    const columns = new Set<string>();
    for (const op of operands) collectColumns(op, columns);

    const subject = await readSubject([...columns], env);
    if ('status' in subject) return subject;

    const lookup = (column: string) => subject.values.get(column);
    return allTruth([subject.live, truthOf(atom((op) => evalPredicateOperand(op, env, lookup)))]);
}

function collectColumns(op: RuleOperand, into: Set<string>): void {
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
    params?: { [name: string]: json.Literal },
): Promise<boolean> {
    const { truth } = await explainRowOpRestriction(op, table, schemaView, authenticated,
        async (name) => truthView(await getTableView(name)),
        async (group, name) => {
            const view = await getForeignTableView(group, name);
            return view === undefined ? undefined : truthView(view);
        },
        params);
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
    params?: { [name: string]: json.Literal },
): Promise<{ truth: Truth; failure?: RowOpRestrictionFailure }> {
    if (!schemaView.hasTable(table)) {
        return { truth: FALSE, failure: { table, action: op.action, rowId: op.rowId, rule: { p: 'false' } } };
    }
    const rule = schemaView.getRestriction(table, op.action, authenticated);
    const def = schemaView.getTable(table);
    const typeOf = (column: string): ColumnType | undefined => column === 'rowAuthor' ? 'string' : def?.columns[column]?.type;
    const paramTypeOf = (param: string): ColumnType | undefined => schemaView.getParam(param)?.type;

    const truth = await evaluatePredicateTruth(rule, {
        getTableView, getForeignTableView, author: op.author,
        subject: opSubject(op, table, def, getTableView),
        params,
        context: 'row',
        typeOf,
        paramTypeOf,
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
