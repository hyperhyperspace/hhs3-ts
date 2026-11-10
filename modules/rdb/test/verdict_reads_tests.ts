import { assertTrue, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { version, TRUE, FALSE, truthOf, allTruth, anyTruth, undecidedOn } from "@hyper-hyper-space/hhs3_mvt";
import type { Truth, Undecided, Verdict, Version } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import { RSchemaImpl, rSchemaFactory } from "../src/rschema/rschema.js";
import { RTableGroupImpl, rTableGroupFactory } from "../src/rtable_group/group.js";
import type { RTableImpl } from "../src/rtable/rtable.js";
import { RTableViewImpl } from "../src/rtable/view.js";
import type { ColumnValue, ReadLeaf, ReadWhere, TableViewTarget } from "../src/rtable/view.js";
import { deriveRowId } from "../src/rtable/hash.js";
import type { Predicate, TableDef, Where } from "../src/rschema/payload.js";
import { whereGroup } from "../src/rschema/payload.js";
import type { OpVoidDetail } from "../src/rtable_group/op_void.js";
import { freshVerdictEvaluation } from "../src/rtable_group/verdict_evaluation.js";
import {
    evaluatePredicateTruth, existsOnView, simplifyWhere, asideArm, touchedArm, touchedRowArm,
} from "../src/rtable_group/predicates.js";
import type { SubjectRow } from "../src/rtable_group/predicates.js";

// The three-valued read rules of RTableViewImpl (VOID_SEMANTICS.md section 4)
// over one fixed DAG, with every entry's verdict injected through a stub
// `entryStatusIn` instead of computed by the group.
//
// Row `r` of `grants`: I inserts it (grantee alice, note n0) and I2, a
// concurrent duplicate insert, carries grantee carol. Above I, U1 writes
// grantee bob, U2 writes note n1 and D deletes the row, all concurrently.
// `merged` sees U1, U2 and I2, with D concurrent (a kill, the table honors
// concurrent deletes); `full` has D in its history.

type Status = 'live' | 'void' | 'undecided';
const NAMES = ['I', 'I2', 'U1', 'U2', 'D'] as const;
type Name = typeof NAMES[number];
type State = { [name in Name]: Status };

type Fixture = {
    table: RTableImpl;
    rowId: B64Hash;
    hashes: { [name in Name]: B64Hash };
    merged: Version;
    full: Version;
};

const grantsTable: TableDef = {
    name: 'grants',
    columns: {
        grantee: { type: 'string', pub: true },
        note: { type: 'string', pub: true, nullable: true },
        level: { type: 'string', pub: true, default: 'low' },
    },
    concurrentDeletes: true,
    restrictions: [{ on: 'all', rule: { p: 'true' } }],
};

async function createFixture(): Promise<Fixture> {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);

    const creator = await createIdentity(SIGNING_ED25519, createBasicCrypto().hash(HASH_SHA256));
    const schemaInit = await RSchemaImpl.create({
        name: 'verdictreads:schema',
        creators: [{ keyId: creator.keyId, publicKey: creator.publicKey }],
        tables: [grantsTable],
    });
    const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
    const pinned = await (await schema.getScopedDag()).getFrontier();
    const groupInit = await RTableGroupImpl.create({
        name: 'verdictreads', seed: 'verdictreads', schemaRef: schema.getId(), schemaVersion: pinned,
    });
    const group = (await ctx.createObject(groupInit)) as RTableGroupImpl;
    const table = await group.getTable('grants');

    const base = await (await group.getScopedDag()).getFrontier();
    const rowId = deriveRowId('r');
    const I = await table.insert('r', { grantee: 'alice', note: 'n0' }, undefined, base);
    const I2 = await table.insert('r', { grantee: 'carol' }, undefined, base);
    const U1 = await table.update(rowId, { grantee: 'bob' }, undefined, version(I));
    const U2 = await table.update(rowId, { note: 'n1' }, undefined, version(I));
    const D = await table.delete(rowId, undefined, version(I));

    return {
        table, rowId, hashes: { I, I2, U1, U2, D },
        merged: version(U1, U2, I2),
        full: version(U1, U2, I2, D),
    };
}

function viewIn(fixture: Fixture, state: State, at: Version, from: Version, exclude?: B64Hash): RTableViewImpl {
    const statusOf = new Map<B64Hash, Status>();
    for (const name of NAMES) statusOf.set(fixture.hashes[name], state[name]);

    const target = Object.create(fixture.table) as TableViewTarget;
    target.entryStatusIn = async (_evaluation, hash): Promise<Verdict<OpVoidDetail>> => {
        const status = statusOf.get(hash) ?? 'live';
        if (status === 'live') return { status: 'live' };
        if (status === 'void') return { status: 'void', reason: { kind: 'undecided-cycle' } };
        return { status: 'undecided', support: new Set([hash]) };
    };
    return new RTableViewImpl(target, at, from, freshVerdictEvaluation(), exclude);
}

function stateOf(partial: Partial<State>): State {
    return { I: 'void', I2: 'void', U1: 'void', U2: 'void', D: 'void', ...partial };
}

// A read's result: a decided value (rendered as a string) or undecided with
// its support.
type Result = { decided: true; value: string } | { decided: false; support: ReadonlySet<string> };

function fromTruth(truth: Truth): Result {
    return truth.status === 'undecided' ? { decided: false, support: truth.support } : { decided: true, value: truth.status };
}

function fromColumn(read: ColumnValue): Result {
    return read.status === 'undecided' ? { decided: false, support: read.support } : { decided: true, value: 'v:' + String(read.value) };
}

// The subject row of an update / delete of `r` with no writes of its own (the
// post-image without overlay): its liveness, and its values with defaults.
function subjectOf(view: RTableViewImpl, rowId: B64Hash) {
    return {
        live: async () => (await view.rowLiveness(rowId)).truth,
        column: async (column: string): Promise<ColumnValue> => {
            const read = await view.columnValue(rowId, column);
            if (read.status === 'undecided') return read;
            return { status: 'decided', value: read.value ?? grantsTable.columns[column]?.default };
        },
    };
}

async function atomOn(view: RTableViewImpl, rowId: B64Hash, pred: Predicate): Promise<Truth> {
    return evaluatePredicateTruth(pred, {
        getTableView: async () => view,
        getForeignTableView: async () => undefined,
        subject: subjectOf(view, rowId),
        context: 'row',
        typeOf: (column) => grantsTable.columns[column]?.type,
    });
}

const GRANTEE_IS_CAROL: Predicate = { p: 'cmp', cmp: 'eq', left: { col: 'grantee' }, right: { lit: 'carol' } };
const NOTE_LIKE_N: Predicate = { p: 'like', value: { col: 'note' }, pattern: { lit: 'n%' } };
const SAME_GRANTEE_EXISTS: Predicate = { p: 'exists', table: 'grants', where: { grantee: '$row.grantee' } };

// The reference: today's two-valued reads over the fixture, written out.
type Reference = {
    live(isLive: (name: Name) => boolean): boolean;
    column(column: string, isLive: (name: Name) => boolean): string | undefined;
};

function referenceFor(fixture: Fixture): Reference {
    const winner = (members: [Name, string][]): string | undefined => {
        let best: [B64Hash, string] | undefined;
        for (const [name, value] of members) {
            const hash = fixture.hashes[name];
            if (best === undefined || hash > best[0]) best = [hash, value];
        }
        return best?.[1];
    };
    return {
        // `merged` kills with D, `full` deletes with D: the same formula
        live: (isLive) => (isLive('I') || isLive('I2')) && !isLive('D'),
        column: (column, isLive) => {
            const members: [Name, string][] = [];
            if (column === 'grantee') {
                if (isLive('U1')) members.push(['U1', 'bob']);
                else if (isLive('I')) members.push(['I', 'alice']);
                if (isLive('I2')) members.push(['I2', 'carol']);
            } else if (column === 'note') {
                if (isLive('U2')) members.push(['U2', 'n1']);
                else if (isLive('I')) members.push(['I', 'n0']);
            }
            return winner(members);
        },
    };
}

type ReadRule = {
    name: string;
    run(view: RTableViewImpl, rowId: B64Hash): Promise<Result>;
    reference(ref: Reference, isLive: (name: Name) => boolean): string;
};

// r is the only row, so a `where` tree holds when r is live and its resolved
// values satisfy the tree.
function existsRule(where: Where): ReadRule {
    const holds = (node: Where, ref: Reference, isLive: (name: Name) => boolean): boolean => {
        const group = whereGroup(node);
        if (group !== undefined) {
            const members = group.members.map((member) => holds(member, ref, isLive));
            return group.op === '$and' ? members.every((m) => m) : members.some((m) => m);
        }
        return Object.entries(node).every(([field, value]) =>
            (ref.column(field, isLive) ?? grantsTable.columns[field]?.default) === value);
    };
    return {
        name: 'exists ' + JSON.stringify(where),
        run: async (view) => fromTruth(await existsOnView(view, where)),
        reference: (ref, isLive) => String(ref.live(isLive) && holds(where, ref, isLive)),
    };
}

const READ_RULES: ReadRule[] = [
    {
        name: 'row liveness',
        run: async (view, rowId) => fromTruth((await view.rowLiveness(rowId)).truth),
        reference: (ref, isLive) => String(ref.live(isLive)),
    },
    {
        name: 'grantee column',
        run: async (view, rowId) => fromColumn(await view.columnValue(rowId, 'grantee')),
        reference: (ref, isLive) => 'v:' + String(ref.column('grantee', isLive)),
    },
    {
        name: 'note column',
        run: async (view, rowId) => fromColumn(await view.columnValue(rowId, 'note')),
        reference: (ref, isLive) => 'v:' + String(ref.column('note', isLive)),
    },
    existsRule({ grantee: 'alice' }),
    existsRule({ grantee: 'bob' }),
    existsRule({ grantee: 'carol' }),
    existsRule({ grantee: 'bob', note: 'n1' }),
    existsRule({ note: 'n0', level: 'low' }),
    // trees: an `$or` of two leaves, an `$and` of a leaf and an `$or`, and an
    // `$or` with a member at its column's default (every row carries it)
    existsRule({ $or: [{ grantee: 'alice' }, { grantee: 'carol' }] }),
    existsRule({ $or: [{ grantee: 'bob', note: 'n1' }, { grantee: 'carol' }] }),
    existsRule({ $and: [{ note: 'n1' }, { $or: [{ grantee: 'bob' }, { grantee: 'carol' }] }] }),
    existsRule({ $and: [{ grantee: 'bob' }, { $or: [{ note: 'n0' }, { level: 'low' }] }] }),
    existsRule({ $or: [{ level: 'high' }, { $and: [{ grantee: 'alice' }, { $or: [{ note: 'n0' }, { note: 'n1' }] }] }] }),
    {
        name: 'grantee == carol',
        run: async (view, rowId) => fromTruth(await atomOn(view, rowId, GRANTEE_IS_CAROL)),
        reference: (ref, isLive) => String(ref.live(isLive) && ref.column('grantee', isLive) === 'carol'),
    },
    {
        name: "note like 'n%'",
        run: async (view, rowId) => fromTruth(await atomOn(view, rowId, NOTE_LIKE_N)),
        reference: (ref, isLive) => {
            const note = ref.column('note', isLive);
            return String(ref.live(isLive) && note !== undefined && note.startsWith('n'));
        },
    },
    {
        name: 'exists grantee = $row.grantee',
        run: async (view, rowId) => fromTruth(await atomOn(view, rowId, SAME_GRANTEE_EXISTS)),
        // r is the only row, so it matches itself whenever it is live
        reference: (ref, isLive) => String(ref.live(isLive)),
    },
];

function allStates(): State[] {
    let states: Partial<State>[] = [{}];
    for (const name of NAMES) {
        states = states.flatMap((s) => (['live', 'void', 'undecided'] as Status[]).map((status) => ({ ...s, [name]: status })));
    }
    return states as State[];
}

function keyOf(state: State): string {
    return NAMES.map((name) => state[name][0]).join('');
}

function completions(state: State): State[] {
    let out: State[] = [state];
    for (const name of NAMES) {
        if (state[name] !== 'undecided') continue;
        out = out.flatMap((s) => [{ ...s, [name]: 'live' as Status }, { ...s, [name]: 'void' as Status }]);
    }
    return out;
}

function show(result: Result): string {
    return result.decided ? result.value : `undecided on {${[...result.support].join(',')}}`;
}

async function assertGuarantees(fixture: Fixture, at: Version, from: Version, label: string): Promise<void> {
    const ref = referenceFor(fixture);
    const states = allStates();
    const results = new Map<string, Result[]>();
    for (const state of states) {
        const view = viewIn(fixture, state, at, from);
        const row: Result[] = [];
        for (const rule of READ_RULES) row.push(await rule.run(view, fixture.rowId));
        results.set(keyOf(state), row);
    }

    const decidedDespite = READ_RULES.map(() => 0);
    const undecidedCount = READ_RULES.map(() => 0);
    for (const state of states) {
        const row = results.get(keyOf(state))!;
        const undecidedHashes = new Set(NAMES.filter((n) => state[n] === 'undecided').map((n) => fixture.hashes[n]));
        for (const [i, rule] of READ_RULES.entries()) {
            const result = row[i];
            const where = `${label}, ${rule.name}, state ${keyOf(state)}`;

            if (undecidedHashes.size === 0) {
                const expected = rule.reference(ref, (n) => state[n] === 'live');
                assertTrue(result.decided && result.value === expected,
                    `exact: ${where}: expected ${expected}, got ${show(result)}`);
                continue;
            }

            if (result.decided) decidedDespite[i]++; else undecidedCount[i]++;
            if (result.decided) {
                for (const completion of completions(state)) {
                    const expected = rule.reference(ref, (n) => completion[n] === 'live');
                    assertEquals(result.value, expected, `sound: ${where}: completion ${keyOf(completion)}`);
                }
            } else {
                assertTrue(result.support.size > 0 && [...result.support].every((h) => undecidedHashes.has(h)),
                    `support: ${where}: ${show(result)} rests on entries that are not undecided`);
            }

            if (!result.decided) continue;
            for (const name of NAMES) {
                if (state[name] !== 'undecided') continue;
                for (const status of ['live', 'void'] as Status[]) {
                    const refined = results.get(keyOf({ ...state, [name]: status }))![i];
                    assertTrue(refined.decided && refined.value === result.value,
                        `monotone: ${where}: deciding ${name} ${status} gives ${show(refined)}, was ${show(result)}`);
                }
            }
        }
    }

    for (const [i, rule] of READ_RULES.entries()) {
        assertTrue(decidedDespite[i] > 0 && undecidedCount[i] > 0,
            `${label}, ${rule.name}: some state decides it despite undecided entries and some leaves it undecided`);
    }
}

// ---------------------------------------------------------------------------
// The model of the exists rule over a `where` tree (HIP-21).
//
// A WAY of satisfying a tree picks one member of every `$or` on its path, so
// it is a conjunction of leaves. The DISTRIBUTED FORM of a tree is the `or` of
// one single-leaf atom per way, each read in the order a single-leaf atom is
// read: a term with no value, the subject row, the subject's values, the
// touched slots, the read. The rule reads the tree as written. The model
// states both over abstract inputs and compares them in every state: the rule
// must have the distributed form's value, on a support it contains.
// ---------------------------------------------------------------------------

// How a leaf is marked once its terms are resolved:
//   plain          no subject-row term, read on the target;
//   row            names a subject-row term, read on the target;
//   aside          names an undecided subject value (set aside);
//   novalue        a term of it has no value: the leaf is false;
//   touched        a pending import writes a slot it reads;
//   touched-row    the same, and it names a subject-row term;
//   touched-aside  set aside, and a pending import writes its slot. Set aside
//                  comes first, so its slots are never checked.
const LEAF_KINDS = ['novalue', 'plain', 'row', 'aside', 'touched', 'touched-row', 'touched-aside'] as const;
type LeafKind = typeof LEAF_KINDS[number];

const namesRow = (k: LeafKind) => k === 'row' || k === 'aside' || k === 'touched-row' || k === 'touched-aside';
const isAside = (k: LeafKind) => k === 'aside' || k === 'touched-aside';
const isTouched = (k: LeafKind) => k === 'touched' || k === 'touched-row';
const isRead = (k: LeafKind) => k === 'plain' || k === 'row';

// A tree over leaves named by their index.
type Shape = number | { and: Shape[] } | { or: Shape[] };

// The inputs of one state. `value[i][r]` is leaf i on target row r (false on a
// row that is not one of its candidates); `asideOn[i]` the support of leaf i's
// undecided subject values; `touchedOn[i]` the imports that touch leaf i.
type ModelState = {
    kinds: LeafKind[];
    value: Truth[][];
    rowLive: Truth[];
    subject: Truth;
    asideOn: Truth[];
    touchedOn: Truth[];
};

function waysOf(shape: Shape): number[][] {
    if (typeof shape === 'number') return [[shape]];
    if ('or' in shape) return shape.or.flatMap(waysOf);
    let ways: number[][] = [[]];
    for (const member of shape.and) {
        const of = waysOf(member);
        ways = ways.flatMap((a) => of.map((b) => [...a, ...b]));
    }
    return ways;
}

function leavesOf(shape: Shape): number[] {
    if (typeof shape === 'number') return [shape];
    return ('or' in shape ? shape.or : shape.and).flatMap(leavesOf);
}

// The tree with the leaves `isFalse` names taken as false: an `and` with a
// false member is false, an `or` loses its false members. undefined = false.
function simplifyShape(shape: Shape, isFalse: (leaf: number) => boolean): Shape | undefined {
    if (typeof shape === 'number') return isFalse(shape) ? undefined : shape;
    if ('and' in shape) {
        const members = shape.and.map((m) => simplifyShape(m, isFalse));
        return members.some((m) => m === undefined) ? undefined : { and: members as Shape[] };
    }
    const members = shape.or.map((m) => simplifyShape(m, isFalse)).filter((m) => m !== undefined) as Shape[];
    return members.length === 0 ? undefined : members.length === 1 ? members[0] : { or: members };
}

// The `or` of one single-leaf atom per way, in today's order.
function distributedForm(shape: Shape, st: ModelState): Truth {
    return anyTruth(waysOf(shape).map((way) => {
        if (way.some((i) => st.kinds[i] === 'novalue')) return FALSE;
        let live: Truth = TRUE;
        if (way.some((i) => namesRow(st.kinds[i]))) {
            if (st.subject.status === 'false') return FALSE;
            const aside = way.filter((i) => isAside(st.kinds[i]));
            if (aside.length > 0) return allTruth([st.subject, ...aside.map((i) => st.asideOn[i])]);
            live = st.subject;
        }
        const touched = way.filter((i) => isTouched(st.kinds[i]));
        if (touched.length > 0) return allTruth([live, anyTruth(touched.map((i) => st.touchedOn[i]))]);
        const read = anyTruth(st.rowLive.map((rowLive, r) => allTruth([...way.map((i) => st.value[i][r]), rowLive])));
        return allTruth([live, read]);
    }));
}

function kleeneOf(shape: Shape, leaf: (i: number) => Truth): Truth {
    if (typeof shape === 'number') return leaf(shape);
    return 'and' in shape ? allTruth(shape.and.map((m) => kleeneOf(m, leaf))) : anyTruth(shape.or.map((m) => kleeneOf(m, leaf)));
}

// [the tree, its matches through a leaf `through` names]: a leaf matches
// through itself, an `or` when a member does, an `and` when one member does
// and its other members match.
function throughOf(shape: Shape, leaf: (i: number) => Truth, through: (i: number) => boolean): [Truth, Truth] {
    if (typeof shape === 'number') {
        const value = leaf(shape);
        return [value, through(shape) ? value : FALSE];
    }
    if ('or' in shape) {
        const members = shape.or.map((m) => throughOf(m, leaf, through));
        return [anyTruth(members.map((m) => m[0])), anyTruth(members.map((m) => m[1]))];
    }
    const members = shape.and.map((m) => throughOf(m, leaf, through));
    return [
        allTruth(members.map((m) => m[0])),
        anyTruth(members.map((m, j) => allTruth([m[1], ...members.filter((_, k) => k !== j).map((o) => o[0])]))),
    ];
}

type Arms = { aside: Truth; touched: Truth; touchedRow: Truth };

// X, T0 and T$ as the rule states them, one pass each over a tree whose
// novalue leaves are already simplified away.
function structuralArms(tree: Shape, st: Pick<ModelState, 'kinds' | 'asideOn' | 'touchedOn'>): Arms {
    const aside = anyTruth(leavesOf(tree).filter((i) => isAside(st.kinds[i])).map((i) => st.asideOn[i]));

    const noRow = simplifyShape(tree, (i) => namesRow(st.kinds[i]));
    const touched = noRow === undefined
        ? FALSE
        : anyTruth(leavesOf(noRow).filter((i) => isTouched(st.kinds[i])).map((i) => st.touchedOn[i]));

    const noAside = simplifyShape(tree, (i) => isAside(st.kinds[i]));
    const counted: number[] = [];
    const holdsRow = (shape: Shape) => leavesOf(shape).some((i) => namesRow(st.kinds[i]));
    const walk = (shape: Shape, joined: boolean): void => {
        if (typeof shape === 'number') {
            if (isTouched(st.kinds[shape]) && (joined || namesRow(st.kinds[shape]))) counted.push(shape);
            return;
        }
        const members = 'and' in shape ? shape.and : shape.or;
        for (const member of members) {
            walk(member, joined || ('and' in shape && members.some((other) => other !== member && holdsRow(other))));
        }
    };
    if (noAside !== undefined) walk(noAside, false);
    return { aside, touched, touchedRow: anyTruth(counted.map((i) => st.touchedOn[i])) };
}

// The same arms by their definitions, listing every way of the tree.
function armsByWays(tree: Shape, st: Pick<ModelState, 'kinds' | 'asideOn' | 'touchedOn'>): Arms {
    const ways = waysOf(tree);
    const over = (pick: (way: number[]) => boolean, of: (i: number) => Truth | undefined): Truth =>
        anyTruth(ways.filter(pick).flatMap((way) => way.map(of).filter((t) => t !== undefined) as Truth[]));
    const kind = (i: number) => st.kinds[i];
    return {
        aside: over((way) => way.some((i) => isAside(kind(i))), (i) => isAside(kind(i)) ? st.asideOn[i] : undefined),
        touched: over(
            (way) => way.some((i) => isTouched(kind(i))) && !way.some((i) => namesRow(kind(i))),
            (i) => isTouched(kind(i)) ? st.touchedOn[i] : undefined),
        touchedRow: over(
            (way) => way.some((i) => isTouched(kind(i))) && way.some((i) => namesRow(kind(i))) && !way.some((i) => isAside(kind(i))),
            (i) => isTouched(kind(i)) ? st.touchedOn[i] : undefined),
    };
}

// The rule: terms, touched leaves, the reads, and the table that combines
// them with the subject row.
function ruleForm(shape: Shape, st: ModelState): Truth {
    const tree = simplifyShape(shape, (i) => st.kinds[i] === 'novalue');
    if (tree === undefined) return FALSE;
    const arms = structuralArms(tree, st);

    const read = (row: (r: number) => Truth): Truth => anyTruth(st.rowLive.map((rowLive, r) => allTruth([row(r), rowLive])));
    const whole = (r: number) => (i: number) => isRead(st.kinds[i]) ? st.value[i][r] : FALSE;
    const plain = (r: number) => (i: number) => st.kinds[i] === 'plain' ? st.value[i][r] : FALSE;
    const R = () => read((r) => kleeneOf(tree, whole(r)));
    const R0 = () => read((r) => kleeneOf(tree, plain(r)));
    const RS = () => read((r) => throughOf(tree, whole(r), (i) => st.kinds[i] === 'row')[1]);

    if (!leavesOf(tree).some((i) => namesRow(st.kinds[i]))) return anyTruth([R(), arms.touched]);
    if (st.subject.status === 'false') return anyTruth([R0(), arms.touched]);
    if (st.subject.status === 'true') return anyTruth([R(), arms.aside, arms.touchedRow, arms.touched]);
    return anyTruth([R0(), arms.touched, allTruth([st.subject, anyTruth([RS(), arms.aside, arms.touchedRow])])]);
}

// X, T0 and T$ as predicates.ts computes them: the marking as a tree of read
// leaves, with the leaves that have no value simplified away first.
function codeArms(shape: Shape, st: Pick<ModelState, 'kinds' | 'asideOn' | 'touchedOn'>): Arms {
    const touched = new Map<ReadLeaf, Undecided>();
    const noValue = new Set<ReadLeaf>();
    const build = (node: Shape): ReadWhere => {
        if (typeof node !== 'number') return 'and' in node ? { and: node.and.map(build) } : { or: node.or.map(build) };
        const kind = st.kinds[node];
        const leaf: ReadLeaf = { fields: {}, row: namesRow(kind) };
        if (isAside(kind)) leaf.aside = st.asideOn[node] as Undecided;
        if (isTouched(kind)) touched.set(leaf, st.touchedOn[node] as Undecided);
        if (kind === 'novalue') noValue.add(leaf);
        return leaf;
    };
    const tree = simplifyWhere(build(shape), (leaf) => noValue.has(leaf));
    if (tree === undefined) return { aside: FALSE, touched: FALSE, touchedRow: FALSE };
    return { aside: asideArm(tree), touched: touchedArm(tree, touched), touchedRow: touchedRowArm(tree, touched) };
}

function sameTruth(a: Truth, b: Truth): boolean {
    if (a.status !== b.status) return false;
    if (a.status !== 'undecided' || b.status !== 'undecided') return true;
    return a.support.size === b.support.size && [...a.support].every((s) => b.support.has(s));
}

function showTruth(t: Truth): string {
    return t.status === 'undecided' ? `undecided on {${[...t.support].sort().join(',')}}` : t.status;
}

const L = (i: number): Shape => i;
const MODEL_SHAPES: { name: string; shape: Shape; leaves: number; rows: number; full?: boolean }[] = [
    { name: 'a', shape: L(0), leaves: 1, rows: 2 },
    { name: 'a | b', shape: { or: [L(0), L(1)] }, leaves: 2, rows: 2 },
    { name: 'a | b | c', shape: { or: [L(0), L(1), L(2)] }, leaves: 3, rows: 2, full: true },
    { name: 'a & (b | c)', shape: { and: [L(0), { or: [L(1), L(2)] }] }, leaves: 3, rows: 2, full: true },
    { name: 'a | (b & (c | d))', shape: { or: [L(0), { and: [L(1), { or: [L(2), L(3)] }] }] }, leaves: 4, rows: 1, full: true },
    { name: '(a | b) & (c | d)', shape: { and: [{ or: [L(0), L(1)] }, { or: [L(2), L(3)] }] }, leaves: 4, rows: 1, full: true },
    { name: 'a & (b | c | d)', shape: { and: [L(0), { or: [L(1), L(2), L(3)] }] }, leaves: 4, rows: 1, full: true },
];

// EXISTS_MODEL=full runs every state of every shape, for a longer run by hand.
// The suite runs the shapes of up to two leaves in full, and the larger ones
// on one target row with every third state.
const MODEL_FULL = process.env['EXISTS_MODEL'] === 'full';

function* productOf<T>(lists: T[][]): Generator<T[]> {
    const pick = lists.map(() => 0);
    if (lists.some((list) => list.length === 0)) return;
    while (true) {
        yield lists.map((list, i) => list[pick[i]]);
        let at = lists.length - 1;
        while (at >= 0 && ++pick[at] === lists[at].length) pick[at--] = 0;
        if (at < 0) return;
    }
}

const three = (name: string): Truth[] => [TRUE, FALSE, undecidedOn([name])];

type ModelTotals = { states: number; smaller: number };

// Every marking of the shape's leaves and, for each, every value of the
// inputs: the rule against the distributed form.
function checkModelShape(entry: typeof MODEL_SHAPES[number]): ModelTotals {
    const { shape, leaves } = entry;
    const sampled = !MODEL_FULL && entry.full === true;
    const rows = sampled ? 1 : entry.rows;
    const totals = { states: 0, smaller: 0 };
    const indices = [...Array(leaves).keys()];
    const asideOn = indices.map((i) => undecidedOn(['v' + i]));
    const touchedOn = indices.map((i) => undecidedOn(['o' + i]));
    const rowLives = [...productOf([...Array(rows).keys()].map((r) => three('L' + r)))];
    const subjects = three('S');
    let nth = 0;

    for (const kinds of productOf(indices.map(() => [...LEAF_KINDS]))) {
        const where = `${entry.name}, leaves ${kinds.join(' / ')}`;

        // the arms read nothing, so they are checked once per marking
        const tree = simplifyShape(shape, (i) => kinds[i] === 'novalue');
        if (tree !== undefined) {
            const marks = { kinds, asideOn, touchedOn };
            const got = structuralArms(tree, marks);
            const want = armsByWays(tree, marks);
            for (const arm of ['aside', 'touched', 'touchedRow'] as const) {
                assertTrue(sameTruth(got[arm], want[arm]),
                    `arm ${arm}: ${where}: one pass gives ${showTruth(got[arm])}, the ways give ${showTruth(want[arm])}`);
            }
        }

        // the functions existsTruth uses, on the same marking
        const code = codeArms(shape, { kinds, asideOn, touchedOn });
        const byWays = tree === undefined ? { aside: FALSE, touched: FALSE, touchedRow: FALSE } : armsByWays(tree, { kinds, asideOn, touchedOn });
        for (const arm of ['aside', 'touched', 'touchedRow'] as const) {
            assertTrue(sameTruth(code[arm], byWays[arm]),
                `code arm ${arm}: ${where}: the code gives ${showTruth(code[arm])}, the ways give ${showTruth(byWays[arm])}`);
        }

        // a leaf's value is enumerated when a single-leaf atom could read it
        const valued = indices.filter((i) => kinds[i] !== 'novalue' && !isAside(kinds[i]));
        const cells = valued.flatMap((i) => [...Array(rows).keys()].map((r) => three(`r${i}.${r}`)));
        for (const cell of productOf(cells)) {
            const value = indices.map(() => Array<Truth>(rows).fill(FALSE));
            valued.forEach((i, j) => { for (let r = 0; r < rows; r++) value[i][r] = cell[j * rows + r]; });
            for (const rowLive of rowLives) {
                for (const subject of subjects) {
                    if (sampled && nth++ % 3 !== 0) continue;
                    totals.states++;
                    const st: ModelState = { kinds, value, rowLive, subject, asideOn, touchedOn };
                    const want = distributedForm(shape, st);
                    const got = ruleForm(shape, st);
                    if (leaves === 1) {
                        assertTrue(sameTruth(got, want),
                            `one leaf: ${where}: the rule gives ${showTruth(got)}, today's atom ${showTruth(want)}`);
                        continue;
                    }
                    assertTrue(got.status === want.status,
                        `value: ${where}: the rule gives ${showTruth(got)}, the distributed form ${showTruth(want)}`);
                    if (got.status === 'undecided' && want.status === 'undecided') {
                        assertTrue([...got.support].every((s) => want.support.has(s)),
                            `support: ${where}: the rule rests on ${showTruth(got)}, the distributed form on ${showTruth(want)}`);
                        if (got.support.size < want.support.size) totals.smaller++;
                    }
                }
            }
        }
    }
    return totals;
}

// ---------------------------------------------------------------------------
// [VREAD07]: a where tree with a subject-row term, read by the code in every
// state, against the model's distributed form.
//
// Two rows of `grants`. The subject row `r`: Ir inserts it (grantee alice),
// Ur writes grantee bob and Dr deletes it, both above Ir. The witness row `w`:
// Iw inserts it (grantee bob, note k), Uw writes note z and Dw deletes it, both
// above Iw. The view sees the updates, with both deletes concurrent.
// ---------------------------------------------------------------------------

const TWO_ROW_NAMES = ['Ir', 'Ur', 'Dr', 'Iw', 'Uw', 'Dw'] as const;
type TwoRowName = typeof TWO_ROW_NAMES[number];

type TwoRowFixture = {
    table: RTableImpl;
    r: B64Hash;
    w: B64Hash;
    hashes: { [name in TwoRowName]: B64Hash };
    at: Version;
    from: Version;
};

// The rows some write of which carries a value, whatever the verdicts.
const TWO_ROW_CARRIERS: { [field: string]: { [value: string]: ('r' | 'w')[] } } = {
    grantee: { alice: ['r'], bob: ['r', 'w'] },
    note: { k: ['w'], z: ['w'] },
};

async function createTwoRowFixture(): Promise<TwoRowFixture> {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);

    const creator = await createIdentity(SIGNING_ED25519, createBasicCrypto().hash(HASH_SHA256));
    const schema = (await ctx.createObject(await RSchemaImpl.create({
        name: 'verdictreads:tworows',
        creators: [{ keyId: creator.keyId, publicKey: creator.publicKey }],
        tables: [grantsTable],
    }))) as RSchemaImpl;
    const group = (await ctx.createObject(await RTableGroupImpl.create({
        name: 'verdictreads-tworows', seed: 'verdictreads-tworows',
        schemaRef: schema.getId(), schemaVersion: await (await schema.getScopedDag()).getFrontier(),
    }))) as RTableGroupImpl;
    const table = await group.getTable('grants');

    const base = await (await group.getScopedDag()).getFrontier();
    const r = deriveRowId('r');
    const w = deriveRowId('w');
    const Ir = await table.insert('r', { grantee: 'alice' }, undefined, base);
    const Ur = await table.update(r, { grantee: 'bob' }, undefined, version(Ir));
    const Dr = await table.delete(r, undefined, version(Ir));
    const Iw = await table.insert('w', { grantee: 'bob', note: 'k' }, undefined, base);
    const Uw = await table.update(w, { note: 'z' }, undefined, version(Iw));
    const Dw = await table.delete(w, undefined, version(Iw));
    return { table, r, w, hashes: { Ir, Ur, Dr, Iw, Uw, Dw }, at: version(Ur, Uw), from: version(Ur, Uw, Dr, Dw) };
}

function twoRowView(fixture: TwoRowFixture, state: { [name in TwoRowName]: Status }): RTableViewImpl {
    const statusOf = new Map<B64Hash, Status>();
    for (const name of TWO_ROW_NAMES) statusOf.set(fixture.hashes[name], state[name]);
    const target = Object.create(fixture.table) as TableViewTarget;
    target.entryStatusIn = async (_evaluation, hash): Promise<Verdict<OpVoidDetail>> => {
        const status = statusOf.get(hash) ?? 'live';
        if (status === 'live') return { status: 'live' };
        if (status === 'void') return { status: 'void', reason: { kind: 'undecided-cycle' } };
        return { status: 'undecided', support: new Set([hash]) };
    };
    return new RTableViewImpl(target, fixture.at, fixture.from, freshVerdictEvaluation());
}

// One tree: its payload, and the same tree for the model, with each leaf's
// fields in the model's leaf order.
type TreeCase = { name: string; where: Where; shape: Shape; leaves: { [field: string]: string }[] };

const ROW_ARM = { grantee: '$row.grantee' };
const NOTE_ARM = { note: 'k' };
const LEVEL_LEAF = { level: 'low' };
const TREE_CASES: TreeCase[] = [
    // the subject's grantee in one arm, a plain arm beside it
    { name: 'row | plain', where: { $or: [ROW_ARM, NOTE_ARM] }, shape: { or: [0, 1] }, leaves: [ROW_ARM, NOTE_ARM] },
    // the same under an `$and` with a leaf at its column's default
    { name: 'default & (row | plain)', where: { $and: [LEVEL_LEAF, { $or: [ROW_ARM, NOTE_ARM] }] },
      shape: { and: [0, { or: [1, 2] }] }, leaves: [LEVEL_LEAF, ROW_ARM, NOTE_ARM] },
    // the subject's grantee and a plain field in one leaf, beside a plain arm
    { name: '(row, plain) | plain', where: { $or: [{ grantee: '$row.grantee', note: 'z' }, NOTE_ARM] },
      shape: { or: [0, 1] }, leaves: [{ grantee: '$row.grantee', note: 'z' }, NOTE_ARM] },
];

// The model's inputs for a tree in one state, from the view's row and column
// reads: the subject row is r, and the target rows are r and w.
async function modelStateOf(fixture: TwoRowFixture, view: RTableViewImpl, tree: TreeCase): Promise<ModelState> {
    const subject = subjectOf(view, fixture.r);
    const live = await subject.live();
    const rows: [('r' | 'w'), B64Hash][] = [['r', fixture.r], ['w', fixture.w]];

    const kinds: LeafKind[] = [];
    const asideOn: Truth[] = [];
    const value: Truth[][] = [];
    for (const leaf of tree.leaves) {
        // resolve the leaf's subject-row terms, as a single-leaf atom does
        const fields: { [field: string]: string } = {};
        const undecided: Truth[] = [];
        let noValue = false;
        let row = false;
        for (const [field, term] of Object.entries(leaf)) {
            if (!term.startsWith('$row.')) {
                fields[field] = term;
                continue;
            }
            row = true;
            if (live.status === 'false') continue;   // the subject's values are not read
            const read = await subject.column(term.substring('$row.'.length));
            if (read.status === 'undecided') undecided.push(read);
            else if (read.value === undefined) noValue = true;
            else fields[field] = read.value as string;
        }
        const kind: LeafKind = noValue ? 'novalue' : undecided.length > 0 ? 'aside' : row ? 'row' : 'plain';
        kinds.push(kind);
        asideOn.push(anyTruth(undecided));

        // the leaf on each target row: false where the row does not carry it
        const onRows: Truth[] = [];
        for (const [name, rowId] of rows) {
            if (!isRead(kind) || (row && live.status === 'false')) {
                onRows.push(FALSE);
                continue;
            }
            const carried = Object.entries(fields).every(([field, v]) =>
                grantsTable.columns[field]?.default === v || (TWO_ROW_CARRIERS[field]?.[v] ?? []).includes(name));
            if (!carried) {
                onRows.push(FALSE);
                continue;
            }
            const checks: Truth[] = [];
            for (const [field, v] of Object.entries(fields)) {
                const read = await view.columnValue(rowId, field);
                checks.push(read.status === 'undecided' ? read : truthOf((read.value ?? grantsTable.columns[field]?.default) === v));
            }
            onRows.push(allTruth(checks));
        }
        value.push(onRows);
    }

    const rowLive: Truth[] = [];
    for (const [, rowId] of rows) rowLive.push((await view.rowLiveness(rowId)).truth);
    return { kinds, value, rowLive, subject: live, asideOn, touchedOn: tree.leaves.map(() => FALSE) };
}

// The distributed form as a predicate: one single-leaf atom per way.
function distributedPredicate(tree: TreeCase): Predicate {
    const atoms = waysOf(tree.shape).map((way): Predicate =>
        ({ p: 'exists', table: 'grants', where: Object.assign({}, ...way.map((i) => tree.leaves[i])) }));
    return atoms.length === 1 ? atoms[0] : { p: 'or', args: atoms };
}

// A subject row with fixed answers, for the term cases.
function fixedSubject(live: Truth, columns: { [column: string]: ColumnValue }): SubjectRow {
    return { live: async () => live, column: async (column) => columns[column] ?? { status: 'decided', value: undefined } };
}

export const verdictReadsTests = {
    title: '[VREAD] Three-valued read rules under injected verdicts',
    tests: [
        {
            name: '[VREAD01] a column read that meets an undecided write is undecided, and so is every atom on it',
            invoke: async () => {
                const fixture = await createFixture();
                const view = viewIn(fixture, stateOf({ I: 'live', U1: 'undecided' }), fixture.merged, fixture.merged);
                const grantee = await view.columnValue(fixture.rowId, 'grantee');
                assertTrue(grantee.status === 'undecided' && grantee.support.has(fixture.hashes.U1),
                    'the grantee read met the undecided update');
                assertEquals((await atomOn(view, fixture.rowId, GRANTEE_IS_CAROL)).status, 'undecided',
                    'grantee == carol is undecided, though no completion makes it true');
                assertEquals((await view.rowLiveness(fixture.rowId)).truth.status, 'true',
                    'the row itself is live');
            },
        },
        {
            name: '[VREAD02] an undecided update that does not write the column leaves it decided',
            invoke: async () => {
                const fixture = await createFixture();
                const view = viewIn(fixture, stateOf({ I: 'live', U2: 'undecided' }), fixture.merged, fixture.merged);
                const grantee = await view.columnValue(fixture.rowId, 'grantee');
                assertTrue(grantee.status === 'decided' && grantee.value === 'alice', 'grantee is alice in every completion');
                assertEquals((await atomOn(view, fixture.rowId, GRANTEE_IS_CAROL)).status, 'false',
                    'grantee == carol is false');
                assertEquals((await view.columnValue(fixture.rowId, 'note')).status, 'undecided',
                    'the note read met the undecided update');
            },
        },
        {
            name: '[VREAD03] an undecided insert makes every $row atom on the row undecided',
            invoke: async () => {
                const fixture = await createFixture();
                const view = viewIn(fixture, stateOf({ I: 'undecided' }), fixture.merged, fixture.merged);
                assertEquals((await view.rowLiveness(fixture.rowId)).truth.status, 'undecided', 'the row may not exist');
                assertEquals((await atomOn(view, fixture.rowId, GRANTEE_IS_CAROL)).status, 'undecided',
                    'grantee == carol is undecided');
                assertEquals((await atomOn(view, fixture.rowId, NOTE_LIKE_N)).status, 'undecided',
                    "note like 'n%' is undecided, though it holds whenever the row exists");
                assertEquals((await existsOnView(view, { grantee: 'alice' })).status, 'undecided',
                    'an exists over the row is undecided');
            },
        },
        {
            name: '[VREAD04] a definite delete makes the row dead even when other entries are undecided',
            invoke: async () => {
                const fixture = await createFixture();
                const state = stateOf({ I: 'undecided', I2: 'undecided', U1: 'undecided', D: 'live' });
                const deleted = viewIn(fixture, state, fixture.full, fixture.full);
                assertEquals((await deleted.rowLiveness(fixture.rowId)).truth.status, 'false', 'a delete in the history kills');
                const killed = viewIn(fixture, state, fixture.merged, fixture.full);
                assertEquals((await killed.rowLiveness(fixture.rowId)).truth.status, 'false', 'a concurrent delete kills');
                assertEquals((await atomOn(killed, fixture.rowId, GRANTEE_IS_CAROL)).status, 'false',
                    'every $row atom on a dead row is false');
            },
        },
        {
            name: '[VREAD05] the entry being diagnosed reads as void in its own views',
            invoke: async () => {
                const fixture = await createFixture();
                const state = stateOf({ I: 'live' });
                const plain = viewIn(fixture, state, fixture.merged, fixture.merged);
                assertEquals((await plain.rowLiveness(fixture.rowId)).truth.status, 'true', 'the insert makes the row live');
                const excluding = viewIn(fixture, state, fixture.merged, fixture.merged, fixture.hashes.I);
                assertEquals((await excluding.rowLiveness(fixture.rowId)).truth.status, 'false',
                    'an insert is not its own witness');
            },
        },
        {
            name: '[VREAD06] every read rule is exact, sound and monotone over all states',
            invoke: async () => {
                const fixture = await createFixture();
                await assertGuarantees(fixture, fixture.merged, fixture.full, 'concurrent delete');
                await assertGuarantees(fixture, fixture.full, fixture.full, 'delete in history');
            },
        },
        {
            name: '[VREAD07] a where tree with a subject-row term in one arm has the value of the model\'s distributed form in every state, on a support it contains',
            invoke: async () => {
                const fixture = await createTwoRowFixture();
                let states: { [name in TwoRowName]?: Status }[] = [{}];
                for (const name of TWO_ROW_NAMES) {
                    states = states.flatMap((st) => (['live', 'void', 'undecided'] as Status[]).map((status) => ({ ...st, [name]: status })));
                }

                for (const tree of TREE_CASES) {
                    let undecided = 0;
                    let smaller = 0;
                    let decidedDespite = 0;
                    for (const partial of states) {
                        const state = partial as { [name in TwoRowName]: Status };
                        const where = `${tree.name}, state ${TWO_ROW_NAMES.map((n) => `${n}=${state[n]}`).join(' ')}`;
                        const env = (view: RTableViewImpl) => ({
                            getTableView: async () => view,
                            getForeignTableView: async () => undefined,
                            subject: subjectOf(view, fixture.r),
                            context: 'row' as const,
                        });

                        const view = twoRowView(fixture, state);
                        const want = distributedForm(tree.shape, await modelStateOf(fixture, view, tree));

                        // the model's distributed form is what single-leaf atoms give
                        const atomsView = twoRowView(fixture, state);
                        const atoms = await evaluatePredicateTruth(distributedPredicate(tree), env(atomsView));
                        assertTrue(sameTruth(atoms, want),
                            `atoms: ${where}: the atoms give ${showTruth(atoms)}, the model ${showTruth(want)}`);

                        const treeView = twoRowView(fixture, state);
                        const got = await evaluatePredicateTruth({ p: 'exists', table: 'grants', where: tree.where }, env(treeView));
                        assertTrue(got.status === want.status,
                            `value: ${where}: the tree gives ${showTruth(got)}, the distributed form ${showTruth(want)}`);
                        if (got.status === 'undecided' && want.status === 'undecided') {
                            undecided++;
                            assertTrue([...got.support].every((h) => want.support.has(h)),
                                `support: ${where}: the tree rests on ${showTruth(got)}, the distributed form on ${showTruth(want)}`);
                            if (got.support.size < want.support.size) smaller++;
                        } else if (Object.values(state).includes('undecided')) {
                            decidedDespite++;
                        }
                    }
                    assertTrue(undecided > 0 && decidedDespite > 0,
                        `${tree.name}: some state leaves it undecided and some decides it despite undecided entries`);
                    console.log(`      ${tree.name}: ${states.length} states, ${undecided} undecided, ${smaller} on a smaller support`);
                }
            },
        },
        {
            name: '[VREAD08] a leaf with a term that has no value is false: under an $and the atom is false, under an $or the other arm decides, and a leaf set aside beside it does not make the atom undecided',
            invoke: async () => {
                const fixture = await createFixture();
                // r is live with grantee bob and note n1
                const view = () => viewIn(fixture, stateOf({ I: 'live', U1: 'live', U2: 'live' }), fixture.merged, fixture.merged);
                const pending: ColumnValue = { status: 'undecided', support: new Set(['p']) };
                const subject = fixedSubject(TRUE, { here: { status: 'decided', value: 'bob' }, pending });
                const atom = async (where: Where, opts: { author?: string; params?: { [name: string]: string } } = {}): Promise<string> =>
                    showTruth(await evaluatePredicateTruth({ p: 'exists', table: 'grants', where }, {
                        getTableView: async () => view(),
                        getForeignTableView: async () => undefined,
                        subject, context: 'row', ...opts,
                    }));

                const terms: [string, string][] = [
                    ['$author of an anonymous op', '$author'],
                    ['an absent parameter', '$param.who'],
                    ['a subject column with no value', '$row.missing'],
                ];
                for (const [what, term] of terms) {
                    assertEquals(await atom({ $and: [{ grantee: term }, { $or: [{ note: 'n1' }, { note: 'n0' }] }] }), 'false',
                        `${what} in a leaf under an $and: the atom is false`);
                    assertEquals(await atom({ $or: [{ grantee: term }, { grantee: 'bob' }] }), 'true',
                        `${what} in a leaf under an $or: the other arm decides`);
                    assertEquals(await atom({ $or: [{ grantee: term }, { grantee: 'zed' }] }), 'false',
                        `${what} in a leaf under an $or whose other arm fails`);
                }

                // the same terms with a value
                assertEquals(await atom({ $and: [{ grantee: '$author' }, { $or: [{ note: 'n1' }, { note: 'n0' }] }] }, { author: 'bob' }), 'true',
                    'an authored op resolves $author');
                assertEquals(await atom({ $and: [{ grantee: '$param.who' }, { $or: [{ note: 'n1' }, { note: 'n0' }] }] }, { params: { who: 'bob' } }), 'true',
                    'a present parameter resolves');
                assertEquals(await atom({ $and: [{ grantee: '$row.here' }, { $or: [{ note: 'n1' }, { note: 'n0' }] }] }), 'true',
                    'a subject value resolves');

                // an undecided subject value beside one with no value
                assertEquals(await atom({ grantee: '$row.pending', note: '$row.missing' }), 'false',
                    'one leaf: a missing subject value makes the atom false though another is undecided');
                assertEquals(await atom({ $and: [{ grantee: '$row.pending' }, { $or: [{ note: '$row.missing' }, { note: '$row.missing' }] }] }), 'false',
                    'a leaf set aside and a leaf with no value in one $and: false, not undecided');
                assertEquals(await atom({ $and: [{ grantee: '$row.pending' }, { $or: [{ note: 'n1' }, { note: '$row.missing' }] }] }), 'undecided on {p}',
                    'with a way left through the leaf set aside, the atom is undecided on the subject value');
                assertEquals(await atom({ $or: [{ grantee: '$row.pending' }, { grantee: 'bob' }] }), 'true',
                    'an arm that names no undecided value still decides');
            },
        },
        {
            name: '[VREAD09] with a target that cannot be looked up, a leaf set aside still makes the atom undecided, and every other leaf is false',
            invoke: async () => {
                const pending: ColumnValue = { status: 'undecided', support: new Set(['p']) };
                const atom = async (where: Where, live: Truth): Promise<string> =>
                    showTruth(await evaluatePredicateTruth({ p: 'exists', table: 'unbound.grants', where }, {
                        getTableView: async () => { throw new Error('the target is not local'); },
                        getForeignTableView: async () => undefined,
                        subject: fixedSubject(live, { here: { status: 'decided', value: 'bob' }, pending }),
                        context: 'row',
                    }));
                const undecidedLive: Truth = { status: 'undecided', support: new Set(['s']) };

                assertEquals(await atom({ grantee: '$row.pending' }, TRUE), 'undecided on {p}',
                    'one leaf with an undecided subject value is undecided, as it is today');
                assertEquals(await atom({ grantee: '$row.pending' }, undecidedLive), 'undecided on {p,s}',
                    'and rests on the subject row too when its liveness is undecided');
                assertEquals(await atom({ grantee: '$row.pending' }, FALSE), 'false', 'a subject row that is not live makes it false');
                assertEquals(await atom({ grantee: '$row.here' }, TRUE), 'false', 'a leaf that would be read is false');
                assertEquals(await atom({ grantee: 'bob' }, TRUE), 'false', 'a plain leaf is false');
                assertEquals(await atom({ $or: [{ grantee: '$row.pending' }, { grantee: 'bob' }] }, TRUE), 'undecided on {p}',
                    'in a tree, the leaf set aside still counts');
                assertEquals(await atom({ $or: [{ grantee: '$row.here' }, { grantee: 'bob' }] }, TRUE), 'false',
                    'a tree with no leaf set aside is false');
            },
        },
        {
            name: '[VREAD10] the model: a where tree read as written has the value of its distributed form in every state, on a support it contains, and one leaf is today\'s atom',
            invoke: async () => {
                let states = 0;
                let smaller = 0;
                for (const entry of MODEL_SHAPES) {
                    const totals = checkModelShape(entry);
                    states += totals.states;
                    smaller += totals.smaller;
                }
                assertTrue(smaller > 0, 'some state has a support strictly inside the distributed form\'s');
                if (MODEL_FULL) {
                    assertEquals(states, 4612221, 'every state of every shape was checked');
                    assertEquals(smaller, 135648, 'the states whose support is strictly smaller');
                }
            },
        },
    ],
};
