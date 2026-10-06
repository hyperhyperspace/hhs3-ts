import { assertTrue, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { version } from "@hyper-hyper-space/hhs3_mvt";
import type { Truth, Verdict, Version } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import { RSchemaImpl, rSchemaFactory } from "../src/rschema/rschema.js";
import { RTableGroupImpl, rTableGroupFactory } from "../src/rtable_group/group.js";
import type { RTableImpl } from "../src/rtable/rtable.js";
import { RTableViewImpl } from "../src/rtable/view.js";
import type { ColumnValue, TableViewTarget } from "../src/rtable/view.js";
import { deriveRowId } from "../src/rtable/hash.js";
import type { Predicate, TableDef } from "../src/rschema/payload.js";
import type { OpVoidDetail } from "../src/rtable_group/op_void.js";
import { freshVerdictEvaluation } from "../src/rtable_group/verdict_evaluation.js";
import { evaluatePredicateTruth } from "../src/rtable_group/predicates.js";

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

function existsRule(where: { [field: string]: string }): ReadRule {
    return {
        name: 'exists ' + JSON.stringify(where),
        run: async (view) => fromTruth(await view.existsMatching(where)),
        reference: (ref, isLive) => {
            if (!ref.live(isLive)) return 'false';
            for (const [field, value] of Object.entries(where)) {
                const resolved = ref.column(field, isLive) ?? grantsTable.columns[field]?.default;
                if (resolved !== value) return 'false';
            }
            return 'true';
        },
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
                assertEquals((await view.existsMatching({ grantee: 'alice' })).status, 'undecided',
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
    ],
};
