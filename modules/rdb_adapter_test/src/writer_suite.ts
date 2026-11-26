// HIP-20's writer suite: each case writes through the target's writer and checks
// what capture recorded, what ingestion made of it, the row after project-back
// and rdb's row, on every target the factory gives. Tables are projected under
// their schema names, as the ingestion suites project them.

import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { deriveRowId, type Row } from "@hyper-hyper-space/hhs3_rdb";
import { serializePublicKeyToBase64 } from "@hyper-hyper-space/hhs3_mvt";
import {
    DEFAULT_KEY_DOMAIN, ingestChanges, isWritableTarget, keyValue, projectGroup, QueryEngine, WriteRefusedError,
    type AdapterConfig, type AuthorSetting, type BidirectionalTarget, type CapturedChange, type KeyIndex, type QueryableTarget,
    type ReactiveChangeTarget, type RowValues, type RowWriter, type WritableTarget,
} from "@hyper-hyper-space/hhs3_rdb_adapter";

import { createAnonymousGroup, createGatedGroup, createTypedGroup, makeIdentity, type GroupFixture } from "./group_fixture.js";
import type { IngestionFactory, IngestionHarness } from "./ingestion_suite.js";
import { localRow, rdbRows, recordWriter, rowOps } from "./ingestion_helpers.js";

type NamedTest = { name: string; invoke: () => Promise<void> };

// A target with no capture, for the writers that can't replicate.
export type BareTargetFactory = () => { target: BidirectionalTarget; cleanup?: () => void | Promise<void> }
    | Promise<{ target: BidirectionalTarget; cleanup?: () => void | Promise<void> }>;

// A target set up as a projection leaves it: the group projected, and the
// writer recorded with the setting.
type Bench = {
    fixture: GroupFixture;
    harness: IngestionHarness;
    target: BidirectionalTarget;
    writer: RowWriter;
    keyId: number | undefined;
    author: AuthorSetting;
    // The local commit events since the bench was set up.
    commits(): number;
    // Ingests the pending changes as the projection would.
    ingest(): Promise<{ accepted: number; rejected: { reason: string }[] }>;
    // Projects rdb back into the target.
    project(): Promise<void>;
    cleanup(): Promise<void>;
};

async function bench(
    factory: IngestionFactory, fixture: GroupFixture, author: AuthorSetting = 'auto',
    opts: { key?: boolean; config?: AdapterConfig } = {},
): Promise<Bench> {
    const harness = await factory();
    const target = harness.target;
    const config: AdapterConfig = { ...opts.config, author };
    await projectGroup(fixture.group, target, config);
    const keyId = await recordWriter(target, author, opts.key === false ? undefined : fixture.admin);
    let commits = 0;
    (target as unknown as ReactiveChangeTarget).addCommitListener((e) => { if (e.source === 'local') commits++; });
    const writer = (target as unknown as WritableTarget).openWriter();
    return {
        fixture, harness, target, writer, keyId, author,
        commits: () => commits,
        ingest: async () => ingestChanges(fixture.group, target, { ...config, ...(opts.key === false ? {} : { writer: fixture.admin }) }),
        project: async () => projectGroup(fixture.group, target, config),
        cleanup: async () => { await harness.cleanup?.(); },
    };
}

// A second device: another target the group is projected into.
async function secondDevice(factory: IngestionFactory, fixture: GroupFixture, config: AdapterConfig = {}): Promise<IngestionHarness> {
    const harness = await factory();
    await projectGroup(fixture.group, harness.target, config);
    return harness;
}

// What a refused write gives: its reason, or what happened instead.
async function refusal(write: Promise<unknown>): Promise<string> {
    try {
        await write;
        return 'no refusal';
    } catch (e) {
        return e instanceof WriteRefusedError ? e.reason : `another error: ${String(e)}`;
    }
}

// Lets a target deliver what a write announced.
const settle = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 20));

async function pending(target: BidirectionalTarget): Promise<CapturedChange[]> {
    return (await target.drainChanges()).changes;
}

const lastChange = async (target: BidirectionalTarget): Promise<CapturedChange | undefined> => (await pending(target)).at(-1);

const valuesOf = (change: CapturedChange | undefined): { [column: string]: unknown } =>
    change !== undefined && change.kind !== 'delete' ? change.values : {};

const json = (v: unknown): string => JSON.stringify(v === undefined ? null : v);

async function rdbRow(fixture: GroupFixture, table: string): Promise<Row | undefined> {
    return (await rdbRows(fixture.group, table))[0];
}

async function keyIdOf(target: BidirectionalTarget, who: OwnIdentity): Promise<number> {
    return (target as unknown as KeyIndex).registerKey(DEFAULT_KEY_DOMAIN, who.keyId, serializePublicKeyToBase64(who.publicKey));
}

export function createWriterSuite(label: string, factory: IngestionFactory, bare?: BareTargetFactory): { title: string; tests: NamedTest[] } {
    const tests: NamedTest[] = [
        {
            name: `[${label}-WR01] values of every type read back as written, on the writing device, in rdb and after project-back`,
            invoke: async () => {
                const fixture = await createTypedGroup();
                const b = await bench(factory, fixture);
                try {
                    const values = {
                        label: 'a', level: 7, ratio: 1.5, flag: true, data: { x: [1, 'two', { y: false }] },
                        big: '12345678901234567890', price: '12.30', blob: 'AQID',
                    };
                    const id = await b.writer.insert('things', values);
                    const row = await localRow(b.target, 'things', id);
                    for (const [column, value] of Object.entries(values)) {
                        assertEquals(json(row?.[column]), json(value), `${column} reads back as written`);
                    }
                    const result = await b.ingest();
                    assertTrue(result.accepted === 1 && result.rejected.length === 0, `the insert lands: ${JSON.stringify(result.rejected)}`);
                    const stored = await rdbRow(fixture, 'things');
                    for (const [column, value] of Object.entries(values)) {
                        assertEquals(json(stored?.values[column]), json(value), `${column} reaches rdb as written`);
                    }
                    await b.project();
                    const after = await localRow(b.target, 'things', id);
                    for (const [column, value] of Object.entries(values)) {
                        assertEquals(json(after?.[column]), json(value), `${column} reads the same after project-back`);
                    }
                } finally {
                    await b.cleanup();
                }
            },
        },
        {
            name: `[${label}-WR02] an insert gets the default of a column it leaves out, in the row and in the op, and a column given undefined counts as left out`,
            invoke: async () => {
                const fixture = await createTypedGroup();
                const b = await bench(factory, fixture);
                try {
                    const id = await b.writer.insert('things', { label: 'a', note: undefined, kind: undefined });
                    const row = await localRow(b.target, 'things', id);
                    assertEquals(row?.kind, 'std', 'the nullable column with a default reads its default');
                    assertEquals(row?.level, 1, 'and so does the required one with a default');
                    const change = valuesOf(await lastChange(b.target));
                    assertEquals(change.kind, 'std', 'the captured insert carries the default');
                    assertEquals(change.note ?? null, null, 'and nothing for the column given undefined');
                    assertEquals((await b.ingest()).accepted, 1, 'the insert lands');
                    const [op] = await rowOps(fixture.group, 'things');
                    assertEquals(op?.values.kind, 'std', 'the op carries the default');
                    assertEquals(op?.values.level, 1, 'both defaults');
                    assertEquals((await rdbRow(fixture, 'things'))?.values.kind, 'std', "and rdb's row reads it");
                } finally {
                    await b.cleanup();
                }
            },
        },
        {
            name: `[${label}-WR03] an insert's null reads NULL on the writing device, in rdb and on a second device, also in a column with a default; the op carries it only there`,
            invoke: async () => {
                const fixture = await createTypedGroup();
                const b = await bench(factory, fixture);
                try {
                    const id = await b.writer.insert('things', { label: 'a', note: null, kind: null });
                    const row = await localRow(b.target, 'things', id);
                    assertEquals(row?.note ?? null, null, 'the nullable column reads NULL');
                    assertEquals(row?.kind ?? null, null, 'and so does the one with a default');
                    const change = valuesOf(await lastChange(b.target));
                    assertTrue('note' in change && change.note === null && 'kind' in change && change.kind === null,
                        `the captured insert carries both nulls: ${JSON.stringify(change)}`);
                    assertEquals((await b.ingest()).accepted, 1, 'the insert lands');
                    const [op] = await rowOps(fixture.group, 'things');
                    assertTrue(!('note' in (op?.values ?? {})), 'the op leaves out the null of a column without a default');
                    assertTrue((op?.nulls ?? []).includes('kind') || op?.values.kind === null, `and carries the one with a default: ${JSON.stringify(op)}`);
                    const stored = await rdbRow(fixture, 'things');
                    assertEquals(stored?.values.kind ?? null, null, "rdb's row reads NULL");
                    const second = await secondDevice(factory, fixture);
                    try {
                        const [other] = await (second.target as unknown as QueryableTarget).executeQuery({ table: 'things' });
                        assertEquals(other?.kind ?? null, null, 'and so does a second device');
                        assertEquals(other?.note ?? null, null, 'for both columns');
                    } finally {
                        await second.cleanup?.();
                    }
                } finally {
                    await b.cleanup();
                }
            },
        },
        {
            name: `[${label}-WR04] an update to null clears a column, also on a second device; with compare, a null over NULL writes and captures nothing`,
            invoke: async () => {
                const fixture = await createTypedGroup();
                const b = await bench(factory, fixture);
                try {
                    const id = await b.writer.insert('things', { label: 'a', note: 'n' });
                    await b.ingest();
                    assertEquals(await b.writer.update('things', id, { note: null }), true, 'the update resolves true');
                    assertEquals((await b.ingest()).accepted, 1, 'and lands');
                    assertEquals((await rdbRow(fixture, 'things'))?.values.note ?? null, null, "rdb's row is cleared");
                    const second = await secondDevice(factory, fixture);
                    try {
                        assertEquals((await localRow(second.target, 'things', 1))?.note ?? null, null, 'and a second device reads NULL');
                    } finally {
                        await second.cleanup?.();
                    }
                    await b.project();
                    const before = b.commits();
                    assertEquals(await b.writer.update('things', id, { note: null }, { compare: true }), true, 'with compare it resolves true');
                    await settle();
                    assertEquals((await pending(b.target)).length, 0, 'and captures nothing');
                    assertEquals(b.commits(), before, 'and fires no commit event');
                } finally {
                    await b.cleanup();
                }
            },
        },
        {
            name: `[${label}-WR05] a missing required column throws missing-required, and a null for a column that isn't nullable throws not-null, in an insert and an update, also where it has a default`,
            invoke: async () => {
                const fixture = await createTypedGroup();
                const b = await bench(factory, fixture);
                try {
                    assertEquals(await refusal(b.writer.insert('things', { note: 'x' })), 'missing-required', 'an insert without the required column');
                    assertEquals(await refusal(b.writer.insert('things', { label: null })), 'not-null', "a null for a column that isn't nullable");
                    assertEquals(await refusal(b.writer.insert('things', { label: 'a', level: null })), 'not-null', 'also where it has a default');
                    const id = await b.writer.insert('things', { label: 'a' });
                    assertEquals(await refusal(b.writer.update('things', id, { label: null })), 'not-null', 'and in an update');
                    assertEquals((await pending(b.target)).length, 1, 'only the insert that passed is captured');
                } finally {
                    await b.cleanup();
                }
            },
        },
        {
            name: `[${label}-WR06] an update that repeats a row's value writes and captures it; with compare it writes nothing and fires no commit event`,
            invoke: async () => {
                const fixture = await createTypedGroup();
                const b = await bench(factory, fixture);
                try {
                    const id = await b.writer.insert('things', { label: 'a', note: 'n' });
                    await b.ingest();
                    let before = b.commits();
                    assertEquals(await b.writer.update('things', id, { note: 'n' }), true, 'the update resolves true');
                    await settle();
                    assertEquals(json(valuesOf(await lastChange(b.target)).note), json('n'), 'and is captured, unchanged value and all');
                    assertEquals(b.commits(), before + 1, 'with a commit event');
                    assertEquals((await b.ingest()).accepted, 1, 'and lands');
                    before = b.commits();
                    assertEquals(await b.writer.update('things', id, { note: 'n' }, { compare: true }), true, 'with compare it resolves true');
                    await settle();
                    assertEquals((await pending(b.target)).length, 0, 'and captures nothing');
                    assertEquals(b.commits(), before, 'and fires no commit event');
                } finally {
                    await b.cleanup();
                }
            },
        },
        {
            name: `[${label}-WR07] a form saves every field with compare while another device clears a field the form didn't change: after both sync, the field is NULL on both`,
            invoke: async () => {
                const fixture = await createTypedGroup();
                const a = await bench(factory, fixture);
                try {
                    const idA = await a.writer.insert('things', { label: 'a', note: 'n', ratio: 1 });
                    await a.ingest();
                    await a.project();
                    const b = await bench(factory, fixture);
                    try {
                        const idB = (await (b.target as unknown as QueryableTarget).executeQuery({ table: 'things' }))[0]!.id as number;
                        assertEquals(await b.writer.update('things', idB, { note: null }), true, 'the other device clears the note');
                        assertEquals((await b.ingest()).accepted, 1, 'and its clear lands');

                        const form = (await localRow(a.target, 'things', idA))!;
                        assertEquals(await a.writer.update('things', idA, { ...form, ratio: 2 } as RowValues, { compare: true }), true,
                            'the form saves every field it read, with compare');
                        assertEquals(Object.keys(valuesOf(await lastChange(a.target))).join(','), 'ratio', 'which writes only the field that changed');
                        assertEquals((await a.ingest()).accepted, 1, 'and lands');
                        await a.project();
                        await b.project();
                        for (const [device, target, id] of [['the form', a.target, idA], ['the other', b.target, idB]] as const) {
                            const row = await localRow(target, 'things', id);
                            assertEquals(row?.note ?? null, null, `${device} device reads the note cleared`);
                            assertEquals(row?.ratio, 2, `and the ratio the form saved`);
                        }
                    } finally {
                        await b.cleanup();
                    }
                } finally {
                    await a.cleanup();
                }
            },
        },
        {
            name: `[${label}-WR08] a row spread into an update lands, and with compare writes only the changed column; a different id or author is refused`,
            invoke: async () => {
                const fixture = await createTypedGroup();
                const b = await bench(factory, fixture);
                try {
                    const id = await b.writer.insert('things', { label: 'a', code: 'C', spec: { s: 1 } });
                    await b.ingest();
                    await b.project();
                    const row = (await localRow(b.target, 'things', id))!;
                    assertEquals(await b.writer.update('things', id, { ...row, note: 'one' } as RowValues), true, 'the spread update resolves true');
                    const whole = valuesOf(await lastChange(b.target));
                    assertTrue(!('id' in whole) && !('author_key_id' in whole), `the row's own id and author are set aside: ${JSON.stringify(whole)}`);
                    const result = await b.ingest();
                    assertTrue(result.accepted === 1 && result.rejected.length === 0, `and it lands, readonly columns at their values: ${JSON.stringify(result.rejected)}`);
                    await b.project();
                    const again = (await localRow(b.target, 'things', id))!;
                    await b.writer.update('things', id, { ...again, note: 'two' } as RowValues, { compare: true });
                    assertEquals(Object.keys(valuesOf(await lastChange(b.target))).join(','), 'note', 'with compare only the changed column is written');
                    await b.ingest();
                    assertEquals(await refusal(b.writer.update('things', id, { ...again, id: id + 1 } as RowValues)), 'id-column', 'a different id is refused');
                    assertEquals(await refusal(b.writer.update('things', id, { ...again, author_key_id: null } as RowValues)), 'author', 'and so is a different author');
                } finally {
                    await b.cleanup();
                }
            },
        },
        {
            name: `[${label}-WR09] an update of one json column writes no other; one with a readonly column at another value is refused whole at ingestion`,
            invoke: async () => {
                const fixture = await createTypedGroup();
                const b = await bench(factory, fixture);
                try {
                    const id = await b.writer.insert('things', { label: 'a', data: { a: 1 }, spec: { s: 1 }, code: 'C' });
                    await b.ingest();
                    await b.writer.update('things', id, { data: { a: 2 } });
                    assertEquals(Object.keys(valuesOf(await lastChange(b.target))).join(','), 'data', 'only the json column given is written');
                    await b.ingest();
                    await b.writer.update('things', id, { code: 'D', note: 'x' });
                    const result = await b.ingest();
                    assertTrue(result.accepted === 0 && result.rejected.length === 1, `the update is refused whole: ${JSON.stringify(result)}`);
                    const row = await localRow(b.target, 'things', id);
                    assertEquals(row?.code, 'C', 'the row is reverted');
                    assertEquals(row?.note ?? null, null, 'both columns');
                } finally {
                    await b.cleanup();
                }
            },
        },
        {
            name: `[${label}-WR10] an update or delete of a missing id writes and captures nothing, fires no commit event and resolves false; of a row the table has it resolves true, also with nothing to write`,
            invoke: async () => {
                const fixture = await createTypedGroup();
                const b = await bench(factory, fixture);
                try {
                    const id = await b.writer.insert('things', { label: 'a', note: 'n' });
                    await b.ingest();
                    const before = b.commits();
                    assertEquals(await b.writer.update('things', id + 100, { note: 'x' }), false, 'an update of a missing id resolves false');
                    assertEquals(await b.writer.delete('things', id + 100), false, 'and so does a delete');
                    assertEquals(await b.writer.update('things', id, {}), true, 'an update given no columns resolves true');
                    assertEquals(await b.writer.update('things', id, { note: 'n' }, { compare: true }), true, 'and so does one that changes no column');
                    await settle();
                    assertEquals((await pending(b.target)).length, 0, 'none of them captures anything');
                    assertEquals(b.commits(), before, 'or fires a commit event');
                    assertEquals(await b.writer.delete('things', id), true, 'a delete of the row resolves true');
                    assertEquals((await lastChange(b.target))?.kind, 'delete', 'and is captured');
                    assertEquals((await b.ingest()).accepted, 1, 'and lands');
                    assertEquals((await rdbRows(fixture.group, 'things')).length, 0, 'rdb has no row');
                } finally {
                    await b.cleanup();
                }
            },
        },
        {
            name: `[${label}-WR11] a { pk } interns the key and reads back as its id, the same id twice, and the op carries the key hash; an identity-provider row written with one is accepted`,
            invoke: async () => {
                const fixture = await createTypedGroup();
                const b = await bench(factory, fixture);
                try {
                    const other = await makeIdentity();
                    const pk = keyValue(other.publicKey);
                    const providerId = await b.writer.insert('identities', { key_id: pk, name: 'other' });
                    const keyId = (await localRow(b.target, 'identities', providerId))?.key_id as number;
                    assertEquals(await (b.target as unknown as KeyIndex).keyHashForId(DEFAULT_KEY_DOMAIN, keyId), other.keyId,
                        'the key is interned under the key hash rdb computes');
                    const id = await b.writer.insert('things', { label: 'a', owner_key_id: pk });
                    assertEquals((await localRow(b.target, 'things', id))?.owner_key_id, keyId, 'the same key gets the same id');
                    const result = await b.ingest();
                    assertTrue(result.accepted === 2 && result.rejected.length === 0, `both land: ${JSON.stringify(result.rejected)}`);
                    const provider = (await rdbRows(fixture.group, 'identities')).find((r) => r.values.name === 'other');
                    assertEquals(provider?.values.keyId, other.keyId, "rdb accepts the identity-provider row, with the key's hash");
                    assertEquals((await rdbRow(fixture, 'things'))?.values.owner, other.keyId, 'and the key column carries it');
                } finally {
                    await b.cleanup();
                }
            },
        },
        {
            name: `[${label}-WR12] key values that aren't keys are refused, and leave the key table as it was; a { pk } in a json column is json, and a null in a nullable key column is NULL`,
            invoke: async () => {
                const fixture = await createTypedGroup();
                const b = await bench(factory, fixture);
                try {
                    const other = await makeIdentity();
                    const pk = keyValue(other.publicKey).pk;
                    const keys = b.target as unknown as KeyIndex;
                    const owner = (v: unknown) => b.writer.insert('things', { label: 'a', owner_key_id: v as RowValues[string] });
                    assertEquals(await refusal(owner(pk)), 'wrong-type', 'a bare string');
                    assertEquals(await refusal(owner({ pk: other.keyId })), 'invalid-key', 'a { pk } holding a key hash');
                    const encode = (suite: string, length: number) =>
                        serializePublicKeyToBase64({ suite, key: new Uint8Array(length).fill(7) });
                    assertEquals(await refusal(owner({ pk: encode('no-such-suite', 32) })), 'invalid-key', 'a key of an unknown suite');
                    assertEquals(await refusal(owner({ pk: encode(other.publicKey.suite, other.publicKey.key.length - 1) })), 'invalid-key',
                        'a key of the wrong length');
                    assertEquals(await refusal(owner({ pk: pk.replace(/=+$/, '') + ' ' })), 'invalid-key', "a { pk } whose base64 isn't canonical");
                    assertEquals(await refusal(owner(9999)), 'invalid-key', "a key id the key table doesn't have");
                    assertEquals(await refusal(b.writer.insert('things', { label: null, owner_key_id: { pk } })), 'not-null',
                        'a write refused for another reason');
                    assertEquals(await b.writer.update('things', 9999, { owner_key_id: { pk } }), false, 'and a write to a missing row');
                    assertEquals(await keys.idForKeyHash(DEFAULT_KEY_DOMAIN, other.keyId), undefined, 'leave the key table as it was');

                    const id = await b.writer.insert('things', { label: 'a', data: { pk }, owner_key_id: null });
                    const row = await localRow(b.target, 'things', id);
                    assertEquals(json(row?.data), json({ pk }), 'a { pk } in a json column is stored as json');
                    assertEquals(row?.owner_key_id ?? null, null, 'and a null in a nullable key column reads NULL');
                    assertEquals(await keys.idForKeyHash(DEFAULT_KEY_DOMAIN, other.keyId), undefined, 'interning nothing');
                    assertEquals(await refusal(b.writer.insert('things', { label: 'a', author_key_id: { pk } })), 'author',
                        'a { pk } in the author column is refused');
                    assertEquals(await refusal(b.writer.insert('things', { label: 'a', author_key_id: keyValue(fixture.admin.publicKey) })), 'author',
                        "also the writer key's");
                } finally {
                    await b.cleanup();
                }
            },
        },
        {
            name: `[${label}-WR13] authorship: each cell of the author table, under auto and manual, in a group with an identity provider and in an anonymous one`,
            invoke: async () => {
                type Given = 'nothing' | 'writer key' | 'null' | 'another key';
                const cell = async (fixture: GroupFixture, author: AuthorSetting, given: Given, expect: 'signed' | 'anonymous' | 'refused') => {
                    const b = await bench(factory, fixture, author);
                    try {
                        const other = await keyIdOf(b.target, await makeIdentity());
                        const value = given === 'writer key' ? { author_key_id: b.keyId! } : given === 'null' ? { author_key_id: null }
                            : given === 'another key' ? { author_key_id: other } : {};
                        const what = `${given} under ${author} in ${fixture.group.getIdProvider() === undefined ? 'an anonymous group' : 'a signed group'}`;
                        if (expect === 'refused') {
                            assertEquals(await refusal(b.writer.insert('things', { label: 'a', ...value })), 'author', `${what} is refused`);
                            return;
                        }
                        const id = await b.writer.insert('things', { label: 'a', ...value });
                        const want = expect === 'signed' ? b.keyId! : null;
                        assertEquals((await localRow(b.target, 'things', id))?.author_key_id ?? null, want, `${what}: the writing device reads it`);
                        const result = await b.ingest();
                        assertTrue(result.accepted === 1, `${what} lands: ${JSON.stringify(result.rejected)}`);
                        const stored = await rdbRow(fixture, 'things');
                        assertEquals(stored?.author, expect === 'signed' ? fixture.admin.keyId : undefined, `${what}: rdb's signer`);
                        await b.project();
                        assertEquals((await localRow(b.target, 'things', id))?.author_key_id ?? null, want, `${what}: and after project-back`);
                        const second = await secondDevice(factory, fixture);
                        try {
                            const [row] = await (second.target as unknown as QueryableTarget).executeQuery({ table: 'things' });
                            const secondWant = expect === 'signed' ? await keyIdOf(second.target, fixture.admin) : null;
                            assertEquals(row?.author_key_id ?? null, secondWant, `${what}: and a second device`);
                        } finally {
                            await second.cleanup?.();
                        }
                    } finally {
                        await b.cleanup();
                    }
                };
                const table: [AuthorSetting, Given, 'signed' | 'anonymous' | 'refused'][] = [
                    ['auto', 'nothing', 'signed'], ['auto', 'writer key', 'signed'], ['auto', 'null', 'refused'], ['auto', 'another key', 'refused'],
                    ['manual', 'nothing', 'anonymous'], ['manual', 'writer key', 'signed'], ['manual', 'null', 'anonymous'], ['manual', 'another key', 'refused'],
                ];
                for (const [author, given, expect] of table) await cell(await createTypedGroup(), author, given, expect);
                for (const author of ['auto', 'manual'] as const) {
                    await cell(await createAnonymousGroup(), author, 'nothing', 'anonymous');
                    await cell(await createAnonymousGroup(), author, 'null', 'anonymous');
                    await cell(await createAnonymousGroup(), author, 'writer key', 'refused');
                }
            },
        },
        {
            name: `[${label}-WR14] refused writes throw a WriteRefusedError with their reason, write nothing and fire no commit event`,
            invoke: async () => {
                const fixture = await createTypedGroup();
                const b = await bench(factory, fixture);
                try {
                    const id = await b.writer.insert('things', { label: 'a' });
                    await b.ingest();
                    const before = b.commits();
                    assertEquals(await refusal(b.writer.insert('nope', { label: 'a' })), 'unknown-table', 'an unknown table');
                    assertEquals(await refusal(b.writer.insert('things', { label: 'a', nope: 1 })), 'unknown-column', 'an unknown column');
                    assertEquals(await refusal(b.writer.insert('things', { label: 'a', id: 77 })), 'id-column', 'the id column in an insert');
                    assertEquals(await refusal(b.writer.insert('things', { label: 'a', flag: 'yes' })), 'wrong-type', 'a value of the wrong JSON type');
                    assertEquals(await refusal(b.writer.insert('things', { label: 'a', level: 1.5 })), 'wrong-type', 'a float in an integer column');
                    assertEquals(await refusal(b.writer.insert('things', { label: 'a', data: { a: null } as unknown as RowValues[string] })), 'wrong-type',
                        'a json value with a null inside it');
                    const other = await keyIdOf(b.target, await makeIdentity());
                    assertEquals(await refusal(b.writer.update('things', id, { author_key_id: other })), 'author', 'another author in an update');
                    await settle();
                    assertEquals((await pending(b.target)).length, 0, 'nothing is captured');
                    assertEquals(b.commits(), before, 'and no commit event fires');
                    assertEquals((await (b.target as unknown as QueryableTarget).executeQuery({ table: 'things' })).length, 1, 'and nothing is written');
                } finally {
                    await b.cleanup();
                }
            },
        },
        {
            name: `[${label}-WR15] a delete made void by a concurrent change: the row returns under the same local id, and the op event is void with kind delete`,
            invoke: async () => {
                const fixture = await createGatedGroup();
                const { group, admin } = fixture;
                await (await group.getTable('grants')).insert('g-1', { label: 'delete' }, admin);
                await (await group.getTable('items')).insert('i-1', { name: 'thing' }, admin);
                const b = await bench(factory, fixture);
                try {
                    const checkpoint = (await b.target.getCheckpoint(group.getId()))!;
                    const [item] = await (b.target as unknown as QueryableTarget).executeQuery({ table: 'items' });
                    const id = item!.id as number;
                    // the grant's revoke, concurrent with the delete: both at the checkpoint
                    await (await group.getTable('grants')).delete(deriveRowId('g-1', admin.keyId), admin, checkpoint);
                    assertEquals(await b.writer.delete('items', id), true, 'the delete resolves true');
                    assertEquals((await b.ingest()).accepted, 1, 'and is accepted where it stands');
                    await b.project();
                    assertEquals((await localRow(b.target, 'items', id))?.name, 'thing', 'the row returns under the same local id');
                    const events = await (b.target as unknown as { drainOpEvents(): Promise<{ event: { direction: string; kind?: string; localId?: number } }[]> }).drainOpEvents();
                    assertTrue(events.some((e) => e.event.direction === 'void' && e.event.kind === 'delete'),
                        `the op event is void, of kind delete: ${JSON.stringify(events.map((e) => e.event))}`);
                } finally {
                    await b.cleanup();
                }
            },
        },
        {
            name: `[${label}-WR16] opening a writer and dropping it adds no commit listener, and leaves the outbox and the tables as they were`,
            invoke: async () => {
                const fixture = await createTypedGroup();
                const b = await bench(factory, fixture);
                try {
                    await b.writer.insert('things', { label: 'a' });
                    const target = b.target as unknown as ReactiveChangeTarget & WritableTarget & QueryableTarget;
                    let added = 0;
                    const add = target.addCommitListener.bind(target);
                    target.addCommitListener = (listener) => { added++; add(listener); };
                    const outbox = JSON.stringify(await pending(b.target));
                    const rows = JSON.stringify(await target.executeQuery({ table: 'things' }));
                    for (let i = 0; i < 3; i++) target.openWriter();
                    await settle();
                    assertEquals(added, 0, 'no commit listener is added');
                    assertEquals(JSON.stringify(await pending(b.target)), outbox, 'the outbox is as it was');
                    assertEquals(JSON.stringify(await target.executeQuery({ table: 'things' })), rows, 'and so are the rows');
                } finally {
                    await b.cleanup();
                }
            },
        },
        {
            name: `[${label}-WR17] writers that can't replicate: no capture, no writer key, and capture switched off each refuse, writing nothing; an anonymous group's write needs no key`,
            invoke: async () => {
                if (bare !== undefined) {
                    const { target, cleanup } = await bare();
                    try {
                        assertTrue(isWritableTarget(target), 'a target without capture is still one a writer can be asked of');
                        let reason = 'no refusal';
                        try { (target as unknown as WritableTarget).openWriter(); } catch (e) { reason = e instanceof WriteRefusedError ? e.reason : String(e); }
                        assertEquals(reason, 'no-capture', 'and openWriter refuses it');
                    } finally {
                        await cleanup?.();
                    }
                }
                const signed = await createTypedGroup();
                const b = await bench(factory, signed, 'auto', { key: false });
                try {
                    assertEquals(await refusal(b.writer.insert('things', { label: 'a' })), 'no-writer-key', 'a write to a signed group without a writer key');
                    assertEquals(await refusal(b.writer.writerKeyId()), 'no-writer-key', 'and so does writerKeyId()');
                    assertEquals((await pending(b.target)).length, 0, 'writing nothing');
                } finally {
                    await b.cleanup();
                }
                const anon = await createAnonymousGroup();
                const a = await bench(factory, anon, 'auto', { key: false });
                try {
                    await a.writer.insert('things', { label: 'a' });
                    assertEquals((await pending(a.target)).length, 1, "an anonymous group's write is captured without a key");
                    assertEquals((await a.ingest()).accepted, 1, 'and ingested without one');
                    await a.harness.local.setCaptureEnabled(false);
                    assertEquals(await refusal(a.writer.insert('things', { label: 'b' })), 'no-capture', 'a write after capture is switched off');
                    assertEquals((await (a.target as unknown as QueryableTarget).executeQuery({ table: 'things' })).length, 1, 'writes nothing');
                } finally {
                    await a.cleanup();
                }
            },
        },
        {
            name: `[${label}-WR18] a query source has openWriter only over a target that can write, and its rowHash is undefined until ingestion, then round-trips with localId`,
            invoke: async () => {
                const fixture = await createTypedGroup();
                const b = await bench(factory, fixture);
                try {
                    const engine = new QueryEngine(b.target as unknown as ConstructorParameters<typeof QueryEngine>[0]);
                    assertTrue(isWritableTarget(engine.source), "a source over a target that can write has openWriter");
                    const readOnly = new QueryEngine({
                        executeQuery: (q: Parameters<QueryableTarget['executeQuery']>[0]) => (b.target as unknown as QueryableTarget).executeQuery(q),
                        addCommitListener: () => {}, removeCommitListener: () => {},
                    } as unknown as ConstructorParameters<typeof QueryEngine>[0]);
                    assertTrue(!isWritableTarget(readOnly.source), "one over a target that can't write has none");
                    const id = await engine.source.openWriter!().insert('things', { label: 'a' });
                    assertEquals(await engine.source.rowHash('things', id), undefined, 'a new row has no row hash before ingestion');
                    await b.ingest();
                    const rowHash = await engine.source.rowHash('things', id);
                    assertTrue(rowHash !== undefined && rowHash === (await rdbRow(fixture, 'things'))?.rowId, "after it, rdb's row hash");
                    assertEquals(await engine.source.localId('things', rowHash!), id, 'which localId maps back to the row');
                } finally {
                    await b.cleanup();
                }
            },
        },
        {
            name: `[${label}-WR19] tableNames gives a member's projected names, for a member name that isn't an SQL identifier and for a table a schema change adds later; an unknown member gives {}`,
            invoke: async () => {
                const fixture = await createTypedGroup();
                const b = await bench(factory, fixture, 'auto', { config: { tablePrefix: 'my_chat' } });
                try {
                    const engine = new QueryEngine(b.target as unknown as ConstructorParameters<typeof QueryEngine>[0]);
                    const names = async (member: string) => JSON.stringify(Object.entries(await engine.source.tableNames(member)).sort());
                    assertEquals(await names('my-chat'), JSON.stringify([['identities', 'my_chat_identities'], ['things', 'my_chat_things']]),
                        "a member name made safe as an SQL identifier, then '_' and the name in the schema");
                    assertEquals(await names('nobody'), '[]', 'an unknown member gives an empty map');
                    await fixture.schema.updateSchema([{ rule: 'add-table', def: {
                        name: 'later', columns: { note: { type: 'string', nullable: true } }, restrictions: [{ on: 'all', rule: { p: 'true' } }],
                    } }], fixture.admin, 'add later');
                    await fixture.group.deploy(await (await fixture.schema.getScopedDag()).getFrontier());
                    await b.project();
                    assertEquals(JSON.parse(await names('my-chat')).find(([s]: [string]) => s === 'later')?.[1], 'my_chat_later',
                        'a table a schema change adds is named the same way');
                    const id = await b.writer.insert((await engine.source.tableNames('my-chat')).later!, { note: 'x' });
                    assertTrue(id > 0, 'and takes writes');
                } finally {
                    await b.cleanup();
                }
            },
        },
    ];
    return { title: `[${label}] HIP-20's writer`, tests };
}
