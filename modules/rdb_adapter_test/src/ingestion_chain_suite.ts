// HIP-29's cases of ingestion at the checkpoint: refusals and their reverts,
// crashes at each point of a pass, the end of a pass, and the writes a target
// can make that an insert or an update can't express in rdb. Each case checks,
// after every step, that each group's rows are rdb's at its checkpoint with
// the outbox applied on top (assertAtCheckpoints). Joined into
// createIngestionSuite.

import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { deriveRowId, PreparedHashMismatchError, RTableGroup } from "@hyper-hyper-space/hhs3_rdb";
import {
    BidirectionalTarget, DEFAULT_KEY_DOMAIN, IngestSettle, ingestChanges, ingestDatabaseChanges, KeyIndex, projectGroup, syncGroup,
} from "@hyper-hyper-space/hhs3_rdb_adapter";
import { refVersionAtOrAbove, serializePublicKeyToBase64 } from "@hyper-hyper-space/hhs3_mvt";

import { createAnonymousGroup, createFkGroup, createGroup, createTypedGroup, makeIdentity, sameVersion } from "./group_fixture.js";
import { beforeSettle, hooked, SimulatedCrash, throwOnCall } from "./target_hooks.js";
import { assertAtCheckpoints, CheckedTarget } from "./consistency.js";
import { CrashBeforeSettle, entryCount, localRow, projectAs, rdbRows } from "./ingestion_helpers.js";
import type { IngestionFactory, LocalValues } from "./ingestion_suite.js";
import { createCapsItems, createGateGraph, createReinstateScenario, createRevocationScenario, createVoidScenario, frontierOf, newIdentity } from "./scenarios.js";

type NamedTest = { name: string; invoke: () => Promise<void> };

function checkedOf(target: BidirectionalTarget): CheckedTarget {
    return target as unknown as CheckedTarget;
}

// A group whose `method` throws `error` on its first call, and passes every
// other call through.
function throwingOnce<T extends object>(inner: T, method: string, error: () => Error): T {
    let thrown = false;
    return hooked(inner, { [method]: () => { if (!thrown) { thrown = true; throw error(); } } });
}

// The revocation scenario in a target: K and G projected with root as the
// writer, and a write into K that moves it, a new identity, so that a pass
// observes K and brings in its removal.
async function revocationIn(
    harness: Awaited<ReturnType<IngestionFactory>>, kind: 'concurrent' | 'after', guard: 'insert' | 'update' | 'delete', writer?: 'root' | 'alice',
) {
    const s = await createRevocationScenario(kind, guard);
    const as = writer === 'alice' ? s.alice : s.root;
    const members = [s.K, s.G].map((group) => ({ group, config: { writer: as } }));
    await projectAs(s.K, harness.target, as);
    await projectAs(s.G, harness.target, as);
    const moveK = async (name: string): Promise<void> => {
        const who = await newIdentity();
        const id = await (harness.target as unknown as KeyIndex).registerKey(DEFAULT_KEY_DOMAIN, who.keyId, serializePublicKeyToBase64(who.publicKey));
        await harness.local.insert('identities', { key_id: id, name });
    };
    return { s, members, moveK };
}

async function eventsOf(target: BidirectionalTarget, direction: 'void' | 'reinstate' | 'failure'): Promise<string[]> {
    return (await target.drainOpEvents()).filter((e) => e.event.direction === direction).map((e) => e.event.rowId ?? '');
}

export function createChainCases(label: string, factory: IngestionFactory, nativeFactory: IngestionFactory): NamedTest[] {
    return [
        {
            name: `[${label}-IN49] a row ingested and then deleted remotely before the next projection is gone after it, also after a reopen`,
            invoke: async () => {
                const { group, admin } = await createGroup();
                let harness = await factory();
                try {
                    await projectAs(group, harness.target, admin);
                    const localId = await harness.local.insert('tags', { code: 'x' });
                    await ingestChanges(group, harness.target, { writer: admin });
                    await assertAtCheckpoints(checkedOf(harness.target), [{ group }], 'after the ingestion');
                    const rowId = (await harness.target.resolveRow('tags', localId))!.rowId;
                    await (await group.getTable('tags')).delete(rowId, admin);   // a remote delete
                    if (harness.reopen !== undefined) harness = await harness.reopen();
                    await syncGroup(group, harness.target, { writer: admin });
                    assertEquals(await localRow(harness.target, 'tags', localId), undefined, 'the deleted row is gone');
                    await assertAtCheckpoints(checkedOf(harness.target), [{ group }], 'after the projection');
                } finally {
                    await harness.cleanup?.();
                }
            },
        },
        {
            name: `[${label}-IN50] a refused change with a later change captured during the pass: the settle puts rdb's row back with the later change on top, and the next pass ingests it, with no projection between`,
            invoke: async () => {
                const { group, admin } = await createTypedGroup();
                await (await group.getTable('things')).insert('t1', { label: 'start', code: 'c1' }, admin);
                const t1 = deriveRowId('t1', admin.keyId);
                const { target, read, local, cleanup } = await factory();
                try {
                    await projectAs(group, target, admin);
                    const localId = (await read.syncId('things', t1))!;
                    await local.update('things', localId, { code: 'refused' });
                    let later = false;
                    const hooked = beforeSettle(target, async () => {
                        if (later) return;
                        later = true;
                        await local.update('things', localId, { label: 'later' });
                    });
                    const first = await ingestChanges(group, hooked, { writer: admin });
                    assertEquals(first.rejected.length, 1, 'the readonly change is refused');
                    const row = await localRow(target, 'things', localId);
                    assertEquals(`${row?.code} ${row?.label}`, 'c1 later', "rdb's code is back, with the later label on top");
                    await assertAtCheckpoints(checkedOf(target), [{ group }], 'after the refusal');

                    const second = await ingestChanges(group, target, { writer: admin });
                    assertEquals(second.accepted, 1, 'the later change lands');
                    assertEquals((await rdbRows(group, 'things'))[0].values.label, 'later', 'in rdb');
                    await assertAtCheckpoints(checkedOf(target), [{ group }], 'after the next pass');
                } finally {
                    await cleanup?.();
                }
            },
        },
        {
            name: `[${label}-IN51] a refused update's revert reads rdb at the end of the chain: an update of a and c is refused, a later one of a and b in the same pass lands, and the row keeps the later a`,
            invoke: async () => {
                const { group, admin } = await createTypedGroup();
                await (await group.getTable('things')).insert('t1', { label: 'start', code: 'c1' }, admin);
                const t1 = deriveRowId('t1', admin.keyId);
                const { target, read, local, cleanup } = await factory();
                try {
                    await projectAs(group, target, admin);
                    const localId = (await read.syncId('things', t1))!;
                    await local.update('things', localId, { label: 'first', code: 'bad' });
                    await local.update('things', localId, { label: 'second', note: 'n' });
                    const result = await ingestChanges(group, target, { writer: admin });
                    assertEquals(`${result.accepted} ${result.rejected.length}`, '1 1', 'the first is refused, the second lands');
                    const row = await localRow(target, 'things', localId);
                    assertEquals(`${row?.label} ${row?.note} ${row?.code}`, 'second n c1', "the row is rdb's at the chain's end");
                    await assertAtCheckpoints(checkedOf(target), [{ group }], 'after the pass');
                } finally {
                    await cleanup?.();
                }
            },
        },
        {
            name: `[${label}-IN52] an update with one bad column among good ones is refused whole, with one failure event, and none of its columns land`,
            invoke: async () => {
                const { group, admin } = await createTypedGroup();
                await (await group.getTable('things')).insert('t1', { label: 'start', code: 'c1' }, admin);
                const t1 = deriveRowId('t1', admin.keyId);
                const { target, read, local, cleanup } = await factory();
                try {
                    await projectAs(group, target, admin);
                    const localId = (await read.syncId('things', t1))!;
                    await local.update('things', localId, { label: 'new', note: 'n', code: 'bad' });
                    const result = await ingestChanges(group, target, { writer: admin });
                    assertEquals(`${result.accepted} ${result.rejected.length}`, '0 1', 'the update is refused whole');
                    assertEquals((await target.drainOpEvents()).filter((e) => e.event.direction === 'failure').length, 1, 'one failure event');
                    const rdbRow = (await rdbRows(group, 'things'))[0];
                    assertEquals(`${rdbRow.values.label} ${rdbRow.values.note ?? null}`, 'start null', 'no column landed');
                    const row = await localRow(target, 'things', localId);
                    assertEquals(`${row?.label} ${row?.note} ${row?.code}`, 'start null c1', 'and the row is put back');
                    await assertAtCheckpoints(checkedOf(target), [{ group }], 'after the pass');
                } finally {
                    await cleanup?.();
                }
            },
        },
        {
            name: `[${label}-IN54] a crash in the middle of the walk: the next cycle settles what was appended, with the checkpoint at the crashed pass's last entry, then ingests the rest with a change captured after the crash`,
            invoke: async () => {
                const { group, admin } = await createGroup();
                const { target, local, cleanup } = await factory();
                try {
                    await projectAs(group, target, admin);
                    const entriesBefore = await entryCount(group);
                    for (const code of ['a', 'b', 'c']) await local.insert('tags', { code });
                    // The third record fails: two entries are appended, the third change gets none.
                    const crashing = hooked(target, { writeRecord: throwOnCall('writeRecord', 3) });
                    let crashed = false;
                    try { await ingestChanges(group, crashing, { writer: admin }); } catch (e) { crashed = e instanceof SimulatedCrash; }
                    assertTrue(crashed, 'the pass ends at the failed record');
                    const records = (await target.drainChanges()).changes.map((c) => c.record?.entry);
                    assertTrue(records[0] !== undefined && records[1] !== undefined && records[2] === undefined, 'two changes have entries');
                    await assertAtCheckpoints(checkedOf(target), [{ group }], 'after the crash');
                    await local.insert('tags', { code: 'd' });   // captured after the crash

                    const settles: IngestSettle[] = [];
                    const watched = beforeSettle(target, (settle) => { settles.push(settle as IngestSettle); });
                    const result = await ingestChanges(group, watched, { writer: admin });
                    assertEquals(result.accepted, 4, 'the crashed pass\'s two and the new pass\'s two');
                    assertTrue(sameVersion(settles[0].checkpoints![0].next, new Set([records[1]!])), "the first settle's checkpoint is the crashed pass's last entry");
                    assertEquals(await entryCount(group), entriesBefore + 4, 'each change appended once');
                    assertEquals((await rdbRows(group, 'tags')).map((r) => r.values.code).sort().join(','), 'a,b,c,d', 'every row');
                    assertEquals((await target.drainChanges()).changes.length, 0, 'the outbox is empty');
                    await assertAtCheckpoints(checkedOf(target), [{ group }], 'after the next cycle');
                } finally {
                    await cleanup?.();
                }
            },
        },
        {
            name: `[${label}-IN55] a crash between recording an entry's hash and its append: the next cycle builds the change again, records the new hash over the old one, and appends it once`,
            invoke: async () => {
                const { group, admin } = await createGroup();
                const { target, local, cleanup } = await factory();
                try {
                    await projectAs(group, target, admin);
                    const entriesBefore = await entryCount(group);
                    const localId = await local.insert('tags', { code: 'once' });
                    const crashing = hooked(group, { appendPrepared: throwOnCall('appendPrepared', 1) }) as RTableGroup;
                    let crashed = false;
                    try { await ingestChanges(crashing, target, { writer: admin }); } catch (e) { crashed = e instanceof SimulatedCrash; }
                    assertTrue(crashed, 'the pass ends at the append');
                    const stale = (await target.drainChanges()).changes[0].record?.entry;
                    assertTrue(stale !== undefined, 'the hash was recorded first');
                    assertEquals(await entryCount(group), entriesBefore, 'and nothing appended');

                    const result = await ingestChanges(group, target, { writer: admin });
                    assertEquals(result.accepted, 1, 'the change lands');
                    assertEquals(await entryCount(group), entriesBefore + 1, 'once');
                    const mapping = await target.resolveRow('tags', localId);
                    assertTrue(mapping?.rowId !== undefined && (await rdbRows(group, 'tags'))[0].rowId === mapping.rowId, 'its mapping is the appended row');
                    await assertAtCheckpoints(checkedOf(target), [{ group }], 'after the next cycle');
                } finally {
                    await cleanup?.();
                }
            },
        },
        {
            name: `[${label}-IN56] a crash after a refusal and a later entry: the next cycle reports the refusal with its recorded reason and reverts the row, also under another writer key, without validating the change again`,
            invoke: async () => {
                const { group, admin } = await createTypedGroup();
                await (await group.getTable('things')).insert('t1', { label: 'start', code: 'c1' }, admin);
                const t1 = deriveRowId('t1', admin.keyId);
                const { target, read, local, cleanup } = await factory();
                try {
                    await projectAs(group, target, admin);
                    const localId = (await read.syncId('things', t1))!;
                    await local.update('things', localId, { code: 'refused' });
                    await local.insert('things', { label: 'later' });
                    const first = await ingestChanges(group, new CrashBeforeSettle(target), { writer: admin });
                    const reason = first.rejected[0]?.reason;
                    assertTrue(reason !== undefined && reason.includes('readonly'), 'the crashed pass refused the update');

                    const other = await makeIdentity();
                    const next = await ingestChanges(group, target, { writer: other });
                    assertEquals(`${next.accepted} ${next.rejected.length}`, '1 1', 'the next cycle reports both');
                    assertEquals(next.rejected[0].reason, reason, 'with the recorded reason');
                    const row = await localRow(target, 'things', localId);
                    assertEquals(row?.code, 'c1', 'and the row is put back');
                    assertEquals((await target.drainOpEvents()).filter((e) => e.event.direction === 'failure').length, 1, 'one failure event');
                    await assertAtCheckpoints(checkedOf(target), [{ group }], 'after the next cycle');
                } finally {
                    await cleanup?.();
                }
            },
        },
        {
            name: `[${label}-IN61] an update with nothing to write leaves the outbox at the settle with no event, also after a crash`,
            invoke: async () => {
                const { group, admin } = await createTypedGroup();
                await (await group.getTable('things')).insert('t1', { label: 'start', code: 'c1' }, admin);
                const t1 = deriveRowId('t1', admin.keyId);
                const { target, read, local, cleanup } = await factory();
                try {
                    await projectAs(group, target, admin);
                    const localId = (await read.syncId('things', t1))!;
                    const entriesBefore = await entryCount(group);
                    await local.update('things', localId, { code: 'c1' });   // a readonly column at its own value
                    const result = await ingestChanges(group, target, { writer: admin });
                    assertEquals(`${result.accepted} ${result.rejected.length}`, '0 0', 'nothing written, nothing refused');
                    assertEquals((await target.drainChanges()).changes.length, 0, 'the outbox is empty');

                    await local.update('things', localId, { code: 'c1' });
                    await ingestChanges(group, new CrashBeforeSettle(target), { writer: admin });
                    assertEquals((await target.drainChanges()).changes[0]?.record?.nothing, true, 'the crashed pass recorded the mark');
                    await ingestChanges(group, target, { writer: admin });
                    assertEquals((await target.drainChanges()).changes.length, 0, 'the next cycle settles it');
                    assertEquals(await entryCount(group), entriesBefore, 'no entry');
                    assertEquals((await target.drainOpEvents()).length, 0, 'no event');
                    await assertAtCheckpoints(checkedOf(target), [{ group }], 'after the next cycle');
                } finally {
                    await cleanup?.();
                }
            },
        },
        {
            name: `[${label}-IN62] a local update concurrent with a remote one of the same column: both reach rdb, and the target shows rdb's winner after the projection`,
            invoke: async () => {
                const { group, admin } = await createTypedGroup();
                const things = await group.getTable('things');
                await things.insert('t1', { label: 'start' }, admin);
                const t1 = deriveRowId('t1', admin.keyId);
                const { target, read, local, cleanup } = await factory();
                try {
                    await projectAs(group, target, admin);
                    const localId = (await read.syncId('things', t1))!;
                    await things.update(t1, { label: 'remote' }, admin);
                    await local.update('things', localId, { label: 'local' });
                    const result = await ingestChanges(group, target, { writer: admin });
                    assertEquals(result.accepted, 1, 'the local update lands at the checkpoint');
                    await assertAtCheckpoints(checkedOf(target), [{ group }], 'after the ingestion');
                    await syncGroup(group, target, { writer: admin });
                    const winner = (await rdbRows(group, 'things'))[0].values.label;
                    assertTrue(winner === 'local' || winner === 'remote', `rdb picks one, got ${String(winner)}`);
                    assertEquals((await localRow(target, 'things', localId))?.label, winner, 'and the target shows it');
                    await assertAtCheckpoints(checkedOf(target), [{ group }], 'after the projection');
                } finally {
                    await cleanup?.();
                }
            },
        },
        {
            name: `[${label}-IN63] a comment on a post deleted remotely after the checkpoint is accepted, then voided by the projection, and logged`,
            invoke: async () => {
                const { group, admin } = await createFkGroup();
                await (await group.getTable('posts')).insert('p1', { title: 'T' }, admin);
                const p1 = deriveRowId('p1', admin.keyId);
                const { target, read, local, cleanup } = await factory();
                try {
                    await projectAs(group, target, admin);
                    const postId = (await read.syncId('posts', p1))!;
                    await (await group.getTable('posts')).delete(p1, admin);   // remote, after the checkpoint
                    const commentId = await local.insert('comments', { body: 'late', post_id: postId });
                    const result = await ingestChanges(group, target, { writer: admin });
                    assertEquals(result.accepted, 1, 'the post is live at the checkpoint, so the comment is accepted');
                    await assertAtCheckpoints(checkedOf(target), [{ group }], 'after the ingestion');
                    await syncGroup(group, target, { writer: admin });
                    assertEquals((await rdbRows(group, 'comments')).length, 0, 'the concurrent delete voids it in rdb');
                    assertEquals(await localRow(target, 'comments', commentId), undefined, 'and the projection removes it');
                    assertTrue((await target.drainOpEvents()).some((e) => e.event.direction === 'void'
                        && e.event.table === 'comments' && e.event.localId === commentId), 'with a void event naming the row by its local id');
                    await assertAtCheckpoints(checkedOf(target), [{ group }], 'after the projection');
                } finally {
                    await cleanup?.();
                }
            },
        },
        {
            name: `[${label}-IN64] a change ingested while a deploy is not yet projected is translated with the schema at the checkpoint, and the projection then applies the deploy`,
            invoke: async () => {
                const { group, schema, admin } = await createGroup();
                const { target, local, cleanup } = await factory();
                try {
                    await projectAs(group, target, admin);
                    await schema.updateSchema([{ rule: 'add-column', table: 'tags', column: 'extra', def: { type: 'string', nullable: true } }], admin, 'add extra');
                    await group.deploy(await (await schema.getScopedDag()).getFrontier());
                    const localId = await local.insert('tags', { code: 'x' });
                    const result = await ingestChanges(group, target, { writer: admin });
                    assertEquals(result.accepted, 1, 'accepted under the schema at the checkpoint');
                    await assertAtCheckpoints(checkedOf(target), [{ group }], 'after the ingestion');
                    await syncGroup(group, target, { writer: admin });
                    const row = await localRow(target, 'tags', localId);
                    assertTrue(row !== undefined && 'extra' in row, 'the deploy is projected');
                    await assertAtCheckpoints(checkedOf(target), [{ group }], 'after the projection');
                } finally {
                    await cleanup?.();
                }
            },
        },
        {
            name: `[${label}-IN71] an INSERT OR REPLACE over a row from rdb, and over one inserted on this device, is refused: rdb's row comes back and the mapping stays active`,
            invoke: async () => {
                const { group, admin } = await createGroup();
                await (await group.getTable('tags')).insert('g1', { code: 'from-rdb' }, admin);
                const g1 = deriveRowId('g1', admin.keyId);
                const { target, read, local, cleanup } = await nativeFactory();
                if (local.replaceAt === undefined) { await cleanup?.(); return; }
                try {
                    await projectAs(group, target, admin);
                    const projected = (await read.syncId('tags', g1))!;
                    const mine = await local.insert('tags', { code: 'mine' });
                    await ingestChanges(group, target, { writer: admin });
                    const failures = async () => (await target.drainOpEvents()).filter((e) => e.event.direction === 'failure').length;
                    for (const [localId, code] of [[projected, 'from-rdb'], [mine, 'mine']] as const) {
                        const logged = await failures();
                        await local.replaceAt('tags', localId, { code: 'replaced' });
                        const result = await ingestChanges(group, target, { writer: admin });
                        assertEquals(`${result.accepted} ${result.rejected.length}`, '0 1', `the replace of ${code} is refused`);
                        assertEquals(await failures(), logged + 1, 'and logged as a failure');
                        assertEquals((await localRow(target, 'tags', localId))?.code, code, "rdb's row is back");
                        assertEquals((await target.resolveRow('tags', localId))?.status, 'active', 'and its mapping stays active');
                        assertEquals((await target.drainChanges()).changes.length, 0, 'the outbox is empty');
                        await assertAtCheckpoints(checkedOf(target), [{ group }], `after the replace of ${code}`);
                    }
                    await projectGroup(group, target);   // a projection applies
                } finally {
                    await cleanup?.();
                }
            },
        },
        {
            name: `[${label}-IN95] under manual, an INSERT OR REPLACE over a row inserted on this device that names another author is refused: rdb's row and author come back, the refusal is logged, and rdb gets no second row`,
            invoke: async () => {
                const { group, admin } = await createTypedGroup();
                const other = await makeIdentity();
                const { target, local, cleanup } = await nativeFactory();
                if (local.replaceAt === undefined) { await cleanup?.(); return; }
                try {
                    await projectAs(group, target, admin, { author: 'manual' });
                    const keys = target as unknown as KeyIndex;
                    const adminId = (await keys.idForKeyHash(DEFAULT_KEY_DOMAIN, admin.keyId))!;
                    const otherId = await keys.registerKey(DEFAULT_KEY_DOMAIN, other.keyId, serializePublicKeyToBase64(other.publicKey));
                    const mine = await local.insert('things', { label: 'mine', author_key_id: adminId });
                    await ingestChanges(group, target, { writer: admin, author: 'manual' });
                    await local.replaceAt('things', mine, { label: 'replaced', author_key_id: otherId });
                    const result = await ingestChanges(group, target, { writer: admin, author: 'manual' });
                    assertEquals(`${result.accepted} ${result.rejected.length}`, '0 1', 'the replace is refused');
                    assertEquals((await target.drainOpEvents()).filter((e) => e.event.direction === 'failure').length, 1, 'and logged');
                    const row = await localRow(target, 'things', mine);
                    assertEquals(`${row?.label} ${row?.author_key_id}`, `mine ${adminId}`, "rdb's row and author are back");
                    const rows = await rdbRows(group, 'things');
                    assertEquals(rows.map((r) => `${r.values.label} ${r.author}`).join(','), `mine ${admin.keyId}`, 'rdb has its one row');
                    await assertAtCheckpoints(checkedOf(target), [{ group }], 'after the refused replace');
                } finally {
                    await cleanup?.();
                }
            },
        },
        {
            name: `[${label}-IN72] an insert that reuses a deleted row's id is refused: the row is removed and the mapping stays deleted`,
            invoke: async () => {
                const { group, admin } = await createGroup();
                await (await group.getTable('tags')).insert('g1', { code: 'old' }, admin);
                const g1 = deriveRowId('g1', admin.keyId);
                const { target, read, local, cleanup } = await nativeFactory();
                if (local.insertAt === undefined) { await cleanup?.(); return; }
                try {
                    await projectAs(group, target, admin);
                    const localId = (await read.syncId('tags', g1))!;
                    await local.delete('tags', localId);
                    await ingestChanges(group, target, { writer: admin });
                    assertEquals((await target.resolveRow('tags', localId))?.status, 'deleted', 'the delete lands');
                    await local.insertAt('tags', localId, { code: 'reused' });
                    const result = await ingestChanges(group, target, { writer: admin });
                    assertEquals(`${result.accepted} ${result.rejected.length}`, '0 1', 'the insert is refused');
                    assertEquals((await target.drainOpEvents()).filter((e) => e.event.direction === 'failure').length, 1,
                        'and logged as a failure');
                    assertEquals(await localRow(target, 'tags', localId), undefined, 'its row is removed');
                    assertEquals((await target.resolveRow('tags', localId))?.status, 'deleted', 'the mapping stays deleted');
                    assertEquals((await target.drainChanges()).changes.length, 0, 'the outbox is empty');
                    await assertAtCheckpoints(checkedOf(target), [{ group }], 'after the refusal');
                    await projectGroup(group, target);
                } finally {
                    await cleanup?.();
                }
            },
        },
        {
            name: `[${label}-IN73] an insert, a delete and an insert of one local id in one pass: the second insert is refused, and the mapping ends deleted`,
            invoke: async () => {
                const { group, admin } = await createGroup();
                const { target, local, cleanup } = await nativeFactory();
                if (local.insertAt === undefined) { await cleanup?.(); return; }
                try {
                    await projectAs(group, target, admin);
                    const localId = await local.insert('tags', { code: 'first' });
                    await local.delete('tags', localId);
                    await local.insertAt('tags', localId, { code: 'second' });
                    const result = await ingestChanges(group, target, { writer: admin });
                    assertEquals(`${result.accepted} ${result.rejected.length}`, '2 1', 'the insert and the delete land, the second insert is refused');
                    assertEquals(await localRow(target, 'tags', localId), undefined, 'no row');
                    assertEquals((await target.resolveRow('tags', localId))?.status, 'deleted', 'the mapping ends deleted');
                    assertEquals((await rdbRows(group, 'tags')).length, 0, 'rdb has no live row');
                    await assertAtCheckpoints(checkedOf(target), [{ group }], 'after the pass');
                    await projectGroup(group, target);
                } finally {
                    await cleanup?.();
                }
            },
        },
        {
            name: `[${label}-IN74] an update of a row whose insert was refused is refused too, and nothing reaches rdb`,
            invoke: async () => {
                const { group, admin } = await createFkGroup();
                const { target, local, cleanup } = await factory();
                try {
                    await projectAs(group, target, admin);
                    const entriesBefore = await entryCount(group);
                    const localId = await local.insert('comments', { body: 'orphan', post_id: 9999 });
                    await local.update('comments', localId, { body: 'edited' });
                    const result = await ingestChanges(group, target, { writer: admin });
                    assertEquals(`${result.accepted} ${result.rejected.length}`, '0 2', 'both are refused');
                    assertEquals((await target.drainOpEvents()).filter((e) => e.event.direction === 'failure').length, 2, 'two failure events');
                    assertEquals(await entryCount(group), entriesBefore, 'nothing reaches rdb');
                    assertEquals(await localRow(target, 'comments', localId), undefined, 'the row is removed');
                    assertEquals((await target.drainChanges()).changes.length, 0, 'the outbox is empty');
                    await assertAtCheckpoints(checkedOf(target), [{ group }], 'after the pass');
                    await projectGroup(group, target);
                } finally {
                    await cleanup?.();
                }
            },
        },
        {
            name: `[${label}-IN75] a change of a table no member projects gets the mark of nothing to write: it leaves the outbox with no event, and a projection then applies`,
            invoke: async () => {
                const a = await createGroup();
                const b = await createFkGroup();
                const { target, local, cleanup } = await factory();
                try {
                    await projectAs(a.group, target, a.admin);
                    await projectAs(b.group, target, b.admin, { tablePrefix: 'b' });
                    await local.insert('b_posts', { title: 'not ingested here' });
                    const result = await ingestChanges(a.group, target, { writer: a.admin });
                    assertEquals(`${result.accepted} ${result.rejected.length}`, '0 0', 'nothing ingested, nothing refused');
                    assertEquals((await target.drainChanges()).changes.length, 0, 'the outbox is empty');
                    assertEquals((await target.drainOpEvents()).length, 0, 'no event');
                    await projectGroup(a.group, target);   // no PendingChangesError
                } finally {
                    await cleanup?.();
                }
            },
        },
        {
            name: `[${label}-IN77] two devices update different fields of one row concurrently, each naming only its field, and both land`,
            invoke: async () => {
                const { group, admin } = await createTypedGroup();
                await (await group.getTable('things')).insert('t1', { label: 'start' }, admin);
                const t1 = deriveRowId('t1', admin.keyId);
                const one = await factory();
                const two = await factory();
                try {
                    await projectAs(group, one.target, admin);
                    await projectAs(group, two.target, admin);
                    await one.local.update('things', (await one.read.syncId('things', t1))!, { label: 'from one' });
                    await two.local.update('things', (await two.read.syncId('things', t1))!, { note: 'from two' });
                    await ingestChanges(group, one.target, { writer: admin });
                    await ingestChanges(group, two.target, { writer: admin });
                    await syncGroup(group, one.target, { writer: admin });
                    await syncGroup(group, two.target, { writer: admin });
                    const values = (await rdbRows(group, 'things'))[0].values;
                    assertEquals(`${values.label} ${values.note}`, 'from one from two', 'both fields land in rdb');
                    for (const h of [one, two]) {
                        const row = await localRow(h.target, 'things', (await h.read.syncId('things', t1))!);
                        assertEquals(`${row?.label} ${row?.note}`, 'from one from two', 'and on each device');
                        await assertAtCheckpoints(checkedOf(h.target), [{ group }], 'after both projections');
                    }
                } finally {
                    await one.cleanup?.();
                    await two.cleanup?.();
                }
            },
        },
        {
            name: `[${label}-IN78] an update of one field after a refused update of another lands, and the row shows rdb's value of the refused field with the new one`,
            invoke: async () => {
                const { group, admin } = await createTypedGroup();
                await (await group.getTable('things')).insert('t1', { label: 'start', code: 'c1' }, admin);
                const t1 = deriveRowId('t1', admin.keyId);
                const { target, read, local, cleanup } = await factory();
                try {
                    await projectAs(group, target, admin);
                    const localId = (await read.syncId('things', t1))!;
                    await local.update('things', localId, { code: 'bad' });
                    await local.update('things', localId, { note: 'kept' });
                    const result = await ingestChanges(group, target, { writer: admin });
                    assertEquals(`${result.accepted} ${result.rejected.length}`, '1 1', 'the second lands');
                    const row = await localRow(target, 'things', localId);
                    assertEquals(`${row?.code} ${row?.note}`, 'c1 kept', "rdb's code with the new note");
                    await assertAtCheckpoints(checkedOf(target), [{ group }], 'after the pass');
                } finally {
                    await cleanup?.();
                }
            },
        },
        {
            name: `[${label}-IN80] a change whose entry throws an error that isn't a refusal gets a failure event with the error's message, leaves the outbox at the settle, and a projection then applies`,
            invoke: async () => {
                const { group, admin } = await createGroup();
                const { target, local, cleanup } = await factory();
                try {
                    await projectAs(group, target, admin);
                    const localId = await local.insert('tags', { code: 'x' });
                    const broken = throwingOnce(group, 'prepareRowOp', () => new Error('the entry could not be built')) as RTableGroup;
                    const result = await ingestChanges(broken, target, { writer: admin });
                    assertEquals(result.rejected.map((r) => r.reason).join(','), 'the entry could not be built', 'a failure with the message');
                    const events = (await target.drainOpEvents()).filter((e) => e.event.direction === 'failure');
                    assertEquals(events.length, 1, 'one failure event');
                    assertEquals(await localRow(target, 'tags', localId), undefined, 'the refused insert is removed');
                    assertEquals((await target.drainChanges()).changes.length, 0, 'the outbox is empty');
                    await assertAtCheckpoints(checkedOf(target), [{ group }], 'after the pass');
                    await projectGroup(group, target);
                } finally {
                    await cleanup?.();
                }
            },
        },
        {
            name: `[${label}-IN81] an append that throws once ends the pass with the error; the next cycle builds the entry again and appends it once`,
            invoke: async () => {
                const { group, admin } = await createTypedGroup();
                await (await group.getTable('things')).insert('t1', { label: 'start' }, admin);
                const t1 = deriveRowId('t1', admin.keyId);
                const { target, read, local, cleanup } = await factory();
                try {
                    await projectAs(group, target, admin);
                    const localId = (await read.syncId('things', t1))!;
                    const entriesBefore = await entryCount(group);
                    await local.update('things', localId, { label: 'again' });
                    const failing = throwingOnce(group, 'appendPrepared', () => new Error('disk full')) as RTableGroup;
                    let error: unknown;
                    try { await ingestChanges(failing, target, { writer: admin }); } catch (e) { error = e; }
                    assertTrue(error instanceof Error && error.message === 'disk full', 'the pass ends with the error');
                    const result = await ingestChanges(group, target, { writer: admin });
                    assertEquals(result.accepted, 1, 'the next cycle lands it');
                    assertEquals(await entryCount(group), entriesBefore + 1, 'once');
                    assertEquals((await rdbRows(group, 'things'))[0].values.label, 'again', 'in rdb');
                    await assertAtCheckpoints(checkedOf(target), [{ group }], 'after the next cycle');
                } finally {
                    await cleanup?.();
                }
            },
        },
        {
            name: `[${label}-IN82] a record that can't be written, an entry's hash or a failure, ends the pass: no later change gets a record, and the next cycle handles each change once`,
            invoke: async () => {
                for (const failAt of ['entry', 'failure'] as const) {
                    const { group, admin } = await createFkGroup();
                    const { target, local, cleanup } = await factory();
                    try {
                        await projectAs(group, target, admin);
                        const entriesBefore = await entryCount(group);
                        // the first change is refused (a dangling reference) or accepted
                        await local.insert('comments', failAt === 'failure' ? { body: 'orphan', post_id: 9999 } : { body: 'fine' });
                        await local.insert('posts', { title: 'second' });
                        const failing = hooked(target, { writeRecord: () => { throw new Error('busy'); } });
                        let error: unknown;
                        try { await ingestChanges(group, failing, { writer: admin }); } catch (e) { error = e; }
                        assertTrue(error instanceof Error && error.message === 'busy', `${failAt}: the pass ends`);
                        assertTrue((await target.drainChanges()).changes.every((c) => c.record === undefined), `${failAt}: no change has a record`);
                        assertEquals(await entryCount(group), entriesBefore, `${failAt}: nothing appended`);
                        const result = await ingestChanges(group, target, { writer: admin });
                        assertEquals(`${result.accepted} ${result.rejected.length}`, failAt === 'failure' ? '1 1' : '2 0', `${failAt}: each change once`);
                        assertEquals(await entryCount(group), entriesBefore + result.accepted, `${failAt}: one entry for each accepted change`);
                        await assertAtCheckpoints(checkedOf(target), [{ group }], `${failAt}: after the next cycle`);
                    } finally {
                        await cleanup?.();
                    }
                }
            },
        },
        {
            name: `[${label}-IN83] an entry the DAG appends under another hash than the one recorded for it stops ingestion with PreparedHashMismatchError`,
            invoke: async () => {
                const { group, admin } = await createGroup();
                const { target, local, cleanup } = await factory();
                try {
                    await projectAs(group, target, admin);
                    await local.insert('tags', { code: 'x' });
                    const skewed = new Proxy(group, {
                        get(obj, prop, receiver) {
                            const value = Reflect.get(obj, prop, receiver);
                            if (prop !== 'prepareRowOp') return typeof value === 'function' ? value.bind(obj) : value;
                            return async (...args: Parameters<RTableGroup['prepareRowOp']>) =>
                                ({ ...(await obj.prepareRowOp(...args)), hash: 'not-the-hash' });
                        },
                    });
                    let error: unknown;
                    try { await ingestChanges(skewed, target, { writer: admin }); } catch (e) { error = e; }
                    assertTrue(error instanceof PreparedHashMismatchError, `the mismatch stops the pass, got ${String(error)}`);
                } finally {
                    await cleanup?.();
                }
            },
        },
        {
            name: `[${label}-IN84] a refused insert, then an insert on its local id, in two passes and in one, and with a crash before the settle: each is refused once, no row is left, and the mapping is ingestion_failure`,
            invoke: async () => {
                const run = async (mode: 'two passes' | 'one pass' | 'crash'): Promise<void> => {
                    const { group, admin } = await createFkGroup();
                    const { target, local, cleanup } = await nativeFactory();
                    try {
                        if (mode === 'two passes' ? local.insertAt === undefined : local.replaceAt === undefined) return;
                        await projectAs(group, target, admin);
                        const localId = await local.insert('comments', { body: 'orphan', post_id: 9999 });
                        if (mode === 'two passes') {
                            await ingestChanges(group, target, { writer: admin });
                            await local.insertAt!('comments', localId, { body: 'again', post_id: 9998 });
                        } else {
                            await local.replaceAt!('comments', localId, { body: 'again', post_id: 9998 });
                        }
                        if (mode === 'crash') await ingestChanges(group, new CrashBeforeSettle(target), { writer: admin });
                        await ingestChanges(group, target, { writer: admin });
                        const failures = (await target.drainOpEvents()).filter((e) => e.event.direction === 'failure');
                        assertEquals(failures.length, 2, `${mode}: two failure events`);
                        assertEquals(await localRow(target, 'comments', localId), undefined, `${mode}: no row`);
                        assertEquals((await target.resolveRow('comments', localId))?.status, 'ingestion_failure', `${mode}: the mapping`);
                        assertEquals((await target.drainChanges()).changes.length, 0, `${mode}: the outbox is empty`);
                        await assertAtCheckpoints(checkedOf(target), [{ group }], `${mode}: after the passes`);
                    } finally {
                        await cleanup?.();
                    }
                };
                await run('two passes');
                await run('one pass');
                await run('crash');
            },
        },
        {
            name: `[${label}-IN85] a revert of a row with a pending insert on its local id shows the insert's row until that insert is refused`,
            invoke: async () => {
                const { group, admin } = await createTypedGroup();
                await (await group.getTable('things')).insert('t1', { label: 'rdb', code: 'c1' }, admin);
                const t1 = deriveRowId('t1', admin.keyId);
                const { target, read, local, cleanup } = await nativeFactory();
                if (local.replaceAt === undefined && local.insertAt === undefined) { await cleanup?.(); return; }
                try {
                    await projectAs(group, target, admin);
                    const localId = (await read.syncId('things', t1))!;
                    await local.update('things', localId, { code: 'refused' });
                    let pending = false;
                    const hooked = beforeSettle(target, async () => {
                        if (pending) return;
                        pending = true;
                        if (local.replaceAt !== undefined) {
                            await local.replaceAt('things', localId, { label: 'inserted' });
                        } else {
                            await local.delete('things', localId);
                            await local.insertAt!('things', localId, { label: 'inserted' });
                        }
                    });
                    await ingestChanges(group, hooked, { writer: admin });
                    assertEquals((await localRow(target, 'things', localId))?.label, 'inserted', "the pending insert's row is shown");
                    await assertAtCheckpoints(checkedOf(target), [{ group }], 'after the revert');
                    await ingestChanges(group, target, { writer: admin });
                    const row = await localRow(target, 'things', localId);
                    if (local.replaceAt !== undefined) {
                        assertEquals(`${row?.label} ${row?.code}`, 'rdb c1', "once it is refused, rdb's row is back");
                    } else {
                        // Without INSERT OR REPLACE the insert follows a delete, which lands.
                        assertEquals(row, undefined, 'once the delete lands and the insert is refused, no row');
                        assertEquals((await rdbRows(group, 'things')).length, 0, 'rdb deleted it');
                    }
                    await assertAtCheckpoints(checkedOf(target), [{ group }], 'after its refusal');
                } finally {
                    await cleanup?.();
                }
            },
        },
        {
            name: `[${label}-IN53] the void scenario, with a rule and with a foreign key: a note that needs Bob's grant, written while the only observe of it is void, is refused and its row removed; after a live observe, the note is accepted`,
            invoke: async () => {
                for (const form of ['rule', 'fk'] as const) {
                    const v = await createVoidScenario(form);
                    const { target, local, cleanup } = await factory();
                    try {
                        await projectAs(v.K, target, v.bob);
                        await projectAs(v.G, target, v.bob);
                        const groups = [{ group: v.K }, { group: v.G }];
                        // In the target, the cross-group key projects as the grant's row hash.
                        const note = (body: string): LocalValues => form === 'rule' ? { body } : { body, grant_row_hash: v.grantRow };
                        const refusedId = await local.insert('notes', note('needs the grant'));
                        const refused = await ingestChanges(v.G, target, { writer: v.bob });
                        assertEquals(`${refused.accepted} ${refused.rejected.length}`, '0 1', `${form}: void where it stands, so refused`);
                        assertEquals(await localRow(target, 'notes', refusedId), undefined, `${form}: its row is removed`);
                        assertEquals((await target.resolveRow('notes', refusedId))?.status, 'ingestion_failure', `${form}: the mapping`);
                        await assertAtCheckpoints(checkedOf(target), groups, `${form}: after the refusal`);

                        await v.G.observe('k', v.kGrant, v.alice);   // a live observe, by an admin
                        await syncGroup(v.G, target, { writer: v.bob });
                        await local.insert('notes', note('after the live observe'));
                        const accepted = await ingestChanges(v.G, target, { writer: v.bob });
                        assertEquals(`${accepted.accepted} ${accepted.rejected.length}`, '1 0', `${form}: then it is accepted`);
                        await assertAtCheckpoints(checkedOf(target), groups, `${form}: after the acceptance`);
                    } finally {
                        await cleanup?.();
                    }
                }
            },
        },
        {
            name: `[${label}-IN57] a crash between a permission's entry and the observe its use needs: the next cycle's end writes the observe, and the use is accepted`,
            invoke: async () => {
                const ci = await createCapsItems();
                const members = [{ group: ci.perm, config: { writer: ci.admin } }, { group: ci.items, config: { writer: ci.admin } }];
                const { target, local, cleanup } = await factory();
                try {
                    await projectAs(ci.perm, target, ci.admin);
                    await projectAs(ci.items, target, ci.admin);
                    const itemsBefore = await entryCount(ci.items);
                    await local.insert('caps', { label: 'grant' });
                    await local.insert('items', { name: 'thing' });
                    // The second record is the observe the use needs: it fails.
                    const crashing = hooked(target, { writeRecord: throwOnCall('writeRecord', 2) });
                    let crashed = false;
                    try { await ingestDatabaseChanges(members, crashing); } catch (e) { crashed = e instanceof SimulatedCrash; }
                    assertTrue(crashed, 'the pass ends before the observe');
                    assertEquals(await entryCount(ci.items), itemsBefore, 'items has nothing new');
                    await assertAtCheckpoints(checkedOf(target), members, 'after the crash');

                    const results = await ingestDatabaseChanges(members, target);
                    assertEquals(results.get(ci.perm.getId())!.accepted, 1, 'the permission is settled');
                    assertEquals(`${results.get(ci.items.getId())!.accepted} ${results.get(ci.items.getId())!.rejected.length}`, '1 0', 'and the use is accepted');
                    assertEquals(await entryCount(ci.items), itemsBefore + 2, 'one observe and the use');
                    await assertAtCheckpoints(checkedOf(target), members, 'after the next cycle');
                } finally {
                    await cleanup?.();
                }
            },
        },
        {
            name: `[${label}-IN58] the end's own observes survive a crash before the settle beside the checkpoints, and none is written twice; one recorded and not appended is written once and the stale one cleared`,
            invoke: async () => {
                for (const where of ['after the append', 'before the append'] as const) {
                    const ci = await createCapsItems();
                    const members = [{ group: ci.perm, config: { writer: ci.admin } }, { group: ci.items, config: { writer: ci.admin } }];
                    const { target, local, cleanup } = await factory();
                    try {
                        await projectAs(ci.perm, target, ci.admin);
                        await projectAs(ci.items, target, ci.admin);
                        const itemsBefore = await entryCount(ci.items);
                        await local.insert('caps', { label: 'grant' });   // no use: the end writes the observe
                        if (where === 'before the append') {
                            const recording = new Proxy(target, {
                                get(obj, prop, receiver) {
                                    const value = Reflect.get(obj, prop, receiver);
                                    if (prop !== 'recordEndObserve') return typeof value === 'function' ? value.bind(obj) : value;
                                    return async (groupId: string, hash: string) => {
                                        await obj.recordEndObserve(groupId, hash);
                                        throw new SimulatedCrash('the append of an end observe');
                                    };
                                },
                            });
                            let crashed = false;
                            try { await ingestDatabaseChanges(members, recording); } catch (e) { crashed = e instanceof SimulatedCrash; }
                            assertTrue(crashed, `${where}: the end stops`);
                            assertEquals(await entryCount(ci.items), itemsBefore, `${where}: the observe isn't appended`);
                        } else {
                            await ingestDatabaseChanges(members, new CrashBeforeSettle(target));
                            assertEquals(await entryCount(ci.items), itemsBefore + 1, `${where}: the end appended its observe`);
                        }
                        assertEquals((await target.getEndObserves(ci.items.getId())).length, 1, `${where}: recorded beside the checkpoint`);
                        await assertAtCheckpoints(checkedOf(target), members, `${where}: after the crash`);

                        await ingestDatabaseChanges(members, target);
                        assertEquals(await entryCount(ci.items), itemsBefore + 1, `${where}: the observe is written once`);
                        assertEquals((await target.getEndObserves(ci.items.getId())).length, 0, `${where}: the settle clears the list`);
                        const [observe] = await target.getCheckpoint(ci.items.getId()).then((v) => [...(v ?? [])]);
                        assertTrue(await (await ci.items.getScopedDag()).loadEntry(observe) !== undefined, `${where}: the checkpoint holds it`);
                        await assertAtCheckpoints(checkedOf(target), members, `${where}: after the next cycle`);
                    } finally {
                        await cleanup?.();
                    }
                }
            },
        },
        {
            name: `[${label}-IN59] a signed observe written for a change that rdb then refuses, with no later change of that group accepted: the end writes no second observe`,
            invoke: async () => {
                const v = await createVoidScenario('rule');
                const members = [{ group: v.K, config: { writer: v.alice } }, { group: v.G, config: { writer: v.alice } }];
                const { target, local, cleanup } = await factory();
                try {
                    await projectAs(v.K, target, v.alice);
                    await projectAs(v.G, target, v.alice);
                    const gBefore = await entryCount(v.G);
                    await local.insert('grants', { holder: 'someone' });     // moves K
                    await local.insert('notes', { body: 'alice holds no grant' });   // G observes K, then is refused
                    const results = await ingestDatabaseChanges(members, target);
                    assertEquals(results.get(v.K.getId())!.accepted, 1, 'the grant lands');
                    assertEquals(`${results.get(v.G.getId())!.accepted} ${results.get(v.G.getId())!.rejected.length}`, '0 1', 'the note is refused');
                    assertEquals(await entryCount(v.G), gBefore + 1, "only the observe the note's change recorded");
                    await assertAtCheckpoints(checkedOf(target), members, 'after the pass');
                } finally {
                    await cleanup?.();
                }
            },
        },
        {
            name: `[${label}-IN60] two targets make the same anonymous update; when the second crashes after its append, rdb already held the entry, and the next cycle finds the change handled`,
            invoke: async () => {
                const anon = await createAnonymousGroup();
                await (await anon.group.getTable('things')).insert('t1', { label: 'start' });
                const t1 = deriveRowId('t1');
                const one = await factory();
                const two = await factory();
                try {
                    await projectAs(anon.group, one.target, anon.admin);
                    await projectAs(anon.group, two.target, anon.admin);
                    const entriesBefore = await entryCount(anon.group);
                    await one.local.update('things', (await one.read.syncId('things', t1))!, { note: 'same' });
                    await two.local.update('things', (await two.read.syncId('things', t1))!, { note: 'same' });
                    await ingestChanges(anon.group, one.target);
                    await ingestChanges(anon.group, new CrashBeforeSettle(two.target));
                    assertEquals(await entryCount(anon.group), entriesBefore + 1, 'one entry for both');
                    const next = await ingestChanges(anon.group, two.target);
                    assertEquals(`${next.accepted} ${next.rejected.length}`, '1 0', 'the next cycle settles it');
                    assertEquals((await two.target.drainChanges()).changes.length, 0, 'the outbox is empty');
                    assertEquals(await entryCount(anon.group), entriesBefore + 1, 'and appends nothing');
                    for (const h of [one, two]) {
                        await syncGroup(anon.group, h.target);
                        await assertAtCheckpoints(checkedOf(h.target), [{ group: anon.group }], 'after the projection');
                    }
                } finally {
                    await one.cleanup?.();
                    await two.cleanup?.();
                }
            },
        },
        {
            name: `[${label}-IN76] the gate graph through ingestion: the pass's observe of K brings in the revocation that voids G's observe of M, and a later write of G that reads M is refused and its row removed`,
            invoke: async () => {
                const gg = await createGateGraph();
                const members = [gg.K, gg.M, gg.G].map((group) => ({ group, config: { writer: gg.root } }));
                const { target, local, cleanup } = await factory();
                try {
                    for (const { group } of members) await projectAs(group, target, gg.root);
                    const carol = await newIdentity();
                    const carolId = await (target as unknown as KeyIndex).registerKey(DEFAULT_KEY_DOMAIN, carol.keyId, serializePublicKeyToBase64(carol.publicKey));
                    await local.insert('identities', { key_id: carolId, name: 'carol' });   // moves K
                    const noteId = await local.insert('notes', { body: 'reads m' });
                    const results = await ingestDatabaseChanges(members, target);
                    assertEquals(results.get(gg.K.getId())!.accepted, 1, "K's change lands");
                    assertEquals(`${results.get(gg.G.getId())!.accepted} ${results.get(gg.G.getId())!.rejected.length}`, '0 1', 'the note is refused');
                    assertEquals(await localRow(target, 'notes', noteId), undefined, 'and its row removed');
                    await assertAtCheckpoints(checkedOf(target), members, 'after the pass');
                } finally {
                    await cleanup?.();
                }
            },
        },
        {
            name: `[${label}-IN65] an observe brings in a removal concurrent with the grant a row of the checkpoint read: the voided insert's row is removed, the voided edit's row is put back under a pending change, and a removal that comes after the grant voids nothing`,
            invoke: async () => {
                {
                    const h = await factory();
                    try {
                        const { s, members, moveK } = await revocationIn(h, 'concurrent', 'insert');
                        const noteId = (await h.read.syncId('notes', s.noteRow))!;
                        assertTrue(await localRow(h.target, 'notes', noteId) !== undefined, "Alice's note is shown");
                        await moveK('carol');
                        await ingestDatabaseChanges(members, h.target);
                        assertEquals(await localRow(h.target, 'notes', noteId), undefined, 'insert: the voided row is removed');
                        assertEquals((await eventsOf(h.target, 'void')).join(','), s.noteRow, 'with a void event');
                        await assertAtCheckpoints(checkedOf(h.target), members, 'insert: after the pass');
                    } finally {
                        await h.cleanup?.();
                    }
                }
                {
                    const h = await factory();
                    try {
                        const { s, members, moveK } = await revocationIn(h, 'concurrent', 'update');
                        const noteId = (await h.read.syncId('notes', s.noteRow))!;
                        await moveK('carol');
                        let once = false;
                        const hooked = beforeSettle(h.target, async () => {
                            if (once) return;
                            once = true;
                            await h.local.update('notes', noteId, { tag: 'pending' });
                        });
                        await ingestDatabaseChanges(members, hooked);
                        const row = await localRow(h.target, 'notes', noteId);
                        assertEquals(`${row?.body} ${row?.tag}`, "root's pending", "update: rdb's body, with the pending tag on top");
                        await assertAtCheckpoints(checkedOf(h.target), members, 'update: after the pass');
                    } finally {
                        await h.cleanup?.();
                    }
                }
                {
                    const h = await factory();
                    try {
                        const { s, members, moveK } = await revocationIn(h, 'after', 'insert');
                        const noteId = (await h.read.syncId('notes', s.noteRow))!;
                        await moveK('carol');
                        await ingestDatabaseChanges(members, h.target);
                        assertEquals((await localRow(h.target, 'notes', noteId))?.body, "alice's", 'after: the row stays');
                        assertEquals((await eventsOf(h.target, 'void')).length, 0, 'after: no void event');
                        await assertAtCheckpoints(checkedOf(h.target), members, 'after: after the pass');
                    } finally {
                        await h.cleanup?.();
                    }
                }
            },
        },
        {
            name: `[${label}-IN66] an observe in the middle of a chain: its delta reverts only the rows it voids, keeping the writes before and after it; also with two observes, and when it voids the pass's own entry`,
            invoke: async () => {
                const h = await factory();
                try {
                    const { s, members, moveK } = await revocationIn(h, 'concurrent', 'insert', 'alice');
                    const noteId = (await h.read.syncId('notes', s.noteRow))!;
                    const own = await h.local.insert('notes', { body: 'own, at the checkpoint' });   // reads K at the grant
                    const before = await h.local.insert('logs', { entry: 'before' });
                    await moveK('carol');
                    const after = await h.local.insert('logs', { entry: 'after' });   // G observes K first
                    await moveK('dave');
                    const last = await h.local.insert('logs', { entry: 'last' });     // and again
                    const gBefore = await entryCount(s.G);
                    const results = await ingestDatabaseChanges(members, h.target);
                    assertEquals(results.get(s.G.getId())!.accepted, 4, "G's four changes land");
                    assertEquals(await entryCount(s.G), gBefore + 6, 'with two observes');
                    assertEquals(await localRow(h.target, 'notes', noteId), undefined, "the checkpoint's voided note is removed");
                    assertEquals(await localRow(h.target, 'notes', own), undefined, "and so is the pass's own");
                    for (const [id, entry] of [[before, 'before'], [after, 'after'], [last, 'last']] as const) {
                        assertEquals((await localRow(h.target, 'logs', id))?.entry, entry, `the ${entry} write stays`);
                    }
                    assertEquals((await h.target.resolveRow('notes', own))?.status, 'deleted', "the own note's mapping is marked deleted");
                    await assertAtCheckpoints(checkedOf(h.target), members, 'after the pass');
                } finally {
                    await h.cleanup?.();
                }
            },
        },
        {
            name: `[${label}-IN67] a crash after an observe that voids a row, with a later entry appended: the next cycle removes the row and stores the void event; the same for the pass's own entry`,
            invoke: async () => {
                const h = await factory();
                try {
                    const { s, members, moveK } = await revocationIn(h, 'concurrent', 'insert', 'alice');
                    const noteId = (await h.read.syncId('notes', s.noteRow))!;
                    const own = await h.local.insert('notes', { body: 'own' });
                    await moveK('carol');
                    await h.local.insert('logs', { entry: 'after the observe' });
                    await ingestDatabaseChanges(members, new CrashBeforeSettle(h.target));
                    assertTrue(await localRow(h.target, 'notes', noteId) !== undefined, 'the crash left the row');
                    await assertAtCheckpoints(checkedOf(h.target), members, 'after the crash');
                    await ingestDatabaseChanges(members, h.target);
                    assertEquals(await localRow(h.target, 'notes', noteId), undefined, 'the next cycle removes it');
                    assertEquals(await localRow(h.target, 'notes', own), undefined, "and the pass's own");
                    const voids = await eventsOf(h.target, 'void');
                    assertTrue(voids.includes(s.noteRow) && voids.length === 2, `both void events are stored, got ${voids.length}`);
                    await assertAtCheckpoints(checkedOf(h.target), members, 'after the next cycle');
                } finally {
                    await h.cleanup?.();
                }
            },
        },
        {
            name: `[${label}-IN68] an observe joins what its group already observes ahead of the bound group's checkpoint, and a member without a checkpoint is left alone`,
            invoke: async () => {
                const ci = await createCapsItems();
                const stranger = await createGroup();   // a member never projected
                const members = [ci.perm, ci.items, stranger.group].map((group) => ({ group, config: { writer: ci.admin } }));
                const { target, local, cleanup } = await factory();
                try {
                    await projectAs(ci.perm, target, ci.admin);
                    await projectAs(ci.items, target, ci.admin);
                    // items observes perm remotely, ahead of perm's checkpoint, and only items is projected again
                    await (await ci.perm.getTable('caps')).insert('c2', { label: 'other' }, ci.admin);
                    await ci.items.observe('perm', await frontierOf(ci.perm));
                    await projectGroup(ci.items, target);
                    await local.insert('caps', { label: 'grant' });
                    await local.insert('items', { name: 'thing' });
                    const results = await ingestDatabaseChanges(members, target);
                    assertEquals(`${results.get(ci.items.getId())!.accepted} ${results.get(ci.items.getId())!.rejected.length}`, '1 0',
                        'the observe extends the earlier one, so the use is accepted');
                    assertEquals(await target.getCheckpoint(stranger.group.getId()), undefined, 'the member without a checkpoint gets none');
                    await assertAtCheckpoints(checkedOf(target), members.slice(0, 2), 'after the pass');
                } finally {
                    await cleanup?.();
                }
            },
        },
        {
            name: `[${label}-IN69] an observe advances the bound group to its checkpoint plus its chain, not to its frontier`,
            invoke: async () => {
                const ci = await createCapsItems();
                const members = [ci.perm, ci.items].map((group) => ({ group, config: { writer: ci.admin } }));
                const { target, local, cleanup } = await factory();
                try {
                    await projectAs(ci.perm, target, ci.admin);
                    await projectAs(ci.items, target, ci.admin);
                    const remote = await (await ci.perm.getTable('caps')).insert('c-remote', { label: 'remote' }, ci.admin);
                    await local.insert('caps', { label: 'grant' });
                    await local.insert('items', { name: 'thing' });
                    await ingestDatabaseChanges(members, target);
                    const at = (await target.getCheckpoint(ci.items.getId()))!;
                    const observed = await ci.items.resolveObservedForeignVersion(ci.perm.getId(), at, at);
                    const permDag = await ci.perm.getCausalDag();
                    assertTrue(!await refVersionAtOrAbove(permDag, observed, new Set([remote])), 'the remote entry is not observed');
                    assertTrue(await refVersionAtOrAbove(permDag, observed, (await target.getCheckpoint(ci.perm.getId()))!), "perm's chain end is");
                    await assertAtCheckpoints(checkedOf(target), members, 'after the pass');
                } finally {
                    await cleanup?.();
                }
            },
        },
        {
            name: `[${label}-IN70] a permission written locally and used right away in another group: the use is accepted in the same pass`,
            invoke: async () => {
                const ci = await createCapsItems();
                const members = [ci.perm, ci.items].map((group) => ({ group, config: { writer: ci.admin } }));
                const { target, local, cleanup } = await factory();
                try {
                    await projectAs(ci.perm, target, ci.admin);
                    await projectAs(ci.items, target, ci.admin);
                    await local.insert('caps', { label: 'grant' });
                    await local.insert('items', { name: 'thing' });
                    const results = await ingestDatabaseChanges(members, target);
                    assertEquals(`${results.get(ci.perm.getId())!.accepted} ${results.get(ci.items.getId())!.accepted}`, '1 1', 'both land');
                    assertEquals((await rdbRows(ci.items, 'items')).length, 1, 'the use is in rdb');
                    await assertAtCheckpoints(checkedOf(target), members, 'after the pass');
                } finally {
                    await cleanup?.();
                }
            },
        },
        {
            name: `[${label}-IN79] an observe that reinstates a row void at the checkpoint: after the settle the target holds the row, written by its row hash under a new local id`,
            invoke: async () => {
                const r = await createReinstateScenario();
                const members = [r.K, r.G].map((group) => ({ group, config: { writer: r.root } }));
                const { target, read, local, cleanup } = await factory();
                try {
                    await projectAs(r.K, target, r.root);
                    await projectAs(r.G, target, r.root);
                    assertEquals(await read.syncId('notes', r.noteRow), undefined, 'the void note was never in the target');
                    const who = await newIdentity();
                    const id = await (target as unknown as KeyIndex).registerKey(DEFAULT_KEY_DOMAIN, who.keyId, serializePublicKeyToBase64(who.publicKey));
                    await local.insert('identities', { key_id: id, name: 'moves k' });   // the end then observes K
                    await ingestDatabaseChanges(members, target);
                    const localId = await read.syncId('notes', r.noteRow);
                    assertTrue(localId !== undefined, 'the reinstated note gets a local id');
                    assertEquals((await localRow(target, 'notes', localId!))?.body, "alice's", 'and its row');
                    assertEquals((await eventsOf(target, 'reinstate')).join(','), r.noteRow, 'with a reinstate event');
                    await assertAtCheckpoints(checkedOf(target), members, 'after the pass');
                } finally {
                    await cleanup?.();
                }
            },
        },
        {
            name: `[${label}-IN86] an insert captured during a pass whose settle reinstates a row never in the target: after the settle the target holds both rows, and the next pass accepts the insert`,
            invoke: async () => {
                const r = await createReinstateScenario();
                const members = [r.K, r.G].map((group) => ({ group, config: { writer: r.root } }));
                const { target, read, local, cleanup } = await factory();
                try {
                    await projectAs(r.K, target, r.root);
                    await projectAs(r.G, target, r.root);
                    const who = await newIdentity();
                    const id = await (target as unknown as KeyIndex).registerKey(DEFAULT_KEY_DOMAIN, who.keyId, serializePublicKeyToBase64(who.publicKey));
                    await local.insert('identities', { key_id: id, name: 'moves k' });
                    let pendingId: number | undefined;
                    const hooked = beforeSettle(target, async () => {
                        if (pendingId !== undefined) return;
                        pendingId = await local.insert('logs', { entry: 'captured during the pass' });
                    });
                    await ingestDatabaseChanges(members, hooked);
                    const reinstated = await read.syncId('notes', r.noteRow);
                    assertTrue(reinstated !== undefined, 'the reinstated row gets a local id');
                    assertEquals((await localRow(target, 'notes', reinstated!))?.body, "alice's", 'it is shown');
                    assertEquals((await localRow(target, 'logs', pendingId!))?.entry, 'captured during the pass', 'and so is the pending insert');
                    await assertAtCheckpoints(checkedOf(target), members, 'after the settle');
                    const next = await ingestDatabaseChanges(members, target);
                    assertEquals(`${next.get(r.G.getId())!.accepted} ${next.get(r.G.getId())!.rejected.length}`, '1 0', 'the next pass accepts the insert');
                    await assertAtCheckpoints(checkedOf(target), members, 'after the next pass');
                } finally {
                    await cleanup?.();
                }
            },
        },
        {
            name: `[${label}-IN87] an observe that voids a delete of the checkpoint: the row is back under its local id, and its mapping is active`,
            invoke: async () => {
                const h = await factory();
                try {
                    const { s, members, moveK } = await revocationIn(h, 'concurrent', 'delete');
                    const noteId = (await h.read.syncId('notes', s.noteRow))!;
                    await s.deleteNote();
                    await projectGroup(s.G, h.target);
                    assertEquals(await localRow(h.target, 'notes', noteId), undefined, "Alice's delete is projected");
                    assertEquals((await h.target.resolveRow('notes', noteId))?.status, 'deleted', 'the mapping is deleted');
                    await moveK('carol');
                    await ingestDatabaseChanges(members, h.target);
                    assertEquals((await localRow(h.target, 'notes', noteId))?.body, "root's", 'the row is back under its local id');
                    assertEquals((await h.target.resolveRow('notes', noteId))?.status, 'active', 'and its mapping is active');
                    await assertAtCheckpoints(checkedOf(h.target), members, 'after the pass');
                } finally {
                    await h.cleanup?.();
                }
            },
        },
        {
            name: `[${label}-IN89] an ingested change is a plain row op of its table, the entry a native write makes, and a void of an own ingested insert names its kind and its row`,
            invoke: async () => {
                {
                    const { group, admin } = await createGroup();
                    await (await group.getTable('ledger')).insert('l1', { ref: 'R-1', amount: '1.00' }, admin);
                    const l1 = deriveRowId('l1', admin.keyId);
                    const { target, read, local, cleanup } = await factory();
                    try {
                        await projectAs(group, target, admin);
                        const tagId = await local.insert('tags', { code: 'x' });
                        await local.update('ledger', (await read.syncId('ledger', l1))!, { memo: 'm' });
                        await local.delete('tags', tagId);
                        let entries: string[] = [];
                        const watched = beforeSettle(target, async () => {
                            entries = (await target.drainChanges()).changes.map((c) => c.record?.entry ?? '');
                        });
                        await ingestChanges(group, watched, { writer: admin });
                        const dag = await group.getScopedDag();
                        const shapes: string[] = [];
                        for (const hash of entries) {
                            const payload = (await dag.loadEntry(hash))?.payload as { action?: string; table?: string; op?: { action?: string } } | undefined;
                            shapes.push(`${payload?.action} ${payload?.table} ${payload?.op?.action}`);
                        }
                        assertEquals(shapes.join('; '), 'row tags insert; row ledger update; row tags delete', 'each a row op of its table, not a bundle');
                    } finally {
                        await cleanup?.();
                    }
                }
                {
                    const h = await factory();
                    try {
                        const { members, moveK } = await revocationIn(h, 'concurrent', 'insert', 'alice');
                        const own = await h.local.insert('notes', { body: 'own' });
                        await moveK('carol');
                        await h.local.insert('logs', { entry: 'after the observe' });
                        await ingestDatabaseChanges(members, h.target);
                        const ownRow = (await h.target.resolveRow('notes', own))?.rowId;
                        const event = (await h.target.drainOpEvents()).find((e) => e.event.direction === 'void' && e.event.rowId === ownRow);
                        assertEquals(`${event?.event.kind} ${event?.event.table} ${event?.event.localId}`, `insert notes ${own}`,
                            "the own insert's void event names its kind, its table and its row, by local id too");
                    } finally {
                        await h.cleanup?.();
                    }
                }
            },
        },
    ];
}
