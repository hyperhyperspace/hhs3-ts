// Backend-agnostic conformance suite for a MaterializedChangeSource (the
// inbound / change-ingestion side). Parameterized by an IngestionFactory that
// supplies a bidirectional backend (materialize + capture), a reader, and a
// LocalMutator that simulates the APP writing directly to the projected tables.
//
// Each test: project an rdb group into the backend, mutate the projected DB
// locally, run ingestChanges, and assert the change flowed back into rdb (and
// that a following projection converges). Engine-specific capture mechanics
// (triggers vs an in-memory outbox) are irrelevant here - only the contract.

import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { json } from "@hyper-hyper-space/hhs3_json";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import { deriveRowId, MigrationRule, Row, RTableGroup, TableDef } from "@hyper-hyper-space/hhs3_rdb";
import {
    AdapterConfig, BidirectionalTarget, CapturedBatch, CapturedChange, CapturedChangeId, ChangeRecord, CheckpointMovedError, DEFAULT_KEY_DOMAIN, IngestSettle,
    ingestChanges, KeyIndex, MemoryTarget, OpEvent, OpEventQuery, PendingChangesError, projectGroup, QueryableTarget, RowAction,
    rowToUpsertAction,
    SchemaAction, StoredOpEvent, SyncMapping, syncGroup, WriterRecordStore,
} from "@hyper-hyper-space/hhs3_rdb_adapter";
import { serializePublicKeyToBase64 } from "@hyper-hyper-space/hhs3_mvt";
import { leaveEndsToNewCycles } from "@hyper-hyper-space/hhs3_rdb_adapter/testing";

import {
    createAnonymousGroup, createFkGroup, createFlipGroup, createGroup, createNullsGroup, createTypedGroup, GroupFixture, makeIdentity,
    sameVersion,
} from "./group_fixture.js";
import { beforeApply, beforeSettle } from "./target_hooks.js";
import { ProjectionReader, RowValues } from "./projection_reader.js";
import { assertAtCheckpoints, CheckedTarget } from "./consistency.js";
import { createChainCases } from "./ingestion_chain_suite.js";
import {
    CrashBeforeSettle, entryCount, failureEvent, localRow, projectAs, rdbRowAction, rdbRows, recordWriter, rowOps, sorted,
} from "./ingestion_helpers.js";

export { projectAs } from "./ingestion_helpers.js";



// What the app writes to a projected row: a logical value per column, or null
// to set the column to NULL. (A row read back has no nulls; see ReadRow.)
export type LocalValues = { [column: string]: json.Literal | null };

// The app-side write surface a backend must expose for the suite: direct
// mutations of the projected tables that the capture machinery observes. Values
// are LOGICAL; the backend converts to its stored form. `insert` returns the
// projection-local id the row was given.
export interface LocalMutator {
    insert(table: string, values: LocalValues, author?: number): number | Promise<number>;
    // An insert under an explicit local id where no row is: SQLite's INSERT
    // with an id, IndexedDB's put of a new record. Absent on memory, which
    // allocates every id.
    insertAt?(table: string, localId: number, values: LocalValues): void | Promise<void>;
    // An insert under an explicit local id that replaces the row there:
    // SQLite's INSERT OR REPLACE. IndexedDB's put over a record is an update.
    replaceAt?(table: string, localId: number, values: LocalValues): void | Promise<void>;
    update(table: string, localId: number, values: LocalValues): void | Promise<void>;
    delete(table: string, localId: number): void | Promise<void>;
    setCaptureEnabled(on: boolean): void | Promise<void>;
}

export type IngestionHarness = {
    target: BidirectionalTarget;
    read: ProjectionReader;
    local: LocalMutator;
    cleanup?: () => void | Promise<void>;
    // Closes this harness and opens the same store again, as a restart after a
    // crash finds it: what was committed, and nothing a process held in
    // memory. Absent on memory, whose store lives only in the process.
    reopen?: () => Promise<IngestionHarness>;
};

export type IngestionFactory = () => IngestionHarness | Promise<IngestionHarness>;

// The reference in-memory ingestion harness: a capture-provisioned MemoryTarget.
export function memoryIngestionHarness(): IngestionHarness {
    const target = new MemoryTarget({ captureChanges: true });
    const read: ProjectionReader = {
        hasTable: (table) => target.hasTable(table),
        listTables: () => target.listTables(),
        getRowIds: (table) => target.getRowIds(table),
        getRow: (table, rowId) => target.getRowByRowId(table, rowId),
        syncId: (table, rowId) => target.syncId(table, rowId),
        columnType: (table, column) => target.columnTypes(table)?.[column],
    };
    const local: LocalMutator = {
        insert: (table, values, author) => target.localInsert(table, values, author),
        update: (table, localId, values) => { target.localUpdate(table, localId, values); },
        delete: (table, localId) => { target.localDelete(table, localId); },
        setCaptureEnabled: (on) => target.setCaptureEnabled(on),
    };
    return { target, read, local };
}

type NamedTest = { name: string; invoke: () => Promise<void> };

// The ingestion suite again, with every end of a pass left to a new cycle, as
// a crash right after a pass's last record would leave it (HIP-29). The end
// reads only what is stored, so every case holds unchanged.
export function createIngestionSuiteWithEndsInNewCycles(label: string, factory: IngestionFactory): { title: string; tests: NamedTest[] } {
    const suite = createIngestionSuite(`${label}-ENDS`, factory);
    return {
        title: `${suite.title}, every end of a pass in a new cycle`,
        tests: suite.tests.map((t) => ({
            name: t.name,
            invoke: async () => {
                leaveEndsToNewCycles(true);
                try { await t.invoke(); } finally { leaveEndsToNewCycles(false); }
            },
        })),
    };
}

export function createIngestionSuite(label: string, factory: IngestionFactory): { title: string; tests: NamedTest[] } {
    return {
        title: `[${label}] rdb_adapter change-ingestion conformance`,
        tests: [
            {
                name: `[${label}-IN01] round-trip insert: a locally inserted row becomes an rdb op`,
                invoke: async () => {
                    const { group, admin } = await createGroup();
                    await (await group.getTable('ledger')).insert('l1', { ref: 'R-1', amount: '10.00' }, admin);

                    const { target, read, local, cleanup } = await factory();
                    try {
                        await projectAs(group, target, admin);
                        const before = (await rdbRows(group, 'tags')).length;

                        const localId = await local.insert('tags', { code: 'urgent' });
                        const result = await ingestChanges(group, target, { writer: admin });

                        assertEquals(result.accepted, 1, 'one op ingested');
                        assertEquals(result.rejected.length, 0, 'nothing rejected');

                        const rows = await rdbRows(group, 'tags');
                        assertEquals(rows.length, before + 1, 'a tag row now exists in rdb');
                        const ingested = rows.find((r) => r.values.code === 'urgent');
                        assertTrue(ingested !== undefined, 'ingested value present in rdb');
                        assertEquals(ingested!.author, admin.keyId, 'op authored by the configured writer');

                        // Convergence: reprojection reuses the local id committed by ingest.
                        await projectAs(group, target, admin);
                        assertTrue(await read.getRow('tags', ingested!.rowId) !== undefined,
                            'ingested row materialized on reprojection');
                        assertEquals(await read.syncId('tags', ingested!.rowId), localId,
                            'reprojection reuses the local id from commitIngest');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN02] round-trip update: a local column edit merges into the rdb row`,
                invoke: async () => {
                    const { group, admin } = await createGroup();
                    await (await group.getTable('ledger')).insert('l1', { ref: 'R-1', amount: '10.00' }, admin);

                    const { target, read, local, cleanup } = await factory();
                    try {
                        await projectAs(group, target, admin);
                        const l1 = deriveRowId('l1', admin.keyId);
                        const localId = await read.syncId('ledger', l1);
                        assertTrue(localId !== undefined, 'l1 has a local id after projection');

                        await local.update('ledger', localId!, { memo: 'ping' });
                        const result = await ingestChanges(group, target, { writer: admin });

                        assertEquals(result.accepted, 1, 'one update ingested');
                        assertEquals(result.rejected.length, 0, 'nothing rejected');
                        const row = (await rdbRows(group, 'ledger')).find((r) => r.rowId === l1);
                        assertEquals(row!.values.memo, 'ping', 'update reflected in rdb');
                        assertEquals(row!.values.ref, 'R-1', 'unrelated column untouched');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN04] insert-then-delete of the same local row submits both faithfully`,
                invoke: async () => {
                    const { group, admin } = await createGroup();
                    await (await group.getTable('ledger')).insert('l1', { ref: 'R-1', amount: '10.00' }, admin);

                    const { target, local, cleanup } = await factory();
                    try {
                        await projectAs(group, target, admin);
                        const before = (await rdbRows(group, 'tags')).length;

                        const localId = await local.insert('tags', { code: 'temp' });
                        await local.delete('tags', localId);
                        const result = await ingestChanges(group, target, { writer: admin });

                        // Faithful: the insert and the delete are BOTH submitted (no
                        // lossy cancel); the row ends non-live, so no live tag remains.
                        assertEquals(result.accepted, 2, 'insert + delete both submitted');
                        assertEquals(result.rejected.length, 0, 'and nothing is rejected');
                        assertEquals((await rdbRows(group, 'tags')).length, before, 'the row is deleted, so no live tag');
                        assertEquals((await target.drainChanges()).changes.length, 0, 'all captured changes acked');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN05] runtime enable toggle: disabled capture records nothing`,
                invoke: async () => {
                    const { group, admin } = await createGroup();
                    await (await group.getTable('ledger')).insert('l1', { ref: 'R-1', amount: '10.00' }, admin);

                    const { target, local, cleanup } = await factory();
                    try {
                        await projectAs(group, target, admin);

                        await local.setCaptureEnabled(false);
                        await local.insert('tags', { code: 'ghost' });
                        let result = await ingestChanges(group, target, { writer: admin });
                        assertEquals(result.accepted, 0, 'disabled capture yields no changes to ingest');
                        const afterDisabled = (await rdbRows(group, 'tags')).length;

                        await local.setCaptureEnabled(true);
                        await local.insert('tags', { code: 'real' });
                        result = await ingestChanges(group, target, { writer: admin });
                        assertEquals(result.accepted, 1, 're-enabled capture ingests');
                        assertEquals((await rdbRows(group, 'tags')).length, afterDisabled + 1,
                            'only the enabled-window insert reached rdb');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN06] readonly-column edits are rejected, not applied`,
                invoke: async () => {
                    const { group, admin } = await createGroup();
                    await (await group.getTable('ledger')).insert('l1', { ref: 'R-1', amount: '10.00' }, admin);

                    const { target, read, local, cleanup } = await factory();
                    try {
                        await projectAs(group, target, admin);
                        const l1 = deriveRowId('l1', admin.keyId);
                        const localId = (await read.syncId('ledger', l1))!;

                        await local.update('ledger', localId, { ref: 'HACKED' });
                        const result = await ingestChanges(group, target, { writer: admin });

                        assertEquals(result.accepted, 0, 'readonly edit not applied');
                        assertEquals(result.rejected.length, 1, 'exactly one rejection');
                        assertTrue(result.rejected[0].reason.includes('readonly'), 'reason names the readonly column');
                        const row = (await rdbRows(group, 'ledger')).find((r) => r.rowId === l1);
                        assertEquals(row!.values.ref, 'R-1', 'rdb readonly value unchanged');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN07] self-FK round-trip: a locally inserted parent+child resolve the FK by minted rowId`,
                invoke: async () => {
                    const { group, admin } = await createFkGroup();

                    const { target, local, cleanup } = await factory();
                    try {
                        await projectAs(group, target, admin);

                        // Both inserted locally in one batch; child references the
                        // parent by its projection-local id.
                        const rootId = await local.insert('comments', { body: 'root' });
                        await local.insert('comments', { body: 'child', parent_id: rootId });
                        const result = await ingestChanges(group, target, { writer: admin });

                        assertEquals(result.accepted, 2, 'both rows ingested');
                        assertEquals(result.rejected.length, 0, 'nothing rejected');

                        const rows = await rdbRows(group, 'comments');
                        const root = rows.find((r) => r.values.body === 'root');
                        const child = rows.find((r) => r.values.body === 'child');
                        assertTrue(root !== undefined && child !== undefined, 'both comments in rdb');
                        assertEquals(child!.values.parent, root!.rowId, 'child FK rewritten to the parent rowId');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN08] cross-table FK: the referenced parent is ordered before the child`,
                invoke: async () => {
                    const { group, admin } = await createFkGroup();

                    const { target, local, cleanup } = await factory();
                    try {
                        await projectAs(group, target, admin);

                        const postId = await local.insert('posts', { title: 'T' });
                        await local.insert('comments', { body: 'c', post_id: postId });
                        const result = await ingestChanges(group, target, { writer: admin });

                        assertEquals(result.accepted, 2, 'post + comment ingested');
                        assertEquals(result.rejected.length, 0, 'nothing rejected (parent ordered first)');

                        const post = (await rdbRows(group, 'posts')).find((r) => r.values.title === 'T');
                        const comment = (await rdbRows(group, 'comments')).find((r) => r.values.body === 'c');
                        assertTrue(post !== undefined && comment !== undefined, 'both rows in rdb');
                        assertEquals(comment!.values.post, post!.rowId, 'comment FK rewritten to the post rowId');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN09] a dangling local FK is rejected and nothing is written`,
                invoke: async () => {
                    const { group, admin } = await createFkGroup();

                    const { target, local, cleanup } = await factory();
                    try {
                        await projectAs(group, target, admin);

                        // post_id points at a local id that was never allocated.
                        await local.insert('comments', { body: 'orphan', post_id: 9999 });
                        const result = await ingestChanges(group, target, { writer: admin });

                        assertEquals(result.accepted, 0, 'the orphan is not written');
                        assertEquals(result.rejected.length, 1, 'exactly one rejection');
                        assertTrue(result.rejected[0].reason.includes('dangling'), 'reason identifies the dangling FK');
                        assertEquals((await rdbRows(group, 'comments')).length, 0, 'no comment reached rdb');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN10] ingestion failure (dangling FK insert): orphan reverted, sync status + op-event recorded`,
                invoke: async () => {
                    const { group, admin } = await createFkGroup();

                    const { target, read, local, cleanup } = await factory();
                    try {
                        await projectAs(group, target, admin);

                        const localId = await local.insert('comments', { body: 'orphan', post_id: 9999 });
                        const result = await ingestChanges(group, target, { writer: admin });

                        assertEquals(result.accepted, 0, 'the failed insert lands no rdb op');
                        assertEquals(result.rejected.length, 1, 'one rejection');

                        // The reserved sync row survives (id never reused) but is
                        // marked ingestion_failure, and its app row was reverted away.
                        const mapping = await target.resolveRow('comments', localId);
                        assertTrue(mapping !== undefined, 'the sync record is kept, not deleted');
                        assertEquals(mapping!.status, 'ingestion_failure', 'status flips to ingestion_failure');
                        assertTrue(await read.getRow('comments', mapping!.rowId) === undefined, 'the orphan app row is reverted away');

                        // Exactly one durable op-event: an ingestion failure carrying
                        // the full (never-appended) op JSON and a structured reason.
                        const events = await target.drainOpEvents();
                        assertEquals(events.length, 1, 'one op-event logged');
                        const e = events[0].event;
                        assertEquals(e.origin, 'ingestion', 'origin ingestion');
                        assertEquals(e.direction, 'failure', 'direction failure');
                        assertEquals(e.table, 'comments', 'names the affected table');
                        assertTrue(e.op !== undefined, 'the full op JSON is stored (values recoverable)');
                        assertTrue(e.reason !== undefined, 'a structured reason is stored');

                        assertEquals((await target.drainChanges()).changes.length, 0, 'the outbox is acked');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN11] ingestion failure (readonly update): row restored from rdb, op-event recorded, status stays active`,
                invoke: async () => {
                    const { group, admin } = await createGroup();
                    await (await group.getTable('ledger')).insert('l1', { ref: 'R-1', amount: '10.00' }, admin);

                    const { target, read, local, cleanup } = await factory();
                    try {
                        await projectAs(group, target, admin);
                        const l1 = deriveRowId('l1', admin.keyId);
                        const localId = (await read.syncId('ledger', l1))!;

                        await local.update('ledger', localId, { ref: 'HACKED' });
                        const result = await ingestChanges(group, target, { writer: admin });

                        assertEquals(result.accepted, 0, 'the readonly edit lands no rdb op');
                        assertEquals(result.rejected.length, 1, 'one rejection');

                        // rdb truth is untouched, and the local row is re-materialized
                        // back to it (the botched local edit is undone).
                        const row = (await rdbRows(group, 'ledger')).find((r) => r.rowId === l1);
                        assertEquals(row!.values.ref, 'R-1', 'rdb readonly value unchanged');
                        assertEquals((await read.getRow('ledger', l1))!.values.ref, 'R-1', 'local row restored from rdb truth');

                        // An update failure is NOT an orphan: the sync row stays active.
                        const mapping = await target.resolveRow('ledger', localId);
                        assertEquals(mapping!.status, 'active', 'status stays active for a restored update');

                        const events = await target.drainOpEvents();
                        assertEquals(events.length, 1, 'one op-event logged');
                        assertEquals(events[0].event.direction, 'failure', 'an ingestion failure');
                        assertTrue(events[0].event.reason !== undefined, 'reason recorded (readonly)');

                        assertEquals((await target.drainChanges()).changes.length, 0, 'the outbox is acked');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN13] a crash after the append and before the settle: the next cycle settles what was appended, with the checkpoint at its entry, and writes no entry twice`,
                invoke: async () => {
                    const { group, admin } = await createGroup();
                    await (await group.getTable('ledger')).insert('l1', { ref: 'R-1', amount: '10.00' }, admin);

                    const { target, local, cleanup } = await factory();
                    try {
                        await projectAs(group, target, admin);
                        const before = (await rdbRows(group, 'tags')).length;
                        const entriesBefore = await entryCount(group);
                        const localId = await local.insert('tags', { code: 'once' });

                        // The settle is dropped after the append lands (a simulated crash).
                        const first = await ingestChanges(group, new CrashBeforeSettle(target), { writer: admin });
                        assertEquals(first.accepted, 1, 'the entry was appended before the crash');
                        const [pending] = (await target.drainChanges()).changes;
                        assertTrue(pending?.record?.entry !== undefined, 'the outbox row holds the entry\'s hash');
                        assertEquals(await target.resolveRow('tags', localId), undefined, 'no identity is saved before the settle');

                        // The next cycle's end settles it, and reports it.
                        const second = await ingestChanges(group, target, { writer: admin });
                        assertEquals(second.accepted, 1, 'the crashed pass\'s change is reported by the cycle that settles it');
                        assertEquals(second.rejected.length, 0, 'and it is not a failure');
                        assertEquals((await rdbRows(group, 'tags')).length, before + 1, 'one row');
                        assertEquals(await entryCount(group), entriesBefore + 1, 'one entry');
                        assertTrue(sameVersion(await target.getCheckpoint(group.getId()), new Set([pending.record!.entry!])),
                            "the checkpoint is the crashed pass's last entry");
                        const mapping = await target.resolveRow('tags', localId);
                        assertEquals(mapping?.rowId, (await rdbRows(group, 'tags')).find((r) => r.values.code === 'once')?.rowId,
                            'the mapping is read from the entry');
                        assertEquals(mapping?.status, 'active', 'active');
                        assertEquals((await target.drainChanges()).changes.length, 0, 'the outbox is empty');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN14] fail -> fix -> re-insert: one rdb row at a NEW local id, the rejected identity is never used`,
                invoke: async () => {
                    const { group, admin } = await createFkGroup();

                    const { target, local, cleanup } = await factory();
                    try {
                        await projectAs(group, target, admin);

                        // 1. A dangling-FK insert fails and its local id is poisoned.
                        const failedId = await local.insert('comments', { body: 'orphan', post_id: 9999 });
                        const r1 = await ingestChanges(group, target, { writer: admin });
                        assertEquals(r1.accepted, 0, 'the dangling insert lands nothing');
                        const poisoned = await target.resolveRow('comments', failedId);
                        assertEquals(poisoned!.status, 'ingestion_failure', 'the failed id is poisoned');
                        const poisonRowId = poisoned!.rowId;

                        // 2. Make the write legal: insert a real post and ingest it.
                        const postId = await local.insert('posts', { title: 'T' });
                        const r2 = await ingestChanges(group, target, { writer: admin });
                        assertEquals(r2.accepted, 1, 'the post ingests');

                        // 3. Re-insert the comment against the real post: a NEW local id.
                        const newId = await local.insert('comments', { body: 'real', post_id: postId });
                        assertTrue(newId !== failedId, 'the re-insert gets a new local id (ids are never reused)');
                        const r3 = await ingestChanges(group, target, { writer: admin });
                        assertEquals(r3.accepted, 1, 'the comment ingests');

                        // The guarantee (the user-reported bug): EXACTLY ONE comment in
                        // rdb, and it is NOT the rejected identity.
                        const comments = await rdbRows(group, 'comments');
                        assertEquals(comments.length, 1, 'exactly one comment row in rdb (no duplicate)');
                        assertTrue(comments[0].rowId !== poisonRowId, 'the row is not the rejected identity');
                        assertEquals(comments[0].values.body, 'real', 'it is the legal re-insert');

                        assertEquals((await target.resolveRow('comments', failedId))!.status, 'ingestion_failure',
                            'the poisoned mapping is kept, never reused');
                        assertEquals((await target.resolveRow('comments', newId))!.status, 'active', 'the new id is active');
                        assertEquals((await target.drainChanges()).changes.length, 0, 'the outbox is acked');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN15] a crash after a prior failure: the next cycle settles the appended insert, and the refused local id keeps its mapping`,
                invoke: async () => {
                    const { group, admin } = await createFkGroup();

                    const { target, local, cleanup } = await factory();
                    try {
                        await projectAs(group, target, admin);

                        // A refused insert, then a legal one at a new id.
                        const failedId = await local.insert('comments', { body: 'orphan', post_id: 9999 });
                        await ingestChanges(group, target, { writer: admin });
                        const postId = await local.insert('posts', { title: 'T' });
                        await ingestChanges(group, target, { writer: admin });
                        await projectAs(group, target, admin);
                        const newId = await local.insert('comments', { body: 'real', post_id: postId });
                        const entriesBefore = await entryCount(group);

                        const a = await ingestChanges(group, new CrashBeforeSettle(target), { writer: admin });
                        assertEquals(a.accepted, 1, 'the comment appended before the crash');
                        const b = await ingestChanges(group, target, { writer: admin });
                        assertEquals(b.accepted, 1, 'the next cycle settles it');
                        assertEquals(b.rejected.length, 0, 'and it is not a failure');

                        const comments = await rdbRows(group, 'comments');
                        assertEquals(comments.length, 1, 'exactly one comment');
                        assertEquals(await entryCount(group), entriesBefore + 1, 'one entry');
                        assertEquals((await target.resolveRow('comments', newId))?.rowId, comments[0].rowId, 'its mapping');
                        assertEquals((await target.resolveRow('comments', failedId))?.status, 'ingestion_failure', 'the refused id keeps its mapping');
                        assertEquals((await target.drainChanges()).changes.length, 0, 'the outbox is empty');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN17] a refused insert's row is removed and its mapping marked ingestion_failure even when a later change of it is pending, which is refused at its turn`,
                invoke: async () => {
                    const { group, admin } = await createFkGroup();

                    const { target, local, cleanup } = await factory();
                    try {
                        await projectAs(group, target, admin);
                        const failedId = await local.insert('comments', { body: 'orphan', post_id: 9999 });

                        // A later change of the same row, captured while the pass runs.
                        const hooked = beforeSettle(target, async () => { await local.update('comments', failedId, { body: 'later' }); });
                        const result = await ingestChanges(group, hooked, { writer: admin });
                        assertEquals(result.accepted, 0, 'the dangling insert lands nothing');
                        const mapping = await target.resolveRow('comments', failedId);
                        assertEquals(mapping?.status, 'ingestion_failure', 'the refused insert keeps its identity');
                        assertEquals(await localRow(target, 'comments', failedId), undefined, 'its row is removed, the pending update finding none');
                        assertEquals((await target.drainChanges()).changes.length, 1, 'the later change is still pending');

                        const next = await ingestChanges(group, target, { writer: admin });
                        assertTrue(next.rejected.length === 1 && next.rejected[0].reason.includes('refused'), 'it is refused at its turn');
                        assertEquals((await target.drainChanges()).changes.length, 0, 'and leaves the outbox');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN18] apply() compare-and-set: a stale expectFrom is rejected and applies nothing`,
                invoke: async () => {
                    const { group } = await createGroup();

                    const { target, cleanup } = await factory();
                    try {
                        await projectGroup(group, target);   // establishes a checkpoint
                        const groupId = group.getId();
                        const current = await target.getCheckpoint(groupId);
                        assertTrue(current !== undefined && current.size > 0, 'a non-empty checkpoint exists after projection');

                        // A stale expectFrom (empty version) is rejected without mutating.
                        let stale = false;
                        try {
                            await target.apply(groupId, [], [], current!, undefined, new Set<string>());
                        } catch (e) { stale = e instanceof CheckpointMovedError; }
                        assertTrue(stale, 'a stale expectFrom throws CheckpointMovedError');

                        // null (expect NO checkpoint) against an existing one is rejected too.
                        let nullExpect = false;
                        try {
                            await target.apply(groupId, [], [], current!, undefined, null);
                        } catch (e) { nullExpect = e instanceof CheckpointMovedError; }
                        assertTrue(nullExpect, 'null expectFrom throws when a checkpoint already exists');

                        const afterCp = await target.getCheckpoint(groupId);
                        assertTrue(afterCp !== undefined && afterCp.size === current!.size, 'the checkpoint is unchanged');

                        // A correct expectFrom still succeeds. An empty batch already
                        // at this checkpoint is a no-op (no checkpoint rewrite).
                        await target.apply(groupId, [], [], current!, undefined, current!);
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN20] a column is dropped while capture is on: the drop projects, and later local writes are captured`,
                invoke: async () => {
                    // Each case: project, deploy a schema change that drops a column of a
                    // table the target captures writes for, project again, then insert
                    // locally and ingest.
                    const cases: {
                        what: string; fixture: () => Promise<GroupFixture>; rules: MigrationRule[];
                        table: string; values: LocalValues; gone: string[]; present: string[];
                    }[] = [
                        {
                            what: 'the drop of a plain column', fixture: createGroup,
                            rules: [{ rule: 'drop-column', table: 'ledger', column: 'memo' }],
                            table: 'ledger', values: { ref: 'R-9', amount: '1.00' }, gone: ['memo'], present: ['ref', 'amount'],
                        },
                        {
                            what: 'a plain column that becomes a foreign key', fixture: createFlipGroup,
                            rules: [{ rule: 'set-fks', table: 'comments', fks: { post: 'posts' } }],
                            table: 'comments', values: { body: 'hello' }, gone: ['post'], present: ['body', 'post_id'],
                        },
                        {
                            what: 'a column dropped and added again in one deploy', fixture: createGroup,
                            rules: [
                                { rule: 'drop-column', table: 'ledger', column: 'memo' },
                                { rule: 'add-column', table: 'ledger', column: 'memo', def: { type: 'string', nullable: true, default: 'none' } },
                            ],
                            table: 'ledger', values: { ref: 'R-9', amount: '1.00' }, gone: [], present: ['ref', 'amount', 'memo'],
                        },
                    ];
                    for (const c of cases) {
                        const { group, schema, admin } = await c.fixture();
                        const { target, read, local, cleanup } = await factory();
                        try {
                            await projectAs(group, target, admin);
                            await schema.updateSchema(c.rules, admin, c.what);
                            await group.deploy(await (await schema.getScopedDag()).getFrontier());
                            await projectGroup(group, target);

                            for (const column of c.gone) {
                                assertTrue(await read.columnType(c.table, column) === undefined, `${c.what}: '${column}' is gone from the target`);
                            }
                            for (const column of c.present) {
                                assertTrue(await read.columnType(c.table, column) !== undefined, `${c.what}: '${column}' is in the target`);
                            }

                            const before = (await rdbRows(group, c.table)).length;
                            await local.insert(c.table, c.values);
                            const result = await ingestChanges(group, target, { writer: admin });
                            assertEquals(result.accepted, 1, `${c.what}: a local insert made after it is captured and ingested`);
                            assertEquals(result.rejected.length, 0, `${c.what}: nothing is rejected`);
                            assertEquals((await rdbRows(group, c.table)).length, before + 1, `${c.what}: the row is in rdb`);
                            await projectGroup(group, target);
                        } finally {
                            await cleanup?.();
                        }
                    }
                },
            },
            {
                name: `[${label}-IN21] a local update to NULL becomes a null write, and every device then reads NULL`,
                invoke: async () => {
                    const { group, admin } = await createNullsGroup();
                    const t1 = deriveRowId('t1');
                    await (await group.getTable('topics')).insert('t1', { name: 'topic' });
                    await (await group.getTable('notes')).insert('n1', { title: 'a', memo: 'm', tag: 'x', done: true, topic: t1 }, admin);
                    const n1 = deriveRowId('n1', admin.keyId);

                    const a = await factory();
                    const b = await factory();
                    try {
                        await projectAs(group, a.target, admin);
                        const localId = (await a.read.syncId('notes', n1))!;

                        // a plain column, one with a default, a boolean and a foreign key
                        await a.local.update('notes', localId, { memo: null, tag: null, done: null, topic_id: null });
                        const result = await ingestChanges(group, a.target, { writer: admin });
                        assertEquals(result.rejected.length, 0, 'nothing is rejected: a NULL foreign key needs no lookup');
                        assertTrue(result.accepted >= 1, 'the update is ingested');

                        const row = (await rdbRows(group, 'notes')).find((r) => r.rowId === n1);
                        assertEquals(sorted(row?.values), sorted({ title: 'a' }), 'rdb reads NULL in each column, also the one with a default');
                        const nulled = (await rowOps(group, 'notes')).filter((op) => op.action === 'update').flatMap((op) => op.nulls ?? []).sort();
                        assertEquals(nulled.join(','), 'done,memo,tag,topic', 'the update ops carry the nulls');
                        assertEquals((await a.target.drainChanges()).changes.length, 0, 'the outbox is acked');

                        // the writing device after the projection of its own write, and a second device
                        await projectAs(group, a.target, admin);
                        await projectAs(group, b.target, admin);
                        assertEquals(sorted((await a.read.getRow('notes', n1))?.values), sorted({ title: 'a' }), 'the writing device still reads NULL');
                        assertEquals(sorted((await b.read.getRow('notes', n1))?.values), sorted({ title: 'a' }), 'a second device reads NULL');
                    } finally {
                        await a.cleanup?.();
                        await b.cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN22] a local update to NULL loses to a concurrent write of a value: every device ends with the value`,
                invoke: async () => {
                    const { group, admin } = await createNullsGroup();
                    const notes = await group.getTable('notes');
                    await notes.insert('n1', { title: 'a', memo: 'old', tag: 'old' }, admin);
                    const n1 = deriveRowId('n1', admin.keyId);

                    const a = await factory();
                    const b = await factory();
                    try {
                        await projectAs(group, a.target, admin);
                        const base = await (await group.getScopedDag()).getFrontier();

                        // device A clears both columns; another replica, not having seen that, sets them
                        await a.local.update('notes', (await a.read.syncId('notes', n1))!, { memo: null, tag: null });
                        const result = await ingestChanges(group, a.target, { writer: admin });
                        assertEquals(result.rejected.length, 0, 'the clear is ingested');
                        await notes.update(n1, { memo: 'new', tag: 'new' }, admin, base);

                        await projectAs(group, a.target, admin);
                        await projectAs(group, b.target, admin);
                        for (const [device, what] of [[a, 'the device that cleared'], [b, 'a second device']] as const) {
                            assertEquals(sorted((await device.read.getRow('notes', n1))?.values), sorted({ title: 'a', memo: 'new', tag: 'new' }),
                                `${what} ends with the concurrent value`);
                        }
                    } finally {
                        await a.cleanup?.();
                        await b.cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN23] a local insert: an explicit NULL is kept where the column has a default, and written nowhere else`,
                invoke: async () => {
                    const { group, admin } = await createNullsGroup();
                    const a = await factory();
                    const b = await factory();
                    try {
                        await projectAs(group, a.target, admin);
                        await a.local.insert('notes', { title: 'nulled', memo: null, tag: null, topic_id: null });
                        await a.local.insert('notes', { title: 'left-out' });
                        const result = await ingestChanges(group, a.target, { writer: admin });
                        assertEquals(result.accepted, 2, 'both inserts are ingested');
                        assertEquals(result.rejected.length, 0, 'nothing is rejected');

                        const rows = await rdbRows(group, 'notes');
                        const nulled = rows.find((r) => r.values.title === 'nulled')!;
                        const leftOut = rows.find((r) => r.values.title === 'left-out')!;
                        assertEquals(sorted(nulled.values), sorted({ title: 'nulled' }), 'the explicit NULL wins over the default of `tag`');
                        assertEquals(sorted(leftOut.values), sorted({ title: 'left-out', tag: 'none' }), 'a column left out gets its default');

                        // a null is written only for a column with a default: elsewhere it
                        // reads the same as leaving the column out
                        const inserts = (await rowOps(group, 'notes')).filter((op) => op.action === 'insert');
                        const insertOf = (title: string) => inserts.find((op) => op.values['title'] === title)!;
                        assertEquals((insertOf('nulled').nulls ?? []).join(','), 'tag', 'only the null of the column with a default is written');
                        assertTrue(insertOf('left-out').nulls === undefined, 'an insert with no NULL of that kind writes no null');

                        await projectAs(group, a.target, admin);
                        await projectAs(group, b.target, admin);
                        assertEquals(sorted((await a.read.getRow('notes', nulled.rowId))?.values), sorted({ title: 'nulled' }), 'the writing device reads NULL');
                        assertEquals(sorted((await b.read.getRow('notes', nulled.rowId))?.values), sorted({ title: 'nulled' }), 'a second device reads NULL');
                        assertEquals(sorted((await b.read.getRow('notes', leftOut.rowId))?.values), sorted({ title: 'left-out', tag: 'none' }),
                            'a second device reads the default of the column left out');
                    } finally {
                        await a.cleanup?.();
                        await b.cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN24] a refused local write is undone also where rdb holds NULL, and ingestion goes on`,
                invoke: async () => {
                    const { group, admin } = await createNullsGroup();
                    await (await group.getTable('notes')).insert('n1', { title: 'a' }, admin);
                    const n1 = deriveRowId('n1', admin.keyId);

                    const { target, read, local, cleanup } = await factory();
                    try {
                        await projectAs(group, target, admin);
                        const localId = (await read.syncId('notes', n1))!;

                        // `topic` is NULL in rdb; the app points it at a row that does not exist
                        await local.update('notes', localId, { topic_id: 987654 });
                        const dangling = await ingestChanges(group, target, { writer: admin });
                        assertEquals(dangling.accepted, 0, 'the dangling reference lands no rdb op');
                        assertEquals(dangling.rejected.length, 1, 'one rejection');
                        assertEquals(sorted((await read.getRow('notes', n1))?.values), sorted({ title: 'a', tag: 'none' }),
                            'the local row is put back to what rdb holds: the foreign key is NULL again');
                        assertEquals((await target.drainChanges()).changes.length, 0, 'the outbox is acked');

                        // a NULL for a NOT NULL column: where the target lets the app write it,
                        // rdb refuses it, the row is put back, and the refusal is recorded
                        let wrote = true;
                        try {
                            await local.update('notes', localId, { title: null });
                        } catch {
                            wrote = false;   // the target itself refuses the write (a NOT NULL column in SQL)
                        }
                        if (wrote) {
                            const refused = await ingestChanges(group, target, { writer: admin });
                            assertEquals(refused.accepted, 0, 'a NULL for a NOT NULL column lands no rdb op');
                            assertEquals(refused.rejected.length, 1, 'one rejection');
                            assertEquals((await read.getRow('notes', n1))?.values.title, 'a', 'the local row has its title back');
                            const events = await target.drainOpEvents();
                            assertTrue(events.some((e) => e.event.direction === 'failure'), 'the refusal is recorded as an op-event');
                            assertEquals((await target.drainChanges()).changes.length, 0, 'the outbox is acked');
                        }

                        // the next pass runs
                        await local.update('notes', localId, { memo: 'ok' });
                        const next = await ingestChanges(group, target, { writer: admin });
                        assertEquals(next.accepted, 1, 'a later local write is ingested');
                        assertEquals((await rdbRows(group, 'notes')).find((r) => r.rowId === n1)?.values.memo, 'ok', 'and it is in rdb');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN25] a json value with a null inside it is refused before rdb sees it, logged, and the pass goes on`,
                invoke: async () => {
                    const { group, admin } = await createTypedGroup();
                    const { target, read, local, cleanup } = await factory();
                    try {
                        await projectAs(group, target, admin);
                        await local.insert('things', { label: 'bad', data: { a: null } as unknown as json.Literal });
                        await local.insert('things', { label: 'good' });
                        const result = await ingestChanges(group, target, { writer: admin });
                        assertEquals(result.accepted, 1, 'the good insert lands');
                        assertEquals(result.rejected.length, 1, 'the bad one is refused');
                        assertEquals((await rdbRows(group, 'things')).map((r) => r.values.label).join(','), 'good',
                            'only the good row is in rdb');
                        const failures = (await target.drainOpEvents()).filter((e) => e.event.direction === 'failure');
                        assertEquals(failures.length, 1, 'the refusal is logged');
                        assertEquals((await target.drainChanges()).changes.length, 0, 'the outbox is acked');
                        await projectAs(group, target, admin);
                        assertEquals((await read.getRowIds('things')).length, 1, 'the target keeps only the good row');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN26] each refused write is logged: two refused updates of one row, in separate passes, log two events`,
                invoke: async () => {
                    const { group, admin } = await createGroup();
                    await (await group.getTable('ledger')).insert('l1', { ref: 'R-1', amount: '10.00' }, admin);
                    const l1 = deriveRowId('l1', admin.keyId);
                    const { target, read, local, cleanup } = await factory();
                    try {
                        await projectAs(group, target, admin);
                        const localId = (await read.syncId('ledger', l1))!;
                        for (const ref of ['R-2', 'R-3']) {
                            await local.update('ledger', localId, { ref });
                            const result = await ingestChanges(group, target, { writer: admin });
                            assertEquals(result.rejected.length, 1, `the update to ${ref} is refused`);
                            await projectAs(group, target, admin);
                        }
                        const failures = (await target.drainOpEvents()).filter((e) => e.event.direction === 'failure');
                        assertEquals(failures.length, 2, 'both refusals are in the event log');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN27] a crash replay of a pass logs each failure once`,
                invoke: async () => {
                    const { group, admin } = await createGroup();
                    await (await group.getTable('ledger')).insert('l1', { ref: 'R-1', amount: '10.00' }, admin);
                    const l1 = deriveRowId('l1', admin.keyId);
                    const inner = await factory();
                    const target = new CrashBeforeSettle(inner.target);
                    try {
                        await projectAs(group, target, admin);
                        const localId = (await inner.read.syncId('ledger', l1))!;
                        await inner.local.update('ledger', localId, { ref: 'R-2' });
                        await ingestChanges(group, target, { writer: admin });   // the settle is lost
                        await ingestChanges(group, target, { writer: admin });   // the replay
                        const failures = (await inner.target.drainOpEvents()).filter((e) => e.event.direction === 'failure');
                        assertEquals(failures.length, 1, 'the replayed refusal is logged once');
                    } finally {
                        await inner.cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN28] a native update that sets two readonly columns of one row is refused whole, with one failure event, and both are put back`,
                invoke: async () => {
                    const { group, admin } = await createTypedGroup();
                    await (await group.getTable('things')).insert('t1', { label: 'a', code: 'c1', spec: { v: 1 } }, admin);
                    const t1 = deriveRowId('t1', admin.keyId);
                    const { target, read, local, cleanup } = await factory();
                    try {
                        await projectAs(group, target, admin);
                        const localId = (await read.syncId('things', t1))!;
                        await local.update('things', localId, { code: 'c2', spec: { v: 2 } });
                        const result = await ingestChanges(group, target, { writer: admin });
                        assertEquals(result.rejected.length, 1, 'the update is refused whole (HIP-29)');
                        const failures = (await target.drainOpEvents()).filter((e) => e.event.direction === 'failure');
                        assertEquals(failures.length, 1, 'with one failure event');
                        const row = await read.getRow('things', t1);
                        assertEquals(row?.values.code, 'c1', 'code is put back');
                        assertEquals(JSON.stringify(row?.values.spec), JSON.stringify({ v: 1 }), 'spec is put back');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN29] a native update records every column it names, changed or not, NULL included, as one change of the row`,
                invoke: async () => {
                    const { group, admin } = await createTypedGroup();
                    await (await group.getTable('things')).insert('t1', { label: 'a', note: 'n', flag: true, data: { x: 1 } }, admin);
                    const t1 = deriveRowId('t1', admin.keyId);
                    const { target, read, local, cleanup } = await factory();
                    try {
                        await projectAs(group, target, admin);
                        const localId = (await read.syncId('things', t1))!;
                        // HIP-29: a named column is a write, so capture no longer compares.
                        await local.update('things', localId, { label: 'a', note: 'n', flag: true, data: { x: 1 }, ratio: null });
                        await local.update('things', localId, { label: 'b', note: 'n', data: { x: 1 } });
                        const captured = (await target.drainChanges()).changes
                            .map((c) => c.kind === 'update' ? Object.keys(c.values).sort().join('+') : c.kind);
                        assertEquals(captured.join(','), 'data+flag+label+note+ratio,data+label+note',
                            'each update is one change with every column it names');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN30] a native insert that names another key as its author is refused and reverted, and logged`,
                invoke: async () => {
                    const { group, admin } = await createTypedGroup();
                    const other = await makeIdentity();
                    const { target, read, local, cleanup } = await factory();
                    try {
                        await projectAs(group, target, admin);
                        const keys = target as unknown as KeyIndex;
                        const otherId = await keys.registerKey(DEFAULT_KEY_DOMAIN, other.keyId, serializePublicKeyToBase64(other.publicKey));
                        await local.insert('things', { label: 'x' }, otherId);
                        const result = await ingestChanges(group, target, { writer: admin });
                        assertEquals(result.accepted, 0, 'nothing lands');
                        assertEquals(result.rejected.length, 1, 'the insert is refused');
                        assertEquals((await rdbRows(group, 'things')).length, 0, 'rdb has no row');
                        const failures = (await target.drainOpEvents()).filter((e) => e.event.direction === 'failure');
                        assertEquals(failures.length, 1, 'the refusal is logged');
                        await projectAs(group, target, admin);
                        assertEquals((await read.getRowIds('things')).length, 0, 'the local row is removed');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN31] a native insert that names the writer key, or no author, is signed with the writer key`,
                invoke: async () => {
                    const { group, admin } = await createTypedGroup();
                    const { target, local, cleanup } = await factory();
                    try {
                        await projectAs(group, target, admin);
                        const keys = target as unknown as KeyIndex;
                        const writerId = await keys.registerKey(DEFAULT_KEY_DOMAIN, admin.keyId, serializePublicKeyToBase64(admin.publicKey));
                        await local.insert('things', { label: 'named' }, writerId);
                        await local.insert('things', { label: 'unnamed' });
                        const result = await ingestChanges(group, target, { writer: admin });
                        assertEquals(result.accepted, 2, 'both land');
                        assertEquals(result.rejected.length, 0, 'nothing is refused');
                        const rows = await rdbRows(group, 'things');
                        assertTrue(rows.length === 2 && rows.every((r) => r.author === admin.keyId), 'both are signed with the writer key');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN32] a native update of the author column is refused whole and reverted, with one failure event, its other columns with it`,
                invoke: async () => {
                    const { group, admin } = await createTypedGroup();
                    await (await group.getTable('things')).insert('t1', { label: 'a' }, admin);
                    const t1 = deriveRowId('t1', admin.keyId);
                    const other = await makeIdentity();
                    const { target, read, local, cleanup } = await factory();
                    try {
                        await projectAs(group, target, admin);
                        const keys = target as unknown as KeyIndex;
                        const adminId = await keys.idForKeyHash(DEFAULT_KEY_DOMAIN, admin.keyId);
                        const otherId = await keys.registerKey(DEFAULT_KEY_DOMAIN, other.keyId, serializePublicKeyToBase64(other.publicKey));
                        const localId = (await read.syncId('things', t1))!;
                        await local.update('things', localId, { author_key_id: otherId, label: 'b' });
                        const result = await ingestChanges(group, target, { writer: admin });
                        assertEquals(result.accepted, 0, 'nothing lands: an update is refused whole (HIP-29)');
                        assertEquals(result.rejected.length, 1, 'the update is refused');
                        const failures = (await target.drainOpEvents()).filter((e) => e.event.direction === 'failure');
                        assertEquals(failures.length, 1, 'with one failure event');
                        const rdbRow = (await rdbRows(group, 'things')).find((r) => r.rowId === t1)!;
                        assertEquals(rdbRow.values.label, 'a', 'rdb keeps the label');
                        assertEquals(rdbRow.author, admin.keyId, 'and the author');
                        const row = await read.getRow('things', t1);
                        assertEquals(row?.author, adminId, 'the settle puts the local author back');
                        assertEquals(row?.values.label, 'a', 'and the label'); 
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN33] without an identity provider, a native insert that names a key is refused, and one that names none is anonymous`,
                invoke: async () => {
                    const { group, admin } = await createAnonymousGroup();
                    const { target, local, cleanup } = await factory();
                    try {
                        await projectAs(group, target, admin);
                        const keys = target as unknown as KeyIndex;
                        const adminId = await keys.registerKey(DEFAULT_KEY_DOMAIN, admin.keyId, serializePublicKeyToBase64(admin.publicKey));
                        await local.insert('things', { label: 'named' }, adminId);
                        await local.insert('things', { label: 'unnamed' });
                        const result = await ingestChanges(group, target, { writer: admin });
                        assertEquals(result.accepted, 1, 'the unnamed insert lands');
                        assertEquals(result.rejected.length, 1, 'the named one is refused');
                        const rows = await rdbRows(group, 'things');
                        assertEquals(rows.map((r) => r.values.label).join(','), 'unnamed', 'only the unnamed row is in rdb');
                        assertEquals(rows[0]?.author, undefined, 'and it is anonymous');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN34] a native insert gets the defaults of the columns it leaves out, in the row and the captured change; an explicit null stays NULL`,
                invoke: async () => {
                    const { group, admin } = await createTypedGroup();
                    const { target, read, local, cleanup } = await factory();
                    try {
                        await projectAs(group, target, admin);
                        await local.insert('things', { label: 'left-out' });
                        await local.insert('things', { label: 'nulled', kind: null });
                        const inserts = (await target.drainChanges()).changes.filter((c) => c.kind === 'insert');
                        const valuesOf = (labelValue: string) => inserts.map((c) => c.kind === 'insert' ? c.values : {})
                            .find((v) => v.label === labelValue)!;
                        assertEquals(valuesOf('left-out').kind, 'std', 'the captured insert carries the default it left out');
                        assertEquals(valuesOf('left-out').level, 1, 'also a required column with a default');
                        assertEquals(valuesOf('nulled').kind ?? null, null, 'an explicit null is not replaced by the default');
                        assertEquals(valuesOf('nulled').level, 1, 'the column it left out gets its default');

                        const result = await ingestChanges(group, target, { writer: admin });
                        assertEquals(result.accepted, 2, 'both land');
                        await projectAs(group, target, admin);
                        const rows = await rdbRows(group, 'things');
                        const leftOut = rows.find((r) => r.values.label === 'left-out')!;
                        const nulled = rows.find((r) => r.values.label === 'nulled')!;
                        assertEquals(sorted((await read.getRow('things', leftOut.rowId))?.values), sorted({ label: 'left-out', kind: 'std', level: 1 }),
                            'the device that wrote it reads the defaults');
                        assertEquals(sorted((await read.getRow('things', nulled.rowId))?.values), sorted({ label: 'nulled', level: 1 }),
                            'and NULL where the app wrote null');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN35] project-back writes NULL to the author column of a row rdb has no author for: after a refused native change, and after a later remote update`,
                invoke: async () => {
                    const { group, admin } = await createAnonymousGroup();
                    const things = await group.getTable('things');
                    await things.insert('t1', { label: 'a' });
                    const t1 = deriveRowId('t1', undefined);
                    const { target, read, local, cleanup } = await factory();
                    try {
                        await projectAs(group, target, admin);
                        const keys = target as unknown as KeyIndex;
                        const adminId = await keys.registerKey(DEFAULT_KEY_DOMAIN, admin.keyId, serializePublicKeyToBase64(admin.publicKey));
                        const localId = (await read.syncId('things', t1))!;
                        assertEquals((await read.getRow('things', t1))?.author ?? null, null, 'the projected row is anonymous');

                        // A captured change of the author column is refused, and the
                        // revert has no author to write.
                        await local.update('things', localId, { author_key_id: adminId });
                        const result = await ingestChanges(group, target, { writer: admin });
                        assertEquals(result.rejected.length, 1, 'the author change is refused');
                        const failures = (await target.drainOpEvents()).filter((e) => e.event.direction === 'failure');
                        assertEquals(failures.length, 1, 'and logged');
                        assertEquals((await read.getRow('things', t1))?.author ?? null, null, 'the revert clears the author column');

                        // An uncaptured change of the column, then a remote update of the
                        // row: projecting it clears the column too.
                        await local.setCaptureEnabled(false);
                        await local.update('things', localId, { author_key_id: adminId });
                        await local.setCaptureEnabled(true);
                        await things.update(t1, { label: 'b' });
                        await projectAs(group, target, admin);
                        const row = await read.getRow('things', t1);
                        assertEquals(row?.values.label, 'b', 'the remote update lands');
                        assertEquals(row?.author ?? null, null, 'and the author column is NULL again');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN36] a projection pass refuses while a change is pending, and changes nothing; a cycle that ingests the change first then projects`,
                invoke: async () => {
                    const { group, admin } = await createGroup();
                    const ledger = await group.getTable('ledger');
                    await ledger.insert('l1', { ref: 'R-1', amount: '1.00' }, admin);

                    const { target, read, local, cleanup } = await factory();
                    try {
                        await projectAs(group, target, admin);
                        const checked = target as unknown as CheckedTarget;
                        const before = await target.getCheckpoint(group.getId());
                        await ledger.insert('l2', { ref: 'R-2', amount: '2.00' }, admin);   // a remote change
                        await local.insert('tags', { code: 'pending' });

                        let refused = false;
                        try { await projectAs(group, target, admin); } catch (e) { refused = e instanceof PendingChangesError; }
                        assertTrue(refused, 'the apply refuses with PendingChangesError');
                        assertTrue(sameVersion(await target.getCheckpoint(group.getId()), before!), 'the checkpoint stays');
                        const l2 = deriveRowId('l2', admin.keyId);
                        assertTrue(await read.getRow('ledger', l2) === undefined, 'the remote row is not projected');
                        await assertAtCheckpoints(checked, [{ group }], 'after the refusal');

                        await syncGroup(group, target, { writer: admin });
                        assertTrue(await read.getRow('ledger', l2) !== undefined, 'a cycle ingests the change, then projects');
                        assertTrue(!await target.hasPendingChanges(), 'the outbox is empty');
                        await assertAtCheckpoints(checked, [{ group }], 'after the cycle');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN37] an edit captured while a delta is computed is kept, the row never shows the remote value over it, and the next cycle ingests it first`,
                invoke: async () => {
                    const { group, admin } = await createGroup();
                    const ledger = await group.getTable('ledger');
                    await ledger.insert('l1', { ref: 'R-1', memo: 'start', amount: '1.00' }, admin);
                    const l1 = deriveRowId('l1', admin.keyId);

                    const { target, read, local, cleanup } = await factory();
                    try {
                        await projectAs(group, target, admin);
                        const checked = target as unknown as CheckedTarget;
                        const localId = (await read.syncId('ledger', l1))!;
                        await ledger.update(l1, { memo: 'remote' }, admin);   // another device's edit reaches rdb

                        let edited = false;
                        const hooked = beforeApply(target, async () => {
                            if (edited) return;
                            edited = true;
                            await local.update('ledger', localId, { memo: 'local' });   // captured during the delta
                        });
                        await syncGroup(group, hooked, { writer: admin });
                        assertTrue(edited, 'the edit was made during the projection pass');
                        assertEquals((await read.getRow('ledger', l1))?.values.memo, 'local', 'the pending edit stays on screen');
                        assertTrue(await target.hasPendingChanges(), 'and pending');
                        await assertAtCheckpoints(checked, [{ group }], 'after the refused apply');

                        await syncGroup(group, target, { writer: admin });
                        assertTrue(!await target.hasPendingChanges(), 'the next cycle ingests it');
                        await assertAtCheckpoints(checked, [{ group }], 'after the next cycle');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN38] a change captured during ingestion is ingested in the same cycle, which then projects`,
                invoke: async () => {
                    const { group, admin } = await createGroup();
                    const { target, local, cleanup } = await factory();
                    try {
                        await projectAs(group, target, admin);
                        const checked = target as unknown as CheckedTarget;
                        await local.insert('tags', { code: 'first' });

                        let wrote = false;
                        const hooked = beforeSettle(target, async () => {
                            if (wrote) return;
                            wrote = true;
                            await local.insert('tags', { code: 'during' });   // after the drain, before the settle
                        });
                        await syncGroup(group, hooked, { writer: admin });
                        assertTrue(wrote, 'the change was made during ingestion');
                        assertTrue(!await target.hasPendingChanges(), 'both changes were ingested in the cycle');
                        const codes = (await rdbRows(group, 'tags')).map((r) => r.values.code).sort();
                        assertEquals(codes.join(','), 'during,first', 'both reached rdb');
                        assertTrue(sameVersion(await target.getCheckpoint(group.getId()), await (await group.getScopedDag()).getFrontier()),
                            'and the cycle projected');
                        await assertAtCheckpoints(checked, [{ group }], 'after the cycle');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN39] a native update that names a column at its current value records it, and rdb takes it as a write`,
                invoke: async () => {
                    const { group, admin } = await createGroup();
                    await (await group.getTable('ledger')).insert('l1', { ref: 'R-1', memo: 'same', amount: '1.00' }, admin);
                    const l1 = deriveRowId('l1', admin.keyId);
                    const { target, read, local, cleanup } = await factory();
                    try {
                        await projectAs(group, target, admin);
                        const localId = (await read.syncId('ledger', l1))!;
                        await local.update('ledger', localId, { memo: 'same' });
                        const changes = (await target.drainChanges()).changes;
                        assertEquals(changes.map((c) => c.kind === 'update' ? JSON.stringify(c.values) : c.kind).join('; '),
                            '{"memo":"same"}', 'the unchanged column is recorded');
                        const before = (await rowOps(group, 'ledger')).length;
                        const result = await ingestChanges(group, target, { writer: admin });
                        assertEquals(result.accepted, 1, 'and ingested');
                        assertEquals((await rowOps(group, 'ledger')).length, before + 1, 'as an update op');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN40] a native insert's empty author column reads the writer key's id at once under auto; under manual and in an anonymous group it stays NULL`,
                invoke: async () => {
                    const authorOf = async (target: BidirectionalTarget, table: string, localId: number): Promise<unknown> =>
                        (await (target as unknown as QueryableTarget).executeQuery({ table })).find((r) => r.id === localId)?.author_key_id ?? null;
                    const capturedAuthor = async (target: BidirectionalTarget, localId: number): Promise<unknown> => {
                        const insert = (await target.drainChanges()).changes.find((c) => c.kind === 'insert' && c.localId === localId);
                        return insert !== undefined && insert.kind === 'insert' ? (insert.values.author_key_id ?? null) : 'no insert';
                    };

                    const { group, admin } = await createGroup();
                    let harness = await factory();
                    try {
                        await projectGroup(group, harness.target);
                        const keyId = await recordWriter(harness.target, 'auto', admin);
                        const localId = await harness.local.insert('tags', { code: 'mine' });
                        assertEquals(await authorOf(harness.target, 'tags', localId), keyId, 'the row reads the writer key at once');
                        assertEquals(await capturedAuthor(harness.target, localId), keyId, 'and so does the captured insert');
                    } finally {
                        await harness.cleanup?.();
                    }

                    harness = await factory();
                    try {
                        await projectGroup(group, harness.target);
                        await recordWriter(harness.target, 'manual', admin);
                        const localId = await harness.local.insert('tags', { code: 'anon' });
                        assertEquals(await authorOf(harness.target, 'tags', localId), null, 'under manual the column stays empty');
                    } finally {
                        await harness.cleanup?.();
                    }

                    const anon = await createAnonymousGroup();
                    harness = await factory();
                    try {
                        await projectGroup(anon.group, harness.target);
                        await recordWriter(harness.target, 'auto', anon.admin);
                        const localId = await harness.local.insert('things', { label: 'x' });
                        assertEquals(await authorOf(harness.target, 'things', localId), null, 'in an anonymous group it stays empty, under auto too');
                    } finally {
                        await harness.cleanup?.();
                    }

                    harness = await factory();
                    try {
                        await projectGroup(group, harness.target);
                        const localId = await harness.local.insert('tags', { code: 'no record' });
                        assertEquals(await authorOf(harness.target, 'tags', localId), null, 'and with no writer record, nothing is filled');
                    } finally {
                        await harness.cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN41] an update naming the author and a readonly column at the row's own values lands; at other values it is refused`,
                invoke: async () => {
                    const { group, admin } = await createGroup();
                    await (await group.getTable('ledger')).insert('l1', { ref: 'R-1', amount: '1.00' }, admin);
                    const l1 = deriveRowId('l1', admin.keyId);
                    const { target, read, local, cleanup } = await factory();
                    try {
                        await projectAs(group, target, admin);
                        const localId = (await read.syncId('ledger', l1))!;
                        const row = (await (target as unknown as QueryableTarget).executeQuery({ table: 'ledger' })).find((r) => r.id === localId)!;
                        const ownAuthor = row.author_key_id as number;

                        // a record read back and written whole: the author and the readonly ref at their own values
                        await local.update('ledger', localId, { ref: 'R-1', author_key_id: ownAuthor, memo: 'whole' });
                        let result = await ingestChanges(group, target, { writer: admin });
                        assertEquals(result.rejected.length, 0, 'the row\'s own values are no refusal');
                        assertEquals(result.accepted, 1, 'the update lands');
                        const after = (await rdbRows(group, 'ledger')).find((r) => r.rowId === l1)!;
                        assertEquals(after.values.memo, 'whole', 'with the column that changed');
                        assertEquals(after.values.ref, 'R-1', 'and the readonly column as it was');

                        await local.update('ledger', localId, { ref: 'R-2' });
                        result = await ingestChanges(group, target, { writer: admin });
                        assertTrue(result.rejected.length === 1 && result.rejected[0].reason.includes('readonly'), 'another readonly value is refused');

                        const other = await makeIdentity();
                        const otherId = await (target as unknown as KeyIndex).registerKey(DEFAULT_KEY_DOMAIN, other.keyId, serializePublicKeyToBase64(other.publicKey));
                        await local.update('ledger', localId, { author_key_id: otherId });
                        result = await ingestChanges(group, target, { writer: admin });
                        assertTrue(result.rejected.length === 1 && result.rejected[0].reason.includes('author'), 'another author is refused');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN42] an insert with an empty author column, captured while no writer key was recorded, is refused`,
                invoke: async () => {
                    const { group, admin } = await createGroup();
                    const { target, local, cleanup } = await factory();
                    try {
                        await projectGroup(group, target);
                        await recordWriter(target, 'auto');          // opened without a writer key
                        await local.insert('tags', { code: 'early' });
                        await recordWriter(target, 'auto', admin);   // then with one
                        const result = await ingestChanges(group, target, { writer: admin });
                        assertEquals(result.accepted, 0, 'nothing lands');
                        assertTrue(result.rejected.length === 1 && result.rejected[0].reason.includes('author'),
                            `the insert is refused for its empty author, got: ${result.rejected.map((r) => r.reason).join('; ')}`);
                        assertEquals((await rdbRows(group, 'tags')).length, 0, 'and rdb has no row');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN43] under manual a native insert that leaves the author empty is ingested anonymously, and one naming the writer key's id is signed; with no author column the writer key signs`,
                invoke: async () => {
                    const { group, admin } = await createGroup();
                    let harness = await factory();
                    try {
                        await projectGroup(group, harness.target, { author: 'manual' });
                        const keyId = await recordWriter(harness.target, 'manual', admin);
                        await harness.local.insert('tags', { code: 'anon' });
                        await harness.local.insert('tags', { code: 'signed', author_key_id: keyId! });
                        const result = await ingestChanges(group, harness.target, { writer: admin, author: 'manual' });
                        assertEquals(result.rejected.length, 0, 'nothing is refused');
                        const rows = await rdbRows(group, 'tags');
                        const byCode = new Map(rows.map((r) => [r.values.code, r]));
                        assertEquals(byCode.get('anon')?.author, undefined, 'the empty author is ingested anonymously');
                        assertEquals(byCode.get('anon')?.rowId, deriveRowId(byCode.get('anon')!.uuid), 'with a row hash from its uuid alone');
                        assertEquals(byCode.get('signed')?.author, admin.keyId, 'the named writer key signs');
                        assertEquals(byCode.get('signed')?.rowId, deriveRowId(byCode.get('signed')!.uuid, admin.keyId), 'with a row hash from its signer');
                    } finally {
                        await harness.cleanup?.();
                    }

                    const second = await createGroup();
                    harness = await factory();
                    try {
                        const config = { author: 'manual' as const, authorColumn: false as const };
                        await projectGroup(second.group, harness.target, config);
                        await harness.local.insert('tags', { code: 'plain' });
                        const result = await ingestChanges(second.group, harness.target, { writer: second.admin, ...config });
                        assertEquals(result.accepted, 1, 'the insert lands');
                        assertEquals((await rdbRows(second.group, 'tags'))[0]?.author, second.admin.keyId, 'signed with the writer key');
                    } finally {
                        await harness.cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN90] each table's meta records whether its group has an identity provider, also for a table a schema change adds later: under auto, a native insert into that table reads the writer key's id at once in a signed group, and stays NULL in an anonymous one`,
                invoke: async () => {
                    type GroupOf = (table: string) => { anonymous: boolean } | undefined | Promise<{ anonymous: boolean } | undefined>;
                    const anonymousOf = async (target: BidirectionalTarget, table: string): Promise<boolean | undefined> =>
                        (await (target as unknown as { tableGroup: GroupOf }).tableGroup(table))?.anonymous;
                    const authorOf = async (target: BidirectionalTarget, table: string, localId: number): Promise<unknown> =>
                        (await (target as unknown as QueryableTarget).executeQuery({ table })).find((r) => r.id === localId)?.author_key_id ?? null;
                    const later: TableDef = {
                        name: 'later', columns: { note: { type: 'string', nullable: true } },
                        restrictions: [{ on: 'all', rule: { p: 'true' } }],
                    };
                    const run = async (fixture: GroupFixture, table: string, anonymous: boolean): Promise<void> => {
                        const { schema, group, admin } = fixture;
                        const harness = await factory();
                        try {
                            await projectGroup(group, harness.target);
                            const keyId = await recordWriter(harness.target, 'auto', admin);
                            assertEquals(await anonymousOf(harness.target, table), anonymous, `${table} records it`);

                            await schema.updateSchema([{ rule: 'add-table', def: later }], admin, 'add later');
                            await group.deploy(await (await schema.getScopedDag()).getFrontier());
                            await projectGroup(group, harness.target);
                            assertEquals(await anonymousOf(harness.target, 'later'), anonymous, 'so does a table added later');
                            const localId = await harness.local.insert('later', { note: 'x' });
                            assertEquals(await authorOf(harness.target, 'later', localId), anonymous ? null : keyId,
                                anonymous ? 'an insert into it stays NULL' : "an insert into it reads the writer key's id at once");
                        } finally {
                            await harness.cleanup?.();
                        }
                    };
                    await run(await createGroup(), 'tags', false);
                    await run(await createAnonymousGroup(), 'things', true);
                },
            },
            {
                name: `[${label}-IN44] a settle with a moved checkpoint throws and changes nothing; with the checkpoint it expects, it writes and moves it`,
                invoke: async () => {
                    const { group } = await createGroup();
                    const groupId = group.getId();
                    const { target, local, cleanup } = await factory();
                    try {
                        await projectGroup(group, target);
                        const cp = (await target.getCheckpoint(groupId))!;
                        const localId = await local.insert('tags', { code: 'x' });
                        const [change] = (await target.drainChanges()).changes;
                        const mapping: SyncMapping = { table: 'tags', localId, rowId: 'ROW-NEW', uuid: 'u-new', status: 'active' };
                        const next = new Set(['the-next-checkpoint']);

                        const wrong = new Set(['not-the-checkpoint']);
                        let moved = false;
                        try {
                            await target.commitIngest({ consumed: [change.id], checkpoints: [{ groupId, expected: wrong, next }], inserts: [mapping],
                                events: [failureEvent(groupId, 'op-x', localId)] });
                        } catch (e) { moved = e instanceof CheckpointMovedError; }
                        assertTrue(moved, 'a moved checkpoint throws CheckpointMovedError');
                        assertEquals((await target.drainChanges()).changes.length, 1, 'the change is still in the outbox');
                        assertEquals((await target.resolveRow('tags', localId))?.rowId, undefined, 'no mapping is written');
                        assertEquals((await target.drainOpEvents()).length, 0, 'no event is stored');
                        assertTrue(sameVersion(await target.getCheckpoint(groupId), cp), 'the checkpoint is as it was');

                        await target.commitIngest({ consumed: [change.id], checkpoints: [{ groupId, expected: cp, next }], inserts: [mapping] });
                        assertEquals((await target.drainChanges()).changes.length, 0, 'the change leaves the outbox');
                        const stored = await target.resolveRow('tags', localId);
                        assertTrue(stored?.rowId === 'ROW-NEW' && stored.uuid === 'u-new' && (stored.status ?? 'active') === 'active', 'the insert\'s mapping is written');
                        assertTrue(sameVersion(await target.getCheckpoint(groupId), next), 'and the checkpoint moves');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN45] a revert with a pending change writes rdb's row with the change on top`,
                invoke: async () => {
                    const { group, admin } = await createGroup();
                    await (await group.getTable('ledger')).insert('l1', { ref: 'R-1', memo: 'start', amount: '1.00' }, admin);
                    const l1 = deriveRowId('l1', admin.keyId);
                    const { target, read, local, cleanup } = await factory();
                    try {
                        await projectAs(group, target, admin);
                        const localId = (await read.syncId('ledger', l1))!;
                        await local.update('ledger', localId, { memo: 'refused' });
                        await local.update('ledger', localId, { amount: '2.00' });
                        const [refused, pending] = (await target.drainChanges()).changes;
                        await target.commitIngest({ consumed: [refused.id],
                            reverts: [{ table: 'ledger', rowId: l1, localId, row: await rdbRowAction(group, 'ledger', l1) }] });
                        const row = await localRow(target, 'ledger', localId);
                        assertEquals(row?.memo, 'start', "rdb's value is back");
                        assertEquals(row?.amount, '2.00', 'with the pending change on top');
                        const left = (await target.drainChanges()).changes;
                        assertTrue(left.length === 1 && left[0].id === pending.id, 'the pending change stays in the outbox');

                        await local.delete('ledger', localId);
                        const [, deleted] = (await target.drainChanges()).changes;
                        await target.commitIngest({ consumed: [pending.id],
                            reverts: [{ table: 'ledger', rowId: l1, localId, row: await rdbRowAction(group, 'ledger', l1) }] });
                        assertEquals(await localRow(target, 'ledger', localId), undefined, 'a pending delete leaves no row');
                        assertEquals((await target.drainChanges()).changes.map((c) => c.id).join(','), String(deleted.id), 'and stays pending');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN46] a revert of a row rdb no longer has removes it and marks its mapping deleted; a pending update of it finds no row`,
                invoke: async () => {
                    const { group, admin } = await createGroup();
                    const ledger = await group.getTable('ledger');
                    await ledger.insert('l1', { ref: 'R-1', amount: '1.00' }, admin);
                    const l1 = deriveRowId('l1', admin.keyId);
                    const { target, read, local, cleanup } = await factory();
                    try {
                        await projectAs(group, target, admin);
                        const localId = (await read.syncId('ledger', l1))!;
                        await ledger.delete(l1, admin);
                        await local.update('ledger', localId, { memo: 'one' });
                        await local.update('ledger', localId, { memo: 'two' });
                        const [first, second] = (await target.drainChanges()).changes;
                        await target.commitIngest({ consumed: [first.id], reverts: [{ table: 'ledger', rowId: l1, localId }] });
                        assertEquals(await localRow(target, 'ledger', localId), undefined, 'the row is removed');
                        assertEquals((await target.resolveRow('ledger', localId))?.status, 'deleted', 'and its mapping marked deleted');
                        assertEquals((await target.drainChanges()).changes.map((c) => c.id).join(','), String(second.id), 'the pending update stays');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN47] writing each kind of record, and an end's observe beside a checkpoint, signals no change and moves no checkpoint`,
                invoke: async () => {
                    const { group } = await createGroup();
                    const groupId = group.getId();
                    const { target, local, cleanup } = await factory();
                    try {
                        await projectGroup(group, target);
                        const cp = (await target.getCheckpoint(groupId))!;
                        for (const code of ['a', 'b', 'c']) await local.insert('tags', { code });
                        const [c1, c2, c3] = (await target.drainChanges()).changes;

                        const signals: string[] = [];
                        const reactive = target as unknown as { addCommitListener?: (l: (e: { source: string }) => void) => void;
                            removeCommitListener?: (l: (e: { source: string }) => void) => void };
                        const listener = (e: { source: string }): void => { signals.push(e.source); };
                        reactive.addCommitListener?.(listener);
                        try {
                            // arming a listener announces the outbox already pending: let it pass
                            await new Promise((resolve) => setTimeout(resolve, 300));
                            signals.length = 0;
                            const failure = failureEvent(groupId, 'op-b', c2.localId);
                            const [o1, o2] = [{ group: 'bound-1', hash: 'obs-1' }, { group: 'bound-2', hash: 'obs-2' }];
                            await target.writeRecord(c1.id, { observes: [o1] });
                            await target.writeRecord(c1.id, { observes: [o2], entry: 'entry-a' });
                            await target.writeRecord(c2.id, { failure });
                            await target.writeRecord(c3.id, { nothing: true });
                            await target.recordEndObserve(groupId, 'end-1');
                            await target.recordEndObserve(groupId, 'end-2');
                            await new Promise((resolve) => setTimeout(resolve, 300));
                            const records = (await target.drainChanges()).changes.map((c) => JSON.stringify(c.record ?? null));
                            assertEquals(records[0], JSON.stringify({ observes: [o1, o2], entry: 'entry-a' }), 'observes add up, and the entry is kept');
                            assertEquals(records[1], JSON.stringify({ failure }), 'a failure event');
                            assertEquals(records[2], JSON.stringify({ nothing: true }), 'the mark of nothing to write');
                            await target.writeRecord(c1.id, { failure });
                            assertEquals(JSON.stringify((await target.drainChanges()).changes[0].record),
                                JSON.stringify({ observes: [o1, o2], failure }), 'an outcome replaces the one there');
                            assertEquals((await target.getEndObserves(groupId)).join(','), 'end-1,end-2', "the end's observes, in order");
                            assertEquals(signals.length, 0, `no change is signalled, got: ${signals.join(',')}`);
                            assertTrue(sameVersion(await target.getCheckpoint(groupId), cp), 'and no checkpoint moves');
                        } finally {
                            reactive.removeCommitListener?.(listener);
                        }
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN88] a settle clears the end's observes of every group, also of a group whose checkpoint it doesn't move; a settle that throws leaves them all`,
                invoke: async () => {
                    const a = await createGroup();
                    const b = await createFkGroup();
                    const [ga, gb] = [a.group.getId(), b.group.getId()];
                    const { target, cleanup } = await factory();
                    try {
                        await projectGroup(a.group, target);
                        await projectGroup(b.group, target, { tablePrefix: 'b' });   // its tables apart from a's
                        const cpA = (await target.getCheckpoint(ga))!;
                        await target.recordEndObserve(ga, 'end-a');
                        await target.recordEndObserve(gb, 'end-b');
                        const wrong = new Set(['not-the-checkpoint']);
                        await target.commitIngest({ consumed: [], checkpoints: [{ groupId: ga, expected: wrong, next: wrong }] })
                            .catch(() => undefined);
                        assertEquals(`${(await target.getEndObserves(ga)).join(',')}|${(await target.getEndObserves(gb)).join(',')}`, 'end-a|end-b',
                            'a settle that throws leaves every list');
                        await target.commitIngest({ consumed: [], checkpoints: [{ groupId: ga, expected: cpA, next: cpA }] });
                        assertEquals((await target.getEndObserves(ga)).length + (await target.getEndObserves(gb)).length, 0,
                            "a settle clears every group's list, the one it names and the other");
                        assertTrue(sameVersion(await target.getCheckpoint(ga), cpA), 'and moves no checkpoint it keeps');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN48] the settle stores the failure events of the handled rows, before its other events, and none of the rows it keeps`,
                invoke: async () => {
                    const { group } = await createGroup();
                    const groupId = group.getId();
                    const { target, local, cleanup } = await factory();
                    try {
                        await projectGroup(group, target);
                        await local.insert('tags', { code: 'a' });
                        await local.insert('tags', { code: 'b' });
                        const [c1, c2] = (await target.drainChanges()).changes;
                        await target.writeRecord(c1.id, { failure: failureEvent(groupId, 'op-1', c1.localId) });
                        await target.writeRecord(c2.id, { failure: failureEvent(groupId, 'op-2', c2.localId) });
                        await target.commitIngest({ consumed: [c1.id], events: [{ ...failureEvent(groupId, 'op-delta', c1.localId), origin: 'concurrency', direction: 'void' }] });
                        const stored = (await target.drainOpEvents()).map((e) => e.event.opHash);
                        assertEquals(stored.join(','), 'op-1,op-delta', "the handled row's failure event, then the settle's own");
                        const left = (await target.drainChanges()).changes;
                        assertTrue(left.length === 1 && left[0].record?.failure?.opHash === 'op-2', 'the kept row keeps its record');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-CHK01] the consistency check holds after a projection and over pending changes, and finds a write capture missed`,
                invoke: async () => {
                    const { group, admin } = await createGroup();
                    const ledger = await group.getTable('ledger');
                    await ledger.insert('l1', { ref: 'R-1', amount: '10.00' }, admin);
                    await ledger.insert('l2', { ref: 'R-2', memo: 'two', amount: '2.00' }, admin);

                    const { target, read, local, cleanup } = await factory();
                    try {
                        await projectAs(group, target, admin);
                        const checked = target as unknown as CheckedTarget;
                        await assertAtCheckpoints(checked, [{ group }], 'after the projection');

                        const l1 = (await read.syncId('ledger', deriveRowId('l1', admin.keyId)))!;
                        const l2 = (await read.syncId('ledger', deriveRowId('l2', admin.keyId)))!;
                        await local.update('ledger', l1, { memo: 'ping' });
                        await local.delete('ledger', l2);
                        await local.insert('tags', { code: 'new' });
                        await assertAtCheckpoints(checked, [{ group }], 'over three pending changes');

                        await local.setCaptureEnabled(false);
                        await local.update('ledger', l1, { memo: 'lost' });
                        await local.setCaptureEnabled(true);
                        let found = false;
                        try {
                            await assertAtCheckpoints(checked, [{ group }], 'after an uncaptured write');
                        } catch (_e) {
                            found = true;
                        }
                        assertTrue(found, 'the check finds a write that capture missed');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-CHK02] a reopened harness finds what was committed: the checkpoint, the rows and the pending change`,
                invoke: async () => {
                    const { group, admin } = await createGroup();
                    await (await group.getTable('ledger')).insert('l1', { ref: 'R-1', amount: '10.00' }, admin);

                    let harness = await factory();
                    try {
                        if (harness.reopen === undefined) return;   // memory: nothing outlives the process
                        await projectAs(group, harness.target, admin);
                        const l1 = (await harness.read.syncId('ledger', deriveRowId('l1', admin.keyId)))!;
                        await harness.local.update('ledger', l1, { memo: 'ping' });

                        harness = await harness.reopen();
                        const checked = harness.target as unknown as CheckedTarget;
                        assertTrue(await harness.target.getCheckpoint(group.getId()) !== undefined, 'the checkpoint is there');
                        assertEquals((await harness.target.drainChanges()).changes.length, 1, 'the pending change is there');
                        await assertAtCheckpoints(checked, [{ group }], 'after the reopen');
                    } finally {
                        await harness.cleanup?.();
                    }
                },
            },
            ...createChainCases(label, factory),
        ],
    };
}
