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
import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import { deriveRowId, Row, RTableGroup } from "@hyper-hyper-space/hhs3_rdb";
import {
    BidirectionalTarget, CapturedBatch, IngestSettle, ingestChanges, MemoryTarget, OpEvent,
    projectGroup, RowAction, SchemaAction, StoredOpEvent, SyncMapping,
} from "@hyper-hyper-space/hhs3_rdb_adapter";

import { createFkGroup, createGroup } from "./group_fixture.js";
import { ProjectionReader, RowValues } from "./projection_reader.js";

// A BidirectionalTarget wrapper that DROPS the first commitIngest (its settle
// is silently discarded) then delegates normally - simulating a crash in the
// window AFTER the rdb append but BEFORE the target ack. reserveMint still runs
// (minted uuids are durable pre-append), and the append lands, so the outbox is
// left un-acked for a deterministic replay. Everything else delegates to inner.
class CrashBeforeSettle implements BidirectionalTarget {
    private commits = 0;
    constructor(private readonly inner: BidirectionalTarget) {}

    apply(g: B64Hash, s: SchemaAction[], r: RowAction[], c: Version, e?: OpEvent[]): Promise<void> {
        return this.inner.apply(g, s, r, c, e);
    }
    getCheckpoint(g: B64Hash): Promise<Version | undefined> { return this.inner.getCheckpoint(g); }
    drainChanges(): Promise<CapturedBatch> { return this.inner.drainChanges(); }
    resolveRow(t: string, l: number): Promise<SyncMapping | undefined> { return this.inner.resolveRow(t, l); }
    reserveMint(m: SyncMapping[]): Promise<void> { return this.inner.reserveMint(m); }
    drainOpEvents(sinceId?: number): Promise<StoredOpEvent[]> { return this.inner.drainOpEvents(sinceId); }
    async commitIngest(settle: IngestSettle): Promise<void> {
        this.commits += 1;
        if (this.commits === 1) return;   // the "crash": drop the settle
        return this.inner.commitIngest(settle);
    }
}

// The app-side write surface a backend must expose for the suite: direct
// mutations of the projected tables that the capture machinery observes. Values
// are LOGICAL; the backend converts to its stored form. `insert` returns the
// projection-local id the row was given.
export interface LocalMutator {
    insert(table: string, values: RowValues, author?: number): number | Promise<number>;
    update(table: string, localId: number, values: RowValues): void | Promise<void>;
    delete(table: string, localId: number): void | Promise<void>;
    setCaptureEnabled(on: boolean): void | Promise<void>;
}

export type IngestionHarness = {
    target: BidirectionalTarget;
    read: ProjectionReader;
    local: LocalMutator;
    cleanup?: () => void | Promise<void>;
};

export type IngestionFactory = () => IngestionHarness | Promise<IngestionHarness>;

// The reference in-memory ingestion harness: a capture-provisioned MemoryTarget.
export function memoryIngestionHarness(): IngestionHarness {
    const target = new MemoryTarget({ captureChanges: true });
    const read: ProjectionReader = {
        hasTable: (table) => target.hasTable(table),
        getRowIds: (table) => target.getRowIds(table),
        getRow: (table, rowId) => target.getRowByRowId(table, rowId),
        syncId: (table, rowId) => target.syncId(table, rowId),
        columnType: (table, column) => target.columnTypes(table)?.[column],
    };
    const local: LocalMutator = {
        insert: (table, values, author) => target.localInsert(table, values, author),
        update: (table, localId, values) => target.localUpdate(table, localId, values),
        delete: (table, localId) => target.localDelete(table, localId),
        setCaptureEnabled: (on) => target.setCaptureEnabled(on),
    };
    return { target, read, local };
}

type NamedTest = { name: string; invoke: () => Promise<void> };

// Live rows of an rdb group table (the assertion target: what ingestion wrote).
async function rdbRows(group: RTableGroup, table: string): Promise<Row[]> {
    const view = await (await group.getTable(table)).getView();
    return view.query({});
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
                        await projectGroup(group, target);
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
                        await projectGroup(group, target);
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
                        await projectGroup(group, target);
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
                name: `[${label}-IN03] updateMerge: consecutive disjoint-field edits fold into a single op`,
                invoke: async () => {
                    const { group, admin } = await createGroup();
                    await (await group.getTable('ledger')).insert('l1', { ref: 'R-1', amount: '10.00' }, admin);

                    const { target, read, local, cleanup } = await factory();
                    try {
                        await projectGroup(group, target);
                        const l1 = deriveRowId('l1', admin.keyId);
                        const localId = (await read.syncId('ledger', l1))!;

                        // Disjoint columns (memo, amount) on the same row, consecutive:
                        // the default updateMerge folds them into ONE rdb update op.
                        await local.update('ledger', localId, { memo: 'a' });
                        await local.update('ledger', localId, { amount: '20.00' });
                        const result = await ingestChanges(group, target, { writer: admin });

                        assertEquals(result.accepted, 1, 'two disjoint edits merge into one op');
                        const row = (await rdbRows(group, 'ledger')).find((r) => r.rowId === l1);
                        assertEquals(row!.values.memo, 'a', 'first field landed');
                        assertEquals(row!.values.amount, '20.00', 'second field landed in the same op');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN03b] updateMerge:false emits one op per update change (order-preserving)`,
                invoke: async () => {
                    const { group, admin } = await createGroup();
                    await (await group.getTable('ledger')).insert('l1', { ref: 'R-1', amount: '10.00' }, admin);

                    const { target, read, local, cleanup } = await factory();
                    try {
                        await projectGroup(group, target);
                        const l1 = deriveRowId('l1', admin.keyId);
                        const localId = (await read.syncId('ledger', l1))!;

                        await local.update('ledger', localId, { memo: 'a' });
                        await local.update('ledger', localId, { amount: '20.00' });
                        const result = await ingestChanges(group, target, { writer: admin, updateMerge: false });

                        assertEquals(result.accepted, 2, 'two changes stay two ops when updateMerge is off');
                        const row = (await rdbRows(group, 'ledger')).find((r) => r.rowId === l1);
                        assertEquals(row!.values.memo, 'a', 'first field landed');
                        assertEquals(row!.values.amount, '20.00', 'second field landed');
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
                        await projectGroup(group, target);
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
                        await projectGroup(group, target);

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
                        await projectGroup(group, target);
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
                        await projectGroup(group, target);

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
                        await projectGroup(group, target);

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
                        await projectGroup(group, target);

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
                        await projectGroup(group, target);

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
                        await projectGroup(group, target);
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
                name: `[${label}-IN12] partial-failure: a multi-field update lands its valid field and records only the offender`,
                invoke: async () => {
                    const { group, admin } = await createGroup();
                    await (await group.getTable('ledger')).insert('l1', { ref: 'R-1', amount: '10.00' }, admin);

                    const { target, read, local, cleanup } = await factory();
                    try {
                        await projectGroup(group, target);
                        const l1 = deriveRowId('l1', admin.keyId);
                        const localId = (await read.syncId('ledger', l1))!;

                        // One editable field (memo) + one readonly field (ref), as two
                        // consecutive updates the planner merges. The offending field
                        // is dropped; the valid field still lands.
                        await local.update('ledger', localId, { memo: 'ok' });
                        await local.update('ledger', localId, { ref: 'HACKED' });
                        const result = await ingestChanges(group, target, { writer: admin });

                        assertEquals(result.accepted, 1, 'the valid field lands as one op');
                        assertEquals(result.rejected.length, 1, 'only the readonly field is rejected');
                        const row = (await rdbRows(group, 'ledger')).find((r) => r.rowId === l1);
                        assertEquals(row!.values.memo, 'ok', 'the valid field landed in rdb');
                        assertEquals(row!.values.ref, 'R-1', 'the readonly field did not');

                        const events = await target.drainOpEvents();
                        assertEquals(events.length, 1, 'one op-event for the offending field');
                        assertEquals(events[0].event.direction, 'failure', 'a failure');
                        assertEquals((await target.drainChanges()).changes.length, 0, 'the outbox is acked');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}-IN13] crash-replay: append landed but settle dropped -> replay reuses the uuid, no duplicate row`,
                invoke: async () => {
                    const { group, admin } = await createGroup();
                    await (await group.getTable('ledger')).insert('l1', { ref: 'R-1', amount: '10.00' }, admin);

                    const { target, local, cleanup } = await factory();
                    try {
                        await projectGroup(group, target);
                        const before = (await rdbRows(group, 'tags')).length;

                        const localId = await local.insert('tags', { code: 'once' });

                        // Pass 1: the wrapper drops the settle AFTER the append lands
                        // (a simulated crash). The reserved uuid is already durable
                        // and the rdb op is appended, but the outbox is not acked.
                        const first = await ingestChanges(group, new CrashBeforeSettle(target), { writer: admin });
                        assertEquals(first.accepted, 1, 'the op was appended before the crash');
                        assertEquals((await rdbRows(group, 'tags')).length, before + 1, 'the row is in rdb');
                        assertTrue((await target.drainChanges()).changes.length >= 1, 'the outbox is NOT acked (crash)');
                        const reserved = await target.resolveRow('tags', localId);
                        assertTrue(reserved !== undefined && reserved.uuid !== '', 'the minted uuid is durable pre-append');
                        const rowId = reserved!.rowId;

                        // Pass 2 (restart): replay drains the same change, reads the
                        // reserved uuid back, re-appends the SAME op (rdb reports it as
                        // already write-once -> idempotent skip), and acks.
                        const second = await ingestChanges(group, target, { writer: admin });
                        assertEquals(second.accepted, 0, 'the replay recognizes the op as already-applied (no re-accept)');
                        assertEquals(second.rejected.length, 0, 'and it is NOT a failure');

                        // The guarantee: exactly one row, same identity, outbox acked.
                        assertEquals((await rdbRows(group, 'tags')).length, before + 1, 'no duplicate row after replay');
                        const after = await target.resolveRow('tags', localId);
                        assertEquals(after!.rowId, rowId, 'the replay reused the reserved uuid (identical rowId)');
                        assertEquals(after!.status, 'active', 'the row is active, not a failure');
                        assertEquals((await target.drainChanges()).changes.length, 0, 'the outbox is now acked');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
        ],
    };
}
