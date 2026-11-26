// Pure-planner unit tests for the inbound inverse planner (ingest.ts). Driven
// with a hand-built resolved-schema stub so FK cases that would need a bound
// foreign group (cross-group) or many rows are cheap and deterministic - no
// crypto, no RTableGroup. End-to-end FK ingestion runs in the conformance suite.

import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { deriveRowId, FKs, RSchemaView, TableDef } from "@hyper-hyper-space/hhs3_rdb";

import { CapturedBatch } from "../src/types.js";
import { changesToEntries, IngestPlan, KeyLookup, MappingLookup, NodeContext, planDatabaseEntries, SubjectRow } from "../src/ingest.js";
import type { AdapterConfig } from "../src/types.js";

const WRITER = 'writer-key-id';

// A minimal resolved-view stub: `comments` has a self FK (parent), a cross-table
// FK (post -> posts), and a cross-group FK (origin -> archive.entries).
function mockView(): RSchemaView {
    const tables: { [name: string]: { def: TableDef; fks: FKs } } = {
        posts: { def: { name: 'posts', columns: { title: { type: 'string' }, note: { type: 'string', nullable: true } } }, fks: {} },
        comments: {
            def: { name: 'comments', columns: {
                body: { type: 'string' },
                post: { type: 'string', nullable: true },
                parent: { type: 'string', nullable: true },
                origin: { type: 'string', nullable: true },
            } },
            fks: { post: 'posts', parent: 'comments', origin: 'archive.entries' },
        },
    };
    return {
        getTableNames: () => Object.keys(tables),
        hasTable: (n: string) => tables[n] !== undefined,
        getTable: (n: string) => tables[n]?.def,
        getFKs: (n: string) => tables[n]?.fks ?? {},
    } as unknown as RSchemaView;
}

const noLookup: MappingLookup = () => undefined;

// The writer key's id in the target's key table. Capture fills it into an
// insert's empty author column, from the writer record the projection keeps
// in the target (HIP-29), so a captured insert of a signed group carries it.
// planCaptured plans a batch as capture records it.
const WRITER_KEY_ID = 1;
function planCaptured(
    batch: CapturedBatch, view: RSchemaView, lookup: MappingLookup, config: AdapterConfig,
    writer: string | undefined, newUuid: () => string, keyLookup: KeyLookup = () => undefined,
): IngestPlan {
    const author = config.authorColumn === false ? undefined : (config.authorColumn ?? 'author_key_id');
    const changes = batch.changes.map((c) => c.kind !== 'insert' || writer === undefined || author === undefined
        || (c.values[author] ?? null) !== null ? c : { ...c, values: { ...c.values, [author]: WRITER_KEY_ID } });
    const keys: KeyLookup = (id) => id === WRITER_KEY_ID && writer !== undefined ? { keyHash: writer } : keyLookup(id);
    return changesToEntries({ changes }, view, lookup, config, writer, newUuid, keys);
}

// Deterministic uuid minter: u1, u2, ... in mint (commit) order.
function counterUuid(): () => string {
    let n = 0;
    return () => 'u' + (++n);
}

export const ingestTests = {
    title: '[ADPTI] rdb_adapter inverse planner',
    tests: [
        {
            name: '[ADPTI01] cross-group FK value passes through as the foreign rowId (row_hash)',
            invoke: async () => {
                const batch: CapturedBatch = { changes: [
                    { id: 1, kind: 'insert', table: 'comments', localId: 1, values: { body: 'x', origin_row_hash: 'HASH123' } },
                ] };
                const plan = planCaptured(batch, mockView(), noLookup, {}, WRITER, counterUuid());

                assertEquals(plan.rejects.length, 0, 'nothing rejected');
                assertEquals(plan.entries.length, 1, 'one entry');
                const op = plan.entries[0].ops[0].write.op;
                assertTrue(op.action === 'insert', 'insert op');
                if (op.action !== 'insert') return;
                assertEquals(op.values.origin, 'HASH123', 'cross-group FK reverse-maps to origin, value passed through');
                assertEquals(op.values.origin_row_hash, undefined, 'the projected column name is not carried into rdb');
            },
        },
        {
            name: '[ADPTI02] local self-FK: a parent and a child inserted in one batch are two entries, parent first, and the child resolves the parent\'s minted rowId',
            invoke: async () => {
                const batch: CapturedBatch = { changes: [
                    { id: 1, kind: 'insert', table: 'comments', localId: 1, values: { body: 'root' } },
                    { id: 2, kind: 'insert', table: 'comments', localId: 2, values: { body: 'child', parent_id: 1 } },
                ] };
                const plan = planCaptured(batch, mockView(), noLookup, {}, WRITER, counterUuid());

                assertEquals(plan.rejects.length, 0, 'nothing rejected');
                assertEquals(plan.entries.map((e) => e.ops.length).join(','), '1,1', 'one entry per change (HIP-29)');

                // Mint order is commit order: root=u1, child=u2.
                const rootRowId = deriveRowId('u1', WRITER);
                const op0 = plan.entries[0].ops[0].write.op;
                const op1 = plan.entries[1].ops[0].write.op;
                assertTrue(op0.action === 'insert' && op0.values.body === 'root', 'the parent comes first');
                assertTrue(op1.action === 'insert' && op1.values.body === 'child', 'the child second');
                if (op1.action !== 'insert') return;
                assertEquals(op1.values.parent, rootRowId, 'child FK rewritten to the parent minted rowId');
                assertEquals(op1.values.parent_id, undefined, 'the projected id column name is not carried into rdb');
            },
        },
        {
            name: '[ADPTI03] a dangling local FK (target neither in-batch nor existing) is rejected',
            invoke: async () => {
                const batch: CapturedBatch = { changes: [
                    { id: 1, kind: 'insert', table: 'comments', localId: 1, values: { body: 'orphan', post_id: 99 } },
                ] };
                const plan = planCaptured(batch, mockView(), noLookup, {}, WRITER, counterUuid());

                assertEquals(plan.entries.length, 0, 'no entry emitted for the dangling row');
                assertEquals(plan.rejects.length, 1, 'one rejection');
                assertTrue(plan.rejects[0].reason.includes('dangling'), 'reason identifies the dangling FK');
            },
        },
        {
            name: '[ADPTI04] a co-projected cross-group FK id reverse-resolves to the foreign rowId via lookup',
            invoke: async () => {
                // 'origin' is a cross-group FK ('archive.entries'); with a crossGroup
                // resolver it projects as an integer 'origin_id' whose value is the
                // foreign table's serial id. The lookup resolves that to a rowId.
                const config = { crossGroup: (ref: string) => ref === 'archive.entries' ? 'archive_entries' : undefined };
                const lookup: MappingLookup = (table, localId) =>
                    table === 'archive_entries' && localId === 7
                        ? { table, localId, rowId: 'FOREIGN_ROWID', uuid: '' }
                        : undefined;

                const batch: CapturedBatch = { changes: [
                    { id: 1, kind: 'insert', table: 'comments', localId: 1, values: { body: 'x', origin_id: 7 } },
                ] };
                const plan = planCaptured(batch, mockView(), lookup, config, WRITER, counterUuid());

                assertEquals(plan.rejects.length, 0, 'nothing rejected');
                assertEquals(plan.entries.length, 1, 'one entry');
                const op = plan.entries[0].ops[0].write.op;
                assertTrue(op.action === 'insert', 'insert op');
                if (op.action !== 'insert') return;
                assertEquals(op.values.origin, 'FOREIGN_ROWID', 'cross-group id reverse-resolved to the foreign rowId');
                assertEquals(op.values.origin_id, undefined, 'the projected id column name is not carried into rdb');
            },
        },
        {
            name: '[ADPTI05] identity <col>_key_id reverse-maps to the rdb column with the key hash',
            invoke: async () => {
                const view = {
                    getTableNames: () => ['caps'],
                    hasTable: (n: string) => n === 'caps',
                    getTable: (n: string) => n === 'caps'
                        ? { name: 'caps', columns: {
                            label: { type: 'string' },
                            grantee: { type: 'identity' },
                        } }
                        : undefined,
                    getFKs: () => ({}),
                } as unknown as RSchemaView;
                const keyLookup = (keyId: number) =>
                    keyId === 7 ? { keyHash: 'KH' } : undefined;

                const batch: CapturedBatch = { changes: [
                    { id: 1, kind: 'insert', table: 'caps', localId: 1, values: { label: 'manager', grantee_key_id: 7 } },
                ] };
                const plan = planCaptured(batch, view, noLookup, {}, WRITER, counterUuid(), keyLookup);

                assertEquals(plan.rejects.length, 0, 'nothing rejected');
                assertEquals(plan.entries.length, 1, 'one entry');
                const op = plan.entries[0].ops[0].write.op;
                assertTrue(op.action === 'insert', 'insert op');
                if (op.action !== 'insert') return;
                assertEquals(op.values.grantee, 'KH', 'identity key-ref reverse-maps to the key hash');
                assertEquals(op.values.grantee_key_id, undefined, 'the projected key-ref name is not carried into rdb');
            },
        },
        {
            name: '[ADPTI06] unknown identity key id is rejected',
            invoke: async () => {
                const view = {
                    getTableNames: () => ['caps'],
                    hasTable: (n: string) => n === 'caps',
                    getTable: (n: string) => n === 'caps'
                        ? { name: 'caps', columns: { grantee: { type: 'identity' } } }
                        : undefined,
                    getFKs: () => ({}),
                } as unknown as RSchemaView;

                const batch: CapturedBatch = { changes: [
                    { id: 1, kind: 'insert', table: 'caps', localId: 1, values: { grantee_key_id: 99 } },
                ] };
                const plan = planCaptured(batch, view, noLookup, {}, WRITER, counterUuid());

                assertEquals(plan.entries.length, 0, 'no entry emitted for the unknown key');
                assertEquals(plan.rejects.length, 1, 'one rejection');
                assertTrue(plan.rejects[0].reason.includes('unknown key id'), 'reason identifies the unknown key id');
            },
        },
        {
            name: '[ADPTI14] an insert on a local id with an ingestion_failure identity is refused, naming that identity, and nothing is minted',
            invoke: async () => {
                const lookup: MappingLookup = (table, localId) =>
                    table === 'comments' && localId === 1
                        ? { table, localId, rowId: 'POISON', uuid: 'u-old', status: 'ingestion_failure' }
                        : undefined;
                const batch: CapturedBatch = { changes: [
                    { id: 1, kind: 'insert', table: 'comments', localId: 1, values: { body: 'stale' } },
                ] };
                const plan = planCaptured(batch, mockView(), lookup, {}, WRITER, counterUuid());

                assertEquals(plan.entries.length, 0, 'no op emitted for the id');
                assertEquals(plan.rejects.length, 1, 'one refusal');
                const r = plan.rejects[0];
                assertTrue(r.reason.includes('already has an rdb identity'), 'the reason names the identity');
                assertEquals(`${r.table}#${r.localId} ${r.kind} ${r.rowId}`, 'comments#1 insert POISON', 'a refusal with a kind, naming the identity\'s row');
            },
        },
        {
            name: '[ADPTI15] an insert on a deleted local id is refused, naming the deleted identity',
            invoke: async () => {
                const lookup: MappingLookup = (table, localId) =>
                    table === 'comments' && localId === 1
                        ? { table, localId, rowId: 'GONE', uuid: 'u-old', status: 'deleted' }
                        : undefined;
                const batch: CapturedBatch = { changes: [
                    { id: 1, kind: 'insert', table: 'comments', localId: 1, values: { body: 'stale' } },
                ] };
                const plan = planCaptured(batch, mockView(), lookup, {}, WRITER, counterUuid());

                assertEquals(plan.entries.length, 0, 'no op emitted for a deleted id');
                assertEquals(plan.rejects.length, 1, 'one refusal');
                assertEquals(`${plan.rejects[0].kind} ${plan.rejects[0].rowId}`, 'insert GONE', 'with a kind and the identity\'s row');
            },
        },
        {
            name: '[ADPTI16] an insert on an active mapping is refused, naming its identity, not planned again with its uuid',
            invoke: async () => {
                const reusedUuid = 'u-existing';
                const rowId = deriveRowId(reusedUuid, WRITER);
                const lookup: MappingLookup = (table, localId) =>
                    table === 'comments' && localId === 1
                        ? { table, localId, rowId, uuid: reusedUuid, status: 'active' }
                        : undefined;
                const batch: CapturedBatch = { changes: [
                    { id: 1, kind: 'insert', table: 'comments', localId: 1, values: { body: 'again' } },
                ] };
                const plan = planCaptured(batch, mockView(), lookup, {}, WRITER, counterUuid());

                assertEquals(plan.entries.length, 0, 'no op');
                assertEquals(plan.rejects.length, 1, 'one refusal');
                assertEquals(`${plan.rejects[0].kind} ${plan.rejects[0].rowId}`, `insert ${rowId}`, 'naming the active identity, whose row the revert reads');
            },
        },
        {
            name: '[ADPTI17] two insert captures for one local id: the first is planned, the second refused on the identity the first gave it',
            invoke: async () => {
                const batch: CapturedBatch = { changes: [
                    { id: 1, kind: 'insert', table: 'comments', localId: 1, values: { body: 'first' } },
                    { id: 2, kind: 'insert', table: 'comments', localId: 1, values: { body: 'dup' } },
                ] };
                const plan = planCaptured(batch, mockView(), noLookup, {}, WRITER, counterUuid());

                assertEquals(plan.entries.length, 1, 'exactly one insert lands');
                assertEquals(plan.rejects.length, 1, 'the second is refused');
                const first = plan.entries[0].ops[0].write.op;
                assertEquals(`${plan.rejects[0].kind} ${plan.rejects[0].rowId}`, `insert ${first.rowId}`, 'on the first one\'s identity');
            },
        },
        {
            name: '[ADPTI18] an update or a delete of a row whose insert was refused is refused, also in the batch that refused it',
            invoke: async () => {
                const lookup: MappingLookup = (table, localId) =>
                    table === 'comments' && localId === 1
                        ? { table, localId, rowId: 'POISON', uuid: 'u-old', status: 'ingestion_failure' }
                        : undefined;
                const upd = planCaptured(
                    { changes: [{ id: 1, kind: 'update', table: 'comments', localId: 1, values: { body: 'x' } }] },
                    mockView(), lookup, {}, WRITER, counterUuid());
                assertEquals(upd.entries.length, 0, 'no update op on the id');
                assertEquals(upd.rejects.length, 1, 'the update is refused');
                assertTrue(upd.rejects[0].reason.includes('insert was refused'), 'the reason explains it');
                assertEquals(`${upd.rejects[0].kind} ${upd.rejects[0].rowId}`, 'update POISON', 'with a kind and the row');

                const del = planCaptured(
                    { changes: [{ id: 1, kind: 'delete', table: 'comments', localId: 1 }] },
                    mockView(), lookup, {}, WRITER, counterUuid());
                assertEquals(del.entries.length, 0, 'no delete op on the id');
                assertEquals(del.rejects.length, 1, 'the delete is refused');

                // The insert refused in the same batch gives its id the identity.
                const batch = planCaptured({ changes: [
                    { id: 1, kind: 'insert', table: 'comments', localId: 3, values: { body: 'orphan', post_id: 99 } },
                    { id: 2, kind: 'update', table: 'comments', localId: 3, values: { body: 'y' } },
                ] }, mockView(), noLookup, {}, WRITER, counterUuid());
                assertEquals(batch.rejects.map((r) => r.kind).join(','), 'insert,update', 'both are refused');
                assertEquals(batch.rejects[1].rowId, batch.rejects[0].rowId, 'the update names the refused insert\'s row');
            },
        },
        {
            name: '[ADPTI19] a captured NULL maps to a null of the rdb column with no lookup, for a plain, a foreign key and a key column',
            invoke: async () => {
                // lookups that fail the test if a NULL ever reaches them
                const noRow: MappingLookup = (table, localId) => {
                    if (table === 'notes' && localId === 1) return { table, localId, rowId: 'ROW1', uuid: 'u-row', status: 'active' };
                    throw new Error(`unexpected row lookup ${table}#${String(localId)}`);
                };
                const noKey = (keyId: number): undefined => { throw new Error(`unexpected key lookup ${String(keyId)}`); };
                const view = {
                    getTableNames: () => ['notes', 'topics'],
                    hasTable: (n: string) => n === 'notes' || n === 'topics',
                    getTable: (n: string) => n === 'notes'
                        ? { name: 'notes', columns: {
                            title: { type: 'string' },
                            memo: { type: 'string', nullable: true },
                            topic: { type: 'string', nullable: true },
                            origin: { type: 'string', nullable: true },
                            owner: { type: 'identity', nullable: true },
                            fixed: { type: 'string', nullable: true, readonly: true },
                        } }
                        : n === 'topics' ? { name: 'topics', columns: { name: { type: 'string' } } } : undefined,
                    getFKs: (n: string) => n === 'notes' ? { topic: 'topics', origin: 'archive.entries' } : {},
                } as unknown as RSchemaView;

                const plan = planCaptured({ changes: [{
                    id: 1, kind: 'update', table: 'notes', localId: 1,
                    values: { memo: null, topic_id: null, origin_row_hash: null, owner_key_id: null },
                }] }, view, noRow, {}, WRITER, counterUuid(), noKey);
                assertEquals(plan.rejects.length, 0, 'nothing rejected');
                assertEquals(plan.entries.length, 1, 'an update of only nulls is an op, not dropped as empty');
                const op = plan.entries[0].ops[0].write.op;
                assertTrue(op.action === 'update', 'an update op');
                if (op.action !== 'update') return;
                assertEquals(JSON.stringify(op.values), '{}', 'no values');
                assertEquals(JSON.stringify(op.nulls), JSON.stringify(['memo', 'origin', 'owner', 'topic']),
                    'each null is carried under its rdb column, sorted');

                // the readonly check still comes first
                const readonly = planCaptured(
                    { changes: [{ id: 1, kind: 'update', table: 'notes', localId: 1, values: { fixed: null } }] },
                    view, noRow, {}, WRITER, counterUuid(), noKey);
                assertEquals(readonly.entries.length, 0, 'a NULL for a readonly column lands no op');
                assertTrue(readonly.rejects[0]?.reason.includes('readonly'), 'and is refused as readonly');

                // values and nulls in one update
                const mixed = planCaptured(
                    { changes: [{ id: 1, kind: 'update', table: 'notes', localId: 1, values: { title: 'b', memo: null } }] },
                    view, noRow, {}, WRITER, counterUuid(), noKey);
                const mixedOp = mixed.entries[0].ops[0].write.op;
                assertTrue(mixedOp.action === 'update' && mixedOp.values.title === 'b' && JSON.stringify(mixedOp.nulls) === '["memo"]',
                    'a value and a null travel in one op');
            },
        },
        {
            name: '[ADPTI20] an insert keeps a NULL only where the column has a default; an update keeps every NULL',
            invoke: async () => {
                const view = {
                    getTableNames: () => ['notes'],
                    hasTable: (n: string) => n === 'notes',
                    getTable: (n: string) => n === 'notes'
                        ? { name: 'notes', columns: {
                            title: { type: 'string' },
                            memo: { type: 'string', nullable: true },
                            tag: { type: 'string', nullable: true, default: 'none' },
                        } }
                        : undefined,
                    getFKs: () => ({}),
                } as unknown as RSchemaView;
                const existing: MappingLookup = (table, localId) =>
                    localId === 9 ? { table, localId, rowId: 'ROW9', uuid: 'u-row', status: 'active' } : undefined;

                // an insert as SQLite captures it: every empty column named with NULL,
                // one of them a column rdb's schema no longer has
                const insert = planCaptured({ changes: [{
                    id: 1, kind: 'insert', table: 'notes', localId: 1,
                    values: { title: 'a', memo: null, tag: null, gone: null },
                }] }, view, existing, {}, WRITER, counterUuid());
                assertEquals(insert.rejects.length, 0, 'nothing rejected');
                const insertOp = insert.entries[0].ops[0].write.op;
                assertTrue(insertOp.action === 'insert', 'an insert op');
                if (insertOp.action !== 'insert') return;
                assertEquals(JSON.stringify(insertOp.values), JSON.stringify({ title: 'a' }), 'the value is kept');
                assertEquals(JSON.stringify(insertOp.nulls), JSON.stringify(['tag']),
                    'only the NULL of the column with a default is written: not the one without, not the one rdb does not have');

                // an insert with no NULL of that kind has no nulls at all
                const plain = planCaptured(
                    { changes: [{ id: 1, kind: 'insert', table: 'notes', localId: 1, values: { title: 'a', memo: null } }] },
                    view, existing, {}, WRITER, counterUuid());
                const plainOp = plain.entries[0].ops[0].write.op;
                assertTrue(plainOp.action === 'insert' && plainOp.nulls === undefined, 'no nulls key when none is kept');

                // an update names only what the app wrote, so every NULL is kept: also for a
                // column rdb does not have, which rdb then refuses as it refuses a value there
                const update = planCaptured({ changes: [{
                    id: 2, kind: 'update', table: 'notes', localId: 9, values: { memo: null, tag: null, gone: null },
                }] }, view, existing, {}, WRITER, counterUuid());
                const updateOp = update.entries[0].ops[0].write.op;
                assertTrue(updateOp.action === 'update', 'an update op');
                if (updateOp.action !== 'update') return;
                assertEquals(JSON.stringify(updateOp.nulls), JSON.stringify(['gone', 'memo', 'tag']), 'an update keeps every null');
            },
        },
        {
            name: '[ADPTI21] a column the drain could not read is refused before any other check, and an update with one is refused whole',
            invoke: async () => {
                const view = {
                    getTableNames: () => ['notes'],
                    hasTable: (n: string) => n === 'notes',
                    getTable: (n: string) => n === 'notes'
                        ? { name: 'notes', columns: {
                            title: { type: 'string' },
                            note: { type: 'string', nullable: true },
                            data: { type: 'json', nullable: true },
                        } }
                        : undefined,
                    getFKs: () => ({}),
                } as unknown as RSchemaView;
                const existing: MappingLookup = (table, localId) =>
                    localId === 9 ? { table, localId, rowId: 'ROW9', uuid: 'u-row', status: 'active' } : undefined;
                const reason = "its text is not json: \"nope\"";
                const plan = planCaptured({ changes: [
                    { id: 1, kind: 'insert', table: 'notes', localId: 1, values: { title: 'a' }, unreadable: { data: reason } },
                    { id: 2, kind: 'update', table: 'notes', localId: 9, values: { note: 'n' }, unreadable: { data: reason } },
                ] }, view, existing, {}, WRITER, counterUuid());

                const insertReject = plan.rejects.find((r) => r.kind === 'insert');
                assertTrue(insertReject !== undefined, 'the insert is refused');
                assertEquals(insertReject!.column, 'data', 'at the column the drain could not read');
                assertTrue(insertReject!.reason.includes(reason), `with the drain's reason, got: ${insertReject!.reason}`);
                assertEquals((insertReject!.sources ?? []).map((s) => s.id).join(','), '1', 'and its own captured change');

                const updateRejects = plan.rejects.filter((r) => r.kind === 'update');
                assertEquals(updateRejects.length, 1, "the update is refused, once");
                assertEquals(updateRejects[0].column, 'data', 'at the unreadable column');
                assertEquals(plan.entries.length, 0, 'and no op is planned: an update lands or is refused whole (HIP-29)');
            },
        },
        {
            name: "[ADPTI22] an update's readonly and author columns at rdb's values for the row are set aside, other values are refused; an insert's empty author is refused in a signed group, and is anonymous under manual",
            invoke: async () => {
                const view = {
                    getTableNames: () => ['notes'],
                    hasTable: (n: string) => n === 'notes',
                    getTable: (n: string) => n === 'notes'
                        ? { name: 'notes', columns: {
                            title: { type: 'string' },
                            code: { type: 'string', readonly: true },
                        } }
                        : undefined,
                    getFKs: () => ({}),
                } as unknown as RSchemaView;
                const existing: MappingLookup = (table, localId) =>
                    localId === 9 ? { table, localId, rowId: 'ROW9', uuid: 'u-row', author: WRITER, status: 'active' } : undefined;
                const subject: SubjectRow = { values: { title: 't', code: 'C1' }, author: WRITER };
                const keys = new Map([[1, { keyHash: WRITER }], [2, { keyHash: 'other-key' }]]);
                const plan = (values: { [c: string]: number | string | null }, kind: 'insert' | 'update', extra: Partial<NodeContext> = {}) => {
                    const ctx: NodeContext = { groupId: 'g', schemaView: view, config: {}, writerKeyId: WRITER,
                        subjectRow: (_t, rowId) => rowId === 'ROW9' ? subject : undefined, ...extra };
                    return planDatabaseEntries({ changes: [{ id: 1, kind, table: 'notes', localId: kind === 'update' ? 9 : 1, values }] },
                        () => ctx, existing, (k) => keys.get(k), counterUuid());
                };
                const opValues = (p: ReturnType<typeof plan>): string =>
                    p.entries.flatMap((e) => e.ops).map((o) => JSON.stringify((o.write.op as { values?: unknown }).values ?? {})).join(';');

                let p = plan({ title: 'new', code: 'C1', author_key_id: 1 }, 'update');
                assertEquals(p.rejects.length, 0, 'the row\'s own values are no refusal');
                assertEquals(opValues(p), '{"title":"new"}', 'and are set aside');

                p = plan({ code: 'C1', author_key_id: 1 }, 'update');
                assertEquals(p.rejects.length + p.entries.length, 0, 'an update with nothing left to write gives no op');

                p = plan({ title: 'new', code: 'C2' }, 'update');
                assertEquals(p.rejects.map((r) => r.column).join(','), 'code', 'another readonly value is refused');
                p = plan({ author_key_id: 2 }, 'update');
                assertEquals(p.rejects.map((r) => r.column).join(','), 'author_key_id', 'another author is refused');
                p = plan({ author_key_id: 77 }, 'update');
                assertEquals(p.rejects.map((r) => r.column).join(','), 'author_key_id', 'an id the key table does not know is refused');
                p = plan({ author_key_id: null }, 'update');
                assertEquals(p.rejects.map((r) => r.column).join(','), 'author_key_id', 'a NULL over a signed row is refused');

                p = plan({ title: 'x', code: 'K' }, 'insert');
                assertEquals(p.rejects.map((r) => r.column).join(','), 'author_key_id', 'an empty author in a signed group is refused: capture fills it');
                p = plan({ title: 'x', code: 'K', author_key_id: 1 }, 'insert');
                assertEquals(p.rejects.length, 0, 'one naming the writer key is not');
                assertEquals(p.entries[0].ops[0].mapping?.author, WRITER, 'and is signed with it');

                p = plan({ title: 'x', code: 'K' }, 'insert', { manual: true });
                const anon = p.entries[0].ops[0];
                assertTrue(anon.anonymous === true && anon.mapping?.author === undefined, 'under manual an empty author is anonymous');
                assertEquals(anon.mapping?.rowId, deriveRowId('u1'), 'its row hash comes from its uuid alone');
                p = plan({ title: 'x', code: 'K', author_key_id: 1 }, 'insert', { manual: true });
                const signed = p.entries[0].ops[0];
                assertTrue(signed.anonymous === undefined && signed.mapping?.author === WRITER, 'and one naming the writer key is signed');
                assertEquals(signed.mapping?.rowId, deriveRowId('u1', WRITER), 'with a row hash from its signer');
            },
        },
    ],
};
