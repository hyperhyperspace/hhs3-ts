// Pure-planner unit tests for the inbound inverse planner (ingest.ts). Driven
// with a hand-built resolved-schema stub so FK cases that would need a bound
// foreign group (cross-group) or many rows are cheap and deterministic - no
// crypto, no RTableGroup. End-to-end FK ingestion runs in the conformance suite.

import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { deriveRowId, FKs, RSchemaView, TableDef } from "@hyper-hyper-space/hhs3_rdb";

import { CapturedBatch } from "../src/types.js";
import { changesToEntries, MappingLookup } from "../src/ingest.js";

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
                const plan = changesToEntries(batch, mockView(), noLookup, {}, WRITER, counterUuid());

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
            name: '[ADPTI02] local self-FK: consecutive FK-linked parent+child bundle, parent first, minted rowId resolved',
            invoke: async () => {
                const batch: CapturedBatch = { changes: [
                    { id: 1, kind: 'insert', table: 'comments', localId: 1, values: { body: 'root' } },
                    { id: 2, kind: 'insert', table: 'comments', localId: 2, values: { body: 'child', parent_id: 1 } },
                ] };
                const plan = changesToEntries(batch, mockView(), noLookup, {}, WRITER, counterUuid());

                assertEquals(plan.rejects.length, 0, 'nothing rejected');
                assertEquals(plan.entries.length, 1, 'one bundled entry (child FK-links to the consecutive parent)');
                assertEquals(plan.entries[0].ops.length, 2, 'both writes in the one bundle');

                // Mint order is commit order: root=u1, child=u2.
                const rootRowId = deriveRowId('u1', WRITER);
                const op0 = plan.entries[0].ops[0].write.op;
                const op1 = plan.entries[0].ops[1].write.op;
                assertTrue(op0.action === 'insert' && op0.values.body === 'root', 'parent ordered first in the bundle');
                assertTrue(op1.action === 'insert' && op1.values.body === 'child', 'child ordered second');
                if (op1.action !== 'insert') return;
                assertEquals(op1.values.parent, rootRowId, 'child FK rewritten to the parent minted rowId');
                assertEquals(op1.values.parent_id, undefined, 'the projected id column name is not carried into rdb');
            },
        },
        {
            name: '[ADPTI02b] fkBundling:false emits the FK-linked parent+child as two single-op entries',
            invoke: async () => {
                const batch: CapturedBatch = { changes: [
                    { id: 1, kind: 'insert', table: 'comments', localId: 1, values: { body: 'root' } },
                    { id: 2, kind: 'insert', table: 'comments', localId: 2, values: { body: 'child', parent_id: 1 } },
                ] };
                const plan = changesToEntries(batch, mockView(), noLookup, { fkBundling: false }, WRITER, counterUuid());

                assertEquals(plan.rejects.length, 0, 'nothing rejected');
                assertEquals(plan.entries.length, 2, 'two single-op entries when bundling is off');
                assertEquals(plan.entries[0].ops.length, 1, 'parent is its own entry');
                assertEquals(plan.entries[1].ops.length, 1, 'child is its own entry');

                const rootRowId = deriveRowId('u1', WRITER);
                const op1 = plan.entries[1].ops[0].write.op;
                assertTrue(op1.action === 'insert', 'child insert');
                if (op1.action !== 'insert') return;
                assertEquals(op1.values.parent, rootRowId, 'child FK still resolves to the parent minted rowId');
            },
        },
        {
            name: '[ADPTI03] a dangling local FK (target neither in-batch nor existing) is rejected',
            invoke: async () => {
                const batch: CapturedBatch = { changes: [
                    { id: 1, kind: 'insert', table: 'comments', localId: 1, values: { body: 'orphan', post_id: 99 } },
                ] };
                const plan = changesToEntries(batch, mockView(), noLookup, {}, WRITER, counterUuid());

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
                const plan = changesToEntries(batch, mockView(), lookup, config, WRITER, counterUuid());

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
                const plan = changesToEntries(batch, view, noLookup, {}, WRITER, counterUuid(), keyLookup);

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
            name: '[ADPTI07] updateMerge: consecutive same-row disjoint-field updates fold into one op carrying per-field fallback',
            invoke: async () => {
                const lookup: MappingLookup = (table, localId) =>
                    table === 'posts' && localId === 5 ? { table, localId, rowId: 'POST5', uuid: '' } : undefined;
                const batch: CapturedBatch = { changes: [
                    { id: 1, kind: 'update', table: 'posts', localId: 5, values: { title: 'A' } },
                    { id: 2, kind: 'update', table: 'posts', localId: 5, values: { note: 'B' } },
                ] };
                const plan = changesToEntries(batch, mockView(), lookup, {}, WRITER, counterUuid());

                assertEquals(plan.rejects.length, 0, 'nothing rejected');
                assertEquals(plan.entries.length, 1, 'the two updates merged into one entry');
                assertEquals(plan.entries[0].ops.length, 1, 'one merged op');
                const merged = plan.entries[0].ops[0];
                assertTrue(merged.write.op.action === 'update', 'update op');
                if (merged.write.op.action !== 'update') return;
                assertEquals(merged.write.op.rowId, 'POST5', 'subject resolved via lookup');
                assertEquals(merged.write.op.values.title, 'A', 'first field folded in');
                assertEquals(merged.write.op.values.note, 'B', 'second field folded in');
                // The merged op carries its per-change fallback so the orchestrator
                // can resubmit each field independently if the merged op is rejected.
                assertTrue(merged.fallback !== undefined && merged.fallback.length === 2, 'two per-field fallback ops');
                const fbFields = merged.fallback!.map((f) => f.write.op.action === 'update' ? Object.keys(f.write.op.values).join(',') : '?');
                assertTrue(fbFields.includes('title') && fbFields.includes('note'), 'fallback ops split by field');
            },
        },
        {
            name: '[ADPTI07b] updateMerge off / repeated-field runs stay one op per change (no fold, order preserved)',
            invoke: async () => {
                const lookup: MappingLookup = (table, localId) =>
                    table === 'posts' && localId === 5 ? { table, localId, rowId: 'POST5', uuid: '' } : undefined;

                // updateMerge:false -> two disjoint updates stay two entries.
                const disjoint: CapturedBatch = { changes: [
                    { id: 1, kind: 'update', table: 'posts', localId: 5, values: { title: 'A' } },
                    { id: 2, kind: 'update', table: 'posts', localId: 5, values: { note: 'B' } },
                ] };
                const off = changesToEntries(disjoint, mockView(), lookup, { updateMerge: false }, WRITER, counterUuid());
                assertEquals(off.entries.length, 2, 'updateMerge off keeps two per-change entries');
                assertTrue(off.entries.every((e) => e.ops.length === 1 && e.ops[0].fallback === undefined), 'no fallback when unmerged');

                // Even with updateMerge on, a REPEATED field breaks the run.
                const repeated: CapturedBatch = { changes: [
                    { id: 1, kind: 'update', table: 'posts', localId: 5, values: { title: 'A' } },
                    { id: 2, kind: 'update', table: 'posts', localId: 5, values: { title: 'B' } },
                ] };
                const on = changesToEntries(repeated, mockView(), lookup, {}, WRITER, counterUuid());
                assertEquals(on.entries.length, 2, 'a repeated field is not folded (would lose the intermediate)');
                const first = on.entries[0].ops[0].write.op;
                const second = on.entries[1].ops[0].write.op;
                assertTrue(first.action === 'update' && first.values.title === 'A', 'first write preserved');
                assertTrue(second.action === 'update' && second.values.title === 'B', 'second write preserved, in order');
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
                const plan = changesToEntries(batch, view, noLookup, {}, WRITER, counterUuid());

                assertEquals(plan.entries.length, 0, 'no entry emitted for the unknown key');
                assertEquals(plan.rejects.length, 1, 'one rejection');
                assertTrue(plan.rejects[0].reason.includes('unknown key id'), 'reason identifies the unknown key id');
            },
        },
        {
            name: '[ADPTI14] an insert on a poisoned (ingestion_failure) local id is dropped, not re-minted',
            invoke: async () => {
                // The local id already maps to a poisoned identity (a prior insert
                // rdb rejected). A fresh insert capture on that id is a stale replay:
                // drop it (acked warning), never mint a NEW identity for it.
                const lookup: MappingLookup = (table, localId) =>
                    table === 'comments' && localId === 1
                        ? { table, localId, rowId: 'POISON', uuid: 'u-old', status: 'ingestion_failure' }
                        : undefined;
                const batch: CapturedBatch = { changes: [
                    { id: 1, kind: 'insert', table: 'comments', localId: 1, values: { body: 'stale' } },
                ] };
                const plan = changesToEntries(batch, mockView(), lookup, {}, WRITER, counterUuid());

                assertEquals(plan.entries.length, 0, 'no op emitted for the poisoned id');
                assertEquals(plan.reservations.length, 0, 'no identity reserved');
                assertEquals(plan.rejects.length, 1, 'one acked warning');
                assertTrue(plan.rejects[0].reason.includes('cannot be reused'), 'reason names the un-reusable identity');
                assertEquals(plan.rejects[0].table, undefined, 'a drop carries no table (no op-event / mark)');
                assertEquals(plan.rejects[0].kind, undefined, 'a drop carries no kind');
            },
        },
        {
            name: '[ADPTI15] an insert on a deleted local id is dropped (id never reused)',
            invoke: async () => {
                const lookup: MappingLookup = (table, localId) =>
                    table === 'comments' && localId === 1
                        ? { table, localId, rowId: 'GONE', uuid: 'u-old', status: 'deleted' }
                        : undefined;
                const batch: CapturedBatch = { changes: [
                    { id: 1, kind: 'insert', table: 'comments', localId: 1, values: { body: 'stale' } },
                ] };
                const plan = changesToEntries(batch, mockView(), lookup, {}, WRITER, counterUuid());

                assertEquals(plan.entries.length, 0, 'no op emitted for a deleted id');
                assertEquals(plan.reservations.length, 0, 'no identity reserved');
                assertEquals(plan.rejects.length, 1, 'one acked warning');
            },
        },
        {
            name: '[ADPTI16] an insert on an ACTIVE mapping reuses its uuid (crash-replay read-back), no new mint',
            invoke: async () => {
                const reusedUuid = 'u-existing';
                const lookup: MappingLookup = (table, localId) =>
                    table === 'comments' && localId === 1
                        ? { table, localId, rowId: deriveRowId(reusedUuid, WRITER), uuid: reusedUuid, status: 'active' }
                        : undefined;
                const batch: CapturedBatch = { changes: [
                    { id: 1, kind: 'insert', table: 'comments', localId: 1, values: { body: 'again' } },
                ] };
                const plan = changesToEntries(batch, mockView(), lookup, {}, WRITER, counterUuid());

                assertEquals(plan.rejects.length, 0, 'nothing dropped');
                assertEquals(plan.entries.length, 1, 'one entry');
                const op = plan.entries[0].ops[0].write.op;
                assertTrue(op.action === 'insert', 'insert op');
                if (op.action !== 'insert') return;
                assertEquals(op.rowId, deriveRowId(reusedUuid, WRITER), 'the reserved uuid is reused (identical rowId)');
                assertEquals(op.uuid, reusedUuid, 'no fresh uuid was minted');
                assertEquals(plan.reservations[0].uuid, reusedUuid, 'the reservation carries the reused uuid');
            },
        },
        {
            name: '[ADPTI17] two insert captures for one local id: mint once, drop the duplicate',
            invoke: async () => {
                const batch: CapturedBatch = { changes: [
                    { id: 1, kind: 'insert', table: 'comments', localId: 1, values: { body: 'first' } },
                    { id: 2, kind: 'insert', table: 'comments', localId: 1, values: { body: 'dup' } },
                ] };
                const plan = changesToEntries(batch, mockView(), noLookup, {}, WRITER, counterUuid());

                assertEquals(plan.entries.length, 1, 'exactly one insert lands');
                assertEquals(plan.reservations.length, 1, 'exactly one identity reserved for the id');
                assertEquals(plan.rejects.length, 1, 'the duplicate is dropped');
                assertTrue(plan.rejects[0].reason.includes('duplicate'), 'reason names the duplicate capture');
            },
        },
        {
            name: '[ADPTI18] an update/delete on a poisoned local id is dropped',
            invoke: async () => {
                const lookup: MappingLookup = (table, localId) =>
                    table === 'comments' && localId === 1
                        ? { table, localId, rowId: 'POISON', uuid: 'u-old', status: 'ingestion_failure' }
                        : undefined;
                const upd = changesToEntries(
                    { changes: [{ id: 1, kind: 'update', table: 'comments', localId: 1, values: { body: 'x' } }] },
                    mockView(), lookup, {}, WRITER, counterUuid());
                assertEquals(upd.entries.length, 0, 'no update op on a poisoned id');
                assertEquals(upd.rejects.length, 1, 'the update is dropped');
                assertTrue(upd.rejects[0].reason.includes('previously failed ingestion'), 'reason explains the drop');

                const del = changesToEntries(
                    { changes: [{ id: 1, kind: 'delete', table: 'comments', localId: 1 }] },
                    mockView(), lookup, {}, WRITER, counterUuid());
                assertEquals(del.entries.length, 0, 'no delete op on a poisoned id');
                assertEquals(del.rejects.length, 1, 'the delete is dropped');
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

                const plan = changesToEntries({ changes: [{
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
                assertEquals(plan.entries[0].ops[0].deps.length, 0, 'a NULL foreign key adds no dependency');

                // the readonly check still comes first
                const readonly = changesToEntries(
                    { changes: [{ id: 1, kind: 'update', table: 'notes', localId: 1, values: { fixed: null } }] },
                    view, noRow, {}, WRITER, counterUuid(), noKey);
                assertEquals(readonly.entries.length, 0, 'a NULL for a readonly column lands no op');
                assertTrue(readonly.rejects[0]?.reason.includes('readonly'), 'and is refused as readonly');

                // values and nulls in one update
                const mixed = changesToEntries(
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
                const insert = changesToEntries({ changes: [{
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
                const plain = changesToEntries(
                    { changes: [{ id: 1, kind: 'insert', table: 'notes', localId: 1, values: { title: 'a', memo: null } }] },
                    view, existing, {}, WRITER, counterUuid());
                const plainOp = plain.entries[0].ops[0].write.op;
                assertTrue(plainOp.action === 'insert' && plainOp.nulls === undefined, 'no nulls key when none is kept');

                // an update names only what the app wrote, so every NULL is kept: also for a
                // column rdb does not have, which rdb then refuses as it refuses a value there
                const update = changesToEntries({ changes: [{
                    id: 2, kind: 'update', table: 'notes', localId: 9, values: { memo: null, tag: null, gone: null },
                }] }, view, existing, {}, WRITER, counterUuid());
                const updateOp = update.entries[0].ops[0].write.op;
                assertTrue(updateOp.action === 'update', 'an update op');
                if (updateOp.action !== 'update') return;
                assertEquals(JSON.stringify(updateOp.nulls), JSON.stringify(['gone', 'memo', 'tag']), 'an update keeps every null');
            },
        },
        {
            name: '[ADPTI21] a column the drain could not read is refused before any other check; an update\'s other columns are still planned',
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
                const plan = changesToEntries({ changes: [
                    { id: 1, kind: 'insert', table: 'notes', localId: 1, values: { title: 'a' }, unreadable: { data: reason } },
                    { id: 2, kind: 'update', table: 'notes', localId: 9, values: { note: 'n' }, unreadable: { data: reason } },
                ] }, view, existing, {}, WRITER, counterUuid());

                const insertReject = plan.rejects.find((r) => r.kind === 'insert');
                assertTrue(insertReject !== undefined, 'the insert is refused');
                assertEquals(insertReject!.column, 'data', 'at the column the drain could not read');
                assertTrue(insertReject!.reason.includes(reason), `with the drain's reason, got: ${insertReject!.reason}`);
                assertEquals((insertReject!.sources ?? []).map((s) => s.id).join(','), '1', 'and its own captured change');

                const updateRejects = plan.rejects.filter((r) => r.kind === 'update');
                assertEquals(updateRejects.length, 1, "the update's unreadable column is refused");
                assertEquals(updateRejects[0].column, 'data', 'at that column');

                const ops = plan.entries.flatMap((e) => e.ops);
                assertEquals(ops.length, 1, 'one op is planned');
                const op = ops[0].write.op;
                assertTrue(op.action === 'update' && JSON.stringify(op.values) === JSON.stringify({ note: 'n' }),
                    "the update's other column is planned, and no op carries the unreadable column");
            },
        },
    ],
};
