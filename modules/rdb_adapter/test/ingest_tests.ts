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
    ],
};
