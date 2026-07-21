import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity, KeyId } from "@hyper-hyper-space/hhs3_crypto";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "../../rdb/test/mock_rcontext.js";
import {
    RSchemaImpl, rSchemaFactory, RTableGroupImpl, rTableGroupFactory,
    TableDef, RSchemaView, RTableChanges,
    deriveRowId, deriveTableId,
} from "@hyper-hyper-space/hhs3_rdb";

import { AdapterConfig } from "../src/types.js";
import { rowActionsForDelta, tableRowActions } from "../src/row_actions.js";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

// Same fixture shape as the schema-action tests: two tables exercising precise
// types + constraints. `ref` is a readonly pub column (set at insert), `amount`
// is a scale-2 decimal (canonical string carrier), `memo` is a bounded string.
function baseTables(): TableDef[] {
    return [
        {
            name: 'ledger',
            columns: {
                ref: { type: 'string', pub: true, readonly: true },
                memo: { type: 'string', nullable: true, constraints: { maxLength: 8 } },
                amount: { type: 'decimal', constraints: { scale: 2 } },
            },
            restrictions: [{ on: 'all', rule: { p: 'true' } }],
        },
        {
            name: 'tags',
            columns: {
                code: { type: 'string', pub: true },
            },
            restrictions: [{ on: 'all', rule: { p: 'true' } }],
        },
    ];
}

async function createGroup() {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);

    const admin = await makeIdentity();
    const schemaInit = await RSchemaImpl.create({
        name: 'finance',
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        tables: baseTables(),
    });
    const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
    const pinned = await (await schema.getScopedDag()).getFrontier();

    const groupInit = await RTableGroupImpl.create({
        name: 'finance-prod', seed: 'finance-prod', schemaRef: schema.getId(), schemaVersion: pinned,
    });
    const group = (await ctx.createObject(groupInit)) as RTableGroupImpl;
    return { ctx, schema, group, admin };
}

async function frontier(group: RTableGroupImpl): Promise<Version> {
    return (await group.getScopedDag()).getFrontier();
}

async function endSchemaView(group: RTableGroupImpl, at: Version): Promise<RSchemaView> {
    return (await group.getView(at, at)).getSchemaView();
}

export const rowActionsTests = {
    title: '[ADPTR] rdb_adapter row planner',
    tests: [
        {
            name: '[ADPTR01] insert -> upsert-row with all written values, author, and canonical decimal',
            invoke: async () => {
                const { group, admin } = await createGroup();
                const ledger = await group.getTable('ledger');

                const start = await frontier(group);
                await ledger.insert('l1', { ref: 'R-1', amount: '10.00', memo: 'paid' }, admin);
                const end = await frontier(group);

                const delta = await group.computeDelta(start, end);
                const actions = rowActionsForDelta(delta, await endSchemaView(group, end), group.getId(), {});

                assertEquals(actions.length, 1, 'one row action for one insert');
                const a = actions[0];
                assertTrue(a.kind === 'upsert-row' && a.table === 'ledger', 'insert projects as an upsert into ledger');
                if (a.kind !== 'upsert-row') return;
                assertEquals(a.rowId, deriveRowId('l1', admin.keyId), 'rowId is deriveRowId(uuid, author)');
                assertEquals(a.author, admin.keyId, 'author carried out-of-band');
                assertEquals(a.values['ref'], 'R-1', 'ref value carried');
                assertEquals(a.values['amount'], '10.00', 'canonical scale-2 decimal preserved verbatim');
                assertEquals(a.values['memo'], 'paid', 'memo value carried');
            },
        },
        {
            name: '[ADPTR02] update -> upsert-row carrying only changed columns; author stable from the insert',
            invoke: async () => {
                const { group, admin } = await createGroup();
                const ledger = await group.getTable('ledger');
                await ledger.insert('l1', { ref: 'R-1', amount: '10.00' }, admin);

                const mid = await frontier(group);
                const rowId = deriveRowId('l1', admin.keyId);
                await ledger.update(rowId, { memo: 'note' }, admin);
                const end = await frontier(group);

                const delta = await group.computeDelta(mid, end);
                const actions = rowActionsForDelta(delta, await endSchemaView(group, end), group.getId(), {});

                assertEquals(actions.length, 1, 'one row action for one update');
                const a = actions[0];
                assertTrue(a.kind === 'upsert-row', 'update projects as an upsert');
                if (a.kind !== 'upsert-row') return;
                assertEquals(Object.keys(a.values).length, 1, 'only the changed column is carried');
                assertEquals(a.values['memo'], 'note', 'changed value carried');
                assertEquals(a.author, admin.keyId, 'author is the insert author, stable across the row life');
            },
        },
        {
            name: '[ADPTR03] delete -> delete-row',
            invoke: async () => {
                const { group, admin } = await createGroup();
                const ledger = await group.getTable('ledger');
                await ledger.insert('l1', { ref: 'R-1', amount: '10.00' }, admin);

                const mid = await frontier(group);
                const rowId = deriveRowId('l1', admin.keyId);
                await ledger.delete(rowId, admin);
                const end = await frontier(group);

                const delta = await group.computeDelta(mid, end);
                const actions = rowActionsForDelta(delta, await endSchemaView(group, end), group.getId(), {});

                assertEquals(actions.length, 1, 'one row action for one delete');
                const a = actions[0];
                assertTrue(a.kind === 'delete-row' && a.table === 'ledger' && a.rowId === rowId,
                    'delete projects as a delete-row for the same rowId');
            },
        },
        {
            name: '[ADPTR04] renames: table and column overrides are applied to actions',
            invoke: async () => {
                const { group, admin } = await createGroup();
                const ledger = await group.getTable('ledger');

                const start = await frontier(group);
                await ledger.insert('l1', { ref: 'R-1', amount: '10.00', memo: 'paid' }, admin);
                const end = await frontier(group);

                const config: AdapterConfig = {
                    tableNames: { ledger: 'accounts' },
                    columnNames: { ledger: { memo: 'note' } },
                };
                const delta = await group.computeDelta(start, end);
                const actions = rowActionsForDelta(delta, await endSchemaView(group, end), group.getId(), config);

                assertEquals(actions.length, 1, 'one row action');
                const a = actions[0];
                assertTrue(a.kind === 'upsert-row' && a.table === 'accounts', 'table renamed to accounts');
                if (a.kind !== 'upsert-row') return;
                assertEquals(a.values['note'], 'paid', 'memo renamed to note');
                assertTrue(!('memo' in a.values), 'original column name not emitted');
                assertEquals(a.values['ref'], 'R-1', 'un-renamed column passes through');
            },
        },
        {
            name: '[ADPTR05] pure tableRowActions: void-flip -> delete-row, live -> upsert, rowId order preserved',
            invoke: async () => {
                const { group } = await createGroup();
                const view = await endSchemaView(group, await frontier(group));

                // Hand-built RTableChanges: a voided row (liveAfter false, e.g. an
                // at-use verdict flip) and a newly-live row. rowChanges arrive sorted.
                const changes: RTableChanges = {
                    rowChanges: [
                        { rowId: 'AAAA', liveBefore: true, liveAfter: false, author: undefined, columnChanges: [] },
                        {
                            rowId: 'BBBB', liveBefore: false, liveAfter: true,
                            author: 'k1' as unknown as KeyId,
                            columnChanges: [{ column: 'ref', before: undefined, after: 'R' }],
                        },
                    ],
                };

                const actions = tableRowActions(changes, 'ledger', view, {});
                assertEquals(actions.length, 2, 'one action per row change');
                assertTrue(actions[0].kind === 'delete-row' && actions[0].rowId === 'AAAA',
                    'voided/deleted row (liveAfter false) becomes delete-row, first in rowId order');
                const up = actions[1];
                assertTrue(up.kind === 'upsert-row' && up.rowId === 'BBBB', 'live row becomes upsert-row');
                if (up.kind !== 'upsert-row') return;
                assertEquals(up.author, 'k1' as unknown as KeyId, 'author carried');
                assertEquals(up.values['ref'], 'R', 'written value carried');
            },
        },
        {
            name: '[ADPTR06] dropped table: row changes for a table absent from the end schema are skipped',
            invoke: async () => {
                const { group, admin } = await createGroup();
                const ledger = await group.getTable('ledger');
                const tags = await group.getTable('tags');

                const start = await frontier(group);
                await ledger.insert('l1', { ref: 'R-1', amount: '10.00' }, admin);
                await tags.insert('t1', { code: 'X' });
                const end = await frontier(group);
                const delta = await group.computeDelta(start, end);

                // Sanity: the real end schema projects both tables.
                const both = rowActionsForDelta(delta, await endSchemaView(group, end), group.getId(), {});
                assertEquals(both.length, 2, 'both tables project under the real end schema view');

                // A schema view that no longer lists `tags` (as if it were dropped)
                // must skip its residual row changes; drop-table already removed them.
                const droppedView = { getTableNames: () => ['ledger'] } as unknown as RSchemaView;
                const actions = rowActionsForDelta(delta, droppedView, group.getId(), {});
                assertEquals(actions.length, 1, 'rows for the dropped table are skipped');
                assertTrue(actions[0].table === 'ledger', 'only the surviving table is projected');
            },
        },
        {
            name: '[ADPTR07] empty delta yields no row actions',
            invoke: async () => {
                const { group } = await createGroup();
                const v = await frontier(group);
                const delta = await group.computeDelta(v, v);
                const actions = rowActionsForDelta(delta, await endSchemaView(group, v), group.getId(), {});
                assertEquals(actions.length, 0, 'no writes -> no row actions');
            },
        },
        {
            name: '[ADPTR08] ordering: all upserts precede all deletes across a mixed batch',
            invoke: async () => {
                const { group, admin } = await createGroup();
                const ledger = await group.getTable('ledger');
                const tags = await group.getTable('tags');

                // Pre-existing rows so the window can update one and delete another.
                await ledger.insert('l1', { ref: 'R-1', amount: '10.00' }, admin);
                await ledger.insert('l2', { ref: 'R-2', amount: '20.00' }, admin);
                const start = await frontier(group);

                const l1 = deriveRowId('l1', admin.keyId);
                const l2 = deriveRowId('l2', admin.keyId);
                await ledger.update(l1, { memo: 'u' }, admin);   // upsert
                await ledger.delete(l2, admin);                  // delete
                await tags.insert('t1', { code: 'X' });          // upsert (anonymous)
                const end = await frontier(group);

                const delta = await group.computeDelta(start, end);
                const actions = rowActionsForDelta(delta, await endSchemaView(group, end), group.getId(), {});

                const kinds = actions.map((a) => a.kind);
                const lastUpsert = kinds.lastIndexOf('upsert-row');
                const firstDelete = kinds.indexOf('delete-row');
                assertTrue(firstDelete >= 0 && lastUpsert >= 0, 'batch has both upserts and deletes');
                assertTrue(lastUpsert < firstDelete, 'every upsert is emitted before any delete');
                assertTrue(actions.some((a) => a.kind === 'delete-row' && a.rowId === l2), 'l2 is deleted');
                assertTrue(actions.some((a) => a.kind === 'upsert-row' && a.rowId === l1), 'l1 is upserted');
            },
        },
    ],
};
