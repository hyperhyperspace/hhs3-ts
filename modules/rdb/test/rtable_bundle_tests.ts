import { assertTrue, assertFalse, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { version } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import { RSchemaImpl, rSchemaFactory } from "../src/rschema/rschema.js";
import { RTableGroupImpl, rTableGroupFactory } from "../src/rtable_group/group.js";
import { deriveRowId } from "../src/rtable/hash.js";
import type { TableDef } from "../src/rschema/payload.js";
import type { InsertRowPayload, DeleteRowPayload, UpdateRowPayload } from "../src/rtable/payload.js";
import type { BundleWrite } from "../src/rtable_group/interfaces.js";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

// orders (no FK) and lines (FK line.order -> orders). Both permit unauthored
// updates/deletes so the bundle mechanics, not restrictions, are under test.
function ordersTable(concurrentDeletes = false): TableDef {
    return {
        name: 'orders',
        columns: {
            customer: { type: 'string', pub: true },
            total: { type: 'float', default: 0 },
        },
        concurrentDeletes,
        restrictions: [{ on: 'all', rule: { p: 'true' } }],
    };
}

function linesTable(): TableDef {
    return {
        name: 'lines',
        columns: {
            order: { type: 'string' },
            qty: { type: 'integer' },
        },
        fks: { order: 'orders' },
        restrictions: [{ on: 'all', rule: { p: 'true' } }],
    };
}

function insertOp(uuid: string, values: json.LiteralMap, author?: string): InsertRowPayload {
    const op: InsertRowPayload = { action: 'insert', rowId: deriveRowId(uuid, author), uuid, values };
    if (author !== undefined) op.author = author;
    return op;
}

function deleteOp(rowId: string): DeleteRowPayload {
    return { action: 'delete', rowId };
}

function updateOp(rowId: string, values: json.LiteralMap): UpdateRowPayload {
    return { action: 'update', rowId, values };
}

async function createTestEnv(opts?: { ordersConcurrentDeletes?: boolean; selfValidate?: boolean }) {
    const ctx = createMockRContext({ selfValidate: opts?.selfValidate ?? true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);

    const admin = await makeIdentity();
    const schemaInit = await RSchemaImpl.create({
        seed: 'bundle-test-schema',
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        tables: [ordersTable(opts?.ordersConcurrentDeletes), linesTable()],
    });
    const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
    const pinned = await (await schema.getScopedDag()).getFrontier();

    const groupInit = await RTableGroupImpl.create({
        seed: 'bundle-test-group',
        schemaRef: schema.getId(),
        schemaVersion: pinned,
    });
    const group = (await ctx.createObject(groupInit)) as RTableGroupImpl;

    return { ctx, schema, group, admin };
}

async function expectBundleFailure(group: RTableGroupImpl, writes: BundleWrite[], why: string) {
    let failed = false;
    try {
        await group.bundle(writes);
    } catch {
        failed = true;
    }
    assertTrue(failed, why);
}

export const rtableBundleTests = {
    title: '[BUNDLE] RTableGroup bundle (atomic multi-table write) tests',
    tests: [
        {
            name: '[BUNDLE01] Atomic multi-table write: all ops visible at one position, none before',
            invoke: async () => {
                const { group } = await createTestEnv();
                const scopedDag = await group.getScopedDag();
                const before = await scopedDag.getFrontier();

                const orderId = deriveRowId('o-1');
                const lineId = deriveRowId('l-1');

                await group.bundle([
                    { table: 'orders', op: insertOp('o-1', { customer: 'ada', total: 10 }) },
                    { table: 'lines', op: insertOp('l-1', { order: orderId, qty: 3 }) },
                ]);

                const orders = await group.getTable('orders');
                const lines = await group.getTable('lines');

                assertTrue(await (await orders.getView()).hasRow(orderId), 'the bundled order should be live');
                assertTrue(await (await lines.getView()).hasRow(lineId), 'the bundled line should be live');

                // neither op is visible at the pre-bundle position (atomicity)
                assertFalse(await (await orders.getView(before, before)).hasRow(orderId),
                    'the order must not be visible before the bundle');
                assertFalse(await (await lines.getView(before, before)).hasRow(lineId),
                    'the line must not be visible before the bundle');
            }
        },
        {
            name: '[BUNDLE02] Bundle rejected when any op is invalid (all-or-nothing at write)',
            invoke: async () => {
                const { group } = await createTestEnv();
                const orderId = deriveRowId('o-1');

                // the order is fine, but the line carries a type-mismatched qty
                await expectBundleFailure(group, [
                    { table: 'orders', op: insertOp('o-1', { customer: 'ada', total: 1 }) },
                    { table: 'lines', op: insertOp('l-1', { order: orderId, qty: 'lots' }) },
                ], 'a bundle with one schema-invalid op should be rejected');

                // and nothing landed: the valid order is not visible either
                const orders = await group.getTable('orders');
                assertFalse(await (await orders.getView()).hasRow(orderId),
                    'a rejected bundle must not land its valid ops');
            }
        },
        {
            name: '[BUNDLE02b] Public bundle API rejects invalid ops even without selfValidate',
            invoke: async () => {
                const { group } = await createTestEnv({ selfValidate: false });
                const orderId = deriveRowId('o-1');

                await expectBundleFailure(group, [
                    { table: 'orders', op: insertOp('o-1', { customer: 'ada', total: 1 }) },
                    { table: 'lines', op: insertOp('l-1', { order: orderId, qty: 'lots' }) },
                ], 'the public bundle API should reject invalid ops even when context selfValidate is off');

                const orders = await group.getTable('orders');
                assertFalse(await (await orders.getView()).hasRow(orderId),
                    'a rejected bundle must not land any sibling ops');
            }
        },
        {
            name: '[BUNDLE03] Intra-bundle FK satisfied by an earlier sibling insert; a later one is rejected',
            invoke: async () => {
                const { group } = await createTestEnv();

                // order BEFORE line: the FK cut for the line includes the order
                const okOrderId = deriveRowId('o-ok');
                const okLineId = deriveRowId('l-ok');
                await group.bundle([
                    { table: 'orders', op: insertOp('o-ok', { customer: 'ada', total: 1 }) },
                    { table: 'lines', op: insertOp('l-ok', { order: okOrderId, qty: 1 }) },
                ]);
                const lines = await group.getTable('lines');
                assertTrue(await (await lines.getView()).hasRow(okLineId),
                    'a line referencing an earlier bundled order should be live');

                // line BEFORE order: the FK target is not yet in the cut
                const lateOrderId = deriveRowId('o-late');
                await expectBundleFailure(group, [
                    { table: 'lines', op: insertOp('l-late', { order: lateOrderId, qty: 1 }) },
                    { table: 'orders', op: insertOp('o-late', { customer: 'bob', total: 2 }) },
                ], 'a line referencing a LATER bundled order should be rejected (bundle order matters)');
            }
        },
        {
            name: '[BUNDLE04] FK to a row deleted earlier in the same bundle is rejected',
            invoke: async () => {
                const { group } = await createTestEnv();

                const orderId = deriveRowId('o-1');
                const orders = await group.getTable('orders');
                await orders.insert('o-1', { customer: 'ada', total: 1 });

                await expectBundleFailure(group, [
                    { table: 'orders', op: deleteOp(orderId) },
                    { table: 'lines', op: insertOp('l-1', { order: orderId, qty: 1 }) },
                ], 'a line referencing a target deleted earlier in the bundle should be rejected');
            }
        },
        {
            name: '[BUNDLE05] Duplicate rowId within a bundle is rejected',
            invoke: async () => {
                const { group } = await createTestEnv();
                const orderId = deriveRowId('dup');

                await expectBundleFailure(group, [
                    { table: 'orders', op: insertOp('dup', { customer: 'ada', total: 1 }) },
                    { table: 'orders', op: updateOp(orderId, { total: 2 }) },
                ], 'two ops for the same rowId in one bundle should be rejected');
            }
        },
        {
            name: '[BUNDLE06] Bundle ops integrate with LWW resolution and findRowIds (meta parity)',
            invoke: async () => {
                const { group } = await createTestEnv();
                const orderId = deriveRowId('o-1');

                await group.bundle([
                    { table: 'orders', op: insertOp('o-1', { customer: 'ada', total: 10 }) },
                ]);

                const orders = await group.getTable('orders');
                let view = await orders.getView();
                assertEquals((await view.getRow(orderId))!.values['customer'], 'ada',
                    'bundled insert values should resolve');
                assertEquals((await view.findRowIds({ customer: 'ada' })).toString(), [orderId].toString(),
                    'bundled insert pub meta should be searchable (parity with envelopes)');

                // a bundled update to a pub column resolves by LWW and re-exports meta
                await group.bundle([
                    { table: 'orders', op: updateOp(orderId, { customer: 'beth' }) },
                ]);
                view = await orders.getView();
                assertEquals((await view.getRow(orderId))!.values['customer'], 'beth',
                    'bundled update should win by LWW');
                assertEquals((await view.findRowIds({ customer: 'beth' })).toString(), [orderId].toString(),
                    'the updated pub value should be found');
                assertEquals((await view.findRowIds({ customer: 'ada' })).length, 0,
                    'the stale pub value should no longer match');
            }
        },
        {
            name: '[BUNDLE07] Bundle delete is a barrier in a concurrentDeletes table',
            invoke: async () => {
                const { group } = await createTestEnv({ ordersConcurrentDeletes: true });
                const scopedDag = await group.getScopedDag();
                const orders = await group.getTable('orders');

                const rowId = deriveRowId('cc-1');
                const base = await scopedDag.getFrontier();

                const insA = await orders.insert('cc-1', { customer: 'x', total: 1 }, undefined, base);
                await group.bundle([{ table: 'orders', op: deleteOp(rowId) }], undefined, version(insA));
                const insB = await orders.insert('cc-1', { customer: 'x', total: 2 }, undefined, base);

                const frontier = await scopedDag.getFrontier();

                const merged = await orders.getView(frontier, frontier);
                assertFalse(await merged.hasRow(rowId), 'the merged-in bundled delete should kill the row');

                const branchB = await orders.getView(version(insB), frontier);
                assertFalse(await branchB.hasRow(rowId),
                    'the bundled delete barrier should reach the concurrent branch');
            }
        },
    ],
};
