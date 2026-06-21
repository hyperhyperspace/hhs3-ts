import { assertTrue, assertFalse, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { signPayload } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import { RSchemaImpl, rSchemaFactory } from "../src/rschema/rschema.js";
import type { TableDef } from "../src/rschema/payload.js";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

function ordersTable(): TableDef {
    return {
        name: 'orders',
        columns: {
            customer: { type: 'string', pub: true },
            total: { type: 'float' },
        },
        concurrentDeletes: false,
    };
}

function capsTable(): TableDef {
    return {
        name: 'caps',
        columns: { label: { type: 'string', pub: true } },
    };
}

async function createTestEnv(tables?: TableDef[]) {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);

    const admin = await makeIdentity();

    const init = await RSchemaImpl.create({
        seed: 'rschema-test',
        name: 'test-schema',
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        tables: tables ?? [ordersTable(), capsTable()],
    });

    const schema = (await ctx.createObject(init)) as RSchemaImpl;

    return { ctx, schema, admin };
}

export const rschemaTests = {
    title: '[RSCHEMA] RSchema object tests',
    tests: [
        {
            name: '[RSCHEMA01] Create and view initial tables',
            invoke: async () => {
                const { schema, admin } = await createTestEnv();

                const view = await schema.getView();
                assertEquals(view.getName(), 'test-schema', 'name should be set');
                assertTrue(view.isCreator(admin.keyId), 'admin should be a creator');
                assertFalse(view.isCreator('someone-else'), 'unknown keyId should not be a creator');

                assertEquals(view.getTableNames().sort().toString(), 'caps,orders', 'both tables should exist');
                assertTrue(view.hasTable('orders'), 'orders should exist');
                assertFalse(view.getConcurrentDeletes('orders'), 'orders should have causal-only deletes');
                assertTrue(view.getConcurrentDeletes('caps'), 'caps should default to concurrent deletes');
                assertEquals(view.getPubColumns('orders').toString(), 'customer', 'customer should be pub');
                assertEquals(json.toStringNormalized(view.getRestriction('orders', 'insert')),
                    json.toStringNormalized({ p: 'true' }), 'insert should default to true');
                assertEquals(json.toStringNormalized(view.getRestriction('orders', 'delete')),
                    json.toStringNormalized({ p: 'cmp', cmp: 'eq', left: { col: 'author' }, right: { lit: '$author' } }), 'delete should default to author');
            }
        },
        {
            name: '[RSCHEMA02] Creation rejected when creator keyId does not match public key',
            invoke: async () => {
                const ctx = createMockRContext({ selfValidate: true });
                ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
                const admin = await makeIdentity();

                const init = await RSchemaImpl.create({
                    seed: 'rschema-bad-creator',
                    creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
                    tables: [ordersTable()],
                });
                (init as { creators: { keyId: string }[] }).creators[0].keyId = 'bogus-key-id';

                let failed = false;
                try {
                    await ctx.createObject(init);
                } catch {
                    failed = true;
                }
                assertTrue(failed, 'creation with mismatched creator keyId should fail');
            }
        },
        {
            name: '[RSCHEMA03] Signed update by a creator is accepted',
            invoke: async () => {
                const { schema, admin } = await createTestEnv();

                await schema.updateSchema(
                    [{ rule: 'add-column', table: 'orders', column: 'status', def: { type: 'string', default: 'new' } }],
                    admin, 'add order status');

                const view = await schema.getView();
                const def = view.getTable('orders')!;
                assertEquals(def.columns['status']?.default, 'new', 'status column should exist with default');
            }
        },
        {
            name: '[RSCHEMA04] Update by a non-creator is rejected',
            invoke: async () => {
                const { schema } = await createTestEnv();
                const alice = await makeIdentity();

                let failed = false;
                try {
                    await schema.updateSchema(
                        [{ rule: 'set-concurrent-deletes', table: 'orders', value: true }],
                        alice);
                } catch {
                    failed = true;
                }
                assertTrue(failed, 'update signed by a non-creator should be rejected');

                const view = await schema.getView();
                assertFalse(view.getConcurrentDeletes('orders'), 'concurrentDeletes should be unchanged');
            }
        },
        {
            name: '[RSCHEMA05] Tampered update payload is rejected',
            invoke: async () => {
                const { schema, admin } = await createTestEnv();
                const scopedDag = await schema.getScopedDag();
                const at = await scopedDag.getFrontier();

                const signed = await signPayload({
                    action: 'schema-update',
                    migration: [{ rule: 'set-concurrent-deletes', table: 'orders', value: true }],
                } as unknown as json.LiteralMap, admin);

                assertTrue((await schema.validatePayload(signed, at)).valid, 'intact signed update should validate');
                assertFalse((await schema.validatePayload({ ...signed, note: 'tampered' }, at)).valid,
                    'tampered signed update should not validate');
            }
        },
        {
            name: '[RSCHEMA06] Per-rule applicability at the parent frontier',
            invoke: async () => {
                const { schema, admin } = await createTestEnv();
                const scopedDag = await schema.getScopedDag();
                const at = await scopedDag.getFrontier();

                const signedUpdate = async (migration: json.Literal) =>
                    signPayload({ action: 'schema-update', migration } as unknown as json.LiteralMap, admin);

                assertFalse((await schema.validatePayload(await signedUpdate(
                    [{ rule: 'add-table', def: ordersTable() }]), at)).valid,
                    'add-table for an existing table should not validate');
                assertFalse((await schema.validatePayload(await signedUpdate(
                    [{ rule: 'add-column', table: 'missing', column: 'c', def: { type: 'string', nullable: true } }]), at)).valid,
                    'add-column on a missing table should not validate');
                assertFalse((await schema.validatePayload(await signedUpdate(
                    [{ rule: 'drop-column', table: 'orders', column: 'missing' }]), at)).valid,
                    'drop-column of a missing column should not validate');
                assertFalse((await schema.validatePayload(await signedUpdate(
                    [{ rule: 'drop-column', table: 'caps', column: 'label' }]), at)).valid,
                    'dropping the last column should not validate');
                assertFalse((await schema.validatePayload(await signedUpdate(
                    [{ rule: 'set-fks', table: 'orders', fks: { missing_col: 'caps' } }]), at)).valid,
                    'set-fks on a missing column should not validate');

                // sequential rules within one update see the effect of earlier ones
                assertTrue((await schema.validatePayload(await signedUpdate([
                    { rule: 'drop-table', table: 'orders' },
                    { rule: 'add-table', def: ordersTable() },
                ]), at)).valid, 'drop + re-add of the same table within one update should validate');
            }
        },
        {
            name: '[RSCHEMA07] Concurrent slot writes resolve by entry-hash tiebreak',
            invoke: async () => {
                const { schema, admin } = await createTestEnv();
                const scopedDag = await schema.getScopedDag();
                const at0 = await scopedDag.getFrontier();

                await schema.updateSchema(
                    [{ rule: 'set-concurrent-deletes', table: 'orders', value: true }], admin, undefined, at0);
                await schema.updateSchema(
                    [{ rule: 'set-concurrent-deletes', table: 'caps', value: false }], admin, undefined, at0);

                // non-conflicting concurrent writes both land
                const merged = await schema.getView();
                assertTrue(merged.getConcurrentDeletes('orders'), 'orders write should land');
                assertFalse(merged.getConcurrentDeletes('caps'), 'caps write should land');

                // conflicting concurrent writes to the SAME slot: larger hash wins
                const at1 = await scopedDag.getFrontier();
                const ha = await schema.updateSchema(
                    [{ rule: 'set-concurrent-deletes', table: 'orders', value: false }], admin, undefined, at1);
                const hb = await schema.updateSchema(
                    [{ rule: 'set-concurrent-deletes', table: 'orders', value: true }], admin, undefined, at1);

                const expected = ha > hb ? false : true;
                const view = await schema.getView();
                assertEquals(view.getConcurrentDeletes('orders'), expected,
                    'concurrent same-slot writes should resolve by entry-hash tiebreak');
            }
        },
        {
            name: '[RSCHEMA08] Drop-table tombstone masks concurrent column writes',
            invoke: async () => {
                const { schema, admin } = await createTestEnv();
                const scopedDag = await schema.getScopedDag();
                const at0 = await scopedDag.getFrontier();

                await schema.updateSchema([{ rule: 'drop-table', table: 'orders' }], admin, undefined, at0);
                await schema.updateSchema(
                    [{ rule: 'add-column', table: 'orders', column: 'status', def: { type: 'string', default: 'new' } }],
                    admin, undefined, at0);

                const view = await schema.getView();
                assertFalse(view.hasTable('orders'),
                    'tombstone (causally after create) should win over the concurrent column write');
            }
        },
        {
            name: '[RSCHEMA09] Re-added table starts a fresh incarnation',
            invoke: async () => {
                const { schema, admin } = await createTestEnv();
                const scopedDag = await schema.getScopedDag();
                const at0 = await scopedDag.getFrontier();

                // concurrent: drop orders | add a column to orders
                await schema.updateSchema([{ rule: 'drop-table', table: 'orders' }], admin, undefined, at0);
                await schema.updateSchema(
                    [{ rule: 'add-column', table: 'orders', column: 'status', def: { type: 'string', default: 'new' } }],
                    admin, undefined, at0);

                // then re-add orders with a different shape, after the merge
                const fresh: TableDef = { name: 'orders', columns: { amount: { type: 'integer' } } };
                await schema.updateSchema([{ rule: 'add-table', def: fresh }], admin);

                const view = await schema.getView();
                const def = view.getTable('orders')!;
                assertEquals(Object.keys(def.columns).sort().toString(), 'amount',
                    'old incarnation columns (customer, total, status) must not bleed into the new one');
                assertTrue(view.getConcurrentDeletes('orders'),
                    'the old incarnation concurrentDeletes value must not bleed either');
            }
        },
        {
            name: '[RSCHEMA10] Resolved states are pure functions of the position',
            invoke: async () => {
                const { schema, admin } = await createTestEnv();
                const scopedDag = await schema.getScopedDag();
                const at0 = await scopedDag.getFrontier();

                await schema.updateSchema(
                    [{ rule: 'add-column', table: 'orders', column: 'status', def: { type: 'string', default: 'new' } }],
                    admin);

                // the view at the old position still shows the old schema
                const oldView = await schema.getView(at0, at0);
                assertFalse(oldView.getTable('orders')!.columns['status'] !== undefined,
                    'old position should not see the new column');

                const newView = await schema.getView();
                assertTrue(newView.getTable('orders')!.columns['status'] !== undefined,
                    'frontier should see the new column');
            }
        },
        {
            name: '[RSCHEMA11] Delta reports changed slots between versions',
            invoke: async () => {
                const { schema, admin } = await createTestEnv();
                const scopedDag = await schema.getScopedDag();
                const start = await scopedDag.getFrontier();

                await schema.updateSchema(
                    [{ rule: 'add-column', table: 'orders', column: 'status', def: { type: 'string', default: 'new' } }],
                    admin);
                await schema.updateSchema(
                    [{ rule: 'set-concurrent-deletes', table: 'caps', value: false }], admin);
                await schema.updateSchema(
                    [{ rule: 'add-table', def: { name: 'notes', columns: { body: { type: 'string' } } } }], admin);

                const end = await scopedDag.getFrontier();
                const delta = await schema.computeDelta(start, end);

                const byTable = new Map(delta.tableChanges.map((c) => [c.table, c]));
                assertEquals(delta.tableChanges.length, 3, 'three tables should have changed');

                const orders = byTable.get('orders')!;
                assertTrue(orders.existedBefore && orders.existsAfter, 'orders should persist');
                assertEquals(orders.columnChanges.length, 1, 'orders should have one column change');
                assertEquals(orders.columnChanges[0].column, 'status', 'the changed column should be status');

                const caps = byTable.get('caps')!;
                assertTrue(caps.concurrentDeletesChanged, 'caps concurrentDeletes should have changed');
                assertEquals(caps.columnChanges.length, 0, 'caps should have no column changes');

                const notes = byTable.get('notes')!;
                assertFalse(notes.existedBefore, 'notes should not exist at start');
                assertTrue(notes.existsAfter, 'notes should exist at end');

                // empty delta when start == end
                const empty = await schema.computeDelta(end, end);
                assertEquals(empty.tableChanges.length, 0, 'identical versions should produce an empty delta');
            }
        },
    ],
};
