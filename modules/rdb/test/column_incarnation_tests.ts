import { assertTrue, assertFalse, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { version, Version } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import { RSchemaImpl, rSchemaFactory } from "../src/rschema/rschema.js";
import { RTableGroupImpl, rTableGroupFactory } from "../src/rtable_group/group.js";
import { deriveRowId } from "../src/rtable/hash.js";
import { deriveEnvelopeMeta, TableScope } from "../src/rtable_group/scopes.js";
import type { TableDef } from "../src/rschema/payload.js";
import type { RTableView } from "../src/rtable/interfaces.js";
import type { RowEnvelopePayload } from "../src/rtable_group/payload.js";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

function open(name: string, columns: TableDef['columns'], extra?: Partial<TableDef>): TableDef {
    return { name, columns, restrictions: [{ on: 'all', rule: { p: 'true' } }], ...extra };
}

async function createEnv(tables: TableDef[]) {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);

    const admin = await makeIdentity();
    const schemaInit = await RSchemaImpl.create({
        name: 'incarnation:test_schema',
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        tables,
    });
    const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
    const pinned = await (await schema.getScopedDag()).getFrontier();

    const groupInit = await RTableGroupImpl.create({
        name: 'incarnation-test-group',
        seed: 'incarnation-test-group',
        schemaRef: schema.getId(),
        schemaVersion: pinned,
    });
    const group = (await ctx.createObject(groupInit)) as RTableGroupImpl;

    return { ctx, schema, group, admin, pinned };
}

async function groupFrontier(group: RTableGroupImpl): Promise<Version> {
    return (await group.getScopedDag()).getFrontier();
}

async function schemaFrontier(schema: RSchemaImpl): Promise<Version> {
    return (await schema.getScopedDag()).getFrontier();
}

async function viewAt(group: RTableGroupImpl, name: string, at: Version, from: Version): Promise<RTableView> {
    return (await group.getView(at, from)).getTableView(name);
}

export const columnIncarnationTests = {
    title: '[INC] Column incarnation pinning tests',
    tests: [
        {
            name: '[INC01] drop and re-add a column does not resurrect old writes',
            invoke: async () => {
                const { group, schema, admin } = await createEnv([
                    open('orders', { customer: { type: 'string' } }),
                ]);
                const orders = await group.getTable('orders');
                const orderId = deriveRowId('o-1');

                await schema.updateSchema([{
                    rule: 'add-column', table: 'orders', column: 'status',
                    def: { type: 'string', default: 'new' },
                }], admin, 'add status');
                const v2 = await schemaFrontier(schema);
                await group.deploy(v2);

                await orders.insert('o-1', { customer: 'ada', status: 'shipped' });
                await orders.update(orderId, { status: 'delivered' });

                await schema.updateSchema([{ rule: 'drop-column', table: 'orders', column: 'status' }], admin, 'drop status');
                const v3 = await schemaFrontier(schema);
                await group.deploy(v3);

                await schema.updateSchema([{
                    rule: 'add-column', table: 'orders', column: 'status',
                    def: { type: 'string', default: 'fresh' },
                }], admin, 're-add status');
                const v4 = await schemaFrontier(schema);
                await group.deploy(v4);

                const row = (await (await orders.getView()).getRow(orderId))!;
                assertEquals(row.values['status'], 'fresh',
                    're-added column should show the new default, not the old incarnation write');
            }
        },
        {
            name: '[INC02] concurrent add-column: only the winning fork values survive at merge',
            invoke: async () => {
                const { group, schema, admin } = await createEnv([
                    open('orders', { customer: { type: 'string' } }),
                ]);
                const orders = await group.getTable('orders');

                const orderA = deriveRowId('o-a');
                const orderB = deriveRowId('o-b');
                await orders.insert('o-a', { customer: 'branch-a' });
                await orders.insert('o-b', { customer: 'branch-b' });
                const base = await groupFrontier(group);
                const schemaBase = await schemaFrontier(schema);

                const ha = await schema.updateSchema([{
                    rule: 'add-column', table: 'orders', column: 'status',
                    def: { type: 'string', default: 'fork-a' },
                }], admin, 'add status A', schemaBase);
                const hb = await schema.updateSchema([{
                    rule: 'add-column', table: 'orders', column: 'status',
                    def: { type: 'string', default: 'fork-b' },
                }], admin, 'add status B', schemaBase);

                const winnerSchema = ha > hb ? ha : hb;
                const loserSchema = ha > hb ? hb : ha;
                const deployWinner = await group.deploy(version(winnerSchema), undefined, base);
                const deployLoser = await group.deploy(version(loserSchema), undefined, base);

                const expectedDefault = ha > hb ? 'fork-a' : 'fork-b';
                await orders.update(orderA, { status: 'from-winner' }, undefined, version(deployWinner));
                await orders.update(orderB, { status: 'from-loser' }, undefined, version(deployLoser));

                const merged = await groupFrontier(group);
                const view = await viewAt(group, 'orders', merged, merged);
                const rowA = (await view.getRow(orderA))!;
                const rowB = (await view.getRow(orderB))!;
                assertEquals(rowA.values['status'], 'from-winner', 'winner-fork write should land');
                assertEquals(rowB.values['status'], expectedDefault,
                    'loser-fork write should not surface; the winning schema default applies');
                assertFalse(rowB.values['status'] === 'from-loser',
                    'explicit loser-fork value must be ignored');
            }
        },
        {
            name: '[INC03] table drop and re-add ignores old incarnation column writes',
            invoke: async () => {
                const { group, schema, admin } = await createEnv([
                    open('orders', { customer: { type: 'string' }, total: { type: 'integer' } }),
                ]);
                const orders = await group.getTable('orders');
                const orderId = deriveRowId('o-1');

                await orders.insert('o-1', { customer: 'ada', total: 99 });
                await orders.update(orderId, { total: 120 });

                await schema.updateSchema([{ rule: 'drop-table', table: 'orders' }], admin, 'drop orders');
                const v2 = await schemaFrontier(schema);
                await group.deploy(v2);

                const fresh: TableDef = {
                    name: 'orders',
                    columns: { amount: { type: 'integer', default: 0 } },
                    restrictions: [{ on: 'all', rule: { p: 'true' } }],
                };
                await schema.updateSchema([{ rule: 'add-table', def: fresh }], admin, 're-add orders');
                const v3 = await schemaFrontier(schema);
                await group.deploy(v3);

                const newId = deriveRowId('o-2');
                await orders.insert('o-2', { amount: 5 });

                const view = await orders.getView();
                const newRow = await view.getRow(newId);
                assertEquals(newRow!.values['amount'], 5, 'new table incarnation should serve new rows');

                const oldRow = await view.getRow(orderId);
                if (oldRow !== undefined) {
                    assertEquals(oldRow.values['amount'], 0,
                        'an old-incarnation row must not carry prior column writes into the new shape');
                    assertTrue(oldRow.values['total'] === undefined,
                        'columns from the prior table incarnation must be absent');
                }
            }
        },
        {
            name: '[INC04] row envelope meta must match incarnation-derived cols tags',
            invoke: async () => {
                const { group, schema, admin } = await createEnv([
                    open('orders', { customer: { type: 'string' } }),
                ]);
                const orders = await group.getTable('orders');

                await schema.updateSchema([{
                    rule: 'add-column', table: 'orders', column: 'status',
                    def: { type: 'string', default: 'new' },
                }], admin, 'add status');
                const v2 = await schemaFrontier(schema);
                await group.deploy(v2);

                const at = await groupFrontier(group);
                const schemaView = await group.resolveSchemaView(at);
                const envelope: RowEnvelopePayload = {
                    action: 'row',
                    table: 'orders',
                    op: {
                        action: 'insert',
                        rowId: deriveRowId('o-1'),
                        uuid: 'o-1',
                        values: { customer: 'ada', status: 'open' },
                    },
                };
                const expected = deriveEnvelopeMeta(envelope, schemaView);
                const forged = { ...expected };
                forged['t-orders-cols'] = json.toSet(['bogus-tag']);

                assertTrue((await group.validatePayload(envelope, at)).valid,
                    'valid envelope should validate');
                assertFalse(
                    (await group.validatePayload({ ...envelope, note: 'tampered' } as json.Literal, at)).valid,
                    'tampered payload should fail',
                );

                const tableScope = new TableScope(group, 'orders');
                const wrapped = tableScope.wrapPayload(envelope.op, at);
                assertFalse(
                    (await tableScope.validateWrappedPayload(wrapped, forged, at)).valid,
                    'mismatched cols meta should be rejected at append validation',
                );
            }
        },
        {
            name: '[INC05] concurrent add-column default activates without a per-row write',
            invoke: async () => {
                const { group, schema, admin } = await createEnv([
                    open('orders', { customer: { type: 'string' } }),
                ]);
                const orders = await group.getTable('orders');
                const base = await groupFrontier(group);

                const orderId = deriveRowId('o-1');
                const orderEntry = await orders.insert('o-1', { customer: 'ada' }, undefined, base);
                const orderPos = version(orderEntry);

                await schema.updateSchema([{
                    rule: 'add-column', table: 'orders', column: 'status',
                    def: { type: 'string', default: 'new' },
                }], admin, 'add status');
                const v2 = await schemaFrontier(schema);
                await group.deploy(v2, undefined, base);

                const merged = await groupFrontier(group);
                const before = await (await viewAt(group, 'orders', orderPos, orderPos)).getRow(orderId);
                assertTrue(before!.values['status'] === undefined,
                    'the column does not exist before the deploy is observed');

                const after = await (await viewAt(group, 'orders', orderPos, merged)).getRow(orderId);
                assertEquals(after!.values['status'], 'new',
                    'a concurrent add-column default activates for the old row at the merged frontier');
            }
        },
        {
            name: '[INC06] getColumnIncarnation tracks add, drop, and re-add',
            invoke: async () => {
                const { schema, admin } = await createEnv([
                    open('orders', { customer: { type: 'string' } }),
                ]);

                const afterCreate = await schema.getView();
                const baseInc = afterCreate.getColumnIncarnation('orders', 'customer')!;
                assertTrue(typeof baseInc === 'string' && baseInc.length > 0,
                    'incarnation id is a structural hash string');

                await schema.updateSchema([{
                    rule: 'add-column', table: 'orders', column: 'status',
                    def: { type: 'string', default: 'new' },
                }], admin, 'add status');
                const afterAdd = await schema.getView();
                const statusInc = afterAdd.getColumnIncarnation('orders', 'status')!;
                assertTrue(statusInc !== baseInc, 'add-column should produce a distinct incarnation id');
                assertEquals(afterAdd.getColumnIncarnation('orders', 'customer'), baseInc,
                    'existing columns keep their incarnation id');

                await schema.updateSchema([{ rule: 'drop-column', table: 'orders', column: 'status' }], admin);
                const afterDrop = await schema.getView();
                assertEquals(afterDrop.getColumnIncarnation('orders', 'status'), undefined,
                    'dropped column should have no live incarnation');

                await schema.updateSchema([{
                    rule: 'add-column', table: 'orders', column: 'status',
                    def: { type: 'string', default: 'again' },
                }], admin, 're-add status');
                const afterReAdd = await schema.getView();
                const reAddInc = afterReAdd.getColumnIncarnation('orders', 'status')!;
                assertTrue(reAddInc !== statusInc, 're-add should start a fresh incarnation id');
            }
        },
        {
            name: '[INC07] concurrent IDENTICAL add-column converges: both forks\' writes survive',
            invoke: async () => {
                const { group, schema, admin } = await createEnv([
                    open('orders', { customer: { type: 'string' } }),
                ]);
                const orders = await group.getTable('orders');

                const orderA = deriveRowId('o-a');
                const orderB = deriveRowId('o-b');
                await orders.insert('o-a', { customer: 'branch-a' });
                await orders.insert('o-b', { customer: 'branch-b' });
                const base = await groupFrontier(group);
                const schemaBase = await schemaFrontier(schema);

                // BYTE-IDENTICAL def on both forks (same default): the two adds
                // share a structural incarnation, so neither masks the other.
                const def = { type: 'string' as const, default: 'same' };
                const ha = await schema.updateSchema([{
                    rule: 'add-column', table: 'orders', column: 'status', def,
                }], admin, 'add status A', schemaBase);
                const hb = await schema.updateSchema([{
                    rule: 'add-column', table: 'orders', column: 'status', def,
                }], admin, 'add status B', schemaBase);

                const deployA = await group.deploy(version(ha), undefined, base);
                const deployB = await group.deploy(version(hb), undefined, base);

                await orders.update(orderA, { status: 'from-a' }, undefined, version(deployA));
                await orders.update(orderB, { status: 'from-b' }, undefined, version(deployB));

                const merged = await groupFrontier(group);
                const view = await viewAt(group, 'orders', merged, merged);
                assertEquals((await view.getRow(orderA))!.values['status'], 'from-a',
                    'fork-a write survives the converged incarnation');
                assertEquals((await view.getRow(orderB))!.values['status'], 'from-b',
                    'fork-b write ALSO survives (identical structural incarnation, no masking)');
            }
        },
        {
            name: '[INC08] concurrent IDENTICAL add-table converges; rows from both forks merge',
            invoke: async () => {
                const { group, schema, admin } = await createEnv([
                    open('orders', { customer: { type: 'string' } }),
                ]);
                const base = await groupFrontier(group);
                const schemaBase = await schemaFrontier(schema);

                // two forks add the SAME new table with a byte-identical def
                const itemsDef: TableDef = open('items', { label: { type: 'string' } });
                const ha = await schema.updateSchema([{ rule: 'add-table', def: itemsDef }], admin, 'add items A', schemaBase);
                const hb = await schema.updateSchema([{ rule: 'add-table', def: itemsDef }], admin, 'add items B', schemaBase);

                const deployA = await group.deploy(version(ha), undefined, base);
                const deployB = await group.deploy(version(hb), undefined, base);

                const items = await group.getTable('items');
                const itemA = deriveRowId('i-a');
                const itemB = deriveRowId('i-b');
                await items.insert('i-a', { label: 'a' }, undefined, version(deployA));
                await items.insert('i-b', { label: 'b' }, undefined, version(deployB));

                const merged = await groupFrontier(group);
                const view = await viewAt(group, 'items', merged, merged);
                assertTrue(await view.hasRow(itemA), 'fork-a row is live under the converged table incarnation');
                assertTrue(await view.hasRow(itemB), 'fork-b row is live under the converged table incarnation');
                assertEquals((await view.getRow(itemA))!.values['label'], 'a', 'fork-a value merges');
                assertEquals((await view.getRow(itemB))!.values['label'], 'b', 'fork-b value merges');
            }
        },
        {
            name: '[INC09] concurrent DIFFERENT add-table forks: only the winning incarnation\'s rows survive',
            invoke: async () => {
                const { group, schema, admin } = await createEnv([
                    open('orders', { customer: { type: 'string' } }),
                ]);
                const base = await groupFrontier(group);
                const schemaBase = await schemaFrontier(schema);

                // structurally DIFFERENT adds of the same name (different default)
                const defA: TableDef = open('items', { label: { type: 'string', default: 'a' } });
                const defB: TableDef = open('items', { label: { type: 'string', default: 'b' } });
                const ha = await schema.updateSchema([{ rule: 'add-table', def: defA }], admin, 'add items A', schemaBase);
                const hb = await schema.updateSchema([{ rule: 'add-table', def: defB }], admin, 'add items B', schemaBase);

                const deployA = await group.deploy(version(ha), undefined, base);
                const deployB = await group.deploy(version(hb), undefined, base);

                const items = await group.getTable('items');
                const itemA = deriveRowId('i-a');
                const itemB = deriveRowId('i-b');
                await items.insert('i-a', { label: 'from-a' }, undefined, version(deployA));
                await items.insert('i-b', { label: 'from-b' }, undefined, version(deployB));

                // existence LWW picks the max-entry-hash add as the live incarnation
                const winnerIsA = ha > hb;
                const winnerRow = winnerIsA ? itemA : itemB;
                const loserRow = winnerIsA ? itemB : itemA;

                const merged = await groupFrontier(group);
                const view = await viewAt(group, 'items', merged, merged);
                assertTrue(await view.hasRow(winnerRow), 'the winning incarnation\'s row is live');
                assertFalse(await view.hasRow(loserRow),
                    'the losing incarnation\'s row is not live at the merge (distinct table incarnation)');
            }
        },
        {
            name: '[INC10] same-shape table drop+re-add resets rows via drop generation',
            invoke: async () => {
                const { group, schema, admin } = await createEnv([
                    open('orders', { customer: { type: 'string' } }),
                ]);
                const orders = await group.getTable('orders');
                const orderId = deriveRowId('o-1');
                await orders.insert('o-1', { customer: 'ada' });

                const incBefore = (await schema.getView()).getTableIncarnation('orders');

                await schema.updateSchema([{ rule: 'drop-table', table: 'orders' }], admin, 'drop orders');
                const v2 = await schemaFrontier(schema);
                await group.deploy(v2);

                // re-add with a BYTE-IDENTICAL def: only the drop generation distinguishes it
                await schema.updateSchema([{ rule: 'add-table', def: open('orders', { customer: { type: 'string' } }) }], admin, 're-add orders');
                const v3 = await schemaFrontier(schema);
                await group.deploy(v3);

                const incAfter = (await schema.getView()).getTableIncarnation('orders');
                assertTrue(incBefore !== undefined && incAfter !== undefined && incBefore !== incAfter,
                    'a same-shape drop+re-add yields a fresh table incarnation (drop generation)');

                const newId = deriveRowId('o-2');
                await orders.insert('o-2', { customer: 'grace' });

                const view = await orders.getView();
                assertTrue(await view.hasRow(newId), 'the new incarnation serves new rows');
                assertFalse(await view.hasRow(orderId),
                    'a prior-incarnation row is not live after a same-shape reset');
            }
        },
    ],
};
