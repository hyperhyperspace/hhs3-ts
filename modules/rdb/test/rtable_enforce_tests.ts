import { assertTrue, assertFalse, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { version, Version } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import { RSchemaImpl, rSchemaFactory } from "../src/rschema/rschema.js";
import { RTableGroupImpl, rTableGroupFactory } from "../src/rtable_group/group.js";
import { deriveRowId } from "../src/rtable/hash.js";
import type { TableDef, Predicate } from "../src/rschema/payload.js";
import type { RTableView } from "../src/rtable/interfaces.js";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

async function createEnv(tables: TableDef[], opts?: { canDeploy?: Predicate; initialRows?: { [t: string]: json.Literal[] }; admin?: OwnIdentity }) {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);

    const admin = opts?.admin ?? await makeIdentity();
    const schemaInit = await RSchemaImpl.create({
        seed: 'enf-test-schema',
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        tables,
    });
    const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
    const pinned = await (await schema.getScopedDag()).getFrontier();

    const groupInit = await RTableGroupImpl.create({
        seed: 'enf-test-group',
        schemaRef: schema.getId(),
        schemaVersion: pinned,
        ...(opts?.canDeploy !== undefined ? { canDeploy: opts.canDeploy } : {}),
        ...(opts?.initialRows !== undefined ? { initialRows: opts.initialRows } : {}),
    });
    const group = (await ctx.createObject(groupInit)) as RTableGroupImpl;

    return { ctx, schema, group, admin, pinned };
}

// A table view anchored at a GROUP position (defaults to the group frontier):
// cross-table FK / exists enforcement requires a consistent group snapshot, so
// reading a single table at its own scope frontier (which can lag other
// tables' writes) would observe a stale, pre-delete world.
async function tableView(group: RTableGroupImpl, name: string, at?: Version): Promise<RTableView> {
    return (await group.getView(at, at)).getTableView(name);
}

async function groupFrontier(group: RTableGroupImpl): Promise<Version> {
    return (await group.getScopedDag()).getFrontier();
}

// permissive table: unauthored ops never void (focus on FK, not restrictions)
function open(name: string, columns: TableDef['columns'], extra?: Partial<TableDef>): TableDef {
    return { name, columns, restrictions: [{ on: 'all', rule: { p: 'true' } }], ...extra };
}

async function expectInsertFailure(table: { insert: (...a: never[]) => Promise<unknown> }, args: unknown[], why: string) {
    let failed = false;
    try {
        await (table.insert as (...a: unknown[]) => Promise<unknown>)(...args);
    } catch {
        failed = true;
    }
    assertTrue(failed, why);
}

export const rtableEnforceTests = {
    title: '[ENF] RTable enforcement (FK liveness + restrictions + canDeploy) tests',
    tests: [
        {
            name: '[ENF01] Insert with dangling FK rejected at write time',
            invoke: async () => {
                const { group } = await createEnv([
                    open('orders', { customer: { type: 'string' } }),
                    open('lines', { order: { type: 'string' }, qty: { type: 'integer' } }, { fks: { order: 'orders' } }),
                ]);
                const lines = await group.getTable('lines');
                await expectInsertFailure(lines, ['l-1', { order: deriveRowId('ghost'), qty: 1 }],
                    'an insert naming a non-existent FK target should be rejected');
            }
        },
        {
            name: '[ENF02] At-use FK: a causally-later delete of the target does NOT hide the dependent',
            invoke: async () => {
                const { group } = await createEnv([
                    open('orders', { customer: { type: 'string' } }),
                    open('lines', { order: { type: 'string' }, qty: { type: 'integer' } }, { fks: { order: 'orders' } }),
                ]);
                const orders = await group.getTable('orders');
                const lines = await group.getTable('lines');

                const orderId = deriveRowId('o-1');
                const lineId = deriveRowId('l-1');
                await orders.insert('o-1', { customer: 'ada' });
                await lines.insert('l-1', { order: orderId, qty: 2 });

                const before = await groupFrontier(group);
                assertTrue(await (await tableView(group, 'lines', before)).hasRow(lineId), 'the line is live while its order is');

                // causal delete: the line's FK write is in the delete's causal
                // PAST, so at-use the verdict is pinned (use-before-revoke) — the
                // dependent stays live (live-but-dangling), NOT cascade-hidden.
                await orders.delete(orderId);
                assertTrue(await (await tableView(group, 'lines')).hasRow(lineId),
                    'a causally-later target delete must not hide the dependent (at-use)');
                assertTrue(await (await tableView(group, 'lines', before)).hasRow(lineId),
                    'the pre-delete snapshot still shows the line');
            }
        },
        {
            name: '[ENF03] At-use FK chain: causal target delete leaves existing dependents live; new dependents rejected',
            invoke: async () => {
                const { group } = await createEnv([
                    open('c', { v: { type: 'string', default: 'x' } }),
                    open('b', { ref: { type: 'string' } }, { fks: { ref: 'c' } }),
                    open('a', { ref: { type: 'string' } }, { fks: { ref: 'b' } }),
                ]);
                const c = await group.getTable('c');
                const b = await group.getTable('b');
                const a = await group.getTable('a');

                const c1 = deriveRowId('c-1');
                const c2 = deriveRowId('c-2');
                const b1 = deriveRowId('b-1');
                const a1 = deriveRowId('a-1');

                await c.insert('c-1', {});
                await c.insert('c-2', {});
                await b.insert('b-1', { ref: c1 });
                await a.insert('a-1', { ref: b1 });

                assertTrue(await (await tableView(group, 'a')).hasRow(a1), 'a1 live while the whole chain is');

                // causal delete of the chain root: existing dependents are inert
                await c.delete(c1);
                assertTrue(await (await tableView(group, 'b')).hasRow(b1), 'b1 stays live (causal target delete is inert)');
                assertTrue(await (await tableView(group, 'a')).hasRow(a1), 'a1 stays live transitively');

                // a NEW dependent of the now-dead c1 is rejected at write time;
                // one of the still-live c2 lands
                await expectInsertFailure(b, ['b-2', { ref: c1 }], 'a new FK write to the dead c1 is rejected at write time');
                const b3 = deriveRowId('b-3');
                await b.insert('b-3', { ref: c2 });
                assertTrue(await (await tableView(group, 'b')).hasRow(b3), 'a new FK write to a live target lands');
            }
        },
        {
            name: '[ENF04] At-use FK: update-carried mutual references do not gate liveness (both rows live)',
            invoke: async () => {
                const { group } = await createEnv([
                    open('x', { ref: { type: 'string', nullable: true } }, { fks: { ref: 'y' } }),
                    open('y', { ref: { type: 'string', nullable: true } }, { fks: { ref: 'x' } }),
                ]);
                const x = await group.getTable('x');
                const y = await group.getTable('y');
                const xId = deriveRowId('x-1');
                const yId = deriveRowId('y-1');

                // y rooted with no FK on its insert; x -> y by insert; y -> x added
                // by UPDATE. FK writes void the OP, never the row's liveness — and
                // both inserts reach a live target — so neither row is hidden. (A
                // genuine mutual insert-FK ring is unconstructable: write-time
                // sequential FK checks reject it, even inside one bundle.)
                await y.insert('y-1', {});
                await x.insert('x-1', { ref: yId });
                await y.update(yId, { ref: xId });

                assertTrue(await (await tableView(group, 'x')).hasRow(xId), 'x stays live (its insert reaches the live y)');
                assertTrue(await (await tableView(group, 'y')).hasRow(yId), 'y stays live (its insert carries no FK)');
            }
        },
        {
            name: '[ENF05] Concurrent delete of an FK target (concurrentDeletes) hides the dependent at the merge',
            invoke: async () => {
                const { group } = await createEnv([
                    open('orders', { customer: { type: 'string' } }, { concurrentDeletes: true }),
                    open('lines', { order: { type: 'string' }, qty: { type: 'integer' } }, { fks: { order: 'orders' } }),
                ]);
                const orders = await group.getTable('orders');
                const lines = await group.getTable('lines');

                const orderId = deriveRowId('o-1');
                await orders.insert('o-1', { customer: 'ada' });
                const base = await groupFrontier(group);

                // concurrent: branch A deletes the order (barrier), branch B
                // inserts a line referencing it. The line's FK write is CONCURRENT
                // with the delete, so the barrier voids it at the merge.
                await orders.delete(orderId, undefined, base);
                const lineId = deriveRowId('l-1');
                await lines.insert('l-1', { order: orderId, qty: 1 }, undefined, undefined, base);

                const frontier = await groupFrontier(group);
                assertFalse(await (await tableView(group, 'lines', frontier)).hasRow(lineId),
                    'a barrier delete concurrent with the FK write voids the dependent at the merge');
            }
        },
        {
            name: '[ENF05b] Concurrent delete with concurrentDeletes:false leaves the dependent live',
            invoke: async () => {
                const { group } = await createEnv([
                    open('orders', { customer: { type: 'string' } }, { concurrentDeletes: false }),
                    open('lines', { order: { type: 'string' }, qty: { type: 'integer' } }, { fks: { order: 'orders' } }),
                ]);
                const orders = await group.getTable('orders');
                const lines = await group.getTable('lines');

                const orderId = deriveRowId('o-1');
                await orders.insert('o-1', { customer: 'ada' });
                const base = await groupFrontier(group);

                await orders.delete(orderId, undefined, base);
                const lineId = deriveRowId('l-1');
                await lines.insert('l-1', { order: orderId, qty: 1 }, undefined, undefined, base);

                const frontier = await groupFrontier(group);
                assertTrue(await (await tableView(group, 'lines', frontier)).hasRow(lineId),
                    'with concurrentDeletes:false the concurrent delete is not honored, so the target (and the dependent) stay live');
            }
        },
        {
            name: '[ENF06] Exists-gated insert without a witness lands at write but is void at view time',
            invoke: async () => {
                const { group } = await createEnv([
                    open('caps', { label: { type: 'string', pub: true } }),
                    {
                        name: 'items',
                        columns: { name: { type: 'string' } },
                        restrictions: [{ on: 'insert', rule: { p: 'exists', table: 'caps', where: { label: 'grant' } } }],
                    },
                ]);
                const items = await group.getTable('items');
                const caps = await group.getTable('caps');

                // no witness yet: the op lands at write time but is void at view time
                const voidId = deriveRowId('i-void');
                await items.insert('i-void', { name: 'thing' });
                assertFalse(await (await tableView(group, 'items')).hasRow(voidId),
                    'the exists-gated insert is void at view time (no witness)');

                // a witness present at-or-before the use validates a later use
                // (at-use semantics: the witness must exist at the op position)
                await caps.insert('c-1', { label: 'grant' });
                const okId = deriveRowId('i-ok');
                await items.insert('i-ok', { name: 'thing' });
                assertTrue(await (await tableView(group, 'items')).hasRow(okId),
                    'an insert with the witness already present is valid');
                assertFalse(await (await tableView(group, 'items')).hasRow(voidId),
                    'the earlier witness-less insert stays void (at-use, not retroactive)');
            }
        },
        {
            name: '[ENF07] Use-before-revoke: a witness deleted causally after the use does not void it',
            invoke: async () => {
                const { group } = await createEnv([
                    open('caps', { label: { type: 'string', pub: true } }),
                    {
                        name: 'items',
                        columns: { name: { type: 'string' } },
                        restrictions: [{ on: 'insert', rule: { p: 'exists', table: 'caps', where: { label: 'grant' } } }],
                    },
                ]);
                const caps = await group.getTable('caps');
                const items = await group.getTable('items');

                const capId = deriveRowId('c-1');
                await caps.insert('c-1', { label: 'grant' });
                const itemId = deriveRowId('i-1');
                await items.insert('i-1', { name: 'thing' });

                // revoke the witness AFTER the use (causally later)
                await caps.delete(capId);
                assertTrue(await (await tableView(group, 'items')).hasRow(itemId),
                    'a causally-later revoke must not void the earlier use');
            }
        },
        {
            name: '[ENF08] Concurrent barrier revoke voids the concurrent use at the merged frontier',
            invoke: async () => {
                const { group } = await createEnv([
                    open('caps', { label: { type: 'string', pub: true } }, { concurrentDeletes: true }),
                    {
                        name: 'items',
                        columns: { name: { type: 'string' } },
                        restrictions: [{ on: 'insert', rule: { p: 'exists', table: 'caps', where: { label: 'grant' } } }],
                    },
                ]);
                const caps = await group.getTable('caps');
                const items = await group.getTable('items');

                const capId = deriveRowId('c-1');
                await caps.insert('c-1', { label: 'grant' });
                const base = await groupFrontier(group);

                // concurrent: branch A revokes the witness (barrier), branch B uses it
                await caps.delete(capId, undefined, base);
                const itemId = deriveRowId('i-1');
                await items.insert('i-1', { name: 'thing' }, undefined, undefined, base);

                const frontier = await groupFrontier(group);
                assertFalse(await (await tableView(group, 'items', frontier)).hasRow(itemId),
                    'a barrier revoke concurrent with the use voids it at the merge');
            }
        },
        {
            name: '[ENF09] Non-barrier revoke does not void the concurrent use',
            invoke: async () => {
                const { group } = await createEnv([
                    open('caps', { label: { type: 'string', pub: true } }, { concurrentDeletes: false }),
                    {
                        name: 'items',
                        columns: { name: { type: 'string' } },
                        restrictions: [{ on: 'insert', rule: { p: 'exists', table: 'caps', where: { label: 'grant' } } }],
                    },
                ]);
                const caps = await group.getTable('caps');
                const items = await group.getTable('items');

                const capId = deriveRowId('c-1');
                await caps.insert('c-1', { label: 'grant' });
                const base = await groupFrontier(group);

                await caps.delete(capId, undefined, base);
                const itemId = deriveRowId('i-1');
                await items.insert('i-1', { name: 'thing' }, undefined, undefined, base);

                const frontier = await groupFrontier(group);
                assertTrue(await (await tableView(group, 'items', frontier)).hasRow(itemId),
                    'a non-barrier (causal-only) revoke does not reach the concurrent use');
            }
        },
        {
            name: '[ENF10] Defaults: unauthored update/delete of an owned row void; authored-by-owner pass',
            invoke: async () => {
                // default restrictions (no `restrictions` declared): insert
                // true, update/delete owner-is-author
                const { group } = await createEnv([
                    { name: 'docs', columns: { body: { type: 'string' } } },
                ]);
                const docs = await group.getTable('docs');
                const owner = await makeIdentity();
                const rowId = deriveRowId('d-1', owner.keyId);

                await docs.insert('d-1', { body: 'v1' }, owner.keyId, owner);

                // unauthored update: lands at write (signature not verified without provider),
                // but voids at view time (owner-is-author fails)
                await docs.update(rowId, { body: 'v2' });
                assertEquals((await (await tableView(group, 'docs')).getRow(rowId))!.values['body'], 'v1',
                    'an unauthored update of an owned row voids at view time');

                // authored by the owner: it lands and resolves
                await docs.update(rowId, { body: 'v3' }, owner);
                assertEquals((await (await tableView(group, 'docs')).getRow(rowId))!.values['body'], 'v3',
                    'an owner-authored update passes the default restriction');
            }
        },
        {
            name: '[ENF11] $rowOwner / owner-term exists predicates resolve correctly',
            invoke: async () => {
                // items may be updated only by someone who owns a caps row
                // (exists caps where owner = $author)
                const { group } = await createEnv([
                    open('caps', { label: { type: 'string', pub: true } }),
                    {
                        name: 'items',
                        columns: { name: { type: 'string' } },
                        restrictions: [
                            { on: 'insert', rule: { p: 'true' } },
                            { on: 'update', rule: { p: 'exists', table: 'caps', owner: '$author' } },
                        ],
                    },
                ]);
                const caps = await group.getTable('caps');
                const items = await group.getTable('items');

                const alice = await makeIdentity();
                const bob = await makeIdentity();

                // alice owns a caps row; bob does not
                await caps.insert('c-alice', { label: 'x' }, alice.keyId, alice);

                const itemId = deriveRowId('i-1');
                await items.insert('i-1', { name: 'v1' });

                // bob's update: no caps row owned by bob -> void
                await items.update(itemId, { name: 'by-bob' }, bob);
                assertEquals((await (await tableView(group, 'items')).getRow(itemId))!.values['name'], 'v1',
                    'an update by a non-cap-holder voids');

                // alice's update: she owns a caps row -> passes
                await items.update(itemId, { name: 'by-alice' }, alice);
                assertEquals((await (await tableView(group, 'items')).getRow(itemId))!.values['name'], 'by-alice',
                    'an update by a cap-holder passes the exists/owner predicate');
            }
        },
        {
            name: '[ENF12] A voided op inside a bundle voids the sibling ops (all-or-nothing visibility)',
            invoke: async () => {
                // bundle of two inserts; one is exists-gated with no witness ->
                // the whole bundle voids
                const { group } = await createEnv([
                    open('caps', { label: { type: 'string', pub: true } }),
                    open('free', { v: { type: 'string' } }),
                    {
                        name: 'gated',
                        columns: { v: { type: 'string' } },
                        restrictions: [{ on: 'insert', rule: { p: 'exists', table: 'caps', where: { label: 'grant' } } }],
                    },
                ]);

                const freeId = deriveRowId('f-1');
                const gatedId = deriveRowId('g-1');
                await group.bundle([
                    { table: 'free', op: { action: 'insert', rowId: freeId, uuid: 'f-1', values: { v: 'a' } } },
                    { table: 'gated', op: { action: 'insert', rowId: gatedId, uuid: 'g-1', values: { v: 'b' } } },
                ]);

                assertFalse(await (await tableView(group, 'gated')).hasRow(gatedId), 'the gated op voids (no witness)');
                assertFalse(await (await tableView(group, 'free')).hasRow(freeId),
                    'its bundle sibling voids too (all-or-nothing)');
            }
        },
        {
            name: '[ENF13] canDeploy predicate evaluated on deploy: passing and failing authors',
            invoke: async () => {
                // canDeploy: exists admins where owner = $author
                const admins = open('admins', { label: { type: 'string', pub: true } });
                const admin = await makeIdentity();
                const { group, schema } = await createEnv(
                    [admins, open('orders', { customer: { type: 'string' } })],
                    {
                        admin,
                        canDeploy: { p: 'exists', table: 'admins', owner: '$author' },
                        initialRows: {
                            admins: [{ action: 'insert', rowId: deriveRowId('seed-admin', admin.keyId), uuid: 'seed-admin', owner: admin.keyId, values: { label: 'root' } }],
                        },
                    },
                );

                await schema.updateSchema([{ rule: 'add-table', def: open('notes', { body: { type: 'string' } }) }], admin);
                const v2 = await (await schema.getScopedDag()).getFrontier();

                const stranger = await makeIdentity();
                let strangerFailed = false;
                try {
                    await group.deploy(v2, stranger);
                } catch {
                    strangerFailed = true;
                }
                assertTrue(strangerFailed, 'a deploy by a non-admin should fail canDeploy');

                await group.deploy(v2, admin);
                assertTrue((await group.getView()).getTableNames().includes('notes'),
                    'a deploy by an admin (owns an admins row) should pass canDeploy');
            }
        },
    ],
};
