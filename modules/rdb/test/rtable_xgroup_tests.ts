import { assertTrue, assertFalse } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { Version } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import { RSchemaImpl, rSchemaFactory } from "../src/rschema/rschema.js";
import { RTableGroupImpl, rTableGroupFactory } from "../src/rtable_group/group.js";
import { deriveRowId } from "../src/rtable/hash.js";
import type { TableDef, Predicate } from "../src/rschema/payload.js";
import type { RContext } from "@hyper-hyper-space/hhs3_mvt";
import type { RTableView } from "../src/rtable/interfaces.js";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

// permissive table: unauthored ops never void (focus on cross-group FK, not
// local restrictions)
function open(name: string, columns: TableDef['columns'], extra?: Partial<TableDef>): TableDef {
    return { name, columns, restrictions: [{ on: 'all', rule: { p: 'true' } }], ...extra };
}

function newCtx(): RContext {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);
    return ctx;
}

async function makeSchemaGroup(ctx: RContext, seed: string, tables: TableDef[], opts?: {
    creator?: OwnIdentity;
    bindings?: { [name: string]: B64Hash };
    initialRows?: { [t: string]: json.Literal[] };
    canDeploy?: Predicate;
}): Promise<{ schema: RSchemaImpl; group: RTableGroupImpl; creator: OwnIdentity }> {
    const creator = opts?.creator ?? await makeIdentity();
    const schemaInit = await RSchemaImpl.create({
        seed: seed + '-schema',
        creators: [{ keyId: creator.keyId, publicKey: creator.publicKey }],
        tables,
    });
    const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
    const pinned = await (await schema.getScopedDag()).getFrontier();

    const groupInit = await RTableGroupImpl.create({
        seed: seed + '-group',
        schemaRef: schema.getId(),
        schemaVersion: pinned,
        ...(opts?.bindings !== undefined ? { bindings: opts.bindings } : {}),
        ...(opts?.initialRows !== undefined ? { initialRows: opts.initialRows } : {}),
        ...(opts?.canDeploy !== undefined ? { canDeploy: opts.canDeploy } : {}),
    });
    const group = (await ctx.createObject(groupInit)) as RTableGroupImpl;
    return { schema, group, creator };
}

// A table view anchored at a GROUP position (defaults to the group frontier):
// cross-group FK enforcement requires a consistent group snapshot.
async function tableView(group: RTableGroupImpl, name: string, at?: Version): Promise<RTableView> {
    return (await group.getView(at, at)).getTableView(name);
}

async function frontier(group: RTableGroupImpl): Promise<Version> {
    return (await group.getScopedDag()).getFrontier();
}

async function expectThrow(fn: () => Promise<unknown>, why: string): Promise<void> {
    let threw = false;
    try {
        await fn();
    } catch {
        threw = true;
    }
    assertTrue(threw, why);
}

export const rtableXGroupTests = {
    title: '[XGROUP] RTable cross-group FK + exists tests',
    tests: [
        {
            name: '[XGROUP01] observe advances the foreign version: a row added after genesis is invisible until observed',
            invoke: async () => {
                const ctx = newCtx();
                const b = await makeSchemaGroup(ctx, 'xg01-b', [open('identities', { name: { type: 'string' } })]);
                const a = await makeSchemaGroup(ctx, 'xg01-a',
                    [open('orders', { customer: { type: 'string' } }, { fks: { customer: 'users.identities' } })],
                    { bindings: { users: b.group.getId() } });

                const identities = await b.group.getTable('identities');
                const uId = deriveRowId('u-1');
                await identities.insert('u-1', { name: 'ada' });   // added after B genesis
                const bAfter = await frontier(b.group);

                // before observing, A sees only B's genesis (the new identity is invisible)
                const beforeView = await a.group.resolveForeignTableView('users', 'identities', await frontier(a.group), await frontier(a.group));
                assertTrue(beforeView !== undefined, 'the foreign identities table resolves');
                assertFalse(await beforeView!.hasRow(uId), 'u-1 is not visible before observing B past genesis');

                // observe B at the version where u-1 exists
                await a.group.observe('users', bAfter);
                const afterView = await a.group.resolveForeignTableView('users', 'identities', await frontier(a.group), await frontier(a.group));
                assertTrue(await afterView!.hasRow(uId), 'u-1 becomes visible after observing B');
            }
        },
        {
            name: '[XGROUP02] cross-group FK satisfied by a live foreign row: the dependent is live',
            invoke: async () => {
                const ctx = newCtx();
                const b = await makeSchemaGroup(ctx, 'xg02-b', [open('identities', { name: { type: 'string' } })]);
                const a = await makeSchemaGroup(ctx, 'xg02-a',
                    [open('orders', { customer: { type: 'string' } }, { fks: { customer: 'users.identities' } })],
                    { bindings: { users: b.group.getId() } });

                const identities = await b.group.getTable('identities');
                const uId = deriveRowId('u-1');
                await identities.insert('u-1', { name: 'ada' });
                await a.group.observe('users', await frontier(b.group));

                const orders = await a.group.getTable('orders');
                const orderId = deriveRowId('o-1');
                await orders.insert('o-1', { customer: uId });

                assertTrue(await (await tableView(a.group, 'orders')).hasRow(orderId),
                    'an order whose cross-group FK target is live is itself live');
            }
        },
        {
            name: '[XGROUP03] write-time dangling cross-group FK is rejected (target not observed live)',
            invoke: async () => {
                const ctx = newCtx();
                const b = await makeSchemaGroup(ctx, 'xg03-b', [open('identities', { name: { type: 'string' } })]);
                const a = await makeSchemaGroup(ctx, 'xg03-a',
                    [open('orders', { customer: { type: 'string' } }, { fks: { customer: 'users.identities' } })],
                    { bindings: { users: b.group.getId() } });

                const orders = await a.group.getTable('orders');
                // u-1 exists in B but A has not observed it: the FK target is
                // not live at A's observed version -> the write is rejected
                const identities = await b.group.getTable('identities');
                await identities.insert('u-1', { name: 'ada' });

                await expectThrow(() => orders.insert('o-1', { customer: deriveRowId('u-1') }),
                    'an order referencing an unobserved foreign row must be rejected at write time');
            }
        },
        {
            name: '[XGROUP04] At-use cross-group FK: a causally-later foreign delete (observed) does NOT hide the dependent',
            invoke: async () => {
                const ctx = newCtx();
                const b = await makeSchemaGroup(ctx, 'xg04-b', [open('identities', { name: { type: 'string' } }, { concurrentDeletes: false })]);
                const a = await makeSchemaGroup(ctx, 'xg04-a',
                    [open('orders', { customer: { type: 'string' } }, { fks: { customer: 'users.identities' } })],
                    { bindings: { users: b.group.getId() } });

                const identities = await b.group.getTable('identities');
                const uId = deriveRowId('u-1');
                await identities.insert('u-1', { name: 'ada' });
                await a.group.observe('users', await frontier(b.group));

                const orders = await a.group.getTable('orders');
                const orderId = deriveRowId('o-1');
                await orders.insert('o-1', { customer: uId });
                assertTrue(await (await tableView(a.group, 'orders')).hasRow(orderId), 'order live while its foreign target is');

                // delete the identity in B and observe the new version CAUSALLY
                // after the order's FK write: the observe-advance is in the order's
                // future, so at-use the verdict is pinned (use-before-revoke).
                await identities.delete(uId);
                await a.group.observe('users', await frontier(b.group));
                assertTrue(await (await tableView(a.group, 'orders')).hasRow(orderId),
                    'a causally-later foreign delete must not hide the dependent (at-use, cross-group analogue of [ENF02])');
            }
        },
        {
            name: '[XGROUP04b] A concurrent barrier observation of a foreign delete voids the dependent at the merge',
            invoke: async () => {
                const ctx = newCtx();
                const b = await makeSchemaGroup(ctx, 'xg04b-b', [open('identities', { name: { type: 'string' } }, { concurrentDeletes: true })]);
                const a = await makeSchemaGroup(ctx, 'xg04b-a',
                    [open('orders', { customer: { type: 'string' } }, { fks: { customer: 'users.identities' } })],
                    { bindings: { users: b.group.getId() } });

                const identities = await b.group.getTable('identities');
                const uId = deriveRowId('u-1');
                await identities.insert('u-1', { name: 'ada' });
                await a.group.observe('users', await frontier(b.group));

                const base = await frontier(a.group);

                // branch 1: write the order (FK -> the observed live identity)
                const orders = await a.group.getTable('orders');
                const orderId = deriveRowId('o-1');
                await orders.insert('o-1', { customer: uId }, undefined, base);

                // branch 2 (concurrent): delete the identity in B and observe the
                // post-delete version via a BARRIER observe advance
                await identities.delete(uId);
                await a.group.observe('users', await frontier(b.group), base);

                const merged = await frontier(a.group);
                assertFalse(await (await tableView(a.group, 'orders', merged)).hasRow(orderId),
                    'a barrier observation of the foreign delete, concurrent with the FK write, voids the dependent at the merge');
            }
        },
        {
            name: '[XGROUP05] At-use: a causally-later foreign-table drop leaves the dependent live, but rejects new writes',
            invoke: async () => {
                const ctx = newCtx();
                // B has identities + a keeper table so dropping identities leaves B non-empty
                const b = await makeSchemaGroup(ctx, 'xg05-b',
                    [open('identities', { name: { type: 'string' } }), open('keep', { v: { type: 'string' } })]);
                const a = await makeSchemaGroup(ctx, 'xg05-a',
                    [open('orders', { customer: { type: 'string' } }, { fks: { customer: 'users.identities' } })],
                    { bindings: { users: b.group.getId() } });

                const identities = await b.group.getTable('identities');
                const uId = deriveRowId('u-1');
                await identities.insert('u-1', { name: 'ada' });
                await a.group.observe('users', await frontier(b.group));

                const orders = await a.group.getTable('orders');
                const orderId = deriveRowId('o-1');
                await orders.insert('o-1', { customer: uId });
                assertTrue(await (await tableView(a.group, 'orders')).hasRow(orderId), 'order live before the foreign table is dropped');

                // drop identities in B's schema, deploy it, and observe the new
                // version (all causally after the order's FK write)
                await b.schema.updateSchema([{ rule: 'drop-table', table: 'identities' }], b.creator);
                const v2 = await (await b.schema.getScopedDag()).getFrontier();
                await b.group.deploy(v2);
                await a.group.observe('users', await frontier(b.group));

                // at-use: the existing order stays live (the drop is in its future)
                assertTrue(await (await tableView(a.group, 'orders')).hasRow(orderId),
                    'a causally-later foreign-table drop does not hide the existing dependent (at-use)');
                // but a NEW write whose FK target table is now absent is rejected
                await expectThrow(() => orders.insert('o-2', { customer: deriveRowId('u-2') }),
                    'a new cross-group FK write into the now-dropped foreign table is rejected at write time');
            }
        },
        {
            name: '[XGROUP06] cross-group exists restriction: reject without a witness, valid with an observed one',
            invoke: async () => {
                const ctx = newCtx();
                const b = await makeSchemaGroup(ctx, 'xg06-b', [open('caps', { label: { type: 'string', pub: true } })]);
                const a = await makeSchemaGroup(ctx, 'xg06-a',
                    [{
                        name: 'items',
                        columns: { name: { type: 'string' } },
                        restrictions: [{ on: 'insert', rule: { p: 'exists', table: 'users.caps', where: { label: 'grant' } } }],
                    }],
                    { bindings: { users: b.group.getId() } });

                const items = await a.group.getTable('items');

                // no observed witness: the insert fails hard validation
                await expectThrow(() => items.insert('i-void', { name: 'thing' }),
                    'a cross-group exists-gated insert with no observed witness is rejected');

                // grant a cap in B and observe it; a later insert is valid
                const caps = await b.group.getTable('caps');
                await caps.insert('c-1', { label: 'grant' });
                await a.group.observe('users', await frontier(b.group));

                const okId = deriveRowId('i-ok');
                await items.insert('i-ok', { name: 'thing' });
                assertTrue(await (await tableView(a.group, 'items')).hasRow(okId),
                    'an insert with the cross-group witness observed is valid');
            }
        },
        {
            name: '[XGROUP07] binding-time validation: an unbound qualified target group-name rejects create',
            invoke: async () => {
                const ctx = newCtx();
                const b = await makeSchemaGroup(ctx, 'xg07-b', [open('identities', { name: { type: 'string' } })]);

                // schema A references users.identities, but group A is created
                // with NO binding for 'users' -> the create is invalid
                const creator = await makeIdentity();
                const schemaInit = await RSchemaImpl.create({
                    seed: 'xg07-a-schema',
                    creators: [{ keyId: creator.keyId, publicKey: creator.publicKey }],
                    tables: [open('orders', { customer: { type: 'string' } }, { fks: { customer: 'users.identities' } })],
                });
                const schemaA = (await ctx.createObject(schemaInit)) as RSchemaImpl;
                const pinned = await (await schemaA.getScopedDag()).getFrontier();

                // unbound: bindings omitted
                await expectThrow(async () => {
                    const init = await RTableGroupImpl.create({ seed: 'xg07-a-group', schemaRef: schemaA.getId(), schemaVersion: pinned });
                    return ctx.createObject(init);
                }, 'a create whose schema names an unbound qualified target must be rejected');

                // bound: the same create with the binding succeeds
                const init = await RTableGroupImpl.create({
                    seed: 'xg07-a-group', schemaRef: schemaA.getId(), schemaVersion: pinned,
                    bindings: { users: b.group.getId() },
                });
                const group = (await ctx.createObject(init)) as RTableGroupImpl;
                assertTrue(group.getId().length > 0, 'binding the qualified target group-name makes the create valid');
            }
        },
        {
            name: '[XGROUP08] a missing bound-group object throws (infrastructure error)',
            invoke: async () => {
                const ctx = newCtx();
                const creator = await makeIdentity();
                const schemaInit = await RSchemaImpl.create({
                    seed: 'xg08-a-schema',
                    creators: [{ keyId: creator.keyId, publicKey: creator.publicKey }],
                    tables: [open('orders', { customer: { type: 'string' } }, { fks: { customer: 'users.identities' } })],
                });
                const schemaA = (await ctx.createObject(schemaInit)) as RSchemaImpl;
                const pinned = await (await schemaA.getScopedDag()).getFrontier();

                await expectThrow(async () => {
                    const init = await RTableGroupImpl.create({
                        seed: 'xg08-a-group', schemaRef: schemaA.getId(), schemaVersion: pinned,
                        bindings: { users: deriveRowId('not-a-real-group') },
                    });
                    return ctx.createObject(init);
                }, 'a binding whose target object is absent throws');
            }
        },
        {
            name: '[XGROUP09] cross-group FK into a foreign all-live (update-linked) ring stays live',
            invoke: async () => {
                const ctx = newCtx();
                // B has a local FK cycle y <-> z
                const b = await makeSchemaGroup(ctx, 'xg09-b', [
                    open('y', { ref: { type: 'string', nullable: true } }, { fks: { ref: 'z' } }),
                    open('z', { ref: { type: 'string', nullable: true } }, { fks: { ref: 'y' } }),
                ]);
                const a = await makeSchemaGroup(ctx, 'xg09-a',
                    [open('orders', { target: { type: 'string' } }, { fks: { target: 'users.y' } })],
                    { bindings: { users: b.group.getId() } });

                const y = await b.group.getTable('y');
                const z = await b.group.getTable('z');
                const yId = deriveRowId('y-1');
                const zId = deriveRowId('z-1');
                // build the all-live cycle: z first (no ref), y -> z, then z -> y
                await z.insert('z-1', {});
                await y.insert('y-1', { ref: zId });
                await z.update(zId, { ref: yId });

                await a.group.observe('users', await frontier(b.group));
                const orders = await a.group.getTable('orders');
                const orderId = deriveRowId('o-1');
                await orders.insert('o-1', { target: yId });

                assertTrue(await (await tableView(a.group, 'orders')).hasRow(orderId),
                    'a cross-group FK into a foreign ring whose rows are all live (linked by updates) stays live');
            }
        },
        {
            name: '[XGROUP10] barrier boundary: a concurrent foreign revoke voids an at-use cross-group exists at merge',
            invoke: async () => {
                const ctx = newCtx();
                const b = await makeSchemaGroup(ctx, 'xg10-b', [open('caps', { label: { type: 'string', pub: true } }, { concurrentDeletes: true })]);
                const a = await makeSchemaGroup(ctx, 'xg10-a',
                    [{
                        name: 'items',
                        columns: { name: { type: 'string' } },
                        restrictions: [{ on: 'insert', rule: { p: 'exists', table: 'users.caps', where: { label: 'grant' } } }],
                    }],
                    { bindings: { users: b.group.getId() } });

                const caps = await b.group.getTable('caps');
                const capId = deriveRowId('c-1');
                await caps.insert('c-1', { label: 'grant' });
                await a.group.observe('users', await frontier(b.group));   // observe the witness

                const base = await frontier(a.group);

                // branch 1: use the (observed) witness
                const items = await a.group.getTable('items');
                const itemId = deriveRowId('i-1');
                await items.insert('i-1', { name: 'thing' }, undefined, base);

                // branch 2 (concurrent): revoke the witness in B and observe the
                // revoke via a BARRIER observation advance
                await caps.delete(capId);
                await a.group.observe('users', await frontier(b.group), base);

                const merged = await frontier(a.group);
                assertFalse(await (await tableView(a.group, 'items', merged)).hasRow(itemId),
                    'a barrier foreign observation widens the concurrent revoke -> the item voids at merge (cross-group analogue of [ENF08])');
            }
        },
        {
            name: '[XGROUP11] cross-group use-before-revoke: a causally-later foreign revoke does NOT void the earlier use',
            invoke: async () => {
                const ctx = newCtx();
                const b = await makeSchemaGroup(ctx, 'xg11-b', [open('caps', { label: { type: 'string', pub: true } }, { concurrentDeletes: true })]);
                const a = await makeSchemaGroup(ctx, 'xg11-a',
                    [{
                        name: 'items',
                        columns: { name: { type: 'string' } },
                        restrictions: [{ on: 'insert', rule: { p: 'exists', table: 'users.caps', where: { label: 'grant' } } }],
                    }],
                    { bindings: { users: b.group.getId() } });

                const caps = await b.group.getTable('caps');
                const capId = deriveRowId('c-1');
                await caps.insert('c-1', { label: 'grant' });
                await a.group.observe('users', await frontier(b.group));   // observe the witness

                // use the witness
                const items = await a.group.getTable('items');
                const itemId = deriveRowId('i-1');
                await items.insert('i-1', { name: 'thing' });

                // THEN revoke in B and observe the revoke causally AFTER the use
                await caps.delete(capId);
                await a.group.observe('users', await frontier(b.group));

                const merged = await frontier(a.group);
                assertTrue(await (await tableView(a.group, 'items', merged)).hasRow(itemId),
                    'a causally-later barrier observation is in the use\'s future -> use-before-revoke holds across the boundary (cross-group analogue of [ENF07])');
            }
        },
    ],
};
