// End-to-end tests for the reactive replica-wide projection supervisor. A real
// two-group RDb (a `catalog` group and an `orders` group with a cross-group FK
// into it) is projected into ONE shared in-memory target, exercising:
//   - group-qualified table names (no collisions in the shared store);
//   - cross-group FK id resolution (an order's item resolves to the product's
//     serial id, NOT an opaque row_hash, because both groups are co-projected);
//   - reactive re-sync when rdb advances after open.

import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { Version, RContext } from "@hyper-hyper-space/hhs3_mvt";
import type { TableDef } from "@hyper-hyper-space/hhs3_rdb";
import {
    RSchemaImpl, rSchemaFactory, RTableGroupImpl, rTableGroupFactory, RDbImpl, rDbFactory, deriveRowId,
} from "@hyper-hyper-space/hhs3_rdb";
import { MemoryTarget, type OpEvent } from "@hyper-hyper-space/hhs3_rdb_adapter";

import { createMockRContext } from "../../rdb/test/mock_rcontext.js";
import { RdbProjection, opEventPushable } from "../src/index.js";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

function newCtx(): RContext {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);
    ctx.getRegistry().register(RDbImpl.typeId, rDbFactory);
    return ctx;
}

function open(name: string, columns: TableDef['columns'], extra?: Partial<TableDef>): TableDef {
    return { name, columns, restrictions: [{ on: 'all', rule: { p: 'true' } }], ...extra };
}

async function makeSchemaGroup(ctx: RContext, name: string, tables: TableDef[], bindings?: { [k: string]: B64Hash }) {
    const admin = await makeIdentity();
    const schemaInit = await RSchemaImpl.create({
        name: `${name}:schema`, creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }], tables,
    });
    const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
    const pinned = await (await schema.getScopedDag()).getFrontier();
    const groupInit = await RTableGroupImpl.create({
        name, seed: `${name}-seed`, schemaRef: schema.getId(), schemaVersion: pinned,
        ...(bindings !== undefined ? { bindings } : {}),
    });
    const group = (await ctx.createObject(groupInit)) as RTableGroupImpl;
    return { schema, group, admin };
}

async function makeRDb(ctx: RContext, seed: string): Promise<RDbImpl> {
    const init = await RDbImpl.create({ seed });
    return (await ctx.createObject(init)) as RDbImpl;
}

async function frontier(group: RTableGroupImpl): Promise<Version> {
    return (await group.getScopedDag()).getFrontier();
}

// Build a catalog group (products) and an orders group whose `item` column is a
// cross-group FK into catalog.products, both registered on one RDb.
async function makeCatalogAndOrders(ctx: RContext) {
    const catalog = await makeSchemaGroup(ctx, 'catalog', [open('products', { title: { type: 'string' } })]);
    const orders = await makeSchemaGroup(ctx, 'orders',
        [open('orders', { item: { type: 'string', nullable: true } }, { fks: { item: 'catalog.products' } })],
        { catalog: catalog.group.getId() });

    const rdb = await makeRDb(ctx, 'shop');
    await rdb.addGroup(catalog.group.getId());
    await rdb.addGroup(orders.group.getId());
    return { catalog, orders, rdb };
}

async function poll(fn: () => boolean, timeoutMs = 2000): Promise<void> {
    const start = Date.now();
    while (!fn()) {
        if (Date.now() - start > timeoutMs) throw new Error('poll timed out');
        await new Promise((r) => setTimeout(r, 10));
    }
}

// A single-group RDb whose `comments` table carries a local FK into `posts`, so
// a dangling-FK local insert produces a deterministic ingestion failure.
async function makeForum(ctx: RContext) {
    const forum = await makeSchemaGroup(ctx, 'forum', [
        open('posts', { title: { type: 'string' } }),
        open('comments', { body: { type: 'string' }, post: { type: 'string', nullable: true } }, { fks: { post: 'posts' } }),
    ]);
    const rdb = await makeRDb(ctx, 'forum-db');
    await rdb.addGroup(forum.group.getId());
    return { forum, rdb };
}

// A single-group RDb reproducing a p2p concurrency void: an `items` insert is
// gated on a `grant` cap; a CONCURRENT cap delete (sibling of the insert's
// parent) leaves the insert accepted at write time but voided at the merged
// head. Mirrors the rdb-level OPDELTA01 barrier, but driven through projection.
async function makeConcurrency(ctx: RContext) {
    const g = await makeSchemaGroup(ctx, 'perm', [
        open('caps', { label: { type: 'string', pub: true } }, { concurrentDeletes: true }),
        open('items', { name: { type: 'string' } },
            { restrictions: [{ on: 'insert', rule: { p: 'exists', table: 'caps', where: { label: 'grant' } } }] }),
    ]);
    const rdb = await makeRDb(ctx, 'perm-db');
    await rdb.addGroup(g.group.getId());
    return { g, rdb };
}

export const projectionTests = {
    title: '[RDBPROJ] rdb_projection replica-wide supervisor',
    tests: [
        {
            name: '[RDBPROJ01] group-qualified tables + cross-group FK id resolution across a shared target',
            invoke: async () => {
                const ctx = newCtx();
                const { catalog, orders, rdb } = await makeCatalogAndOrders(ctx);

                // A product in catalog; observe it from orders so the cross-group
                // FK target is live; then an order referencing it.
                const products = await catalog.group.getTable('products');
                await products.insert('p1', { title: 'Widget' }, catalog.admin);
                const p1 = deriveRowId('p1', catalog.admin.keyId);
                await orders.group.observe('catalog', await frontier(catalog.group));

                const ordersTable = await orders.group.getTable('orders');
                await ordersTable.insert('o1', { item: p1 }, orders.admin);
                const o1 = deriveRowId('o1', orders.admin.keyId);

                const target = new MemoryTarget();
                const projection = await RdbProjection.open(rdb, ctx, target);
                try {
                    // Group-qualified names, no collisions.
                    assertTrue(target.hasTable('catalog_products'), 'catalog.products -> catalog_products');
                    assertTrue(target.hasTable('orders_orders'), 'orders.orders -> orders_orders');

                    // The order's cross-group FK projects as an integer id companion...
                    const types = target.columnTypes('orders_orders');
                    assertEquals(types?.item_id, 'integer', 'cross-group FK is an integer <col>_id (co-projected)');
                    assertEquals(types?.item_row_hash, undefined, 'no row_hash passthrough for a co-projected ref');

                    // ...whose VALUE is the product's serial id in the shared store.
                    const productSerial = target.syncId('catalog_products', p1);
                    assertTrue(productSerial !== undefined, 'the product has a serial id');
                    const orderRow = target.getRowByRowId('orders_orders', o1);
                    assertTrue(orderRow !== undefined, 'the order row is materialized');
                    assertEquals(orderRow!.values.item_id, productSerial, 'order.item_id resolves to the product serial id');
                } finally {
                    await projection.stop();
                }
            },
        },
        {
            name: '[RDBPROJ02] reactive: a product added after open re-materializes without an explicit sync',
            invoke: async () => {
                const ctx = newCtx();
                const { catalog, rdb } = await makeCatalogAndOrders(ctx);

                const target = new MemoryTarget();
                const projection = await RdbProjection.open(rdb, ctx, target, { debounceMs: 10 });
                try {
                    assertEquals(target.getRowIds('catalog_products').length, 0, 'no products before any insert');

                    const products = await catalog.group.getTable('products');
                    await products.insert('p1', { title: 'Widget' }, catalog.admin);
                    const p1 = deriveRowId('p1', catalog.admin.keyId);

                    // The group.subscribe trigger fires a debounced sync; wait for it.
                    await poll(() => target.getRowByRowId('catalog_products', p1) !== undefined);
                    assertEquals(target.getRowByRowId('catalog_products', p1)!.values.title, 'Widget',
                        'the new product materialized reactively');
                } finally {
                    await projection.stop();
                }
            },
        },
        {
            name: '[RDBPROJ03] explicit sync() is idempotent and reports member group ids',
            invoke: async () => {
                const ctx = newCtx();
                const { catalog, orders, rdb } = await makeCatalogAndOrders(ctx);
                const products = await catalog.group.getTable('products');
                await products.insert('p1', { title: 'Widget' }, catalog.admin);

                const target = new MemoryTarget();
                const projection = await RdbProjection.open(rdb, ctx, target);
                try {
                    const ids = projection.memberGroupIds().sort();
                    assertEquals(ids.length, 2, 'both member groups in scope');
                    assertEquals(ids.includes(catalog.group.getId()) && ids.includes(orders.group.getId()), true,
                        'scope contains catalog + orders');

                    const before = target.getRowIds('catalog_products').length;
                    await projection.sync();   // no intervening rdb change
                    assertEquals(target.getRowIds('catalog_products').length, before, 're-sync is idempotent');
                } finally {
                    await projection.stop();
                }
            },
        },
        {
            name: '[RDBPROJ04] push channel: an ingestion failure is delivered via onOpEvents (ingestion always passes the author filter)',
            invoke: async () => {
                const ctx = newCtx();
                const { forum, rdb } = await makeForum(ctx);

                const target = new MemoryTarget({ captureChanges: true });
                const received: OpEvent[] = [];
                // eventAuthors: [] would suppress EVERY concurrency event, yet
                // ingestion failures must still be delivered.
                const projection = await RdbProjection.open(rdb, ctx, target, {
                    writer: forum.admin,
                    eventAuthors: [],
                    onOpEvents: (events) => { received.push(...events); },
                });
                try {
                    // A local comment pointing at a non-existent post: ingestion
                    // rejects it, records an op-event, and reverts the orphan.
                    target.localInsert('forum_comments', { body: 'orphan', post_id: 9999 });
                    await projection.sync();

                    assertEquals(received.length, 1, 'exactly one op-event pushed');
                    assertEquals(received[0].origin, 'ingestion', 'it is an ingestion failure');
                    assertEquals(received[0].direction, 'failure', 'direction failure');
                    assertEquals(received[0].table, 'forum_comments', 'names the group-qualified target table');
                } finally {
                    await projection.stop();
                }
            },
        },
        {
            name: '[RDBPROJ05] apply() logs concurrency void+reinstate to the op-event log; the author filter gates the push',
            invoke: async () => {
                const target = new MemoryTarget();
                const g = 'GROUP' as unknown as B64Hash;
                const v = new Set(['V1']) as unknown as Version;
                const voided: OpEvent = { origin: 'concurrency', direction: 'void', groupId: g, opHash: 'OP1', kind: 'update', author: 'AUTHOR_A' };
                const reinstated: OpEvent = { origin: 'concurrency', direction: 'reinstate', groupId: g, opHash: 'OP1', kind: 'update', author: 'AUTHOR_A' };

                // A projection apply carries the flips in the SAME call as the delta;
                // both directions land in the durable log (idempotent by op+dir).
                await target.apply(g, [], [], v, [voided, reinstated]);
                const stored = await target.drainOpEvents();
                assertEquals(stored.length, 2, 'both a void and a later reinstate of the same op are logged');
                assertEquals(stored[0].event.direction, 'void', 'void first');
                assertEquals(stored[1].event.direction, 'reinstate', 'reinstate second');

                // The author filter: 'all' (default) and a matching allow-list pass;
                // a non-matching allow-list suppresses a concurrency event.
                assertEquals(opEventPushable(voided, 'all'), true, "'all' passes a concurrency event");
                assertEquals(opEventPushable(voided, ['AUTHOR_A']), true, 'a matching author passes');
                assertEquals(opEventPushable(voided, ['AUTHOR_B']), false, 'a non-matching author is suppressed');
                assertEquals(opEventPushable({ ...voided, origin: 'ingestion' }, ['AUTHOR_B']), true,
                    'an ingestion event ALWAYS passes, regardless of the allow-list');
            },
        },
        {
            name: '[RDBPROJ06] a real concurrency void flows projectGroup -> op-event log -> onOpEvents (op hash only, resolvable via loadEntry)',
            invoke: async () => {
                const ctx = newCtx();
                const { g, rdb } = await makeConcurrency(ctx);
                const admin = g.admin;
                const caps = await g.group.getTable('caps');
                const items = await g.group.getTable('items');

                // Grant the cap, then project up to that horizon: the initial
                // checkpoint becomes `base` (cap live, no items).
                await caps.insert('c-1', { label: 'grant' }, admin);

                const target = new MemoryTarget();
                const received: OpEvent[] = [];
                const projection = await RdbProjection.open(rdb, ctx, target, {
                    eventAuthors: 'all',
                    onOpEvents: (events) => { received.push(...events); },
                });
                try {
                    const base = await frontier(g.group);

                    // Concurrent siblings off `base`: revoke the cap AND insert the
                    // gated item. The insert is valid at its parent (cap still
                    // live) but the merged head sees the revoke and voids it.
                    await caps.delete(deriveRowId('c-1', admin.keyId), admin, base);
                    const insertHash = await items.insert('i-1', { name: 'thing' }, admin, base);

                    await projection.sync();

                    const voids = received.filter((e) => e.origin === 'concurrency' && e.direction === 'void');
                    assertEquals(voids.length, 1, 'exactly one concurrency void pushed');
                    assertEquals(voids[0].opHash, insertHash, 'the event carries the real op id (bundle/entry hash)');
                    assertEquals(voids[0].op, undefined, 'a concurrency event stores only the hash (op is fetchable)');
                    assertEquals(voids[0].kind, 'insert', 'the voided op kind');
                    assertEquals(voids[0].reason?.source, 'void', 'a concurrency reason is a structured void detail');
                    if (voids[0].reason?.source === 'void') {
                        assertEquals(voids[0].reason.detail.kind, 'restriction', 'the void was a restriction failure');
                    }

                    // The hash resolves to the real appended op in the DAG.
                    const entry = await (await g.group.getScopedDag()).loadEntry(insertHash);
                    assertTrue(entry !== undefined, 'opHash resolves via loadEntry');

                    // The voided insert never materializes in the target.
                    assertEquals(target.getRowIds('perm_items').length, 0, 'voided row is not projected');
                } finally {
                    await projection.stop();
                }
            },
        },
    ],
};
