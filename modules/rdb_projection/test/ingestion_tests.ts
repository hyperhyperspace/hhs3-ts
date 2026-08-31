// Cross-group ingestion tests for the replica-wide inverse pipeline. These
// exercise the adapter's dirty-map ref-advance: when co-projected groups are
// drained in commit order, an observer group is advanced to a bound group's
// freshly-ingested version so its own writes (cross-group FKs AND exists /
// restriction reads, which carry no FK) validate. A closing drain advances
// observers that never wrote, transitively.
//
// They drive buildScope + projectDatabase + ingestDatabaseChanges directly (no
// reactive supervisor) for deterministic, single-pass assertions.

import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { Version, RContext } from "@hyper-hyper-space/hhs3_mvt";
import type { Predicate, Row, TableDef } from "@hyper-hyper-space/hhs3_rdb";
import {
    RSchemaImpl, rSchemaFactory, RTableGroupImpl, rTableGroupFactory,
} from "@hyper-hyper-space/hhs3_rdb";
import {
    GroupProjection, ingestDatabaseChanges, MemoryTarget, projectDatabase,
} from "@hyper-hyper-space/hhs3_rdb_adapter";

import { createMockRContext } from "../../rdb/test/mock_rcontext.js";
import { buildScope } from "../src/scope.js";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

function newCtx(): RContext {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);
    return ctx;
}

function open(name: string, columns: TableDef['columns'], extra?: Partial<TableDef>): TableDef {
    return { name, columns, restrictions: [{ on: 'all', rule: { p: 'true' } }], ...extra };
}

type MadeGroup = { schema: RSchemaImpl; group: RTableGroupImpl };

async function makeGroup(ctx: RContext, name: string, tables: TableDef[], opts?: {
    bindings?: { [k: string]: B64Hash };
    canObserve?: { [k: string]: Predicate };
}): Promise<MadeGroup> {
    const admin = await makeIdentity();
    const schemaInit = await RSchemaImpl.create({
        name: `${name}:schema`, creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }], tables,
    });
    const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
    const pinned = await (await schema.getScopedDag()).getFrontier();
    const groupInit = await RTableGroupImpl.create({
        name, seed: `${name}-seed`, schemaRef: schema.getId(), schemaVersion: pinned,
        ...(opts?.bindings !== undefined ? { bindings: opts.bindings } : {}),
        ...(opts?.canObserve !== undefined ? { canObserve: opts.canObserve } : {}),
    });
    const group = (await ctx.createObject(groupInit)) as RTableGroupImpl;
    return { schema, group };
}

async function frontier(group: RTableGroupImpl): Promise<Version> {
    return (await group.getScopedDag()).getFrontier();
}

function sameVersion(a: Version | undefined, b: Version): boolean {
    if (a === undefined || a.size !== b.size) return false;
    for (const h of b) if (!a.has(h)) return false;
    return true;
}

async function rdbRows(group: RTableGroupImpl, table: string): Promise<Row[]> {
    return (await (await group.getTable(table)).getView()).query({});
}

export const ingestionTests = {
    title: '[RDBING] rdb_projection cross-group ingestion (dirty-map ref-advance)',
    tests: [
        {
            name: '[RDBING01] a co-projected cross-group FK write validates after an in-pass ref-advance',
            invoke: async () => {
                const ctx = newCtx();
                const catalog = await makeGroup(ctx, 'catalog', [open('products', { title: { type: 'string' } })]);
                const orders = await makeGroup(ctx, 'orders',
                    [open('orders', { item: { type: 'string', nullable: true } }, { fks: { item: 'catalog.products' } })],
                    { bindings: { catalog: catalog.group.getId() } });

                const writer = await makeIdentity();
                const members = await buildScope([catalog.group, orders.group], { writer });
                const target = new MemoryTarget({ captureChanges: true });
                await projectDatabase(members, target);

                // Local: a product, then an order referencing it by its local id.
                // Neither exists in rdb yet - both are ingested in ONE pass, so the
                // order's cross-group FK is only satisfiable if ingestion advances
                // the orders group's `catalog` ref to the product's fresh version.
                const productLocal = target.localInsert('catalog_products', { title: 'Widget' });
                target.localInsert('orders_orders', { item_id: productLocal });

                const results = await ingestDatabaseChanges(members, target);

                const cat = results.get(catalog.group.getId())!;
                const ord = results.get(orders.group.getId())!;
                assertEquals(cat.accepted, 1, 'the product was ingested');
                assertEquals(ord.rejected.length, 0, 'the order was NOT rejected (ref-advance made its FK target live)');
                assertEquals(ord.accepted, 1, 'the order was ingested');

                const product = (await rdbRows(catalog.group, 'products'))[0];
                const order = (await rdbRows(orders.group, 'orders'))[0];
                assertTrue(product !== undefined && order !== undefined, 'both rows landed in rdb');
                assertEquals(order.values.item, product.rowId, 'the order FK resolves to the product rowId');
            },
        },
        {
            name: '[RDBING02] a plain (non-FK) write triggers the ref-advance a cross-group exists needs',
            invoke: async () => {
                const ctx = newCtx();
                // `items` reads `caps` only through an insert restriction (an
                // `exists` over the bound group) - the item write carries NO FK,
                // yet it must still ref-advance `caps` for the exists to hold.
                const caps = await makeGroup(ctx, 'caps', [open('caps', { label: { type: 'string', pub: true } })]);
                const items = await makeGroup(ctx, 'items', [{
                    name: 'items',
                    columns: { name: { type: 'string' } },
                    restrictions: [{ on: 'insert', rule: { p: 'exists', table: 'perm.caps', where: { label: 'grant' } } }],
                }], { bindings: { perm: caps.group.getId() } });

                const writer = await makeIdentity();
                const members = await buildScope([caps.group, items.group], { writer });
                const target = new MemoryTarget({ captureChanges: true });
                await projectDatabase(members, target);

                target.localInsert('caps_caps', { label: 'grant' });
                target.localInsert('items_items', { name: 'thing' });

                const results = await ingestDatabaseChanges(members, target);

                const itemsResult = results.get(items.group.getId())!;
                assertEquals(results.get(caps.group.getId())!.accepted, 1, 'the cap was ingested');
                assertEquals(itemsResult.rejected.length, 0, 'the exists-gated item was NOT rejected');
                assertEquals(itemsResult.accepted, 1, 'the item was ingested (its own write drove the ref-advance)');
                assertEquals((await rdbRows(items.group, 'items')).length, 1, 'the item reached rdb');
            },
        },
        {
            name: '[RDBING03] an ungrantable observe gate surfaces the ref-advance failure as a rejection',
            invoke: async () => {
                const ctx = newCtx();
                const caps = await makeGroup(ctx, 'caps', [open('caps', { label: { type: 'string', pub: true } })]);
                // The `perm` binding is gated by an unsatisfiable predicate: no
                // author can observe caps, so the item's exists can never hold.
                const items = await makeGroup(ctx, 'items', [{
                    name: 'items',
                    columns: { name: { type: 'string' } },
                    restrictions: [{ on: 'insert', rule: { p: 'exists', table: 'perm.caps', where: { label: 'grant' } } }],
                }], { bindings: { perm: caps.group.getId() }, canObserve: { perm: { p: 'false' } } });

                const writer = await makeIdentity();
                const members = await buildScope([caps.group, items.group], { writer });
                const target = new MemoryTarget({ captureChanges: true });
                await projectDatabase(members, target);

                target.localInsert('caps_caps', { label: 'grant' });
                target.localInsert('items_items', { name: 'thing' });

                const results = await ingestDatabaseChanges(members, target);

                const itemsResult = results.get(items.group.getId())!;
                assertEquals(results.get(caps.group.getId())!.accepted, 1, 'the cap still ingested');
                assertEquals(itemsResult.accepted, 0, 'the item did not ingest (its exists cannot hold)');
                assertTrue(itemsResult.rejected.length >= 1, 'the item pass reports a rejection');
                assertTrue(itemsResult.rejected.some((r) => r.reason.includes('ref-advance')),
                    'a ref-advance failure is surfaced');
                assertEquals((await rdbRows(items.group, 'items')).length, 0, 'nothing reached rdb');
            },
        },
        {
            name: '[RDBING04] the end-of-pass drain advances no-write observers transitively (A->B->C)',
            invoke: async () => {
                const ctx = newCtx();
                // C is observed by B; B is observed by A. Only C gets a write; the
                // drain must still advance B->C and then (transitively) A->B, even
                // though B and A wrote nothing this pass.
                const c = await makeGroup(ctx, 'cee', [open('crows', { v: { type: 'string' } })]);
                const b = await makeGroup(ctx, 'bee',
                    [open('brows', { cref: { type: 'string', nullable: true } }, { fks: { cref: 'cbind.crows' } })],
                    { bindings: { cbind: c.group.getId() } });
                const a = await makeGroup(ctx, 'aay',
                    [open('arows', { bref: { type: 'string', nullable: true } }, { fks: { bref: 'bbind.brows' } })],
                    { bindings: { bbind: b.group.getId() } });

                const writer = await makeIdentity();
                const members = await buildScope([a.group, b.group, c.group], { writer });
                const target = new MemoryTarget({ captureChanges: true });
                await projectDatabase(members, target);

                // Only C is written locally.
                target.localInsert('cee_crows', { v: 'x' });
                const results = await ingestDatabaseChanges(members, target);

                assertEquals(results.get(c.group.getId())!.accepted, 1, 'C ingested its row');
                for (const g of [a.group, b.group]) {
                    assertEquals(results.get(g.getId())!.rejected.length, 0, 'no rejections for the no-write observers');
                }

                const cFrontier = await frontier(c.group);
                const bObservesC = await (await b.group.getView()).resolveRefVersion(c.group.getId());
                assertTrue(sameVersion(bObservesC, cFrontier), 'B was advanced to C\'s frontier by the drain');

                const bFrontier = await frontier(b.group);
                const aObservesB = await (await a.group.getView()).resolveRefVersion(b.group.getId());
                assertTrue(sameVersion(aObservesB, bFrontier),
                    'A was advanced transitively to B\'s (post-observe) frontier by the drain');
            },
        },
    ],
};
