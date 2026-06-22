import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity, B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { version, Version } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "../mock_rcontext.js";
import { RSchemaImpl, rSchemaFactory } from "../../src/rschema/rschema.js";
import { RTableGroupImpl, rTableGroupFactory } from "../../src/rtable_group/group.js";
import { deriveRowId } from "../../src/rtable/hash.js";
import type { TableDef } from "../../src/rschema/payload.js";

import { pickConcurrentAt, recordCheckpoint } from "./checkpoints.js";
import { PRNG } from "./prng.js";
import type { GroupHistory } from "./parity.js";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

// open table: unauthored ops never void on restrictions, isolating FK / delete
// liveness and schema effects.
function open(name: string, columns: TableDef['columns'], extra?: Partial<TableDef>): TableDef {
    return { name, columns, restrictions: [{ on: 'all', rule: { p: 'true' } }], ...extra };
}

const ORDER_POOL = 8;
const LINE_POOL = 12;

function orderUuid(i: number): string { return `o-${i}`; }
function lineUuid(i: number): string { return `l-${i}`; }
function orderRowId(i: number): B64Hash { return deriveRowId(orderUuid(i)); }
function lineRowId(i: number): B64Hash { return deriveRowId(lineUuid(i)); }

async function liveOrders(group: RTableGroupImpl, at: Version): Promise<number[]> {
    const view = await (await group.getView(at, at)).getTableView('orders');
    const live: number[] = [];
    for (let i = 0; i < ORDER_POOL; i++) {
        if (await view.hasRow(orderRowId(i))) live.push(i);
    }
    return live;
}

async function liveLines(group: RTableGroupImpl, at: Version): Promise<number[]> {
    const view = await (await group.getView(at, at)).getTableView('lines');
    const live: number[] = [];
    for (let i = 0; i < LINE_POOL; i++) {
        if (await view.hasRow(lineRowId(i))) live.push(i);
    }
    return live;
}

function pick<T>(prng: PRNG, arr: T[]): T | undefined {
    if (arr.length === 0) return undefined;
    return arr[prng.nextInt(0, arr.length - 1)];
}

// One group with two tables (orders, lines order->orders FK) exercised by a
// random history of rows / updates / deletes (incl. FK-target deletes) /
// bundles / schema deploys (add-column-default, drop-column, drop/re-add FK,
// set-concurrent-deletes). Every op is attempted at a possibly-concurrent
// checkpoint; write-time rejections (dangling FK, add-fk prerequisite, etc.)
// are caught and skipped without aborting the history.
export async function generateSingleGroupHistory(seed: number, ops: number): Promise<GroupHistory> {
    const prng = new PRNG(seed);
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);

    const admin = await makeIdentity();
    const schemaInit = await RSchemaImpl.create({
        name: `parity:schema_${seed}`,
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        tables: [
            open('orders', { customer: { type: 'string' } }),
            open('lines', { order: { type: 'string' }, qty: { type: 'integer' } }, { fks: { order: 'orders' } }),
        ],
    });
    const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
    const pinned = await (await schema.getScopedDag()).getFrontier();

    const groupInit = await RTableGroupImpl.create({
        seed: `parity-group-${seed}`,
        schemaRef: schema.getId(),
        schemaVersion: pinned,
    });
    const group = (await ctx.createObject(groupInit)) as RTableGroupImpl;
    const rawDag = (await ctx.getDag(group.getId()))!;

    const orders = await group.getTable('orders');
    const lines = await group.getTable('lines');

    const checkpoints: Version[] = [version(group.getId())];

    // tracked schema state for choosing valid deploys
    let hasStatus = false;
    let hasFk = true;
    let cd = true;

    const groupFrontier = async () => (await group.getScopedDag()).getFrontier();
    const schemaFrontier = async () => (await schema.getScopedDag()).getFrontier();

    for (let opIndex = 0; opIndex < ops; opIndex++) {
        const at = pickConcurrentAt(prng, checkpoints);
        const atV = at ?? await groupFrontier();
        const roll = prng.nextInt(0, 99);

        try {
            if (roll < 22) {
                // insert order
                const i = prng.nextInt(0, ORDER_POOL - 1);
                if (await (await (await group.getView(atV, atV)).getTableView('orders')).hasRow(orderRowId(i))) continue;
                const vals: { [c: string]: string } = { customer: `c-${prng.nextInt(0, 5)}` };
                if (hasStatus && prng.next() < 0.5) vals['status'] = `s-${prng.nextInt(0, 3)}`;
                await orders.insert(orderUuid(i), vals, undefined, at);
            } else if (roll < 44) {
                // insert line referencing a live order (FK)
                const live = await liveOrders(group, atV);
                const target = pick(prng, live);
                if (target === undefined) continue;
                const i = prng.nextInt(0, LINE_POOL - 1);
                if (await (await (await group.getView(atV, atV)).getTableView('lines')).hasRow(lineRowId(i))) continue;
                await lines.insert(lineUuid(i), { order: orderRowId(target), qty: prng.nextInt(1, 9) }, undefined, at);
            } else if (roll < 56) {
                // update order customer
                const target = pick(prng, await liveOrders(group, atV));
                if (target === undefined) continue;
                await orders.update(orderRowId(target), { customer: `c-${prng.nextInt(0, 5)}` }, undefined, at);
            } else if (roll < 66) {
                // update line qty
                const target = pick(prng, await liveLines(group, atV));
                if (target === undefined) continue;
                await lines.update(lineRowId(target), { qty: prng.nextInt(1, 9) }, undefined, at);
            } else if (roll < 76) {
                // delete order (FK-target delete -> may void referencing lines at-use)
                const target = pick(prng, await liveOrders(group, atV));
                if (target === undefined) continue;
                await orders.delete(orderRowId(target), undefined, at);
            } else if (roll < 84) {
                // delete line
                const target = pick(prng, await liveLines(group, atV));
                if (target === undefined) continue;
                await lines.delete(lineRowId(target), undefined, at);
            } else if (roll < 92) {
                // bundle: atomic order + line (line refs the in-bundle order)
                const oi = prng.nextInt(0, ORDER_POOL - 1);
                const li = prng.nextInt(0, LINE_POOL - 1);
                const gv = await group.getView(atV, atV);
                if (await (await gv.getTableView('orders')).hasRow(orderRowId(oi))) continue;
                if (await (await gv.getTableView('lines')).hasRow(lineRowId(li))) continue;
                await group.bundle([
                    { table: 'orders', op: { action: 'insert', rowId: orderRowId(oi), uuid: orderUuid(oi), values: { customer: `c-${prng.nextInt(0, 5)}` } } },
                    { table: 'lines', op: { action: 'insert', rowId: lineRowId(li), uuid: lineUuid(li), values: { order: orderRowId(oi), qty: prng.nextInt(1, 9) } } },
                ], undefined, at);
            } else {
                // schema deploy (barrier ref-advance)
                const kinds: string[] = [];
                if (!hasStatus) kinds.push('add-status'); else kinds.push('drop-status');
                kinds.push('toggle-cd');
                if (hasFk) kinds.push('drop-fk'); else kinds.push('add-fk');
                const kind = kinds[prng.nextInt(0, kinds.length - 1)];

                if (kind === 'add-status') {
                    await schema.updateSchema([{ rule: 'add-column', table: 'orders', column: 'status', def: { type: 'string', default: 'new' } }], admin, 'add status');
                } else if (kind === 'drop-status') {
                    await schema.updateSchema([{ rule: 'drop-column', table: 'orders', column: 'status' }], admin, 'drop status');
                } else if (kind === 'toggle-cd') {
                    await schema.updateSchema([{ rule: 'set-concurrent-deletes', table: 'lines', value: !cd }], admin, 'toggle cd');
                } else if (kind === 'drop-fk') {
                    await schema.updateSchema([{ rule: 'set-fks', table: 'lines', fks: {} }], admin, 'drop fk');
                } else {
                    await schema.updateSchema([{ rule: 'set-fks', table: 'lines', fks: { order: 'orders' } }], admin, 'add fk');
                }

                const v2 = await schemaFrontier();
                await group.deploy(v2, undefined, at);   // may throw (add-fk prerequisite) -> caught

                // only reached when the deploy applied
                if (kind === 'add-status') hasStatus = true;
                else if (kind === 'drop-status') hasStatus = false;
                else if (kind === 'toggle-cd') cd = !cd;
                else if (kind === 'drop-fk') hasFk = false;
                else hasFk = true;
            }
        } catch {
            continue;   // invalid attempt (rejected write/deploy): not recorded
        }

        await recordCheckpoint(checkpoints, await groupFrontier());
    }

    return { group, rawDag, checkpoints, seed };
}

// Cross-group: a dependent group A binds and observes a foreign group B, with a
// cross-group FK A.lines.order -> B.orders. Concurrent B-side deletes and A-side
// deploys, combined with observe ref-advances, exercise the multi-observer
// revision bound (GLB over A's schema AND B). Parity is over A's delta.
export async function generateCrossGroupHistory(seed: number, ops: number): Promise<GroupHistory> {
    const prng = new PRNG(seed);
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);

    const admin = await makeIdentity();

    // foreign schema/group B: orders
    const schemaBInit = await RSchemaImpl.create({
        name: `parity:xschemaB_${seed}`,
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        tables: [open('orders', { customer: { type: 'string' } })],
    });
    const schemaB = (await ctx.createObject(schemaBInit)) as RSchemaImpl;
    const pinnedB = await (await schemaB.getScopedDag()).getFrontier();
    const groupBInit = await RTableGroupImpl.create({ seed: `parity-xgroupB-${seed}`, schemaRef: schemaB.getId(), schemaVersion: pinnedB });
    const groupB = (await ctx.createObject(groupBInit)) as RTableGroupImpl;
    const bDag = (await ctx.getDag(groupB.getId()))!;
    const ordersB = await groupB.getTable('orders');

    // dependent schema/group A: lines with a cross-group FK to B.orders
    const schemaAInit = await RSchemaImpl.create({
        name: `parity:xschemaA_${seed}`,
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        tables: [open('lines', { order: { type: 'string' }, qty: { type: 'integer' } }, { fks: { order: 'B.orders' } })],
    });
    const schemaA = (await ctx.createObject(schemaAInit)) as RSchemaImpl;
    const pinnedA = await (await schemaA.getScopedDag()).getFrontier();
    const groupAInit = await RTableGroupImpl.create({
        seed: `parity-xgroupA-${seed}`,
        schemaRef: schemaA.getId(),
        schemaVersion: pinnedA,
        bindings: { B: groupB.getId() },
    });
    const groupA = (await ctx.createObject(groupAInit)) as RTableGroupImpl;
    const aDag = (await ctx.getDag(groupA.getId()))!;
    const linesA = await groupA.getTable('lines');

    const checkpoints: Version[] = [version(groupA.getId())];

    const observeLatestB = async (at?: Version) => {
        const bFrontier = await bDag.getFrontier();
        await groupA.observe('B', bFrontier, at);
    };

    // seed a couple of B orders and observe them so A can reference them
    for (let i = 0; i < 3; i++) {
        await ordersB.insert(orderUuid(i), { customer: `c-${i}` });
    }
    await observeLatestB();
    await recordCheckpoint(checkpoints, await aDag.getFrontier());

    let cd = true;

    // A-side ops occasionally branch off an older A checkpoint, so a concurrent
    // observe / deploy barrier sits below the fork meet — that is what forces
    // BOTH the schema bound AND the foreign-group bound to project below the
    // meet, exercising combineObserverRevisionBounds' GLB.
    for (let opIndex = 0; opIndex < ops; opIndex++) {
        const roll = prng.nextInt(0, 99);
        const at = pickConcurrentAt(prng, checkpoints);
        const atV = at ?? await aDag.getFrontier();

        try {
            if (roll < 22) {
                // B: insert an order, then A observes the new B frontier (at a
                // possibly-concurrent A position -> a barrier observe)
                const i = prng.nextInt(0, ORDER_POOL - 1);
                if (!(await (await groupB.getView()).getTableView('orders')).hasRow(orderRowId(i))) {
                    await ordersB.insert(orderUuid(i), { customer: `c-${prng.nextInt(0, 5)}` });
                }
                await observeLatestB(at);
            } else if (roll < 42) {
                // B: delete an order (foreign FK-target delete), then A observes
                const bv = await groupB.getView();
                const liveB: number[] = [];
                for (let i = 0; i < ORDER_POOL; i++) if (await (await bv.getTableView('orders')).hasRow(orderRowId(i))) liveB.push(i);
                const target = pick(prng, liveB);
                if (target === undefined) continue;
                await ordersB.delete(orderRowId(target));
                await observeLatestB(at);
            } else if (roll < 70) {
                // A: insert a line referencing a B order observed-live at atV
                const av = await groupA.getView(atV, atV);
                const linesView = await av.getTableView('lines');
                const liveTargets: number[] = [];
                const fview = await groupA.resolveForeignTableView('B', 'orders', atV, atV);
                if (fview !== undefined) {
                    for (let i = 0; i < ORDER_POOL; i++) if (await fview.hasRow(orderRowId(i))) liveTargets.push(i);
                }
                const target = pick(prng, liveTargets);
                if (target === undefined) continue;
                const li = prng.nextInt(0, LINE_POOL - 1);
                if (await linesView.hasRow(lineRowId(li))) continue;
                await linesA.insert(lineUuid(li), { order: orderRowId(target), qty: prng.nextInt(1, 9) }, undefined, at);
            } else if (roll < 85) {
                // A: delete a line at atV
                const linesView = await (await groupA.getView(atV, atV)).getTableView('lines');
                const liveL: number[] = [];
                for (let i = 0; i < LINE_POOL; i++) if (await linesView.hasRow(lineRowId(i))) liveL.push(i);
                const target = pick(prng, liveL);
                if (target === undefined) continue;
                await linesA.delete(lineRowId(target), undefined, at);
            } else {
                // A: deploy a set-concurrent-deletes toggle (local barrier) at atV
                await schemaA.updateSchema([{ rule: 'set-concurrent-deletes', table: 'lines', value: !cd }], admin, 'toggle cd');
                const v2 = await (await schemaA.getScopedDag()).getFrontier();
                await groupA.deploy(v2, undefined, at);
                cd = !cd;
            }
        } catch {
            continue;
        }

        await recordCheckpoint(checkpoints, await aDag.getFrontier());
    }

    return { group: groupA, rawDag: aDag, checkpoints, seed };
}
