import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity, B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { version, Version } from "@hyper-hyper-space/hhs3_mvt";
import type { RContext } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "../mock_rcontext.js";
import { RSchemaImpl, rSchemaFactory } from "../../src/rschema/rschema.js";
import { RTableGroupImpl, rTableGroupFactory } from "../../src/rtable_group/group.js";
import type { RTableImpl } from "../../src/rtable/rtable.js";
import { deriveRowId } from "../../src/rtable/hash.js";
import type { MigrationRule, Predicate, TableDef } from "../../src/rschema/payload.js";

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
// checkpoint; write-time rejections (dangling FK, etc.) are caught and skipped
// without aborting the history. A re-added FK always deploys, deleting the
// lines it strands, so parity covers deploy kills.
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
        name: `parity-group-${seed}`,
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
                // whole-table drop+re-add of `orders` while it holds live rows
                // referenced by `lines`: resets the table incarnation (old order
                // rows go non-live, referencing lines dangle at-use).
                kinds.push('reincarnate-orders');
                const kind = kinds[prng.nextInt(0, kinds.length - 1)];

                if (kind === 'add-status') {
                    await schema.updateSchema([{ rule: 'add-column', table: 'orders', column: 'status', def: { type: 'string', default: 'new' } }], admin, 'add status');
                } else if (kind === 'drop-status') {
                    await schema.updateSchema([{ rule: 'drop-column', table: 'orders', column: 'status' }], admin, 'drop status');
                } else if (kind === 'toggle-cd') {
                    await schema.updateSchema([{ rule: 'set-concurrent-deletes', table: 'lines', value: !cd }], admin, 'toggle cd');
                } else if (kind === 'drop-fk') {
                    await schema.updateSchema([{ rule: 'set-fks', table: 'lines', fks: {} }], admin, 'drop fk');
                } else if (kind === 'add-fk') {
                    await schema.updateSchema([{ rule: 'set-fks', table: 'lines', fks: { order: 'orders' } }], admin, 'add fk');
                } else {
                    await schema.updateSchema([
                        { rule: 'drop-table', table: 'orders' },
                        { rule: 'add-table', def: open('orders', { customer: { type: 'string' } }) },
                    ], admin, 'reincarnate orders');
                }

                const v2 = await schemaFrontier();
                await group.deploy(v2, undefined, at);

                // only reached when the deploy applied
                if (kind === 'add-status') hasStatus = true;
                else if (kind === 'drop-status') hasStatus = false;
                else if (kind === 'toggle-cd') cd = !cd;
                else if (kind === 'drop-fk') hasFk = false;
                else if (kind === 'add-fk') hasFk = true;
                else hasStatus = false;   // reincarnate-orders: fresh base def
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
    const groupBInit = await RTableGroupImpl.create({ name: `parity-xgroupB-${seed}`, seed: `parity-xgroupB-${seed}`, schemaRef: schemaB.getId(), schemaVersion: pinnedB });
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
        name: `parity-xgroupA-${seed}`,
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
        await groupA.observe('B', bFrontier, undefined, at);
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
    // the foreign-group bound to project below the meet, and the walk to lower
    // the bound below a side-branch barrier.
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

// ---------------------------------------------------------------------------
// Gated histories: deletes whose rules read another table, so a concurrent
// delete can void a delete and, through it, every entry that reads its row.
// Deleting a target needs a perm row labelled 'ok'; deleting a perm row needs
// a gate row. That two-level chain lets a verdict flip and flip back across
// versions. items.t is an FK to targets; deploys toggle concurrentDeletes and
// drop / re-adopt that FK (the re-adoption deletes the items it strands).

function newParityContext(): RContext {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);
    return ctx;
}

async function createGroup(
    ctx: RContext, admin: OwnIdentity, name: string, tables: TableDef[], bindings?: { [name: string]: B64Hash },
): Promise<{ group: RTableGroupImpl; schema: RSchemaImpl }> {
    const schema = (await ctx.createObject(await RSchemaImpl.create({
        name: `parity:${name.replace(/-/g, '_')}_schema`,
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        tables,
    }))) as RSchemaImpl;
    const group = (await ctx.createObject(await RTableGroupImpl.create({
        name, seed: name,
        schemaRef: schema.getId(),
        schemaVersion: await (await schema.getScopedDag()).getFrontier(),
        ...(bindings !== undefined ? { bindings } : {}),
    }))) as RTableGroupImpl;
    return { group, schema };
}

async function groupFrontier(group: RTableGroupImpl): Promise<Version> {
    return (await group.getScopedDag()).getFrontier();
}

function gatedDelete(name: string, columns: TableDef['columns'], rule: Predicate): TableDef {
    return {
        name, columns,
        restrictions: [{ on: 'insert', rule: { p: 'true' } }, { on: 'update', rule: { p: 'true' } }, { on: 'delete', rule }],
        concurrentDeletes: true,
    };
}

function gatedTables(): TableDef[] {
    return [
        gatedDelete('perm', { label: { type: 'string', pub: true } }, { p: 'exists', table: 'gate', where: { k: 'g' } }),
        open('gate', { k: { type: 'string', pub: true } }, { concurrentDeletes: true }),
        gatedDelete('targets', { n: { type: 'string' } }, { p: 'exists', table: 'perm', where: { label: 'ok' } }),
        open('items', { t: { type: 'string' } }, { fks: { t: 'targets' }, concurrentDeletes: true }),
        open('notes', { n: { type: 'string' } }),
    ];
}

type GatedGroup = {
    group: RTableGroupImpl;
    schema: RSchemaImpl;
    admin: OwnIdentity;
    tables: { [name: string]: RTableImpl };
    checkpoints: Version[];
    state: { cdTargets: boolean; cdPerm: boolean; itemsFk: boolean };
};

async function createGatedGroup(ctx: RContext, admin: OwnIdentity, name: string): Promise<GatedGroup> {
    const { group, schema } = await createGroup(ctx, admin, name, gatedTables());
    const tables: { [name: string]: RTableImpl } = {};
    for (const table of ['perm', 'gate', 'targets', 'items', 'notes']) tables[table] = await group.getTable(table);
    return {
        group, schema, admin, tables,
        checkpoints: [version(group.getId())],
        state: { cdTargets: true, cdPerm: true, itemsFk: true },
    };
}

// Like pickConcurrentAt, but draws from the last few checkpoints: a branch off
// a recent checkpoint is concurrent with entries that still matter, which is
// what puts an old barrier next to an entry below the fork meet.
function pickRecentAt(prng: PRNG, checkpoints: Version[], rate: number, window = 6): Version | undefined {
    if (checkpoints.length < 2 || prng.next() >= rate) return undefined;
    const lo = Math.max(0, checkpoints.length - 1 - window);
    return checkpoints[prng.nextInt(lo, checkpoints.length - 2)];
}

// One random op on a gated group, at a possibly-concurrent checkpoint. The
// row pools are small so ops collide on the same rows.
async function gatedOp(prng: PRNG, g: GatedGroup): Promise<void> {
    const at = pickRecentAt(prng, g.checkpoints, 0.5);
    const t = g.tables;
    const roll = prng.nextInt(0, 99);
    try {
        if (roll < 10) {
            await t.perm.insert(`p-${prng.nextInt(0, 1)}`, { label: prng.next() < 0.75 ? 'ok' : 'no' }, undefined, at);
        } else if (roll < 16) {
            await t.gate.insert(`g-${prng.nextInt(0, 1)}`, { k: 'g' }, undefined, at);
        } else if (roll < 26) {
            await t.targets.insert(`x-${prng.nextInt(0, 2)}`, { n: 'x' }, undefined, at);
        } else if (roll < 42) {
            const target = prng.next() < 0.85 ? deriveRowId(`x-${prng.nextInt(0, 2)}`) : 'missing';
            await t.items.insert(`i-${prng.nextInt(0, 4)}`, { t: target }, undefined, at);
        } else if (roll < 52) {
            await t.perm.delete(deriveRowId(`p-${prng.nextInt(0, 1)}`), undefined, at);
        } else if (roll < 58) {
            await t.gate.delete(deriveRowId(`g-${prng.nextInt(0, 1)}`), undefined, at);
        } else if (roll < 70) {
            await t.targets.delete(deriveRowId(`x-${prng.nextInt(0, 2)}`), undefined, at);
        } else if (roll < 76) {
            await t.items.delete(deriveRowId(`i-${prng.nextInt(0, 4)}`), undefined, at);
        } else if (roll < 86) {
            await t.notes.insert(`n-${prng.nextInt(0, 1_000_000)}`, { n: 'n' }, undefined, at);
        } else {
            const kind = prng.nextInt(0, 2);
            const rules: MigrationRule[] = kind === 0
                ? [{ rule: 'set-concurrent-deletes', table: 'targets', value: !g.state.cdTargets }]
                : kind === 1
                    ? [{ rule: 'set-concurrent-deletes', table: 'perm', value: !g.state.cdPerm }]
                    : [{ rule: 'set-fks', table: 'items', fks: g.state.itemsFk ? {} : { t: 'targets' } }];
            await g.schema.updateSchema(rules, g.admin, 'toggle');
            if (kind === 0) g.state.cdTargets = !g.state.cdTargets;
            else if (kind === 1) g.state.cdPerm = !g.state.cdPerm;
            else g.state.itemsFk = !g.state.itemsFk;
            await g.group.deploy(await (await g.schema.getScopedDag()).getFrontier(), undefined, at);
        }
    } catch {
        return;   // rejected write / deploy: not recorded
    }
    await recordCheckpoint(g.checkpoints, await groupFrontier(g.group));
}

// One gated group under random ops. Parity is over its own delta.
export async function generateGatedGroupHistory(seed: number, ops: number): Promise<GroupHistory> {
    const prng = new PRNG(seed);
    const ctx = newParityContext();
    const g = await createGatedGroup(ctx, await makeIdentity(), `parity-gated-${seed}`);
    for (let i = 0; i < ops; i++) await gatedOp(prng, g);
    return { group: g.group, rawDag: (await ctx.getDag(g.group.getId()))!, checkpoints: g.checkpoints, seed };
}

// A gated foreign group F under random ops, and an observer G with a
// cross-group FK refs.item -> F.items. G observes F at its frontier or at an
// older F checkpoint, possibly from an older G checkpoint, so side-branch
// observes publish older foreign versions than the main branch. Parity is
// over G's delta.
export async function generateGatedCrossGroupHistory(seed: number, ops: number): Promise<GroupHistory> {
    const prng = new PRNG(seed);
    const ctx = newParityContext();
    const admin = await makeIdentity();
    const foreign = await createGatedGroup(ctx, admin, `parity-gated-f-${seed}`);
    const { group } = await createGroup(ctx, admin, `parity-gated-g-${seed}`, [
        open('refs', { item: { type: 'string' } }, { fks: { item: 'F.items' } }),
        open('notes', { n: { type: 'string' } }),
    ], { F: foreign.group.getId() });
    const refs = await group.getTable('refs');
    const notes = await group.getTable('notes');
    const checkpoints: Version[] = [version(group.getId())];

    for (let i = 0; i < ops; i++) {
        if (prng.next() < 0.5) {
            await gatedOp(prng, foreign);
            continue;
        }
        const at = pickRecentAt(prng, checkpoints, 0.5);
        const roll = prng.nextInt(0, 99);
        try {
            if (roll < 25) {
                await group.observe('F', await groupFrontier(foreign.group), undefined, at);
            } else if (roll < 45) {
                await group.observe('F', foreign.checkpoints[prng.nextInt(0, foreign.checkpoints.length - 1)], undefined, at);
            } else if (roll < 80) {
                await refs.insert(`r-${prng.nextInt(0, 4)}`, { item: deriveRowId(`i-${prng.nextInt(0, 4)}`) }, undefined, at);
            } else if (roll < 90) {
                await refs.delete(deriveRowId(`r-${prng.nextInt(0, 4)}`), undefined, at);
            } else {
                await notes.insert(`n-${prng.nextInt(0, 1_000_000)}`, { n: 'n' }, undefined, at);
            }
        } catch {
            continue;
        }
        await recordCheckpoint(checkpoints, await groupFrontier(group));
    }

    return { group, rawDag: (await ctx.getDag(group.getId()))!, checkpoints, seed };
}

// ---------------------------------------------------------------------------
// Shaped histories: the skeleton of the bounded-delta counterexamples, with
// randomized barriers on it. A random history rarely produces the concurrency
// these need:
//
//     x ── E ── s1 ── N         start = {s1, T}, end = {N, T}
//      \
//       T                       T is concurrent with E and with s1
//
// T is an old barrier on a side branch, E reads what T does, and N is the new
// entry that may change T's effect. Each variant picks the barrier kinds, adds
// 0-2 notes after E and after T, and (usually) makes N change T's effect.
// Checkpoints are the frontier after every append, so the pairs include
// (start, end) as well as the prefixes.

type ShapedBuilder = (prng: PRNG, ctx: RContext, admin: OwnIdentity, name: string) => Promise<{ group: RTableGroupImpl; checkpoints: Version[] }>;

// Records the frontier after each append; `noise` appends 0-2 notes after a tip.
function shapedRecorder(group: RTableGroupImpl, notes: RTableImpl, prng: PRNG) {
    const checkpoints: Version[] = [version(group.getId())];
    const record = async <T>(appended: Promise<T>): Promise<T> => {
        const result = await appended;
        await recordCheckpoint(checkpoints, await groupFrontier(group));
        return result;
    };
    const noise = async (tip: B64Hash): Promise<B64Hash> => {
        for (let i = prng.nextInt(0, 2); i > 0; i--) {
            tip = await record(notes.insert(`noise-${prng.nextInt(0, 1_000_000_000)}`, { n: 'noise' }, undefined, version(tip)));
        }
        return tip;
    };
    return { checkpoints, record, noise };
}

// T deletes a target (rule: some perm row is 'ok'); N deletes that perm row.
const localGatedShape: ShapedBuilder = async (prng, ctx, admin, name) => {
    const { group } = await createGroup(ctx, admin, name, [
        open('perm', { label: { type: 'string', pub: true } }, { concurrentDeletes: true }),
        gatedDelete('targets', { n: { type: 'string' } }, { p: 'exists', table: 'perm', where: { label: 'ok' } }),
        open('items', { t: { type: 'string' } }, { fks: { t: 'targets' } }),
        open('notes', { n: { type: 'string' } }),
    ]);
    const [perm, targets, items, notes] = await Promise.all(['perm', 'targets', 'items', 'notes'].map((t) => group.getTable(t)));
    const { checkpoints, record, noise } = shapedRecorder(group, notes, prng);
    const p = await record(perm.insert('p-1', { label: 'ok' }));
    const x = await record(targets.insert('x-1', { n: 'x' }, undefined, version(p)));
    const E = await record(items.insert('i-1', { t: deriveRowId('x-1') }, undefined, version(x)));
    const s1 = await record(notes.insert('s-1', { n: 's1' }, undefined, version(await noise(E))));
    await noise(await record(targets.delete(deriveRowId('x-1'), undefined, version(x))));
    if (prng.next() < 0.75) await record(perm.delete(deriveRowId('p-1'), undefined, version(s1)));
    else await record(notes.insert('n-1', { n: 'N' }, undefined, version(s1)));
    return { group, checkpoints };
};

// T deletes a target; N deploys a schema that stops honoring concurrent
// deletes on targets, so T no longer reaches E.
const localHonorShape: ShapedBuilder = async (prng, ctx, admin, name) => {
    const { group, schema } = await createGroup(ctx, admin, name, [
        open('targets', { n: { type: 'string' } }, { concurrentDeletes: true }),
        open('items', { t: { type: 'string' } }, { fks: { t: 'targets' } }),
        open('notes', { n: { type: 'string' } }),
    ]);
    const [targets, items, notes] = await Promise.all(['targets', 'items', 'notes'].map((t) => group.getTable(t)));
    const pinned = await (await schema.getScopedDag()).getFrontier();
    const cdOff = await schema.updateSchema([{ rule: 'set-concurrent-deletes', table: 'targets', value: false }], admin, 'cd off', pinned);
    const addColumn = await schema.updateSchema([{ rule: 'add-column', table: 'notes', column: 'extra', def: { type: 'string', default: 'd' } }], admin, 'add column', pinned);
    const { checkpoints, record, noise } = shapedRecorder(group, notes, prng);
    const x = await record(targets.insert('x-1', { n: 'x' }));
    const E = await record(items.insert('i-1', { t: deriveRowId('x-1') }, undefined, version(x)));
    const s1 = await record(notes.insert('s-1', { n: 's1' }, undefined, version(await noise(E))));
    await noise(await record(targets.delete(deriveRowId('x-1'), undefined, version(x))));
    await record(group.deploy(version(prng.next() < 0.75 ? cdOff : addColumn), undefined, version(s1)));
    return { group, checkpoints };
};

// T is a schema deploy that adopts an FK and so deletes the item it strands;
// E references that item. Optionally the same deploy also lands on the main
// branch after E. N stops honoring concurrent deletes on items.
const deployKillShape: ShapedBuilder = async (prng, ctx, admin, name) => {
    const { group, schema } = await createGroup(ctx, admin, name, [
        open('items', { t: { type: 'string' } }, { concurrentDeletes: true }),
        open('targets', { n: { type: 'string' } }),
        open('refs', { item: { type: 'string' } }, { fks: { item: 'items' } }),
        open('notes', { n: { type: 'string' } }),
    ]);
    const [items, refs, notes] = await Promise.all(['items', 'refs', 'notes'].map((t) => group.getTable(t)));
    const pinned = await (await schema.getScopedDag()).getFrontier();
    const adopt = await schema.updateSchema([{ rule: 'set-fks', table: 'items', fks: { t: 'targets' } }], admin, 'adopt fk', pinned);
    const cdOff = await schema.updateSchema([{ rule: 'set-concurrent-deletes', table: 'items', value: false }], admin, 'cd off', version(adopt));
    const { checkpoints, record, noise } = shapedRecorder(group, notes, prng);
    const R = await record(items.insert('r-1', { t: 'missing' }));
    let tip = await record(refs.insert('e-1', { item: deriveRowId('r-1') }, undefined, version(R)));
    if (prng.next() < 0.5) tip = await record(group.deploy(version(adopt), undefined, version(tip)));
    const s1 = await record(notes.insert('s-1', { n: 's1' }, undefined, version(await noise(tip))));
    await noise(await record(group.deploy(version(adopt), undefined, version(R))));
    if (prng.next() < 0.75) await record(group.deploy(version(cdOff), undefined, version(s1)));
    else await record(notes.insert('n-1', { n: 'N' }, undefined, version(s1)));
    return { group, checkpoints };
};

// Cross-group: E's insert requires a 'grant' cap in the foreign group; T
// observes a foreign version where the cap still stands; N observes the
// foreign revoke, which is concurrent with T's version. T stays live.
const observeShape: ShapedBuilder = async (prng, ctx, admin, name) => {
    const { group: users } = await createGroup(ctx, admin, name + '-users', [
        open('caps', { label: { type: 'string', pub: true } }, { concurrentDeletes: true }),
        open('fill', { n: { type: 'string' } }),
    ]);
    const caps = await users.getTable('caps');
    const fill = await users.getTable('fill');
    const grant = await caps.insert('c-1', { label: 'grant' });
    const filled = await fill.insert('w', { n: 'w' }, undefined, version(grant));
    const revoke = await caps.delete(deriveRowId('c-1'), undefined, version(grant));

    const { group } = await createGroup(ctx, admin, name, [
        { name: 'items', columns: { n: { type: 'string' } }, restrictions: [{ on: 'insert', rule: { p: 'exists', table: 'users.caps', where: { label: 'grant' } } }] },
        open('notes', { n: { type: 'string' } }),
    ], { users: users.getId() });
    const [items, notes] = await Promise.all(['items', 'notes'].map((t) => group.getTable(t)));
    const { checkpoints, record, noise } = shapedRecorder(group, notes, prng);
    const x = await record(group.observe('users', version(grant)));
    const E = await record(items.insert('i-1', { n: 'item' }, undefined, version(x)));
    const s1 = await record(notes.insert('s-1', { n: 's1' }, undefined, version(await noise(E))));
    await noise(await record(group.observe('users', version(filled), undefined, version(x))));
    await record(group.observe('users', version(prng.next() < 0.75 ? revoke : filled), undefined, version(s1)));
    return { group, checkpoints };
};

// Cross-group, two levels: the foreign delete T_F is live, then void, then
// live again across the three foreign versions v1, v2, v3. G's E reads an
// item T_F's target supports. The meet observes v1, the side branch T
// observes v2 (so start reads v2), and N observes v3.
const intermediateVersionShape: ShapedBuilder = async (prng, ctx, admin, name) => {
    const foreign = await createGatedGroup(ctx, admin, name + '-f');
    const t = foreign.tables;
    const p = await t.perm.insert('p-1', { label: 'ok' });
    const g = await t.gate.insert('g-1', { k: 'g' }, undefined, version(p));
    const fx = await t.targets.insert('x-1', { n: 'x' }, undefined, version(g));
    const fE = await t.items.insert('i-1', { t: deriveRowId('x-1') }, undefined, version(fx));
    const fs1 = await t.notes.insert('s-1', { n: 's1' }, undefined, version(fE));
    const fT = await t.targets.delete(deriveRowId('x-1'), undefined, version(fx));
    const fN1 = await t.perm.delete(deriveRowId('p-1'), undefined, version(fs1));
    const fN3 = await t.gate.delete(deriveRowId('g-1'), undefined, version(fs1));
    const v1 = version(fs1, fT), v2 = version(fN1, fT), v3 = version(fN1, fN3, fT);

    const { group } = await createGroup(ctx, admin, name, [
        open('refs', { item: { type: 'string' } }, { fks: { item: 'F.items' } }),
        open('notes', { n: { type: 'string' } }),
    ], { F: foreign.group.getId() });
    const [refs, notes] = await Promise.all(['refs', 'notes'].map((t) => group.getTable(t)));
    const { checkpoints, record, noise } = shapedRecorder(group, notes, prng);
    const x = await record(group.observe('F', version(fs1)));
    const E = await record(refs.insert('r-1', { item: deriveRowId('i-1') }, undefined, version(x)));
    const meetObserve = await record(group.observe('F', v1, undefined, version(await noise(E))));
    const s1 = await record(notes.insert('s-1', { n: 's1' }, undefined, version(meetObserve)));
    await noise(await record(group.observe('F', v2, undefined, version(E))));
    await record(group.observe('F', prng.next() < 0.75 ? v3 : v2, undefined, version(s1)));
    return { group, checkpoints };
};

const SHAPES: [string, ShapedBuilder][] = [
    ['local-gated', localGatedShape],
    ['local-honor', localHonorShape],
    ['deploy-kill', deployKillShape],
    ['observe', observeShape],
    ['intermediate-version', intermediateVersionShape],
];

// One history per shape for this seed. `ops` is unused: the shapes are fixed.
export async function generateShapedHistories(seed: number, _ops: number): Promise<GroupHistory[]> {
    const histories: GroupHistory[] = [];
    for (const [index, [shape, build]] of SHAPES.entries()) {
        const prng = new PRNG(seed * 31 + index);
        const ctx = newParityContext();
        const { group, checkpoints } = await build(prng, ctx, await makeIdentity(), `parity-${shape}-${seed}`);
        histories.push({ group, rawDag: (await ctx.getDag(group.getId()))!, checkpoints, seed });
    }
    return histories;
}
