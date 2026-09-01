import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity, B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { dag } from "@hyper-hyper-space/hhs3_dag";
import { version, Version } from "@hyper-hyper-space/hhs3_mvt";
import {
    RSchemaImpl, rSchemaFactory, RTableGroupImpl, rTableGroupFactory,
    deriveRowId, TableDef, MigrationRule, ColumnDef,
} from "@hyper-hyper-space/hhs3_rdb";

import { createMockRContext } from "./mock_rcontext.js";
import { pickConcurrentAt, recordCheckpoint } from "./checkpoints.js";
import { PRNG } from "./prng.js";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

function open(name: string, columns: TableDef['columns'], extra?: Partial<TableDef>): TableDef {
    return { name, columns, restrictions: [{ on: 'all', rule: { p: 'true' } }], ...extra };
}

const ORDER_POOL = 8;
const LINE_POOL = 12;
const TAG_POOL = 4;
const ITEM_POOL = 4;

function orderUuid(i: number): string { return `o-${i}`; }
function lineUuid(i: number): string { return `l-${i}`; }
function tagUuid(i: number): string { return `t-${i}`; }
function itemUuid(i: number): string { return `i-${i}`; }
function orderRowId(i: number): B64Hash { return deriveRowId(orderUuid(i)); }
function lineRowId(i: number): B64Hash { return deriveRowId(lineUuid(i)); }
function tagRowId(i: number): B64Hash { return deriveRowId(tagUuid(i)); }
function itemRowId(i: number): B64Hash { return deriveRowId(itemUuid(i)); }

export type Tally = { accepted: number; rejected: number };
export type Tallies = { [kind: string]: Tally };

export type ProjectHistory = {
    group: RTableGroupImpl;
    schema: RSchemaImpl;
    rawDag: dag.Dag;
    checkpoints: Version[];
    seed: number;
    opLog: string[];
    tallies: Tallies;
};

// Pathological schema kinds that are first-class menu entries AND injected on
// a seeded schedule so they occur even at small `ops`. Tallied as `schema-<kind>`.
export const PATHOLOGICAL_KINDS = [
    'reincarnate-orders-table',
    'reincarnate-orders-reshaped',
    'reincarnate-status-column',
    'concurrent-identical-add-column',
    'concurrent-identical-add-table',
    'concurrent-different-add-column',
    'concurrent-different-add-table',
    'toggle-cd-concurrent-delete',
] as const;

export type PathologicalKind = typeof PATHOLOGICAL_KINDS[number];

function pick<T>(prng: PRNG, arr: T[]): T | undefined {
    if (arr.length === 0) return undefined;
    return arr[prng.nextInt(0, arr.length - 1)];
}

function bump(tallies: Tallies, kind: string, which: 'accepted' | 'rejected'): void {
    const cur = tallies[kind] ?? { accepted: 0, rejected: 0 };
    cur[which]++;
    tallies[kind] = cur;
}

export function mergeTallies(into: Tallies, add: Tallies): void {
    for (const [kind, tally] of Object.entries(add)) {
        const cur = into[kind] ?? { accepted: 0, rejected: 0 };
        cur.accepted += tally.accepted;
        cur.rejected += tally.rejected;
        into[kind] = cur;
    }
}

export function assertPathologicalCoverage(tallies: Tallies, context: string): void {
    const missing: string[] = [];
    for (const kind of PATHOLOGICAL_KINDS) {
        if ((tallies['schema-' + kind]?.accepted ?? 0) < 1) missing.push(kind);
    }
    if (missing.length > 0) {
        throw new Error(
            `${context}: pathological coverage floor failed, missing ${missing.join(', ')} `
            + `tallies=${JSON.stringify(tallies)}`,
        );
    }
}

async function liveIds(
    group: RTableGroupImpl, table: string, at: Version, pool: number, rowId: (i: number) => B64Hash,
): Promise<number[]> {
    if (!(await group.getView(at, at)).getTableNames().includes(table)) return [];
    const view = await (await group.getView(at, at)).getTableView(table);
    const live: number[] = [];
    for (let i = 0; i < pool; i++) {
        if (await view.hasRow(rowId(i))) live.push(i);
    }
    return live;
}

function acceptedOf(tallies: Tallies, ...kinds: string[]): number {
    let n = 0;
    for (const k of kinds) n += tallies[k]?.accepted ?? 0;
    return n;
}

function shuffle<T>(prng: PRNG, arr: readonly T[]): T[] {
    const a = [...arr];
    for (let i = a.length - 1; i > 0; i--) {
        const j = prng.nextInt(0, i);
        const tmp = a[i];
        a[i] = a[j];
        a[j] = tmp;
    }
    return a;
}

// Place every pathological kind at a deterministic late op index (leave the
// first few ops for row traffic). If `ops` is smaller than the menu, drop the
// tail of the shuffled list.
function forcedRareAt(ops: number, prng: PRNG): Map<number, PathologicalKind> {
    const kinds = shuffle(prng, PATHOLOGICAL_KINDS);
    const reserved = Math.min(kinds.length, Math.max(0, ops - 6));
    const start = ops - reserved;
    const map = new Map<number, PathologicalKind>();
    for (let i = 0; i < reserved; i++) map.set(start + i, kinds[i]);
    return map;
}

type SchemaState = {
    hasStatus: boolean;
    hasFk: boolean;
    hasOrderColumn: boolean;
    hasTags: boolean;
    hasItems: boolean;
    hasNote: boolean;
    memo: 'absent' | 'string' | 'integer';
    concurrentDeletes: boolean;
};

const STATUS_DEF: ColumnDef = { type: 'string', default: 'new' };

// Seeded orders/lines(+optional tags/items) history with schema deploys that
// stress projection: add/drop defaulted columns, set-fks flips, FK-column
// drop+re-add, type change via drop+add, add/drop table, same-shape and
// reshaped table reincarnation, column reincarnation, concurrent identical /
// different adds, concurrentDeletes paired with a concurrent delete.
// Concurrent `at` ~30%. Pathological kinds are also forced on a seeded schedule.
// `onOp` is called once per attempted op (accepted or rejected) so a caller
// can tick progress during the expensive selfValidate loop.
export async function generateProjectHistory(
    seed: number, ops: number, onOp?: () => void,
): Promise<ProjectHistory> {
    const prng = new PRNG(seed);
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);

    const admin = await makeIdentity();
    const schemaInit = await RSchemaImpl.create({
        name: `planner:schema_${seed}`,
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        tables: [
            open('orders', { customer: { type: 'string' } }),
            open('lines', { order: { type: 'string' }, qty: { type: 'integer' } }, { fks: { order: 'orders' } }),
        ],
    });
    const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
    const pinned = await (await schema.getScopedDag()).getFrontier();

    const groupInit = await RTableGroupImpl.create({
        name: `planner-group-${seed}`,
        seed: `planner-group-${seed}`,
        schemaRef: schema.getId(),
        schemaVersion: pinned,
    });
    const group = (await ctx.createObject(groupInit)) as RTableGroupImpl;
    const rawDag = (await ctx.getDag(group.getId()))!;

    const checkpoints: Version[] = [version(group.getId())];
    const opLog: string[] = [];
    const tallies: Tallies = {};
    const forced = forcedRareAt(ops, new PRNG(seed ^ 0x51ed));

    const st: SchemaState = {
        hasStatus: false,
        hasFk: true,
        hasOrderColumn: true,
        hasTags: false,
        hasItems: false,
        hasNote: false,
        memo: 'absent',
        concurrentDeletes: false,
    };

    const groupFrontier = async () => (await group.getScopedDag()).getFrontier();
    const schemaFrontier = async () => (await schema.getScopedDag()).getFrontier();

    const deploy = async (migration: MigrationRule[], note: string, at: Version | undefined): Promise<void> => {
        await schema.updateSchema(migration, admin, note);
        const v2 = await schemaFrontier();
        await group.deploy(v2, undefined, at);
    };

    const concurrentAdds = async (
        migA: MigrationRule[], noteA: string,
        migB: MigrationRule[], noteB: string,
        groupAt: Version | undefined,
    ): Promise<{ ha: B64Hash; hb: B64Hash; deployA: B64Hash; deployB: B64Hash }> => {
        const schemaBase = await schemaFrontier();
        const groupBase = groupAt ?? await groupFrontier();
        const ha = await schema.updateSchema(migA, admin, noteA, schemaBase);
        const hb = await schema.updateSchema(migB, admin, noteB, schemaBase);
        const deployA = await group.deploy(version(ha), undefined, groupBase);
        const deployB = await group.deploy(version(hb), undefined, groupBase);
        return { ha, hb, deployA, deployB };
    };

    const applySchemaKind = async (schemaKind: string, at: Version | undefined, atV: Version, opIndex: number): Promise<string> => {
        if (schemaKind === 'add-status') {
            await deploy([{ rule: 'add-column', table: 'orders', column: 'status', def: STATUS_DEF }], 'add status', at);
            st.hasStatus = true;
        } else if (schemaKind === 'drop-status') {
            await deploy([{ rule: 'drop-column', table: 'orders', column: 'status' }], 'drop status', at);
            st.hasStatus = false;
        } else if (schemaKind === 'drop-fk') {
            await deploy([{ rule: 'set-fks', table: 'lines', fks: {} }], 'drop fk', at);
            st.hasFk = false;
        } else if (schemaKind === 'add-fk') {
            await deploy([{ rule: 'set-fks', table: 'lines', fks: { order: 'orders' } }], 'add fk', at);
            st.hasFk = true;
        } else if (schemaKind === 'recreate-order-plain' || schemaKind === 'recreate-order-fk') {
            const rules: MigrationRule[] = [
                { rule: 'set-fks', table: 'lines', fks: {} },
                { rule: 'drop-column', table: 'lines', column: 'order' },
                { rule: 'add-column', table: 'lines', column: 'order', def: { type: 'string', nullable: true } },
            ];
            if (schemaKind === 'recreate-order-fk') rules.push({ rule: 'set-fks', table: 'lines', fks: { order: 'orders' } });
            await deploy(rules, schemaKind, at);
            st.hasOrderColumn = true;
            st.hasFk = schemaKind === 'recreate-order-fk';
        } else if (schemaKind === 'add-order-plain' || schemaKind === 'add-order-fk') {
            const rules: MigrationRule[] = [
                { rule: 'add-column', table: 'lines', column: 'order', def: { type: 'string', nullable: true } },
            ];
            if (schemaKind === 'add-order-fk') rules.push({ rule: 'set-fks', table: 'lines', fks: { order: 'orders' } });
            await deploy(rules, schemaKind, at);
            st.hasOrderColumn = true;
            st.hasFk = schemaKind === 'add-order-fk';
        } else if (schemaKind === 'reincarnate-orders-table') {
            await deploy([
                { rule: 'drop-table', table: 'orders' },
                { rule: 'add-table', def: open('orders', { customer: { type: 'string' } }) },
            ], 'reincarnate orders', at);
            st.hasStatus = false;
            st.hasNote = false;
            st.memo = 'absent';
        } else if (schemaKind === 'reincarnate-orders-reshaped') {
            await deploy([
                { rule: 'drop-table', table: 'orders' },
                { rule: 'add-table', def: open('orders', {
                    customer: { type: 'string' },
                    note: { type: 'string', nullable: true },
                }) },
            ], 'reincarnate orders reshaped', at);
            st.hasStatus = false;
            st.hasNote = true;
            st.memo = 'absent';
        } else if (schemaKind === 'reincarnate-status-column') {
            if (!st.hasStatus) {
                await deploy([{ rule: 'add-column', table: 'orders', column: 'status', def: STATUS_DEF }], 'add status before reincarnate', at);
                st.hasStatus = true;
            }
            await deploy([
                { rule: 'drop-column', table: 'orders', column: 'status' },
                { rule: 'add-column', table: 'orders', column: 'status', def: STATUS_DEF },
            ], 'reincarnate status', at);
            st.hasStatus = true;
        } else if (schemaKind === 'concurrent-identical-add-column') {
            if (st.hasStatus) {
                await deploy([{ rule: 'drop-column', table: 'orders', column: 'status' }], 'drop status for concurrent re-add', at);
                st.hasStatus = false;
            }
            const def: ColumnDef = { type: 'string', default: 'same' };
            await concurrentAdds(
                [{ rule: 'add-column', table: 'orders', column: 'status', def }], 'add status A',
                [{ rule: 'add-column', table: 'orders', column: 'status', def }], 'add status B',
                at,
            );
            st.hasStatus = true;
        } else if (schemaKind === 'concurrent-different-add-column') {
            if (st.hasStatus) {
                await deploy([{ rule: 'drop-column', table: 'orders', column: 'status' }], 'drop status for concurrent re-add', at);
                st.hasStatus = false;
            }
            await concurrentAdds(
                [{ rule: 'add-column', table: 'orders', column: 'status', def: { type: 'string', default: 'from-a' } }], 'add status A',
                [{ rule: 'add-column', table: 'orders', column: 'status', def: { type: 'string', default: 'from-b' } }], 'add status B',
                at,
            );
            st.hasStatus = true;
        } else if (schemaKind === 'concurrent-identical-add-table') {
            if (st.hasItems) {
                await deploy([{ rule: 'drop-table', table: 'items' }], 'drop items for concurrent re-add', at);
                st.hasItems = false;
            }
            const itemsDef = open('items', { label: { type: 'string' } });
            const { deployA, deployB } = await concurrentAdds(
                [{ rule: 'add-table', def: itemsDef }], 'add items A',
                [{ rule: 'add-table', def: itemsDef }], 'add items B',
                at,
            );
            const items = await group.getTable('items');
            await items.insert(`ia-${opIndex}`, { label: 'a' }, undefined, version(deployA));
            await items.insert(`ib-${opIndex}`, { label: 'b' }, undefined, version(deployB));
            st.hasItems = true;
        } else if (schemaKind === 'concurrent-different-add-table') {
            if (st.hasItems) {
                await deploy([{ rule: 'drop-table', table: 'items' }], 'drop items for concurrent re-add', at);
                st.hasItems = false;
            }
            const defA = open('items', { label: { type: 'string', default: 'a' } });
            const defB = open('items', { label: { type: 'string', default: 'b' } });
            const { deployA, deployB } = await concurrentAdds(
                [{ rule: 'add-table', def: defA }], 'add items A',
                [{ rule: 'add-table', def: defB }], 'add items B',
                at,
            );
            const items = await group.getTable('items');
            await items.insert(`ia-${opIndex}`, { label: 'from-a' }, undefined, version(deployA));
            await items.insert(`ib-${opIndex}`, { label: 'from-b' }, undefined, version(deployB));
            st.hasItems = true;
        } else if (schemaKind === 'toggle-cd-concurrent-delete') {
            st.concurrentDeletes = !st.concurrentDeletes;
            await deploy(
                [{ rule: 'set-concurrent-deletes', table: 'orders', value: st.concurrentDeletes }],
                'toggle cd',
                undefined,
            );
            const live = await liveIds(group, 'orders', atV, ORDER_POOL, orderRowId);
            const target = pick(prng, live);
            if (target === undefined) throw new Error('no live order for concurrent delete');
            await (await group.getTable('orders')).delete(orderRowId(target), undefined, at);
        } else if (schemaKind === 'add-tags') {
            await deploy([{ rule: 'add-table', def: open('tags', { code: { type: 'string' } }) }], 'add tags', at);
            st.hasTags = true;
        } else if (schemaKind === 'drop-tags') {
            await deploy([{ rule: 'drop-table', table: 'tags' }], 'drop tags', at);
            st.hasTags = false;
        } else if (schemaKind === 'drop-items') {
            await deploy([{ rule: 'drop-table', table: 'items' }], 'drop items', at);
            st.hasItems = false;
        } else if (schemaKind === 'add-memo') {
            await deploy([{ rule: 'add-column', table: 'orders', column: 'memo', def: { type: 'string', nullable: true } }], 'add memo', at);
            st.memo = 'string';
        } else if (schemaKind === 'drop-memo') {
            await deploy([{ rule: 'drop-column', table: 'orders', column: 'memo' }], 'drop memo', at);
            st.memo = 'absent';
        } else if (schemaKind === 'retype-memo') {
            const next: 'string' | 'integer' = st.memo === 'string' ? 'integer' : 'string';
            await deploy([
                { rule: 'drop-column', table: 'orders', column: 'memo' },
                { rule: 'add-column', table: 'orders', column: 'memo', def: { type: next, nullable: true } },
            ], 'retype memo', at);
            st.memo = next;
        } else {
            throw new Error(`unknown schema kind '${schemaKind}'`);
        }
        return `[${opIndex}] schema ${schemaKind}`;
    };

    for (let opIndex = 0; opIndex < ops; opIndex++) {
        const at = pickConcurrentAt(prng, checkpoints);
        const atV = at ?? await groupFrontier();
        const roll = prng.nextInt(0, 99);
        let kind = '';
        let log = '';
        const forcedKind = forced.get(opIndex);

        try {
            if (forcedKind !== undefined) {
                kind = 'schema-' + forcedKind;
                log = await applySchemaKind(forcedKind, at, atV, opIndex);
            } else if (roll < 14) {
                kind = 'insert-order';
                const i = prng.nextInt(0, ORDER_POOL - 1);
                if (await (await (await group.getView(atV, atV)).getTableView('orders')).hasRow(orderRowId(i))) {
                    bump(tallies, kind, 'rejected');
                    continue;
                }
                const vals: { [c: string]: string | number } = { customer: `c-${prng.nextInt(0, 5)}` };
                if (st.hasStatus && prng.next() < 0.5) vals['status'] = `s-${prng.nextInt(0, 3)}`;
                if (st.memo !== 'absent' && prng.next() < 0.4) {
                    vals['memo'] = st.memo === 'integer' ? prng.nextInt(0, 9) : `m-${prng.nextInt(0, 3)}`;
                }
                if (st.hasNote && prng.next() < 0.4) vals['note'] = `n-${prng.nextInt(0, 3)}`;
                await (await group.getTable('orders')).insert(orderUuid(i), vals, undefined, at);
                log = `[${opIndex}] insert orders ${orderUuid(i)}`;
            } else if (roll < 28) {
                kind = 'insert-line';
                const live = await liveIds(group, 'orders', atV, ORDER_POOL, orderRowId);
                const target = pick(prng, live);
                if (target === undefined) { bump(tallies, kind, 'rejected'); continue; }
                const i = prng.nextInt(0, LINE_POOL - 1);
                if (await (await (await group.getView(atV, atV)).getTableView('lines')).hasRow(lineRowId(i))) {
                    bump(tallies, kind, 'rejected');
                    continue;
                }
                const vals: { [c: string]: string | number } = { qty: prng.nextInt(1, 9) };
                if (st.hasOrderColumn) vals['order'] = orderRowId(target);
                await (await group.getTable('lines')).insert(lineUuid(i), vals, undefined, at);
                log = `[${opIndex}] insert lines ${lineUuid(i)} order=${orderUuid(target)}`;
            } else if (roll < 36) {
                kind = 'update-order';
                const target = pick(prng, await liveIds(group, 'orders', atV, ORDER_POOL, orderRowId));
                if (target === undefined) { bump(tallies, kind, 'rejected'); continue; }
                await (await group.getTable('orders')).update(orderRowId(target), { customer: `c-${prng.nextInt(0, 5)}` }, undefined, at);
                log = `[${opIndex}] update orders ${orderUuid(target)}`;
            } else if (roll < 44) {
                kind = 'update-line';
                const target = pick(prng, await liveIds(group, 'lines', atV, LINE_POOL, lineRowId));
                if (target === undefined) { bump(tallies, kind, 'rejected'); continue; }
                await (await group.getTable('lines')).update(lineRowId(target), { qty: prng.nextInt(1, 9) }, undefined, at);
                log = `[${opIndex}] update lines ${lineUuid(target)}`;
            } else if (roll < 52) {
                kind = 'delete-order';
                const target = pick(prng, await liveIds(group, 'orders', atV, ORDER_POOL, orderRowId));
                if (target === undefined) { bump(tallies, kind, 'rejected'); continue; }
                await (await group.getTable('orders')).delete(orderRowId(target), undefined, at);
                log = `[${opIndex}] delete orders ${orderUuid(target)}`;
            } else if (roll < 58) {
                kind = 'delete-line';
                const target = pick(prng, await liveIds(group, 'lines', atV, LINE_POOL, lineRowId));
                if (target === undefined) { bump(tallies, kind, 'rejected'); continue; }
                await (await group.getTable('lines')).delete(lineRowId(target), undefined, at);
                log = `[${opIndex}] delete lines ${lineUuid(target)}`;
            } else if (roll < 66) {
                kind = 'bundle';
                const oi = prng.nextInt(0, ORDER_POOL - 1);
                const li = prng.nextInt(0, LINE_POOL - 1);
                const gv = await group.getView(atV, atV);
                if (await (await gv.getTableView('orders')).hasRow(orderRowId(oi))) { bump(tallies, kind, 'rejected'); continue; }
                if (await (await gv.getTableView('lines')).hasRow(lineRowId(li))) { bump(tallies, kind, 'rejected'); continue; }
                const lineVals: { [c: string]: string | number } = { qty: prng.nextInt(1, 9) };
                if (st.hasOrderColumn) lineVals['order'] = orderRowId(oi);
                await group.bundle([
                    { table: 'orders', op: { action: 'insert', rowId: orderRowId(oi), uuid: orderUuid(oi), values: { customer: `c-${prng.nextInt(0, 5)}` } } },
                    { table: 'lines', op: { action: 'insert', rowId: lineRowId(li), uuid: lineUuid(li), values: lineVals } },
                ], undefined, at);
                log = `[${opIndex}] bundle order=${orderUuid(oi)} line=${lineUuid(li)}`;
            } else if (roll < 72 && st.hasTags) {
                kind = 'insert-tag';
                const i = prng.nextInt(0, TAG_POOL - 1);
                if (await (await (await group.getView(atV, atV)).getTableView('tags')).hasRow(tagRowId(i))) {
                    bump(tallies, kind, 'rejected');
                    continue;
                }
                await (await group.getTable('tags')).insert(tagUuid(i), { code: `g-${prng.nextInt(0, 5)}` }, undefined, at);
                log = `[${opIndex}] insert tags ${tagUuid(i)}`;
            } else if (roll < 76 && st.hasItems) {
                kind = 'insert-item';
                const i = prng.nextInt(0, ITEM_POOL - 1);
                if (await (await (await group.getView(atV, atV)).getTableView('items')).hasRow(itemRowId(i))) {
                    bump(tallies, kind, 'rejected');
                    continue;
                }
                await (await group.getTable('items')).insert(itemUuid(i), { label: `x-${prng.nextInt(0, 5)}` }, undefined, at);
                log = `[${opIndex}] insert items ${itemUuid(i)}`;
            } else {
                const kinds: string[] = [];
                if (!st.hasStatus) kinds.push('add-status'); else kinds.push('drop-status');
                if (st.hasFk) kinds.push('drop-fk');
                else if (st.hasOrderColumn) kinds.push('add-fk');
                if (st.hasOrderColumn) kinds.push(prng.next() < 0.5 ? 'recreate-order-plain' : 'recreate-order-fk');
                else kinds.push(prng.next() < 0.5 ? 'add-order-plain' : 'add-order-fk');
                kinds.push('reincarnate-orders-table');
                kinds.push('reincarnate-orders-reshaped');
                kinds.push('reincarnate-status-column');
                kinds.push('concurrent-identical-add-column');
                kinds.push('concurrent-identical-add-table');
                kinds.push('concurrent-different-add-column');
                kinds.push('concurrent-different-add-table');
                kinds.push('toggle-cd-concurrent-delete');
                if (!st.hasTags) kinds.push('add-tags'); else kinds.push('drop-tags');
                if (st.hasItems) kinds.push('drop-items');
                if (st.memo === 'absent') kinds.push('add-memo');
                else { kinds.push('drop-memo'); kinds.push('retype-memo'); }
                const schemaKind = kinds[prng.nextInt(0, kinds.length - 1)];
                kind = 'schema-' + schemaKind;
                log = await applySchemaKind(schemaKind, at, atV, opIndex);
            }
        } catch {
            if (kind !== '') bump(tallies, kind, 'rejected');
            continue;
        } finally {
            onOp?.();
        }

        bump(tallies, kind, 'accepted');
        opLog.push(log);
        await recordCheckpoint(checkpoints, await groupFrontier());
    }

    const rowAccepted = acceptedOf(tallies,
        'insert-order', 'insert-line', 'update-order', 'update-line', 'delete-order', 'delete-line',
        'bundle', 'insert-tag', 'insert-item');
    const schemaAccepted = Object.keys(tallies).filter((k) => k.startsWith('schema-'))
        .reduce((n, k) => n + tallies[k].accepted, 0);

    if (rowAccepted < 1 || (ops >= 18 && schemaAccepted < 1)) {
        throw new Error(
            `kind=project-generate seed=${seed} ops=${ops}: coverage floor failed `
            + `(rowAccepted=${rowAccepted} schemaAccepted=${schemaAccepted}) tallies=${JSON.stringify(tallies)}\n`
            + opLog.join('\n'),
        );
    }

    return { group, schema, rawDag, checkpoints, seed, opLog, tallies };
}
