import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity, B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { dag } from "@hyper-hyper-space/hhs3_dag";
import { version, Version } from "@hyper-hyper-space/hhs3_mvt";
import {
    RSchemaImpl, rSchemaFactory, RTableGroupImpl, rTableGroupFactory,
    deriveRowId, TableDef, MigrationRule,
} from "@hyper-hyper-space/hhs3_rdb";

import { createMockRContext } from "../../../rdb/test/mock_rcontext.js";
import { pickConcurrentAt, recordCheckpoint } from "../../../rdb/test/delta_parity/checkpoints.js";
import { PRNG } from "../../../rdb/test/delta_parity/prng.js";

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

function orderUuid(i: number): string { return `o-${i}`; }
function lineUuid(i: number): string { return `l-${i}`; }
function tagUuid(i: number): string { return `t-${i}`; }
function orderRowId(i: number): B64Hash { return deriveRowId(orderUuid(i)); }
function lineRowId(i: number): B64Hash { return deriveRowId(lineUuid(i)); }
function tagRowId(i: number): B64Hash { return deriveRowId(tagUuid(i)); }

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

function pick<T>(prng: PRNG, arr: T[]): T | undefined {
    if (arr.length === 0) return undefined;
    return arr[prng.nextInt(0, arr.length - 1)];
}

function bump(tallies: Tallies, kind: string, which: 'accepted' | 'rejected'): void {
    const cur = tallies[kind] ?? { accepted: 0, rejected: 0 };
    cur[which]++;
    tallies[kind] = cur;
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

// Seeded orders/lines(+optional tags) history with schema deploys that stress
// the project planner: add/drop defaulted columns, set-fks flips, FK-column
// drop+re-add, type change via drop+add, add/drop table. Concurrent `at` ~30%.
export async function generateProjectHistory(seed: number, ops: number): Promise<ProjectHistory> {
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

    let hasStatus = false;
    let hasFk = true;
    let hasOrderColumn = true;
    let hasTags = false;
    let memo: 'absent' | 'string' | 'integer' = 'absent';

    const groupFrontier = async () => (await group.getScopedDag()).getFrontier();
    const schemaFrontier = async () => (await schema.getScopedDag()).getFrontier();

    const deploy = async (migration: MigrationRule[], note: string, at: Version | undefined): Promise<boolean> => {
        await schema.updateSchema(migration, admin, note);
        const v2 = await schemaFrontier();
        await group.deploy(v2, undefined, at);
        return true;
    };

    for (let opIndex = 0; opIndex < ops; opIndex++) {
        const at = pickConcurrentAt(prng, checkpoints);
        const atV = at ?? await groupFrontier();
        const roll = prng.nextInt(0, 99);
        let kind = '';
        let log = '';

        try {
            if (roll < 16) {
                kind = 'insert-order';
                const i = prng.nextInt(0, ORDER_POOL - 1);
                if (await (await (await group.getView(atV, atV)).getTableView('orders')).hasRow(orderRowId(i))) {
                    bump(tallies, kind, 'rejected');
                    continue;
                }
                const vals: { [c: string]: string | number } = { customer: `c-${prng.nextInt(0, 5)}` };
                if (hasStatus && prng.next() < 0.5) vals['status'] = `s-${prng.nextInt(0, 3)}`;
                if (memo !== 'absent' && prng.next() < 0.4) {
                    vals['memo'] = memo === 'integer' ? prng.nextInt(0, 9) : `m-${prng.nextInt(0, 3)}`;
                }
                await (await group.getTable('orders')).insert(orderUuid(i), vals, undefined, at);
                log = `[${opIndex}] insert orders ${orderUuid(i)}`;
            } else if (roll < 32) {
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
                if (hasOrderColumn) vals['order'] = orderRowId(target);
                await (await group.getTable('lines')).insert(lineUuid(i), vals, undefined, at);
                log = `[${opIndex}] insert lines ${lineUuid(i)} order=${orderUuid(target)}`;
            } else if (roll < 42) {
                kind = 'update-order';
                const target = pick(prng, await liveIds(group, 'orders', atV, ORDER_POOL, orderRowId));
                if (target === undefined) { bump(tallies, kind, 'rejected'); continue; }
                await (await group.getTable('orders')).update(orderRowId(target), { customer: `c-${prng.nextInt(0, 5)}` }, undefined, at);
                log = `[${opIndex}] update orders ${orderUuid(target)}`;
            } else if (roll < 52) {
                kind = 'update-line';
                const target = pick(prng, await liveIds(group, 'lines', atV, LINE_POOL, lineRowId));
                if (target === undefined) { bump(tallies, kind, 'rejected'); continue; }
                await (await group.getTable('lines')).update(lineRowId(target), { qty: prng.nextInt(1, 9) }, undefined, at);
                log = `[${opIndex}] update lines ${lineUuid(target)}`;
            } else if (roll < 62) {
                kind = 'delete-order';
                const target = pick(prng, await liveIds(group, 'orders', atV, ORDER_POOL, orderRowId));
                if (target === undefined) { bump(tallies, kind, 'rejected'); continue; }
                await (await group.getTable('orders')).delete(orderRowId(target), undefined, at);
                log = `[${opIndex}] delete orders ${orderUuid(target)}`;
            } else if (roll < 70) {
                kind = 'delete-line';
                const target = pick(prng, await liveIds(group, 'lines', atV, LINE_POOL, lineRowId));
                if (target === undefined) { bump(tallies, kind, 'rejected'); continue; }
                await (await group.getTable('lines')).delete(lineRowId(target), undefined, at);
                log = `[${opIndex}] delete lines ${lineUuid(target)}`;
            } else if (roll < 78) {
                kind = 'bundle';
                const oi = prng.nextInt(0, ORDER_POOL - 1);
                const li = prng.nextInt(0, LINE_POOL - 1);
                const gv = await group.getView(atV, atV);
                if (await (await gv.getTableView('orders')).hasRow(orderRowId(oi))) { bump(tallies, kind, 'rejected'); continue; }
                if (await (await gv.getTableView('lines')).hasRow(lineRowId(li))) { bump(tallies, kind, 'rejected'); continue; }
                const lineVals: { [c: string]: string | number } = { qty: prng.nextInt(1, 9) };
                if (hasOrderColumn) lineVals['order'] = orderRowId(oi);
                await group.bundle([
                    { table: 'orders', op: { action: 'insert', rowId: orderRowId(oi), uuid: orderUuid(oi), values: { customer: `c-${prng.nextInt(0, 5)}` } } },
                    { table: 'lines', op: { action: 'insert', rowId: lineRowId(li), uuid: lineUuid(li), values: lineVals } },
                ], undefined, at);
                log = `[${opIndex}] bundle order=${orderUuid(oi)} line=${lineUuid(li)}`;
            } else if (roll < 84 && hasTags) {
                kind = 'insert-tag';
                const i = prng.nextInt(0, TAG_POOL - 1);
                if (await (await (await group.getView(atV, atV)).getTableView('tags')).hasRow(tagRowId(i))) {
                    bump(tallies, kind, 'rejected');
                    continue;
                }
                await (await group.getTable('tags')).insert(tagUuid(i), { code: `g-${prng.nextInt(0, 5)}` }, undefined, at);
                log = `[${opIndex}] insert tags ${tagUuid(i)}`;
            } else {
                const kinds: string[] = [];
                if (!hasStatus) kinds.push('add-status'); else kinds.push('drop-status');
                if (hasFk) kinds.push('drop-fk');
                else if (hasOrderColumn) kinds.push('add-fk');
                if (hasOrderColumn) kinds.push(prng.next() < 0.5 ? 'recreate-order-plain' : 'recreate-order-fk');
                else kinds.push(prng.next() < 0.5 ? 'add-order-plain' : 'add-order-fk');
                if (!hasTags) kinds.push('add-tags'); else kinds.push('drop-tags');
                if (memo === 'absent') kinds.push('add-memo');
                else { kinds.push('drop-memo'); kinds.push('retype-memo'); }
                const schemaKind = kinds[prng.nextInt(0, kinds.length - 1)];
                kind = 'schema-' + schemaKind;

                if (schemaKind === 'add-status') {
                    await deploy([{ rule: 'add-column', table: 'orders', column: 'status', def: { type: 'string', default: 'new' } }], 'add status', at);
                    hasStatus = true;
                } else if (schemaKind === 'drop-status') {
                    await deploy([{ rule: 'drop-column', table: 'orders', column: 'status' }], 'drop status', at);
                    hasStatus = false;
                } else if (schemaKind === 'drop-fk') {
                    await deploy([{ rule: 'set-fks', table: 'lines', fks: {} }], 'drop fk', at);
                    hasFk = false;
                } else if (schemaKind === 'add-fk') {
                    await deploy([{ rule: 'set-fks', table: 'lines', fks: { order: 'orders' } }], 'add fk', at);
                    hasFk = true;
                } else if (schemaKind === 'recreate-order-plain' || schemaKind === 'recreate-order-fk') {
                    const rules: MigrationRule[] = [
                        { rule: 'set-fks', table: 'lines', fks: {} },
                        { rule: 'drop-column', table: 'lines', column: 'order' },
                        { rule: 'add-column', table: 'lines', column: 'order', def: { type: 'string', nullable: true } },
                    ];
                    if (schemaKind === 'recreate-order-fk') rules.push({ rule: 'set-fks', table: 'lines', fks: { order: 'orders' } });
                    await deploy(rules, schemaKind, at);
                    hasOrderColumn = true;
                    hasFk = schemaKind === 'recreate-order-fk';
                } else if (schemaKind === 'add-order-plain' || schemaKind === 'add-order-fk') {
                    const rules: MigrationRule[] = [
                        { rule: 'add-column', table: 'lines', column: 'order', def: { type: 'string', nullable: true } },
                    ];
                    if (schemaKind === 'add-order-fk') rules.push({ rule: 'set-fks', table: 'lines', fks: { order: 'orders' } });
                    await deploy(rules, schemaKind, at);
                    hasOrderColumn = true;
                    hasFk = schemaKind === 'add-order-fk';
                } else if (schemaKind === 'add-tags') {
                    await deploy([{ rule: 'add-table', def: open('tags', { code: { type: 'string' } }) }], 'add tags', at);
                    hasTags = true;
                } else if (schemaKind === 'drop-tags') {
                    await deploy([{ rule: 'drop-table', table: 'tags' }], 'drop tags', at);
                    hasTags = false;
                } else if (schemaKind === 'add-memo') {
                    await deploy([{ rule: 'add-column', table: 'orders', column: 'memo', def: { type: 'string', nullable: true } }], 'add memo', at);
                    memo = 'string';
                } else if (schemaKind === 'drop-memo') {
                    await deploy([{ rule: 'drop-column', table: 'orders', column: 'memo' }], 'drop memo', at);
                    memo = 'absent';
                } else {
                    const next: 'string' | 'integer' = memo === 'string' ? 'integer' : 'string';
                    await deploy([
                        { rule: 'drop-column', table: 'orders', column: 'memo' },
                        { rule: 'add-column', table: 'orders', column: 'memo', def: { type: next, nullable: true } },
                    ], 'retype memo', at);
                    memo = next;
                }
                log = `[${opIndex}] schema ${schemaKind}`;
            }
        } catch {
            if (kind !== '') bump(tallies, kind, 'rejected');
            continue;
        }

        bump(tallies, kind, 'accepted');
        opLog.push(log);
        await recordCheckpoint(checkpoints, await groupFrontier());
    }

    const rowAccepted = acceptedOf(tallies,
        'insert-order', 'insert-line', 'update-order', 'update-line', 'delete-order', 'delete-line', 'bundle', 'insert-tag');
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
