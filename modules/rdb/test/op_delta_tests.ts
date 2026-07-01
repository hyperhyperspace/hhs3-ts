import { assertTrue, assertFalse, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { Version, serializePublicKeyToBase64 } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import { RSchemaImpl, rSchemaFactory } from "../src/rschema/rschema.js";
import { RTableGroupImpl, rTableGroupFactory } from "../src/rtable_group/group.js";
import { deriveRowId } from "../src/rtable/hash.js";
import type { TableDef } from "../src/rschema/payload.js";
import type { RTableView } from "../src/rtable/interfaces.js";
import {
    usersSchemaTables, capRow, revokeCap,
    CAPS_TABLE, USERS_MANAGER_LABEL, USERS_SCHEMA_NAME,
    USERS_BINDING, IDENTITIES_TABLE,
} from "../src/users/users.js";

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
        name: 'opdelta:test_schema',
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        tables,
    });
    const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
    const pinned = await (await schema.getScopedDag()).getFrontier();

    const groupInit = await RTableGroupImpl.create({
        name: 'opdelta-test-group',
        seed: 'opdelta-test-group',
        schemaRef: schema.getId(),
        schemaVersion: pinned,
    });
    const group = (await ctx.createObject(groupInit)) as RTableGroupImpl;

    return { group, admin };
}

async function tableView(group: RTableGroupImpl, name: string, at?: Version): Promise<RTableView> {
    return (await group.getView(at, at)).getTableView(name);
}

async function groupFrontier(group: RTableGroupImpl): Promise<Version> {
    return (await group.getScopedDag()).getFrontier();
}

function findOpFlip(delta: Awaited<ReturnType<RTableGroupImpl['computeDelta']>>, entry: string, kind?: string) {
    return delta.opVerdictChanges.find((c) => c.entry === entry && (kind === undefined || c.kind === kind));
}

export const opDeltaTests = {
    title: '[OP_DELTA] group delta op verdict flips',
    tests: [
        {
            name: '[OPDELTA01] concurrent barrier revoke voids insert — op channel reports insert void flip',
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

                await caps.delete(capId, undefined, base);
                const insertHash = await items.insert('i-1', { name: 'thing' }, undefined, base);

                const merged = await groupFrontier(group);
                const delta = await group.computeDelta(base, merged);

                const flip = findOpFlip(delta, insertHash, 'insert');
                assertTrue(flip !== undefined, 'insert op appears in opVerdictChanges');
                assertFalse(flip!.voidBefore, 'insert was live at start horizon');
                assertTrue(flip!.voidAfter, 'insert voided at end horizon');
                assertEquals(flip!.table, 'items');
                assertEquals(flip!.voidHorizon, 'end');
                assertEquals(flip!.reason?.kind, 'restriction');
                if (flip!.reason?.kind === 'restriction') {
                    assertEquals(flip!.reason.table, 'items');
                    assertEquals(flip!.reason.action, 'insert');
                }
            },
        },
        {
            name: '[OPDELTA02] voided delete — op channel reports delete un-void flip, row resurrects',
            invoke: async () => {
                const { group } = await createEnv([
                    open('caps', { label: { type: 'string', pub: true } }, { concurrentDeletes: true }),
                    {
                        name: 'items',
                        columns: { name: { type: 'string' } },
                        restrictions: [
                            { on: 'insert', rule: { p: 'true' } },
                            { on: 'delete', rule: { p: 'exists', table: 'caps', where: { label: 'grant' } } },
                        ],
                    },
                ]);
                const caps = await group.getTable('caps');
                const items = await group.getTable('items');

                await caps.insert('c-1', { label: 'grant' });
                const itemId = deriveRowId('i-1');
                await items.insert('i-1', { name: 'keep' });

                const base = await groupFrontier(group);
                await caps.delete(deriveRowId('c-1'), undefined, base);
                const deleteHash = await items.delete(itemId, undefined, base);

                const merged = await groupFrontier(group);
                assertTrue(await (await tableView(group, 'items', merged)).hasRow(itemId),
                    'voided delete leaves row live');

                const delta = await group.computeDelta(base, merged);
                const flip = findOpFlip(delta, deleteHash, 'delete');
                assertTrue(flip !== undefined, 'delete op appears in opVerdictChanges');
                assertFalse(flip!.voidBefore, 'delete valid at start horizon');
                assertTrue(flip!.voidAfter, 'delete voided at end horizon');
                assertEquals(flip!.voidHorizon, 'end');
                assertEquals(flip!.reason?.kind, 'restriction');
                if (flip!.reason?.kind === 'restriction') {
                    assertEquals(flip!.reason.action, 'delete');
                }
            },
        },
        {
            name: '[OPDELTA03] voided update with stable row — op channel only',
            invoke: async () => {
                const { group } = await createEnv([
                    open('caps', { label: { type: 'string', pub: true } }, { concurrentDeletes: true }),
                    {
                        name: 'docs',
                        columns: { body: { type: 'string' } },
                        restrictions: [
                            { on: 'insert', rule: { p: 'true' } },
                            { on: 'update', rule: { p: 'exists', table: 'caps', where: { label: 'grant' } } },
                        ],
                    },
                ]);
                const caps = await group.getTable('caps');
                const docs = await group.getTable('docs');

                await caps.insert('c-1', { label: 'grant' });
                const rowId = deriveRowId('d-1');
                await docs.insert('d-1', { body: 'v1' });

                const base = await groupFrontier(group);
                await caps.delete(deriveRowId('c-1'), undefined, base);
                const updateHash = await docs.update(rowId, { body: 'v2' }, undefined, base);

                const merged = await groupFrontier(group);
                assertEquals((await (await tableView(group, 'docs', merged)).getRow(rowId))!.values['body'], 'v1',
                    'voided update does not change resolved column');

                const delta = await group.computeDelta(base, merged);
                const flip = findOpFlip(delta, updateHash, 'update');
                assertTrue(flip !== undefined, 'update op flip reported');
                assertFalse(flip!.voidBefore);
                assertTrue(flip!.voidAfter);
                assertEquals(flip!.voidHorizon, 'end');
                assertEquals(flip!.reason?.kind, 'restriction');
                if (flip!.reason?.kind === 'restriction') {
                    assertEquals(flip!.reason.action, 'update');
                }

                const rowDelta = [...delta.tableChanges.values()].flatMap((t) => t.rowChanges);
                assertFalse(rowDelta.some((r) => r.rowId === rowId), 'row channel silent when columns unchanged');
            },
        },
        {
            name: '[OPDELTA04] gated observe void flip',
            invoke: async () => {
                const ctx = createMockRContext({ selfValidate: true });
                ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
                ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);

                const admin = await makeIdentity();
                const p = await makeIdentity();
                const usersSchemaInit = await RSchemaImpl.create({
                    name: USERS_SCHEMA_NAME,
                    creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
                    tables: usersSchemaTables(),
                });
                const usersSchema = (await ctx.createObject(usersSchemaInit)) as RSchemaImpl;
                const usersPinned = await (await usersSchema.getScopedDag()).getFrontier();
                const usersInit = await RTableGroupImpl.create({
                    name: 'opdelta-users', seed: 'opdelta-users-g',
                    schemaRef: usersSchema.getId(), schemaVersion: usersPinned,
                    idProvider: IDENTITIES_TABLE,
                    initialRows: {
                        [IDENTITIES_TABLE]: [
                            { action: 'insert', rowId: deriveRowId('admin'), uuid: 'admin', values: { keyId: admin.keyId, publicKey: serializePublicKeyToBase64(admin.publicKey) } },
                            { action: 'insert', rowId: deriveRowId('p'), uuid: 'p', values: { keyId: p.keyId, publicKey: serializePublicKeyToBase64(p.publicKey) } },
                        ],
                        [CAPS_TABLE]: [
                            capRow('admin-cap', admin.keyId, USERS_MANAGER_LABEL),
                            capRow('p-cap', p.keyId, USERS_MANAGER_LABEL),
                        ],
                    },
                });
                const users = (await ctx.createObject(usersInit)) as RTableGroupImpl;

                const appSchemaInit = await RSchemaImpl.create({
                    name: 'opdelta:app',
                    creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
                    tables: [open('notes', { body: { type: 'string' } })],
                });
                const appSchema = (await ctx.createObject(appSchemaInit)) as RSchemaImpl;
                const appPinned = await (await appSchema.getScopedDag()).getFrontier();
                const appInit = await RTableGroupImpl.create({
                    name: 'opdelta-app', seed: 'opdelta-app-g',
                    schemaRef: appSchema.getId(), schemaVersion: appPinned,
                    bindings: { [USERS_BINDING]: users.getId() },
                    canObserve: {
                        [USERS_BINDING]: { p: 'exists', table: CAPS_TABLE, where: { label: USERS_MANAGER_LABEL, grantee: '$author' } },
                    },
                });
                const app = (await ctx.createObject(appInit)) as RTableGroupImpl;

                const genesis = await groupFrontier(app);
                const v1 = await groupFrontier(users);
                const base = await groupFrontier(app);
                const observeHash = await app.observe(USERS_BINDING, v1, p, base);

                await revokeCap(users, admin, p.keyId, USERS_MANAGER_LABEL);
                const v2 = await groupFrontier(users);
                await app.observe(USERS_BINDING, v2, admin, base);
                const end = await groupFrontier(app);

                assertTrue(await app.isEntryVoided(observeHash, end), 'observe voided at end');

                const delta = await app.computeDelta(genesis, end);
                const flip = findOpFlip(delta, observeHash, 'observe');
                assertTrue(flip !== undefined, 'observe void flip reported');
                assertFalse(flip!.voidBefore, 'observe live at start horizon');
                assertTrue(flip!.voidAfter, 'observe voided at end horizon');
                assertEquals(flip!.binding, USERS_BINDING);
                assertEquals(flip!.voidHorizon, 'end');
                assertEquals(flip!.reason?.kind, 'observe-gate');
                if (flip!.reason?.kind === 'observe-gate') {
                    assertEquals(flip!.reason.binding, USERS_BINDING);
                }
            },
        },
        {
            name: '[OPDELTA05] valid update in interval — no op flip',
            invoke: async () => {
                const { group } = await createEnv([
                    open('docs', { body: { type: 'string' } }),
                ]);
                const docs = await group.getTable('docs');
                const start = await groupFrontier(group);
                await docs.insert('d-1', { body: 'v1' });
                const mid = await groupFrontier(group);
                await docs.update(deriveRowId('d-1'), { body: 'v2' });
                const end = await groupFrontier(group);

                const delta = await group.computeDelta(mid, end);
                assertEquals(delta.opVerdictChanges.length, 0, 'stable void produces no op flips');
            },
        },
        {
            name: '[OPDELTA06] concurrent FK target delete voids insert — op channel reports fk reason',
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

                await orders.delete(orderId, undefined, base);
                const insertHash = await lines.insert('l-1', { order: orderId, qty: 1 }, undefined, base);

                const merged = await groupFrontier(group);
                const delta = await group.computeDelta(base, merged);

                const flip = findOpFlip(delta, insertHash, 'insert');
                assertTrue(flip !== undefined, 'insert op flip reported');
                assertEquals(flip!.voidHorizon, 'end');
                assertEquals(flip!.reason?.kind, 'fk');
                if (flip!.reason?.kind === 'fk') {
                    assertEquals(flip!.reason.column, 'order');
                    assertEquals(flip!.reason.targetRef, 'orders');
                    assertEquals(flip!.reason.targetRowId, orderId);
                }
            },
        },
    ],
};
