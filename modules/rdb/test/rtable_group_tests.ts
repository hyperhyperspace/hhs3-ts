import { assertTrue, assertFalse, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { version } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import { RSchemaImpl, rSchemaFactory } from "../src/rschema/rschema.js";
import { RTableGroupImpl, rTableGroupFactory } from "../src/rtable_group/group.js";
import { deriveRowId } from "../src/rtable/hash.js";
import type { TableDef, Predicate } from "../src/rschema/payload.js";
import type { InsertRowPayload } from "../src/rtable/payload.js";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

// Fixture tables permit unauthored updates/deletes (`on: 'all'` -> true) so
// these structural tests aren't subject to the default author-is-author
// restriction, which the [ENF] suite exercises directly.
function ordersTable(): TableDef {
    return {
        name: 'orders',
        columns: {
            customer: { type: 'string', pub: true },
            total: { type: 'float' },
            status: { type: 'string', default: 'new' },
        },
        concurrentDeletes: false,
        restrictions: [{ on: 'all', rule: { p: 'true' } }],
    };
}

function capsTable(): TableDef {
    return {
        name: 'caps',
        columns: { label: { type: 'string', pub: true } },
        concurrentDeletes: true,
        restrictions: [{ on: 'all', rule: { p: 'true' } }],
    };
}

function notesTable(): TableDef {
    return { name: 'notes', columns: { body: { type: 'string' } } };
}

async function createTestEnv(groupExtras?: {
    initialRows?: { [table: string]: json.Literal[] };
    bindings?: { [name: string]: string };
    canDeploy?: Predicate;
}) {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);

    const admin = await makeIdentity();

    const schemaInit = await RSchemaImpl.create({
        seed: 'group-test-schema',
        name: 'shop',
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        tables: [ordersTable(), capsTable()],
    });
    const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;

    const pinned = await (await schema.getScopedDag()).getFrontier();

    const groupInit = await RTableGroupImpl.create({
        seed: 'group-test',
        schemaRef: schema.getId(),
        schemaVersion: pinned,
        ...groupExtras,
    });
    const group = (await ctx.createObject(groupInit)) as RTableGroupImpl;

    return { ctx, schema, group, admin, pinned };
}

function initialCapRow(uuid: string, label: string): InsertRowPayload {
    return {
        action: 'insert',
        rowId: deriveRowId(uuid),
        uuid,
        values: { label },
    };
}

export const rtableGroupTests = {
    title: '[RGROUP] RTableGroup + RTable object tests',
    tests: [
        {
            name: '[RGROUP01] Create group and view initial rows at genesis',
            invoke: async () => {
                const { group, admin } = await createTestEnv({
                    initialRows: { caps: [initialCapRow('seed-admin', 'admin')] },
                });

                assertEquals(group.seed(), 'group-test', 'seed should be set');
                assertTrue(admin.keyId.length > 0, 'sanity: identity exists');

                const view = await group.getView();
                assertEquals(view.getTableNames().sort().toString(), 'caps,orders',
                    'the effective schema tables should be visible through the group view');

                const caps = await view.getTableView('caps');
                const initialRowId = deriveRowId('seed-admin');
                assertTrue(await caps.hasRow(initialRowId), 'the initial row should be live at genesis');

                const row = (await caps.getRow(initialRowId))!;
                assertEquals(row.values['label'], 'admin', 'initial values should read back');
                assertEquals(row.author, undefined, 'initial row should be unauthored');
                assertEquals(await caps.getAuthor(initialRowId), undefined, 'getAuthor should match');
            }
        },
        {
            name: '[RGROUP02] Creation rejected for invalid initial rows or missing schema',
            invoke: async () => {
                const expectCreateFailure = async (
                    extras: Parameters<typeof createTestEnv>[0], why: string, schemaRefOverride?: string,
                ) => {
                    let failed = false;
                    try {
                        const ctx = createMockRContext({ selfValidate: true });
                        ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
                        ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);
                        const admin = await makeIdentity();
                        const schemaInit = await RSchemaImpl.create({
                            seed: 's', creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
                            tables: [ordersTable(), capsTable()],
                        });
                        const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
                        const pinned = await (await schema.getScopedDag()).getFrontier();
                        const init = await RTableGroupImpl.create({
                            seed: 'g', schemaRef: schemaRefOverride ?? schema.getId(), schemaVersion: pinned, ...extras,
                        });
                        await ctx.createObject(init);
                    } catch {
                        failed = true;
                    }
                    assertTrue(failed, why);
                };

                const badRowId: InsertRowPayload = {
                    action: 'insert', rowId: deriveRowId('other-uuid'), uuid: 'seed-x', values: { label: 'a' },
                };
                await expectCreateFailure({ initialRows: { caps: [badRowId] } },
                    'an initial row with a mismatched rowId should be rejected');

                await expectCreateFailure({ initialRows: { nope: [initialCapRow('seed-1', 'a')] } },
                    'an initial row for a table missing from the pinned schema should be rejected');

                const badType: InsertRowPayload = {
                    action: 'insert', rowId: deriveRowId('seed-2'), uuid: 'seed-2', values: { label: 42 },
                };
                await expectCreateFailure({ initialRows: { caps: [badType] } },
                    'an initial row with a type mismatch should be rejected');

                await expectCreateFailure({}, 'a missing schema object should fail group creation (throw)',
                    'bogus-schema-id');

                await expectCreateFailure({ bindings: { users: 'bogus-group-id' } },
                    'a binding to an object missing from the replica should fail group creation (throw)');
            }
        },
        {
            name: '[RGROUP03] Insert and delete accepted and duplicate insert rejected',
            invoke: async () => {
                const { group } = await createTestEnv();
                const orders = await group.getTable('orders');

                const anonId = deriveRowId('o-1');
                await orders.insert('o-1', { customer: 'ada', total: 10 });

                const authored = await makeIdentity();
                const authoredId = deriveRowId('o-2', authored.keyId);
                await orders.insert('o-2', { customer: 'bob', total: 20 }, authored);

                const view = await orders.getView();
                assertTrue(await view.hasRow(anonId), 'anonymous row should be live');
                assertTrue(await view.hasRow(authoredId), 'authored row should be live');
                assertEquals(await view.getAuthor(anonId), undefined, 'anonymous row should have no author');
                assertEquals(await view.getAuthor(authoredId), authored.keyId, 'authored row should expose its author');

                const row = (await view.getRow(anonId))!;
                assertEquals(row.values['customer'], 'ada', 'carried values should read back');
                assertEquals(row.values['status'], 'new', 'schema defaults should fill absent columns');

                let duplicateFailed = false;
                try {
                    await orders.insert('o-1', { customer: 'ada', total: 11 });
                } catch {
                    duplicateFailed = true;
                }
                assertTrue(duplicateFailed, 'duplicate insert of an existing rowId should be rejected');

                await orders.delete(anonId);
                const after = await orders.getView();
                assertFalse(await after.hasRow(anonId), 'deleted row should not be live');
                assertTrue(await after.hasRow(authoredId), 'other rows should be unaffected');
                assertEquals(await after.getRow(anonId), undefined, 'getRow of a deleted row should be undefined');

                // a delete needs a live row
                let redundantDeleteFailed = false;
                try {
                    await orders.delete(anonId);
                } catch {
                    redundantDeleteFailed = true;
                }
                assertTrue(redundantDeleteFailed, 'deleting a dead row should be rejected');

                // rowIds are write-once: re-insertion after a delete is invalid
                let reinsertFailed = false;
                try {
                    await orders.insert('o-1', { customer: 'ada', total: 12 });
                } catch {
                    reinsertFailed = true;
                }
                assertTrue(reinsertFailed, 're-insertion of a deleted rowId should be rejected');

                // "restoring" means a new row with a fresh uuid
                await orders.insert('o-1b', { customer: 'ada', total: 12 });
                const restored = await orders.getView();
                assertTrue(await restored.hasRow(deriveRowId('o-1b')), 'a fresh-uuid insert should be live');
                assertFalse(await restored.hasRow(anonId), 'the deleted rowId should stay dead');
            }
        },
        {
            name: '[RGROUP04] Deletes are permanent; concurrentDeletes controls cross-branch reach',
            invoke: async () => {
                const { group } = await createTestEnv();
                const scopedDag = await group.getScopedDag();

                const forkInsertDelete = async (table: string, uuid: string, valuesA: json.LiteralMap, valuesB: json.LiteralMap) => {
                    const t = await group.getTable(table);
                    const rowId = deriveRowId(uuid);
                    const base = await scopedDag.getFrontier();

                    // branch A: insert then delete; branch B: a concurrent
                    // duplicate insert of the same rowId (valid there: the
                    // rowId is unseen at branch B's positions)
                    const insA = await t.insert(uuid, valuesA, undefined, base);
                    await t.delete(rowId, undefined, version(insA));
                    const insB = await t.insert(uuid, valuesB, undefined, base);

                    const frontier = await scopedDag.getFrontier();

                    // at the merged frontier the delete is in the row's
                    // history: permanent, dead regardless of the flag
                    const merged = await t.getView(frontier, frontier);
                    const deadAtMerge = !(await merged.hasRow(rowId));

                    // at a position on branch B only (no delete in the row's
                    // history) with the delete visible from the horizon:
                    // reach across concurrency depends on the flag
                    const branchB = await t.getView(version(insB), frontier);
                    const visibleOnBranchB = await branchB.hasRow(rowId);

                    return { deadAtMerge, visibleOnBranchB };
                };

                const orders = await forkInsertDelete('orders', 'cc-1',
                    { customer: 'x', total: 1 }, { customer: 'x', total: 2 });
                assertTrue(orders.deadAtMerge,
                    'causal-only table: the merged-in delete should kill the row');
                assertTrue(orders.visibleOnBranchB,
                    'causal-only table: a merely concurrent delete must not reach branch B');

                const caps = await forkInsertDelete('caps', 'cc-2',
                    { label: 'z1' }, { label: 'z2' });
                assertTrue(caps.deadAtMerge,
                    'concurrentDeletes table: the merged-in delete should kill the row');
                assertFalse(caps.visibleOnBranchB,
                    'concurrentDeletes table: the delete barrier should reach the concurrent branch');
            }
        },
        {
            name: '[RGROUP05] Member table views are consistent at one group position',
            invoke: async () => {
                const { group } = await createTestEnv();
                const scopedDag = await group.getScopedDag();

                const orders = await group.getTable('orders');
                const caps = await group.getTable('caps');

                const orderId = deriveRowId('o-1');
                const capId = deriveRowId('c-1');

                await orders.insert('o-1', { customer: 'ada', total: 10 });
                const mid = await scopedDag.getFrontier();
                await caps.insert('c-1', { label: 'deploy' });
                const end = await scopedDag.getFrontier();

                const midView = await group.getView(mid, mid);
                assertTrue(await (await midView.getTableView('orders')).hasRow(orderId),
                    'the order should be visible at the mid position');
                assertFalse(await (await midView.getTableView('caps')).hasRow(capId),
                    'the cap insert is after the mid position and must not be visible');

                const endView = await group.getView(end, end);
                assertTrue(await (await endView.getTableView('orders')).hasRow(orderId),
                    'the order should be visible at the end position');
                assertTrue(await (await endView.getTableView('caps')).hasRow(capId),
                    'the cap should be visible at the end position');

                // old positions are unaffected by later writes
                await orders.delete(orderId);
                assertTrue(await (await group.getView(mid, mid)).getTableView('orders').then((v) => v.hasRow(orderId)),
                    'the old snapshot should still show the row after a later delete');
            }
        },
        {
            name: '[RGROUP06] Deploy ref-advance updates the effective schema',
            invoke: async () => {
                const { group, schema, admin, pinned } = await createTestEnv();

                await schema.updateSchema([{ rule: 'add-table', def: notesTable() }], admin, 'add notes');
                const v2 = await (await schema.getScopedDag()).getFrontier();

                const before = await group.getView();
                assertFalse(before.getTableNames().includes('notes'),
                    'the new table must not appear before the deploy');

                let earlyAccessFailed = false;
                try {
                    await group.getTable('notes');
                } catch {
                    earlyAccessFailed = true;
                }
                assertTrue(earlyAccessFailed, 'getTable for an undeployed table should throw');

                await group.deploy(v2);

                const after = await group.getView();
                assertTrue(after.getTableNames().includes('notes'),
                    'the new table should appear after the deploy');

                const notes = await group.getTable('notes');
                const noteId = deriveRowId('n-1');
                await notes.insert('n-1', { body: 'hello' });
                assertTrue(await (await notes.getView()).hasRow(noteId),
                    'writes to the newly deployed table should work');

                // the old position still resolves the old schema
                const genesis = version(group.getId());
                const oldView = await group.getView(genesis, genesis);
                assertFalse(oldView.getTableNames().includes('notes'),
                    'the genesis position should still resolve the pinned schema');

                let nonMonotoneFailed = false;
                try {
                    await group.deploy(pinned);
                } catch {
                    nonMonotoneFailed = true;
                }
                assertTrue(nonMonotoneFailed, 'a ref-advance below the current deployed version should be rejected');
            }
        },
        {
            name: '[RGROUP07] Deploy rejected when canDeploy requires authoring',
            invoke: async () => {
                const { group, schema, admin } = await createTestEnv({ canDeploy: { p: 'true' } });

                await schema.updateSchema([{ rule: 'add-table', def: notesTable() }], admin);
                const v2 = await (await schema.getScopedDag()).getFrontier();

                let unauthoredFailed = false;
                try {
                    await group.deploy(v2);
                } catch {
                    unauthoredFailed = true;
                }
                assertTrue(unauthoredFailed, 'an unauthored deploy should be rejected when canDeploy is declared');

                await group.deploy(v2, admin);
                const view = await group.getView();
                assertTrue(view.getTableNames().includes('notes'), 'the authored deploy should land');
            }
        },
        {
            name: '[RGROUP08] findRowIds returns live rows matching pub column values',
            invoke: async () => {
                const { group } = await createTestEnv({
                    initialRows: { caps: [initialCapRow('seed-admin', 'admin')] },
                });
                const caps = await group.getTable('caps');

                const alice = await makeIdentity();
                const d1 = deriveRowId('d-1', alice.keyId);
                const d2 = deriveRowId('d-2');
                const d3 = deriveRowId('d-3');

                await caps.insert('d-1', { label: 'deploy' }, alice);
                await caps.insert('d-2', { label: 'deploy' });
                await caps.insert('d-3', { label: 'deploy' });
                await caps.delete(d3);

                const view = await caps.getView();
                const found = await view.findRowIds({ label: 'deploy' });
                assertEquals(found.toString(), [d1, d2].sort().toString(),
                    'findRowIds should return exactly the live matching rows');

                const admins = await view.findRowIds({ label: 'admin' });
                assertEquals(admins.toString(), [deriveRowId('seed-admin')].toString(),
                    'initial rows should be searchable through pub meta');

                const none = await view.findRowIds({ label: 'nope' });
                assertEquals(none.length, 0, 'no rows should match an absent value');

                const orders = await group.getTable('orders');
                await orders.insert('o-1', { customer: 'ada', total: 1 });
                const ordersView = await orders.getView();

                let nonPubFailed = false;
                try {
                    await ordersView.findRowIds({ total: 1 });
                } catch {
                    nonPubFailed = true;
                }
                assertTrue(nonPubFailed, 'findRowIds over a non-pub column should throw');

                const byCustomer = await ordersView.findRowIds({ customer: 'ada' });
                assertEquals(byCustomer.toString(), [deriveRowId('o-1')].toString(),
                    'pub search should work on other tables too');
            }
        },
        {
            name: '[RGROUP09] Insert rejected when values fail schema conformance',
            invoke: async () => {
                const { group } = await createTestEnv();
                const orders = await group.getTable('orders');

                const expectInsertFailure = async (uuid: string, values: json.LiteralMap, why: string) => {
                    let failed = false;
                    try {
                        await orders.insert(uuid, values);
                    } catch {
                        failed = true;
                    }
                    assertTrue(failed, why);
                };

                await expectInsertFailure('b-1', { customer: 'ada', total: 1, nope: 'x' },
                    'an insert carrying an unknown column should be rejected');
                await expectInsertFailure('b-2', { customer: 'ada', total: 'lots' },
                    'an insert with a type mismatch should be rejected');
                await expectInsertFailure('b-3', { customer: 'ada' },
                    'an insert missing a non-nullable column without default should be rejected');

                // defaulted columns may be omitted
                await orders.insert('g-1', { customer: 'ada', total: 1 });

                // updates work (LWW semantics tested in [LWW])
                await orders.update(deriveRowId('g-1'), { total: 2 });
                const updated = await orders.getView();
                assertEquals((await updated.getRow(deriveRowId('g-1')))!.values['total'], 2,
                    'a valid update should land');
            }
        },
    ],
};
