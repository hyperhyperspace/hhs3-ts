import { assertTrue, assertFalse, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { formatValidationFailure, ValidationRejectedError, version, Version, serializePublicKeyToBase64 } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import { RSchemaImpl, rSchemaFactory } from "../src/rschema/rschema.js";
import { RTableGroupImpl, rTableGroupFactory } from "../src/rtable_group/group.js";
import { deriveRowId } from "../src/rtable/hash.js";
import type { TableDef, Predicate } from "../src/rschema/payload.js";
import type { RTableView } from "../src/rtable/interfaces.js";
import { identitiesTableDef, localIdentityProvider } from "./identity_fixture.js";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

// `authors` get a local identity provider, so their signed row ops verify.
async function createEnv(tables: TableDef[], opts?: {
    canDeploy?: Predicate;
    deployKeys?: OwnIdentity[];
    initialRows?: { [t: string]: json.Literal[] };
    authors?: OwnIdentity[];
    admin?: OwnIdentity;
    selfValidate?: boolean;
}) {
    const ctx = createMockRContext({ selfValidate: opts?.selfValidate ?? true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);

    const admin = opts?.admin ?? await makeIdentity();
    const schemaInit = await RSchemaImpl.create({
        name: 'enf:test_schema',
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        tables: opts?.authors === undefined ? tables : [...tables, identitiesTableDef()],
    });
    const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
    const pinned = await (await schema.getScopedDag()).getFrontier();

    const groupInit = await RTableGroupImpl.create({
        name: 'enf-test-group',
        seed: 'enf-test-group',
        schemaRef: schema.getId(),
        schemaVersion: pinned,
        ...(opts?.canDeploy !== undefined ? { canDeploy: opts.canDeploy } : {}),
        ...(opts?.deployKeys !== undefined ? {
            deployKeys: opts.deployKeys.map((k) => ({ keyId: k.keyId, publicKey: serializePublicKeyToBase64(k.publicKey) })),
        } : {}),
        ...(opts?.authors !== undefined ? localIdentityProvider(opts.authors, opts.initialRows)
            : opts?.initialRows !== undefined ? { initialRows: opts.initialRows } : {}),
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

async function expectFailure(fn: () => Promise<unknown>, why: string, messageIncludes?: string): Promise<void> {
    let error: unknown;
    try {
        await fn();
    } catch (e) {
        error = e;
    }
    assertTrue(error !== undefined, why);
    if (messageIncludes !== undefined) {
        const msg = error instanceof ValidationRejectedError
            ? formatValidationFailure(error.why)
            : error instanceof Error ? error.message : String(error);
        assertTrue(msg.includes(messageIncludes), `expected message to include '${messageIncludes}', got: ${msg}`);
    }
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
                // The line insert is a head of the position the orders table is read at.
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
                await lines.insert('l-1', { order: orderId, qty: 1 }, undefined, base);

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
                await lines.insert('l-1', { order: orderId, qty: 1 }, undefined, base);

                const frontier = await groupFrontier(group);
                assertTrue(await (await tableView(group, 'lines', frontier)).hasRow(lineId),
                    'with concurrentDeletes:false the concurrent delete is not honored, so the target (and the dependent) stay live');
            }
        },
        {
            name: '[ENF06] Exists-gated insert without a witness rejects at validation',
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

                // no witness yet: restrictions are hard validation at `(at, at)`
                await expectFailure(() => items.insert('i-void', { name: 'thing' }),
                    'the exists-gated insert is rejected with no witness',
                    "does not satisfy ALLOW insert IF EXISTS (caps WHERE caps.label = 'grant')");

                // a witness present at-or-before the use validates a later use
                // (at-use semantics: the witness must exist at the op position)
                await caps.insert('c-1', { label: 'grant' });
                const okId = deriveRowId('i-ok');
                await items.insert('i-ok', { name: 'thing' });
                assertTrue(await (await tableView(group, 'items')).hasRow(okId),
                    'an insert with the witness already present is valid');
            }
        },
        {
            name: '[ENF06b] Public row writers reject invalid ops even without selfValidate',
            invoke: async () => {
                const { group } = await createEnv([
                    open('caps', { label: { type: 'string', pub: true } }),
                    {
                        name: 'items',
                        columns: { name: { type: 'string' } },
                        restrictions: [{ on: 'insert', rule: { p: 'exists', table: 'caps', where: { label: 'grant' } } }],
                    },
                ], { selfValidate: false });
                const items = await group.getTable('items');
                await expectFailure(() => items.insert('i-void', { name: 'thing' }),
                    'the public insert API should reject invalid ops even when context selfValidate is off',
                    "does not satisfy ALLOW insert IF EXISTS (caps WHERE caps.label = 'grant')");
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
                // The item insert is a head of the position the caps table is read at.
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
                await items.insert('i-1', { name: 'thing' }, undefined, base);

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
                await items.insert('i-1', { name: 'thing' }, undefined, base);

                const frontier = await groupFrontier(group);
                assertTrue(await (await tableView(group, 'items', frontier)).hasRow(itemId),
                    'a non-barrier (causal-only) revoke does not reach the concurrent use');
            }
        },
        {
            name: '[ENF10] Defaults: unauthored update/delete of an authored row reject; authored-by-author pass',
            invoke: async () => {
                // default restrictions (no `restrictions` declared) in a group
                // with a provider: insert true, update/delete author-is-author
                const author = await makeIdentity();
                const { group } = await createEnv([
                    { name: 'docs', columns: { body: { type: 'string' } } },
                ], { authors: [author] });
                const docs = await group.getTable('docs');
                const rowId = deriveRowId('d-1', author.keyId);

                await docs.insert('d-1', { body: 'v1' }, author);

                // unauthored writes fail the default author-is-author restriction
                // during validation.
                await expectFailure(() => docs.update(rowId, { body: 'v2' }),
                    'an unauthored update of an authored row rejects at validation',
                    'does not satisfy ALLOW update IF docs.rowAuthor = $author');
                await expectFailure(() => docs.delete(rowId),
                    'an unauthored delete of an authored row rejects at validation',
                    'does not satisfy ALLOW delete IF docs.rowAuthor = $author');
                assertEquals((await (await tableView(group, 'docs')).getRow(rowId))!.values['body'], 'v1',
                    'failed unauthored writes leave the row unchanged');

                // authored by the insert author: it lands and resolves
                await docs.update(rowId, { body: 'v3' }, author);
                assertEquals((await (await tableView(group, 'docs')).getRow(rowId))!.values['body'], 'v3',
                    'an author-authored update passes the default restriction');
                await docs.delete(rowId, author);
                assertFalse(await (await tableView(group, 'docs')).hasRow(rowId),
                    'an author-authored delete passes the default restriction');
            }
        },
        {
            name: '[ENF11] grantee-based exists predicates resolve correctly',
            invoke: async () => {
                // items may be updated only by someone named as a cap grantee
                const alice = await makeIdentity();
                const bob = await makeIdentity();
                const { group } = await createEnv([
                    open('caps', { label: { type: 'string', pub: true }, grantee: { type: 'string', pub: true } }),
                    {
                        name: 'items',
                        columns: { name: { type: 'string' } },
                        restrictions: [
                            { on: 'insert', rule: { p: 'true' } },
                            { on: 'update', rule: { p: 'exists', table: 'caps', where: { grantee: '$author' } } },
                        ],
                    },
                ], { authors: [alice, bob] });
                const caps = await group.getTable('caps');
                const items = await group.getTable('items');

                // alice is named as a caps grantee; bob is not
                await caps.insert('c-alice', { label: 'x', grantee: alice.keyId }, alice);

                const itemId = deriveRowId('i-1');
                await items.insert('i-1', { name: 'v1' });

                // bob's update: no caps row owned by bob -> rejected
                await expectFailure(() => items.update(itemId, { name: 'by-bob' }, bob),
                    'an update by a non-cap-holder rejects');
                assertEquals((await (await tableView(group, 'items')).getRow(itemId))!.values['name'], 'v1',
                    'a rejected non-cap-holder update leaves the row unchanged');

                // alice's update: she owns a caps row -> passes
                await items.update(itemId, { name: 'by-alice' }, alice);
                assertEquals((await (await tableView(group, 'items')).getRow(itemId))!.values['name'], 'by-alice',
                    'an update by a cap-holder passes the exists/grantee predicate');
            }
        },
        {
            name: '[ENF12] An unauthorized op inside a bundle rejects the whole bundle',
            invoke: async () => {
                // bundle of two inserts; one is exists-gated with no witness ->
                // the whole bundle rejects before append
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
                await expectFailure(() => group.bundle([
                    { table: 'free', op: { action: 'insert', rowId: freeId, uuid: 'f-1', values: { v: 'a' } } },
                    { table: 'gated', op: { action: 'insert', rowId: gatedId, uuid: 'g-1', values: { v: 'b' } } },
                ]), 'a bundle with an unauthorized op rejects at validation');

                assertFalse(await (await tableView(group, 'gated')).hasRow(gatedId), 'the gated op never appends');
                assertFalse(await (await tableView(group, 'free')).hasRow(freeId),
                    'its bundle sibling never appends either (all-or-nothing)');
            }
        },
        {
            name: '[ENF13] canDeploy predicate evaluated on deploy: passing and failing authors',
            invoke: async () => {
                // canDeploy: exists admins where grantee = $author
                // both keys are embedded, so both signatures verify and the
                // predicate alone decides
                const admins = open('admins', { label: { type: 'string', pub: true }, grantee: { type: 'string', pub: true } });
                const admin = await makeIdentity();
                const stranger = await makeIdentity();
                const { group, schema } = await createEnv(
                    [admins, open('orders', { customer: { type: 'string' } })],
                    {
                        admin,
                        canDeploy: { p: 'exists', table: 'admins', where: { grantee: '$author' } },
                        deployKeys: [admin, stranger],
                        initialRows: {
                            admins: [{ action: 'insert', rowId: deriveRowId('seed-admin'), uuid: 'seed-admin', values: { label: 'root', grantee: admin.keyId } }],
                        },
                    },
                );

                await schema.updateSchema([{ rule: 'add-table', def: open('notes', { body: { type: 'string' } }) }], admin);
                const v2 = await (await schema.getScopedDag()).getFrontier();

                let strangerFailed = false;
                let strangerMessage = '';
                try {
                    await group.deploy(v2, stranger);
                } catch (e) {
                    strangerFailed = true;
                    if (e instanceof ValidationRejectedError) {
                        strangerMessage = formatValidationFailure(e.why);
                    }
                }
                assertTrue(strangerFailed, 'a deploy by a non-admin should fail canDeploy');
                assertTrue(
                    strangerMessage.includes('canDeploy predicate rejected schema deploy: EXISTS (admins WHERE admins.grantee = $author)'),
                    `canDeploy failure should name the predicate, got: ${strangerMessage}`,
                );

                await group.deploy(v2, admin);
                assertTrue((await group.getView()).getTableNames().includes('notes'),
                    'a deploy by an admin grantee should pass canDeploy');
            }
        },
        {
            name: '[ENF14] A null write is accepted for a nullable column only, and never in an update of a readonly one',
            invoke: async () => {
                const { group } = await createEnv([
                    open('notes', {
                        title: { type: 'string' },
                        memo: { type: 'string', nullable: true },
                        tag: { type: 'string', nullable: true, default: 'none' },
                        origin: { type: 'string', nullable: true, readonly: true },
                    }),
                ]);
                const notes = await group.getTable('notes');

                // inserts: a null is fine for any nullable column, with a default or without
                await notes.insert('n-1', { title: 'a', memo: null });
                await notes.insert('n-2', { title: 'a', tag: null });
                await notes.insert('n-3', { title: 'a', origin: null });
                await notes.insert('n-4', { title: 'a', memo: null, tag: null, origin: null });
                await expectFailure(() => notes.insert('n-5', { title: null }),
                    'a null for a NOT NULL column is refused in an insert', "column 'title' is not nullable");
                await expectFailure(() => notes.insert('n-6', { title: 'a', nope: null }),
                    'a null for an unknown column is refused in an insert', "column 'nope' does not exist");

                // updates: the same, and a readonly column cannot be nulled
                const row = deriveRowId('n-1');
                await notes.update(row, { memo: null });
                await notes.update(row, { tag: null, title: 'b' });
                await expectFailure(() => notes.update(row, { title: null }),
                    'a null for a NOT NULL column is refused in an update', "column 'title' is not nullable");
                await expectFailure(() => notes.update(row, { origin: null }),
                    'a null for a readonly column is refused in an update', "column 'origin' is readonly");
                await expectFailure(() => notes.update(row, { nope: null }),
                    'a null for an unknown column is refused in an update', "column 'nope' does not exist");
                await expectFailure(() => notes.update(row, {}), 'an update that writes nothing is refused');
            }
        },
        {
            name: '[ENF15] An identity provider row cannot null its key id or its public key',
            invoke: async () => {
                const someone = await makeIdentity();
                const publicKey = serializePublicKeyToBase64(someone.publicKey);
                // the provider columns are nullable here, so only the provider rule stands in the way
                const { group } = await createEnv([{
                    name: 'people',
                    columns: {
                        keyId: { type: 'string', pub: true, readonly: true, nullable: true },
                        publicKey: { type: 'string', pub: true, readonly: true, nullable: true },
                        name: { type: 'string', nullable: true },
                    },
                    idProvider: { keyIdColumn: 'keyId', publicKeyColumn: 'publicKey' },
                    restrictions: [{ on: 'all', rule: { p: 'true' } }],
                }]);
                const people = await group.getTable('people');

                await people.insert('p-1', { keyId: someone.keyId, publicKey, name: null });
                await expectFailure(() => people.insert('p-2', { keyId: null, publicKey }),
                    'a provider row that nulls its key id is refused', 'provider row must carry');
                await expectFailure(() => people.insert('p-3', { keyId: someone.keyId, publicKey: null }),
                    'a provider row that nulls its public key is refused', 'provider row must carry');
            }
        },
        {
            name: '[ENF16] A null write that becomes void is ignored: the earlier value shows again, or the default',
            invoke: async () => {
                const { group } = await createEnv([
                    open('targets', { name: { type: 'string' } }, { concurrentDeletes: true }),
                    open('notes', {
                        title: { type: 'string' },
                        memo: { type: 'string', nullable: true },
                        tag: { type: 'string', nullable: true, default: 'none' },
                        ref: { type: 'string', nullable: true },
                    }, { fks: { ref: 'targets' } }),
                ]);
                const targets = await group.getTable('targets');
                const notes = await group.getTable('notes');
                const target = deriveRowId('t-1');
                const wrote = deriveRowId('n-1');
                const neverWrote = deriveRowId('n-2');

                await targets.insert('t-1', { name: 't' });
                await notes.insert('n-1', { title: 'a', memo: 'm', tag: 'x' });
                await notes.insert('n-2', { title: 'a' });
                const base = await groupFrontier(group);

                // one branch deletes the target; on the other, an update of each
                // note nulls columns and points `ref` at the target, so the
                // delete voids both updates at the merge
                await targets.delete(target, undefined, base);
                const h1 = await notes.update(wrote, { memo: null, tag: null, ref: target }, undefined, base);
                const h2 = await notes.update(neverWrote, { tag: null, ref: target }, undefined, base);

                const branch = await tableView(group, 'notes', version(h1, h2));
                const onBranch1 = (await branch.getRow(wrote))!.values;
                assertTrue(!('memo' in onBranch1) && !('tag' in onBranch1), 'on their own branch the nulls show');
                assertTrue(!('tag' in (await branch.getRow(neverWrote))!.values), 'on their own branch the null hides the default');

                const merged = await tableView(group, 'notes');
                const after1 = (await merged.getRow(wrote))!.values;
                assertEquals(after1['memo'], 'm', 'a void null does not hide the earlier value');
                assertEquals(after1['tag'], 'x', 'a void null does not hide the earlier value of a column with a default');
                const after2 = (await merged.getRow(neverWrote))!.values;
                assertEquals(after2['tag'], 'none', 'with no earlier write, the default shows again');
                assertTrue(!('ref' in after1) && !('ref' in after2), 'the void reference is gone too');
            }
        },
        {
            name: '[ENF17] A null in a foreign key column is no reference: nothing is checked for it',
            invoke: async () => {
                const { group } = await createEnv([
                    open('targets', { name: { type: 'string' } }),
                    open('notes', { title: { type: 'string' }, ref: { type: 'string', nullable: true } }, { fks: { ref: 'targets' } }),
                ]);
                const targets = await group.getTable('targets');
                const notes = await group.getTable('notes');
                const target = deriveRowId('t-1');
                const noteId = deriveRowId('n-1');

                await targets.insert('t-1', { name: 't' });
                await notes.insert('n-1', { title: 'a', ref: target });
                await notes.update(noteId, { ref: null });
                await targets.delete(target);

                // the row dropped its reference before the target went away
                await notes.update(noteId, { title: 'b' });
                await notes.update(noteId, { ref: null, title: 'c' });
                await notes.insert('n-2', { title: 'a', ref: null });

                const view = await tableView(group, 'notes');
                const row = (await view.getRow(noteId))!;
                assertEquals(row.values['title'], 'c', 'the row is live and takes updates');
                assertTrue(!('ref' in row.values), 'its foreign key column reads NULL');
                assertTrue(await view.hasRow(deriveRowId('n-2')), 'an insert with a null foreign key is live');
                await expectFailure(() => notes.update(noteId, { ref: target }),
                    'a reference to the deleted target is still refused');
            }
        },
        {
            name: '[ENF18] An update answers only for the foreign keys it sets, an insert also for the defaults it leaves in place',
            invoke: async () => {
                const inbox = deriveRowId('inbox');
                const p1 = deriveRowId('p-1');
                const { group } = await createEnv([
                    open('projects', { name: { type: 'string' } }),
                    open('tasks', { title: { type: 'string' }, project: { type: 'string', nullable: true, default: inbox } },
                        { fks: { project: 'projects' } }),
                ]);
                const projects = await group.getTable('projects');
                const tasks = await group.getTable('tasks');
                const taskId = deriveRowId('t-1');

                await projects.insert('inbox', { name: 'inbox' });
                await projects.insert('p-1', { name: 'p1' });
                await tasks.insert('t-1', { title: 'a', project: p1 });   // never pointed at the default
                await projects.delete(inbox);

                // the update does not write `project`, so the default's dead target is not its concern
                await tasks.update(taskId, { title: 'b' });
                assertEquals((await (await tableView(group, 'tasks')).getRow(taskId))!.values['title'], 'b',
                    'an update of another column is accepted after the default target was deleted');

                // nor is the row's own target, once it is deleted later
                await projects.delete(p1);
                await tasks.update(taskId, { title: 'c' });
                assertEquals((await (await tableView(group, 'tasks')).getRow(taskId))!.values['title'], 'c',
                    'a row whose own target was deleted later still takes updates');

                // what an operation does write, or leaves to the default in an insert, it answers for
                await expectFailure(() => tasks.update(taskId, { project: inbox }),
                    'an update that sets the column to a dead target is refused', "FK column 'project'");
                await expectFailure(() => tasks.insert('t-2', { title: 'a' }),
                    'an insert that leaves the column out answers for the default', "FK column 'project'");
            }
        },
        {
            name: '[ENF19] An explicit null in a foreign key column drops the reference its default would make',
            invoke: async () => {
                const inbox = deriveRowId('inbox');
                const p1 = deriveRowId('p-1');
                const { group } = await createEnv([
                    open('projects', { name: { type: 'string' } }),
                    open('tasks', { title: { type: 'string' }, project: { type: 'string', nullable: true, default: inbox } },
                        { fks: { project: 'projects' } }),
                ]);
                const projects = await group.getTable('projects');
                const tasks = await group.getTable('tasks');

                await projects.insert('inbox', { name: 'inbox' });
                await projects.insert('p-1', { name: 'p1' });
                await tasks.insert('t-1', { title: 'a', project: p1 });
                await projects.delete(inbox);

                // an insert that nulls the column makes no reference at all
                await tasks.insert('t-2', { title: 'a', project: null });
                const inserted = (await (await tableView(group, 'tasks')).getRow(deriveRowId('t-2')))!;
                assertTrue(!('project' in inserted.values), 'the row is live and reads NULL, not the default');

                // an update to null drops the reference the row held
                await tasks.update(deriveRowId('t-1'), { project: null });
                await projects.delete(p1);
                await tasks.update(deriveRowId('t-1'), { title: 'b' });
                const cleared = (await (await tableView(group, 'tasks')).getRow(deriveRowId('t-1')))!;
                assertTrue(!('project' in cleared.values) && cleared.values['title'] === 'b', 'the row is live with no reference');
            }
        },
    ],
};
