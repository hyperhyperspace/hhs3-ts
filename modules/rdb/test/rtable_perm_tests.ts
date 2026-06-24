import { assertTrue, assertFalse, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { B64Hash, KeyId, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { Version } from "@hyper-hyper-space/hhs3_mvt";
import { signPayload, serializePublicKeyToBase64 } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import { RSchemaImpl, rSchemaFactory } from "../src/rschema/rschema.js";
import { RTableGroupImpl, rTableGroupFactory } from "../src/rtable_group/group.js";
import { deriveRowId } from "../src/rtable/hash.js";
import type { TableDef, Predicate } from "../src/rschema/payload.js";
import type { RContext } from "@hyper-hyper-space/hhs3_mvt";
import type { RTableView } from "../src/rtable/interfaces.js";
import {
    createUsersGroup, registerIdentity, grantCap, revokeCap, findCapGrants,
    usersSchemaTables, IDENTITIES_TABLE, CAPS_TABLE, USERS_IDENTITIES_PROVIDER,
} from "../src/users/users.js";

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

async function frontier(group: RTableGroupImpl): Promise<Version> {
    return (await group.getScopedDag()).getFrontier();
}

async function tableView(group: RTableGroupImpl, name: string, at?: Version): Promise<RTableView> {
    return (await group.getView(at, at)).getTableView(name);
}

async function expectThrow(fn: () => Promise<unknown>, why: string): Promise<void> {
    let threw = false;
    try { await fn(); } catch { threw = true; }
    assertTrue(threw, why);
}

function sameVersion(a: Version, b: Version): boolean {
    return [...a].sort().join('|') === [...b].sort().join('|');
}

// An app group that verifies authorship via a bound Users group's identities
// provider ('users.identities'), cross-group.
async function makeAppGroup(ctx: RContext, seed: string, tables: TableDef[], usersGroupId: B64Hash, opts?: {
    creator?: OwnIdentity;
    canDeploy?: Predicate;
    initialRows?: { [t: string]: json.Literal[] };
}): Promise<{ schema: RSchemaImpl; group: RTableGroupImpl; creator: OwnIdentity }> {
    const creator = opts?.creator ?? await makeIdentity();
    const schemaInit = await RSchemaImpl.create({
        name: `${seed.replace(/[^a-zA-Z0-9_]+/g, '_')}:schema`,
        creators: [{ keyId: creator.keyId, publicKey: creator.publicKey }],
        tables,
    });
    const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
    const pinned = await (await schema.getScopedDag()).getFrontier();

    const groupInit = await RTableGroupImpl.create({
        name: seed,
        seed: seed + '-group',
        schemaRef: schema.getId(),
        schemaVersion: pinned,
        bindings: { users: usersGroupId },
        idProvider: USERS_IDENTITIES_PROVIDER,
        ...(opts?.canDeploy !== undefined ? { canDeploy: opts.canDeploy } : {}),
        ...(opts?.initialRows !== undefined ? { initialRows: opts.initialRows } : {}),
    });
    const group = (await ctx.createObject(groupInit)) as RTableGroupImpl;
    return { schema, group, creator };
}

// A signed row-envelope payload (for direct validatePayload assertions).
async function authoredInsertEnvelope(
    table: string, uuid: string, values: { [c: string]: json.Literal }, author: OwnIdentity,
): Promise<json.LiteralMap> {
    const base: { [k: string]: json.Literal } = { action: 'insert', rowId: deriveRowId(uuid, author.keyId), uuid, values };
    const op = await signPayload(base, author);
    return { action: 'row', table, op: op as unknown as json.Literal };
}

// docs: insert open (default true), update/delete author-is-author (defaults)
const docsTable: TableDef = { name: 'docs', columns: { body: { type: 'string' } } };

export const rtablePermTests = {
    title: '[PERM] Signed ops + permissions tests',
    tests: [
        {
            name: '[PERM01] a registered author validates; a tampered signature is REJECTED at validation',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const users = await createUsersGroup(ctx, admin);
                const app = await makeAppGroup(ctx, 'perm01', [docsTable], users.group.getId());

                const alice = await makeIdentity();
                await registerIdentity(users.group, alice);
                await app.group.observe('users', await frontier(users.group));

                const at = await frontier(app.group);
                const env = await authoredInsertEnvelope('docs', 'd-1', { body: 'x' }, alice);
                assertTrue((await app.group.validatePayload(env, at)).valid,
                    "a registered author's signed insert validates");

                const sig = (env.op as json.LiteralMap).signature as string;
                const tamperedSig = (sig[0] === 'A' ? 'B' : 'A') + sig.slice(1);
                const tampered: json.LiteralMap = { ...env, op: { ...(env.op as json.LiteralMap), signature: tamperedSig } };
                assertFalse((await app.group.validatePayload(tampered, at)).valid,
                    'a tampered signature is rejected at validation (never enters the DAG)');
            }
        },
        {
            name: '[PERM02] impersonation (keyId != hash(publicKey)) is rejected at insert',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const users = await createUsersGroup(ctx, admin);

                const alice = await makeIdentity();
                const mallory = await makeIdentity();

                const at = await frontier(users.group);
                // claim alice's keyId but supply mallory's public key
                const bad: json.LiteralMap = {
                    action: 'row', table: IDENTITIES_TABLE,
                    op: {
                        action: 'insert', rowId: deriveRowId('imp'), uuid: 'imp',
                        values: { keyId: alice.keyId, publicKey: serializePublicKeyToBase64(mallory.publicKey) },
                    },
                };
                const badResult = await users.group.validatePayload(bad, at);
                assertFalse(badResult.valid,
                    'a provider row whose keyId is not the hash of its publicKey is rejected');
                assertTrue(!badResult.valid && badResult.why.parent?.parent?.reason === 'provider keyId does not match public key',
                    'provider integrity failure includes the nested direct cause');

                // the self-consistent registration validates
                const good: json.LiteralMap = {
                    action: 'row', table: IDENTITIES_TABLE,
                    op: {
                        action: 'insert', rowId: deriveRowId('ok'), uuid: 'ok',
                        values: { keyId: alice.keyId, publicKey: serializePublicKeyToBase64(alice.publicKey) },
                    },
                };
                assertTrue((await users.group.validatePayload(good, at)).valid, 'a self-certifying registration validates');
            }
        },
        {
            name: '[PERM03] a truly unauthored op is anonymous (passes authentication trivially)',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const users = await createUsersGroup(ctx, admin);
                const app = await makeAppGroup(ctx, 'perm03', [docsTable], users.group.getId());

                const docs = await app.group.getTable('docs');
                const dId = deriveRowId('d-anon');
                await docs.insert('d-anon', { body: 'x' });   // no author
                assertTrue(await (await tableView(app.group, 'docs')).hasRow(dId),
                    'an unauthored insert (insert restriction true) is live');
            }
        },
        {
            name: '[PERM04] a claimed author that cannot be resolved is REJECTED (not downgraded to anonymous)',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const users = await createUsersGroup(ctx, admin);
                const app = await makeAppGroup(ctx, 'perm04', [docsTable], users.group.getId());

                // bob is never registered (and never observed)
                const bob = await makeIdentity();
                await app.group.observe('users', await frontier(users.group));

                const at = await frontier(app.group);
                const env = await authoredInsertEnvelope('docs', 'd-1', { body: 'x' }, bob);
                assertFalse((await app.group.validatePayload(env, at)).valid,
                    'a signed op whose author is unresolvable is rejected (no anonymous downgrade)');

                // the high-level writer throws (reject), confirming it never lands
                const docs = await app.group.getTable('docs');
                await expectThrow(() => docs.insert('d-1', { body: 'x' }, bob),
                    'writing an op by an unresolvable author is rejected');
            }
        },
        {
            name: '[PERM05] authentication and authorization are validation: registered-but-unauthorized update rejects',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const users = await createUsersGroup(ctx, admin);
                const app = await makeAppGroup(ctx, 'perm05', [docsTable], users.group.getId());

                const alice = await makeIdentity();
                const bob = await makeIdentity();
                await registerIdentity(users.group, alice);
                await registerIdentity(users.group, bob);
                await app.group.observe('users', await frontier(users.group));

                const docs = await app.group.getTable('docs');
                const rowId = deriveRowId('d-1', alice.keyId);
                await docs.insert('d-1', { body: 'v1' }, alice);

                // bob is registered, so his signature verifies, but
                // author-is-author fails hard validation.
                await expectThrow(() => docs.update(rowId, { body: 'by-bob' }, bob),
                    "a registered non-author's update rejects at validation");
                assertEquals((await (await tableView(app.group, 'docs')).getRow(rowId))!.values['body'], 'v1',
                    "a rejected non-author update leaves the row unchanged");

                // the insert author's update passes both authentication and authorization
                await docs.update(rowId, { body: 'v3' }, alice);
                assertEquals((await (await tableView(app.group, 'docs')).getRow(rowId))!.values['body'], 'v3',
                    "the author's signed update resolves");
            }
        },
        {
            name: '[PERM06] deploy authorization: signature verified at validation, then canDeploy',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();

                // a group with a LOCAL provider + admins table, canDeploy gated
                const tables: TableDef[] = [
                    ...usersSchemaTables(),   // identities (provider) + caps
                    { name: 'admins', columns: { label: { type: 'string', pub: true, readonly: true }, grantee: { type: 'string', pub: true, readonly: true } } },
                    { name: 'orders', columns: { customer: { type: 'string' } } },
                ];
                const schemaInit = await RSchemaImpl.create({
                    name: 'perm06:schema',
                    creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
                    tables,
                });
                const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
                const pinned = await (await schema.getScopedDag()).getFrontier();

                const groupInit = await RTableGroupImpl.create({
                    name: 'perm06-group',
                    seed: 'perm06-group',
                    schemaRef: schema.getId(),
                    schemaVersion: pinned,
                    idProvider: IDENTITIES_TABLE,   // LOCAL provider
                    canDeploy: { p: 'exists', table: 'admins', where: { grantee: '$author' } },
                    initialRows: {
                        [IDENTITIES_TABLE]: [{ action: 'insert', rowId: deriveRowId('admin'), uuid: 'admin', values: { keyId: admin.keyId, publicKey: serializePublicKeyToBase64(admin.publicKey) } }],
                        admins: [{ action: 'insert', rowId: deriveRowId('a-admin'), uuid: 'a-admin', values: { label: 'root', grantee: admin.keyId } }],
                    },
                });
                const group = (await ctx.createObject(groupInit)) as RTableGroupImpl;

                // a registered-but-not-admin identity
                const mallory = await makeIdentity();
                await registerIdentity(group, mallory);

                // a schema-update to deploy
                await schema.updateSchema([{ rule: 'add-table', def: { name: 'notes', columns: { body: { type: 'string' } } } }], admin);
                const v2 = await (await schema.getScopedDag()).getFrontier();

                // admin: signature resolves + canDeploy holds -> valid
                await group.deploy(v2, admin);
                assertTrue((await group.getView()).getTableNames().includes('notes'), 'admin deploy applies');

                // mallory: signature resolves but canDeploy fails -> rejected
                await expectThrow(() => group.deploy(v2, mallory),
                    'a registered non-admin deploy is rejected (canDeploy)');

                // stranger: signature unresolvable -> rejected at validation
                const stranger = await makeIdentity();
                await expectThrow(() => group.deploy(v2, stranger),
                    'an unresolvable deploy author is rejected at validation');
            }
        },
        {
            name: '[PERM07] authorization cycle is DENIED: mutually-gated caps in one bundle reject',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const users = await createUsersGroup(ctx, admin);

                // ca insert gated by `exists cb owned by $author`, and vice versa
                const tables: TableDef[] = [
                    { name: 'ca', columns: { v: { type: 'string' }, grantee: { type: 'string', pub: true } },
                      restrictions: [{ on: 'insert', rule: { p: 'exists', table: 'cb', where: { grantee: '$author' } } }] },
                    { name: 'cb', columns: { v: { type: 'string' }, grantee: { type: 'string', pub: true } },
                      restrictions: [{ on: 'insert', rule: { p: 'exists', table: 'ca', where: { grantee: '$author' } } }] },
                ];
                const app = await makeAppGroup(ctx, 'perm07', tables, users.group.getId());

                const alice = await makeIdentity();
                await registerIdentity(users.group, alice);
                await app.group.observe('users', await frontier(users.group));

                const caId = deriveRowId('ca-1', alice.keyId);
                const cbId = deriveRowId('cb-1', alice.keyId);
                // Bundle siblings cannot authorize each other; both grants
                // would need to exist at the parent frontier.
                await expectThrow(() => app.group.bundle([
                    { table: 'ca', op: { action: 'insert', rowId: caId, uuid: 'ca-1', values: { v: 'a', grantee: alice.keyId } } },
                    { table: 'cb', op: { action: 'insert', rowId: cbId, uuid: 'cb-1', values: { v: 'b', grantee: alice.keyId } } },
                ], alice), 'a self-supporting authorization cycle is rejected at validation');

                assertFalse(await (await tableView(app.group, 'ca')).hasRow(caId),
                    'a self-supporting authorization cycle never appends ca');
                assertFalse(await (await tableView(app.group, 'cb')).hasRow(cbId),
                    'a self-supporting authorization cycle never appends cb');
            }
        },
        {
            name: '[PERM08] resolveRefVersion returns the observed foreign version of a bound group',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const users = await createUsersGroup(ctx, admin);
                const app = await makeAppGroup(ctx, 'perm08', [docsTable], users.group.getId());

                const alice = await makeIdentity();
                await registerIdentity(users.group, alice);
                const observed = await frontier(users.group);
                await app.group.observe('users', observed);

                const view = await app.group.getView();
                const resolved = await view.resolveRefVersion(users.group.getId());
                assertTrue(sameVersion(resolved, observed),
                    'the group view resolves the bound group to the observed foreign version');
            }
        },
        {
            name: '[PERM09] intra-group cap: use-before-revoke valid; concurrent barrier revoke voids the use',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();

                // a single group with a LOCAL provider, an open caps table, and a
                // docs table gated by `exists caps where grantee=$author, label=writer`
                const tables: TableDef[] = [
                    ...usersSchemaTables(),
                    { name: 'wcaps', columns: { label: { type: 'string', pub: true, readonly: true }, grantee: { type: 'string', pub: true, readonly: true } },
                      concurrentDeletes: true, restrictions: [{ on: 'all', rule: { p: 'true' } }] },
                    { name: 'docs', columns: { body: { type: 'string' } },
                      restrictions: [{ on: 'insert', rule: { p: 'exists', table: 'wcaps', where: { label: 'writer', grantee: '$author' } } }] },
                ];
                const schemaInit = await RSchemaImpl.create({
                    name: 'perm09:schema',
                    creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
                    tables,
                });
                const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
                const pinned = await (await schema.getScopedDag()).getFrontier();
                const groupInit = await RTableGroupImpl.create({
                    name: 'perm09-group', seed: 'perm09-group', schemaRef: schema.getId(), schemaVersion: pinned,
                    idProvider: IDENTITIES_TABLE,
                    initialRows: {
                        [IDENTITIES_TABLE]: [{ action: 'insert', rowId: deriveRowId('admin'), uuid: 'admin', values: { keyId: admin.keyId, publicKey: serializePublicKeyToBase64(admin.publicKey) } }],
                    },
                });
                const group = (await ctx.createObject(groupInit)) as RTableGroupImpl;

                const alice = await makeIdentity();
                await registerIdentity(group, alice);

                // alice gets a writer cap (open insert), granted to alice
                const wcaps = await group.getTable('wcaps');
                const capId = deriveRowId('w-alice');
                await wcaps.insert('w-alice', { label: 'writer', grantee: alice.keyId });

                const base = await frontier(group);
                const docs = await group.getTable('docs');

                // branch 1: alice uses the cap (authored) at base
                const dId = deriveRowId('d-1', alice.keyId);
                await docs.insert('d-1', { body: 'x' }, alice, base);

                // branch 2 (concurrent): revoke the cap (barrier) at base
                await wcaps.delete(capId, undefined, base);

                const merged = await frontier(group);
                assertFalse(await (await tableView(group, 'docs', merged)).hasRow(dId),
                    'a concurrent barrier revoke of the witnessing cap voids the use');
            }
        },
        {
            name: '[USERS01] cap delegation chain: admin -> manager(alice) -> editor(bob); an unauthorized grant rejects',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const users = await createUsersGroup(ctx, admin);

                const alice = await makeIdentity();
                const bob = await makeIdentity();
                const carol = await makeIdentity();
                await registerIdentity(users.group, alice);
                await registerIdentity(users.group, bob);
                await registerIdentity(users.group, carol);

                // admin (holds the genesis manager cap) grants a manager cap to alice
                await grantCap(users.group, admin, alice.keyId, users.managerLabel);
                assertTrue((await (await tableView(users.group, CAPS_TABLE)).findRowIds({ label: users.managerLabel, grantee: alice.keyId })).length === 1,
                    'alice now holds a manager cap (granted by admin)');

                // alice (now a manager) grants an editor cap to bob
                await grantCap(users.group, alice, bob.keyId, 'editor');
                assertTrue((await (await tableView(users.group, CAPS_TABLE)).findRowIds({ label: 'editor', grantee: bob.keyId })).length === 1,
                    'bob holds an editor cap (granted by manager alice)');

                // bob (only an editor, not a manager) tries to grant: it fails
                // hard validation.
                await expectThrow(() => grantCap(users.group, bob, carol.keyId, 'editor'),
                    "a non-manager's grant rejects (no manager cap granted to the author)");
                assertTrue((await (await tableView(users.group, CAPS_TABLE)).findRowIds({ label: 'editor', grantee: carol.keyId })).length === 0,
                    "a rejected non-manager grant leaves carol without an editor cap");
            }
        },
        {
            name: '[USERS02] cross-group provider fail-closed: dropping the foreign provider table -> authored ops REJECTED at validation (no throw)',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const users = await createUsersGroup(ctx, admin);
                const app = await makeAppGroup(ctx, 'users02', [docsTable], users.group.getId());

                const alice = await makeIdentity();
                await registerIdentity(users.group, alice);
                await app.group.observe('users', await frontier(users.group));

                // while the provider is present, alice's authored op validates
                const at1 = await frontier(app.group);
                const env1 = await authoredInsertEnvelope('docs', 'd-1', { body: 'x' }, alice);
                assertTrue((await app.group.validatePayload(env1, at1)).valid, 'authored op validates while the provider is present');

                // drop the identities table in the Users schema, deploy, observe
                await users.schema.updateSchema([{ rule: 'drop-table', table: IDENTITIES_TABLE }], admin);
                const v2 = await (await users.schema.getScopedDag()).getFrontier();
                await users.group.deploy(v2);
                await app.group.observe('users', await frontier(users.group));

                // now alice is unresolvable through the (present but provider-less)
                // foreign group -> the authored op is REJECTED at validation, and
                // crucially this is `false`, not a throw (the object IS present)
                const at2 = await frontier(app.group);
                const env2 = await authoredInsertEnvelope('docs', 'd-2', { body: 'y' }, alice);
                assertFalse((await app.group.validatePayload(env2, at2)).valid,
                    'a present-but-unresolvable provider fail-closes (reject), without throwing');
            }
        },
        {
            name: '[USERS03] a missing bound provider OBJECT throws (sync defers)',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const users = await createUsersGroup(ctx, admin);

                const alice = await makeIdentity();
                await registerIdentity(users.group, alice);

                // app binds a NON-EXISTENT users group id -> the create itself
                // throws (binding object absent), confirming a missing bound
                // object is an infrastructure (defer) condition, not data
                const creator = await makeIdentity();
                const schemaInit = await RSchemaImpl.create({
                    name: 'users03:schema',
                    creators: [{ keyId: creator.keyId, publicKey: creator.publicKey }],
                    tables: [docsTable],
                });
                const schemaA = (await ctx.createObject(schemaInit)) as RSchemaImpl;
                const pinned = await (await schemaA.getScopedDag()).getFrontier();

                await expectThrow(async () => {
                    const init = await RTableGroupImpl.create({
                        name: 'users03-group', seed: 'users03-group', schemaRef: schemaA.getId(), schemaVersion: pinned,
                        bindings: { users: deriveRowId('no-such-users-group') },
                        idProvider: USERS_IDENTITIES_PROVIDER,
                    });
                    return ctx.createObject(init);
                }, 'binding a missing provider group object throws (defer)');
            }
        },
        {
            name: '[USERS04] re-grant after revoke',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const users = await createUsersGroup(ctx, admin);

                const bob = await makeIdentity();
                await registerIdentity(users.group, bob);

                await grantCap(users.group, admin, bob.keyId, 'editor');
                assertEquals((await findCapGrants(users.group, bob.keyId, 'editor')).length, 1,
                    'bob holds editor after grant');

                await revokeCap(users.group, admin, bob.keyId, 'editor');
                assertEquals((await findCapGrants(users.group, bob.keyId, 'editor')).length, 0,
                    'bob loses editor after revoke');

                await grantCap(users.group, admin, bob.keyId, 'editor');
                assertEquals((await findCapGrants(users.group, bob.keyId, 'editor')).length, 1,
                    'bob holds editor again after re-grant');
            }
        },
        {
            name: '[USERS05] grantCap is a no-op when grant already live',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const users = await createUsersGroup(ctx, admin);

                const bob = await makeIdentity();
                await registerIdentity(users.group, bob);

                const first = await grantCap(users.group, admin, bob.keyId, 'editor');
                assertTrue(first !== undefined, 'first grant should return an entry hash');

                const rowIds = await findCapGrants(users.group, bob.keyId, 'editor');
                assertEquals(rowIds.length, 1);

                const second = await grantCap(users.group, admin, bob.keyId, 'editor');
                assertTrue(second === undefined, 'second grant should no-op');

                const rowIdsAfter = await findCapGrants(users.group, bob.keyId, 'editor');
                assertEquals(rowIdsAfter.length, 1);
                assertEquals(rowIdsAfter[0], rowIds[0], 'same rowId should remain live');
            }
        },
        {
            name: '[USERS06] revokeCap is a no-op when no live grant',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const users = await createUsersGroup(ctx, admin);

                const bob = await makeIdentity();
                await registerIdentity(users.group, bob);

                const result = await revokeCap(users.group, admin, bob.keyId, 'editor');
                assertTrue(result === undefined, 'revoke with no grant should no-op');
            }
        },
        {
            name: '[USERS07] revokeCap clears all live matching grants in one bundle',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const users = await createUsersGroup(ctx, admin);

                const bob = await makeIdentity();
                await registerIdentity(users.group, bob);

                const base = await frontier(users.group);
                const capId1 = deriveRowId('dup-cap-a', admin.keyId);
                const capId2 = deriveRowId('dup-cap-b', admin.keyId);
                await users.group.bundle([
                    { table: CAPS_TABLE, op: { action: 'insert', rowId: capId1, uuid: 'dup-cap-a', values: { label: 'editor', grantee: bob.keyId } } },
                    { table: CAPS_TABLE, op: { action: 'insert', rowId: capId2, uuid: 'dup-cap-b', values: { label: 'editor', grantee: bob.keyId } } },
                ], admin, base);

                assertEquals((await findCapGrants(users.group, bob.keyId, 'editor')).length, 2,
                    'two duplicate witnesses should be live');

                await revokeCap(users.group, admin, bob.keyId, 'editor');

                assertEquals((await findCapGrants(users.group, bob.keyId, 'editor')).length, 0);
                const view = await tableView(users.group, CAPS_TABLE);
                assertFalse(await view.hasRow(capId1), 'first witness should be dead');
                assertFalse(await view.hasRow(capId2), 'second witness should be dead');
            }
        },
        {
            name: '[USERS08] re-grant uses a fresh rowId',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const users = await createUsersGroup(ctx, admin);

                const bob = await makeIdentity();
                await registerIdentity(users.group, bob);

                await grantCap(users.group, admin, bob.keyId, 'editor');
                const oldRowId = (await findCapGrants(users.group, bob.keyId, 'editor'))[0];

                await revokeCap(users.group, admin, bob.keyId, 'editor');
                const view = await tableView(users.group, CAPS_TABLE);
                assertFalse(await view.hasRow(oldRowId), 'old rowId should stay dead after revoke');

                await grantCap(users.group, admin, bob.keyId, 'editor');
                const newRowId = (await findCapGrants(users.group, bob.keyId, 'editor'))[0];
                assertTrue(newRowId !== oldRowId, 're-grant should use a fresh rowId');
            }
        },
        {
            name: '[USERS09] bundled revoke is atomic',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const users = await createUsersGroup(ctx, admin);

                const bob = await makeIdentity();
                await registerIdentity(users.group, bob);

                const base = await frontier(users.group);
                const capId1 = deriveRowId('atomic-a', admin.keyId);
                const capId2 = deriveRowId('atomic-b', admin.keyId);
                await users.group.bundle([
                    { table: CAPS_TABLE, op: { action: 'insert', rowId: capId1, uuid: 'atomic-a', values: { label: 'writer', grantee: bob.keyId } } },
                    { table: CAPS_TABLE, op: { action: 'insert', rowId: capId2, uuid: 'atomic-b', values: { label: 'writer', grantee: bob.keyId } } },
                ], admin, base);

                await revokeCap(users.group, admin, bob.keyId, 'writer');

                const merged = await tableView(users.group, CAPS_TABLE);
                assertFalse(await merged.hasRow(capId1), 'bundled revoke should kill first witness');
                assertFalse(await merged.hasRow(capId2), 'bundled revoke should kill second witness');
            }
        },
        {
            name: '[PERM10] Tier 1: an insert gated by cmp/str over the subject row rejects when the predicate fails',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();

                // docs insert allowed only for low-priority rows whose body has
                // the 'ok-' prefix: an `and` of a cmp over a readonly integer
                // column and a str over the body.
                const tables: TableDef[] = [
                    { name: 'docs', columns: {
                        body: { type: 'string' },                  // mutable, not referenced
                        name: { type: 'string', readonly: true },
                        level: { type: 'integer', readonly: true },
                      },
                      restrictions: [{ on: 'insert', rule: { p: 'and', args: [
                          { p: 'cmp', cmp: 'lt', left: { col: 'level' }, right: { lit: 3 } },
                          { p: 'str', str: 'prefix', value: { col: 'name' }, sub: { lit: 'ok-' } },
                      ] } }] },
                ];
                const schemaInit = await RSchemaImpl.create({
                    name: 'perm10:schema',
                    creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
                    tables,
                });
                const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
                const pinned = await (await schema.getScopedDag()).getFrontier();
                const groupInit = await RTableGroupImpl.create({
                    name: 'perm10-group', seed: 'perm10-group', schemaRef: schema.getId(), schemaVersion: pinned,
                });
                const group = (await ctx.createObject(groupInit)) as RTableGroupImpl;

                const docs = await group.getTable('docs');

                // name and level are readonly -> $row reads the insert values.
                // No author needed (the rule names no identity).
                await docs.insert('d-ok', { body: 'hello', name: 'ok-1', level: 2 });
                await expectThrow(() => docs.insert('d-badname', { body: 'hello', name: 'nope', level: 2 }),
                    'an insert failing the str prefix rejects');
                await expectThrow(() => docs.insert('d-highlevel', { body: 'hello', name: 'ok-2', level: 9 }),
                    'an insert failing the cmp on the readonly level rejects');

                const view = await tableView(group, 'docs');
                assertTrue(await view.hasRow(deriveRowId('d-ok')),
                    'an insert satisfying both cmp and str survives');
                assertFalse(await view.hasRow(deriveRowId('d-badname')),
                    'an insert failing the str prefix never appends');
                assertFalse(await view.hasRow(deriveRowId('d-highlevel')),
                    'an insert failing the cmp on the readonly level never appends');
            }
        },
        {
            name: '[PERM11] Tier 2: $row correlation attenuates by resource; a concurrent revoke of the correlated grant voids the use',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();

                // grants: open insert, explicit grantee, resource pub+readonly,
                // concurrentDeletes so a revoke reaches concurrent uses.
                // docs: insert allowed only if the author is grantee on a grant for the
                // SAME resource (correlated via $row.resource).
                const tables: TableDef[] = [
                    ...usersSchemaTables(),
                    { name: 'grants', columns: { resource: { type: 'string', pub: true, readonly: true }, grantee: { type: 'string', pub: true, readonly: true } },
                      concurrentDeletes: true, restrictions: [{ on: 'all', rule: { p: 'true' } }] },
                    { name: 'docs', columns: { body: { type: 'string' }, resource: { type: 'string', readonly: true } },
                      restrictions: [{ on: 'insert', rule: {
                          p: 'exists', table: 'grants', where: { resource: '$row.resource', grantee: '$author' },
                      } }] },
                ];
                const schemaInit = await RSchemaImpl.create({
                    name: 'perm11:schema',
                    creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
                    tables,
                });
                const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
                const pinned = await (await schema.getScopedDag()).getFrontier();
                const groupInit = await RTableGroupImpl.create({
                    name: 'perm11-group', seed: 'perm11-group', schemaRef: schema.getId(), schemaVersion: pinned,
                    idProvider: IDENTITIES_TABLE,
                    initialRows: {
                        [IDENTITIES_TABLE]: [{ action: 'insert', rowId: deriveRowId('admin'), uuid: 'admin', values: { keyId: admin.keyId, publicKey: serializePublicKeyToBase64(admin.publicKey) } }],
                    },
                });
                const group = (await ctx.createObject(groupInit)) as RTableGroupImpl;

                const alice = await makeIdentity();
                await registerIdentity(group, alice);

                // alice holds a grant for resource 'R1'
                const grants = await group.getTable('grants');
                const grantId = deriveRowId('g-R1');
                await grants.insert('g-R1', { resource: 'R1', grantee: alice.keyId });

                const docs = await group.getTable('docs');

                // attenuation: a doc for R1 survives, a doc for R2 (no grant)
                // rejects at validation.
                await docs.insert('d-R1', { body: 'x', resource: 'R1' }, alice);
                await expectThrow(() => docs.insert('d-R2', { body: 'y', resource: 'R2' }, alice),
                    'a doc whose resource has no matching grant rejects (attenuation)');

                const v1 = await tableView(group, 'docs');
                assertTrue(await v1.hasRow(deriveRowId('d-R1', alice.keyId)),
                    'a doc whose resource matches a grant survives ($row correlation)');
                assertFalse(await v1.hasRow(deriveRowId('d-R2', alice.keyId)),
                    'a doc whose resource has no matching grant never appends');

                // at-use: a use of the grant concurrent with a barrier revoke voids
                const base = await frontier(group);
                const dId = deriveRowId('d-concurrent', alice.keyId);
                await docs.insert('d-concurrent', { body: 'z', resource: 'R1' }, alice, base);
                await grants.delete(grantId, undefined, base);   // concurrent barrier revoke

                const merged = await frontier(group);
                assertFalse(await (await tableView(group, 'docs', merged)).hasRow(dId),
                    'a concurrent barrier revoke of the correlated grant voids the use');
            }
        },
    ],
};
