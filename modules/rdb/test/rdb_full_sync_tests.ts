// Real two-replica integration tests for the RDb sync root (Replica + Mesh +
// MemDagBackend). See rdb_full_sync_harness.ts for shared wiring and
// catalog_fixture.ts for the catalog-based database builders.

import { assertTrue, assertEquals, assertFalse } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createIdentity, SIGNING_ED25519, HASH_SHA256 } from "@hyper-hyper-space/hhs3_crypto";
import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { dag, position } from "@hyper-hyper-space/hhs3_dag";
import type { IssueReport } from "@hyper-hyper-space/hhs3_util";
import type { TopicId } from "@hyper-hyper-space/hhs3_mesh";
import type { RObject, RObjectFactory, RContext, Payload, Version } from "@hyper-hyper-space/hhs3_mvt";
import { validationOk, validationFailure, RootScopedDag, ScopedDagSubscription, serializePublicKeyToBase64, version } from "@hyper-hyper-space/hhs3_mvt";
import { Replica, MemDagBackend } from "@hyper-hyper-space/hhs3_replica";

import { RSchemaImpl } from "../src/rschema/rschema.js";
import type { TableDef, MigrationRule } from "../src/rschema/payload.js";
import type { RTableGroupImpl } from "../src/rtable_group/group.js";
import type { CatalogGroupDef } from "../src/rcatalog/payload.js";
import { RDbImpl } from "../src/rdb/rdb.js";
import type { ParamValue } from "../src/rdb/payload.js";
import { deployCatalogRelease } from "../src/rdb/catalog_update.js";
import { catalogStatus } from "../src/rdb/conformance.js";
import { deriveRowId } from "../src/rtable/hash.js";
import {
    registerIdentity, grantCap, USERS_IDENTITIES_PROVIDER,
    usersSchemaTables, IDENTITIES_TABLE, CAPS_TABLE,
} from "../src/users/users.js";

import {
    crypto, hashSuite, wait, waitUntil,
    createAliceBobPeers, cleanup, registerRdbTypes,
    frontier, waitForRowOn,
} from "./rdb_full_sync_harness.js";
import {
    openTable, buildCatalog, buildDatabase, materializeCatalog, materializeDatabase, databaseTopics,
    offlineGroupId, rootIdOf, FixtureSpec,
} from "./catalog_fixture.js";

async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

function identityParam(identity: OwnIdentity): ParamValue {
    return { identity: { keyId: identity.keyId, publicKey: serializePublicKeyToBase64(identity.publicKey) } };
}

function oneGroup(dev: OwnIdentity, name: string): FixtureSpec {
    return { dev, name, groups: [{ name: 'main', tables: [openTable('t', { name: { type: 'string' } })] }] };
}

type Setup = Awaited<ReturnType<typeof setupDatabase>>;

// Builds the catalog and database offline, wires Alice and Bob on its topics,
// and materializes everything on Alice.
async function setupDatabase(testId: string, spec: FixtureSpec, db: {
    seed: string;
    creators?: OwnIdentity[];
    params?: { [name: string]: ParamValue };
    author?: OwnIdentity;
}, opts?: {
    extraTopics?: B64Hash[];
    bobTopicsOnlyRdb?: boolean;
    beforeAlice?: (alice: Replica) => Promise<void>;
}) {
    const built = await buildCatalog(spec);
    const builtDb = await buildDatabase(built, db);
    const topics = databaseTopics(built, builtDb, opts?.extraTopics ?? []) as TopicId[];
    const peers = await createAliceBobPeers(testId, topics, opts?.bobTopicsOnlyRdb === true
        ? { aliceTopics: topics, bobTopics: [builtDb.rdbId as TopicId], bobPoolReuse: true }
        : undefined);
    if (opts?.beforeAlice !== undefined) await opts.beforeAlice(peers.alice.replica);
    const aliceCatalog = await materializeCatalog(peers.alice.replica, built);
    const aliceDb = await materializeDatabase(peers.alice.replica, builtDb, db.author);
    aliceDb.rdb.setRuntimeConfig({ fetchTimeoutMs: 8000 });
    return { built, builtDb, ...peers, aliceCatalog, aliceDb };
}

async function joinBob(s: Setup, config?: { adoptionRange?: string; report?: (r: IssueReport) => void }): Promise<RDbImpl> {
    const rdbB = (await s.bob.replica.createObject(s.builtDb.rdbPayload)) as RDbImpl;
    rdbB.setRuntimeConfig({ fetchTimeoutMs: 8000, ...config });
    await rdbB.startSync();
    return rdbB;
}

function groupOf(s: Setup, name: string): RTableGroupImpl {
    return s.aliceDb.groups.get(name)!;
}

async function present(replica: Replica, id: B64Hash): Promise<boolean> {
    return (await replica.getObject(id)) !== undefined;
}

async function schemaVersionOn(replica: Replica, groupId: B64Hash): Promise<string> {
    const group = (await replica.getObject(groupId)) as RTableGroupImpl;
    const schemaDag = await (await group.getSchemaObject()).getScopedDag();
    return [...await schemaDag.findMinimalCover(await group.resolveSchemaVersion(await frontier(group)))].sort().join(',');
}

// A group added by a later release, with a schema of its own.
async function laterGroup(dev: OwnIdentity, catalogName: string, name: string) {
    const schemaPayload = await RSchemaImpl.create({
        name: `${catalogName}:${name}`,
        creators: [{ keyId: dev.keyId, publicKey: dev.publicKey }],
        tables: [openTable('t', { name: { type: 'string' } })],
    });
    const schemaId = rootIdOf(schemaPayload);
    const def: CatalogGroupDef = { name, seedSource: 'rdb', schemaRef: schemaId, schemaVersion: json.toSet([schemaId]) };
    return { schemaPayload, schemaId, def };
}

// --- [RDB-FULL] One-way smoke ---

async function testRDbDrivenSync() {
    const dev = await makeIdentity();
    const s = await setupDatabase('full01', oneGroup(dev, 'full01'), { seed: 'full01-db' });
    const groupId = s.builtDb.groupIds.get('main')!;

    await (await groupOf(s, 'main').getTable('t')).insert('row-1', { name: 'alice' });
    await s.aliceDb.rdb.startSync();

    assertFalse(await present(s.bob.replica, s.built.catalogId), 'B has no catalog before startSync');
    assertFalse(await present(s.bob.replica, groupId), 'B has no group before startSync');

    await joinBob(s);

    await waitUntil(() => present(s.bob.replica, s.built.catalogId));
    await waitUntil(() => present(s.bob.replica, groupId));

    const rowId = deriveRowId('row-1');
    await waitForRowOn(s.bob.replica, groupId, 't', rowId);
    const row = await (await (await (await s.bob.replica.getObject(groupId) as RTableGroupImpl).getTable('t')).getView()).getRow(rowId);
    assertTrue(row !== undefined && row.values['name'] === 'alice', 'row values converged');

    await cleanup([s.alice, s.bob], s.provider);
}

// --- [RDB-FULL02] Bidirectional row writes ---

async function testBidirectionalWrites() {
    const dev = await makeIdentity();
    const s = await setupDatabase('full02', oneGroup(dev, 'full02'), { seed: 'full02-db' });
    const groupId = s.builtDb.groupIds.get('main')!;

    await s.aliceDb.rdb.startSync();
    await joinBob(s);
    await waitUntil(() => present(s.bob.replica, groupId));

    const rowAliceId = deriveRowId('row-alice');
    const rowBobId = deriveRowId('row-bob');

    await (await groupOf(s, 'main').getTable('t')).insert('row-alice', { name: 'from-alice' });
    await waitForRowOn(s.bob.replica, groupId, 't', rowAliceId);

    const groupB = (await s.bob.replica.getObject(groupId)) as RTableGroupImpl;
    await (await groupB.getTable('t')).insert('row-bob', { name: 'from-bob' });
    await waitForRowOn(s.alice.replica, groupId, 't', rowBobId);

    const aliceView = await (await groupOf(s, 'main').getTable('t')).getView();
    const bobView = await (await groupB.getTable('t')).getView();
    assertTrue(await aliceView.hasRow(rowAliceId) && await aliceView.hasRow(rowBobId), 'alice sees both rows');
    assertTrue(await bobView.hasRow(rowAliceId) && await bobView.hasRow(rowBobId), 'bob sees both rows');

    await cleanup([s.alice, s.bob], s.provider);
}

// --- [RDB-FULL03] A release adding a group with a declared schema fans out without stop/start ---

async function testDynamicMembership() {
    const dev = await makeIdentity();
    const built = await buildCatalog(oneGroup(dev, 'full03'));
    const builtDb = await buildDatabase(built, { seed: 'full03-db' });
    const second = await laterGroup(dev, 'full03', 'second');
    const secondId = offlineGroupId(builtDb, second.def);

    const s = await setupDatabase('full03', oneGroup(dev, 'full03'), { seed: 'full03-db' },
        { extraTopics: [second.schemaId, secondId] });
    const firstId = s.builtDb.groupIds.get('main')!;

    await s.aliceDb.rdb.startSync();
    const rdbB = await joinBob(s);

    const row1Id = deriveRowId('g1-row');
    await (await groupOf(s, 'main').getTable('t')).insert('g1-row', { name: 'group1' });
    await waitForRowOn(s.bob.replica, firstId, 't', row1Id);

    await s.alice.replica.createObject(second.schemaPayload);
    const { release } = await s.aliceCatalog.catalog.publishRelease({ version: '1.1.0', add: [second.def] }, dev);
    await deployCatalogRelease(s.aliceDb.rdb, { release });
    const secondA = (await s.alice.replica.getObject(secondId)) as RTableGroupImpl;
    assertTrue(secondA !== undefined, 'the planner created the new group on Alice');

    const row2Id = deriveRowId('g2-row');
    await (await secondA.getTable('t')).insert('g2-row', { name: 'group2' });
    await waitForRowOn(s.bob.replica, secondId, 't', row2Id);
    assertTrue((await rdbB.getMemberGroups()).includes(secondId), 'bob computes the new member');

    await cleanup([s.alice, s.bob], s.provider);
}

// --- [RDB-FULL04] Cross-group FK + observe convergence ---

async function testCrossGroupFkObserve() {
    const dev = await makeIdentity();
    const spec: FixtureSpec = {
        dev, name: 'full04', groups: [
            { name: 'users', tables: [openTable('identities', { name: { type: 'string' } })] },
            {
                name: 'app',
                tables: [openTable('orders', { customer: { type: 'string' } }, { fks: { customer: 'users.identities' } })],
                bindings: { users: 'users' },
            },
        ],
    };
    const s = await setupDatabase('full04', spec, { seed: 'full04-db' });
    const usersId = s.builtDb.groupIds.get('users')!;
    const appId = s.builtDb.groupIds.get('app')!;

    await s.aliceDb.rdb.startSync();
    await joinBob(s);
    await waitUntil(() => present(s.bob.replica, appId));

    const uId = deriveRowId('u-1');
    const usersAlice = groupOf(s, 'users');
    await (await usersAlice.getTable('identities')).insert('u-1', { name: 'ada' });
    const bFrontier = await frontier(usersAlice);
    await waitForRowOn(s.bob.replica, usersId, 'identities', uId);

    const appAlice = groupOf(s, 'app');
    await appAlice.observe('users', bFrontier);
    const orderId = deriveRowId('o-1');
    await (await appAlice.getTable('orders')).insert('o-1', { customer: uId });

    await waitUntil(async () => {
        const appOnBob = await s.bob.replica.getObject(appId) as RTableGroupImpl;
        const foreignView = await appOnBob.resolveForeignTableView('users', 'identities', await frontier(appOnBob), await frontier(appOnBob));
        return foreignView !== undefined && await foreignView.hasRow(uId);
    });
    await waitForRowOn(s.bob.replica, appId, 'orders', orderId);

    await cleanup([s.alice, s.bob], s.provider);
}

// --- [RDB-FULL05] Signed ops + Users caps under RDb orchestration ---

async function testSignedOpsMeshSync() {
    const dev = await makeIdentity();
    const admin = await makeIdentity();
    const bobSigning = await makeIdentity();

    const appDocsTable: TableDef = {
        name: 'docs',
        columns: { body: { type: 'string' } },
        restrictions: [{ on: 'insert', rule: { p: 'exists', table: 'users.caps', where: { label: 'editor', grantee: '$author' } } }],
    };
    const spec: FixtureSpec = {
        dev, name: 'full05', params: [{ name: 'admin', type: 'identity' }],
        groups: [
            {
                name: 'users', tables: usersSchemaTables(), idProvider: IDENTITIES_TABLE,
                initialRows: {
                    [IDENTITIES_TABLE]: [{ values: {}, params: { keyId: { param: 'admin' }, publicKey: { param: 'admin', fn: 'publicKey' } } }],
                    [CAPS_TABLE]: [{ values: { label: 'manager' }, params: { grantee: { param: 'admin' } } }],
                },
            },
            { name: 'app', tables: [appDocsTable], bindings: { users: 'users' }, idProvider: USERS_IDENTITIES_PROVIDER },
        ],
    };
    const s = await setupDatabase('full05', spec, { seed: 'full05-db', params: { admin: identityParam(admin) } });
    const usersId = s.builtDb.groupIds.get('users')!;
    const appId = s.builtDb.groupIds.get('app')!;

    const usersAlice = groupOf(s, 'users');
    await registerIdentity(usersAlice, bobSigning);
    await grantCap(usersAlice, admin, bobSigning.keyId, 'editor');
    await groupOf(s, 'app').observe('users', await frontier(usersAlice));

    await s.aliceDb.rdb.startSync();
    const rdbB = await joinBob(s);

    await waitUntil(async () => {
        const groups = await rdbB.getMemberGroups();
        return groups.includes(usersId) && groups.includes(appId);
    });
    await waitUntil(async () => {
        const usersOnBob = await s.bob.replica.getObject(usersId) as RTableGroupImpl | undefined;
        if (usersOnBob === undefined) return false;
        const capsView = await (await usersOnBob.getView()).getTableView(CAPS_TABLE);
        return (await capsView.findRowIds({ label: 'editor', grantee: bobSigning.keyId })).length > 0;
    });
    await waitUntil(async () => {
        const appOnBob = await s.bob.replica.getObject(appId) as RTableGroupImpl | undefined;
        if (appOnBob === undefined) return false;
        const observed = await (await appOnBob.getView()).resolveRefVersion(usersId);
        return observed.size > 1 || !observed.has(usersId);
    });

    const appOnBob = await s.bob.replica.getObject(appId) as RTableGroupImpl;
    const docsBob = await appOnBob.getTable('docs');
    await docsBob.insert('doc-1', { body: 'bob wrote this' }, bobSigning);
    await waitForRowOn(s.alice.replica, appId, 'docs', deriveRowId('doc-1', bobSigning.keyId));

    const wrongSigner = await makeIdentity();
    let badSigThrew = false;
    try { await docsBob.insert('doc-bad', { body: 'bad sig' }, wrongSigner); } catch { badSigThrew = true; }
    assertTrue(badSigThrew, 'an unregistered signer fails locally before sync');

    await cleanup([s.alice, s.bob], s.provider);
}

// --- [RDB-FULL06] Multi-DAG deployment closure ---

async function testMultiDagDeployment() {
    const dev = await makeIdentity();
    const admin = await makeIdentity();
    const spec: FixtureSpec = {
        dev, name: 'full06', params: [{ name: 'admin', type: 'identity' }],
        groups: [
            {
                name: 'users', tables: usersSchemaTables(), idProvider: IDENTITIES_TABLE,
                initialRows: { [IDENTITIES_TABLE]: [{ values: {}, params: { keyId: { param: 'admin' }, publicKey: { param: 'admin', fn: 'publicKey' } } }] },
            },
            { name: 'shop', tables: [openTable('orders', { item: { type: 'string' } })] },
            { name: 'inv', tables: [openTable('items', { sku: { type: 'string' } })], bindings: { users: 'users' }, idProvider: USERS_IDENTITIES_PROVIDER },
        ],
    };
    const s = await setupDatabase('full06', spec, { seed: 'full06-db', params: { admin: identityParam(admin) } });

    await (await groupOf(s, 'shop').getTable('orders')).insert('order-1', { item: 'widget' });
    await (await groupOf(s, 'inv').getTable('items')).insert('item-1', { sku: 'SKU-42' });

    await s.aliceDb.rdb.startSync();
    await joinBob(s);

    const ids = [...s.built.schemaIds.values(), ...s.builtDb.groupIds.values(), s.built.catalogId];
    await waitUntil(async () => {
        for (const id of ids) if (!await present(s.bob.replica, id)) return false;
        return true;
    });

    await waitForRowOn(s.bob.replica, s.builtDb.groupIds.get('shop')!, 'orders', deriveRowId('order-1'));
    await waitForRowOn(s.bob.replica, s.builtDb.groupIds.get('inv')!, 'items', deriveRowId('item-1'));

    await cleanup([s.alice, s.bob], s.provider);
}

// --- [RDB-FULL07] Hash-only RDb join: fetchObject(rdbId), no local create payload ---

async function testHashOnlyRdbJoin() {
    const dev = await makeIdentity();
    const s = await setupDatabase('full07', oneGroup(dev, 'full07'), { seed: 'full07-db' });
    const groupId = s.builtDb.groupIds.get('main')!;

    await (await groupOf(s, 'main').getTable('t')).insert('row-1', { name: 'alice' });
    await s.aliceDb.rdb.startSync();

    assertFalse(await present(s.bob.replica, s.builtDb.rdbId), 'B has no RDb before fetchObject');
    const rdbB = (await s.bob.replica.fetchObject(s.builtDb.rdbId)) as RDbImpl;
    assertEquals(rdbB.getId(), s.builtDb.rdbId, 'fetched RDb has the expected id');
    assertFalse(await present(s.bob.replica, s.built.catalogId), 'B has no catalog after the RDb fetch');
    assertFalse(await present(s.bob.replica, groupId), 'B has no group after the RDb fetch');

    rdbB.setRuntimeConfig({ fetchTimeoutMs: 8000 });
    await rdbB.startSync();

    await waitUntil(() => present(s.bob.replica, groupId));
    await waitForRowOn(s.bob.replica, groupId, 't', deriveRowId('row-1'));

    await cleanup([s.alice, s.bob], s.provider);
}

// --- [RDB-FULL08] fetchObject(schemaId) then fetchObject(groupId), no RDb on Bob ---

async function testFetchSchemaThenGroup() {
    const dev = await makeIdentity();
    const s = await setupDatabase('full08', oneGroup(dev, 'full08'), { seed: 'full08-db' });
    const schemaId = s.built.schemaIds.get('main')!;
    const groupId = s.builtDb.groupIds.get('main')!;
    await s.aliceDb.rdb.startSync();

    assertFalse(await present(s.bob.replica, s.builtDb.rdbId), 'B has no RDb');
    const schemaB = (await s.bob.replica.fetchObject(schemaId)) as RSchemaImpl;
    assertEquals(schemaB.getId(), schemaId, 'fetched schema has the expected id');
    assertFalse(await present(s.bob.replica, groupId), 'group still absent after the schema fetch');

    const groupB = (await s.bob.replica.fetchObject(groupId)) as RTableGroupImpl;
    assertEquals(groupB.getId(), groupId, 'fetched group has the expected id');
    assertEquals(groupB.getSchemaRef(), schemaId, 'fetched group pins the schema');

    await cleanup([s.alice, s.bob], s.provider);
}

// --- [RDB-FULL09] fetchObject(groupId) without schema present is rejected ---

async function testFetchGroupWithoutSchema() {
    const dev = await makeIdentity();
    const s = await setupDatabase('full09', oneGroup(dev, 'full09'), { seed: 'full09-db' });
    const schemaId = s.built.schemaIds.get('main')!;
    const groupId = s.builtDb.groupIds.get('main')!;
    await s.aliceDb.rdb.startSync();

    let threw = false;
    try {
        await s.bob.replica.fetchObject(groupId);
    } catch (e) {
        threw = true;
        const message = (e as Error).message;
        assertTrue(typeof message === 'string' && message.includes(schemaId), `error should mention the missing schema id, got: ${message}`);
    }
    assertTrue(threw, 'fetchObject(groupId) must throw when the schema is not local');
    assertFalse(await present(s.bob.replica, groupId), 'group was not materialized');
    assertFalse(await present(s.bob.replica, schemaId), 'schema was not fetched as a side effect');

    await cleanup([s.alice, s.bob], s.provider);
}

// --- [RDB-FULL10] Hash-only join when Bob's discovery only knows rdbId ---

async function testHashOnlyJoinDiscoveryNotPreseeded() {
    const dev = await makeIdentity();
    const s = await setupDatabase('full10', oneGroup(dev, 'full10'), { seed: 'full10-db' }, { bobTopicsOnlyRdb: true });
    const groupId = s.builtDb.groupIds.get('main')!;

    await (await groupOf(s, 'main').getTable('t')).insert('row-1', { name: 'alice' });
    await s.aliceDb.rdb.startSync();

    const rdbB = (await s.bob.replica.fetchObject(s.builtDb.rdbId)) as RDbImpl;
    assertFalse(await present(s.bob.replica, groupId), 'B has no group after the RDb fetch');
    rdbB.setRuntimeConfig({ fetchTimeoutMs: 8000 });
    await rdbB.startSync();

    await waitUntil(() => present(s.bob.replica, s.built.catalogId));
    await waitUntil(() => present(s.bob.replica, groupId));
    await waitForRowOn(s.bob.replica, groupId, 't', deriveRowId('row-1'));

    await cleanup([s.alice, s.bob], s.provider);
}

// --- [RDB-FULL11] A catalog pinning a POST-genesis schema version materializes ---

async function testPostGenesisSchemaPinMaterializes() {
    const dev = await makeIdentity();
    const schemaPayload = await RSchemaImpl.create({
        name: 'full11:main',
        creators: [{ keyId: dev.keyId, publicKey: dev.publicKey }],
        tables: [openTable('t', { name: { type: 'string' } })],
    });
    const schemaId = rootIdOf(schemaPayload);

    // Ed25519 signing + hashing are deterministic, so a throwaway replica
    // yields the exact version hashes Alice will produce.
    const migration: MigrationRule[] = [{ rule: 'add-column', table: 't', column: 'tag', def: { type: 'string', nullable: true } }];
    const setup = new Replica({ crypto, hashSuite, config: { selfValidate: true } });
    setup.attachBackend('default', new MemDagBackend(hashSuite));
    registerRdbTypes(setup);
    const schemaSetup = (await setup.createObject(schemaPayload)) as RSchemaImpl;
    await schemaSetup.updateSchema(migration, dev, 'v2');
    const pinnedV2 = await (await schemaSetup.getScopedDag()).getFrontier();
    await setup.close();
    assertFalse(pinnedV2.has(schemaId), 'the pin is post-genesis');

    const spec: FixtureSpec = { dev, name: 'full11', groups: [{ name: 'main', tables: [openTable('t', { name: { type: 'string' } })], pin: pinnedV2 }] };
    const s = await setupDatabase('full11', spec, { seed: 'full11-db' }, {
        beforeAlice: async (alice) => {
            const schemaA = (await alice.createObject(schemaPayload)) as RSchemaImpl;
            await schemaA.updateSchema(migration, dev, 'v2');
        },
    });
    const groupId = s.builtDb.groupIds.get('main')!;

    await (await groupOf(s, 'main').getTable('t')).insert('row-1', { name: 'alice' });
    await s.aliceDb.rdb.startSync();

    // The catalog genesis pins schema v2 (a creation dep), and so does the
    // group: both must wait for the schema session to reach v2.
    await joinBob(s);
    await waitUntil(() => present(s.bob.replica, s.built.catalogId));
    await waitUntil(() => present(s.bob.replica, groupId));
    await waitForRowOn(s.bob.replica, groupId, 't', deriveRowId('row-1'));

    await cleanup([s.alice, s.bob], s.provider);
}

// --- [RDB-FULL12] A schema introduced through a declare reaches a joining peer ---

async function testDeclaredSchemaReachesJoiningPeer() {
    const dev = await makeIdentity();
    const built = await buildCatalog(oneGroup(dev, 'full12'));
    const builtDb = await buildDatabase(built, { seed: 'full12-db' });
    const second = await laterGroup(dev, 'full12', 'second');
    const secondId = offlineGroupId(builtDb, second.def);

    const s = await setupDatabase('full12', oneGroup(dev, 'full12'), { seed: 'full12-db' },
        { extraTopics: [second.schemaId, secondId] });

    await s.alice.replica.createObject(second.schemaPayload);
    const published = await s.aliceCatalog.catalog.publishRelease({ version: '1.1.0', add: [second.def] }, dev);
    assertTrue(published.declare !== undefined, 'the release is preceded by a declare');
    await deployCatalogRelease(s.aliceDb.rdb, { release: published.release });
    const secondA = (await s.alice.replica.getObject(secondId)) as RTableGroupImpl;
    await (await secondA.getTable('t')).insert('row-2', { name: 'second' });
    await s.aliceDb.rdb.startSync();

    const rdbB = (await s.bob.replica.fetchObject(s.builtDb.rdbId)) as RDbImpl;
    rdbB.setRuntimeConfig({ fetchTimeoutMs: 8000 });
    await rdbB.startSync();

    await waitUntil(() => present(s.bob.replica, second.schemaId));
    await waitUntil(() => present(s.bob.replica, secondId));
    await waitForRowOn(s.bob.replica, secondId, 't', deriveRowId('row-2'));
    assertEquals((await rdbB.getDeployedReleases()).join(','), published.release, 'Bob resolves the deployed release');

    await cleanup([s.alice, s.bob], s.provider);
}

// --- [RDB-FULL13] An invalid create in the closure is reported once and not retried ---

const POISON_TYPE_ID = 'test/poison-create';

// Alice hosts this type leniently (so it can serve the create genesis); Bob
// rejects it, standing in for a peer that serves a malformed create.
function makePoisonFactory(strict: boolean): RObjectFactory {
    return {
        computeRootObjectId: async (payload: Payload, ctx: RContext) =>
            dag.createEntry(payload, {}, position(), ctx.getCrypto().hash(HASH_SHA256)).hash,
        validateCreationPayload: async () =>
            strict ? validationFailure('poison create is invalid') : validationOk(),
        executeCreationPayload: async (payload: Payload, _ctx: RContext, scopedDag) =>
            scopedDag.append(payload, {}, position()),
        loadObject: async (id: B64Hash, ctx: RContext, opts) =>
            makePoisonObject(id, ctx, opts?.backendLabel ?? 'default'),
    };
}

function makePoisonObject(id: B64Hash, ctx: RContext, backendLabel: string): RObject {
    let scoped: RootScopedDag | undefined;
    let sub: ScopedDagSubscription | undefined;
    const getScoped = async () => {
        if (scoped === undefined) {
            const raw = await ctx.getDag(id, backendLabel);
            if (raw === undefined) throw new Error(`DAG '${id}' not found`);
            scoped = new RootScopedDag(raw);
        }
        return scoped;
    };
    const subscription = () => (sub ??= new ScopedDagSubscription(getScoped));
    return {
        getId: () => id,
        getType: () => POISON_TYPE_ID,
        getBackendLabel: () => backendLabel,
        validatePayload: async () => validationOk(),
        applyPayload: async (payload: Payload, at: Version) => (await getScoped()).append(payload, {}, at),
        getView: async () => { throw new Error('not implemented'); },
        computeDelta: async () => { throw new Error('not implemented'); },
        createDeltaAccumulator: () => { throw new Error('not implemented'); },
        getScopedDag: getScoped,
        getCausalDag: async () => {
            const raw = await ctx.getDag(id, backendLabel);
            if (raw === undefined) throw new Error(`DAG '${id}' not found`);
            return raw;
        },
        extractForeignDeps: () => undefined,
        subscribe: (cb: (version: Version) => void) => subscription().subscribe(cb),
        unsubscribe: (cb: (version: Version) => void) => subscription().unsubscribe(cb),
        destroy: async () => {},
    };
}

async function testInvalidCreateReportedOnceNotRetried() {
    const dev = await makeIdentity();
    const poisonPayload = { action: 'create', type: POISON_TYPE_ID, seed: 'full13-poison' } as unknown as Payload;
    const poisonId = rootIdOf(poisonPayload as object);

    const s = await setupDatabase('full13', oneGroup(dev, 'full13'), { seed: 'full13-db' }, {
        extraTopics: [poisonId],
        beforeAlice: async (alice) => {
            alice.registerType(POISON_TYPE_ID, makePoisonFactory(false));
            await alice.createObject(poisonPayload);
        },
    });
    s.bob.replica.registerType(POISON_TYPE_ID, makePoisonFactory(true));
    const groupId = s.builtDb.groupIds.get('main')!;

    // the catalog names the poison id as a schema to fetch
    await s.aliceCatalog.catalog.declare([poisonId], dev);
    await (await groupOf(s, 'main').getTable('t')).insert('row-1', { name: 'alice' });
    await s.aliceDb.rdb.startSync();

    const reports: IssueReport[] = [];
    await joinBob(s, { report: (r) => reports.push(r) });

    await waitUntil(() => present(s.bob.replica, groupId));
    await waitForRowOn(s.bob.replica, groupId, 't', deriveRowId('row-1'));

    await wait(300);
    assertFalse(await present(s.bob.replica, poisonId), 'the invalid create is not materialized on B');
    const poisonReports = reports.filter((r) => r.opHash === poisonId && r.kind === 'validation-failed');
    assertEquals(poisonReports.length, 1, 'the invalid create is reported exactly once');

    await cleanup([s.alice, s.bob], s.provider);
}

// --- Adoption over sync ---

async function addColumn(schema: RSchemaImpl, dev: OwnIdentity, column: string): Promise<Version> {
    await schema.updateSchema([{ rule: 'add-column', table: 't', column, def: { type: 'string', nullable: true } }], dev);
    return (await schema.getScopedDag()).getFrontier();
}

async function releaseChanging(s: Setup, dev: OwnIdentity, v: string, target: Version): Promise<B64Hash> {
    const schema = s.aliceCatalog.schemas.get('main')!;
    const hash = s.built.hashes.get('main')!;
    return s.aliceCatalog.catalog.release({
        version: v, changes: { [hash]: { schema: schema.getId(), version: json.toSet([...target]) } },
    }, dev);
}

// [RDB-FULL14] a synced deploy waits until the replica adopts its release
async function testSyncedDeployWaitsForAdoption() {
    const dev = await makeIdentity();
    const s = await setupDatabase('full14', oneGroup(dev, 'full14'), { seed: 'full14-db' });
    const groupId = s.builtDb.groupIds.get('main')!;
    const schema = s.aliceCatalog.schemas.get('main')!;
    await s.aliceDb.rdb.startSync();
    const rdbB = await joinBob(s, { adoptionRange: '1.0.0' });
    await waitUntil(() => present(s.bob.replica, groupId));

    const v2 = await addColumn(schema, dev, 'extra');
    const r11 = await releaseChanging(s, dev, '1.1.0', v2);
    await deployCatalogRelease(s.aliceDb.rdb, { release: r11 });
    await (await groupOf(s, 'main').getTable('t')).insert('after-deploy', { name: 'x', extra: 'y' });

    await waitUntil(async () => (await rdbB.getDeployedReleases()).includes(r11));
    await wait(400);
    assertEquals(await schemaVersionOn(s.bob.replica, groupId), schema.getId(), 'the deploy is held: Bob stays at the genesis version');
    const holdStatus = await catalogStatus(rdbB);
    assertEquals(holdStatus.held.map((r) => r.version).join(','), '1.1.0', 'Bob reports 1.1.0 as held');
    assertEquals(holdStatus.members[0].state, 'behind', 'the member is behind');
    const groupB = (await s.bob.replica.getObject(groupId)) as RTableGroupImpl;
    assertFalse(await (await (await groupB.getView()).getTableView('t')).hasRow(deriveRowId('after-deploy')),
        'a row written after the deploy is held with it');

    await rdbB.setAdoptionRange('^1');
    await waitForRowOn(s.bob.replica, groupId, 't', deriveRowId('after-deploy'));
    assertEquals(await schemaVersionOn(s.bob.replica, groupId), [...v2].sort().join(','), 'once adopted, the deploy applies');

    await cleanup([s.alice, s.bob], s.provider);
}

// [RDB-FULL15] a major is held under ^1 and flows after widening to ^2
async function testMajorHeldUntilRangeWidens() {
    const dev = await makeIdentity();
    const s = await setupDatabase('full15', oneGroup(dev, 'full15'), { seed: 'full15-db' });
    const groupId = s.builtDb.groupIds.get('main')!;
    const schema = s.aliceCatalog.schemas.get('main')!;
    await s.aliceDb.rdb.startSync();
    const rdbB = await joinBob(s);
    await waitUntil(() => present(s.bob.replica, groupId));
    assertEquals(await rdbB.getAdoptionRange(), '^1', 'Bob defaults to ^1');

    const v2 = await addColumn(schema, dev, 'a');
    const r11 = await releaseChanging(s, dev, '1.1.0', v2);
    await deployCatalogRelease(s.aliceDb.rdb, { release: r11 });
    await waitUntil(async () => await schemaVersionOn(s.bob.replica, groupId) === [...v2].sort().join(','));

    const v3 = await addColumn(schema, dev, 'b');
    const r20 = await releaseChanging(s, dev, '2.0.0', v3);
    await deployCatalogRelease(s.aliceDb.rdb, { release: r20 });
    await waitUntil(async () => (await rdbB.getDeployedReleases()).includes(r20));
    await wait(400);
    assertEquals(await schemaVersionOn(s.bob.replica, groupId), [...v2].sort().join(','), 'the 2.0.0 deploy is held under ^1');

    await rdbB.setAdoptionRange('^2');
    await waitUntil(async () => await schemaVersionOn(s.bob.replica, groupId) === [...v3].sort().join(','));

    await cleanup([s.alice, s.bob], s.provider);
}

// [RDB-FULL16] an intermediate deploy passes the gate and catalogStatus flags it
async function testIntermediateDeployFlagged() {
    const dev = await makeIdentity();
    const s = await setupDatabase('full16', oneGroup(dev, 'full16'), { seed: 'full16-db' });
    const groupId = s.builtDb.groupIds.get('main')!;
    const schema = s.aliceCatalog.schemas.get('main')!;
    await s.aliceDb.rdb.startSync();
    const rdbB = await joinBob(s);
    await waitUntil(() => present(s.bob.replica, groupId));

    const v2 = await addColumn(schema, dev, 'a');
    const v3 = await addColumn(schema, dev, 'b');
    const r11 = await releaseChanging(s, dev, '1.1.0', v3);
    await s.aliceDb.rdb.updateCatalog(r11);
    await groupOf(s, 'main').deploy(v2);   // a modified client stopping at v2

    await waitUntil(async () => await schemaVersionOn(s.bob.replica, groupId) === [...v2].sort().join(','));
    const status = await catalogStatus(rdbB);
    assertEquals(status.members[0].state, 'intermediate', 'Bob flags the intermediate version');
    assertTrue(version(...status.members[0].adopted!).size > 0, 'Bob adopted the release closure');

    await cleanup([s.alice, s.bob], s.provider);
}

export const rdbFullSyncTests = {
    title: '[RDB-FULL] RDb-driven cross-replica sync',
    tests: [
        { name: '[RDB-FULL] RDb.startSync fetches the catalog, creates members and converges a row', invoke: testRDbDrivenSync },
        { name: '[RDB-FULL02] bidirectional row inserts converge on both replicas', invoke: testBidirectionalWrites },
        { name: '[RDB-FULL03] a release adding a group fans out without stop/start', invoke: testDynamicMembership },
        { name: '[RDB-FULL04] cross-group FK + observe convergence across replicas', invoke: testCrossGroupFkObserve },
        { name: '[RDB-FULL05] Users caps + signed cross-peer insert under RDb orchestration', invoke: testSignedOpsMeshSync },
        { name: '[RDB-FULL06] multi-DAG deployment closure fetch + row convergence', invoke: testMultiDagDeployment },
        { name: '[RDB-FULL07] hash-only fetchObject(rdbId) then fan-out members and row', invoke: testHashOnlyRdbJoin },
        { name: '[RDB-FULL08] fetchObject(schemaId) then fetchObject(groupId) without an RDb', invoke: testFetchSchemaThenGroup },
        { name: '[RDB-FULL09] fetchObject(groupId) without schema present is rejected', invoke: testFetchGroupWithoutSchema },
        { name: '[RDB-FULL10] hash-only join with Bob discovery limited to rdbId', invoke: testHashOnlyJoinDiscoveryNotPreseeded },
        { name: '[RDB-FULL11] a catalog pinning a post-genesis schema version materializes', invoke: testPostGenesisSchemaPinMaterializes },
        { name: '[RDB-FULL12] a schema introduced through a declare reaches a joining peer', invoke: testDeclaredSchemaReachesJoiningPeer },
        { name: '[RDB-FULL13] an invalid create in the closure is reported once and not retried', invoke: testInvalidCreateReportedOnceNotRetried },
        { name: '[RDB-FULL14] a synced deploy waits until it is adopted', invoke: testSyncedDeployWaitsForAdoption },
        { name: '[RDB-FULL15] a major is held under ^1 and flows after widening to ^2', invoke: testMajorHeldUntilRangeWidens },
        { name: '[RDB-FULL16] an intermediate deploy passes and catalogStatus flags it', invoke: testIntermediateDeployFlagged },
    ],
};
