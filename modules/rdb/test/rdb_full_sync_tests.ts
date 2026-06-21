// Real two-replica integration tests for the RDb sync root (Replica + Mesh +
// MemDagBackend). See rdb_full_sync_harness.ts for shared wiring.

import { assertTrue, assertFalse } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";

import { RSchemaImpl, rSchemaFactory } from "../src/rschema/rschema.js";
import { RTableGroupImpl, rTableGroupFactory } from "../src/rtable_group/group.js";
import { RDbImpl, rDbFactory } from "../src/rdb/rdb.js";
import { deriveRowId } from "../src/rtable/hash.js";
import type { TableDef } from "../src/rschema/payload.js";
import {
    registerIdentity, grantCap, USERS_IDENTITIES_PROVIDER,
    usersSchemaTables, IDENTITIES_TABLE, CAPS_TABLE, identityRow, capRow,
} from "../src/users/users.js";

import {
    dummyCtx, hashSuite, wait, waitUntil,
    computePinnedVersion, closureTopicIds,
    createAliceBobPeers, cleanup,
    getRDb, frontier, waitForRowOn, hasRowOn, openTable,
} from "./rdb_full_sync_harness.js";

// --- [RDB-FULL] One-way smoke (refactored to harness) ---

async function testRDbDrivenSync() {
    const creator = await createIdentity(SIGNING_ED25519, hashSuite);

    const schemaInit = await RSchemaImpl.create({
        seed: 'rdb-full-schema',
        creators: [{ keyId: creator.keyId, publicKey: creator.publicKey }],
        tables: [openTable('t', { name: { type: 'string' } })],
    });
    const schemaId = await rSchemaFactory.computeRootObjectId(schemaInit, dummyCtx);
    const pinned = computePinnedVersion(schemaId);

    const groupInit = await RTableGroupImpl.create({
        seed: 'rdb-full-group',
        schemaRef: schemaId,
        schemaVersion: pinned,
    });
    const groupId = await rTableGroupFactory.computeRootObjectId(groupInit, dummyCtx);

    const rdbInit = await RDbImpl.create({ seed: 'rdb-full-db' });
    const rdbId = await rDbFactory.computeRootObjectId(rdbInit, dummyCtx);

    const topics = closureTopicIds(rdbId, [schemaId], [groupId]);
    const { provider, alice, bob } = await createAliceBobPeers('full01', topics);

    const schemaA = (await alice.replica.createObject(schemaInit)) as RSchemaImpl;
    const groupA = (await alice.replica.createObject(groupInit)) as RTableGroupImpl;
    const rdbA = (await alice.replica.createObject(rdbInit)) as RDbImpl;

    assertTrue(schemaA.getId() === schemaId, 'schema id is deterministic');
    assertTrue(groupA.getId() === groupId, 'group id is deterministic');
    assertTrue(rdbA.getId() === rdbId, 'rdb id is deterministic');

    await (await groupA.getTable('t')).insert('row-1', { name: 'alice' });

    await rdbA.addSchema(schemaId);
    await rdbA.addGroup(groupId);
    rdbA.setRuntimeConfig({ fetchTimeoutMs: 8000 });
    await rdbA.startSync();

    const rdbB = (await bob.replica.createObject(rdbInit)) as RDbImpl;
    await rdbB.addSchema(schemaId);
    await rdbB.addGroup(groupId);
    rdbB.setRuntimeConfig({ fetchTimeoutMs: 8000 });

    assertTrue((await bob.replica.getObject(schemaId)) === undefined, 'B has no schema before startSync');
    assertTrue((await bob.replica.getObject(groupId)) === undefined, 'B has no group before startSync');

    await rdbB.startSync();

    assertTrue((await bob.replica.getObject(schemaId)) !== undefined, 'B fetched schema via RDb fan-out');
    assertTrue((await bob.replica.getObject(groupId)) !== undefined, 'B fetched group via RDb fan-out');

    const rowId = deriveRowId('row-1');
    await waitForRowOn(bob.replica, groupId, 't', rowId);

    const finalView = await (await (await bob.replica.getObject(groupId) as RTableGroupImpl).getTable('t')).getView();
    assertTrue(await finalView.hasRow(rowId), 'B converged the row inserted on A');
    const row = await finalView.getRow(rowId);
    assertTrue(row !== undefined && row.values['name'] === 'alice', 'row values converged');

    await cleanup([alice, bob], provider);
}

// --- [RDB-FULL02] Bidirectional row writes ---

async function testBidirectionalWrites() {
    const creator = await createIdentity(SIGNING_ED25519, hashSuite);

    const schemaInit = await RSchemaImpl.create({
        seed: 'rdb-full02-schema',
        creators: [{ keyId: creator.keyId, publicKey: creator.publicKey }],
        tables: [openTable('t', { name: { type: 'string' } })],
    });
    const schemaId = await rSchemaFactory.computeRootObjectId(schemaInit, dummyCtx);
    const pinned = computePinnedVersion(schemaId);

    const groupInit = await RTableGroupImpl.create({
        seed: 'rdb-full02-group',
        schemaRef: schemaId,
        schemaVersion: pinned,
    });
    const groupId = await rTableGroupFactory.computeRootObjectId(groupInit, dummyCtx);

    const rdbInit = await RDbImpl.create({ seed: 'rdb-full02-db' });
    const rdbId = await rDbFactory.computeRootObjectId(rdbInit, dummyCtx);

    const topics = closureTopicIds(rdbId, [schemaId], [groupId]);
    const { provider, alice, bob } = await createAliceBobPeers('full02', topics);

    await alice.replica.createObject(schemaInit);
    const groupA = (await alice.replica.createObject(groupInit)) as RTableGroupImpl;
    const rdbA = (await alice.replica.createObject(rdbInit)) as RDbImpl;

    await rdbA.addSchema(schemaId);
    await rdbA.addGroup(groupId);
    rdbA.setRuntimeConfig({ fetchTimeoutMs: 8000 });

    const rdbB = (await bob.replica.createObject(rdbInit)) as RDbImpl;
    await rdbB.addSchema(schemaId);
    await rdbB.addGroup(groupId);
    rdbB.setRuntimeConfig({ fetchTimeoutMs: 8000 });

    await rdbA.startSync();
    await rdbB.startSync();
    await wait(300);

    assertTrue((await bob.replica.getObject(schemaId)) !== undefined, 'bob fetched schema via RDb fan-out');

    const rowAliceId = deriveRowId('row-alice');
    const rowBobId = deriveRowId('row-bob');

    await (await groupA.getTable('t')).insert('row-alice', { name: 'from-alice' });
    await waitForRowOn(bob.replica, groupId, 't', rowAliceId);

    const groupB = (await bob.replica.getObject(groupId)) as RTableGroupImpl;
    await (await groupB.getTable('t')).insert('row-bob', { name: 'from-bob' });
    await waitForRowOn(alice.replica, groupId, 't', rowBobId);

    const aliceView = await (await groupA.getTable('t')).getView();
    const bobView = await (await groupB.getTable('t')).getView();

    assertTrue(await aliceView.hasRow(rowAliceId) && await aliceView.hasRow(rowBobId), 'alice sees both rows');
    assertTrue(await bobView.hasRow(rowAliceId) && await bobView.hasRow(rowBobId), 'bob sees both rows');

    const aliceBobRow = await aliceView.getRow(rowBobId);
    const bobAliceRow = await bobView.getRow(rowAliceId);
    assertTrue(aliceBobRow?.values['name'] === 'from-bob', 'alice row values from bob');
    assertTrue(bobAliceRow?.values['name'] === 'from-alice', 'bob row values from alice');

    await cleanup([alice, bob], provider);
}

// --- [RDB-FULL03] Dynamic membership via RDb DAG sync (v1 stop/start) ---

async function testDynamicMembership() {
    const creator = await createIdentity(SIGNING_ED25519, hashSuite);

    const schema1Init = await RSchemaImpl.create({
        seed: 'rdb-full03-schema1',
        creators: [{ keyId: creator.keyId, publicKey: creator.publicKey }],
        tables: [openTable('t', { name: { type: 'string' } })],
    });
    const schema1Id = await rSchemaFactory.computeRootObjectId(schema1Init, dummyCtx);
    const pinned1 = computePinnedVersion(schema1Id);

    const group1Init = await RTableGroupImpl.create({
        seed: 'rdb-full03-group1',
        schemaRef: schema1Id,
        schemaVersion: pinned1,
    });
    const group1Id = await rTableGroupFactory.computeRootObjectId(group1Init, dummyCtx);

    const schema2Init = await RSchemaImpl.create({
        seed: 'rdb-full03-schema2',
        creators: [{ keyId: creator.keyId, publicKey: creator.publicKey }],
        tables: [openTable('t', { name: { type: 'string' } })],
    });
    const schema2Id = await rSchemaFactory.computeRootObjectId(schema2Init, dummyCtx);
    const pinned2 = computePinnedVersion(schema2Id);

    const group2Init = await RTableGroupImpl.create({
        seed: 'rdb-full03-group2',
        schemaRef: schema2Id,
        schemaVersion: pinned2,
    });
    const group2Id = await rTableGroupFactory.computeRootObjectId(group2Init, dummyCtx);

    const rdbInit = await RDbImpl.create({ seed: 'rdb-full03-db' });
    const rdbId = await rDbFactory.computeRootObjectId(rdbInit, dummyCtx);

    const topics = closureTopicIds(rdbId, [schema1Id, schema2Id], [group1Id, group2Id]);
    const { provider, alice, bob } = await createAliceBobPeers('full03', topics);

    await alice.replica.createObject(schema1Init);
    const group1A = (await alice.replica.createObject(group1Init)) as RTableGroupImpl;
    const rdbA = (await alice.replica.createObject(rdbInit)) as RDbImpl;
    const rdbB = (await bob.replica.createObject(rdbInit)) as RDbImpl;

    await rdbA.addSchema(schema1Id);
    await rdbA.addGroup(group1Id);
    rdbA.setRuntimeConfig({ fetchTimeoutMs: 15000 });
    rdbB.setRuntimeConfig({ fetchTimeoutMs: 15000 });

    await rdbA.startSync();
    await rdbB.startSync();
    await wait(300);

    await waitUntil(async () => {
        const groups = await getRDb(bob.replica, rdbId).then(r => r.getMemberGroups());
        return groups.includes(group1Id);
    }, 20, 10000);

    const row1Id = deriveRowId('g1-row');
    await (await group1A.getTable('t')).insert('g1-row', { name: 'group1' });

    const bobHasGroup1 = (await bob.replica.getObject(group1Id)) !== undefined;
    const bobSeesRow1 = bobHasGroup1 && await hasRowOn(bob.replica, group1Id, 't', row1Id);
    assertFalse(bobSeesRow1, 'v1: bob cannot see group1 row before fan-out refresh');

    await rdbB.stopSync();
    await rdbB.startSync();
    await waitForRowOn(bob.replica, group1Id, 't', row1Id);

    await alice.replica.createObject(schema2Init);
    const group2A = (await alice.replica.createObject(group2Init)) as RTableGroupImpl;
    await rdbA.addSchema(schema2Id);
    await rdbA.addGroup(group2Id);

    await waitUntil(async () => {
        const groups = await getRDb(bob.replica, rdbId).then(r => r.getMemberGroups());
        return groups.includes(group2Id);
    });

    // v1: both peers refresh fan-out so new member DAGs are served and fetched
    await rdbA.stopSync();
    await rdbA.startSync();
    await wait(300);

    const row2Id = deriveRowId('g2-row');
    await (await group2A.getTable('t')).insert('g2-row', { name: 'group2' });

    await rdbB.stopSync();
    await rdbB.startSync();
    await waitForRowOn(bob.replica, group2Id, 't', row2Id);

    await cleanup([alice, bob], provider);
}

// --- [RDB-FULL04] Cross-group FK + observe convergence ---

async function testCrossGroupFkObserve() {
    const creator = await createIdentity(SIGNING_ED25519, hashSuite);

    const schemaBInit = await RSchemaImpl.create({
        seed: 'rdb-full04-schema-b',
        creators: [{ keyId: creator.keyId, publicKey: creator.publicKey }],
        tables: [openTable('identities', { name: { type: 'string' } })],
    });
    const schemaBId = await rSchemaFactory.computeRootObjectId(schemaBInit, dummyCtx);
    const pinnedB = computePinnedVersion(schemaBId);

    const groupBInit = await RTableGroupImpl.create({
        seed: 'rdb-full04-group-b',
        schemaRef: schemaBId,
        schemaVersion: pinnedB,
    });
    const groupBId = await rTableGroupFactory.computeRootObjectId(groupBInit, dummyCtx);

    const schemaAInit = await RSchemaImpl.create({
        seed: 'rdb-full04-schema-a',
        creators: [{ keyId: creator.keyId, publicKey: creator.publicKey }],
        tables: [openTable('orders', { customer: { type: 'string' } }, {
            fks: { customer: 'users.identities' },
        })],
    });
    const schemaAId = await rSchemaFactory.computeRootObjectId(schemaAInit, dummyCtx);
    const pinnedA = computePinnedVersion(schemaAId);

    const groupAInit = await RTableGroupImpl.create({
        seed: 'rdb-full04-group-a',
        schemaRef: schemaAId,
        schemaVersion: pinnedA,
        bindings: { users: groupBId },
    });
    const groupAId = await rTableGroupFactory.computeRootObjectId(groupAInit, dummyCtx);

    const rdbInit = await RDbImpl.create({ seed: 'rdb-full04-db' });
    const rdbId = await rDbFactory.computeRootObjectId(rdbInit, dummyCtx);

    const topics = closureTopicIds(rdbId, [schemaAId, schemaBId], [groupAId, groupBId]);
    const { provider, alice, bob } = await createAliceBobPeers('full04', topics);

    await alice.replica.createObject(schemaBInit);
    const groupBAlice = (await alice.replica.createObject(groupBInit)) as RTableGroupImpl;
    await alice.replica.createObject(schemaAInit);
    const groupAAlice = (await alice.replica.createObject(groupAInit)) as RTableGroupImpl;
    const rdbA = (await alice.replica.createObject(rdbInit)) as RDbImpl;

    await rdbA.addSchema(schemaAId);
    await rdbA.addSchema(schemaBId);
    await rdbA.addGroup(groupBId);
    await rdbA.addGroup(groupAId);
    rdbA.setRuntimeConfig({ fetchTimeoutMs: 8000 });
    await rdbA.startSync();

    const rdbB = (await bob.replica.createObject(rdbInit)) as RDbImpl;
    await rdbB.addSchema(schemaAId);
    await rdbB.addSchema(schemaBId);
    await rdbB.addGroup(groupBId);
    await rdbB.addGroup(groupAId);
    rdbB.setRuntimeConfig({ fetchTimeoutMs: 8000 });
    await rdbB.startSync();
    await wait(300);

    const uId = deriveRowId('u-1');
    await (await groupBAlice.getTable('identities')).insert('u-1', { name: 'ada' });
    const bFrontier = await frontier(groupBAlice);

    await waitForRowOn(bob.replica, groupBId, 'identities', uId);

    await groupAAlice.observe('users', bFrontier);

    const orderId = deriveRowId('o-1');
    await (await groupAAlice.getTable('orders')).insert('o-1', { customer: uId });

    await waitUntil(async () => {
        const groupAOnBob = await bob.replica.getObject(groupAId) as RTableGroupImpl;
        const foreignView = await groupAOnBob.resolveForeignTableView(
            'users', 'identities', await frontier(groupAOnBob), await frontier(groupAOnBob),
        );
        return foreignView !== undefined && await foreignView.hasRow(uId);
    });

    await waitForRowOn(bob.replica, groupAId, 'orders', orderId);

    await cleanup([alice, bob], provider);
}

// --- [RDB-FULL05] Signed ops + Users caps under RDb orchestration ---

async function testSignedOpsMeshSync() {
    const admin = await createIdentity(SIGNING_ED25519, hashSuite);
    const bobSigning = await createIdentity(SIGNING_ED25519, hashSuite);

    const usersSchemaInit = await RSchemaImpl.create({
        seed: 'rdb-full05-users-schema',
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        tables: usersSchemaTables(),
    });
    const usersSchemaId = await rSchemaFactory.computeRootObjectId(usersSchemaInit, dummyCtx);
    const usersPinned = computePinnedVersion(usersSchemaId);

    const usersGroupInit = await RTableGroupImpl.create({
        seed: 'rdb-full05-users-group',
        schemaRef: usersSchemaId,
        schemaVersion: usersPinned,
        idProvider: IDENTITIES_TABLE,
        initialRows: {
            [IDENTITIES_TABLE]: [identityRow('admin', admin)],
            [CAPS_TABLE]: [capRow('root-cap', admin.keyId, 'manager')],
        },
    });
    const usersGroupId = await rTableGroupFactory.computeRootObjectId(usersGroupInit, dummyCtx);

    const appDocsTable: TableDef = {
        name: 'docs',
        columns: { body: { type: 'string' } },
        restrictions: [{
            on: 'insert',
            rule: { p: 'exists', table: 'users.caps', where: { label: 'editor', grantee: '$author' } },
        }],
    };

    const appSchemaInit = await RSchemaImpl.create({
        seed: 'rdb-full05-app-schema',
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        tables: [appDocsTable],
    });
    const appSchemaId = await rSchemaFactory.computeRootObjectId(appSchemaInit, dummyCtx);
    const appPinned = computePinnedVersion(appSchemaId);

    const appGroupInit = await RTableGroupImpl.create({
        seed: 'rdb-full05-app-group',
        schemaRef: appSchemaId,
        schemaVersion: appPinned,
        bindings: { users: usersGroupId },
        idProvider: USERS_IDENTITIES_PROVIDER,
    });
    const appGroupId = await rTableGroupFactory.computeRootObjectId(appGroupInit, dummyCtx);

    const rdbInit = await RDbImpl.create({ seed: 'rdb-full05-db' });
    const rdbId = await rDbFactory.computeRootObjectId(rdbInit, dummyCtx);

    const topics = closureTopicIds(rdbId, [usersSchemaId, appSchemaId], [usersGroupId, appGroupId]);
    const { provider, alice, bob } = await createAliceBobPeers('full05', topics);

    await alice.replica.createObject(usersSchemaInit);
    const usersGroupAlice = (await alice.replica.createObject(usersGroupInit)) as RTableGroupImpl;
    await alice.replica.createObject(appSchemaInit);
    const appGroupAlice = (await alice.replica.createObject(appGroupInit)) as RTableGroupImpl;
    const rdbA = (await alice.replica.createObject(rdbInit)) as RDbImpl;

    await registerIdentity(usersGroupAlice, bobSigning);
    await grantCap(usersGroupAlice, admin, bobSigning.keyId, 'editor');
    const usersFrontier = await frontier(usersGroupAlice);
    await appGroupAlice.observe('users', usersFrontier);

    await rdbA.addSchema(usersSchemaId);
    await rdbA.addSchema(appSchemaId);
    await rdbA.addGroup(usersGroupId);
    await rdbA.addGroup(appGroupId);
    rdbA.setRuntimeConfig({ fetchTimeoutMs: 8000 });
    await rdbA.startSync();

    const rdbB = (await bob.replica.createObject(rdbInit)) as RDbImpl;
    rdbB.setRuntimeConfig({ fetchTimeoutMs: 8000 });
    await rdbB.startSync();

    await waitUntil(async () => {
        const groups = await getRDb(bob.replica, rdbId).then(r => r.getMemberGroups());
        return groups.includes(usersGroupId) && groups.includes(appGroupId);
    });

    await rdbB.stopSync();
    await rdbB.startSync();
    await wait(300);

    await waitUntil(async () => {
        const usersOnBob = await bob.replica.getObject(usersGroupId) as RTableGroupImpl;
        const capsView = await (await usersOnBob.getView()).getTableView(CAPS_TABLE);
        return (await capsView.findRowIds({ label: 'editor', grantee: bobSigning.keyId })).length > 0;
    });

    await waitUntil(async () => {
        const appOnBob = await bob.replica.getObject(appGroupId) as RTableGroupImpl;
        const observed = await (await appOnBob.getView()).resolveRefVersion(usersGroupId);
        return observed.size > 1 || !observed.has(usersGroupId);
    });

    const appOnBob = await bob.replica.getObject(appGroupId) as RTableGroupImpl;
    const docsBob = await appOnBob.getTable('docs');
    await docsBob.insert('doc-1', { body: 'bob wrote this' }, bobSigning);

    const docRowId = deriveRowId('doc-1', bobSigning.keyId);
    await waitForRowOn(alice.replica, appGroupId, 'docs', docRowId);

    const wrongSigner = await createIdentity(SIGNING_ED25519, hashSuite);
    let badSigThrew = false;
    try {
        await docsBob.insert('doc-bad', { body: 'bad sig' }, wrongSigner);
    } catch {
        badSigThrew = true;
    }
    assertTrue(badSigThrew, 'tampered signature should fail locally before sync');

    await cleanup([alice, bob], provider);
}

// --- [RDB-FULL06] Multi-DAG deployment closure ---

async function testMultiDagDeployment() {
    const admin = await createIdentity(SIGNING_ED25519, hashSuite);

    const usersSchemaInit = await RSchemaImpl.create({
        seed: 'rdb-full06-users-schema',
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        tables: usersSchemaTables(),
    });
    const usersSchemaId = await rSchemaFactory.computeRootObjectId(usersSchemaInit, dummyCtx);
    const usersPinned = computePinnedVersion(usersSchemaId);

    const usersGroupInit = await RTableGroupImpl.create({
        seed: 'rdb-full06-users-group',
        schemaRef: usersSchemaId,
        schemaVersion: usersPinned,
        idProvider: IDENTITIES_TABLE,
        initialRows: {
            [IDENTITIES_TABLE]: [identityRow('admin', admin)],
            [CAPS_TABLE]: [capRow('root-cap', admin.keyId, 'manager')],
        },
    });
    const usersGroupId = await rTableGroupFactory.computeRootObjectId(usersGroupInit, dummyCtx);

    const shopSchemaInit = await RSchemaImpl.create({
        seed: 'rdb-full06-shop-schema',
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        tables: [openTable('orders', { item: { type: 'string' } })],
    });
    const shopSchemaId = await rSchemaFactory.computeRootObjectId(shopSchemaInit, dummyCtx);
    const shopPinned = computePinnedVersion(shopSchemaId);

    const shopGroupInit = await RTableGroupImpl.create({
        seed: 'rdb-full06-shop-group',
        schemaRef: shopSchemaId,
        schemaVersion: shopPinned,
    });
    const shopGroupId = await rTableGroupFactory.computeRootObjectId(shopGroupInit, dummyCtx);

    const invSchemaInit = await RSchemaImpl.create({
        seed: 'rdb-full06-inv-schema',
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        tables: [openTable('items', { sku: { type: 'string' } })],
    });
    const invSchemaId = await rSchemaFactory.computeRootObjectId(invSchemaInit, dummyCtx);
    const invPinned = computePinnedVersion(invSchemaId);

    const invGroupInit = await RTableGroupImpl.create({
        seed: 'rdb-full06-inv-group',
        schemaRef: invSchemaId,
        schemaVersion: invPinned,
        bindings: { users: usersGroupId },
        idProvider: USERS_IDENTITIES_PROVIDER,
    });
    const invGroupId = await rTableGroupFactory.computeRootObjectId(invGroupInit, dummyCtx);

    const rdbInit = await RDbImpl.create({ seed: 'rdb-full06-db' });
    const rdbId = await rDbFactory.computeRootObjectId(rdbInit, dummyCtx);

    const topics = closureTopicIds(
        rdbId,
        [usersSchemaId, shopSchemaId, invSchemaId],
        [usersGroupId, shopGroupId, invGroupId],
    );
    const { provider, alice, bob } = await createAliceBobPeers('full06', topics);

    await alice.replica.createObject(usersSchemaInit);
    await alice.replica.createObject(usersGroupInit);
    await alice.replica.createObject(shopSchemaInit);
    const shopGroupAlice = (await alice.replica.createObject(shopGroupInit)) as RTableGroupImpl;
    await alice.replica.createObject(invSchemaInit);
    const invGroupAlice = (await alice.replica.createObject(invGroupInit)) as RTableGroupImpl;
    const rdbA = (await alice.replica.createObject(rdbInit)) as RDbImpl;

    await rdbA.addSchema(usersSchemaId);
    await rdbA.addSchema(shopSchemaId);
    await rdbA.addSchema(invSchemaId);
    await rdbA.addGroup(usersGroupId);
    await rdbA.addGroup(shopGroupId);
    await rdbA.addGroup(invGroupId);

    const shopRowId = deriveRowId('order-1');
    const invRowId = deriveRowId('item-1');
    await (await shopGroupAlice.getTable('orders')).insert('order-1', { item: 'widget' });
    await (await invGroupAlice.getTable('items')).insert('item-1', { sku: 'SKU-42' });

    rdbA.setRuntimeConfig({ fetchTimeoutMs: 8000 });
    await rdbA.startSync();

    const rdbB = (await bob.replica.createObject(rdbInit)) as RDbImpl;
    rdbB.setRuntimeConfig({ fetchTimeoutMs: 8000 });
    await rdbB.startSync();

    await waitUntil(async () => {
        const groups = await getRDb(bob.replica, rdbId).then(r => r.getMemberGroups());
        return groups.includes(usersGroupId) && groups.includes(shopGroupId) && groups.includes(invGroupId);
    });

    await rdbB.stopSync();
    await rdbB.startSync();
    await wait(300);

    for (const id of [usersSchemaId, usersGroupId, shopSchemaId, shopGroupId, invSchemaId, invGroupId]) {
        assertTrue((await bob.replica.getObject(id)) !== undefined, `bob fetched member ${id}`);
    }

    await waitForRowOn(bob.replica, shopGroupId, 'orders', shopRowId);
    await waitForRowOn(bob.replica, invGroupId, 'items', invRowId);

    await cleanup([alice, bob], provider);
}

export const rdbFullSyncTests = {
    title: '[RDB-FULL] RDb-driven cross-replica sync',
    tests: [
        { name: '[RDB-FULL] RDb.startSync fetches members and converges a row', invoke: testRDbDrivenSync },
        { name: '[RDB-FULL02] bidirectional row inserts converge on both replicas', invoke: testBidirectionalWrites },
        { name: '[RDB-FULL03] dynamic membership syncs on RDb DAG; v1 needs stop/start fan-out', invoke: testDynamicMembership },
        { name: '[RDB-FULL04] cross-group FK + observe convergence across replicas', invoke: testCrossGroupFkObserve },
        { name: '[RDB-FULL05] Users caps + signed cross-peer insert under RDb orchestration', invoke: testSignedOpsMeshSync },
        { name: '[RDB-FULL06] multi-DAG deployment closure fetch + row convergence', invoke: testMultiDagDeployment },
    ],
};
