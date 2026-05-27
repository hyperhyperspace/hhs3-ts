import { assertTrue, assertFalse } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import {
    createBasicCrypto, HASH_SHA256, SIGNING_ED25519, KEM_X25519_HKDF,
    sha256, createIdentity,
} from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { NetworkAddress, PeerInfo, TopicId } from "@hyper-hyper-space/hhs3_mesh";
import {
    Mesh, StaticDiscovery, MemTransportProvider, createAuthenticator,
} from "@hyper-hyper-space/hhs3_mesh";
import { Replica, MemDagBackend } from "../src/index.js";
import {
    RSet, rSetFactory,
    RCap, rCapFactory,
    serializePublicKeyToBase64,
} from "@hyper-hyper-space/hhs3_std_types";
import type { RContext } from "@hyper-hyper-space/hhs3_mvt";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);
const dummyCtx = { getCrypto: () => crypto } as RContext;

function wait(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function waitUntil(predicate: () => Promise<boolean>, intervalMs = 20, timeoutMs = 10000): Promise<void> {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
        if (await predicate()) return;
        await wait(intervalMs);
    }
    throw new Error(`waitUntil timed out after ${timeoutMs}ms`);
}

type PeerSetup = {
    replica: Replica;
    mesh: Mesh;
};

function createPeer(
    testId: string,
    peerName: string,
    provider: MemTransportProvider,
    noiseId: OwnIdentity,
    remotePeer: PeerInfo,
    topics: TopicId[],
    config?: { selfValidate?: boolean },
): PeerSetup {
    const addr: NetworkAddress = `mem://${peerName}-${testId}`;

    const mesh = new Mesh({
        transports: [provider],
        discovery: new StaticDiscovery([remotePeer], topics),
        authenticator: createAuthenticator({
            localKey: noiseId,
            signingName: SIGNING_ED25519,
            kemPrefs: [KEM_X25519_HKDF],
        }),
        localKeyId: noiseId.keyId,
        listenAddresses: [addr],
    });

    const replica = new Replica({ crypto, hashSuite, config: { selfValidate: config?.selfValidate ?? true } });
    replica.attachBackend('default', new MemDagBackend(hashSuite));
    replica.attachMesh('default', mesh);
    replica.registerType(RSet.typeId, rSetFactory);
    replica.registerType(RCap.typeId, rCapFactory);

    return { replica, mesh };
}

async function createPermissionedPair(admin: OwnIdentity) {
    const capInit = await RCap.create({
        seed: 'integ-cap',
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        initialCaps: {
            'admin':  { managedBy: ['creator'] },
            'enroll': { managedBy: ['admin'] },
            'write':  { managedBy: ['admin'] },
        },
    });
    const capId = await rCapFactory.computeRootObjectId(capInit.payload, dummyCtx);

    const setInit = await RSet.create({
        seed: 'integ-set',
        initialElements: [],
        hashAlgorithm: 'sha256',
        capabilityRef: capId,
        capRequirements: { add: 'write', delete: 'write' },
    });

    return { capInit, setInit, capId };
}

async function cleanup(peers: PeerSetup[], provider: MemTransportProvider) {
    for (const p of peers) {
        await p.replica.close();
        p.mesh.close();
    }
    provider.close();
}

// ---------- PS01: One-way sync of permissioned RSet + RCap ----------

async function testOneWaySyncPermissioned() {
    const admin = await await createIdentity(SIGNING_ED25519, sha256);
    const { capInit, setInit, capId } = await createPermissionedPair(admin);

    const capTopic = capId as TopicId;
    const setTopic = await rSetFactory.computeRootObjectId(setInit.payload, dummyCtx) as TopicId;
    const topics = [capTopic, setTopic];

    const provider = new MemTransportProvider();
    const aliceNoise = await await createIdentity(SIGNING_ED25519, sha256);
    const bobNoise = await await createIdentity(SIGNING_ED25519, sha256);
    const alicePeer: PeerInfo = { keyId: aliceNoise.keyId, addresses: ['mem://alice-ps01'] };
    const bobPeer: PeerInfo = { keyId: bobNoise.keyId, addresses: ['mem://bob-ps01'] };

    const alice = createPeer('ps01', 'alice', provider, aliceNoise, bobPeer, topics);
    const bob = createPeer('ps01', 'bob', provider, bobNoise, alicePeer, topics);

    const aliceCap = (await alice.replica.createObject(capInit)) as RCap;
    const aliceSet = (await alice.replica.createObject(setInit)) as RSet;
    const bobCap = (await bob.replica.createObject(capInit)) as RCap;
    const bobSet = (await bob.replica.createObject(setInit)) as RSet;

    // Admin (creator) does everything -- no external grants needed
    const capDag = await aliceCap.getScopedDag();
    const capFrontier = await capDag.getFrontier();
    await aliceSet.refAdvance(capFrontier, admin);
    await aliceSet.addSigned('hello', admin);

    await aliceCap.startSync();
    await aliceSet.startSync();
    await bobCap.startSync();
    await bobSet.startSync();

    await waitUntil(async () => {
        const view = await bobSet.getView();
        return view.has('hello');
    });

    const bobView = await bobSet.getView();
    assertTrue(await bobView.has('hello'), 'bob should have hello after sync');

    await cleanup([alice, bob], provider);
}

// ---------- PS02: Cross-peer write by grantee ----------

async function testCrossPeerWrite() {
    const admin = await await createIdentity(SIGNING_ED25519, sha256);
    const bobSigning = await await createIdentity(SIGNING_ED25519, sha256);
    const { capInit, setInit, capId } = await createPermissionedPair(admin);

    const capTopic = capId as TopicId;
    const setTopic = await rSetFactory.computeRootObjectId(setInit.payload, dummyCtx) as TopicId;
    const topics = [capTopic, setTopic];

    const provider = new MemTransportProvider();
    const aliceNoise = await await createIdentity(SIGNING_ED25519, sha256);
    const bobNoise = await await createIdentity(SIGNING_ED25519, sha256);
    const alicePeer: PeerInfo = { keyId: aliceNoise.keyId, addresses: ['mem://alice-ps02'] };
    const bobPeer: PeerInfo = { keyId: bobNoise.keyId, addresses: ['mem://bob-ps02'] };

    const alice = createPeer('ps02', 'alice', provider, aliceNoise, bobPeer, topics);
    const bob = createPeer('ps02', 'bob', provider, bobNoise, alicePeer, topics);

    const aliceCap = (await alice.replica.createObject(capInit)) as RCap;
    const aliceSet = (await alice.replica.createObject(setInit)) as RSet;
    const bobCap = (await bob.replica.createObject(capInit)) as RCap;
    const bobSet = (await bob.replica.createObject(setInit)) as RSet;

    await aliceCap.addIdentity(
        bobSigning.keyId, serializePublicKeyToBase64(bobSigning.publicKey),
        admin,
    );
    await aliceCap.grant(
        bobSigning.keyId, 'write',
        admin,
    );

    const capDag = await aliceCap.getScopedDag();
    const capFrontier = await capDag.getFrontier();
    await aliceSet.refAdvance(capFrontier, admin);

    await aliceCap.startSync();
    await bobCap.startSync();
    await aliceSet.startSync();
    await bobSet.startSync();

    await wait(2000);

    await bobSet.addSigned('from-bob', bobSigning);

    await waitUntil(async () => {
        const view = await aliceSet.getView();
        return view.has('from-bob');
    });

    const aliceView = await aliceSet.getView();
    assertTrue(await aliceView.has('from-bob'), 'alice should see from-bob after sync');

    await cleanup([alice, bob], provider);
}

// ---------- PS03: Foreign-dep deferral ----------

async function testForeignDepDeferral() {
    const admin = await await createIdentity(SIGNING_ED25519, sha256);
    const { capInit, setInit, capId } = await createPermissionedPair(admin);

    const capTopic = capId as TopicId;
    const setTopic = await rSetFactory.computeRootObjectId(setInit.payload, dummyCtx) as TopicId;
    const topics = [capTopic, setTopic];

    const provider = new MemTransportProvider();
    const aliceNoise = await await createIdentity(SIGNING_ED25519, sha256);
    const bobNoise = await await createIdentity(SIGNING_ED25519, sha256);
    const alicePeer: PeerInfo = { keyId: aliceNoise.keyId, addresses: ['mem://alice-ps03'] };
    const bobPeer: PeerInfo = { keyId: bobNoise.keyId, addresses: ['mem://bob-ps03'] };

    const alice = createPeer('ps03', 'alice', provider, aliceNoise, bobPeer, topics);
    const bob = createPeer('ps03', 'bob', provider, bobNoise, alicePeer, topics);

    // Alice creates both objects and adds data
    const aliceCap = (await alice.replica.createObject(capInit)) as RCap;
    const aliceSet = (await alice.replica.createObject(setInit)) as RSet;

    const capDag = await aliceCap.getScopedDag();
    const capFrontier = await capDag.getFrontier();
    await aliceSet.refAdvance(capFrontier, admin);
    await aliceSet.addSigned('deferred', admin);

    // Bob creates ONLY the RSet (cap DAG absent)
    const bobSet = (await bob.replica.createObject(setInit)) as RSet;

    // Start sync: Alice syncs both, Bob syncs only RSet
    await aliceCap.startSync();
    await aliceSet.startSync();
    await bobSet.startSync();

    // Wait a bit for RSet entries to arrive and be deferred on Bob
    await wait(500);

    // Bob should NOT have the deferred entries yet (foreign dep missing).
    // Check the raw DAG: only the creation entry should be present.
    const bobSetDag = (await bob.replica.getDag(setTopic))!;
    let entryCount = 0;
    for await (const _ of bobSetDag.loadAllEntries()) entryCount++;
    assertTrue(entryCount === 1, 'only the creation entry should be present (others deferred)');

    // Now create the RCap on Bob and start syncing it
    const bobCap = (await bob.replica.createObject(capInit)) as RCap;
    await bobCap.startSync();

    // Wait for RCap to sync
    await wait(300);

    // Add a new element on Alice to trigger retry of deferred entries on Bob
    await aliceSet.addSigned('trigger', admin);

    await waitUntil(async () => {
        const view = await bobSet.getView();
        return (await view.has('deferred')) && (await view.has('trigger'));
    });

    const bobViewAfter = await bobSet.getView();
    assertTrue(await bobViewAfter.has('deferred'), 'deferred element should appear after cap DAG available');
    assertTrue(await bobViewAfter.has('trigger'), 'trigger element should also appear');

    await cleanup([alice, bob], provider);
}

// ---------- PS04: Concurrent revocation voids add across peers ----------

async function testRevocationPropagation() {
    const admin = await await createIdentity(SIGNING_ED25519, sha256);
    const bobSigning = await await createIdentity(SIGNING_ED25519, sha256);
    const { capInit, setInit, capId } = await createPermissionedPair(admin);

    const capTopic = capId as TopicId;
    const setTopic = await rSetFactory.computeRootObjectId(setInit.payload, dummyCtx) as TopicId;
    const topics = [capTopic, setTopic];

    const provider = new MemTransportProvider();
    const aliceNoise = await await createIdentity(SIGNING_ED25519, sha256);
    const bobNoise = await await createIdentity(SIGNING_ED25519, sha256);
    const alicePeer: PeerInfo = { keyId: aliceNoise.keyId, addresses: ['mem://alice-ps04'] };
    const bobPeer: PeerInfo = { keyId: bobNoise.keyId, addresses: ['mem://bob-ps04'] };

    const alice = createPeer('ps04', 'alice', provider, aliceNoise, bobPeer, topics);
    const bob = createPeer('ps04', 'bob', provider, bobNoise, alicePeer, topics);

    const aliceCap = (await alice.replica.createObject(capInit)) as RCap;
    const aliceSet = (await alice.replica.createObject(setInit)) as RSet;
    const bobCap = (await bob.replica.createObject(capInit)) as RCap;
    const bobSet = (await bob.replica.createObject(setInit)) as RSet;

    // Grant Bob write cap
    await aliceCap.addIdentity(
        bobSigning.keyId, serializePublicKeyToBase64(bobSigning.publicKey),
        admin,
    );
    await aliceCap.grant(
        bobSigning.keyId, 'write',
        admin,
    );

    const capDag = await aliceCap.getScopedDag();
    const capF1 = await capDag.getFrontier();
    await aliceSet.refAdvance(capF1, admin);

    // Sync the grant + ref-advance to Bob
    await aliceCap.startSync();
    await aliceSet.startSync();
    await bobCap.startSync();
    await bobSet.startSync();

    await waitUntil(async () => {
        const capView = await bobCap.getView();
        if (!(await capView.hasCapability(bobSigning.keyId, 'write'))) return false;
        const setView = await bobSet.getView();
        const refVersion = await setView.resolveRefVersion(capId);
        return !(refVersion.size === 1 && refVersion.has(capId));
    });

    // Save Alice's set frontier (= ref-advance(capF1)) so both operations
    // fork from this same point, making them concurrent.
    const aliceSetDag = await aliceSet.getScopedDag();
    const forkPoint = await aliceSetDag.getFrontier();

    // Bob adds an element (parent = ref-advance(capF1) on Bob's side)
    await bobSet.addSigned('bob-data', bobSigning);

    // Alice revokes Bob and ref-advances from the SAME forkPoint
    // (concurrent with Bob's add since Alice hasn't received it yet)
    await aliceCap.revoke(bobSigning.keyId, 'write', admin);
    const capF2 = await capDag.getFrontier();
    await aliceSet.refAdvance(capF2, admin, forkPoint);

    // Wait for Bob's add to sync to Alice (so both branches are present)
    await waitUntil(async () => {
        const dag = await alice.replica.getDag(setTopic);
        if (dag === undefined) return false;
        let count = 0;
        for await (const _ of dag.loadAllEntries()) count++;
        return count >= 4;
    });

    const aliceViewAfter = await aliceSet.getView();
    assertFalse(await aliceViewAfter.has('bob-data'), 'bob-data should be void after concurrent revocation');

    await waitUntil(async () => {
        const view = await bobSet.getView();
        return !(await view.has('bob-data'));
    });

    const bobViewAfter = await bobSet.getView();
    assertFalse(await bobViewAfter.has('bob-data'), 'bob-data should also be void on bob\'s replica');

    await cleanup([alice, bob], provider);
}

// ---------- PS05: Unauthorized payload rejected during sync ----------

async function testUnauthorizedPayloadRejected() {
    const admin = await await createIdentity(SIGNING_ED25519, sha256);
    const { capInit, setInit, capId } = await createPermissionedPair(admin);

    const capTopic = capId as TopicId;
    const setTopic = await rSetFactory.computeRootObjectId(setInit.payload, dummyCtx) as TopicId;
    const topics = [capTopic, setTopic];

    const provider = new MemTransportProvider();
    const rogueNoise = await await createIdentity(SIGNING_ED25519, sha256);
    const honestNoise = await await createIdentity(SIGNING_ED25519, sha256);
    const roguePeer: PeerInfo = { keyId: rogueNoise.keyId, addresses: ['mem://rogue-ps05'] };
    const honestPeer: PeerInfo = { keyId: honestNoise.keyId, addresses: ['mem://honest-ps05'] };

    // Rogue has selfValidate: false so it can inject arbitrary payloads locally
    const rogue = createPeer('ps05', 'rogue', provider, rogueNoise, honestPeer, topics, { selfValidate: false });
    const honest = createPeer('ps05', 'honest', provider, honestNoise, roguePeer, topics);

    const rogueCap = (await rogue.replica.createObject(capInit)) as RCap;
    const rogueSet = (await rogue.replica.createObject(setInit)) as RSet;
    const honestCap = (await honest.replica.createObject(capInit)) as RCap;
    const honestSet = (await honest.replica.createObject(setInit)) as RSet;

    // Rogue appends an unsigned add directly (bypassing local validation)
    const rogueDag = await rogueSet.getScopedDag();
    const rogueFrontier = await rogueDag.getFrontier();
    await rogueSet.applyPayload(
        { action: 'add', element: 'rogue-data' } as any,
        rogueFrontier,
    );

    // Start sync for all
    await rogueCap.startSync();
    await rogueSet.startSync();
    await honestCap.startSync();
    await honestSet.startSync();

    // Wait for sync to stabilize
    await wait(1000);

    // The honest replica should NOT have the rogue unsigned element
    const honestView = await honestSet.getView();
    assertFalse(await honestView.has('rogue-data'), 'rogue unsigned entry should be rejected by honest replica');

    await cleanup([rogue, honest], provider);
}

export const replicaPermissionedSyncTests = {
    title: '[REP-PERM-SYNC] Permissioned RSet + RCap sync integration tests',
    tests: [
        { name: '[PS01] One-way sync of permissioned RSet + RCap', invoke: testOneWaySyncPermissioned },
        { name: '[PS02] Cross-peer write by grantee', invoke: testCrossPeerWrite },
        { name: '[PS03] Foreign-dep deferral: RSet entries arrive before RCap', invoke: testForeignDepDeferral },
        { name: '[PS04] Revocation propagates across peers', invoke: testRevocationPropagation },
        { name: '[PS05] Unauthorized payload rejected during sync', invoke: testUnauthorizedPayloadRejected },
    ],
};
