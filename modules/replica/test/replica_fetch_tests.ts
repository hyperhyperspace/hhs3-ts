import { assertTrue, assertFalse } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import {
    createBasicCrypto, HASH_SHA256, SIGNING_ED25519, KEM_X25519_HKDF,
    sha256, createIdentity,
} from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { NetworkAddress, PeerInfo, TopicId } from "@hyper-hyper-space/hhs3_mesh";
import {
    Mesh, StaticDiscovery,
    MemTransportProvider, createAuthenticator,
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

function createMesh(
    provider: MemTransportProvider,
    noiseId: OwnIdentity,
    listenAddr: NetworkAddress,
    remotePeer: PeerInfo,
    topics: TopicId[],
) {
    return new Mesh({
        transports: [provider],
        discovery: new StaticDiscovery([remotePeer], topics),
        authenticator: createAuthenticator({
            localKey: noiseId,
            signingName: SIGNING_ED25519,
            kemPrefs: [KEM_X25519_HKDF],
        }),
        localKeyId: noiseId.keyId,
        listenAddresses: [listenAddr],
    });
}

// ---------- FO00: Fetch and sync a single RSet ----------

async function testFetchAndSync() {
    const rsetInit = await RSet.create({ seed: 'fo00-fetch', initialElements: [], hashAlgorithm: 'sha256' });
    const setId = await rSetFactory.computeRootObjectId(rsetInit, dummyCtx) as TopicId;

    const provider = new MemTransportProvider();
    const aliceNoise = await await createIdentity(SIGNING_ED25519, sha256);
    const bobNoise = await await createIdentity(SIGNING_ED25519, sha256);
    const alicePeer: PeerInfo = { keyId: aliceNoise.keyId, addresses: ['mem://alice-fo00'] };
    const bobPeer: PeerInfo = { keyId: bobNoise.keyId, addresses: ['mem://bob-fo00'] };

    const aliceMesh = createMesh(provider, aliceNoise, 'mem://alice-fo00', bobPeer, [setId]);
    const aliceReplica = new Replica({ crypto, hashSuite, config: { selfValidate: true } });
    aliceReplica.attachBackend('default', new MemDagBackend(hashSuite));
    aliceReplica.attachMesh('default', aliceMesh);
    aliceReplica.registerType(RSet.typeId, rSetFactory);

    const aliceSet = (await aliceReplica.createObject(rsetInit)) as RSet;
    await aliceSet.add('x');
    await aliceSet.add('y');

    await aliceSet.startSync();

    const bobMesh = createMesh(provider, bobNoise, 'mem://bob-fo00', alicePeer, [setId]);
    const bobReplica = new Replica({ crypto, hashSuite, config: { selfValidate: true } });
    bobReplica.attachBackend('default', new MemDagBackend(hashSuite));
    bobReplica.attachMesh('default', bobMesh);
    bobReplica.registerType(RSet.typeId, rSetFactory);

    const bobSet = (await bobReplica.fetchObject(setId)) as RSet;
    assertTrue(bobSet !== undefined, 'fetchObject should return an object');
    assertTrue(bobSet.getId() === setId, 'fetched object should have the expected ID');

    await bobSet.startSync();

    await waitUntil(async () => {
        const view = await bobSet.getView();
        return (await view.has('x')) && (await view.has('y'));
    });

    const bobView = await bobSet.getView();
    assertTrue(await bobView.has('x'), 'bob should have x');
    assertTrue(await bobView.has('y'), 'bob should have y');

    await aliceReplica.close();
    await bobReplica.close();
    aliceMesh.close();
    bobMesh.close();
    provider.close();
}

// ---------- FO01: Fetch permissioned RCap + RSet ----------

async function testFetchPermissionedPair() {
    const admin = await await createIdentity(SIGNING_ED25519, sha256);

    const capInit = await RCap.create({
        seed: 'fo01-cap',
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        initialCaps: {
            'admin':  { managedBy: ['creator'] },
            'write':  { managedBy: ['admin'] },
        },
    });
    const capId = await rCapFactory.computeRootObjectId(capInit, dummyCtx);

    const setInit = await RSet.create({
        seed: 'fo01-set',
        initialElements: [],
        hashAlgorithm: 'sha256',
        capabilityRef: capId,
        capRequirements: { add: 'write', delete: 'write' },
    });
    const setId = await rSetFactory.computeRootObjectId(setInit, dummyCtx);

    const provider = new MemTransportProvider();
    const aliceNoise = await await createIdentity(SIGNING_ED25519, sha256);
    const bobNoise = await await createIdentity(SIGNING_ED25519, sha256);
    const alicePeer: PeerInfo = { keyId: aliceNoise.keyId, addresses: ['mem://alice-fo01'] };
    const bobPeer: PeerInfo = { keyId: bobNoise.keyId, addresses: ['mem://bob-fo01'] };

    const topics = [capId as TopicId, setId as TopicId];

    const aliceMesh = createMesh(provider, aliceNoise, 'mem://alice-fo01', bobPeer, topics);
    const aliceReplica = new Replica({ crypto, hashSuite, config: { selfValidate: true } });
    aliceReplica.attachBackend('default', new MemDagBackend(hashSuite));
    aliceReplica.attachMesh('default', aliceMesh);
    aliceReplica.registerType(RSet.typeId, rSetFactory);
    aliceReplica.registerType(RCap.typeId, rCapFactory);

    const aliceCap = (await aliceReplica.createObject(capInit)) as RCap;
    const aliceSet = (await aliceReplica.createObject(setInit)) as RSet;

    const capDag = await aliceCap.getScopedDag();
    const capFrontier = await capDag.getFrontier();
    await aliceSet.refAdvance(capFrontier, admin);
    await aliceSet.addSigned('hello', admin);

    await aliceCap.startSync();
    await aliceSet.startSync();

    const bobMesh = createMesh(provider, bobNoise, 'mem://bob-fo01', alicePeer, topics);
    const bobReplica = new Replica({ crypto, hashSuite, config: { selfValidate: true } });
    bobReplica.attachBackend('default', new MemDagBackend(hashSuite));
    bobReplica.attachMesh('default', bobMesh);
    bobReplica.registerType(RSet.typeId, rSetFactory);
    bobReplica.registerType(RCap.typeId, rCapFactory);

    const bobCap = (await bobReplica.fetchObject(capId)) as RCap;
    const bobSet = (await bobReplica.fetchObject(setId)) as RSet;

    assertTrue(bobCap.getId() === capId, 'fetched cap should have correct ID');
    assertTrue(bobSet.getId() === setId, 'fetched set should have correct ID');

    await bobCap.startSync();
    await bobSet.startSync();

    await waitUntil(async () => {
        const view = await bobSet.getView();
        return view.has('hello');
    });

    const bobView = await bobSet.getView();
    assertTrue(await bobView.has('hello'), 'bob should have hello after sync');

    await aliceReplica.close();
    await bobReplica.close();
    aliceMesh.close();
    bobMesh.close();
    provider.close();
}

// ---------- FO02: fetchObject is idempotent ----------

async function testFetchIdempotent() {
    const rsetInit = await RSet.create({ seed: 'fo02-idem', initialElements: ['alpha'], hashAlgorithm: 'sha256' });
    const setId = await rSetFactory.computeRootObjectId(rsetInit, dummyCtx);

    const provider = new MemTransportProvider();
    const aliceNoise = await await createIdentity(SIGNING_ED25519, sha256);
    const bobNoise = await await createIdentity(SIGNING_ED25519, sha256);
    const alicePeer: PeerInfo = { keyId: aliceNoise.keyId, addresses: ['mem://alice-fo02'] };
    const bobPeer: PeerInfo = { keyId: bobNoise.keyId, addresses: ['mem://bob-fo02'] };

    const aliceMesh = createMesh(provider, aliceNoise, 'mem://alice-fo02', bobPeer, [setId as TopicId]);
    const aliceReplica = new Replica({ crypto, hashSuite, config: { selfValidate: true } });
    aliceReplica.attachBackend('default', new MemDagBackend(hashSuite));
    aliceReplica.attachMesh('default', aliceMesh);
    aliceReplica.registerType(RSet.typeId, rSetFactory);

    const aliceSet = (await aliceReplica.createObject(rsetInit)) as RSet;
    await aliceSet.startSync();

    const bobMesh = createMesh(provider, bobNoise, 'mem://bob-fo02', alicePeer, [setId as TopicId]);
    const bobReplica = new Replica({ crypto, hashSuite, config: { selfValidate: true } });
    bobReplica.attachBackend('default', new MemDagBackend(hashSuite));
    bobReplica.attachMesh('default', bobMesh);
    bobReplica.registerType(RSet.typeId, rSetFactory);

    const first = await bobReplica.fetchObject(setId);
    const second = await bobReplica.fetchObject(setId);

    assertTrue(first === second, 'second fetchObject should return the same cached instance');

    await aliceReplica.close();
    await bobReplica.close();
    aliceMesh.close();
    bobMesh.close();
    provider.close();
}

// ---------- FO03: fetchObject with unknown type rejects ----------

async function testFetchUnknownType() {
    const rsetInit = await RSet.create({ seed: 'fo03-unknown', initialElements: [], hashAlgorithm: 'sha256' });
    const setId = await rSetFactory.computeRootObjectId(rsetInit, dummyCtx);

    const provider = new MemTransportProvider();
    const aliceNoise = await await createIdentity(SIGNING_ED25519, sha256);
    const bobNoise = await await createIdentity(SIGNING_ED25519, sha256);
    const alicePeer: PeerInfo = { keyId: aliceNoise.keyId, addresses: ['mem://alice-fo03'] };
    const bobPeer: PeerInfo = { keyId: bobNoise.keyId, addresses: ['mem://bob-fo03'] };

    const aliceMesh = createMesh(provider, aliceNoise, 'mem://alice-fo03', bobPeer, [setId as TopicId]);
    const aliceReplica = new Replica({ crypto, hashSuite, config: { selfValidate: true } });
    aliceReplica.attachBackend('default', new MemDagBackend(hashSuite));
    aliceReplica.attachMesh('default', aliceMesh);
    aliceReplica.registerType(RSet.typeId, rSetFactory);

    const aliceSet = (await aliceReplica.createObject(rsetInit)) as RSet;
    await aliceSet.startSync();

    // Bob does NOT register the RSet type
    const bobMesh = createMesh(provider, bobNoise, 'mem://bob-fo03', alicePeer, [setId as TopicId]);
    const bobReplica = new Replica({ crypto, hashSuite, config: { selfValidate: true } });
    bobReplica.attachBackend('default', new MemDagBackend(hashSuite));
    bobReplica.attachMesh('default', bobMesh);

    let threw = false;
    try {
        await bobReplica.fetchObject(setId);
    } catch (e) {
        threw = true;
    }
    assertTrue(threw, 'fetchObject should throw when the type is not registered');

    await aliceReplica.close();
    await bobReplica.close();
    aliceMesh.close();
    bobMesh.close();
    provider.close();
}

// ---------- FO04: fetchObject times out with no peers ----------

async function testFetchTimeout() {
    const rsetInit = await RSet.create({ seed: 'fo04-timeout', initialElements: [], hashAlgorithm: 'sha256' });
    const setId = await rSetFactory.computeRootObjectId(rsetInit, dummyCtx);

    const provider = new MemTransportProvider();
    const bobNoise = await await createIdentity(SIGNING_ED25519, sha256);

    const fakePeer: PeerInfo = { keyId: bobNoise.keyId, addresses: ['mem://nobody-fo04'] };
    const bobMesh = createMesh(provider, bobNoise, 'mem://bob-fo04', fakePeer, [setId as TopicId]);
    const bobReplica = new Replica({ crypto, hashSuite, config: { selfValidate: true } });
    bobReplica.attachBackend('default', new MemDagBackend(hashSuite));
    bobReplica.attachMesh('default', bobMesh);
    bobReplica.registerType(RSet.typeId, rSetFactory);

    let threw = false;
    try {
        await bobReplica.fetchObject(setId, { timeoutMs: 500 });
    } catch (e: any) {
        threw = true;
        assertTrue(e.message.includes('timed out'), 'error should mention timeout');
    }
    assertTrue(threw, 'fetchObject should throw on timeout');

    await bobReplica.close();
    bobMesh.close();
    provider.close();
}

export const replicaFetchTests = {
    title: '[REP-FETCH] Object fetching via creation payload bootstrap',
    tests: [
        { name: '[FO00] Fetch and sync a single RSet', invoke: testFetchAndSync },
        { name: '[FO01] Fetch permissioned RCap + RSet pair', invoke: testFetchPermissionedPair },
        { name: '[FO02] fetchObject is idempotent', invoke: testFetchIdempotent },
        { name: '[FO03] fetchObject with unknown type rejects', invoke: testFetchUnknownType },
        { name: '[FO04] fetchObject times out with no peers', invoke: testFetchTimeout },
    ],
};
