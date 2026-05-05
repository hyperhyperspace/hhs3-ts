import { assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import {
    createBasicCrypto, HASH_SHA256, ed25519, SIGNING_ED25519, KEM_X25519_HKDF,
    keyIdFromPublicKey, sha256,
    stringToUint8Array,
} from "@hyper-hyper-space/hhs3_crypto";
import type { PublicKey, KeyId } from "@hyper-hyper-space/hhs3_crypto";
import type { NetworkAddress, PeerInfo, TopicId } from "@hyper-hyper-space/hhs3_mesh";
import {
    Mesh, StaticDiscovery, MemTransportProvider, createNoiseAuthenticator,
} from "@hyper-hyper-space/hhs3_mesh";
import { Replica, MemDagBackend } from "../src/index.js";
import { RSet, rSetFactory } from "@hyper-hyper-space/hhs3_std_types";
import type { RContext } from "@hyper-hyper-space/hhs3_mvt";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

const dummyCtx = { getCrypto: () => crypto } as RContext;

async function makeNoiseKeyPair() {
    const kp = await ed25519.generateKeyPair();
    const pk: PublicKey = { suite: SIGNING_ED25519, key: kp.publicKey };
    const keyId = keyIdFromPublicKey(pk, sha256);
    return { publicKey: pk, secretKey: kp.secretKey, keyId };
}

async function createSyncableReplica(
    provider: MemTransportProvider,
    listenAddr: NetworkAddress,
    remotePeer: PeerInfo,
    topic: TopicId,
) {
    const identity = await makeNoiseKeyPair();

    const authenticator = createNoiseAuthenticator({
        localKey: { publicKey: identity.publicKey, secretKey: identity.secretKey },
        signingName: SIGNING_ED25519,
        kemPrefs: [KEM_X25519_HKDF],
    });

    const discovery = new StaticDiscovery([remotePeer], [topic]);

    const mesh = new Mesh({
        transports: [provider],
        discovery,
        authenticator,
        localKeyId: identity.keyId,
        listenAddresses: [listenAddr],
    });

    const replica = new Replica({ crypto, hashSuite, config: { selfValidate: true } });
    replica.attachBackend('default', new MemDagBackend(hashSuite));
    replica.attachMesh('default', mesh);
    replica.registerType(RSet.typeId, rSetFactory);

    return { replica, mesh, keyId: identity.keyId, peerInfo: { keyId: identity.keyId, addresses: [listenAddr] } as PeerInfo };
}

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

async function testOneWaySync() {
    const rsetInit = await RSet.create({ seed: 'fs00-sync', initialElements: [], hashAlgorithm: 'sha256' });
    const topic = await rSetFactory.computeRootObjectId(rsetInit.payload, dummyCtx) as TopicId;

    const provider = new MemTransportProvider();

    const aliceIdentity = await makeNoiseKeyPair();
    const bobIdentity = await makeNoiseKeyPair();

    const alicePeer: PeerInfo = { keyId: aliceIdentity.keyId, addresses: ['mem://alice-fs00'] };
    const bobPeer: PeerInfo = { keyId: bobIdentity.keyId, addresses: ['mem://bob-fs00'] };

    const aliceAuth = createNoiseAuthenticator({
        localKey: { publicKey: aliceIdentity.publicKey, secretKey: aliceIdentity.secretKey },
        signingName: SIGNING_ED25519, kemPrefs: [KEM_X25519_HKDF],
    });
    const bobAuth = createNoiseAuthenticator({
        localKey: { publicKey: bobIdentity.publicKey, secretKey: bobIdentity.secretKey },
        signingName: SIGNING_ED25519, kemPrefs: [KEM_X25519_HKDF],
    });

    const aliceMesh = new Mesh({
        transports: [provider],
        discovery: new StaticDiscovery([bobPeer], [topic]),
        authenticator: aliceAuth,
        localKeyId: aliceIdentity.keyId,
        listenAddresses: ['mem://alice-fs00'],
    });

    const bobMesh = new Mesh({
        transports: [provider],
        discovery: new StaticDiscovery([alicePeer], [topic]),
        authenticator: bobAuth,
        localKeyId: bobIdentity.keyId,
        listenAddresses: ['mem://bob-fs00'],
    });

    const aliceReplica = new Replica({ crypto, hashSuite, config: { selfValidate: true } });
    aliceReplica.attachBackend('default', new MemDagBackend(hashSuite));
    aliceReplica.attachMesh('default', aliceMesh);
    aliceReplica.registerType(RSet.typeId, rSetFactory);

    const bobReplica = new Replica({ crypto, hashSuite, config: { selfValidate: true } });
    bobReplica.attachBackend('default', new MemDagBackend(hashSuite));
    bobReplica.attachMesh('default', bobMesh);
    bobReplica.registerType(RSet.typeId, rSetFactory);

    const aliceSet = (await aliceReplica.createObject(rsetInit)) as RSet;
    const bobSet = (await bobReplica.createObject(rsetInit)) as RSet;

    assertTrue(aliceSet.getId() === bobSet.getId(), 'both sets should have the same ID');

    aliceSet.configure({ meshLabel: 'default', backendLabel: 'default' });
    bobSet.configure({ meshLabel: 'default', backendLabel: 'default' });

    await aliceSet.startSync();
    await bobSet.startSync();

    await wait(300);

    await aliceSet.add('x');
    await aliceSet.add('y');
    await aliceSet.add('z');

    await waitUntil(async () => {
        const view = await bobSet.getView();
        return (await view.has('x')) && (await view.has('y')) && (await view.has('z'));
    });

    const bobView = await bobSet.getView();
    assertTrue(await bobView.has('x'), 'bob should have x');
    assertTrue(await bobView.has('y'), 'bob should have y');
    assertTrue(await bobView.has('z'), 'bob should have z');

    await aliceReplica.close();
    await bobReplica.close();
    aliceMesh.close();
    bobMesh.close();
    provider.close();
}

async function testBidirectionalSync() {
    const rsetInit = await RSet.create({ seed: 'fs01-bidi', initialElements: [], hashAlgorithm: 'sha256' });
    const topic = await rSetFactory.computeRootObjectId(rsetInit.payload, dummyCtx) as TopicId;

    const provider = new MemTransportProvider();

    const aliceIdentity = await makeNoiseKeyPair();
    const bobIdentity = await makeNoiseKeyPair();

    const alicePeer: PeerInfo = { keyId: aliceIdentity.keyId, addresses: ['mem://alice-fs01'] };
    const bobPeer: PeerInfo = { keyId: bobIdentity.keyId, addresses: ['mem://bob-fs01'] };

    const aliceMesh = new Mesh({
        transports: [provider],
        discovery: new StaticDiscovery([bobPeer], [topic]),
        authenticator: createNoiseAuthenticator({
            localKey: { publicKey: aliceIdentity.publicKey, secretKey: aliceIdentity.secretKey },
            signingName: SIGNING_ED25519, kemPrefs: [KEM_X25519_HKDF],
        }),
        localKeyId: aliceIdentity.keyId,
        listenAddresses: ['mem://alice-fs01'],
    });

    const bobMesh = new Mesh({
        transports: [provider],
        discovery: new StaticDiscovery([alicePeer], [topic]),
        authenticator: createNoiseAuthenticator({
            localKey: { publicKey: bobIdentity.publicKey, secretKey: bobIdentity.secretKey },
            signingName: SIGNING_ED25519, kemPrefs: [KEM_X25519_HKDF],
        }),
        localKeyId: bobIdentity.keyId,
        listenAddresses: ['mem://bob-fs01'],
    });

    const aliceReplica = new Replica({ crypto, hashSuite, config: { selfValidate: true } });
    aliceReplica.attachBackend('default', new MemDagBackend(hashSuite));
    aliceReplica.attachMesh('default', aliceMesh);
    aliceReplica.registerType(RSet.typeId, rSetFactory);

    const bobReplica = new Replica({ crypto, hashSuite, config: { selfValidate: true } });
    bobReplica.attachBackend('default', new MemDagBackend(hashSuite));
    bobReplica.attachMesh('default', bobMesh);
    bobReplica.registerType(RSet.typeId, rSetFactory);

    const aliceSet = (await aliceReplica.createObject(rsetInit)) as RSet;
    const bobSet = (await bobReplica.createObject(rsetInit)) as RSet;

    aliceSet.configure({ meshLabel: 'default', backendLabel: 'default' });
    bobSet.configure({ meshLabel: 'default', backendLabel: 'default' });

    await aliceSet.startSync();
    await bobSet.startSync();

    await wait(500);

    await aliceSet.add('from-alice');
    await wait(50);
    await bobSet.add('from-bob');

    await waitUntil(async () => {
        const aliceView = await aliceSet.getView();
        const bobView = await bobSet.getView();
        return (await aliceView.has('from-bob')) && (await bobView.has('from-alice'));
    });

    const aliceView = await aliceSet.getView();
    const bobView = await bobSet.getView();
    assertTrue(await aliceView.has('from-bob'), 'alice should have from-bob');
    assertTrue(await aliceView.has('from-alice'), 'alice should have from-alice');
    assertTrue(await bobView.has('from-alice'), 'bob should have from-alice');
    assertTrue(await bobView.has('from-bob'), 'bob should have from-bob');

    await aliceReplica.close();
    await bobReplica.close();
    aliceMesh.close();
    bobMesh.close();
    provider.close();
}

async function testLateJoinSync() {
    const rsetInit = await RSet.create({ seed: 'fs02-late', initialElements: [], hashAlgorithm: 'sha256' });
    const topic = await rSetFactory.computeRootObjectId(rsetInit.payload, dummyCtx) as TopicId;

    const provider = new MemTransportProvider();

    const aliceIdentity = await makeNoiseKeyPair();
    const bobIdentity = await makeNoiseKeyPair();

    const alicePeer: PeerInfo = { keyId: aliceIdentity.keyId, addresses: ['mem://alice-fs02'] };
    const bobPeer: PeerInfo = { keyId: bobIdentity.keyId, addresses: ['mem://bob-fs02'] };

    const aliceMesh = new Mesh({
        transports: [provider],
        discovery: new StaticDiscovery([bobPeer], [topic]),
        authenticator: createNoiseAuthenticator({
            localKey: { publicKey: aliceIdentity.publicKey, secretKey: aliceIdentity.secretKey },
            signingName: SIGNING_ED25519, kemPrefs: [KEM_X25519_HKDF],
        }),
        localKeyId: aliceIdentity.keyId,
        listenAddresses: ['mem://alice-fs02'],
    });

    const aliceReplica = new Replica({ crypto, hashSuite, config: { selfValidate: true } });
    aliceReplica.attachBackend('default', new MemDagBackend(hashSuite));
    aliceReplica.attachMesh('default', aliceMesh);
    aliceReplica.registerType(RSet.typeId, rSetFactory);

    const aliceSet = (await aliceReplica.createObject(rsetInit)) as RSet;
    aliceSet.configure({ meshLabel: 'default', backendLabel: 'default' });
    await aliceSet.startSync();

    await aliceSet.add('early-1');
    await aliceSet.add('early-2');
    await aliceSet.add('early-3');

    const bobMesh = new Mesh({
        transports: [provider],
        discovery: new StaticDiscovery([alicePeer], [topic]),
        authenticator: createNoiseAuthenticator({
            localKey: { publicKey: bobIdentity.publicKey, secretKey: bobIdentity.secretKey },
            signingName: SIGNING_ED25519, kemPrefs: [KEM_X25519_HKDF],
        }),
        localKeyId: bobIdentity.keyId,
        listenAddresses: ['mem://bob-fs02'],
    });

    const bobReplica = new Replica({ crypto, hashSuite, config: { selfValidate: true } });
    bobReplica.attachBackend('default', new MemDagBackend(hashSuite));
    bobReplica.attachMesh('default', bobMesh);
    bobReplica.registerType(RSet.typeId, rSetFactory);

    const bobSet = (await bobReplica.createObject(rsetInit)) as RSet;
    bobSet.configure({ meshLabel: 'default', backendLabel: 'default' });
    await bobSet.startSync();

    await waitUntil(async () => {
        const view = await bobSet.getView();
        return (await view.has('early-1')) && (await view.has('early-2')) && (await view.has('early-3'));
    });

    const bobView = await bobSet.getView();
    assertTrue(await bobView.has('early-1'), 'bob should have early-1');
    assertTrue(await bobView.has('early-2'), 'bob should have early-2');
    assertTrue(await bobView.has('early-3'), 'bob should have early-3');

    await aliceReplica.close();
    await bobReplica.close();
    aliceMesh.close();
    bobMesh.close();
    provider.close();
}

export const replicaFullSyncTests = {
    title: '[REP-FULL-SYNC] Full discovery-based sync between replicas',
    tests: [
        { name: '[FS00] One-way sync: elements added on A appear on B', invoke: testOneWaySync },
        { name: '[FS01] Bidirectional sync: elements added on both sides converge', invoke: testBidirectionalSync },
        { name: '[FS02] Late join: B starts sync after A has written data', invoke: testLateJoinSync },
    ],
};
