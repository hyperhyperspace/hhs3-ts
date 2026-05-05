import { testing } from '@hyper-hyper-space/hhs3_util';
import type { KeyId, PublicKey } from '@hyper-hyper-space/hhs3_crypto';
import {
    sha256,
    stringToUint8Array,
    keyIdFromPublicKey,
    ed25519,
    x25519Hkdf,
    SIGNING_ED25519,
    KEM_X25519_HKDF,
    KEM_ML_KEM_768,
    KEM_X25519_HKDF_ML_KEM_768,
} from '@hyper-hyper-space/hhs3_crypto';

import { createMemTransportPair, MemTransportProvider } from '../src/mem_transport.js';
import { ConnectionPool, connectionKey } from '../src/connection_pool.js';
import type { AuthenticatedChannel } from '../src/authenticator.js';
import type { PeerDiscovery, PeerInfo } from '../src/discovery.js';
import type { TopicId } from '../src/discovery.js';
import type { PeerAuthenticator } from '../src/authenticator.js';
import type { Transport, NetworkAddress } from '../src/transport.js';
import { createSwarm } from '../src/swarm.js';
import type { PeerAuthorizer } from '../src/swarm.js';
import { Mesh } from '../src/mesh.js';
import { createNoiseAuthenticator } from '../src/noise_authenticator.js';
import { StaticDiscovery } from '../src/static_discovery.js';
import { DiscoveryStack } from '../src/discovery_stack.js';
import { PoolReuseDiscovery } from '../src/pool_reuse_discovery.js';
import {
    encodeTopicMessage, encodeControlMessage, decodeMessage,
    MSG_TYPE_TOPIC, MSG_TYPE_CONTROL,
    CTRL_TOPIC_INTEREST, CTRL_TOPIC_ACCEPT, CTRL_TOPIC_REJECT,
    encodeControlTopicInterest, encodeControlTopicAccept, encodeControlTopicReject,
    decodeControlPayload,
} from '../src/mux.js';

// --- helpers ---

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) {
        if (a[i] !== b[i]) return false;
    }
    return true;
}

let peerCounter = 0;

function makeFakePeer(endpoint: NetworkAddress = 'mem://default'): { publicKey: PublicKey; keyId: KeyId; endpoint: NetworkAddress } {
    peerCounter++;
    const key = stringToUint8Array(`fake-peer-key-${peerCounter}`);
    const publicKey: PublicKey = { suite: 'test', key };
    const keyId = keyIdFromPublicKey(publicKey, sha256);
    return { publicKey, keyId, endpoint };
}

function makeFakeChannel(
    peer: { publicKey: PublicKey; keyId: KeyId; endpoint: NetworkAddress }
): { channel: AuthenticatedChannel; remote: AuthenticatedChannel } {
    const [a, b] = createMemTransportPair();
    const channel: AuthenticatedChannel = {
        remotePeer: peer.publicKey,
        remoteKeyId: peer.keyId,
        get open() { return a.open; },
        send: (msg) => a.send(msg),
        close: () => a.close(),
        onMessage: (cb) => a.onMessage(cb),
        onClose: (cb) => a.onClose(cb),
    };
    const remote: AuthenticatedChannel = {
        remotePeer: peer.publicKey,
        remoteKeyId: peer.keyId,
        get open() { return b.open; },
        send: (msg) => b.send(msg),
        close: () => b.close(),
        onMessage: (cb) => b.onMessage(cb),
        onClose: (cb) => b.onClose(cb),
    };
    return { channel, remote };
}

// --- transport tests ---

async function testMemTransportSendReceive() {
    const [a, b] = createMemTransportPair();

    const received: Uint8Array[] = [];
    b.onMessage((msg) => received.push(msg));

    const msg = new TextEncoder().encode('hello mesh');
    a.send(msg);

    testing.assertEquals(received.length, 1, 'should receive one message');
    testing.assertTrue(bytesEqual(received[0], msg), 'message content should match');
}

async function testMemTransportBidirectional() {
    const [a, b] = createMemTransportPair();

    const fromA: Uint8Array[] = [];
    const fromB: Uint8Array[] = [];
    b.onMessage((msg) => fromA.push(msg));
    a.onMessage((msg) => fromB.push(msg));

    a.send(new TextEncoder().encode('to-b'));
    b.send(new TextEncoder().encode('to-a'));

    testing.assertEquals(fromA.length, 1, 'b should receive from a');
    testing.assertEquals(fromB.length, 1, 'a should receive from b');
}

async function testMemTransportClose() {
    const [a, b] = createMemTransportPair();

    let aClosed = false;
    let bClosed = false;
    a.onClose(() => { aClosed = true; });
    b.onClose(() => { bClosed = true; });

    a.close();

    testing.assertTrue(aClosed, 'a should fire close');
    testing.assertTrue(bClosed, 'b should fire close when peer closes');
    testing.assertFalse(a.open, 'a should not be open');
    testing.assertFalse(b.open, 'b should not be open');
}

async function testMemTransportProviderConnectListen() {
    const provider = new MemTransportProvider();

    let serverTransport: Transport | undefined;
    await provider.listen('mem://test-addr', (t) => { serverTransport = t; });

    const client = await provider.connect('mem://test-addr');

    testing.assertTrue(serverTransport !== undefined, 'server should receive connection');

    const received: Uint8Array[] = [];
    serverTransport!.onMessage((msg) => received.push(msg));
    client.send(new TextEncoder().encode('from-client'));

    testing.assertEquals(received.length, 1, 'server should receive message');

    provider.close();
}

// --- mux framing tests ---

async function testMuxTopicEncodeDecode() {
    const topic = sha256.hashToB64(stringToUint8Array('test-topic'));
    const payload = new TextEncoder().encode('hello');

    const frame = encodeTopicMessage(topic, payload);
    const decoded = decodeMessage(frame);

    testing.assertEquals(decoded.type, MSG_TYPE_TOPIC, 'type should be topic');
    testing.assertEquals(decoded.topic, topic, 'topic should match');
    testing.assertTrue(bytesEqual(decoded.payload, payload), 'payload should match');
}

async function testMuxControlEncodeDecode() {
    const payload = new TextEncoder().encode('control-data');

    const frame = encodeControlMessage(payload);
    const decoded = decodeMessage(frame);

    testing.assertEquals(decoded.type, MSG_TYPE_CONTROL, 'type should be control');
    testing.assertTrue(decoded.topic === undefined, 'control has no topic');
    testing.assertTrue(bytesEqual(decoded.payload, payload), 'payload should match');
}

async function testMuxTopicIsolation() {
    const topicA = sha256.hashToB64(stringToUint8Array('topic-a'));
    const topicB = sha256.hashToB64(stringToUint8Array('topic-b'));
    const payloadA = new TextEncoder().encode('for-a');
    const payloadB = new TextEncoder().encode('for-b');

    const frameA = encodeTopicMessage(topicA, payloadA);
    const frameB = encodeTopicMessage(topicB, payloadB);

    const decodedA = decodeMessage(frameA);
    const decodedB = decodeMessage(frameB);

    testing.assertEquals(decodedA.topic, topicA, 'frame A topic');
    testing.assertEquals(decodedB.topic, topicB, 'frame B topic');
    testing.assertTrue(decodedA.topic !== decodedB.topic, 'topics should differ');
}

async function testMuxEmptyPayload() {
    const topic = sha256.hashToB64(stringToUint8Array('empty-topic'));
    const frame = encodeTopicMessage(topic, new Uint8Array(0));
    const decoded = decodeMessage(frame);

    testing.assertEquals(decoded.type, MSG_TYPE_TOPIC, 'type should be topic');
    testing.assertEquals(decoded.payload.length, 0, 'payload should be empty');
}

// --- connection pool tests ---

async function testPoolAddGetRemove() {
    const pool = new ConnectionPool();
    const peer = makeFakePeer('mem://device-1');
    const { channel } = makeFakeChannel(peer);

    const conn = pool.add(channel, peer.endpoint);
    testing.assertEquals(conn.peerId, peer.keyId, 'connection peerId should match');
    testing.assertEquals(conn.endpoint, peer.endpoint, 'connection endpoint should match');
    testing.assertEquals(pool.size(), 1, 'pool size should be 1');

    const got = pool.get(peer.keyId, peer.endpoint);
    testing.assertTrue(got !== undefined, 'should get connection by (keyId, endpoint)');

    pool.remove(peer.keyId, peer.endpoint);
    testing.assertEquals(pool.size(), 0, 'pool size should be 0 after remove');
}

async function testPoolDedupByEndpoint() {
    const pool = new ConnectionPool();
    const peer = makeFakePeer('mem://device-1');
    const { channel: ch1 } = makeFakeChannel(peer);
    const { channel: ch2 } = makeFakeChannel(peer);

    pool.add(ch1, peer.endpoint);
    pool.add(ch2, peer.endpoint);

    testing.assertEquals(pool.size(), 1, 'pool should deduplicate by (keyId, endpoint)');
    testing.assertFalse(ch2.open, 'duplicate channel should be closed');
}

async function testPoolMultipleDevicesSameKeyId() {
    const pool = new ConnectionPool();
    const peerA = makeFakePeer('mem://device-a');
    const keyBytes = stringToUint8Array(`shared-key`);
    const pk: PublicKey = { suite: 'test', key: keyBytes };
    const keyId = keyIdFromPublicKey(pk, sha256);

    const peerOnDeviceA = { publicKey: pk, keyId, endpoint: 'mem://device-a' as NetworkAddress };
    const peerOnDeviceB = { publicKey: pk, keyId, endpoint: 'mem://device-b' as NetworkAddress };

    const { channel: chA } = makeFakeChannel(peerOnDeviceA);
    const { channel: chB } = makeFakeChannel(peerOnDeviceB);

    pool.add(chA, peerOnDeviceA.endpoint);
    pool.add(chB, peerOnDeviceB.endpoint);

    testing.assertEquals(pool.size(), 2, 'same keyId on different endpoints = 2 connections');
    testing.assertEquals(pool.getByKeyId(keyId).length, 2, 'getByKeyId should return both');
}

async function testPoolDisconnectCleanup() {
    const pool = new ConnectionPool();
    const peer = makeFakePeer('mem://device-1');
    const { channel } = makeFakeChannel(peer);

    pool.add(channel, peer.endpoint);
    testing.assertEquals(pool.size(), 1, 'pool has one connection');

    channel.close();
    testing.assertEquals(pool.size(), 0, 'pool should remove disconnected channel');
}

async function testPoolEvents() {
    const pool = new ConnectionPool();
    const peer = makeFakePeer('mem://device-1');

    const connected: string[] = [];
    const disconnected: string[] = [];

    pool.onConnect((conn) => connected.push(connectionKey(conn.peerId, conn.endpoint)));
    pool.onDisconnect((key) => disconnected.push(key));

    const { channel } = makeFakeChannel(peer);
    pool.add(channel, peer.endpoint);

    const expectedKey = connectionKey(peer.keyId, peer.endpoint);
    testing.assertEquals(connected.length, 1, 'onConnect should fire');
    testing.assertEquals(connected[0], expectedKey, 'onConnect key should match');

    channel.close();
    testing.assertEquals(disconnected.length, 1, 'onDisconnect should fire');
    testing.assertEquals(disconnected[0], expectedKey, 'onDisconnect key should match');
}

async function testPoolOpenTopicSendReceive() {
    const pool = new ConnectionPool();
    const peer = makeFakePeer('mem://device-1');
    const topic = sha256.hashToB64(stringToUint8Array('test-topic'));

    const { channel, remote } = makeFakeChannel(peer);
    pool.add(channel, peer.endpoint);

    const tc = pool.openTopic(peer.keyId, peer.endpoint, topic);
    testing.assertEquals(tc.topic, topic, 'topic channel topic should match');
    testing.assertTrue(tc.open, 'topic channel should be open');

    const received: Uint8Array[] = [];
    tc.onMessage((msg) => received.push(msg));

    // Simulate remote side sending a framed topic message
    const payload = new TextEncoder().encode('hello from remote');
    remote.send(encodeTopicMessage(topic, payload));

    testing.assertEquals(received.length, 1, 'topic channel should receive message');
    testing.assertTrue(bytesEqual(received[0], payload), 'payload should match');

    // Send from our topic channel, verify remote receives framed message
    const outgoing = new TextEncoder().encode('hello from local');
    tc.send(outgoing);

    const remoteReceived: Uint8Array[] = [];
    remote.onMessage((frame) => {
        const decoded = decodeMessage(frame);
        if (decoded.type === MSG_TYPE_TOPIC) {
            remoteReceived.push(decoded.payload);
        }
    });

    tc.send(new TextEncoder().encode('second'));

    testing.assertEquals(remoteReceived.length, 1, 'remote should receive framed message');
    testing.assertTrue(bytesEqual(remoteReceived[0], new TextEncoder().encode('second')), 'remote payload should match');

    pool.close();
}

async function testPoolTopicIsolation() {
    const pool = new ConnectionPool();
    const peer = makeFakePeer('mem://device-1');
    const topicA = sha256.hashToB64(stringToUint8Array('topic-a'));
    const topicB = sha256.hashToB64(stringToUint8Array('topic-b'));

    const { channel, remote } = makeFakeChannel(peer);
    pool.add(channel, peer.endpoint);

    const tcA = pool.openTopic(peer.keyId, peer.endpoint, topicA);
    const tcB = pool.openTopic(peer.keyId, peer.endpoint, topicB);

    const receivedA: Uint8Array[] = [];
    const receivedB: Uint8Array[] = [];
    tcA.onMessage((msg) => receivedA.push(msg));
    tcB.onMessage((msg) => receivedB.push(msg));

    remote.send(encodeTopicMessage(topicA, new TextEncoder().encode('for-a')));
    remote.send(encodeTopicMessage(topicB, new TextEncoder().encode('for-b')));

    testing.assertEquals(receivedA.length, 1, 'topic A should get one message');
    testing.assertEquals(receivedB.length, 1, 'topic B should get one message');
    testing.assertTrue(bytesEqual(receivedA[0], new TextEncoder().encode('for-a')), 'topic A payload');
    testing.assertTrue(bytesEqual(receivedB[0], new TextEncoder().encode('for-b')), 'topic B payload');

    pool.close();
}

async function testPoolTopicChannelCloseDoesNotCloseConnection() {
    const pool = new ConnectionPool();
    const peer = makeFakePeer('mem://device-1');
    const topic = sha256.hashToB64(stringToUint8Array('test-topic'));

    const { channel } = makeFakeChannel(peer);
    pool.add(channel, peer.endpoint);

    const tc = pool.openTopic(peer.keyId, peer.endpoint, topic);
    tc.close();

    testing.assertFalse(tc.open, 'topic channel should be closed');
    testing.assertTrue(channel.open, 'underlying connection should still be open');
    testing.assertEquals(pool.size(), 1, 'pool should still have connection');

    pool.close();
}

async function testPoolTopicDedup() {
    const pool = new ConnectionPool();
    const peer = makeFakePeer('mem://device-1');
    const topic = sha256.hashToB64(stringToUint8Array('test-topic'));

    const { channel } = makeFakeChannel(peer);
    pool.add(channel, peer.endpoint);

    const tc1 = pool.openTopic(peer.keyId, peer.endpoint, topic);
    const tc2 = pool.openTopic(peer.keyId, peer.endpoint, topic);

    testing.assertTrue(tc1 === tc2, 'openTopic should return same shim for same triple');

    pool.close();
}

async function testPoolConnectionCloseClosesTopics() {
    const pool = new ConnectionPool();
    const peer = makeFakePeer('mem://device-1');
    const topic = sha256.hashToB64(stringToUint8Array('test-topic'));

    const { channel } = makeFakeChannel(peer);
    pool.add(channel, peer.endpoint);

    const tc = pool.openTopic(peer.keyId, peer.endpoint, topic);
    let topicClosed = false;
    tc.onClose(() => { topicClosed = true; });

    channel.close();

    testing.assertTrue(topicClosed, 'topic channel should close when connection closes');
    testing.assertFalse(tc.open, 'topic channel should not be open');
}

// --- swarm tests ---

function makeStubDiscovery(peers: PeerInfo[]): PeerDiscovery {
    return {
        async *discover(_topic: TopicId, _schemes?: string[]) {
            for (const p of peers) yield p;
        },
        async announce() {},
        async leave() {},
    };
}

function makeStubAuthenticator(peerMap: Map<string, PublicKey>): PeerAuthenticator {
    return {
        async authenticate(transport: Transport, role: 'initiator' | 'responder', expectedRemote?: KeyId) {
            if (expectedRemote === undefined) throw new Error('expected remote required in tests');
            const pk = peerMap.get(expectedRemote);
            if (!pk) throw new Error('unknown peer in test authenticator');
            return {
                remotePeer: pk,
                remoteKeyId: expectedRemote,
                get open() { return transport.open; },
                send: (msg: Uint8Array) => transport.send(msg),
                close: () => transport.close(),
                onMessage: (cb: (msg: Uint8Array) => void) => transport.onMessage(cb),
                onClose: (cb: () => void) => transport.onClose(cb),
            };
        },
    };
}

async function testSwarmLifecycle() {
    const pool = new ConnectionPool();
    const topic = sha256.hashToB64(stringToUint8Array('swarm-topic'));

    const swarm = createSwarm({ topic, mode: 'dormant' }, {
        pool,
        discovery: makeStubDiscovery([]),
        authenticator: makeStubAuthenticator(new Map()),
        transports: [],
    });

    testing.assertEquals(swarm.mode, 'dormant', 'initial mode should be dormant');
    testing.assertEquals(swarm.peers().length, 0, 'no peers in dormant mode');

    swarm.deactivate();
    testing.assertEquals(swarm.mode, 'passive', 'deactivate should set passive');

    swarm.activate();
    testing.assertEquals(swarm.mode, 'active', 'activate should set active');

    swarm.sleep();
    testing.assertEquals(swarm.mode, 'dormant', 'sleep should set dormant');

    swarm.destroy();
}

async function testSwarmPeerJoinViaPool() {
    const pool = new ConnectionPool();
    const topic = sha256.hashToB64(stringToUint8Array('swarm-pool-topic'));
    const peer = makeFakePeer('mem://device-1');

    const swarm = createSwarm({ topic, mode: 'passive' }, {
        pool,
        discovery: makeStubDiscovery([]),
        authenticator: makeStubAuthenticator(new Map()),
        transports: [],
    });

    swarm.deactivate();

    const joined: string[] = [];
    swarm.onPeerJoin((p) => joined.push(connectionKey(p.keyId, p.endpoint)));

    const { channel } = makeFakeChannel(peer);
    pool.add(channel, peer.endpoint);

    testing.assertEquals(joined.length, 1, 'passive swarm should adopt pool peers');
    testing.assertEquals(swarm.peers().length, 1, 'swarm should have one peer');
    testing.assertEquals(swarm.peers()[0].keyId, peer.keyId, 'peer keyId should match');

    swarm.destroy();
}

async function testSwarmPeerLeaveOnDisconnect() {
    const pool = new ConnectionPool();
    const topic = sha256.hashToB64(stringToUint8Array('swarm-leave-topic'));
    const peer = makeFakePeer('mem://device-1');

    const swarm = createSwarm({ topic, mode: 'passive' }, {
        pool,
        discovery: makeStubDiscovery([]),
        authenticator: makeStubAuthenticator(new Map()),
        transports: [],
    });

    swarm.deactivate();

    const left: string[] = [];
    swarm.onPeerLeave((p) => left.push(connectionKey(p.keyId, p.endpoint)));

    const { channel } = makeFakeChannel(peer);
    pool.add(channel, peer.endpoint);
    testing.assertEquals(swarm.peers().length, 1, 'should have peer');

    channel.close();
    testing.assertEquals(left.length, 1, 'should fire onPeerLeave');
    testing.assertEquals(swarm.peers().length, 0, 'should have no peers');

    swarm.destroy();
}

async function testSwarmDiscoveryAndConnect() {
    const pool = new ConnectionPool();
    const topic = sha256.hashToB64(stringToUint8Array('swarm-discovery-topic'));

    const peer1 = makeFakePeer('mem://peer1');
    const peer2 = makeFakePeer('mem://peer2');

    const provider = new MemTransportProvider();
    await provider.listen('mem://peer1', (_t) => {});
    await provider.listen('mem://peer2', (_t) => {});

    const peerMap = new Map<string, PublicKey>();
    peerMap.set(peer1.keyId, peer1.publicKey);
    peerMap.set(peer2.keyId, peer2.publicKey);

    const discoveredPeers: PeerInfo[] = [
        { keyId: peer1.keyId, addresses: ['mem://peer1'] },
        { keyId: peer2.keyId, addresses: ['mem://peer2'] },
    ];

    const stubAuth: PeerAuthenticator = {
        async authenticate(transport: Transport, role: 'initiator' | 'responder', expectedRemote?: KeyId) {
            if (expectedRemote === undefined) throw new Error('expected remote required');
            const pk = peerMap.get(expectedRemote);
            if (!pk) throw new Error('unknown peer');
            return {
                remotePeer: pk,
                remoteKeyId: expectedRemote,
                get open() { return transport.open; },
                send: (msg: Uint8Array) => transport.send(msg),
                close: () => transport.close(),
                onMessage: (cb: (msg: Uint8Array) => void) => transport.onMessage(cb),
                onClose: (cb: () => void) => transport.onClose(cb),
            };
        },
    };

    const swarm = createSwarm({ topic, targetPeers: 2 }, {
        pool,
        discovery: makeStubDiscovery(discoveredPeers),
        authenticator: stubAuth,
        transports: [provider],
    });

    swarm.activate();

    await new Promise(resolve => setTimeout(resolve, 50));

    testing.assertEquals(swarm.peers().length, 2, 'swarm should discover and connect to 2 peers');
    testing.assertEquals(pool.size(), 2, 'pool should have 2 connections');

    swarm.destroy();
    provider.close();
}

async function testSwarmPoolReuse() {
    const pool = new ConnectionPool();
    const topic1 = sha256.hashToB64(stringToUint8Array('topic-1'));
    const topic2 = sha256.hashToB64(stringToUint8Array('topic-2'));
    const peer = makeFakePeer('mem://device-1');

    const deps = {
        pool,
        discovery: makeStubDiscovery([]),
        authenticator: makeStubAuthenticator(new Map()),
        transports: [],
    };

    const swarm1 = createSwarm({ topic: topic1 }, deps);
    const swarm2 = createSwarm({ topic: topic2 }, deps);

    swarm1.deactivate();
    swarm2.deactivate();

    const join1: string[] = [];
    const join2: string[] = [];
    swarm1.onPeerJoin((p) => join1.push(connectionKey(p.keyId, p.endpoint)));
    swarm2.onPeerJoin((p) => join2.push(connectionKey(p.keyId, p.endpoint)));

    const { channel } = makeFakeChannel(peer);
    pool.add(channel, peer.endpoint);

    testing.assertEquals(join1.length, 1, 'swarm1 should see peer from pool');
    testing.assertEquals(join2.length, 1, 'swarm2 should see same peer from pool');

    swarm1.destroy();
    swarm2.destroy();
}

async function testSwarmDormantIgnoresPool() {
    const pool = new ConnectionPool();
    const topic = sha256.hashToB64(stringToUint8Array('dormant-topic'));
    const peer = makeFakePeer('mem://device-1');

    const swarm = createSwarm({ topic, mode: 'dormant' }, {
        pool,
        discovery: makeStubDiscovery([]),
        authenticator: makeStubAuthenticator(new Map()),
        transports: [],
    });

    const { channel } = makeFakeChannel(peer);
    pool.add(channel, peer.endpoint);

    testing.assertEquals(swarm.peers().length, 0, 'dormant swarm should not adopt pool peers');

    swarm.destroy();
}

// --- mesh facade tests ---

async function testMeshCreateAndPool() {
    const mesh = new Mesh({
        transports:    [],
        discovery:     makeStubDiscovery([]),
        authenticator: makeStubAuthenticator(new Map()),
    });

    testing.assertTrue(mesh.pool !== undefined, 'mesh should have a pool');
    testing.assertEquals(mesh.pool.size(), 0, 'pool should start empty');

    mesh.close();
}

async function testMeshCreateSwarm() {
    const topic = sha256.hashToB64(stringToUint8Array('mesh-swarm-topic'));
    const peer = makeFakePeer('mem://device-1');

    const mesh = new Mesh({
        transports:    [],
        discovery:     makeStubDiscovery([]),
        authenticator: makeStubAuthenticator(new Map()),
    });

    const swarm = mesh.createSwarm(topic, { mode: 'passive' });
    testing.assertEquals(swarm.topic, topic, 'swarm topic should match');
    testing.assertEquals(swarm.mode, 'passive', 'swarm mode should be passive');

    const { channel } = makeFakeChannel(peer);
    mesh.pool.add(channel, peer.endpoint);

    testing.assertEquals(swarm.peers().length, 1, 'swarm should adopt peer from mesh pool');

    mesh.close();
}

async function testMeshMultipleSwarmsSharePool() {
    const topicA = sha256.hashToB64(stringToUint8Array('mesh-topic-a'));
    const topicB = sha256.hashToB64(stringToUint8Array('mesh-topic-b'));
    const peer = makeFakePeer('mem://device-1');

    const mesh = new Mesh({
        transports:    [],
        discovery:     makeStubDiscovery([]),
        authenticator: makeStubAuthenticator(new Map()),
    });

    const swarmA = mesh.createSwarm(topicA, { mode: 'passive' });
    const swarmB = mesh.createSwarm(topicB, { mode: 'passive' });

    const { channel } = makeFakeChannel(peer);
    mesh.pool.add(channel, peer.endpoint);

    testing.assertEquals(swarmA.peers().length, 1, 'swarm A should see peer');
    testing.assertEquals(swarmB.peers().length, 1, 'swarm B should see same peer');

    mesh.close();
}

async function testMeshCloseDestroysAll() {
    const topicA = sha256.hashToB64(stringToUint8Array('mesh-close-a'));
    const topicB = sha256.hashToB64(stringToUint8Array('mesh-close-b'));
    const peer = makeFakePeer('mem://device-1');

    const mesh = new Mesh({
        transports:    [],
        discovery:     makeStubDiscovery([]),
        authenticator: makeStubAuthenticator(new Map()),
    });

    const swarmA = mesh.createSwarm(topicA, { mode: 'passive' });
    const swarmB = mesh.createSwarm(topicB, { mode: 'passive' });

    const { channel } = makeFakeChannel(peer);
    mesh.pool.add(channel, peer.endpoint);

    mesh.close();

    testing.assertEquals(mesh.pool.size(), 0, 'pool should be empty after close');
    testing.assertEquals(mesh.swarms().length, 0, 'no swarms after close');
    testing.assertEquals(swarmA.peers().length, 0, 'swarm A peers gone');
    testing.assertEquals(swarmB.peers().length, 0, 'swarm B peers gone');
}

async function testMeshSwarmsTracking() {
    const topicA = sha256.hashToB64(stringToUint8Array('mesh-track-a'));
    const topicB = sha256.hashToB64(stringToUint8Array('mesh-track-b'));

    const mesh = new Mesh({
        transports:    [],
        discovery:     makeStubDiscovery([]),
        authenticator: makeStubAuthenticator(new Map()),
    });

    testing.assertEquals(mesh.swarms().length, 0, 'no swarms initially');

    const swarmA = mesh.createSwarm(topicA);
    testing.assertEquals(mesh.swarms().length, 1, 'one swarm after first create');

    const swarmB = mesh.createSwarm(topicB);
    testing.assertEquals(mesh.swarms().length, 2, 'two swarms after second create');

    mesh.close();
}

// --- noise authenticator tests ---

async function makeNoiseKeyPair() {
    const kp = await ed25519.generateKeyPair();
    const pk: PublicKey = { suite: SIGNING_ED25519, key: kp.publicKey };
    const keyId = keyIdFromPublicKey(pk, sha256);
    return { publicKey: pk, secretKey: kp.secretKey, keyId };
}

async function testAuthHandshakeSuccess() {
    const alice = await makeNoiseKeyPair();
    const bob = await makeNoiseKeyPair();

    const aliceAuth = createNoiseAuthenticator({
        localKey: { publicKey: alice.publicKey, secretKey: alice.secretKey },
        signingName: SIGNING_ED25519,
        kemPrefs: [KEM_X25519_HKDF],
    });
    const bobAuth = createNoiseAuthenticator({
        localKey: { publicKey: bob.publicKey, secretKey: bob.secretKey },
        signingName: SIGNING_ED25519,
        kemPrefs: [KEM_X25519_HKDF],
    });

    const [tA, tB] = createMemTransportPair();

    const [chanA, chanB] = await Promise.all([
        aliceAuth.authenticate(tA, 'initiator', bob.keyId),
        bobAuth.authenticate(tB, 'responder'),
    ]);

    testing.assertEquals(chanA.remoteKeyId, bob.keyId, 'alice should see bob keyId');
    testing.assertEquals(chanB.remoteKeyId, alice.keyId, 'bob should see alice keyId');
    testing.assertTrue(chanA.open, 'alice channel should be open');
    testing.assertTrue(chanB.open, 'bob channel should be open');
}

async function testAuthEncryptedRoundTrip() {
    const alice = await makeNoiseKeyPair();
    const bob = await makeNoiseKeyPair();

    const aliceAuth = createNoiseAuthenticator({
        localKey: { publicKey: alice.publicKey, secretKey: alice.secretKey },
        signingName: SIGNING_ED25519,
        kemPrefs: [KEM_X25519_HKDF],
    });
    const bobAuth = createNoiseAuthenticator({
        localKey: { publicKey: bob.publicKey, secretKey: bob.secretKey },
        signingName: SIGNING_ED25519,
        kemPrefs: [KEM_X25519_HKDF],
    });

    const [tA, tB] = createMemTransportPair();
    const [chanA, chanB] = await Promise.all([
        aliceAuth.authenticate(tA, 'initiator', bob.keyId),
        bobAuth.authenticate(tB, 'responder'),
    ]);

    const received: Uint8Array[] = [];
    chanB.onMessage((msg) => received.push(msg));

    const payload = new TextEncoder().encode('hello encrypted world');
    chanA.send(payload);

    testing.assertEquals(received.length, 1, 'bob should receive one message');
    testing.assertTrue(bytesEqual(received[0], payload), 'decrypted payload should match');
}

async function testAuthBidirectionalEncryption() {
    const alice = await makeNoiseKeyPair();
    const bob = await makeNoiseKeyPair();

    const aliceAuth = createNoiseAuthenticator({
        localKey: { publicKey: alice.publicKey, secretKey: alice.secretKey },
        signingName: SIGNING_ED25519,
        kemPrefs: [KEM_X25519_HKDF],
    });
    const bobAuth = createNoiseAuthenticator({
        localKey: { publicKey: bob.publicKey, secretKey: bob.secretKey },
        signingName: SIGNING_ED25519,
        kemPrefs: [KEM_X25519_HKDF],
    });

    const [tA, tB] = createMemTransportPair();
    const [chanA, chanB] = await Promise.all([
        aliceAuth.authenticate(tA, 'initiator', bob.keyId),
        bobAuth.authenticate(tB, 'responder'),
    ]);

    const fromAlice: Uint8Array[] = [];
    const fromBob: Uint8Array[] = [];
    chanB.onMessage((msg) => fromAlice.push(msg));
    chanA.onMessage((msg) => fromBob.push(msg));

    chanA.send(new TextEncoder().encode('a-to-b'));
    chanB.send(new TextEncoder().encode('b-to-a'));

    testing.assertEquals(fromAlice.length, 1, 'bob should get message from alice');
    testing.assertEquals(fromBob.length, 1, 'alice should get message from bob');
    testing.assertTrue(bytesEqual(fromAlice[0], new TextEncoder().encode('a-to-b')), 'alice payload');
    testing.assertTrue(bytesEqual(fromBob[0], new TextEncoder().encode('b-to-a')), 'bob payload');
}

async function testAuthKemNegotiation() {
    const alice = await makeNoiseKeyPair();
    const bob = await makeNoiseKeyPair();

    const aliceAuth = createNoiseAuthenticator({
        localKey: { publicKey: alice.publicKey, secretKey: alice.secretKey },
        signingName: SIGNING_ED25519,
        kemPrefs: [KEM_X25519_HKDF_ML_KEM_768, KEM_X25519_HKDF],
    });
    const bobAuth = createNoiseAuthenticator({
        localKey: { publicKey: bob.publicKey, secretKey: bob.secretKey },
        signingName: SIGNING_ED25519,
        kemPrefs: [KEM_X25519_HKDF],
    });

    const [tA, tB] = createMemTransportPair();
    const [chanA, chanB] = await Promise.all([
        aliceAuth.authenticate(tA, 'initiator', bob.keyId),
        bobAuth.authenticate(tB, 'responder'),
    ]);

    testing.assertTrue(chanA.open, 'should negotiate to x25519-hkdf');

    const received: Uint8Array[] = [];
    chanB.onMessage((msg) => received.push(msg));
    chanA.send(new TextEncoder().encode('negotiated'));
    testing.assertEquals(received.length, 1, 'should work after negotiation');
    testing.assertTrue(bytesEqual(received[0], new TextEncoder().encode('negotiated')), 'payload match');
}

async function testAuthKemNegotiationNoCommon() {
    const alice = await makeNoiseKeyPair();
    const bob = await makeNoiseKeyPair();

    const aliceAuth = createNoiseAuthenticator({
        localKey: { publicKey: alice.publicKey, secretKey: alice.secretKey },
        signingName: SIGNING_ED25519,
        kemPrefs: [KEM_ML_KEM_768],
    });
    const bobAuth = createNoiseAuthenticator({
        localKey: { publicKey: bob.publicKey, secretKey: bob.secretKey },
        signingName: SIGNING_ED25519,
        kemPrefs: [KEM_X25519_HKDF],
    });

    const [tA, tB] = createMemTransportPair();

    let failed = false;
    try {
        await Promise.all([
            aliceAuth.authenticate(tA, 'initiator', bob.keyId),
            bobAuth.authenticate(tB, 'responder'),
        ]);
    } catch {
        failed = true;
    }

    testing.assertTrue(failed, 'handshake should fail with no common KEM suite');
}

async function testAuthExpectedRemoteMismatch() {
    const alice = await makeNoiseKeyPair();
    const bob = await makeNoiseKeyPair();
    const charlie = await makeNoiseKeyPair();

    const aliceAuth = createNoiseAuthenticator({
        localKey: { publicKey: alice.publicKey, secretKey: alice.secretKey },
        signingName: SIGNING_ED25519,
        kemPrefs: [KEM_X25519_HKDF],
    });
    const bobAuth = createNoiseAuthenticator({
        localKey: { publicKey: bob.publicKey, secretKey: bob.secretKey },
        signingName: SIGNING_ED25519,
        kemPrefs: [KEM_X25519_HKDF],
    });

    const [tA, tB] = createMemTransportPair();

    let failed = false;
    try {
        await Promise.all([
            aliceAuth.authenticate(tA, 'initiator', charlie.keyId),
            bobAuth.authenticate(tB, 'responder'),
        ]);
    } catch {
        failed = true;
    }

    testing.assertTrue(failed, 'handshake should fail when remote is not expected peer');
}

async function testAuthTamperedCiphertext() {
    const alice = await makeNoiseKeyPair();
    const bob = await makeNoiseKeyPair();

    const aliceAuth = createNoiseAuthenticator({
        localKey: { publicKey: alice.publicKey, secretKey: alice.secretKey },
        signingName: SIGNING_ED25519,
        kemPrefs: [KEM_X25519_HKDF],
    });
    const bobAuth = createNoiseAuthenticator({
        localKey: { publicKey: bob.publicKey, secretKey: bob.secretKey },
        signingName: SIGNING_ED25519,
        kemPrefs: [KEM_X25519_HKDF],
    });

    const [tA, tB] = createMemTransportPair();
    const [chanA, chanB] = await Promise.all([
        aliceAuth.authenticate(tA, 'initiator', bob.keyId),
        bobAuth.authenticate(tB, 'responder'),
    ]);

    let closeFired = false;
    chanB.onClose(() => { closeFired = true; });

    // Send a valid message first to establish baseline
    const received: Uint8Array[] = [];
    chanB.onMessage((msg) => received.push(msg));
    chanA.send(new TextEncoder().encode('valid'));
    testing.assertEquals(received.length, 1, 'first message should work');

    // Tamper: send raw garbage directly on the transport (bypassing AEAD)
    tA.send(new Uint8Array([0xFF, 0xFE, 0xFD, 0xFC]));

    testing.assertTrue(closeFired, 'tampered message should close channel');
}

async function testAuthIndependentSessions() {
    const alice = await makeNoiseKeyPair();
    const bob = await makeNoiseKeyPair();

    const aliceAuth = createNoiseAuthenticator({
        localKey: { publicKey: alice.publicKey, secretKey: alice.secretKey },
        signingName: SIGNING_ED25519,
        kemPrefs: [KEM_X25519_HKDF],
    });
    const bobAuth = createNoiseAuthenticator({
        localKey: { publicKey: bob.publicKey, secretKey: bob.secretKey },
        signingName: SIGNING_ED25519,
        kemPrefs: [KEM_X25519_HKDF],
    });

    // Session 1
    const [t1A, t1B] = createMemTransportPair();
    const [chan1A, chan1B] = await Promise.all([
        aliceAuth.authenticate(t1A, 'initiator', bob.keyId),
        bobAuth.authenticate(t1B, 'responder'),
    ]);

    // Session 2
    const [t2A, t2B] = createMemTransportPair();
    const [chan2A, chan2B] = await Promise.all([
        aliceAuth.authenticate(t2A, 'initiator', bob.keyId),
        bobAuth.authenticate(t2B, 'responder'),
    ]);

    const recv1: Uint8Array[] = [];
    const recv2: Uint8Array[] = [];
    chan1B.onMessage((msg) => recv1.push(msg));
    chan2B.onMessage((msg) => recv2.push(msg));

    chan1A.send(new TextEncoder().encode('session1'));
    chan2A.send(new TextEncoder().encode('session2'));

    testing.assertEquals(recv1.length, 1, 'session 1 should receive');
    testing.assertEquals(recv2.length, 1, 'session 2 should receive');
    testing.assertTrue(bytesEqual(recv1[0], new TextEncoder().encode('session1')), 'session 1 payload');
    testing.assertTrue(bytesEqual(recv2[0], new TextEncoder().encode('session2')), 'session 2 payload');

    // Cross-check: sessions are independent
    testing.assertTrue(chan1A.remoteKeyId === chan2A.remoteKeyId, 'same remote identity');
}

async function testAuthTofu() {
    const alice = await makeNoiseKeyPair();
    const bob = await makeNoiseKeyPair();

    const aliceAuth = createNoiseAuthenticator({
        localKey: { publicKey: alice.publicKey, secretKey: alice.secretKey },
        signingName: SIGNING_ED25519,
        kemPrefs: [KEM_X25519_HKDF],
    });
    const bobAuth = createNoiseAuthenticator({
        localKey: { publicKey: bob.publicKey, secretKey: bob.secretKey },
        signingName: SIGNING_ED25519,
        kemPrefs: [KEM_X25519_HKDF],
    });

    const [tA, tB] = createMemTransportPair();

    // Initiator does NOT pass expectedRemote (TOFU: first contact)
    const [chanA, chanB] = await Promise.all([
        aliceAuth.authenticate(tA, 'initiator'),
        bobAuth.authenticate(tB, 'responder'),
    ]);

    testing.assertTrue(chanA.open, 'channel should be open');
    testing.assertEquals(chanA.remoteKeyId, bob.keyId, 'initiator learns remote keyId after handshake');
    testing.assertEquals(chanB.remoteKeyId, alice.keyId, 'responder learns initiator keyId');

    const received: Uint8Array[] = [];
    chanB.onMessage((msg) => received.push(msg));
    chanA.send(new TextEncoder().encode('tofu-hello'));
    testing.assertEquals(received.length, 1, 'message should arrive');
    testing.assertTrue(bytesEqual(received[0], new TextEncoder().encode('tofu-hello')), 'payload match');
}

async function testAuthIdentityProtection() {
    const alice = await makeNoiseKeyPair();
    const bob = await makeNoiseKeyPair();
    const charlie = await makeNoiseKeyPair();

    const aliceAuth = createNoiseAuthenticator({
        localKey: { publicKey: alice.publicKey, secretKey: alice.secretKey },
        signingName: SIGNING_ED25519,
        kemPrefs: [KEM_X25519_HKDF],
    });
    const bobAuth = createNoiseAuthenticator({
        localKey: { publicKey: bob.publicKey, secretKey: bob.secretKey },
        signingName: SIGNING_ED25519,
        kemPrefs: [KEM_X25519_HKDF],
    });

    const [tA, tB] = createMemTransportPair();

    // Track messages sent on the initiator's transport
    const sentByInitiator: Uint8Array[] = [];
    const origSend = tA.send.bind(tA);
    tA.send = (msg: Uint8Array) => { sentByInitiator.push(msg); origSend(msg); };

    // Alice expects charlie but gets bob — should abort before sending Msg3
    let failed = false;
    try {
        await Promise.all([
            aliceAuth.authenticate(tA, 'initiator', charlie.keyId),
            bobAuth.authenticate(tB, 'responder'),
        ]);
    } catch {
        failed = true;
    }

    testing.assertTrue(failed, 'handshake should fail on identity mismatch');
    // Msg1 is sent (anonymous), but Msg3 (with identity) should NOT have been sent
    testing.assertEquals(sentByInitiator.length, 1, 'only Msg1 should be sent, not Msg3');
}

// --- [STATIC] StaticDiscovery tests ---

async function collectAll(iter: AsyncIterable<PeerInfo>): Promise<PeerInfo[]> {
    const out: PeerInfo[] = [];
    for await (const p of iter) out.push(p);
    return out;
}

async function testStaticYieldsMatchingTopic() {
    const topicA: TopicId = sha256.hashToB64(stringToUint8Array('topic-a'));
    const peer = makeFakePeer('ws://a:1');
    const sd = new StaticDiscovery([{ keyId: peer.keyId, addresses: [peer.endpoint] }], [topicA]);

    const results = await collectAll(sd.discover(topicA));
    testing.assertEquals(results.length, 1, 'should yield one peer');
    testing.assertEquals(results[0].keyId, peer.keyId, 'keyId matches');
}

async function testStaticYieldsNothingNonMatchingTopic() {
    const topicA: TopicId = sha256.hashToB64(stringToUint8Array('topic-a'));
    const topicB: TopicId = sha256.hashToB64(stringToUint8Array('topic-b'));
    const peer = makeFakePeer('ws://a:1');
    const sd = new StaticDiscovery([{ keyId: peer.keyId, addresses: [peer.endpoint] }], [topicA]);

    const results = await collectAll(sd.discover(topicB));
    testing.assertEquals(results.length, 0, 'should yield nothing for non-matching topic');
}

async function testStaticFiltersByScheme() {
    const topic: TopicId = sha256.hashToB64(stringToUint8Array('topic-scheme'));
    const peer = makeFakePeer('ws://a:1');
    const info: PeerInfo = { keyId: peer.keyId, addresses: ['ws://a:1', 'tcp://a:2'] };
    const sd = new StaticDiscovery([info], [topic]);

    const wsOnly = await collectAll(sd.discover(topic, ['ws']));
    testing.assertEquals(wsOnly.length, 1, 'should yield peer');
    testing.assertEquals(wsOnly[0].addresses.length, 1, 'one address');
    testing.assertEquals(wsOnly[0].addresses[0], 'ws://a:1', 'only ws address');

    const tcpOnly = await collectAll(sd.discover(topic, ['tcp']));
    testing.assertEquals(tcpOnly.length, 1, 'should yield peer for tcp');
    testing.assertEquals(tcpOnly[0].addresses[0], 'tcp://a:2', 'only tcp address');

    const quicOnly = await collectAll(sd.discover(topic, ['quic']));
    testing.assertEquals(quicOnly.length, 0, 'no peers with quic');
}

async function testStaticResultsAreShuffled() {
    const topic: TopicId = sha256.hashToB64(stringToUint8Array('topic-shuffle'));
    const peers: PeerInfo[] = [];
    for (let i = 0; i < 20; i++) {
        const p = makeFakePeer(`ws://peer-${i}:1`);
        peers.push({ keyId: p.keyId, addresses: [p.endpoint] });
    }
    const sd = new StaticDiscovery(peers, [topic]);

    const orders: string[] = [];
    for (let trial = 0; trial < 5; trial++) {
        const result = await collectAll(sd.discover(topic));
        orders.push(result.map(r => r.keyId).join(','));
    }

    const allSame = orders.every(o => o === orders[0]);
    testing.assertTrue(!allSame, 'results should not be in the same order every time (20 peers, 5 trials)');
}

async function testStaticAnnounceLeaveNoOps() {
    const topic: TopicId = sha256.hashToB64(stringToUint8Array('topic-noop'));
    const sd = new StaticDiscovery([], [topic]);
    const peer = makeFakePeer('ws://a:1');

    await sd.announce(topic, { keyId: peer.keyId, addresses: [peer.endpoint] });
    await sd.leave(topic, peer.keyId);
}

async function testStaticEmptyPeerList() {
    const topic: TopicId = sha256.hashToB64(stringToUint8Array('topic-empty'));
    const sd = new StaticDiscovery([], [topic]);

    const results = await collectAll(sd.discover(topic));
    testing.assertEquals(results.length, 0, 'empty peer list yields nothing');
}

// --- [STACK] DiscoveryStack tests ---

function makeSimpleDiscovery(peers: PeerInfo[]): PeerDiscovery {
    return {
        async *discover(_topic: TopicId, _schemes?: string[]): AsyncIterable<PeerInfo> {
            for (const p of peers) yield p;
        },
        async announce(): Promise<void> {},
        async leave(): Promise<void> {},
    };
}

async function testStackSingleLayer() {
    const topic: TopicId = sha256.hashToB64(stringToUint8Array('stack-single'));
    const peer = makeFakePeer('ws://s:1');
    const info: PeerInfo = { keyId: peer.keyId, addresses: [peer.endpoint] };
    const stack = new DiscoveryStack([
        { source: makeSimpleDiscovery([info]), priority: 0 },
    ]);

    const results = await collectAll(stack.discover(topic));
    testing.assertEquals(results.length, 1, 'single layer yields its peers');
    testing.assertEquals(results[0].keyId, peer.keyId, 'keyId matches');
}

async function testStackStopsAtTarget() {
    const topic: TopicId = sha256.hashToB64(stringToUint8Array('stack-target'));
    const peers: PeerInfo[] = [];
    for (let i = 0; i < 10; i++) {
        const p = makeFakePeer(`ws://t-${i}:1`);
        peers.push({ keyId: p.keyId, addresses: [p.endpoint] });
    }
    const stack = new DiscoveryStack([
        { source: makeSimpleDiscovery(peers), priority: 0 },
    ]);

    const results = await collectAll(stack.discover(topic, undefined, 3));
    testing.assertEquals(results.length, 3, 'should stop at targetPeers');
}

async function testStackFallsThrough() {
    const topic: TopicId = sha256.hashToB64(stringToUint8Array('stack-fallthrough'));
    const peerA = makeFakePeer('ws://ft-a:1');
    const peerB = makeFakePeer('ws://ft-b:1');
    const stack = new DiscoveryStack([
        { source: makeSimpleDiscovery([{ keyId: peerA.keyId, addresses: [peerA.endpoint] }]), priority: 0 },
        { source: makeSimpleDiscovery([{ keyId: peerB.keyId, addresses: [peerB.endpoint] }]), priority: 10 },
    ]);

    const results = await collectAll(stack.discover(topic, undefined, 5));
    testing.assertEquals(results.length, 2, 'falls through to lower priority');
    const ids = results.map(r => r.keyId);
    testing.assertTrue(ids.includes(peerA.keyId), 'includes peer from priority 0');
    testing.assertTrue(ids.includes(peerB.keyId), 'includes peer from priority 10');
}

async function testStackParallelMerge() {
    const topic: TopicId = sha256.hashToB64(stringToUint8Array('stack-parallel'));
    const peerA = makeFakePeer('ws://pm-a:1');
    const peerB = makeFakePeer('ws://pm-b:1');
    const stack = new DiscoveryStack([
        { source: makeSimpleDiscovery([{ keyId: peerA.keyId, addresses: [peerA.endpoint] }]), priority: 0 },
        { source: makeSimpleDiscovery([{ keyId: peerB.keyId, addresses: [peerB.endpoint] }]), priority: 0 },
    ]);

    const results = await collectAll(stack.discover(topic, undefined, 10));
    testing.assertEquals(results.length, 2, 'both sources in same priority merged');
    const ids = new Set(results.map(r => r.keyId));
    testing.assertTrue(ids.has(peerA.keyId), 'has peer A');
    testing.assertTrue(ids.has(peerB.keyId), 'has peer B');
}

async function testStackDeduplication() {
    const topic: TopicId = sha256.hashToB64(stringToUint8Array('stack-dedup'));
    const peer = makeFakePeer('ws://dup:1');
    const info: PeerInfo = { keyId: peer.keyId, addresses: [peer.endpoint] };
    const stack = new DiscoveryStack([
        { source: makeSimpleDiscovery([info]), priority: 0 },
        { source: makeSimpleDiscovery([info]), priority: 10 },
    ]);

    const results = await collectAll(stack.discover(topic, undefined, 10));
    testing.assertEquals(results.length, 1, 'duplicate peer yielded only once');
}

async function testStackFewerThanTarget() {
    const topic: TopicId = sha256.hashToB64(stringToUint8Array('stack-fewer'));
    const peer = makeFakePeer('ws://fewer:1');
    const info: PeerInfo = { keyId: peer.keyId, addresses: [peer.endpoint] };
    const stack = new DiscoveryStack([
        { source: makeSimpleDiscovery([info]), priority: 0 },
    ]);

    const results = await collectAll(stack.discover(topic, undefined, 100));
    testing.assertEquals(results.length, 1, 'returns fewer than target without error');
}

async function testStackAnnounceLeaveBroadcast() {
    const topic: TopicId = sha256.hashToB64(stringToUint8Array('stack-broadcast'));
    let announceCalls = 0;
    let leaveCalls = 0;
    const source: PeerDiscovery = {
        async *discover(): AsyncIterable<PeerInfo> {},
        async announce() { announceCalls++; },
        async leave() { leaveCalls++; },
    };
    const stack = new DiscoveryStack([
        { source, priority: 0 },
        { source, priority: 10 },
    ]);
    const peer = makeFakePeer('ws://bc:1');
    await stack.announce(topic, { keyId: peer.keyId, addresses: [peer.endpoint] });
    await stack.leave(topic, peer.keyId);

    testing.assertEquals(announceCalls, 2, 'announce broadcast to all');
    testing.assertEquals(leaveCalls, 2, 'leave broadcast to all');
}

async function testStackAnnounceResilience() {
    const topic: TopicId = sha256.hashToB64(stringToUint8Array('stack-resilient'));
    let successCalls = 0;
    const failing: PeerDiscovery = {
        async *discover(): AsyncIterable<PeerInfo> {},
        async announce() { throw new Error('boom'); },
        async leave() { throw new Error('boom'); },
    };
    const succeeding: PeerDiscovery = {
        async *discover(): AsyncIterable<PeerInfo> {},
        async announce() { successCalls++; },
        async leave() { successCalls++; },
    };
    const stack = new DiscoveryStack([
        { source: failing, priority: 0 },
        { source: succeeding, priority: 10 },
    ]);
    const peer = makeFakePeer('ws://res:1');
    await stack.announce(topic, { keyId: peer.keyId, addresses: [peer.endpoint] });
    await stack.leave(topic, peer.keyId);

    testing.assertEquals(successCalls, 2, 'succeeding source still called despite failing sibling');
}

async function testStackEmpty() {
    const topic: TopicId = sha256.hashToB64(stringToUint8Array('stack-empty'));
    const stack = new DiscoveryStack([]);
    const results = await collectAll(stack.discover(topic, undefined, 10));
    testing.assertEquals(results.length, 0, 'empty stack yields nothing');
}

// --- control channel protocol tests ---

async function testCtrlTopicInterestRoundTrip() {
    const topic = sha256.hashToB64(stringToUint8Array('ctrl-topic'));
    const frame = encodeControlTopicInterest(topic);
    const decoded = decodeMessage(frame);
    testing.assertEquals(decoded.type, MSG_TYPE_CONTROL, 'type should be control');
    const ctrl = decodeControlPayload(decoded.payload);
    testing.assertEquals(ctrl.ctrl, CTRL_TOPIC_INTEREST, 'ctrl should be interest');
    testing.assertEquals(ctrl.topic, topic, 'topic should match');
}

async function testCtrlTopicAcceptRoundTrip() {
    const topic = sha256.hashToB64(stringToUint8Array('ctrl-accept'));
    const frame = encodeControlTopicAccept(topic);
    const decoded = decodeMessage(frame);
    testing.assertEquals(decoded.type, MSG_TYPE_CONTROL, 'type should be control');
    const ctrl = decodeControlPayload(decoded.payload);
    testing.assertEquals(ctrl.ctrl, CTRL_TOPIC_ACCEPT, 'ctrl should be accept');
    testing.assertEquals(ctrl.topic, topic, 'topic should match');
}

async function testCtrlTopicRejectRoundTrip() {
    const topic = sha256.hashToB64(stringToUint8Array('ctrl-reject'));
    const frame = encodeControlTopicReject(topic);
    const decoded = decodeMessage(frame);
    testing.assertEquals(decoded.type, MSG_TYPE_CONTROL, 'type should be control');
    const ctrl = decodeControlPayload(decoded.payload);
    testing.assertEquals(ctrl.ctrl, CTRL_TOPIC_REJECT, 'ctrl should be reject');
    testing.assertEquals(ctrl.topic, topic, 'topic should match');
}

// --- per-swarm authorizer tests ---

async function testAuthorizerWouldAcceptTrue() {
    const pool = new ConnectionPool();
    const topic = sha256.hashToB64(stringToUint8Array('authz-accept'));
    const peer = makeFakePeer('mem://device-1');

    const authorizer: PeerAuthorizer = { authorize: async () => true };

    const swarm = createSwarm({ topic, mode: 'passive', authorizer }, {
        pool,
        discovery: makeStubDiscovery([]),
        authenticator: makeStubAuthenticator(new Map()),
        transports: [],
    });
    swarm.deactivate();

    const result = await swarm.wouldAccept(peer.keyId);
    testing.assertTrue(result, 'wouldAccept should return true when authorized');

    swarm.destroy();
}

async function testAuthorizerWouldAcceptFalseDormant() {
    const pool = new ConnectionPool();
    const topic = sha256.hashToB64(stringToUint8Array('authz-dormant'));
    const peer = makeFakePeer('mem://device-1');

    const swarm = createSwarm({ topic, mode: 'dormant' }, {
        pool,
        discovery: makeStubDiscovery([]),
        authenticator: makeStubAuthenticator(new Map()),
        transports: [],
    });

    const result = await swarm.wouldAccept(peer.keyId);
    testing.assertFalse(result, 'wouldAccept should return false when dormant');

    swarm.destroy();
}

async function testAuthorizerWouldAcceptFalseRejected() {
    const pool = new ConnectionPool();
    const topic = sha256.hashToB64(stringToUint8Array('authz-reject'));
    const peer = makeFakePeer('mem://device-1');

    const authorizer: PeerAuthorizer = { authorize: async () => false };

    const swarm = createSwarm({ topic, mode: 'passive', authorizer }, {
        pool,
        discovery: makeStubDiscovery([]),
        authenticator: makeStubAuthenticator(new Map()),
        transports: [],
    });
    swarm.deactivate();

    const result = await swarm.wouldAccept(peer.keyId);
    testing.assertFalse(result, 'wouldAccept should return false when authorizer rejects');

    swarm.destroy();
}

async function testAuthorizerFiltersOutbound() {
    const pool = new ConnectionPool();
    const topic = sha256.hashToB64(stringToUint8Array('authz-outbound'));

    const allowedPeer = makeFakePeer('mem://allowed');
    const blockedPeer = makeFakePeer('mem://blocked');

    const provider = new MemTransportProvider();
    await provider.listen('mem://allowed', (_t) => {});
    await provider.listen('mem://blocked', (_t) => {});

    const peerMap = new Map<string, PublicKey>();
    peerMap.set(allowedPeer.keyId, allowedPeer.publicKey);
    peerMap.set(blockedPeer.keyId, blockedPeer.publicKey);

    const authorizer: PeerAuthorizer = {
        authorize: async (keyId) => keyId === allowedPeer.keyId,
    };

    const discoveredPeers: PeerInfo[] = [
        { keyId: blockedPeer.keyId, addresses: ['mem://blocked'] },
        { keyId: allowedPeer.keyId, addresses: ['mem://allowed'] },
    ];

    const swarm = createSwarm({ topic, targetPeers: 2, authorizer }, {
        pool,
        discovery: makeStubDiscovery(discoveredPeers),
        authenticator: makeStubAuthenticator(peerMap),
        transports: [provider],
    });

    swarm.activate();
    await new Promise(resolve => setTimeout(resolve, 50));

    testing.assertEquals(swarm.peers().length, 1, 'should only connect to allowed peer');
    testing.assertEquals(swarm.peers()[0].keyId, allowedPeer.keyId, 'connected peer should be the allowed one');

    swarm.destroy();
    provider.close();
}

async function testTargetPeersCapsInbound() {
    const pool = new ConnectionPool();
    const topic = sha256.hashToB64(stringToUint8Array('cap-topic'));

    const swarm = createSwarm({ topic, mode: 'passive', targetPeers: 1 }, {
        pool,
        discovery: makeStubDiscovery([]),
        authenticator: makeStubAuthenticator(new Map()),
        transports: [],
    });
    swarm.deactivate();

    const peer1 = makeFakePeer('mem://p1');
    const peer2 = makeFakePeer('mem://p2');

    const { channel: ch1 } = makeFakeChannel(peer1);
    const { channel: ch2 } = makeFakeChannel(peer2);

    pool.add(ch1, peer1.endpoint);
    pool.add(ch2, peer2.endpoint);

    testing.assertEquals(swarm.peers().length, 1, 'should cap at targetPeers=1');

    swarm.destroy();
}

// --- announce / leave lifecycle tests ---

async function testAnnounceOnActivate() {
    const pool = new ConnectionPool();
    const topic = sha256.hashToB64(stringToUint8Array('announce-topic'));
    const localPeer: PeerInfo = { keyId: 'local-key' as KeyId, addresses: ['mem://local'] };

    let announcedTopic: TopicId | undefined;
    let announcedSelf: PeerInfo | undefined;
    const discovery: PeerDiscovery = {
        async *discover() {},
        async announce(t, s) { announcedTopic = t; announcedSelf = s; },
        async leave() {},
    };

    const swarm = createSwarm({ topic }, {
        pool, discovery,
        authenticator: makeStubAuthenticator(new Map()),
        transports: [],
        localPeer,
    });

    swarm.activate();
    await new Promise(resolve => setTimeout(resolve, 10));

    testing.assertEquals(announcedTopic, topic, 'announce should be called with topic');
    testing.assertEquals(announcedSelf?.keyId, localPeer.keyId, 'announce should be called with localPeer');

    swarm.destroy();
}

async function testLeaveOnSleep() {
    const pool = new ConnectionPool();
    const topic = sha256.hashToB64(stringToUint8Array('leave-sleep-topic'));
    const localPeer: PeerInfo = { keyId: 'local-key' as KeyId, addresses: ['mem://local'] };

    let leftTopic: TopicId | undefined;
    let leftKeyId: KeyId | undefined;
    const discovery: PeerDiscovery = {
        async *discover() {},
        async announce() {},
        async leave(t, k) { leftTopic = t; leftKeyId = k; },
    };

    const swarm = createSwarm({ topic }, {
        pool, discovery,
        authenticator: makeStubAuthenticator(new Map()),
        transports: [],
        localPeer,
    });

    swarm.activate();
    await new Promise(resolve => setTimeout(resolve, 10));
    swarm.sleep();
    await new Promise(resolve => setTimeout(resolve, 10));

    testing.assertEquals(leftTopic, topic, 'leave should be called with topic');
    testing.assertEquals(leftKeyId, localPeer.keyId, 'leave should be called with localPeer keyId');

    swarm.destroy();
}

async function testLeaveOnDestroy() {
    const pool = new ConnectionPool();
    const topic = sha256.hashToB64(stringToUint8Array('leave-destroy-topic'));
    const localPeer: PeerInfo = { keyId: 'local-key' as KeyId, addresses: ['mem://local'] };

    let leftTopic: TopicId | undefined;
    const discovery: PeerDiscovery = {
        async *discover() {},
        async announce() {},
        async leave(t) { leftTopic = t; },
    };

    const swarm = createSwarm({ topic }, {
        pool, discovery,
        authenticator: makeStubAuthenticator(new Map()),
        transports: [],
        localPeer,
    });

    swarm.activate();
    await new Promise(resolve => setTimeout(resolve, 10));
    swarm.destroy();
    await new Promise(resolve => setTimeout(resolve, 10));

    testing.assertEquals(leftTopic, topic, 'leave should be called on destroy');
}

// --- end-to-end listen tests ---

async function testMeshListenEndToEnd() {
    const alice = await makeNoiseKeyPair();
    const bob = await makeNoiseKeyPair();
    const topic = sha256.hashToB64(stringToUint8Array('listen-e2e-topic'));

    const provider = new MemTransportProvider();

    const aliceAuth = createNoiseAuthenticator({
        localKey: { publicKey: alice.publicKey, secretKey: alice.secretKey },
        signingName: SIGNING_ED25519,
        kemPrefs: [KEM_X25519_HKDF],
    });
    const bobAuth = createNoiseAuthenticator({
        localKey: { publicKey: bob.publicKey, secretKey: bob.secretKey },
        signingName: SIGNING_ED25519,
        kemPrefs: [KEM_X25519_HKDF],
    });

    const bobMesh = new Mesh({
        transports: [provider],
        discovery: makeStubDiscovery([]),
        authenticator: bobAuth,
        localKeyId: bob.keyId,
        listenAddresses: ['mem://bob'],
    });
    const bobSwarm = bobMesh.createSwarm(topic, { mode: 'passive' });

    const aliceMesh = new Mesh({
        transports: [provider],
        discovery: makeStubDiscovery([{ keyId: bob.keyId, addresses: ['mem://bob'] }]),
        authenticator: aliceAuth,
        localKeyId: alice.keyId,
    });
    const aliceSwarm = aliceMesh.createSwarm(topic);
    aliceSwarm.activate();

    await new Promise(resolve => setTimeout(resolve, 200));

    testing.assertEquals(aliceSwarm.peers().length, 1, 'alice should have bob as peer');
    testing.assertEquals(aliceSwarm.peers()[0].keyId, bob.keyId, 'alice peer should be bob');
    testing.assertEquals(bobSwarm.peers().length, 1, 'bob should have alice as peer');
    testing.assertEquals(bobSwarm.peers()[0].keyId, alice.keyId, 'bob peer should be alice');

    aliceMesh.close();
    bobMesh.close();
}

async function testMeshListenRejection() {
    const alice = await makeNoiseKeyPair();
    const bob = await makeNoiseKeyPair();
    const topic = sha256.hashToB64(stringToUint8Array('listen-reject-topic'));

    const provider = new MemTransportProvider();

    const aliceAuth = createNoiseAuthenticator({
        localKey: { publicKey: alice.publicKey, secretKey: alice.secretKey },
        signingName: SIGNING_ED25519,
        kemPrefs: [KEM_X25519_HKDF],
    });
    const bobAuth = createNoiseAuthenticator({
        localKey: { publicKey: bob.publicKey, secretKey: bob.secretKey },
        signingName: SIGNING_ED25519,
        kemPrefs: [KEM_X25519_HKDF],
    });

    const rejectAll: PeerAuthorizer = { authorize: async () => false };

    const bobMesh = new Mesh({
        transports: [provider],
        discovery: makeStubDiscovery([]),
        authenticator: bobAuth,
        localKeyId: bob.keyId,
        listenAddresses: ['mem://bob-reject'],
    });
    const bobSwarm = bobMesh.createSwarm(topic, { mode: 'passive', authorizer: rejectAll });

    const aliceMesh = new Mesh({
        transports: [provider],
        discovery: makeStubDiscovery([{ keyId: bob.keyId, addresses: ['mem://bob-reject'] }]),
        authenticator: aliceAuth,
        localKeyId: alice.keyId,
    });
    const aliceSwarm = aliceMesh.createSwarm(topic);
    aliceSwarm.activate();

    await new Promise(resolve => setTimeout(resolve, 200));

    testing.assertEquals(aliceSwarm.peers().length, 0, 'alice should have no peers (rejected)');
    testing.assertEquals(bobSwarm.peers().length, 0, 'bob should have no peers (rejected)');

    aliceMesh.close();
    bobMesh.close();
}

async function testMeshListenTopicData() {
    const alice = await makeNoiseKeyPair();
    const bob = await makeNoiseKeyPair();
    const topic = sha256.hashToB64(stringToUint8Array('listen-data-topic'));

    const provider = new MemTransportProvider();

    const aliceAuth = createNoiseAuthenticator({
        localKey: { publicKey: alice.publicKey, secretKey: alice.secretKey },
        signingName: SIGNING_ED25519,
        kemPrefs: [KEM_X25519_HKDF],
    });
    const bobAuth = createNoiseAuthenticator({
        localKey: { publicKey: bob.publicKey, secretKey: bob.secretKey },
        signingName: SIGNING_ED25519,
        kemPrefs: [KEM_X25519_HKDF],
    });

    const bobMesh = new Mesh({
        transports: [provider],
        discovery: makeStubDiscovery([]),
        authenticator: bobAuth,
        localKeyId: bob.keyId,
        listenAddresses: ['mem://bob-data'],
    });
    const bobSwarm = bobMesh.createSwarm(topic, { mode: 'passive' });

    const aliceMesh = new Mesh({
        transports: [provider],
        discovery: makeStubDiscovery([{ keyId: bob.keyId, addresses: ['mem://bob-data'] }]),
        authenticator: aliceAuth,
        localKeyId: alice.keyId,
    });
    const aliceSwarm = aliceMesh.createSwarm(topic);
    aliceSwarm.activate();

    await new Promise(resolve => setTimeout(resolve, 200));

    testing.assertEquals(aliceSwarm.peers().length, 1, 'alice should have bob');
    testing.assertEquals(bobSwarm.peers().length, 1, 'bob should have alice');

    const received: Uint8Array[] = [];
    bobSwarm.peers()[0].channel.onMessage((msg) => received.push(msg));

    const payload = new TextEncoder().encode('hello via mesh');
    aliceSwarm.peers()[0].channel.send(payload);

    testing.assertEquals(received.length, 1, 'bob should receive topic message');
    testing.assertTrue(bytesEqual(received[0], payload), 'payload should match');

    aliceMesh.close();
    bobMesh.close();
}

// --- pool control dispatch test ---

async function testPoolControlMessageDispatch() {
    const pool = new ConnectionPool();
    const peer = makeFakePeer('mem://device-1');
    const topic = sha256.hashToB64(stringToUint8Array('pool-ctrl-topic'));

    const { channel, remote } = makeFakeChannel(peer);
    pool.add(channel, peer.endpoint);

    const received: { connKey: string; peerId: KeyId; payload: Uint8Array }[] = [];
    pool.onControlMessage((connKey, peerId, _endpoint, payload) => {
        received.push({ connKey, peerId, payload });
    });

    remote.send(encodeControlTopicInterest(topic));

    testing.assertEquals(received.length, 1, 'control callback should fire');
    testing.assertEquals(received[0].peerId, peer.keyId, 'peerId should match');
    const ctrl = decodeControlPayload(received[0].payload);
    testing.assertEquals(ctrl.ctrl, CTRL_TOPIC_INTEREST, 'should be topic_interest');
    testing.assertEquals(ctrl.topic, topic, 'topic should match');

    pool.close();
}

// --- pool reuse discovery test ---

async function testPoolReuseDiscovery() {
    const alice = await makeNoiseKeyPair();
    const bob = await makeNoiseKeyPair();
    const topicA = sha256.hashToB64(stringToUint8Array('reuse-topic-a'));
    const topicB = sha256.hashToB64(stringToUint8Array('reuse-topic-b'));

    const provider = new MemTransportProvider();

    const aliceAuth = createNoiseAuthenticator({
        localKey: { publicKey: alice.publicKey, secretKey: alice.secretKey },
        signingName: SIGNING_ED25519,
        kemPrefs: [KEM_X25519_HKDF],
    });
    const bobAuth = createNoiseAuthenticator({
        localKey: { publicKey: bob.publicKey, secretKey: bob.secretKey },
        signingName: SIGNING_ED25519,
        kemPrefs: [KEM_X25519_HKDF],
    });

    // Bob listens, has swarms for both topics
    const bobMesh = new Mesh({
        transports: [provider],
        discovery: makeStubDiscovery([]),
        authenticator: bobAuth,
        localKeyId: bob.keyId,
        listenAddresses: ['mem://bob-reuse'],
    });
    const bobSwarmA = bobMesh.createSwarm(topicA, { mode: 'passive' });
    const bobSwarmB = bobMesh.createSwarm(topicB, { mode: 'passive' });

    // Alice connects for topic A
    const aliceMesh = new Mesh({
        transports: [provider],
        discovery: makeStubDiscovery([{ keyId: bob.keyId, addresses: ['mem://bob-reuse'] }]),
        authenticator: aliceAuth,
        localKeyId: alice.keyId,
    });
    const aliceSwarmA = aliceMesh.createSwarm(topicA);
    aliceSwarmA.activate();

    await new Promise(resolve => setTimeout(resolve, 200));

    testing.assertEquals(aliceSwarmA.peers().length, 1, 'alice swarmA should have bob');
    testing.assertEquals(aliceMesh.pool.size(), 1, 'alice should have 1 connection');

    // Now alice creates swarmB with PoolReuseDiscovery
    const reuseDiscovery = new PoolReuseDiscovery(aliceMesh.pool);
    const aliceDiscoveryB = new DiscoveryStack([{ source: reuseDiscovery, priority: 0 }]);

    const aliceMeshB = new Mesh({
        transports: [provider],
        discovery: aliceDiscoveryB,
        authenticator: aliceAuth,
        localKeyId: alice.keyId,
    });

    // Manually share the pool: create a swarm on the original mesh with the reuse discovery
    // Actually, for simplicity, let's just test PoolReuseDiscovery directly
    const results: PeerInfo[] = [];
    for await (const peer of reuseDiscovery.discover(topicB)) {
        results.push(peer);
    }

    testing.assertEquals(results.length, 1, 'reuse discovery should yield 1 peer');
    testing.assertEquals(results[0].keyId, bob.keyId, 'yielded peer should be bob');

    // Verify bob's swarmB got the adoption
    testing.assertEquals(bobSwarmB.peers().length, 1, 'bob swarmB should have alice via pool reuse');

    aliceMesh.close();
    aliceMeshB.close();
    bobMesh.close();
}

// --- main ---

const transportTests = {
    title: '[TRANSPORT] In-memory transport',
    tests: [
        { name: '[TRANSPORT_00] Send/receive round-trip', invoke: testMemTransportSendReceive },
        { name: '[TRANSPORT_01] Bidirectional messaging', invoke: testMemTransportBidirectional },
        { name: '[TRANSPORT_02] Close propagation', invoke: testMemTransportClose },
        { name: '[TRANSPORT_03] Provider connect/listen', invoke: testMemTransportProviderConnectListen },
    ],
};

const muxTests = {
    title: '[MUX] Message framing',
    tests: [
        { name: '[MUX_00] Topic encode/decode round-trip', invoke: testMuxTopicEncodeDecode },
        { name: '[MUX_01] Control encode/decode round-trip', invoke: testMuxControlEncodeDecode },
        { name: '[MUX_02] Topic isolation in framing', invoke: testMuxTopicIsolation },
        { name: '[MUX_03] Empty payload', invoke: testMuxEmptyPayload },
    ],
};

const poolTests = {
    title: '[POOL] Connection pool',
    tests: [
        { name: '[POOL_00] Add/get/remove by (keyId, endpoint)', invoke: testPoolAddGetRemove },
        { name: '[POOL_01] Dedup by (keyId, endpoint)', invoke: testPoolDedupByEndpoint },
        { name: '[POOL_02] Multiple devices same keyId', invoke: testPoolMultipleDevicesSameKeyId },
        { name: '[POOL_03] Disconnect cleanup', invoke: testPoolDisconnectCleanup },
        { name: '[POOL_04] Connect/disconnect events', invoke: testPoolEvents },
        { name: '[POOL_05] openTopic send/receive', invoke: testPoolOpenTopicSendReceive },
        { name: '[POOL_06] Topic isolation across channels', invoke: testPoolTopicIsolation },
        { name: '[POOL_07] Topic close does not close connection', invoke: testPoolTopicChannelCloseDoesNotCloseConnection },
        { name: '[POOL_08] openTopic dedup', invoke: testPoolTopicDedup },
        { name: '[POOL_09] Connection close closes topics', invoke: testPoolConnectionCloseClosesTopics },
    ],
};

const swarmTests = {
    title: '[SWARM] Swarm lifecycle and peers',
    tests: [
        { name: '[SWARM_00] Lifecycle mode transitions', invoke: testSwarmLifecycle },
        { name: '[SWARM_01] Peer join via pool', invoke: testSwarmPeerJoinViaPool },
        { name: '[SWARM_02] Peer leave on disconnect', invoke: testSwarmPeerLeaveOnDisconnect },
        { name: '[SWARM_03] Discovery and connect', invoke: testSwarmDiscoveryAndConnect },
        { name: '[SWARM_04] Pool reuse across topics', invoke: testSwarmPoolReuse },
        { name: '[SWARM_05] Dormant ignores pool', invoke: testSwarmDormantIgnoresPool },
    ],
};

const meshTests = {
    title: '[MESH] Mesh facade',
    tests: [
        { name: '[MESH_00] Create mesh and verify pool exists', invoke: testMeshCreateAndPool },
        { name: '[MESH_01] createSwarm returns a working swarm', invoke: testMeshCreateSwarm },
        { name: '[MESH_02] Multiple swarms share the pool', invoke: testMeshMultipleSwarmsSharePool },
        { name: '[MESH_03] Mesh.close() destroys all swarms and closes pool', invoke: testMeshCloseDestroysAll },
        { name: '[MESH_04] swarms() tracks active swarms', invoke: testMeshSwarmsTracking },
    ],
};

const authTests = {
    title: '[AUTH] Noise authenticator',
    tests: [
        { name: '[AUTH_00] Successful handshake produces authenticated channel', invoke: testAuthHandshakeSuccess },
        { name: '[AUTH_01] Encrypted messages round-trip after handshake', invoke: testAuthEncryptedRoundTrip },
        { name: '[AUTH_02] Bidirectional encrypted communication', invoke: testAuthBidirectionalEncryption },
        { name: '[AUTH_03] KEM negotiation picks best common suite', invoke: testAuthKemNegotiation },
        { name: '[AUTH_04] KEM negotiation fails when no common suite', invoke: testAuthKemNegotiationNoCommon },
        { name: '[AUTH_05] ExpectedRemote mismatch rejects handshake', invoke: testAuthExpectedRemoteMismatch },
        { name: '[AUTH_06] Tampered ciphertext is rejected by AEAD', invoke: testAuthTamperedCiphertext },
        { name: '[AUTH_07] Independent sessions derive different keys', invoke: testAuthIndependentSessions },
        { name: '[AUTH_08] TOFU mode completes without expectedRemote', invoke: testAuthTofu },
        { name: '[AUTH_09] Identity protected on expectedRemote mismatch', invoke: testAuthIdentityProtection },
    ],
};

const staticTests = {
    title: '[STATIC] StaticDiscovery',
    tests: [
        { name: '[STATIC_00] Yields peers for a matching topic', invoke: testStaticYieldsMatchingTopic },
        { name: '[STATIC_01] Yields nothing for a non-matching topic', invoke: testStaticYieldsNothingNonMatchingTopic },
        { name: '[STATIC_02] Filters by scheme', invoke: testStaticFiltersByScheme },
        { name: '[STATIC_03] Results are shuffled', invoke: testStaticResultsAreShuffled },
        { name: '[STATIC_04] announce/leave are no-ops', invoke: testStaticAnnounceLeaveNoOps },
        { name: '[STATIC_05] Empty peer list yields nothing', invoke: testStaticEmptyPeerList },
    ],
};

const stackTests = {
    title: '[STACK] DiscoveryStack',
    tests: [
        { name: '[STACK_00] Single layer yields its peers', invoke: testStackSingleLayer },
        { name: '[STACK_01] Stops after reaching targetPeers', invoke: testStackStopsAtTarget },
        { name: '[STACK_02] Falls through to next priority', invoke: testStackFallsThrough },
        { name: '[STACK_03] Same-priority layers run in parallel', invoke: testStackParallelMerge },
        { name: '[STACK_04] Deduplication across layers', invoke: testStackDeduplication },
        { name: '[STACK_05] Returns fewer than targetPeers when exhausted', invoke: testStackFewerThanTarget },
        { name: '[STACK_06] announce/leave broadcast to all layers', invoke: testStackAnnounceLeaveBroadcast },
        { name: '[STACK_07] One failing source does not break others', invoke: testStackAnnounceResilience },
        { name: '[STACK_08] Empty stack yields nothing', invoke: testStackEmpty },
    ],
};

const ctrlTests = {
    title: '[CTRL] Control channel protocol',
    tests: [
        { name: '[CTRL_00] topic_interest encode/decode round-trip', invoke: testCtrlTopicInterestRoundTrip },
        { name: '[CTRL_01] topic_accept encode/decode round-trip', invoke: testCtrlTopicAcceptRoundTrip },
        { name: '[CTRL_02] topic_reject encode/decode round-trip', invoke: testCtrlTopicRejectRoundTrip },
    ],
};

const authZTests = {
    title: '[AUTH_Z] Per-swarm authorizer',
    tests: [
        { name: '[AUTH_Z_00] wouldAccept returns true when authorized', invoke: testAuthorizerWouldAcceptTrue },
        { name: '[AUTH_Z_01] wouldAccept returns false when dormant', invoke: testAuthorizerWouldAcceptFalseDormant },
        { name: '[AUTH_Z_02] wouldAccept returns false when authorizer rejects', invoke: testAuthorizerWouldAcceptFalseRejected },
        { name: '[AUTH_Z_03] authorizer filters outbound discovery candidates', invoke: testAuthorizerFiltersOutbound },
        { name: '[AUTH_Z_04] targetPeers caps inbound adoption', invoke: testTargetPeersCapsInbound },
    ],
};

const announceTests = {
    title: '[ANNOUNCE] Announce/leave lifecycle',
    tests: [
        { name: '[ANNOUNCE_00] activate calls announce', invoke: testAnnounceOnActivate },
        { name: '[ANNOUNCE_01] sleep calls leave', invoke: testLeaveOnSleep },
        { name: '[ANNOUNCE_02] destroy calls leave', invoke: testLeaveOnDestroy },
    ],
};

const listenTests = {
    title: '[LISTEN] Mesh listen + topic negotiation',
    tests: [
        { name: '[LISTEN_00] End-to-end: two meshes, topic negotiation, both sides see peer', invoke: testMeshListenEndToEnd },
        { name: '[LISTEN_01] Rejection: authorizer refuses, no peers on either side', invoke: testMeshListenRejection },
        { name: '[LISTEN_02] Topic data flows after negotiation', invoke: testMeshListenTopicData },
    ],
};

const poolCtrlTests = {
    title: '[POOL_CTRL] Pool control message dispatch',
    tests: [
        { name: '[POOL_CTRL_00] Control messages dispatched to callbacks', invoke: testPoolControlMessageDispatch },
    ],
};

const reuseTests = {
    title: '[REUSE] Pool-reuse discovery',
    tests: [
        { name: '[REUSE_00] Existing connection, new topic via control channel', invoke: testPoolReuseDiscovery },
    ],
};

const allSuites = [
    transportTests, muxTests, poolTests, swarmTests, meshTests, authTests, staticTests, stackTests,
    ctrlTests, authZTests, announceTests, listenTests, poolCtrlTests, reuseTests,
];

async function main() {
    const filters = process.argv.slice(2);
    console.log('Running tests for HHSv3 mesh module' + (filters.length > 0 ? ` (filter: ${filters})` : '') + '\n');

    for (const suite of allSuites) {
        console.log(suite.title);
        for (const test of suite.tests) {
            let match = true;
            for (const filter of filters) {
                match = match && test.name.indexOf(filter) >= 0;
            }
            if (match) {
                const result = await testing.run(test.name, test.invoke);
                if (!result) process.exit(1);
            } else {
                await testing.skip(test.name);
            }
        }
        console.log();
    }
}

main();
