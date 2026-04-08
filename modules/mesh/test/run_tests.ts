import { testing } from '@hyper-hyper-space/hhs3_util';
import type { KeyId, PublicKey } from '@hyper-hyper-space/hhs3_crypto';
import { sha256, stringToUint8Array, keyIdFromPublicKey, ed25519, x25519Hkdf } from '@hyper-hyper-space/hhs3_crypto';

import { createMemTransportPair, MemTransportProvider } from './mem_transport.js';
import { ConnectionPool, connectionKey } from '../src/connection_pool.js';
import type { AuthenticatedChannel } from '../src/authenticator.js';
import type { PeerDiscovery, PeerInfo } from '../src/discovery.js';
import type { TopicId } from '../src/discovery.js';
import type { PeerAuthenticator } from '../src/authenticator.js';
import type { Transport, NetworkAddress } from '../src/transport.js';
import { createSwarm } from '../src/swarm.js';
import { Mesh } from '../src/mesh.js';
import { createNoiseAuthenticator } from '../src/noise_authenticator.js';
import {
    encodeTopicMessage, encodeControlMessage, decodeMessage,
    MSG_TYPE_TOPIC, MSG_TYPE_CONTROL,
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
        async authenticate(transport: Transport, expectedRemote?: KeyId) {
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
        async authenticate(transport: Transport, expectedRemote?: KeyId) {
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
    const pk: PublicKey = { suite: 'ed25519', key: kp.publicKey };
    const keyId = keyIdFromPublicKey(pk, sha256);
    return { publicKey: pk, secretKey: kp.secretKey, keyId };
}

async function testAuthHandshakeSuccess() {
    const alice = await makeNoiseKeyPair();
    const bob = await makeNoiseKeyPair();

    const aliceAuth = createNoiseAuthenticator({
        localKey: { publicKey: alice.publicKey, secretKey: alice.secretKey },
        signingName: 'ed25519',
        kemPrefs: ['x25519-hkdf'],
    });
    const bobAuth = createNoiseAuthenticator({
        localKey: { publicKey: bob.publicKey, secretKey: bob.secretKey },
        signingName: 'ed25519',
        kemPrefs: ['x25519-hkdf'],
    });

    const [tA, tB] = createMemTransportPair();

    const [chanA, chanB] = await Promise.all([
        aliceAuth.authenticate(tA, bob.keyId),
        bobAuth.authenticate(tB),
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
        signingName: 'ed25519',
        kemPrefs: ['x25519-hkdf'],
    });
    const bobAuth = createNoiseAuthenticator({
        localKey: { publicKey: bob.publicKey, secretKey: bob.secretKey },
        signingName: 'ed25519',
        kemPrefs: ['x25519-hkdf'],
    });

    const [tA, tB] = createMemTransportPair();
    const [chanA, chanB] = await Promise.all([
        aliceAuth.authenticate(tA, bob.keyId),
        bobAuth.authenticate(tB),
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
        signingName: 'ed25519',
        kemPrefs: ['x25519-hkdf'],
    });
    const bobAuth = createNoiseAuthenticator({
        localKey: { publicKey: bob.publicKey, secretKey: bob.secretKey },
        signingName: 'ed25519',
        kemPrefs: ['x25519-hkdf'],
    });

    const [tA, tB] = createMemTransportPair();
    const [chanA, chanB] = await Promise.all([
        aliceAuth.authenticate(tA, bob.keyId),
        bobAuth.authenticate(tB),
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
        signingName: 'ed25519',
        kemPrefs: ['x25519-hkdf+ml-kem-768', 'x25519-hkdf'],
    });
    const bobAuth = createNoiseAuthenticator({
        localKey: { publicKey: bob.publicKey, secretKey: bob.secretKey },
        signingName: 'ed25519',
        kemPrefs: ['x25519-hkdf'],
    });

    const [tA, tB] = createMemTransportPair();
    const [chanA, chanB] = await Promise.all([
        aliceAuth.authenticate(tA, bob.keyId),
        bobAuth.authenticate(tB),
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
        signingName: 'ed25519',
        kemPrefs: ['ml-kem-768'],
    });
    const bobAuth = createNoiseAuthenticator({
        localKey: { publicKey: bob.publicKey, secretKey: bob.secretKey },
        signingName: 'ed25519',
        kemPrefs: ['x25519-hkdf'],
    });

    const [tA, tB] = createMemTransportPair();

    let failed = false;
    try {
        await Promise.all([
            aliceAuth.authenticate(tA, bob.keyId),
            bobAuth.authenticate(tB),
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
        signingName: 'ed25519',
        kemPrefs: ['x25519-hkdf'],
    });
    const bobAuth = createNoiseAuthenticator({
        localKey: { publicKey: bob.publicKey, secretKey: bob.secretKey },
        signingName: 'ed25519',
        kemPrefs: ['x25519-hkdf'],
    });

    const [tA, tB] = createMemTransportPair();

    let failed = false;
    try {
        await Promise.all([
            aliceAuth.authenticate(tA, charlie.keyId),
            bobAuth.authenticate(tB),
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
        signingName: 'ed25519',
        kemPrefs: ['x25519-hkdf'],
    });
    const bobAuth = createNoiseAuthenticator({
        localKey: { publicKey: bob.publicKey, secretKey: bob.secretKey },
        signingName: 'ed25519',
        kemPrefs: ['x25519-hkdf'],
    });

    const [tA, tB] = createMemTransportPair();
    const [chanA, chanB] = await Promise.all([
        aliceAuth.authenticate(tA, bob.keyId),
        bobAuth.authenticate(tB),
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
        signingName: 'ed25519',
        kemPrefs: ['x25519-hkdf'],
    });
    const bobAuth = createNoiseAuthenticator({
        localKey: { publicKey: bob.publicKey, secretKey: bob.secretKey },
        signingName: 'ed25519',
        kemPrefs: ['x25519-hkdf'],
    });

    // Session 1
    const [t1A, t1B] = createMemTransportPair();
    const [chan1A, chan1B] = await Promise.all([
        aliceAuth.authenticate(t1A, bob.keyId),
        bobAuth.authenticate(t1B),
    ]);

    // Session 2
    const [t2A, t2B] = createMemTransportPair();
    const [chan2A, chan2B] = await Promise.all([
        aliceAuth.authenticate(t2A, bob.keyId),
        bobAuth.authenticate(t2B),
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
    ],
};

const allSuites = [transportTests, muxTests, poolTests, swarmTests, meshTests, authTests];

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
