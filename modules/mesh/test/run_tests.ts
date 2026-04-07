import { testing } from '@hyper-hyper-space/hhs3_util';
import type { KeyId, PublicKey } from '@hyper-hyper-space/hhs3_crypto';
import { sha256, stringToUint8Array, keyIdFromPublicKey } from '@hyper-hyper-space/hhs3_crypto';

import { createMemTransportPair, MemTransportProvider } from './mem_transport.js';
import { ConnectionPool } from '../src/connection_pool.js';
import type { AuthenticatedChannel } from '../src/authenticator.js';
import type { PeerDiscovery } from '../src/discovery.js';
import type { TopicId } from '../src/discovery.js';
import type { PeerResolver } from '../src/resolver.js';
import type { PeerAuthenticator } from '../src/authenticator.js';
import type { Transport } from '../src/transport.js';
import { createSwarm } from '../src/swarm.js';

// --- helpers ---

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) {
        if (a[i] !== b[i]) return false;
    }
    return true;
}

let peerCounter = 0;

function makeFakePeer(): { publicKey: PublicKey; keyId: KeyId } {
    peerCounter++;
    const key = stringToUint8Array(`fake-peer-key-${peerCounter}`);
    const publicKey: PublicKey = { suite: 'test', key };
    const keyId = keyIdFromPublicKey(publicKey, sha256);
    return { publicKey, keyId };
}

function makeFakeChannel(localPeer: { publicKey: PublicKey; keyId: KeyId }): AuthenticatedChannel {
    const [a, _b] = createMemTransportPair();
    return {
        remotePeer: localPeer.publicKey,
        remoteKeyId: localPeer.keyId,
        get open() { return a.open; },
        send: (msg) => a.send(msg),
        close: () => a.close(),
        onMessage: (cb) => a.onMessage(cb),
        onClose: (cb) => a.onClose(cb),
    };
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

// --- connection pool tests ---

async function testPoolAddGetRemove() {
    const pool = new ConnectionPool();
    const peer = makeFakePeer();
    const channel = makeFakeChannel(peer);

    const conn = pool.add(channel);
    testing.assertEquals(conn.peerId, peer.keyId, 'connection peerId should match');
    testing.assertEquals(pool.size(), 1, 'pool size should be 1');

    const got = pool.get(peer.keyId);
    testing.assertTrue(got !== undefined, 'should get connection by keyId');
    testing.assertEquals(got!.peerId, peer.keyId, 'retrieved connection should match');

    pool.remove(peer.keyId);
    testing.assertEquals(pool.size(), 0, 'pool size should be 0 after remove');
}

async function testPoolDedup() {
    const pool = new ConnectionPool();
    const peer = makeFakePeer();
    const ch1 = makeFakeChannel(peer);
    const ch2 = makeFakeChannel(peer);

    pool.add(ch1);
    pool.add(ch2);

    testing.assertEquals(pool.size(), 1, 'pool should deduplicate by KeyId');
    testing.assertFalse(ch2.open, 'duplicate channel should be closed');
}

async function testPoolDisconnectCleanup() {
    const pool = new ConnectionPool();
    const peer = makeFakePeer();
    const channel = makeFakeChannel(peer);

    pool.add(channel);
    testing.assertEquals(pool.size(), 1, 'pool has one connection');

    channel.close();
    testing.assertEquals(pool.size(), 0, 'pool should remove disconnected channel');
}

async function testPoolEvents() {
    const pool = new ConnectionPool();
    const peer = makeFakePeer();

    const connected: KeyId[] = [];
    const disconnected: KeyId[] = [];

    pool.onConnect((conn) => connected.push(conn.peerId));
    pool.onDisconnect((id) => disconnected.push(id));

    const channel = makeFakeChannel(peer);
    pool.add(channel);
    testing.assertEquals(connected.length, 1, 'onConnect should fire');
    testing.assertEquals(connected[0], peer.keyId, 'onConnect peerId should match');

    channel.close();
    testing.assertEquals(disconnected.length, 1, 'onDisconnect should fire');
    testing.assertEquals(disconnected[0], peer.keyId, 'onDisconnect peerId should match');
}

async function testPoolQueryInterest() {
    const pool = new ConnectionPool();
    const peer1 = makeFakePeer();
    const peer2 = makeFakePeer();
    const topic = sha256.hash(stringToUint8Array('test-topic'));

    pool.add(makeFakeChannel(peer1));
    pool.add(makeFakeChannel(peer2));

    pool.setInterestQuery(async (peerId, _topic) => peerId === peer1.keyId);

    const interested = await pool.queryInterest(topic);
    testing.assertEquals(interested.length, 1, 'only one peer should be interested');
    testing.assertEquals(interested[0], peer1.keyId, 'interested peer should match');
}

// --- swarm tests ---

function makeStubDiscovery(peers: KeyId[]): PeerDiscovery {
    return {
        async *discover(_topic: TopicId) {
            for (const p of peers) yield p;
        },
        async announce() {},
        async leave() {},
    };
}

function makeStubResolver(addressMap: Map<KeyId, string[]>): PeerResolver {
    return {
        async resolve(peer: KeyId, schemes?: string[]) {
            const addrs = addressMap.get(peer) ?? [];
            if (schemes === undefined) return addrs;
            return addrs.filter(a => schemes.some(s => a.startsWith(s + '://')));
        },
        async *resolveAny(peers: KeyId[], schemes?: string[]) {
            for (const peer of peers) {
                const addrs = addressMap.get(peer) ?? [];
                const filtered = schemes
                    ? addrs.filter(a => schemes.some(s => a.startsWith(s + '://')))
                    : addrs;
                if (filtered.length > 0) {
                    yield { peer, addresses: filtered };
                }
            }
        },
        async publish() {},
        async unpublish() {},
    };
}

function makeStubAuthenticator(peerMap: Map<KeyId, PublicKey>): PeerAuthenticator {
    return {
        async authenticate(transport: Transport, _localKey, expectedRemote?: KeyId) {
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
    const topic = sha256.hash(stringToUint8Array('swarm-topic'));

    const swarm = createSwarm({ topic, mode: 'dormant' }, {
        pool,
        discovery: makeStubDiscovery([]),
        resolver: makeStubResolver(new Map()),
        authenticator: makeStubAuthenticator(new Map()),
        transports: [],
        localKey: { publicKey: { suite: 'test', key: new Uint8Array(0) }, secretKey: new Uint8Array(0) },
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
    const topic = sha256.hash(stringToUint8Array('swarm-pool-topic'));
    const peer = makeFakePeer();

    const swarm = createSwarm({ topic, mode: 'passive' }, {
        pool,
        discovery: makeStubDiscovery([]),
        resolver: makeStubResolver(new Map()),
        authenticator: makeStubAuthenticator(new Map()),
        transports: [],
        localKey: { publicKey: { suite: 'test', key: new Uint8Array(0) }, secretKey: new Uint8Array(0) },
    });

    swarm.deactivate();

    const joined: KeyId[] = [];
    swarm.onPeerJoin((id) => joined.push(id));

    pool.add(makeFakeChannel(peer));

    testing.assertEquals(joined.length, 1, 'passive swarm should adopt pool peers');
    testing.assertEquals(joined[0], peer.keyId, 'joined peerId should match');
    testing.assertEquals(swarm.peers().length, 1, 'swarm should have one peer');

    swarm.destroy();
}

async function testSwarmPeerLeaveOnDisconnect() {
    const pool = new ConnectionPool();
    const topic = sha256.hash(stringToUint8Array('swarm-leave-topic'));
    const peer = makeFakePeer();

    const swarm = createSwarm({ topic, mode: 'passive' }, {
        pool,
        discovery: makeStubDiscovery([]),
        resolver: makeStubResolver(new Map()),
        authenticator: makeStubAuthenticator(new Map()),
        transports: [],
        localKey: { publicKey: { suite: 'test', key: new Uint8Array(0) }, secretKey: new Uint8Array(0) },
    });

    swarm.deactivate();

    const left: KeyId[] = [];
    swarm.onPeerLeave((id) => left.push(id));

    const channel = makeFakeChannel(peer);
    pool.add(channel);
    testing.assertEquals(swarm.peers().length, 1, 'should have peer');

    channel.close();
    testing.assertEquals(left.length, 1, 'should fire onPeerLeave');
    testing.assertEquals(swarm.peers().length, 0, 'should have no peers');

    swarm.destroy();
}

async function testSwarmDiscoveryAndConnect() {
    const pool = new ConnectionPool();
    const topic = sha256.hash(stringToUint8Array('swarm-discovery-topic'));

    const peer1 = makeFakePeer();
    const peer2 = makeFakePeer();

    const provider = new MemTransportProvider();
    await provider.listen('mem://peer1', (_t) => {});
    await provider.listen('mem://peer2', (_t) => {});

    const addressMap = new Map<KeyId, string[]>();
    addressMap.set(peer1.keyId, ['mem://peer1']);
    addressMap.set(peer2.keyId, ['mem://peer2']);

    const peerMap = new Map<KeyId, PublicKey>();
    peerMap.set(peer1.keyId, peer1.publicKey);
    peerMap.set(peer2.keyId, peer2.publicKey);

    const swarm = createSwarm({ topic, targetPeers: 2 }, {
        pool,
        discovery: makeStubDiscovery([peer1.keyId, peer2.keyId]),
        resolver: makeStubResolver(addressMap),
        authenticator: makeStubAuthenticator(peerMap),
        transports: [provider],
        localKey: { publicKey: { suite: 'test', key: new Uint8Array(0) }, secretKey: new Uint8Array(0) },
    });

    swarm.activate();

    // Give the async discovery loop a tick to complete
    await new Promise(resolve => setTimeout(resolve, 50));

    testing.assertEquals(swarm.peers().length, 2, 'swarm should discover and connect to 2 peers');
    testing.assertEquals(pool.size(), 2, 'pool should have 2 connections');

    swarm.destroy();
    provider.close();
}

async function testSwarmPoolReuse() {
    const pool = new ConnectionPool();
    const topic1 = sha256.hash(stringToUint8Array('topic-1'));
    const topic2 = sha256.hash(stringToUint8Array('topic-2'));
    const peer = makeFakePeer();

    const deps = {
        pool,
        discovery: makeStubDiscovery([]),
        resolver: makeStubResolver(new Map()),
        authenticator: makeStubAuthenticator(new Map()),
        transports: [],
        localKey: { publicKey: { suite: 'test', key: new Uint8Array(0) }, secretKey: new Uint8Array(0) },
    };

    const swarm1 = createSwarm({ topic: topic1 }, deps);
    const swarm2 = createSwarm({ topic: topic2 }, deps);

    swarm1.deactivate();
    swarm2.deactivate();

    const join1: KeyId[] = [];
    const join2: KeyId[] = [];
    swarm1.onPeerJoin((id) => join1.push(id));
    swarm2.onPeerJoin((id) => join2.push(id));

    pool.add(makeFakeChannel(peer));

    testing.assertEquals(join1.length, 1, 'swarm1 should see peer from pool');
    testing.assertEquals(join2.length, 1, 'swarm2 should see same peer from pool');

    swarm1.destroy();
    swarm2.destroy();
}

async function testSwarmDormantIgnoresPool() {
    const pool = new ConnectionPool();
    const topic = sha256.hash(stringToUint8Array('dormant-topic'));
    const peer = makeFakePeer();

    const swarm = createSwarm({ topic, mode: 'dormant' }, {
        pool,
        discovery: makeStubDiscovery([]),
        resolver: makeStubResolver(new Map()),
        authenticator: makeStubAuthenticator(new Map()),
        transports: [],
        localKey: { publicKey: { suite: 'test', key: new Uint8Array(0) }, secretKey: new Uint8Array(0) },
    });

    pool.add(makeFakeChannel(peer));

    testing.assertEquals(swarm.peers().length, 0, 'dormant swarm should not adopt pool peers');

    swarm.destroy();
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

const poolTests = {
    title: '[POOL] Connection pool',
    tests: [
        { name: '[POOL_00] Add/get/remove', invoke: testPoolAddGetRemove },
        { name: '[POOL_01] Deduplication', invoke: testPoolDedup },
        { name: '[POOL_02] Disconnect cleanup', invoke: testPoolDisconnectCleanup },
        { name: '[POOL_03] Connect/disconnect events', invoke: testPoolEvents },
        { name: '[POOL_04] Query interest', invoke: testPoolQueryInterest },
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

const allSuites = [transportTests, poolTests, swarmTests];

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
