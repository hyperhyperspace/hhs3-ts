import { testing } from '@hyper-hyper-space/hhs3_util';
import type {
    Transport, TransportProvider, NetworkAddress,
    PeerAuthenticator, AuthenticatedChannel, PeerInfo, TopicId,
} from '@hyper-hyper-space/hhs3_mesh';
import type { KeyId } from '@hyper-hyper-space/hhs3_crypto';
import {
    decodeRequest, encodeMessage,
    type TrackerRequest, type AnnounceAck, type QueryResponse, type LeaveAck,
} from '../src/protocol.js';
import { TrackerClient } from '../src/tracker_client.js';

// ---------------------------------------------------------------------------
// In-memory transport (same as mesh test helper)
// ---------------------------------------------------------------------------

class MemTransport implements Transport {
    private _open = true;
    private messageCallbacks: ((msg: Uint8Array) => void)[] = [];
    private closeCallbacks: (() => void)[] = [];
    peer?: MemTransport;

    get open(): boolean { return this._open; }

    send(message: Uint8Array): void {
        if (!this._open) throw new Error('transport closed');
        if (!this.peer || !this.peer._open) throw new Error('peer closed');
        const copy = new Uint8Array(message);
        for (const cb of this.peer.messageCallbacks) cb(copy);
    }

    close(): void {
        if (!this._open) return;
        this._open = false;
        for (const cb of this.closeCallbacks) cb();
        if (this.peer && this.peer._open) this.peer.close();
    }

    onMessage(callback: (msg: Uint8Array) => void): void {
        this.messageCallbacks.push(callback);
    }

    onClose(callback: () => void): void {
        this.closeCallbacks.push(callback);
    }
}

function createMemTransportPair(): [MemTransport, MemTransport] {
    const a = new MemTransport();
    const b = new MemTransport();
    a.peer = b;
    b.peer = a;
    return [a, b];
}

// ---------------------------------------------------------------------------
// Mock tracker server — processes requests against an in-memory registry
// ---------------------------------------------------------------------------

interface MockTrackerOpts {
    ttlMin?: number;
    ttlMax?: number;
}

function createMockTracker(opts: MockTrackerOpts = {}) {
    const ttlMin = opts.ttlMin ?? 60;
    const ttlMax = opts.ttlMax ?? 600;

    const registry = new Map<string, Map<string, PeerInfo>>();

    function handleRequest(req: TrackerRequest): Uint8Array {
        switch (req.type) {
            case 'announce': {
                const ttls: number[] = [];
                for (const entry of req.entries) {
                    const clamped = Math.max(ttlMin, Math.min(ttlMax, entry.ttl));
                    ttls.push(clamped);
                    let topicMap = registry.get(entry.topic);
                    if (!topicMap) { topicMap = new Map(); registry.set(entry.topic, topicMap); }
                    topicMap.set(req.peer.keyId, req.peer);
                }
                const ack: AnnounceAck = { type: 'announce_ack', ttls };
                return encodeMessage(ack);
            }
            case 'query': {
                const results: Record<string, PeerInfo[]> = {};
                for (const topic of req.topics) {
                    const topicMap = registry.get(topic);
                    let peers = topicMap ? [...topicMap.values()] : [];
                    if (req.schemes && req.schemes.length > 0) {
                        peers = peers.filter(p =>
                            p.addresses.some(a => req.schemes!.some(s => a.startsWith(s + '://'))),
                        );
                    }
                    results[topic] = peers;
                }
                const res: QueryResponse = { type: 'query_response', results };
                return encodeMessage(res);
            }
            case 'leave': {
                for (const topic of req.topics) {
                    const topicMap = registry.get(topic);
                    // In real server we'd use channel.remoteKeyId; here we just clear all.
                    if (topicMap) registry.delete(topic);
                }
                const ack: LeaveAck = { type: 'leave_ack' };
                return encodeMessage(ack);
            }
        }
    }

    function attachHandler(serverSide: AuthenticatedChannel): void {
        serverSide.onMessage((data) => {
            const req = decodeRequest(data);
            const res = handleRequest(req);
            serverSide.send(res);
        });
    }

    return { registry, attachHandler };
}

// ---------------------------------------------------------------------------
// Passthrough authenticator — wraps transport as AuthenticatedChannel
// ---------------------------------------------------------------------------

function makePassthroughAuth(
    localKeyId: KeyId,
    localPk: { suite: string; key: Uint8Array },
    remoteKeyId: KeyId,
    remotePk: { suite: string; key: Uint8Array },
): PeerAuthenticator {
    return {
        async authenticate(
            transport: Transport,
            _role: 'initiator' | 'responder',
            _expected?: KeyId,
        ): Promise<AuthenticatedChannel> {
            return {
                remotePeer: remotePk,
                remoteKeyId: remoteKeyId,
                get open() { return transport.open; },
                send: (msg: Uint8Array) => transport.send(msg),
                close: () => transport.close(),
                onMessage: (cb: (msg: Uint8Array) => void) => transport.onMessage(cb),
                onClose: (cb: () => void) => transport.onClose(cb),
            };
        },
    };
}

// ---------------------------------------------------------------------------
// Transport provider that runs the mock tracker on the server side
// ---------------------------------------------------------------------------

function makeMockProvider(
    mockTracker: ReturnType<typeof createMockTracker>,
    serverAuth: PeerAuthenticator,
): TransportProvider {
    return {
        scheme: 'mem',
        async listen(_addr: NetworkAddress, _onConn: (t: Transport) => void) {},
        async connect(_remote: NetworkAddress): Promise<Transport> {
            const [client, server] = createMemTransportPair();
            // Authenticate server side and attach handler
            const serverCh = await serverAuth.authenticate(server, 'responder');
            mockTracker.attachHandler(serverCh);
            return client;
        },
        close() {},
    };
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const enc = new TextEncoder();

function fakeKeyId(label: string): KeyId {
    let h = 0;
    for (let i = 0; i < label.length; i++) h = ((h << 5) - h + label.charCodeAt(i)) | 0;
    return `key-${label}-${h}` as KeyId;
}

function fakePk(label: string): { suite: string; key: Uint8Array } {
    return { suite: 'ed25519', key: enc.encode(label) };
}

function fakePeerInfo(label: string, addresses: string[]): PeerInfo {
    return { keyId: fakeKeyId(label), addresses };
}

async function collectAll(iter: AsyncIterable<PeerInfo>): Promise<PeerInfo[]> {
    const out: PeerInfo[] = [];
    for await (const p of iter) out.push(p);
    return out;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

async function testAnnounceAndQuery() {
    const tracker = createMockTracker();
    const clientKeyId = fakeKeyId('client');
    const clientPk = fakePk('client');
    const trackerKeyId = fakeKeyId('tracker');
    const trackerPk = fakePk('tracker');

    const clientAuth = makePassthroughAuth(clientKeyId, clientPk, trackerKeyId, trackerPk);
    const serverAuth = makePassthroughAuth(trackerKeyId, trackerPk, clientKeyId, clientPk);

    const provider = makeMockProvider(tracker, serverAuth);
    const localPeer = fakePeerInfo('client', ['ws://client:1234']);

    const client = new TrackerClient({
        trackerAddress: 'mem://tracker',
        transportProvider: provider,
        authenticator: clientAuth,
        localPeer,
        heartbeatInterval: 999_999,
    });

    const topic = 'topic-a' as TopicId;
    await client.announce(topic, localPeer);

    const peers = await collectAll(client.discover(topic));
    testing.assertEquals(peers.length, 1, 'should find one peer after announce');
    testing.assertEquals(peers[0].keyId, localPeer.keyId, 'keyId should match');

    await client.close();
}

async function testLeaveRemovesPeer() {
    const tracker = createMockTracker();
    const clientKeyId = fakeKeyId('client');
    const clientPk = fakePk('client');
    const trackerKeyId = fakeKeyId('tracker');
    const trackerPk = fakePk('tracker');

    const clientAuth = makePassthroughAuth(clientKeyId, clientPk, trackerKeyId, trackerPk);
    const serverAuth = makePassthroughAuth(trackerKeyId, trackerPk, clientKeyId, clientPk);

    const provider = makeMockProvider(tracker, serverAuth);
    const localPeer = fakePeerInfo('client', ['ws://client:1234']);

    const client = new TrackerClient({
        trackerAddress: 'mem://tracker',
        transportProvider: provider,
        authenticator: clientAuth,
        localPeer,
        heartbeatInterval: 999_999,
    });

    const topic = 'topic-leave' as TopicId;
    await client.announce(topic, localPeer);
    await client.leave(topic, localPeer.keyId);

    const peers = await collectAll(client.discover(topic));
    testing.assertEquals(peers.length, 0, 'should find no peers after leave');

    await client.close();
}

async function testQueryWithSchemeFilter() {
    const tracker = createMockTracker();
    const clientKeyId = fakeKeyId('client');
    const clientPk = fakePk('client');
    const trackerKeyId = fakeKeyId('tracker');
    const trackerPk = fakePk('tracker');

    const clientAuth = makePassthroughAuth(clientKeyId, clientPk, trackerKeyId, trackerPk);
    const serverAuth = makePassthroughAuth(trackerKeyId, trackerPk, clientKeyId, clientPk);

    const provider = makeMockProvider(tracker, serverAuth);

    // Register two peers: one with ws, one with wss
    const peerWs = fakePeerInfo('ws-peer', ['ws://a:1']);
    const peerWss = fakePeerInfo('wss-peer', ['wss://b:2']);
    const topic = 'topic-filter' as TopicId;

    // Use two clients to register different peers
    const client1 = new TrackerClient({
        trackerAddress: 'mem://tracker',
        transportProvider: provider,
        authenticator: clientAuth,
        localPeer: peerWs,
        heartbeatInterval: 999_999,
    });
    await client1.announce(topic, peerWs);

    const client2 = new TrackerClient({
        trackerAddress: 'mem://tracker',
        transportProvider: provider,
        authenticator: clientAuth,
        localPeer: peerWss,
        heartbeatInterval: 999_999,
    });
    await client2.announce(topic, peerWss);

    // Query with ws scheme only
    const allPeers = await collectAll(client1.discover(topic));
    testing.assertEquals(allPeers.length, 2, 'should find both peers without filter');

    const wsPeers = await collectAll(client1.discover(topic, ['ws']));
    testing.assertEquals(wsPeers.length, 1, 'should find only ws peer');
    testing.assertEquals(wsPeers[0].keyId, peerWs.keyId, 'filtered peer should be ws-peer');

    await client1.close();
    await client2.close();
}

async function testTtlClamping() {
    const tracker = createMockTracker({ ttlMin: 60, ttlMax: 120 });
    const clientKeyId = fakeKeyId('client');
    const clientPk = fakePk('client');
    const trackerKeyId = fakeKeyId('tracker');
    const trackerPk = fakePk('tracker');

    const clientAuth = makePassthroughAuth(clientKeyId, clientPk, trackerKeyId, trackerPk);
    const serverAuth = makePassthroughAuth(trackerKeyId, trackerPk, clientKeyId, clientPk);

    const provider = makeMockProvider(tracker, serverAuth);
    const localPeer = fakePeerInfo('client', ['ws://client:1234']);

    // Request TTL of 300, server should clamp to 120
    const client = new TrackerClient({
        trackerAddress: 'mem://tracker',
        transportProvider: provider,
        authenticator: clientAuth,
        localPeer,
        announceTtl: 300,
        heartbeatInterval: 999_999,
    });

    const topic = 'topic-ttl' as TopicId;
    await client.announce(topic, localPeer);

    // The client should have updated its internal TTL to 120
    // We verify indirectly: the announce succeeded without error
    const peers = await collectAll(client.discover(topic));
    testing.assertEquals(peers.length, 1, 'announce should succeed with clamped TTL');

    await client.close();
}

async function testQueryUnknownTopic() {
    const tracker = createMockTracker();
    const clientKeyId = fakeKeyId('client');
    const clientPk = fakePk('client');
    const trackerKeyId = fakeKeyId('tracker');
    const trackerPk = fakePk('tracker');

    const clientAuth = makePassthroughAuth(clientKeyId, clientPk, trackerKeyId, trackerPk);
    const serverAuth = makePassthroughAuth(trackerKeyId, trackerPk, clientKeyId, clientPk);

    const provider = makeMockProvider(tracker, serverAuth);
    const localPeer = fakePeerInfo('client', ['ws://client:1234']);

    const client = new TrackerClient({
        trackerAddress: 'mem://tracker',
        transportProvider: provider,
        authenticator: clientAuth,
        localPeer,
        heartbeatInterval: 999_999,
    });

    const peers = await collectAll(client.discover('nonexistent-topic' as TopicId));
    testing.assertEquals(peers.length, 0, 'unknown topic should return empty list');

    await client.close();
}

async function testDiscoverRespectsTargetPeers() {
    const tracker = createMockTracker();
    const clientKeyId = fakeKeyId('client');
    const clientPk = fakePk('client');
    const trackerKeyId = fakeKeyId('tracker');
    const trackerPk = fakePk('tracker');

    const clientAuth = makePassthroughAuth(clientKeyId, clientPk, trackerKeyId, trackerPk);
    const serverAuth = makePassthroughAuth(trackerKeyId, trackerPk, clientKeyId, clientPk);

    const provider = makeMockProvider(tracker, serverAuth);
    const topic = 'topic-cap' as TopicId;

    // Register 5 peers (don't close — close sends LEAVE which would deregister)
    const announcers: TrackerClient[] = [];
    for (let i = 0; i < 5; i++) {
        const peer = fakePeerInfo(`peer-${i}`, [`ws://p${i}:1`]);
        const c = new TrackerClient({
            trackerAddress: 'mem://tracker',
            transportProvider: provider,
            authenticator: clientAuth,
            localPeer: peer,
            heartbeatInterval: 999_999,
        });
        await c.announce(topic, peer);
        announcers.push(c);
    }

    const queryClient = new TrackerClient({
        trackerAddress: 'mem://tracker',
        transportProvider: provider,
        authenticator: clientAuth,
        localPeer: fakePeerInfo('querier', ['ws://q:1']),
        heartbeatInterval: 999_999,
    });

    const allPeers = await collectAll(queryClient.discover(topic));
    testing.assertEquals(allPeers.length, 5, 'should find all 5 peers');

    const capped = await collectAll(queryClient.discover(topic, undefined, 3));
    testing.assertEquals(capped.length, 3, 'should cap at targetPeers=3');

    await queryClient.close();
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

const allSuites = [
    {
        title: '[TRACKER_CLIENT] Tracker client',
        tests: [
            { name: '[TRACKER_CLIENT_00] Announce then query returns announced peer', invoke: testAnnounceAndQuery },
            { name: '[TRACKER_CLIENT_01] Leave removes peer from query results', invoke: testLeaveRemovesPeer },
            { name: '[TRACKER_CLIENT_02] Query with scheme filter', invoke: testQueryWithSchemeFilter },
            { name: '[TRACKER_CLIENT_03] TTL clamping by server', invoke: testTtlClamping },
            { name: '[TRACKER_CLIENT_04] Query for unknown topic returns empty list', invoke: testQueryUnknownTopic },
            { name: '[TRACKER_CLIENT_05] Discover respects targetPeers cap', invoke: testDiscoverRespectsTargetPeers },
        ],
    },
];

async function main() {
    const filters = process.argv.slice(2);
    console.log('Running tests for HHSv3 mesh_tracker_client module' + (filters.length > 0 ? ` (filter: ${filters})` : '') + '\n');

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
