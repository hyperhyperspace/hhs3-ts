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
import {
    DEFAULT_INTERNET_TRACKER,
    DEFAULT_INTERNET_TRACKER_KEY,
    DEFAULT_LOCAL_TRACKER,
    resolveTrackerConfig,
} from '../src/tracker_config.js';

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
    onRequest?: (req: TrackerRequest) => void;
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
            opts.onRequest?.(req);
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

    // Query from a distinct identity: a client filters its own advertised
    // addresses out of discovery results (self-dial defense).
    const querier = new TrackerClient({
        trackerAddress: 'mem://tracker',
        transportProvider: provider,
        authenticator: clientAuth,
        localPeer: fakePeerInfo('querier', []),
        heartbeatInterval: 999_999,
    });

    const peers = await collectAll(querier.discover(topic));
    testing.assertEquals(peers.length, 1, 'should find one peer after announce');
    testing.assertEquals(peers[0].keyId, localPeer.keyId, 'keyId should match');

    await client.close();
    await querier.close();
}

async function testDiscoverFiltersSelf() {
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

    const topic = 'topic-self' as TopicId;
    await client.announce(topic, localPeer);

    // The client itself must not discover its own advertised endpoint.
    const selfView = await collectAll(client.discover(topic));
    testing.assertEquals(selfView.length, 0, 'client should filter its own address');

    // A different identity still sees the announced peer, and multi-device
    // (same key, different address) survives the filter.
    const querier = new TrackerClient({
        trackerAddress: 'mem://tracker',
        transportProvider: provider,
        authenticator: clientAuth,
        localPeer: fakePeerInfo('querier', []),
        heartbeatInterval: 999_999,
    });
    const otherView = await collectAll(querier.discover(topic));
    testing.assertEquals(otherView.length, 1, 'a different identity still sees the peer');

    const otherDevice = new TrackerClient({
        trackerAddress: 'mem://tracker',
        transportProvider: provider,
        authenticator: clientAuth,
        localPeer: { keyId: localPeer.keyId, addresses: ['ws://client:9999'] },
        heartbeatInterval: 999_999,
    });
    const deviceView = await collectAll(otherDevice.discover(topic));
    testing.assertEquals(deviceView.length, 1, 'same key at a different address is still a peer');

    await client.close();
    await querier.close();
    await otherDevice.close();
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

    // Query from a distinct identity so neither announced peer is filtered as
    // self.
    const querier = new TrackerClient({
        trackerAddress: 'mem://tracker',
        transportProvider: provider,
        authenticator: clientAuth,
        localPeer: fakePeerInfo('querier', []),
        heartbeatInterval: 999_999,
    });

    // Query with ws scheme only
    const allPeers = await collectAll(querier.discover(topic));
    testing.assertEquals(allPeers.length, 2, 'should find both peers without filter');

    const wsPeers = await collectAll(querier.discover(topic, ['ws']));
    testing.assertEquals(wsPeers.length, 1, 'should find only ws peer');
    testing.assertEquals(wsPeers[0].keyId, peerWs.keyId, 'filtered peer should be ws-peer');

    await client1.close();
    await client2.close();
    await querier.close();
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

    // The client should have updated its internal TTL to 120. We verify
    // indirectly via a separate querier (a client filters its own address).
    const querier = new TrackerClient({
        trackerAddress: 'mem://tracker',
        transportProvider: provider,
        authenticator: clientAuth,
        localPeer: fakePeerInfo('querier', []),
        heartbeatInterval: 999_999,
    });
    const peers = await collectAll(querier.discover(topic));
    testing.assertEquals(peers.length, 1, 'announce should succeed with clamped TTL');

    await client.close();
    await querier.close();
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

async function testConcurrentAnnounceAndDiscoverDoNotOverlap() {
    const tracker = createMockTracker();
    const clientKeyId = fakeKeyId('client');
    const clientPk = fakePk('client');
    const trackerKeyId = fakeKeyId('tracker');
    const trackerPk = fakePk('tracker');

    const clientAuth = makePassthroughAuth(clientKeyId, clientPk, trackerKeyId, trackerPk);
    const serverAuth = makePassthroughAuth(trackerKeyId, trackerPk, clientKeyId, clientPk);

    const inner = makeMockProvider(tracker, serverAuth);
    let inFlight = 0;
    let maxInFlight = 0;
    let connects = 0;
    const provider: TransportProvider = {
        scheme: inner.scheme,
        async listen(addr, onConn) { return inner.listen(addr, onConn); },
        async connect(remote) {
            connects += 1;
            inFlight += 1;
            if (inFlight > maxInFlight) maxInFlight = inFlight;
            try {
                // Hold the slot long enough that a second overlapping connect
                // would bump maxInFlight if the client did not serialize.
                await new Promise((resolve) => setTimeout(resolve, 20));
                return await inner.connect(remote);
            } finally {
                inFlight -= 1;
            }
        },
        close() { inner.close(); },
    };

    const localPeer = fakePeerInfo('client', ['ws://client:1234']);
    const client = new TrackerClient({
        trackerAddress: 'mem://tracker',
        transportProvider: provider,
        authenticator: clientAuth,
        localPeer,
        heartbeatInterval: 999_999,
    });

    const topic = 'topic-serial' as TopicId;
    const [, peers] = await Promise.all([
        client.announce(topic, localPeer),
        collectAll(client.discover(topic)),
    ]);

    testing.assertEquals(connects, 2, 'announce and discover each open a connection');
    testing.assertEquals(maxInFlight, 1, 'exchanges must not overlap');
    testing.assertEquals(peers.length, 0, 'discover filters the client own announce');

    await client.close();
}

async function testExchangeTimesOutOnHungConnect() {
    const hanging: TransportProvider = {
        scheme: 'mem',
        async listen() {},
        connect() {
            return new Promise<Transport>(() => {});
        },
        close() {},
    };

    const localPeer = fakePeerInfo('client', ['ws://client:1234']);
    const client = new TrackerClient({
        trackerAddress: 'mem://tracker',
        transportProvider: hanging,
        authenticator: makePassthroughAuth(
            fakeKeyId('client'), fakePk('client'),
            fakeKeyId('tracker'), fakePk('tracker'),
        ),
        localPeer,
        heartbeatInterval: 999_999,
        timeoutMs: 50,
    });

    const started = Date.now();
    let threw = false;
    try {
        await collectAll(client.discover('topic-hang' as TopicId));
    } catch (e) {
        threw = true;
        testing.assertTrue(
            (e as Error).message.includes('timed out'),
            `discover should mention timeout (got: ${(e as Error).message})`,
        );
    }
    testing.assertTrue(threw, 'discover should reject when connect never completes');
    testing.assertTrue(Date.now() - started < 1000, 'discover should fail well under a second');
    await client.close();
}

async function testCloseTimesOutOnHungLeave() {
    const tracker = createMockTracker();
    const clientKeyId = fakeKeyId('client');
    const clientPk = fakePk('client');
    const trackerKeyId = fakeKeyId('tracker');
    const trackerPk = fakePk('tracker');
    const clientAuth = makePassthroughAuth(clientKeyId, clientPk, trackerKeyId, trackerPk);
    const serverAuth = makePassthroughAuth(trackerKeyId, trackerPk, clientKeyId, clientPk);
    const working = makeMockProvider(tracker, serverAuth);

    let connects = 0;
    const provider: TransportProvider = {
        scheme: 'mem',
        async listen() {},
        connect(remote) {
            connects += 1;
            if (connects === 1) return working.connect(remote);
            return new Promise<Transport>(() => {});
        },
        close() {},
    };

    const localPeer = fakePeerInfo('client', ['ws://client:1234']);
    const client = new TrackerClient({
        trackerAddress: 'mem://tracker',
        transportProvider: provider,
        authenticator: clientAuth,
        localPeer,
        heartbeatInterval: 999_999,
        timeoutMs: 50,
    });

    await client.announce('topic-hang' as TopicId, localPeer);

    const started = Date.now();
    await client.close();
    testing.assertTrue(Date.now() - started < 1000, 'close should return well under a second when leave hangs');
}

async function testCloseAfterSuccessDoesNotUnhandledReject() {
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

    const unhandled: unknown[] = [];
    const onUnhandled = (err: unknown) => { unhandled.push(err); };
    process.on('unhandledRejection', onUnhandled);
    try {
        await client.announce('topic-ok' as TopicId, localPeer);
        await client.close();
        // Let any deferred onClose reject surface.
        await new Promise(resolve => setTimeout(resolve, 20));
        testing.assertEquals(unhandled.length, 0, 'successful exchange must not reject on channel close');
    } finally {
        process.off('unhandledRejection', onUnhandled);
    }
}

async function testCloseDrainsInFlightDiscover() {
    const tracker = createMockTracker();
    const clientKeyId = fakeKeyId('client');
    const clientPk = fakePk('client');
    const trackerKeyId = fakeKeyId('tracker');
    const trackerPk = fakePk('tracker');
    const clientAuth = makePassthroughAuth(clientKeyId, clientPk, trackerKeyId, trackerPk);
    const serverAuth = makePassthroughAuth(trackerKeyId, trackerPk, clientKeyId, clientPk);

    let releaseReply: (() => void) | undefined;
    const gate = new Promise<void>(resolve => { releaseReply = resolve; });
    const provider: TransportProvider = {
        scheme: 'mem',
        async listen() {},
        async connect(_remote: NetworkAddress): Promise<Transport> {
            const [client, server] = createMemTransportPair();
            const serverCh = await serverAuth.authenticate(server, 'responder');
            serverCh.onMessage(async (data) => {
                try {
                    await gate;
                    const req = decodeRequest(data);
                    if (req.type === 'announce') {
                        serverCh.send(encodeMessage({ type: 'announce_ack', ttls: [180] }));
                        return;
                    }
                    if (req.type === 'query') {
                        serverCh.send(encodeMessage({
                            type: 'query_response',
                            results: { [req.topics[0]!]: [] },
                        }));
                        return;
                    }
                    if (req.type === 'leave') {
                        serverCh.send(encodeMessage({ type: 'leave_ack' }));
                    }
                } catch {
                    // close() aborts the socket while the reply is still gated
                }
            });
            return client;
        },
        close() {},
    };

    const localPeer = fakePeerInfo('client', ['ws://client:1234']);
    const client = new TrackerClient({
        trackerAddress: 'mem://tracker',
        transportProvider: provider,
        authenticator: clientAuth,
        localPeer,
        heartbeatInterval: 999_999,
        timeoutMs: 500,
    });

    const unhandled: unknown[] = [];
    const onUnhandled = (err: unknown) => { unhandled.push(err); };
    process.on('unhandledRejection', onUnhandled);
    try {
        const discoverDone = collectAll(client.discover('topic-slow' as TopicId));
        // Abandon the consumer while the exchange is gated (swarm destroy pattern).
        void discoverDone.catch(() => {});
        await new Promise(resolve => setTimeout(resolve, 10));

        const closeStarted = Date.now();
        await client.close();
        testing.assertTrue(
            Date.now() - closeStarted < 1000,
            'close should abort an in-flight query instead of waiting for it',
        );
        await new Promise(resolve => setTimeout(resolve, 20));
        testing.assertEquals(unhandled.length, 0, 'aborted discover must not unhandled-reject');
    } finally {
        process.off('unhandledRejection', onUnhandled);
        releaseReply?.();
    }
}

async function testLeaveAfterCloseDoesNotConnect() {
    const tracker = createMockTracker();
    const clientKeyId = fakeKeyId('client');
    const clientPk = fakePk('client');
    const trackerKeyId = fakeKeyId('tracker');
    const trackerPk = fakePk('tracker');
    const clientAuth = makePassthroughAuth(clientKeyId, clientPk, trackerKeyId, trackerPk);
    const serverAuth = makePassthroughAuth(trackerKeyId, trackerPk, clientKeyId, clientPk);
    const inner = makeMockProvider(tracker, serverAuth);

    let connects = 0;
    const provider: TransportProvider = {
        scheme: inner.scheme,
        async listen(addr, onConn) { return inner.listen(addr, onConn); },
        async connect(remote) {
            connects += 1;
            return inner.connect(remote);
        },
        close() { inner.close(); },
    };

    const localPeer = fakePeerInfo('client', ['ws://client:1234']);
    const client = new TrackerClient({
        trackerAddress: 'mem://tracker',
        transportProvider: provider,
        authenticator: clientAuth,
        localPeer,
        heartbeatInterval: 999_999,
    });

    const topic = 'topic-post-close' as TopicId;
    await client.announce(topic, localPeer);
    const beforeClose = connects;
    await client.close();
    const afterClose = connects;
    await client.leave(topic, localPeer.keyId);
    testing.assertEquals(connects, afterClose, 'leave after close must not open another socket');
    testing.assertTrue(afterClose >= beforeClose, 'close may open one leave exchange');
}

async function testInstanceCarriedOnAnnounceAndLeave() {
    const seen: TrackerRequest[] = [];
    const tracker = createMockTracker({ onRequest: (req) => seen.push(req) });
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
        instanceId: 'proc-a',
    });

    const topic = 'topic-instance' as TopicId;
    await client.announce(topic, localPeer);
    await client.leave(topic, localPeer.keyId);

    const announced = seen.find((req) => req.type === 'announce');
    const left = seen.find((req) => req.type === 'leave');
    testing.assertEquals(announced?.type === 'announce' ? announced.instance : undefined, 'proc-a', 'announce carries the instance');
    testing.assertEquals(left?.type === 'leave' ? left.instance : undefined, 'proc-a', 'leave carries the same instance');

    const other = new TrackerClient({
        trackerAddress: 'mem://tracker',
        transportProvider: provider,
        authenticator: clientAuth,
        localPeer: fakePeerInfo('other', []),
        heartbeatInterval: 999_999,
    });
    const another = new TrackerClient({
        trackerAddress: 'mem://tracker',
        transportProvider: provider,
        authenticator: clientAuth,
        localPeer: fakePeerInfo('another', []),
        heartbeatInterval: 999_999,
    });
    testing.assertTrue(other.instanceId.length > 0, 'a generated instance is non-empty');
    testing.assertTrue(other.instanceId !== another.instanceId, 'two clients get different instances');
    testing.assertEquals(client.instanceId, 'proc-a', 'an explicit instance is kept');

    await client.close();
    await other.close();
    await another.close();
}

async function testConcurrentLeavesAreOneRequest() {
    const clientKeyId = fakeKeyId('client');
    const clientPk = fakePk('client');
    const trackerKeyId = fakeKeyId('tracker');
    const trackerPk = fakePk('tracker');
    const clientAuth = makePassthroughAuth(clientKeyId, clientPk, trackerKeyId, trackerPk);
    const serverAuth = makePassthroughAuth(trackerKeyId, trackerPk, clientKeyId, clientPk);

    const delayMs = 200;
    const seen: TrackerRequest[] = [];
    let connects = 0;
    const provider: TransportProvider = {
        scheme: 'mem',
        async listen() {},
        async connect(_remote: NetworkAddress): Promise<Transport> {
            connects += 1;
            const [client, server] = createMemTransportPair();
            const serverCh = await serverAuth.authenticate(server, 'responder');
            serverCh.onMessage(async (data) => {
                const req = decodeRequest(data);
                if (req.type === 'announce') {
                    serverCh.send(encodeMessage({ type: 'announce_ack', ttls: req.entries.map(() => 180) }));
                    return;
                }
                if (req.type !== 'leave') return;
                seen.push(req);
                await new Promise(resolve => setTimeout(resolve, delayMs));
                serverCh.send(encodeMessage({ type: 'leave_ack' }));
            });
            return client;
        },
        close() {},
    };

    const localPeer = fakePeerInfo('client', ['ws://client:1234']);
    const client = new TrackerClient({
        trackerAddress: 'mem://tracker',
        transportProvider: provider,
        authenticator: clientAuth,
        localPeer,
        heartbeatInterval: 999_999,
        instanceId: 'quit-1',
    });

    const topics = ['topic-a', 'topic-b', 'topic-c', 'topic-d'] as TopicId[];
    for (const topic of topics) await client.announce(topic, localPeer);
    const afterAnnounce = connects;

    const started = Date.now();
    await Promise.all(topics.map(topic => client.leave(topic, localPeer.keyId)));
    await client.close();
    const elapsed = Date.now() - started;

    const leaves = seen.filter(req => req.type === 'leave');
    testing.assertEquals(leaves.length, 1, 'concurrent leaves are one request');
    const leave = leaves[0];
    testing.assertTrue(
        leave !== undefined && topics.every(topic => leave.topics.includes(topic)),
        `the leave lists every topic (got ${leave?.topics.join(', ')})`,
    );
    testing.assertEquals(leave?.instance, 'quit-1', 'coalesced leave carries the instance');
    testing.assertTrue(elapsed < delayMs * 2, `quit should wait for one reply (took ${elapsed}ms)`);
    testing.assertEquals(connects, afterAnnounce + 1, 'close does not open a second leave socket');
}

async function testConcurrentAnnouncesAreOneRequest() {
    const seen: TrackerRequest[] = [];
    const tracker = createMockTracker({ onRequest: (req) => seen.push(req) });
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
        instanceId: 'batch-announce',
    });

    const topics = ['topic-a', 'topic-b', 'topic-c', 'topic-d'] as TopicId[];
    await Promise.all(topics.map((topic) => client.announce(topic, localPeer)));

    const announces = seen.filter((req) => req.type === 'announce');
    testing.assertEquals(announces.length, 1, 'same-turn announces share one request');
    const announced = announces[0];
    testing.assertTrue(
        announced?.type === 'announce' && topics.every((topic) => announced.entries.some((entry) => entry.topic === topic)),
        'the announce lists every topic',
    );
    await client.close();
    const leaves = seen.filter((req) => req.type === 'leave');
    testing.assertEquals(leaves.length, 1, 'close sends one leave for the batch');
    const leave = leaves[0];
    testing.assertTrue(
        leave?.type === 'leave' && topics.every((topic) => leave.topics.includes(topic)),
        'the leave lists every announced topic',
    );
}

async function testConcurrentDiscoversAreOneRequest() {
    const seen: TrackerRequest[] = [];
    const tracker = createMockTracker({ onRequest: (req) => seen.push(req) });
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

    const topics = ['topic-a', 'topic-b', 'topic-c'] as TopicId[];
    await Promise.all(topics.map((topic) => collectAll(client.discover(topic, ['ws']))));
    const queries = seen.filter((req) => req.type === 'query');
    testing.assertEquals(queries.length, 1, 'same-turn discovers share one query');
    const query = queries[0];
    testing.assertTrue(
        query?.type === 'query' && topics.every((topic) => query.topics.includes(topic)),
        'the query lists every topic',
    );
    await client.close();
}

async function testCloseSkipsUnsentAnnounce() {
    const seen: TrackerRequest[] = [];
    const tracker = createMockTracker({ onRequest: (req) => seen.push(req) });
    const clientKeyId = fakeKeyId('client');
    const clientPk = fakePk('client');
    const trackerKeyId = fakeKeyId('tracker');
    const trackerPk = fakePk('tracker');
    const clientAuth = makePassthroughAuth(clientKeyId, clientPk, trackerKeyId, trackerPk);
    const serverAuth = makePassthroughAuth(trackerKeyId, trackerPk, clientKeyId, clientPk);
    const inner = makeMockProvider(tracker, serverAuth);
    let connects = 0;
    const provider: TransportProvider = {
        scheme: inner.scheme,
        async listen(addr, onConn) { return inner.listen(addr, onConn); },
        async connect(remote) {
            connects += 1;
            return inner.connect(remote);
        },
        close() { inner.close(); },
    };
    const localPeer = fakePeerInfo('client', ['ws://client:1234']);
    const client = new TrackerClient({
        trackerAddress: 'mem://tracker',
        transportProvider: provider,
        authenticator: clientAuth,
        localPeer,
        heartbeatInterval: 999_999,
    });

    const announcing = client.announce('topic-unsent' as TopicId, localPeer);
    await client.close();
    await announcing;
    testing.assertEquals(connects, 0, 'close cancels an announce that has not started');
    testing.assertEquals(seen.length, 0, 'no announce and no leave when nothing was registered');
}

async function testCloseDuringInFlightAnnounceStillLeaves() {
    const seen: TrackerRequest[] = [];
    const clientKeyId = fakeKeyId('client');
    const clientPk = fakePk('client');
    const trackerKeyId = fakeKeyId('tracker');
    const trackerPk = fakePk('tracker');
    const clientAuth = makePassthroughAuth(clientKeyId, clientPk, trackerKeyId, trackerPk);
    const serverAuth = makePassthroughAuth(trackerKeyId, trackerPk, clientKeyId, clientPk);

    let releaseReply: (() => void) | undefined;
    const gate = new Promise<void>((resolve) => { releaseReply = resolve; });
    const provider: TransportProvider = {
        scheme: 'mem',
        async listen() {},
        async connect(_remote: NetworkAddress): Promise<Transport> {
            const [client, server] = createMemTransportPair();
            const serverCh = await serverAuth.authenticate(server, 'responder');
            serverCh.onMessage(async (data) => {
                const req = decodeRequest(data);
                seen.push(req);
                if (req.type === 'announce') await gate;
                const res = req.type === 'announce'
                    ? { type: 'announce_ack' as const, ttls: req.entries.map(() => 180) }
                    : req.type === 'leave'
                        ? { type: 'leave_ack' as const }
                        : { type: 'query_response' as const, results: {} };
                try { serverCh.send(encodeMessage(res)); } catch { /* client closed */ }
            });
            return client;
        },
        close() {},
    };

    const localPeer = fakePeerInfo('client', ['ws://client:1234']);
    const client = new TrackerClient({
        trackerAddress: 'mem://tracker',
        transportProvider: provider,
        authenticator: clientAuth,
        localPeer,
        heartbeatInterval: 999_999,
        instanceId: 'inflight',
    });

    const topic = 'topic-inflight' as TopicId;
    const announcing = client.announce(topic, localPeer);
    await new Promise((resolve) => setTimeout(resolve, 20));
    const closing = client.close();
    releaseReply!();
    await announcing;
    await closing;

    const announced = seen.find((req) => req.type === 'announce');
    const left = seen.find((req) => req.type === 'leave');
    testing.assertTrue(announced?.type === 'announce' && announced.entries.some((entry) => entry.topic === topic), 'in-flight announce is allowed to finish');
    testing.assertTrue(left?.type === 'leave' && left.topics.includes(topic), 'close deregisters the topic the announce registered');
    testing.assertEquals(seen.filter((req) => req.type === 'leave').length, 1, 'one leave');
}

async function testResolveTrackerConfig() {
    const local = resolveTrackerConfig('localhost', {});
    testing.assertEquals(local.address, DEFAULT_LOCAL_TRACKER, 'localhost tracker default');
    testing.assertTrue(local.keyId === undefined, 'localhost tracker is TOFU');

    const internet = resolveTrackerConfig('internet', {});
    testing.assertEquals(internet.address, DEFAULT_INTERNET_TRACKER, 'internet tracker default');
    testing.assertEquals(internet.keyId, DEFAULT_INTERNET_TRACKER_KEY, 'internet tracker is pinned');

    const override = resolveTrackerConfig('internet', { tracker: 'wss://other:4610' });
    testing.assertEquals(override.address, 'wss://other:4610', 'flag overrides tracker URL');
    testing.assertTrue(override.keyId === undefined, 'overridden tracker is TOFU unless keyed');

    const envWinsOverDefault = resolveTrackerConfig('localhost', {}, { tracker: 'ws://env:1' });
    testing.assertEquals(envWinsOverDefault.address, 'ws://env:1', 'env overrides scope default');

    const flagWinsOverEnv = resolveTrackerConfig('localhost', { tracker: 'ws://flag:1' }, { tracker: 'ws://env:1' });
    testing.assertEquals(flagWinsOverEnv.address, 'ws://flag:1', 'flag overrides env');
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

const allSuites = [
    {
        title: '[TRACKER_CLIENT] Tracker client',
        tests: [
            { name: '[TRACKER_CLIENT_00] Announce then query returns announced peer', invoke: testAnnounceAndQuery },
            { name: '[TRACKER_CLIENT_00b] Discover filters self, keeps others and multi-device', invoke: testDiscoverFiltersSelf },
            { name: '[TRACKER_CLIENT_01] Leave removes peer from query results', invoke: testLeaveRemovesPeer },
            { name: '[TRACKER_CLIENT_02] Query with scheme filter', invoke: testQueryWithSchemeFilter },
            { name: '[TRACKER_CLIENT_03] TTL clamping by server', invoke: testTtlClamping },
            { name: '[TRACKER_CLIENT_04] Query for unknown topic returns empty list', invoke: testQueryUnknownTopic },
            { name: '[TRACKER_CLIENT_05] Discover respects targetPeers cap', invoke: testDiscoverRespectsTargetPeers },
            { name: '[TRACKER_CLIENT_05b] Concurrent announce and discover do not overlap connects', invoke: testConcurrentAnnounceAndDiscoverDoNotOverlap },
            { name: '[TRACKER_CLIENT_06] Exchange times out on hung connect', invoke: testExchangeTimesOutOnHungConnect },
            { name: '[TRACKER_CLIENT_07] Close times out on hung leave', invoke: testCloseTimesOutOnHungLeave },
            { name: '[TRACKER_CLIENT_07b] Close after success does not unhandled-reject', invoke: testCloseAfterSuccessDoesNotUnhandledReject },
            { name: '[TRACKER_CLIENT_07c] Close aborts in-flight discover without unhandled reject', invoke: testCloseDrainsInFlightDiscover },
            { name: '[TRACKER_CLIENT_07d] Leave after close does not connect', invoke: testLeaveAfterCloseDoesNotConnect },
            { name: '[TRACKER_CLIENT_07e] Concurrent leaves are one request', invoke: testConcurrentLeavesAreOneRequest },
            { name: '[TRACKER_CLIENT_07f] Concurrent announces are one request and close leaves them', invoke: testConcurrentAnnouncesAreOneRequest },
            { name: '[TRACKER_CLIENT_07g] Concurrent discovers are one query', invoke: testConcurrentDiscoversAreOneRequest },
            { name: '[TRACKER_CLIENT_07h] Close skips an announce that has not started', invoke: testCloseSkipsUnsentAnnounce },
            { name: '[TRACKER_CLIENT_07i] Close leaves a topic whose announce is in flight', invoke: testCloseDuringInFlightAnnounceStillLeaves },
            { name: '[TRACKER_CLIENT_08] resolveTrackerConfig scope defaults and overrides', invoke: testResolveTrackerConfig },
            { name: '[TRACKER_CLIENT_09] Announce and leave carry one instance per client', invoke: testInstanceCarriedOnAnnounceAndLeave },
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
                testing.exitIfFailed(await testing.run(test.name, test.invoke));
            } else {
                await testing.skip(test.name);
            }
        }
        console.log();
    }
}

main();
