import { testing } from '@hyper-hyper-space/hhs3_util';
import type { IssueReport } from '@hyper-hyper-space/hhs3_util';
import { B64Hash, sha256, stringToUint8Array } from '@hyper-hyper-space/hhs3_crypto';
import { dag, Position, Header, Entry } from '@hyper-hyper-space/hhs3_dag';
import { json } from '@hyper-hyper-space/hhs3_json';
import type { TopicChannel } from '@hyper-hyper-space/hhs3_mesh';
import type { RObject, Version, Payload, View, ForeignDep, Delta, DeltaAccumulator, RContext } from '@hyper-hyper-space/hhs3_mvt';
import { RootScopedDag, ScopedDagSubscription, validationOk, validationFailure } from '@hyper-hyper-space/hhs3_mvt';

import { createDagProvider } from '../src/provider.js';
import { createDagSynchronizer, REJECTED_ENTRIES_CAP } from '../src/synchronizer.js';
import { createSyncSession } from '../src/session.js';
import { encode, decode } from '../src/codec.js';
import { fetchInit } from '../src/fetch_init.js';
import type {
    SyncMsg, NewFrontierMsg, InitResponse, HeaderRequest,
    HeaderResponseMeta, HeaderBatch, PayloadRequest, PayloadResponseMeta, PayloadMsg,
} from '../src/protocol.js';
import type { Swarm, SwarmPeer } from '@hyper-hyper-space/hhs3_mesh';

// --- Helpers ---

function createTestDag(): dag.Dag {
    const store = new dag.store.MemDagStorage();
    const index = dag.idx.flat.createFlatIndex(
        store,
        new dag.idx.flat.mem.MemFlatIndexStore()
    );
    return dag.create(store, index, sha256);
}

function wait(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function waitUntil(predicate: () => Promise<boolean>, intervalMs = 20, timeoutMs = 5000): Promise<void> {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
        if (await predicate()) return;
        await wait(intervalMs);
    }
    throw new Error(`waitUntil timed out after ${timeoutMs}ms`);
}

type ChannelPair = { local: MockChannel; remote: MockChannel };

class MockChannel implements TopicChannel {
    readonly topic: string;
    readonly peerId: string;
    readonly endpoint: string;
    private _open = true;
    private msgCbs: Array<(msg: Uint8Array) => void> = [];
    private closeCbs: Array<() => void> = [];
    peer?: MockChannel;

    constructor(topic: string, peerId: string, endpoint: string) {
        this.topic = topic;
        this.peerId = peerId;
        this.endpoint = endpoint;
    }

    get open() { return this._open; }

    send(message: Uint8Array): void {
        if (!this._open) throw new Error('channel closed');
        if (!this.peer || !this.peer._open) throw new Error('peer closed');
        const copy = new Uint8Array(message);
        // Async delivery to avoid stack overflow in tight loops
        setTimeout(() => {
            for (const cb of this.peer!.msgCbs) cb(copy);
        }, 0);
    }

    onMessage(callback: (message: Uint8Array) => void): void {
        this.msgCbs.push(callback);
    }

    close(): void {
        if (!this._open) return;
        this._open = false;
        for (const cb of this.closeCbs) cb();
        if (this.peer && this.peer._open) {
            this.peer.close();
        }
    }

    onClose(callback: () => void): void {
        this.closeCbs.push(callback);
    }
}

function fifoOnMessage(ch: MockChannel, handler: (data: Uint8Array) => void | Promise<void>): void {
    let chain: Promise<void> = Promise.resolve();
    ch.onMessage((data) => {
        chain = chain.then(() => handler(data)).catch(() => {});
    });
}

function wrapLoadHeader(d: dag.Dag, delayMs: number): void {
    const orig = d.loadHeader.bind(d);
    d.loadHeader = async (h: B64Hash) => {
        await wait(delayMs);
        return orig(h);
    };
}

function createChannelPair(topic: string, peerAId: string, peerBId: string): ChannelPair {
    const local = new MockChannel(topic, peerBId, `mem://${peerBId}`);
    const remote = new MockChannel(topic, peerAId, `mem://${peerAId}`);
    local.peer = remote;
    remote.peer = local;
    return { local, remote };
}

function createMockRObject(d: dag.Dag, id: B64Hash, opts?: {
    extractForeignDeps?: (payload: Payload, at: Version) => ForeignDep[] | undefined,
    validatePayload?: (payload: Payload, at: Version) => Promise<ReturnType<typeof validationOk>>,
}): RObject {
    const scoped = new RootScopedDag(d);
    const subscription = new ScopedDagSubscription(async () => scoped);
    return {
        getId: () => id,
        getType: () => 'test-object',
        getBackendLabel: () => 'default',
        validatePayload: opts?.validatePayload ?? (async (_payload: Payload, _at: Version) => validationOk()),
        applyPayload: async (payload: Payload, at: Version) => {
            return await d.append(payload, {}, at);
        },
        getView: async (_at?: Version, _from?: Version): Promise<View> => { throw new Error('not implemented'); },
        computeDelta: async (_start: Version, _end: Version): Promise<Delta> => { throw new Error('not implemented'); },
        createDeltaAccumulator: (_start: Version, _end: Version): DeltaAccumulator => { throw new Error('not implemented'); },
        getScopedDag: async () => scoped,
        getCausalDag: async () => d,
        extractForeignDeps: opts?.extractForeignDeps ?? ((_payload: Payload, _at: Version) => undefined),
        subscribe: (cb: (version: Version) => void) => subscription.subscribe(cb),
        unsubscribe: (cb: (version: Version) => void) => { subscription.unsubscribe(cb); },
        destroy: async () => {},
    };
}

function createTestSyncCtx(): RContext {
    const objects = new Map<B64Hash, RObject>();
    const newObjectCallbacks = new Set<(obj: RObject) => void>();

    function record(obj: RObject): void {
        const id = obj.getId();
        const isNew = !objects.has(id);
        objects.set(id, obj);
        if (isNew) {
            for (const cb of [...newObjectCallbacks]) {
                try { cb(obj); } catch { /* keep firing */ }
            }
        }
    }

    return {
        getCrypto: () => { throw new Error('not implemented'); },
        getHashSuite: () => sha256,
        getConfig: () => ({}),
        getRegistry: () => { throw new Error('not implemented'); },
        getObject: async (id: B64Hash) => objects.get(id),
        getDag: async (_id: B64Hash) => undefined,
        getBackendLabel: async () => 'default',
        getMesh: () => { throw new Error('not implemented'); },
        createObject: async () => { throw new Error('not implemented'); },
        registerObject: record,
        unregisterObject: async (id: B64Hash) => { objects.delete(id); },
        subscribeNewObject: (cb) => { newObjectCallbacks.add(cb); },
        unsubscribeNewObject: (cb) => { newObjectCallbacks.delete(cb); },
    };
}

// Wire up a synchronizer and provider pair over mock channels.
// chALocal: A's local end (peerId=peerB, i.e. "who is the remote"),
// chBLocal: B's local end (peerId=peerA).
// chALocal.send() → chBLocal receives (B gets the message)
// chBLocal.send() → chALocal receives (A gets the message)
function wireUpSync(
    dagA: dag.Dag,
    dagB: dag.Dag,
    rObjectA: RObject,
    dagId: B64Hash,
    topic: string,
) {
    const { local: chALocal, remote: chBLocal } = createChannelPair(topic, 'peerA', 'peerB');

    const peerBHandle = { key: `peerB@mem://peerB`, channel: chALocal };

    const providerB = createDagProvider(dagB);
    const synchronizerA = createDagSynchronizer(
        dagId,
        dagA,
        rObjectA,
        sha256,
        () => [peerBHandle],
        (peer, msg) => { try { peer.channel.send(encode(msg)); return 'sent' as const; } catch { return 'error' as const; } },
    );

    fifoOnMessage(chBLocal, (data) => {
        providerB.handleMessage(decode(data), chBLocal);
    });

    fifoOnMessage(chALocal, async (data) => {
        await synchronizerA.handleMessage(decode(data), chALocal);
    });

    synchronizerA.addPeer(peerBHandle);

    return { synchronizerA, providerB, chALocal, chBLocal };
}

// --- Tests ---

async function testCodecRoundTrip() {
    const msg: SyncMsg = {
        type: 'new-frontier',
        dagId: 'testDagId',
        frontier: ['hash1', 'hash2'],
    };
    const encoded = encode(msg);
    const decoded = decode(encoded);

    testing.assertEquals(decoded.type, 'new-frontier', 'type preserved');
    testing.assertEquals((decoded as NewFrontierMsg).dagId, 'testDagId', 'dagId preserved');
    testing.assertEquals((decoded as NewFrontierMsg).frontier.length, 2, 'frontier length preserved');
}

async function testSmallDivergence() {
    const dagId = 'test-dag-small';
    const topic = 'sync-small';

    const dagA = createTestDag();
    const dagB = createTestDag();

    // Build shared base: root -> A -> B
    const root = await dagB.append({ op: 'root' }, {});
    const a = await dagB.append({ op: 'A' }, {}, new Set([root]));
    const b = await dagB.append({ op: 'B' }, {}, new Set([a]));

    // Copy base to A
    await dagA.append({ op: 'root' }, {});
    await dagA.append({ op: 'A' }, {}, new Set([root]));
    await dagA.append({ op: 'B' }, {}, new Set([a]));

    // B extends with C, D
    const c = await dagB.append({ op: 'C' }, {}, new Set([b]));
    const d = await dagB.append({ op: 'D' }, {}, new Set([c]));

    const rObjectA = createMockRObject(dagA, dagId);
    const { synchronizerA, providerB, chALocal } = wireUpSync(dagA, dagB, rObjectA, dagId, topic);

    // Simulate B gossiping its frontier to A via the wired channel
    const frontierB = await dagB.getFrontier();
    const gossipMsg: NewFrontierMsg = {
        type: 'new-frontier',
        dagId,
        frontier: [...frontierB],
    };
    synchronizerA.handleMessage(gossipMsg, chALocal);

    await waitUntil(async () => {
        const ec = await dagA.loadEntry(c);
        const ed = await dagA.loadEntry(d);
        return ec !== undefined && ed !== undefined;
    });

    testing.assertTrue(true, 'A should have entries C and D');

    synchronizerA.destroy();
    providerB.destroy();
}

async function testYFork() {
    const dagId = 'test-dag-yfork';
    const topic = 'sync-yfork';

    const dagA = createTestDag();
    const dagB = createTestDag();

    // Build common root on both
    const rootA = await dagA.append({ op: 'root' }, {});
    const rootB = await dagB.append({ op: 'root' }, {});

    testing.assertEquals(rootA, rootB, 'roots should have same hash');

    // A extends with branch A1, A2
    const a1 = await dagA.append({ op: 'A1' }, {}, new Set([rootA]));
    const a2 = await dagA.append({ op: 'A2' }, {}, new Set([a1]));

    // B extends with branch B1, B2
    const b1 = await dagB.append({ op: 'B1' }, {}, new Set([rootB]));
    const b2 = await dagB.append({ op: 'B2' }, {}, new Set([b1]));

    // Sync A <- B (A pulls B's branch)
    const rObjectA = createMockRObject(dagA, dagId);
    const { synchronizerA, providerB, chALocal } = wireUpSync(dagA, dagB, rObjectA, dagId, topic);

    const frontierB = await dagB.getFrontier();
    const gossipMsg: NewFrontierMsg = {
        type: 'new-frontier',
        dagId,
        frontier: [...frontierB],
    };
    synchronizerA.handleMessage(gossipMsg, chALocal);

    await waitUntil(async () => {
        const eb1 = await dagA.loadEntry(b1);
        const eb2 = await dagA.loadEntry(b2);
        return eb1 !== undefined && eb2 !== undefined;
    });

    // A should still have its own entries
    const entryA1 = await dagA.loadEntry(a1);
    const entryA2 = await dagA.loadEntry(a2);

    testing.assertTrue(entryA1 !== undefined, 'A should still have A1');
    testing.assertTrue(entryA2 !== undefined, 'A should still have A2');

    synchronizerA.destroy();
    providerB.destroy();
}

async function testLargeDivergence() {
    const dagId = 'test-dag-large';
    const topic = 'sync-large';

    const dagA = createTestDag();
    const dagB = createTestDag();

    // Build shared base
    const root = await dagB.append({ op: 'root' }, {});
    await dagA.append({ op: 'root' }, {});

    // B adds a chain of 40 entries (40 payloads × 100ms tick = ~4s streaming)
    const CHAIN_LENGTH = 40;
    let prev = root;
    const expectedHashes: B64Hash[] = [];
    for (let i = 0; i < CHAIN_LENGTH; i++) {
        prev = await dagB.append({ op: `entry-${i}` }, {}, new Set([prev]));
        expectedHashes.push(prev);
    }

    const rObjectA = createMockRObject(dagA, dagId);
    const { synchronizerA, providerB, chALocal } = wireUpSync(dagA, dagB, rObjectA, dagId, topic);

    const frontierB = await dagB.getFrontier();
    const gossipMsg: NewFrontierMsg = {
        type: 'new-frontier',
        dagId,
        frontier: [...frontierB],
    };
    synchronizerA.handleMessage(gossipMsg, chALocal);

    const lastHash = expectedHashes[expectedHashes.length - 1];
    await waitUntil(async () => {
        const entry = await dagA.loadEntry(lastHash);
        return entry !== undefined;
    });

    // Verify all entries are synced (not just the last)
    for (let i = 0; i < expectedHashes.length; i++) {
        const entry = await dagA.loadEntry(expectedHashes[i]);
        testing.assertTrue(entry !== undefined, `A should have entry ${i}`);
    }

    synchronizerA.destroy();
    providerB.destroy();
}

async function testProviderHeaderBFS() {
    const dagId = 'test-provider-bfs';
    const topic = 'provider-test';

    const d = createTestDag();
    const root = await d.append({ op: 'root' }, {});
    const a = await d.append({ op: 'A' }, {}, new Set([root]));
    const b = await d.append({ op: 'B' }, {}, new Set([a]));
    const c = await d.append({ op: 'C' }, {}, new Set([b]));

    const provider = createDagProvider(d);

    // Collect messages sent
    const sent: SyncMsg[] = [];
    const ch = new MockChannel(topic, 'requester', 'mem://requester');
    const remoteCh = new MockChannel(topic, 'provider', 'mem://provider');
    ch.peer = remoteCh;
    remoteCh.peer = ch;

    remoteCh.onMessage((data) => {
        sent.push(decode(data));
    });

    provider.handleMessage({
        type: 'header-request',
        requestId: 'req1',
        dagId,
        start: [c],
        limits: [root],
        maxHeaders: 100,
        autoPayload: false,
    }, ch);

    await waitUntil(async () => sent.some(m => m.type === 'header-batch'));

    const metaMsg = sent.find(m => m.type === 'header-response-meta');
    testing.assertTrue(metaMsg !== undefined, 'should receive header-response-meta');
    testing.assertEquals((metaMsg as any).headerCount, 3, 'should have 3 headers (A, B, C)');
    testing.assertTrue((metaMsg as any).complete, 'should be complete');

    const batches = sent.filter(m => m.type === 'header-batch');
    testing.assertTrue(batches.length >= 1, 'should have at least 1 batch');

    const allHeaders = batches.flatMap(b => (b as any).headers);
    testing.assertEquals(allHeaders.length, 3, 'total headers should be 3');

    const hashes = new Set(allHeaders.map((h: any) => h.hash));
    testing.assertTrue(hashes.has(a), 'should include A');
    testing.assertTrue(hashes.has(b), 'should include B');
    testing.assertTrue(hashes.has(c), 'should include C');

    provider.destroy();
}

async function testProviderPayloadServing() {
    const dagId = 'test-provider-payload';
    const topic = 'provider-payload-test';

    const d = createTestDag();
    const root = await d.append({ op: 'root' }, {});
    const a = await d.append({ op: 'A' }, {}, new Set([root]));

    const provider = createDagProvider(d);

    const sent: SyncMsg[] = [];
    const ch = new MockChannel(topic, 'requester', 'mem://requester');
    const remoteCh = new MockChannel(topic, 'provider', 'mem://provider');
    ch.peer = remoteCh;
    remoteCh.peer = ch;

    remoteCh.onMessage((data) => {
        sent.push(decode(data));
    });

    provider.handleMessage({
        type: 'payload-request',
        requestId: 'req1',
        dagId,
        hashes: [root, a],
    }, ch);

    await waitUntil(async () => sent.filter(m => m.type === 'payload-msg').length >= 2);

    const metaMsg = sent.find(m => m.type === 'payload-response-meta');
    testing.assertTrue(metaMsg !== undefined, 'should receive payload-response-meta');
    testing.assertEquals((metaMsg as any).payloadCount, 2, 'should announce 2 payloads');

    const payloads = sent.filter(m => m.type === 'payload-msg');
    testing.assertEquals(payloads.length, 2, 'should have 2 payload messages');

    testing.assertEquals((payloads[0] as any).sequence, 0, 'first payload sequence');
    testing.assertEquals((payloads[1] as any).sequence, 1, 'second payload sequence');

    provider.destroy();
}

async function testCancelRequest() {
    const dagId = 'test-cancel';
    const topic = 'cancel-test';

    const d = createTestDag();
    const root = await d.append({ op: 'root' }, {});
    let prev = root;
    for (let i = 0; i < 50; i++) {
        prev = await d.append({ op: `entry-${i}` }, {}, new Set([prev]));
    }

    const provider = createDagProvider(d);

    const sent: SyncMsg[] = [];
    const ch = new MockChannel(topic, 'requester', 'mem://requester');
    const remoteCh = new MockChannel(topic, 'provider', 'mem://provider');
    ch.peer = remoteCh;
    remoteCh.peer = ch;

    remoteCh.onMessage((data) => {
        sent.push(decode(data));
    });

    provider.handleMessage({
        type: 'payload-request',
        requestId: 'req-cancel',
        dagId,
        hashes: Array.from({ length: 50 }, (_, i) => `hash-${i}`),
    }, ch);

    // Cancel immediately
    provider.handleMessage({
        type: 'cancel-request',
        requestId: 'req-cancel',
    }, ch);

    await wait(200);

    // Some messages may have been sent before cancel, but we won't get all 50
    const payloads = sent.filter(m => m.type === 'payload-msg');
    testing.assertTrue(payloads.length < 50, 'cancel should prevent all 50 payloads from being sent');

    provider.destroy();
}

async function testFrontierDuringSync() {
    const dagId = 'test-dag-frontier-during';
    const topic = 'sync-frontier-during';

    const dagA = createTestDag();
    const dagB = createTestDag();

    // Shared root
    const root = await dagB.append({ op: 'root' }, {});
    await dagA.append({ op: 'root' }, {});

    // B builds a chain: root -> C -> D -> E -> F -> G
    let prev = root;
    const chainHashes: B64Hash[] = [];
    for (const label of ['C', 'D', 'E', 'F', 'G']) {
        prev = await dagB.append({ op: label }, {}, new Set([prev]));
        chainHashes.push(prev);
    }
    const g = chainHashes[chainHashes.length - 1];

    const rObjectA = createMockRObject(dagA, dagId);
    const { synchronizerA, providerB, chALocal } = wireUpSync(dagA, dagB, rObjectA, dagId, topic);

    // Send first frontier [G]
    synchronizerA.handleMessage({
        type: 'new-frontier', dagId, frontier: [g],
    }, chALocal);

    // Wait a bit for header fetch to be in-flight, then add H on B and send new frontier
    await wait(50);
    const h = await dagB.append({ op: 'H' }, {}, new Set([g]));

    synchronizerA.handleMessage({
        type: 'new-frontier', dagId, frontier: [h],
    }, chALocal);

    // A should eventually have all entries C through H
    await waitUntil(async () => {
        const entryH = await dagA.loadEntry(h);
        return entryH !== undefined;
    });

    for (let i = 0; i < chainHashes.length; i++) {
        const entry = await dagA.loadEntry(chainHashes[i]);
        testing.assertTrue(entry !== undefined, `A should have chain entry ${i}`);
    }
    const entryH = await dagA.loadEntry(h);
    testing.assertTrue(entryH !== undefined, 'A should have entry H');

    synchronizerA.destroy();
    providerB.destroy();
}

async function testMultiPeerSync() {
    const dagId = 'test-dag-multi-peer';
    const topic = 'sync-multi-peer';

    const dagA = createTestDag();
    const dagB = createTestDag();
    const dagC = createTestDag();

    // Shared root on all three
    const root = await dagB.append({ op: 'root' }, {});
    await dagA.append({ op: 'root' }, {});
    await dagC.append({ op: 'root' }, {});

    // B builds a chain: root -> E1 -> E2 -> ... -> E10
    let prev = root;
    const expectedHashes: B64Hash[] = [];
    for (let i = 0; i < 10; i++) {
        prev = await dagB.append({ op: `entry-${i}` }, {}, new Set([prev]));
        expectedHashes.push(prev);
    }

    // Copy B's chain into C so both can serve payloads
    prev = root;
    for (let i = 0; i < 10; i++) {
        prev = await dagC.append({ op: `entry-${i}` }, {}, new Set([prev]));
    }

    // Wire up A with both B and C as providers
    const { local: chAB, remote: chBA } = createChannelPair(topic, 'peerA', 'peerB');
    const { local: chAC, remote: chCA } = createChannelPair(topic, 'peerA', 'peerC');

    const peerBHandle = { key: 'peerB@mem://peerB', channel: chAB };
    const peerCHandle = { key: 'peerC@mem://peerC', channel: chAC };

    const providerB = createDagProvider(dagB);
    const providerC = createDagProvider(dagC);

    const rObjectA = createMockRObject(dagA, dagId);
    const synchronizerA = createDagSynchronizer(
        dagId, dagA, rObjectA, sha256,
        () => [peerBHandle, peerCHandle],
        (peer, msg) => { try { peer.channel.send(encode(msg)); return 'sent' as const; } catch { return 'error' as const; } },
    );

    fifoOnMessage(chBA, (data) => {
        providerB.handleMessage(decode(data), chBA);
    });
    fifoOnMessage(chAB, async (data) => {
        await synchronizerA.handleMessage(decode(data), chAB);
    });

    fifoOnMessage(chCA, (data) => {
        providerC.handleMessage(decode(data), chCA);
    });
    fifoOnMessage(chAC, async (data) => {
        await synchronizerA.handleMessage(decode(data), chAC);
    });

    synchronizerA.addPeer(peerBHandle);
    synchronizerA.addPeer(peerCHandle);

    // Both B and C gossip the same frontier
    const frontierB = await dagB.getFrontier();
    synchronizerA.handleMessage({
        type: 'new-frontier', dagId, frontier: [...frontierB],
    }, chAB);

    const frontierC = await dagC.getFrontier();
    synchronizerA.handleMessage({
        type: 'new-frontier', dagId, frontier: [...frontierC],
    }, chAC);

    const lastHash = expectedHashes[expectedHashes.length - 1];
    await waitUntil(async () => {
        const entry = await dagA.loadEntry(lastHash);
        return entry !== undefined;
    });

    for (let i = 0; i < expectedHashes.length; i++) {
        const entry = await dagA.loadEntry(expectedHashes[i]);
        testing.assertTrue(entry !== undefined, `A should have entry ${i}`);
    }

    synchronizerA.destroy();
    providerB.destroy();
    providerC.destroy();
}

function wireUpSyncWithCtx(
    dagA: dag.Dag,
    dagB: dag.Dag,
    rObjectA: RObject,
    dagId: B64Hash,
    topic: string,
    ctx?: RContext,
) {
    const { local: chALocal, remote: chBLocal } = createChannelPair(topic, 'peerA', 'peerB');

    const peerBHandle = { key: `peerB@mem://peerB`, channel: chALocal };

    const providerB = createDagProvider(dagB);
    const synchronizerA = createDagSynchronizer(
        dagId,
        dagA,
        rObjectA,
        sha256,
        () => [peerBHandle],
        (peer, msg) => { try { peer.channel.send(encode(msg)); return 'sent' as const; } catch { return 'error' as const; } },
        ctx,
    );

    fifoOnMessage(chBLocal, (data) => {
        providerB.handleMessage(decode(data), chBLocal);
    });

    fifoOnMessage(chALocal, async (data) => {
        await synchronizerA.handleMessage(decode(data), chALocal);
    });

    synchronizerA.addPeer(peerBHandle);

    return { synchronizerA, providerB, chALocal, chBLocal };
}

async function testForeignDepDeferral() {
    const dagId = 'test-dag-foreign-dep';
    const topic = 'sync-foreign-dep';
    const refObjectId = 'ref-permissions-obj';

    const dagA = createTestDag();
    const dagB = createTestDag();
    const refDag = createTestDag();

    const root = await dagB.append({ op: 'root' }, {});
    await dagA.append({ op: 'root' }, {});

    const refPayload = { op: 'ref-init' };
    const refEntry = dag.createEntry(refPayload, {}, undefined, sha256);
    const requiredRefHash = refEntry.hash;

    const normalEntry = await dagB.append({ op: 'normal' }, {}, new Set([root]));
    const refAdvanceEntry = await dagB.append(
        { action: 'ref-advance', refId: refObjectId, refVersion: { 'v1': '' } },
        {},
        new Set([normalEntry]),
    );

    const rObjectA = createMockRObject(dagA, dagId, {
        extractForeignDeps: (payload: Payload, _at: Version) => {
            if (typeof payload === 'object' && !Array.isArray(payload) && payload['action'] === 'ref-advance') {
                return [{ objectId: (payload as any).refId, requiredHashes: [requiredRefHash] }];
            }
            return undefined;
        },
    });

    const refObject = createMockRObject(refDag, refObjectId);
    const ctx = createTestSyncCtx();
    ctx.registerObject(refObject);

    const { synchronizerA, providerB, chALocal } = wireUpSyncWithCtx(
        dagA, dagB, rObjectA, dagId, topic, ctx,
    );

    const frontierB = await dagB.getFrontier();
    synchronizerA.handleMessage({
        type: 'new-frontier', dagId, frontier: [...frontierB],
    }, chALocal);

    await waitUntil(async () => {
        const entry = await dagA.loadEntry(normalEntry);
        return entry !== undefined;
    });

    const normalSynced = await dagA.loadEntry(normalEntry);
    testing.assertTrue(normalSynced !== undefined, 'normal entry without foreign deps should sync');

    await wait(300);
    const refAdvanceBefore = await dagA.loadEntry(refAdvanceEntry);
    testing.assertTrue(refAdvanceBefore === undefined, 'ref-advance entry should be deferred (not synced yet)');

    const actualRefHash = await refDag.append(refPayload, {});
    testing.assertEquals(actualRefHash, requiredRefHash, 'pre-computed and actual ref entry hash should match');

    await waitUntil(async () => {
        const entry = await dagA.loadEntry(refAdvanceEntry);
        return entry !== undefined;
    });

    const refAdvanceAfter = await dagA.loadEntry(refAdvanceEntry);
    testing.assertTrue(refAdvanceAfter !== undefined, 'ref-advance entry should sync after foreign deps are available');

    synchronizerA.destroy();
    providerB.destroy();
}

async function testForeignDepAppearance() {
    const dagId = 'test-dag-foreign-appear';
    const topic = 'sync-foreign-appear';
    const refObjectId = 'ref-late-obj';

    const dagA = createTestDag();
    const dagB = createTestDag();
    const refDag = createTestDag();

    const root = await dagB.append({ op: 'root' }, {});
    await dagA.append({ op: 'root' }, {});

    const refPayload = { op: 'ref-init' };
    const refEntry = dag.createEntry(refPayload, {}, undefined, sha256);
    const requiredRefHash = refEntry.hash;

    const normalEntry = await dagB.append({ op: 'normal' }, {}, new Set([root]));
    const refAdvanceEntry = await dagB.append(
        { action: 'ref-advance', refId: refObjectId, refVersion: { 'v1': '' } },
        {},
        new Set([normalEntry]),
    );

    const rObjectA = createMockRObject(dagA, dagId, {
        extractForeignDeps: (payload: Payload, _at: Version) => {
            if (typeof payload === 'object' && !Array.isArray(payload) && payload['action'] === 'ref-advance') {
                return [{ objectId: (payload as any).refId, requiredHashes: [requiredRefHash] }];
            }
            return undefined;
        },
    });

    const refObject = createMockRObject(refDag, refObjectId);
    const ctx = createTestSyncCtx();

    const { synchronizerA, providerB, chALocal } = wireUpSyncWithCtx(
        dagA, dagB, rObjectA, dagId, topic, ctx,
    );

    const frontierB = await dagB.getFrontier();
    synchronizerA.handleMessage({
        type: 'new-frontier', dagId, frontier: [...frontierB],
    }, chALocal);

    await waitUntil(async () => {
        const entry = await dagA.loadEntry(normalEntry);
        return entry !== undefined;
    });

    await wait(300);
    testing.assertTrue(
        (await dagA.loadEntry(refAdvanceEntry)) === undefined,
        'ref-advance should stay deferred while the referenced object is unregistered',
    );

    ctx.registerObject(refObject);

    const actualRefHash = await refDag.append(refPayload, {});
    testing.assertEquals(actualRefHash, requiredRefHash, 'pre-computed and actual ref entry hash should match');

    await waitUntil(async () => {
        const entry = await dagA.loadEntry(refAdvanceEntry);
        return entry !== undefined;
    });

    testing.assertTrue(
        (await dagA.loadEntry(refAdvanceEntry)) !== undefined,
        'ref-advance should apply after the referenced object is registered and grows',
    );

    synchronizerA.destroy();
    providerB.destroy();
}

function stubSwarm(peers: SwarmPeer[]): Swarm {
    return {
        topic: 'fetch-init',
        mode: 'active',
        destroyed: false,
        activate() {},
        deactivate() {},
        sleep() {},
        destroy() {},
        peers: () => peers,
        onPeerJoin() {},
        onPeerLeave() {},
        blockPeer() {},
        wouldAccept() { return Promise.resolve(true); },
        wouldServe: () => true as const,
        adopt() { return false; },
    };
}

function joinableSwarm(): { swarm: Swarm; join(peer: SwarmPeer): void } {
    const peers: SwarmPeer[] = [];
    const joins: Array<(peer: SwarmPeer) => void> = [];
    const swarm: Swarm = {
        topic: 'fetch-init',
        mode: 'active',
        destroyed: false,
        activate() {},
        deactivate() {},
        sleep() {},
        destroy() {},
        peers: () => peers,
        onPeerJoin(cb) { joins.push(cb); },
        onPeerLeave() {},
        blockPeer() {},
        wouldAccept() { return Promise.resolve(true); },
        wouldServe: () => true as const,
        adopt() { return false; },
    };
    return {
        swarm,
        join(peer) {
            peers.push(peer);
            for (const cb of joins) cb(peer);
        },
    };
}

async function testFetchInitHashMismatch() {
    const expectedPayload = { action: 'create', type: 'hhs/test', seed: 'expected' };
    const wrongPayload = { action: 'create', type: 'hhs/test', seed: 'wrong' };
    const objectId = dag.createEntry(expectedPayload, {}, dag.position(), sha256).hash;

    const { local, remote } = createChannelPair('fetch-init', 'peerA', 'peerB');
    const swarmPeer: SwarmPeer = { keyId: 'peerB', endpoint: 'mem://peerB', channel: local };
    const swarm = stubSwarm([swarmPeer]);

    remote.onMessage((data) => {
        const msg = decode(data);
        if (msg.type !== 'init-request' || msg.objectId !== objectId) return;
        const resp: InitResponse = {
            type: 'init-response',
            objectId,
            createPayload: wrongPayload,
        };
        remote.send(encode(resp));
    });

    let threw = false;
    try {
        await fetchInit(objectId, [swarm], sha256, { timeoutMs: 2000 });
    } catch (e: any) {
        threw = true;
        testing.assertTrue(
            typeof e.message === 'string' && e.message.includes('Creation payload hash mismatch'),
            `error should mention hash mismatch, got: ${e.message}`,
        );
        testing.assertTrue(
            e.message.includes(objectId),
            'error should include the expected object id',
        );
    }
    testing.assertTrue(threw, 'fetchInit should reject a createPayload that does not hash to objectId');

    local.close();
}

// A message must be a Literal. JSON text can carry a lone surrogate as an
// escape, and a UTF-8 encoder writes U+FFFD for it, so a payload holding one
// would hash like a different payload.
async function testCodecRejectsNonLiterals() {
    const throws = (data: Uint8Array) => { try { decode(data); return false; } catch { return true; } };
    const text = (data: Uint8Array) => new TextDecoder().decode(data);

    const illFormedPayload: PayloadMsg = {
        type: 'payload-msg', requestId: 'r1', sequence: 0, hash: 'h1',
        payload: { s: 'x\ud800y' },
    };
    const encoded = encode(illFormedPayload);
    testing.assertTrue(text(encoded).includes('\\ud800'), 'the lone surrogate travels as an escape');
    testing.assertTrue(throws(encoded), 'a payload with an ill-formed string is rejected');

    const illFormedKey: HeaderBatch = {
        type: 'header-batch', requestId: 'r1', sequence: 0,
        headers: [{ hash: 'h1', header: { payloadHash: 'p1', prevEntryHashes: { 'k\udc00': '' } } }],
    };
    testing.assertTrue(throws(encode(illFormedKey)), 'a header with an ill-formed key is rejected');

    const withNull = new TextEncoder().encode('{"type":"new-frontier","dagId":"d","frontier":[null]}');
    testing.assertTrue(throws(withNull), 'a message containing null is rejected');

    const wellFormed: PayloadMsg = {
        type: 'payload-msg', requestId: 'r1', sequence: 0, hash: 'h1',
        payload: { s: 'x�y', pair: 'a😀b' },
    };
    const decoded = decode(encode(wellFormed)) as PayloadMsg;
    testing.assertTrue(json.eq(decoded.payload, wellFormed.payload), 'U+FFFD and a surrogate pair round-trip');
}

// A payload nests at most json.MAX_LITERAL_DEPTH levels, and a message holds
// it one level in. JSON.parse reads text nested far deeper than the code that
// then hashes the payload can walk, so decode counts the levels.
async function testCodecRejectsDeepNesting() {
    const error = (data: Uint8Array) => { try { decode(data); return undefined; } catch (e: any) { return String(e.message); } };
    const nested = (depth: number): json.Literal => { let v: json.Literal = 1; for (let i = 0; i < depth; i++) v = [v]; return v; };
    const payloadMsg = (payload: json.Literal): PayloadMsg =>
        ({ type: 'payload-msg', requestId: 'r1', sequence: 0, hash: 'h1', payload });

    const atLimit = nested(json.MAX_LITERAL_DEPTH);
    const decoded = decode(encode(payloadMsg(atLimit))) as PayloadMsg;
    testing.assertTrue(json.eq(decoded.payload, atLimit), 'a payload nested exactly to the limit round-trips');

    const tooDeep = error(encode(payloadMsg(nested(json.MAX_LITERAL_DEPTH + 1))));
    testing.assertTrue(tooDeep !== undefined && tooDeep.includes('levels of nesting'),
        `a payload nested one level too deep is rejected: ${tooDeep}`);

    const init = new TextEncoder().encode(
        '{"type":"init-response","objectId":"o","createPayload":' + '['.repeat(json.MAX_LITERAL_DEPTH + 1) + ']'.repeat(json.MAX_LITERAL_DEPTH + 1) + '}');
    testing.assertTrue(error(init) !== undefined, 'a create payload nested one level too deep is rejected');

    // Far past what the hashing code could walk: named by the count, with no
    // stack overflow.
    const depth = 100000;
    const huge = new TextEncoder().encode(
        '{"type":"payload-msg","requestId":"r1","sequence":0,"hash":"h1","payload":' + '['.repeat(depth) + ']'.repeat(depth) + '}');
    const hugeError = error(huge);
    testing.assertTrue(hugeError !== undefined && hugeError.includes('levels of nesting'),
        `a payload nested ${depth} levels deep is rejected by the count: ${hugeError}`);
}

// The object wanted has U+FFFD in its create payload. The peer answers with a
// lone surrogate in that place, which hashes to the same object id.
async function testFetchInitIgnoresIllFormedCreatePayload() {
    const expectedPayload = { action: 'create', type: 'hhs/test', seed: 'x�y' };
    const forgedPayload = { action: 'create', type: 'hhs/test', seed: 'x\ud800y' };
    const objectId = dag.createEntry(expectedPayload, {}, dag.position(), sha256).hash;

    const { local, remote } = createChannelPair('fetch-init-ill-formed', 'peerA', 'peerB');
    const swarmPeer: SwarmPeer = { keyId: 'peerB', endpoint: 'mem://peerB', channel: local };
    const swarm = stubSwarm([swarmPeer]);

    let answered = 0;
    remote.onMessage((data) => {
        const msg = decode(data);
        if (msg.type !== 'init-request' || msg.objectId !== objectId) return;
        const resp: InitResponse = {
            type: 'init-response',
            objectId,
            createPayload: forgedPayload,
        };
        answered++;
        remote.send(encode(resp));
    });

    const initTimeoutMs = 300;
    let threw = false;
    try {
        await fetchInit(objectId, [swarm], sha256, { timeoutMs: initTimeoutMs, peerTimeoutMs: 5_000 });
    } catch (e: any) {
        threw = true;
        testing.assertTrue(
            typeof e.message === 'string' && e.message.includes(`fetchInit timed out after ${initTimeoutMs}ms`),
            `the forged answer is ignored, so the fetch times out, got: ${e.message}`,
        );
    }
    testing.assertTrue(answered > 0, 'the peer did answer');
    testing.assertTrue(threw, 'fetchInit should not accept a create payload with an ill-formed string');

    local.close();
}

async function testFetchInitLatePeer() {
    const payload = { action: 'create', type: 'hhs/test', seed: 'late' };
    const objectId = dag.createEntry(payload, {}, dag.position(), sha256).hash;
    const initTimeoutMs = 200;
    const peerTimeoutMs = 2_000;

    const { local, remote } = createChannelPair('fetch-init-late', 'peerA', 'peerB');
    const { swarm, join } = joinableSwarm();
    remote.onMessage((data) => {
        const msg = decode(data);
        if (msg.type !== 'init-request' || msg.objectId !== objectId) return;
        const resp: InitResponse = { type: 'init-response', objectId, createPayload: payload };
        remote.send(encode(resp));
    });

    const started = Date.now();
    const pending = fetchInit(objectId, [swarm], sha256, { timeoutMs: initTimeoutMs, peerTimeoutMs });
    // Past the init budget. The old single clock would already have rejected.
    await wait(initTimeoutMs + 100);
    join({ keyId: 'peerB', endpoint: 'mem://peerB', channel: local });

    const got = await pending as { seed?: string };
    testing.assertTrue(Date.now() - started > initTimeoutMs, 'peer arrived after the init budget');
    testing.assertTrue(Date.now() - started < peerTimeoutMs, 'resolved inside the peer budget');
    testing.assertEquals(got.seed, 'late', 'late peer still delivers the create payload');

    local.close();
}

async function testFetchInitNoPeer() {
    const { swarm } = joinableSwarm();
    const peerTimeoutMs = 150;
    const started = Date.now();
    let threw = false;
    try {
        await fetchInit('no-peer-object', [swarm], sha256, { timeoutMs: 5_000, peerTimeoutMs });
    } catch (e: any) {
        threw = true;
        testing.assertTrue(
            typeof e.message === 'string' && e.message.includes('no peer for no-peer-object'),
            `error should be the peer budget, got: ${e.message}`,
        );
        testing.assertTrue(
            !e.message.includes('fetchInit timed out'),
            'a missing peer is not an init timeout',
        );
    }
    testing.assertTrue(threw, 'fetchInit should reject when no peer arrives');
    const elapsed = Date.now() - started;
    testing.assertTrue(elapsed < 1_000, `rejected on the peer budget (elapsed ${elapsed}ms)`);
}

async function testFetchInitAlreadyConnectedTimeout() {
    const payload = { action: 'create', type: 'hhs/test', seed: 'silent' };
    const objectId = dag.createEntry(payload, {}, dag.position(), sha256).hash;
    const { local } = createChannelPair('fetch-init-silent', 'peerA', 'peerB');
    const swarmPeer: SwarmPeer = { keyId: 'peerB', endpoint: 'mem://peerB', channel: local };
    const swarm = stubSwarm([swarmPeer]);

    const initTimeoutMs = 200;
    const started = Date.now();
    let threw = false;
    try {
        await fetchInit(objectId, [swarm], sha256, { timeoutMs: initTimeoutMs, peerTimeoutMs: 5_000 });
    } catch (e: any) {
        threw = true;
        testing.assertTrue(
            typeof e.message === 'string' && e.message.includes(`fetchInit timed out after ${initTimeoutMs}ms`),
            `error should be the init budget, got: ${e.message}`,
        );
    }
    testing.assertTrue(threw, 'a present peer that never answers should hit the init budget');
    const elapsed = Date.now() - started;
    testing.assertTrue(elapsed < 1_000, `init budget, not the peer budget (elapsed ${elapsed}ms)`);

    local.close();
}

async function testAutoPayloadFifo() {
    const dagId = 'test-dag-auto-payload-fifo';
    const topic = 'sync-auto-payload-fifo';

    const dagA = createTestDag();
    const dagB = createTestDag();

    const root = await dagB.append({ op: 'root' }, {});
    await dagA.append({ op: 'root' }, {});
    const a = await dagB.append({ op: 'A' }, {}, new Set([root]));
    const b = await dagB.append({ op: 'B' }, {}, new Set([a]));
    const c = await dagB.append({ op: 'C' }, {}, new Set([b]));

    // Yield inside loadHeader so auto-payload frames arrive while the header
    // batch is still in flight. FIFO must hold them until hashes are bound.
    wrapLoadHeader(dagA, 20);

    const rObjectA = createMockRObject(dagA, dagId);
    const { local: chALocal, remote: chBLocal } = createChannelPair(topic, 'peerA', 'peerB');
    const swarmPeer: SwarmPeer = { keyId: 'peerB', endpoint: 'mem://peerB', channel: chALocal };
    const swarm = stubSwarm([swarmPeer]);

    const providerB = createDagProvider(dagB);
    fifoOnMessage(chBLocal, (data) => {
        providerB.handleMessage(decode(data), chBLocal);
    });

    const session = createSyncSession(
        { dagId, dag: dagA, rObject: rObjectA, hashSuite: sha256 },
        [swarm],
    );

    const started = Date.now();
    const frontierB = await dagB.getFrontier();
    chBLocal.send(encode({
        type: 'new-frontier',
        dagId,
        frontier: [...frontierB],
    }));

    await waitUntil(async () => {
        const ea = await dagA.loadEntry(a);
        const eb = await dagA.loadEntry(b);
        const ec = await dagA.loadEntry(c);
        return ea !== undefined && eb !== undefined && ec !== undefined;
    }, 20, 3000);

    testing.assertTrue(Date.now() - started < 3000, 'auto-payload FIFO must apply without the 30s request timeout');
    testing.assertEquals(session.getDiagnostics().pendingPayloadRequests, 0, 'no leftover payload wait');

    session.destroy();
    providerB.destroy();
    chALocal.close();
}

async function testAutoPayloadCountMismatch() {
    const dagId = 'test-dag-payload-count-mismatch';
    const topic = 'sync-payload-count-mismatch';

    const dagA = createTestDag();
    const dagB = createTestDag();

    const root = await dagB.append({ op: 'root' }, {});
    await dagA.append({ op: 'root' }, {});
    const a = await dagB.append({ op: 'A' }, {}, new Set([root]));

    const rObjectA = createMockRObject(dagA, dagId);
    const { local: chALocal, remote: chBLocal } = createChannelPair(topic, 'peerA', 'peerB');
    const swarmPeer: SwarmPeer = { keyId: 'peerB', endpoint: 'mem://peerB', channel: chALocal };
    const swarm = stubSwarm([swarmPeer]);

    let captured: HeaderRequest | undefined;
    fifoOnMessage(chBLocal, (data) => {
        const msg = decode(data);
        if (msg.type === 'header-request' && captured === undefined) {
            captured = msg;
            return;
        }
    });

    const reports: IssueReport[] = [];
    const session = createSyncSession(
        { dagId, dag: dagA, rObject: rObjectA, hashSuite: sha256 },
        [swarm],
        { report: (r) => reports.push(r) },
    );

    chBLocal.send(encode({
        type: 'new-frontier',
        dagId,
        frontier: [a],
    }));

    await waitUntil(async () => captured !== undefined, 20, 2000);
    const req = captured!;
    const header = await dagB.loadHeader(a);
    testing.assertTrue(header !== undefined, 'B should have header A');

    chBLocal.send(encode({
        type: 'header-response-meta',
        requestId: req.requestId,
        headerCount: 1,
        complete: true,
        payloadCount: 1,
    }));
    chBLocal.send(encode({
        type: 'header-batch',
        requestId: req.requestId,
        sequence: 0,
        headers: [{ hash: a, header: header! }],
    }));
    chBLocal.send(encode({
        type: 'payload-response-meta',
        requestId: req.requestId,
        payloadCount: 99,
    }));

    await waitUntil(async () => reports.some((r) => r.kind === 'protocol'), 20, 2000);
    const protoReport = reports.find((r) => r.kind === 'protocol');
    testing.assertTrue(protoReport !== undefined, 'wrong payloadCount must fail the request as a protocol issue');
    testing.assertEquals(protoReport!.keyId, 'peerB', 'protocol issue blames the delivering peer');
    testing.assertEquals(protoReport!.endpoint, 'mem://peerB', 'protocol issue carries the peer endpoint');
    testing.assertEquals(protoReport!.severity, 'high', 'protocol violation is high severity');

    session.destroy();
    chALocal.close();
}

// A manually-driven synchronizer A with a single peer B. Outgoing requests are
// captured in `sent`; structured reports in `reports`. Responses are fed by
// calling sync.handleMessage(msg, channel) directly, so tests control ordering,
// provenance (which channel delivers), and payload bytes precisely.
// --- [SYNC_25]: a session gives entries only to the peers its swarm serves ---

// The messages that give a peer something: a frontier, a creation payload,
// headers or payloads.
const GIVES = new Set<SyncMsg['type']>([
    'new-frontier', 'init-response', 'header-response-meta', 'header-batch', 'payload-response-meta', 'payload-msg',
]);

function wrapLoadEntry(d: dag.Dag, delayMs: number): void {
    const orig = d.loadEntry.bind(d);
    d.loadEntry = async (h: B64Hash) => {
        await wait(delayMs);
        return orig(h);
    };
}

// A dag that starts with a creation payload, followed by a chain of `length` entries.
async function chainDag(length: number): Promise<{ d: dag.Dag; root: B64Hash; hashes: B64Hash[] }> {
    const d = createTestDag();
    const root = await d.append({ action: 'create', type: 'test-object' }, {});
    const hashes: B64Hash[] = [];
    let prev = root;
    for (let i = 0; i < length; i++) {
        prev = await d.append({ op: i }, {}, new Set([prev]));
        hashes.push(prev);
    }
    return { d, root, hashes };
}

// A session over a swarm whose serve answer the test sets. The peer is driven
// by hand: `sent` is what the session sent it. With `answer`, the peer also
// answers the session's own requests from that dag.
function gatedSession(dagId: B64Hash, d: dag.Dag, serve: { on: boolean }, answer?: dag.Dag) {
    const { local, remote } = createChannelPair(`sync25-${dagId}`, 'host', 'peer');
    const swarmPeer: SwarmPeer = { keyId: 'peer', endpoint: 'mem://peer', channel: local };
    const swarm: Swarm = { ...stubSwarm([swarmPeer]), wouldServe: async () => serve.on };
    const sent: SyncMsg[] = [];
    const peerProvider = answer === undefined ? undefined : createDagProvider(answer);
    fifoOnMessage(remote, (data) => {
        const msg = decode(data);
        sent.push(msg);
        peerProvider?.handleMessage(msg, remote);
    });
    const session = createSyncSession(
        { dagId, dag: d, rObject: createMockRObject(d, dagId), hashSuite: sha256 },
        [swarm],
    );
    return {
        sent,
        count: (type: SyncMsg['type']) => sent.filter((m) => m.type === type).length,
        given: () => sent.filter((m) => GIVES.has(m.type)).length,
        say: (msg: SyncMsg) => { remote.send(encode(msg)); },
        close: () => {
            session.destroy();
            peerProvider?.destroy();
            local.close();
        },
    };
}

async function testServeCheck() {
    // 1. A peer that is not served is given nothing, and is still listened to.
    {
        const { d, root, hashes } = await chainDag(2);
        const tip = hashes[hashes.length - 1]!;
        const peer = await chainDag(0);
        testing.assertEquals(peer.root, root, 'both sides share the creation entry');
        const x = await peer.d.append({ op: 'X' }, {}, new Set([peer.root]));
        const serve = { on: false };
        const s = gatedSession(root, d, serve, peer.d);

        s.say({ type: 'init-request', objectId: root });
        s.say({ type: 'header-request', requestId: 'h1', dagId: root, start: [tip], limits: [], maxHeaders: 1024, autoPayload: true });
        s.say({ type: 'payload-request', requestId: 'p1', dagId: root, hashes: [tip] });
        await wait(250);
        testing.assertEquals(s.given(), 0, 'not served: no frontier, and no answer to any request');

        s.say({ type: 'new-frontier', dagId: root, frontier: [x] });
        await waitUntil(async () => (await d.loadEntry(x)) !== undefined, 20, 3000);
        await wait(150);
        testing.assertEquals(s.given(), 0, 'what the peer serves is applied, and the new frontier is withheld too');

        // 2. It is served once the swarm's answer changes.
        serve.on = true;
        s.say({ type: 'init-request', objectId: root });
        s.say({ type: 'header-request', requestId: 'h2', dagId: root, start: [tip], limits: [], maxHeaders: 1024, autoPayload: false });
        s.say({ type: 'payload-request', requestId: 'p2', dagId: root, hashes: [tip] });
        await waitUntil(
            async () => s.count('init-response') === 1 && s.count('header-batch') === 1 && s.count('payload-msg') === 1,
            20, 3000,
        );
        testing.assertEquals(s.count('new-frontier'), 0, 'nothing is announced until this side has a new entry');
        await d.append({ op: 'later' }, {}, new Set(await d.getFrontier()));
        await waitUntil(async () => s.count('new-frontier') === 1, 20, 3000);
        s.close();
    }

    // 3. A withheld frontier does not count as sent. Once the peer is served,
    //    its next announcement is answered, well inside the 15 s in which a
    //    repeated frontier is held back.
    {
        const { d, root, hashes } = await chainDag(1);
        const serve = { on: false };
        const s = gatedSession(root, d, serve);

        s.say({ type: 'new-frontier', dagId: root, frontier: [root] });
        await wait(200);
        testing.assertEquals(s.given(), 0, 'the answer to the announcement is withheld');

        serve.on = true;
        s.say({ type: 'new-frontier', dagId: root, frontier: [root] });
        await waitUntil(async () => s.count('new-frontier') === 1, 20, 3000);
        const answer = s.sent.find((m) => m.type === 'new-frontier') as NewFrontierMsg;
        testing.assertEquals(answer.frontier.join(','), hashes[0], 'the peer gets this side\'s frontier');
        s.close();
    }

    // 4. A no part way through an answer stops it: no further batch, no
    //    payloads after the headers, and nothing for a request that was
    //    waiting behind it.
    {
        const { d, root, hashes } = await chainDag(300);
        const tip = hashes[hashes.length - 1]!;
        const serve = { on: true };
        const s = gatedSession(root, d, serve);

        s.say({ type: 'header-request', requestId: 'h1', dagId: root, start: [tip], limits: [], maxHeaders: 1024, autoPayload: true });
        await waitUntil(async () => s.count('header-batch') === 1, 5, 3000);
        s.say({ type: 'payload-request', requestId: 'p1', dagId: root, hashes: [tip] });
        await wait(30);
        serve.on = false;
        await wait(500);
        testing.assertEquals(s.count('header-batch'), 1, 'no batch after the no: one of the three went out');
        testing.assertEquals(s.count('payload-msg'), 0, 'no payloads after the headers, and none for the request that waited');
        s.close();
    }

    // 5. An answer that starts later is asked about before its first batch:
    //    the payloads that follow a header answer, and a request that waited
    //    its turn. The provider here says yes to the first question only.
    {
        const { d, root, hashes } = await chainDag(3);
        const tip = hashes[hashes.length - 1]!;
        const { local, remote } = createChannelPair('sync25-provider', 'host', 'peer');
        const got: SyncMsg[] = [];
        fifoOnMessage(remote, (data) => { got.push(decode(data)); });
        const count = (type: SyncMsg['type']) => got.filter((m) => m.type === type).length;

        let asked = 0;
        const afterHeaders = createDagProvider(d, async () => ++asked === 1);
        afterHeaders.handleMessage(
            { type: 'header-request', requestId: 'h1', dagId: root, start: [tip], limits: [], maxHeaders: 1024, autoPayload: true },
            local,
        );
        await wait(250);
        testing.assertEquals(count('header-batch'), 1, 'the header answer goes out on the yes');
        testing.assertEquals(count('payload-msg'), 0, 'its payloads start as an answer of their own, and get the no');
        testing.assertEquals(asked, 2, 'the provider asked once for each');
        afterHeaders.destroy();

        got.length = 0;
        asked = 0;
        wrapLoadEntry(d, 30);
        const queued = createDagProvider(d, async () => ++asked === 1);
        queued.handleMessage({ type: 'payload-request', requestId: 'p1', dagId: root, hashes: hashes.slice(0, 2) }, local);
        queued.handleMessage({ type: 'payload-request', requestId: 'p2', dagId: root, hashes: [tip] }, local);
        await wait(400);
        const payloads = got.filter((m): m is PayloadMsg => m.type === 'payload-msg');
        testing.assertEquals(payloads.map((m) => m.requestId).join(','), 'p1,p1', 'the first request is answered, the one that waited is not');
        queued.destroy();
        local.close();
    }
}

function manualDriver(dagId: B64Hash, dagA: dag.Dag, rObjectA: RObject, opts?: { rejectedCap?: number }) {
    const topic = 'drv';
    const chB = new MockChannel(topic, 'peerB', 'mem://peerB');
    const peerB = { key: 'peerB@mem://peerB', channel: chB };
    const sent: SyncMsg[] = [];
    const reports: IssueReport[] = [];
    const sync = createDagSynchronizer(
        dagId,
        dagA,
        rObjectA,
        sha256,
        () => [peerB],
        (_peer, msg) => { sent.push(msg); return 'sent' as const; },
        undefined,
        { report: (r) => reports.push(r), rejectedCap: opts?.rejectedCap },
    );
    sync.addPeer(peerB);
    return { sync, chB, sent, reports, topic };
}

function lastRequest<T extends 'header-request' | 'payload-request'>(
    sent: SyncMsg[], type: T,
): Extract<SyncMsg, { type: T }> | undefined {
    for (let i = sent.length - 1; i >= 0; i--) {
        if (sent[i].type === type) return sent[i] as Extract<SyncMsg, { type: T }>;
    }
    return undefined;
}

// Feed an auto-payload response (meta + header batch + payload meta + payload
// frames) for `requestId`, sourcing headers/payloads from `dagS`. Payloads are
// sent in the given `hashes` order (pass causal order so predecessors apply
// first). `payloadOverride` lets a test send tampered bytes for a hash.
async function serveAuto(
    driver: ReturnType<typeof manualDriver>,
    requestId: string,
    dagS: dag.Dag,
    hashes: B64Hash[],
    payloadOverride?: Map<B64Hash, json.Literal>,
): Promise<void> {
    const headers: Array<{ hash: B64Hash; header: Header }> = [];
    for (const h of hashes) {
        const header = await dagS.loadHeader(h);
        headers.push({ hash: h, header: header! });
    }
    await driver.sync.handleMessage({
        type: 'header-response-meta', requestId,
        headerCount: hashes.length, complete: true, payloadCount: hashes.length,
    } as HeaderResponseMeta, driver.chB);
    await driver.sync.handleMessage({
        type: 'header-batch', requestId, sequence: 0, headers,
    } as HeaderBatch, driver.chB);
    await driver.sync.handleMessage({
        type: 'payload-response-meta', requestId, payloadCount: hashes.length,
    } as PayloadResponseMeta, driver.chB);
    let seq = 0;
    for (const h of hashes) {
        const payload = payloadOverride?.get(h) ?? (await dagS.loadEntry(h))!.payload;
        await driver.sync.handleMessage({
            type: 'payload-msg', requestId, sequence: seq++, hash: h, payload,
        } as PayloadMsg, driver.chB);
    }
}

async function testRequestIdCollision() {
    const dagId = 'drv-collision';
    const dagS = createTestDag();
    const root = await dagS.append({ op: 'root' }, {});
    const x = await dagS.append({ op: 'X' }, {}, new Set([root]));

    const dagA = createTestDag();
    await dagA.append({ op: 'root' }, {});

    const d = manualDriver(dagId, dagA, createMockRObject(dagA, dagId));

    await d.sync.handleMessage({ type: 'new-frontier', dagId, frontier: [x] } as NewFrontierMsg, d.chB);
    await wait(30);

    const hreq = lastRequest(d.sent, 'header-request');
    testing.assertTrue(hreq !== undefined, 'A should request headers for the unknown frontier hash');
    testing.assertEquals(d.sync.getDiagnostics().pendingHeaderRequests, 1, 'one header request in flight');

    // A frame carrying A's requestId but delivered on a different peer's channel.
    const chC = new MockChannel(d.topic, 'peerC', 'mem://peerC');
    await d.sync.handleMessage({
        type: 'header-response-meta', requestId: hreq!.requestId,
        headerCount: 1, complete: true, payloadCount: 0,
    } as HeaderResponseMeta, chC);

    testing.assertEquals(
        d.sync.getDiagnostics().pendingHeaderRequests, 1,
        'a colliding requestId on another channel must not mutate the real request',
    );
    const collision = d.reports.find((r) => r.kind === 'protocol' && r.keyId === 'peerC');
    testing.assertTrue(collision !== undefined, 'the delivering channel (peerC) is blamed, not peerB');
    testing.assertEquals(collision!.endpoint, 'mem://peerC', 'collision report carries peerC endpoint');
    testing.assertEquals(collision!.severity, 'moderate', 'requestId collision is moderate severity');

    d.sync.destroy();
}

async function testTypeRejectRemembersReportsAndDiscards() {
    const dagId = 'drv-reject';
    const dagS = createTestDag();
    const root = await dagS.append({ op: 'root' }, {});
    const a = await dagS.append({ op: 'A' }, {}, new Set([root]));
    const b = await dagS.append({ op: 'B' }, {}, new Set([a]));
    const c = await dagS.append({ op: 'C' }, {}, new Set([b]));

    const dagA = createTestDag();
    await dagA.append({ op: 'root' }, {});

    const rObjectA = createMockRObject(dagA, dagId, {
        validatePayload: async (payload) =>
            (payload as { op?: string }).op === 'B' ? validationFailure('bad B') : validationOk(),
    });
    const d = manualDriver(dagId, dagA, rObjectA);

    await d.sync.handleMessage({ type: 'new-frontier', dagId, frontier: [c] } as NewFrontierMsg, d.chB);
    await wait(30);
    const hreq = lastRequest(d.sent, 'header-request');
    testing.assertTrue(hreq !== undefined, 'A requests headers');

    await serveAuto(d, hreq!.requestId, dagS, [a, b, c]);
    await wait(50);

    testing.assertTrue((await dagA.loadEntry(a)) !== undefined, 'entry before the reject applies');
    testing.assertTrue((await dagA.loadEntry(b)) === undefined, 'type-rejected entry B is not applied');
    testing.assertTrue((await dagA.loadEntry(c)) === undefined, 'dependent C is discarded, not applied');
    testing.assertEquals(
        d.sync.getDiagnostics().pendingPayloadRequests, 0,
        'the payload stream completes and drains without the 30s timeout',
    );

    const rej = d.reports.find((r) => r.kind === 'validation-failed' && r.opHash === b);
    testing.assertTrue(rej !== undefined, 'type reject is reported with the offending op hash');
    testing.assertEquals(rej!.keyId, 'peerB', 'reject blames the channel that delivered the verified payload');
    testing.assertEquals(rej!.endpoint, 'mem://peerB', 'reject carries the peer endpoint');
    testing.assertEquals(rej!.severity, 'moderate', 'type reject is moderate severity');
    testing.assertTrue(typeof rej!.message === 'string' && rej!.message!.length > 0, 'reject carries a message');

    d.sync.destroy();
}

async function testPayloadHashMismatchNotPoisoned() {
    const dagId = 'drv-mismatch';
    const dagS = createTestDag();
    const root = await dagS.append({ op: 'root' }, {});
    const x = await dagS.append({ op: 'X' }, {}, new Set([root]));

    const dagA = createTestDag();
    await dagA.append({ op: 'root' }, {});

    const d = manualDriver(dagId, dagA, createMockRObject(dagA, dagId));

    await d.sync.handleMessage({ type: 'new-frontier', dagId, frontier: [x] } as NewFrontierMsg, d.chB);
    await wait(30);
    const hreq = lastRequest(d.sent, 'header-request');

    // Deliver a payload whose bytes do not hash to header.payloadHash.
    await serveAuto(d, hreq!.requestId, dagS, [x], new Map([[x, { op: 'TAMPERED' }]]));
    await wait(30);

    const mm = d.reports.find((r) => r.kind === 'hash-mismatch' && r.opHash === x);
    testing.assertTrue(mm !== undefined, 'payload hash mismatch is reported');
    testing.assertEquals(mm!.severity, 'high', 'hash mismatch is high severity');
    testing.assertTrue((await dagA.loadEntry(x)) === undefined, 'the tampered payload is not applied');

    // Not poisoned: A re-fetches the payload; correct bytes then apply.
    await wait(30);
    const preq = lastRequest(d.sent, 'payload-request');
    testing.assertTrue(preq !== undefined && preq!.hashes.includes(x), 'A re-requests the payload (hash was not remembered)');
    await d.sync.handleMessage({ type: 'payload-response-meta', requestId: preq!.requestId, payloadCount: 1 } as PayloadResponseMeta, d.chB);
    await d.sync.handleMessage({
        type: 'payload-msg', requestId: preq!.requestId, sequence: 0, hash: x,
        payload: (await dagS.loadEntry(x))!.payload,
    } as PayloadMsg, d.chB);
    await wait(30);
    testing.assertTrue((await dagA.loadEntry(x)) !== undefined, 'a correct payload applies; the mismatch did not poison the hash');

    d.sync.destroy();
}

async function testRejectedCapEviction() {
    const dagId = 'drv-cap';
    const dagS = createTestDag();
    const root = await dagS.append({ op: 'root' }, {});
    const px = await dagS.append({ op: 'PX' }, {}, new Set([root]));
    const py = await dagS.append({ op: 'PY' }, {}, new Set([root]));

    const dagA = createTestDag();
    await dagA.append({ op: 'root' }, {});

    const rObjectA = createMockRObject(dagA, dagId, {
        validatePayload: async (payload) => {
            const op = (payload as { op?: string }).op;
            return op === 'PX' || op === 'PY' ? validationFailure(`bad ${op}`) : validationOk();
        },
    });
    // Cap of 1: rejecting a second hash evicts the first.
    const d = manualDriver(dagId, dagA, rObjectA, { rejectedCap: 1 });

    await d.sync.handleMessage({ type: 'new-frontier', dagId, frontier: [px, py] } as NewFrontierMsg, d.chB);
    await wait(30);
    const hreq1 = lastRequest(d.sent, 'header-request');
    await serveAuto(d, hreq1!.requestId, dagS, [px, py]);
    await wait(40);

    const countReject = (h: B64Hash) => d.reports.filter((r) => r.kind === 'validation-failed' && r.opHash === h).length;
    testing.assertEquals(countReject(px), 1, 'PX rejected once in phase 1');
    testing.assertEquals(countReject(py), 1, 'PY rejected once in phase 1 (evicts PX from the cap-1 set)');

    // Re-advertise: PX was evicted so it is fetched and processed again; PY is
    // still remembered so it is not even requested, and its header is skipped.
    await d.sync.handleMessage({ type: 'new-frontier', dagId, frontier: [px, py] } as NewFrontierMsg, d.chB);
    await wait(30);
    const hreq2 = lastRequest(d.sent, 'header-request');
    testing.assertTrue(hreq2 !== undefined && hreq2!.requestId !== hreq1!.requestId, 'a fresh header request is issued');
    testing.assertTrue(hreq2!.start.includes(px), 'the evicted hash PX is requested again');
    testing.assertTrue(!hreq2!.start.includes(py), 'the still-remembered hash PY is not requested');

    await serveAuto(d, hreq2!.requestId, dagS, [px, py]);
    await wait(40);
    testing.assertEquals(countReject(px), 2, 'evicted PX is admitted again and re-rejected');
    testing.assertEquals(countReject(py), 1, 'remembered PY is skipped on re-admission (no second reject)');
    testing.assertEquals(d.sync.getDiagnostics().pendingPayloadRequests, 0, 'no leftover payload wait');

    testing.assertTrue(REJECTED_ENTRIES_CAP === 4096, 'production cap stays at 4096');

    d.sync.destroy();
}

async function testValidateThrowDefersNotRejects() {
    const dagId = 'drv-throw-defer';
    const dagS = createTestDag();
    const root = await dagS.append({ op: 'root' }, {});
    const x = await dagS.append({ op: 'X' }, {}, new Set([root]));
    const y = await dagS.append({ op: 'Y' }, {}, new Set([root]));

    const dagA = createTestDag();
    await dagA.append({ op: 'root' }, {});

    // X's validation throws a plain Error (as a missing-replica-data path would)
    // until the flag clears; Y always validates OK.
    let failX = true;
    const rObjectA = createMockRObject(dagA, dagId, {
        validatePayload: async (payload) => {
            if ((payload as { op?: string }).op === 'X' && failX) {
                throw new Error('transient infrastructure error: dependency not ready');
            }
            return validationOk();
        },
    });
    const d = manualDriver(dagId, dagA, rObjectA);

    await d.sync.handleMessage({ type: 'new-frontier', dagId, frontier: [x, y] } as NewFrontierMsg, d.chB);
    await wait(30);
    const hreq = lastRequest(d.sent, 'header-request');
    testing.assertTrue(hreq !== undefined, 'A requests headers');

    await serveAuto(d, hreq!.requestId, dagS, [x, y]);
    await wait(50);

    // X threw -> deferred; the independent sibling Y still applied in the same
    // pass, so one throwing hash does not abort the rest of the pass.
    testing.assertTrue((await dagA.loadEntry(y)) !== undefined, 'a sibling that validates OK applies despite X throwing');
    testing.assertTrue((await dagA.loadEntry(x)) === undefined, 'the throwing entry X is not applied');

    // A throw is a defer, not a reject: no validation-failed report for X.
    testing.assertTrue(
        !d.reports.some((r) => r.kind === 'validation-failed' && r.opHash === x),
        'an infrastructure throw is deferred, not reported as a validation failure',
    );
    testing.assertEquals(
        d.sync.getDiagnostics().pendingPayloadRequests, 0,
        'X payload was received (and deferred), so the payload stream drains without a 30s stall',
    );

    // Stop throwing and wake the loop. X applies -- proving it was kept in
    // readyToApply (deferred), not rejected/remembered (a reject would evict it
    // and it would never re-admit without a fresh fetch).
    failX = false;
    await d.sync.handleMessage({ type: 'new-frontier', dagId, frontier: [x, y] } as NewFrontierMsg, d.chB);
    await wait(50);
    testing.assertTrue((await dagA.loadEntry(x)) !== undefined, 'once the throw clears, the deferred entry applies on the next wake');

    d.sync.destroy();
}

export const syncSuite = {
    title: '[SYNC] DAG sync protocol',
    tests: [
        { name: '[SYNC_00] Codec round-trip', invoke: testCodecRoundTrip },
        { name: '[SYNC_01] Provider header BFS walk', invoke: testProviderHeaderBFS },
        { name: '[SYNC_02] Provider payload serving', invoke: testProviderPayloadServing },
        { name: '[SYNC_03] Cancel request', invoke: testCancelRequest },
        { name: '[SYNC_04] Small divergence sync', invoke: testSmallDivergence },
        { name: '[SYNC_05] Y-fork sync', invoke: testYFork },
        { name: '[SYNC_06] Large divergence sync (40 entries)', invoke: testLargeDivergence },
        { name: '[SYNC_07] Frontier during sync', invoke: testFrontierDuringSync },
        { name: '[SYNC_08] Multi-peer sync', invoke: testMultiPeerSync },
        { name: '[SYNC_09] Foreign dep deferral', invoke: testForeignDepDeferral },
        { name: '[SYNC_10] fetchInit rejects createPayload hash mismatch', invoke: testFetchInitHashMismatch },
        { name: '[SYNC_19] fetchInit accepts a peer that arrives after the init budget', invoke: testFetchInitLatePeer },
        { name: '[SYNC_20] fetchInit rejects on the peer budget when nobody arrives', invoke: testFetchInitNoPeer },
        { name: '[SYNC_21] fetchInit with a peer already present times out on the init budget', invoke: testFetchInitAlreadyConnectedTimeout },
        { name: '[SYNC_11] Foreign dep appearance then growth', invoke: testForeignDepAppearance },
        { name: '[SYNC_12] Auto-payload FIFO survives yielding loadHeader', invoke: testAutoPayloadFifo },
        { name: '[SYNC_13] Auto-payload payloadCount mismatch fails', invoke: testAutoPayloadCountMismatch },
        { name: '[SYNC_14] requestId collision on another channel is dropped', invoke: testRequestIdCollision },
        { name: '[SYNC_15] Type reject remembers, reports, discards dependents, no stall', invoke: testTypeRejectRemembersReportsAndDiscards },
        { name: '[SYNC_16] Payload hash mismatch is not poisoned', invoke: testPayloadHashMismatchNotPoisoned },
        { name: '[SYNC_17] rejectedEntries eviction re-admits oldest, keeps newest', invoke: testRejectedCapEviction },
        { name: '[SYNC_18] validate/apply throw defers (not rejects) and does not abort the pass', invoke: testValidateThrowDefersNotRejects },
        { name: '[SYNC_22] Codec rejects a message that is not a Literal', invoke: testCodecRejectsNonLiterals },
        { name: '[SYNC_23] fetchInit ignores a create payload with an ill-formed string', invoke: testFetchInitIgnoresIllFormedCreatePayload },
        { name: '[SYNC_24] Codec rejects a payload nested deeper than 128 levels', invoke: testCodecRejectsDeepNesting },
        { name: '[SYNC_25] A session gives entries only to the peers its swarm serves', invoke: testServeCheck },
    ],
};
