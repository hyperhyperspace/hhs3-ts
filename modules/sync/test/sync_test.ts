import { testing } from '@hyper-hyper-space/hhs3_util';
import { B64Hash, sha256, stringToUint8Array } from '@hyper-hyper-space/hhs3_crypto';
import { dag, Position, Header, Entry } from '@hyper-hyper-space/hhs3_dag';
import { json } from '@hyper-hyper-space/hhs3_json';
import type { TopicChannel } from '@hyper-hyper-space/hhs3_mesh';
import type { RObject, Version, Payload, View, Event } from '@hyper-hyper-space/hhs3_mvt';

import { createDagProvider } from '../src/provider.js';
import { createDagSynchronizer } from '../src/synchronizer.js';
import { encode, decode } from '../src/codec.js';
import type { SyncMsg, NewFrontierMsg } from '../src/protocol.js';

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

function createChannelPair(topic: string, peerAId: string, peerBId: string): ChannelPair {
    const local = new MockChannel(topic, peerBId, `mem://${peerBId}`);
    const remote = new MockChannel(topic, peerAId, `mem://${peerAId}`);
    local.peer = remote;
    remote.peer = local;
    return { local, remote };
}

function createMockRObject(d: dag.Dag, id: B64Hash): RObject {
    return {
        getId: () => id,
        getType: () => 'test-object',
        validatePayload: async (_payload: Payload, _at: Version) => true,
        applyPayload: async (payload: Payload, at: Version) => {
            return await d.append(payload, {}, at);
        },
        getView: async (_at?: Version, _from?: Version): Promise<View> => { throw new Error('not implemented'); },
        subscribe: (_cb: (event: Event) => void) => {},
        unsubscribe: (_cb: (event: Event) => void) => {},
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
        (peer, msg) => { try { peer.channel.send(encode(msg)); } catch {} },
    );

    // chBLocal.onMessage: fires when chALocal.send() delivers → A sent to B → provider handles
    chBLocal.onMessage((data) => {
        const msg = decode(data);
        providerB.handleMessage(msg, chBLocal);
    });

    // chALocal.onMessage: fires when chBLocal.send() delivers → B sent to A → synchronizer handles
    chALocal.onMessage((data) => {
        const msg = decode(data);
        synchronizerA.handleMessage(msg, chALocal);
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
        (peer, msg) => { try { peer.channel.send(encode(msg)); } catch {} },
    );

    // Wire B's provider
    chBA.onMessage((data) => {
        providerB.handleMessage(decode(data), chBA);
    });
    chAB.onMessage((data) => {
        synchronizerA.handleMessage(decode(data), chAB);
    });

    // Wire C's provider
    chCA.onMessage((data) => {
        providerC.handleMessage(decode(data), chCA);
    });
    chAC.onMessage((data) => {
        synchronizerA.handleMessage(decode(data), chAC);
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
    ],
};
