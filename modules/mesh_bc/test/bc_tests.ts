import { testing } from '@hyper-hyper-space/hhs3_util';
import type { KeyId } from '@hyper-hyper-space/hhs3_crypto';
import type { PeerInfo, TopicId, Transport } from '@hyper-hyper-space/hhs3_mesh';
import {
    BroadcastChannelDiscovery,
    BroadcastChannelTransportProvider,
    type BroadcastChannelCtor,
    type BroadcastChannelLike,
} from '../src/index.js';

// In-process BroadcastChannel shim: same-name instances share a bus, the
// sender never receives its own message (WHATWG semantics).

const buses = new Map<string, Set<FakeBroadcastChannel>>();

class FakeBroadcastChannel implements BroadcastChannelLike {
    readonly name: string;
    onmessage: ((ev: MessageEvent) => void) | null = null;
    private closed = false;

    constructor(name: string) {
        this.name = name;
        let set = buses.get(name);
        if (set === undefined) {
            set = new Set();
            buses.set(name, set);
        }
        set.add(this);
    }

    postMessage(message: unknown): void {
        if (this.closed) return;
        const set = buses.get(this.name);
        if (set === undefined) return;
        const copy = structuredClone(message);
        for (const peer of set) {
            if (peer === this || peer.closed) continue;
            const ev = { data: copy } as MessageEvent;
            queueMicrotask(() => {
                if (!peer.closed) peer.onmessage?.(ev);
            });
        }
    }

    close(): void {
        if (this.closed) return;
        this.closed = true;
        this.onmessage = null;
        buses.get(this.name)?.delete(this);
    }
}

const FakeCtor = FakeBroadcastChannel as unknown as BroadcastChannelCtor;

let seq = 0;
function nextBase(): string {
    seq++;
    return `hhs3-bc-test-${seq}`;
}

function delay(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) {
        if (a[i] !== b[i]) return false;
    }
    return true;
}

function provider(base: string, endpointId: string): BroadcastChannelTransportProvider {
    return new BroadcastChannelTransportProvider({
        base,
        endpointId,
        BroadcastChannelCtor: FakeCtor,
        connectTimeoutMs: 150,
    });
}

function discovery(base: string, self: PeerInfo): BroadcastChannelDiscovery {
    return new BroadcastChannelDiscovery({
        self,
        base,
        BroadcastChannelCtor: FakeCtor,
        collectWindowMs: 30,
    });
}

async function collect(
    d: BroadcastChannelDiscovery,
    topic: TopicId,
    schemes?: string[],
): Promise<PeerInfo[]> {
    const out: PeerInfo[] = [];
    for await (const p of d.discover(topic, schemes)) out.push(p);
    return out;
}

// --- transport ---

async function testSchemeAndLocalAddress() {
    const p = provider(nextBase(), 'ep-a');
    testing.assertEquals(p.scheme, 'bc', 'scheme should be bc');
    testing.assertEquals(p.localAddress, 'bc://ep-a', 'localAddress from endpointId');
    p.close();
}

async function testListenRejectsForeignAddress() {
    const p = provider(nextBase(), 'ep-a');
    let threw = false;
    try {
        await p.listen('bc://other', () => {});
    } catch (e) {
        threw = true;
        testing.assertTrue(
            (e as Error).message.includes('cannot listen'),
            'listen mismatch should mention cannot listen',
        );
    }
    testing.assertTrue(threw, 'listen on a foreign address should throw');
    p.close();
}

async function testConnectAcceptHandshake() {
    const base = nextBase();
    const a = provider(base, 'ep-a');
    const b = provider(base, 'ep-b');
    let inbound: Transport | undefined;
    await b.listen(b.localAddress, (t) => { inbound = t; });

    const client = await a.connect(b.localAddress);
    await delay(10);

    testing.assertTrue(inbound !== undefined, 'listener should receive inbound transport');
    testing.assertEquals(client.remoteAddress, b.localAddress, 'client remote is b');
    testing.assertEquals(inbound!.remoteAddress, a.localAddress, 'server remote is a');

    a.close();
    b.close();
}

async function testSendReceiveRoundTrip() {
    const base = nextBase();
    const a = provider(base, 'ep-a');
    const b = provider(base, 'ep-b');
    let inbound: Transport | undefined;
    await b.listen(b.localAddress, (t) => { inbound = t; });
    const client = await a.connect(b.localAddress);
    await delay(10);

    const serverReceived: Uint8Array[] = [];
    const clientReceived: Uint8Array[] = [];
    inbound!.onMessage((msg) => {
        serverReceived.push(msg);
        inbound!.send(msg);
    });
    client.onMessage((msg) => clientReceived.push(msg));

    const message = new TextEncoder().encode('hello bc');
    client.send(message);
    await delay(20);

    testing.assertEquals(serverReceived.length, 1, 'server should receive one message');
    testing.assertTrue(bytesEqual(serverReceived[0], message), 'server content matches');
    testing.assertEquals(clientReceived.length, 1, 'client should receive echo');
    testing.assertTrue(bytesEqual(clientReceived[0], message), 'echo content matches');

    a.close();
    b.close();
}

async function testBidirectionalAndBinary() {
    const base = nextBase();
    const a = provider(base, 'ep-a');
    const b = provider(base, 'ep-b');
    let inbound: Transport | undefined;
    await b.listen(b.localAddress, (t) => { inbound = t; });
    const client = await a.connect(b.localAddress);
    await delay(10);

    const fromClient: Uint8Array[] = [];
    const fromServer: Uint8Array[] = [];
    inbound!.onMessage((msg) => fromClient.push(msg));
    client.onMessage((msg) => fromServer.push(msg));

    const binary = new Uint8Array(256);
    for (let i = 0; i < 256; i++) binary[i] = i;

    client.send(binary);
    inbound!.send(new TextEncoder().encode('to-client'));
    await delay(20);

    testing.assertEquals(fromClient.length, 1, 'server got client binary');
    testing.assertTrue(bytesEqual(fromClient[0], binary), 'binary integrity');
    testing.assertEquals(fromServer.length, 1, 'client got server text');

    a.close();
    b.close();
}

async function testClosePropagation() {
    const base = nextBase();
    const a = provider(base, 'ep-a');
    const b = provider(base, 'ep-b');
    let inbound: Transport | undefined;
    await b.listen(b.localAddress, (t) => { inbound = t; });
    const client = await a.connect(b.localAddress);
    await delay(10);

    let clientClosed = false;
    let serverClosed = false;
    client.onClose(() => { clientClosed = true; });
    inbound!.onClose(() => { serverClosed = true; });

    client.close();
    await delay(20);

    testing.assertTrue(clientClosed, 'client onClose fires');
    testing.assertTrue(serverClosed, 'server onClose fires from remote close');

    a.close();
    b.close();
}

async function testConnectTimeoutWithoutListen() {
    const base = nextBase();
    const a = provider(base, 'ep-a');
    const b = provider(base, 'ep-b');
    // b never listen()s
    let threw = false;
    try {
        await a.connect(b.localAddress);
    } catch (e) {
        threw = true;
        testing.assertTrue(
            (e as Error).message.includes('timeout'),
            'connect should time out',
        );
    }
    testing.assertTrue(threw, 'connect without listen should reject');
    a.close();
    b.close();
}

async function testProviderClose() {
    const base = nextBase();
    const a = provider(base, 'ep-a');
    const b = provider(base, 'ep-b');
    let inbound: Transport | undefined;
    await b.listen(b.localAddress, (t) => { inbound = t; });
    const client = await a.connect(b.localAddress);
    await delay(10);

    let clientClosed = false;
    client.onClose(() => { clientClosed = true; });
    a.close();
    await delay(20);
    testing.assertTrue(clientClosed, 'provider close should close transports');
    testing.assertTrue(inbound !== undefined, 'inbound existed');
    b.close();
}

// --- discovery ---

const topic = 'topic-bc' as TopicId;
const aliceId = 'alice' as KeyId;
const bobId = 'bob' as KeyId;

async function testDiscoverySnapshot() {
    const base = nextBase();
    const a = discovery(base, { keyId: aliceId, addresses: ['bc://alice'] });
    const b = discovery(base, { keyId: bobId, addresses: ['bc://bob'] });
    await a.announce(topic, a.self);
    await b.announce(topic, b.self);

    const peers = await collect(a, topic);
    testing.assertEquals(peers.length, 1, 'alice should see bob');
    testing.assertEquals(peers[0].keyId, bobId, 'peer keyId is bob');
    testing.assertEquals(peers[0].addresses[0], 'bc://bob', 'peer address is bob');

    a.close();
    b.close();
}

async function testLeaveUnregisters() {
    const base = nextBase();
    const a = discovery(base, { keyId: aliceId, addresses: ['bc://alice'] });
    const b = discovery(base, { keyId: bobId, addresses: ['bc://bob'] });
    await a.announce(topic, a.self);
    await b.announce(topic, b.self);

    testing.assertEquals((await collect(a, topic)).length, 1, 'bob visible before leave');
    await b.leave(topic, bobId);
    testing.assertEquals((await collect(a, topic)).length, 0, 'bob gone after leave');

    a.close();
    b.close();
}

async function testDiscoverySchemeFilter() {
    const base = nextBase();
    const a = discovery(base, { keyId: aliceId, addresses: ['bc://alice'] });
    const b = discovery(base, { keyId: bobId, addresses: ['bc://bob', 'ws://x'] });
    await a.announce(topic, a.self);
    await b.announce(topic, b.self);

    const bcOnly = await collect(a, topic, ['bc']);
    testing.assertEquals(bcOnly.length, 1, 'bc scheme keeps bob');
    testing.assertEquals(bcOnly[0].addresses.length, 1, 'ws address filtered out');
    testing.assertEquals(bcOnly[0].addresses[0], 'bc://bob', 'bc address kept');

    const memOnly = await collect(a, topic, ['mem']);
    testing.assertEquals(memOnly.length, 0, 'mem scheme drops bob');

    a.close();
    b.close();
}

async function testDiscoverWithoutAnnounceDoesNotAnswer() {
    const base = nextBase();
    const a = discovery(base, { keyId: aliceId, addresses: ['bc://alice'] });
    const b = discovery(base, { keyId: bobId, addresses: ['bc://bob'] });
    // b never announces
    await a.announce(topic, a.self);
    const peers = await collect(a, topic);
    testing.assertEquals(peers.length, 0, 'silent peer should not appear');
    a.close();
    b.close();
}

export const bcTests = {
    title: '[MESH_BC] BroadcastChannel transport + discovery',
    tests: [
        { name: '[BC_00] scheme and localAddress', invoke: testSchemeAndLocalAddress },
        { name: '[BC_01] listen rejects foreign address', invoke: testListenRejectsForeignAddress },
        { name: '[BC_02] connect/accept handshake', invoke: testConnectAcceptHandshake },
        { name: '[BC_03] send/receive round-trip', invoke: testSendReceiveRoundTrip },
        { name: '[BC_04] bidirectional binary', invoke: testBidirectionalAndBinary },
        { name: '[BC_05] close propagation', invoke: testClosePropagation },
        { name: '[BC_06] connect timeout without listen', invoke: testConnectTimeoutWithoutListen },
        { name: '[BC_07] provider close', invoke: testProviderClose },
        { name: '[BC_08] discovery snapshot', invoke: testDiscoverySnapshot },
        { name: '[BC_09] leave unregisters', invoke: testLeaveUnregisters },
        { name: '[BC_10] discovery scheme filter', invoke: testDiscoverySchemeFilter },
        { name: '[BC_11] no announce means no answer', invoke: testDiscoverWithoutAnnounceDoesNotAnswer },
    ],
};
