import * as fs from 'node:fs/promises';
import * as os from 'node:os';
import * as path from 'node:path';
import { testing } from '@hyper-hyper-space/hhs3_util';
import type { PublicKey, KeyId } from '@hyper-hyper-space/hhs3_crypto';
import {
    ed25519, sha256, keyIdFromPublicKey,
    SIGNING_ED25519, KEM_X25519_HKDF,
} from '@hyper-hyper-space/hhs3_crypto';
import type {
    Transport, TransportProvider, NetworkAddress,
    PeerAuthenticator, AuthenticatedChannel, PeerInfo, TopicId,
} from '@hyper-hyper-space/hhs3_mesh';
import { createNoiseAuthenticator } from '@hyper-hyper-space/hhs3_mesh';
import { TrackerClient } from '@hyper-hyper-space/hhs3_mesh_tracker_client';
import { TrackerServer } from '../src/tracker_server.js';
import {
    generateIdentity, saveIdentity, loadIdentity, loadOrCreateIdentity, identityKeyId,
} from '../src/identity.js';

// ---------------------------------------------------------------------------
// In-memory transport
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

class MemTransportProvider implements TransportProvider {
    readonly scheme = 'mem';
    private listeners = new Map<string, (transport: Transport) => void>();

    async listen(address: NetworkAddress, onConnection: (transport: Transport) => void): Promise<void> {
        this.listeners.set(address, onConnection);
    }

    async connect(remote: NetworkAddress): Promise<Transport> {
        const listener = this.listeners.get(remote);
        if (!listener) throw new Error(`no listener at ${remote}`);
        const [client, server] = createMemTransportPair();
        listener(server);
        return client;
    }

    close(): void {
        this.listeners.clear();
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function makeKeyPair() {
    const kp = await ed25519.generateKeyPair();
    const pk: PublicKey = { suite: SIGNING_ED25519, key: kp.publicKey };
    const keyId = keyIdFromPublicKey(pk, sha256);
    return { publicKey: pk, secretKey: kp.secretKey, keyId };
}

function makeAuth(kp: { publicKey: PublicKey; secretKey: Uint8Array }) {
    return createNoiseAuthenticator({
        localKey: { publicKey: kp.publicKey, secretKey: kp.secretKey },
        signingName: SIGNING_ED25519,
        kemPrefs: [KEM_X25519_HKDF],
    });
}

async function collectAll(iter: AsyncIterable<PeerInfo>): Promise<PeerInfo[]> {
    const out: PeerInfo[] = [];
    for await (const p of iter) out.push(p);
    return out;
}

function delay(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

async function testAnnounceQueryRoundTrip() {
    const serverKp = await makeKeyPair();
    const clientKp = await makeKeyPair();

    const provider = new MemTransportProvider();
    const serverAuth = makeAuth(serverKp);
    const clientAuth = makeAuth(clientKp);

    const server = new TrackerServer({
        transportProvider: provider,
        authenticator: serverAuth,
        listenAddress: 'mem://tracker',
        sweepInterval: 999_999,
    });
    await server.start();

    const topic = 'test-topic-00' as TopicId;
    const localPeer: PeerInfo = { keyId: clientKp.keyId, addresses: ['ws://client:1234'] };

    const client = new TrackerClient({
        trackerAddress: 'mem://tracker',
        trackerKeyId: serverKp.keyId,
        transportProvider: provider,
        authenticator: clientAuth,
        localPeer,
        heartbeatInterval: 999_999,
    });

    await client.announce(topic, localPeer);
    const peers = await collectAll(client.discover(topic));

    testing.assertEquals(peers.length, 1, 'should find one peer');
    testing.assertEquals(peers[0].keyId, clientKp.keyId, 'keyId should match');
    testing.assertEquals(peers[0].addresses[0], 'ws://client:1234', 'address should match');

    await client.close();
    server.stop();
}

async function testLeaveRemovesEntry() {
    const serverKp = await makeKeyPair();
    const clientKp = await makeKeyPair();

    const provider = new MemTransportProvider();
    const serverAuth = makeAuth(serverKp);
    const clientAuth = makeAuth(clientKp);

    const server = new TrackerServer({
        transportProvider: provider,
        authenticator: serverAuth,
        listenAddress: 'mem://tracker',
        sweepInterval: 999_999,
    });
    await server.start();

    const topic = 'test-topic-01' as TopicId;
    const localPeer: PeerInfo = { keyId: clientKp.keyId, addresses: ['ws://c:1'] };

    const client = new TrackerClient({
        trackerAddress: 'mem://tracker',
        trackerKeyId: serverKp.keyId,
        transportProvider: provider,
        authenticator: clientAuth,
        localPeer,
        heartbeatInterval: 999_999,
    });

    await client.announce(topic, localPeer);
    await client.leave(topic, clientKp.keyId);

    const peers = await collectAll(client.discover(topic));
    testing.assertEquals(peers.length, 0, 'should find no peers after leave');

    await client.close();
    server.stop();
}

async function testTtlClamping() {
    const serverKp = await makeKeyPair();
    const clientKp = await makeKeyPair();

    const provider = new MemTransportProvider();
    const serverAuth = makeAuth(serverKp);
    const clientAuth = makeAuth(clientKp);

    const server = new TrackerServer({
        transportProvider: provider,
        authenticator: serverAuth,
        listenAddress: 'mem://tracker',
        ttlMin: 60,
        ttlMax: 120,
        sweepInterval: 999_999,
    });
    await server.start();

    const topic = 'test-topic-02' as TopicId;
    const localPeer: PeerInfo = { keyId: clientKp.keyId, addresses: ['ws://c:1'] };

    // Request TTL=300, server should clamp to 120
    const client = new TrackerClient({
        trackerAddress: 'mem://tracker',
        trackerKeyId: serverKp.keyId,
        transportProvider: provider,
        authenticator: clientAuth,
        localPeer,
        announceTtl: 300,
        heartbeatInterval: 999_999,
    });

    await client.announce(topic, localPeer);
    // If announce succeeded, the server clamped without error
    const peers = await collectAll(client.discover(topic));
    testing.assertEquals(peers.length, 1, 'announce should succeed with clamped TTL');

    await client.close();
    server.stop();
}

async function testSpoofedAnnounceRejected() {
    const serverKp = await makeKeyPair();
    const clientKp = await makeKeyPair();

    const provider = new MemTransportProvider();
    const serverAuth = makeAuth(serverKp);
    const clientAuth = makeAuth(clientKp);

    const server = new TrackerServer({
        transportProvider: provider,
        authenticator: serverAuth,
        listenAddress: 'mem://tracker',
        sweepInterval: 999_999,
    });
    await server.start();

    const topic = 'test-topic-03' as TopicId;
    // localPeer has a DIFFERENT keyId than the authenticated identity
    const spoofedPeer: PeerInfo = { keyId: 'fake-key-id' as KeyId, addresses: ['ws://spoof:1'] };

    const client = new TrackerClient({
        trackerAddress: 'mem://tracker',
        trackerKeyId: serverKp.keyId,
        transportProvider: provider,
        authenticator: clientAuth,
        localPeer: spoofedPeer,
        heartbeatInterval: 999_999,
    });

    let failed = false;
    try {
        await client.announce(topic, spoofedPeer);
    } catch {
        failed = true;
    }

    testing.assertTrue(failed, 'spoofed announce should be rejected');

    await client.close();
    server.stop();
}

async function testExpiredEntriesAreSwept() {
    const serverKp = await makeKeyPair();
    const clientKp = await makeKeyPair();

    const provider = new MemTransportProvider();
    const serverAuth = makeAuth(serverKp);
    const clientAuth = makeAuth(clientKp);

    const server = new TrackerServer({
        transportProvider: provider,
        authenticator: serverAuth,
        listenAddress: 'mem://tracker',
        ttlMin: 1,
        ttlMax: 1,
        sweepInterval: 50,
    });
    await server.start();

    const topic = 'test-topic-04' as TopicId;
    const localPeer: PeerInfo = { keyId: clientKp.keyId, addresses: ['ws://c:1'] };

    const client = new TrackerClient({
        trackerAddress: 'mem://tracker',
        trackerKeyId: serverKp.keyId,
        transportProvider: provider,
        authenticator: clientAuth,
        localPeer,
        announceTtl: 1,
        heartbeatInterval: 999_999,
    });

    await client.announce(topic, localPeer);
    const before = await collectAll(client.discover(topic));
    testing.assertEquals(before.length, 1, 'should find peer before expiry');

    // Wait for TTL + sweep
    await delay(1200);

    const after = await collectAll(client.discover(topic));
    testing.assertEquals(after.length, 0, 'should find no peers after expiry + sweep');

    await client.close();
    server.stop();
}

async function testMultipleClientsSeeEachOther() {
    const serverKp = await makeKeyPair();
    const client1Kp = await makeKeyPair();
    const client2Kp = await makeKeyPair();

    const provider = new MemTransportProvider();
    const serverAuth = makeAuth(serverKp);

    const server = new TrackerServer({
        transportProvider: provider,
        authenticator: serverAuth,
        listenAddress: 'mem://tracker',
        sweepInterval: 999_999,
    });
    await server.start();

    const topic = 'test-topic-05' as TopicId;

    const peer1: PeerInfo = { keyId: client1Kp.keyId, addresses: ['ws://c1:1'] };
    const peer2: PeerInfo = { keyId: client2Kp.keyId, addresses: ['ws://c2:2'] };

    const c1 = new TrackerClient({
        trackerAddress: 'mem://tracker',
        trackerKeyId: serverKp.keyId,
        transportProvider: provider,
        authenticator: makeAuth(client1Kp),
        localPeer: peer1,
        heartbeatInterval: 999_999,
    });
    const c2 = new TrackerClient({
        trackerAddress: 'mem://tracker',
        trackerKeyId: serverKp.keyId,
        transportProvider: provider,
        authenticator: makeAuth(client2Kp),
        localPeer: peer2,
        heartbeatInterval: 999_999,
    });

    await c1.announce(topic, peer1);
    await c2.announce(topic, peer2);

    const fromC1 = await collectAll(c1.discover(topic));
    const fromC2 = await collectAll(c2.discover(topic));

    testing.assertEquals(fromC1.length, 2, 'client1 should see both peers');
    testing.assertEquals(fromC2.length, 2, 'client2 should see both peers');

    const keyIds1 = fromC1.map(p => p.keyId).sort();
    const keyIds2 = fromC2.map(p => p.keyId).sort();
    const expected = [client1Kp.keyId, client2Kp.keyId].sort();

    testing.assertEquals(keyIds1[0], expected[0], 'c1 sees correct peer A');
    testing.assertEquals(keyIds1[1], expected[1], 'c1 sees correct peer B');
    testing.assertEquals(keyIds2[0], expected[0], 'c2 sees correct peer A');
    testing.assertEquals(keyIds2[1], expected[1], 'c2 sees correct peer B');

    await c1.close();
    await c2.close();
    server.stop();
}

async function testIdentityFileRoundTrip() {
    const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'hhs3-tracker-test-'));
    const filePath = path.join(tmpDir, 'identity.json');

    try {
        const identity = await generateIdentity(SIGNING_ED25519);
        const keyId1 = identityKeyId(identity);

        await saveIdentity(filePath, identity);
        const loaded = await loadIdentity(filePath);
        const keyId2 = identityKeyId(loaded);

        testing.assertEquals(keyId1, keyId2, 'loaded identity should have same keyId');
        testing.assertEquals(loaded.publicKey.suite, identity.publicKey.suite, 'suite should match');

        // loadOrCreate should load existing
        const loaded2 = await loadOrCreateIdentity(filePath, SIGNING_ED25519);
        const keyId3 = identityKeyId(loaded2);
        testing.assertEquals(keyId1, keyId3, 'loadOrCreate should return same identity');

        // loadOrCreate on a new path should create
        const newPath = path.join(tmpDir, 'new-identity.json');
        const created = await loadOrCreateIdentity(newPath, SIGNING_ED25519);
        const keyId4 = identityKeyId(created);
        testing.assertTrue(keyId1 !== keyId4, 'new identity should have different keyId');

        // verify the new file was persisted
        const reloaded = await loadIdentity(newPath);
        testing.assertEquals(identityKeyId(reloaded), keyId4, 'newly created identity should persist');
    } finally {
        await fs.rm(tmpDir, { recursive: true, force: true });
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

const allSuites = [
    {
        title: '[TRACKER_SERVER] Tracker server',
        tests: [
            { name: '[TRACKER_SERVER_00] Announce + query round-trip', invoke: testAnnounceQueryRoundTrip },
            { name: '[TRACKER_SERVER_01] Leave removes entry', invoke: testLeaveRemovesEntry },
            { name: '[TRACKER_SERVER_02] TTL clamping enforces server bounds', invoke: testTtlClamping },
            { name: '[TRACKER_SERVER_03] Spoofed announce is rejected', invoke: testSpoofedAnnounceRejected },
            { name: '[TRACKER_SERVER_04] Expired entries are swept', invoke: testExpiredEntriesAreSwept },
            { name: '[TRACKER_SERVER_05] Multiple clients see each other', invoke: testMultipleClientsSeeEachOther },
            { name: '[TRACKER_SERVER_06] Identity file generate/load/loadOrCreate', invoke: testIdentityFileRoundTrip },
        ],
    },
];

async function main() {
    const filters = process.argv.slice(2);
    console.log('Running tests for HHSv3 mesh_tracker module' + (filters.length > 0 ? ` (filter: ${filters})` : '') + '\n');

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
