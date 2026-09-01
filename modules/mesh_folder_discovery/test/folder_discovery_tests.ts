import { mkdir, mkdtemp, readdir, rm, utimes, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { testing } from '@hyper-hyper-space/hhs3_util';
import type { KeyId } from '@hyper-hyper-space/hhs3_crypto';
import type { PeerInfo, TopicId } from '@hyper-hyper-space/hhs3_mesh';
import { FolderDiscovery, defaultMeshFolderRoot } from '../src/index.js';

const topic = 'topic-alpha' as TopicId;
const aliceId = 'alice-key' as KeyId;
const bobId = 'bob-key' as KeyId;

function alice(root: string, addresses = ['ws://127.0.0.1:9001']): FolderDiscovery {
    return new FolderDiscovery({
        root,
        self: { keyId: aliceId, addresses },
        instanceId: 'aliceinst',
        heartbeatMs: 999_999,
    });
}

function bob(root: string, addresses = ['ws://127.0.0.1:9002']): FolderDiscovery {
    return new FolderDiscovery({
        root,
        self: { keyId: bobId, addresses },
        instanceId: 'bobinst',
        heartbeatMs: 999_999,
    });
}

async function collect(
    d: FolderDiscovery,
    t: TopicId,
    schemes?: string[],
): Promise<PeerInfo[]> {
    const out: PeerInfo[] = [];
    for await (const p of d.discover(t, schemes)) out.push(p);
    return out;
}

async function withRoot(fn: (root: string) => Promise<void>): Promise<void> {
    const root = await mkdtemp(join(tmpdir(), 'hhs3-mesh-folder-'));
    try {
        await fn(root);
    } finally {
        await rm(root, { recursive: true, force: true });
    }
}

async function testAnnounceDiscoverRoundTrip() {
    await withRoot(async (root) => {
        const a = alice(root);
        const b = bob(root);
        await a.announce(topic, a.self);
        const peers = await collect(b, topic);
        testing.assertEquals(peers.length, 1, 'bob should see alice');
        testing.assertEquals(peers[0].keyId, aliceId, 'keyId should match');
        testing.assertEquals(peers[0].addresses[0], 'ws://127.0.0.1:9001', 'address should match');
        await a.close();
        await b.close();
    });
}

async function testSkipsOwnFile() {
    await withRoot(async (root) => {
        const a = alice(root);
        await a.announce(topic, a.self);
        const peers = await collect(a, topic);
        testing.assertEquals(peers.length, 0, 'announce+discover should not yield self');
        await a.close();
    });
}

async function testSchemeFiltering() {
    await withRoot(async (root) => {
        const a = alice(root, ['ws://127.0.0.1:9001', 'wss://example.test:443']);
        const b = bob(root);
        await a.announce(topic, a.self);

        const wsOnly = await collect(b, topic, ['ws']);
        testing.assertEquals(wsOnly.length, 1, 'ws filter should yield alice');
        testing.assertEquals(wsOnly[0].addresses.length, 1, 'only ws address remains');
        testing.assertEquals(wsOnly[0].addresses[0], 'ws://127.0.0.1:9001', 'ws address kept');

        const memOnly = await collect(b, topic, ['mem']);
        testing.assertEquals(memOnly.length, 0, 'mem filter should drop alice');

        await a.close();
        await b.close();
    });
}

async function testLeaveRemovesPresence() {
    await withRoot(async (root) => {
        const a = alice(root);
        const b = bob(root);
        await a.announce(topic, a.self);
        testing.assertEquals((await collect(b, topic)).length, 1, 'alice visible before leave');
        await a.leave(topic, aliceId);
        testing.assertEquals((await collect(b, topic)).length, 0, 'alice gone after leave');
        await a.close();
        await b.close();
    });
}

async function testTtlStaleness() {
    await withRoot(async (root) => {
        const ttlMs = 200;
        const a = new FolderDiscovery({
            root,
            self: { keyId: aliceId, addresses: ['ws://127.0.0.1:9001'] },
            instanceId: 'aliceinst',
            ttlMs,
            heartbeatMs: 999_999,
        });
        const b = new FolderDiscovery({
            root,
            self: { keyId: bobId, addresses: ['ws://127.0.0.1:9002'] },
            instanceId: 'bobinst',
            ttlMs,
            heartbeatMs: 999_999,
        });
        await a.announce(topic, a.self);

        const dirents = await readdir(join(root, Buffer.from(topic, 'utf8').toString('base64url')));
        const jsonName = dirents.find(n => n.endsWith('.json'));
        testing.assertTrue(jsonName !== undefined, 'presence file should exist');
        const path = join(root, Buffer.from(topic, 'utf8').toString('base64url'), jsonName!);
        const stale = new Date(Date.now() - ttlMs - 50);
        await utimes(path, stale, stale);

        const peers = await collect(b, topic);
        testing.assertEquals(peers.length, 0, 'stale mtime should be skipped');
        await a.close();
        await b.close();
    });
}

async function testGcDeletesVeryOldFiles() {
    await withRoot(async (root) => {
        const ttlMs = 100;
        const b = new FolderDiscovery({
            root,
            self: { keyId: bobId, addresses: ['ws://127.0.0.1:9002'] },
            instanceId: 'bobinst',
            ttlMs,
            heartbeatMs: 999_999,
        });

        const dir = join(root, Buffer.from(topic, 'utf8').toString('base64url'));
        await mkdir(dir, { recursive: true });
        const path = join(dir, 'orphan.json');
        await writeFile(path, JSON.stringify({
            keyId: 'orphan',
            addresses: ['ws://127.0.0.1:1'],
            topic,
            updatedAt: 0,
        }), 'utf8');
        const old = new Date(Date.now() - ttlMs * 2 - 50);
        await utimes(path, old, old);

        await collect(b, topic);
        const names = await readdir(dir);
        testing.assertTrue(!names.includes('orphan.json'), 'file older than 2*ttl should be unlinked');
        await b.close();
    });
}

async function testInstanceIdsDoNotCollide() {
    await withRoot(async (root) => {
        const a1 = new FolderDiscovery({
            root,
            self: { keyId: aliceId, addresses: ['ws://127.0.0.1:1'] },
            instanceId: 'inst1',
            heartbeatMs: 999_999,
        });
        const a2 = new FolderDiscovery({
            root,
            self: { keyId: aliceId, addresses: ['ws://127.0.0.1:2'] },
            instanceId: 'inst2',
            heartbeatMs: 999_999,
        });
        const b = bob(root);
        await a1.announce(topic, a1.self);
        await a2.announce(topic, a2.self);
        const peers = await collect(b, topic);
        testing.assertEquals(peers.length, 2, 'two instances of the same key should both appear');
        const ports = peers.map(p => p.addresses[0]).sort();
        testing.assertEquals(ports[0], 'ws://127.0.0.1:1', 'first instance address');
        testing.assertEquals(ports[1], 'ws://127.0.0.1:2', 'second instance address');
        await a1.close();
        await a2.close();
        await b.close();
    });
}

async function testDefaultRootHonorsEnv() {
    testing.assertTrue(defaultMeshFolderRoot().length > 0, 'default root is non-empty');
}

export const folderDiscoveryTests = {
    title: '[FOLDER_DISCOVERY] Folder-based peer discovery',
    tests: [
        { name: '[FD_00] announce/discover round-trip', invoke: testAnnounceDiscoverRoundTrip },
        { name: '[FD_01] discover skips own file', invoke: testSkipsOwnFile },
        { name: '[FD_02] scheme filtering', invoke: testSchemeFiltering },
        { name: '[FD_03] leave removes presence', invoke: testLeaveRemovesPresence },
        { name: '[FD_04] TTL staleness skips old mtime', invoke: testTtlStaleness },
        { name: '[FD_05] GC unlinks files older than 2*ttl', invoke: testGcDeletesVeryOldFiles },
        { name: '[FD_06] instance ids do not collide', invoke: testInstanceIdsDoNotCollide },
        { name: '[FD_07] default root is defined', invoke: testDefaultRootHonorsEnv },
    ],
};
