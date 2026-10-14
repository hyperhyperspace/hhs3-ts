import "fake-indexeddb/auto";

import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

import { testing } from "@hyper-hyper-space/hhs3_util";
import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import type { BroadcastChannelCtor } from "@hyper-hyper-space/hhs3_mesh_bc";
import { createBrowserMesh } from "@hyper-hyper-space/hhs3_mesh_browser";
import type { RCatalogImpl } from "@hyper-hyper-space/hhs3_rdb";
import { MemoryDirectory } from "@hyper-hyper-space/hhs3_rdb_projection";
import { MemoryKeyVault, RdbRuntime } from "@hyper-hyper-space/hhs3_rdb_runtime";
import {
    Rhost,
    type AppConfig, type FilesMountConfig, type HostKeyConfig, type MeshConfig, type ScopedMeshConfig,
} from "@hyper-hyper-space/hhs3_rhost";
import { browserProjectionDatabase, browserReplicaDatabase } from "@hyper-hyper-space/hhs3_rhost_client";
import { exportRelease, type ReleaseFile } from "@hyper-hyper-space/hhs3_rpack";

import { browserPlatform, createQueueLocks, serve, type BrowserFiles } from "../src/index.js";

const EDITOR_SQL = resolve('../rdb/examples/editor.sql');

const buses = new Map<string, Set<FakeBroadcastChannel>>();

class FakeBroadcastChannel {
    readonly name: string;
    onmessage: ((ev: { data: unknown }) => void) | null = null;
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
        const copy = structuredClone(message);
        for (const peer of buses.get(this.name) ?? []) {
            if (peer === this || peer.closed) continue;
            const ev = { data: copy };
            queueMicrotask(() => { if (!peer.closed) peer.onmessage?.(ev); });
        }
    }

    close(): void {
        if (this.closed) return;
        this.closed = true;
        this.onmessage = null;
        buses.get(this.name)?.delete(this);
    }
}

(globalThis as unknown as { BroadcastChannel: typeof FakeBroadcastChannel }).BroadcastChannel = FakeBroadcastChannel;
const channelCtor = FakeBroadcastChannel as unknown as BroadcastChannelCtor;

// Each test gets its own BroadcastChannel base, so a mesh that is still
// finishing discovery cannot meet the next test's mesh.
function quietMesh(base: string): MeshConfig {
    return {
        scope: 'localhost',
        transports: { ws: false, webrtc: false, broadcastChannel: { base } },
        discovery: [{ type: 'broadcast-channel' }],
    };
}

function nameRef(text: string) {
    return { kind: 'name' as const, text, parts: text.split('.'), span: { start: 0, end: text.length, line: 1, column: 1 } };
}

let editorRelease: Promise<ReleaseFile> | undefined;

function editor(): Promise<ReleaseFile> {
    editorRelease ??= (async () => {
        const runtime = await RdbRuntime.openMemory({ keyVault: new MemoryKeyVault() });
        try {
            await runtime.session.createKey('admin', 'pw');
            runtime.session.selectAuthor('admin');
            await runtime.execute(await readFile(EDITOR_SQL, 'utf8'));
            const catalogId = (await runtime.workspace.roots.resolveCatalog(nameRef('editor'))).id;
            const catalog = (await runtime.workspace.replica.getObject(catalogId)) as unknown as RCatalogImpl;
            const frontier = await (await catalog.getScopedDag()).getFrontier();
            const [hash] = (await catalog.getIndex()).findReleasesByVersion('1.0.0', frontier);
            if (hash === undefined) throw new Error('no editor 1.0.0');
            return await exportRelease(runtime.workspace.replica, catalogId, hash);
        } finally {
            await runtime.close();
        }
    })();
    return editorRelease;
}

function appConfig(): AppConfig {
    return {
        releases: 'catalogs/',
        params: { admin: '$me' },
        projection: { path: 'db/data.sqlite', files: [{ name: 'media', path: 'files/media' }] },
    };
}

function memoryFiles(): { dirs: Map<string, MemoryDirectory>; files: BrowserFiles } {
    const dirs = new Map<string, MemoryDirectory>();
    return {
        dirs,
        files: {
            async directory(host: string, mount: FilesMountConfig) {
                const key = `${host}/${mount.path}`;
                let dir = dirs.get(key);
                if (dir === undefined) {
                    dir = new MemoryDirectory();
                    dirs.set(key, dir);
                }
                return dir;
            },
            async removeHost(host: string) {
                for (const key of [...dirs.keys()]) {
                    if (key === host || key.startsWith(`${host}/`)) dirs.delete(key);
                }
            },
        },
    };
}

async function resetDatabases(): Promise<void> {
    const names = (await indexedDB.databases()).map((db) => db.name).filter((name): name is string => name !== undefined);
    for (const name of names) {
        await new Promise<void>((resolve, reject) => {
            const req = indexedDB.deleteDatabase(name);
            req.onsuccess = () => resolve();
            req.onerror = () => reject(req.error);
            req.onblocked = () => resolve();
        });
    }
}

async function rejects(fn: () => Promise<unknown>, fragment: string, what: string): Promise<void> {
    try {
        await fn();
    } catch (err) {
        assertTrue(String(err).includes(fragment), `${what}: expected '${fragment}' in '${String(err)}'`);
        return;
    }
    throw new Error(`${what}: expected an error`);
}

async function waitFor(check: () => Promise<boolean> | boolean, what: string, timeoutMs = 15_000): Promise<void> {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
        if (await check()) return;
        await new Promise((resolve) => setTimeout(resolve, 50));
    }
    if (await check()) return;
    throw new Error(`timed out waiting for ${what}`);
}

async function projectionKeyCount(app: string, host: string): Promise<number> {
    const db = await new Promise<IDBDatabase>((resolve, reject) => {
        const req = indexedDB.open(browserProjectionDatabase(app, host));
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error);
    });
    try {
        const tx = db.transaction('keys', 'readonly');
        const keys = await new Promise<unknown[]>((resolve, reject) => {
            const req = tx.objectStore('keys').getAll();
            req.onsuccess = () => resolve(req.result as unknown[]);
            req.onerror = () => reject(req.error);
        });
        return keys.length;
    } finally {
        db.close();
    }
}

const tests: { name: string; invoke: () => Promise<void> }[] = [
    {
        name: '[RHOST_BROWSER01] create deploys into IndexedDB, the mesh factory gets the merged config, project fills the projection and releases the lock, remove deletes the host',
        invoke: async () => {
            await resetDatabases();
            const release = await editor();
            const locks = createQueueLocks();
            const { dirs, files } = memoryFiles();
            const bare = browserPlatform({ name: 'phrases', releases: [], locks, files, BroadcastChannel: FakeBroadcastChannel });
            const key = (passphrase?: string): HostKeyConfig => ({ label: 'me', keyId: 'k', publicKey: 'pk', ...(passphrase !== undefined ? { passphrase } : {}) });
            assertEquals(await bare.passphrase(key()), '', 'a key stored without a passphrase');
            await rejects(() => bare.passphrase(key('prompt')), 'prompt callback', 'prompt needs a callback');
            await rejects(() => bare.passphrase(key('env:SECRET')), 'a browser has no environment variables', 'there is no environment');
            await bare.close();

            const seen: ScopedMeshConfig[] = [];
            const platform = browserPlatform({
                name: 'editor',
                releases: [release],
                passphrase: async () => 'pw',
                locks,
                files,
                BroadcastChannel: FakeBroadcastChannel,
                mesh: { BroadcastChannelCtor: channelCtor },
                meshFactory: async (config, runtime) => {
                    seen.push(config);
                    return createBrowserMesh(config, runtime, { BroadcastChannelCtor: channelCtor });
                },
            });
            await (await platform.keyVault()).create('me', 'pw');
            const app = await Rhost.open(appConfig(), platform);
            try {
                const host = await app.create('default', { key: 'me', passphrase: 'prompt', mesh: quietMesh('b01') });
                const [created] = await app.status('default');
                assertEquals(created.deployed.map((item) => item.version).join(','), '1.0.0', 'the shipped release is deployed');
                assertEquals(created.database.length > 0, true, 'the database id is stored');

                await app.start('default');
                assertEquals(seen[0]?.scope, 'localhost', 'the factory got the merged scope');
                assertEquals(seen[0]?.discovery?.[0]?.type, 'broadcast-channel', 'and the host discovery');
                assertEquals(seen[0]?.transports?.ws, false, 'and the host transports');
                await app.stop('default');

                await host.project();
                assertTrue(await projectionKeyCount('editor', 'default') > 0, 'project wrote the host key into the projection');
                assertEquals((await locks.query()).held?.length ?? 0, 0, 'project did not leave the lock held');

                dirs.set('default/sentinel', new MemoryDirectory());
                await app.remove('default');
                assertEquals((await app.hosts()).join(','), '', 'the host record is gone');
                assertEquals(dirs.has('default/sentinel'), false, 'the host file directory is gone');
                const names = (await indexedDB.databases()).map((db) => db.name);
                assertEquals(names.includes(browserReplicaDatabase('editor', 'default')), false, 'the replica database is gone');
            } finally {
                await app.close();
                await platform.close();
            }
        },
    },
    {
        name: '[RHOST_BROWSER02] a queued serve takes over when the holder closes, stop leaves it idle, and a browser-illegal mesh fails in the factory',
        invoke: async () => {
            await resetDatabases();
            const release = await editor();
            const locks = createQueueLocks();
            const { files } = memoryFiles();
            const base = {
                releases: [release],
                passphrase: async () => 'pw',
                locks,
                files,
                BroadcastChannel: FakeBroadcastChannel,
                mesh: { BroadcastChannelCtor: channelCtor },
            };

            const lifeA = new AbortController();
            const lifeB = new AbortController();
            const platformA = browserPlatform({ ...base, name: 'takeover', signal: lifeA.signal });
            await (await platformA.keyVault()).create('me', 'pw');
            const appA = await Rhost.open(appConfig(), platformA);
            const platformB = browserPlatform({ ...base, name: 'takeover', signal: lifeB.signal });
            const appB = await Rhost.open(appConfig(), platformB);
            try {
                await appA.create('default', { key: 'me', passphrase: 'prompt', mesh: quietMesh('takeover') });
                await serve(appA, 'default');
                const running = await (await appA.host('default')).status();
                assertEquals(running.running, true, 'A is serving');

                await serve(appB, 'default');
                assertEquals((await appB.host('default')).running, false, 'B is queued and has not started');
                lifeA.abort();
                await waitFor(async () => (await appB.host('default')).running, 'B serves after A closes');
            } finally {
                lifeB.abort();
                await appA.close().catch(() => undefined);
                await appB.close().catch(() => undefined);
                await platformA.close();
                await platformB.close();
            }
            const stopA = new AbortController();
            const stopB = new AbortController();
            const holdA = browserPlatform({ ...base, name: 'idle', signal: stopA.signal });
            await (await holdA.keyVault()).create('me', 'pw');
            const idleA = await Rhost.open(appConfig(), holdA);
            const holdB = browserPlatform({ ...base, name: 'idle', signal: stopB.signal });
            const idleB = await Rhost.open(appConfig(), holdB);
            try {
                await idleA.create('default', { key: 'me', passphrase: 'prompt', mesh: quietMesh('idle') });
                await serve(idleA, 'default');
                await serve(idleB, 'default');
                await idleA.stop('default');
                await new Promise((resolve) => setTimeout(resolve, 100));
                assertEquals((await idleB.host('default')).running, false, 'stop does not hand the host to B');
                const [stopped] = await idleA.status('default');
                assertEquals(stopped.running, false, 'A is stopped');
            } finally {
                stopA.abort();
                stopB.abort();
                await idleA.close().catch(() => undefined);
                await idleB.close().catch(() => undefined);
                await holdA.close();
                await holdB.close();
            }

            const meshPlatform = browserPlatform({ ...base, name: 'mesh' });
            await (await meshPlatform.keyVault()).create('me', 'pw');
            const meshApp = await Rhost.open(appConfig(), meshPlatform);
            try {
                await meshApp.create('ws', {
                    key: 'me', passphrase: 'prompt',
                    mesh: { scope: 'localhost', transports: { ws: { listen: 'ws://127.0.0.1:7400' }, webrtc: false } },
                });
                await rejects(() => meshApp.start('ws'), 'the browser cannot listen on ws', 'a tab cannot bind a port');
                await meshApp.create('folder', {
                    key: 'me', passphrase: 'prompt',
                    mesh: { scope: 'localhost', transports: { ws: false, webrtc: false }, discovery: [{ type: 'folder' }] },
                });
                await rejects(() => meshApp.start('folder'), 'folder discovery is only available on Node', 'folder discovery is refused');
                assertEquals((await locks.query()).held?.length ?? 0, 0, 'a failed start released the lock');
            } finally {
                await meshApp.close().catch(() => undefined);
                await meshPlatform.close();
            }
        },
    },
];

async function main() {
    const filters = process.argv.slice(2);
    console.log('Running tests for Hyper Hyper Space v3 rhost_browser module'
        + (filters.length > 0 ? ' (applying filter: ' + filters.toString() + ')' : '') + '\n');
    console.log('rhost_browser');
    for (const test of tests) {
        let match = true;
        for (const filter of filters) match = match && test.name.indexOf(filter) >= 0;
        if (match) testing.exitIfFailed(await testing.run(test.name, test.invoke));
        else await testing.skip(test.name);
    }
    console.log();
}

main();
