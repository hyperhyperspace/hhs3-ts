import "fake-indexeddb/auto";

import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

import { createBasicCrypto, createIdentity, HASH_SHA256, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import { serializePublicKeyToBase64 } from "@hyper-hyper-space/hhs3_mvt";
import type { BroadcastChannelCtor } from "@hyper-hyper-space/hhs3_mesh_bc";
import type { RCatalogImpl } from "@hyper-hyper-space/hhs3_rdb";
import { IdbTarget } from "@hyper-hyper-space/hhs3_rdb_adapter_idb";
import { MemoryDirectory } from "@hyper-hyper-space/hhs3_rdb_projection";
import { MemoryKeyVault, RdbRuntime } from "@hyper-hyper-space/hhs3_rdb_runtime";
import { Rhost, type AppConfig, type FilesMountConfig, type MeshConfig } from "@hyper-hyper-space/hhs3_rhost";
import { browserProjectionDatabase, isHostStatus, type ClientEvent } from "@hyper-hyper-space/hhs3_rhost_client";
import { browserPlatform, createQueueLocks, serve, type BrowserFiles } from "@hyper-hyper-space/hhs3_rhost_browser";
import { exportRelease, type ReleaseFile } from "@hyper-hyper-space/hhs3_rpack";
import { testing } from "@hyper-hyper-space/hhs3_util";
import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import { openClient } from "../src/index.js";

const EDITOR_SQL = resolve('../rdb/examples/editor.sql');
const APP = 'client-app';
const hashSuite = createBasicCrypto().hash(HASH_SHA256);

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

const QUIET_MESH: MeshConfig = {
    scope: 'localhost',
    transports: { ws: false, webrtc: false },
    discovery: [{ type: 'broadcast-channel' }],
};

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
    return { releases: 'catalogs/', params: { admin: '$me' }, projection: { path: 'db/data.sqlite' } };
}

function memoryFiles(): BrowserFiles {
    const dirs = new Map<string, MemoryDirectory>();
    return {
        async directory(host: string, mount: FilesMountConfig) {
            const key = `${host}/${mount.path}`;
            let dir = dirs.get(key);
            if (dir === undefined) {
                dir = new MemoryDirectory();
                dirs.set(key, dir);
            }
            return dir;
        },
        async removeHost() {},
    };
}

async function newPublicKey(): Promise<string> {
    return serializePublicKeyToBase64((await createIdentity(SIGNING_ED25519, hashSuite)).publicKey);
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

function txDone(tx: { oncomplete: ((ev: Event) => void) | null; onabort: ((ev: Event) => void) | null; onerror: ((ev: Event) => void) | null; error: DOMException | null }): Promise<void> {
    return new Promise((resolve, reject) => {
        tx.oncomplete = () => resolve();
        tx.onabort = () => reject(tx.error ?? new Error('transaction aborted'));
        tx.onerror = () => reject(tx.error ?? new Error('transaction failed'));
    });
}

const tests: { name: string; invoke: () => Promise<void> }[] = [
    {
        name: '[RHOST_CLIENT_BROWSER01] me, registerKey and events match the projection, including while stopped; a tab that never served sees running, then stopped',
        invoke: async () => {
            const life = new AbortController();
            const platform = browserPlatform({
                name: APP,
                releases: [await editor()],
                passphrase: async () => 'pw',
                locks: createQueueLocks(),
                files: memoryFiles(),
                BroadcastChannel: FakeBroadcastChannel,
                mesh: { BroadcastChannelCtor: channelCtor },
                signal: life.signal,
            });
            await (await platform.keyVault()).create('me', 'pw');
            const app = await Rhost.open(appConfig(), platform);
            const clientOptions = { BroadcastChannel: FakeBroadcastChannel, indexedDB };
            try {
                await app.create('default', { key: 'me', passphrase: 'prompt', mesh: QUIET_MESH });
                const early = openClient(APP, 'default', clientOptions);
                assertEquals(JSON.stringify(await early.events.since()), '[]', 'no events before the first start');
                await rejects(() => early.me(), 'no key table', 'me needs the projection');
                const earlyKey = await newPublicKey();
                await rejects(() => early.registerKey(earlyKey), 'no key table', 'registerKey needs the projection');
                await early.close();

                await serve(app, 'default');
                const host = await app.host('default');
                const client = openClient(APP, 'default', clientOptions);
                const other = openClient(APP, 'default', clientOptions);
                try {
                    const me = await client.me();
                    assertEquals(JSON.stringify(me), JSON.stringify(await host.client.me()), 'me matches the projection');
                    const bob = await newPublicKey();
                    const bobId = await client.registerKey(bob);
                    assertEquals(await client.registerKey(bob), bobId, 'registerKey is idempotent');
                    assertEquals(await host.client.registerKey(bob), bobId, 'the projection gives the same id');
                    const carol = await newPublicKey();
                    const carolId = await host.client.registerKey(carol);
                    assertEquals(await client.registerKey(carol), carolId, 'and the other way around');

                    const watched: ClientEvent[] = [];
                    const unwatch = client.events.watch((batch) => { watched.push(...batch); });
                    const target = await IdbTarget.open(browserProjectionDatabase(APP, 'default'), { captureChanges: true, indexedDB });
                    try {
                        const tx = target.database.transaction('doc_pages', 'readwrite');
                        const done = txDone(tx);
                        const add = tx.objectStore('doc_pages').add({ title: 'not a writer', deleted: false });
                        await new Promise((resolve, reject) => {
                            add.onsuccess = () => resolve(add.result);
                            add.onerror = () => reject(add.error);
                        });
                        await done;
                    } finally {
                        target.close();
                    }
                    await waitFor(() => watched.some((event) => event.table === 'doc_pages'), 'the rejected write reaches the watch');
                    unwatch();
                    assertEquals(JSON.stringify(await client.events.since()), JSON.stringify(await host.client.events.since()),
                        'events match the projection');

                    const live = await other.status();
                    assertTrue(isHostStatus(live) && live.running && live.host === 'default', `a third tab sees the host running (${JSON.stringify(live)})`);

                    const saved = me.id;
                    await app.stop('default');
                    assertEquals((await client.me()).id, saved, 'me works while the host is stopped');
                    assertTrue((await client.events.since()).some((event) => event.table === 'doc_pages'), 'events work while it is stopped');
                    const stopped = await other.status();
                    assertTrue(!isHostStatus(stopped) && stopped.running === false && stopped.catalog === 'editor',
                        `the third tab sees it stopped (${JSON.stringify(stopped)})`);
                } finally {
                    await client.close();
                    await other.close();
                }
            } finally {
                life.abort();
                await app.close().catch(() => undefined);
                await platform.close();
            }
        },
    },
];

async function main() {
    const filters = process.argv.slice(2);
    console.log('Running tests for Hyper Hyper Space v3 rhost_client_browser module'
        + (filters.length > 0 ? ' (applying filter: ' + filters.toString() + ')' : '') + '\n');
    console.log('rhost_client_browser');
    for (const test of tests) {
        let match = true;
        for (const filter of filters) match = match && test.name.indexOf(filter) >= 0;
        if (match) testing.exitIfFailed(await testing.run(test.name, test.invoke));
        else await testing.skip(test.name);
    }
    console.log();
}

main();
