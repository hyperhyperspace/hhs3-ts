// The browser platform. One app name on an origin: host records, keys, a
// replica and a projection per host in IndexedDB, file mounts in the origin
// private file system, and the browser mesh on the host's merged MeshConfig.
//
// `serve` is what a tab calls. The tab that gets the Web Lock runs start().
// A tab that finds the lock held stays queued and takes over when the holder
// closes. stop() sets a flag and the queued tabs abort, so a stop is not a
// handover. Web Locks do not reenter: while this tab's serve() callback holds
// the lock, start() and project() on this tab get a hold whose release does
// not free it.

import { createBasicCrypto, HASH_SHA256 } from "@hyper-hyper-space/hhs3_crypto";
import { createBrowserMesh, type BrowserMeshOptions } from "@hyper-hyper-space/hhs3_mesh_browser";
import { IdbTarget } from "@hyper-hyper-space/hhs3_rdb_adapter_idb";
import type { FileDirectory } from "@hyper-hyper-space/hhs3_rdb_projection";
import type { KeyVault } from "@hyper-hyper-space/hhs3_rdb_runtime";
import {
    Rhost,
    type Connection, type FilesMountConfig, type HostKeyConfig, type HostPlatform, type ProjectionConfig,
    type SyncMeshFactory,
} from "@hyper-hyper-space/hhs3_rhost";
import {
    browserClientChannel, browserHostsDatabase, browserKeysDatabase, browserLockName, browserProjectionDatabase,
    decodeServerMessage, encodeMessage, isBrowserChannelMessage,
} from "@hyper-hyper-space/hhs3_rhost_client";
import type { ReleaseFile } from "@hyper-hyper-space/hhs3_rpack";

import { opfsDirectory, removeOpfsHost } from "./files.js";
import { IdbHosts } from "./hosts.js";
import { IdbKeyStore } from "./keystore.js";
import { listenOnChannel, type BrowserChannelCtor } from "./listen.js";
import type { WebLockManager } from "./queue_locks.js";

export type { WebLockManager } from "./queue_locks.js";

const hashSuite = createBasicCrypto().hash(HASH_SHA256);
const STATUS_WAIT_MS = 15_000;

export type BrowserFiles = {
    directory(host: string, mount: FilesMountConfig): Promise<FileDirectory>;
    removeHost(host: string): Promise<void>;
};

export type BrowserPlatformOptions = {
    // One app on this origin. It namespaces every database and file mount.
    name: string;
    releases: ReleaseFile[];
    // Replaces every passphrase source. Tests and embedded apps use this.
    passphrase?: (key: HostKeyConfig) => Promise<string>;
    // Used when a host key's source is `prompt` and `passphrase` was not set.
    prompt?: (key: HostKeyConfig) => Promise<string>;
    indexedDB?: IDBFactory;
    locks?: WebLockManager;
    BroadcastChannel?: BrowserChannelCtor;
    // Passed to createBrowserMesh. Tests inject BroadcastChannelCtor here.
    mesh?: BrowserMeshOptions;
    meshFactory?: SyncMeshFactory;
    files?: BrowserFiles;
    // Aborting it is the tab closing: services stop, the lock is released,
    // and the stopped flag is left clear so a queued tab can take over.
    signal?: AbortSignal;
};

type Session = {
    originalStop: () => Promise<void>;
    closeRuntime: () => Promise<void>;
    release: () => void;
};

function defaultLocks(): WebLockManager | undefined {
    const locks = globalThis.navigator?.locks;
    if (locks === undefined) return undefined;
    return locks as unknown as WebLockManager;
}

function defaultChannel(): BrowserChannelCtor | undefined {
    const ctor = (globalThis as { BroadcastChannel?: BrowserChannelCtor }).BroadcastChannel;
    return ctor;
}

function randomId(): string {
    const bytes = crypto.getRandomValues(new Uint8Array(16));
    return [...bytes].map((byte) => byte.toString(16).padStart(2, '0')).join('');
}

export class BrowserPlatform implements HostPlatform {
    readonly hosts: IdbHosts;
    readonly meshFactory: SyncMeshFactory;
    readonly keystoreLocation: string;
    private readonly options: BrowserPlatformOptions;
    private readonly factory: IDBFactory;
    private readonly locks: WebLockManager;
    private readonly Channel: BrowserChannelCtor;
    private readonly appName: string;
    private vault: Promise<KeyVault> | undefined;
    // Hosts whose Web Lock this tab's serve() callback is inside.
    private readonly serveHolds = new Set<string>();
    private readonly sessions = new Map<string, Session>();
    private readonly serveCalls = new Map<string, Promise<void>>();

    constructor(options: BrowserPlatformOptions) {
        if (options.name.length === 0) throw new Error('an app name is required');
        const factory = options.indexedDB ?? globalThis.indexedDB;
        if (factory === undefined) throw new Error('no IndexedDB; pass indexedDB');
        const locks = options.locks ?? defaultLocks();
        if (locks === undefined) throw new Error('no Web Lock manager; pass locks');
        const Channel = options.BroadcastChannel ?? defaultChannel();
        if (Channel === undefined) throw new Error('no BroadcastChannel; pass BroadcastChannel');
        this.options = options;
        this.factory = factory;
        this.locks = locks;
        this.Channel = Channel;
        this.appName = options.name;
        this.keystoreLocation = browserKeysDatabase(options.name);
        this.hosts = new IdbHosts(
            options.name,
            factory,
            browserHostsDatabase(options.name),
            (host) => this.removeHostFiles(host),
            (host) => this.lockHeld(host),
        );
        this.meshFactory = options.meshFactory ?? (async (config, runtime) => {
            return createBrowserMesh(config, runtime, options.mesh ?? {});
        });
        options.signal?.addEventListener('abort', () => { void this.discard(); });
    }

    keyVault(): Promise<KeyVault> {
        this.vault ??= IdbKeyStore.open(this.keystoreLocation, this.factory, hashSuite);
        return this.vault;
    }

    async passphrase(key: HostKeyConfig): Promise<string> {
        if (this.options.passphrase !== undefined) return this.options.passphrase(key);
        if (key.passphrase === undefined) return '';
        if (key.passphrase === 'prompt') {
            if (this.options.prompt === undefined) {
                throw new Error(`the passphrase of key '${key.label}' is needed; pass a prompt callback`);
            }
            return this.options.prompt(key);
        }
        if (key.passphrase.startsWith('env:')) throw new Error('a browser has no environment variables');
        throw new Error(`the passphrase of key '${key.label}' is needed; pass a prompt callback`);
    }

    async readReleases(_folder: string): Promise<ReleaseFile[]> {
        return this.options.releases;
    }

    async projectionTarget(host: string, _config: ProjectionConfig): Promise<IdbTarget> {
        return IdbTarget.open(browserProjectionDatabase(this.appName, host), {
            captureChanges: true,
            indexedDB: this.factory,
        });
    }

    filesDirectory(host: string, mount: FilesMountConfig): Promise<FileDirectory> {
        if (this.options.files !== undefined) return this.options.files.directory(host, mount);
        return opfsDirectory(this.appName, host, mount);
    }

    async acquireLock(host: string): Promise<() => Promise<void>> {
        if (this.serveHolds.has(host)) return async () => {};
        const name = browserLockName(this.appName, host);
        return new Promise((resolve, reject) => {
            let settled = false;
            void this.locks.request(name, { ifAvailable: true }, async (lock) => {
                if (lock === null) {
                    settled = true;
                    reject(new Error(`host '${host}' is already running`));
                    return;
                }
                let release!: () => void;
                const done = new Promise<void>((resolveDone) => { release = resolveDone; });
                settled = true;
                resolve(async () => { release(); });
                await done;
            }).catch((err) => { if (!settled) reject(err); });
        });
    }

    listen(host: string, onConnection: (connection: Connection) => void): Promise<() => Promise<void>> {
        return Promise.resolve(listenOnChannel(browserClientChannel(this.appName, host), this.Channel, onConnection));
    }

    // Resolves when this tab is serving, or when it is queued and the current
    // host has answered status. The queued request stays until the holder
    // closes (this tab starts) or stop() aborts it.
    serveHost(app: Rhost, host: string): Promise<void> {
        if (this.serveHolds.has(host)) return Promise.resolve();
        const existing = this.serveCalls.get(host);
        if (existing !== undefined) return existing;
        const call = this.doServe(app, host);
        this.serveCalls.set(host, call);
        void call.finally(() => { if (this.serveCalls.get(host) === call) this.serveCalls.delete(host); });
        return call;
    }

    private async doServe(app: Rhost, host: string): Promise<void> {
        const name = browserLockName(this.appName, host);
        let decide!: (immediate: boolean) => void;
        const decision = new Promise<boolean>((resolve) => { decide = resolve; });
        let settled = false;
        let succeed!: () => void;
        let fail!: (err: unknown) => void;
        const ready = new Promise<void>((resolve, reject) => {
            succeed = () => { if (!settled) { settled = true; resolve(); } };
            fail = (err) => { if (!settled) { settled = true; reject(err); } };
        });

        const run = async (fresh: boolean): Promise<void> => {
            if (fresh) await this.hosts.setStopped(host, false);
            if (await this.hosts.isStopped(host)) return;
            this.serveHolds.add(host);
            let release!: () => void;
            let released = false;
            const gate = new Promise<void>((resolve) => {
                release = () => { if (!released) { released = true; resolve(); } };
            });
            try {
                const running = await app.host(host);
                const originalStop = running.stop.bind(running);
                (running as { stop: () => Promise<void> }).stop = async () => {
                    await this.hosts.setStopped(host, true);
                    this.broadcastStop(host);
                    try { await originalStop(); }
                    finally {
                        release();
                        // The lock callback frees the Web Lock after this returns.
                        await new Promise((resolve) => setTimeout(resolve, 0));
                    }
                };
                this.sessions.set(host, {
                    originalStop,
                    closeRuntime: () => running.runtime.close(),
                    release,
                });
                await running.start();
                if (fresh) succeed();
                await gate;
            } catch (err) {
                if (fresh) fail(err);
                throw err;
            } finally {
                this.serveHolds.delete(host);
                this.sessions.delete(host);
            }
        };

        const immediate = this.locks.request(name, { ifAvailable: true }, async (lock) => {
            decide(lock !== null);
            if (lock === null) return;
            await run(true);
        });
        void immediate.catch((err) => fail(err));

        if (await decision) {
            await ready;
            return;
        }

        const abort = new AbortController();
        const channel = new this.Channel(browserClientChannel(this.appName, host));
        const closeChannel = () => {
            channel.onmessage = null;
            try { channel.close(); } catch { /* already closed */ }
        };
        channel.onmessage = (ev) => {
            if (isBrowserChannelMessage(ev.data) && 'stop' in ev.data) abort.abort();
        };
        abort.signal.addEventListener('abort', closeChannel);
        const takeover = this.locks.request(name, { signal: abort.signal }, async (lock) => {
            closeChannel();
            if (lock === null || abort.signal.aborted) return;
            await run(false);
        });
        void takeover.catch(() => { /* aborted, or this tab closed */ });
        try {
            await this.waitForStatus(host);
        } catch (err) {
            abort.abort();
            throw err;
        }
    }

    private broadcastStop(host: string): void {
        const channel = new this.Channel(browserClientChannel(this.appName, host));
        try { channel.postMessage({ stop: true }); }
        finally { channel.close(); }
    }

    private waitForStatus(host: string): Promise<void> {
        const channel = new this.Channel(browserClientChannel(this.appName, host));
        const client = randomId();
        const requestId = 1;
        return new Promise((resolve, reject) => {
            let done = false;
            let retry: ReturnType<typeof setInterval> | undefined;
            const finish = (settle: () => void) => {
                if (done) return;
                done = true;
                clearTimeout(timer);
                if (retry !== undefined) clearInterval(retry);
                channel.onmessage = null;
                try { channel.postMessage({ client, close: true }); } catch { /* gone */ }
                channel.close();
                settle();
            };
            const timer = setTimeout(() => finish(() => reject(new Error(`host '${host}' did not answer`))), STATUS_WAIT_MS);
            const ask = () => {
                try { channel.postMessage({ client, line: encodeMessage({ id: requestId, method: 'status' }) }); }
                catch { /* closed */ }
            };
            channel.onmessage = (ev) => {
                const data = ev.data;
                if (!isBrowserChannelMessage(data) || 'stop' in data || 'close' in data || data.client !== client) return;
                try {
                    const message = decodeServerMessage(data.line);
                    if (message.id === requestId && 'result' in message && message.result !== null) finish(() => resolve());
                } catch { /* not a status reply */ }
            };
            retry = setInterval(ask, 200);
            ask();
        });
    }

    // The document went away. Stop services without the stopped flag, then let
    // the lock callback finish so the next queued tab can start.
    private async discard(): Promise<void> {
        const sessions = [...this.sessions.entries()];
        this.sessions.clear();
        for (const [host, session] of sessions) {
            try { await session.originalStop(); } catch { /* already stopped */ }
            try { await session.closeRuntime(); } catch { /* already closed */ }
            session.release();
            this.serveHolds.delete(host);
        }
    }

    async close(): Promise<void> {
        if (this.vault !== undefined) {
            const vault = await this.vault;
            if (vault instanceof IdbKeyStore) vault.close();
            this.vault = undefined;
        }
        await this.hosts.close();
    }

    private async removeHostFiles(host: string): Promise<void> {
        if (this.options.files !== undefined) {
            await this.options.files.removeHost(host);
            return;
        }
        await removeOpfsHost(this.appName, host);
    }

    private async lockHeld(host: string): Promise<boolean> {
        const name = browserLockName(this.appName, host);
        const state = await this.locks.query();
        return (state.held ?? []).some((lock) => lock.name === name);
    }
}

export function browserPlatform(options: BrowserPlatformOptions): BrowserPlatform {
    return new BrowserPlatform(options);
}

export function serve(app: Rhost, host: string): Promise<void> {
    const platform = app.platform;
    if (!(platform instanceof BrowserPlatform)) throw new Error('serve() needs a browser platform');
    return platform.serveHost(app, host);
}
