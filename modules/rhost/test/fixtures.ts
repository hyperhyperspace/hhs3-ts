import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

import { createBasicCrypto, HASH_SHA256, KEM_X25519_HKDF, SIGNING_ED25519, type B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import {
    Mesh, MemTransportProvider, createAuthenticator,
    type NetworkAddress, type PeerDiscovery, type PeerInfo, type TopicId, type Transport, type TransportProvider,
} from "@hyper-hyper-space/hhs3_mesh";
import type { RCatalogImpl } from "@hyper-hyper-space/hhs3_rdb";
import { MemoryTarget } from "@hyper-hyper-space/hhs3_rdb_adapter";
import { MemoryDirectory } from "@hyper-hyper-space/hhs3_rdb_projection";
import {
    MemDagBackend, MemoryKeyVault, RdbRuntime, type DagBackend, type KeyRecord, type KeyVault,
} from "@hyper-hyper-space/hhs3_rdb_runtime";
import { exportRelease, type ReleaseFile } from "@hyper-hyper-space/hhs3_rpack";

import type {
    AppConfig, FilesMountConfig, HostKeyConfig, HostPlatform, HostRecord, HostSetup, HostStore, SyncMeshFactory,
} from "../src/index.js";

const EDITOR_SQL = resolve('../rdb/examples/editor.sql');

const SECOND_RELEASE = `
    ALTER SCHEMA hhs:doc VERSION '1.1.0' AS (ADD COLUMN pages.tag string NULL);
    ALTER CATALOG editor VERSION '1.1.0' AS (UPDATE SCHEMA hhs:doc TO LATEST ON doc) NOTE 'tags' BY $admin;
`;

const THIRD_RELEASE = `
    ALTER SCHEMA hhs:doc VERSION '2.0.0' AS (ADD COLUMN pages.color string NULL);
    ALTER CATALOG editor VERSION '2.0.0' AS (UPDATE SCHEMA hhs:doc TO LATEST ON doc) NOTE 'colors' BY $admin;
`;

export type EditorReleases = { v100: ReleaseFile; v110: ReleaseFile; v200: ReleaseFile };

function nameRef(text: string) {
    return { kind: 'name' as const, text, parts: text.split('.'), span: { start: 0, end: text.length, line: 1, column: 1 } };
}

async function exportVersion(runtime: RdbRuntime, catalogId: B64Hash, version: string): Promise<ReleaseFile> {
    const catalog = (await runtime.workspace.replica.getObject(catalogId)) as unknown as RCatalogImpl;
    const frontier = await (await catalog.getScopedDag()).getFrontier();
    const [release] = (await catalog.getIndex()).findReleasesByVersion(version, frontier);
    if (release === undefined) throw new Error(`no release ${version}`);
    return exportRelease(runtime.workspace.replica, catalogId, release);
}

let editorReleases: Promise<EditorReleases> | undefined;

// The editor catalog at 1.0.0, 1.1.0 (adds pages.tag) and 2.0.0 (adds
// pages.color), released by a developer key that no host has.
export function editor(): Promise<EditorReleases> {
    editorReleases ??= (async () => {
        const runtime = await RdbRuntime.openMemory({ keyVault: new MemoryKeyVault() });
        try {
            await runtime.session.createKey('admin', 'pw');
            runtime.session.selectAuthor('admin');
            await runtime.execute(await readFile(EDITOR_SQL, 'utf8'));
            const catalogId = (await runtime.workspace.roots.resolveCatalog(nameRef('editor'))).id;
            const v100 = await exportVersion(runtime, catalogId, '1.0.0');
            await runtime.execute(SECOND_RELEASE);
            const v110 = await exportVersion(runtime, catalogId, '1.1.0');
            await runtime.execute(THIRD_RELEASE);
            const v200 = await exportVersion(runtime, catalogId, '2.0.0');
            return { v100, v110, v200 };
        } finally {
            await runtime.close();
        }
    })();
    return editorReleases;
}

// 1.0.0, then 1.1.0 which only declares a new identity param.
export async function withParam(): Promise<{ v100: ReleaseFile; v110: ReleaseFile }> {
    const runtime = await RdbRuntime.openMemory({ keyVault: new MemoryKeyVault() });
    try {
        await runtime.session.createKey('admin', 'pw');
        runtime.session.selectAuthor('admin');
        await runtime.execute(await readFile(EDITOR_SQL, 'utf8'));
        const catalogId = (await runtime.workspace.roots.resolveCatalog(nameRef('editor'))).id;
        const v100 = await exportVersion(runtime, catalogId, '1.0.0');
        await runtime.execute("ALTER CATALOG editor VERSION '1.1.0' PARAMS (:moderator identity) BY $admin;");
        return { v100, v110: await exportVersion(runtime, catalogId, '1.1.0') };
    } finally {
        await runtime.close();
    }
}

// 1.0.0, then 1.1.0 which adds FILES media, writable by managers.
export async function withFiles(): Promise<{ v100: ReleaseFile; v110: ReleaseFile }> {
    const runtime = await RdbRuntime.openMemory({ keyVault: new MemoryKeyVault() });
    try {
        await runtime.session.createKey('admin', 'pw');
        runtime.session.selectAuthor('admin');
        await runtime.execute(await readFile(EDITOR_SQL, 'utf8'));
        const catalogId = (await runtime.workspace.roots.resolveCatalog(nameRef('editor'))).id;
        const v100 = await exportVersion(runtime, catalogId, '1.0.0');
        await runtime.execute(`ALTER CATALOG editor VERSION '1.1.0' AS (
            ADD FILES media USING IDENTITIES user.identities
              ALLOW WRITE IF EXISTS user.caps WHERE user.caps.label = 'manager' AND user.caps.grantee = $author
        ) NOTE 'media' BY $admin;`);
        return { v100, v110: await exportVersion(runtime, catalogId, '1.1.0') };
    } finally {
        await runtime.close();
    }
}

export function appConfig(overrides: Partial<AppConfig> = {}): AppConfig {
    return {
        releases: 'catalogs/',
        params: { admin: '$me' },
        projection: { path: 'data.sqlite' },
        ...overrides,
    };
}

// What create and join get: the platform's key 'me', and a localhost mesh.
export function hostSetup(overrides: Partial<HostSetup> = {}): HostSetup {
    return { key: 'me', passphrase: 'prompt', sync: { scope: 'localhost' }, ...overrides };
}

// --- A shared in-memory mesh, so hosts in one process find each other ---

class PeerRegistry {
    private readonly topics = new Map<TopicId, Map<string, PeerInfo>>();

    announce(topic: TopicId, self: PeerInfo): void {
        let peers = this.topics.get(topic);
        if (peers === undefined) {
            peers = new Map();
            this.topics.set(topic, peers);
        }
        peers.set(self.keyId, self);
    }

    leave(topic: TopicId, keyId: string): void {
        this.topics.get(topic)?.delete(keyId);
    }

    list(topic: TopicId): PeerInfo[] {
        return [...(this.topics.get(topic)?.values() ?? [])];
    }
}

// Mesh.close() shuts its providers; each mesh gets one that leaves the shared
// provider open.
class NonClosingProvider implements TransportProvider {
    readonly scheme: string;
    constructor(private readonly inner: TransportProvider) {
        this.scheme = inner.scheme;
    }
    listen(address: NetworkAddress, onConnection: (transport: Transport) => void): Promise<void> {
        return this.inner.listen(address, onConnection);
    }
    connect(remote: NetworkAddress, local?: NetworkAddress): Promise<Transport> {
        return this.inner.connect(remote, local);
    }
    close(): void {}
}

function registryDiscovery(registry: PeerRegistry, selfKeyId: string): PeerDiscovery {
    return {
        async *discover(topic: TopicId, schemes?: string[]): AsyncIterable<PeerInfo> {
            for (const peer of registry.list(topic)) {
                if (peer.keyId === selfKeyId) continue;
                const addresses = schemes === undefined || schemes.length === 0
                    ? peer.addresses
                    : peer.addresses.filter((addr) => schemes.some((s) => addr.startsWith(`${s}://`)));
                if (addresses.length === 0) continue;
                yield { keyId: peer.keyId, addresses };
            }
        },
        announce(topic, self) {
            registry.announce(topic, self);
            return Promise.resolve();
        },
        leave(topic, self) {
            registry.leave(topic, self);
            return Promise.resolve();
        },
    };
}

export class SharedMesh {
    readonly provider = new MemTransportProvider();
    private readonly registry = new PeerRegistry();
    private counter = 0;

    readonly factory: SyncMeshFactory = async (req) => {
        this.counter += 1;
        const addr = `mem://rhost-${req.identity.keyId.slice(0, 12)}-${this.counter}`;
        const discovery = registryDiscovery(this.registry, req.identity.keyId);
        const mesh = new Mesh({
            transports: [new NonClosingProvider(this.provider)],
            discovery,
            authenticator: createAuthenticator({
                localKey: req.identity,
                signingName: SIGNING_ED25519,
                kemPrefs: [KEM_X25519_HKDF],
            }),
            localKeyId: req.identity.keyId,
            listenAddresses: [addr],
        });
        return { mesh, discovery, listenAddresses: [addr], discoveryNotes: ['mem-shared'], closeables: [] };
    };

    close(): void {
        this.provider.close();
    }
}

// --- A memory platform ---

class MemHosts implements HostStore {
    private readonly records = new Map<string, HostRecord>();
    private readonly backends = new Map<string, MemDagBackend>();
    private readonly hashSuite = createBasicCrypto().hash(HASH_SHA256);

    async list(): Promise<string[]> {
        return [...this.records.keys()].sort();
    }

    async read(name: string): Promise<HostRecord | undefined> {
        const record = this.records.get(name);
        return record === undefined ? undefined : JSON.parse(JSON.stringify(record)) as HostRecord;
    }

    async create(name: string, build: (backend: DagBackend) => Promise<HostRecord>): Promise<void> {
        const backend = new MemDagBackend(this.hashSuite);
        const record = await build(backend);
        this.backends.set(name, backend);
        this.records.set(name, record);
    }

    async openReplica(name: string): Promise<DagBackend> {
        const backend = this.backends.get(name);
        if (backend === undefined) throw new Error(`no host '${name}'`);
        return backend;
    }

    async remove(name: string): Promise<void> {
        this.records.delete(name);
        this.backends.delete(name);
    }
}

// A vault whose key `from` is labeled `to`, as after renaming it in a keystore.
export class RelabeledVault implements KeyVault {
    constructor(private readonly inner: KeyVault, private readonly from: string, private readonly to: string) {}

    private spec(labelOrPrefix: string): string {
        if (labelOrPrefix === this.to) return this.from;
        if (labelOrPrefix === this.from) return '#unknown';
        return labelOrPrefix;
    }

    private relabel(record: KeyRecord): KeyRecord {
        return record.label === this.from ? { ...record, label: this.to } : record;
    }

    list(): KeyRecord[] {
        return this.inner.list().map((record) => this.relabel(record));
    }

    create(label: string, passphrase: string) {
        return this.inner.create(label, passphrase);
    }

    unlock(labelOrPrefix: string, passphrase: string) {
        return this.inner.unlock(this.spec(labelOrPrefix), passphrase);
    }

    resolvePublic(labelOrPrefix: string) {
        return this.inner.resolvePublic(this.spec(labelOrPrefix));
    }

    resolveRecord(labelOrPrefix: string): KeyRecord {
        return this.relabel(this.inner.resolveRecord(this.spec(labelOrPrefix)));
    }
}

export class MemPlatform implements HostPlatform {
    readonly vault = new MemoryKeyVault();
    // What keyVault() hands out instead of `vault`, when set.
    vaultView: KeyVault | undefined;
    readonly keystoreLocation = 'the test vault';
    readonly hosts = new MemHosts();
    readonly targets = new Map<string, MemoryTarget>();
    readonly locks = new Set<string>();
    releases: ReleaseFile[];
    readonly meshFactory: SyncMeshFactory;

    constructor(releases: ReleaseFile[], meshFactory: SyncMeshFactory) {
        this.releases = releases;
        this.meshFactory = meshFactory;
    }

    static async withKey(releases: ReleaseFile[], mesh: SharedMesh): Promise<MemPlatform> {
        const platform = new MemPlatform(releases, mesh.factory);
        await platform.vault.create('me', 'pw');
        return platform;
    }

    async keyVault(): Promise<KeyVault> {
        return this.vaultView ?? this.vault;
    }

    async passphrase(key: HostKeyConfig): Promise<string> {
        return key.passphrase === undefined ? '' : 'pw';
    }

    async readReleases(): Promise<ReleaseFile[]> {
        return this.releases;
    }

    async projectionTarget(host: string): Promise<MemoryTarget> {
        const target = new MemoryTarget({ captureChanges: true });
        this.targets.set(host, target);
        return target;
    }

    // One folder per host and mount name, kept across starts.
    readonly folders = new Map<string, MemoryDirectory>();

    async filesDirectory(host: string, mount: FilesMountConfig): Promise<MemoryDirectory> {
        const key = `${host}/${mount.name}`;
        let folder = this.folders.get(key);
        if (folder === undefined) {
            folder = new MemoryDirectory();
            this.folders.set(key, folder);
        }
        return folder;
    }

    async acquireLock(host: string): Promise<() => Promise<void>> {
        if (this.locks.has(host)) throw new Error(`host '${host}' is already running`);
        this.locks.add(host);
        return async () => { this.locks.delete(host); };
    }
}

export async function waitFor(check: () => Promise<boolean> | boolean, what: string, timeoutMs = 10_000): Promise<void> {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
        if (await check()) return;
        await new Promise((resolve) => setTimeout(resolve, 50));
    }
    if (await check()) return;
    throw new Error(`timed out waiting for ${what}`);
}

export function rowsOf(target: MemoryTarget | undefined, table: string): Record<string, unknown>[] {
    if (target === undefined || !target.hasTable(table)) return [];
    return target.getRowIds(table).map((id) => (target.getRowByRowId(table, id)?.values ?? {}) as Record<string, unknown>);
}
