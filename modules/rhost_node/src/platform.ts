// The Node platform for rhost: an app folder with app.json, the release files,
// and one folder per host under hosts/. Keys stay in the user's keystore
// (~/.rdb/keys.json by default), or wherever app.json's `keystore` says.
//
//   my-app/
//     app.json  catalogs/
//     hosts/<name>/
//       host.json
//       rdb/replica.rdb     the replica
//       db/data.sqlite      the projection (projection.path)
//       files/<mount>/      each projection.files path: common/, keys/<id>/, .hhs/
//       run/                rhost.lock, rhost.sock and rhost.log

import { promises as fs } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { stdin } from "node:process";
import Database from "better-sqlite3";

import { createBasicCrypto, HASH_SHA256 } from "@hyper-hyper-space/hhs3_crypto";
import { SqliteDagDb } from "@hyper-hyper-space/hhs3_dag_sqlite";
import { SqliteTarget } from "@hyper-hyper-space/hhs3_rdb_adapter_sqlite";
import type { DagBackend } from "@hyper-hyper-space/hhs3_replica";
import {
    parseHostRecord,
    type Connection, type FilesMountConfig, type HostKeyConfig, type HostPlatform, type HostRecord, type HostStore,
    type ProjectionConfig, type SyncMeshFactory,
} from "@hyper-hyper-space/hhs3_rhost";
import { NodeDirectory } from "@hyper-hyper-space/hhs3_rdb_files_node";
import { socketPathFor } from "@hyper-hyper-space/hhs3_rhost_client";
import { parseReleaseFile, type ReleaseFile } from "@hyper-hyper-space/hhs3_rpack";

import { defaultKeystorePath, KeyStore } from "./keystore.js";
import { acquireLock, lockHolder } from "./lock.js";
import { createNodeSyncMeshFactory } from "./node_mesh.js";
import { NON_INTERACTIVE, type Prompter } from "./prompter.js";
import { listenOnSocket } from "./socket.js";

export const HOST_FILE = 'host.json';
export const STORE_FILE = 'rdb/replica.rdb';
export const LOCK_FILE = 'run/rhost.lock';
export const LOG_FILE = 'run/rhost.log';

const HOST_NAME_RE = /^[A-Za-z0-9][A-Za-z0-9_-]*$/;

const hashSuite = createBasicCrypto().hash(HASH_SHA256);

export function hostsDir(dir: string): string {
    return join(dir, 'hosts');
}

export function hostDir(dir: string, name: string): string {
    return join(hostsDir(dir), name);
}

export function checkHostName(name: string): void {
    if (!HOST_NAME_RE.test(name)) {
        throw new Error(`'${name}' is not a valid host name (letters, digits, '-' and '_', not starting with a symbol)`);
    }
}

async function exists(path: string): Promise<boolean> {
    try {
        await fs.access(path);
        return true;
    } catch {
        return false;
    }
}

async function openStore(folder: string): Promise<SqliteDagDb> {
    const path = join(folder, STORE_FILE);
    await fs.mkdir(dirname(path), { recursive: true });
    return SqliteDagDb.open(path, { hashSuite });
}

// The pid serving a host, if one is alive.
export async function readLock(dir: string, name: string): Promise<number | undefined> {
    return lockHolder(join(hostDir(dir, name), LOCK_FILE));
}

class NodeHosts implements HostStore {
    constructor(private readonly dir: string) {}

    async list(): Promise<string[]> {
        let entries: string[];
        try {
            entries = await fs.readdir(hostsDir(this.dir));
        } catch (e) {
            if ((e as NodeJS.ErrnoException).code === 'ENOENT') return [];
            throw e;
        }
        const names: string[] = [];
        for (const name of entries.sort()) {
            if (name.startsWith('.')) continue;
            if (await exists(join(hostDir(this.dir, name), HOST_FILE))) names.push(name);
        }
        return names;
    }

    async read(name: string): Promise<HostRecord | undefined> {
        const path = join(hostDir(this.dir, name), HOST_FILE);
        let text: string;
        try {
            text = await fs.readFile(path, 'utf8');
        } catch (e) {
            if ((e as NodeJS.ErrnoException).code === 'ENOENT') return undefined;
            throw e;
        }
        const source = `hosts/${name}/${HOST_FILE}`;
        let value: unknown;
        try {
            value = JSON.parse(text);
        } catch (e) {
            throw new Error(`${source}: not valid JSON: ${(e as Error).message}`);
        }
        return parseHostRecord(value, source);
    }

    // Built in hosts/.<name>.pending/ and renamed into place once complete.
    async create(name: string, build: (backend: DagBackend) => Promise<HostRecord>): Promise<void> {
        checkHostName(name);
        const final = hostDir(this.dir, name);
        if (await exists(final)) throw new Error(`host '${name}' already exists`);
        const pending = join(hostsDir(this.dir), `.${name}.pending`);
        await fs.rm(pending, { recursive: true, force: true });
        await fs.mkdir(pending, { recursive: true });
        try {
            const backend = await openStore(pending);
            let record: HostRecord;
            try {
                record = await build(backend);
            } finally {
                try { backend.close(); } catch { /* the runtime closed it */ }
            }
            await fs.writeFile(join(pending, HOST_FILE), JSON.stringify(record, undefined, 2) + '\n');
            if (await exists(final)) throw new Error(`host '${name}' already exists`);
            await fs.rename(pending, final);
        } catch (err) {
            await fs.rm(pending, { recursive: true, force: true });
            throw err;
        }
    }

    async openReplica(name: string): Promise<DagBackend> {
        const folder = hostDir(this.dir, name);
        if (!await exists(join(folder, HOST_FILE))) throw new Error(`no host '${name}'`);
        return openStore(folder);
    }

    async remove(name: string): Promise<void> {
        checkHostName(name);
        const pid = await readLock(this.dir, name);
        if (pid !== undefined) throw new Error(`host '${name}' is running (pid ${pid}); stop it first`);
        const folder = hostDir(this.dir, name);
        if (!await exists(folder)) throw new Error(`no host '${name}'`);
        await fs.rm(folder, { recursive: true, force: true });
    }
}

export type NodePlatformOptions = {
    // The keystore file, relative to the app folder (app.json's `keystore`).
    // Without it, the user's keystore: defaultKeystorePath().
    keystore?: string;
    // Overrides every passphrase source (tests, embedding apps).
    passphrase?: (key: HostKeyConfig) => Promise<string>;
    // Read the passphrase from stdin once (`rhost serve --passphrase-stdin`).
    passphraseStdin?: boolean;
    prompter?: Prompter;
    meshFactory?: SyncMeshFactory;
    env?: NodeJS.ProcessEnv;
};

let stdinText: Promise<string> | undefined;

function readStdinOnce(): Promise<string> {
    stdinText ??= new Promise((resolveText, reject) => {
        const chunks: Buffer[] = [];
        stdin.on('data', (chunk: Buffer) => chunks.push(chunk));
        stdin.on('end', () => resolveText(Buffer.concat(chunks).toString('utf8').replace(/\r?\n$/, '')));
        stdin.on('error', reject);
    });
    return stdinText;
}

// The keystore an app uses: `keystore` resolved in the app folder, or the
// user's.
export function appKeystorePath(dir: string, keystore: string | undefined): string {
    return keystore !== undefined ? resolve(dir, keystore) : resolve(defaultKeystorePath());
}

export class NodePlatform implements HostPlatform {
    readonly dir: string;
    readonly hosts: HostStore;
    readonly meshFactory: SyncMeshFactory;
    readonly keystoreLocation: string;
    private readonly options: NodePlatformOptions;
    private vault: Promise<KeyStore> | undefined;
    private readonly known = new Map<string, Promise<string>>();

    constructor(dir: string, options: NodePlatformOptions = {}) {
        this.dir = resolve(dir);
        this.options = options;
        this.hosts = new NodeHosts(this.dir);
        this.meshFactory = options.meshFactory ?? createNodeSyncMeshFactory();
        this.keystoreLocation = appKeystorePath(this.dir, options.keystore);
    }

    keyVault(): Promise<KeyStore> {
        this.vault ??= KeyStore.open(this.keystoreLocation, hashSuite);
        return this.vault;
    }

    // A passphrase the caller already has, such as a key's it just created:
    // used for that key whatever its source says.
    rememberPassphrase(keyId: string, passphrase: string): void {
        this.known.set(keyId, Promise.resolve(passphrase));
    }

    // A prompt is asked once per key.
    async passphrase(key: HostKeyConfig): Promise<string> {
        if (this.options.passphrase !== undefined) return this.options.passphrase(key);
        const known = this.known.get(key.keyId);
        if (known !== undefined) return known;
        if (this.options.passphraseStdin === true) return readStdinOnce();
        const source = key.passphrase;
        if (source === undefined) return '';
        if (source.startsWith('env:')) {
            const name = source.slice('env:'.length);
            const value = (this.options.env ?? process.env)[name];
            if (value === undefined) throw new Error(`the key '${key.label}' takes its passphrase from ${name}, which is not set`);
            return value;
        }
        const prompter = this.options.prompter ?? NON_INTERACTIVE;
        if (!prompter.interactive) {
            throw new Error(`the passphrase of key '${key.label}' is needed; run in a terminal, or set key.passphrase in host.json to env:<VAR>`);
        }
        const asked = prompter.secret(`passphrase (${key.label}): `);
        this.known.set(key.keyId, asked);
        return asked;
    }

    async readReleases(folder: string): Promise<ReleaseFile[]> {
        const path = resolve(this.dir, folder);
        let names: string[];
        try {
            names = await fs.readdir(path);
        } catch (e) {
            if ((e as NodeJS.ErrnoException).code === 'ENOENT') throw new Error(`no releases folder at ${path}`);
            throw e;
        }
        const files: ReleaseFile[] = [];
        for (const name of names.filter((n) => n.endsWith('.rpack')).sort()) {
            try {
                files.push(parseReleaseFile(await fs.readFile(join(path, name), 'utf8')));
            } catch (e) {
                throw new Error(`${join(folder, name)}: ${(e as Error).message}`);
            }
        }
        return files;
    }

    async projectionTarget(host: string, config: ProjectionConfig): Promise<SqliteTarget> {
        const path = resolve(hostDir(this.dir, host), config.path);
        await fs.mkdir(dirname(path), { recursive: true });
        return new SqliteTarget(new Database(path), { captureChanges: true, dbPath: path });
    }

    async filesDirectory(host: string, mount: FilesMountConfig): Promise<NodeDirectory> {
        return NodeDirectory.open(resolve(hostDir(this.dir, host), mount.path));
    }

    acquireLock(host: string): Promise<() => Promise<void>> {
        return acquireLock(join(hostDir(this.dir, host), LOCK_FILE), `host '${host}'`);
    }

    listen(host: string, onConnection: (connection: Connection) => void): Promise<() => Promise<void>> {
        return listenOnSocket(socketPath(this.dir, host), onConnection);
    }
}

// Where the running host listens.
export function socketPath(dir: string, name: string): string {
    return socketPathFor(hostDir(resolve(dir), name), process.platform);
}

export function nodePlatform(dir: string, options: NodePlatformOptions = {}): NodePlatform {
    return new NodePlatform(dir, options);
}
