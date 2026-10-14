// Host records in one IndexedDB database. A host is pending while it is built
// and published only once `build` returns its record. `list` is the published
// names. The stopped flag lives on the same row: a fresh serve clears it, and
// stop sets it so a queued tab does not take over.

import { createBasicCrypto, HASH_SHA256 } from "@hyper-hyper-space/hhs3_crypto";
import { IdbDagDb } from "@hyper-hyper-space/hhs3_dag_idb";
import type { DagBackend } from "@hyper-hyper-space/hhs3_replica";
import type { HostRecord, HostStore } from "@hyper-hyper-space/hhs3_rhost";
import { BROWSER_HOSTS_STORE, BROWSER_HOSTS_VERSION, browserReplicaDatabase } from "@hyper-hyper-space/hhs3_rhost_client";

import { deleteDatabase, idbRequest, idbTransactionDone, openDatabase } from "./idb.js";

const hashSuite = createBasicCrypto().hash(HASH_SHA256);

export type HostRow = {
    name: string;
    state: 'pending' | 'published';
    record?: HostRecord;
    stopped?: boolean;
};

export class IdbHosts implements HostStore {
    private db: Promise<IDBDatabase> | undefined;

    constructor(
        private readonly app: string,
        private readonly factory: IDBFactory,
        private readonly databaseName: string,
        private readonly removeFiles: (host: string) => Promise<void>,
        private readonly lockHeld: (host: string) => Promise<boolean>,
    ) {}

    private database(): Promise<IDBDatabase> {
        this.db ??= openDatabase(this.factory, this.databaseName, BROWSER_HOSTS_VERSION, (db) => {
            if (!db.objectStoreNames.contains(BROWSER_HOSTS_STORE)) {
                db.createObjectStore(BROWSER_HOSTS_STORE, { keyPath: 'name' });
            }
        });
        return this.db;
    }

    private async readRow(name: string): Promise<HostRow | undefined> {
        const db = await this.database();
        const tx = db.transaction(BROWSER_HOSTS_STORE, 'readonly');
        const row = await idbRequest<HostRow | undefined>(tx.objectStore(BROWSER_HOSTS_STORE).get(name));
        await idbTransactionDone(tx);
        return row;
    }

    private async writeRow(row: HostRow): Promise<void> {
        const db = await this.database();
        const tx = db.transaction(BROWSER_HOSTS_STORE, 'readwrite');
        await idbRequest(tx.objectStore(BROWSER_HOSTS_STORE).put(row));
        await idbTransactionDone(tx);
    }

    async close(): Promise<void> {
        if (this.db === undefined) return;
        const db = await this.db;
        db.close();
        this.db = undefined;
    }

    async list(): Promise<string[]> {
        const db = await this.database();
        const tx = db.transaction(BROWSER_HOSTS_STORE, 'readonly');
        const rows = await idbRequest<HostRow[]>(tx.objectStore(BROWSER_HOSTS_STORE).getAll());
        await idbTransactionDone(tx);
        return rows.filter((row) => row.state === 'published').map((row) => row.name).sort();
    }

    async read(name: string): Promise<HostRecord | undefined> {
        const row = await this.readRow(name);
        if (row?.state !== 'published' || row.record === undefined) return undefined;
        return row.record;
    }

    async isStopped(name: string): Promise<boolean> {
        return (await this.readRow(name))?.stopped === true;
    }

    async setStopped(name: string, stopped: boolean): Promise<void> {
        const row = await this.readRow(name);
        if (row === undefined) return;
        if (stopped) row.stopped = true;
        else delete row.stopped;
        await this.writeRow(row);
    }

    async create(name: string, build: (backend: DagBackend) => Promise<HostRecord>): Promise<void> {
        const existing = await this.readRow(name);
        if (existing !== undefined) throw new Error(`host '${name}' already exists`);
        await this.writeRow({ name, state: 'pending' });
        const replicaName = browserReplicaDatabase(this.app, name);
        const backend = await IdbDagDb.open(replicaName, { hashSuite, indexedDB: this.factory });
        try {
            const record = await build(backend);
            try { backend.close(); } catch { /* the runtime closed it */ }
            const pending = await this.readRow(name);
            await this.writeRow({
                name, state: 'published', record,
                ...(pending?.stopped === true ? { stopped: true } : {}),
            });
        } catch (err) {
            try { backend.close(); } catch { /* already closed */ }
            await this.discard(name, replicaName);
            throw err;
        }
    }

    async openReplica(name: string): Promise<DagBackend> {
        const row = await this.readRow(name);
        if (row?.state !== 'published') throw new Error(`no host '${name}'`);
        return IdbDagDb.open(browserReplicaDatabase(this.app, name), { hashSuite, indexedDB: this.factory });
    }

    async remove(name: string): Promise<void> {
        if (await this.lockHeld(name)) throw new Error(`host '${name}' is running; stop it first`);
        const row = await this.readRow(name);
        if (row === undefined) throw new Error(`no host '${name}'`);
        await this.discard(name, browserReplicaDatabase(this.app, name));
    }

    private async discard(name: string, replicaName: string): Promise<void> {
        const db = await this.database();
        const tx = db.transaction(BROWSER_HOSTS_STORE, 'readwrite');
        await idbRequest(tx.objectStore(BROWSER_HOSTS_STORE).delete(name));
        await idbTransactionDone(tx);
        await deleteDatabase(this.factory, replicaName);
        await this.removeFiles(name);
    }
}
