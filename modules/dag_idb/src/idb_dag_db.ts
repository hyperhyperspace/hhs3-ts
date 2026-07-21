import { dag, Dag, HashSuite } from "@hyper-hyper-space/hhs3_dag";
import { createDagLevelIndex } from "@hyper-hyper-space/hhs3_dag/dist/idx/level/level_idx.js";
import { createDagTopoIndex } from "@hyper-hyper-space/hhs3_dag/dist/idx/topo/topo_idx.js";

import { DagRecord, DAGS, IdxType, getDag, getOrCreateDag as getOrCreateIdbDag } from "./idb_schema.js";
import { IdbEnv, IdbTx } from "./idb_env.js";
import { BroadcastIdbDagStore } from "./broadcast_idb_dag_store.js";
import { IdbLevelIndexStore } from "./idb_level_index_store.js";
import { IdbTopoIndexStore } from "./idb_topo_index_store.js";

export type IdbDagDbOptions = {
    hashSuite: HashSuite;
    defaultIdxType?: IdxType;
    // Explicit IndexedDB factory. Defaults to the global `indexedDB`. Useful for
    // tests (fake-indexeddb) or non-window environments.
    indexedDB?: IDBFactory;
};

export type IdbDagMeta = {
    type: string;
    idxType?: IdxType;
};

export type IdbDagEntry = {
    id: string;
    type: string;
    createdAt: number;
};

type CachedDag = {
    dag: Dag;
    store: BroadcastIdbDagStore;
    entry: IdbDagEntry;
    idxType: IdxType;
};

const DEFAULT_IDX_TYPE: IdxType = 'level';

// High-level manager for a set of DAGs living in one IndexedDB database. Analog
// of dag_sqlite's SqliteDagDb. All DAGs share a single IDBDatabase connection
// (IndexedDB is single-threaded, so there is no benefit to per-DAG connections).
export class IdbDagDb {

    private env: IdbEnv;
    private name: string;
    private hash: HashSuite;
    private defaultIdxType: IdxType;
    private dagCache: Map<string, CachedDag>;
    private closed = false;

    private constructor(env: IdbEnv, name: string, opts: IdbDagDbOptions) {
        this.env = env;
        this.name = name;
        this.hash = opts.hashSuite;
        this.defaultIdxType = opts.defaultIdxType ?? DEFAULT_IDX_TYPE;
        this.dagCache = new Map();
    }

    static async open(name: string, opts: IdbDagDbOptions): Promise<IdbDagDb> {
        const factory = opts.indexedDB ?? globalThis.indexedDB;
        if (factory === undefined) {
            throw new Error('No IndexedDB factory available; pass opts.indexedDB in non-browser environments');
        }
        const env = await IdbEnv.open(name, factory);
        return new IdbDagDb(env, name, opts);
    }

    async getOrCreateDag(id: string, meta: IdbDagMeta): Promise<{ dag: Dag; created: boolean }> {
        this.ensureOpen();

        const cached = this.dagCache.get(id);
        if (cached !== undefined) {
            this.validateCachedDag(id, cached, meta);
            return { dag: cached.dag, created: false };
        }

        const existing = await getDag(this.env.db, id);

        let record: DagRecord;
        let created: boolean;

        if (existing !== undefined) {
            if (meta.idxType !== undefined && meta.idxType !== existing.idxType) {
                throw new Error('DAG "' + id + '" already exists with idx_type "' + existing.idxType + '", cannot open with "' + meta.idxType + '"');
            }
            if (existing.type !== meta.type) {
                throw new Error('DAG "' + id + '" already exists with type "' + existing.type + '", cannot open with "' + meta.type + '"');
            }
            record = existing;
            created = false;
        } else {
            const idxType = meta.idxType ?? this.defaultIdxType;
            const result = await getOrCreateIdbDag(this.env.db, id, idxType, meta.type);
            record = result.record;
            created = result.created;
        }

        const cachedDag = this.buildAndCache(id, record);
        return { dag: cachedDag.dag, created };
    }

    async openDag(id: string): Promise<Dag | undefined> {
        this.ensureOpen();

        const cached = this.dagCache.get(id);
        if (cached !== undefined) return cached.dag;

        const record = await getDag(this.env.db, id);
        if (record === undefined) return undefined;

        return this.buildAndCache(id, record).dag;
    }

    async listDags(): Promise<IdbDagEntry[]> {
        this.ensureOpen();

        const records = await this.env.getAll(DAGS) as DagRecord[];
        return records
            .map(r => ({ id: r.dagHash, type: r.type, createdAt: r.createdAt }))
            .sort((a, b) => a.createdAt - b.createdAt);
    }

    close(): void {
        if (this.closed) return;
        this.closed = true;

        for (const cached of this.dagCache.values()) {
            cached.store.close();
        }
        this.dagCache.clear();
        this.env.close();
    }

    private buildAndCache(id: string, record: DagRecord): CachedDag {
        const store = new BroadcastIdbDagStore(this.env, record.dagId, this.name);
        const dagInstance = buildIdbDag(this.env, store, record.dagId, record.idxType, this.hash);
        const cached: CachedDag = {
            dag: dagInstance,
            store,
            entry: { id, type: record.type, createdAt: record.createdAt },
            idxType: record.idxType,
        };
        this.dagCache.set(id, cached);
        return cached;
    }

    private validateCachedDag(id: string, cached: CachedDag, meta: IdbDagMeta): void {
        if (meta.idxType !== undefined && meta.idxType !== cached.idxType) {
            throw new Error('DAG "' + id + '" already exists with idx_type "' + cached.idxType + '", cannot open with "' + meta.idxType + '"');
        }
        if (meta.type !== cached.entry.type) {
            throw new Error('DAG "' + id + '" already exists with type "' + cached.entry.type + '", cannot open with "' + meta.type + '"');
        }
    }

    private ensureOpen(): void {
        if (this.closed) {
            throw new Error('IdbDagDb is closed');
        }
    }
}

function buildIdbDag(env: IdbEnv, store: BroadcastIdbDagStore, dagId: number, idxType: IdxType, hash: HashSuite): Dag {
    if (idxType === 'level') {
        const indexStore = new IdbLevelIndexStore(env, dagId);
        const index = createDagLevelIndex<IdbTx>(store, indexStore);
        return dag.create(store, index, hash);
    } else {
        const indexStore = new IdbTopoIndexStore(env, dagId);
        const index = createDagTopoIndex<IdbTx>(store, indexStore);
        return dag.create(store, index, hash);
    }
}
