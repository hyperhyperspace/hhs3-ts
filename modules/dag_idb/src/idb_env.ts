// IdbEnv owns the IDBDatabase and a per-tab serialization queue. IdbTx is the
// unit-of-work context passed as the generic `Tx` to the DAG store and index
// stores: it buffers writes and reads through to persisted state, then flushes
// everything in a single readwrite transaction.
//
// Why buffer instead of using a raw IDBTransaction as Tx? An IndexedDB
// transaction auto-commits as soon as control returns to the event loop with no
// pending request. The DAG's append flow interleaves index computation (pure JS)
// with reads and writes, so a raw transaction would die mid-flight. Buffering
// sidesteps this: only the flush touches IndexedDB, as one atomic transaction.

import {
    DAGS,
    ENTRIES,
    ENTRY_INFO,
    TOPO_INDEX,
    DagRecord,
    keyHasPrefix,
    keyToStr,
    openDatabase,
    prefixRange,
    recordKey,
    reqToPromise,
    txDone,
} from "./idb_schema.js";

// Point-get results from these stores are safe to cache forever with no
// invalidation: their records are write-once (content-addressed entries, and
// per-node index info assigned exactly once). The only rule is to never cache a
// miss, so a later append of a new key can't turn a stale `undefined` into a
// wrong hit. Mutable stores (level_succs, frontier, dags) are never cached.
const CACHEABLE_POINT_STORES = new Set<string>([ENTRIES, ENTRY_INFO, TOPO_INDEX]);

// A read source: either the environment (reads committed state) or a unit of
// work (reads committed state with buffered writes overlaid).
export interface IdbReader {
    get(store: string, key: IDBValidKey): Promise<any | undefined>;
    getAllByPrefix(store: string, indexName: string | null, prefix: IDBValidKey[]): Promise<any[]>;
}

// A counter to assign at flush time, inside the readwrite transaction.
export type CounterAssign = { field: string; counter: 'seq' | 'topo' };

type BufferedOp = {
    store: string;
    key: IDBValidKey;
    kind: 'put' | 'del';
    value?: any;
    assign?: CounterAssign;
};

export class IdbEnv implements IdbReader {

    readonly db: IDBDatabase;
    private queue: Promise<unknown> = Promise.resolve();
    // Keyed by `${store}|${keyToStr(key)}`; only ever holds found, immutable
    // records from CACHEABLE_POINT_STORES. Never needs invalidation.
    private pointCache = new Map<string, any>();

    constructor(db: IDBDatabase) {
        this.db = db;
    }

    static async open(name: string, factory: IDBFactory): Promise<IdbEnv> {
        const db = await openDatabase(name, factory);
        return new IdbEnv(db);
    }

    close(): void {
        this.db.close();
    }

    async get(store: string, key: IDBValidKey): Promise<any | undefined> {
        const cacheable = CACHEABLE_POINT_STORES.has(store);
        const cacheKey = cacheable ? store + '|' + keyToStr(key) : '';

        if (cacheable) {
            const hit = this.pointCache.get(cacheKey);
            if (hit !== undefined) return hit;
        }

        const tx = this.db.transaction(store, 'readonly');
        const result = await reqToPromise(tx.objectStore(store).get(key));

        // Cache only found records; never cache a miss (see CACHEABLE_POINT_STORES).
        if (cacheable && result !== undefined) {
            this.pointCache.set(cacheKey, result);
        }

        return result;
    }

    async getAllByPrefix(store: string, indexName: string | null, prefix: IDBValidKey[]): Promise<any[]> {
        const tx = this.db.transaction(store, 'readonly');
        const objStore = tx.objectStore(store);
        const source: IDBObjectStore | IDBIndex = indexName === null ? objStore : objStore.index(indexName);
        return reqToPromise(source.getAll(prefixRange(prefix)));
    }

    async getAll(store: string): Promise<any[]> {
        const tx = this.db.transaction(store, 'readonly');
        return reqToPromise(tx.objectStore(store).getAll());
    }

    // Run a unit of work. Only one runs at a time within this tab; all writes are
    // buffered and flushed atomically on success. `committed` is false when the
    // flush was a no-op because the appended entry already existed (idempotency).
    withUnitOfWork<T>(dagId: number, fn: (tx: IdbTx) => Promise<T>): Promise<{ result: T; committed: boolean }> {
        const run = this.queue.then(async () => {
            const tx = new IdbTx(this, dagId);
            const result = await fn(tx);
            const committed = await tx.commit();
            return { result, committed };
        });
        this.queue = run.catch(() => undefined);
        return run;
    }
}

export class IdbTx implements IdbReader {

    private env: IdbEnv;
    private dagId: number;
    // Latest-write-wins per key, in insertion order (Map preserves it). Insertion
    // order determines counter assignment order at flush.
    private ops = new Map<string, BufferedOp>();

    constructor(env: IdbEnv, dagId: number) {
        this.env = env;
        this.dagId = dagId;
    }

    private opKey(store: string, key: IDBValidKey): string {
        return store + '|' + keyToStr(key);
    }

    putRecord(store: string, value: any, key: IDBValidKey, assign?: CounterAssign): void {
        this.ops.set(this.opKey(store, key), { store, key, kind: 'put', value, assign });
    }

    deleteRecord(store: string, key: IDBValidKey): void {
        this.ops.set(this.opKey(store, key), { store, key, kind: 'del' });
    }

    async get(store: string, key: IDBValidKey): Promise<any | undefined> {
        const op = this.ops.get(this.opKey(store, key));
        if (op !== undefined) {
            return op.kind === 'del' ? undefined : op.value;
        }
        return this.env.get(store, key);
    }

    async getAllByPrefix(store: string, indexName: string | null, prefix: IDBValidKey[]): Promise<any[]> {
        const base = await this.env.getAllByPrefix(store, indexName, prefix);

        const merged = new Map<string, any>();
        for (const rec of base) {
            merged.set(keyToStr(recordKey(store, rec)), rec);
        }
        for (const op of this.ops.values()) {
            if (op.store !== store) continue;
            if (!keyHasPrefix(op.key, prefix)) continue;
            const ks = keyToStr(op.key);
            if (op.kind === 'del') {
                merged.delete(ks);
            } else {
                merged.set(ks, op.value);
            }
        }
        return [...merged.values()];
    }

    // Flush all buffered writes in a single readwrite transaction. Returns false
    // if the append was a no-op because its entry already existed (committed by a
    // previous transaction or concurrently by another tab).
    async commit(): Promise<boolean> {
        if (this.ops.size === 0) return false;

        const storeNames = new Set<string>([DAGS, ENTRIES]);
        for (const op of this.ops.values()) storeNames.add(op.store);

        const tx = this.env.db.transaction([...storeNames], 'readwrite');

        // Idempotency: if any appended entry already exists, skip the whole unit
        // of work. Reading `entries` inside this readwrite transaction is what
        // serializes concurrent appends across tabs (IndexedDB per-store locking).
        for (const op of this.ops.values()) {
            if (op.store === ENTRIES && op.kind === 'put') {
                const existing = await reqToPromise(tx.objectStore(ENTRIES).get(op.key));
                if (existing !== undefined) {
                    await txDone(tx);
                    return false;
                }
            }
        }

        // Resolve counters inside the transaction so no two flushes collide.
        const dagStore = tx.objectStore(DAGS);
        const dagRec = await reqToPromise<DagRecord>(dagStore.get(this.dagId));
        let nextSeq = dagRec.nextSeq;
        let nextTopo = dagRec.nextTopo;

        for (const op of this.ops.values()) {
            const os = tx.objectStore(op.store);
            if (op.kind === 'del') {
                os.delete(op.key);
                continue;
            }
            const value = op.value;
            if (op.assign !== undefined) {
                value[op.assign.field] = op.assign.counter === 'seq' ? nextSeq++ : nextTopo++;
            }
            os.put(value);
        }

        dagRec.nextSeq = nextSeq;
        dagRec.nextTopo = nextTopo;
        dagStore.put(dagRec);

        await txDone(tx);
        return true;
    }
}
