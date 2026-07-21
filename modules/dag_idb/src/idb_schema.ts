// IndexedDB schema for the DAG storage backend.
//
// The layout mirrors the SQL schema in dag_sql/src/sql_schema.ts, translated to
// IndexedDB object stores. Multiple DAGs share one database, keyed by a numeric
// dagId. Per-dag monotonic counters (nextSeq, nextTopo) live on the dags record
// and are read-incremented-written inside the flush transaction (see idb_env.ts)
// so IndexedDB's per-store locking serializes them across tabs.

export const SCHEMA_VERSION = 1;

export type IdxType = 'level' | 'topo';

export const META = 'meta';
export const DAGS = 'dags';
export const ENTRIES = 'entries';
export const FRONTIER = 'frontier';
export const ENTRY_INFO = 'entry_info';
export const LEVEL_PREDS = 'level_preds';
export const LEVEL_SUCCS = 'level_succs';
export const TOPO_INDEX = 'topo_index';
export const TOPO_PREDS = 'topo_preds';

// Primary key paths for each store. Used to derive a record's key when merging
// buffered writes with persisted range-query results.
export const KEY_PATHS: { [store: string]: string[] } = {
    [META]: ['key'],
    [DAGS]: ['dagId'],
    [ENTRIES]: ['dagId', 'hash'],
    [FRONTIER]: ['dagId', 'hash'],
    [ENTRY_INFO]: ['dagId', 'hash'],
    [LEVEL_PREDS]: ['dagId', 'level', 'node', 'pred'],
    [LEVEL_SUCCS]: ['dagId', 'level', 'node', 'succ'],
    [TOPO_INDEX]: ['dagId', 'hash'],
    [TOPO_PREDS]: ['dagId', 'node', 'pred'],
};

export type DagRecord = {
    dagId: number;
    dagHash: string;
    idxType: IdxType;
    type: string;
    createdAt: number;
    nextSeq: number;
    nextTopo: number;
};

export function reqToPromise<T>(req: IDBRequest<T>): Promise<T> {
    return new Promise<T>((resolve, reject) => {
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error);
    });
}

export function txDone(tx: IDBTransaction): Promise<void> {
    return new Promise<void>((resolve, reject) => {
        tx.oncomplete = () => resolve();
        tx.onerror = () => reject(tx.error);
        tx.onabort = () => reject(tx.error ?? new Error('transaction aborted'));
    });
}

// An empty array sorts after any number/string/date key in IndexedDB's key
// ordering, so it works as an exclusive upper bound for prefix ranges.
const MAX_KEY: IDBValidKey = [];

export function prefixRange(prefix: IDBValidKey[]): IDBKeyRange {
    return IDBKeyRange.bound(prefix as IDBValidKey, [...prefix, MAX_KEY], false, true);
}

export function keyToStr(key: IDBValidKey): string {
    return JSON.stringify(key);
}

export function keyHasPrefix(key: IDBValidKey, prefix: IDBValidKey[]): boolean {
    if (!Array.isArray(key)) return false;
    if (key.length < prefix.length) return false;
    for (let i = 0; i < prefix.length; i++) {
        if (key[i] !== prefix[i]) return false;
    }
    return true;
}

export function recordKey(store: string, rec: { [k: string]: unknown }): IDBValidKey {
    const kp = KEY_PATHS[store];
    if (kp.length === 1) return rec[kp[0]] as IDBValidKey;
    return kp.map(f => rec[f]) as IDBValidKey;
}

export function openDatabase(name: string, factory: IDBFactory): Promise<IDBDatabase> {
    return new Promise<IDBDatabase>((resolve, reject) => {
        const req = factory.open(name, SCHEMA_VERSION);

        req.onupgradeneeded = () => {
            const db = req.result;
            createStores(db);
        };

        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error);
        // Another open connection is blocking the version upgrade. Nothing we can
        // do but wait; onsuccess/onerror will fire once it resolves.
        req.onblocked = () => { /* ignore */ };
    });
}

function createStores(db: IDBDatabase): void {
    const meta = db.createObjectStore(META, { keyPath: 'key' });
    meta.put({ key: 'schema_version', value: SCHEMA_VERSION });

    const dags = db.createObjectStore(DAGS, { keyPath: 'dagId', autoIncrement: true });
    dags.createIndex('by_hash', 'dagHash', { unique: true });

    const entries = db.createObjectStore(ENTRIES, { keyPath: ['dagId', 'hash'] });
    entries.createIndex('by_seq', ['dagId', 'seq'], { unique: false });

    db.createObjectStore(FRONTIER, { keyPath: ['dagId', 'hash'] });

    db.createObjectStore(ENTRY_INFO, { keyPath: ['dagId', 'hash'] });

    const levelPreds = db.createObjectStore(LEVEL_PREDS, { keyPath: ['dagId', 'level', 'node', 'pred'] });
    levelPreds.createIndex('by_node', ['dagId', 'level', 'node'], { unique: false });

    const levelSuccs = db.createObjectStore(LEVEL_SUCCS, { keyPath: ['dagId', 'level', 'node', 'succ'] });
    levelSuccs.createIndex('by_node', ['dagId', 'level', 'node'], { unique: false });

    db.createObjectStore(TOPO_INDEX, { keyPath: ['dagId', 'hash'] });

    const topoPreds = db.createObjectStore(TOPO_PREDS, { keyPath: ['dagId', 'node', 'pred'] });
    topoPreds.createIndex('by_node', ['dagId', 'node'], { unique: false });
}

export async function getDag(db: IDBDatabase, dagHash: string): Promise<DagRecord | undefined> {
    const tx = db.transaction(DAGS, 'readonly');
    const rec = await reqToPromise<DagRecord | undefined>(
        tx.objectStore(DAGS).index('by_hash').get(dagHash) as IDBRequest<DagRecord | undefined>
    );
    return rec;
}

export async function getOrCreateDag(
    db: IDBDatabase,
    dagHash: string,
    idxType: IdxType,
    type: string
): Promise<{ record: DagRecord; created: boolean }> {
    const tx = db.transaction(DAGS, 'readwrite');
    const store = tx.objectStore(DAGS);

    const existing = await reqToPromise<DagRecord | undefined>(
        store.index('by_hash').get(dagHash) as IDBRequest<DagRecord | undefined>
    );

    if (existing !== undefined) {
        if (existing.idxType !== idxType) {
            throw new Error('DAG "' + dagHash + '" already exists with idx_type "' + existing.idxType + '", cannot open with "' + idxType + '"');
        }
        return { record: existing, created: false };
    }

    const record: Omit<DagRecord, 'dagId'> = {
        dagHash,
        idxType,
        type,
        createdAt: Date.now(),
        nextSeq: 0,
        nextTopo: 0,
    };

    const dagId = await reqToPromise<IDBValidKey>(store.add(record)) as number;
    await txDone(tx);

    return { record: { dagId, ...record }, created: true };
}
