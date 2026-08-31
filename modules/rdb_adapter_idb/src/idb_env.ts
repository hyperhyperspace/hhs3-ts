// IdbEnv owns the IDBDatabase and a per-tab write queue. Adapter apply() /
// commitIngest / reserveMint already have their action lists computed, so they
// run inside one live readwrite transaction (awaiting only IDB requests keeps
// the tx alive). That is the same atomic flush as dag_idb's IdbTx, without a
// JS write buffer: there is no CPU gap between planner and IO.
//
// Id allocation happens inside that transaction so IndexedDB's per-store
// locking serializes nextId / nextKeyId across tabs.

import {
    ALL_STORES,
    openDatabase,
    prefixRange,
    reqToPromise,
    txDone,
} from "./idb_schema.js";

export class IdbEnv {

    readonly db: IDBDatabase;
    private queue: Promise<unknown> = Promise.resolve();

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

    // One readwrite transaction over every physical store. `fn` must only await
    // IDB requests (no timers / network) so the transaction does not autocommit.
    // A throw aborts, rolling back schema+rows+checkpoint together.
    withReadWrite<T>(fn: (tx: IDBTransaction) => Promise<T>): Promise<T> {
        const run = this.queue.then(async () => {
            const tx = this.db.transaction(ALL_STORES, 'readwrite');
            try {
                const result = await fn(tx);
                await txDone(tx);
                return result;
            } catch (e) {
                try { tx.abort(); } catch { /* already finished or aborting */ }
                throw e;
            }
        });
        this.queue = run.catch(() => undefined);
        return run;
    }

    async withRead<T>(stores: string | string[], fn: (tx: IDBTransaction) => Promise<T>): Promise<T> {
        const names = Array.isArray(stores) ? stores : [stores];
        const tx = this.db.transaction(names, 'readonly');
        const result = await fn(tx);
        await txDone(tx);
        return result;
    }
}

export function storeGet<T>(tx: IDBTransaction, store: string, key: IDBValidKey): Promise<T | undefined> {
    return reqToPromise(tx.objectStore(store).get(key) as IDBRequest<T | undefined>);
}

export function storePut(tx: IDBTransaction, store: string, value: unknown): Promise<IDBValidKey> {
    return reqToPromise(tx.objectStore(store).put(value));
}

export function storeAdd(tx: IDBTransaction, store: string, value: unknown): Promise<IDBValidKey> {
    return reqToPromise(tx.objectStore(store).add(value));
}

export function storeDelete(tx: IDBTransaction, store: string, key: IDBValidKey): Promise<void> {
    return reqToPromise(tx.objectStore(store).delete(key)).then(() => undefined);
}

export function storeGetAll<T>(tx: IDBTransaction, store: string, range?: IDBKeyRange): Promise<T[]> {
    return reqToPromise(tx.objectStore(store).getAll(range) as IDBRequest<T[]>);
}

export function storeGetAllPrefix<T>(tx: IDBTransaction, store: string, prefix: IDBValidKey[]): Promise<T[]> {
    return storeGetAll<T>(tx, store, prefixRange(prefix));
}

export function indexGet<T>(
    tx: IDBTransaction, store: string, indexName: string, key: IDBValidKey,
): Promise<T | undefined> {
    return reqToPromise(tx.objectStore(store).index(indexName).get(key) as IDBRequest<T | undefined>);
}

export async function deletePrefix(tx: IDBTransaction, store: string, prefix: IDBValidKey[], keyOf: (rec: any) => IDBValidKey): Promise<void> {
    const recs = await storeGetAllPrefix<any>(tx, store, prefix);
    for (const rec of recs) await storeDelete(tx, store, keyOf(rec));
}
