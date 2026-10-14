// Small IndexedDB helpers. Transactions stay open across awaited requests as
// long as the next request is issued in the request's success turn.

export function idbRequest<T>(req: IDBRequest<T>): Promise<T> {
    return new Promise((resolve, reject) => {
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error ?? new Error('IndexedDB request failed'));
    });
}

export function idbTransactionDone(tx: IDBTransaction): Promise<void> {
    return new Promise((resolve, reject) => {
        tx.oncomplete = () => resolve();
        tx.onabort = () => reject(tx.error ?? new Error('transaction aborted'));
        tx.onerror = () => reject(tx.error ?? new Error('transaction failed'));
    });
}

export function openDatabase(
    factory: IDBFactory, name: string, version: number, upgrade: (db: IDBDatabase) => void,
): Promise<IDBDatabase> {
    return new Promise((resolve, reject) => {
        const req = factory.open(name, version);
        req.onupgradeneeded = () => upgrade(req.result);
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error ?? new Error(`could not open '${name}'`));
    });
}

export function deleteDatabase(factory: IDBFactory, name: string): Promise<void> {
    return new Promise((resolve, reject) => {
        const req = factory.deleteDatabase(name);
        req.onsuccess = () => resolve();
        req.onerror = () => reject(req.error ?? new Error(`could not delete '${name}'`));
        req.onblocked = () => { /* onsuccess follows once open connections close */ };
    });
}
