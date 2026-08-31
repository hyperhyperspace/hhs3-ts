// IndexedDB schema for the rdb_adapter projection target.
//
// Logical app tables are data, not object stores: a fixed set of stores holds
// rows, sync mappings, meta, keys, outbox, and op-events. create-table is a
// meta put, so apply() never needs a versionchange upgrade (and never blocks
// other tabs with onblocked).

import type { json } from "@hyper-hyper-space/hhs3_json";
import type { ColumnType } from "@hyper-hyper-space/hhs3_rdb";
import type { OpEvent, SyncStatus } from "@hyper-hyper-space/hhs3_rdb_adapter";

export const SCHEMA_VERSION = 1;

export const TABLE_META = 'table_meta';
export const CHECKPOINT = 'checkpoint';
export const ROWS = 'rows';
export const SYNC = 'sync';
export const KEYS = 'keys';
export const COUNTERS = 'counters';
export const OUTBOX = 'outbox';
export const OP_EVENTS = 'op_events';
export const CAPTURE_CONFIG = 'capture_config';

export const ALL_STORES = [
    TABLE_META, CHECKPOINT, ROWS, SYNC, KEYS, COUNTERS, OUTBOX, OP_EVENTS, CAPTURE_CONFIG,
];

export const COUNTER_NEXT_KEY_ID = 'nextKeyId';
export const CAPTURE_KEY = 'capture';

export type TableMetaRecord = {
    table: string;
    idColumn: string;
    authorColumn?: string;
    syncTable: string;
    columnTypes: { [column: string]: ColumnType };
    fkColumns: { [column: string]: { targetTable: string } };
    keyRefColumns: string[];
    defaults: { [column: string]: json.Literal };
    nextId: number;
};

export type CheckpointRecord = {
    groupId: string;
    version: string[];
};

export type RowRecord = {
    table: string;
    id: number;
    [column: string]: json.Literal | undefined;
};

export type SyncRecord = {
    table: string;
    rowHash: string;
    id: number;
    uuid: string;
    status: SyncStatus;
};

export type KeyRecord = {
    id: number;
    keyHash: string;
    publicKey?: string;
};

export type CounterRecord = {
    key: string;
    value: number;
};

export type OutboxRecord = {
    id?: number;
    table: string;
    localId: number;
    op: 'insert' | 'update' | 'delete';
    changed?: { [column: string]: json.Literal };
};

export type OpEventRecord = OpEvent & { id?: number };

export type CaptureConfigRecord = {
    key: string;
    enabled: boolean;
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

export function openDatabase(name: string, factory: IDBFactory): Promise<IDBDatabase> {
    return new Promise<IDBDatabase>((resolve, reject) => {
        const req = factory.open(name, SCHEMA_VERSION);

        req.onupgradeneeded = () => {
            createStores(req.result);
        };

        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error);
        req.onblocked = () => { /* wait for other connections to close */ };
    });
}

function createStores(db: IDBDatabase): void {
    db.createObjectStore(TABLE_META, { keyPath: 'table' });
    db.createObjectStore(CHECKPOINT, { keyPath: 'groupId' });

    db.createObjectStore(ROWS, { keyPath: ['table', 'id'] });

    const sync = db.createObjectStore(SYNC, { keyPath: ['table', 'rowHash'] });
    sync.createIndex('by_local_id', ['table', 'id'], { unique: true });

    const keys = db.createObjectStore(KEYS, { keyPath: 'id' });
    keys.createIndex('by_hash', 'keyHash', { unique: true });

    db.createObjectStore(COUNTERS, { keyPath: 'key' });

    db.createObjectStore(OUTBOX, { keyPath: 'id', autoIncrement: true });

    const events = db.createObjectStore(OP_EVENTS, { keyPath: 'id', autoIncrement: true });
    events.createIndex('by_op', ['opHash', 'direction'], { unique: true });

    db.createObjectStore(CAPTURE_CONFIG, { keyPath: 'key' });
}
