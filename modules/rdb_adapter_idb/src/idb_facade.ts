// Duck-typed IDBDatabase / IDBTransaction / IDBObjectStore over the fixed
// physical stores. Virtual store names are the materialized app tables plus
// `rdb_keys`. Schema ops throw: the adapter owns schema via SchemaAction.
//
// App writes (put/add/delete/clear) go through this facade and, when capture
// is on, also insert an outbox row in the SAME native transaction. Adapter
// apply() never uses the facade, so materialization cannot echo.

import type { json } from "@hyper-hyper-space/hhs3_json";
import { DEFAULT_KEY_TABLE } from "@hyper-hyper-space/hhs3_rdb_adapter";

import {
    KEYS, OUTBOX, ROWS, TABLE_META, prefixRange,
    type OutboxRecord, type RowRecord, type TableMetaRecord,
} from "./idb_schema.js";

export interface FacadeHost {
    readonly db: IDBDatabase;
    appTables(): string[];
    isCaptureOn(): boolean;
    onOutboxCommitted(): void;
}

class NameList {
    private names: string[];
    constructor(names: string[]) { this.names = [...names]; }
    get length(): number { return this.names.length; }
    contains(name: string): boolean { return this.names.includes(name); }
    item(i: number): string | null { return this.names[i] ?? null; }
    *[Symbol.iterator](): IterableIterator<string> { yield* this.names; }
}

function notFound(name: string): DOMException {
    return new DOMException(`One of the specified object stores was not found: '${name}'`, 'NotFoundError');
}

function constraintError(): DOMException {
    return new DOMException('Key already exists in the object store.', 'ConstraintError');
}

function invalidState(message: string): DOMException {
    return new DOMException(message, 'InvalidStateError');
}

export class FacadeRequest<T> {
    result!: T;
    error: DOMException | null = null;
    readyState: IDBRequestReadyState = 'pending';
    source: object | null = null;
    private _onsuccess: ((ev: Event) => void) | null = null;
    private _onerror: ((ev: Event) => void) | null = null;

    get onsuccess(): ((ev: Event) => void) | null { return this._onsuccess; }
    set onsuccess(h: ((ev: Event) => void) | null) {
        this._onsuccess = h;
        if (this.readyState === 'done' && this.error === null && h !== null) {
            queueMicrotask(() => h.call(this, new Event('success')));
        }
    }

    get onerror(): ((ev: Event) => void) | null { return this._onerror; }
    set onerror(h: ((ev: Event) => void) | null) {
        this._onerror = h;
        if (this.readyState === 'done' && this.error !== null && h !== null) {
            queueMicrotask(() => h.call(this, new Event('error')));
        }
    }

    succeed(value: T): void {
        this.result = value;
        this.readyState = 'done';
        this._onsuccess?.(new Event('success'));
    }

    fail(error: DOMException | Error): void {
        this.error = error as DOMException;
        this.readyState = 'done';
        this._onerror?.(new Event('error'));
    }
}

function stripTable(rec: RowRecord | undefined): Record<string, json.Literal> | undefined {
    if (rec === undefined) return undefined;
    const out: Record<string, json.Literal> = {};
    for (const [k, v] of Object.entries(rec)) {
        if (k === 'table' || v === undefined) continue;
        out[k] = v as json.Literal;
    }
    return out;
}

function businessValues(rec: Record<string, json.Literal>, meta: TableMetaRecord): { [column: string]: json.Literal } {
    const values: { [column: string]: json.Literal } = {};
    for (const col of Object.keys(meta.columnTypes)) {
        if (rec[col] !== undefined) values[col] = rec[col];
    }
    return values;
}

function changedColumns(
    oldRec: Record<string, json.Literal> | undefined,
    newRec: Record<string, json.Literal>,
    meta: TableMetaRecord,
): { [column: string]: json.Literal } {
    const changed: { [column: string]: json.Literal } = {};
    for (const col of Object.keys(meta.columnTypes)) {
        const next = newRec[col];
        const prev = oldRec?.[col];
        if (next !== prev) {
            if (next !== undefined) changed[col] = next;
        }
    }
    return changed;
}

export class FacadeObjectStore {
    readonly name: string;
    readonly keyPath: string | string[] = 'id';
    readonly autoIncrement = false;
    readonly indexNames = new NameList([]);
    readonly transaction: FacadeTransaction;

    constructor(name: string, transaction: FacadeTransaction) {
        this.name = name;
        this.transaction = transaction;
    }

    createIndex(_name: string, _keyPath: string | string[], _options?: IDBIndexParameters): never {
        throw invalidState('The database is not running a version change transaction.');
    }

    deleteIndex(_name: string): never {
        throw invalidState('The database is not running a version change transaction.');
    }

    index(_name: string): never {
        throw new DOMException(`Index '${_name}' does not exist`, 'NotFoundError');
    }

    get(key: IDBValidKey): FacadeRequest<Record<string, json.Literal> | undefined> {
        const req = new FacadeRequest<Record<string, json.Literal> | undefined>();
        if (this.name === DEFAULT_KEY_TABLE) {
            const inner = this.transaction.real.objectStore(KEYS).get(key);
            inner.onsuccess = () => req.succeed(inner.result as Record<string, json.Literal> | undefined);
            inner.onerror = () => req.fail(inner.error ?? new Error('get failed'));
            return req;
        }
        const inner = this.transaction.real.objectStore(ROWS).get([this.name, key]);
        inner.onsuccess = () => req.succeed(stripTable(inner.result as RowRecord | undefined));
        inner.onerror = () => req.fail(inner.error ?? new Error('get failed'));
        return req;
    }

    getAll(query?: IDBValidKey | IDBKeyRange, count?: number): FacadeRequest<Record<string, json.Literal>[]> {
        const req = new FacadeRequest<Record<string, json.Literal>[]>();
        if (this.name === DEFAULT_KEY_TABLE) {
            const inner = this.transaction.real.objectStore(KEYS).getAll(query, count);
            inner.onsuccess = () => req.succeed(inner.result as Record<string, json.Literal>[]);
            inner.onerror = () => req.fail(inner.error ?? new Error('getAll failed'));
            return req;
        }
        const range = query === undefined
            ? undefined
            : query instanceof IDBKeyRange
                ? query
                : undefined;
        // Virtual getAll is a table-prefix scan. A key-range on the virtual id
        // is not forwarded (physical keys are [table, id]).
        const inner = range === undefined
            ? this.transaction.real.objectStore(ROWS).getAll(prefixRange([this.name]))
            : this.transaction.real.objectStore(ROWS).getAll(range, count);
        inner.onsuccess = () => {
            let rows = (inner.result as RowRecord[]).map((r) => stripTable(r)!);
            if (count !== undefined) rows = rows.slice(0, count);
            req.succeed(rows);
        };
        inner.onerror = () => req.fail(inner.error ?? new Error('getAll failed'));
        return req;
    }

    getAllKeys(query?: IDBValidKey | IDBKeyRange, count?: number): FacadeRequest<IDBValidKey[]> {
        const req = new FacadeRequest<IDBValidKey[]>();
        const all = this.getAll(query, count);
        all.onsuccess = () => {
            const keys = (all.result ?? []).map((r) => r.id as IDBValidKey);
            req.succeed(keys);
        };
        all.onerror = () => req.fail(all.error ?? new Error('getAllKeys failed'));
        return req;
    }

    count(query?: IDBValidKey | IDBKeyRange): FacadeRequest<number> {
        const req = new FacadeRequest<number>();
        const all = this.getAll(query);
        all.onsuccess = () => req.succeed((all.result ?? []).length);
        all.onerror = () => req.fail(all.error ?? new Error('count failed'));
        return req;
    }

    add(value: Record<string, json.Literal>, key?: IDBValidKey): FacadeRequest<IDBValidKey> {
        return this.write('add', value, key);
    }

    put(value: Record<string, json.Literal>, key?: IDBValidKey): FacadeRequest<IDBValidKey> {
        return this.write('put', value, key);
    }

    delete(key: IDBValidKey): FacadeRequest<undefined> {
        const req = new FacadeRequest<undefined>();
        if (this.name === DEFAULT_KEY_TABLE) {
            const inner = this.transaction.real.objectStore(KEYS).delete(key);
            inner.onsuccess = () => req.succeed(undefined);
            inner.onerror = () => req.fail(inner.error ?? new Error('delete failed'));
            return req;
        }
        const rows = this.transaction.real.objectStore(ROWS);
        const getReq = rows.get([this.name, key]);
        getReq.onsuccess = () => {
            const delReq = rows.delete([this.name, key]);
            delReq.onsuccess = () => {
                if (getReq.result !== undefined) {
                    this.queueOutbox({ table: this.name, localId: key as number, op: 'delete' });
                }
                req.succeed(undefined);
            };
            delReq.onerror = () => req.fail(delReq.error ?? new Error('delete failed'));
        };
        getReq.onerror = () => req.fail(getReq.error ?? new Error('delete failed'));
        return req;
    }

    clear(): FacadeRequest<undefined> {
        const req = new FacadeRequest<undefined>();
        const all = this.getAll();
        all.onsuccess = () => {
            const rows = all.result ?? [];
            const deleteNext = (i: number): void => {
                if (i >= rows.length) { req.succeed(undefined); return; }
                const id = rows[i].id as IDBValidKey;
                const del = this.delete(id);
                del.onsuccess = () => deleteNext(i + 1);
                del.onerror = () => req.fail(del.error ?? new Error('clear failed'));
            };
            deleteNext(0);
        };
        all.onerror = () => req.fail(all.error ?? new Error('clear failed'));
        return req;
    }

    private write(kind: 'add' | 'put', value: Record<string, json.Literal>, explicitKey?: IDBValidKey): FacadeRequest<IDBValidKey> {
        const req = new FacadeRequest<IDBValidKey>();
        if (this.name === DEFAULT_KEY_TABLE) {
            const rec = { ...value };
            if (explicitKey !== undefined && rec.id === undefined) rec.id = explicitKey as number;
            const store = this.transaction.real.objectStore(KEYS);
            const inner = kind === 'add' ? store.add(rec) : store.put(rec);
            inner.onsuccess = () => req.succeed(inner.result);
            inner.onerror = () => req.fail(inner.error ?? new Error(kind + ' failed'));
            return req;
        }

        const metaReq = this.transaction.real.objectStore(TABLE_META).get(this.name);
        metaReq.onsuccess = () => {
            const meta = metaReq.result as TableMetaRecord | undefined;
            if (meta === undefined) {
                req.fail(notFound(this.name));
                return;
            }
            let id = (explicitKey as number | undefined) ?? (value[meta.idColumn] as number | undefined);
            const rows = this.transaction.real.objectStore(ROWS);

            const finish = (assignedId: number, old: RowRecord | undefined): void => {
                if (kind === 'add' && old !== undefined) {
                    req.fail(constraintError());
                    return;
                }
                const rec: RowRecord = { ...value, table: this.name, [meta.idColumn]: assignedId, id: assignedId };
                if (assignedId >= meta.nextId) {
                    meta.nextId = assignedId + 1;
                    this.transaction.real.objectStore(TABLE_META).put(meta);
                }
                const putReq = rows.put(rec);
                putReq.onsuccess = () => {
                    const stripped = stripTable(rec)!;
                    const oldStripped = stripTable(old);
                    if (old === undefined) {
                        this.queueOutbox({
                            table: this.name, localId: assignedId, op: 'insert',
                            changed: businessValues(stripped, meta),
                        });
                    } else {
                        const changed = changedColumns(oldStripped, stripped, meta);
                        if (Object.keys(changed).length > 0) {
                            this.queueOutbox({
                                table: this.name, localId: assignedId, op: 'update', changed,
                            });
                        }
                    }
                    req.succeed(assignedId);
                };
                putReq.onerror = () => req.fail(putReq.error ?? new Error(kind + ' failed'));
            };

            if (id === undefined) {
                id = meta.nextId++;
                this.transaction.real.objectStore(TABLE_META).put(meta);
                finish(id, undefined);
                return;
            }

            const getReq = rows.get([this.name, id]);
            getReq.onsuccess = () => finish(id!, getReq.result as RowRecord | undefined);
            getReq.onerror = () => req.fail(getReq.error ?? new Error(kind + ' failed'));
        };
        metaReq.onerror = () => req.fail(metaReq.error ?? new Error(kind + ' failed'));
        return req;
    }

    private queueOutbox(rec: OutboxRecord): void {
        if (!this.transaction.host.isCaptureOn()) return;
        this.transaction.real.objectStore(OUTBOX).add(rec);
        this.transaction.captured = true;
    }
}

export class FacadeTransaction {
    readonly real: IDBTransaction;
    readonly host: FacadeHost;
    readonly mode: IDBTransactionMode;
    readonly db: FacadeDatabase;
    captured = false;
    error: DOMException | null = null;
    oncomplete: ((ev: Event) => void) | null = null;
    onabort: ((ev: Event) => void) | null = null;
    onerror: ((ev: Event) => void) | null = null;

    constructor(host: FacadeHost, db: FacadeDatabase, storeNames: string[], mode: IDBTransactionMode) {
        this.host = host;
        this.db = db;
        this.mode = mode;
        this.real = host.db.transaction(
            [ROWS, TABLE_META, KEYS, OUTBOX],
            mode,
        );
        this.real.oncomplete = (ev) => {
            if (this.captured) host.onOutboxCommitted();
            this.oncomplete?.(ev);
        };
        this.real.onabort = (ev) => {
            this.error = this.real.error;
            this.onabort?.(ev);
        };
        this.real.onerror = (ev) => {
            this.error = this.real.error;
            this.onerror?.(ev);
        };
        void storeNames;
    }

    objectStore(name: string): FacadeObjectStore {
        if (name !== DEFAULT_KEY_TABLE && !this.host.appTables().includes(name)) {
            throw notFound(name);
        }
        return new FacadeObjectStore(name, this);
    }

    abort(): void {
        this.real.abort();
    }

    commit(): void {
        this.real.commit?.();
    }
}

export class FacadeDatabase {
    constructor(private readonly host: FacadeHost) {}

    get name(): string { return this.host.db.name; }
    get version(): number { return this.host.db.version; }

    get objectStoreNames(): NameList {
        return new NameList([...this.host.appTables(), DEFAULT_KEY_TABLE]);
    }

    transaction(storeNames: string | string[], mode: IDBTransactionMode = 'readonly'): FacadeTransaction {
        const names = Array.isArray(storeNames) ? storeNames : [storeNames];
        const known = this.objectStoreNames;
        for (const n of names) {
            if (!known.contains(n)) throw notFound(n);
        }
        return new FacadeTransaction(this.host, this, names, mode);
    }

    createObjectStore(_name: string, _options?: IDBObjectStoreParameters): never {
        throw invalidState('The database is not running a version change transaction.');
    }

    deleteObjectStore(_name: string): never {
        throw invalidState('The database is not running a version change transaction.');
    }

    close(): void {
        // The target owns the physical connection; closing the facade is a no-op.
    }
}
