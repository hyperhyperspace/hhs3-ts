// Duck-typed IDBDatabase / IDBTransaction / IDBObjectStore over the fixed
// physical stores. Virtual store names are the materialized app tables plus
// `rdb_keys`. Schema ops throw: the adapter owns schema via SchemaAction.
//
// App writes (put/add/delete/clear) go through this facade and, when capture
// is on, also insert an outbox row in the SAME native transaction. Adapter
// apply() never uses the facade, so materialization cannot echo.
//
// Projection indexes appear as read-only native-shaped IDBIndexes
// (`store.index(name)`), backed by the index_entries shadow store. Facade
// writes maintain the entries of every index listed in index_meta, read in
// the same transaction.

import type { json } from "@hyper-hyper-space/hhs3_json";
import { DEFAULT_KEY_TABLE, type ResolvedIndex } from "@hyper-hyper-space/hhs3_rdb_adapter";

import {
    INDEX_ENTRIES, INDEX_META, KEYS, OUTBOX, ROWS, TABLE_META,
    type IndexEntryRecord, type OutboxRecord, type RowRecord, type TableMetaRecord,
} from "./idb_schema.js";
import {
    entryKeyRange, indexEntryRange, indexKeyPath, rowRange, syncEntryRequests, validateKey, wholeTableRange,
    AFTER_ANY_ID, type IndexQuery,
} from "./idb_index_keys.js";

export interface FacadeHost {
    readonly db: IDBDatabase;
    appTables(): string[];
    // The table's indexes, sorted by name, as last seen by this tab.
    cachedIndexes(table: string): ResolvedIndex[];
    cmp(a: unknown, b: unknown): number;
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

function readOnly(): DOMException {
    return new DOMException('Projection index cursors are read-only.', 'ReadOnlyError');
}

function abortError(): DOMException {
    return new DOMException('The transaction was aborted.', 'AbortError');
}

function readOnlyTransaction(): DOMException {
    return new DOMException('The transaction is read-only.', 'ReadOnlyError');
}

function asDomError(e: unknown): DOMException {
    if (e instanceof DOMException) return e;
    return new DOMException(e instanceof Error ? e.message : String(e), 'UnknownError');
}

// An exception thrown by an app handler aborts the transaction, as natively,
// and is reported rather than swallowed.
function reportException(e: unknown): void {
    const report = (globalThis as { reportError?: (e: unknown) => void }).reportError;
    if (report !== undefined) report(e);
    else console.error(e);
}

function eventFor(type: 'success' | 'error', target: object): Event {
    const ev = new Event(type, type === 'error' ? { bubbles: true, cancelable: true } : {});
    Object.defineProperty(ev, 'target', { value: target, configurable: true });
    return ev;
}

type StepKind = 'read' | 'write';

// Wire native request `n` into facade request `req`. Success continues with
// `next`; an exception there fails `req` for good. A native error is
// recoverable (the app may preventDefault(), as natively) only if `n` is the
// only write `req` has issued, or `req` has issued none: a failed native
// request writes nothing itself, so nothing would be left half applied.
//
// The native error event is always cancelled and stopped: the facade request
// decides, and aborts through abortWith. Left to its default action, the
// native abort would run a second time, after abortWith's.
function wire<R>(req: FacadeRequest<unknown>, n: IDBRequest<R>, kind: StepKind, next: (result: R) => void): void {
    const prior = req.writes;
    if (kind === 'write') req.writes++;
    n.onsuccess = () => {
        try {
            next(n.result);
        } catch (e) {
            req.failHard(asDomError(e));
        }
    };
    n.onerror = (ev) => {
        ev.preventDefault();
        ev.stopPropagation();
        const error = n.error ?? new DOMException('The request failed.', 'UnknownError');
        const alone = prior === 0 && req.writes === (kind === 'write' ? 1 : 0);
        if (alone) req.fail(error, true);
        else req.failHard(error);
    };
}

// Wire `reqs` (issued in order) and call `done` once all have succeeded.
function whenAll(req: FacadeRequest<unknown>, reqs: IDBRequest[], kind: StepKind, done: () => void): void {
    if (reqs.length === 0) { done(); return; }
    let left = reqs.length;
    for (const r of reqs) wire(req, r, kind, () => { if (--left === 0) done(); });
}

export class FacadeRequest<T> {
    result!: T;
    error: DOMException | null = null;
    readyState: IDBRequestReadyState = 'pending';
    source: object | null = null;
    transaction: FacadeTransaction | null = null;
    // Native writes this request has issued. Once it has one, a failure the
    // facade detects can no longer be recovered from: the transaction aborts.
    writes = 0;
    private _onsuccess: ((ev: Event) => void) | null = null;
    private _onerror: ((ev: Event) => void) | null = null;
    private settledHooks: Array<() => void> = [];

    // Internal: run `hook` when this request first settles, before the app's
    // handler.
    onSettled(hook: () => void): void {
        this.settledHooks.push(hook);
    }

    private settle(): void {
        const hooks = this.settledHooks;
        this.settledHooks = [];
        for (const h of hooks) h();
    }

    get onsuccess(): ((ev: Event) => void) | null { return this._onsuccess; }
    set onsuccess(h: ((ev: Event) => void) | null) {
        this._onsuccess = h;
        if (this.readyState === 'done' && this.error === null && h !== null) {
            queueMicrotask(() => h.call(this, eventFor('success', this)));
        }
    }

    get onerror(): ((ev: Event) => void) | null { return this._onerror; }
    set onerror(h: ((ev: Event) => void) | null) {
        this._onerror = h;
        if (this.readyState === 'done' && this.error !== null && h !== null) {
            queueMicrotask(() => h.call(this, eventFor('error', this)));
        }
    }

    succeed(value: T): void {
        this.result = value;
        this.readyState = 'done';
        this.settle();
        const h = this._onsuccess;
        if (h === null) return;
        try {
            h.call(this, eventFor('success', this));
        } catch (e) {
            reportException(e);
            this.transaction?.abortWith(abortError());
        }
    }

    // Fail with an error the facade detected (or, with `native`, a native
    // error that is safe to recover from). As natively, the error event
    // reaches the request's then the transaction's onerror, and the
    // transaction aborts unless a handler calls preventDefault(). That choice
    // exists only while this request has written nothing.
    fail(error: DOMException | Error, native = false): void {
        this.dispatchError(asDomError(error), native ? true : this.writes === 0);
    }

    // Fail, and abort the transaction whatever the handlers do.
    failHard(error: DOMException | Error): void {
        this.dispatchError(asDomError(error), false);
    }

    // A request still queued when its transaction aborts.
    failAborted(): void {
        this.dispatchError(abortError(), false);
    }

    private dispatchError(error: DOMException, recoverable: boolean): void {
        if (this.readyState === 'done') {
            // Already reported (e.g. a late native failure): only the abort remains.
            if (!recoverable) this.transaction?.abortWith(error);
            return;
        }
        this.error = error;
        this.readyState = 'done';
        this.settle();
        const ev = eventFor('error', this);
        let threw = false;
        const call = (h: ((ev: Event) => void) | null | undefined): void => {
            if (h === null || h === undefined) return;
            try {
                h.call(this, ev);
            } catch (e) {
                threw = true;
                reportException(e);
            }
        };
        call(this._onerror);
        if (!ev.cancelBubble) call(this.transaction?.onerror);
        if (threw) this.transaction?.abortWith(abortError());
        else if (!recoverable || !ev.defaultPrevented) this.transaction?.abortWith(error);
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
    readonly transaction: FacadeTransaction;

    constructor(name: string, transaction: FacadeTransaction) {
        this.name = name;
        this.transaction = transaction;
    }

    get indexNames(): NameList {
        if (this.name === DEFAULT_KEY_TABLE) return new NameList([]);
        return new NameList(this.transaction.host.cachedIndexes(this.name).map((i) => i.name));
    }

    createIndex(_name: string, _keyPath: string | string[], _options?: IDBIndexParameters): never {
        throw invalidState('The database is not running a version change transaction.');
    }

    deleteIndex(_name: string): never {
        throw invalidState('The database is not running a version change transaction.');
    }

    index(name: string): FacadeIndex {
        const index = this.name === DEFAULT_KEY_TABLE
            ? undefined
            : this.transaction.host.cachedIndexes(this.name).find((i) => i.name === name);
        if (index === undefined) throw new DOMException(`Index '${name}' does not exist`, 'NotFoundError');
        return new FacadeIndex(this, index);
    }

    get(key: IDBValidKey): FacadeRequest<Record<string, json.Literal> | undefined> {
        this.transaction.checkActive();
        this.requiredKey(key);
        const req = new FacadeRequest<Record<string, json.Literal> | undefined>();
        return this.transaction.schedule(req, () => {
            if (this.name === DEFAULT_KEY_TABLE) {
                wire(req, this.transaction.real.objectStore(KEYS).get(key), 'read',
                    (r) => req.succeed(r as Record<string, json.Literal> | undefined));
                return;
            }
            wire(req, this.transaction.real.objectStore(ROWS).get([this.name, key]), 'read',
                (r) => req.succeed(stripTable(r as RowRecord | undefined)));
        });
    }

    getAll(query?: IndexQuery, count?: number): FacadeRequest<Record<string, json.Literal>[]> {
        this.transaction.checkActive();
        const req = new FacadeRequest<Record<string, json.Literal>[]>();
        if (this.name === DEFAULT_KEY_TABLE) {
            const q = this.keysQuery(query);
            return this.transaction.schedule(req, () => {
                wire(req, this.transaction.real.objectStore(KEYS).getAll(q, count), 'read',
                    (r) => req.succeed(r as Record<string, json.Literal>[]));
            });
        }
        // Physical row keys are [table, id]: translate the query on the virtual id.
        const range = rowRange(this.cmpFn, this.name, query);
        return this.transaction.schedule(req, () => {
            wire(req, this.transaction.real.objectStore(ROWS).getAll(range, count), 'read',
                (r) => req.succeed((r as RowRecord[]).map((row) => stripTable(row)!)));
        });
    }

    getAllKeys(query?: IndexQuery, count?: number): FacadeRequest<IDBValidKey[]> {
        this.transaction.checkActive();
        const req = new FacadeRequest<IDBValidKey[]>();
        if (this.name === DEFAULT_KEY_TABLE) {
            const q = this.keysQuery(query);
            return this.transaction.schedule(req, () => {
                wire(req, this.transaction.real.objectStore(KEYS).getAllKeys(q, count), 'read', (r) => req.succeed(r));
            });
        }
        const range = rowRange(this.cmpFn, this.name, query);
        return this.transaction.schedule(req, () => {
            wire(req, this.transaction.real.objectStore(ROWS).getAllKeys(range, count), 'read',
                (r) => req.succeed(r.map((k) => (k as IDBValidKey[])[1]!)));
        });
    }

    count(query?: IndexQuery): FacadeRequest<number> {
        this.transaction.checkActive();
        const req = new FacadeRequest<number>();
        if (this.name === DEFAULT_KEY_TABLE) {
            const q = this.keysQuery(query);
            return this.transaction.schedule(req, () => {
                wire(req, this.transaction.real.objectStore(KEYS).count(q), 'read', (r) => req.succeed(r));
            });
        }
        const range = rowRange(this.cmpFn, this.name, query);
        return this.transaction.schedule(req, () => {
            wire(req, this.transaction.real.objectStore(ROWS).count(range), 'read', (r) => req.succeed(r));
        });
    }

    private get cmpFn(): IDBFactory['cmp'] {
        const host = this.transaction.host;
        return (a, b) => host.cmp(a, b);
    }

    private requiredKey(key: unknown): void {
        if (key === undefined || key === null) throw dataError('No key specified.');
        validateKey(this.cmpFn, key);
    }

    // rdb_keys queries go to the native store as given, but are checked when
    // issued: a request may start later, when a throw could no longer reach
    // the caller.
    private keysQuery(query: IndexQuery): IDBValidKey | IDBKeyRange | undefined {
        if (query === undefined || query === null) return undefined;
        if (query instanceof IDBKeyRange) return query;
        return validateKey(this.cmpFn, query);
    }

    // The table's indexes as listed in index_meta right now (in this
    // transaction), so an index installed by another tab is maintained.
    private withIndexes(req: FacadeRequest<unknown>, then: (indexes: ResolvedIndex[]) => void): void {
        const r = this.transaction.real.objectStore(INDEX_META).getAll(wholeTableRange(this.name));
        wire(req, r, 'read', (indexes) => then(indexes as ResolvedIndex[]));
    }

    private syncEntries(
        req: FacadeRequest<unknown>, indexes: ResolvedIndex[], id: number,
        oldRow: RowRecord | undefined, newRow: RowRecord | undefined, done: () => void,
    ): void {
        const entries = this.transaction.real.objectStore(INDEX_ENTRIES);
        whenAll(req, syncEntryRequests(entries, indexes, id, oldRow, newRow), 'write', done);
    }

    add(value: Record<string, json.Literal>, key?: IDBValidKey): FacadeRequest<IDBValidKey> {
        return this.issueWrite('add', value, key);
    }

    put(value: Record<string, json.Literal>, key?: IDBValidKey): FacadeRequest<IDBValidKey> {
        return this.issueWrite('put', value, key);
    }

    delete(key: IDBValidKey): FacadeRequest<undefined> {
        this.checkWritable();
        this.requiredKey(key);
        const req = new FacadeRequest<undefined>();
        return this.transaction.schedule(req, () => this.deleteOne(req, key, () => req.succeed(undefined)));
    }

    clear(): FacadeRequest<undefined> {
        this.checkWritable();
        const req = new FacadeRequest<undefined>();
        return this.transaction.schedule(req, () => {
            if (this.name === DEFAULT_KEY_TABLE) {
                wire(req, this.transaction.real.objectStore(KEYS).clear(), 'write', () => req.succeed(undefined));
                return;
            }
            const keys = this.transaction.real.objectStore(ROWS).getAllKeys(rowRange(this.cmpFn, this.name, undefined));
            wire(req, keys, 'read', (physical) => {
                const ids = physical.map((k) => (k as IDBValidKey[])[1]!);
                const deleteNext = (i: number): void => {
                    if (i >= ids.length) { req.succeed(undefined); return; }
                    this.deleteOne(req, ids[i]!, () => deleteNext(i + 1));
                };
                deleteNext(0);
            });
        });
    }

    // Issue-time checks, as natively: they throw instead of failing the request.
    private checkWritable(): void {
        this.transaction.checkActive();
        if (this.transaction.mode === 'readonly') throw readOnlyTransaction();
    }

    private issueWrite(kind: 'add' | 'put', value: Record<string, json.Literal>, key?: IDBValidKey): FacadeRequest<IDBValidKey> {
        this.checkWritable();
        if (key !== undefined) validateKey(this.cmpFn, key);
        // The value is cloned when issued, so later changes to it are not
        // written, and a value that cannot be cloned throws DataCloneError.
        const doc = structuredClone(value);
        if (typeof doc !== 'object' || doc === null || Array.isArray(doc)) {
            throw dataError('The value is not a record.');
        }
        if (doc.id !== undefined) validateKey(this.cmpFn, doc.id);
        const req = new FacadeRequest<IDBValidKey>();
        return this.transaction.schedule(req, () => this.write(req, kind, doc, key));
    }

    private deleteOne(req: FacadeRequest<undefined>, key: IDBValidKey, done: () => void): void {
        if (this.name === DEFAULT_KEY_TABLE) {
            wire(req, this.transaction.real.objectStore(KEYS).delete(key), 'write', () => done());
            return;
        }
        const rows = this.transaction.real.objectStore(ROWS);
        wire(req, rows.get([this.name, key]), 'read', (r) => {
            const old = r as RowRecord | undefined;
            if (old === undefined) { done(); return; }
            wire(req, rows.delete([this.name, key]), 'write', () => {
                this.queueOutbox(req, { table: this.name, localId: key as number, op: 'delete' });
                this.withIndexes(req, (indexes) => this.syncEntries(req, indexes, old.id, old, undefined, done));
            });
        });
    }

    private write(
        req: FacadeRequest<IDBValidKey>, kind: 'add' | 'put', value: Record<string, json.Literal>, explicitKey?: IDBValidKey,
    ): void {
        const tx = this.transaction.real;
        if (this.name === DEFAULT_KEY_TABLE) {
            const rec = { ...value };
            if (explicitKey !== undefined && rec.id === undefined) rec.id = explicitKey as number;
            const store = tx.objectStore(KEYS);
            wire(req, kind === 'add' ? store.add(rec) : store.put(rec), 'write', (k) => req.succeed(k));
            return;
        }

        wire(req, tx.objectStore(TABLE_META).get(this.name), 'read', (m) => {
            const meta = m as TableMetaRecord | undefined;
            if (meta === undefined) {
                req.fail(notFound(this.name));
                return;
            }
            this.withIndexes(req, (indexes) => {
                let id = (explicitKey as number | undefined) ?? (value[meta.idColumn] as number | undefined);
                const rows = tx.objectStore(ROWS);

                const finish = (assignedId: number, old: RowRecord | undefined): void => {
                    if (kind === 'add' && old !== undefined) {
                        req.fail(constraintError());
                        return;
                    }
                    const rec: RowRecord = { ...value, table: this.name, [meta.idColumn]: assignedId, id: assignedId };
                    if (assignedId >= meta.nextId) {
                        meta.nextId = assignedId + 1;
                        wire(req, tx.objectStore(TABLE_META).put(meta), 'write', () => {});
                    }
                    wire(req, rows.put(rec), 'write', () => {
                        const stripped = stripTable(rec)!;
                        if (old === undefined) {
                            this.queueOutbox(req, {
                                table: this.name, localId: assignedId, op: 'insert',
                                changed: businessValues(stripped, meta),
                            });
                        } else {
                            const changed = changedColumns(stripTable(old), stripped, meta);
                            if (Object.keys(changed).length > 0) {
                                this.queueOutbox(req, { table: this.name, localId: assignedId, op: 'update', changed });
                            }
                        }
                        this.syncEntries(req, indexes, assignedId, old, rec, () => req.succeed(assignedId));
                    });
                };

                if (id === undefined) {
                    id = meta.nextId++;
                    wire(req, tx.objectStore(TABLE_META).put(meta), 'write', () => {});
                    finish(id, undefined);
                    return;
                }

                wire(req, rows.get([this.name, id]), 'read', (r) => finish(id!, r as RowRecord | undefined));
            });
        });
    }

    private queueOutbox(req: FacadeRequest<unknown>, rec: OutboxRecord): void {
        if (!this.transaction.host.isCaptureOn()) return;
        wire(req, this.transaction.real.objectStore(OUTBOX).add(rec), 'write', () => {});
        this.transaction.captured = true;
    }
}

const DIRECTIONS: ReadonlySet<string> = new Set(['next', 'nextunique', 'prev', 'prevunique']);

function dataError(message: string): DOMException {
    return new DOMException(message, 'DataError');
}

// A projection index, shaped like a native IDBIndex over the table's
// documents. Queries are translated to exact ranges of index_entries.
export class FacadeIndex {
    readonly name: string;
    readonly objectStore: FacadeObjectStore;
    readonly keyPath: string | string[];
    readonly unique = false;
    readonly multiEntry = false;
    private readonly table: string;

    constructor(objectStore: FacadeObjectStore, index: ResolvedIndex) {
        this.objectStore = objectStore;
        this.name = index.name;
        this.table = index.table;
        this.keyPath = indexKeyPath(index);
    }

    get(query: IDBValidKey | IDBKeyRange): FacadeRequest<Record<string, json.Literal> | undefined> {
        const range = this.requiredRange(query);
        const req = this.newRequest<Record<string, json.Literal> | undefined>();
        this.withEntries(req, (entries, rows) => {
            wire(req, entries.get(range), 'read', (e) => {
                const entry = e as IndexEntryRecord | undefined;
                if (entry === undefined) { req.succeed(undefined); return; }
                wire(req, rows.get([this.table, entry.id]), 'read', (r) => req.succeed(stripTable(r as RowRecord | undefined)));
            });
        });
        return req;
    }

    getKey(query: IDBValidKey | IDBKeyRange): FacadeRequest<IDBValidKey | undefined> {
        const range = this.requiredRange(query);
        const req = this.newRequest<IDBValidKey | undefined>();
        this.withEntries(req, (entries) => {
            wire(req, entries.get(range), 'read', (e) => req.succeed((e as IndexEntryRecord | undefined)?.id));
        });
        return req;
    }

    getAll(query?: IndexQuery, count?: number): FacadeRequest<Record<string, json.Literal>[]> {
        const range = this.range(query);
        const req = this.newRequest<Record<string, json.Literal>[]>();
        this.withEntries(req, (entries, rows) => {
            wire(req, entries.getAll(range, count), 'read', (e) => {
                const gets = (e as IndexEntryRecord[]).map((entry) => rows.get([this.table, entry.id]));
                whenAll(req, gets, 'read', () => {
                    const docs = gets.map((g) => stripTable(g.result as RowRecord | undefined));
                    req.succeed(docs.filter((d): d is Record<string, json.Literal> => d !== undefined));
                });
            });
        });
        return req;
    }

    getAllKeys(query?: IndexQuery, count?: number): FacadeRequest<IDBValidKey[]> {
        const range = this.range(query);
        const req = this.newRequest<IDBValidKey[]>();
        this.withEntries(req, (entries) => {
            wire(req, entries.getAllKeys(range, count), 'read', (e) => req.succeed(e.map((k) => (k as IDBValidKey[])[3]!)));
        });
        return req;
    }

    count(query?: IndexQuery): FacadeRequest<number> {
        const range = this.range(query);
        const req = this.newRequest<number>();
        this.withEntries(req, (entries) => {
            wire(req, entries.count(range), 'read', (n) => req.succeed(n));
        });
        return req;
    }

    openCursor(query?: IndexQuery, direction: IDBCursorDirection = 'next'): FacadeRequest<FacadeCursor | null> {
        return this.open(query, direction, true);
    }

    openKeyCursor(query?: IndexQuery, direction: IDBCursorDirection = 'next'): FacadeRequest<FacadeCursor | null> {
        return this.open(query, direction, false);
    }

    private open(query: IndexQuery, direction: IDBCursorDirection, withValue: boolean): FacadeRequest<FacadeCursor | null> {
        if (!DIRECTIONS.has(direction)) throw new TypeError(`'${direction}' is not a valid cursor direction`);
        const range = this.range(query);
        const req = this.newRequest<FacadeCursor | null>();
        this.withEntries(req, (entries, rows) => {
            new FacadeCursor(this, req, direction, withValue, this.table, entries, rows).start(range);
        });
        return req;
    }

    get cmp(): IDBFactory['cmp'] {
        const host = this.objectStore.transaction.host;
        return (a, b) => host.cmp(a, b);
    }

    private range(query: IndexQuery): IDBKeyRange {
        this.objectStore.transaction.checkActive();
        return indexEntryRange(this.cmp, this.table, this.name, query);
    }

    // get / getKey need a key or range, as natively.
    private requiredRange(query: IndexQuery): IDBKeyRange {
        this.objectStore.transaction.checkActive();
        if (query === undefined || query === null) throw dataError('No key or key range specified.');
        return this.range(query);
    }

    private newRequest<T>(): FacadeRequest<T> {
        const req = new FacadeRequest<T>();
        req.source = this;
        return req;
    }

    // Every request re-checks index_meta in its own transaction: an index
    // dropped since `index()` (e.g. by another tab) fails the request.
    private withEntries<T>(req: FacadeRequest<T>, then: (entries: IDBObjectStore, rows: IDBObjectStore) => void): void {
        const tx = this.objectStore.transaction;
        tx.schedule(req, () => {
            wire(req, tx.real.objectStore(INDEX_META).get([this.table, this.name]), 'read', (m) => {
                if (m === undefined) {
                    req.fail(invalidState(`Index '${this.name}' has been deleted.`));
                    return;
                }
                then(tx.real.objectStore(INDEX_ENTRIES), tx.real.objectStore(ROWS));
            });
        });
    }
}

// A read-only cursor over one index. It wraps a native `next` / `prev` cursor
// over the index's entries; the unique directions jump past each key.
export class FacadeCursor {
    readonly source: FacadeIndex;
    readonly request: FacadeRequest<FacadeCursor | null>;
    readonly direction: IDBCursorDirection;
    key: IDBValidKey | undefined = undefined;
    primaryKey: IDBValidKey | undefined = undefined;
    // Only set on cursors opened with openCursor.
    value: Record<string, json.Literal> | undefined = undefined;

    private native: IDBCursorWithValue | null = null;
    private gotValue = false;
    // Keys still to pass before the next emit (advance on unique directions).
    private skip = 0;
    private readonly prefix: IDBValidKey[];

    constructor(
        source: FacadeIndex, request: FacadeRequest<FacadeCursor | null>, direction: IDBCursorDirection,
        private readonly withValue: boolean, private readonly table: string,
        private readonly entries: IDBObjectStore, private readonly rows: IDBObjectStore,
    ) {
        this.source = source;
        this.request = request;
        this.direction = direction;
        this.prefix = [table, source.name];
    }

    start(range: IDBKeyRange): void {
        wire(this.request, this.entries.openCursor(range, this.forward ? 'next' : 'prev'), 'read', (c) => this.landed(c));
    }

    continue(key?: IDBValidKey): void {
        this.checkIterable();
        if (key === undefined) {
            const k = this.key!;
            this.step(() => {
                if (this.unique) this.jump(k);
                else this.native!.continue();
            });
            return;
        }
        validateKey(this.source.cmp, key);
        const c = this.source.cmp(key, this.key);
        if (this.forward ? c <= 0 : c >= 0) {
            throw dataError('The key is not past the cursor position in its direction.');
        }
        const target = this.forward ? [...this.prefix, key] : [...this.prefix, key, AFTER_ANY_ID];
        this.step(() => this.native!.continue(target));
    }

    continuePrimaryKey(key: IDBValidKey, primaryKey: IDBValidKey): void {
        if (this.unique) {
            throw new DOMException('continuePrimaryKey requires a next or prev cursor.', 'InvalidAccessError');
        }
        this.checkIterable();
        const cmp = this.source.cmp;
        validateKey(cmp, key);
        validateKey(cmp, primaryKey);
        const c = cmp(key, this.key);
        const p = cmp(primaryKey, this.primaryKey);
        const behind = this.forward ? (c < 0 || (c === 0 && p <= 0)) : (c > 0 || (c === 0 && p >= 0));
        if (behind) throw dataError('The key and primary key are not past the cursor position in its direction.');
        const target = [...this.prefix, key, primaryKey];
        this.step(() => this.native!.continue(target));
    }

    advance(count: number): void {
        if (!Number.isInteger(count) || count <= 0) throw new TypeError('advance count must be a positive integer');
        this.checkIterable();
        const k = this.key!;
        this.step(() => {
            if (this.unique) {
                this.skip = count - 1;
                this.jump(k);
            } else {
                this.native!.advance(count);
            }
        });
    }

    update(_value: unknown): never { throw readOnly(); }

    delete(): never { throw readOnly(); }

    private get forward(): boolean { return this.direction === 'next' || this.direction === 'nextunique'; }

    private get unique(): boolean { return this.direction === 'nextunique' || this.direction === 'prevunique'; }

    private checkIterable(): void {
        this.source.objectStore.transaction.checkActive();
        if (!this.gotValue || this.native === null) {
            throw invalidState('The cursor is being iterated or has iterated past its end.');
        }
    }

    // Each step is a request of its own: it waits behind the requests issued
    // before it, so it sees their writes, as a native cursor does.
    private step(move: () => void): void {
        this.gotValue = false;
        this.request.readyState = 'pending';
        this.source.objectStore.transaction.schedule(this.request, move);
    }

    // Move past logical key `k`. Physical entries are [t, n, k, id], so
    // [t, n, k, []] is after all of k's entries and [t, n, k] before them.
    private jump(k: IDBValidKey): void {
        this.native!.continue(this.forward ? [...this.prefix, k, AFTER_ANY_ID] : [...this.prefix, k]);
    }

    private landed(c: IDBCursorWithValue | null): void {
        this.native = c;
        if (c === null) {
            this.key = undefined;
            this.primaryKey = undefined;
            this.value = undefined;
            this.request.succeed(null);
            return;
        }
        const entry = c.value as IndexEntryRecord;
        if (this.skip > 0) {
            this.skip--;
            this.jump(entry.key);
            return;
        }
        if (this.direction === 'prevunique') {
            // Natively, prevunique yields each key's lowest primary key, which a
            // backward walk reaches last: fetch it directly.
            wire(this.request, this.entries.get(entryKeyRange(this.table, this.source.name, entry.key)), 'read',
                (first) => this.emit(first as IndexEntryRecord));
            return;
        }
        this.emit(entry);
    }

    private emit(entry: IndexEntryRecord): void {
        this.key = entry.key;
        this.primaryKey = entry.id;
        if (!this.withValue) {
            this.gotValue = true;
            this.request.succeed(this);
            return;
        }
        wire(this.request, this.rows.get([this.table, entry.id]), 'read', (r) => {
            this.value = stripTable(r as RowRecord | undefined);
            this.gotValue = true;
            this.request.succeed(this);
        });
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
            [ROWS, TABLE_META, KEYS, OUTBOX, INDEX_ENTRIES, INDEX_META],
            mode,
        );
        this.real.oncomplete = (ev) => {
            this.finished = true;
            if (this.captured) host.onOutboxCommitted();
            this.oncomplete?.(ev);
        };
        this.real.onabort = (ev) => {
            this.finished = true;
            this.error = this.abortCause ?? this.real.error;
            // Requests still queued never reached the native transaction: they
            // fail with AbortError before the app hears of the abort, as the
            // pending requests of a native transaction do.
            const queued = this.waiting.splice(0);
            for (const { req } of queued) req.failAborted();
            this.onabort?.(ev);
        };
        // Facade steps stop their native error events, so this sees only
        // errors of native requests the facade does not follow.
        this.real.onerror = (ev) => this.onerror?.(ev);
        void storeNames;
    }

    // Set once the native transaction has completed or aborted.
    private finished = false;
    // Set once an abort has begun. As natively, the transaction is then
    // inactive, and requests still queued never start.
    private aborting = false;
    // The error the transaction aborts with: the failed request's, as natively
    // (the native transaction, aborted by the facade, would report none).
    private abortCause: DOMException | null = null;
    private busy = false;
    private readonly waiting: Array<{ req: FacadeRequest<unknown>; run: () => void }> = [];

    checkActive(): void {
        if (this.finished || this.aborting) {
            throw new DOMException('The transaction has finished.', 'TransactionInactiveError');
        }
    }

    // Abort because of `error` (a request failed or a handler threw). Only the
    // first abort counts. If the native transaction is already aborting on its
    // own, its error stands.
    abortWith(error: DOMException): void {
        if (this.finished || this.aborting) return;
        this.aborting = true;
        this.abortCause = this.real.error ?? error;
        try {
            this.real.abort();
        } catch (e) {
            if (!(e instanceof DOMException && e.name === 'InvalidStateError')) throw e;
        }
    }

    // A facade request can take several native steps. Requests run one at a
    // time, in issue order, so each sees the effects of those issued before
    // it, as the requests of a native transaction do. The next one starts from
    // inside the previous one's last native callback, so the transaction stays
    // active. Each cursor step is scheduled like a request.
    schedule<T>(req: FacadeRequest<T>, start: () => void): FacadeRequest<T> {
        this.checkActive();
        req.transaction = this;
        const entry = { req: req as FacadeRequest<unknown>, run: () => {} };
        const advance = (): void => {
            this.busy = false;
            this.waiting.shift()?.run();
        };
        entry.run = (): void => {
            if (this.aborting || this.finished) {
                // The native abort event fails it, with the rest of the queue.
                this.waiting.unshift(entry);
                return;
            }
            this.busy = true;
            req.onSettled(advance);
            try {
                start();
            } catch (e) {
                if (this.aborting) req.failAborted();
                else req.failHard(asDomError(e));
            }
        };
        if (this.busy) {
            this.waiting.push(entry);
            return req;
        }
        // Issued with the queue idle: a throw here comes before any native
        // request, so it reaches the caller, as an issue-time check would.
        this.busy = true;
        try {
            start();
        } catch (e) {
            this.busy = false;
            throw e;
        }
        req.onSettled(advance);
        return req;
    }

    objectStore(name: string): FacadeObjectStore {
        if (name !== DEFAULT_KEY_TABLE && !this.host.appTables().includes(name)) {
            throw notFound(name);
        }
        return new FacadeObjectStore(name, this);
    }

    abort(): void {
        if (this.aborting) throw invalidState('The transaction is already aborting.');
        this.real.abort();
        this.aborting = true;
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
