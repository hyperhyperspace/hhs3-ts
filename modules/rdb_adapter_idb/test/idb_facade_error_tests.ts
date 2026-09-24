import "fake-indexeddb/auto";

import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import type { json } from "@hyper-hyper-space/hhs3_json";
import type { ResolvedIndex, SchemaAction } from "@hyper-hyper-space/hhs3_rdb_adapter";

import { IdbTarget } from "../src/idb_target.js";
import { INDEX_ENTRIES, OUTBOX, ROWS, TABLE_META } from "../src/idb_schema.js";

let dbCounter = 0;
function uniqueDbName(label: string): string {
    dbCounter++;
    return `hhs3-rdb-idb-err-${label}-${Date.now()}-${dbCounter}-${Math.random().toString(36).slice(2)}`;
}

// JSON with sorted object keys, so property order does not matter.
function stable(v: unknown): string {
    return JSON.stringify(v, (_k, x: unknown) => (x !== null && typeof x === 'object' && !Array.isArray(x))
        ? Object.fromEntries(Object.entries(x as Record<string, unknown>).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)))
        : x);
}

function errorName(fn: () => unknown): string {
    try { fn(); } catch (e) { return (e as { name?: string }).name ?? 'Error'; }
    return 'none';
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function done<T>(req: any): Promise<T> {
    return new Promise((resolve, reject) => {
        req.onsuccess = () => resolve(req.result as T);
        req.onerror = () => reject(req.error ?? new Error('request failed'));
    });
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function ended(tx: any): Promise<string> {
    return new Promise((resolve) => {
        tx.oncomplete = () => resolve('complete');
        tx.onabort = () => resolve('abort');
    });
}

// App handlers that throw are reported (console.error without reportError).
async function quietly<T>(fn: () => Promise<T>): Promise<T> {
    const original = console.error;
    console.error = () => {};
    try { return await fn(); } finally { console.error = original; }
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyDb = { transaction(names: string[], mode: IDBTransactionMode): any };

const tCreate: SchemaAction = {
    kind: 'create-table', table: 't', syncTable: 't_sync', primaryKey: 'id',
    columns: [{ name: 'a', def: { type: 'json', nullable: true } }],
};

const ia: ResolvedIndex = { name: 'ia', groupId: 'g', table: 't', columns: [{ rdb: 'a', target: 'a' }], fingerprint: 'ia' };

const SEED: Record<string, json.Literal>[] = [{ a: 'x' }, { a: 'y' }, { a: 'z' }];

async function seed(db: AnyDb): Promise<void> {
    const tx = db.transaction(['t'], 'readwrite');
    const finished = ended(tx);
    for (const doc of SEED) tx.objectStore('t').add({ ...doc });
    assertEquals(await finished, 'complete', 'seeded');
}

// A facade target and a native store with the same shape and documents.
async function pair(label: string, opts: { captureChanges?: boolean } = {}): Promise<{ target: IdbTarget; native: IDBDatabase }> {
    const target = await IdbTarget.open(uniqueDbName(label), opts);
    await target.apply('g', [tCreate, { kind: 'ensure-index', index: ia }], [], new Set(['v1']));
    const native = await new Promise<IDBDatabase>((resolve, reject) => {
        const open = indexedDB.open(uniqueDbName(`${label}-native`), 1);
        open.onupgradeneeded = () => {
            open.result.createObjectStore('t', { keyPath: 'id', autoIncrement: true }).createIndex('ia', 'a');
        };
        open.onsuccess = () => resolve(open.result);
        open.onerror = () => reject(open.error);
    });
    await seed(target.database);
    await seed(native);
    return { target, native };
}

async function documents(db: AnyDb): Promise<string> {
    const tx = db.transaction(['t'], 'readonly');
    const store = tx.objectStore('t');
    const [docs, keys] = await Promise.all([done(store.getAll()), done(store.index('ia').getAllKeys())]);
    return stable([docs, keys]);
}

interface TrackOpts {
    prevent?: boolean;
    stop?: boolean;
    throwInSuccess?: boolean;
    throwInError?: boolean;
    // Called inside the error handler, before it returns.
    onError?: () => void;
}

interface Ctx {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    tx: any;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    store: any;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    track(label: string, req: any, opts?: TrackOpts): void;
}

// Run `script` in one transaction and log, in order, every request event, the
// transaction's error events (with the request they came from), and how the
// transaction ended, with its error.
async function scenario(db: AnyDb, script: (ctx: Ctx) => void, txOpts: { prevent?: boolean } = {}): Promise<string> {
    const tx = db.transaction(['t'], 'readwrite');
    const log: unknown[] = [];
    const labels = new Map<object, string>();
    const finished = new Promise<void>((resolve) => {
        tx.oncomplete = () => { log.push(['complete']); resolve(); };
        tx.onabort = () => { log.push(['abort', tx.error?.name ?? null]); resolve(); };
    });
    tx.onerror = (ev: Event) => {
        const target = ev.target as { error?: DOMException | null } | null;
        log.push(['tx.onerror', labels.get(target as object) ?? '?', target?.error?.name]);
        if (txOpts.prevent === true) ev.preventDefault();
    };
    const ctx: Ctx = {
        tx,
        store: tx.objectStore('t'),
        track(label, req, opts = {}) {
            labels.set(req, label);
            req.onsuccess = (ev: Event) => {
                log.push([label, 'success', req.result ?? null, ev.target === req]);
                if (opts.throwInSuccess === true) throw new Error(`${label} success handler threw`);
            };
            req.onerror = (ev: Event) => {
                log.push([label, 'error', req.error?.name, ev.target === req]);
                if (opts.prevent === true) ev.preventDefault();
                if (opts.stop === true) ev.stopPropagation();
                opts.onError?.();
                if (opts.throwInError === true) throw new Error(`${label} error handler threw`);
            };
        },
    };
    script(ctx);
    await finished;
    return stable(log);
}

type Script = (ctx: Ctx) => void;

// Two writes around a duplicate add, and a write issued after it.
function duplicateAdd(opts: TrackOpts): Script {
    return ({ store, track }) => {
        track('put', store.put({ id: 1, a: 'p' }));
        track('dup', store.add({ id: 2, a: 'q' }), opts);
        track('after', store.put({ a: 'n' }));
        track('read', store.index('ia').getAllKeys());
    };
}

const DIFFERENTIAL: Array<[string, Script, { prevent?: boolean }?]> = [
    ['add on an existing key, not prevented, aborts with ConstraintError', duplicateAdd({})],
    ['add on an existing key, prevented, lets the rest commit', duplicateAdd({ prevent: true })],
    ['preventDefault in the transaction onerror also recovers', duplicateAdd({}), { prevent: true }],
    ['stopPropagation without preventDefault still aborts', duplicateAdd({ stop: true })],
    // (Per spec the event still bubbles to the transaction after a handler
    // throws; fake-indexeddb stops it there. stop: true keeps both comparable.)
    ['an error handler that throws aborts with AbortError, even after preventDefault',
        duplicateAdd({ prevent: true, stop: true, throwInError: true })],
    ['requests issued from the error handler get AbortError', ({ store, track }) => {
        track('dup', store.add({ id: 1, a: 'q' }), {
            onError: () => track('late', store.put({ id: 2, a: 'late' })),
        });
        track('after', store.get(3));
    }],
    ['a success handler that throws aborts with AbortError', ({ store, track }) => {
        track('put', store.put({ id: 1, a: 'p' }), { throwInSuccess: true });
        track('after', store.put({ a: 'n' }));
    }],
    ['tx.abort() fails the pending requests with AbortError and leaves no error', ({ tx, store, track }) => {
        track('put', store.put({ id: 1, a: 'p' }));
        track('add', store.add({ a: 'n' }));
        tx.abort();
    }],
    ['a value changed after put is written as it was when issued', ({ store, track }) => {
        const doc: Record<string, json.Literal> = { id: 2, a: 'issued' };
        track('put', store.put(doc));
        doc.a = 'changed-after';
        track('get', store.get(2));
    }],
    ['reads and missing keys succeed', ({ store, track }) => {
        track('get-missing', store.get(99));
        track('delete-missing', store.delete(99));
        track('index-get', store.index('ia').get('y'));
        track('count', store.count());
    }],
];

// Issue-time checks: each call's thrown error name (or 'none').
function syncChecks(db: AnyDb): Promise<string[]> {
    const names: string[] = [];
    const ro = db.transaction(['t'], 'readonly');
    const roStore = ro.objectStore('t');
    names.push(
        errorName(() => roStore.put({ a: 1 })),
        errorName(() => roStore.add({ a: 1 })),
        errorName(() => roStore.delete(1)),
        errorName(() => roStore.clear()),
    );
    const rw = db.transaction(['t'], 'readwrite');
    const store = rw.objectStore('t');
    const index = store.index('ia');
    // A request in flight, so the checks below are made while the queue is busy.
    store.put({ id: 1, a: 'busy' });
    names.push(
        errorName(() => store.put({ a: () => 1 })),
        errorName(() => store.put({ id: {}, a: 1 })),
        errorName(() => store.put(5)),
        errorName(() => store.get({})),
        errorName(() => store.get(undefined)),
        errorName(() => store.delete(true)),
        errorName(() => store.count({})),
        errorName(() => index.get(undefined)),
    );
    const finished = ended(rw);
    return finished.then((how) => {
        names.push(how);
        names.push(
            errorName(() => store.put({ a: 1 })),
            errorName(() => store.get(1)),
            errorName(() => store.getAll()),
            errorName(() => index.getAll()),
            errorName(() => index.openCursor()),
        );
        const aborted = db.transaction(['t'], 'readwrite');
        const abortedStore = aborted.objectStore('t');
        aborted.abort();
        names.push(
            errorName(() => abortedStore.put({ a: 1 })),
            errorName(() => abortedStore.get(1)),
            errorName(() => aborted.abort()),
        );
        return ended(aborted).then((h) => { names.push(h); return names; });
    });
}

// ---------------------------------------------------------------------------
// Fault injection: native failures in the middle of a facade write.
// ---------------------------------------------------------------------------

type Fault = 'throw' | 'native-error';

// Make the next index_entries put fail: by throwing when issued, or with a
// native ConstraintError event (an extra add of the record it just put).
function armIndexEntryFault(fault: Fault): () => void {
    const proto = IDBObjectStore.prototype;
    const original = proto.put;
    let armed = true;
    proto.put = function (this: IDBObjectStore, ...args: Parameters<IDBObjectStore['put']>): IDBRequest<IDBValidKey> {
        if (!armed || this.name !== INDEX_ENTRIES) return original.apply(this, args);
        armed = false;
        if (fault === 'throw') throw new DOMException('injected fault', 'UnknownError');
        original.apply(this, args);
        return this.add(...args);
    };
    return () => { proto.put = original; };
}

// Every physical store the facade writes, read natively.
async function physical(target: IdbTarget): Promise<string> {
    const tx = target.env.db.transaction([ROWS, INDEX_ENTRIES, OUTBOX, TABLE_META], 'readonly');
    const all = await Promise.all([ROWS, INDEX_ENTRIES, OUTBOX, TABLE_META].map((s) => done(tx.objectStore(s).getAll())));
    return stable(all);
}

async function faultCase(fault: Fault, first: 'update' | 'insert'): Promise<void> {
    const why = `${fault} during ${first}`;
    const target = await IdbTarget.open(uniqueDbName(`fault-${fault}-${first}`), { captureChanges: true });
    try {
        await target.apply('g', [tCreate, { kind: 'ensure-index', index: ia }], [], new Set(['v1']));
        await seed(target.database);
        const before = await physical(target);

        const tx = target.database.transaction(['t'], 'readwrite');
        const store = tx.objectStore('t');
        const settled = new Map<string, string[]>();
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const track = (label: string, req: any, cursor = false): void => {
            settled.set(label, []);
            req.onsuccess = () => {
                settled.get(label)!.push('success');
                if (cursor && req.result !== null) req.result.continue();
            };
            // Tries to recover: a write that failed half way must abort anyway.
            req.onerror = (ev: Event) => { settled.get(label)!.push(`error:${req.error?.name}`); ev.preventDefault(); };
        };
        const how = ended(tx);
        const restore = armIndexEntryFault(fault);
        try {
            if (first === 'update') track('first', store.put({ id: 1, a: 'moved' }));
            else track('first', store.add({ a: 'new' }));
            track('queued-put', store.put({ id: 2, a: 'other' }));
            track('queued-get', store.get(3));
            track('queued-cursor', store.index('ia').openCursor(), true);
            assertEquals(await how, 'abort', `${why}: the transaction aborts even though onerror called preventDefault`);
        } finally {
            restore();
        }
        const firstEvents = settled.get('first')!;
        const expectFirst = fault === 'throw' ? 'error:UnknownError' : 'error:ConstraintError';
        assertEquals(firstEvents.join(','), expectFirst, `${why}: the failing request reports the native error once`);
        assertEquals(tx.error?.name ?? null, expectFirst.slice('error:'.length), `${why}: tx.error is that error`);
        for (const label of ['queued-put', 'queued-get', 'queued-cursor']) {
            assertEquals(settled.get(label)!.join(','), 'error:AbortError', `${why}: ${label} settles with AbortError`);
        }
        assertEquals(await physical(target), before, `${why}: rows, index entries, outbox and nextId are unchanged`);

        // The facade is usable afterwards, and the index still matches the rows.
        const again = target.database.transaction(['t'], 'readwrite');
        const next = ended(again);
        again.objectStore('t').add({ a: 'after' });
        assertEquals(await next, 'complete', `${why}: a later transaction commits`);
        const keys = await done<IDBValidKey[]>(target.database.transaction(['t'], 'readonly').objectStore('t').index('ia').getAllKeys());
        assertEquals(keys.join(','), '4,1,2,3', `${why}: the index maps the committed rows only`);
    } finally {
        target.close();
    }
}

export const idbFacadeErrorTests = {
    title: '[ADPTI-ERR] rdb_adapter IndexedDB facade error and abort semantics',
    tests: [
        {
            name: '[ADPTI-ERR01] differential: request errors, handler exceptions and aborts match native',
            invoke: async () => {
                for (const [label, script, txOpts] of DIFFERENTIAL) {
                    const { target, native } = await pair('diff');
                    try {
                        const [f, n] = await quietly(() => Promise.all([
                            scenario(target.database, script, txOpts), scenario(native, script, txOpts),
                        ]));
                        assertEquals(f, n, `${label}: same events, in the same order, and the same outcome`);
                        assertEquals(await documents(target.database), await documents(native), `${label}: same final state`);
                    } finally {
                        native.close();
                        target.close();
                    }
                }
            },
        },
        {
            name: '[ADPTI-ERR02] issue-time checks throw as natively: ReadOnly, DataClone, Data, TransactionInactive',
            invoke: async () => {
                const { target, native } = await pair('sync');
                try {
                    const [f, n] = await Promise.all([syncChecks(target.database), syncChecks(native)]);
                    assertEquals(f.join(','), n.join(','), 'the same calls throw the same errors');
                    assertTrue(f.includes('DataCloneError') && f.includes('ReadOnlyError') && f.includes('TransactionInactiveError'),
                        `sanity: the checks fire (${f.join(',')})`);
                    assertEquals(await documents(target.database), await documents(native), 'same final state');
                } finally {
                    native.close();
                    target.close();
                }
            },
        },
        {
            name: '[ADPTI-ERR03] fault injection: a native failure mid-write aborts, rolls back and settles every request',
            invoke: async () => {
                for (const fault of ['throw', 'native-error'] as const) {
                    for (const first of ['update', 'insert'] as const) await faultCase(fault, first);
                }
            },
        },
    ],
};
