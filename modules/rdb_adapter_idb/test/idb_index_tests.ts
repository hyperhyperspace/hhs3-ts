import "fake-indexeddb/auto";

import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { json } from "@hyper-hyper-space/hhs3_json";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import {
    CheckpointMovedError, projectGroup, reconcileIndexes,
    type GroupProjection, type IndexDecl, type IndexSpec, type ResolvedIndex, type RowAction, type SchemaAction,
} from "@hyper-hyper-space/hhs3_rdb_adapter";
import { deriveRowId } from "@hyper-hyper-space/hhs3_rdb";
import { createFlipGroup, createGroup, sameVersion } from "@hyper-hyper-space/hhs3_rdb_adapter_test";

import { IdbTarget } from "../src/idb_target.js";
import type { FacadeIndex, FacadeTransaction } from "../src/idb_facade.js";

let dbCounter = 0;
function uniqueDbName(label: string): string {
    dbCounter++;
    return `hhs3-rdb-idb-idx-${label}-${Date.now()}-${dbCounter}-${Math.random().toString(36).slice(2)}`;
}

// Works for both native IDBRequests and facade requests.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function done<T>(req: any): Promise<T> {
    return new Promise((resolve, reject) => {
        req.onsuccess = () => resolve(req.result as T);
        req.onerror = () => reject(req.error ?? new Error('request failed'));
    });
}

function txDone(tx: FacadeTransaction | IDBTransaction): Promise<void> {
    return new Promise((resolve, reject) => {
        tx.oncomplete = () => resolve();
        tx.onabort = () => reject(tx.error ?? new Error('transaction aborted'));
    });
}

function mulberry32(seed: number): () => number {
    let a = seed >>> 0;
    return () => {
        a = (a + 0x6D2B79F5) >>> 0;
        let t = a;
        t = Math.imul(t ^ (t >>> 15), t | 1);
        t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
        return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
}

function pick<T>(rng: () => number, xs: readonly T[]): T {
    return xs[Math.floor(rng() * xs.length)]!;
}

async function expectThrows(fn: () => Promise<unknown>, match: string | typeof CheckpointMovedError, why: string) {
    let err: unknown;
    try { await fn(); } catch (e) { err = e; }
    assertTrue(err !== undefined, `${why}: expected a throw`);
    if (typeof match === 'string') assertTrue(err instanceof Error && err.message.includes(match), `${why}: got ${String(err)}`);
    else assertTrue(err instanceof match, `${why}: got ${String(err)}`);
}

function errorName(fn: () => unknown): string {
    try { fn(); } catch (e) { return (e as { name?: string }).name ?? 'Error'; }
    return 'none';
}

function handIndex(name: string, table: string, columns: string[], fingerprint = name): ResolvedIndex {
    return { name, groupId: 'g', table, columns: columns.map((c) => ({ rdb: c, target: c })), fingerprint };
}

function financeDecl(name: string, table: string, columns: string[], options?: IndexDecl['options']): IndexDecl {
    const d: IndexDecl = { name, group: 'finance-prod', table, columns };
    if (options !== undefined) d.options = options;
    return d;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type FacadeDatabaseLike = { transaction(names: string[], mode: IDBTransactionMode): any };

function readIndex(target: IdbTarget, table: string, name: string): { tx: FacadeTransaction; index: FacadeIndex } {
    const tx = target.database.transaction([table], 'readonly');
    return { tx, index: tx.objectStore(table).index(name) };
}

async function indexIds(target: IdbTarget, table: string, name: string, query?: IDBValidKey | IDBKeyRange): Promise<IDBValidKey[]> {
    const { index } = readIndex(target, table, name);
    return done<IDBValidKey[]>(index.getAllKeys(query));
}

async function entryCount(target: IdbTarget, table: string, name: string): Promise<number> {
    const { index } = readIndex(target, table, name);
    return done<number>(index.count());
}

// ---------------------------------------------------------------------------
// Differential fixture: the same documents behind a facade index and behind a
// native index on a scratch database.
// ---------------------------------------------------------------------------

// Nullable columns carry null, which json.Literal does not model.
const NULL = null as unknown as json.Literal;

const VALUE_POOL: readonly (json.Literal | undefined)[] = [
    -1, 0, 1, 2.5, 3, '', 'a', 'ab', 'b', NULL, true, false, undefined,
    [], [1], ['a'], [1, 'a'], [0, 'b'], { x: 1 },
];
const SCALAR_KEYS: readonly IDBValidKey[] = [-2, -1, 0, 0.5, 1, 2.5, 3, 4, '', 'a', 'aa', 'ab', 'b', 'c', [], [1], ['a'], [1, 'a']];
const COMPOUND_KEYS: readonly IDBValidKey[] = [
    [], [0], [1], [-1, 'a'], [0, 0], [1, 'a'], [1, 1], [2.5, 'b'], ['a', 'a'], ['a', 3], ['b', []], [[], 'a'], [[1], [1, 'a']],
];
const DIRECTIONS: readonly IDBCursorDirection[] = ['next', 'nextunique', 'prev', 'prevunique'];

function randomDoc(rng: () => number): { [column: string]: json.Literal } {
    const values: { [column: string]: json.Literal } = {};
    for (const col of ['a', 'b']) {
        const v = pick(rng, VALUE_POOL);
        if (v !== undefined) values[col] = v;
    }
    return values;
}

function randomQuery(rng: () => number, pool: readonly IDBValidKey[], compound: boolean): IDBValidKey | IDBKeyRange | undefined {
    const r = rng();
    if (r < 0.1) return undefined;
    if (r < 0.3) return pick(rng, pool);
    if (compound && r < 0.4) {
        const head = pick(rng, SCALAR_KEYS);
        return IDBKeyRange.bound([head], [head, []]);
    }
    if (r < 0.55) return IDBKeyRange.lowerBound(pick(rng, pool), rng() < 0.5);
    if (r < 0.7) return IDBKeyRange.upperBound(pick(rng, pool), rng() < 0.5);
    let l = pick(rng, pool);
    let u = pick(rng, pool);
    const c = indexedDB.cmp(l, u);
    if (c > 0) [l, u] = [u, l];
    if (c === 0) return IDBKeyRange.bound(l, u);
    return IDBKeyRange.bound(l, u, rng() < 0.5, rng() < 0.5);
}

type AnyCursor = {
    key: IDBValidKey; primaryKey: IDBValidKey; value?: unknown;
    continue(key?: IDBValidKey): void; continuePrimaryKey(key: IDBValidKey, pk: IDBValidKey): void; advance(n: number): void;
};

// A callback-driven walk (moves are issued inside onsuccess, as native code
// must). `seed` drives the mixed moves; the same seed on both sides yields the
// same moves as long as both sides see the same cursor states.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function walk(req: any, direction: IDBCursorDirection, withValue: boolean, pool: readonly IDBValidKey[], seed?: number): Promise<unknown[]> {
    const rng = seed === undefined ? undefined : mulberry32(seed);
    const forward = direction === 'next' || direction === 'nextunique';
    const unique = direction.endsWith('unique');
    return new Promise((resolve, reject) => {
        const out: unknown[] = [];
        req.onsuccess = () => {
            const c = req.result as AnyCursor | null;
            if (c === null) { resolve(out); return; }
            out.push(withValue ? [c.key, c.primaryKey, c.value] : [c.key, c.primaryKey]);
            if (out.length > 1000) { reject(new Error('runaway cursor')); return; }
            if (rng === undefined) { c.continue(); return; }
            const r = rng();
            const past = pool.filter((k) => indexedDB.cmp(k, c.key) * (forward ? 1 : -1) > 0);
            if (r < 0.35) c.continue();
            else if (r < 0.55) c.advance(1 + Math.floor(rng() * 3));
            else if (r < 0.75 && past.length > 0) c.continue(pick(rng, past));
            else if (!unique && r < 0.88) c.continuePrimaryKey(c.key, (c.primaryKey as number) + (forward ? 1 : -1));
            else if (!unique && past.length > 0) c.continuePrimaryKey(pick(rng, past), forward ? 0 : 1_000_000);
            else c.continue();
        };
        req.onerror = () => reject(req.error ?? new Error('cursor failed'));
    });
}

// Every read of one query against one index, issued together in one
// transaction (so cursor walks keep it alive), as a JSON string to compare.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
async function readAll(index: any, query: IDBValidKey | IDBKeyRange | undefined, pool: readonly IDBValidKey[], seed: number): Promise<string> {
    const ops: Promise<unknown>[] = [];
    if (query !== undefined) {
        ops.push(done(index.get(query)), done(index.getKey(query)));
    }
    ops.push(
        done(index.getAll(query)), done(index.getAll(query, 3)),
        done(index.getAllKeys(query)), done(index.getAllKeys(query, 2)), done(index.count(query)),
    );
    DIRECTIONS.forEach((d, i) => {
        ops.push(walk(index.openCursor(query, d), d, true, pool));
        ops.push(walk(index.openKeyCursor(query, d), d, false, pool));
        ops.push(walk(index.openCursor(query, d), d, true, pool, seed * 17 + i));
        ops.push(walk(index.openKeyCursor(query, d), d, false, pool, seed * 31 + i));
    });
    return JSON.stringify(await Promise.all(ops));
}

async function openNative(name: string, docs: Record<string, json.Literal>[], indexes: Record<string, string | string[]>): Promise<IDBDatabase> {
    const db = await new Promise<IDBDatabase>((resolve, reject) => {
        const open = indexedDB.open(name, 1);
        open.onupgradeneeded = () => {
            const store = open.result.createObjectStore('t', { keyPath: 'id' });
            for (const [n, keyPath] of Object.entries(indexes)) store.createIndex(n, keyPath);
        };
        open.onsuccess = () => resolve(open.result);
        open.onerror = () => reject(open.error);
    });
    const tx = db.transaction(['t'], 'readwrite');
    const written = txDone(tx);
    for (const doc of docs) tx.objectStore('t').put(doc);
    await written;
    return db;
}

// JSON with sorted object keys, so property order does not matter.
function stable(v: unknown): string {
    return JSON.stringify(v, (_k, x: unknown) => (x !== null && typeof x === 'object' && !Array.isArray(x))
        ? Object.fromEntries(Object.entries(x as Record<string, unknown>).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)))
        : x);
}

type Op =
    | { kind: 'add' | 'put-new'; doc: Record<string, json.Literal> }
    | { kind: 'put'; id: number; doc: Record<string, json.Literal> }
    | { kind: 'delete' | 'get'; id: number }
    | { kind: 'getAll' | 'count' | 'idx-getAllKeys' | 'idx-count' }
    | { kind: 'getAll-range'; lo: number; hi: number }
    | { kind: 'idx-getAll' | 'idx-get'; key: IDBValidKey }
    | { kind: 'walk'; direction: IDBCursorDirection; writes: Array<Op | null> };

function randomOp(rng: () => number, depth = 0): Op {
    const id = 1 + Math.floor(rng() * 12);
    const r = rng();
    if (r < 0.15) return { kind: 'add', doc: randomDoc(rng) };
    if (r < 0.2) return { kind: 'put-new', doc: randomDoc(rng) };
    if (r < 0.35) return { kind: 'put', id, doc: randomDoc(rng) };
    if (r < 0.45) return { kind: 'delete', id };
    if (r < 0.52) return { kind: 'get', id };
    if (r < 0.57) return { kind: 'getAll' };
    if (r < 0.61) return { kind: 'count' };
    if (r < 0.66) return { kind: 'getAll-range', lo: id, hi: id + Math.floor(rng() * 4) };
    if (r < 0.72) return { kind: 'idx-getAll', key: pick(rng, SCALAR_KEYS) };
    if (r < 0.76) return { kind: 'idx-get', key: pick(rng, SCALAR_KEYS) };
    if (r < 0.8) return { kind: 'idx-getAllKeys' };
    if (r < 0.84) return { kind: 'idx-count' };
    if (depth > 0) return { kind: 'get', id };
    // A walk that writes between its steps: the write must land before the step.
    const writes: Array<Op | null> = [];
    for (let i = 0; i < 12; i++) {
        writes.push(rng() < 0.4 ? pick(rng, [
            { kind: 'put', id: 1 + Math.floor(rng() * 12), doc: randomDoc(rng) } as Op,
            { kind: 'delete', id: 1 + Math.floor(rng() * 12) } as Op,
            { kind: 'add', doc: randomDoc(rng) } as Op,
        ]) : null);
    }
    return { kind: 'walk', direction: pick(rng, DIRECTIONS), writes };
}

// Issue `ops` back to back, never awaiting, and log every completion in order.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function issueOps(store: any, ops: Op[], log: unknown[]): void {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const track = (label: string, req: any): void => {
        req.onsuccess = () => log.push([label, req.result]);
        req.onerror = (ev: Event) => { ev.preventDefault?.(); log.push([label, 'error', req.error?.name]); };
    };
    ops.forEach((op, i) => issueOp(store, op, `${i}`, track, log));
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function issueOp(store: any, op: Op, label: string, track: (l: string, r: any) => void, log: unknown[]): void {
    const index = () => store.index('ia');
    switch (op.kind) {
        case 'add': return track(label, store.add({ ...op.doc }));
        case 'put-new': return track(label, store.put({ ...op.doc }));
        case 'put': return track(label, store.put({ ...op.doc, id: op.id }));
        case 'delete': return track(label, store.delete(op.id));
        case 'get': return track(label, store.get(op.id));
        case 'getAll': return track(label, store.getAll());
        case 'count': return track(label, store.count());
        case 'getAll-range': return track(label, store.getAll(IDBKeyRange.bound(op.lo, op.hi)));
        case 'idx-getAll': return track(label, index().getAll(op.key));
        case 'idx-get': return track(label, index().get(op.key));
        case 'idx-getAllKeys': return track(label, index().getAllKeys());
        case 'idx-count': return track(label, index().count());
        case 'walk': {
            const req = index().openCursor(null, op.direction);
            let step = 0;
            req.onsuccess = () => {
                const c = req.result as AnyCursor | null;
                if (c === null) { log.push([label, 'end']); return; }
                log.push([label, c.key, c.primaryKey, c.value]);
                const write = op.writes[step++ % op.writes.length];
                if (write !== null && write !== undefined) issueOp(store, write, `${label}.${step}`, track, log);
                if (step > 200) return;
                c.continue();
            };
            req.onerror = () => log.push([label, 'error', req.error?.name]);
            return;
        }
    }
}

const tCreate: SchemaAction = {
    kind: 'create-table', table: 't', syncTable: 't_sync', primaryKey: 'id',
    columns: [{ name: 'a', def: { type: 'json', nullable: true } }, { name: 'b', def: { type: 'json', nullable: true } }],
};

const acctCreate: SchemaAction = {
    kind: 'create-table', table: 'acct', syncTable: 'acct_sync', primaryKey: 'id',
    columns: [{ name: 'ref', def: { type: 'string', nullable: true } }, { name: 'n', def: { type: 'integer', nullable: true } }],
};

export const idbIndexTests = {
    title: '[ADPTI-IDX] rdb_adapter IndexedDB projection indexes',
    tests: [
        {
            name: '[ADPTI-IDX01] differential: facade indexes match native IDBIndexes (requests, cursors, all directions)',
            invoke: async () => {
                const target = await IdbTarget.open(uniqueDbName('diff'));
                let native: IDBDatabase | undefined;
                try {
                    const rng = mulberry32(20260924);
                    // `ia` is built before the rows (maintained by apply), `iab` after
                    // (built from existing documents).
                    await target.apply('g', [tCreate, { kind: 'ensure-index', index: handIndex('ia', 't', ['a']) }], [], new Set(['v1']));
                    const rows: RowAction[] = [];
                    for (let i = 0; i < 60; i++) rows.push({ kind: 'upsert-row', table: 't', rowId: `r${i}`, values: randomDoc(rng) });
                    await target.apply('g', [], rows, new Set(['v2']));
                    const churn: RowAction[] = [];
                    for (let i = 0; i < 20; i++) {
                        const doc = randomDoc(rng);
                        churn.push({ kind: 'upsert-row', table: 't', rowId: `r${Math.floor(rng() * 60)}`, values: { a: doc.a ?? NULL } });
                    }
                    for (let i = 0; i < 6; i++) churn.push({ kind: 'delete-row', table: 't', rowId: `r${Math.floor(rng() * 60)}` });
                    await target.apply('g', [], churn, new Set(['v3']));
                    await target.apply('g', [{ kind: 'ensure-index', index: handIndex('iab', 't', ['a', 'b']) }], [], new Set(['v4']));

                    const docTx = target.database.transaction(['t'], 'readonly');
                    const docs = await done<Record<string, json.Literal>[]>(docTx.objectStore('t').getAll());
                    assertTrue(docs.length > 40, 'sanity: documents materialized');
                    native = await openNative(uniqueDbName('native'), docs, { ia: 'a', iab: ['a', 'b'] });

                    const fStore = target.database.transaction(['t'], 'readonly').objectStore('t');
                    assertEquals([...fStore.indexNames].join(','), 'ia,iab', 'indexNames lists the projection indexes');
                    assertEquals(JSON.stringify(fStore.index('iab').keyPath), '["a","b"]', 'compound keyPath');
                    assertEquals(fStore.index('ia').keyPath, 'a', 'single-column keyPath');

                    const cases: Array<{ name: string; pool: readonly IDBValidKey[] }> = [
                        { name: 'ia', pool: SCALAR_KEYS },
                        { name: 'iab', pool: COMPOUND_KEYS },
                    ];
                    let checked = 0;
                    for (const { name, pool } of cases) {
                        for (let q = 0; q < 50; q++) {
                            const query = randomQuery(rng, pool, name === 'iab');
                            const seed = Math.floor(rng() * 1e9);
                            const fIndex = target.database.transaction(['t'], 'readonly').objectStore('t').index(name);
                            const nIndex = native.transaction(['t'], 'readonly').objectStore('t').index(name);
                            const [f, n] = await Promise.all([readAll(fIndex, query, pool, seed), readAll(nIndex, query, pool, seed)]);
                            const shown = query instanceof IDBKeyRange
                                ? JSON.stringify({ lower: query.lower, upper: query.upper, lo: query.lowerOpen, uo: query.upperOpen })
                                : JSON.stringify(query);
                            assertEquals(f, n, `index ${name}, query ${shown}: facade and native agree`);
                            checked++;
                        }
                    }
                    assertEquals(checked, 100, 'all queries compared');

                    // Errors match too.
                    const fIdx = target.database.transaction(['t'], 'readonly').objectStore('t').index('ia');
                    const nIdx = native.transaction(['t'], 'readonly').objectStore('t').index('ia');
                    for (const [label, op] of [
                        ['get(undefined)', (i: FacadeIndex | IDBIndex) => i.get(undefined as unknown as IDBValidKey)],
                        ['get(null key)', (i: FacadeIndex | IDBIndex) => i.get(null as unknown as IDBValidKey)],
                        // (fake-indexeddb's getAll accepts an invalid key; the facade throws DataError, per spec.)
                        ['get(boolean)', (i: FacadeIndex | IDBIndex) => i.get(true as unknown as IDBValidKey)],
                        ['openCursor(boolean)', (i: FacadeIndex | IDBIndex) => i.openCursor(true as unknown as IDBValidKey)],
                        ['count({})', (i: FacadeIndex | IDBIndex) => i.count({} as unknown as IDBValidKey)],
                    ] as const) {
                        assertEquals(errorName(() => op(fIdx)), errorName(() => op(nIdx)), `${label} fails the same way`);
                    }
                    const cursorErrors = async (index: FacadeIndex | IDBIndex, direction: IDBCursorDirection): Promise<string[]> => {
                        const req = index.openCursor(undefined, direction);
                        return new Promise((resolve) => {
                            req.onsuccess = () => {
                                req.onsuccess = null;
                                const c = req.result as unknown as AnyCursor & { update(v: unknown): unknown; delete(): unknown };
                                const names = [
                                    errorName(() => c.continue(c.key)),
                                    errorName(() => c.continuePrimaryKey(c.key, c.primaryKey)),
                                    errorName(() => c.advance(0)),
                                ];
                                c.continue();
                                names.push(errorName(() => c.continue()));
                                resolve(names);
                            };
                        });
                    };
                    for (const d of DIRECTIONS) {
                        const fi = target.database.transaction(['t'], 'readonly').objectStore('t').index('ia');
                        const ni = native.transaction(['t'], 'readonly').objectStore('t').index('ia');
                        const [fe, ne] = await Promise.all([cursorErrors(fi, d), cursorErrors(ni, d)]);
                        assertEquals(fe.join(','), ne.join(','), `cursor misuse fails the same way (${d})`);
                    }

                    const ro = target.database.transaction(['t'], 'readonly').objectStore('t').index('ia').openCursor();
                    const roNames = await new Promise<string[]>((resolve) => {
                        ro.onsuccess = () => {
                            const c = ro.result!;
                            resolve([errorName(() => c.update({})), errorName(() => c.delete())]);
                        };
                    });
                    assertEquals(roNames.join(','), 'ReadOnlyError,ReadOnlyError', 'cursor update/delete are read-only');
                    assertEquals(errorName(() => fStore.index('nope')), 'NotFoundError', 'unknown index');
                } finally {
                    native?.close();
                    target.close();
                }
            },
        },
        {
            name: '[ADPTI-IDX02] reconcile: install, unchanged, refused options, drop-column and re-add (pending), reopen',
            invoke: async () => {
                const name = uniqueDbName('life');
                const { schema, group, admin } = await createGroup();
                const ledger = await group.getTable('ledger');
                await ledger.insert('l1', { ref: 'R-1', amount: '10.00', memo: 'x' }, admin);
                await ledger.insert('l2', { ref: 'R-2', amount: '5.00' }, admin);
                await ledger.insert('l3', { ref: 'R-3', amount: '7.50', memo: 'a' }, admin);
                const target = await IdbTarget.open(name);
                try {
                    await projectGroup(group, target);
                    const members: GroupProjection[] = [{ group, config: {} }];
                    const spec: IndexSpec = {
                        version: 1,
                        indexes: [financeDecl('by_memo', 'ledger', ['memo', 'ref']), financeDecl('by_amount', 'ledger', ['amount'])],
                    };

                    await expectThrows(() => reconcileIndexes(members, target, {
                        version: 1, indexes: [financeDecl('by_memo', 'ledger', ['memo'], {})],
                    }), 'the indexeddb target takes no index options', 'options are refused');
                    assertEquals((await target.getIndexState()).spec, undefined, 'nothing installed by the refused spec');

                    const report = await reconcileIndexes(members, target, spec);
                    assertEquals(report.status, 'installed', 'installed');
                    assertEquals((await reconcileIndexes(members, target, spec)).status, 'unchanged', 'second run is a no-op');
                    assertEquals(await entryCount(target, 'ledger', 'by_amount'), 3, 'every row has an amount');
                    assertEquals(await entryCount(target, 'ledger', 'by_memo'), 2, 'a null memo is not a key (sparse)');
                    const byAmount = readIndex(target, 'ledger', 'by_amount').index;
                    const amounts = await done<Record<string, json.Literal>[]>(byAmount.getAll());
                    assertEquals(amounts.map((r) => r.amount).join(','), '10.00,5.00,7.50', 'decimals order as stored strings');

                    await schema.updateSchema([{ rule: 'drop-column', table: 'ledger', column: 'memo' }], admin, 'migrate');
                    await group.deploy(await (await schema.getScopedDag()).getFrontier());
                    await projectGroup(group, target);
                    assertEquals((await target.getIndexState()).materialized.map((m) => m.name).join(','), 'by_amount',
                        'the index on the dropped column is gone (dropped before the column)');
                    assertEquals(target.cachedIndexes('ledger').map((i) => i.name).join(','), 'by_amount', 'name cache refreshed');

                    await schema.updateSchema([{
                        rule: 'add-column', table: 'ledger', column: 'memo', def: { type: 'string', nullable: true },
                    }], admin, 'migrate');
                    await group.deploy(await (await schema.getScopedDag()).getFrontier());
                    await ledger.insert('l4', { ref: 'R-4', amount: '1.00', memo: 'm' }, admin);
                    await projectGroup(group, target);
                    assertEquals((await target.getIndexState()).materialized.map((m) => m.name).join(','), 'by_amount,by_memo',
                        'the pending declaration is built when its column returns');
                    assertEquals(await entryCount(target, 'ledger', 'by_memo'), 1, 'only the new row has a memo');
                } finally {
                    target.close();
                }

                const reopened = await IdbTarget.open(name);
                try {
                    const state = await reopened.getIndexState();
                    assertEquals(state.spec?.version, 1, 'spec survived reopen');
                    assertEquals(state.materialized.map((m) => m.name).join(','), 'by_amount,by_memo', 'records survived reopen');
                    assertEquals(await entryCount(reopened, 'ledger', 'by_amount'), 4, 'entries survived reopen');
                    const byMemo = readIndex(reopened, 'ledger', 'by_memo').index;
                    const hit = await done<Record<string, json.Literal> | undefined>(byMemo.get(IDBKeyRange.bound(['m'], ['m', []])));
                    assertEquals(hit?.ref, 'R-4', 'compound prefix query after reopen');
                } finally {
                    reopened.close();
                }
            },
        },
        {
            name: '[ADPTI-IDX03] FK flip moves the key to post_id inside apply; drop-table and re-created table',
            invoke: async () => {
                const target = await IdbTarget.open(uniqueDbName('flip'));
                try {
                    const { schema, group, admin } = await createFlipGroup();
                    await (await group.getTable('posts')).insert('p1', { title: 'hello' }, admin);
                    const p1 = deriveRowId('p1', admin.keyId);
                    // The plain value already honors the future FK, so the set-fks deploy is accepted.
                    await (await group.getTable('comments')).insert('c1', { body: 'hi', post: p1 }, admin);
                    await projectGroup(group, target);
                    const members: GroupProjection[] = [{ group, config: {} }];
                    const decl = (n: string, table: string, columns: string[]): IndexDecl =>
                        ({ name: n, group: 'flip-prod', table, columns });
                    await reconcileIndexes(members, target, {
                        version: 1, indexes: [decl('by_title', 'posts', ['title']), decl('by_post', 'comments', ['post'])],
                    });
                    assertEquals(await entryCount(target, 'comments', 'by_post'), 1, 'by_post built on the plain column');

                    await schema.updateSchema([{ rule: 'set-fks', table: 'comments', fks: { post: 'posts' } }], admin, 'migrate');
                    await group.deploy(await (await schema.getScopedDag()).getFrontier());
                    await projectGroup(group, target);
                    const flipped = (await target.getIndexState()).materialized.find((m) => m.name === 'by_post');
                    assertEquals(flipped?.columns[0]?.target, 'post_id', 'the index follows the FK companion');
                    const postId = await target.syncId('posts', p1);
                    const { index } = readIndex(target, 'comments', 'by_post');
                    assertEquals(index.keyPath, 'post_id', 'keyPath follows the companion');
                    const hit = await done<Record<string, json.Literal> | undefined>(index.get(postId!));
                    assertEquals(hit?.body, 'hi', 'the comment is found by its local post id');
                    assertEquals(await entryCount(target, 'comments', 'by_post'), 1, 'exactly one entry after the flip');

                    await schema.updateSchema([{ rule: 'drop-table', table: 'comments' }], admin, 'migrate');
                    await group.deploy(await (await schema.getScopedDag()).getFrontier());
                    await projectGroup(group, target);
                    assertEquals((await target.getIndexState()).materialized.map((m) => `${m.table}.${m.name}`).join(','),
                        'posts.by_title', 'the dropped table took its index; the other survives');
                    assertEquals(target.cachedIndexes('comments').length, 0, 'cache forgot the table');

                    await schema.updateSchema([{ rule: 'add-table', def: {
                        name: 'comments', columns: { body: { type: 'string' }, post: { type: 'string', nullable: true } },
                        restrictions: [{ on: 'all', rule: { p: 'true' } }],
                    } }], admin, 'migrate');
                    await group.deploy(await (await schema.getScopedDag()).getFrontier());
                    await (await group.getTable('comments')).insert('c2', { body: 'again', post: p1 }, admin);
                    await projectGroup(group, target);
                    assertEquals(await entryCount(target, 'comments', 'by_post'), 1, 'the re-created table is indexed from scratch');
                    const again = await done<Record<string, json.Literal>[]>(readIndex(target, 'comments', 'by_post').index.getAll());
                    assertEquals(again.map((c) => c.body).join(','), 'again', 'no entry of the dropped table survived');
                    assertEquals(await entryCount(target, 'posts', 'by_title'), 1, 'the untouched index is intact');
                } finally {
                    target.close();
                }
            },
        },
        {
            name: '[ADPTI-IDX04] installIndexSpec is atomic and compare-and-set guarded; apply checks expectIndexSpec',
            invoke: async () => {
                const target = await IdbTarget.open(uniqueDbName('cas'));
                try {
                    const gid = 'g';
                    const v1: Version = new Set(['v1']);
                    await target.apply(gid, [acctCreate], [
                        { kind: 'upsert-row', table: 'acct', rowId: 'r1', values: { ref: 'a' } },
                    ], v1);
                    const spec: IndexSpec = { version: 1, indexes: [] };
                    const expect = { specFingerprint: undefined, checkpoints: new Map([[gid, v1]]) };

                    await expectThrows(() => target.installIndexSpec(spec, 'fp1', [
                        { kind: 'ensure-index', index: handIndex('by_ref', 'acct', ['ref']) },
                        { kind: 'ensure-index', index: handIndex('by_ghost', 'acct', ['ghost']) },
                    ], expect), "cannot index 'acct.ghost'", 'an unbuildable ensure fails the install');
                    const failed = await target.getIndexState();
                    assertEquals(failed.spec, undefined, 'no spec was recorded');
                    assertEquals(failed.materialized.length, 0, 'the valid ensure was rolled back');

                    await expectThrows(() => target.installIndexSpec(spec, 'fp1', [],
                        { specFingerprint: undefined, checkpoints: new Map([[gid, new Set(['elsewhere'])]]) }),
                        CheckpointMovedError, 'a moved checkpoint aborts the install');
                    await expectThrows(() => target.installIndexSpec(spec, 'fp1', [{
                        kind: 'add-column', table: 'acct', column: 'x', def: { type: 'string' },
                    }], expect), 'accepts only index actions', 'non-index actions are refused');

                    await target.installIndexSpec(spec, 'fp1',
                        [{ kind: 'ensure-index', index: handIndex('by_ref', 'acct', ['ref']) }], expect);
                    assertEquals((await target.getIndexState()).specFingerprint, 'fp1', 'installed');
                    assertEquals(await entryCount(target, 'acct', 'by_ref'), 1, 'built from the existing row');
                    await expectThrows(() => target.installIndexSpec(spec, 'fp2', [], expect),
                        CheckpointMovedError, 'a stale installed-spec expectation aborts the install');

                    await expectThrows(() => target.apply(gid, [], [], new Set(['v2']), undefined, v1, null),
                        CheckpointMovedError, 'apply planned against no spec is refused once one is installed');
                    await expectThrows(() => target.apply(gid, [], [], new Set(['v2']), undefined, v1, 'fp0'),
                        CheckpointMovedError, 'apply planned against another spec is refused');
                    await target.apply(gid, [], [], new Set(['v2']), undefined, v1, 'fp1');
                    assertTrue(sameVersion(await target.getCheckpoint(gid), new Set(['v2'])), 'apply with the right spec lands');

                    await expectThrows(() => target.apply(gid, [{ kind: 'drop-column', table: 'acct', column: 'ref' }],
                        [], new Set(['v3'])), "index 'by_ref' uses it", 'an indexed column cannot be dropped');
                    await target.apply(gid, [
                        { kind: 'drop-index', table: 'acct', name: 'by_ref' },
                        { kind: 'drop-column', table: 'acct', column: 'ref' },
                    ], [], new Set(['v3']));
                    assertEquals((await target.getIndexState()).materialized.length, 0, 'dropped with its column');

                    // ensure-index is idempotent: rebuilding replaces the entries.
                    const byN = handIndex('by_n', 'acct', ['n']);
                    await target.apply(gid, [{ kind: 'ensure-index', index: byN }],
                        [{ kind: 'upsert-row', table: 'acct', rowId: 'r1', values: { n: 7 } }], new Set(['v4']));
                    await target.apply(gid, [{ kind: 'ensure-index', index: byN }], [], new Set(['v5']));
                    assertEquals(await entryCount(target, 'acct', 'by_n'), 1, 'rebuild did not duplicate entries');
                    assertEquals(target.validateIndexOptions(financeDecl('x', 'acct', ['n'])), undefined, 'no options is fine');
                    assertTrue(target.validateIndexOptions(financeDecl('x', 'acct', ['n'], {})) !== undefined, '{} is refused');
                } finally {
                    target.close();
                }
            },
        },
        {
            name: '[ADPTI-IDX05] facade writes maintain entries: move, null, delete, clear, abort',
            invoke: async () => {
                const target = await IdbTarget.open(uniqueDbName('writes'), { captureChanges: true });
                try {
                    await target.apply('g', [acctCreate, { kind: 'ensure-index', index: handIndex('by_ref', 'acct', ['ref']) }],
                        [], new Set(['v1']));
                    const write = async (fn: (store: ReturnType<FacadeTransaction['objectStore']>) => void): Promise<void> => {
                        const tx = target.database.transaction(['acct'], 'readwrite');
                        const finished = txDone(tx);
                        fn(tx.objectStore('acct'));
                        await finished;
                    };
                    await write((s) => { s.add({ ref: 'a' }); s.add({ ref: 'b' }); s.add({ ref: 'c' }); });
                    const byRef = async (q?: IDBValidKey | IDBKeyRange) => indexIds(target, 'acct', 'by_ref', q);
                    assertEquals((await byRef()).join(','), '1,2,3', 'three entries in key order');

                    await write((s) => { s.put({ id: 1, ref: 'z' }); });
                    assertEquals((await byRef()).join(','), '2,3,1', 'a changed key moves its entry');
                    assertEquals((await byRef('a')).length, 0, 'the old key is gone');

                    await write((s) => { s.put({ id: 2, ref: NULL }); });
                    assertEquals((await byRef()).join(','), '3,1', 'a null key removes the entry');

                    await write((s) => { s.delete(3); });
                    assertEquals((await byRef()).join(','), '1', 'delete removes the entry');

                    const tx = target.database.transaction(['acct'], 'readwrite');
                    tx.objectStore('acct').put({ id: 1, ref: 'q' });
                    tx.objectStore('acct').add({ ref: 'r' });
                    tx.abort();
                    await new Promise<void>((resolve) => { tx.onabort = () => resolve(); });
                    assertEquals((await byRef()).join(','), '1', 'an aborted transaction leaves the entries unchanged');
                    assertEquals((await byRef('z')).join(','), '1', 'and the old key still maps');

                    await write((s) => { s.add({ ref: 'y' }); });
                    await write((s) => { s.clear(); });
                    assertEquals(await entryCount(target, 'acct', 'by_ref'), 0, 'clear removes every entry');
                } finally {
                    target.close();
                }
            },
        },
        {
            name: '[ADPTI-IDX06] cross-tab: an index installed by another target is maintained and a dropped one fails reads',
            invoke: async () => {
                const name = uniqueDbName('tabs');
                const a = await IdbTarget.open(name);
                await a.apply('g', [acctCreate], [], new Set(['v1']));
                const b = await IdbTarget.open(name);
                try {
                    await b.apply('g', [{ kind: 'ensure-index', index: handIndex('by_ref', 'acct', ['ref']) }], [], new Set(['v2']));
                    assertEquals(a.cachedIndexes('acct').length, 0, "sanity: a's cache is stale");

                    const tx = a.database.transaction(['acct'], 'readwrite');
                    const finished = txDone(tx);
                    tx.objectStore('acct').add({ ref: 'from-a' });
                    await finished;
                    assertEquals((await indexIds(b, 'acct', 'by_ref', 'from-a')).join(','), '1',
                        "a's facade write maintained b's index");

                    // a learns about it (e.g. after its own next index change), then b drops it.
                    await a.apply('g', [{ kind: 'ensure-index', index: handIndex('by_n', 'acct', ['n']) }], [], new Set(['v3']));
                    assertEquals(a.cachedIndexes('acct').map((i) => i.name).join(','), 'by_n,by_ref', 'cache refreshed');
                    await b.apply('g', [{ kind: 'drop-index', table: 'acct', name: 'by_ref' }], [], new Set(['v4']));
                    const stale = readIndex(a, 'acct', 'by_ref').index;
                    let err: unknown;
                    try { await done(stale.count()); } catch (e) { err = e; }
                    assertEquals((err as DOMException | undefined)?.name, 'InvalidStateError', 'a read on a dropped index fails');
                } finally {
                    a.close();
                    b.close();
                }
            },
        },
        {
            name: '[ADPTI-IDX08] ordering differential: unawaited requests and writes inside cursor walks match native',
            invoke: async () => {
                const target = await IdbTarget.open(uniqueDbName('order'));
                let native: IDBDatabase | undefined;
                try {
                    await target.apply('g', [tCreate, { kind: 'ensure-index', index: handIndex('ia', 't', ['a']) }],
                        [], new Set(['v1']));
                    native = await new Promise<IDBDatabase>((resolve, reject) => {
                        const open = indexedDB.open(uniqueDbName('order-native'), 1);
                        open.onupgradeneeded = () => {
                            open.result.createObjectStore('t', { keyPath: 'id', autoIncrement: true }).createIndex('ia', 'a');
                        };
                        open.onsuccess = () => resolve(open.result);
                        open.onerror = () => reject(open.error);
                    });

                    const rng = mulberry32(7);
                    let walks = 0;
                    for (let round = 0; round < 40; round++) {
                        const ops: Op[] = [];
                        for (let i = 0; i < 30; i++) ops.push(randomOp(rng));
                        walks += ops.filter((o) => o.kind === 'walk').length;
                        const run = async (db: FacadeDatabaseLike): Promise<string> => {
                            const tx = db.transaction(['t'], 'readwrite');
                            const finished = txDone(tx);
                            const log: unknown[] = [];
                            issueOps(tx.objectStore('t'), ops, log);
                            await finished;
                            return stable(log);
                        };
                        const [f, n] = await Promise.all([run(target.database), run(native)]);
                        assertEquals(f, n, `round ${round}: same results in the same completion order`);
                    }
                    assertTrue(walks > 20, `sanity: cursor walks were exercised (${walks})`);

                    const all = async (db: FacadeDatabaseLike) => {
                        const tx = db.transaction(['t'], 'readonly');
                        const store = tx.objectStore('t');
                        const [docs, keys] = await Promise.all([done(store.getAll()), done(store.index('ia').getAllKeys())]);
                        return stable([docs, keys]);
                    };
                    assertEquals(await all(target.database), await all(native), 'final documents and index agree');
                } finally {
                    native?.close();
                    target.close();
                }
            },
        },
        {
            name: '[ADPTI-IDX07] store-level getAll / getAllKeys / count honour the query and count',
            invoke: async () => {
                const target = await IdbTarget.open(uniqueDbName('store'));
                try {
                    const rows: RowAction[] = [1, 2, 3, 4, 5].map((i) => ({
                        kind: 'upsert-row', table: 'acct', rowId: `r${i}`, values: { ref: `x${i}` },
                    }));
                    await target.apply('g', [acctCreate], rows, new Set(['v1']));
                    const store = () => target.database.transaction(['acct'], 'readonly').objectStore('acct');
                    const ids = (docs: Record<string, json.Literal>[]) => docs.map((d) => d.id).join(',');

                    assertEquals(ids(await done(store().getAll(IDBKeyRange.bound(2, 4)))), '2,3,4', 'getAll(range)');
                    assertEquals(ids(await done(store().getAll(IDBKeyRange.bound(2, 4, true, true)))), '3', 'getAll(open range)');
                    assertEquals(ids(await done(store().getAll(3))), '3', 'getAll(key)');
                    assertEquals(ids(await done(store().getAll(undefined, 2))), '1,2', 'getAll count');
                    assertEquals(ids(await done(store().getAll(IDBKeyRange.lowerBound(2), 2))), '2,3', 'getAll(range, count)');
                    assertEquals((await done<IDBValidKey[]>(store().getAllKeys(IDBKeyRange.upperBound(2, true)))).join(','), '1',
                        'getAllKeys(range)');
                    assertEquals((await done<IDBValidKey[]>(store().getAllKeys(null, 3))).join(','), '1,2,3', 'getAllKeys count');
                    assertEquals(await done<number>(store().count(IDBKeyRange.lowerBound(3))), 3, 'count(range)');
                    assertEquals(await done<number>(store().count(9)), 0, 'count(missing key)');
                    assertEquals(await done<number>(store().count()), 5, 'count()');
                    assertEquals(errorName(() => store().getAll(true as unknown as IDBValidKey)), 'DataError', 'invalid query');
                } finally {
                    target.close();
                }
            },
        },
    ],
};
