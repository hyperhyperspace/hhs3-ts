import "fake-indexeddb/auto";

import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { json } from "@hyper-hyper-space/hhs3_json";
import { serializePublicKeyToBase64, type Version } from "@hyper-hyper-space/hhs3_mvt";
import {
    PendingChangesError, projectGroup, SchemaAction, DEFAULT_AUTHOR_COLUMN, syncGroup, type ResolvedIndex,
} from "@hyper-hyper-space/hhs3_rdb_adapter";
import {
    createGroup, sameVersion,
    TargetHarness, ProjectionReader, ReadRow, RowValues,
    IngestionHarness, LocalMutator,
} from "@hyper-hyper-space/hhs3_rdb_adapter_test";

import { IdbTarget } from "../src/idb_target.js";
import { FacadeRequest, FacadeTransaction } from "../src/idb_facade.js";

let dbCounter = 0;
function uniqueDbName(label: string): string {
    dbCounter++;
    return `hhs3-rdb-idb-${label}-${Date.now()}-${dbCounter}-${Math.random().toString(36).slice(2)}`;
}

function waitReq<T>(req: FacadeRequest<T>): Promise<T> {
    return new Promise((resolve, reject) => {
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error ?? new Error('idb request failed'));
    });
}

function waitTx(tx: FacadeTransaction): Promise<void> {
    return new Promise((resolve, reject) => {
        tx.oncomplete = () => resolve();
        tx.onabort = () => reject(tx.error ?? new Error('transaction aborted'));
        tx.onerror = () => reject(tx.error ?? new Error('transaction error'));
    });
}

function idbReader(target: IdbTarget): ProjectionReader {
    return {
        hasTable: (table) => target.hasTable(table),
        listTables: () => target.appTables(),
        getRowIds: (table) => target.getRowIds(table),
        getRow: async (table, rowId): Promise<ReadRow | undefined> => {
            const row = await target.getRowByRowId(table, rowId);
            return row === undefined ? undefined : row;
        },
        syncId: (table, rowId) => target.syncId(table, rowId),
        columnType: (table, column) => target.columnType(table, column),
    };
}

// The shared suite's updates go through the facade's `patch` once it exists
// (HIP-29), so that each case names the same columns on the three targets, as
// an UPDATE and memory's localUpdate do. `put` keeps cases of its own.
const UPDATE_THROUGH_PATCH = true;
type PatchFn = (fields: Record<string, json.Literal>, opts: { delete: string[] }) => FacadeRequest<unknown>;

function idbLocal(target: IdbTarget): LocalMutator {
    return {
        insert: async (table, values, author) => {
            const db = target.database;
            const tx = db.transaction([table], 'readwrite');
            const done = waitTx(tx);
            const row: Record<string, json.Literal | null> = { ...values };
            if (author !== undefined) row[DEFAULT_AUTHOR_COLUMN] = author;
            const id = await waitReq(tx.objectStore(table).add(row));
            await done;
            return id as number;
        },
        insertAt: async (table, localId, values) => {
            const tx = target.database.transaction([table], 'readwrite');
            const done = waitTx(tx);
            const store = tx.objectStore(table);
            if (await waitReq(store.get(localId)) !== undefined) throw new Error(`a record holds ${table}#${localId}`);
            await waitReq(store.put({ ...values, id: localId }));
            await done;
        },
        update: async (table, localId, values) => {
            const db = target.database;
            const tx = db.transaction([table], 'readwrite');
            const done = waitTx(tx);
            const store = tx.objectStore(table);
            const existing = await waitReq(store.get(localId));
            if (existing === undefined) throw new Error(`no local row ${table}#${localId}`);
            const patch = (store as unknown as { patch?: PatchFn }).patch;
            if (UPDATE_THROUGH_PATCH && patch !== undefined) {
                // Name only the columns given, as an UPDATE does: a null clears its column.
                const set: Record<string, json.Literal> = { id: localId };
                const clear: string[] = [];
                for (const [column, value] of Object.entries(values)) {
                    if (value === null) clear.push(column);
                    else set[column] = value;
                }
                await waitReq(patch.call(store, set, { delete: clear }));
            } else {
                await waitReq(store.put({ ...existing, ...values }));
            }
            await done;
        },
        delete: async (table, localId) => {
            const db = target.database;
            const tx = db.transaction([table], 'readwrite');
            const done = waitTx(tx);
            await waitReq(tx.objectStore(table).delete(localId));
            await done;
        },
        setCaptureEnabled: (on) => target.setCaptureEnabled(on),
    };
}

export async function idbHarness(): Promise<TargetHarness> {
    const name = uniqueDbName('proj');
    const target = await IdbTarget.open(name);
    return {
        target,
        read: idbReader(target),
        cleanup: () => { target.close(); },
    };
}

export async function idbIngestionHarness(): Promise<IngestionHarness> {
    return idbIngestionHarnessOn(uniqueDbName('ing'));
}

// A harness over the database `name`. Its reopen closes the target and opens
// the database again, as a new tab or a restarted page would.
async function idbIngestionHarnessOn(name: string): Promise<IngestionHarness> {
    const target = await IdbTarget.open(name, { captureChanges: true });
    return {
        target,
        read: idbReader(target),
        local: idbLocal(target),
        cleanup: () => { target.close(); },
        reopen: async () => {
            target.close();
            return idbIngestionHarnessOn(name);
        },
    };
}

const acctCreate: SchemaAction = {
    kind: 'create-table', table: 'acct', syncTable: 'acct_sync', primaryKey: 'id',
    columns: [{ name: 'ref', def: { type: 'string' } }],
};

export const idbSpecificTests = {
    title: '[ADPTI] rdb_adapter IndexedDB target (engine-specific)',
    tests: [
        {
            name: '[ADPTI-01] adapter apply does not land in the outbox (echo suppression)',
            invoke: async () => {
                const name = uniqueDbName('echo');
                const target = await IdbTarget.open(name, { captureChanges: true });
                try {
                    const { group, admin } = await createGroup();
                    await (await group.getTable('ledger')).insert('l1', { ref: 'R-1', amount: '10.00' }, admin);
                    await projectGroup(group, target);
                    const batch = await target.drainChanges();
                    assertEquals(batch.changes.length, 0, 'materialization writes are not captured');
                } finally {
                    target.close();
                }
            },
        },
        {
            name: '[ADPTI-02] facade transaction abort drops both the row and the outbox',
            invoke: async () => {
                const name = uniqueDbName('abort');
                const target = await IdbTarget.open(name, { captureChanges: true });
                try {
                    const v1: Version = new Set(['v1']);
                    await target.apply('g', [acctCreate], [], v1);

                    const db = target.database;
                    const tx = db.transaction(['acct'], 'readwrite');
                    tx.objectStore('acct').add({ ref: 'nope' });
                    tx.abort();
                    await new Promise<void>((resolve) => { tx.onabort = () => resolve(); });

                    assertEquals(await target.hasTable('acct'), true, 'table still exists');
                    assertEquals((await target.getRowIds('acct')).length, 0, 'aborted insert left no row');
                    assertEquals((await target.drainChanges()).changes.length, 0, 'aborted insert left no outbox row');
                } finally {
                    target.close();
                }
            },
        },
        {
            name: '[ADPTI-03] reopen sees persisted tables, rows, and checkpoint',
            invoke: async () => {
                const name = uniqueDbName('reopen');
                const { group, admin } = await createGroup();
                await (await group.getTable('ledger')).insert('l1', { ref: 'R-1', amount: '10.00' }, admin);
                const first = await IdbTarget.open(name);
                try {
                    await projectGroup(group, first);
                } finally {
                    first.close();
                }

                const second = await IdbTarget.open(name);
                try {
                    assertTrue(await second.hasTable('ledger'), 'ledger survived reopen');
                    assertEquals((await second.getRowIds('ledger')).length, 1, 'row survived reopen');
                    assertTrue(await second.getCheckpoint(group.getId()) !== undefined, 'checkpoint survived reopen');
                } finally {
                    second.close();
                }
            },
        },
        {
            name: '[ADPTI-04] a local write is announced to another IdbTarget on the same database',
            invoke: async () => {
                const name = uniqueDbName('chan');
                const a = await IdbTarget.open(name, { captureChanges: true });
                const b = await IdbTarget.open(name, { captureChanges: true });
                try {
                    const { group, admin } = await createGroup();
                    await (await group.getTable('ledger')).insert('l1', { ref: 'R-1', amount: '10.00' }, admin);
                    await projectGroup(group, a);

                    let fires = 0;
                    const wrote = new Promise<void>((resolve, reject) => {
                        const timer = setTimeout(() => reject(new Error('no commit event within timeout')), 2000);
                        b.addCommitListener((commit) => {
                            if (commit.source !== 'local') return;
                            fires++;
                            clearTimeout(timer);
                            resolve();
                        });
                    });
                    assertEquals(fires, 0, 'arming does not invent a local write');

                    const local = idbLocal(a);
                    await local.insert('tags', { code: 'urgent' });
                    await wrote;
                } finally {
                    a.close();
                    b.close();
                }
            },
        },
        {
            name: '[ADPTI-05] two connections serialize id allocation',
            invoke: async () => {
                const name = uniqueDbName('ids');
                const a = await IdbTarget.open(name);
                const b = await IdbTarget.open(name);
                try {
                    const v1: Version = new Set(['v1']);
                    await Promise.all([
                        a.apply('g', [acctCreate], [
                            { kind: 'upsert-row', table: 'acct', rowId: 'r1', values: { ref: 'a' } },
                        ], v1),
                        b.apply('g', [acctCreate], [
                            { kind: 'upsert-row', table: 'acct', rowId: 'r2', values: { ref: 'b' } },
                        ], v1),
                    ]);
                    const id1 = await a.syncId('acct', 'r1');
                    const id2 = await a.syncId('acct', 'r2');
                    assertTrue(id1 !== undefined && id2 !== undefined, 'both rows allocated');
                    assertTrue(id1 !== id2, 'concurrent applies did not hand out the same local id');
                } finally {
                    a.close();
                    b.close();
                }
            },
        },
        {
            name: '[ADPTI-06] a null property is stored as a missing key, and a put captures a null or a dropped property as the same clear, with every other column',
            invoke: async () => {
                const target = await IdbTarget.open(uniqueDbName('nulls'), { captureChanges: true });
                try {
                    const { group, admin } = await createGroup();
                    await (await group.getTable('ledger')).insert('l1', { ref: 'R-1', amount: '10.00', memo: 'paid' }, admin);
                    await (await group.getTable('ledger')).insert('l2', { ref: 'R-2', amount: '20.00' }, admin);
                    await projectGroup(group, target);

                    const write = async (fn: (store: ReturnType<FacadeTransaction['objectStore']>) => Promise<void>): Promise<void> => {
                        const tx = target.database.transaction(['ledger'], 'readwrite');
                        const done = waitTx(tx);
                        await fn(tx.objectStore('ledger'));
                        await done;
                    };
                    const readAll = async (): Promise<{ [id: string]: Record<string, json.Literal> }> => {
                        const tx = target.database.transaction(['ledger'], 'readonly');
                        const docs = await waitReq(tx.objectStore('ledger').getAll());
                        return Object.fromEntries(docs.map((d) => [String(d.ref), d]));
                    };
                    // A put captures every column (HIP-29): each update names ref, memo
                    // and amount, and the memo is what each step looks at.
                    const captured = async (): Promise<string> => {
                        const { changes } = await target.drainChanges();
                        return changes.map((c) => {
                            if (c.kind !== 'update') return c.kind === 'delete' ? 'delete' : `${c.kind} ${JSON.stringify(c.values)}`;
                            const named = ['amount', 'memo', 'ref'].every((col) => col in c.values);
                            return `update memo=${JSON.stringify(c.values.memo ?? null)}${named ? '' : ' (not every column)'}`;
                        }).join('; ');
                    };
                    const before = await readAll();
                    const withMemo = before['R-1'];   // memo is 'paid'
                    const noMemo = before['R-2'];     // the target left memo empty
                    assertTrue(!('memo' in noMemo), 'a column that is NULL in rdb is a missing key');

                    // a put with memo: null over a value, and a put that drops the property, are the same clear
                    await write(async (store) => { await waitReq(store.put({ ...withMemo, memo: null })); });
                    assertTrue(!('memo' in (await readAll())['R-1']), 'a null property is stored as a missing key');
                    assertEquals(await captured(), 'update memo=null', 'a put of null over a value is captured as a change to null');

                    // a null put over an empty column captures it too, with either spelling: a put names every column
                    await write(async (store) => { await waitReq(store.put({ ...noMemo, memo: null })); });
                    await write(async (store) => { await waitReq(store.put({ ...noMemo })); });
                    assertEquals(await captured(), 'update memo=null; update memo=null; update memo=null',
                        'a null put over an empty column is captured as NULL'); 

                    // back to a value, then drop the property
                    await write(async (store) => { await waitReq(store.put({ ...withMemo, memo: 'again' })); });
                    const { memo: _dropped, ...withoutMemo } = withMemo;
                    await write(async (store) => { await waitReq(store.put(withoutMemo)); });
                    assertEquals(await captured(), 'update memo=null; update memo=null; update memo=null; update memo="again"; update memo=null',
                        'a put that drops a property the row had is captured as a change to null');

                    // an added record keeps no key for a null, and its capture names the null
                    await write(async (store) => { await waitReq(store.add({ ref: 'R-3', amount: '1.00', memo: null })); });
                    assertTrue(!('memo' in (await readAll())['R-3']), 'an added null property is stored as a missing key');
                    assertTrue((await captured()).endsWith('insert {"ref":"R-3","memo":null,"amount":"1.00"}'),
                        'the captured insert names the null the app wrote');
                } finally {
                    target.close();
                }
            },
        },
        {
            name: '[ADPTI-07] a put that leaves out the author column, or gives it null, keeps the stored author and captures nothing for it',
            invoke: async () => {
                const target = await IdbTarget.open(uniqueDbName('keep-author'), { captureChanges: true });
                try {
                    const { group, admin } = await createGroup();
                    await (await group.getTable('ledger')).insert('l1', { ref: 'R-1', amount: '10.00' }, admin);
                    await projectGroup(group, target);

                    const write = async (fn: (store: ReturnType<FacadeTransaction['objectStore']>) => Promise<void>): Promise<void> => {
                        const tx = target.database.transaction(['ledger'], 'readwrite');
                        const done = waitTx(tx);
                        await fn(tx.objectStore('ledger'));
                        await done;
                    };
                    const readOne = async (): Promise<Record<string, json.Literal>> => {
                        const tx = target.database.transaction(['ledger'], 'readonly');
                        return (await waitReq(tx.objectStore('ledger').getAll()))[0];
                    };
                    const stored = await readOne();
                    const author = stored[DEFAULT_AUTHOR_COLUMN];
                    assertTrue(typeof author === 'number', 'the projected row has an author');

                    // a record built from a form: no author column
                    const { [DEFAULT_AUTHOR_COLUMN]: _a, ...formRecord } = stored;
                    await write(async (store) => { await waitReq(store.put({ ...formRecord, memo: 'one' })); });
                    assertEquals((await readOne())[DEFAULT_AUTHOR_COLUMN], author, 'a put without the author column keeps it');
                    await write(async (store) => { await waitReq(store.put({ ...stored, memo: 'two', [DEFAULT_AUTHOR_COLUMN]: null })); });
                    assertEquals((await readOne())[DEFAULT_AUTHOR_COLUMN], author, 'a put of a null author keeps it');

                    // A put captures every column (HIP-29), and nothing for an author it leaves out or gives null.
                    const { changes } = await target.drainChanges();
                    assertEquals(changes.map((c) => c.kind === 'update' ? Object.keys(c.values).sort().join(',') : c.kind).join('; '),
                        'amount,memo,ref; amount,memo,ref', 'every column but the author is captured');
                } finally {
                    target.close();
                }
            },
        },
        {
            name: "[ADPTI-10] a put over an existing record captures every column; an author it leaves out is kept; an insert's filled author gets its '@author' index entry",
            invoke: async () => {
                const target = await IdbTarget.open(uniqueDbName('put-every'), { captureChanges: true });
                try {
                    const { group, admin } = await createGroup();
                    await (await group.getTable('ledger')).insert('l1', { ref: 'R-1', memo: 'paid', amount: '10.00' }, admin);
                    await projectGroup(group, target);
                    const byAuthor: ResolvedIndex = { name: 'by_author', groupId: group.getId(), table: 'tags',
                        columns: [{ rdb: '@author', target: DEFAULT_AUTHOR_COLUMN }], fingerprint: 'by_author' };
                    await target.apply(group.getId(), [{ kind: 'ensure-index', index: byAuthor }], [],
                        (await target.getCheckpoint(group.getId()))!);

                    const tx = target.database.transaction(['ledger'], 'readwrite');
                    const done = waitTx(tx);
                    const store = tx.objectStore('ledger');
                    const [row] = await waitReq(store.getAll());
                    const { [DEFAULT_AUTHOR_COLUMN]: _a, ...form } = row;
                    await waitReq(store.put({ ...form, memo: 'paid' }));
                    await done;
                    const [update] = (await target.drainChanges()).changes;
                    assertTrue(update?.kind === 'update', 'one update');
                    assertEquals(Object.keys(update.kind === 'update' ? update.values : {}).sort().join(','), 'amount,memo,ref',
                        'every column, the unchanged ones too, and nothing for the author it left out');

                    const keyId = await target.registerKey('default', admin.keyId, serializePublicKeyToBase64(admin.publicKey));
                    await target.recordWriter({ writerKeyId: keyId, hashSuite: 'sha-256', groups: { [group.getId()]: { name: 'finance', mode: 'on' } } });
                    const tx2 = target.database.transaction(['tags'], 'readwrite');
                    const done2 = waitTx(tx2);
                    const id = await waitReq(tx2.objectStore('tags').add({ code: 'mine' }));
                    await done2;
                    const read = target.database.transaction(['tags'], 'readonly');
                    const found = await waitReq(read.objectStore('tags').index('by_author').getAll(keyId));
                    assertEquals(found.map((r) => r.id).join(','), String(id), "the filled author is in the '@author' index");
                } finally {
                    target.close();
                }
            },
        },
        {
            name: '[ADPTI-11] a patch writes the fields it is given, clears the ones it lists, keeps the others and their index entries, and captures exactly the fields it names; a patch of a missing key writes nothing; a field both given and listed is refused',
            invoke: async () => {
                const target = await IdbTarget.open(uniqueDbName('patch'), { captureChanges: true });
                try {
                    const { group, admin } = await createGroup();
                    await (await group.getTable('ledger')).insert('l1', { ref: 'R-1', memo: 'paid', amount: '10.00' }, admin);
                    await projectGroup(group, target);
                    const byRef: ResolvedIndex = { name: 'by_ref', groupId: group.getId(), table: 'ledger',
                        columns: [{ rdb: 'ref', target: 'ref' }], fingerprint: 'by_ref' };
                    await target.apply(group.getId(), [{ kind: 'ensure-index', index: byRef }], [],
                        (await target.getCheckpoint(group.getId()))!);
                    const patchOf = async (fields: Record<string, json.Literal>, opts?: { delete?: string[] }): Promise<void> => {
                        const tx = target.database.transaction(['ledger'], 'readwrite');
                        const done = waitTx(tx);
                        await waitReq(tx.objectStore('ledger').patch(fields, opts));
                        await done;
                    };
                    const [before] = await waitReq(target.database.transaction(['ledger'], 'readonly').objectStore('ledger').getAll());
                    const id = before.id as number;

                    await patchOf({ id, amount: '10.00' }, { delete: ['memo'] });
                    const [after] = await waitReq(target.database.transaction(['ledger'], 'readonly').objectStore('ledger').getAll());
                    assertEquals(after.ref, 'R-1', 'a field it doesn\'t name is kept');
                    assertTrue(!('memo' in after), 'a field it lists is cleared');
                    assertEquals(after[DEFAULT_AUTHOR_COLUMN], before[DEFAULT_AUTHOR_COLUMN], 'and so is the author');
                    const indexed = await waitReq(target.database.transaction(['ledger'], 'readonly').objectStore('ledger').index('by_ref').getAll('R-1'));
                    assertEquals(indexed.length, 1, 'the index entry of a kept field stays');
                    const changes = (await target.drainChanges()).changes;
                    assertEquals(changes.map((c) => c.kind === 'update' ? JSON.stringify(c.values) : c.kind).join('; '),
                        '{"amount":"10.00","memo":null}', 'exactly the named fields, the unchanged one too, and the cleared one as NULL');

                    await patchOf({ id: 999, memo: 'ghost' });
                    assertEquals((await target.drainChanges()).changes.length, 1, 'a patch of a missing key captures nothing');
                    const all = await waitReq(target.database.transaction(['ledger'], 'readonly').objectStore('ledger').getAll());
                    assertEquals(all.length, 1, 'and writes nothing');

                    let refused = false;
                    try { await patchOf({ id, memo: 'x' }, { delete: ['memo'] }); } catch (e) { refused = e instanceof DOMException && e.name === 'DataError'; }
                    assertTrue(refused, 'a field both given and listed is a DataError');
                } finally {
                    target.close();
                }
            },
        },
        {
            name: "[ADPTI-09] a change written in another tab makes this tab's projection refuse, until a cycle ingests it",
            invoke: async () => {
                const name = uniqueDbName('pending-across');
                const a = await IdbTarget.open(name, { captureChanges: true });
                try {
                    const { group, admin } = await createGroup();
                    const ledger = await group.getTable('ledger');
                    await ledger.insert('l1', { ref: 'R-1', amount: '1.00' }, admin);
                    await projectGroup(group, a);

                    const b = await IdbTarget.open(name, { captureChanges: true });
                    try {
                        await ledger.insert('l2', { ref: 'R-2', amount: '2.00' }, admin);
                        const tx = b.database.transaction(['tags'], 'readwrite');
                        const done = waitTx(tx);
                        await waitReq(tx.objectStore('tags').add({ code: 'from-b' }));
                        await done;
                    } finally {
                        b.close();
                    }
                    let refused = false;
                    try { await projectGroup(group, a); } catch (e) { refused = e instanceof PendingChangesError; }
                    assertTrue(refused, "this tab's projection refuses over the other tab's pending change");

                    await syncGroup(group, a, { writer: admin });
                    assertTrue(!await a.hasPendingChanges(), 'a cycle ingests it');
                    assertTrue(sameVersion(await a.getCheckpoint(group.getId()), await (await group.getScopedDag()).getFrontier()),
                        'and then projects');
                } finally {
                    a.close();
                }
            },
        },
        {
            name: '[ADPTI-08] another target on the same database follows schema changes: a listener can open a new table and its index as soon as the apply is delivered, and a reconnect refreshes a target with no listener',
            invoke: async () => {
                const name = uniqueDbName('meta-follows');
                const a = await IdbTarget.open(name, { captureChanges: true });
                const b = await IdbTarget.open(name, { captureChanges: true });
                const c = await IdbTarget.open(name, { captureChanges: true });
                const byRef: ResolvedIndex = { name: 'by_ref', groupId: 'g', table: 'acct', columns: [{ rdb: 'ref', target: 'ref' }], fingerprint: 'by_ref' };
                const opens = (target: IdbTarget): string => {
                    try {
                        const store = target.database.transaction(['acct'], 'readonly').objectStore('acct');
                        return store.indexNames.contains('by_ref') ? 'table and index' : 'table only';
                    } catch (e) {
                        return e instanceof Error ? e.name : String(e);
                    }
                };
                try {
                    const delivered = new Promise<string>((resolve, reject) => {
                        const timer = setTimeout(() => reject(new Error('no apply event within timeout')), 2000);
                        b.addCommitListener((commit) => {
                            if (commit.source !== 'apply' || !commit.schemaTables.includes('acct')) return;
                            clearTimeout(timer);
                            resolve(opens(b));
                        });
                    });
                    await a.apply('g', [acctCreate, { kind: 'ensure-index', index: byRef }],
                        [{ kind: 'upsert-row', table: 'acct', rowId: 'r1', values: { ref: 'a' } }], new Set(['v1']));
                    assertEquals(opens(a), 'table and index', 'the target that applied knows the table and index when apply() returns');
                    assertEquals(await delivered, 'table and index', 'a listening target knows them when the apply is delivered');

                    assertEquals(opens(c), 'NotFoundError', 'a target with no listener is not told');
                    c.notifyExternalCommit({ source: 'reconnect' });
                    const deadline = Date.now() + 2000;
                    while (opens(c) !== 'table and index' && Date.now() < deadline) await new Promise((r) => setTimeout(r, 10));
                    assertEquals(opens(c), 'table and index', 'a reconnect makes it read them again');
                } finally {
                    a.close();
                    b.close();
                    c.close();
                }
            },
        },
    ],
};
