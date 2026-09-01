import "fake-indexeddb/auto";

import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { json } from "@hyper-hyper-space/hhs3_json";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import { projectGroup, SchemaAction, DEFAULT_AUTHOR_COLUMN } from "@hyper-hyper-space/hhs3_rdb_adapter";
import {
    createGroup,
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

function idbLocal(target: IdbTarget): LocalMutator {
    return {
        insert: async (table, values, author) => {
            const db = target.database;
            const tx = db.transaction([table], 'readwrite');
            const done = waitTx(tx);
            const row: Record<string, json.Literal> = { ...values };
            if (author !== undefined) row[DEFAULT_AUTHOR_COLUMN] = author;
            const id = await waitReq(tx.objectStore(table).add(row));
            await done;
            return id as number;
        },
        update: async (table, localId, values: RowValues) => {
            const db = target.database;
            const tx = db.transaction([table], 'readwrite');
            const done = waitTx(tx);
            const store = tx.objectStore(table);
            const existing = await waitReq(store.get(localId));
            if (existing === undefined) throw new Error(`no local row ${table}#${localId}`);
            await waitReq(store.put({ ...existing, ...values }));
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
    const name = uniqueDbName('ing');
    const target = await IdbTarget.open(name, { captureChanges: true });
    return {
        target,
        read: idbReader(target),
        local: idbLocal(target),
        cleanup: () => { target.close(); },
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
            name: '[ADPTI-04] ChangeSignalSource fires on another IdbTarget sharing the db (BroadcastChannel)',
            invoke: async () => {
                const name = uniqueDbName('chan');
                const a = await IdbTarget.open(name, { captureChanges: true });
                const b = await IdbTarget.open(name, { captureChanges: true });
                try {
                    const { group, admin } = await createGroup();
                    await (await group.getTable('ledger')).insert('l1', { ref: 'R-1', amount: '10.00' }, admin);
                    await projectGroup(group, a);

                    let fires = 0;
                    const listener = (): void => { fires++; };
                    const wrote = new Promise<void>((resolve, reject) => {
                        const timer = setTimeout(() => reject(new Error('no change signal within timeout')), 2000);
                        b.addChangeListener(() => {
                            listener();
                            if (fires >= 2) { clearTimeout(timer); resolve(); }
                        });
                    });
                    assertEquals(fires, 1, 'channel primes one signal on arm');

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
    ],
};
