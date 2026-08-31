import { dag, DagGrowth, Entry } from "@hyper-hyper-space/hhs3_dag";
import { sha256 } from "@hyper-hyper-space/hhs3_crypto";
import { assertTrue, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import { createSqliteConnection } from "./sqlite_connection.js";
import { initSchema, getOrCreateDag } from "../src/sql_schema.js";
import { PollingSqlDagStore } from "../src/polling_sql_dag_store.js";
import { SqlDagStore } from "../src/sql_dag_store.js";
import { SqlLevelIndexStore } from "../src/sql_level_index_store.js";
import { SqlTopoIndexStore } from "../src/sql_topo_index_store.js";
import { SqlConnection, SqlRow } from "../src/sql_connection.js";

import { createDagLevelIndex } from "@hyper-hyper-space/hhs3_dag/dist/idx/level/level_idx.js";
import { createDagTopoIndex } from "@hyper-hyper-space/hhs3_dag/dist/idx/topo/topo_idx.js";

import { createBackendTestSuite, createParitySuite, createGrowthEventSuite } from "@hyper-hyper-space/hhs3_dag_test";

async function createSqlDag(indexType: 'level' | 'topo'): Promise<dag.Dag> {
    const conn = createSqliteConnection(":memory:");
    await initSchema(conn);
    const dagId = await getOrCreateDag(conn, "test-dag-" + Math.random(), indexType);

    const store = new PollingSqlDagStore(conn, dagId);

    if (indexType === 'level') {
        const indexStore = new SqlLevelIndexStore(conn, dagId);
        const index = createDagLevelIndex<SqlConnection>(store, indexStore);
        return dag.create(store, index, sha256);
    } else {
        const indexStore = new SqlTopoIndexStore(conn, dagId);
        const index = createDagTopoIndex<SqlConnection>(store, indexStore);
        return dag.create(store, index, sha256);
    }
}

async function testMultipleDagsInSameDb() {
    const conn = createSqliteConnection(":memory:");
    await initSchema(conn);

    const dagId1 = await getOrCreateDag(conn, "dag-1", "topo");
    const dagId2 = await getOrCreateDag(conn, "dag-2", "topo");

    assertTrue(dagId1 !== dagId2, 'dag ids should be different');

    const store1 = new PollingSqlDagStore(conn, dagId1);
    const store2 = new PollingSqlDagStore(conn, dagId2);

    const idx1 = new SqlTopoIndexStore(conn, dagId1);
    const idx2 = new SqlTopoIndexStore(conn, dagId2);

    const index1 = createDagTopoIndex<SqlConnection>(store1, idx1);
    const index2 = createDagTopoIndex<SqlConnection>(store2, idx2);

    const d1 = dag.create(store1, index1, sha256);
    const d2 = dag.create(store2, index2, sha256);

    const h1 = await d1.append({ 'in-dag-1': true }, {});
    const h2 = await d2.append({ 'in-dag-2': true }, {});

    const e1from1 = await d1.loadEntry(h1);
    assertTrue(e1from1 !== undefined, 'dag1 should have its entry');

    const e1from2 = await d2.loadEntry(h1);
    assertTrue(e1from2 === undefined, 'dag2 should not have dag1 entry');

    const e2from2 = await d2.loadEntry(h2);
    assertTrue(e2from2 !== undefined, 'dag2 should have its entry');
}

async function testGetOrCreateDagIdempotent() {
    const conn = createSqliteConnection(":memory:");
    await initSchema(conn);

    const id1 = await getOrCreateDag(conn, "same-dag", "level");
    const id2 = await getOrCreateDag(conn, "same-dag", "level");

    assertEquals(id1, id2, 'getOrCreateDag should return same id for same hash');
}

export const schemaSuite = {
    title: "\n[SQL_SCHMA] SQL Schema Tests\n",
    tests: [
        { name: "[SQL_SCHMA_00] Multiple DAGs in same database", invoke: testMultipleDagsInSameDb },
        { name: "[SQL_SCHMA_01] getOrCreateDag is idempotent", invoke: testGetOrCreateDagIdempotent },
    ],
};

export const levelBackendSuite = createBackendTestSuite(
    "SQL_LEVEL",
    () => createSqlDag('level')
);

export const topoBackendSuite = createBackendTestSuite(
    "SQL_TOPO",
    () => createSqlDag('topo')
);

export const levelParitySuite = createParitySuite(
    "SQL_LEVEL_PAR",
    () => createSqlDag('level')
);

export const topoParitySuite = createParitySuite(
    "SQL_TOPO_PAR",
    () => createSqlDag('topo')
);

export const growthEventSuite = createGrowthEventSuite(
    "SQL_GROW",
    () => createSqlDag('level')
);

async function testPollingObserverDetectsExternalWrite() {
    const conn = createSqliteConnection(":memory:");
    await initSchema(conn);
    const dagId = await getOrCreateDag(conn, "polling-test", "topo");

    const storeA = new PollingSqlDagStore(conn, dagId, 50);
    const storeB = new PollingSqlDagStore(conn, dagId, 50);

    let listenerCalled = 0;
    const listener = () => { listenerCalled++; };
    storeA.addListener(listener);

    // Let the first polling tick run so the baseline MAX(rowid) is established
    await new Promise(resolve => setTimeout(resolve, 100));

    const entry = dag.createEntry({ ext: true }, {}, undefined, sha256);
    await storeB.withTransaction(async (tx) => {
        await storeB.append(entry, tx);
        return { fireListeners: true };
    });

    // Wait for the next polling tick to detect the change
    await new Promise(resolve => setTimeout(resolve, 200));

    storeA.removeListener(listener);

    // storeA should have detected the write via polling
    assertTrue(listenerCalled >= 1, 'polling observer on store A should have detected the write (got ' + listenerCalled + ')');
}

export const pollingObserverSuite = {
    title: "\n[SQL_POLL] Polling External Observer Tests\n",
    tests: [
        { name: "[SQL_POLL_00] Polling observer detects external write", invoke: testPollingObserverDetectsExternalWrite },
    ],
};

// ---- Concurrency hardening tests ------------------------------------------

const tick = () => new Promise<void>(resolve => setTimeout(resolve, 0));

async function waitUntil(cond: () => boolean, tries = 200): Promise<boolean> {
    for (let i = 0; i < tries; i++) {
        if (cond()) return true;
        await tick();
    }
    return cond();
}

// Test-only store whose external observer is driven manually: `startExternalObserver`
// captures the notify callback (which internally calls handleExternalGrowth with the
// armed epoch) instead of wiring up a real polling/watcher strategy.
class ManualObserverStore extends SqlDagStore {
    notify: (() => void) | undefined = undefined;
    startCount = 0;
    stopCount = 0;
    protected startExternalObserver(notify: () => void): unknown {
        this.startCount++;
        this.notify = notify;
        return { armed: true };
    }
    protected stopExternalObserver(_handle: unknown): void {
        this.stopCount++;
        this.notify = undefined;
    }
}

// Connection wrapper that can pause the external-growth read (the only query
// containing `rowid > ?`) so a second notification can be injected while the
// handler is mid-flight.
class GatedConnection implements SqlConnection {
    gateOn = false;
    private queue: Array<() => void> = [];
    constructor(private inner: SqlConnection) {}
    pendingCount(): number { return this.queue.length; }
    releaseOne(): void { const r = this.queue.shift(); if (r) r(); }
    async query(sql: string, params: unknown[] = []): Promise<SqlRow[]> {
        if (this.gateOn && sql.includes('rowid > ?')) {
            await new Promise<void>(resolve => this.queue.push(resolve));
        }
        return this.inner.query(sql, params);
    }
    execute(sql: string, params: unknown[] = []): Promise<number> {
        return this.inner.execute(sql, params);
    }
    transaction<T>(fn: (conn: SqlConnection) => Promise<T>): Promise<T> {
        return this.inner.transaction(fn);
    }
}

async function appendExternal(writer: PollingSqlDagStore, entry: Entry): Promise<void> {
    await writer.withTransaction(async (tx) => {
        await writer.append(entry, tx);
        return { fireListeners: true, entries: [entry] };
    });
}

async function testReArmReBaselinesWithoutReplay() {
    const conn = createSqliteConnection(":memory:");
    await initSchema(conn);
    const dagId = await getOrCreateDag(conn, "rebaseline", "topo");

    const store = new ManualObserverStore(conn, dagId);
    const writer = new PollingSqlDagStore(conn, dagId); // never armed: simulates an external writer

    const delivered: string[] = [];
    const listener = (g: DagGrowth) => { for (const e of g.entries) delivered.push(e.hash); };

    store.addListener(listener); // arm (epoch 1), baseline = current head (0)
    const e1 = dag.createEntry({ n: 1 }, {}, undefined, sha256);
    await appendExternal(writer, e1);
    store.notify!();
    await waitUntil(() => delivered.includes(e1.hash));
    assertTrue(delivered.includes(e1.hash), 'e1 should be delivered while armed');
    assertEquals(store.startCount, 1, 'observer should have started exactly once');

    store.removeListener(listener); // disarm (epoch bumped, cursor cleared)
    assertEquals(store.stopCount, 1, 'observer should have stopped on disarm');

    // A write that lands while disarmed must not be replayed on re-arm: a
    // re-subscribing consumer reads current state, then expects at-least-once
    // for subsequent changes only.
    const e2 = dag.createEntry({ n: 2 }, {}, undefined, sha256);
    await appendExternal(writer, e2);

    delivered.length = 0;
    store.addListener(listener); // re-arm (epoch 2), baseline must move to current head
    await tick();
    store.notify!();
    await tick(); await tick();
    assertTrue(delivered.length === 0, 're-arm must not replay pre-existing history (got ' + JSON.stringify(delivered) + ')');
    assertEquals(store.startCount, 2, 'observer should have re-armed');

    // A fresh write after re-arm is delivered, and only that one.
    const e3 = dag.createEntry({ n: 3 }, {}, undefined, sha256);
    await appendExternal(writer, e3);
    store.notify!();
    await waitUntil(() => delivered.includes(e3.hash));
    assertTrue(delivered.includes(e3.hash), 'post-re-arm write should be delivered');
    assertTrue(!delivered.includes(e1.hash) && !delivered.includes(e2.hash), 'no stale history replay after re-arm');

    store.removeListener(listener);
}

async function testCoalescedNotifyNotDropped() {
    const inner = createSqliteConnection(":memory:");
    const conn = new GatedConnection(inner);
    await initSchema(conn);
    const dagId = await getOrCreateDag(conn, "rescan", "topo");

    const store = new ManualObserverStore(conn, dagId);
    const writer = new PollingSqlDagStore(conn, dagId);

    const delivered: string[] = [];
    const listener = (g: DagGrowth) => { for (const e of g.entries) delivered.push(e.hash); };
    store.addListener(listener); // arm, baseline = 0
    await tick();

    const e1 = dag.createEntry({ n: 1 }, {}, undefined, sha256);
    await appendExternal(writer, e1);

    conn.gateOn = true;
    store.notify!(); // pass 1 begins, blocks at the gated growth read
    await waitUntil(() => conn.pendingCount() >= 1);
    store.notify!(); // arrives mid-flight: must set rescanRequested, not be dropped

    conn.releaseOne(); // pass 1 reads e1 (only committed row), fires, then loops (rescan)
    await waitUntil(() => conn.pendingCount() >= 1); // pass 2 (rescan) now blocked at the gate

    const e2 = dag.createEntry({ n: 2 }, {}, undefined, sha256); // arrives before the rescan read
    await appendExternal(writer, e2);
    conn.releaseOne(); // pass 2 reads e2

    await waitUntil(() => delivered.includes(e2.hash));
    conn.gateOn = false;

    assertTrue(delivered.includes(e1.hash), 'e1 should be delivered by the first pass');
    assertTrue(delivered.includes(e2.hash), 'coalesced notification must not be dropped: e2 delivered by rescan');

    store.removeListener(listener);
}

export const hardeningSuite = {
    title: "\n[SQL_HARD] Reactivity Concurrency Hardening Tests\n",
    tests: [
        { name: "[SQL_HARD_00] Re-arm re-baselines without history replay", invoke: testReArmReBaselinesWithoutReplay },
        { name: "[SQL_HARD_01] Coalesced notify is not dropped (rescan)",    invoke: testCoalescedNotifyNotDropped },
    ],
};
