import { dag } from "@hyper-hyper-space/hhs3_dag";
import { sha256 } from "@hyper-hyper-space/hhs3_crypto";
import { assertTrue, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import { createSqliteConnection } from "./sqlite_connection.js";
import { initSchema, getOrCreateDag } from "../src/sql_schema.js";
import { PollingSqlDagStore } from "../src/polling_sql_dag_store.js";
import { SqlLevelIndexStore } from "../src/sql_level_index_store.js";
import { SqlTopoIndexStore } from "../src/sql_topo_index_store.js";
import { SqlConnection } from "../src/sql_connection.js";

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
