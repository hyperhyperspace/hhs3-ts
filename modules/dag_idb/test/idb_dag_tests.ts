// Provide IndexedDB globals (indexedDB, IDBKeyRange, ...) in Node.
import "fake-indexeddb/auto";

import { dag, position } from "@hyper-hyper-space/hhs3_dag";
import { sha256 } from "@hyper-hyper-space/hhs3_crypto";
import { assertTrue, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import { createBackendTestSuite, createParitySuite, createGrowthEventSuite } from "@hyper-hyper-space/hhs3_dag_test";
import { IdbDagDb } from "../src/idb_dag_db.js";
import { ENTRIES, TOPO_INDEX, ENTRY_INFO, openDatabase, reqToPromise } from "../src/idb_schema.js";

let dbCounter = 0;
function uniqueDbName(label: string): string {
    dbCounter++;
    return `hhs3-idb-${label}-${Date.now()}-${dbCounter}-${Math.random().toString(36).slice(2)}`;
}

async function createIdbDag(indexType: 'level' | 'topo'): Promise<dag.Dag> {
    const db = await IdbDagDb.open(uniqueDbName("suite"), { hashSuite: sha256 });
    const result = await db.getOrCreateDag(
        "test-dag-" + Math.random(),
        { type: "test/dag", idxType: indexType }
    );
    return result.dag;
}

export const levelBackendSuite = createBackendTestSuite(
    "IDB_LEVEL",
    () => createIdbDag('level')
);

export const topoBackendSuite = createBackendTestSuite(
    "IDB_TOPO",
    () => createIdbDag('topo')
);

export const levelParitySuite = createParitySuite(
    "IDB_LEVEL_PAR",
    () => createIdbDag('level')
);

export const topoParitySuite = createParitySuite(
    "IDB_TOPO_PAR",
    () => createIdbDag('topo')
);

export const growthEventSuite = createGrowthEventSuite(
    "IDB_GROW",
    () => createIdbDag('level')
);

// ---- IdbDagDb manager tests ---------------------------------------------

async function testDefaultIndexIsLevel() {
    const db = await IdbDagDb.open(uniqueDbName("default-index"), { hashSuite: sha256 });
    try {
        const { created } = await db.getOrCreateDag("default-index-test", { type: "test/default-index" });
        assertTrue(created, "first getOrCreateDag should create the DAG");

        let threw = false;
        try {
            await db.getOrCreateDag("default-index-test", { type: "test/default-index", idxType: "topo" });
        } catch {
            threw = true;
        }
        assertTrue(threw, "default index should be level, so reopening as topo should throw");
    } finally {
        db.close();
    }
}

async function testExplicitIndexMismatchThrows() {
    const db = await IdbDagDb.open(uniqueDbName("idx-mismatch"), { hashSuite: sha256 });
    try {
        await db.getOrCreateDag("idx-mismatch-test", { type: "test/idx-mismatch", idxType: "topo" });

        let threw = false;
        try {
            await db.getOrCreateDag("idx-mismatch-test", { type: "test/idx-mismatch", idxType: "level" });
        } catch {
            threw = true;
        }
        assertTrue(threw, "reopening an existing topo DAG as level should throw");
    } finally {
        db.close();
    }
}

async function testListDagsReturnsMetadata() {
    const db = await IdbDagDb.open(uniqueDbName("list"), { hashSuite: sha256 });
    try {
        await db.getOrCreateDag("list-a", { type: "test/list-a" });
        await db.getOrCreateDag("list-b", { type: "test/list-b", idxType: "topo" });

        const entries = await db.listDags();
        const a = entries.find(entry => entry.id === "list-a");
        const b = entries.find(entry => entry.id === "list-b");

        assertTrue(a !== undefined, "listDags should include list-a");
        assertTrue(a!.type === "test/list-a", "list-a should have its stored type");
        assertTrue(a!.createdAt > 0, "list-a should have a createdAt timestamp");
        assertTrue(b !== undefined, "listDags should include list-b");
        assertTrue(b!.type === "test/list-b", "list-b should have its stored type");
    } finally {
        db.close();
    }
}

async function testReopenByHash() {
    const name = uniqueDbName("reopen");
    const dbA = await IdbDagDb.open(name, { hashSuite: sha256 });
    let hash: string;
    try {
        const { dag } = await dbA.getOrCreateDag("reopen-dag", { type: "test/reopen" });
        hash = await dag.append({ hello: 1 }, {});
    } finally {
        dbA.close();
    }

    const dbB = await IdbDagDb.open(name, { hashSuite: sha256 });
    try {
        const reopened = await dbB.openDag("reopen-dag");
        assertTrue(reopened !== undefined, "openDag should find the persisted DAG");
        const entry = await reopened!.loadEntry(hash);
        assertTrue(entry !== undefined, "entry appended before reopen should be loadable");
    } finally {
        dbB.close();
    }
}

async function testCloseRejectsFurtherUse() {
    const db = await IdbDagDb.open(uniqueDbName("close"), { hashSuite: sha256 });
    await db.getOrCreateDag("close-test", { type: "test/close" });
    db.close();

    let threw = false;
    try {
        await db.listDags();
    } catch {
        threw = true;
    }
    assertTrue(threw, "closed IdbDagDb should reject further use");
}

async function testConcurrentAppendsToOneDag() {
    const db = await IdbDagDb.open(uniqueDbName("concurrent"), { hashSuite: sha256 });
    try {
        const { dag } = await db.getOrCreateDag("concurrent-test", { type: "test/concurrent" });
        const hashes = await Promise.all(
            Array.from({ length: 8 }, (_value, index) => dag.append({ index }, {}))
        );

        for (const hash of hashes) {
            const entry = await dag.loadEntry(hash);
            assertTrue(entry !== undefined, "concurrent append entry should be loadable");
        }
    } finally {
        db.close();
    }
}

export const idbDagDbSuite = {
    title: "\n[IDB_DB] IdbDagDb Backend Tests\n",
    tests: [
        { name: "[IDB_DB_00] Default index is level", invoke: testDefaultIndexIsLevel },
        { name: "[IDB_DB_01] Explicit index mismatch throws", invoke: testExplicitIndexMismatchThrows },
        { name: "[IDB_DB_02] listDags returns metadata", invoke: testListDagsReturnsMetadata },
        { name: "[IDB_DB_03] reopen by hash after close", invoke: testReopenByHash },
        { name: "[IDB_DB_04] close rejects further use", invoke: testCloseRejectsFurtherUse },
        { name: "[IDB_DB_05] concurrent appends to one DAG are serialized", invoke: testConcurrentAppendsToOneDag },
    ],
};

// ---- Cross-tab simulation ------------------------------------------------
//
// Two IdbDagDb instances open the same underlying database (fake-indexeddb
// shares a single global factory within one process), approximating two browser
// tabs. Interleaving appends must produce dense, unique seq/topoIndex numbering
// and idempotent duplicate appends -- the flush-time counter path in IdbTx.

async function readCounterColumns(dbName: string, idxType: 'level' | 'topo'): Promise<{ seqs: number[]; topos: number[] }> {
    const db = await openDatabase(dbName, indexedDB);
    try {
        const entryTx = db.transaction(ENTRIES, 'readonly');
        const entryRecs = await reqToPromise<any[]>(entryTx.objectStore(ENTRIES).getAll());
        const seqs = entryRecs.map(r => r.seq as number);

        const idxStore = idxType === 'level' ? ENTRY_INFO : TOPO_INDEX;
        const idxTx = db.transaction(idxStore, 'readonly');
        const idxRecs = await reqToPromise<any[]>(idxTx.objectStore(idxStore).getAll());
        const topos = idxRecs.map(r => (idxType === 'level' ? r.topoIndex : r.topoOrder) as number);

        return { seqs, topos };
    } finally {
        db.close();
    }
}

function assertDense(values: number[], label: string): void {
    const sorted = [...values].sort((a, b) => a - b);
    for (let i = 0; i < sorted.length; i++) {
        assertEquals(sorted[i], i, `${label} should be dense and unique (index ${i})`);
    }
}

async function testCrossTabConcurrentDistinctAppends() {
    const name = uniqueDbName("xtab-distinct");
    const dbA = await IdbDagDb.open(name, { hashSuite: sha256 });
    const dbB = await IdbDagDb.open(name, { hashSuite: sha256 });
    const dagHash = "xtab-distinct-dag";

    try {
        const { dag: dagA } = await dbA.getOrCreateDag(dagHash, { type: "test/xtab", idxType: "level" });
        const { dag: dagB } = await dbB.getOrCreateDag(dagHash, { type: "test/xtab", idxType: "level" });

        const root = await dagA.append({ root: true }, {});

        // Concurrent, distinct children of root from both "tabs".
        const children = await Promise.all([
            dagA.append({ tab: 'a', n: 1 }, {}, position(root)),
            dagB.append({ tab: 'b', n: 2 }, {}, position(root)),
            dagA.append({ tab: 'a', n: 3 }, {}, position(root)),
            dagB.append({ tab: 'b', n: 4 }, {}, position(root)),
        ]);

        const uniqueChildren = new Set(children);
        assertEquals(uniqueChildren.size, 4, "the four children should be distinct entries");

        // Frontier (read from a fresh reopen) should be exactly the four children.
        const frontier = await dagA.getFrontier();
        assertEquals(frontier.size, 4, "frontier should hold the four concurrent children");
        for (const child of children) {
            assertTrue(frontier.has(child), "frontier should contain each child");
        }
        assertTrue(!frontier.has(root), "root should have been removed from the frontier");

        const { seqs, topos } = await readCounterColumns(name, 'level');
        assertEquals(seqs.length, 5, "there should be 5 entries total (root + 4)");
        assertDense(seqs, "entry seq");
        assertEquals(topos.length, 5, "there should be 5 indexed entries");
        assertDense(topos, "topoIndex");
    } finally {
        dbA.close();
        dbB.close();
    }
}

async function testCrossTabDuplicateAppendsAreIdempotent() {
    const name = uniqueDbName("xtab-dup");
    const dbA = await IdbDagDb.open(name, { hashSuite: sha256 });
    const dbB = await IdbDagDb.open(name, { hashSuite: sha256 });
    const dagHash = "xtab-dup-dag";

    try {
        const { dag: dagA } = await dbA.getOrCreateDag(dagHash, { type: "test/xtab", idxType: "level" });
        const { dag: dagB } = await dbB.getOrCreateDag(dagHash, { type: "test/xtab", idxType: "level" });

        // The same logical entry appended from both "tabs" concurrently.
        const [hA, hB] = await Promise.all([
            dagA.append({ same: 'entry' }, {}),
            dagB.append({ same: 'entry' }, {}),
        ]);
        assertEquals(hA, hB, "identical payload/position must produce the same content hash");

        // And appended once more, after the fact, from each side.
        await dagA.append({ same: 'entry' }, {});
        await dagB.append({ same: 'entry' }, {});

        const all: string[] = [];
        for await (const e of dagA.loadAllEntries()) all.push(e.hash);
        assertEquals(all.length, 1, "duplicate appends should collapse to a single entry");

        const frontier = await dagA.getFrontier();
        assertEquals(frontier.size, 1, "frontier should hold the single entry once");
        assertTrue(frontier.has(hA), "frontier should contain the entry");

        const { seqs, topos } = await readCounterColumns(name, 'level');
        assertEquals(seqs.length, 1, "only one entry record should exist");
        assertDense(seqs, "entry seq");
        assertDense(topos, "topoIndex");
    } finally {
        dbA.close();
        dbB.close();
    }
}

export const crossTabSuite = {
    title: "\n[IDB_XTAB] Cross-tab Simulation Tests\n",
    tests: [
        { name: "[IDB_XTAB_00] concurrent distinct appends: dense unique numbering", invoke: testCrossTabConcurrentDistinctAppends },
        { name: "[IDB_XTAB_01] duplicate appends are idempotent", invoke: testCrossTabDuplicateAppendsAreIdempotent },
    ],
};
