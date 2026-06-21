import * as os from "node:os";
import * as path from "node:path";
import * as fs from "node:fs";
import { dag } from "@hyper-hyper-space/hhs3_dag";
import { sha256 } from "@hyper-hyper-space/hhs3_crypto";
import { assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import { createBackendTestSuite, createParitySuite, createGrowthEventSuite } from "@hyper-hyper-space/hhs3_dag_test";
import { SqliteDagDb } from "../src/sqlite_dag_db.js";

function tmpDbPath(label: string): string {
    return path.join(os.tmpdir(), `hhs3-${label}-${Date.now()}-${Math.random().toString(36).slice(2)}.db`);
}

function cleanupDb(path: string): void {
    for (const suffix of ['', '-wal', '-shm']) {
        try { fs.unlinkSync(path + suffix); } catch (_e) { /* ignore */ }
    }
}

async function createSqliteDag(indexType: 'level' | 'topo'): Promise<dag.Dag> {
    const db = await SqliteDagDb.open(tmpDbPath("suite"), { hashSuite: sha256 });
    const result = await db.getOrCreateDag(
        "test-dag-" + Math.random(),
        { type: "test/dag", idxType: indexType }
    );
    return result.dag;
}

export const levelBackendSuite = createBackendTestSuite(
    "SQLITE_LEVEL",
    () => createSqliteDag('level')
);

export const topoBackendSuite = createBackendTestSuite(
    "SQLITE_TOPO",
    () => createSqliteDag('topo')
);

export const levelParitySuite = createParitySuite(
    "SQLITE_LEVEL_PAR",
    () => createSqliteDag('level')
);

export const topoParitySuite = createParitySuite(
    "SQLITE_TOPO_PAR",
    () => createSqliteDag('topo')
);

export const growthEventSuite = createGrowthEventSuite(
    "SQLITE_GROW",
    () => createSqliteDag('level')
);

async function testWalWatcherDetectsExternalWrite() {
    const tmpFile = tmpDbPath("watcher");

    const dbA = await SqliteDagDb.open(tmpFile, { hashSuite: sha256 });
    const dbB = await SqliteDagDb.open(tmpFile, { hashSuite: sha256 });

    try {
        const dagHash = "watcher-test-" + Math.random();
        const { dag: dagA } = await dbA.getOrCreateDag(dagHash, { type: "test/watcher", idxType: "level" });
        const { dag: dagB } = await dbB.getOrCreateDag(dagHash, { type: "test/watcher", idxType: "level" });

        // Seed dagA with an initial append so the WAL file definitely exists
        await dagA.append({ seed: true }, {});

        let listenerCalled = 0;
        const listener = () => { listenerCalled++; };
        dagA.addListener(listener);

        // Small delay so fs.watch has time to attach
        await new Promise(resolve => setTimeout(resolve, 100));

        await dagB.append({ external: true }, {});

        // Wait for fs.watch to deliver the event
        await new Promise(resolve => setTimeout(resolve, 500));

        dagA.removeListener(listener);

        assertTrue(listenerCalled >= 1,
            'WAL watcher on dag A should have detected the write from dag B (got ' + listenerCalled + ')');
    } finally {
        dbA.close();
        dbB.close();
        cleanupDb(tmpFile);
    }
}

async function testDefaultIndexIsLevel() {
    const tmpFile = tmpDbPath("default-index");
    const db = await SqliteDagDb.open(tmpFile, { hashSuite: sha256 });

    try {
        const dagHash = "default-index-test";
        const { created } = await db.getOrCreateDag(dagHash, { type: "test/default-index" });
        assertTrue(created, "first getOrCreateDag should create the DAG");

        let threw = false;
        try {
            await db.getOrCreateDag(dagHash, { type: "test/default-index", idxType: "topo" });
        } catch {
            threw = true;
        }
        assertTrue(threw, "default index should be level, so reopening as topo should throw");
    } finally {
        db.close();
        cleanupDb(tmpFile);
    }
}

async function testExplicitIndexMismatchThrows() {
    const tmpFile = tmpDbPath("idx-mismatch");
    const db = await SqliteDagDb.open(tmpFile, { hashSuite: sha256 });

    try {
        const dagHash = "idx-mismatch-test";
        await db.getOrCreateDag(dagHash, { type: "test/idx-mismatch", idxType: "topo" });

        let threw = false;
        try {
            await db.getOrCreateDag(dagHash, { type: "test/idx-mismatch", idxType: "level" });
        } catch {
            threw = true;
        }
        assertTrue(threw, "reopening an existing topo DAG as level should throw");
    } finally {
        db.close();
        cleanupDb(tmpFile);
    }
}

async function testListDagsReturnsMetadata() {
    const tmpFile = tmpDbPath("list");
    const db = await SqliteDagDb.open(tmpFile, { hashSuite: sha256 });

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
        cleanupDb(tmpFile);
    }
}

async function testCloseRejectsFurtherUse() {
    const tmpFile = tmpDbPath("close");
    const db = await SqliteDagDb.open(tmpFile, { hashSuite: sha256 });
    await db.getOrCreateDag("close-test", { type: "test/close" });
    db.close();

    let threw = false;
    try {
        await db.listDags();
    } catch {
        threw = true;
    } finally {
        cleanupDb(tmpFile);
    }
    assertTrue(threw, "closed SqliteDagDb should reject further use");
}

async function testConcurrentAppendsToOneDag() {
    const tmpFile = tmpDbPath("concurrent");
    const db = await SqliteDagDb.open(tmpFile, { hashSuite: sha256 });

    try {
        const { dag } = await db.getOrCreateDag("concurrent-test", { type: "test/concurrent" });
        const hashes = await Promise.all(
            Array.from({ length: 8 }, (_value, index) =>
                dag.append({ index }, {})
            )
        );

        for (const hash of hashes) {
            const entry = await dag.loadEntry(hash);
            assertTrue(entry !== undefined, "concurrent append entry should be loadable");
        }
    } finally {
        db.close();
        cleanupDb(tmpFile);
    }
}

export const walWatcherSuite = {
    title: "\n[SQLITE_WAL] WAL Watcher External Observer Tests\n",
    tests: [
        { name: "[SQLITE_WAL_00] WAL watcher detects external write", invoke: testWalWatcherDetectsExternalWrite },
    ],
};

export const sqliteDagDbSuite = {
    title: "\n[SQLITE_DB] SqliteDagDb Backend Tests\n",
    tests: [
        { name: "[SQLITE_DB_00] Default index is level", invoke: testDefaultIndexIsLevel },
        { name: "[SQLITE_DB_01] Explicit index mismatch throws", invoke: testExplicitIndexMismatchThrows },
        { name: "[SQLITE_DB_02] listDags returns metadata", invoke: testListDagsReturnsMetadata },
        { name: "[SQLITE_DB_03] close rejects further use", invoke: testCloseRejectsFurtherUse },
        { name: "[SQLITE_DB_04] concurrent appends to one DAG are serialized", invoke: testConcurrentAppendsToOneDag },
    ],
};
