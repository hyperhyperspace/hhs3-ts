import * as os from "node:os";
import * as path from "node:path";
import * as fs from "node:fs";
import { dag } from "@hyper-hyper-space/hhs3_dag";
import { sha256 } from "@hyper-hyper-space/hhs3_crypto";
import { assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import { createBackendTestSuite, createParitySuite, createGrowthEventSuite } from "@hyper-hyper-space/hhs3_dag_test";
import { SqliteDagDb } from "../src/sqlite_dag_db.js";

async function createSqliteDag(indexType: 'level' | 'topo'): Promise<dag.Dag> {
    const db = await SqliteDagDb.open(":memory:");
    return db.createDag("test-dag-" + Math.random(), indexType, sha256);
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
    const tmpFile = path.join(os.tmpdir(), `hhs3-test-${Date.now()}-${Math.random().toString(36).slice(2)}.db`);

    const dbA = await SqliteDagDb.open(tmpFile);
    const dbB = await SqliteDagDb.open(tmpFile);

    try {
        const dagHash = "watcher-test-" + Math.random();
        const dagA = await dbA.createDag(dagHash, "level", sha256);
        const dagB = await dbB.createDag(dagHash, "level", sha256);

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
        for (const suffix of ['', '-wal', '-shm']) {
            try { fs.unlinkSync(tmpFile + suffix); } catch (_e) { /* ignore */ }
        }
    }
}

export const walWatcherSuite = {
    title: "\n[SQLITE_WAL] WAL Watcher External Observer Tests\n",
    tests: [
        { name: "[SQLITE_WAL_00] WAL watcher detects external write", invoke: testWalWatcherDetectsExternalWrite },
    ],
};
