import { dag } from "@hyper-hyper-space/hhs3_dag";
import { sha256 } from "@hyper-hyper-space/hhs3_crypto";

import { createBackendTestSuite, createParitySuite } from "@hyper-hyper-space/hhs3_dag_test";
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
