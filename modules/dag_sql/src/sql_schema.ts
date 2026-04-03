import { SqlConnection } from "./sql_connection";

const DDL = [
    `CREATE TABLE IF NOT EXISTS dags (
        dag_id INTEGER PRIMARY KEY AUTOINCREMENT,
        dag_hash TEXT NOT NULL UNIQUE
    )`,

    `CREATE TABLE IF NOT EXISTS entries (
        dag_id  INTEGER NOT NULL,
        hash    TEXT NOT NULL,
        payload TEXT NOT NULL,
        meta    TEXT NOT NULL,
        header  TEXT NOT NULL,
        PRIMARY KEY (dag_id, hash)
    )`,

    `CREATE TABLE IF NOT EXISTS frontier (
        dag_id  INTEGER NOT NULL,
        hash    TEXT NOT NULL,
        PRIMARY KEY (dag_id, hash)
    )`,

    `CREATE TABLE IF NOT EXISTS entry_info (
        dag_id           INTEGER NOT NULL,
        hash             TEXT NOT NULL,
        topo_index       INTEGER NOT NULL,
        level            INTEGER NOT NULL,
        distance_to_root INTEGER NOT NULL,
        PRIMARY KEY (dag_id, hash)
    )`,

    `CREATE TABLE IF NOT EXISTS level_preds (
        dag_id  INTEGER NOT NULL,
        level   INTEGER NOT NULL,
        node    TEXT NOT NULL,
        pred    TEXT NOT NULL,
        PRIMARY KEY (dag_id, level, node, pred)
    )`,

    `CREATE TABLE IF NOT EXISTS level_succs (
        dag_id  INTEGER NOT NULL,
        level   INTEGER NOT NULL,
        node    TEXT NOT NULL,
        succ    TEXT NOT NULL,
        PRIMARY KEY (dag_id, level, node, succ)
    )`,

    `CREATE TABLE IF NOT EXISTS topo_index (
        dag_id     INTEGER NOT NULL,
        hash       TEXT NOT NULL,
        topo_order INTEGER NOT NULL,
        PRIMARY KEY (dag_id, hash)
    )`,

    `CREATE TABLE IF NOT EXISTS topo_preds (
        dag_id  INTEGER NOT NULL,
        node    TEXT NOT NULL,
        pred    TEXT NOT NULL,
        PRIMARY KEY (dag_id, node, pred)
    )`,
];

export async function initSchema(conn: SqlConnection): Promise<void> {
    for (const stmt of DDL) {
        await conn.execute(stmt);
    }
}

export async function getOrCreateDag(conn: SqlConnection, dagHash: string): Promise<number> {
    await conn.execute(
        `INSERT OR IGNORE INTO dags (dag_hash) VALUES (?)`,
        [dagHash]
    );
    const rows = await conn.query(
        `SELECT dag_id FROM dags WHERE dag_hash = ?`,
        [dagHash]
    );
    return rows[0].dag_id as number;
}
