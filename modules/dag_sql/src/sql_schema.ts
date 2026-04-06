import { SqlConnection } from "./sql_connection";

export const SCHEMA_VERSION = 1;

export type IdxType = 'level' | 'topo';

const DDL = [
    `CREATE TABLE IF NOT EXISTS schema_version (
        version INTEGER NOT NULL
    )`,

    `CREATE TABLE IF NOT EXISTS dags (
        dag_id   INTEGER PRIMARY KEY AUTOINCREMENT,
        dag_hash TEXT NOT NULL UNIQUE,
        idx_type TEXT NOT NULL
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

    const rows = await conn.query(`SELECT version FROM schema_version`);
    if (rows.length === 0) {
        await conn.execute(`INSERT INTO schema_version (version) VALUES (?)`, [SCHEMA_VERSION]);
    }
}

export async function checkSchemaVersion(conn: SqlConnection): Promise<void> {
    const rows = await conn.query(`SELECT version FROM schema_version`);
    if (rows.length === 0) {
        throw new Error('schema_version table is empty — database may be corrupt');
    }
    const version = rows[0].version as number;
    if (version > SCHEMA_VERSION) {
        throw new Error('database is newer than this software (db version ' + version + ', code version ' + SCHEMA_VERSION + ')');
    }
    if (version < SCHEMA_VERSION) {
        throw new Error('database needs migration (db version ' + version + ', code version ' + SCHEMA_VERSION + ')');
    }
}

export async function getOrCreateDag(conn: SqlConnection, dagHash: string, idxType: IdxType): Promise<number> {
    await conn.execute(
        `INSERT OR IGNORE INTO dags (dag_hash, idx_type) VALUES (?, ?)`,
        [dagHash, idxType]
    );
    const rows = await conn.query(
        `SELECT dag_id, idx_type FROM dags WHERE dag_hash = ?`,
        [dagHash]
    );
    const storedType = rows[0].idx_type as string;
    if (storedType !== idxType) {
        throw new Error('DAG "' + dagHash + '" already exists with idx_type "' + storedType + '", cannot open with "' + idxType + '"');
    }
    return rows[0].dag_id as number;
}

export async function getDag(conn: SqlConnection, dagHash: string): Promise<{ dagId: number; idxType: IdxType } | undefined> {
    const rows = await conn.query(
        `SELECT dag_id, idx_type FROM dags WHERE dag_hash = ?`,
        [dagHash]
    );
    if (rows.length === 0) return undefined;
    return { dagId: rows[0].dag_id as number, idxType: rows[0].idx_type as IdxType };
}
