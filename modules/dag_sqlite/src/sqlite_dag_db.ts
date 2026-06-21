import { dag, HashSuite, Dag } from "@hyper-hyper-space/hhs3_dag";
import { createDagLevelIndex } from "@hyper-hyper-space/hhs3_dag/dist/idx/level/level_idx.js";
import { createDagTopoIndex } from "@hyper-hyper-space/hhs3_dag/dist/idx/topo/topo_idx.js";
import {
    SqlConnection,
    initSchema,
    checkSchemaVersion,
    getOrCreateDag as getOrCreateSqlDag,
    getDag,
    SqlLevelIndexStore,
    SqlTopoIndexStore,
    IdxType,
} from "@hyper-hyper-space/hhs3_dag_sql";

import { SqliteHandle, openSqliteConnection } from "./sqlite_connection.js";
import { WatcherSqliteDagStore } from "./watcher_sqlite_dag_store.js";

export type SqliteDagDbOptions = {
    hashSuite: HashSuite;
    defaultIdxType?: IdxType;
};

export type SqliteDagMeta = {
    type: string;
    idxType?: IdxType;
};

export type SqliteDagEntry = {
    id: string;
    type: string;
    createdAt: number;
};

type CachedDag = {
    dag: Dag;
    handle: SqliteHandle;
    entry: SqliteDagEntry;
    idxType: IdxType;
};

type DagOpenInfo = {
    dagId: number;
    idxType: IdxType;
    entry: SqliteDagEntry;
};

const DEFAULT_IDX_TYPE: IdxType = 'level';

export class SqliteDagDb {
    private control: SqliteHandle;
    private path: string;
    private hash: HashSuite;
    private defaultIdxType: IdxType;
    private dagCache: Map<string, CachedDag>;
    private openingDags: Map<string, Promise<CachedDag>>;
    private closed = false;

    private constructor(control: SqliteHandle, path: string, opts: SqliteDagDbOptions) {
        this.control = control;
        this.path = path;
        this.hash = opts.hashSuite;
        this.defaultIdxType = opts.defaultIdxType ?? DEFAULT_IDX_TYPE;
        this.dagCache = new Map();
        this.openingDags = new Map();
    }

    static async open(path: string, opts: SqliteDagDbOptions): Promise<SqliteDagDb> {
        const control = openSqliteConnection(path);
        try {
            await initSchema(control.conn);
            await checkSchemaVersion(control.conn);
            await ensureBackendSchema(control.conn);
            return new SqliteDagDb(control, path, opts);
        } catch (e) {
            control.close();
            throw e;
        }
    }

    async getOrCreateDag(id: string, meta: SqliteDagMeta): Promise<{ dag: Dag; created: boolean }> {
        this.ensureOpen();

        const cached = this.dagCache.get(id);
        if (cached !== undefined) {
            this.validateCachedDag(id, cached, meta);
            return { dag: cached.dag, created: false };
        }

        const result = await this.control.conn.transaction(async tx => {
            const existing = await getDag(tx, id);

            if (existing !== undefined) {
                if (meta.idxType !== undefined && meta.idxType !== existing.idxType) {
                    throw new Error('DAG "' + id + '" already exists with idx_type "' + existing.idxType + '", cannot open with "' + meta.idxType + '"');
                }

                const entry = await this.getOrRepairMeta(tx, id, meta.type);
                return {
                    dagId: existing.dagId,
                    idxType: existing.idxType,
                    entry,
                    created: false,
                };
            }

            const idxType = meta.idxType ?? this.defaultIdxType;
            const dagId = await getOrCreateSqlDag(tx, id, idxType);
            const entry = {
                id,
                type: meta.type,
                createdAt: Date.now(),
            };
            await tx.execute(
                `INSERT INTO dag_object_meta (dag_hash, object_type, created_at) VALUES (?, ?, ?)`,
                [entry.id, entry.type, entry.createdAt]
            );
            return {
                dagId,
                idxType,
                entry,
                created: true,
            };
        });

        const cachedDag = await this.openCachedDag(id, result);
        return { dag: cachedDag.dag, created: result.created };
    }

    async openDag(id: string): Promise<Dag | undefined> {
        this.ensureOpen();

        const cached = this.dagCache.get(id);
        if (cached !== undefined) return cached.dag;

        const info = await getDag(this.control.conn, id);
        if (info === undefined) return undefined;

        const entry = await this.getMeta(id) ?? { id, type: '', createdAt: 0 };
        const cachedDag = await this.openCachedDag(id, {
            dagId: info.dagId,
            idxType: info.idxType,
            entry,
        });
        return cachedDag.dag;
    }

    async listDags(): Promise<SqliteDagEntry[]> {
        this.ensureOpen();

        const rows = await this.control.conn.query(
            `SELECT d.dag_hash, m.object_type, m.created_at
               FROM dags d
               JOIN dag_object_meta m ON m.dag_hash = d.dag_hash
              ORDER BY m.created_at ASC`
        );
        return rows.map(row => ({
            id: row.dag_hash as string,
            type: row.object_type as string,
            createdAt: row.created_at as number,
        }));
    }

    close(): void {
        if (this.closed) return;
        this.closed = true;

        for (const cached of this.dagCache.values()) {
            cached.handle.close();
        }
        this.dagCache.clear();
        this.openingDags.clear();
        this.control.close();
    }

    private async getOrRepairMeta(tx: SqlConnection, id: string, type: string): Promise<SqliteDagEntry> {
        const rows = await tx.query(
            `SELECT object_type, created_at FROM dag_object_meta WHERE dag_hash = ?`,
            [id]
        );

        if (rows.length > 0) {
            const storedType = rows[0].object_type as string;
            if (storedType !== type) {
                throw new Error('DAG "' + id + '" already exists with type "' + storedType + '", cannot open with "' + type + '"');
            }
            return {
                id,
                type: storedType,
                createdAt: rows[0].created_at as number,
            };
        }

        const entry = {
            id,
            type,
            createdAt: Date.now(),
        };
        await tx.execute(
            `INSERT OR IGNORE INTO dag_object_meta (dag_hash, object_type, created_at) VALUES (?, ?, ?)`,
            [entry.id, entry.type, entry.createdAt]
        );
        return entry;
    }

    private async getMeta(id: string): Promise<SqliteDagEntry | undefined> {
        const rows = await this.control.conn.query(
            `SELECT object_type, created_at FROM dag_object_meta WHERE dag_hash = ?`,
            [id]
        );

        if (rows.length === 0) return undefined;
        return {
            id,
            type: rows[0].object_type as string,
            createdAt: rows[0].created_at as number,
        };
    }

    private async openCachedDag(id: string, info: DagOpenInfo): Promise<CachedDag> {
        const cached = this.dagCache.get(id);
        if (cached !== undefined) return cached;

        const opening = this.openingDags.get(id);
        if (opening !== undefined) return opening;

        const task = this.openDagHandle(id, info);
        this.openingDags.set(id, task);
        try {
            return await task;
        } finally {
            if (this.openingDags.get(id) === task) {
                this.openingDags.delete(id);
            }
        }
    }

    private async openDagHandle(id: string, info: DagOpenInfo): Promise<CachedDag> {
        const handle = openSqliteConnection(this.path);
        try {
            await checkSchemaVersion(handle.conn);
            const cached = {
                dag: buildSqliteDag(handle.conn, this.path, info.dagId, info.idxType, this.hash),
                handle,
                entry: info.entry,
                idxType: info.idxType,
            };
            this.dagCache.set(id, cached);
            return cached;
        } catch (e) {
            handle.close();
            throw e;
        }
    }

    private validateCachedDag(id: string, cached: CachedDag, meta: SqliteDagMeta): void {
        if (meta.idxType !== undefined && meta.idxType !== cached.idxType) {
            throw new Error('DAG "' + id + '" already exists with idx_type "' + cached.idxType + '", cannot open with "' + meta.idxType + '"');
        }
        if (meta.type !== cached.entry.type) {
            throw new Error('DAG "' + id + '" already exists with type "' + cached.entry.type + '", cannot open with "' + meta.type + '"');
        }
    }

    private ensureOpen(): void {
        if (this.closed) {
            throw new Error('SqliteDagDb is closed');
        }
    }
}

async function ensureBackendSchema(conn: SqlConnection): Promise<void> {
    await conn.execute(
        `CREATE TABLE IF NOT EXISTS dag_object_meta (
            dag_hash    TEXT PRIMARY KEY,
            object_type TEXT NOT NULL,
            created_at  INTEGER NOT NULL
        )`
    );
}

function buildSqliteDag(conn: SqlConnection, dbPath: string, dagId: number, idxType: IdxType, hash: HashSuite): Dag {
    const store = new WatcherSqliteDagStore(conn, dagId, dbPath);

    if (idxType === 'level') {
        const indexStore = new SqlLevelIndexStore(conn, dagId);
        const index = createDagLevelIndex<SqlConnection>(store, indexStore);
        return dag.create(store, index, hash);
    } else {
        const indexStore = new SqlTopoIndexStore(conn, dagId);
        const index = createDagTopoIndex<SqlConnection>(store, indexStore);
        return dag.create(store, index, hash);
    }
}
