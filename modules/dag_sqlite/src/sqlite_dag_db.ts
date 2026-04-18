import { dag, HashSuite, Dag } from "@hyper-hyper-space/hhs3_dag";
import { createDagLevelIndex } from "@hyper-hyper-space/hhs3_dag/dist/idx/level/level_idx.js";
import { createDagTopoIndex } from "@hyper-hyper-space/hhs3_dag/dist/idx/topo/topo_idx.js";
import {
    SqlConnection,
    initSchema,
    checkSchemaVersion,
    getOrCreateDag,
    getDag,
    SqlLevelIndexStore,
    SqlTopoIndexStore,
    IdxType,
} from "@hyper-hyper-space/hhs3_dag_sql";

import { SqliteHandle, openSqliteConnection } from "./sqlite_connection.js";
import { WatcherSqliteDagStore } from "./watcher_sqlite_dag_store.js";

export class SqliteDagDb {
    private handle: SqliteHandle;
    private path: string;
    private dagCache: Map<string, Dag>;

    private constructor(handle: SqliteHandle, path: string) {
        this.handle = handle;
        this.path = path;
        this.dagCache = new Map();
    }

    static async open(path: string): Promise<SqliteDagDb> {
        const handle = openSqliteConnection(path);
        await initSchema(handle.conn);
        await checkSchemaVersion(handle.conn);
        return new SqliteDagDb(handle, path);
    }

    async createDag(dagHash: string, indexType: IdxType, hash: HashSuite): Promise<Dag> {
        const cached = this.dagCache.get(dagHash);
        if (cached !== undefined) return cached;

        const dagId = await getOrCreateDag(this.handle.conn, dagHash, indexType);
        const d = this.buildDag(dagId, indexType, hash);
        this.dagCache.set(dagHash, d);
        return d;
    }

    async openDag(dagHash: string, hash: HashSuite): Promise<Dag> {
        const cached = this.dagCache.get(dagHash);
        if (cached !== undefined) return cached;

        const info = await getDag(this.handle.conn, dagHash);
        if (info === undefined) {
            throw new Error('DAG "' + dagHash + '" does not exist — use createDag() first');
        }

        const d = this.buildDag(info.dagId, info.idxType, hash);
        this.dagCache.set(dagHash, d);
        return d;
    }

    close(): void {
        this.handle.close();
    }

    private buildDag(dagId: number, idxType: IdxType, hash: HashSuite): Dag {
        const conn = this.handle.conn;
        const store = new WatcherSqliteDagStore(conn, dagId, this.path);

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
}
