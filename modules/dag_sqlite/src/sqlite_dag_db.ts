import { dag, HashFn, Dag } from "@hyper-hyper-space/hhs3_dag";
import { createDagLevelIndex } from "@hyper-hyper-space/hhs3_dag/dist/idx/level/level_idx";
import { createDagTopoIndex } from "@hyper-hyper-space/hhs3_dag/dist/idx/topo/topo_idx";
import {
    SqlConnection,
    initSchema,
    checkSchemaVersion,
    getOrCreateDag,
    getDag,
    SqlDagStore,
    SqlLevelIndexStore,
    SqlTopoIndexStore,
    IdxType,
} from "@hyper-hyper-space/hhs3_dag_sql";

import { SqliteHandle, openSqliteConnection } from "./sqlite_connection";

export class SqliteDagDb {
    private handle: SqliteHandle;
    private dagCache: Map<string, Dag>;

    private constructor(handle: SqliteHandle) {
        this.handle = handle;
        this.dagCache = new Map();
    }

    static async open(path: string): Promise<SqliteDagDb> {
        const handle = openSqliteConnection(path);
        await initSchema(handle.conn);
        await checkSchemaVersion(handle.conn);
        return new SqliteDagDb(handle);
    }

    async createDag(dagHash: string, indexType: IdxType, hashFn: HashFn): Promise<Dag> {
        const cached = this.dagCache.get(dagHash);
        if (cached !== undefined) return cached;

        const dagId = await getOrCreateDag(this.handle.conn, dagHash, indexType);
        const d = this.buildDag(dagId, indexType, hashFn);
        this.dagCache.set(dagHash, d);
        return d;
    }

    async openDag(dagHash: string, hashFn: HashFn): Promise<Dag> {
        const cached = this.dagCache.get(dagHash);
        if (cached !== undefined) return cached;

        const info = await getDag(this.handle.conn, dagHash);
        if (info === undefined) {
            throw new Error('DAG "' + dagHash + '" does not exist — use createDag() first');
        }

        const d = this.buildDag(info.dagId, info.idxType, hashFn);
        this.dagCache.set(dagHash, d);
        return d;
    }

    close(): void {
        this.handle.close();
    }

    private buildDag(dagId: number, idxType: IdxType, hashFn: HashFn): Dag {
        const conn = this.handle.conn;
        const store = new SqlDagStore(conn, dagId);

        if (idxType === 'level') {
            const indexStore = new SqlLevelIndexStore(conn, dagId);
            const index = createDagLevelIndex<SqlConnection>(store, indexStore);
            return dag.create(store, index, hashFn);
        } else {
            const indexStore = new SqlTopoIndexStore(conn, dagId);
            const index = createDagTopoIndex<SqlConnection>(store, indexStore);
            return dag.create(store, index, hashFn);
        }
    }
}
