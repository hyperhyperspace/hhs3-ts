import { B64Hash, HashSuite } from "@hyper-hyper-space/hhs3_crypto";
import { dag, Dag } from "@hyper-hyper-space/hhs3_dag";
import { createDagLevelIndex } from "@hyper-hyper-space/hhs3_dag/dist/idx/level/level_idx.js";
import { createDagTopoIndex } from "@hyper-hyper-space/hhs3_dag/dist/idx/topo/topo_idx.js";
import {
    checkSchemaVersion,
    getDag,
    getOrCreateDag,
    IdxType,
    initSchema,
    SqlConnection,
    SqlLevelIndexStore,
    SqlTopoIndexStore,
} from "@hyper-hyper-space/hhs3_dag_sql";
import { openSqliteConnection, SqliteHandle, WatcherSqliteDagStore } from "@hyper-hyper-space/hhs3_dag_sqlite";
import { json } from "@hyper-hyper-space/hhs3_json";
import { extractCreatePayloadType } from "@hyper-hyper-space/hhs3_mvt";
import type { DagBackend, DagEntry } from "@hyper-hyper-space/hhs3_replica";

export type SqliteReplicaDagBackendOptions = {
    path: string;
    hashSuite: HashSuite;
    indexType?: IdxType;
};

export class SqliteReplicaDagBackend implements DagBackend {
    private readonly handle: SqliteHandle;
    private readonly path: string;
    private readonly hashSuite: HashSuite;
    private readonly indexType: IdxType;
    private readonly dagCache = new Map<B64Hash, Dag>();

    private constructor(path: string, handle: SqliteHandle, hashSuite: HashSuite, indexType: IdxType) {
        this.path = path;
        this.handle = handle;
        this.hashSuite = hashSuite;
        this.indexType = indexType;
    }

    static async open(options: SqliteReplicaDagBackendOptions): Promise<SqliteReplicaDagBackend> {
        const handle = openSqliteConnection(options.path);
        await initSchema(handle.conn);
        await checkSchemaVersion(handle.conn);
        return new SqliteReplicaDagBackend(options.path, handle, options.hashSuite, options.indexType ?? 'topo');
    }

    async getOrCreateDag(id: B64Hash, _meta: { type: string }): Promise<{ dag: Dag; created: boolean }> {
        const existing = await getDag(this.handle.conn, id);
        const dag = await this.createDag(id);
        return { dag, created: existing === undefined };
    }

    async openDag(id: B64Hash): Promise<Dag | undefined> {
        const cached = this.dagCache.get(id);
        if (cached !== undefined) return cached;

        const info = await getDag(this.handle.conn, id);
        if (info === undefined) return undefined;

        const dag = this.buildDag(info.dagId, info.idxType);
        this.dagCache.set(id, dag);
        return dag;
    }

    async listDags(): Promise<DagEntry[]> {
        const rows = await this.handle.conn.query(
            `SELECT d.dag_id AS dagId, d.dag_hash AS dagHash, e.payload AS payload
             FROM dags d
             JOIN entries e ON e.dag_id = d.dag_id AND e.hash = d.dag_hash
             ORDER BY d.dag_id ASC`
        );

        const entries: DagEntry[] = [];
        for (const row of rows) {
            const payloadText = row.payload;
            if (typeof payloadText !== 'string') continue;
            const payload = JSON.parse(payloadText) as json.Literal;
            const type = extractCreatePayloadType(payload);
            if (type === undefined) continue;
            entries.push({
                id: row.dagHash as B64Hash,
                type,
                createdAt: row.dagId as number,
            });
        }
        return entries;
    }

    close(): void {
        this.handle.close();
    }

    private async createDag(id: B64Hash): Promise<Dag> {
        const cached = this.dagCache.get(id);
        if (cached !== undefined) return cached;

        const dagId = await getOrCreateDag(this.handle.conn, id, this.indexType);
        const created = this.buildDag(dagId, this.indexType);
        this.dagCache.set(id, created);
        return created;
    }

    private buildDag(dagId: number, indexType: IdxType): Dag {
        const store = new WatcherSqliteDagStore(this.handle.conn, dagId, this.path);
        if (indexType === 'level') {
            const indexStore = new SqlLevelIndexStore(this.handle.conn, dagId);
            return dag.create(store, createDagLevelIndex<SqlConnection>(store, indexStore), this.hashSuite);
        }
        const indexStore = new SqlTopoIndexStore(this.handle.conn, dagId);
        return dag.create(store, createDagTopoIndex<SqlConnection>(store, indexStore), this.hashSuite);
    }
}
