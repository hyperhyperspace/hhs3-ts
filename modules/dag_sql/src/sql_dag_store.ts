import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { Entry, Header, Position } from "@hyper-hyper-space/hhs3_dag";
import { DagGrowthListener, DagStore, TxResult } from "@hyper-hyper-space/hhs3_dag/dist/store/dag_store.js";

import { SqlConnection } from "./sql_connection.js";

export abstract class SqlDagStore implements DagStore<SqlConnection> {

    protected conn: SqlConnection;
    protected dagId: number;

    private listeners = new Set<DagGrowthListener>();
    private externalHandle: unknown = undefined;

    constructor(conn: SqlConnection, dagId: number) {
        this.conn = conn;
        this.dagId = dagId;
    }

    async withTransaction<T extends TxResult>(fn: (tx: SqlConnection) => Promise<T>): Promise<T> {
        const result = await this.conn.transaction(fn);
        if (result.fireListeners) this.fireListeners();
        return result;
    }

    async append(entry: Entry, tx: SqlConnection): Promise<void> {
        const c = tx;
        const { hash, header, payload, meta } = entry;

        await c.execute(
            `INSERT OR IGNORE INTO entries (dag_id, hash, payload, meta, header) VALUES (?, ?, ?, ?, ?)`,
            [this.dagId, hash, JSON.stringify(payload), JSON.stringify(meta), JSON.stringify(header)]
        );

        for (const prevHash of json.fromSet(header.prevEntryHashes)) {
            await c.execute(
                `DELETE FROM frontier WHERE dag_id = ? AND hash = ?`,
                [this.dagId, prevHash]
            );
        }

        await c.execute(
            `INSERT OR IGNORE INTO frontier (dag_id, hash) VALUES (?, ?)`,
            [this.dagId, hash]
        );
    }

    async loadEntry(h: B64Hash, ...tx: [tx: SqlConnection] | []): Promise<Entry | undefined> {
        const c = tx[0] ?? this.conn;
        const rows = await c.query(
            `SELECT hash, payload, meta, header FROM entries WHERE dag_id = ? AND hash = ?`,
            [this.dagId, h]
        );

        if (rows.length === 0) return undefined;

        const row = rows[0];
        return {
            hash: row.hash as B64Hash,
            payload: JSON.parse(row.payload as string),
            meta: JSON.parse(row.meta as string),
            header: JSON.parse(row.header as string),
        };
    }

    async loadHeader(h: B64Hash, ...tx: [tx: SqlConnection] | []): Promise<Header | undefined> {
        const c = tx[0] ?? this.conn;
        const rows = await c.query(
            `SELECT header FROM entries WHERE dag_id = ? AND hash = ?`,
            [this.dagId, h]
        );

        if (rows.length === 0) return undefined;

        return JSON.parse(rows[0].header as string);
    }

    async getFrontier(...tx: [tx: SqlConnection] | []): Promise<Position> {
        const c = tx[0] ?? this.conn;
        const rows = await c.query(
            `SELECT hash FROM frontier WHERE dag_id = ?`,
            [this.dagId]
        );
        return new Set(rows.map(r => r.hash as B64Hash));
    }

    loadAllEntries(...tx: [tx: SqlConnection] | []): AsyncIterable<Entry> {
        const c = tx[0] ?? this.conn;
        const dagId = this.dagId;

        return {
            async *[Symbol.asyncIterator]() {
                const rows = await c.query(
                    `SELECT hash, payload, meta, header FROM entries WHERE dag_id = ? ORDER BY rowid`,
                    [dagId]
                );
                for (const row of rows) {
                    yield {
                        hash: row.hash as B64Hash,
                        payload: JSON.parse(row.payload as string),
                        meta: JSON.parse(row.meta as string),
                        header: JSON.parse(row.header as string),
                    };
                }
            }
        };
    }

    addListener(listener: DagGrowthListener): void {
        const wasEmpty = this.listeners.size === 0;
        this.listeners.add(listener);
        if (wasEmpty && this.listeners.size === 1) {
            this.externalHandle = this.startExternalObserver(() => this.fireListeners());
        }
    }

    removeListener(listener: DagGrowthListener): void {
        this.listeners.delete(listener);
        if (this.listeners.size === 0 && this.externalHandle !== undefined) {
            this.stopExternalObserver(this.externalHandle);
            this.externalHandle = undefined;
        }
    }

    private fireListeners(): void {
        for (const cb of this.listeners) {
            try { cb(); } catch (_e) { /* keep firing even if a listener throws */ }
        }
    }

    // Subclasses implement these to plug in an external observation strategy
    // (e.g. polling, fs.watch on the WAL file, BroadcastChannel...). They are
    // started lazily when the first listener subscribes and stopped when the
    // last listener unsubscribes. `notify` should be called whenever the
    // subclass detects a potential change in the underlying store; it is safe
    // to over-notify, per the at-least-once contract on DagStore.
    protected abstract startExternalObserver(notify: () => void): unknown;
    protected abstract stopExternalObserver(handle: unknown): void;
}
