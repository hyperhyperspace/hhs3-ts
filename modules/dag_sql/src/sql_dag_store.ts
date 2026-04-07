import { Hash } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { Entry, Header, Position } from "@hyper-hyper-space/hhs3_dag";
import { DagStore } from "@hyper-hyper-space/hhs3_dag/dist/store/dag_store.js";

import { SqlConnection } from "./sql_connection.js";

export class SqlDagStore implements DagStore<SqlConnection> {

    private conn: SqlConnection;
    private dagId: number;

    constructor(conn: SqlConnection, dagId: number) {
        this.conn = conn;
        this.dagId = dagId;
    }

    async withTransaction<T>(fn: (tx: SqlConnection) => Promise<T>): Promise<T> {
        return this.conn.transaction(fn);
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

    async loadEntry(h: Hash, ...tx: [tx: SqlConnection] | []): Promise<Entry | undefined> {
        const c = tx[0] ?? this.conn;
        const rows = await c.query(
            `SELECT hash, payload, meta, header FROM entries WHERE dag_id = ? AND hash = ?`,
            [this.dagId, h]
        );

        if (rows.length === 0) return undefined;

        const row = rows[0];
        return {
            hash: row.hash as Hash,
            payload: JSON.parse(row.payload as string),
            meta: JSON.parse(row.meta as string),
            header: JSON.parse(row.header as string),
        };
    }

    async loadHeader(h: Hash, ...tx: [tx: SqlConnection] | []): Promise<Header | undefined> {
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
        return new Set(rows.map(r => r.hash as Hash));
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
                        hash: row.hash as Hash,
                        payload: JSON.parse(row.payload as string),
                        meta: JSON.parse(row.meta as string),
                        header: JSON.parse(row.header as string),
                    };
                }
            }
        };
    }
}
