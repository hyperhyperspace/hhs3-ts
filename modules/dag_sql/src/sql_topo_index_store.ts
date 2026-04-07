import { Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { TopoIndexStore } from "@hyper-hyper-space/hhs3_dag/dist/idx/topo/topo_idx.js";

import { SqlConnection } from "./sql_connection.js";

export class SqlTopoIndexStore implements TopoIndexStore<SqlConnection> {

    private conn: SqlConnection;
    private dagId: number;

    constructor(conn: SqlConnection, dagId: number) {
        this.conn = conn;
        this.dagId = dagId;
    }

    assignNextTopoIndex = async (node: Hash, tx: SqlConnection): Promise<void> => {
        const c = tx;

        const existing = await c.query(
            `SELECT topo_order FROM topo_index WHERE dag_id = ? AND hash = ?`,
            [this.dagId, node]
        );

        if (existing.length > 0) return;

        const topoRows = await c.query(
            `SELECT COALESCE(MAX(topo_order), -1) + 1 AS next_topo FROM topo_index WHERE dag_id = ?`,
            [this.dagId]
        );
        const idx = topoRows[0].next_topo as number;

        await c.execute(
            `INSERT INTO topo_index (dag_id, hash, topo_order) VALUES (?, ?, ?)`,
            [this.dagId, node, idx]
        );
    };

    getTopoIndex = async (node: Hash, ...tx: [tx: SqlConnection] | []): Promise<number> => {
        const c = tx[0] ?? this.conn;
        const rows = await c.query(
            `SELECT topo_order FROM topo_index WHERE dag_id = ? AND hash = ?`,
            [this.dagId, node]
        );
        return rows[0].topo_order as number;
    };

    addPred = async (node: Hash, pred: Hash, tx: SqlConnection): Promise<void> => {
        const c = tx;
        await c.execute(
            `INSERT OR IGNORE INTO topo_preds (dag_id, node, pred) VALUES (?, ?, ?)`,
            [this.dagId, node, pred]
        );
    };

    getPreds = async (node: Hash, ...tx: [tx: SqlConnection] | []): Promise<Set<Hash>> => {
        const c = tx[0] ?? this.conn;
        const rows = await c.query(
            `SELECT pred FROM topo_preds WHERE dag_id = ? AND node = ?`,
            [this.dagId, node]
        );
        return new Set(rows.map(r => r.pred as Hash));
    };
}
