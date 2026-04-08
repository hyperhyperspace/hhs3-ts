import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { Position } from "@hyper-hyper-space/hhs3_dag";
import type { EntryInfo, LevelIndexStore } from "@hyper-hyper-space/hhs3_dag/dist/idx/level/level_idx.js";

import { SqlConnection } from "./sql_connection.js";

export class SqlLevelIndexStore implements LevelIndexStore<SqlConnection> {

    private conn: SqlConnection;
    private dagId: number;
    private levelFactor: number;

    constructor(conn: SqlConnection, dagId: number, opts?: { levelFactor?: number }) {
        this.conn = conn;
        this.dagId = dagId;
        this.levelFactor = opts?.levelFactor ?? 64;
    }

    assignEntryInfo = async (node: B64Hash, after: Position, tx: SqlConnection): Promise<EntryInfo> => {
        const c = tx;

        const existing = await c.query(
            `SELECT topo_index, level, distance_to_root FROM entry_info WHERE dag_id = ? AND hash = ?`,
            [this.dagId, node]
        );

        if (existing.length > 0) {
            const row = existing[0];
            return {
                topoIndex: row.topo_index as number,
                level: row.level as number,
                distanceToARoot: row.distance_to_root as number,
            };
        }

        const topoRows = await c.query(
            `SELECT COALESCE(MAX(topo_index), -1) + 1 AS next_topo FROM entry_info WHERE dag_id = ?`,
            [this.dagId]
        );
        const topoIndex = topoRows[0].next_topo as number;

        let distanceToARoot = 0;
        for (const pred of (after ?? [])) {
            const predRows = await c.query(
                `SELECT distance_to_root FROM entry_info WHERE dag_id = ? AND hash = ?`,
                [this.dagId, pred]
            );

            if (predRows.length === 0) {
                throw new Error('Attempted to index entry ' + node + ' but its predecessor ' + pred + ' is not indexed.');
            }

            const predDistance = predRows[0].distance_to_root as number;
            const newHeight = predDistance + 1;

            if (distanceToARoot === 0 || newHeight < distanceToARoot) {
                distanceToARoot = newHeight;
            }
        }

        let level = 0;
        if (distanceToARoot === 0) {
            level = Number.MAX_SAFE_INTEGER;
        } else {
            let i = distanceToARoot;
            while (i > 1 && i % this.levelFactor === 0) {
                level++;
                i = i / this.levelFactor;
            }
        }

        await c.execute(
            `INSERT INTO entry_info (dag_id, hash, topo_index, level, distance_to_root) VALUES (?, ?, ?, ?, ?)`,
            [this.dagId, node, topoIndex, level, distanceToARoot]
        );

        return { topoIndex, level, distanceToARoot };
    };

    getEntryInfo = async (node: B64Hash, ...tx: [tx: SqlConnection] | []): Promise<EntryInfo> => {
        const c = tx[0] ?? this.conn;
        const rows = await c.query(
            `SELECT topo_index, level, distance_to_root FROM entry_info WHERE dag_id = ? AND hash = ?`,
            [this.dagId, node]
        );

        const row = rows[0];
        return {
            topoIndex: row.topo_index as number,
            level: row.level as number,
            distanceToARoot: row.distance_to_root as number,
        };
    };

    addPred = async (level: number, node: B64Hash, pred: B64Hash, tx: SqlConnection): Promise<void> => {
        const c = tx;
        await c.execute(
            `INSERT OR IGNORE INTO level_preds (dag_id, level, node, pred) VALUES (?, ?, ?, ?)`,
            [this.dagId, level, node, pred]
        );
    };

    getPreds = async (level: number, node: B64Hash, ...tx: [tx: SqlConnection] | []): Promise<Set<B64Hash>> => {
        const c = tx[0] ?? this.conn;
        const rows = await c.query(
            `SELECT pred FROM level_preds WHERE dag_id = ? AND level = ? AND node = ?`,
            [this.dagId, level, node]
        );
        return new Set(rows.map(r => r.pred as B64Hash));
    };

    addSucc = async (level: number, node: B64Hash, succ: B64Hash, tx: SqlConnection): Promise<void> => {
        const c = tx;
        await c.execute(
            `INSERT OR IGNORE INTO level_succs (dag_id, level, node, succ) VALUES (?, ?, ?, ?)`,
            [this.dagId, level, node, succ]
        );
    };

    getSuccs = async (level: number, node: B64Hash, ...tx: [tx: SqlConnection] | []): Promise<Set<B64Hash>> => {
        const c = tx[0] ?? this.conn;
        const rows = await c.query(
            `SELECT succ FROM level_succs WHERE dag_id = ? AND level = ? AND node = ?`,
            [this.dagId, level, node]
        );
        return new Set(rows.map(r => r.succ as B64Hash));
    };
}
