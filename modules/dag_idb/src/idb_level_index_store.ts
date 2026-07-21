import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { Position } from "@hyper-hyper-space/hhs3_dag";
import type { EntryInfo, LevelIndexStore } from "@hyper-hyper-space/hhs3_dag/dist/idx/level/level_idx.js";

import { ENTRY_INFO, LEVEL_PREDS, LEVEL_SUCCS } from "./idb_schema.js";
import { IdbEnv, IdbReader, IdbTx } from "./idb_env.js";

// IndexedDB port of dag_sql/src/sql_level_index_store.ts.
//
// level and distanceToARoot depend only on predecessors, which are immutable
// (grow-only, content-addressed), so they are computed eagerly during the unit
// of work. topoIndex is a dense per-dag counter, so it is left provisional (-1)
// here and assigned inside the flush transaction (see IdbEnv/IdbTx). Nothing in
// the index-building algorithms reads the new node's topoIndex during its own
// append, so the provisional value is never observed.

export class IdbLevelIndexStore implements LevelIndexStore<IdbTx> {

    private env: IdbEnv;
    private dagId: number;
    private levelFactor: number;

    constructor(env: IdbEnv, dagId: number, opts?: { levelFactor?: number }) {
        this.env = env;
        this.dagId = dagId;
        this.levelFactor = opts?.levelFactor ?? 64;
    }

    assignEntryInfo = async (node: B64Hash, after: Position, tx: IdbTx): Promise<EntryInfo> => {
        const existing = await tx.get(ENTRY_INFO, [this.dagId, node]);
        if (existing !== undefined) {
            return {
                topoIndex: existing.topoIndex as number,
                level: existing.level as number,
                distanceToARoot: existing.distanceToARoot as number,
            };
        }

        let distanceToARoot = 0;
        for (const pred of (after ?? [])) {
            const predRec = await tx.get(ENTRY_INFO, [this.dagId, pred]);
            if (predRec === undefined) {
                throw new Error('Attempted to index entry ' + node + ' but its predecessor ' + pred + ' is not indexed.');
            }
            const newHeight = (predRec.distanceToARoot as number) + 1;
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

        tx.putRecord(
            ENTRY_INFO,
            { dagId: this.dagId, hash: node, topoIndex: -1, level, distanceToARoot },
            [this.dagId, node],
            { field: 'topoIndex', counter: 'topo' }
        );

        return { topoIndex: -1, level, distanceToARoot };
    };

    getEntryInfo = async (node: B64Hash, ...tx: [tx: IdbTx] | []): Promise<EntryInfo> => {
        const reader: IdbReader = tx[0] ?? this.env;
        const rec = await reader.get(ENTRY_INFO, [this.dagId, node]);
        if (rec === undefined) {
            throw new Error('Attempted to read entry info for ' + node + ' but it is not indexed.');
        }
        return {
            topoIndex: rec.topoIndex as number,
            level: rec.level as number,
            distanceToARoot: rec.distanceToARoot as number,
        };
    };

    addPred = async (level: number, node: B64Hash, pred: B64Hash, tx: IdbTx): Promise<void> => {
        tx.putRecord(
            LEVEL_PREDS,
            { dagId: this.dagId, level, node, pred },
            [this.dagId, level, node, pred]
        );
    };

    getPreds = async (level: number, node: B64Hash, ...tx: [tx: IdbTx] | []): Promise<Set<B64Hash>> => {
        const reader: IdbReader = tx[0] ?? this.env;
        const recs = await reader.getAllByPrefix(LEVEL_PREDS, 'by_node', [this.dagId, level, node]);
        return new Set(recs.map(r => r.pred as B64Hash));
    };

    addSucc = async (level: number, node: B64Hash, succ: B64Hash, tx: IdbTx): Promise<void> => {
        tx.putRecord(
            LEVEL_SUCCS,
            { dagId: this.dagId, level, node, succ },
            [this.dagId, level, node, succ]
        );
    };

    getSuccs = async (level: number, node: B64Hash, ...tx: [tx: IdbTx] | []): Promise<Set<B64Hash>> => {
        const reader: IdbReader = tx[0] ?? this.env;
        const recs = await reader.getAllByPrefix(LEVEL_SUCCS, 'by_node', [this.dagId, level, node]);
        return new Set(recs.map(r => r.succ as B64Hash));
    };
}
