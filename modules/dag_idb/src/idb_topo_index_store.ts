import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { TopoIndexStore } from "@hyper-hyper-space/hhs3_dag/dist/idx/topo/topo_idx.js";

import { TOPO_INDEX, TOPO_PREDS } from "./idb_schema.js";
import { IdbEnv, IdbReader, IdbTx } from "./idb_env.js";

// IndexedDB port of dag_sql/src/sql_topo_index_store.ts.
//
// topoOrder is a dense per-dag counter, left provisional (-1) here and assigned
// inside the flush transaction. It is not read for the new node during its own
// append, so the provisional value is never observed.

export class IdbTopoIndexStore implements TopoIndexStore<IdbTx> {

    private env: IdbEnv;
    private dagId: number;

    constructor(env: IdbEnv, dagId: number) {
        this.env = env;
        this.dagId = dagId;
    }

    assignNextTopoIndex = async (node: B64Hash, tx: IdbTx): Promise<void> => {
        const existing = await tx.get(TOPO_INDEX, [this.dagId, node]);
        if (existing !== undefined) return;

        tx.putRecord(
            TOPO_INDEX,
            { dagId: this.dagId, hash: node, topoOrder: -1 },
            [this.dagId, node],
            { field: 'topoOrder', counter: 'topo' }
        );
    };

    getTopoIndex = async (node: B64Hash, ...tx: [tx: IdbTx] | []): Promise<number> => {
        const reader: IdbReader = tx[0] ?? this.env;
        const rec = await reader.get(TOPO_INDEX, [this.dagId, node]);
        if (rec === undefined) {
            throw new Error('Attempted to read topo index for ' + node + ' but it is not indexed.');
        }
        return rec.topoOrder as number;
    };

    addPred = async (node: B64Hash, pred: B64Hash, tx: IdbTx): Promise<void> => {
        tx.putRecord(
            TOPO_PREDS,
            { dagId: this.dagId, node, pred },
            [this.dagId, node, pred]
        );
    };

    getPreds = async (child: B64Hash, ...tx: [tx: IdbTx] | []): Promise<Set<B64Hash>> => {
        const reader: IdbReader = tx[0] ?? this.env;
        const recs = await reader.getAllByPrefix(TOPO_PREDS, 'by_node', [this.dagId, child]);
        return new Set(recs.map(r => r.pred as B64Hash));
    };
}
