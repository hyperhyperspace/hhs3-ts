import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { MultiMap } from "@hyper-hyper-space/hhs3_util";
import { TopoIndexStore } from "./topo_idx.js";


export class MemTopoIndexStore implements TopoIndexStore {

    nextTopoIndex: number = 0;
    topoIndex: Map<B64Hash, number> = new Map();

    pred: MultiMap<B64Hash, B64Hash> = new MultiMap();

    assignNextTopoIndex = async (node: B64Hash) => {
        if (!this.topoIndex.has(node)) {
            const i = this.nextTopoIndex;
            this.nextTopoIndex = this.nextTopoIndex + 1;
            this.topoIndex.set(node, i);
        }
    };
    
    getTopoIndex = async (node: B64Hash) => this.topoIndex.get(node)!;

    addPred = async (node: B64Hash, pred: B64Hash) => {
        this.pred.add(node, pred);
    };

    getPreds = async (node: B64Hash) => this.pred.get(node);
    
}