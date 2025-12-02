import { Hash } from "@hyper-hyper-space/hhs3_crypto";
import { MultiMap } from "@hyper-hyper-space/hhs3_util";
import { TopoIndexStore } from "./topo_idx";


export class MemTopoIndexStore implements TopoIndexStore {

    nextTopoIndex: number = 0;
    topoIndex: Map<Hash, number> = new Map();

    pred: MultiMap<Hash, Hash> = new MultiMap();

    assignNextTopoIndex = async (node: Hash) => {
        if (!this.topoIndex.has(node)) {
            const i = this.nextTopoIndex;
            this.nextTopoIndex = this.nextTopoIndex + 1;
            this.topoIndex.set(node, i);
        }
    };
    
    getTopoIndex = async (node: Hash) => this.topoIndex.get(node)!;

    addPred = async (node: Hash, pred: Hash) => {
        this.pred.add(node, pred);
    };

    getPreds = async (node: Hash) => this.pred.get(node);
    
}