import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { MultiMap } from "@hyper-hyper-space/hhs3_util";
import { FlatIndexStore } from "./flat_idx.js";


class MemFlatIndexStore implements FlatIndexStore {

    pred: MultiMap<B64Hash, B64Hash> = new MultiMap();

    addPred = async (node: B64Hash, pred: B64Hash) => {
        this.pred.add(node, pred);
    };

    getPreds = async (node: B64Hash) => this.pred.get(node);
};

export { MemFlatIndexStore };