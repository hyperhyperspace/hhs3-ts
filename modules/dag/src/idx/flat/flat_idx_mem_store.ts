import { Hash } from "@hyper-hyper-space/hhs3_crypto";
import { MultiMap } from "@hyper-hyper-space/hhs3_util";
import { FlatIndexStore } from "./flat_idx";


class MemFlatIndexStore implements FlatIndexStore {

    pred: MultiMap<Hash, Hash> = new MultiMap();

    addPred = async (node: Hash, pred: Hash) => {
        this.pred.add(node, pred);
    };

    getPreds = async (node: Hash) => this.pred.get(node);
};

export { MemFlatIndexStore };