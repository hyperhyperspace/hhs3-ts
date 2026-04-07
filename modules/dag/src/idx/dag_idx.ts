import { Hash } from "@hyper-hyper-space/hhs3_crypto";
import { FindForkPositionFn, FindMinimalCoverFn, FindCoverWithFilterFn, FindConcurrentCoverWithFilterFn, Position, EntryMetaFilter, MetaProps, MetaContainsValues } from "../dag_defs.js";
import { json } from "@hyper-hyper-space/hhs3_json";

export type DagIndex<Tx = void> = {

    index(h: Hash, after?: Position, ...tx: Tx extends void ? [] : [tx: Tx]): Promise<void> | void;

    findMinimalCover: FindMinimalCoverFn;
    findForkPosition: FindForkPositionFn;
    
    findCoverWithFilter: FindCoverWithFilterFn;
    findConcurrentCoverWithFilter: FindConcurrentCoverWithFilterFn;
    
    getIndexStore: () => Object;

};