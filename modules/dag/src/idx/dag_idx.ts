import { Hash } from "@hyper-hyper-space/hhs3_crypto";
import { FindForkPositionFn, FindMinimalCoverFn, FindCoverWithFilterFn, FindConcurrentCoverWithFilterFn, Position, EntryMetaFilter, MetaProps, MetaContainsValues } from "../dag_defs";
import { json } from "@hyper-hyper-space/hhs3_json";

export type DagIndex = {

    index(h: Hash, after?: Position): Promise<void> | void;

    findMinimalCover: FindMinimalCoverFn;
    findForkPosition: FindForkPositionFn;
    
    findCoverWithFilter: FindCoverWithFilterFn;
    findConcurrentCoverWithFilter: FindConcurrentCoverWithFilterFn;
    
    getIndexStore: () => Object;

};