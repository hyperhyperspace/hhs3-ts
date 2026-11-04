import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { FindForkPositionFn, FindMinimalCoverFn, FindCoverWithFilterFn, FindConcurrentCoverWithFilterFn, Position, EntryMetaFilter, MetaProps, MetaContainsValues } from "../dag_defs.js";
import { json } from "@hyper-hyper-space/hhs3_json";

export type DagIndex<Tx = void> = {

    index(h: B64Hash, after?: Position, ...tx: Tx extends void ? [] : [tx: Tx]): Promise<void> | void;

    findMinimalCover: FindMinimalCoverFn;
    findForkPosition: FindForkPositionFn;
    
    // Both covers return exactly the topmost entries of their filtered set,
    // with or without a predicate, and see through the matches a predicate
    // rejects (the contract stated on Dag in dag.ts). A new index must pass
    // test/touch_cover_tests.ts, [TOUCH_03].
    findCoverWithFilter: FindCoverWithFilterFn;
    findConcurrentCoverWithFilter: FindConcurrentCoverWithFilterFn;
    
    getIndexStore: () => Object;

};