import { Hash } from "@hyper-hyper-space/hhs3_crypto";
import { FindForkPositionFn, FindMinimalCoverFn, FindCoverWithFilterFn, FindConcurrentCoverWithFilterFn, Position, EntryMetaFilter, MetaProps } from "../dag_defs";
import { json } from "@hyper-hyper-space/hhs3_json";

export type DagIndex = {

    index(h: Hash, after?: Position): Promise<void> | void;

    findMinimalCover: FindMinimalCoverFn;
    findForkPosition: FindForkPositionFn;
    
    findCoverWithFilter: FindCoverWithFilterFn;
    findConcurrentCoverWithFilter: FindConcurrentCoverWithFilterFn;
    
    getIndexStore: () => Object;

};

export function checkFilter(meta: MetaProps, filter: EntryMetaFilter): boolean {
  
    for (const key of filter.containsKeys || []) {
        if (!json.hasKey(meta, key)) {
            return false;
        }
    }

    for (const [key, values] of Object.entries(filter.containsValues || [])) {

        if (!json.hasKey(meta, key) && values.length > 0) {
            return false;
        }

        for (const value of values) {

            if (!json.hasKey(meta[key], value)) {
                return false;
            }
        }
    }

  return true;
}