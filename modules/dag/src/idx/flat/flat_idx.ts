import { hash, Hash } from "@hyper-hyper-space/hhs3_crypto";
import { Entry, EntryMetaFilter, ForkPosition, Position, checkFilter } from "../../dag_defs";
import { DagIndex } from "../../idx/dag_idx";
import { DagStore } from "store";
import { MultiMap } from "@hyper-hyper-space/hhs3_util";

export * as mem from './flat_idx_mem_store';

// This naive implementation of the DAG index is intended to be used as a
// reliable baseline for tests.

// It is declarative and expected to be slow.

export type LevelFn = (e: Entry) => number;

export type FlatIndexStore = {
    addPred: (node: Hash, pred: Hash) => Promise<void>;
    getPreds: (child: Hash) => Promise<Set<Hash>>;
};

export async function addToFlatIndex(index: FlatIndexStore, n: Hash, preds?: Iterable<Hash>): Promise<void> {

    for (const pred of (preds || [])) {
        await index.addPred(n, pred);
    }
};

export async function findMinimalCoverUsingFlatIndex(index: FlatIndexStore, p: Position): Promise<Position> {

    const minCover = new Set<Hash>([...p]);

    const pending = new Set<Hash>([...p]);
    //const visited = new Set<Hash>(); // un-comment for faster execution, but much worse memory usage
    
    while (pending.size > 0) {
        const n = pending.values().next().value!;
        pending.delete(n);
        //visited.add(n);

        for (const pred of await index.getPreds(n)) {

            minCover.delete(pred);
            // visiting is idempotent, so we don't need to re-visit nodes
            //if (!visited.has(pred)) {
                pending.add(pred);
            //}
        }
    }
    
    return minCover;
}

async function findAllPreds(index: FlatIndexStore, p: Position): Promise<Set<Hash>> {

    const pending = new Set<Hash>([...p]);
    const visited = new Set<Hash>();

    while (pending.size > 0) {
        const n = pending.values().next().value!;
        pending.delete(n);
        visited.add(n);

        for (const pred of await index.getPreds(n)) {
            if (!visited.has(pred)) {
                pending.add(pred);
            }
        }
    }

    return visited;
}

export async function findForkPositionUsingFlatIndex(index: FlatIndexStore, a: Position, b: Position): Promise<ForkPosition> {

    const reachFromA = await findAllPreds(index, a);
    const reachFromB = await findAllPreds(index, b);

    const reachFromAB = new Set<Hash>();

    for (const n of reachFromA) {
        if (reachFromB.has(n)) {
            reachFromAB.add(n);
        }
    }

    const forkA = new Set<Hash>();
    const forkB = new Set<Hash>();

    const common = new Set<Hash>();

    for (const n of [...reachFromA]) {
        if (!reachFromB.has(n)) {
            const preds = await index.getPreds(n);

            if (preds.size === 0) {
                forkA.add(n);
            }

            for (const pred of preds) {
                if (reachFromAB.has(pred)) {
                    forkA.add(n);
                    common.add(pred);
                }
            }
        }
    }

    for (const n of [...reachFromB]) {
        if (!reachFromA.has(n)) {
            const preds = await index.getPreds(n);

            if (preds.size === 0) {
                forkB.add(n);
            }

            for (const pred of preds) {
                if (reachFromAB.has(pred)) {
                    forkB.add(n);
                    common.add(pred);
                }
            }
        }
    }

    const commonFrontier = await findMinimalCoverUsingFlatIndex(index, reachFromAB);

    return {commonFrontier, common, forkA, forkB};
}

export async function findCoverWithFilterUsingFlatIndex(dag: DagStore, index: FlatIndexStore, from: Position, filter: EntryMetaFilter): Promise<Position> {
    
    //const minCover = new Set<Hash>([...from].filter(async (e: Hash) => checkFilter((await dag.loadHeader(e))!.meta, filter)));

    const preCover = new Set<Hash>();

    const pending = new Set<Hash>([...from]);
    const visited = new Set<Hash>();
    
    while (pending.size > 0) {
        const n = pending.values().next().value!;
        pending.delete(n);

        if (!visited.has(n)) {

            if (await dag.loadHeader(n) === undefined) {
                throw new Error('node ' + n + ' not found');
            }

            if (checkFilter((await dag.loadEntry(n))!.meta, filter)) {
                preCover.add(n);
            } else {
                for (const pred of await index.getPreds(n)) {
                    pending.add(pred);
                }
            }
        }

        visited.add(n);
    }
    
    return findMinimalCoverUsingFlatIndex(index, preCover);

}

export async function findConcurrentCoverWithFilterUsingFlatIndex(store: DagStore, index: FlatIndexStore, from: Position, concurrentTo: Position, meta: EntryMetaFilter): Promise<Position> {


    // Create a successor map in forwardMap

    const forwardMap = new MultiMap<Hash, Hash>();

    let pending = new Set<Hash>([...from]);
    let visited = new Set<Hash>();

    while (pending.size > 0) {
        const n = pending.values().next().value!;
        pending.delete(n);

        for (const pred of await index.getPreds(n)) {
            forwardMap.add(pred, n);

            if (!visited.has(pred)) {
                pending.add(pred);
            }
        }

        visited.add(n);
    }

    // Use the forward map to close the concurrentTo set upwards

    pending = new Set<Hash>([...concurrentTo]);
    visited = new Set<Hash>();

    const notConcurrentTo = new Set<Hash>([...concurrentTo]);

    while (pending.size > 0) {
        const n = pending.values().next().value!;
        pending.delete(n);

        for (const succ of forwardMap.get(n) || []) {
            if (!visited.has(succ)) {
                pending.add(succ);
                notConcurrentTo.add(succ);
            }
        }

        visited.add(n);
    }

    // And the backwards map to close the concurrentTo set downwards

    pending = new Set<Hash>([...concurrentTo]);
    visited = new Set<Hash>();

    while (pending.size > 0) {
        const n = pending.values().next().value!;
        pending.delete(n);

        for (const pred of await index.getPreds(n)) {
            if (!visited.has(pred)) {
                pending.add(pred);
                notConcurrentTo.add(pred);
            }
        }

        visited.add(n);
    }

    // Do a search for a pre cover, starting at the "from" position backwards, ignoring the nodes in notConcurrentTo

    pending = new Set<Hash>([...from]);
    visited = new Set<Hash>();

    const preConcCover = new Set<Hash>();

    while (pending.size > 0) {
        const n = pending.values().next().value!;
        pending.delete(n);

        if (!notConcurrentTo.has(n) && checkFilter((await store.loadEntry(n))!.meta, meta)) {
            preConcCover.add(n);
        } else {
            for (const pred of await index.getPreds(n)) {
                if (!visited.has(pred)) {
                    pending.add(pred);
                }
            }
        }

        visited.add(n);
    }

    return findMinimalCoverUsingFlatIndex(index, preConcCover);
}

export function createFlatIndex(store: DagStore, indexStore: FlatIndexStore): DagIndex {

    return {
        index: function (node: Hash, after?: Position): Promise<void> {
            return addToFlatIndex(indexStore, node, after);
        },

        findMinimalCover(p: Position): Promise<Position> {
            return findMinimalCoverUsingFlatIndex(indexStore, p);
        },

        findForkPosition: function (a: Position, b: Position): Promise<ForkPosition> {
            return findForkPositionUsingFlatIndex(indexStore, a, b)
        },
        
        findCoverWithFilter: function (from: Position, meta: EntryMetaFilter): Promise<Position> {
            return findCoverWithFilterUsingFlatIndex(store, indexStore, from, meta);
        },

        findConcurrentCoverWithFilter: function (from: Position, concurrentTo: Position, meta: EntryMetaFilter): Promise<Position> {
            return findConcurrentCoverWithFilterUsingFlatIndex(store, indexStore, from, concurrentTo, meta);
        },

        getIndexStore: () => indexStore
    }
};