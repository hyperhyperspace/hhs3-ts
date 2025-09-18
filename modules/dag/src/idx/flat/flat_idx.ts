import { Hash } from "@hyper-hyper-space/hhs3_crypto";
import { Entry, ForkPosition, Position } from "dag_defs";
import { DagIndex } from "idx/dag_idx";

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
    const visited = new Set<Hash>();
    
    while (pending.size > 0) {
        const n = pending.values().next().value!;
        pending.delete(n);
        visited.add(n);

        for (const pred of await index.getPreds(n)) {

            minCover.delete(pred);
            if (!visited.has(pred)) {
                pending.add(pred);
            }
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

export function createFlatIndex(index: FlatIndexStore): DagIndex {

    return {
        index: function (node: Hash, after?: Position): Promise<void> {
            return addToFlatIndex(index, node, after);
        },

        findMinimalCover(p: Position): Promise<Position> {
            return findMinimalCoverUsingFlatIndex(index, p);
        },

        findForkPosition: function (a: Position, b: Position): Promise<ForkPosition> {
            return findForkPositionUsingFlatIndex(index, a, b)
        },
        getIndexStore: () => index
    }
};