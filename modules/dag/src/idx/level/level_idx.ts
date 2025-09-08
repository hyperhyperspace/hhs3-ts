import { Hash } from "@hyper-hyper-space/hhs3_crypto";
import { MultiMap, PriorityQueue } from "@hyper-hyper-space/hhs3_util";
import { ForkPosition, Position } from "dag_defs";
import { DagIndex } from "idx/dag_idx";

export * as mem from './level_idx_mem_store';

// Implementation of the fork finding alogrithm using a multi-level index and graph fast traversal.

// Each entry is assinged a level (0, 1, 2...) using the distance to the root that is closer in the DAG.

// A sub-graph is built at each level. Level 0 is just the DAG, while there is an arc between entries i, j
// in level i+1 iif there is a path from i to j using only entries in level <=i.

// The fast fork finding algorithm works by projecting the two forks into the next level, recursively solving
// a slightly strengthened version of the fork problem there, and then extending that solution for the current
// level.

export type EntryInfo = {
    topoIndex: number, // topological order is still used within each level
    level: number,
    distanceToARoot: number
}

export type LevelIndexStore = {

    assignEntryInfo: (node: Hash, after: Position) => Promise<EntryInfo>;
    getEntryInfo: (node: Hash) => Promise<EntryInfo>;

    addPred: (level: number, node: Hash, pred: Hash) => Promise<void>;
    getPreds: (level: number, node: Hash) => Promise<Set<Hash>>; 
}

export async function addToLevelIndex(index: LevelIndexStore, n: Hash, preds: Position): Promise<void> {

    const { level } = await index.assignEntryInfo(n, preds);

    if (preds.size > 0) {
    
        for (const pred of preds) {
            await index.addPred(0, n, pred);
        }

        let i = 0;

        while (i<level) { // this iteration follows i level indexed preds to
                          // build the i+1 level pred index

            const projection = await projectIntoNextLevel(index, await index.getPreds(i, n), i, {minimal: false});
            // it's important to project using {minimal: false}, otherwise some predecessors can be "lost" when
            // coming back from a higher level in the fork finding function below.

            for (const predInNextLevel of projection.keys()) {
                await index.addPred(i+1, n, predInNextLevel);
            }

            i = i+1;
        }
    }
}

export async function findMinimalCoverUsingLevelIndex(index: LevelIndexStore, p: Position): Promise<Position> {
    
    const queue = new PriorityQueue<Hash>();
    const enqueued = new Set<Hash>();
    let minTopoIdx = Number.MAX_SAFE_INTEGER;

    for (const n of p) {
        const idx = (await index.getEntryInfo(n)).topoIndex;

        queue.enqueue(n, -idx);
        enqueued.add(n);

        if (idx < minTopoIdx) {
            minTopoIdx = idx;
        }
    }

    const minCover = new Set<Hash>(p);

    while (!queue.isEmpty()) {
        const n = queue.dequeue()!;
        enqueued.delete(n);

        const preds = await index.getPreds(0, n);

        for (const pred of preds) {
            minCover.delete(pred);

            const idx = (await index.getEntryInfo(pred)).topoIndex;

            if (idx >= minTopoIdx) {
                queue.enqueue(pred, -idx);
                enqueued.add(pred);
            }
        }
    }
    
    return minCover;
}

// Projection: given a set of starting nodes, traverse the DAG until a set of
// predecessors of the given level is found.

// options.minimal: if true, make the result a minimal covering.

// For example, if projecting A1 into level 1 (the numbers indicate the level of 
// each entry):

// A0 --> B1 --> C0 --> D1
//   \                  /
//    \-- E0 --> D0 ---/

// with options.minimal == true, the projection is { B1 }
// with options.minimal == false, the projection is { B1, D1 }

// Both projections include all the predecessors with paths composed of entries of
// lesser levels, but in the minimal case a minimal cover of the resulting set is
// returned.


async function projectIntoNextLevel(index: LevelIndexStore, nodes :Set<Hash>, level: number, options: {minimal:boolean}): Promise<Map<Hash, EntryInfo>> {
    
    const start = performance.now();

    const projection = new Map<Hash, EntryInfo>();
    
    let queue = new PriorityQueue<Hash>();
    let enqueued = new Set<Hash>();

    let covered = new Set<Hash>();
    let uncoveredPaths = new Set<Hash>();

    for (const n of nodes) {
        const topoIndex = (await index.getEntryInfo(n)).topoIndex;
        queue.enqueue(n, -topoIndex);
        enqueued.add(n);
        uncoveredPaths.add(n);
    }

    let c = 0;
    while (covered.size < queue.size() || uncoveredPaths.size > 0) {
        const n = queue.dequeue()!;
        enqueued.delete(n);

        let isCovered = covered.has(n);
        let hasUncoveredPath = uncoveredPaths.has(n);

        const info = await index.getEntryInfo(n);
        let nLevel = info.level;

        let project = nLevel > level;

        if (project) {
            if (!isCovered || (!options.minimal && hasUncoveredPath)) {
                projection.set(n, info);
                isCovered = true;
            }
        }

        const nextPreds = await index.getPreds(level, n);

        for (const nextPred of nextPreds) {
            const nextInfo = await index.getEntryInfo(nextPred);
            if (!enqueued.has(nextPred)) {
                let topoIndex = nextInfo.topoIndex;
                queue.enqueue(nextPred, -topoIndex);
                enqueued.add(nextPred);
            }

            if (isCovered && !nodes.has(nextPred)) {
                covered.add(nextPred);
            }

            //if ((!isCovered || nextInfo.level <= level) && hasUncoveredPath) {
            if (!project && hasUncoveredPath) {
                uncoveredPaths.add(nextPred);
            }
        }

        covered.delete(n);
        uncoveredPaths.delete(n);
        c++;
    }

    const end = performance.now();

    //console.log('computed projection in', end-start, ', visited', c, 'nodes for a start set of', nodes.size, 'and a projected size of', projection.size);

    return projection;
}

// This is an expansion of ForkPosition necessary for processing the DAG recursively:

type LevelForkPosition = {
    level: number,
    commonFrontier: Position,
    common: Position,
    forkA: Position,
    forkB: Position,
    forkSiblings: Position // nodes in both in h(a) and in h(b) 
                           // with a predecessor in the "common" set

                    // (or: nodes that are "siblings" with an element in
                   //               forkA or forkB)

            //      * forkSib * forkA
            //       \       /
            //        \     /
            //         \   /
            //           * common
}

export async function findForkPositionUsingLevelIndex(index: LevelIndexStore, a: Position, b: Position): Promise<ForkPosition> {

    const start = performance.now();

    const levelFP = await findForkPositionAtLevel(index, 0, a, b);

    const end = performance.now();

    //console.log('computed fork using level index in ', end-start)

    return {
        commonFrontier: levelFP.commonFrontier,
        common: levelFP.common,
        forkA: levelFP.forkA,
        forkB: levelFP.forkB
    };

}


export async function findForkPositionAtLevel(index: LevelIndexStore, level: number, a: Position, b: Position): Promise<LevelForkPosition> {

    const queue = new PriorityQueue<Hash>();
    const enqueued = new Set<Hash>();

    const reachFromA = new Set<Hash>();  // reachable from a node in b
    const reachFromB = new Set<Hash>();  // reachable from a node in a
    const reachFromAB = new Set<Hash>(); // reachable from a node that is in h(a) & h(b)

    const succsInA = new MultiMap<Hash, Hash>(); // set of succesors of a in reachFromA
    const succsInB = new MultiMap<Hash, Hash>(); // set of succesors of a in reachFromB
    const succsInAB = new MultiMap<Hash, Hash>(); // set of successors of a in reachFromAB

    // see defs in dag_defs.ts and above:
    const commonFrontier = new Set<Hash>();
    const common = new Set<Hash>();
    const forkA = new Set<Hash>();
    const forkB = new Set<Hash>();
    const forkSiblings = new Set<Hash>();

    const toCover = new Set<Hash>(); // nodes in aUb, we need to make sure we cover them all.

    // Build the initial queue state

    const toEnqueue = new Array<[Hash, number]>;

    for (const n of chain(a, b)) {
        const idxInfo = await index.getEntryInfo(n);
        toEnqueue.push([n, -idxInfo.topoIndex]);
        toCover.add(n);
    }

    const projA = await projectIntoNextLevel(index, a, level, {minimal: true});
    const projB = await projectIntoNextLevel(index, b, level, {minimal: true});

    let nextLevel = Number.MAX_SAFE_INTEGER;

    for (const info of chain(projA.values(), projB.values())) {
        if (info.level < nextLevel) {
            nextLevel = info.level;
        }
    }

    const nextLevelA = new Set<Hash>(projA.keys());
    const nextLevelB = new Set<Hash>(projB.keys());
    if (nextLevel < Number.MAX_SAFE_INTEGER) {

        const nextLevelFP = await findForkPositionAtLevel(index, level+1, nextLevelA, nextLevelB);
        
        for (const n of nextLevelFP.forkA) {
            if (!a.has(n)) {
                reachFromA.add(n);

                const idxInfo = await index.getEntryInfo(n);
                toEnqueue.push([n, -idxInfo.topoIndex])
                toCover.add(n);
            }
        }

        for (const n of nextLevelFP.forkB) {
            if (!b.has(n)) {
                reachFromB.add(n);

                const idxInfo = await index.getEntryInfo(n);
                toEnqueue.push([n, -idxInfo.topoIndex])
                toCover.add(n);
            }
        }
        

        for (const n of nextLevelFP.forkSiblings) {
            if (!nextLevelFP.common.has(n) && !nextLevelA.has(n) && !nextLevelB.has(n)/*&& !a.has(n) && !b.has(n)*/) {
                reachFromAB.add(n);
                toCover.add(n);
            }
            const idxInfo = await index.getEntryInfo(n);
            toEnqueue.push([n, -idxInfo.topoIndex]);
        }

        
        for (const n of chain(nextLevelFP.common, nextLevelFP.commonFrontier)) {
            const idxInfo = await index.getEntryInfo(n);
            toEnqueue.push([n, -idxInfo.topoIndex]);
        }
        
    }

    for (const [n, prio] of toEnqueue) {
        if (!enqueued.has(n)) {
            queue.enqueue(n, prio);
            enqueued.add(n);
        }
    }

    // Process the queue

    while ((!queue.isEmpty() && toCover.size + reachFromA.size + reachFromB.size > 0)) {

        const n = queue.dequeue()!;
        enqueued.delete(n);
        
        const nIsInA = a.has(n);
        const nIsInB = b.has(n);
        const nReachFromA = reachFromA.has(n);
        const nReachFromB = reachFromB.has(n);
        const nReachFromAClosure = nIsInA || nReachFromA;
        const nReachFromBClosure = nIsInB || nReachFromB;
        const nReachFromAB = reachFromAB.has(n);

        const preds = await index.getPreds(level, n);

        if ((nReachFromAClosure && nReachFromBClosure) || nReachFromAB) {

            // n is in h(a) and in h(b): update common and commonFrontier
        
            if (nReachFromA || nReachFromB) {
                //console.log('adding ', label(n), 'to common: its reachable from A and from B')
                common.add(n);
            }
            
            if (!reachFromAB.has(n)) {
                commonFrontier.add(n);
            }

            // Also: analyze n's successors and update forkA, forkB, forkSiblings
            
            let hasForkSib = false;

            for (const succInA of succsInA.get(n)) {
                forkA.add(succInA);
                hasForkSib = true;
            }

            for (const succInB of succsInB.get(n)) {
                forkB.add(succInB);
                hasForkSib = true;
            }

            if (hasForkSib) {
                for (const succInAB of succsInAB.get(n)) {
                    forkSiblings.add(succInAB);
                }
            }
        }

        // Account for the special case of forked nodes with no predecessors:

        if (preds.size === 0) {
            if (nReachFromAClosure && !nReachFromBClosure && !nReachFromAB) {
                forkA.add(n);
            }
            if (nReachFromBClosure && !nReachFromAClosure && !nReachFromAB) {
                forkB.add(n);
            }
        }

        // Update the queue, adding any predecessors of n that are new, and updating
        // the various reach* and succ* mappings. Also save any nodes into nextLevel*
        // that we'll need to analyze in a recursive call into the next level index.

        for (const pred of preds) {
            if (nReachFromAClosure && !nReachFromBClosure && !nReachFromAB) {
                succsInA.add(pred, n);
                reachFromA.add(pred);
            }

            if (nReachFromBClosure && !nReachFromAClosure && !nReachFromAB) {
                succsInB.add(pred, n);
                reachFromB.add(pred);
            }

            if ((nReachFromAClosure && nReachFromBClosure) || nReachFromAB) {
                succsInAB.add(pred, n);
                reachFromAB.add(pred);
            }
            
            const predInfo = await index.getEntryInfo(pred);
            if (!enqueued.has(pred)) { 
                
                if (toCover.has(n) || predInfo.level <= level || predInfo.level === Number.MAX_SAFE_INTEGER) {
                    queue.enqueue(pred, -(predInfo.topoIndex));
                    enqueued.add(pred);
                }/* else {
                    console.log('skipping ', label(pred), 'of level', predInfo.level);
                }*/

            }
        }
        

        toCover.delete(n);

        reachFromA.delete(n);
        reachFromB.delete(n);
        reachFromAB.delete(n);

        succsInA.deleteKey(n);
        succsInB.deleteKey(n);
        succsInAB.deleteKey(n);
    }

    const end = performance.now();

    return { level, commonFrontier, common, forkA, forkB, forkSiblings };

}

export function createDagLevelIndex(index: LevelIndexStore): DagIndex {

    return {
        index: function (node: Hash, after: Position): Promise<void> {
            return addToLevelIndex(index, node, after);
        },

        findMinimalCover(p: Position): Promise<Position> {
            return findMinimalCoverUsingLevelIndex(index, p);
        },

        findForkPosition: function (a: Position, b: Position): Promise<ForkPosition> {
            return findForkPositionUsingLevelIndex(index, a, b)
        },
        getIndexStore: () => index
    }
};

function* chain<T>(...iterables: Iterable<T>[]): Iterable<T> {
    for (const it of iterables) {
        yield* it;
    }
}