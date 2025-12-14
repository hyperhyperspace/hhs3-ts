import { Hash } from "@hyper-hyper-space/hhs3_crypto";
import { MultiMap, PriorityQueue } from "@hyper-hyper-space/hhs3_util";
import { checkFilter, EntryMetaFilter, ForkPosition, Position } from "../../dag_defs";
import { DagIndex } from "../../idx/dag_idx";
import { position } from "../../dag";
import { DagStore } from "../../store";

export * as mem from './level_idx_mem_store';

function label(h: Hash) { return "_" + h.replace(/[^a-zA-Z0-9]/g, "").slice(-6, -1); }

// Implementation of the fork finding alogrithm using a multi-level index for fast graph traversal.

// Each entry is assinged a level (0, 1, 2...) using the distance to the root that is closer in the DAG.

// A sub-graph is built at each level. Level 0 is just the DAG, while there is an arc between entries m, n
// in level i+1 iif there is a path from m to n using only entries in level <=i.

// The fast fork finding algorithm works by projecting the fork into the next level, recursively solving a
// slightly strengthened version of the fork problem there, and then extending that solution for the level
// below. Finally, a ForkPosition is extracted from the strengthened result.

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

    addSucc: (level: number, node: Hash, succ: Hash) => Promise<void>;
    getSuccs: (level: number, node: Hash) => Promise<Set<Hash>>;
}

export async function addToLevelIndex(index: LevelIndexStore, n: Hash, preds: Position): Promise<void> {

    const { level } = await index.assignEntryInfo(n, preds);

    if (preds.size > 0) {
    
        for (const pred of preds) {
            await index.addPred(0, n, pred);
            await index.addSucc(0, pred, n);
        }

        let i = 0;

        while (i<level) { // this iteration follows i level indexed preds to
                          // build the i+1 level pred index

            const projection = await projectIntoNextLevel(index, await index.getPreds(i, n), i, {minimal: false});
            // It's important to project using {minimal: false}, otherwise some predecessors can be "lost" when
            // coming back from a higher level in the fork position finding function below.

            for (const predInNextLevel of projection.keys()) {
                await index.addPred(i+1, n, predInNextLevel);
            }

            const forwardProjection = await projectForwardIntoNextLevel(index, await index.getSuccs(i, n), i, {minimal: false});
            
            for (const succInNextLevel of forwardProjection.keys()) {
                await index.addSucc(i+1, n, succInNextLevel);
            }

            i = i+1;
        }
    }
}

export async function findMinimalCoverUsingLevelIndex(index: LevelIndexStore, p: Position): Promise<Position> {
    

    //const startTime = performance.now();

    const start = new Set<Hash>();

    for (const n of p) {
        const preds = await index.getPreds(0, n);
        for (const pred of preds) {
            start.add(pred);
        }
    }

    const reachable = await reachabilityAtLevel(index, 0, start, p);

    const minCover = new Set<Hash>();

    for (const n of p) {
        if (!reachable.has(n)) {
            minCover.add(n);
        }
    }

    //const endTime = performance.now();
    //console.log('findMinimalCoverUsingLevelIndex took', (endTime - startTime).toFixed(2), 'ms');
    
    //console.log('min cover is', [...minCover].map(label));

    return minCover;

}

async function reachabilityAtLevel(index: LevelIndexStore, level: number, start: Position, target: Position): Promise<Position> {



    //console.log('reachability at level', level, 'from', [...start].map(label), 'to', [...target].map(label));

    let minTopoIdx = Number.MAX_SAFE_INTEGER;

    for (const n of target) {
        const idx = (await index.getEntryInfo(n)).topoIndex;
        if (idx < minTopoIdx) {
            minTopoIdx = idx;
        }
    }

    let maxTopoIdx = Number.MIN_SAFE_INTEGER;

    for (const n of start) {
        const idx = (await index.getEntryInfo(n)).topoIndex;
        if (idx > maxTopoIdx) {
            maxTopoIdx = idx;
        }
    }

    //let startTime = performance.now();
    const projectStart = await projectIntoNextLevelFaster(index, start, level, {minTopoIdx: minTopoIdx});
    //let endTime = performance.now();
    //console.log('project into next level from ', level, 'took', (endTime - startTime).toFixed(2), 'ms');
    
    //startTime = performance.now();
    const projectTarget = projectStart.size > 0 ? await projectForwardIntoNextLevelFaster(index, target, level, {maxTopoIdx: maxTopoIdx}) : new Map<Hash, EntryInfo>();
    //endTime = performance.now();
    //console.log('project forward into next level from ', level, 'took', (endTime - startTime).toFixed(2), 'ms');
    

    const properProjectTarget = new Set<Hash>(projectTarget.keys());
    for (const n of projectStart.keys()) {
        properProjectTarget.delete(n);
    }

    let reachabilityAtNext = new Set<Hash>();

    let minLevel = Number.MAX_SAFE_INTEGER;

    for (const info of chain(projectStart.values(), projectTarget.values())) {
        if (info.level < minLevel) {
            minLevel = info.level;
        }
    }

    if (minLevel < Number.MAX_SAFE_INTEGER && projectStart.size > 0 && properProjectTarget.size > 0) {
        //startTime = performance.now();
        reachabilityAtNext = await reachabilityAtLevel(index, level+1, position(...projectStart.keys()), position(...projectTarget.keys()));
        //endTime = performance.now();
        //console.log('reachability at level', level+1, 'took', (endTime - startTime).toFixed(2), 'ms');
    } else if (minLevel === Number.MAX_SAFE_INTEGER) {
        // projected start and target have only root nodes, hence they are reachable at next only if they are in the start set.
        for (const n of projectStart.keys()) {
            if (projectTarget.has(n)) {
                reachabilityAtNext.add(n);
            }
        }
    }


    //startTime = performance.now();
    const queue = new PriorityQueue<Hash>();
    const enqueued = new Set<Hash>();

    for (const n of chain(start, projectStart.keys(), reachabilityAtNext)) {
        const info = await index.getEntryInfo(n);
        const idx = info.topoIndex;

        if (idx >= minTopoIdx && idx <= maxTopoIdx) {
            queue.enqueue(n, -idx);
            enqueued.add(n);
        }
    }


    const reachable = new Set<Hash>();

    //console.log('reach at l=', level, 'from', [...start].map(label), 'to', [...target].map(label), 'queue size is', queue.size(), 'minIdx is', minTopoIdx);

    while (!queue.isEmpty()) {
        const n = queue.dequeue()!;
        enqueued.delete(n);

        //console.log('dequeued', label(n));

        if (target.has(n)) {
            reachable.add(n);
            //console.log('added', label(n), 'to reachable');
        }

        const preds = await index.getPreds(level, n);

        for (const pred of preds) {

            const predInfo = await index.getEntryInfo(pred);

            const predIdx = predInfo.topoIndex;
            const predLevel = predInfo.level;

            if ((predLevel === level || predLevel === Number.MAX_SAFE_INTEGER) && predIdx >= minTopoIdx && !enqueued.has(pred)) {
                queue.enqueue(pred, -predIdx);
                enqueued.add(pred);
                //console.log('enqueued', label(pred));
            }
        }

        if (reachable.size >= target.size) {
            break;
        }
    }

    //console.log('reachable at l=', level, 'from', [...start].map(label), 'to', [...target].map(label), 'is', [...reachable].map(label));
    //endTime = performance.now();
    //console.log('reachability at level', level, 'took', (endTime - startTime).toFixed(2), 'ms');
    return reachable;
}

// Projection: given a set of starting nodes, traverse the DAG until a set of
// predecessors of the given level is found.

// options.minimal: if true, make the result a minimal covering.

// For example, if projecting A0 into level 1 (the numbers indicate the level of 
// each entry):

// A0 --> B1 --> C0 --> D1
//   \                  /
//    \-- E0 --> D0 ---/

// with options.minimal == true, the projection is { B1 }
// with options.minimal == false, the projection is { B1, D1 }

// Both projections include all the predecessors with paths composed of entries of
// lesser levels, but in the minimal case a minimal cover of the resulting set is
// returned.


async function projectIntoNextLevel(index: LevelIndexStore, nodes :Set<Hash>, level: number, options: {minimal:boolean, minTopoIdx?: number}): Promise<Map<Hash, EntryInfo>> {
    
    const start = performance.now();

    const projection = new Map<Hash, EntryInfo>();
    
    let queue = new PriorityQueue<Hash>();
    let enqueued = new Set<Hash>();

    let covered = new Set<Hash>();
    let uncoveredPaths = new Set<Hash>();

    for (const n of nodes) {
        const topoIndex = (await index.getEntryInfo(n)).topoIndex;
        if (options.minTopoIdx === undefined || topoIndex >= options.minTopoIdx) {
            queue.enqueue(n, -topoIndex);
            enqueued.add(n);
            uncoveredPaths.add(n);
        }
    }

    while (covered.size < queue.size() || uncoveredPaths.size > 0) {
        //console.log('queue size is', queue.size(), 'covered size is', covered.size, 'uncoveredPaths size is', uncoveredPaths.size);
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
            if (options.minTopoIdx === undefined || nextInfo.topoIndex >= options.minTopoIdx) {
                if (!enqueued.has(nextPred)) {
                    queue.enqueue(nextPred, -nextInfo.topoIndex);
                    enqueued.add(nextPred);
                }

                if (isCovered && !nodes.has(nextPred)) {
                    covered.add(nextPred);
                }

                if (!project && hasUncoveredPath) {
                    uncoveredPaths.add(nextPred);
                }
            }
        }

        covered.delete(n);
        uncoveredPaths.delete(n);
    }

    //console.log('project into next level for', [...nodes].map(label), 'at level', level, 'is', [...projection.keys()].map(label));

    return projection;
}

// this can only do minimal: false
async function projectIntoNextLevelFaster(index: LevelIndexStore, nodes :Set<Hash>, level: number, options: {minTopoIdx?: number}): Promise<Map<Hash, EntryInfo>> {
    
    const start = performance.now();

    const projection = new Map<Hash, EntryInfo>();
    
    let queue = new PriorityQueue<Hash>();
    let enqueued = new Set<Hash>();

    for (const n of nodes) {
        const topoIndex = (await index.getEntryInfo(n)).topoIndex;
        if (options.minTopoIdx === undefined || topoIndex >= options.minTopoIdx) {
            queue.enqueue(n, -topoIndex);
            enqueued.add(n);
        }
    }

    while (queue.size() > 0) {
        //console.log('queue size is', queue.size(), 'covered size is', covered.size, 'uncoveredPaths size is', uncoveredPaths.size);
        const n = queue.dequeue()!;
        enqueued.delete(n);

        const info = await index.getEntryInfo(n);
        let nLevel = info.level;

        let project = nLevel > level;

        if (project) {
            projection.set(n, info);
        } else {

            const nextPreds = await index.getPreds(level, n);

            for (const nextPred of nextPreds) {
                if (!enqueued.has(nextPred)) {
                    
                    const nextInfo = await index.getEntryInfo(nextPred);

                    if (options.minTopoIdx === undefined || nextInfo.topoIndex >= options.minTopoIdx) {        
                        queue.enqueue(nextPred, -nextInfo.topoIndex);
                        enqueued.add(nextPred);
                    }
                }
            }
        }
    }

    //console.log('project into next level for', [...nodes].map(label), 'at level', level, 'is', [...projection.keys()].map(label));

    return projection;
}

// this can only do minimal: false
async function projectForwardIntoNextLevelFaster(index: LevelIndexStore, nodes :Set<Hash>, level: number, options: {maxTopoIdx?: number}): Promise<Map<Hash, EntryInfo>> {
    
    const start = performance.now();

    const projection = new Map<Hash, EntryInfo>();
    
    let queue = new PriorityQueue<Hash>();
    let enqueued = new Set<Hash>();

    for (const n of nodes) {
        const topoIndex = (await index.getEntryInfo(n)).topoIndex;
        if (options.maxTopoIdx === undefined || topoIndex <= options.maxTopoIdx) {
            queue.enqueue(n, -topoIndex);
            enqueued.add(n);
        }
    }

    while (queue.size() > 0) {
        //console.log('queue size is', queue.size(), 'covered size is', covered.size, 'uncoveredPaths size is', uncoveredPaths.size);
        const n = queue.dequeue()!;
        enqueued.delete(n);

        const info = await index.getEntryInfo(n);
        let nLevel = info.level;

        let project = nLevel > level;

        if (project) {
            projection.set(n, info);
        } else {

            const nextSuccs = await index.getSuccs(level, n);

            for (const nextSucc of nextSuccs) {
                if (!enqueued.has(nextSucc)) {
                    
                    const nextInfo = await index.getEntryInfo(nextSucc);

                    if (options.maxTopoIdx === undefined || nextInfo.topoIndex <= options.maxTopoIdx) {        
                        queue.enqueue(nextSucc, -nextInfo.topoIndex);
                        enqueued.add(nextSucc);
                    }
                }
            }
        }
    }

    //console.log('project into next level for', [...nodes].map(label), 'at level', level, 'is', [...projection.keys()].map(label));

    return projection;
}


async function projectForwardIntoNextLevel(index: LevelIndexStore, nodes :Set<Hash>, level: number, options: {minimal:boolean, maxTopoIdx?: number}): Promise<Map<Hash, EntryInfo>> {
    
    const start = performance.now();

    const projection = new Map<Hash, EntryInfo>();
    
    let queue = new PriorityQueue<Hash>();
    let enqueued = new Set<Hash>();

    let covered = new Set<Hash>();
    let uncoveredPaths = new Set<Hash>();

    for (const n of nodes) {
        const topoIndex = (await index.getEntryInfo(n)).topoIndex;
        if (options.maxTopoIdx === undefined || topoIndex <= options.maxTopoIdx) {
            queue.enqueue(n, topoIndex);
            enqueued.add(n);
            uncoveredPaths.add(n);
        }
    }

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

        const nextSuccs = await index.getSuccs(level, n);

        for (const nextSucc of nextSuccs) {
            const nextInfo = await index.getEntryInfo(nextSucc);
            
            if (options.maxTopoIdx === undefined || nextInfo.topoIndex <= options.maxTopoIdx) {
                if (!enqueued.has(nextSucc)) {
                    let topoIndex = nextInfo.topoIndex;
                    queue.enqueue(nextSucc, topoIndex);
                    enqueued.add(nextSucc);
                }

                if (isCovered && !nodes.has(nextSucc)) {
                    covered.add(nextSucc);
                }

                if (!project && hasUncoveredPath) {
                    uncoveredPaths.add(nextSucc);
                }
            }
        }

        covered.delete(n);
        uncoveredPaths.delete(n);
    }

    //console.log('project forward into next level for', [...nodes].map(label), 'at level', level, 'is', [...projection.keys()].map(label));
    return projection;
}

// This is an expansion of ForkPosition necessary for processing the DAG recursively:

type LevelForkPosition = {
    level: number,
    commonFrontier: Position,
    common: Position,
    forkA: Position,
    forkB: Position,
    forkSiblings: Position // Nodes both in h(a) and in h(b) with
                           // a predecessor in the "common" set

                    // (or: nodes that are "siblings" of an element in
                   //               forkA or forkB)

            //      * forkSib * forkA
            //       \       /
            //        \     /
            //         \   /
            //           * common
}

export async function findForkPositionUsingLevelIndex(index: LevelIndexStore, a: Position, b: Position): Promise<ForkPosition> {

    const levelFP = await findForkPositionAtLevel(index, 0, a, b);

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

    // See defs in dag_defs.ts and above:
    const commonFrontier = new Set<Hash>();
    const common = new Set<Hash>();
    const forkA = new Set<Hash>();
    const forkB = new Set<Hash>();
    const forkSiblings = new Set<Hash>();

    const toCover = new Set<Hash>(); // Nodes in aUb, we need to make sure we cover them all.

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

export async function findCoverWithMetaUsingLevelIndex(store: DagStore, index: LevelIndexStore, from: Position, meta: EntryMetaFilter): Promise<Position> {
    const queue = new PriorityQueue<Hash>();
    const enqueued = new Set<Hash>();
    const visited = new Set<Hash>();
    const preCover = new Set<Hash>();

    const enqueue = async (node: Hash): Promise<void> => {
        if (enqueued.has(node) || visited.has(node)) {
            return;
        }
        queue.enqueue(node, -(await index.getEntryInfo(node)).topoIndex);
        enqueued.add(node);
    };

    for (const hash of from) {
        await enqueue(hash);
    }

    while (!queue.isEmpty()) {
        const node = queue.dequeue()!;
        enqueued.delete(node);

        if (visited.has(node)) {
            continue;
        }

        visited.add(node);

        const entry = await store.loadEntry(node);

        if (entry === undefined) {
            throw new Error('node ' + node + ' not found');
        }

        if (checkFilter(entry.meta, meta)) {
            preCover.add(node);
            continue;
        }

        const preds = await index.getPreds(0, node);

        for (const pred of preds) {
            await enqueue(pred);
        }
    }

    return findMinimalCoverUsingLevelIndex(index, preCover);
}

export async function findConcurrentCoverWithMetaUsingLevelIndex(store: DagStore, index: LevelIndexStore, from: Position, concurrentTo: Position, meta: EntryMetaFilter): Promise<Position> {
    // Create a successor map in forwardMap

    const forwardMap = new MultiMap<Hash, Hash>();

    let pending = new Set<Hash>([...from]);
    let visited = new Set<Hash>();

    while (pending.size > 0) {
        const n = pending.values().next().value!;
        pending.delete(n);

        for (const pred of await index.getPreds(0, n)) {
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

        for (const pred of await index.getPreds(0, n)) {
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
            for (const pred of await index.getPreds(0, n)) {
                if (!visited.has(pred)) {
                    pending.add(pred);
                }
            }
        }

        visited.add(n);
    }

    return findMinimalCoverUsingLevelIndex(index, preConcCover);
}

export function createDagLevelIndex(store: DagStore, index: LevelIndexStore): DagIndex {

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

        findCoverWithFilter(from: Position, meta: EntryMetaFilter): Promise<Position> {
            return findCoverWithMetaUsingLevelIndex(store, index, from, meta);
        },

        findConcurrentCoverWithFilter(from: Position, concurrentTo: Position, meta: EntryMetaFilter): Promise<Position> {
            return findConcurrentCoverWithMetaUsingLevelIndex(store, index, from, concurrentTo, meta);
        },

        
        getIndexStore: () => index
    }
};

function* chain<T>(...iterables: Iterable<T>[]): Iterable<T> {
    for (const it of iterables) {
        yield* it;
    }
}