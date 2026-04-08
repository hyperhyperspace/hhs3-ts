import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { MultiMap, PriorityQueue, Queue } from "@hyper-hyper-space/hhs3_util";
import { Entry, EntryMetaFilter, ForkPosition, Position, checkFilter } from "../../dag_defs.js";
import { DagIndex } from "../../idx/dag_idx.js";
import { DagStore } from "../../store/index.js";

export * as mem from './topo_idx_mem_store.js';

// Implementation of the fork finding alogrithm using a topological traversal of the base graph.

export type LevelFn = (e: Entry) => number;

export type TopoIndexStore<Tx = void> = {

    assignNextTopoIndex: (node: B64Hash, ...tx: Tx extends void ? [] : [tx: Tx]) => Promise<void>;
    getTopoIndex: (node: B64Hash, ...tx: Tx extends void ? [] : [tx: Tx] | []) => Promise<number>;

    addPred: (node: B64Hash, pred: B64Hash, ...tx: Tx extends void ? [] : [tx: Tx]) => Promise<void>;
    getPreds: (child: B64Hash, ...tx: Tx extends void ? [] : [tx: Tx] | []) => Promise<Set<B64Hash>>;
};

export async function addToTopoIndex<Tx = void>(index: TopoIndexStore<Tx>, n: B64Hash, preds?: Iterable<B64Hash>, ...tx: Tx extends void ? [] : [tx: Tx]): Promise<void> {

    await index.assignNextTopoIndex(n, ...tx);

    for (const pred of (preds || [])) {
        await index.addPred(n, pred, ...tx);
    }
};

export async function findMinimalCoverUsingTopoIndex(index: TopoIndexStore<any>, p: Position): Promise<Position> {
    
    //const startTime = performance.now();

    const queue = new PriorityQueue<B64Hash>();
    const enqueued = new Set<B64Hash>();
    let minTopoIdx = Number.MAX_SAFE_INTEGER;

    for (const n of p) {
        const idx = await index.getTopoIndex(n);

        queue.enqueue(n, -idx);
        enqueued.add(n);

        if (idx < minTopoIdx) {
            minTopoIdx = idx;
        }
    }

    const minCover = new Set<B64Hash>(p);

    while (!queue.isEmpty()) {
        const n = queue.dequeue()!;
        enqueued.delete(n);

        const preds = await index.getPreds(n);

        for (const pred of preds) {
            minCover.delete(pred);

            const idx = await index.getTopoIndex(pred);

            if (idx >= minTopoIdx && !enqueued.has(pred)) {
                queue.enqueue(pred, -idx);
                enqueued.add(pred);
            }
        }
    }
    
    //const endTime = performance.now();
    //console.log('findMinimalCoverUsingTopoIndex took', (endTime - startTime).toFixed(2), 'ms');

    return minCover;
}

// Traverse the graph in reverse topological order starting from a U b using
// a priority queue.

// Keep track of which elements in the queue are reachable purely through A's
// predecessors, purely through B's, or through aUb's predecessors.

// Also keep track of which elements of the queue have _successors_ that are
// reachable only through a's (and b's) predecessors.

// These suffice to compute the ForkPosition:

// common, forkA, forkB: whenever we dequeue an element in history(aUb), 
//                       we check if any of its successors (they have already
//                       been processed b/c of topo order) are reachable only
//                       throgh history(a) or history(b). If that's the case,
//                       the dequeued element is in the fork position's common
//                       set, and the succesors in forkA and/or forkB.

// commonFrontier: we keep track of when an element in the queue is reachable
//                 just from a's predecessors, just from b', or whether it is
//                 reachable from an element in aUb's predecessors. Hence,
//                 when an element reachable from both a and b, but not from
//                 another element in aUb's predecessor set, is dequeued, we
//                 know it belongs in the commonFrontier.


export async function findForkPositionUsingTopoIndex(index: TopoIndexStore<any>, a: Position, b: Position): Promise<ForkPosition> {

    const start = performance.now();

    // Nodes to visit
    const queue = new PriorityQueue<B64Hash>();
    const enqueued = new Set<B64Hash>();

    // Which of the nodes to visit is reachable from a purely A, B path
    const reachFromA = new Set<B64Hash>();
    const reachFromB = new Set<B64Hash>();

    //const reachFromAStrict = new Set<B64Hash>();
    //const reachFromBStrict = new Set<B64Hash>();

    // Which of the nodes to visit is reachable from a node reachable from A U B
    // (used to only add maximal nodes to maxJoin)
    const reachFromAB = new Set<B64Hash>();

    // Which of the nodes to visit have direct successors only in A or only in B
    // (used to construct forkA & forkB)
    const succsInA = new MultiMap<B64Hash, B64Hash>();
    const succsInB = new MultiMap<B64Hash, B64Hash>();

    // What we're actually after:
    const forkA = new Set<B64Hash>(); // A nodes pointing directly into A \int B
    const forkB = new Set<B64Hash>(); // B nodes pointing directly into A \int B
    const common = new Set<B64Hash>(); // nodes in A \int B being pointed at by nodes only in B or only in A
    const commonFrontier = new Set<B64Hash>(); // maximal nodes in A \int B

    const toCover = new Set<B64Hash>();

    for (const n of a) {
        toCover.add(n);
        
        queue.enqueue(n, -(await index.getTopoIndex(n)));
        enqueued.add(n);
    }

    for (const n of b) {
        toCover.add(n);
        
        if (!a.has(n)) {
            queue.enqueue(n, -(await index.getTopoIndex(n)));
            enqueued.add(n);
        }
    }

    let c=0;

    while (!queue.isEmpty() && toCover.size + reachFromA.size + reachFromB.size > 0) {

        c++;
        //console.log('toCover', toCover.size, 'reachFromA', reachFromA.size, 'reachFromB', reachFromB.size);

        const n = queue.dequeue()!;
        enqueued.delete(n);

        toCover.delete(n);

        //console.log('dequeued', label(n));

        const nIsInA = a.has(n);
        const nIsInB = b.has(n);
        const nReachFromA = reachFromA.has(n);
        const nReachFromB = reachFromB.has(n);
        const nReachFromAClosure = nIsInA || nReachFromA;
        const nReachFromBClosure = nIsInB || nReachFromB;
        const nReachFromAB = reachFromAB.has(n);

        const preds = await index.getPreds(n);
        //console.log('preds: ', [...preds].map(label));

        // Update the fork position, if necessary:

        if ((nReachFromAClosure && nReachFromBClosure) || nReachFromAB) {
            
            if (nReachFromA || nReachFromB) {
                //console.log('adding ', label(n), 'to common: its reachable from A and from B')
                common.add(n);
            }
            
            if (!reachFromAB.has(n)) {
                commonFrontier.add(n);
            }
            
            for (const succInA of succsInA.get(n)) {
                //console.log('adding', label(succInA), 'to forkA (merge)')
                forkA.add(succInA);
            }

            for (const succInB of succsInB.get(n)) {
                //console.log('adding', label(succInB), 'to forkB (merge)')
                forkB.add(succInB);
            }
            
        }

        if (preds.size === 0) {
            if (nReachFromAClosure && !nReachFromBClosure && !nReachFromAB) {
                //console.log('adding', label(n), 'to forkA (sump)')
                forkA.add(n);
            }
            if (nReachFromBClosure && !nReachFromAClosure && !nReachFromAB) {
                //console.log('adding', label(n), 'to forkB (sump)')
                forkB.add(n);
            }
        }

        // Update the queue and its associated state:

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
                reachFromAB.add(pred);
            }

            if (!enqueued.has(pred)) {
                queue.enqueue(pred, -(await index.getTopoIndex(pred)));
                enqueued.add(pred);
            }

        }

        reachFromA.delete(n);
        reachFromB.delete(n);
        reachFromAB.delete(n);

        succsInA.deleteKey(n);
        succsInB.deleteKey(n);

    }

    //console.log('queue size:', queue.size(), ', toCover size', toCover.size, ', reachFromA size', reachFromA.size, ', reachFromB size', reachFromB.size);

    const end = performance.now();

    //console.log('computing using topo index took ' + (end-start) + ', visited ' + c + ' nodes');

    return {commonFrontier, common, forkA, forkB};
}

export async function findCoverWithFilterUsingTopoIndex(store: DagStore<any>, index: TopoIndexStore<any>, from: Position, meta: EntryMetaFilter): Promise<Position> {

    const queue = new PriorityQueue<B64Hash>();
    const enqueued = new Set<B64Hash>();
    const visited = new Set<B64Hash>();
    const preCover = new Set<B64Hash>();

    const enqueue = async (node: B64Hash): Promise<void> => {
        if (enqueued.has(node) || visited.has(node)) {
            return;
        }
        queue.enqueue(node, -(await index.getTopoIndex(node)));
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

        const preds = await index.getPreds(node);

        for (const pred of preds) {
            await enqueue(pred);
        }
    }

    return findMinimalCoverUsingTopoIndex(index, preCover);
}

export async function findConcurrentCoverWithFilterUsingTopoIndex(store: DagStore<any>, index: TopoIndexStore<any>, from: Position, concurrentTo: Position, meta: EntryMetaFilter): Promise<Position> {
    // Create a successor map in forwardMap

    const forwardMap = new MultiMap<B64Hash, B64Hash>();

    let pending = new Set<B64Hash>([...from]);
    let visited = new Set<B64Hash>();

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

    pending = new Set<B64Hash>([...concurrentTo]);
    visited = new Set<B64Hash>();

    const notConcurrentTo = new Set<B64Hash>([...concurrentTo]);

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

    pending = new Set<B64Hash>([...concurrentTo]);
    visited = new Set<B64Hash>();

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

    pending = new Set<B64Hash>([...from]);
    visited = new Set<B64Hash>();

    const preConcCover = new Set<B64Hash>();

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

    return findMinimalCoverUsingTopoIndex(index, preConcCover);
}

export function createDagTopoIndex<Tx = void>(store: DagStore<Tx>, index: TopoIndexStore<Tx>): DagIndex<Tx> {

    return {
        index: function (node: B64Hash, after?: Position, ...tx: Tx extends void ? [] : [tx: Tx]): Promise<void> {
            return addToTopoIndex(index, node, after, ...tx);
        },

        findMinimalCover(p: Position): Promise<Position> {
            return findMinimalCoverUsingTopoIndex(index, p);
        },

        findForkPosition: function (a: Position, b: Position): Promise<ForkPosition> {
            return findForkPositionUsingTopoIndex(index, a, b)
        },

        findCoverWithFilter(from: Position, meta: EntryMetaFilter): Promise<Position> {
            return findCoverWithFilterUsingTopoIndex(store, index, from, meta);
        },

        findConcurrentCoverWithFilter(from: Position, concurrentTo: Position, meta: EntryMetaFilter): Promise<Position> {
            return findConcurrentCoverWithFilterUsingTopoIndex(store, index, from, concurrentTo, meta);
        },

        getIndexStore: () => index
    }
};