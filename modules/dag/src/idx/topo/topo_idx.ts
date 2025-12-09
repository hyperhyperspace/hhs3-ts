import { Hash } from "@hyper-hyper-space/hhs3_crypto";
import { MultiMap, PriorityQueue, Queue } from "@hyper-hyper-space/hhs3_util";
import { Entry, EntryMetaFilter, ForkPosition, Position, checkFilter } from "../../dag_defs";
import { DagIndex } from "../../idx/dag_idx";
import { DagStore } from "../../store";

export * as mem from './topo_idx_mem_store';

// Implementation of the fork finding alogrithm using a topological traversal of the base graph.

export type LevelFn = (e: Entry) => number;

export type TopoIndexStore = {

    assignNextTopoIndex: (node: Hash) => Promise<void>;
    getTopoIndex: (node: Hash) => Promise<number>;

    addPred: (node: Hash, pred: Hash) => Promise<void>;
    getPreds: (child: Hash) => Promise<Set<Hash>>;
};

export async function addToTopoIndex(index: TopoIndexStore, n: Hash, preds?: Iterable<Hash>): Promise<void> {

    await index.assignNextTopoIndex(n);

    for (const pred of (preds || [])) {
        await index.addPred(n, pred);
    }
};

export async function findMinimalCoverUsingTopoIndex(index: TopoIndexStore, p: Position): Promise<Position> {
    
    const queue = new PriorityQueue<Hash>();
    const enqueued = new Set<Hash>();
    let minTopoIdx = Number.MAX_SAFE_INTEGER;

    for (const n of p) {
        const idx = await index.getTopoIndex(n);

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

        const preds = await index.getPreds(n);

        for (const pred of preds) {
            minCover.delete(pred);

            const idx = await index.getTopoIndex(pred);

            if (idx >= minTopoIdx) {
                queue.enqueue(pred, -idx);
                enqueued.add(pred);
            }
        }
    }
    
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


export async function findForkPositionUsingTopoIndex(index: TopoIndexStore, a: Position, b: Position): Promise<ForkPosition> {

    const start = performance.now();

    // Nodes to visit
    const queue = new PriorityQueue<Hash>();
    const enqueued = new Set<Hash>();

    // Which of the nodes to visit is reachable from a purely A, B path
    const reachFromA = new Set<Hash>();
    const reachFromB = new Set<Hash>();

    //const reachFromAStrict = new Set<Hash>();
    //const reachFromBStrict = new Set<Hash>();

    // Which of the nodes to visit is reachable from a node reachable from A U B
    // (used to only add maximal nodes to maxJoin)
    const reachFromAB = new Set<Hash>();

    // Which of the nodes to visit have direct successors only in A or only in B
    // (used to construct forkA & forkB)
    const succsInA = new MultiMap<Hash, Hash>();
    const succsInB = new MultiMap<Hash, Hash>();

    // What we're actually after:
    const forkA = new Set<Hash>(); // A nodes pointing directly into A \int B
    const forkB = new Set<Hash>(); // B nodes pointing directly into A \int B
    const common = new Set<Hash>(); // nodes in A \int B being pointed at by nodes only in B or only in A
    const commonFrontier = new Set<Hash>(); // maximal nodes in A \int B

    const toCover = new Set<Hash>();

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

export async function findCoverWithFilterUsingTopoIndex(store: DagStore, index: TopoIndexStore, from: Position, meta: EntryMetaFilter): Promise<Position> {

    const queue = new PriorityQueue<Hash>();
    const enqueued = new Set<Hash>();
    const visited = new Set<Hash>();
    const preCover = new Set<Hash>();

    const enqueue = async (node: Hash): Promise<void> => {
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

export async function findConcurrentCoverWithFilterUsingTopoIndex(store: DagStore, index: TopoIndexStore, from: Position, concurrentTo: Position, meta: EntryMetaFilter): Promise<Position> {
    throw new Error('not implemented');
}

export function createDagTopoIndex(store: DagStore, index: TopoIndexStore): DagIndex {

    return {
        index: function (node: Hash, after?: Position): Promise<void> {
            return addToTopoIndex(index, node, after);
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