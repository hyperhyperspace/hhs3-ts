// Ideal concurrency ("ANY"): an entry is concurrent with a position iff it is
// outside the position's causal past and not after every head of the position.
// The future of a position is the intersection of its heads' futures, so the
// result depends only on the causal past: a dominated element changes nothing.
//
// The empty position is after everything (its past is contained in every
// entry's past), so nothing is concurrent with it.

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { checkFilter } from "../dag_defs.js";
import type { Entry, EntryMetaFilter, EntryPredicate, Position } from "../dag_defs.js";

export type ConcurrentCoverOps = {
    getPreds(n: B64Hash): Promise<Iterable<B64Hash>>;
    loadEntry(n: B64Hash): Promise<Entry | undefined>;
    minimalCover(p: Position): Promise<Position>;
};

export async function findConcurrentCoverUsing(
    ops: ConcurrentCoverOps,
    from: Position,
    concurrentTo: Position,
    meta: EntryMetaFilter,
    predicate?: EntryPredicate,
): Promise<Position> {
    if (concurrentTo.size === 0) return new Set();

    const forward = new Map<B64Hash, B64Hash[]>();
    let pending = new Set<B64Hash>([...from]);
    let visited = new Set<B64Hash>();
    while (pending.size > 0) {
        const n = pending.values().next().value!;
        pending.delete(n);
        for (const pred of await ops.getPreds(n)) {
            const succs = forward.get(pred);
            if (succs === undefined) forward.set(pred, [n]);
            else succs.push(n);
            if (!visited.has(pred)) pending.add(pred);
        }
        visited.add(n);
    }

    const below = new Set<B64Hash>();
    pending = new Set<B64Hash>([...concurrentTo]);
    visited = new Set<B64Hash>();
    while (pending.size > 0) {
        const n = pending.values().next().value!;
        pending.delete(n);
        if (visited.has(n)) continue;
        visited.add(n);
        below.add(n);
        for (const pred of await ops.getPreds(n)) {
            if (!visited.has(pred)) pending.add(pred);
        }
    }

    // Nodes reached by the upward walk of every head: after the whole position.
    const reached = new Map<B64Hash, number>();
    for (const head of concurrentTo) {
        const seen = new Set<B64Hash>();
        const stack = [head];
        while (stack.length > 0) {
            const n = stack.pop()!;
            if (seen.has(n)) continue;
            seen.add(n);
            reached.set(n, (reached.get(n) ?? 0) + 1);
            const succs = forward.get(n);
            if (succs !== undefined) stack.push(...succs);
        }
    }
    const afterAll = new Set<B64Hash>();
    for (const [n, count] of reached) {
        if (count === concurrentTo.size) afterAll.add(n);
    }

    const notConcurrentTo = new Set<B64Hash>([...below, ...afterAll]);

    pending = new Set<B64Hash>([...from]);
    visited = new Set<B64Hash>();
    const preConcCover = new Set<B64Hash>();
    while (pending.size > 0) {
        const n = pending.values().next().value!;
        pending.delete(n);
        const entry = (await ops.loadEntry(n))!;
        if (!notConcurrentTo.has(n) && checkFilter(entry.meta, meta) && (!predicate || await predicate(n, entry))) {
            preConcCover.add(n);
        } else {
            for (const pred of await ops.getPreds(n)) {
                if (!visited.has(pred)) pending.add(pred);
            }
        }
        visited.add(n);
    }

    return ops.minimalCover(preConcCover);
}
