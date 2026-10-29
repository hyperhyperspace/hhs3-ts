// Building-block helpers for delta computation between two versions.
//
// meet — fork GLB from fork.common (geometry only).
// walkDelta — feeds every entry above a bound to an accumulator.
//
// A sound revision bound for a type whose verdicts read barriers or other
// objects is walkRevisionBound (revision_bound.ts), not the meet.

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { dag, Position, position } from "@hyper-hyper-space/hhs3_dag";
import { json } from "@hyper-hyper-space/hhs3_json";

import type { DeltaAccumulator, DeltaChanges, Version } from "./mvt.js";

// Greatest lower bound (meet) of fork.common. Folding over common directly (not an
// antichain) is correct: dominated elements never lower the GLB.
//
// FIXME: computeMeet is a left fold, not a meet operation. The binary step is an
// argument, and the step passed here is findForkPosition with every field but
// commonFrontier discarded, so the fork is standing in for the meet. The hashes
// being folded are fork.common from findForkPosition(start, end), which puts the
// same query on both sides of the call. The result is a different cut from that
// outer commonFrontier: the frontier of the intersection of the principal histories
// of the attachment points, not the frontier of H(start). Make this readable. Do not
// replace it with the outer fork's commonFrontier.
export async function computeForkMeet(
    rawDag: dag.Dag, forkCommon: Iterable<B64Hash>,
): Promise<Version> {
    return dag.computeMeet(
        [...forkCommon].map((h) => position(h)),
        (a, b) => rawDag.findForkPosition(a, b).then((f) => f.commonFrontier),
    );
}

// BFS backward from `from`, excluding entries in `bound`. Returns every entry strictly
// above the revision bound — candidates whose state can differ between start and end.
export async function walkEntriesBackwardsToBound(
    rawDag: dag.Dag, from: Version, bound: Position,
): Promise<dag.Entry[]> {
    const visited = new Set<B64Hash>();
    const queue: B64Hash[] = Array.from(from);
    const walked: dag.Entry[] = [];

    while (queue.length > 0) {
        const hash = queue.shift()!;
        if (visited.has(hash)) continue;
        visited.add(hash);

        if (bound.has(hash)) continue;

        const entry = await rawDag.loadEntry(hash);
        if (entry === undefined) continue;
        walked.push(entry);

        for (const prevHash of json.fromSet(entry.header.prevEntryHashes)) {
            if (!visited.has(prevHash)) {
                queue.push(prevHash);
            }
        }
    }

    return walked;
}

// Generic, nesting-unaware backward walk. Walks every entry from `end` down to `bound`
// on the raw causal DAG and feeds each into the root accumulator, then returns the
// finalized changes tree. Routing to nested children, unwrapping, and assembling the
// nested subtree all live inside the accumulator, so this helper knows nothing about
// nesting. `start` is unused here.
export async function walkDelta(
    rawDag: dag.Dag,
    start: Version,
    end: Version,
    bound: Position,
    rootAcc: DeltaAccumulator,
): Promise<DeltaChanges> {
    const entries = await walkEntriesBackwardsToBound(rawDag, end, bound);
    for (const entry of entries) {
        await rootAcc.ingest(entry);
    }
    return rootAcc.finalize();
}

// On the reported revision bound.
//
// The meet is a sound bound only for a type with no barriers whose effect can change and
// no references: "below the fork GLB the two branches are identical" is type-agnostic. A
// type whose verdicts read barriers across branches needs more. An old barrier on a side
// branch of `start` can be concurrent with an entry below the meet, and a new entry can
// change that barrier's effect, so the entry's verdict changes without being walked
// (rdb/INCREMENTAL_DELTA_BOUND.md). Walking from `end` and stopping only at the bound's
// heads (walkEntriesBackwardsToBound) sometimes reaches such an entry anyway, through the
// whole past of a side branch's attachment point; that over-walk is not a guarantee.
//
// walkRevisionBound (revision_bound.ts) computes a sound bound: one walk that stops at the
// exit border of the bound's history and lowers the bound below every barrier of `start`
// concurrent with it whose effect changed. A referenced object's bound there is taken
// between the versions observed at `start` and at `end`, not at the meet: a bound driven by
// what changed is only sound for the exact pair of versions it compared. RTableGroup (rdb),
// RCap and RSet (std_types) use it. RSchema (rdb) uses the meet.
