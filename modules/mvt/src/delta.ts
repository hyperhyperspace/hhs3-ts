// Building-block helpers for bounded delta computation between two versions.
//
// meet — fork GLB from fork.common (geometry only).
// revisionBound / bound — walk stop and Delta.getRevisionBound() value; may sit below
// the meet when an observer's referenced object revision requires projection.

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { dag, Position, position } from "@hyper-hyper-space/hhs3_dag";
import { json } from "@hyper-hyper-space/hhs3_json";

import { projectForeignBound, resolveRefVersionAtPosition } from "./refs.js";
import type { DeltaAccumulator, DeltaChanges, RObject, Version } from "./mvt.js";

// Greatest lower bound (meet) of fork.common. Folding over common directly (not an
// antichain) is correct: dominated elements never lower the GLB.
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
// nesting. `start` is plumbed for a future tight change-driven bound (see note below); it
// is unused here.
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

// On the reported revision bound, and a deliberately-deferred tightening.
//
// We report the geometric meet (fork GLB, via computeForkMeet / computeObserverRevisionBound)
// as the revision bound. Its appeal is conceptual: "below the fork GLB the two branches are
// identical, so any resolution agrees" is a single, type-agnostic invariant that needs no
// knowledge of barriers, authorization, or BFT revision. It is provably safe for any object.
//
// A tighter bound is possible: walk in reverse-topological order and track an antichain of
// the deepest entries whose ingest reported an actual change at or below `start` (the
// `changed` boolean ingest already returns is the hook; `start` above is the gate). That
// "change-driven frontier" generally sits above the meet and falls back to `start` when
// nothing changed.
//
// The hard part is composing it across a reference (observer -> observed, e.g. permissioned
// RSet -> RCap). computeObserverRevisionBound projects the observed object's bound into the
// observer DAG; the tight bound is NOT safe to project in general. The reason is BFT
// concurrent-barrier widening: an observer's per-entry authorization check resolves the
// observed object at (at = the entry, from = the view horizon), so for the end view a
// sub-`start` entry E is resolved with from = end. A ref-advance barrier introduced in the
// observer's (start, end) interval that is concurrent to E can flip E's authorization. Such
// a "flipped barrier" can void an entry sitting between the meet and the tight frontier, so
// only the geometric meet is deep enough to be safe to project. Tightening would be safe
// precisely when there is no flipped barrier in the interval (then resolution is monotone
// and the tight bound composes); otherwise the safe floor is the meet over just the flipped
// barriers. We defer all of this: nothing consumes a tighter bound yet, and it would entangle
// type-specific authorization/barrier semantics into what is currently pure DAG geometry.

// Compute the observer's revision bound when a referenced object can revise state
// below the fork meet. `observerMeet` is the fork meet on the observer (input, not the
// result). Assumes referenced.getId() is the ref-advance refId in the observer DAG.
export async function computeObserverRevisionBound(
    observer: RObject,
    observerMeet: Version,
    observerEnd: Version,
    referenced: RObject,
): Promise<Version> {
    const observerDag = await observer.getScopedDag();
    const refId = referenced.getId();

    const refAtMeet = await resolveRefVersionAtPosition(observerDag, refId, observerMeet, observerMeet);
    const refAtEnd = await resolveRefVersionAtPosition(observerDag, refId, observerEnd, observerEnd);

    const referencedRevisionBound = (await referenced.computeDelta(refAtMeet, refAtEnd)).revisionBound;

    return projectForeignBound(
        observerDag, refId, await referenced.getCausalDag(), observerMeet, referencedRevisionBound,
    );
}
