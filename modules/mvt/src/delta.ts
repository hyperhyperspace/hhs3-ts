// Building-block helpers for bounded delta computation between two versions.
//
// meet — fork GLB from fork.common (geometry only).
// revisionBound / bound — walk stop and Delta.getRevisionBound() value; may sit below
// the meet when an observer's referenced object revision requires projection.

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { dag, Position, position } from "@hyper-hyper-space/hhs3_dag";
import { json } from "@hyper-hyper-space/hhs3_json";

import { projectForeignBound, resolveRefVersionAtPosition } from "./refs.js";
import type { RObject, Version } from "./mvt.js";

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

// Compute the observer's revision bound when a referenced object can revise authorization
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

    const referencedRevisionBound = (await referenced.computeDelta(refAtMeet, refAtEnd)).getRevisionBound();

    return projectForeignBound(
        observerDag, refId, await referenced.getCausalDag(), observerMeet, referencedRevisionBound,
    );
}
