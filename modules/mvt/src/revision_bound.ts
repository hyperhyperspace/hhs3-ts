// The revision bound of a bounded delta between two versions of a root object,
// computed in one backward walk from `end` (rdb/INCREMENTAL_DELTA_BOUND.md).
//
// The bound starts at the fork meet. Phase A lowers it below every ref-advance
// in the meet's history whose published version is above its referenced
// object's own bound, taken between the versions observed at `start` and at
// `end`. Phase B walks back from `end` and lowers it below every walked barrier
// of `start` that is concurrent with it and whose effect changed between
// `start` and `end`: the visitor says so, or it is a ref-advance above its
// referenced object's bound. The walk stops at the exit border of the bound's
// history and resumes from it when the bound drops, so no entry is visited
// twice.
//
// The walk knows the geometry, the `barrier` meta tag and references. What a
// type does with a walked entry (accumulating its delta, reading verdicts,
// deciding whether a barrier's effect changed) is behind the visitor.

import { json } from "@hyper-hyper-space/hhs3_json";
import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { dag, position } from "@hyper-hyper-space/hhs3_dag";
import type { EntryMetaFilter, ForkPosition } from "@hyper-hyper-space/hhs3_dag";

import { version } from "./mvt.js";
import type { Version } from "./mvt.js";
import type { CausalDag, ScopedDag } from "./dag/dag_nesting.js";
import { isRefAdvancePayload, extractRefVersion, refVersionAtOrBelow } from "./refs.js";
import type { RefAdvancePayload } from "./refs.js";
import { computeForkMeet } from "./delta.js";

// A referenced object, as the walk needs it.
export type ReferencedObject = {
    computeRevisionBound(start: Version, end: Version): Promise<Version>;
    getCausalDag(): Promise<CausalDag>;
};

// A reference of the object whose delta is bounded, by how its views read the
// referenced object at a version `v` this object observed.
export type Reference =
    // Read at getView(v, w), with w the version observed at the horizon. The
    // walk bounds it between the versions observed at `start` and at `end`.
    | {
        refId: B64Hash;
        from: 'horizon';
        getObject(): Promise<ReferencedObject>;
        // The canonical version this object observes at (horizon, horizon):
        // the `from` its views use on the referenced object.
        resolveVersion(horizon: Version): Promise<Version>;
      }
    // Read at getView(v, v): moving the horizon never changes it.
    | { refId: B64Hash; from: 'at' };

// What the walk needs from the object whose delta it bounds. Fixed per object.
export type RevisionBoundHost = {
    getScopedDag(): Promise<ScopedDag>;
    // Every object this one references, each once, in a fixed order.
    references(): Reference[];
};

// One per walk. Owns everything the type does with a walked entry.
export type RevisionBoundVisitor = {
    // Called once for every walked entry, in walk order. When `candidate` is
    // true, the entry is a barrier of `start` that can lower the bound, and the
    // result says whether its effect on concurrent entries differs between
    // `start` and `end`. When it is false, the result is ignored.
    visit(entry: dag.Entry, candidate: boolean): Promise<boolean>;
};

// The fork of `start` and `end`, for a delta that requires `end` to extend
// `start`. `caller` names the operation in the error.
export async function requireExtendingFork(
    rawDag: dag.Dag, start: Version, end: Version, caller: string,
): Promise<ForkPosition> {
    const fork = await rawDag.findForkPosition(start, end);
    if (fork.forkA.size > 0) {
        throw new Error(caller + " requires END to extend START");
    }
    return fork;
}

type ReferencedBound = { dag: CausalDag; bound: Version };

// The revision bound between `start` and `end`. `fork` is
// requireExtendingFork(rawDag, start, end). Every walked entry is passed to the
// visitor exactly once; with an empty fork.forkB nothing is walked and the
// bound is fork.commonFrontier.
//
// The bound is sound, and every entry the type must reprocess is walked, when
// the host and the visitor guarantee:
//
//   1. One root. `rawDag` is the raw DAG of a root object, and its create
//      entry is the only entry without predecessors.
//   2. Determinism. A view's reads at a horizon, and the visitor's answer for a
//      candidate, are functions of the entries, `start` and `end`. Mutually
//      dependent reads are solved together and deterministically.
//   3. Cross-branch reads go through barriers. A view at (P, F) reads entries
//      outside H(P) only through entries in H(F) that carry the `barrier` tag.
//      That includes every read the accumulator makes on the views at `start`
//      and `end`.
//   4. The visitor covers a barrier's effect. For a barrier T, every view
//      anchored at a position concurrent with T reads the same from T at
//      F = start and at F = end, unless visit(T, true) returns true or T is an
//      unstable ref-advance of a 'horizon' reference. For a ref-advance with a
//      liveness verdict (6), the same holds for the views anchored after T,
//      and its verdict is the same at both: an unstable or flipped one lowers
//      the bound below it, so every entry after it is walked too.
//   5. Every reference is declared. Every ref id that appears in a ref-advance
//      of the object is declared exactly once in references(), with the
//      `from` its views read it at.
//   6. Referenced versions. A referenced object is read through views whose
//      `at` folds the ref-advances in the reading entry's past and the
//      ref-advance barriers concurrent with it: all of them, or the ones whose
//      liveness verdict holds. A verdict is itself such a read, at the
//      ref-advance's position, that may fold only some of the concurrent
//      barriers (widenRefAtUpward). For a 'horizon' reference, their `from` is
//      resolveVersion(horizon). Ref-advances are monotone and carry the `ref`
//      meta tag with the ref id.
//
//      Below the bound, a stable ref-advance keeps its verdict: no new entry is
//      concurrent with it, and each ref-advance of `start` concurrent with it
//      is below the bound and keeps its own, or is walked and, if unstable or
//      flipped, lowers the bound below both of them.
//   7. Sound referenced bounds. For a 'horizon' reference,
//      ReferencedObject.computeRevisionBound is sound for the exact pair of
//      versions it is given. References form a DAG.
//   8. References read at (v, v). An object declared 'at' is read at a
//      geometric fold, with no liveness filter, and its view at a version does
//      not depend on `from`. The walk computes no bound for it and never finds
//      its ref-advances unstable.
export async function walkRevisionBound(
    rawDag: dag.Dag, host: RevisionBoundHost, visitor: RevisionBoundVisitor,
    fork: ForkPosition, start: Version, end: Version,
): Promise<Version> {
    const references = declaredReferences(host);

    // `end` adds nothing: both histories are the same.
    if (fork.forkB.size === 0) return fork.commonFrontier;

    const scopedDag = await host.getScopedDag();
    const meet = await computeForkMeet(rawDag, fork.common);

    // The meet, lowered below every ref-advance in its history whose published
    // version is above its referenced object's bound between the versions
    // observed at `start` and at `end`.
    let bound: Version = meet;
    const referencedBounds = new Map<B64Hash, ReferencedBound>();
    for (const reference of references.values()) {
        if (reference.from !== 'horizon') continue;
        if (bound.size === 0) break;
        const object = await reference.getObject();
        const referenced: ReferencedBound = {
            dag: await object.getCausalDag(),
            bound: await object.computeRevisionBound(
                await reference.resolveVersion(start),
                await reference.resolveVersion(end),
            ),
        };
        referencedBounds.set(reference.refId, referenced);
        bound = await lowerBelowUnstableRefAdvances(rawDag, scopedDag, reference.refId, referenced, meet, bound);
    }

    let stopSet = await computeStopSet(rawDag, bound, end);
    // H(start) membership, exact for every walked entry: paths from `end`
    // enter H(start) at start, its frontier or fork.common, and stay there.
    const inStart = new Set<B64Hash>([...start, ...fork.commonFrontier, ...fork.common]);
    const visited = new Set<B64Hash>();
    const stoppedAtBound = new Set<B64Hash>();
    const queue: B64Hash[] = [...end];

    while (queue.length > 0) {
        const hash = queue.shift()!;
        if (visited.has(hash)) continue;

        // The exit border of H(bound): do not walk into it, but remember the
        // entry in case the bound drops below it.
        if (stopSet.has(hash)) {
            stoppedAtBound.add(hash);
            continue;
        }
        visited.add(hash);

        const entry = await rawDag.loadEntry(hash);
        if (entry === undefined) continue;

        const entryInStart = inStart.has(hash);
        const preds = json.fromSet(entry.header.prevEntryHashes);
        if (entryInStart) {
            for (const prev of preds) inStart.add(prev);
        }

        if (isRefAdvancePayload(entry.payload) && !references.has(entry.payload.refId)) {
            throw new Error(`ref-advance ${hash} advances ref '${entry.payload.refId}', which the host does not declare`);
        }

        // A new entry is strictly after the meet, so only a barrier of
        // `start` can be concurrent with the bound.
        const candidate = bound.size > 0 && entryInStart && isBarrierEntry(entry);
        const changed = await visitor.visit(entry, candidate);

        const triggered = candidate && (changed || await isUnstableRefAdvance(entry, referencedBounds));
        if (triggered) {
            const predPos = position(...preds);
            if (!await isStrictlyAfter(rawDag, predPos, bound)) {
                bound = await computeGLB(rawDag, bound, predPos);
                stopSet = await computeStopSet(rawDag, bound, end);
                // A stopped entry still inside the new history is on its new
                // border; one that left it is walked from here.
                for (const stopped of [...stoppedAtBound]) {
                    if (stopSet.has(stopped)) continue;
                    stoppedAtBound.delete(stopped);
                    queue.push(stopped);
                }
            }
        }

        for (const prev of preds) {
            if (!visited.has(prev)) queue.push(prev);
        }
    }

    return bound;
}

function declaredReferences(host: RevisionBoundHost): Map<B64Hash, Reference> {
    const references = new Map<B64Hash, Reference>();
    for (const reference of host.references()) {
        if (references.has(reference.refId)) {
            throw new Error(`ref '${reference.refId}' is declared twice`);
        }
        references.set(reference.refId, reference);
    }
    return references;
}

// Lower `bound` below each lowest ref-advance of `refId` in H(meet) whose
// published version is not at or below the referenced bound, so that
// ref-advance is walked. The descent stops at the first stable ref-advance on
// each branch: ref-advances are monotone, so everything below a stable one is
// stable.
async function lowerBelowUnstableRefAdvances(
    rawDag: dag.Dag, scopedDag: ScopedDag, refId: B64Hash, referenced: ReferencedBound,
    meet: Version, bound: Version,
): Promise<Version> {
    const filter: EntryMetaFilter = { containsValues: { ref: [refId] } };
    const stability = new Map<B64Hash, boolean>();
    const isStable = async (hash: B64Hash): Promise<boolean> => {
        let stable = stability.get(hash);
        if (stable === undefined) {
            const entry = await scopedDag.loadEntry(hash);
            stable = await refVersionAtOrBelow(referenced.dag, extractRefVersion(entry!.payload as RefAdvancePayload), referenced.bound);
            stability.set(hash, stable);
        }
        return stable;
    };

    const queue: B64Hash[] = [...await scopedDag.findCoverWithFilter(meet, filter)];
    const visited = new Set<B64Hash>();
    while (queue.length > 0) {
        const refAdvance = queue.shift()!;
        if (visited.has(refAdvance)) continue;
        visited.add(refAdvance);
        if (await isStable(refAdvance)) continue;

        const entry = await scopedDag.loadEntry(refAdvance);
        const preds = position(...json.fromSet(entry!.header.prevEntryHashes));
        const unstableBelow: B64Hash[] = [];
        for (const below of await scopedDag.findCoverWithFilter(preds, filter)) {
            if (!await isStable(below)) unstableBelow.push(below);
        }
        if (unstableBelow.length > 0) {
            queue.push(...unstableBelow);
        } else {
            bound = await computeGLB(rawDag, bound, preds);
        }
    }
    return bound;
}

// The exit border of H(bound) within H(from): the entries of H(bound) that are
// heads of `from` or have a direct successor outside H(bound). Every backward
// path from `from` into H(bound) enters it there.
async function computeStopSet(rawDag: dag.Dag, bound: Version, from: Version): Promise<Set<B64Hash>> {
    if (bound.size === 0) return new Set();
    const fork = await rawDag.findForkPosition(bound, from);
    return new Set<B64Hash>([...fork.common, ...fork.commonFrontier, ...bound]);
}

function isBarrierEntry(entry: dag.Entry): boolean {
    return entry.meta['barrier'] !== undefined;
}

// Every head of `bound` is in the history of `preds`.
async function isStrictlyAfter(rawDag: dag.Dag, preds: Version, bound: Version): Promise<boolean> {
    if (bound.size === 0) return true;
    if (preds.size === 0) return false;
    return (await rawDag.findForkPosition(bound, preds)).forkA.size === 0;
}

async function computeGLB(rawDag: dag.Dag, a: Version, b: Version): Promise<Version> {
    if (a.size === 0 || b.size === 0) return version();
    return (await rawDag.findForkPosition(a, b)).commonFrontier;
}

// A ref-advance of a 'horizon' reference whose published version is not at or
// below that reference's bound.
async function isUnstableRefAdvance(entry: dag.Entry, referencedBounds: Map<B64Hash, ReferencedBound>): Promise<boolean> {
    if (!isRefAdvancePayload(entry.payload)) return false;
    const referenced = referencedBounds.get(entry.payload.refId);
    if (referenced === undefined) return false;   // an 'at' reference, or no bound computed
    return !await refVersionAtOrBelow(referenced.dag, extractRefVersion(entry.payload), referenced.bound);
}
