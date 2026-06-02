import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, KeyId } from "@hyper-hyper-space/hhs3_crypto";
import { dag, Position, position, EntryMetaFilter } from "@hyper-hyper-space/hhs3_dag";
import {
    version, Version, Delta, ScopedDag,
    resolveRefVersionAtPosition, extractRefVersion,
} from "@hyper-hyper-space/hhs3_mvt";
import type { RefAdvancePayload } from "@hyper-hyper-space/hhs3_mvt";

import { isAuthoredPayload, extractAuthor } from "../../authorship.js";

import { SetPayload } from "./payload.js";

import type { RSet } from "./interfaces.js";

export type RSetDeltaStrategy = 'full' | 'bounded';

export type ValidityChange = {
    entryHash: B64Hash;
    elementHash: B64Hash;
    action: 'add' | 'delete' | 'create';
    author: KeyId | undefined;
    wasValid: boolean;
    nowValid: boolean;
};

export class RSetDelta implements Delta {
    constructor(
        private start: Version,
        private end: Version,
        private revisionBound: Version,
        public readonly added: B64Hash[],
        public readonly removed: B64Hash[],
        public readonly validityChanges: ValidityChange[],
    ) {}

    getStartVersion(): Version { return this.start; }
    getEndVersion(): Version { return this.end; }
    getRevisionBound(): Version { return this.revisionBound; }
}

export async function computeRSetDelta(
    set: RSet, rawDag: dag.Dag, strategy: RSetDeltaStrategy,
    start: Version, end: Version,
): Promise<RSetDelta> {
    if (strategy === 'bounded') return computeDeltaBounded(set, rawDag, start, end);
    if (strategy === 'full') return computeDeltaFull(set, rawDag, start, end);
    throw new Error("Invalid delta strategy: " + strategy);
}

// BFS back from `from`, stopping at (and excluding) entries in `stopAt`. Returns every
// entry strictly above the stop floor -- the candidates whose membership or authorization
// can have changed between a start at/below the floor and `from`.
async function walkNewEntries(rawDag: dag.Dag, from: Version, stopAt: Position): Promise<dag.Entry[]> {
    const visited = new Set<B64Hash>();
    const queue: B64Hash[] = Array.from(from);
    const walked: dag.Entry[] = [];

    while (queue.length > 0) {
        const hash = queue.shift()!;
        if (visited.has(hash)) continue;
        visited.add(hash);

        if (stopAt.has(hash)) continue;

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

// Shared comparison core: given the candidate entries, compute element-level membership
// changes and (for permissioned sets) authorization changes between `start` and `end`.
// Both the full and bounded strategies feed their candidate set through here.
async function computeDeltaFromCandidateEntries(
    set: RSet,
    start: Version, end: Version, revisionBound: Version, candidateEntries: dag.Entry[],
): Promise<RSetDelta> {
    const startView = await set.getView(start, start);
    const endView = await set.getView(end, end);

    const elmtHashes = new Set<B64Hash>();
    for (const entry of candidateEntries) {
        const elmts = entry.meta['elmts'];
        if (elmts !== undefined) {
            for (const h of json.fromSet(elmts)) elmtHashes.add(h);
        }
    }

    const added: B64Hash[] = [];
    const removed: B64Hash[] = [];

    for (const h of elmtHashes) {
        const inStart = await startView.hasByHash(h);
        const inEnd = await endView.hasByHash(h);
        if (!inStart && inEnd) added.push(h);
        if (inStart && !inEnd) removed.push(h);
    }

    const validityChanges: ValidityChange[] = [];

    if (set.isPermissioned()) {
        for (const entry of candidateEntries) {
            const p = entry.payload as unknown as SetPayload;
            if (p['action'] !== 'add' && p['action'] !== 'delete') continue;

            const wasValid = await startView.checkEntryAuthorization(entry.hash);
            const nowValid = await endView.checkEntryAuthorization(entry.hash);

            if (wasValid !== nowValid) {
                const elmts = entry.meta['elmts'];
                if (elmts === undefined) continue;

                for (const elementHash of json.fromSet(elmts)) {
                    validityChanges.push({
                        entryHash: entry.hash,
                        elementHash,
                        action: p['action'],
                        author: isAuthoredPayload(entry.payload) ? extractAuthor(entry.payload) : undefined,
                        wasValid,
                        nowValid,
                    });
                }
            }
        }
    }

    return new RSetDelta(start, end, revisionBound, added, removed, validityChanges);
}

async function computeDeltaFull(set: RSet, _rawDag: dag.Dag, start: Version, end: Version): Promise<RSetDelta> {
    const scopedDag = await set.getScopedDag();

    const entries: dag.Entry[] = [];
    for await (const entry of scopedDag.loadAllEntries()) {
        entries.push(entry);
    }

    return computeDeltaFromCandidateEntries(set, start, end, version(), entries);
}

async function computeDeltaBounded(set: RSet, rawDag: dag.Dag, start: Version, end: Version): Promise<RSetDelta> {
    const fork = await rawDag.findForkPosition(start, end);
    if (fork.forkA.size > 0) {
        throw new Error("bounded computeDelta requires END to extend START");
    }
    if (fork.forkB.size === 0) {
        return new RSetDelta(start, end, fork.commonFrontier, [], [], []);
    }

    // Meet of the fork points: the floor for a plain set. Folding over fork.common
    // directly (not an antichain) is correct -- dominated elements never lower the GLB.
    const rsetMeet = await dag.computeMeet(
        [...fork.common].map((h) => position(h)),
        (a, b) => rawDag.findForkPosition(a, b).then((f) => f.commonFrontier),
    );

    // For a permissioned set, authorization can change below the RSet meet because the
    // end-view observes the RCap from a later `from` (pulling in concurrent RCap barriers).
    // Weave in the RCap delta to drop the floor to where the referenced RCap version is
    // bounded by the RCap revision bound.
    const floor = set.isPermissioned()
        ? await computeBound(set, rsetMeet, end)
        : rsetMeet;

    const candidateEntries = await walkNewEntries(rawDag, end, floor);

    return computeDeltaFromCandidateEntries(set, start, end, floor, candidateEntries);
}

// Compute a bound that takes into consideration possible view revisions in the
// nested RCap instance, and ensures that all chnages before the bound do NOT
// affect the delta being computed in RSet (see projectRCapBound).
async function computeBound(set: RSet, rsetMeet: Version, end: Version): Promise<Version> {
    const scopedDag = await set.getScopedDag();
    const refId = set.capabilityRef()!;

    // RCap versions referenced at the RSet meet and at end. rcap_at_end extends
    // rcap_at_meet whenever ref-advances are monotonic (the expected case).
    const rcapAtMeet = await resolveRefVersionAtPosition(scopedDag, refId, rsetMeet, rsetMeet);
    const rcapAtEnd = await resolveRefVersionAtPosition(scopedDag, refId, end, end);

    const rcap = await set.loadRCap();
    if (rcap === undefined) throw new Error("Cannot load referenced RCap");

    // loadRCap returns a fresh instance, so setting its strategy has no shared side effect.
    rcap.setDeltaStrategy('bounded');
    const rcapDelta = await rcap.computeDelta(rcapAtMeet, rcapAtEnd);
    const rcapBound = rcapDelta.getRevisionBound();

    const rcapRawDag = await set.getContext().getDag(refId);
    if (rcapRawDag === undefined) throw new Error("Referenced RCap DAG not found");

    return projectRCapBound(scopedDag, refId, rcapRawDag, rsetMeet, rcapBound);
}

// RCap bound projetion:t find the earliest unstable ref-advance(s) at or below `rsetMeet`.
// A ref-advance is "stable" iff its referenced RCap version is at or below `rcapBound`,
// "unstable" otherwise.
//
// Starting from the ref-advance cover at `rsetMeet`, descend through unstable ref-advances via
// their preds; a branch settles when no unstable ref-advance sits below it. The create op is an
// implicit stable ref-advance to version(refId) (the RCap root, always at or below `rcapBound`),
// so an empty below-cover settles the branch. Below the returned floor the referenced RCap
// version is bounded by `rcapBound`, so RCap authorization is identical observed from start and
// from end. If no ref-advance is unstable, return `rsetMeet` (nothing to drop below the meet).
//
// Assumes monotonic ref-advances: a stable ref-advance has only stable ref-advances below it,
// so the descent can stop at the first stable ref-advance on each branch.
async function projectRCapBound(
    scopedDag: ScopedDag, refId: B64Hash, rcapRawDag: dag.Dag, rsetMeet: Version, rcapBound: Version,
): Promise<Version> {
    const refFilter: EntryMetaFilter = { containsValues: { ref: [refId] } };

    const floor = version();
    const visited = new Set<B64Hash>();
    const stabilityCache = new Map<B64Hash, boolean>();

    // The ref filter guarantees every visited entry is a ref-advance, so the payload cast holds.
    const isStable = async (hash: B64Hash): Promise<boolean> => {
        const cached = stabilityCache.get(hash);
        if (cached !== undefined) return cached;
        const entry = await scopedDag.loadEntry(hash);
        const refVersion = extractRefVersion(entry!.payload as RefAdvancePayload);
        const fork = await rcapRawDag.findForkPosition(refVersion, rcapBound);
        const result = fork.forkA.size === 0;
        stabilityCache.set(hash, result);
        return result;
    };

    const queue: B64Hash[] = [...(await scopedDag.findCoverWithFilter(rsetMeet, refFilter))];
    while (queue.length > 0) {
        const r = queue.shift()!;
        if (visited.has(r)) continue;
        visited.add(r);
        if (await isStable(r)) continue;

        const entry = await scopedDag.loadEntry(r);
        const preds = position(...json.fromSet(entry!.header.prevEntryHashes));
        // maybe desdending using BFT here directly would be better?
        const below = await scopedDag.findCoverWithFilter(preds, refFilter);

        const unstableBelow: B64Hash[] = [];
        for (const b of below) {
            if (!(await isStable(b))) unstableBelow.push(b);
        }

        if (unstableBelow.length === 0) {
            floor.add(r);
        } else {
            queue.push(...unstableBelow);
        }
    }

    return floor.size > 0 ? floor : rsetMeet;
}
