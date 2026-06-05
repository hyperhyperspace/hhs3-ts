import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, KeyId } from "@hyper-hyper-space/hhs3_crypto";
import { dag, position } from "@hyper-hyper-space/hhs3_dag";
import {
    version, Version, Delta,
    walkEntriesBackwardsToBound, computeForkMeet, computeObserverRevisionBound,
} from "@hyper-hyper-space/hhs3_mvt";

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

    const meet = await computeForkMeet(rawDag, fork.common);

    let revisionBound = meet;
    if (set.isPermissioned()) {
        const rcap = await set.loadRCap();
        if (rcap === undefined) throw new Error("Cannot load referenced RCap");
        rcap.setDeltaStrategy('bounded');
        revisionBound = await computeObserverRevisionBound(set, meet, end, rcap);
    }

    const candidateEntries = await walkEntriesBackwardsToBound(rawDag, end, revisionBound);

    return computeDeltaFromCandidateEntries(set, start, end, revisionBound, candidateEntries);
}
