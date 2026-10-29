// RCap delta: identity, capability and grant changes between two versions.
//
// Strategies (RObject.setDeltaStrategy switch on the object):
//   - full: walkDelta from genesis, the reference for parity.
//   - bounded: mvt's walkRevisionBound (README.md, RCap's delta computation).
//     RCap references no other object. The bound starts at the fork meet and
//     drops below every walked revoke or delete-cap of `start` that is
//     concurrent with it and whose verdict differs between `start` and `end`.
//     Only a revoke's can: a delete-cap is creator-only and always live.
//
// The accumulator collects the identities, capabilities and grant pairs that
// walked entries touch, and compares each at the two horizons in finalize. A
// capability that reappears revives every pair written before its delete, so
// finalize also adds every pair of it, walked or not.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, KeyId } from "@hyper-hyper-space/hhs3_crypto";
import { dag } from "@hyper-hyper-space/hhs3_dag";
import {
    version, Version, Delta, DeltaChanges, DeltaAccumulator,
    walkDelta, requireExtendingFork, walkRevisionBound,
} from "@hyper-hyper-space/hhs3_mvt";
import type { RevisionBoundHost, RevisionBoundVisitor } from "@hyper-hyper-space/hhs3_mvt";

import { CapPayload } from "./payload.js";

import type { RCap } from "./interfaces.js";

export type RCapDeltaStrategy = 'full' | 'bounded';

export type IdentityChange = {
    keyId: KeyId;
    added: boolean;
};

export type CapabilityChange = {
    capName: string;
    existed: boolean;
    exists: boolean;
};

export type GrantChange = {
    keyId: KeyId;
    capName: string;
    wasGranted: boolean;
    nowGranted: boolean;
};

export type RCapChanges = {
    identityChanges: IdentityChange[];
    capabilityChanges: CapabilityChange[];
    grantChanges: GrantChange[];
};

export class RCapDelta implements Delta<RCapChanges> {
    readonly type: string;
    readonly changes: RCapChanges;
    readonly nested: ReadonlyMap<B64Hash, DeltaChanges>;

    constructor(
        public readonly start: Version,
        public readonly end: Version,
        public readonly revisionBound: Version,
        root: DeltaChanges<RCapChanges>,
    ) {
        this.type = root.type;
        this.changes = root.changes;
        this.nested = root.nested;
    }

    getRevisionBound(): Version { return this.revisionBound; }

    get identityChanges(): IdentityChange[] { return this.changes.identityChanges; }
    get capabilityChanges(): CapabilityChange[] { return this.changes.capabilityChanges; }
    get grantChanges(): GrantChange[] { return this.changes.grantChanges; }
}

function emptyRCapChanges(type: string): DeltaChanges<RCapChanges> {
    return {
        type,
        changes: { identityChanges: [], capabilityChanges: [], grantChanges: [] },
        nested: new Map(),
    };
}

function addGrantPair(grantPairs: Map<string, Set<KeyId>>, capName: string, keyId: KeyId): void {
    if (!grantPairs.has(capName)) grantPairs.set(capName, new Set());
    grantPairs.get(capName)!.add(keyId);
}

// Accumulator for RCap. Collects the candidate subjects (identities, capability names,
// grant pairs) touched by each ingested entry, then compares the start and end views once
// per subject in finalize. RCap has no nested objects, so the nested map is always empty.
export class RCapDeltaAccumulator implements DeltaAccumulator<RCapChanges> {

    private readonly keyIds = new Set<KeyId>();
    private readonly capNames: Set<string>;
    private readonly grantPairs = new Map<string, Set<KeyId>>();

    constructor(
        private readonly cap: RCap,
        private readonly start: Version,
        private readonly end: Version,
    ) {
        this.capNames = new Set<string>(Object.keys(cap.getInitialCaps()));
    }

    async ingest(entry: dag.Entry): Promise<boolean> {
        const p = entry.payload as CapPayload;
        switch (p.action) {
            case 'add-identity':
                this.keyIds.add(p.keyId);
                return true;
            case 'create-cap':
                this.capNames.add(p.capName);
                return true;
            case 'delete-cap':
                this.capNames.add(p.capName);
                return true;
            case 'grant':
            case 'revoke':
                addGrantPair(this.grantPairs, p.capName, p.grantee);
                return true;
        }
        return false;
    }

    // Every pair with a grant or revoke of one of `capNames`. A filtered cover
    // returns only the topmost matching entries, so this scans the whole DAG; a
    // pair with no entry in H(end) holds at neither horizon and reports nothing.
    private async addPairsOf(capNames: Set<string>): Promise<void> {
        const scopedDag = await this.cap.getScopedDag();
        for await (const entry of scopedDag.loadAllEntries()) {
            const p = entry.payload as CapPayload;
            if ((p.action === 'grant' || p.action === 'revoke') && capNames.has(p.capName)) {
                addGrantPair(this.grantPairs, p.capName, p.grantee);
            }
        }
    }

    async finalize(): Promise<DeltaChanges<RCapChanges>> {
        const cap = this.cap;
        const startView = await cap.getView(this.start, this.start);
        const endView = await cap.getView(this.end, this.end);

        const identityChanges: IdentityChange[] = [];
        for (const keyId of this.keyIds) {
            const wasIdentity = await startView.isIdentity(keyId);
            const nowIdentity = await endView.isIdentity(keyId);
            if (wasIdentity !== nowIdentity) {
                identityChanges.push({ keyId, added: nowIdentity });
            }
        }

        const capabilityChanges: CapabilityChange[] = [];
        for (const capName of this.capNames) {
            const existed = await startView.capabilityExists(capName);
            const exists = await endView.capabilityExists(capName);
            if (existed !== exists) {
                capabilityChanges.push({ capName, existed, exists });
            }
        }

        // A capability that reappears turns its slot back on, so the grants and
        // revokes written before its delete are about it again: every pair of it
        // can change, whether or not one of its entries was walked.
        const reappeared = new Set(capabilityChanges.filter((c) => !c.existed && c.exists).map((c) => c.capName));
        if (reappeared.size > 0) await this.addPairsOf(reappeared);

        const grantChanges: GrantChange[] = [];
        const endCapExists = new Map<string, boolean>();
        for (const [capName, grantKeyIds] of this.grantPairs) {
            let capExistsInEnd = endCapExists.get(capName);
            if (capExistsInEnd === undefined) {
                capExistsInEnd = await endView.capabilityExists(capName);
                endCapExists.set(capName, capExistsInEnd);
            }
            if (!capExistsInEnd) continue;

            for (const keyId of grantKeyIds) {
                if (cap.isCreator(keyId)) continue;
                const wasGranted = await startView.hasCapability(keyId, capName);
                const nowGranted = await endView.hasCapability(keyId, capName);
                if (wasGranted !== nowGranted) {
                    grantChanges.push({ keyId, capName, wasGranted, nowGranted });
                }
            }
        }

        return {
            type: cap.getType(),
            changes: { identityChanges, capabilityChanges, grantChanges },
            nested: new Map(),
        };
    }
}

// What the bounded delta needs from its object (RCapImpl): the verdict of a
// barrier observed from a horizon.
export type RCapDeltaHost = RCap & {
    isEntryVoided(hash: B64Hash, from: Version): Promise<boolean>;
};

export async function computeRCapDelta(
    cap: RCapDeltaHost, rawDag: dag.Dag, strategy: RCapDeltaStrategy,
    start: Version, end: Version,
): Promise<RCapDelta> {
    if (strategy === 'bounded') return computeDeltaBounded(cap, rawDag, start, end);
    if (strategy === 'full') return computeDeltaFull(cap, rawDag, start, end);
    throw new Error("Invalid delta strategy: " + strategy);
}

async function computeDeltaFull(cap: RCap, rawDag: dag.Dag, start: Version, end: Version): Promise<RCapDelta> {
    // Full scan: empty bound, so the walk visits the entire history of `end`.
    const root = await walkDelta(rawDag, start, end, version(), cap.createDeltaAccumulator(start, end));
    return new RCapDelta(start, end, version(), root as DeltaChanges<RCapChanges>);
}

async function computeDeltaBounded(cap: RCapDeltaHost, rawDag: dag.Dag, start: Version, end: Version): Promise<RCapDelta> {
    const fork = await requireExtendingFork(rawDag, start, end, "bounded computeDelta");
    if (fork.forkB.size === 0) {
        return new RCapDelta(start, end, fork.commonFrontier, emptyRCapChanges(cap.getType()));
    }

    const acc = cap.createDeltaAccumulator(start, end);
    const bound = await walkRevisionBound(rawDag, revisionBoundHost(cap), deltaVisitor(cap, acc, start, end), fork, start, end);
    return new RCapDelta(start, end, bound, await acc.finalize() as DeltaChanges<RCapChanges>);
}

// The bounded revision bound between two versions of `cap`, without the delta:
// the same walk with no accumulator.
export async function computeRCapRevisionBound(
    cap: RCapDeltaHost, rawDag: dag.Dag, start: Version, end: Version,
): Promise<Version> {
    const fork = await requireExtendingFork(rawDag, start, end, "computeRevisionBound");
    if (fork.forkB.size === 0) return fork.commonFrontier;
    return walkRevisionBound(rawDag, revisionBoundHost(cap), boundVisitor(cap, start, end), fork, start, end);
}

// RCap references no other object: it has no ref-advance action.
function revisionBoundHost(cap: RCap): RevisionBoundHost {
    return { getScopedDag: () => cap.getScopedDag(), references: () => [] };
}

// A candidate is a revoke or a delete-cap of `start`. A revoke reaches
// concurrent entries only through Kill 2, which reads live revokes, so its
// effect changed exactly when its verdict did. A delete-cap is creator-only, so
// it is always live and this reports no change for it.
async function verdictChanged(cap: RCapDeltaHost, entry: dag.Entry, start: Version, end: Version): Promise<boolean> {
    return await cap.isEntryVoided(entry.hash, start) !== await cap.isEntryVoided(entry.hash, end);
}

// Feeds every walked entry to the accumulator. RCap's delta reports no verdict
// flips, so verdicts are read only for candidates.
function deltaVisitor(cap: RCapDeltaHost, acc: DeltaAccumulator, start: Version, end: Version): RevisionBoundVisitor {
    return {
        async visit(entry, candidate) {
            await acc.ingest(entry);
            return candidate && await verdictChanged(cap, entry, start, end);
        },
    };
}

function boundVisitor(cap: RCapDeltaHost, start: Version, end: Version): RevisionBoundVisitor {
    return {
        visit: async (entry, candidate) => candidate && await verdictChanged(cap, entry, start, end),
    };
}
