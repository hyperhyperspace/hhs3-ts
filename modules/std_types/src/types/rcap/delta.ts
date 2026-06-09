import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, KeyId } from "@hyper-hyper-space/hhs3_crypto";
import { dag } from "@hyper-hyper-space/hhs3_dag";
import {
    version, Version, Delta, DeltaChanges, DeltaAccumulator,
    walkDelta, computeForkMeet,
} from "@hyper-hyper-space/hhs3_mvt";

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

export async function computeRCapDelta(
    cap: RCap, rawDag: dag.Dag, strategy: RCapDeltaStrategy,
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

async function computeDeltaBounded(cap: RCap, rawDag: dag.Dag, start: Version, end: Version): Promise<RCapDelta> {
    const fork = await rawDag.findForkPosition(start, end);
    if (fork.forkA.size > 0) {
        throw new Error("bounded computeDelta requires END to extend START");
    }
    if (fork.forkB.size === 0) {
        return new RCapDelta(start, end, fork.commonFrontier, emptyRCapChanges(cap.getType()));
    }

    const revisionBound = await computeForkMeet(rawDag, fork.common);

    const root = await walkDelta(rawDag, start, end, revisionBound, cap.createDeltaAccumulator(start, end));
    return new RCapDelta(start, end, revisionBound, root as DeltaChanges<RCapChanges>);
}
