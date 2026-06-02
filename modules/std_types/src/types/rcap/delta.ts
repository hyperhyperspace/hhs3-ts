import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, KeyId } from "@hyper-hyper-space/hhs3_crypto";
import { dag, Position, position } from "@hyper-hyper-space/hhs3_dag";
import { version, Version, Delta } from "@hyper-hyper-space/hhs3_mvt";

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

export class RCapDelta implements Delta {
    constructor(
        private start: Version,
        private end: Version,
        private revisionBound: Version,
        public readonly identityChanges: IdentityChange[],
        public readonly capabilityChanges: CapabilityChange[],
        public readonly grantChanges: GrantChange[],
    ) {}

    getStartVersion(): Version { return this.start; }
    getEndVersion(): Version { return this.end; }
    getRevisionBound(): Version { return this.revisionBound; }
}

export async function computeRCapDelta(
    cap: RCap, rawDag: dag.Dag, strategy: RCapDeltaStrategy,
    start: Version, end: Version,
): Promise<RCapDelta> {
    if (strategy === 'bounded') return computeDeltaBounded(cap, rawDag, start, end);
    if (strategy === 'full') return computeDeltaFull(cap, rawDag, start, end);
    throw new Error("Invalid delta strategy: " + strategy);
}

function addGrantPair(grantPairs: Map<string, Set<KeyId>>, capName: string, keyId: KeyId): void {
    if (!grantPairs.has(capName)) grantPairs.set(capName, new Set());
    grantPairs.get(capName)!.add(keyId);
}

function collectCandidatesFromEntries(
    cap: RCap,
    entries: Iterable<dag.Entry>,
): { keyIds: Set<KeyId>; capNames: Set<string>; grantPairs: Map<string, Set<KeyId>> } {
    const keyIds = new Set<KeyId>();
    const capNames = new Set<string>(Object.keys(cap.getInitialCaps()));
    const grantPairs = new Map<string, Set<KeyId>>();

    for (const entry of entries) {
        const p = entry.payload as CapPayload;
        switch (p.action) {
            case 'add-identity':
                keyIds.add(p.keyId);
                break;
            case 'create-cap':
                capNames.add(p.capName);
                break;
            case 'delete-cap':
                capNames.add(p.capName);
                break;
            case 'grant':
            case 'revoke':
                addGrantPair(grantPairs, p.capName, p.grantee);
                break;
        }
    }

    return { keyIds, capNames, grantPairs };
}

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

async function computeDeltaFromCandidates(
    cap: RCap,
    start: Version,
    end: Version,
    revisionBound: Version,
    keyIds: Set<KeyId>,
    capNames: Set<string>,
    grantPairs: Map<string, Set<KeyId>>,
): Promise<RCapDelta> {
    const startView = await cap.getView(start, start);
    const endView = await cap.getView(end, end);

    const identityChanges: IdentityChange[] = [];
    for (const keyId of keyIds) {
        const wasIdentity = await startView.isIdentity(keyId);
        const nowIdentity = await endView.isIdentity(keyId);
        if (wasIdentity !== nowIdentity) {
            identityChanges.push({ keyId, added: nowIdentity });
        }
    }

    const capabilityChanges: CapabilityChange[] = [];
    for (const capName of capNames) {
        const existed = await startView.capabilityExists(capName);
        const exists = await endView.capabilityExists(capName);
        if (existed !== exists) {
            capabilityChanges.push({ capName, existed, exists });
        }
    }

    const grantChanges: GrantChange[] = [];
    const endCapExists = new Map<string, boolean>();
    for (const [capName, grantKeyIds] of grantPairs) {
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

    return new RCapDelta(start, end, revisionBound, identityChanges, capabilityChanges, grantChanges);
}

async function computeDeltaFull(cap: RCap, _rawDag: dag.Dag, start: Version, end: Version): Promise<RCapDelta> {
    const scopedDag = await cap.getScopedDag();
    const entries: dag.Entry[] = [];
    for await (const entry of scopedDag.loadAllEntries()) {
        entries.push(entry);
    }
    const { keyIds, capNames, grantPairs } = collectCandidatesFromEntries(cap, entries);

    return computeDeltaFromCandidates(cap, start, end, version(), keyIds, capNames, grantPairs);
}

async function computeDeltaBounded(cap: RCap, rawDag: dag.Dag, start: Version, end: Version): Promise<RCapDelta> {
    const fork = await rawDag.findForkPosition(start, end);
    if (fork.forkA.size > 0) {
        throw new Error("bounded computeDelta requires END to extend START");
    }
    if (fork.forkB.size === 0) {
        return new RCapDelta(start, end, fork.commonFrontier, [], [], []);
    }

    // Walk back to the meet of the fork points (fork.common). Folding over common
    // directly (not an antichain) is correct: dominated elements never lower the GLB.
    const meet = await dag.computeMeet(
        [...fork.common].map((h) => position(h)),
        (a, b) => rawDag.findForkPosition(a, b).then((f) => f.commonFrontier),
    );

    const walkedEntries = await walkNewEntries(rawDag, end, meet);
    const { keyIds, capNames, grantPairs } = collectCandidatesFromEntries(cap, walkedEntries);

    return computeDeltaFromCandidates(cap, start, end, meet, keyIds, capNames, grantPairs);
}
