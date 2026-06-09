import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, KeyId } from "@hyper-hyper-space/hhs3_crypto";
import { dag } from "@hyper-hyper-space/hhs3_dag";
import {
    version, Version, Delta, DeltaChanges, DeltaAccumulator,
    RObject, ScopedDag,
    walkDelta, computeForkMeet, computeObserverRevisionBound,
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

export type RSetChanges = {
    added: B64Hash[];
    removed: B64Hash[];
    validityChanges: ValidityChange[];
};

export class RSetDelta implements Delta<RSetChanges> {
    readonly type: string;
    readonly changes: RSetChanges;
    readonly nested: ReadonlyMap<B64Hash, DeltaChanges>;

    constructor(
        public readonly start: Version,
        public readonly end: Version,
        public readonly revisionBound: Version,
        root: DeltaChanges<RSetChanges>,
    ) {
        this.type = root.type;
        this.changes = root.changes;
        this.nested = root.nested;
    }

    getRevisionBound(): Version { return this.revisionBound; }

    get added(): B64Hash[] { return this.changes.added; }
    get removed(): B64Hash[] { return this.changes.removed; }
    get validityChanges(): ValidityChange[] { return this.changes.validityChanges; }
}

function emptyRSetChanges(type: string): DeltaChanges<RSetChanges> {
    return {
        type,
        changes: { added: [], removed: [], validityChanges: [] },
        nested: new Map(),
    };
}

// Accumulator for RSet. Per ingested entry it records element-level membership candidates
// and (for permissioned sets) per-entry authorization flips, comparing the start and end
// views once per element. When an entry carries nested ops (inner-elmts), the accumulator
// spawns the child object on the fly, creates its accumulator, unwraps the entry into the
// child's space, and delegates -- self-assembling the nested subtree in finalize.
export class RSetDeltaAccumulator implements DeltaAccumulator<RSetChanges> {

    private readonly elmtCache = new Set<B64Hash>();
    private readonly added: B64Hash[] = [];
    private readonly removed: B64Hash[] = [];
    private readonly validityChanges: ValidityChange[] = [];

    private readonly childAccs = new Map<B64Hash, { acc: DeltaAccumulator; scopedDag: ScopedDag }>();

    private startView: Awaited<ReturnType<RSet['getView']>> | undefined;
    private endView: Awaited<ReturnType<RSet['getView']>> | undefined;

    constructor(
        private readonly set: RSet,
        private readonly start: Version,
        private readonly end: Version,
    ) {}

    private async views(): Promise<{ startView: Awaited<ReturnType<RSet['getView']>>; endView: Awaited<ReturnType<RSet['getView']>> }> {
        if (this.startView === undefined) this.startView = await this.set.getView(this.start, this.start);
        if (this.endView === undefined) this.endView = await this.set.getView(this.end, this.end);
        return { startView: this.startView, endView: this.endView };
    }

    async ingest(entry: dag.Entry): Promise<boolean> {
        let changed = false;

        const { startView, endView } = await this.views();

        const elmts = entry.meta['elmts'];
        if (elmts !== undefined) {
            for (const h of json.fromSet(elmts)) {
                if (this.elmtCache.has(h)) continue;
                this.elmtCache.add(h);
                const inStart = await startView.hasByHash(h);
                const inEnd = await endView.hasByHash(h);
                if (!inStart && inEnd) { this.added.push(h); changed = true; }
                else if (inStart && !inEnd) { this.removed.push(h); changed = true; }
            }
        }

        if (this.set.isPermissioned()) {
            const p = entry.payload as unknown as SetPayload;
            if (p['action'] === 'add' || p['action'] === 'delete') {
                const wasValid = await startView.checkEntryAuthorization(entry.hash);
                const nowValid = await endView.checkEntryAuthorization(entry.hash);
                if (wasValid !== nowValid) {
                    const entryElmts = entry.meta['elmts'];
                    if (entryElmts !== undefined) {
                        for (const elementHash of json.fromSet(entryElmts)) {
                            this.validityChanges.push({
                                entryHash: entry.hash,
                                elementHash,
                                action: p['action'],
                                author: isAuthoredPayload(entry.payload) ? extractAuthor(entry.payload) : undefined,
                                wasValid,
                                nowValid,
                            });
                            changed = true;
                        }
                    }
                }
            }
        }

        const innerElmts = entry.meta['inner-elmts'];
        if (innerElmts !== undefined) {
            for (const childId of json.fromSet(innerElmts)) {
                const childChanged = await this.ingestNested(childId, entry);
                changed = changed || childChanged;
            }
        }

        return changed;
    }

    private async ingestNested(childId: B64Hash, rawEntry: dag.Entry): Promise<boolean> {
        let child = this.childAccs.get(childId);
        if (child === undefined) child = await this.spawnChild(childId);

        // Unwrap the raw entry into the child's scope (NestedScopedDag.loadEntry unwraps
        // payload and meta while preserving the raw hash) before delegating.
        const unwrapped = await child.scopedDag.loadEntry(rawEntry.hash);
        if (unwrapped === undefined) return false;
        return child.acc.ingest(unwrapped);
    }

    private async spawnChild(childId: B64Hash): Promise<{ acc: DeltaAccumulator; scopedDag: ScopedDag }> {
        const contentType = this.set.contentType();
        if (contentType === undefined) {
            throw new Error("Encountered a nested op in an RSet without a contentType");
        }
        const innerFactory = await this.set.getContext().getRegistry().lookup(contentType);
        const childObj: RObject = await this.set.loadChildObject(innerFactory, childId);
        const acc = childObj.createDeltaAccumulator(this.start, this.end);
        const scopedDag = await childObj.getScopedDag();
        const entry = { acc, scopedDag };
        this.childAccs.set(childId, entry);
        return entry;
    }

    async finalize(): Promise<DeltaChanges<RSetChanges>> {
        const nested = new Map<B64Hash, DeltaChanges>();
        for (const [childId, child] of this.childAccs) {
            nested.set(childId, await child.acc.finalize());
        }

        return {
            type: this.set.getType(),
            changes: {
                added: this.added,
                removed: this.removed,
                validityChanges: this.validityChanges,
            },
            nested,
        };
    }
}

export async function computeRSetDelta(
    set: RSet, rawDag: dag.Dag, strategy: RSetDeltaStrategy,
    start: Version, end: Version,
): Promise<RSetDelta> {
    if (strategy === 'bounded') return computeDeltaBounded(set, rawDag, start, end);
    if (strategy === 'full') return computeDeltaFull(set, rawDag, start, end);
    throw new Error("Invalid delta strategy: " + strategy);
}

async function computeDeltaFull(set: RSet, rawDag: dag.Dag, start: Version, end: Version): Promise<RSetDelta> {
    // Full scan: empty bound, so the walk visits the entire history of `end`.
    const root = await walkDelta(rawDag, start, end, version(), set.createDeltaAccumulator(start, end));
    return new RSetDelta(start, end, version(), root as DeltaChanges<RSetChanges>);
}

async function computeDeltaBounded(set: RSet, rawDag: dag.Dag, start: Version, end: Version): Promise<RSetDelta> {
    const fork = await rawDag.findForkPosition(start, end);
    if (fork.forkA.size > 0) {
        throw new Error("bounded computeDelta requires END to extend START");
    }
    if (fork.forkB.size === 0) {
        return new RSetDelta(start, end, fork.commonFrontier, emptyRSetChanges(set.getType()));
    }

    const meet = await computeForkMeet(rawDag, fork.common);

    let revisionBound = meet;
    if (set.isPermissioned()) {
        const rcap = await set.loadRCap();
        if (rcap === undefined) throw new Error("Cannot load referenced RCap");
        rcap.setDeltaStrategy('bounded');
        revisionBound = await computeObserverRevisionBound(set, meet, end, rcap);
    }

    const root = await walkDelta(rawDag, start, end, revisionBound, set.createDeltaAccumulator(start, end));
    return new RSetDelta(start, end, revisionBound, root as DeltaChanges<RSetChanges>);
}
