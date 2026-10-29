// RSet delta: membership changes between two versions, and the authorization
// flips of adds and deletes in a permissioned set.
//
// Strategies (RObject.setDeltaStrategy switch on the object):
//   - full: walkDelta from genesis, the reference for parity.
//   - bounded (the default): mvt's walkRevisionBound. A permissioned set
//     references its RCap at the horizon. The bound starts at the fork meet
//     and drops below every
//     ref-advance whose published RCap version is above RCap's own bound
//     between the versions observed at `start` and at `end`, including a
//     side-branch ref-advance of `start` that widens a concurrent entry's
//     refAt. It also drops below every barrier add or delete of `start`
//     concurrent with it whose authorization flipped. The delta's own views,
//     at (start, start) and (end, end), read nothing concurrent: that lowering
//     keeps the bound sound for views at an earlier `at`. A permissioned nested
//     set is not supported (boundedChildAccumulator).
//
// The accumulator compares, at the two horizons, every element a walked entry
// touches and the authorization of every walked add and delete. An element's
// membership at a horizon changes only through a new entry for it or an
// authorization flip of an old one, and the walk reaches both.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, KeyId } from "@hyper-hyper-space/hhs3_crypto";
import { dag } from "@hyper-hyper-space/hhs3_dag";
import {
    version, Version, Delta, DeltaChanges, DeltaAccumulator,
    RObject, ScopedDag,
    walkDelta, requireExtendingFork, walkRevisionBound, isRefAdvancePayload,
} from "@hyper-hyper-space/hhs3_mvt";
import type { Reference, RevisionBoundHost, RevisionBoundVisitor } from "@hyper-hyper-space/hhs3_mvt";

import { isAuthoredPayload, extractAuthor } from "../../authorship.js";

import { SetPayload, RSET_TYPE_ID } from "./payload.js";

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
    private readonly authorizationFlips = new Set<B64Hash>();

    private readonly childAccs = new Map<B64Hash, { acc: DeltaAccumulator; scopedDag: ScopedDag }>();

    private startView: Awaited<ReturnType<RSet['getView']>> | undefined;
    private endView: Awaited<ReturnType<RSet['getView']>> | undefined;

    constructor(
        private readonly set: RSet,
        private readonly start: Version,
        private readonly end: Version,
        // Set for the bounded strategy, which restricts nested sets (spawnChild).
        private readonly bounded = false,
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
                    this.authorizationFlips.add(entry.hash);
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

    // Whether an ingested add or delete is authorized at only one of `start` and `end`.
    authorizationFlipped(entryHash: B64Hash): boolean {
        return this.authorizationFlips.has(entryHash);
    }

    // Whether `entry` is a ref-advance that is live at only one of `start` and
    // `end`. The delta does not report it: its effect shows in the adds and
    // deletes whose authorization it changes.
    async refAdvanceFlipped(entry: dag.Entry): Promise<boolean> {
        if (!this.set.isPermissioned() || !isRefAdvancePayload(entry.payload)) return false;
        const { startView, endView } = await this.views();
        return await startView.isRefAdvanceLive(entry.hash) !== await endView.isRefAdvanceLive(entry.hash);
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
        const acc = this.bounded
            ? boundedChildAccumulator(childObj, this.start, this.end)
            : childObj.createDeltaAccumulator(this.start, this.end);
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
    const fork = await requireExtendingFork(rawDag, start, end, "bounded computeDelta");
    if (fork.forkB.size === 0) {
        return new RSetDelta(start, end, fork.commonFrontier, emptyRSetChanges(set.getType()));
    }

    const acc = new RSetDeltaAccumulator(set, start, end, true);
    const bound = await walkRevisionBound(rawDag, revisionBoundHost(set), deltaVisitor(acc), fork, start, end);
    return new RSetDelta(start, end, bound, await acc.finalize() as DeltaChanges<RSetChanges>);
}

// A permissioned set references its RCap at the horizon: an entry's
// authorization reads RCap at (refAt, refFrom), and refFrom is the fold of the
// ref-advances at the horizon (RSetViewImpl.resolveRefVersion).
function revisionBoundHost(set: RSet): RevisionBoundHost {
    const refId = set.capabilityRef();
    return {
        getScopedDag: () => set.getScopedDag(),
        references: (): Reference[] => refId === undefined ? [] : [{
            refId,
            from: 'horizon',
            getObject: async () => {
                const rcap = await set.loadRCap();
                if (rcap === undefined) throw new Error("Cannot load referenced RCap");
                return rcap;
            },
            resolveVersion: async (horizon) => (await set.getView(horizon, horizon)).resolveRefVersion(refId),
        }],
    };
}

// A candidate is a barrier of `start`: a barrier add or delete, or a
// ref-advance. An add or delete reaches concurrent views only through whether
// it passes its authorization, so its effect changed exactly when that flipped,
// which the accumulator has already compared. A ref-advance reaches the entries
// after it and concurrent with it through the versions it adds to their folds,
// which it adds only while it is live. The walk finds an unstable one itself;
// a stable one can still flip through its widened cut, so its liveness is
// compared here.
function deltaVisitor(acc: RSetDeltaAccumulator): RevisionBoundVisitor {
    return {
        async visit(entry, candidate) {
            await acc.ingest(entry);
            return candidate && (acc.authorizationFlipped(entry.hash) || await acc.refAdvanceFlipped(entry));
        },
    };
}

// The bounded walk sees only the root's meta. A nested set's entries carry
// theirs as inner-<hash>-..., so the walk misses its barriers and its
// ref-advances. That is harmless for a nested RSet without a capability ref,
// which has no verdicts, and unsound for a permissioned one, whose
// authorizations read ref-advances the walk cannot see. A nested set that no
// walked entry touches gained nothing between `start` and `end`, so rejecting
// one here, when a walked entry reaches it, is enough.
function boundedChildAccumulator(child: RObject, start: Version, end: Version): RSetDeltaAccumulator {
    if (child.getType() !== RSET_TYPE_ID) {
        throw new Error(`bounded computeDelta supports only nested sets, not a nested '${child.getType()}'`);
    }
    if ((child as RSet).isPermissioned()) {
        throw new Error(`bounded computeDelta does not support a permissioned nested set (${child.getId()}); use the full strategy`);
    }
    return new RSetDeltaAccumulator(child as RSet, start, end, true);
}
