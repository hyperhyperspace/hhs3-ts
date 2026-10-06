import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { KeyId } from "@hyper-hyper-space/hhs3_crypto";
import { version, Version } from "@hyper-hyper-space/hhs3_mvt";
import {
    VerdictEvaluation, TRUE, FALSE, verdictTruth, notTruth, allTruth, anyTruth, someTruth,
} from "@hyper-hyper-space/hhs3_mvt";
import type { Truth, Verdict } from "@hyper-hyper-space/hhs3_mvt";
import { EntryPredicate } from "@hyper-hyper-space/hhs3_dag";

import {
    CreateRCapPayload,
    GrantPayload,
    RevokePayload,
    CapPayload,
} from "./payload.js";

import type { RCap, RCapView } from "./interfaces.js";

// Why a grant or revoke is void: its author held no managing cap at the op, or
// a dependency cycle left it undecided (denied).
type CapVoidReason = 'unauthorized' | 'undecided-cycle';

export class RCapViewImpl implements RCapView {

    private target: RCap;
    private at: Version;
    private from: Version;

    constructor(target: RCap, at: Version, from: Version) {
        this.target = target;
        this.at = at;
        this.from = from;
    }

    getObject(): RCap { return this.target; }
    getVersion(): Version { return this.at; }
    getFromVersion(): Version { return this.from; }

    async getReferences(): Promise<B64Hash[]> { return []; }
    async resolveRefVersion(_refId: B64Hash): Promise<Version> {
        throw new Error("RCap does not have outgoing references");
    }

    async isIdentity(keyId: KeyId): Promise<boolean> {
        if (this.target.isCreator(keyId)) return true;

        const scopedDag = await this.target.getScopedDag();
        const cover = await scopedDag.findCoverWithFilter(this.at, { containsValues: { ids: [keyId] } });
        return cover.size > 0;
    }

    private async getFirstSurvivingCapOrigin(capName: string): Promise<B64Hash | undefined> {
        const scopedDag = await this.target.getScopedDag();
        const cover = await scopedDag.findCoverWithFilter(this.at, { containsValues: { caps: [capName] } });

        for (const hash of cover) {
            const entry = await scopedDag.loadEntry(hash);
            if (entry === undefined) continue;
            const p = entry.payload as CapPayload;

            const isPositiveCandidate =
                (p.action === 'create-cap' && p.capName === capName) ||
                (p.action === 'create' && capName in (p as CreateRCapPayload).initialCaps);

            if (!isPositiveCandidate) continue;

            const concurrentDeleteBarriers = await scopedDag.findConcurrentCoverWithFilter(
                this.from, version(hash), { containsValues: { caps: [capName], barrier: ['t'] } },
            );

            if (concurrentDeleteBarriers.size === 0) {
                return hash;
            }
        }

        return undefined;
    }

    async capabilityExists(capName: string): Promise<boolean> {
        return (await this.getFirstSurvivingCapOrigin(capName)) !== undefined;
    }

    // Admissibility check: "would an op appended at `this.at` that requires `grantee` to
    // hold `capName` be admissible when observed from `this.from`?" The answer is a pure
    // function of (this.at, this.from, grantee, capName). Each grant and revoke has a
    // verdict, keyed by the op and `from`: its author held a managing cap at the op's
    // own position. Ops whose verdicts depend on each other are solved together by a
    // VerdictEvaluation (hhs3_mvt), which gives the well-founded verdicts; a revoke
    // ring that nothing outside decides is void, so every holder on it keeps the cap.
    //
    // Collapsed use point (collapse-X model): when `this.at` is a multi-hash frontier it is
    // modeled as a single imaginary node X that inherits the union of predecessors AND
    // successors of its elements. So an external op u is concurrent with X iff it is
    // concurrent with EVERY element of `this.at`; if u is after even one element it is
    // "later on that branch", where use-before-revoke applies.
    //
    // Two see-through barriers express use-before-revoke and concurrent-void:
    //   B1 (grant-anchored): a live revoke of the pair concurrent with the grant op.
    //   B2 (use-anchored):   a live revoke of the pair concurrent with the use point X.
    // Both observe from `this.from`. Division of labor:
    //   - B2 is coarse and grant-independent: it fires only for a revoke concurrent with the
    //     WHOLE use point (concurrent with every element of `this.at`). A revoke that is
    //     merely after some element of `this.at` is not concurrent with X, so B2 defers it.
    //   - B1 (with the cover) is grant-specific and handles that deferred case: the grant
    //     survives unless a revoke is concurrent with the authorizing grant op itself.
    // B2 is always-on: a barrier visible from `this.from` and concurrent with X would void an
    // op appended at `this.at`, so the query must return false. It is vacuous when
    // `from == at` (append/delta) since nothing is concurrent with the whole horizon, and
    // concurrent-only (a sequential revoke never fires it), so it never breaks
    // use-before-revoke.
    async hasCapability(grantee: KeyId, capName: string): Promise<boolean> {
        const holding = await this.holdingIn(new VerdictEvaluation<CapVoidReason>('undecided-cycle'), grantee, capName);
        if (holding.status === 'undecided') {
            throw new Error(`whether '${grantee}' holds '${capName}' is undecided outside the component being solved`);
        }
        return holding.status === 'true';
    }

    // Whether `grantee` holds `capName` here, within `evaluation`. It is undecided
    // only while the walks meet an op of the component being solved: a live
    // use-anchored revoke still makes it false, an undecided one leaves it undecided
    // unless no grant holds anyway, and an undecided op in the cover leaves it
    // undecided. `exclude` is the op whose authorization this holding decides: an op
    // is not its own barrier.
    private async holdingIn(
        evaluation: VerdictEvaluation<CapVoidReason>, grantee: KeyId, capName: string, exclude?: B64Hash,
    ): Promise<Truth> {
        if (this.target.isCreator(grantee)) return TRUE;
        if (!await this.capabilityExists(capName)) return FALSE;

        const scopedDag = await this.target.getScopedDag();
        const grantKey = capName + ':' + grantee;

        // See-through liveness predicate. A grant whose origin no longer survives here
        // is void. Otherwise an op of this pair is live when its verdict is: its author
        // was authorized AS OF the op's own version, so a later revoke of the author's
        // managing cap does not retroactively void it (use-before-revoke). The walks
        // "see through" a void op to the last live one beneath it, instead of being
        // masked by a dominating void op; undecided ops are seen through and recorded.
        const liveRecording = (met: Truth[]): EntryPredicate => async (hash, entry) => {
            if (hash === exclude) return false;
            const p = entry.payload as CapPayload;
            if (p.action === 'grant'
                && !await this.hasAnySurvivingOriginIn(capName, new Set((p as GrantPayload).capOrigins))) {
                return false;
            }
            const truth = verdictTruth(await this.opVerdict(evaluation, hash));
            if (truth.status === 'undecided') {
                met.push(truth);
                return false;
            }
            return truth.status === 'true';
        };

        // B2 (use-anchored): a live revoke of this pair concurrent with the collapsed use
        // point X -- i.e. concurrent with EVERY element of this.at (findConcurrentCoverWithFilter
        // excludes any op that is after, or before, any element). A revoke that is after only
        // some elements of this.at is left to the grant-anchored B1 below. Observed from
        // this.from; vacuous when from == at.
        const useUndecided: Truth[] = [];
        const useRevokes = await scopedDag.findConcurrentCoverWithFilter(
            this.from, this.at, { containsValues: { grants: [grantKey], barrier: ['t'] } }, liveRecording(useUndecided),
        );
        if (useRevokes.size > 0) return FALSE;

        // See-through cover: the last live grant/revoke of this pair in past(at).
        const coverUndecided: Truth[] = [];
        const cover = await scopedDag.findCoverWithFilter(
            this.at, { containsValues: { grants: [grantKey] } }, liveRecording(coverUndecided),
        );
        if (coverUndecided.length > 0) return anyTruth([...useUndecided, ...coverUndecided]);

        // A grant in the cover holds unless B1 (grant-anchored) finds a live revoke of
        // this pair concurrent with the grant op.
        const grants: B64Hash[] = [];
        for (const hash of cover) {
            const entry = await scopedDag.loadEntry(hash);
            if (entry !== undefined && (entry.payload as CapPayload).action === 'grant') grants.push(hash);
        }
        const granted = await someTruth(grants, async (hash) => {
            const revokeUndecided: Truth[] = [];
            const concurrentRevokes = await scopedDag.findConcurrentCoverWithFilter(
                this.from, version(hash), { containsValues: { grants: [grantKey], barrier: ['t'] } },
                liveRecording(revokeUndecided),
            );
            if (concurrentRevokes.size > 0) return FALSE;
            return notTruth(anyTruth(revokeUndecided));
        });

        return allTruth([notTruth(anyTruth(useUndecided)), granted]);
    }

    // The verdict of a grant or revoke observed from this view's `from`: live when its
    // author is a creator or held one of the cap's managing caps at the op's own
    // position. Keyed by op and `from` only, so every view sharing the evaluation
    // shares it.
    private opVerdict(evaluation: VerdictEvaluation<CapVoidReason>, hash: B64Hash): Promise<Verdict<CapVoidReason>> {
        const key = hash + '|' + [...this.from].sort().join(',');
        return evaluation.resolve(key, async () => {
            const entry = await (await this.target.getScopedDag()).loadEntry(hash);
            if (entry === undefined) return { status: 'live' };
            const p = entry.payload as GrantPayload | RevokePayload;
            const author = p.author as KeyId;
            if (this.target.isCreator(author)) return { status: 'live' };

            const opView = new RCapViewImpl(this.target, version(hash), this.from);
            const managers = (await opView.getManagedBy(p.capName)).filter((mgr) => mgr !== 'creator');
            const authorized = await someTruth(managers, (mgr) => opView.holdingIn(evaluation, author, mgr, hash));
            if (authorized.status === 'true') return { status: 'live' };
            if (authorized.status === 'false') return { status: 'void', reason: 'unauthorized' };
            return authorized;
        });
    }

    async getManagedBy(capName: string): Promise<string[]> {
        const initialCaps = this.target.getInitialCaps();
        if (capName in initialCaps) {
            return initialCaps[capName].managedBy;
        }

        const scopedDag = await this.target.getScopedDag();
        const cover = await scopedDag.findCoverWithFilter(this.at, { containsValues: { caps: [capName] } });

        for (const hash of cover) {
            const entry = await scopedDag.loadEntry(hash);
            if (entry === undefined) continue;
            const p = entry.payload as CapPayload;
            if (p.action === 'create-cap') {
                return p.managedBy;
            }
        }

        return [];
    }

    async currentCapCreationVersion(capName: string): Promise<Version> {
        const scopedDag = await this.target.getScopedDag();
        const cover = await scopedDag.findCoverWithFilter(this.at, { containsValues: { caps: [capName] } });
        const surviving = new Set<B64Hash>();

        for (const hash of cover) {
            const entry = await scopedDag.loadEntry(hash);
            if (entry === undefined) continue;
            const p = entry.payload as CapPayload;

            const isPositiveCandidate =
                (p.action === 'create-cap' && p.capName === capName) ||
                (p.action === 'create' && capName in (p as CreateRCapPayload).initialCaps);

            if (!isPositiveCandidate) continue;

            const concurrentDeleteBarriers = await scopedDag.findConcurrentCoverWithFilter(
                this.from, version(hash), { containsValues: { caps: [capName], barrier: ['t'] } },
            );

            if (concurrentDeleteBarriers.size === 0) {
                surviving.add(hash);
            }
        }

        return surviving;
    }

    private async hasAnySurvivingOriginIn(capName: string, origins: Set<string>): Promise<boolean> {
        const scopedDag = await this.target.getScopedDag();
        const cover = await scopedDag.findCoverWithFilter(this.at, { containsValues: { caps: [capName] } });

        for (const hash of cover) {
            if (!origins.has(hash)) continue;

            const entry = await scopedDag.loadEntry(hash);
            if (entry === undefined) continue;
            const p = entry.payload as CapPayload;

            const isPositiveCandidate =
                (p.action === 'create-cap' && p.capName === capName) ||
                (p.action === 'create' && capName in (p as CreateRCapPayload).initialCaps);

            if (!isPositiveCandidate) continue;

            const concurrentDeleteBarriers = await scopedDag.findConcurrentCoverWithFilter(
                this.from, version(hash), { containsValues: { caps: [capName], barrier: ['t'] } },
            );

            if (concurrentDeleteBarriers.size === 0) {
                return true;
            }
        }

        return false;
    }

    async getCapabilities(): Promise<string[]> {
        const caps: string[] = [];
        const all = new Set(Object.keys(this.target.getInitialCaps()));

        const scopedDag = await this.target.getScopedDag();
        const cover = await scopedDag.findCoverWithFilter(this.at, { containsKeys: ['caps'] });
        for (const hash of cover) {
            if (hash === this.target.getId()) continue;
            const entry = await scopedDag.loadEntry(hash);
            if (entry === undefined) continue;
            const p = entry.payload as CapPayload;
            if (p.action === 'create-cap') all.add(p.capName);
        }

        for (const name of all) {
            if (await this.capabilityExists(name)) {
                caps.push(name);
            }
        }

        return caps;
    }
}
