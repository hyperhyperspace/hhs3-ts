import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { KeyId } from "@hyper-hyper-space/hhs3_crypto";
import { version, Version } from "@hyper-hyper-space/hhs3_mvt";
import { EntryPredicate } from "@hyper-hyper-space/hhs3_dag";

import {
    CreateRCapPayload,
    GrantPayload,
    RevokePayload,
    CapPayload,
} from "./payload.js";

import type { RCap, RCapView } from "./interfaces.js";

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
    // function of (this.at, this.from, grantee, capName) -- it does not depend on whether
    // the call is top-level or recursive.
    //
    // Collapsed use point (collapse-X model): when `this.at` is a multi-hash frontier it is
    // modeled as a single imaginary node X that inherits the union of predecessors AND
    // successors of its elements. So an external op u is concurrent with X iff it is
    // concurrent with EVERY element of `this.at`; if u is after even one element it is
    // "later on that branch", where use-before-revoke applies.
    //
    // Two see-through barriers express use-before-revoke and concurrent-void:
    //   B1 (grant-anchored): a valid revoke of the pair concurrent with the grant op.
    //   B2 (use-anchored):   a valid revoke of the pair concurrent with the use point X.
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
    async hasCapability(grantee: KeyId, capName: string, visiting?: Set<string>): Promise<boolean> {
        if (this.target.isCreator(grantee)) return true;

        const visitKey = grantee + '\0' + capName;
        if (visiting !== undefined && visiting.has(visitKey)) return false;
        visiting = new Set(visiting);
        visiting.add(visitKey);

        if (!await this.capabilityExists(capName)) return false;

        const scopedDag = await this.target.getScopedDag();
        const grantKey = capName + ':' + grantee;
        const managedBy = await this.getManagedBy(capName);

        // See-through validity predicate. An op (grant or revoke) of this pair is valid
        // only if its author was authorized AS OF the op's own version -- evaluated on a
        // view pinned at the using op (version(hash)) so a later revoke of the author's
        // managing cap does not retroactively invalidate it (use-before-revoke). Hosting
        // the recursion here lets the cover/barrier walks "see through" an invalid op to
        // the last valid one beneath it, instead of being masked by a dominating invalid op.
        const valid: EntryPredicate = async (hash, entry) => {
            const p = entry.payload as CapPayload;
            if (p.action === 'grant'
                && !await this.hasAnySurvivingOriginIn(capName, new Set((p as GrantPayload).capOrigins))) {
                return false;
            }
            const author = (p as GrantPayload | RevokePayload).author as KeyId;
            if (this.target.isCreator(author)) return true;
            const useView = await this.target.getView(version(hash), this.from);
            for (const mgr of managedBy) {
                if (mgr === 'creator') continue;
                if (await useView.hasCapability(author, mgr, visiting)) return true;
            }
            return false;
        };

        // B2 (use-anchored): a valid revoke of this pair concurrent with the collapsed use
        // point X -- i.e. concurrent with EVERY element of this.at (findConcurrentCoverWithFilter
        // excludes any op that is after, or before, any element). A revoke that is after only
        // some elements of this.at is left to the grant-anchored B1 below. Observed from
        // this.from; vacuous when from == at.
        const useRevokes = await scopedDag.findConcurrentCoverWithFilter(
            this.from, this.at, { containsValues: { grants: [grantKey], barrier: ['t'] } }, valid,
        );
        if (useRevokes.size > 0) return false;

        // See-through cover: the last VALID grant/revoke of this pair in past(at).
        const cover = await scopedDag.findCoverWithFilter(
            this.at, { containsValues: { grants: [grantKey] } }, valid,
        );

        for (const hash of cover) {
            const entry = await scopedDag.loadEntry(hash);
            if (entry === undefined) continue;
            const p = entry.payload as CapPayload;
            if (p.action !== 'grant') continue;

            // B1 (grant-anchored): a valid revoke of this pair concurrent with this grant op.
            const concurrentRevokes = await scopedDag.findConcurrentCoverWithFilter(
                this.from, version(hash), { containsValues: { grants: [grantKey], barrier: ['t'] } }, valid,
            );
            if (concurrentRevokes.size > 0) continue;

            return true;
        }

        return false;
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
