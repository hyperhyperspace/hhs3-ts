import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { EntryPredicate } from "@hyper-hyper-space/hhs3_dag";
import { Version, version, RObject, resolveRefVersionAtPosition, canonicalVersion, widenRefAtUpward, ScopedDag } from "@hyper-hyper-space/hhs3_mvt";

import { isAuthoredPayload, extractAuthor } from "../../authorship.js";

import { SetPayload } from "./payload.js";
import { hashElement } from "./hash.js";

import type { RSet, RSetView } from "./interfaces.js";
import type { RCap, RCapView } from "../rcap/interfaces.js";

export class RSetViewImpl<T extends json.Literal> implements RSetView<T> {

    private target: RSet<T>;
    private at: Version;
    private from: Version;

    // Ref-advance verdicts from `from`, one computation per advance however
    // many reads share it: the promise is stored before its first await.
    private readonly refAdvanceVerdicts = new Map<B64Hash, Promise<boolean>>();
    private refFromVersion: Promise<Version> | undefined;

    constructor(target: RSet<T>, at: Version, from: Version) {
        this.target = target;
        this.at = at;
        this.from = from;
    }

    getObject(): RSet<T> {
        return this.target;
    }

    getVersion(): Version {
        return this.at;
    }

    getFromVersion(): Version {
        return this.from;
    }

    async has(element: T): Promise<boolean> {

        if (this.target.contentType() !== undefined) {
            throw new Error("RSetView.has(element) is not well defined when contentType is present, please use hasByHash");
        }

        return this.hasByHash(await hashElement(element));
    }

    async hasByHash(elementHash: B64Hash): Promise<boolean> {
        const dag = await this.target.getScopedDag();

        let predicate: EntryPredicate | undefined;
        if (this.target.isPermissioned()) {
            const rcap = await this.target.loadRCap();
            if (rcap === undefined) throw new Error("Cannot load referenced RCap");
            const refId = this.target.capabilityRef()!;
            predicate = async (hash, entry) => {
                const rcapView = await this.rcapViewForEntry(dag, rcap, refId, hash);
                return this.isEntryAuthorized(entry.payload, rcapView);
            };
        }

        const cover = await dag.findCoverWithFilter(
            this.at,
            { containsValues: { elmts: [elementHash] } },
            predicate,
        );

        const adds: B64Hash[] = [];
        let barrierDeleteInCover = false;
        for (const hash of cover) {
            const payload = (await dag.loadEntry(hash))!.payload as unknown as SetPayload;
            if (payload['action'] === 'add' || payload['action'] === 'create') adds.push(hash);
            // On a barrier set every delete is a barrier. On a set without the
            // flag the bit is ignored: a delete removes an element only by
            // coming after its add, which the early return below already does.
            else if (payload['action'] === 'delete' && this.target.supportBarrierDelete()) barrierDeleteInCover = true;
        }

        if (this.target.supportBarrierAdd() && adds.length === 0) {
            const barrierAdds = await dag.findConcurrentCoverWithFilter(
                this.from, this.at,
                { containsValues: { barrier: ['t'], elmts: [elementHash] } },
                predicate,
            );
            for (const hash of barrierAdds) {
                const payload = (await dag.loadEntry(hash))!.payload as unknown as SetPayload;
                if (payload['action'] === 'add') return true;
            }
        }

        if (adds.length === 0) return false;
        if (!this.target.supportBarrierDelete()) return true;
        if (barrierDeleteInCover) return false;

        const concurrentDeletes = await dag.findConcurrentCoverWithFilter(
            this.from, this.at,
            { containsValues: { barrier: ['t'], elmts: [elementHash] } },
            predicate,
        );
        for (const hash of concurrentDeletes) {
            const payload = (await dag.loadEntry(hash))!.payload as unknown as SetPayload;
            if (payload['action'] === 'delete') return false;
        }
        return true;
    }

    // The RCap view an entry's authorization reads. refAt folds the live
    // ref-advances in the entry's past and the live ref-advance barriers
    // concurrent with it, seeing through void ones (Layer 2). refFrom counts
    // every ref-advance.
    private async rcapViewForEntry(
        dag: ScopedDag, rcap: RCap, refId: B64Hash, entryHash: B64Hash,
    ): Promise<RCapView> {
        const refAt = await resolveRefVersionAtPosition(
            dag, refId, version(entryHash), this.from, (hash) => this.isRefAdvanceLive(hash),
        );
        const causal = await rcap.getCausalDag();
        return rcap.getView(await canonicalVersion(causal, refAt), await this.refFrom(dag, rcap, refId)) as Promise<RCapView>;
    }

    // The geometric fold at (from, from), canonical in the RCap DAG.
    private refFrom(dag: ScopedDag, rcap: RCap, refId: B64Hash): Promise<Version> {
        if (this.refFromVersion === undefined) {
            this.refFromVersion = (async () => canonicalVersion(
                await rcap.getCausalDag(), await resolveRefVersionAtPosition(dag, refId, this.from, this.from),
            ))();
        }
        return this.refFromVersion;
    }

    async isRefAdvanceLive(entryHash: B64Hash): Promise<boolean> {
        return this.refAdvanceLive(entryHash, []);
    }

    // `chain` holds the advances whose verdicts wait on this one. Strict
    // domination rules out a cycle; the check makes one fail instead of
    // waiting on its own promise.
    private refAdvanceLive(entryHash: B64Hash, chain: B64Hash[]): Promise<boolean> {
        if (chain.includes(entryHash)) {
            return Promise.reject(new Error(`ref-advance '${entryHash}' depends on its own verdict`));
        }
        let verdict = this.refAdvanceVerdicts.get(entryHash);
        if (verdict === undefined) {
            verdict = this.decideRefAdvance(entryHash, [...chain, entryHash]);
            this.refAdvanceVerdicts.set(entryHash, verdict);
        }
        return verdict;
    }

    // The set's ref-advance rule, as admission applies it, at the advance's own
    // cut (Layer 1): the version it publishes, widened by the live concurrent
    // ref-advances strictly above it, read from the geometric fold at `from`.
    // A revoke of the author concurrent with that cut and visible from `from`
    // voids it.
    private async decideRefAdvance(entryHash: B64Hash, chain: B64Hash[]): Promise<boolean> {
        const dag = await this.target.getScopedDag();
        const entry = await dag.loadEntry(entryHash);
        if (entry === undefined) throw new Error(`ref-advance '${entryHash}' is not in the set`);
        if (!isAuthoredPayload(entry.payload)) return false;

        const rcap = await this.target.loadRCap();
        if (rcap === undefined) throw new Error("Cannot load referenced RCap");
        const author = extractAuthor(entry.payload)!;
        if (this.target.refAdvanceCreators() && rcap.isCreator(author)) return true;
        const caps = this.target.refAdvanceCaps();
        if (caps.length === 0) return false;

        const refId = this.target.capabilityRef()!;
        const refAt = await widenRefAtUpward(
            dag, refId, version(entryHash), this.from, await rcap.getCausalDag(),
            (hash) => this.refAdvanceLive(hash, chain),
        );
        const rcapView = await rcap.getView(refAt, await this.refFrom(dag, rcap, refId));
        for (const cap of caps) {
            if (await rcapView.hasCapability(author, cap)) return true;
        }
        return false;
    }

    private async isEntryAuthorized(payload: json.Literal, rcapView: RCapView): Promise<boolean> {
        const p = payload as unknown as SetPayload;

        if (p['action'] === 'create') return true;

        const capName = p['action'] === 'add'
            ? this.target.capRequirementForAdd()
            : this.target.capRequirementForDelete();

        if (capName === undefined) return true;

        if (!isAuthoredPayload(payload)) return false;

        const authorId = extractAuthor(payload)!;
        return rcapView.hasCapability(authorId, capName);
    }

    async checkEntryAuthorization(entryHash: B64Hash): Promise<boolean> {
        if (!this.target.isPermissioned()) return true;
        const dag = await this.target.getScopedDag();
        const rcap = await this.target.loadRCap();
        if (rcap === undefined) throw new Error("Cannot load referenced RCap");
        const refId = this.target.capabilityRef()!;
        const entry = await dag.loadEntry(entryHash);
        if (entry === undefined) return false;
        const rcapView = await this.rcapViewForEntry(dag, rcap, refId, entryHash);
        return this.isEntryAuthorized(entry.payload, rcapView);
    }

    async getReferences(): Promise<B64Hash[]> {
        const ref = this.target.capabilityRef();
        if (ref === undefined) return [];
        return [ref];
    }

    async resolveRefVersion(refId: B64Hash): Promise<Version> {
        if (refId !== this.target.capabilityRef()) {
            throw new Error("Unknown reference: " + refId);
        }

        const dag = await this.target.getScopedDag();
        const resolved = await resolveRefVersionAtPosition(dag, refId, this.at, this.from);
        const rcap = await this.target.loadRCap();
        if (rcap === undefined) throw new Error("Cannot load referenced RCap");
        return canonicalVersion(await rcap.getCausalDag(), resolved);
    }

    async loadRObjectByHash(elementHash: B64Hash): Promise<RObject | undefined> {

        if (this.target.contentType() === undefined) {
            throw new Error("RSetView.getRObjectByHash is not supported when RSet has no contentType");
        }

        const innerFactory = await this.target.getContext().getRegistry().lookup(this.target.contentType()!);
        return innerFactory.loadObject(elementHash, this.target.getContext(), { parent: this.target });
    }
}
