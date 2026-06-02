import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { EntryPredicate } from "@hyper-hyper-space/hhs3_dag";
import { version, Version, RObject, resolveRefVersionAtPosition } from "@hyper-hyper-space/hhs3_mvt";
import { ScopedDag } from "@hyper-hyper-space/hhs3_mvt";

import { isAuthoredPayload, extractAuthor } from "../../authorship.js";

import { SetPayload } from "./payload.js";
import { hashElement } from "./hash.js";

import type { RSet, RSetView } from "./interfaces.js";
import type { RCap, RCapView } from "../rcap/interfaces.js";

export class RSetViewImpl<T extends json.Literal> implements RSetView<T> {

    private target: RSet<T>;
    private at: Version;
    private from: Version;

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
                const rcapView = await this.resolveRCapViewForEntry(dag, rcap, refId, hash);
                return this.isEntryAuthorized(entry.payload, rcapView);
            };
        }

        const cover = await dag.findCoverWithFilter(
            this.at,
            { containsValues: { elmts: [elementHash] } },
            predicate,
        );

        for (const hash of cover) {
            const payload = (await dag.loadEntry(hash))!.payload as unknown as SetPayload;
            if (payload['action'] !== 'add' && payload['action'] !== 'create') continue;

            if (!this.target.supportBarrierDelete()) {
                return true;
            }

            const concurrentBarriers = await dag.findConcurrentCoverWithFilter(
                this.from, version(hash),
                { containsValues: { barrier: ['t'], elmts: [elementHash] } },
                predicate,
            );
            if (concurrentBarriers.size === 0) return true;
        }

        if (this.target.supportBarrierAdd()) {
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

        return false;
    }

    // Compositional: rcapAt = which RCap version E is checked against (RSet barrier
    // ref-advances may widen this); rcapFrom = observation frontier for RCap revision.
    private async resolveRCapViewForEntry(
        dag: ScopedDag, rcap: RCap, refId: B64Hash, entryHash: B64Hash,
    ): Promise<RCapView> {
        const rcapAt = await resolveRefVersionAtPosition(dag, refId, version(entryHash), this.from);
        const rcapFrom = await resolveRefVersionAtPosition(dag, refId, this.from, this.from);
        return rcap.getView(rcapAt, rcapFrom) as Promise<RCapView>;
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
        const rcapView = await this.resolveRCapViewForEntry(dag, rcap, refId, entryHash);
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
        return resolveRefVersionAtPosition(dag, refId, this.at, this.from);
    }

    async loadRObjectByHash(elementHash: B64Hash): Promise<RObject | undefined> {

        if (this.target.contentType() === undefined) {
            throw new Error("RSetView.getRObjectByHash is not supported when RSet has no contentType");
        }

        const innerFactory = await this.target.getContext().getRegistry().lookup(this.target.contentType()!);
        return innerFactory.loadObject(elementHash, this.target.getContext(), this.target);
    }
}
