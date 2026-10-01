// RBlobStoreView: the store at a version. Contents are read by chain tail
// (RBlobStore.readFile), so the view only resolves the bound group it observes.

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { resolveRefVersionAtPosition, Version } from "@hyper-hyper-space/hhs3_mvt";

import type { RBlobStore, RBlobStoreView } from "./interfaces.js";

export class RBlobStoreViewImpl implements RBlobStoreView {

    constructor(private readonly target: RBlobStore, private readonly at: Version, private readonly from: Version) {}

    getObject(): RBlobStore { return this.target; }
    getVersion(): Version { return this.at; }
    getFromVersion(): Version { return this.from; }

    async getReferences(): Promise<B64Hash[]> {
        return [this.target.getGroupId()];
    }

    async resolveRefVersion(refId: B64Hash): Promise<Version> {
        if (refId !== this.target.getGroupId()) throw new Error(`Unknown reference: ${refId}`);
        return resolveRefVersionAtPosition(await this.target.getScopedDag(), refId, this.at, this.from);
    }
}
