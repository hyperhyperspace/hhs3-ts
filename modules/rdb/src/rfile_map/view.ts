// RFileMapView: the elements present at View(at, from).

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { dag } from "@hyper-hyper-space/hhs3_dag";
import { Version } from "@hyper-hyper-space/hhs3_mvt";

import type { ListedFile, RFileMap, RFileMapView } from "./interfaces.js";
import { FileElement, elementIdOf } from "./payload.js";
import { isPresent, listPresent } from "./presence.js";

export class RFileMapViewImpl implements RFileMapView {

    constructor(
        private readonly target: RFileMap,
        private readonly rawDag: dag.Dag,
        private readonly at: Version,
        private readonly from: Version,
    ) {}

    getObject(): RFileMap { return this.target; }
    getVersion(): Version { return this.at; }
    getFromVersion(): Version { return this.from; }

    has(element: FileElement): Promise<boolean> {
        return isPresent(this.rawDag, elementIdOf(element), this.at, this.from);
    }

    list(): Promise<ListedFile[]> {
        return listPresent(this.rawDag, this.at, this.from);
    }

    async getReferences(): Promise<B64Hash[]> {
        return [this.target.getGroupId()];
    }

    async resolveRefVersion(refId: B64Hash): Promise<Version> {
        if (refId !== this.target.getGroupId()) throw new Error(`Unknown reference: ${refId}`);
        return this.target.resolveObservedGroupVersion(this.at, this.from);
    }
}
