// RTableGroupView: a consistent snapshot of all member tables at one group
// position. The schema is resolved eagerly when the view is built (mirrors
// RSchemaViewImpl holding a resolved SchemaState), so getSchemaView() is
// synchronous; member table views are anchored at THIS view's (at, from) —
// the cohesion guarantee.

import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { Version } from "@hyper-hyper-space/hhs3_mvt";

import type { RSchemaView } from "../rschema/interfaces.js";
import type { RTable, RTableView } from "../rtable/interfaces.js";
import type { RTableGroup, RTableGroupView } from "./interfaces.js";

// What the view needs from its group beyond the public contract.
export type GroupViewTarget = RTableGroup & {
    makeTable(name: string): RTable;
    resolveSchemaVersion(at: Version, from?: Version): Promise<Version>;
    resolveObservedForeignVersion(groupId: B64Hash, at: Version, from: Version): Promise<Version>;
};

export class RTableGroupViewImpl implements RTableGroupView {

    private target: GroupViewTarget;
    private at: Version;
    private from: Version;
    private schemaVersion: Version;
    private schemaView: RSchemaView;

    constructor(target: GroupViewTarget, at: Version, from: Version, schemaVersion: Version, schemaView: RSchemaView) {
        this.target = target;
        this.at = at;
        this.from = from;
        this.schemaVersion = schemaVersion;
        this.schemaView = schemaView;
    }

    getObject(): RTableGroup {
        return this.target;
    }

    getVersion(): Version {
        return this.at;
    }

    getFromVersion(): Version {
        return this.from;
    }

    async getReferences(): Promise<B64Hash[]> {
        return [this.target.getSchemaRef(), ...Object.values(this.target.getBindings())];
    }

    async resolveRefVersion(refId: B64Hash): Promise<Version> {
        if (refId === this.target.getSchemaRef()) {
            return this.schemaVersion;
        }
        if (Object.values(this.target.getBindings()).includes(refId)) {
            // The observed foreign version of a bound group at this view's
            // position — the same resolveRefVersionAtPosition source that
            // cross-group FK / exists resolution uses on the group DAG.
            return this.target.resolveObservedForeignVersion(refId, this.at, this.from);
        }
        throw new Error("Unknown reference: " + refId);
    }

    // The effective schema at this position

    getSchemaVersion(): Version {
        return this.schemaVersion;
    }

    getSchemaView(): RSchemaView {
        return this.schemaView;
    }

    getTableNames(): string[] {
        return this.schemaView.getTableNames();
    }

    // Member table view pinned at this view's position: all tables observed
    // at the same snapshot.
    async getTableView(name: string): Promise<RTableView> {
        if (!this.schemaView.hasTable(name)) {
            throw new Error(`Table '${name}' does not exist in the effective schema at this position`);
        }
        const table = this.target.makeTable(name);
        return table.getView(this.at, this.from);
    }
}
