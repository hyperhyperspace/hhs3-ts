// Public RTableGroup interfaces.
//
// An RTableGroup owns one physical DAG: it is the unit of atomicity, snapshot,
// observation and composition. Member RTables live on scoped projections of
// this DAG (NestingParent), so one group position is a consistent snapshot of
// all member tables.

import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { NestingParent, RObject, Version, View } from "@hyper-hyper-space/hhs3_mvt";

import type { Predicate } from "../rschema/payload.js";
import type { RSchemaView } from "../rschema/interfaces.js";
import type { RTable, RTableView } from "../rtable/interfaces.js";
import type { RowOpPayload } from "../rtable/payload.js";
import type { RTableGroupDeltaStrategy } from "./delta.js";

// One write in a bundle: a row op tagged with its target table. The bundle
// carries these in order (the bundle order; see RTableGroup.bundle).
export type BundleWrite = {
    table: string;
    op: RowOpPayload;
};

export interface RTableGroup extends RObject, NestingParent {
    // Create-time facts
    seed(): string;
    hashAlgorithm(): string | undefined;
    getSchemaRef(): B64Hash;
    getPinnedSchemaVersion(): Version;
    getBindings(): { [name: string]: B64Hash };
    getCanDeploy(): Predicate | undefined;
    // The selected identity provider (local table name or 'group.table'), or
    // undefined if the group performs no authentication.
    getIdProvider(): string | undefined;

    // Member access: loads the nested RTable. The table must exist in the
    // effective schema at the group frontier; throws otherwise.
    getTable(name: string): Promise<RTable>;

    // THE deploy moment: barrier ref-advance of the schema ref to refVersion.
    // When authored, the deploy signature is verified at validation and the
    // group's canDeploy predicate is evaluated against the verified author.
    deploy(refVersion: Version, author?: OwnIdentity, at?: Version): Promise<B64Hash>;

    // Observe a bound foreign group at refVersion: a BARRIER ref-advance of
    // that group's id, making its rows visible to cross-group FK /
    // exists targets (group.table) up to refVersion. A concurrent observation
    // revises the merged frontier, so a concurrent foreign revoke / deploy
    // voids a concurrent cross-group use there. `group` is a binding name or a
    // bound group id. No deploy authority is required to advance observation.
    observe(group: string | B64Hash, refVersion: Version, at?: Version): Promise<B64Hash>;

    // Single-entry atomic multi-table write. `writes` is ordered (the bundle
    // order): op i's FK conditions are checked at the sequential cut of `at`
    // and the earlier ops. The whole entry validates and applies atomically.
    bundle(writes: BundleWrite[], author?: OwnIdentity, at?: Version): Promise<B64Hash>;

    getView(at?: Version, from?: Version): Promise<RTableGroupView>;

    // Delta strategy: 'bounded' (default; projected-meet walk) or 'full'
    // (genesis recompute, the reference implementation). See computeDelta /
    // src/rtable_group/delta.ts.
    setDeltaStrategy(strategy: RTableGroupDeltaStrategy): void;
}

// A consistent snapshot of all member tables at one group position.
export interface RTableGroupView extends View {
    getObject(): RTableGroup;

    // The effective schema at this position: the RSchema resolved at the
    // ref-advance-resolved version (pinned version ∪ ref-advances at or
    // below the position). getSchemaView() is synchronous: the group view
    // resolves the schema eagerly when it is built.
    getSchemaVersion(): Version;
    getSchemaView(): RSchemaView;
    getTableNames(): string[];

    // Member table view anchored at THIS view's position: all tables
    // observed at the same snapshot (the cohesion guarantee).
    getTableView(name: string): Promise<RTableView>;
}
