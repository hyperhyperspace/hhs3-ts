// Public RTableGroup interfaces.
//
// An RTableGroup owns one physical DAG: it is the unit of atomicity, snapshot,
// observation and composition. Member RTables live on scoped projections of
// this DAG (NestingParent), so one group position is a consistent snapshot of
// all member tables.

import type { B64Hash, KeyId, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { ObserveGateOptions } from "./validate_ops.js";
import type { NestingParent, RObject, Version, View } from "@hyper-hyper-space/hhs3_mvt";
import type { MetaProps } from "@hyper-hyper-space/hhs3_dag";
import type { json } from "@hyper-hyper-space/hhs3_json";

import type { Predicate, SchemaCreator } from "../rschema/payload.js";
import type { RSchemaView } from "../rschema/interfaces.js";
import type { RTable, RTableView } from "../rtable/interfaces.js";
import type { RowOpPayload } from "../rtable/payload.js";
import type { RTableGroupDeltaStrategy } from "./delta.js";
import type { OpVoidDetail } from "./op_void.js";
import type { GroupParams, TableMaps } from "./payload.js";

// One write in a bundle: a row op tagged with its target table. The bundle
// carries these in order (the bundle order; see RTableGroup.bundle).
export type BundleWrite = {
    table: string;
    op: RowOpPayload;
};

// An entry built, signed and validated but not appended yet (HIP-29): the
// first of the two steps of a row op, a bundle or an observe. `hash` is the
// entry's hash, which appendPrepared checks the DAG gives it. `table` is set
// for a row op: appendPrepared appends it through that table's scope, which
// wraps it in a row envelope, and `payload` and `meta` are the op's own.
export type PreparedEntry = {
    payload: json.LiteralMap;
    meta: MetaProps;
    at: Version;
    hash: B64Hash;
    table?: string;
};

// `liveReads`: also validate with the bound groups read through the live
// observes only, as the entry's verdict reads them.
export type PrepareOptions = { liveReads?: boolean };

// Thrown by appendPrepared when the DAG gives the appended entry another hash
// than the one prepared.
export class PreparedHashMismatchError extends Error {
    constructor(readonly prepared: B64Hash, readonly appended: B64Hash) {
        super(`the entry appended as '${appended}' was prepared as '${prepared}'`);
        this.name = 'PreparedHashMismatchError';
    }
}

export interface RTableGroup extends RObject, NestingParent {
    // Create-time facts
    getName(): string;
    seed(): string;
    hashAlgorithm(): string | undefined;
    getSchemaRef(): B64Hash;
    getPinnedSchemaVersion(): Version;
    getBindings(): { [name: string]: B64Hash };
    getTableMaps(): TableMaps;
    getParams(): GroupParams;
    resolveGroupParams(at?: Version, from?: Version): Promise<GroupParams>;
    resolveEffectiveParams(at: Version, from?: Version): Promise<GroupParams>;
    resolveForeignTableName(alias: string, schemaTable: string): string;
    resolveForeignTableView(groupName: string, table: string, at: Version, from: Version): Promise<RTableView | undefined>;
    resolveSchemaForeignTableView(groupName: string, schemaTable: string, at: Version, from: Version): Promise<RTableView | undefined>;
    getCanDeploy(): Predicate | undefined;
    // Per-binding gate on foreign-group observations (keyed by binding name),
    // or undefined when the group declares none.
    getCanObserve(): { [binding: string]: Predicate } | undefined;
    // The selected identity provider (local table name or 'group.table'), or
    // undefined for an anonymous group (its row ops and observations must be
    // unauthored).
    getIdProvider(): string | undefined;
    // Embedded deploy signature keys (a key source beside the idProvider).
    getDeployKeys(): SchemaCreator[];
    // The id of this group's replica-local RDeployGate.
    getDeployGateId(): B64Hash;

    // Member access: loads the nested RTable. The table must exist in the
    // effective schema at the group frontier; throws otherwise.
    getTable(name: string): Promise<RTable>;

    // THE deploy moment: barrier ref-advance of the schema ref to refVersion.
    // When authored, the deploy signature is verified at validation and the
    // group's canDeploy predicate is evaluated against the verified author.
    deploy(refVersion: Version, author?: OwnIdentity, at?: Version, params?: GroupParams): Promise<B64Hash>;

    // Whether a deploy must be authored: the group's canDeploy reads $author.
    deployNeedsAuthor(): boolean;

    // Observe a bound foreign group at refVersion: a BARRIER ref-advance of
    // that group's id, making its rows visible to cross-group FK /
    // exists targets (group.table) up to refVersion. A concurrent observation
    // revises the merged frontier, so a concurrent foreign revoke / deploy
    // voids a concurrent cross-group use there. `group` is a binding name or a
    // bound group id. A claimed author must verify through the idProvider.
    // When the binding's canObserve reads $author, the observation must be
    // authored (the gate is verified at validation and re-evaluated at-use).
    observe(group: string | B64Hash, refVersion: Version, author?: OwnIdentity, at?: Version): Promise<B64Hash>;

    // Whether an observation of the bound `group` (a binding name or a bound
    // group id) must be authored: its canObserve gate reads $author.
    observeNeedsAuthor(group: string | B64Hash): boolean;

    // Single-entry atomic multi-table write. `writes` is ordered (the bundle
    // order): op i's FK conditions are checked at the sequential cut of `at`
    // and the earlier ops. The whole entry validates and applies atomically.
    bundle(writes: BundleWrite[], author?: OwnIdentity, at?: Version): Promise<B64Hash>;

    // The two steps of a row op, a bundle and an observe (HIP-29).
    // prepareRowOp, prepareBundle and prepareObserve build the entry, sign it
    // and validate it, and return it with its hash, without appending it; a
    // table's insert, update and delete are prepareRowOp and appendPrepared in
    // a row. prepareObserve always validates. appendPrepared appends a
    // prepared entry, and throws PreparedHashMismatchError when the DAG gives
    // it another hash.
    prepareRowOp(table: string, op: RowOpPayload, author?: OwnIdentity, at?: Version, opts?: PrepareOptions): Promise<PreparedEntry>;
    prepareBundle(writes: BundleWrite[], author?: OwnIdentity, at?: Version, opts?: PrepareOptions): Promise<PreparedEntry>;
    prepareObserve(group: string | B64Hash, refVersion: Version, author?: OwnIdentity, at?: Version, opts?: PrepareOptions): Promise<PreparedEntry>;
    appendPrepared(prepared: PreparedEntry): Promise<B64Hash>;

    // The two readings of the bound group `groupId` at `at` (HIP-29): through
    // every observe, as an op's checks read it, and through the live observes
    // only, as its verdict reads it. They differ only while an observe of it
    // is void.
    resolveObservedForeignVersion(groupId: B64Hash, at: Version, from: Version): Promise<Version>;
    resolveLiveObservedForeignVersion(groupId: B64Hash, at: Version): Promise<Version>;

    // A binding's canObserve gate for `author` importing `options.imported`, as
    // an observe at `options.opPos` is checked: `fold: 'admission'` as
    // validation reads it, `fold: 'use'` as its verdict does. True when the
    // binding is ungated.
    evaluateObserveGate(refId: B64Hash, author: KeyId | undefined, options: ObserveGateOptions): Promise<boolean>;

    getView(at?: Version, from?: Version): Promise<RTableGroupView>;

    // Whether an entry is void (drop-on-void) at its own position observed from `from`.
    isEntryVoided(entryHash: B64Hash, from: Version): Promise<boolean>;

    // Structured void reason at `from`, when the entry is voided there.
    explainEntryVoided(entryHash: B64Hash, from: Version): Promise<OpVoidDetail | undefined>;

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
