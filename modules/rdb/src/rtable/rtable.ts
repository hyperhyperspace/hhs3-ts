// RTable: one member table of an RTableGroup, as a proper nested RObject
// living on a scoped projection of the group's DAG (the nested-RSet-element
// pattern). Tables exist by schema — there is no creation op and no factory:
// tableId = deriveTableId(groupId, tableName), and tables are only
// constructed through their group (group.getTable), never via the registry.
//
// ACTIONS (see payload.ts for formats; ops never carry the table name — the
// group-level envelope tags it):
//
//   insert
//     Creates a row. Carries: `rowId` (must equal deriveRowId(uuid, author)),
//     `uuid`, column `values`, optional author/signature. Values of
//     `pub` columns are exported to entry meta (derived, re-checked at
//     validation).
//
//   update
//     Partial row update: `rowId` + only the changed `values` (per-field
//     last-writer-wins, deterministic entry-hash tiebreak for concurrent
//     writers). The row must be live at the op's position. `readonly`
//     columns are fixed at insert and cannot be updated; `pub` columns CAN
//     be updated (independent modifiers) and export their new values to
//     entry meta. Optional authoring. Empty `values` rejected.
//
//   delete
//     Deletes a row by `rowId`. PERMANENT: rowIds are write-once identities,
//     so a deleted rowId can never be re-inserted ("restoring" means
//     inserting a new row with a fresh uuid). Always barrier-tagged; the
//     concurrentDeletes flag (default true) is resolved at-use at the delete's
//     own position (observed from the view `from`) and governs whether that
//     barrier hides the row at concurrent view positions. Optional authoring.
//
// Preconditions (see ../rschema/payload.ts) are derived from the schema
// only — FKs (column -> table, AT-USE) and restrictions (at-use predicates
// tagged insert / update / delete / all). Never serialized in row op payloads.
//
// Provides insert + update + delete writers, permanent-delete liveness
// (causal + at-use concurrentDeletes barriers), per-field LWW value resolution,
// pub meta export (inserts and updates), views, plus the enforcement layers in
// view.ts. Validation hard-rejects row ops whose restriction predicate fails at
// the parent frontier `(at, at)`. Entries are still rechecked at-use in views:
// a concurrent barrier delete of a witness / FK target voids the use, while a
// causally-later one does not. Voided ops are invisible (a voided insert never
// lives; a voided FK-update reverts via LWW); FK reference cycles resolve to
// DENY. Cross-group FK / exists targets resolve through bound foreign groups.
// When the group selects an identity provider, an authored op's signature is
// verified AT VALIDATION (hard reject / defer), so view-time evaluation trusts
// op.author. Write-time validation rejects dangling FKs (local and cross-group);
// the subject-row liveness check for update/delete is BASE delete-state only
// (an undeleted row stays repairable). Delta is the group's (a table never leads
// one): a member table contributes a per-table row-change accumulator
// (createDeltaAccumulator -> ./delta.ts) that the group routes walked entries
// to; computeDelta throws here.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { KeyId, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";

import {
    Payload, Version, ForeignDep, Event, Delta, DeltaAccumulator,
    formatValidationFailure, ValidationRejectedError, ValidationResult,
} from "@hyper-hyper-space/hhs3_mvt";
import type { ScopedDag, CausalDag } from "@hyper-hyper-space/hhs3_mvt";
import { signPayload as signPayloadHelper } from "@hyper-hyper-space/hhs3_mvt";

import type { RSchemaView } from "../rschema/interfaces.js";
import { deriveRowOpInnerMeta } from "../rtable_group/scopes.js";
import type { RowEnvelopePayload } from "../rtable_group/payload.js";

import type { RTable as RTableContract, RTableView as RTableViewContract, RowValues } from "./interfaces.js";
import { InsertRowPayload, UpdateRowPayload, DeleteRowPayload, RowOpPayload } from "./payload.js";
import { deriveRowId } from "./hash.js";
import { RTableViewImpl } from "./view.js";
import { RTableDeltaAccumulator } from "./delta.js";

export const RTABLE_TYPE_ID = 'hhs/rtable_v1';

// What an RTable needs from its group (implemented by RTableGroupImpl; a
// structural type to avoid an import cycle).
export type TableGroupHost = {
    getId(): B64Hash;
    getBackendLabel(): string;
    getScopedDag(): Promise<ScopedDag>;
    getScopedDagForChild(childId: B64Hash): Promise<ScopedDag>;
    getCausalDag(): Promise<CausalDag>;
    resolveSchemaView(at: Version, from?: Version): Promise<RSchemaView>;
    selfValidate(): boolean;
    validatePayload(payload: Payload, at: Version): Promise<ValidationResult>;
    makeTable(name: string): RTableImpl;
    isEntryVoided(entryHash: B64Hash, from: Version): Promise<boolean>;
    resolveForeignTableView(
        group: string, table: string, at: Version, from: Version,
    ): Promise<RTableViewContract | undefined>;
};

export class RTableImpl implements RTableContract {

    static typeId = RTABLE_TYPE_ID;

    private tableId: B64Hash;
    private tableName: string;
    private group: TableGroupHost;

    private _scopedDag: ScopedDag | undefined;

    constructor(tableId: B64Hash, tableName: string, group: TableGroupHost) {
        this.tableId = tableId;
        this.tableName = tableName;
        this.group = group;
    }

    getId(): B64Hash { return this.tableId; }
    getType(): string { return RTableImpl.typeId; }
    getBackendLabel(): string { return this.group.getBackendLabel(); }

    getTableName(): string { return this.tableName; }
    getGroupId(): B64Hash { return this.group.getId(); }

    // The group resolves the effective schema (pinned version ∪ ref-advances);
    // versions are identity across nesting, so the table's positions are
    // valid group positions.
    async resolveSchemaView(at: Version, from?: Version): Promise<RSchemaView> {
        return this.group.resolveSchemaView(at, from);
    }

    // A cross-group FK / exists target: the group resolves it through the
    // bound foreign group at the observed foreign version (see group.ts).
    resolveForeignTableView(
        group: string, table: string, at: Version, from: Version,
    ): Promise<RTableViewContract | undefined> {
        return this.group.resolveForeignTableView(group, table, at, from);
    }

    // Entry voiding is the group's computation (an entry may carry ops for
    // several tables and voids as a unit); see isEntryVoided in group.ts.
    isEntryVoided(entryHash: B64Hash, from: Version): Promise<boolean> {
        return this.group.isEntryVoided(entryHash, from);
    }

    // Write-time identity check: base delete-state liveness (no FK reach, no
    // view-time restriction recheck — see view.ts). An undeleted but FK-hidden
    // row is still a valid update target (it can be repaired).
    async baseHasRow(rowId: B64Hash, at: Version): Promise<boolean> {
        return new RTableViewImpl(this, at, at).hasRowBase(rowId);
    }

    // Row writers. `at` defaults to the GROUP frontier: by default a write
    // extends the group's consistent snapshot (not just this table's scope).

    async insert(uuid: string, values: RowValues, author?: OwnIdentity, at?: Version): Promise<B64Hash> {
        at = at ?? await (await this.group.getScopedDag()).getFrontier();

        const base: InsertRowPayload = {
            action: 'insert',
            rowId: deriveRowId(uuid, author?.keyId),
            uuid,
            values,
        };

        const payload = author !== undefined
            ? await signPayloadHelper(base as unknown as json.LiteralMap, author) as unknown as InsertRowPayload
            : base;

        return this.appendRowOp(payload, at);
    }

    async update(rowId: B64Hash, values: RowValues, author?: OwnIdentity, at?: Version): Promise<B64Hash> {
        at = at ?? await (await this.group.getScopedDag()).getFrontier();

        const base: UpdateRowPayload = { action: 'update', rowId, values };
        const payload = author !== undefined
            ? await signPayloadHelper(base as unknown as json.LiteralMap, author) as unknown as UpdateRowPayload
            : base;

        return this.appendRowOp(payload, at);
    }

    async delete(rowId: B64Hash, author?: OwnIdentity, at?: Version): Promise<B64Hash> {
        at = at ?? await (await this.group.getScopedDag()).getFrontier();

        const base: DeleteRowPayload = { action: 'delete', rowId };
        const payload = author !== undefined
            ? await signPayloadHelper(base as unknown as json.LiteralMap, author) as unknown as DeleteRowPayload
            : base;

        return this.appendRowOp(payload, at);
    }

    // Append one row op through the table's scoped DAG: the TableScope wraps
    // it into a row envelope, wraps the inner meta (t-<table>- prefix, outer
    // `tables` tag, hoisted barrier) and re-checks meta + payload validity.
    private async appendRowOp(op: RowOpPayload, at: Version): Promise<B64Hash> {
        const schemaView = await this.group.resolveSchemaView(at);
        if (!schemaView.hasTable(this.tableName)) {
            throw new Error(`Table '${this.tableName}' does not exist in the effective schema at this position`);
        }

        const result = await this.validatePayload(op, at);
        if (!result.valid) {
            throw new ValidationRejectedError(formatValidationFailure(result.why), result.why);
        }

        const innerMeta = deriveRowOpInnerMeta(op, this.tableName, schemaView);

        const scopedDag = await this.getScopedDag();
        return scopedDag.append(op, innerMeta, at);
    }

    // RObject interface

    async validatePayload(payload: Payload, at: Version): Promise<ValidationResult> {
        const envelope: RowEnvelopePayload = { action: 'row', table: this.tableName, op: payload };
        return this.group.validatePayload(envelope, at);
    }

    async applyPayload(payload: Payload, at: Version): Promise<B64Hash> {
        return this.appendRowOp(payload as RowOpPayload, at);
    }

    // Default horizon is the GROUP frontier, not this table's scoped frontier.
    // The table scope's frontier projects the group tip down through
    // baseFilter (tables=this table), which drops trailing ref-advance
    // barriers (schema deploys, foreign observe ops). Those barriers are
    // exactly what at-use voiding must see: a concurrent cap revoke observed
    // via UPDATE REF lives in a barrier entry above this table's last row
    // write. Anchoring at the group frontier keeps those barriers in scope
    // (the extra non-barrier other-table entries it also drags in are inert
    // under this table's cover queries), and matches what RTableGroup.getView
    // / RTableGroupView.getTableView already pass down.
    async getView(at?: Version, from?: Version): Promise<RTableViewContract> {
        const groupDag = await this.group.getScopedDag();
        const frontier = await groupDag.getFrontier();

        at = at ?? frontier;
        from = from ?? frontier;

        return new RTableViewImpl(this, at, from);
    }

    // A nested table never leads a delta (the group orchestrates bounds + walk);
    // it participates only via createDeltaAccumulator, like a nested RSet.
    async computeDelta(_start: Version, _end: Version): Promise<Delta> {
        throw new Error("computeDelta is not supported on a nested RTable (the group leads the delta)");
    }

    createDeltaAccumulator(start: Version, end: Version): DeltaAccumulator {
        return new RTableDeltaAccumulator(this, start, end);
    }

    extractForeignDeps(_payload: Payload, _at: Version): ForeignDep[] | undefined {
        return undefined;   // foreign deps are the group's concern
    }

    subscribe(_callback: (event: Event) => void): void {
        throw new Error("Method not implemented.");
    }

    unsubscribe(_callback: (event: Event) => void): void {
        throw new Error("Method not implemented.");
    }

    async getScopedDag(): Promise<ScopedDag> {
        if (this._scopedDag === undefined) {
            this._scopedDag = await this.group.getScopedDagForChild(this.tableId);
        }
        return this._scopedDag;
    }

    async getCausalDag(): Promise<CausalDag> {
        return this.group.getCausalDag();
    }

    async destroy(): Promise<void> {
        this._scopedDag = undefined;
    }
}

export { RTableViewImpl } from "./view.js";
