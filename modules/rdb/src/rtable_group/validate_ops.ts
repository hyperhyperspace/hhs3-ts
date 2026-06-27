// Semantic (position-dependent) validation for RTableGroup payloads, layered
// on top of the format checks in validate.ts.
//
//   create       - format + the pinned RSchema resolves (a MISSING schema
//                  object or binding target is an infrastructure error:
//                  throw, never `false`) + every initial row validates as an
//                  insert against the pinned schema.
//   row          - format + table exists in the effective schema at `at` +
//                  the op conforms to it + identity rules: rowIds are
//                  write-once (an insert is valid only if NO op for its
//                  rowId — insert or delete — is at or below `at`, so a
//                  deleted rowId can never be re-inserted); updates and
//                  deletes need a live row + the row restriction passes at the
//                  parent frontier `(at, at)`.
//   ref-advance  - EITHER the schema deploy (refId is the group's schema ref:
//                  monotonic against the schema DAG, at or above the pinned
//                  version; when authored, the signature is verified at
//                  validation and canDeploy is evaluated in 'object' context
//                  against the verified author; PLUS the one-time add-fk
//                  prerequisite — a deploy whose newly-added/retargeted FK
//                  would strand an existing live row at `at` is hard-rejected,
//                  since FK reach is at-use and would otherwise leave the old
//                  row live-but-dangling) OR a foreign-group observation (refId
//                  is a bound group: monotonic against that group's DAG, no
//                  deploy gate). Any other refId is unknown (false).
//   bundle       - format + every table exists + per-op schema conformance +
//                  per-rowId uniqueness across the bundle + identity/liveness
//                  and restrictions at the PRE-state (`at`) + FK checks at each
//                  op's sequential cut (`at` plus the inserts before it in
//                  bundle order, minus the deletes before it).
//
// FK write-time checks (insert/update ops carrying FK columns): the named
// target must be live. A local target resolves on a sibling table at `at`
// (sequential cut inside a bundle); a `group.table` target resolves through
// the bound foreign group at the foreign version observed at `at` (an absent
// foreign table is a missing reference, so the dangling write is rejected).
// At-use op-voiding (view-time, in computeEntryVoided) handles cases the
// parent-frontier validation check cannot catch: a barrier delete of a
// restriction witness or FK target CONCURRENT with the write voids it at the
// merge (a causally-later delete is inert).

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, KeyId, PublicKey, HashSuite } from "@hyper-hyper-space/hhs3_crypto";
import {
    RContext, ScopedDag, CausalDag, Version, version,
    validationFailure, validationOk, ValidationResult, wrapValidationFailure,
} from "@hyper-hyper-space/hhs3_mvt";
import {
    isRefAdvancePayload, extractRefVersion, validateRefAdvanceMonotonicity, refVersionAtOrAbove,
    extractAuthor, verifyPayloadSignature,
} from "@hyper-hyper-space/hhs3_mvt";
import type { RefAdvancePayload } from "@hyper-hyper-space/hhs3_mvt";

import type { RSchema, RSchemaView } from "../rschema/interfaces.js";
import type { Predicate } from "../rschema/payload.js";
import { splitTableRef } from "../rschema/payload.js";
import { formatPredicate, formatRestrictionFailureReason } from "../rschema/format_predicate.js";
import { collectExistsAtoms } from "../rschema/validate.js";
import { isValidTableRef } from "../rschema/validate.js";
import type { RTable, RTableView } from "../rtable/interfaces.js";
import type { InsertRowPayload, RowOpPayload } from "../rtable/payload.js";
import { validateInsertAgainstSchema, validateRowOpAgainstSchema, validateProviderInsertIntegrity } from "../rtable/validate_ops.js";

import { CreateTableGroupPayload, RowEnvelopePayload, BundlePayload } from "./payload.js";
import { validateTableGroupPayloadFormat } from "./validate.js";
import { evaluatePredicate, evaluateRowOpRestriction } from "./predicates.js";

// What op-mode validation needs from the group (implemented by
// RTableGroupImpl; a structural type to avoid an import cycle).
// The member table as op-validation sees it: the public contract plus the
// write-time base-liveness probe.
export type ValidationTable = RTable & {
    baseHasRow(rowId: B64Hash, at: Version): Promise<boolean>;
};

export type GroupOpHost = {
    getId(): B64Hash;
    getSchemaRef(): B64Hash;
    getPinnedSchemaVersion(): Version;
    getCanDeploy(): Predicate | undefined;
    // The canObserve gate for an observation of `refId` (a bound group id), or
    // undefined when the binding is ungated.
    observeGateFor(refId: B64Hash): Predicate | undefined;
    // Evaluate a binding's canObserve gate in the OBSERVED group's frame at
    // (refAt, refFrom) against `author`. True when ungated.
    evaluateObserveGate(refId: B64Hash, author: KeyId | undefined, refAt: Version, refFrom: Version): Promise<boolean>;
    getBindings(): { [name: string]: B64Hash };
    getIdProvider(): string | undefined;
    getHashSuite(): HashSuite;
    getScopedDag(): Promise<ScopedDag>;
    getSchemaObject(): Promise<RSchema>;
    getForeignGroupCausalDag(groupId: B64Hash): Promise<CausalDag>;
    resolveSchemaView(at: Version, from?: Version): Promise<RSchemaView>;
    // KeyLookup over the selected provider, anchored at a group position,
    // liveness-bypassed. undefined = unresolvable (reject); throws on a missing
    // bound provider object (defer).
    resolveAuthorKey(keyId: KeyId, at: Version): Promise<PublicKey | undefined>;
    makeTable(name: string): ValidationTable;
    // A bound foreign group's member-table view at the resolved foreign
    // version. undefined = unbound name or table absent at that version (the
    // caller treats both as a missing reference); throws if the bound object
    // is not present in the replica.
    resolveForeignTableView(groupName: string, table: string, at: Version, from: Version): Promise<RTableView | undefined>;
};

export type TableGroupValidationContext =
    | { mode: 'create'; ctx: RContext }
    | { mode: 'op'; group: GroupOpHost; at: Version };

export async function validateTableGroupPayload(payload: json.Literal, context: TableGroupValidationContext): Promise<ValidationResult> {
    const formatResult = validateTableGroupPayloadFormat(payload);
    if (!formatResult.valid) return formatResult;

    if (context.mode === 'create') {
        return validateCreate(payload as CreateTableGroupPayload, context.ctx);
    }

    const { group, at } = context;

    if (isRefAdvancePayload(payload)) {
        return validateRefAdvance(payload as RefAdvancePayload, group, at);
    }

    const action = (payload as json.LiteralMap)['action'];

    if (action === 'row') {
        return wrapValidationFailure(
            'row envelope rejected',
            await validateRowEnvelope(payload as RowEnvelopePayload, group, at),
            group.getId(),
        );
    }

    if (action === 'bundle') {
        return wrapValidationFailure(
            `bundle rejected by group '${group.getId()}'`,
            await validateBundle(payload as BundlePayload, group, at),
            group.getId(),
        );
    }

    // 'create' is never applied to an existing group
    return validationFailure("create payload cannot be applied to an existing table group", { objectHash: group.getId() });
}

async function validateCreate(create: CreateTableGroupPayload, ctx: RContext): Promise<ValidationResult> {
    // a missing referenced object is an infrastructure error, never an MVT
    // data condition: throw
    const schemaObj = await ctx.getObject(create.schemaRef);
    if (schemaObj === undefined) {
        throw new Error(`RSchema '${create.schemaRef}' is not present in the replica`);
    }
    const schema = schemaObj as RSchema;

    const pinned = version(...json.fromSet(create.schemaVersion));
    const schemaView = await schema.getView(pinned, pinned);

    const hashSuite = ctx.getHashSuite();

    for (const table of Object.keys(create.initialRows ?? {})) {
        if (!schemaView.hasTable(table)) return validationFailure(`initial row table '${table}' does not exist in pinned schema`);

        const seenRowIds = new Set<B64Hash>();
        for (const [index, row] of create.initialRows![table].entries()) {
            const insert = row as InsertRowPayload;
            if (seenRowIds.has(insert.rowId)) return validationFailure(`duplicate initial rowId '${insert.rowId}' in table '${table}'`);
            seenRowIds.add(insert.rowId);
            const schemaResult = validateInsertAgainstSchema(insert, schemaView, table);
            if (!schemaResult.valid) {
                return wrapValidationFailure(`initial row ${index} in table '${table}' does not match schema`, schemaResult);
            }
            // genesis rows of a provider table must be self-certifying too
            const providerResult = validateProviderInsertIntegrity(insert, schemaView, table, hashSuite);
            if (!providerResult.valid) {
                return wrapValidationFailure(`initial provider row ${index} in table '${table}' is not self-certifying`, providerResult);
            }
        }
    }

    for (const [name, target] of Object.entries(create.bindings ?? {})) {
        const bound = await ctx.getObject(target);
        if (bound === undefined) {
            throw new Error(`Binding '${name}' -> '${target}' is not present in the replica`);
        }
    }

    // Binding-time validation, name-resolvability only: every qualified
    // (group.table) FK / exists target named by the pinned schema must have
    // its group-name declared in `bindings`. Whether that bound group's
    // schema currently has the table/columns is NOT checked here — a missing
    // foreign table/column is a runtime data condition (an FK write against it
    // is voided at-use; an `exists` over it is false), not a create rejection.
    // (The schema itself cannot check this: it is shared across group instances
    // and knows no bindings.)
    const bindings = create.bindings ?? {};
    for (const groupName of qualifiedTargetGroups(schemaView)) {
        if (!Object.prototype.hasOwnProperty.call(bindings, groupName)) {
            return validationFailure(`schema target group '${groupName}' is not bound`);
        }
    }

    // every canObserve gate must key a DECLARED binding name: the gate is
    // attributed to an observation through the binding's id, so an entry for a
    // non-bound name could never be reached (and signals a malformed create).
    for (const groupName of Object.keys(create.canObserve ?? {})) {
        if (!Object.prototype.hasOwnProperty.call(bindings, groupName)) {
            return validationFailure(`canObserve gate names group '${groupName}' which is not bound`);
        }
    }

    // idProvider selection: a LOCAL provider must exist in the pinned schema and
    // be flagged idProvider; a qualified 'group.table' provider is
    // name-resolvability only (its group must be bound — the foreign table being
    // a provider is checked at runtime, fail-closed).
    if (create.idProvider !== undefined) {
        if (!isValidTableRef(create.idProvider)) return validationFailure(`idProvider '${create.idProvider}' is not a valid table reference`);
        const [groupName, table] = splitTableRef(create.idProvider);
        if (groupName === undefined) {
            if (!schemaView.hasTable(table)) return validationFailure(`idProvider table '${table}' does not exist`);
            if (schemaView.getIdProvider(table) === undefined) return validationFailure(`table '${table}' is not an identity provider`);
        } else {
            if (!Object.prototype.hasOwnProperty.call(bindings, groupName)) {
                return validationFailure(`idProvider group '${groupName}' is not bound`);
            }
        }
    }

    return validationOk();
}

// The distinct group-names of all qualified (group.table) FK and exists
// targets in the effective schema.
function qualifiedTargetGroups(schemaView: RSchemaView): Set<string> {
    const groups = new Set<string>();

    for (const table of schemaView.getTableNames()) {
        for (const target of Object.values(schemaView.getFKs(table))) {
            const [group] = splitTableRef(target);
            if (group !== undefined) groups.add(group);
        }

        for (const restriction of schemaView.getTable(table)?.restrictions ?? []) {
            for (const atom of collectExistsAtoms(restriction.rule)) {
                const [group] = splitTableRef(atom.table);
                if (group !== undefined) groups.add(group);
            }
        }
    }

    return groups;
}

// AUTHENTICATION at validation (the op's own (at, at) position). A bad or
// present-but-unresolvable signature is a HARD REJECT (false): the op is
// discarded, never a prev, never gossiped. A missing bound provider OBJECT
// throws out of resolveAuthorKey (the sync layer defers, then revalidates). An
// unauthored op is validly anonymous. A group with no idProvider performs no
// authentication — the claimed author is trusted (configure an idProvider to
// make authorship sound). Verdict is monotone, so the view-time `from` never
// refines it (computeEntryVoided then TRUSTS op.author).
async function verifyOpAuthorship(op: json.Literal, group: GroupOpHost, at: Version): Promise<ValidationResult> {
    const author = extractAuthor(op);
    if (author === undefined) return validationOk();                   // anonymous: nothing to verify
    if (group.getIdProvider() === undefined) return validationOk();    // no authentication configured
    return await verifyPayloadSignature(op as json.LiteralMap, (keyId) => group.resolveAuthorKey(keyId, at))
        ? validationOk()
        : validationFailure(`signature from author '${author}' could not be verified`);
}

// Authorization is a hard validation gate at the parent frontier. Bundle
// sibling writes do not authorize each other; grants must causally precede the
// op they authorize.
async function validateRowOpRestrictionAt(
    op: RowOpPayload,
    table: string,
    schemaView: RSchemaView,
    group: GroupOpHost,
    at: Version,
): Promise<ValidationResult> {
    const rule = schemaView.getRestriction(table, op.action);
    const ok = await evaluateRowOpRestriction(op, table, schemaView,
        (targetTable) => group.makeTable(targetTable).getView(at, at),
        (groupName, targetTable) => group.resolveForeignTableView(groupName, targetTable, at, at),
    );
    return ok
        ? validationOk()
        : validationFailure(formatRestrictionFailureReason(table, op, rule));
}

async function validateRowEnvelope(envelope: RowEnvelopePayload, group: GroupOpHost, at: Version): Promise<ValidationResult> {
    const schemaView = await group.resolveSchemaView(at);
    const op = envelope.op as RowOpPayload;

    const schemaResult = validateRowOpAgainstSchema(op, schemaView, envelope.table);
    if (!schemaResult.valid) return wrapValidationFailure(`row op for table '${envelope.table}' does not match schema`, schemaResult);

    // provider content-integrity (self-certifying identity rows)
    const providerResult = validateProviderInsertIntegrity(op, schemaView, envelope.table, group.getHashSuite());
    if (!providerResult.valid) return wrapValidationFailure(`provider row for table '${envelope.table}' is not self-certifying`, providerResult);

    // authentication (validation, at the op's own position)
    const authorshipResult = await verifyOpAuthorship(op, group, at);
    if (!authorshipResult.valid) return authorshipResult;

    // identity / liveness at the op's own position
    const table = group.makeTable(envelope.table);

    if (op.action === 'insert') {
        // rowIds are write-once: any prior op for this rowId (insert OR
        // delete) at or below `at` makes the insert invalid — in particular
        // a deleted rowId can never be re-inserted
        const tableDag = await table.getScopedDag();
        const cover = await tableDag.findCoverWithFilter(at, { containsValues: { rows: [op.rowId] } });
        if (cover.size !== 0) return validationFailure(`rowId '${op.rowId}' already exists or was deleted in table '${envelope.table}'`);
    } else {
        // updates and deletes need an undeleted row at the op's own position
        // (base liveness: an FK-hidden but undeleted row can still be updated)
        if (!await table.baseHasRow(op.rowId, at)) return validationFailure(`rowId '${op.rowId}' is not live in table '${envelope.table}'`);
    }

    // Restrictions must pass at the parent frontier `(at, at)`; concurrent
    // revokes are still handled by view-time rechecks.
    const restrictionResult = await validateRowOpRestrictionAt(op, envelope.table, schemaView, group, at);
    if (!restrictionResult.valid) return restrictionResult;

    // FK targets must be live at `at` (single-op entry: no bundle siblings).
    // A cross-group target resolves through the bound foreign group at the
    // foreign version observed at `at`; an absent foreign table is a missing
    // reference (not live), so the dangling write is rejected.
    return fkTargetsLive(op, envelope.table, schemaView,
        async (ref, rowId) => {
            const [groupName, targetTable] = splitTableRef(ref);
            if (groupName !== undefined) {
                const fv = await group.resolveForeignTableView(groupName, targetTable, at, at);
                return fv !== undefined && fv.hasRow(rowId);
            }
            return (await group.makeTable(targetTable).getView(at, at)).hasRow(rowId);
        });
}

// Every FK column the op carries (or inherits from a default) must name a
// live target row, per `isTargetLive` (the full target ref, local or
// `group.table`). Deletes carry no FK obligations.
async function fkTargetsLive(
    op: RowOpPayload,
    table: string,
    schemaView: RSchemaView,
    isTargetLive: (targetRef: string, rowId: B64Hash) => Promise<boolean>,
): Promise<ValidationResult> {
    if (op.action === 'delete') return validationOk();

    const fks = schemaView.getFKs(table);
    const def = schemaView.getTable(table);

    for (const column of Object.keys(fks)) {
        const value = op.values[column] ?? def?.columns[column]?.default;
        if (value === undefined) continue;             // absent (nullable): unconstrained
        if (typeof value !== 'string') return validationFailure(`FK column '${column}' in table '${table}' must be a rowId string`);

        if (!await isTargetLive(fks[column], value)) {
            return validationFailure(`FK column '${column}' in table '${table}' points to non-live row '${value}' in '${fks[column]}'`);
        }
    }

    return validationOk();
}

// A bundle is a single entry carrying an ordered list of ops across member
// tables. All-or-nothing: any invalid op rejects the whole bundle.
//
//   - every table exists; each op conforms to the schema;
//   - each rowId appears in at most one op across the bundle;
//   - identity / liveness and restrictions at the PRE-state (`at`): write-once
//     inserts, live rows for updates/deletes, authorizing rows already present;
//   - FK checks at each op's SEQUENTIAL CUT: a target is live iff it is live
//     at `at` and not deleted by an earlier op, OR inserted by an earlier op
//     (and not since deleted) — bundle order matters (decision 4).
async function validateBundle(bundle: BundlePayload, group: GroupOpHost, at: Version): Promise<ValidationResult> {
    const schemaView = await group.resolveSchemaView(at);

    const seenRowIds = new Set<B64Hash>();
    // effective insert/delete state contributed by earlier bundle ops, per table
    const insertedBefore = new Map<string, Set<B64Hash>>();
    const deletedBefore = new Map<string, Set<B64Hash>>();

    // Local targets use the sequential-cut overlay (earlier bundle ops);
    // cross-group targets resolve through the bound foreign group (no bundle
    // op can touch a foreign group, so no overlay applies there).
    const isTargetLive = async (targetRef: string, rowId: B64Hash): Promise<boolean> => {
        const [groupName, targetTable] = splitTableRef(targetRef);
        if (groupName !== undefined) {
            const fv = await group.resolveForeignTableView(groupName, targetTable, at, at);
            return fv !== undefined && fv.hasRow(rowId);
        }
        if ((deletedBefore.get(targetTable)?.has(rowId)) ?? false) return false;
        if ((insertedBefore.get(targetTable)?.has(rowId)) ?? false) return true;
        return (await group.makeTable(targetTable).getView(at, at)).hasRow(rowId);
    };

    for (const [index, write] of bundle.writes.entries()) {
        const table = write.table;
        const op = write.op as RowOpPayload;

        const schemaResult = validateRowOpAgainstSchema(op, schemaView, table);
        if (!schemaResult.valid) {
            return wrapValidationFailure(`bundle write ${index} for table '${table}' does not match schema`, schemaResult);
        }

        // provider content-integrity + authentication (each bundle op is signed)
        const providerResult = validateProviderInsertIntegrity(op, schemaView, table, group.getHashSuite());
        if (!providerResult.valid) {
            return wrapValidationFailure(`bundle write ${index} provider row for table '${table}' is not self-certifying`, providerResult);
        }
        const authorshipResult = await verifyOpAuthorship(op, group, at);
        if (!authorshipResult.valid) return wrapValidationFailure(`bundle write ${index} has invalid authorship`, authorshipResult);

        if (seenRowIds.has(op.rowId)) {
            return validationFailure(`bundle writes rowId '${op.rowId}' more than once`);
        }
        seenRowIds.add(op.rowId);

        const tbl = group.makeTable(table);
        if (op.action === 'insert') {
            const tableDag = await tbl.getScopedDag();
            const cover = await tableDag.findCoverWithFilter(at, { containsValues: { rows: [op.rowId] } });
            if (cover.size !== 0) return validationFailure(`bundle insert rowId '${op.rowId}' already exists or was deleted in table '${table}'`);
        } else {
            if (!await tbl.baseHasRow(op.rowId, at)) return validationFailure(`bundle op rowId '${op.rowId}' is not live in table '${table}'`);
        }

        // Authorization reads only the parent frontier; sibling writes in this
        // bundle can satisfy FK targets, but not permission predicates.
        const restrictionResult = await validateRowOpRestrictionAt(op, table, schemaView, group, at);
        if (!restrictionResult.valid) return wrapValidationFailure(`bundle write ${index} failed restrictions`, restrictionResult);

        const fkResult = await fkTargetsLive(op, table, schemaView, isTargetLive);
        if (!fkResult.valid) return wrapValidationFailure(`bundle write ${index} failed FK checks`, fkResult);

        if (op.action === 'insert') {
            (insertedBefore.get(table) ?? setInMap(insertedBefore, table)).add(op.rowId);
        } else if (op.action === 'delete') {
            (deletedBefore.get(table) ?? setInMap(deletedBefore, table)).add(op.rowId);
        }
    }

    return validationOk();
}

function setInMap(map: Map<string, Set<B64Hash>>, key: string): Set<B64Hash> {
    const set = new Set<B64Hash>();
    map.set(key, set);
    return set;
}

// A ref-advance is either THE schema deploy (the group's own schema ref,
// canDeploy-gated) or a foreign-group observation (a bound group's id, no
// deploy gate). Both are barriers. Any other refId is unknown.
async function validateRefAdvance(payload: RefAdvancePayload, group: GroupOpHost, at: Version): Promise<ValidationResult> {
    if (payload.refId === group.getSchemaRef()) {
        return validateDeploy(payload, group, at);
    }

    // foreign-group observation (cross-group FK / exists). The refId must be a
    // bound group; advancing observation needs no deploy authority unless the
    // binding declares a canObserve gate. A missing bound-group object is an
    // infrastructure error (throw, in getForeignGroupCausalDag), never a `false`.
    const boundIds = new Set(Object.values(group.getBindings()));
    if (!boundIds.has(payload.refId)) return validationFailure(`ref '${payload.refId}' is neither the schema nor a bound foreign group`);

    const foreignCausalDag = await group.getForeignGroupCausalDag(payload.refId);
    const newRefVersion = extractRefVersion(payload);
    const groupDag = await group.getScopedDag();
    if (!await validateRefAdvanceMonotonicity(groupDag, foreignCausalDag, payload.refId, newRefVersion, at)) {
        return validationFailure(`ref-advance for bound group '${payload.refId}' is not monotonic`);
    }

    return validateObserveGate(payload, group, at, newRefVersion);
}

// canObserve gate at write-admission (mirrors validateDeploy's canDeploy block):
// when the observed binding declares a gate, the observation must be authored,
// its signature must verify (when a provider is configured), and the gate must
// hold in the OBSERVED group's frame AT THE IMPORTED VERSION (refAt = refFrom =
// newRefVersion): "was the author authorized in G at the version they import".
// At-use voiding (computeEntryVoided) then catches a back-dated observation a
// later concurrent revoke retroactively unauthorizes.
async function validateObserveGate(
    payload: RefAdvancePayload, group: GroupOpHost, at: Version, newRefVersion: Version,
): Promise<ValidationResult> {
    if (group.observeGateFor(payload.refId) === undefined) return validationOk();   // ungated binding

    const p = payload as unknown as json.LiteralMap;
    const author = extractAuthor(p);
    if (author === undefined) {
        return validationFailure(`observation of gated group '${payload.refId}' must be authored`);
    }

    // AUTHENTICATION at the op's own position (the group's OWN provider), like
    // a deploy: a present-but-unverifiable signature is a hard reject.
    if (group.getIdProvider() !== undefined) {
        if (!await verifyPayloadSignature(p, (keyId) => group.resolveAuthorKey(keyId, at))) {
            return validationFailure(`observation signature from author '${author}' could not be verified`);
        }
    }

    return await group.evaluateObserveGate(payload.refId, author, newRefVersion, newRefVersion)
        ? validationOk()
        : validationFailure(`canObserve predicate rejected observation of '${bindingNameFor(group, payload.refId)}': ${formatPredicate(group.observeGateFor(payload.refId)!)}`);
}

function bindingNameFor(group: GroupOpHost, refId: B64Hash): string {
    for (const [name, id] of Object.entries(group.getBindings())) {
        if (id === refId) return name;
    }
    return refId;
}

async function validateDeploy(payload: RefAdvancePayload, group: GroupOpHost, at: Version): Promise<ValidationResult> {
    const p = payload as unknown as json.LiteralMap;
    const author = extractAuthor(p);

    // AUTHENTICATION at validation: when the deploy is authored and the group
    // configures a provider (its OWN provider — the deploying group's scope),
    // the signature must verify against the resolved key, else HARD REJECT. A
    // missing bound provider object throws (defer). canDeploy then evaluates
    // $author against the VERIFIED author.
    if (author !== undefined && group.getIdProvider() !== undefined) {
        if (!await verifyPayloadSignature(p, (keyId) => group.resolveAuthorKey(keyId, at))) {
            return validationFailure(`deploy signature from author '${author}' could not be verified`);
        }
    }

    // canDeploy is evaluated against the (now verified) ref-advance author
    // ('object' context: $author only, no subject row). A missing author makes
    // $author-dependent terms unprovable.
    const canDeploy = group.getCanDeploy();
    if (canDeploy !== undefined) {
        const ok = await evaluatePredicate(canDeploy, {
            getTableView: async (table) => group.makeTable(table).getView(at, at),
            getForeignTableView: (groupName, table) => group.resolveForeignTableView(groupName, table, at, at),
            author,
            context: 'object',
        });
        if (!ok) return validationFailure(`canDeploy predicate rejected schema deploy: ${formatPredicate(canDeploy)}`);
    }

    const newRefVersion = extractRefVersion(payload);
    const schema = await group.getSchemaObject();
    const schemaCausalDag = await schema.getCausalDag();

    // never deploy below the pinned genesis version
    if (!await refVersionAtOrAbove(schemaCausalDag, newRefVersion, group.getPinnedSchemaVersion())) {
        return validationFailure("schema deploy is below the pinned schema version");
    }

    const groupDag = await group.getScopedDag();
    if (!await validateRefAdvanceMonotonicity(groupDag, schemaCausalDag, payload.refId, newRefVersion, at)) {
        return validationFailure("schema deploy ref-advance is not monotonic");
    }

    return validateAddFkPrerequisite(group, at, newRefVersion);
}

// add-fk PREREQUISITE (one-time, hard reject): a deploy that newly enforces an
// FK must not strand existing data. For each FK added or retargeted by this
// deploy (vs the currently-deployed schema), every row live at `at` whose
// (pre-existing) FK column carries a value must reach a live target at `at`.
// Rationale: FK reach is at-use, so once deployed the FK is inert for these old
// rows (a causally-later target delete never voids them) — this is the single
// point-in-time consistency check that the data honored the FK when adopted.
// New columns hold no old explicit values (only the uniform schema default, a
// schema-level effect), so they are not enumerated here.
async function validateAddFkPrerequisite(group: GroupOpHost, at: Version, newRefVersion: Version): Promise<ValidationResult> {
    const oldSchema = await group.resolveSchemaView(at);

    const schema = await group.getSchemaObject();
    const newVersion = version(...group.getPinnedSchemaVersion());
    for (const hash of newRefVersion) newVersion.add(hash);
    const newSchema = await schema.getView(newVersion, newVersion);

    const isTargetLive = async (targetRef: string, rowId: B64Hash): Promise<boolean> => {
        const [groupName, targetTable] = splitTableRef(targetRef);
        if (groupName !== undefined) {
            const fv = await group.resolveForeignTableView(groupName, targetTable, at, at);
            return fv !== undefined && fv.hasRow(rowId);
        }
        return (await group.makeTable(targetTable).getView(at, at)).hasRow(rowId);
    };

    for (const table of newSchema.getTableNames()) {
        const newFks = newSchema.getFKs(table);
        const oldFks = oldSchema.hasTable(table) ? oldSchema.getFKs(table) : {};
        const addedColumns = Object.keys(newFks).filter((c) => oldFks[c] !== newFks[c]);
        if (addedColumns.length === 0) continue;

        const view = await group.makeTable(table).getView(at, at);
        for (const rowId of await view.liveRowIds()) {
            const row = await view.getRow(rowId);
            if (row === undefined) continue;
            for (const column of addedColumns) {
                const value = row.values[column];
                if (value === undefined) continue;        // nullable / absent: unconstrained
                if (typeof value !== 'string') {
                    return validationFailure(`existing FK column '${column}' in table '${table}' is not a rowId string`);
                }
                if (!await isTargetLive(newFks[column], value)) {
                    return validationFailure(`deploy would strand row '${rowId}' in table '${table}' on FK '${column}' -> '${newFks[column]}'`);
                }
            }
        }
    }

    return validationOk();
}
