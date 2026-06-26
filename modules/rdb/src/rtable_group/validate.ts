// Format-level validation for RTableGroup payloads (shape only).
//
// Position-dependent validation against DAG state (table existence at the
// resolved schema version, identity/liveness, FK targets, ref-advance
// monotonicity + canDeploy, bundle sequential cut) lives in validate_ops.ts.

import { json } from "@hyper-hyper-space/hhs3_json";
import {
    isRefAdvancePayload, refAdvanceFormat,
    validationFailure, validationOk, ValidationResult, wrapValidationFailure,
} from "@hyper-hyper-space/hhs3_mvt";

import {
    createTableGroupFormat, CreateTableGroupPayload,
    rowEnvelopeFormat, RowEnvelopePayload,
    bundleFormat, BundlePayload,
} from "./payload.js";

import { isValidName, isValidTableRef, validatePredicate } from "../rschema/validate.js";
import { validateRowOpFormat } from "../rtable/validate.js";
import { InsertRowPayload } from "../rtable/payload.js";

export function validateTableGroupPayloadFormat(payload: json.Literal): ValidationResult {
    if (typeof payload !== 'object' || payload === null || Array.isArray(payload)) {
        return validationFailure("table group payload must be an object");
    }

    if (isRefAdvancePayload(payload)) {
        // canonical mvt ref-advance; used for both the RSchema ref (deploy)
        // and foreign group refs (cross-group FKs). Non-strict: deploy-gated
        // ref-advances carry author/signature as extra fields.
        return json.checkFormat(refAdvanceFormat, payload, { strict: false })
            ? validationOk()
            : validationFailure("ref-advance payload format is invalid");
    }

    const action = (payload as json.LiteralMap)['action'];

    if (action === 'create') {
        if (!json.checkFormat(createTableGroupFormat, payload)) return validationFailure("table group create payload format is invalid");
        const create = payload as CreateTableGroupPayload;

        // initial rows are genesis fiat data: structurally valid insert ops,
        // but carrying no authoring (they are given, not validated as ops)
        for (const table of Object.keys(create.initialRows ?? {})) {
            if (!isValidName(table)) return validationFailure(`invalid initial row table name '${table}'`);
            for (const [index, row] of create.initialRows![table].entries()) {
                const result = validateRowOpFormat(row);
                if (!result.valid) return wrapValidationFailure(`initial row ${index} in table '${table}' has invalid format`, result);
                const insert = row as InsertRowPayload;
                if (insert.action !== 'insert') return validationFailure(`initial row ${index} in table '${table}' is not an insert`);
                if (insert.author !== undefined || insert.signature !== undefined) {
                    return validationFailure(`initial row ${index} in table '${table}' must not be authored`);
                }
            }
        }

        // bindings must be INJECTIVE (name -> id is one-to-one): the reverse
        // map (group id -> binding name) has to be well-defined so a ref-advance
        // / observe op (keyed by object id) can be attributed to a single
        // binding name and its gate. A pure structural property of the payload,
        // so the rejection is deterministic and replica-convergent.
        const seenTargets = new Map<string, string>();
        for (const [name, target] of Object.entries(create.bindings ?? {})) {
            if (!isValidName(name)) return validationFailure(`invalid binding name '${name}'`);
            const prior = seenTargets.get(target);
            if (prior !== undefined) {
                return validationFailure(`bindings must be injective: names '${prior}' and '${name}' both bind group id '${target}'`);
            }
            seenTargets.set(target, name);
        }

        // canDeploy runs without a subject row: 'object' context ($row.* rejected)
        if (create.canDeploy !== undefined && !validatePredicate(create.canDeploy, 'object')) {
            return validationFailure("canDeploy predicate is invalid");
        }

        // each canObserve predicate is an 'object'-context gate (no subject row).
        // Binding-name resolvability (every key is a declared binding) is a
        // position-independent payload property checked in validate_ops.ts.
        for (const [binding, pred] of Object.entries(create.canObserve ?? {})) {
            if (!validatePredicate(pred, 'object')) {
                return validationFailure(`canObserve predicate for binding '${binding}' is invalid`);
            }
        }

        // idProvider, if present, is a table ref (local name or 'group.table')
        if (create.idProvider !== undefined && !isValidTableRef(create.idProvider)) {
            return validationFailure(`idProvider '${create.idProvider}' is not a valid table reference`);
        }

        return validationOk();
    }

    if (action === 'row') {
        if (!json.checkFormat(rowEnvelopeFormat, payload)) return validationFailure("row envelope payload format is invalid");
        const envelope = payload as RowEnvelopePayload;
        if (!isValidName(envelope.table)) return validationFailure(`invalid row envelope table name '${envelope.table}'`);
        return wrapValidationFailure(`row op for table '${envelope.table}' has invalid format`, validateRowOpFormat(envelope.op));
    }

    if (action === 'bundle') {
        if (!json.checkFormat(bundleFormat, payload)) return validationFailure("bundle payload format is invalid");
        const bundle = payload as BundlePayload;

        if (bundle.writes.length === 0) return validationFailure("bundle must carry at least one write");

        for (const [index, write] of bundle.writes.entries()) {
            if (!isValidName(write.table)) return validationFailure(`bundle write ${index} has invalid table name '${write.table}'`);
            const result = validateRowOpFormat(write.op);
            if (!result.valid) return wrapValidationFailure(`bundle write ${index} for table '${write.table}' has invalid format`, result);
        }

        return validationOk();
    }

    return validationFailure(`unknown table group action '${String(action)}'`);
}
