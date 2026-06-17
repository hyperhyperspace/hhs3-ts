// Format-level validation for RTableGroup payloads (shape only).
//
// Position-dependent validation against DAG state (table existence at the
// resolved schema version, identity/liveness, FK targets, ref-advance
// monotonicity + canDeploy, bundle sequential cut) lives in validate_ops.ts.

import { json } from "@hyper-hyper-space/hhs3_json";
import { isRefAdvancePayload, refAdvanceFormat } from "@hyper-hyper-space/hhs3_mvt";

import {
    createTableGroupFormat, CreateTableGroupPayload,
    rowEnvelopeFormat, RowEnvelopePayload,
    bundleFormat, BundlePayload,
} from "./payload.js";

import { isValidName, isValidTableRef, validatePredicate } from "../rschema/validate.js";
import { validateRowOpFormat } from "../rtable/validate.js";
import { InsertRowPayload } from "../rtable/payload.js";

export function validateTableGroupPayloadFormat(payload: json.Literal): boolean {
    if (typeof payload !== 'object' || payload === null || Array.isArray(payload)) return false;

    if (isRefAdvancePayload(payload)) {
        // canonical mvt ref-advance; used for both the RSchema ref (deploy)
        // and foreign group refs (cross-group FKs). Non-strict: deploy-gated
        // ref-advances carry author/signature as extra fields.
        return json.checkFormat(refAdvanceFormat, payload, { strict: false });
    }

    const action = (payload as json.LiteralMap)['action'];

    if (action === 'create') {
        if (!json.checkFormat(createTableGroupFormat, payload)) return false;
        const create = payload as CreateTableGroupPayload;

        // initial rows are genesis fiat data: structurally valid insert ops,
        // but carrying no authoring (they are given, not validated as ops)
        for (const table of Object.keys(create.initialRows ?? {})) {
            if (!isValidName(table)) return false;
            for (const row of create.initialRows![table]) {
                if (!validateRowOpFormat(row)) return false;
                const insert = row as InsertRowPayload;
                if (insert.action !== 'insert') return false;
                if (insert.author !== undefined || insert.signature !== undefined) return false;
            }
        }

        for (const name of Object.keys(create.bindings ?? {})) {
            if (!isValidName(name)) return false;
        }

        // canDeploy runs without a subject row: 'object' context ($rowOwner rejected)
        if (create.canDeploy !== undefined && !validatePredicate(create.canDeploy, 'object')) return false;

        // idProvider, if present, is a table ref (local name or 'group.table')
        if (create.idProvider !== undefined && !isValidTableRef(create.idProvider)) return false;

        return true;
    }

    if (action === 'row') {
        if (!json.checkFormat(rowEnvelopeFormat, payload)) return false;
        const envelope = payload as RowEnvelopePayload;
        if (!isValidName(envelope.table)) return false;
        return validateRowOpFormat(envelope.op);
    }

    if (action === 'bundle') {
        if (!json.checkFormat(bundleFormat, payload)) return false;
        const bundle = payload as BundlePayload;

        if (bundle.writes.length === 0) return false;

        for (const write of bundle.writes) {
            if (!isValidName(write.table)) return false;
            if (!validateRowOpFormat(write.op)) return false;
        }

        return true;
    }

    return false;
}
