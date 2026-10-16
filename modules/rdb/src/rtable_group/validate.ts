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
    groupParamsFormat,
} from "./payload.js";

import { isValidName, isValidTableRef, validatePredicate } from "../rschema/validate.js";
import { validateRowOpFormat } from "../rtable/validate.js";
import { InsertRowPayload } from "../rtable/payload.js";

function validateGroupParamsFormat(params: json.Literal, label: string): ValidationResult {
    if (!json.checkFormat(groupParamsFormat, params)) {
        return validationFailure(`${label} params format is invalid`);
    }
    const map = params as json.LiteralMap;
    const names = Object.keys(map);
    if (names.length === 0) return validationFailure(`${label} params must be omitted when empty`);
    for (const name of names) {
        if (!isValidName(name)) return validationFailure(`invalid parameter name '${name}' in ${label}`);
        const val = map[name];
        if (!json.isLiteral(val)) return validationFailure(`invalid parameter value for ':${name}' in ${label}`);
        if (typeof val === 'string' && val.startsWith('$')) {
            return validationFailure(`parameter ':${name}' string value cannot start with '$'`);
        }
    }
    return validationOk();
}

export function validateTableGroupPayloadFormat(payload: json.Literal): ValidationResult {
    if (typeof payload !== 'object' || payload === null || Array.isArray(payload)) {
        return validationFailure("table group payload must be an object");
    }

    if (isRefAdvancePayload(payload)) {
        // canonical mvt ref-advance; used for both the RSchema ref (deploy)
        // and foreign group refs (cross-group FKs). Non-strict: deploy-gated
        // ref-advances carry author/signature/gate/params as extra fields.
        if (!json.checkFormat(refAdvanceFormat, payload, { strict: false })) {
            return validationFailure("ref-advance payload format is invalid");
        }
        const p = payload as json.LiteralMap;
        if (p['params'] !== undefined) {
            const paramsRes = validateGroupParamsFormat(p['params'], 'ref-advance');
            if (!paramsRes.valid) return paramsRes;
        }
        return validationOk();
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

        for (const name of Object.keys(create.bindings ?? {})) {
            if (!isValidName(name)) return validationFailure(`invalid binding name '${name}'`);
        }

        if (create.tableMaps !== undefined) {
            const aliases = Object.keys(create.tableMaps);
            if (aliases.length === 0) return validationFailure('tableMaps must be omitted when empty');
            const bindings = create.bindings ?? {};
            for (const alias of aliases) {
                if (!Object.prototype.hasOwnProperty.call(bindings, alias)) {
                    return validationFailure(`tableMaps alias '${alias}' is not a bound group`);
                }
                const entries = Object.entries(create.tableMaps[alias]);
                if (entries.length === 0) {
                    return validationFailure(`tableMaps for '${alias}' must not be empty`);
                }
                for (const [schemaTable, foreignTable] of entries) {
                    if (!isValidName(schemaTable)) {
                        return validationFailure(`invalid schema table name '${schemaTable}' in tableMaps for '${alias}'`);
                    }
                    if (!isValidName(foreignTable)) {
                        return validationFailure(`invalid foreign table name '${foreignTable}' in tableMaps for '${alias}'`);
                    }
                    if (schemaTable === foreignTable) {
                        return validationFailure(`tableMaps for '${alias}' maps '${schemaTable}' to itself`);
                    }
                }
            }
        }

        if (create.params !== undefined) {
            const paramsRes = validateGroupParamsFormat(create.params, 'group create');
            if (!paramsRes.valid) return paramsRes;
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

        // single signer: the bundle is signed as a whole, its ops only claim
        // the bundle's author
        if ((bundle.author === undefined) !== (bundle.signature === undefined)) {
            return validationFailure("bundle author and signature must be both present or both absent");
        }

        for (const [index, write] of bundle.writes.entries()) {
            if (!isValidName(write.table)) return validationFailure(`bundle write ${index} has invalid table name '${write.table}'`);
            const result = validateRowOpFormat(write.op);
            if (!result.valid) return wrapValidationFailure(`bundle write ${index} for table '${write.table}' has invalid format`, result);
            const op = write.op as json.LiteralMap;
            if (op['signature'] !== undefined) return validationFailure(`bundle write ${index} must not carry its own signature`);
            if (op['author'] !== bundle.author) {
                return validationFailure(`bundle write ${index} author must match the bundle author`);
            }
        }

        return validationOk();
    }

    return validationFailure(`unknown table group action '${String(action)}'`);
}
