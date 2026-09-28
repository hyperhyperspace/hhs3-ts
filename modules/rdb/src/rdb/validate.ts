// Format-level validation for RDb payloads.

import { json } from "@hyper-hyper-space/hhs3_json";
import { validationFailure, validationOk, ValidationResult } from "@hyper-hyper-space/hhs3_mvt";

import { isValidName } from "../rschema/validate.js";
import { createRDbFormat, updateCatalogFormat, ParamValue } from "./payload.js";

function checkParamNames(params: { [name: string]: ParamValue } | undefined): string | undefined {
    for (const name of Object.keys(params ?? {})) {
        if (!isValidName(name)) return `invalid param name '${name}'`;
    }
    return undefined;
}

export function validateRDbPayloadFormat(payload: json.Literal): ValidationResult {
    if (typeof payload !== 'object' || payload === null || Array.isArray(payload)) {
        return validationFailure("RDb payload must be an object");
    }

    const action = (payload as json.LiteralMap)['action'];

    if (action === 'create') {
        if (!json.checkFormat(createRDbFormat, payload)) return validationFailure("RDb create payload format is invalid");
        const reason = checkParamNames((payload as { params?: { [name: string]: ParamValue } }).params);
        return reason === undefined ? validationOk() : validationFailure(`RDb create: ${reason}`);
    }

    if (action === 'update-catalog') {
        if (!json.checkFormat(updateCatalogFormat, payload)) return validationFailure("RDb update-catalog payload format is invalid");
        const reason = checkParamNames((payload as { params?: { [name: string]: ParamValue } }).params);
        return reason === undefined ? validationOk() : validationFailure(`RDb update-catalog: ${reason}`);
    }

    return validationFailure(`unknown RDb action '${String(action)}'`);
}
