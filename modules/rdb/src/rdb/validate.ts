// Format-level validation for RDb payloads.

import { json } from "@hyper-hyper-space/hhs3_json";
import { validationFailure, validationOk, ValidationResult } from "@hyper-hyper-space/hhs3_mvt";

import {
    createRDbFormat,
    addSchemaFormat,
    addGroupFormat,
} from "./payload.js";

export function validateRDbPayloadFormat(payload: json.Literal): ValidationResult {
    if (typeof payload !== 'object' || payload === null || Array.isArray(payload)) {
        return validationFailure("RDb payload must be an object");
    }

    const action = (payload as json.LiteralMap)['action'];

    if (action === 'create') {
        return json.checkFormat(createRDbFormat, payload)
            ? validationOk()
            : validationFailure("RDb create payload format is invalid");
    }

    if (action === 'add-schema') {
        // `note` is free-form text: format check (bounded string) suffices.
        return json.checkFormat(addSchemaFormat, payload)
            ? validationOk()
            : validationFailure("RDb add-schema payload format is invalid");
    }

    if (action === 'add-group') {
        return json.checkFormat(addGroupFormat, payload)
            ? validationOk()
            : validationFailure("RDb add-group payload format is invalid");
    }

    return validationFailure(`unknown RDb action '${String(action)}'`);
}
