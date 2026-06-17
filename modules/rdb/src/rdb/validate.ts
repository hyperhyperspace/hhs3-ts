// Format-level validation for RDb payloads.

import { json } from "@hyper-hyper-space/hhs3_json";

import {
    createRDbFormat,
    addSchemaFormat,
    addGroupFormat,
} from "./payload.js";

export function validateRDbPayloadFormat(payload: json.Literal): boolean {
    if (typeof payload !== 'object' || payload === null || Array.isArray(payload)) return false;

    const action = (payload as json.LiteralMap)['action'];

    if (action === 'create') {
        return json.checkFormat(createRDbFormat, payload);
    }

    if (action === 'add-schema') {
        // `note` is free-form text: format check (bounded string) suffices.
        return json.checkFormat(addSchemaFormat, payload);
    }

    if (action === 'add-group') {
        return json.checkFormat(addGroupFormat, payload);
    }

    return false;
}
