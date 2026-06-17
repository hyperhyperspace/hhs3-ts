// Format-level validation for RTable row op payloads.
//
// Semantic validation against DAG state (schema conformance, ownership,
// signature verification, derived precondition enforcement) is in validate_ops.ts.

import { json } from "@hyper-hyper-space/hhs3_json";

import {
    insertRowFormat, InsertRowPayload,
    updateRowFormat, UpdateRowPayload,
    deleteRowFormat, DeleteRowPayload,
} from "./payload.js";

import { isValidName } from "../rschema/validate.js";
import { checkRowId } from "./hash.js";

function validateColumnNames(values: { [column: string]: json.Literal }): boolean {
    for (const column of Object.keys(values)) {
        if (!isValidName(column)) return false;
    }
    return true;
}

export function validateRowOpFormat(payload: json.Literal): boolean {
    if (typeof payload !== 'object' || payload === null || Array.isArray(payload)) return false;

    const action = (payload as json.LiteralMap)['action'];

    if (action === 'insert') {
        if (!json.checkFormat(insertRowFormat, payload)) return false;
        const insert = payload as InsertRowPayload;

        // the rowId must match the (uuid, owner) derivation
        if (!checkRowId(insert.rowId, insert.uuid, insert.owner)) return false;

        return validateColumnNames(insert.values);
    }

    if (action === 'update') {
        if (!json.checkFormat(updateRowFormat, payload)) return false;
        const update = payload as UpdateRowPayload;
        if (Object.keys(update.values).length === 0) return false;
        return validateColumnNames(update.values);
    }

    if (action === 'delete') {
        return json.checkFormat(deleteRowFormat, payload);
    }

    return false;
}
