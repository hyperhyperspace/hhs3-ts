// Format-level validation for RTable row op payloads.
//
// Semantic validation against DAG state (schema conformance, ownership,
// signature verification, derived precondition enforcement) is in validate_ops.ts.

import { json } from "@hyper-hyper-space/hhs3_json";
import { validationFailure, validationOk, ValidationResult } from "@hyper-hyper-space/hhs3_mvt";

import {
    insertRowFormat, InsertRowPayload,
    updateRowFormat, UpdateRowPayload,
    deleteRowFormat, DeleteRowPayload,
} from "./payload.js";

import { isValidName } from "../rschema/validate.js";
import { checkRowId } from "./hash.js";

function validateColumnNames(values: { [column: string]: json.Literal }): ValidationResult {
    for (const column of Object.keys(values)) {
        if (!isValidName(column)) return validationFailure(`invalid column name '${column}'`);
    }
    return validationOk();
}

export function validateRowOpFormat(payload: json.Literal): ValidationResult {
    if (typeof payload !== 'object' || payload === null || Array.isArray(payload)) {
        return validationFailure("row op payload must be an object");
    }

    const action = (payload as json.LiteralMap)['action'];

    if (action === 'insert') {
        if (!json.checkFormat(insertRowFormat, payload)) return validationFailure("insert row payload format is invalid");
        const insert = payload as InsertRowPayload;

        // the rowId must match the (uuid, author) derivation
        if (!checkRowId(insert.rowId, insert.uuid, insert.author)) return validationFailure("insert rowId does not match uuid/author");

        return validateColumnNames(insert.values);
    }

    if (action === 'update') {
        if (!json.checkFormat(updateRowFormat, payload)) return validationFailure("update row payload format is invalid");
        const update = payload as UpdateRowPayload;
        if (Object.keys(update.values).length === 0) return validationFailure("update carries no column values");
        return validateColumnNames(update.values);
    }

    if (action === 'delete') {
        return json.checkFormat(deleteRowFormat, payload)
            ? validationOk()
            : validationFailure("delete row payload format is invalid");
    }

    return validationFailure(`unknown row op action '${String(action)}'`);
}
