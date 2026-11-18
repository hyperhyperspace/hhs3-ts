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

// `nulls` has one spelling, so that one write has one payload and one hash:
// left out when empty, valid column names in strictly ascending order, none of
// them also in `values`. The order is by UTF-16 code unit (`<` on strings), the
// same on every runtime. Strictly ascending also rules out a repeated name.
function validateNulls(nulls: string[] | undefined, values: { [column: string]: json.Literal }): ValidationResult {
    if (nulls === undefined) return validationOk();
    if (nulls.length === 0) return validationFailure("nulls must be left out when empty");
    for (let i = 0; i < nulls.length; i++) {
        const column = nulls[i];
        if (!isValidName(column)) return validationFailure(`invalid column name '${column}'`);
        if (i > 0 && !(nulls[i - 1] < column)) return validationFailure("nulls must be in strictly ascending order");
        if (Object.prototype.hasOwnProperty.call(values, column)) {
            return validationFailure(`column '${column}' is both set to a value and to null`);
        }
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

        const names = validateColumnNames(insert.values);
        if (!names.valid) return names;
        return validateNulls(insert.nulls, insert.values);
    }

    if (action === 'update') {
        if (!json.checkFormat(updateRowFormat, payload)) return validationFailure("update row payload format is invalid");
        const update = payload as UpdateRowPayload;
        if (Object.keys(update.values).length === 0 && update.nulls === undefined) {
            return validationFailure("update carries no column values");
        }
        const names = validateColumnNames(update.values);
        if (!names.valid) return names;
        return validateNulls(update.nulls, update.values);
    }

    if (action === 'delete') {
        return json.checkFormat(deleteRowFormat, payload)
            ? validationOk()
            : validationFailure("delete row payload format is invalid");
    }

    return validationFailure(`unknown row op action '${String(action)}'`);
}
