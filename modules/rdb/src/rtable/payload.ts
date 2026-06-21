// Payloads for RTable row operations, and their format validators.
//
// An RTable is a nested RObject living on a scoped projection of its
// RTableGroup's DAG. Row ops do not carry the table name: the group-level
// envelope (see ../rtable_group/payload.ts) tags entries with the table, the
// same way nested RSet element ops don't repeat their elementHash.
//
// Preconditions (FKs and restrictions) live only in the schema: validators
// derive them from the declaration plus the op's row values and enforce them
// whether or not the op mentions them; they are never serialized in payloads.
//
// v1 semantics is drop-on-void: views filter ops whose derived preconditions
// are voided; there is no general meaning reinterpretation.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, KeyId } from "@hyper-hyper-space/hhs3_crypto";

import {
    MAX_NAME_LENGTH, MAX_COLUMNS,
    MAX_HASH_LENGTH, MAX_KEY_ID_LENGTH, MAX_SIGNATURE_LENGTH,
} from "../rschema/payload.js";

export const MAX_UUID_LENGTH = 128;

export type RowOpPayload = InsertRowPayload | UpdateRowPayload | DeleteRowPayload;

// Insert a row:

export type InsertRowPayload = {
    action: 'insert';
    rowId: B64Hash;                               // must equal deriveRowId(uuid, author)
    uuid: string;
    values: { [column: string]: json.Literal };
    author?: KeyId;                               // authored variant (signature verified at validation)
    signature?: string;
};

export const insertRowFormat: json.Format = {
    action: [json.Type.Constant, 'insert'],
    rowId: [json.Type.BoundedString, MAX_HASH_LENGTH],
    uuid: [json.Type.BoundedString, MAX_UUID_LENGTH],
    values: [json.Type.BoundedMap, [json.Type.BoundedString, MAX_NAME_LENGTH], json.Type.Something, MAX_COLUMNS],
    author: [json.Type.Option, [json.Type.BoundedString, MAX_KEY_ID_LENGTH]],
    signature: [json.Type.Option, [json.Type.BoundedString, MAX_SIGNATURE_LENGTH]],
};

// Update a row (partial: only the updated fields are carried; per-field
// last-writer-wins with a deterministic entry-hash tiebreak for concurrent
// writers):

export type UpdateRowPayload = {
    action: 'update';
    rowId: B64Hash;
    values: { [column: string]: json.Literal };
    author?: KeyId;
    signature?: string;
};

export const updateRowFormat: json.Format = {
    action: [json.Type.Constant, 'update'],
    rowId: [json.Type.BoundedString, MAX_HASH_LENGTH],
    values: [json.Type.BoundedMap, [json.Type.BoundedString, MAX_NAME_LENGTH], json.Type.Something, MAX_COLUMNS],
    author: [json.Type.Option, [json.Type.BoundedString, MAX_KEY_ID_LENGTH]],
    signature: [json.Type.Option, [json.Type.BoundedString, MAX_SIGNATURE_LENGTH]],
};

// Delete a row. PERMANENT: rowIds are write-once, so re-insertion of a
// deleted rowId is invalid. Always barrier-tagged at write; whether the
// barrier is honored (reaches concurrent branches) is decided at the view
// (at, from) horizon by the concurrentDeletes flag (default true), so a
// delete authored while the flag was off is honored once a deploy enables it.

export type DeleteRowPayload = {
    action: 'delete';
    rowId: B64Hash;
    author?: KeyId;
    signature?: string;
};

export const deleteRowFormat: json.Format = {
    action: [json.Type.Constant, 'delete'],
    rowId: [json.Type.BoundedString, MAX_HASH_LENGTH],
    author: [json.Type.Option, [json.Type.BoundedString, MAX_KEY_ID_LENGTH]],
    signature: [json.Type.Option, [json.Type.BoundedString, MAX_SIGNATURE_LENGTH]],
};
