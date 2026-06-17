// Schema-conformance validation for RTable row ops.
//
// These checks are position-independent GIVEN a resolved schema view: the
// caller (the group's validate_ops) resolves the effective schema at the
// entry's position and passes the view. Liveness / identity checks (write-once
// rowIds for inserts, live row for updates and deletes) live with the group
// too, since they need a table view over the group DAG.
//
// Restriction / signature enforcement (op authorship + authorization) is the
// group's concern (rtable_group/validate_ops.ts + computeEntryVoided); this
// file adds the provider content-integrity check for identity-provider rows.

import type { HashSuite } from "@hyper-hyper-space/hhs3_crypto";
import { computeKeyId, deserializePublicKeyFromBase64 } from "@hyper-hyper-space/hhs3_mvt";

import type { RSchemaView } from "../rschema/interfaces.js";
import { columnValueMatchesType } from "../rschema/validate.js";
import type { InsertRowPayload, UpdateRowPayload, RowOpPayload } from "./payload.js";

// An insert conforms to the schema iff the table exists, every carried column
// exists with a matching value type, and every non-nullable column is covered
// (carried, or backed by a default).
export function validateInsertAgainstSchema(insert: InsertRowPayload, view: RSchemaView, table: string): boolean {
    const def = view.getTable(table);
    if (def === undefined) return false;

    for (const column of Object.keys(insert.values)) {
        const columnDef = def.columns[column];
        if (columnDef === undefined) return false;
        if (!columnValueMatchesType(insert.values[column], columnDef.type)) return false;
    }

    for (const column of Object.keys(def.columns)) {
        const columnDef = def.columns[column];
        if (columnDef.nullable ?? false) continue;
        if (columnDef.default !== undefined) continue;
        if (insert.values[column] === undefined) return false;
    }

    return true;
}

// An update conforms to the schema iff the table exists, at least one column
// is carried, and every carried column exists with a matching value type and
// is not readonly (readonly columns are fixed at insert).
export function validateUpdateAgainstSchema(update: UpdateRowPayload, view: RSchemaView, table: string): boolean {
    const def = view.getTable(table);
    if (def === undefined) return false;

    const columns = Object.keys(update.values);
    if (columns.length === 0) return false;

    for (const column of columns) {
        const columnDef = def.columns[column];
        if (columnDef === undefined) return false;
        if (columnDef.readonly ?? false) return false;
        if (!columnValueMatchesType(update.values[column], columnDef.type)) return false;
    }

    return true;
}

// Provider content-integrity (position-independent given the schema view): a
// row inserted into an identity-provider table must be self-certifying —
// keyId == keyIdFromPublicKey(publicKey). This is a BINDING-integrity check
// (you cannot register a publicKey under a keyId that is not its hash); it
// proves nothing about private-key possession, which only matters when signing
// ops. Non-provider tables and non-insert ops are unconstrained here. The
// hashSuite is the replica's keyId hash (the same one identities are minted
// with).
export function validateProviderInsertIntegrity(
    op: RowOpPayload, view: RSchemaView, table: string, hashSuite: HashSuite,
): boolean {
    if (op.action !== 'insert') return true;
    const provider = view.getIdProvider(table);
    if (provider === undefined) return true;

    const keyIdVal = op.values[provider.keyIdColumn];
    const pkVal = op.values[provider.publicKeyColumn];
    if (typeof keyIdVal !== 'string' || typeof pkVal !== 'string') return false;

    let pk;
    try {
        pk = deserializePublicKeyFromBase64(pkVal);
    } catch {
        return false;
    }
    return computeKeyId(pk, hashSuite) === keyIdVal;
}

// Row op conformance: inserts and updates as above, deletes just need the
// table (the liveness check is the group's).
export function validateRowOpAgainstSchema(op: RowOpPayload, view: RSchemaView, table: string): boolean {
    if (!view.hasTable(table)) return false;

    switch (op.action) {
        case 'insert':
            return validateInsertAgainstSchema(op, view, table);
        case 'update':
            return validateUpdateAgainstSchema(op, view, table);
        case 'delete':
            return true;
        default:
            return false;
    }
}
