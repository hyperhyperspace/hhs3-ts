// The checks of HIP-20's writer, the same on every target. A target reads, inside
// the write's transaction, what the checks need: the table's meta as a
// TableShape, what the projection recorded, whether capture is on, the key
// table and, for an update, the row. checkWrite then throws a
// WriteRefusedError, writing nothing, or returns the write as the target's
// native path is to make it: the row's own id and author set aside, the
// unchanged columns dropped under `compare`, each key id checked, and each
// `{ pk }` checked and given its key hash, for the target to intern last.
//
// What depends on the schema view or on history stays with ingestion: readonly
// columns, FK validity, ALLOW rules and voiding (HIP-20's Checks).

import { json } from "@hyper-hyper-space/hhs3_json";
import { getHashSuite, keyIdFromPublicKey } from "@hyper-hyper-space/hhs3_crypto";
import { deserializePublicKeyFromBase64, validateKey } from "@hyper-hyper-space/hhs3_mvt";
import type { ColumnType } from "@hyper-hyper-space/hhs3_rdb";

import { WriteRefusedError, type KeyValue, type RowValues, type WriteRefusal, type WriterRecord } from "./types.js";

// What a write needs to know of one projected column.
export type ColumnShape = {
    type: ColumnType;
    nullable: boolean;
    hasDefault: boolean;
    // A key column: it takes a key id of the key table, or a `{ pk }` to intern.
    key: boolean;
};

// What a write needs to know of a table, from its meta.
export type TableShape = {
    table: string;
    idColumn: string;
    authorColumn?: string;
    // Every projected column but the id and author columns.
    columns: { [column: string]: ColumnShape };
    // Whether the table's group has no identity provider.
    anonymous: boolean;
};

// The key table, as the checks read it.
export type KeyTableLookup = {
    // Whether the key table holds this key id.
    hasId(id: number): boolean;
    // The id of a key hash the key table already holds.
    idOf(keyHash: string): number | undefined;
};

// What a write is checked against besides the table, read in its transaction.
export type WriteContext = {
    // What the projection recorded in the target, if it has.
    record: WriterRecord | undefined;
    // Whether capture is set up and on, so that the write reaches rdb.
    captureOn: boolean;
    keys: KeyTableLookup;
};

// The row an update is made to, as the target stores it.
export type CurrentRow = {
    id: number;
    values: { [column: string]: json.Literal | null | undefined };
    // The row's author column: its key id, or undefined for an anonymous row.
    author: number | undefined;
};

export type WriteRequest =
    | { kind: 'insert'; values: RowValues }
    | { kind: 'update'; values: RowValues; row: CurrentRow; compare: boolean };

// A checked write: the values to write natively, keys as their ids, and the
// keys still to intern. The target interns each, last, and puts its id in
// `values` under the column.
export type CheckedWrite = {
    values: { [column: string]: json.Literal | null };
    intern: { column: string; keyHash: string; publicKey: string }[];
};

function refuse(reason: WriteRefusal, message: string): never {
    throw new WriteRefusedError(reason, message);
}

export function isKeyValue(value: unknown): value is KeyValue {
    if (typeof value !== 'object' || value === null || Array.isArray(value)) return false;
    const keys = Object.keys(value);
    return keys.length === 1 && keys[0] === 'pk' && typeof (value as { pk: unknown }).pk === 'string';
}

// What every write checks first, before it reads a row: that it reaches rdb,
// and that a write to a group with an identity provider has a writer key to be
// signed with. A write to an anonymous group needs none (HIP-29).
export function checkWritable(shape: TableShape, ctx: WriteContext): void {
    if (!ctx.captureOn) {
        refuse('no-capture', `a write to '${shape.table}' would never reach rdb: capture is off in this target`);
    }
    if (!shape.anonymous && ctx.record?.writerKeyId === undefined) {
        refuse('no-writer-key', `'${shape.table}' belongs to a group with an identity provider, `
            + 'and no projection has recorded a writer key in this target');
    }
}

// The writer key's id, for writerKeyId().
export function recordedWriterKeyId(record: WriterRecord | undefined): number {
    if (record?.writerKeyId === undefined) refuse('no-writer-key', 'no projection has recorded a writer key in this target');
    return record.writerKeyId;
}

export function checkWrite(shape: TableShape, ctx: WriteContext, write: WriteRequest): CheckedWrite {
    checkWritable(shape, ctx);
    const out: CheckedWrite = { values: {}, intern: [] };
    for (const [column, value] of Object.entries(write.values)) {
        if (value === undefined) continue;   // not given
        if (column === shape.idColumn) {
            checkIdColumn(shape, write, value);
            continue;
        }
        if (column === shape.authorColumn) {
            const author = checkAuthorColumn(shape, ctx, write, value);
            if (author !== undefined) out.values[column] = author;
            continue;
        }
        const col = shape.columns[column];
        if (col === undefined) refuse('unknown-column', `'${shape.table}' has no column '${column}'`);
        // With `compare`, an unchanged column is set aside before any other check.
        if (write.kind === 'update' && write.compare && unchanged(shape, ctx, column, col, value, write.row.values[column])) continue;
        checkValue(shape, ctx, column, col, value, out);
    }
    if (write.kind === 'insert') {
        for (const [column, col] of Object.entries(shape.columns)) {
            if (col.nullable || col.hasDefault || column in out.values || out.intern.some((k) => k.column === column)) continue;
            refuse('missing-required', `an insert into '${shape.table}' needs a value for '${column}', which has no default and isn't nullable`);
        }
        // Under `auto`, an insert that names no author is signed with the writer
        // key: the row reads its id at once, as capture's fill would have it.
        if (shape.authorColumn !== undefined && !shape.anonymous && !(shape.authorColumn in out.values)
            && ctx.record?.author === 'auto') {
            out.values[shape.authorColumn] = ctx.record.writerKeyId!;
        }
    }
    return out;
}

// The id column: never in an insert, and in an update only at the row's own
// id, which is set aside (HIP-20's Write semantics).
function checkIdColumn(shape: TableShape, write: WriteRequest, value: json.Literal | KeyValue | null): void {
    if (write.kind === 'insert') {
        refuse('id-column', `an insert into '${shape.table}' can't give the id column '${shape.idColumn}' a value: the target allocates ids`);
    }
    if (value !== write.row.id) {
        refuse('id-column', `the id of row ${write.row.id} of '${shape.table}' can't change (the update gives '${shape.idColumn}' ${JSON.stringify(value)})`);
    }
}

// The author column (HIP-20's Authorship). Returns the value to write, or
// undefined to leave the column out.
function checkAuthorColumn(
    shape: TableShape, ctx: WriteContext, write: WriteRequest, value: json.Literal | KeyValue | null,
): number | null | undefined {
    const column = shape.authorColumn!;
    if (isKeyValue(value)) {
        refuse('author', `'${shape.table}.${column}' takes the writer key's id or null, not a public key`);
    }
    if (value !== null && !Number.isSafeInteger(value)) {
        refuse('wrong-type', `'${shape.table}.${column}' takes a key id or null, not ${JSON.stringify(value)}`);
    }
    const id = value as number | null;
    if (write.kind === 'update') {
        // No op can change a row's author: the row's own value is set aside,
        // and any other is refused.
        if (id === (write.row.author ?? null)) return undefined;
        refuse('author', `row ${write.row.id} of '${shape.table}' can't change its author: no op can`);
    }
    if (shape.anonymous) {
        if (id === null) return null;
        refuse('author', `an insert into '${shape.table}' can't name an author: its group has no identity provider, so every insert there is anonymous`);
    }
    if (id === null) {
        if (ctx.record?.author === 'manual') return null;
        refuse('author', `an insert into '${shape.table}' with a null author asks for an anonymous row, which only the 'manual' setting gives`);
    }
    if (id !== ctx.record?.writerKeyId) {
        refuse('author', `an insert into '${shape.table}' can only name the writer key's id as its author, the one key this device signs with`);
    }
    return id;
}

// A business column's value: its type, its nullability, and for a key column
// the key. A key id must be in the key table; a `{ pk }` is checked and
// hashed, and interned last unless the key table already holds it.
function checkValue(
    shape: TableShape, ctx: WriteContext, column: string, col: ColumnShape, value: json.Literal | KeyValue | null, out: CheckedWrite,
): void {
    const where = `'${shape.table}.${column}'`;
    if (value === null) {
        if (!col.nullable) refuse('not-null', `${where} isn't nullable`);
        out.values[column] = null;
        return;
    }
    if (col.key) {
        if (isKeyValue(value)) {
            const key = keyOf(where, value, ctx);
            const id = ctx.keys.idOf(key.keyHash);
            if (id !== undefined) out.values[column] = id;
            else out.intern.push({ column, ...key });
            return;
        }
        if (!Number.isSafeInteger(value)) {
            refuse('wrong-type', `${where} is a key column: it takes a key id or a { pk }, not ${JSON.stringify(value)}`);
        }
        if (!ctx.keys.hasId(value as number)) refuse('invalid-key', `${where}: the key table has no key id ${value}`);
        out.values[column] = value;
        return;
    }
    const problem = json.literalProblem(value);
    if (problem !== undefined) refuse('wrong-type', `${where} takes a json literal, and the value holds ${problem}`);
    if (!hasType(col.type, value as json.Literal)) {
        refuse('wrong-type', `${where} is of type ${col.type}, which reads as ${readForm(col.type)}, not ${JSON.stringify(value)}`);
    }
    out.values[column] = value as json.Literal;
}

// Whether a value has the JSON type reads return for a column type.
function hasType(type: ColumnType, value: json.Literal): boolean {
    switch (type) {
        case 'string': case 'bigint': case 'decimal': case 'bytes':
            return typeof value === 'string';
        case 'integer':
            return Number.isSafeInteger(value);
        case 'float':
            return typeof value === 'number';
        case 'boolean':
            return typeof value === 'boolean';
        case 'json':
            return true;
        case 'identity':
            return false;   // projected as a key column
    }
}

function readForm(type: ColumnType): string {
    switch (type) {
        case 'string': return 'a string';
        case 'bigint': case 'decimal': return 'a numeric string';
        case 'bytes': return 'a base64 string';
        case 'integer': return 'a safe integer';
        case 'float': return 'a number';
        case 'boolean': return 'a boolean';
        case 'json': return 'json';
        case 'identity': return 'a key id';
    }
}

// A `{ pk }`'s key, checked with validateKey: the deserializer checks nothing,
// so a key hash passed by mistake would parse into a garbage suite and an
// empty key. The key hash is computed with the hash suite the projection
// recorded, the replica's (HIP-20's Key columns).
function keyOf(where: string, value: KeyValue, ctx: WriteContext): { keyHash: string; publicKey: string } {
    const hash = ctx.record === undefined ? undefined : getHashSuite(ctx.record.hashSuite);
    if (hash === undefined) {
        refuse('invalid-key', `${where}: no projection has recorded the hash suite a key is hashed with in this target`);
    }
    const valid = validateKey(value.pk);
    if (!valid.valid) refuse('invalid-key', `${where}: ${valid.why.reason}`);
    return { keyHash: keyIdFromPublicKey(deserializePublicKeyFromBase64(value.pk), hash), publicKey: value.pk };
}

// Whether an update's value for a column equals the row's, by value, json
// included, with null equal to NULL. A `{ pk }` equals the key id it was
// interned under.
function unchanged(
    shape: TableShape, ctx: WriteContext, column: string, col: ColumnShape,
    value: json.Literal | KeyValue | null, current: json.Literal | null | undefined,
): boolean {
    const stored = current ?? null;
    if (value === null || stored === null) return value === stored;
    if (col.key && isKeyValue(value)) {
        try {
            return ctx.keys.idOf(keyOf(`'${shape.table}.${column}'`, value, ctx).keyHash) === stored;
        } catch {
            return false;   // checked, and refused, as a changed value
        }
    }
    if (json.literalProblem(value) !== undefined) return false;
    return json.eq(value as json.Literal, stored);
}

// The key table lookups a write's checks make, for a target that reads its key
// table asynchronously and answers the checks from what it read: the key ids
// the write names in key columns, and the key hashes of its `{ pk }`s. A value
// that isn't a key is left out: the checks refuse it.
export function keyLookups(shape: TableShape, record: WriterRecord | undefined, values: RowValues): { ids: number[]; hashes: string[] } {
    const ids: number[] = [];
    const hashes: string[] = [];
    const ctx: WriteContext = { record, captureOn: true, keys: { hasId: () => false, idOf: () => undefined } };
    for (const [column, value] of Object.entries(values)) {
        if (shape.columns[column]?.key !== true || value === undefined || value === null) continue;
        if (isKeyValue(value)) {
            try {
                hashes.push(keyOf(`'${shape.table}.${column}'`, value, ctx).keyHash);
            } catch {
                // refused by the checks
            }
        } else if (Number.isSafeInteger(value)) {
            ids.push(value as number);
        }
    }
    return { ids, hashes };
}
