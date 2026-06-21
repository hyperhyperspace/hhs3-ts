// Single-table query grammar, evaluation and user-facing validation.
//
// A RowQuery is a LOCAL read at a fixed (at, from) horizon over ONE table (no
// joins). It voids nothing and needs no cross-replica agreement, so it is a
// RICHER front-end than restriction predicates: it adds `not`, allows floats,
// and filters over ANY column (pub or not). `pub` only buys index acceleration
// in the engine; a non-pub filter is correct but scans.
//
// RowFilter is a SEPARATE type from the restriction Predicate. It shares only
// the pure value-expression core (operand / cmp / str) via ../rschema/expr.ts;
// it must never be accepted where a Predicate is expected (schema restrictions,
// canDeploy, voiding). Validation here is USER-FACING — it throws descriptive
// errors on mistakes (unknown columns, malformed filters, type-incoherent
// comparisons, bad limit/offset) — NOT the BFT op validation used for incoming
// writes.

import { json } from "@hyper-hyper-space/hhs3_json";
import type { ColumnType, CmpOp, Operand, StrOp } from "../rschema/payload.js";
import { CMP_OPS, STR_OPS, ARITH_FNS } from "../rschema/payload.js";
import { evalOperand, compareOperands, cmpTypesOk, strTypesOk } from "../rschema/expr.js";
import { MAX_EXPR_DEPTH, MAX_EXPR_ARGS } from "../rschema/validate.js";

import type { Row, RowValues } from "./interfaces.js";

// Two-valued logic: an operand that does not resolve (missing column value)
// makes its cmp/str atom false; `not` negates normally (no SQL three-valued
// NULL). The implicit `author` system column can be queried like a string
// column.
export type RowFilter =
    | { p: 'true' }
    | { p: 'cmp'; cmp: CmpOp; left: Operand; right: Operand }
    | { p: 'str'; str: StrOp; value: Operand; sub: Operand }
    | { p: 'not'; arg: RowFilter }         // query-only (forbidden in restrictions)
    | { p: 'and'; args: RowFilter[] }
    | { p: 'or'; args: RowFilter[] };

export type OrderBy = { column: string; dir?: 'asc' | 'desc' };

export type RowQuery = {
    where?: RowFilter;                     // default: match all live rows
    select?: string[];                     // projection; default: all columns
    orderBy?: OrderBy[];
    limit?: number;
    offset?: number;
};

// The scalar column types of the queried table (what validation needs).
export type ColumnTypes = { [column: string]: ColumnType };

// ---------------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------------

export function evalRowFilter(filter: RowFilter, row: Row): boolean {
    const lookup = (column: string): json.Literal | undefined => column === 'author' ? row.author : row.values[column];

    switch (filter.p) {
        case 'true':
            return true;
        case 'cmp': {
            const l = evalOperand(filter.left, lookup);
            const r = evalOperand(filter.right, lookup);
            if (l === undefined || r === undefined) return false;
            return compareOperands(filter.cmp, l, r);
        }
        case 'str': {
            const v = evalOperand(filter.value, lookup);
            const s = evalOperand(filter.sub, lookup);
            if (typeof v !== 'string' || typeof s !== 'string') return false;
            if (filter.str === 'prefix') return v.startsWith(s);
            if (filter.str === 'suffix') return v.endsWith(s);
            return v.includes(s);
        }
        case 'not':
            return !evalRowFilter(filter.arg, row);
        case 'and':
            return filter.args.every((a) => evalRowFilter(a, row));
        case 'or':
            return filter.args.some((a) => evalRowFilter(a, row));
    }
}

// ---------------------------------------------------------------------------
// Post-processing (order / project)
// ---------------------------------------------------------------------------

// Total order over two present scalar values within their type; mixed or
// non-scalar values fall back to normalized-string comparison (deterministic).
function compareValues(a: json.Literal, b: json.Literal): number {
    if (typeof a === 'number' && typeof b === 'number') return a < b ? -1 : a > b ? 1 : 0;
    if (typeof a === 'string' && typeof b === 'string') return a < b ? -1 : a > b ? 1 : 0;
    if (typeof a === 'boolean' && typeof b === 'boolean') return (a ? 1 : 0) - (b ? 1 : 0);
    const sa = json.toStringNormalized(a);
    const sb = json.toStringNormalized(b);
    return sa < sb ? -1 : sa > sb ? 1 : 0;
}

// orderBy with rows missing a value sorted LAST (asc or desc), and a stable
// rowId tiebreak for total determinism.
export function orderRows(rows: Row[], orderBy: OrderBy[]): Row[] {
    return rows.slice().sort((x, y) => {
        for (const { column, dir } of orderBy) {
            const av = column === 'author' ? x.author : x.values[column];
            const bv = column === 'author' ? y.author : y.values[column];
            const aMissing = av === undefined;
            const bMissing = bv === undefined;
            if (aMissing || bMissing) {
                if (aMissing && bMissing) continue;
                return aMissing ? 1 : -1;   // missing last regardless of direction
            }
            let c = compareValues(av, bv);
            if (dir === 'desc') c = -c;
            if (c !== 0) return c;
        }
        return x.rowId < y.rowId ? -1 : x.rowId > y.rowId ? 1 : 0;
    });
}

// Projection: keep the row's identity (rowId / uuid / author) and restrict
// `values` to the selected columns. A select naming a column the row does not
// carry simply omits it (no error — validation already proved the columns
// exist in the schema).
export function projectRow(row: Row, select: string[]): Row {
    const values: RowValues = {};
    for (const column of select) {
        if (column !== 'author' && row.values[column] !== undefined) values[column] = row.values[column];
    }
    const projected: Row = { rowId: row.rowId, uuid: row.uuid, values };
    if (row.author !== undefined) projected.author = row.author;
    return projected;
}

// ---------------------------------------------------------------------------
// Validation (user-facing — throws descriptive errors)
// ---------------------------------------------------------------------------

function validateOperand(op: Operand, columns: ColumnTypes, depth: number): void {
    if (depth > MAX_EXPR_DEPTH) throw new Error("query operand nesting too deep");
    if (op === null || typeof op !== 'object' || Array.isArray(op)) {
        throw new Error("malformed query operand");
    }
    if ('lit' in op) {
        const v = op.lit;
        if (!(typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean')) {
            throw new Error("query operand literal must be a string, number or boolean");
        }
        return;
    }
    if ('col' in op) {
        if (typeof op.col !== 'string') throw new Error("query operand column must be a string");
        if (!(op.col in columns) && op.col !== 'author') throw new Error(`unknown column '${op.col}' in query filter`);
        return;
    }
    if ('fn' in op) {
        if (op.fn === 'len') {
            if (!Array.isArray(op.args) || op.args.length !== 1) {
                throw new Error("query operand 'len' takes exactly one argument");
            }
            validateOperand(op.args[0], columns, depth + 1);
            return;
        }
        if (ARITH_FNS.includes(op.fn)) {
            if (!Array.isArray(op.args) || op.args.length !== 2) {
                throw new Error(`query operand '${op.fn}' takes exactly two arguments`);
            }
            validateOperand(op.args[0], columns, depth + 1);
            validateOperand(op.args[1], columns, depth + 1);
            return;
        }
        throw new Error(`unknown query operand function '${String(op.fn)}'`);
    }
    throw new Error("malformed query operand");
}

function validateRowFilter(filter: RowFilter, columns: ColumnTypes, depth: number): void {
    if (depth > MAX_EXPR_DEPTH) throw new Error("query filter nesting too deep");
    if (filter === null || typeof filter !== 'object' || Array.isArray(filter)) {
        throw new Error("malformed query filter");
    }

    const typeOf = (column: string): ColumnType | undefined => column === 'author' ? 'string' : columns[column];

    switch (filter.p) {
        case 'true':
            return;
        case 'cmp': {
            if (!CMP_OPS.includes(filter.cmp)) throw new Error(`unknown comparison operator '${String(filter.cmp)}'`);
            validateOperand(filter.left, columns, depth + 1);
            validateOperand(filter.right, columns, depth + 1);
            if (!cmpTypesOk(filter.cmp, filter.left, filter.right, typeOf)) {
                throw new Error("type-incoherent comparison in query filter");
            }
            return;
        }
        case 'str': {
            if (!STR_OPS.includes(filter.str)) throw new Error(`unknown string operator '${String(filter.str)}'`);
            validateOperand(filter.value, columns, depth + 1);
            validateOperand(filter.sub, columns, depth + 1);
            if (!strTypesOk(filter.value, filter.sub, typeOf)) {
                throw new Error("non-string operand in string-match query filter");
            }
            return;
        }
        case 'not':
            validateRowFilter(filter.arg, columns, depth + 1);
            return;
        case 'and':
        case 'or': {
            if (!Array.isArray(filter.args) || filter.args.length === 0 || filter.args.length > MAX_EXPR_ARGS) {
                throw new Error(`query '${filter.p}' filter requires 1..${MAX_EXPR_ARGS} arguments`);
            }
            for (const arg of filter.args) validateRowFilter(arg, columns, depth + 1);
            return;
        }
        default:
            throw new Error(`unknown query filter '${String((filter as { p: unknown }).p)}'`);
    }
}

function isNonNegativeInteger(n: number): boolean {
    return typeof n === 'number' && Number.isInteger(n) && n >= 0;
}

// Validate a query against the table's columns; throws a descriptive Error on
// any user mistake. Returns normally for a valid query.
export function validateRowQuery(q: RowQuery, columns: ColumnTypes): void {
    if (q === null || typeof q !== 'object' || Array.isArray(q)) {
        throw new Error("malformed query");
    }

    if (q.where !== undefined) validateRowFilter(q.where, columns, 0);

    if (q.select !== undefined) {
        if (!Array.isArray(q.select)) throw new Error("query 'select' must be an array of column names");
        for (const column of q.select) {
            if (typeof column !== 'string' || (!(column in columns) && column !== 'author')) {
                throw new Error(`unknown column '${String(column)}' in query select`);
            }
        }
    }

    if (q.orderBy !== undefined) {
        if (!Array.isArray(q.orderBy)) throw new Error("query 'orderBy' must be an array");
        for (const ob of q.orderBy) {
            if (ob === null || typeof ob !== 'object' || typeof ob.column !== 'string' || (!(ob.column in columns) && ob.column !== 'author')) {
                throw new Error(`unknown column '${String(ob?.column)}' in query orderBy`);
            }
            if (ob.dir !== undefined && ob.dir !== 'asc' && ob.dir !== 'desc') {
                throw new Error("query orderBy 'dir' must be 'asc' or 'desc'");
            }
        }
    }

    if (q.limit !== undefined && !isNonNegativeInteger(q.limit)) {
        throw new Error("query 'limit' must be a non-negative integer");
    }
    if (q.offset !== undefined && !isNonNegativeInteger(q.offset)) {
        throw new Error("query 'offset' must be a non-negative integer");
    }
}
