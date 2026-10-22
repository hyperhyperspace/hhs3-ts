// Predicate evaluation and the projected-storage sort order. `evalWhere` false
// is a proof a row is outside the predicate. Sort comparison matches SQLite
// BINARY on text (Unicode code points) and treats NULL as smaller than every
// other value.

import type { json } from "@hyper-hyper-space/hhs3_json";
import { likeMatch } from "@hyper-hyper-space/hhs3_rdb";
import type { ColumnType } from "@hyper-hyper-space/hhs3_rdb";

import type { BoundOperand, BoundValue, BoundWhereExpr, OrderBySpec, ProjectedRow } from "./types.js";

export type Cell = json.Literal | null | undefined;
export type RowView = { [column: string]: Cell };

const TEXT_TYPES = new Set<ColumnType>(['string', 'bigint', 'decimal', 'bytes', 'json', 'identity']);

export function evalWhere(expr: BoundWhereExpr | undefined, row: RowView, localId?: number): boolean {
    if (expr === undefined) return true;
    return evalPredicate(expr, row, localId) === true;
}

function evalPredicate(expr: BoundWhereExpr, row: RowView, localId: number | undefined): boolean | null {
    switch (expr.kind) {
        case 'not': {
            const v = evalPredicate(expr.arg, row, localId);
            return v === null ? null : !v;
        }
        case 'and': {
            let unknown = false;
            for (const arg of expr.args) {
                const v = evalPredicate(arg, row, localId);
                if (v === false) return false;
                if (v === null) unknown = true;
            }
            return unknown ? null : true;
        }
        case 'or': {
            let unknown = false;
            for (const arg of expr.args) {
                const v = evalPredicate(arg, row, localId);
                if (v === true) return true;
                if (v === null) unknown = true;
            }
            return unknown ? null : false;
        }
        case 'isNull':
            return (evalOperand(expr.arg, row, localId) === null) !== expr.not;
        case 'like': {
            const value = evalOperand(expr.arg, row, localId);
            const pattern = evalOperand(expr.pattern, row, localId);
            if (value === null || pattern === null) return null;
            if (typeof value !== 'string' || typeof pattern !== 'string') return false;
            return likeMatch(value, pattern);
        }
        case 'in': {
            const value = evalOperand(expr.arg, row, localId);
            if (value === null) return null;
            let sawNull = false;
            for (const candidate of expr.values) {
                if (candidate === null) { sawNull = true; continue; }
                if (sqlEqual(value, candidate)) return expr.not ? false : true;
            }
            if (sawNull) return null;
            return expr.not ? true : false;
        }
        case 'cmp': {
            const left = evalOperand(expr.left, row, localId);
            const right = evalOperand(expr.right, row, localId);
            if (left === null || right === null) return null;
            const order = compareProjectedValues(left, right);
            if (order === null) return false;
            switch (expr.op) {
                case '=': return order === 0;
                case '!=': return order !== 0;
                case '<': return order < 0;
                case '<=': return order <= 0;
                case '>': return order > 0;
                case '>=': return order >= 0;
            }
        }
    }
}

function evalOperand(operand: BoundOperand, row: RowView, localId: number | undefined): BoundValue {
    switch (operand.kind) {
        case 'lit': return operand.value;
        case 'col': {
            const value = row[operand.name];
            if (value === undefined && operand.name === 'id' && localId !== undefined) return localId;
            return value === undefined ? null : value;
        }
        case 'length': {
            const value = evalOperand(operand.arg, row, localId);
            if (value === null) return null;
            return typeof value === 'string' ? Array.from(value).length : null;
        }
        case 'arith': {
            const left = evalOperand(operand.left, row, localId);
            const right = evalOperand(operand.right, row, localId);
            if (typeof left !== 'number' || typeof right !== 'number') return null;
            switch (operand.op) {
                case '+': return left + right;
                case '-': return left - right;
                case '*': return left * right;
            }
        }
    }
}

function sqlEqual(a: json.Literal, b: json.Literal): boolean {
    return compareProjectedValues(a, b) === 0;
}

// NULL is smaller than every non-null value. Text compares by Unicode code
// point, which is SQLite's UTF-8 binary order. Numbers and booleans compare
// numerically (false < true, matching stored 0/1).
export function compareProjectedValues(
    a: Cell, b: Cell, type?: ColumnType,
): number | null {
    const aNull = a === null || a === undefined;
    const bNull = b === null || b === undefined;
    if (aNull || bNull) {
        if (aNull && bNull) return 0;
        return aNull ? -1 : 1;
    }
    if (type === 'boolean' || typeof a === 'boolean' || typeof b === 'boolean') {
        const an = a === true || a === 1 ? 1 : a === false || a === 0 ? 0 : null;
        const bn = b === true || b === 1 ? 1 : b === false || b === 0 ? 0 : null;
        if (an === null || bn === null) return null;
        return an - bn;
    }
    if ((type === undefined || type === 'integer' || type === 'float') && typeof a === 'number' && typeof b === 'number') {
        if (Number.isNaN(a) || Number.isNaN(b)) return null;
        return a < b ? -1 : a > b ? 1 : 0;
    }
    if (type === undefined || TEXT_TYPES.has(type)) {
        if (typeof a === 'string' && typeof b === 'string') return compareCodePoints(a, b);
        if (typeof a === 'number' && typeof b === 'number') return a < b ? -1 : a > b ? 1 : 0;
    }
    if (typeof a === 'string' && typeof b === 'string') return compareCodePoints(a, b);
    if (typeof a === 'number' && typeof b === 'number') return a < b ? -1 : a > b ? 1 : 0;
    return null;
}

export function compareCodePoints(a: string, b: string): number {
    const aa = Array.from(a);
    const bb = Array.from(b);
    const n = Math.min(aa.length, bb.length);
    for (let i = 0; i < n; i++) {
        const ca = aa[i]!.codePointAt(0)!;
        const cb = bb[i]!.codePointAt(0)!;
        if (ca !== cb) return ca < cb ? -1 : 1;
    }
    return aa.length === bb.length ? 0 : aa.length < bb.length ? -1 : 1;
}

export function compareSortTuples(
    left: RowView, right: RowView, orderBy: readonly OrderBySpec[],
    typeOf?: (column: string) => ColumnType | undefined,
): number {
    for (const { column, direction } of orderBy) {
        let order = compareProjectedValues(left[column], right[column], typeOf?.(column));
        if (order === null) order = 0;
        if (direction === 'desc') order = -order;
        if (order !== 0) return order;
    }
    return 0;
}

export function normalizeOrder(
    orderBy: readonly OrderBySpec[] | undefined, idColumn: string, needsOrder: boolean,
): OrderBySpec[] {
    const given = orderBy === undefined ? [] : [...orderBy];
    if (given.length === 0) return needsOrder ? [{ column: idColumn, direction: 'asc' }] : [];
    const last = given[given.length - 1]!;
    if (last.column !== idColumn) given.push({ column: idColumn, direction: last.direction });
    return given;
}

// BEFORE/AFTER compare the order tuple in value order. Sort direction decides
// which end LIMIT keeps, not which side of the anchor the bound selects.
export function rowOnSide(
    row: RowView, anchor: RowView, orderBy: readonly OrderBySpec[], side: 'before' | 'after',
    typeOf?: (column: string) => ColumnType | undefined,
): boolean {
    for (const { column } of orderBy) {
        const order = compareProjectedValues(row[column], anchor[column], typeOf?.(column));
        if (order === null || order === 0) continue;
        const lesser = order < 0;
        return side === 'before' ? lesser : !lesser;
    }
    return false;
}

export function tupleOf(row: RowView, orderBy: readonly OrderBySpec[]): { [column: string]: json.Literal | null } {
    const tuple: { [column: string]: json.Literal | null } = {};
    for (const { column } of orderBy) {
        const value = row[column];
        tuple[column] = value === undefined ? null : value;
    }
    return tuple;
}

export function projectColumns(row: ProjectedRow, select: readonly string[] | undefined, idColumn: string): ProjectedRow {
    if (select === undefined) return row;
    const out: ProjectedRow = { id: row.id };
    const keep = new Set(select);
    keep.add(idColumn);
    keep.add('id');
    for (const column of keep) {
        if (column === 'id') continue;
        if (column in row) out[column] = row[column] ?? null;
    }
    return out;
}
