// Shared value-expression primitives for the operand / cmp / str grammar.
//
// These are grammar-agnostic and pure: they operate over a column lookup
// callback, so the SAME core serves two callers with different front-ends:
//
//   - restriction predicates (BFT, at-use voiding): see ../rtable_group/
//     predicates.ts. The restriction grammar is a CONSTRAINED subset (positive
//     logic, readonly-only $row refs, integer-only arithmetic) and its
//     behavior must not change — this module is a pure extraction of the logic
//     that already lived in predicates.ts / validate.ts.
//   - single-table queries (local reads, richer grammar): see ../rtable/
//     query.ts. Queries may filter over any column and add `not`, but reuse
//     these same operand evaluation and type-check rules.
//
// Nothing here touches the DAG, identities, or voiding: it is value math over
// a row's resolved column values.

import { json } from "@hyper-hyper-space/hhs3_json";

import type { ColumnType, CmpOp, Operand } from "./payload.js";

// Evaluate an operand to a concrete literal, or undefined when it cannot be
// resolved (missing column, non-string len arg, or non-integer / unsafe
// arithmetic). undefined collapses the enclosing atom to false. `lookup`
// resolves a `{col}` operand to the row's value for that column.
export function evalOperand(op: Operand, lookup: (column: string) => json.Literal | undefined): json.Literal | undefined {
    if ('lit' in op) return op.lit;
    if ('col' in op) return lookup(op.col);

    if (op.fn === 'len') {
        const v = evalOperand(op.args[0], lookup);
        return typeof v === 'string' ? v.length : undefined;
    }

    const a = evalOperand(op.args[0], lookup);
    const b = evalOperand(op.args[1], lookup);
    if (typeof a !== 'number' || typeof b !== 'number') return undefined;
    if (!Number.isInteger(a) || !Number.isInteger(b)) return undefined;

    const r = op.fn === 'add' ? a + b : op.fn === 'sub' ? a - b : a * b;
    return Number.isSafeInteger(r) ? r : undefined;
}

function order<T extends number | string>(cmp: CmpOp, l: T, r: T): boolean {
    switch (cmp) {
        case 'lt': return l < r;
        case 'le': return l <= r;
        case 'gt': return l > r;
        case 'ge': return l >= r;
        default: return false;
    }
}

// Compare two resolved literals under `cmp`. eq/ne use normalized-string
// equality (works for any scalar); ordering is defined only for two numbers or
// two strings (otherwise false).
export function compareOperands(cmp: CmpOp, l: json.Literal, r: json.Literal): boolean {
    if (cmp === 'eq') return json.toStringNormalized(l) === json.toStringNormalized(r);
    if (cmp === 'ne') return json.toStringNormalized(l) !== json.toStringNormalized(r);

    if (typeof l === 'number' && typeof r === 'number') return order(cmp, l, r);
    if (typeof l === 'string' && typeof r === 'string') return order(cmp, l, r);
    return false;
}

// The scalar type an operand resolves to over a column-type lookup, or
// undefined if it cannot be type-checked (unknown column, type-incoherent
// arithmetic, or a non-scalar literal). Arithmetic is integer-only; len yields
// an integer from a string.
export function operandType(op: Operand, typeOf: (column: string) => ColumnType | undefined): ColumnType | undefined {
    if ('lit' in op) {
        const v = op.lit;
        if (typeof v === 'string') return 'string';
        if (typeof v === 'number') return Number.isInteger(v) ? 'integer' : 'float';
        if (typeof v === 'boolean') return 'boolean';
        return undefined;   // null / object: not a comparable scalar
    }
    if ('col' in op) {
        return typeOf(op.col);
    }
    if (op.fn === 'len') {
        return operandType(op.args[0], typeOf) === 'string' ? 'integer' : undefined;
    }
    // add / sub / mul: integer-only
    return operandType(op.args[0], typeOf) === 'integer'
        && operandType(op.args[1], typeOf) === 'integer'
        ? 'integer'
        : undefined;
}

// Type coherence of a cmp atom's two operands: same scalar type, and a type
// that supports the operator (eq/ne over any scalar; ordering excludes boolean).
export function cmpTypesOk(cmp: CmpOp, left: Operand, right: Operand, typeOf: (column: string) => ColumnType | undefined): boolean {
    const lt = operandType(left, typeOf);
    const rt = operandType(right, typeOf);
    if (lt === undefined || rt === undefined || lt !== rt) return false;
    if (cmp === 'eq' || cmp === 'ne') {
        return ['integer', 'float', 'string', 'boolean'].includes(lt);
    }
    return ['integer', 'float', 'string'].includes(lt);   // ordering
}

// Type coherence of a str atom: both operands resolve to strings.
export function strTypesOk(value: Operand, sub: Operand, typeOf: (column: string) => ColumnType | undefined): boolean {
    return operandType(value, typeOf) === 'string'
        && operandType(sub, typeOf) === 'string';
}
