// Shared value-expression primitives for the operand / cmp / like grammar.
//
// These are grammar-agnostic and pure: they operate over a column lookup
// callback, so the SAME core serves two callers with different front-ends:
//
//   - restriction predicates (BFT validation + at-use rechecks): see
//     ../rtable_group/predicates.ts. The restriction grammar is a CONSTRAINED
//     subset (positive logic, readonly-only $row refs, integer-only arithmetic)
//     and its behavior must not change — this module is a pure extraction of
//     the logic that already lived in predicates.ts / validate.ts.
//   - single-table queries (local reads, richer grammar): see ../rtable/
//     query.ts. Queries may filter over any column and add `not`, but reuse
//     these same operand evaluation and type-check rules.
//
// Nothing here touches the DAG, identities, or voiding: it is value math over
// a row's resolved column values.

import { json } from "@hyper-hyper-space/hhs3_json";

import type { ColumnType, CmpOp, Operand } from "./payload.js";
import { compareNumericStr, isCanonicalBigint, parseDecimal, bigintArith, decimalArith } from "./canonical.js";

// Evaluate an operand to a concrete literal, or undefined when it cannot be
// resolved (missing column, non-string len arg, or type-incoherent / unsafe
// arithmetic). undefined collapses the enclosing atom to false. `lookup`
// resolves a `{col}` operand to the row's value for that column.
//
// Arithmetic (add/sub/mul) is defined on:
//   - integer operands (JS numbers): exact within the safe-integer range;
//   - bigint operands (canonical strings): exact via BigInt;
//   - decimal operands (canonical strings): exact via scaled-integer, with the
//     scale read from the operand strings (add/sub align to the larger scale,
//     mul sums scales).
// Mixed or non-numeric operands resolve to undefined.
export function evalOperand(op: Operand, lookup: (column: string) => json.Literal | undefined): json.Literal | undefined {
    if ('lit' in op) return op.lit;
    if ('col' in op) return lookup(op.col);

    if (op.fn === 'len') {
        const v = evalOperand(op.args[0], lookup);
        return typeof v === 'string' ? v.length : undefined;
    }

    const a = evalOperand(op.args[0], lookup);
    const b = evalOperand(op.args[1], lookup);
    if (a === undefined || b === undefined) return undefined;

    if (typeof a === 'number' && typeof b === 'number') {
        if (!Number.isInteger(a) || !Number.isInteger(b)) return undefined;
        const r = op.fn === 'add' ? a + b : op.fn === 'sub' ? a - b : a * b;
        return Number.isSafeInteger(r) ? r : undefined;
    }

    if (typeof a === 'string' && typeof b === 'string') {
        if (isCanonicalBigint(a) && isCanonicalBigint(b)) return bigintArith(op.fn, a, b);
        if (parseDecimal(a) !== undefined && parseDecimal(b) !== undefined) return decimalArith(op.fn, a, b);
        return undefined;
    }

    return undefined;
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

const NUMERIC_TYPES: ColumnType[] = ['integer', 'float', 'bigint', 'decimal'];
// String-carried types a bare string literal can stand in for.
const STRING_CARRIED: ColumnType[] = ['string', 'bigint', 'decimal', 'bytes', 'identity'];

// Compare two resolved literals under `cmp`. eq/ne use normalized-string
// equality (works for any scalar, and for the canonical string carriers of
// bigint/decimal/bytes). Ordering uses type-aware numeric comparison when a
// numeric `type` is supplied (so bigint/decimal order by value, not lexically),
// falling back to JS ordering over two numbers or two strings otherwise.
export function compareOperands(cmp: CmpOp, l: json.Literal, r: json.Literal, type?: ColumnType): boolean {
    if (cmp === 'eq') return json.toStringNormalized(l) === json.toStringNormalized(r);
    if (cmp === 'ne') return json.toStringNormalized(l) !== json.toStringNormalized(r);

    if (type !== undefined && NUMERIC_TYPES.includes(type)) {
        return order(cmp, compareNumericStr(l, r, type), 0);
    }
    if (typeof l === 'number' && typeof r === 'number') return order(cmp, l, r);
    if (typeof l === 'string' && typeof r === 'string') return order(cmp, l, r);
    return false;
}

// The scalar type an operand resolves to over a column-type lookup, or
// undefined if it cannot be type-checked. `hint` disambiguates a bare string
// literal (which could stand for any string-carried type) toward the expected
// type of its sibling operand. Arithmetic (add/sub/mul) preserves the shared
// numeric family (integer / bigint / decimal); len yields an integer.
export function operandType(op: Operand, typeOf: (column: string) => ColumnType | undefined, hint?: ColumnType): ColumnType | undefined {
    if ('lit' in op) {
        const v = op.lit;
        if (typeof v === 'string') {
            return hint !== undefined && STRING_CARRIED.includes(hint) ? hint : 'string';
        }
        if (typeof v === 'number') return Number.isInteger(v) ? 'integer' : 'float';
        if (typeof v === 'boolean') return 'boolean';
        return undefined;   // null / object: not a comparable scalar
    }
    if ('col' in op) {
        return typeOf(op.col);
    }
    if (op.fn === 'len') {
        return operandType(op.args[0], typeOf, 'string') === 'string' ? 'integer' : undefined;
    }
    // add / sub / mul: both args must resolve to the same numeric family. A
    // 'string' hint is uninformative here (arithmetic never yields a string),
    // so bare string literals are disambiguated by the sibling argument.
    const h = hint === 'string' ? undefined : hint;
    const b0 = operandType(op.args[1], typeOf, h);
    const a = operandType(op.args[0], typeOf, h ?? b0);
    const b = operandType(op.args[1], typeOf, h ?? a);
    const at = a ?? b;
    const bt = b ?? a;
    if (at === undefined || at !== bt) return undefined;
    return at === 'integer' || at === 'bigint' || at === 'decimal' ? at : undefined;
}

// Resolve the common type of a cmp atom's two operands, disambiguating string
// literals against the concrete side, or undefined if incoherent.
export function resolveCmpType(left: Operand, right: Operand, typeOf: (column: string) => ColumnType | undefined): ColumnType | undefined {
    const rt0 = operandType(right, typeOf);
    const lt = operandType(left, typeOf, rt0);
    const rt = operandType(right, typeOf, lt);
    if (lt === undefined || rt === undefined || lt !== rt) return undefined;
    return lt;
}

// Type coherence of a cmp atom's two operands: a common type that supports the
// operator (eq/ne over any scalar including identity; identity also equals
// string, for key-hash columns). Ordering excludes boolean, bytes, and identity.
export function cmpTypesOk(cmp: CmpOp, left: Operand, right: Operand, typeOf: (column: string) => ColumnType | undefined): boolean {
    const t = resolveCmpType(left, right, typeOf);
    if (cmp === 'eq' || cmp === 'ne') {
        if (t !== undefined) {
            return ['integer', 'float', 'string', 'boolean', 'bigint', 'decimal', 'bytes', 'identity'].includes(t);
        }
        const lt = operandType(left, typeOf);
        const rt = operandType(right, typeOf);
        return (lt === 'identity' && rt === 'string') || (lt === 'string' && rt === 'identity');
    }
    return t !== undefined && ['integer', 'float', 'string', 'bigint', 'decimal'].includes(t);
}

// Type coherence of a like atom: value and pattern both resolve to strings.
export function likeTypesOk(value: Operand, pattern: Operand, typeOf: (column: string) => ColumnType | undefined): boolean {
    return operandType(value, typeOf, 'string') === 'string'
        && operandType(pattern, typeOf, 'string') === 'string';
}

// LIKE patterns: `%` matches any run of code points (including none), `_`
// exactly one code point, and `\` makes the next code point literal. Matching
// is case-sensitive, with no other escape or collation rules.
const LIKE_ANY = 0;
const LIKE_ONE = 1;
type LikeToken = typeof LIKE_ANY | typeof LIKE_ONE | string;

// undefined when the pattern ends in a lone `\`.
function tokenizeLikePattern(pattern: string): LikeToken[] | undefined {
    const cps = Array.from(pattern);
    const tokens: LikeToken[] = [];
    for (let k = 0; k < cps.length; k++) {
        const c = cps[k];
        if (c === '\\') {
            if (k + 1 === cps.length) return undefined;
            tokens.push(cps[++k]);
        } else if (c === '%') {
            tokens.push(LIKE_ANY);
        } else if (c === '_') {
            tokens.push(LIKE_ONE);
        } else {
            tokens.push(c);
        }
    }
    return tokens;
}

export function isValidLikePattern(pattern: string): boolean {
    return tokenizeLikePattern(pattern) !== undefined;
}

// Whether `value` matches the LIKE `pattern`. A malformed pattern matches
// nothing. Backtracks only to the most recent `%`, so it runs in
// O(len(value) * len(pattern)) regardless of the pattern shape.
export function likeMatch(value: string, pattern: string): boolean {
    const tokens = tokenizeLikePattern(pattern);
    if (tokens === undefined) return false;
    const v = Array.from(value);

    let i = 0;
    let j = 0;
    let anyAt = -1;     // token index of the last `%` seen
    let anyFrom = 0;    // value index that `%` is currently extended to
    while (i < v.length) {
        const t = tokens[j];
        if (j < tokens.length && t !== LIKE_ANY && (t === LIKE_ONE || t === v[i])) {
            i++;
            j++;
        } else if (j < tokens.length && t === LIKE_ANY) {
            anyAt = j++;
            anyFrom = i;
        } else if (anyAt >= 0) {
            j = anyAt + 1;
            i = ++anyFrom;
        } else {
            return false;
        }
    }
    while (j < tokens.length && tokens[j] === LIKE_ANY) j++;
    return j === tokens.length;
}
