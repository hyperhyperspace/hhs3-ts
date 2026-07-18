// Canonical carriers, range checks, comparison and exact arithmetic for the
// string-carried column types (bigint, decimal, bytes) plus the numeric helpers
// used by expr.ts.
//
// bigint / decimal / bytes values live in RowValues as canonical STRINGS. The
// canonical form is the single string that json.toStringNormalized will hash,
// so it must be unique per value (no leading zeros, one representation of zero,
// fixed decimal scale, canonical base64). Normalization is deterministic and
// LOSSLESS: inputs that cannot be represented exactly (e.g. a decimal with more
// fractional digits than the column scale) are REJECTED (undefined), never
// rounded.

import { json } from "@hyper-hyper-space/hhs3_json";

import type { ColumnType, ColumnConstraints } from "./payload.js";

// --------------------------------------------------------------------------
// bigint
// --------------------------------------------------------------------------

const BIGINT_CANONICAL = /^(0|-?[1-9][0-9]*)$/;
const BIGINT_LOOSE = /^[+-]?[0-9]+$/;

export function isCanonicalBigint(s: string): boolean {
    return BIGINT_CANONICAL.test(s) && s !== '-0';
}

// Coerce a string- or number-form integer input to canonical bigint form, or
// undefined if it is not an exact integer.
export function normalizeBigint(input: string | number): string | undefined {
    let s: string;
    if (typeof input === 'number') {
        if (!Number.isInteger(input)) return undefined;
        s = BigInt(input).toString();
    } else {
        if (!BIGINT_LOOSE.test(input)) return undefined;
        s = BigInt(input).toString();   // strips +, leading zeros, -0
    }
    return s;
}

// --------------------------------------------------------------------------
// decimal (exact fixed-point)
// --------------------------------------------------------------------------

export type DecimalParts = { unscaled: bigint; scale: number };

const DECIMAL_LOOSE = /^[+-]?[0-9]+(\.[0-9]+)?$/;

// Parse a well-formed decimal string into { unscaled, scale }. `scale` is the
// number of fractional digits actually present. Returns undefined if malformed.
export function parseDecimal(s: string): DecimalParts | undefined {
    if (!DECIMAL_LOOSE.test(s)) return undefined;
    const neg = s.startsWith('-');
    const body = s.startsWith('+') || s.startsWith('-') ? s.slice(1) : s;
    const dot = body.indexOf('.');
    const digits = dot < 0 ? body : body.slice(0, dot) + body.slice(dot + 1);
    const scale = dot < 0 ? 0 : body.length - dot - 1;
    let unscaled = BigInt(digits);
    if (neg) unscaled = -unscaled;
    return { unscaled, scale };
}

// Render an exact (unscaled, scale) as a canonical fixed-scale decimal string.
export function formatDecimal(unscaled: bigint, scale: number): string {
    const neg = unscaled < BigInt(0);
    let digits = (neg ? -unscaled : unscaled).toString();
    if (scale === 0) return neg && unscaled !== BigInt(0) ? '-' + digits : digits;
    if (digits.length <= scale) digits = '0'.repeat(scale - digits.length + 1) + digits;
    const intPart = digits.slice(0, digits.length - scale);
    const fracPart = digits.slice(digits.length - scale);
    const body = `${intPart}.${fracPart}`;
    return neg && unscaled !== BigInt(0) ? '-' + body : body;
}

// Coerce a string- or number-form decimal input to the canonical form at the
// given scale. Returns undefined if malformed OR if it carries MORE fractional
// digits than `scale` (reject, never round).
export function normalizeDecimal(input: string | number, scale: number): string | undefined {
    const s = typeof input === 'number'
        ? (Number.isFinite(input) ? formatFloatAsDecimalInput(input) : undefined)
        : input;
    if (s === undefined) return undefined;
    const parts = parseDecimal(s);
    if (parts === undefined) return undefined;
    if (parts.scale > scale) return undefined;   // would lose precision
    const unscaled = parts.unscaled * BigInt(10) ** BigInt(scale - parts.scale);
    return formatDecimal(unscaled, scale);
}

// A JS number typed literally in SQL (e.g. 1.5) is turned into a plain decimal
// string without exponent notation for parseDecimal to consume.
function formatFloatAsDecimalInput(n: number): string | undefined {
    const s = n.toString();
    if (s.includes('e') || s.includes('E')) return undefined;   // out of exact decimal range
    return s;
}

export function isCanonicalDecimal(s: string, scale: number, precision?: number): boolean {
    if (typeof scale !== 'number' || scale < 0) return false;
    if (normalizeDecimal(s, scale) !== s) return false;
    if (precision !== undefined) {
        const parts = parseDecimal(s);
        if (parts === undefined) return false;
        const absUnscaled = parts.unscaled < BigInt(0) ? -parts.unscaled : parts.unscaled;
        if (absUnscaled >= BigInt(10) ** BigInt(precision)) return false;
    }
    return true;
}

// --------------------------------------------------------------------------
// bytes (canonical base64, RFC 4648 standard alphabet, fixed padding)
// --------------------------------------------------------------------------

const B64_ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';
const B64_LOOKUP: { [c: string]: number } = (() => {
    const m: { [c: string]: number } = {};
    for (let i = 0; i < B64_ALPHABET.length; i++) m[B64_ALPHABET[i]] = i;
    return m;
})();
const B64_SHAPE = /^[A-Za-z0-9+/]*={0,2}$/;

function base64Decode(s: string): Uint8Array | undefined {
    if (s.length % 4 !== 0 || !B64_SHAPE.test(s)) return undefined;
    if (s.length === 0) return new Uint8Array(0);
    let pad = 0;
    if (s.endsWith('==')) pad = 2; else if (s.endsWith('=')) pad = 1;
    const outLen = (s.length / 4) * 3 - pad;
    const out = new Uint8Array(outLen);
    let oi = 0;
    for (let i = 0; i < s.length; i += 4) {
        const c0 = B64_LOOKUP[s[i]];
        const c1 = B64_LOOKUP[s[i + 1]];
        const c2 = s[i + 2] === '=' ? 0 : B64_LOOKUP[s[i + 2]];
        const c3 = s[i + 3] === '=' ? 0 : B64_LOOKUP[s[i + 3]];
        const n = (c0 << 18) | (c1 << 12) | (c2 << 6) | c3;
        if (oi < outLen) out[oi++] = (n >> 16) & 0xff;
        if (oi < outLen) out[oi++] = (n >> 8) & 0xff;
        if (oi < outLen) out[oi++] = n & 0xff;
    }
    return out;
}

function base64Encode(bytes: Uint8Array): string {
    let out = '';
    for (let i = 0; i < bytes.length; i += 3) {
        const b0 = bytes[i];
        const b1 = i + 1 < bytes.length ? bytes[i + 1] : 0;
        const b2 = i + 2 < bytes.length ? bytes[i + 2] : 0;
        const n = (b0 << 16) | (b1 << 8) | b2;
        out += B64_ALPHABET[(n >> 18) & 0x3f];
        out += B64_ALPHABET[(n >> 12) & 0x3f];
        out += i + 1 < bytes.length ? B64_ALPHABET[(n >> 6) & 0x3f] : '=';
        out += i + 2 < bytes.length ? B64_ALPHABET[n & 0x3f] : '=';
    }
    return out;
}

// Canonical iff it decodes AND re-encodes to itself (rules out non-zero padding
// bits and non-standard shapes that would give two encodings for one byte
// string).
export function isCanonicalBase64(s: string): boolean {
    const bytes = base64Decode(s);
    if (bytes === undefined) return false;
    return base64Encode(bytes) === s;
}

export function normalizeBase64(s: string): string | undefined {
    const bytes = base64Decode(s);
    if (bytes === undefined) return undefined;
    return base64Encode(bytes);
}

export function base64ByteLen(s: string): number {
    const bytes = base64Decode(s);
    return bytes === undefined ? 0 : bytes.length;
}

// --------------------------------------------------------------------------
// Range checks (inclusive), against canonical string bounds in constraints
// --------------------------------------------------------------------------

export function intInRange(value: number, c?: ColumnConstraints): boolean {
    if (c === undefined) return true;
    const v = BigInt(value);
    if (c.min !== undefined && v < BigInt(c.min)) return false;
    if (c.max !== undefined && v > BigInt(c.max)) return false;
    return true;
}

export function bigintInRange(value: string, c?: ColumnConstraints): boolean {
    if (c === undefined) return true;
    const v = BigInt(value);
    if (c.min !== undefined && v < BigInt(c.min)) return false;
    if (c.max !== undefined && v > BigInt(c.max)) return false;
    return true;
}

export function decInRange(value: string, c?: ColumnConstraints): boolean {
    if (c === undefined) return true;
    if (c.min !== undefined && compareDecimalStr(value, c.min) < 0) return false;
    if (c.max !== undefined && compareDecimalStr(value, c.max) > 0) return false;
    return true;
}

// --------------------------------------------------------------------------
// Comparison
// --------------------------------------------------------------------------

function compareDecimalStr(a: string, b: string): number {
    const pa = parseDecimal(a);
    const pb = parseDecimal(b);
    if (pa === undefined || pb === undefined) {
        return a < b ? -1 : a > b ? 1 : 0;
    }
    const scale = Math.max(pa.scale, pb.scale);
    const ua = pa.unscaled * BigInt(10) ** BigInt(scale - pa.scale);
    const ub = pb.unscaled * BigInt(10) ** BigInt(scale - pb.scale);
    return ua < ub ? -1 : ua > ub ? 1 : 0;
}

// Total order over two resolved literals of a KNOWN column type. Numeric types
// (integer/float/bigint/decimal) compare by value; other types fall back to
// normalized-string comparison (deterministic).
export function compareNumericStr(a: json.Literal, b: json.Literal, type: ColumnType): number {
    switch (type) {
        case 'integer':
        case 'float':
            if (typeof a === 'number' && typeof b === 'number') return a < b ? -1 : a > b ? 1 : 0;
            break;
        case 'bigint':
            if (typeof a === 'string' && typeof b === 'string') {
                const va = BigInt(a); const vb = BigInt(b);
                return va < vb ? -1 : va > vb ? 1 : 0;
            }
            break;
        case 'decimal':
            if (typeof a === 'string' && typeof b === 'string') return compareDecimalStr(a, b);
            break;
        default:
            break;
    }
    const sa = json.toStringNormalized(a);
    const sb = json.toStringNormalized(b);
    return sa < sb ? -1 : sa > sb ? 1 : 0;
}

// --------------------------------------------------------------------------
// Exact arithmetic (add / sub / mul) for bigint and decimal operands
// --------------------------------------------------------------------------

export type ArithOp = 'add' | 'sub' | 'mul';

export function bigintArith(op: ArithOp, a: string, b: string): string | undefined {
    if (!isCanonicalBigint(a) || !isCanonicalBigint(b)) return undefined;
    const va = BigInt(a); const vb = BigInt(b);
    const r = op === 'add' ? va + vb : op === 'sub' ? va - vb : va * vb;
    return r.toString();
}

// decimal add/sub align to max(scaleA, scaleB); mul uses scaleA + scaleB.
export function decimalArith(op: ArithOp, a: string, b: string): string | undefined {
    const pa = parseDecimal(a);
    const pb = parseDecimal(b);
    if (pa === undefined || pb === undefined) return undefined;
    if (op === 'mul') {
        return formatDecimal(pa.unscaled * pb.unscaled, pa.scale + pb.scale);
    }
    const scale = Math.max(pa.scale, pb.scale);
    const ua = pa.unscaled * BigInt(10) ** BigInt(scale - pa.scale);
    const ub = pb.unscaled * BigInt(10) ** BigInt(scale - pb.scale);
    return formatDecimal(op === 'add' ? ua + ub : ua - ub, scale);
}
