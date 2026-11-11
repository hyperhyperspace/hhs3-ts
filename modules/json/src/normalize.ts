import { Literal } from "./literal.js";

// The canonical text of a Literal, as RFC 8785 (JSON Canonicalization Scheme)
// defines it: no whitespace, object keys sorted by UTF-16 code unit, arrays in
// index order, strings and numbers as JSON.stringify writes them. Every
// content hash is the hash of this text in UTF-8, and JSON.parse reads it back
// to the same literal.
//
// It serializes; it does not validate. Run literalProblem first on a value
// that comes from outside.
function toStringNormalized(literal: Literal): string {
    if (typeof literal === 'string') return JSON.stringify(literal);
    if (typeof literal === 'boolean') return literal ? 'true' : 'false';
    if (typeof literal === 'number') {
        if (!Number.isFinite(literal)) {
            throw new Error('Cannot serialize ' + literal + ': NaN and Infinity are not valid Literals.');
        }
        return Object.is(literal, -0) ? '0' : JSON.stringify(literal);
    }
    if (Array.isArray(literal)) return '[' + literal.map((element) => toStringNormalized(element)).join(',') + ']';
    if (typeof literal === 'object' && literal !== null) {
        const keys = Object.keys(literal).sort();
        return '{' + keys.map((key) => JSON.stringify(key) + ':' + toStringNormalized(literal[key])).join(',') + '}';
    }
    throw new Error('Cannot serialize ' + literal + ', its type ' + (typeof literal) + ' is illegal for a literal.');
}

// Whether a string is well-formed Unicode: every surrogate is half of a pair.
// A lone surrogate has no UTF-8 encoding. Encoders replace it with U+FFFD, so
// strings that differ only in their lone surrogates would share one hash.
function isWellFormedString(s: string): boolean {
    for (let i = 0; i < s.length; i++) {
        const c = s.charCodeAt(i);
        if (c >= 0xd800 && c <= 0xdbff) {
            const d = i + 1 < s.length ? s.charCodeAt(i + 1) : 0;
            if (d < 0xdc00 || d > 0xdfff) return false;
            i++;
        } else if (c >= 0xdc00 && c <= 0xdfff) {
            return false;
        }
    }
    return true;
}

const ILL_FORMED_STRING = 'a string that is not well-formed Unicode';
const ILL_FORMED_KEY = 'a key that is not well-formed Unicode';

// How deep a Literal may nest. A scalar has depth 0; an array or an object has
// one more than its deepest member, and 1 when empty. JSON.parse reads text
// nested millions of levels deep, while the code that then walks the value
// (this check, the serializer, the engine's JSON.stringify) recurses and runs
// out of stack at a depth that differs between runtimes. A fixed limit makes
// every runtime accept the same values.
const MAX_LITERAL_DEPTH = 128;

// undefined when `value` is a Literal, else the first thing found in it that a
// Literal cannot contain, as a noun phrase: 'null', 'a non-finite number',
// 'a string that is not well-formed Unicode', 'more than 128 levels of
// nesting', and so on.
//
// `maxDepth` is for a caller that checks a value wrapping a Literal, such as
// a message that carries a payload: it adds the depth of its own wrapping.
function literalProblem(value: unknown, maxDepth: number = MAX_LITERAL_DEPTH): string | undefined {
    return problemWithin(value, maxDepth, maxDepth);
}

// `levelsLeft` counts down from `maxDepth`, so the recursion stops at the
// limit and never depends on how much stack the runtime has.
function problemWithin(value: unknown, levelsLeft: number, maxDepth: number): string | undefined {
    if (typeof value === 'string') return isWellFormedString(value) ? undefined : ILL_FORMED_STRING;
    if (typeof value === 'boolean') return undefined;
    if (typeof value === 'number') return Number.isFinite(value) ? undefined : 'a non-finite number';
    if (value === null) return 'null';
    if (typeof value !== 'object') return 'a value of type ' + (typeof value);

    if (levelsLeft <= 0) return 'more than ' + maxDepth + ' levels of nesting';

    if (Array.isArray(value)) {
        for (const element of value) {
            const problem = problemWithin(element, levelsLeft - 1, maxDepth);
            if (problem !== undefined) return problem;
        }
        return undefined;
    }
    for (const key of Object.keys(value)) {
        if (!isWellFormedString(key)) return ILL_FORMED_KEY;
        const problem = problemWithin((value as { [key: string]: unknown })[key], levelsLeft - 1, maxDepth);
        if (problem !== undefined) return problem;
    }
    return undefined;
}

// Whether a parsed value is a Literal: at any depth, no null, no non-finite
// number, no string or key with a lone surrogate, and no more than
// MAX_LITERAL_DEPTH levels of nesting.
function isLiteral(value: unknown): value is Literal {
    return literalProblem(value) === undefined;
}

function eq(a: Literal, b: Literal): boolean {
    return toStringNormalized(a) === toStringNormalized(b);
}

function strongEq(a?: Literal, b?: Literal): boolean {
    if (a === undefined && b === undefined) {
        return true;
    }
    if (a === undefined || b === undefined) {
        return false;
    }
    return eq(a, b);
}

export { toStringNormalized, MAX_LITERAL_DEPTH, isLiteral, literalProblem, isWellFormedString, eq, strongEq };