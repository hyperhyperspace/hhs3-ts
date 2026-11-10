import { Literal } from "./literal.js";

// Hash input only, not JSON text: array indices sort as strings ('10' before
// '2') and control characters are left raw. Every content hash depends on this
// exact output, so it must not change; use toStringCanonical for JSON text.
function toStringNormalized(literal: Literal): string {
    var plain = '';
    
    if (typeof literal === 'object') {

        const arr = Array.isArray(literal);

        plain = plain + (arr? '[' : '{');

        var keys = Object.keys(literal);
        keys.sort();

        
        let c = 0;
        keys.forEach(key => {
        plain = plain +
                (arr? '' : escapeString(key) + ':') + toStringNormalized((literal as any)[key]);

                c = c + 1;
                if (c<keys.length) {
                    plain = plain + ',';
                }
        });

        plain = plain + (arr? ']' : '}');
    } else if (typeof literal === 'string') {
        plain = escapeString(literal);
    } else if (typeof literal === 'boolean') {
        plain = literal.toString();
    } else if (typeof literal === 'number') {
        if (!Number.isFinite(literal)) {
            throw new Error('Cannot serialize ' + literal + ': NaN and Infinity are not valid Literals.');
        }
        plain = Object.is(literal, -0) ? '0' : literal.toString();
    } else {
        throw new Error('Cannot serialize ' + literal + ', its type ' + (typeof literal) + ' is illegal for a literal.');
    }

    return plain;
}

function escapeString(text: string) {
    return '"' + text.replaceAll("\\", "\\\\").replaceAll('"', '\\"') + '"';
}

// Deterministic JSON text that JSON.parse reads back to the same literal:
// arrays in index order, object keys sorted, strings fully escaped.
function toStringCanonical(literal: Literal): string {
    if (typeof literal === 'string') return JSON.stringify(literal);
    if (typeof literal === 'boolean') return literal ? 'true' : 'false';
    if (typeof literal === 'number') {
        if (!Number.isFinite(literal)) {
            throw new Error('Cannot serialize ' + literal + ': NaN and Infinity are not valid Literals.');
        }
        return Object.is(literal, -0) ? '0' : JSON.stringify(literal);
    }
    if (Array.isArray(literal)) return '[' + literal.map(toStringCanonical).join(',') + ']';
    if (typeof literal === 'object' && literal !== null) {
        const keys = Object.keys(literal).sort();
        return '{' + keys.map((key) => JSON.stringify(key) + ':' + toStringCanonical(literal[key])).join(',') + '}';
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

// undefined when `value` is a Literal, else the first thing found in it that a
// Literal cannot contain, as a noun phrase: 'null', 'a non-finite number',
// 'a string that is not well-formed Unicode', and so on.
function literalProblem(value: unknown): string | undefined {
    if (typeof value === 'string') return isWellFormedString(value) ? undefined : ILL_FORMED_STRING;
    if (typeof value === 'boolean') return undefined;
    if (typeof value === 'number') return Number.isFinite(value) ? undefined : 'a non-finite number';
    if (value === null) return 'null';
    if (Array.isArray(value)) {
        for (const element of value) {
            const problem = literalProblem(element);
            if (problem !== undefined) return problem;
        }
        return undefined;
    }
    if (typeof value === 'object') {
        for (const key of Object.keys(value)) {
            if (!isWellFormedString(key)) return ILL_FORMED_KEY;
            const problem = literalProblem((value as { [key: string]: unknown })[key]);
            if (problem !== undefined) return problem;
        }
        return undefined;
    }
    return 'a value of type ' + (typeof value);
}

// Whether a parsed value is a Literal: at any depth, no null, no non-finite
// number, and no string or key with a lone surrogate.
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

export { toStringNormalized, toStringCanonical, isLiteral, literalProblem, isWellFormedString, eq, strongEq };