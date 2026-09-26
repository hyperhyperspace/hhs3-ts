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

// Whether a parsed value is a Literal: no null and no non-finite number at any depth.
function isLiteral(value: unknown): value is Literal {
    if (typeof value === 'string' || typeof value === 'boolean') return true;
    if (typeof value === 'number') return Number.isFinite(value);
    if (Array.isArray(value)) return value.every(isLiteral);
    if (typeof value === 'object' && value !== null) return Object.values(value).every(isLiteral);
    return false;
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

export { toStringNormalized, toStringCanonical, isLiteral, eq, strongEq };