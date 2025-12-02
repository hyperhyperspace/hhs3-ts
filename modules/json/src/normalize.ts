import { Literal } from "./literal";

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
    } else if (typeof literal === 'boolean' || typeof literal === 'number') {
        plain = literal.toString();
        // important notice: because of how the javascript number type works, we are sure that
        //                   integer numbers always get serialized without a fractional part
        //                   (e.g. '1.0' cannot happen)
    } else {
        throw new Error('Cannot serialize ' + literal + ', its type ' + (typeof literal) + ' is illegal for a literal.');
    }

    return plain;
}

function escapeString(text: string) {
    return '"' + text.replaceAll("\\", "\\\\").replaceAll('"', '\\"') + '"';
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

export { toStringNormalized, eq, strongEq };