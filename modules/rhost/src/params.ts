import type { KeyId, PublicKey } from "@hyper-hyper-space/hhs3_crypto";
import type { json } from "@hyper-hyper-space/hhs3_json";
import { serializePublicKeyToBase64 } from "@hyper-hyper-space/hhs3_mvt";
import {
    normalizeBase64, normalizeBigint, paramTypeFits,
    type CatalogParamDecl, type ParamValue,
} from "@hyper-hyper-space/hhs3_rdb";
import type { KeyVault } from "@hyper-hyper-space/hhs3_rdb_runtime";

import type { ParamsConfig } from "./config.js";

export class ParamError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'ParamError';
    }
}

// The canonical form of a value param, as C-SQL's binder encodes a literal.
function encodeValue(name: string, decl: CatalogParamDecl, value: json.Literal): json.Literal {
    switch (decl.type) {
        case 'bigint': {
            const s = typeof value === 'string' || typeof value === 'number' ? normalizeBigint(value) : undefined;
            if (s === undefined) throw new ParamError(`param '${name}' needs a bigint, got ${JSON.stringify(value)}`);
            return s;
        }
        case 'decimal':
            throw new ParamError(`param '${name}' is a decimal, and catalog params declare no scale for one`);
        case 'bytes': {
            const s = typeof value === 'string' ? normalizeBase64(value) : undefined;
            if (s === undefined) throw new ParamError(`param '${name}' needs a base64 string, got ${JSON.stringify(value)}`);
            return s;
        }
        default:
            return value;
    }
}

// The params for a database created from a release that declares `decls`.
// Identity params take '$me' (the host's key) or '$<label>' (a key in the
// keystore); value params take a JSON literal of the declared type.
export function resolveParams(
    params: ParamsConfig,
    decls: Map<string, CatalogParamDecl>,
    keyVault: KeyVault,
    me: { keyId: KeyId; publicKey: PublicKey },
): { [name: string]: ParamValue } {
    const undeclared = Object.keys(params).filter((name) => !decls.has(name)).sort();
    if (undeclared.length > 0) {
        throw new ParamError(`the release declares no param ${undeclared.map((n) => `'${n}'`).join(', ')}`);
    }
    const missing = [...decls.keys()].filter((name) => params[name] === undefined).sort();
    if (missing.length > 0) {
        throw new ParamError(`the release needs ${missing.map((n) => `'${n}'`).join(', ')}; set them in params`);
    }

    const out: { [name: string]: ParamValue } = {};
    for (const [name, decl] of [...decls.entries()].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))) {
        const given = params[name]!;
        if (decl.type === 'identity') {
            if (typeof given !== 'string' || !given.startsWith('$') || given.length < 2) {
                throw new ParamError(`param '${name}' is an identity: use '$me' or '$<key label>', got ${JSON.stringify(given)}`);
            }
            const label = given.slice(1);
            let key: { keyId: KeyId; publicKey: PublicKey };
            try {
                key = label === 'me' ? me : keyVault.resolvePublic(label);
            } catch {
                throw new ParamError(`param '${name}' names the key '${label}', which isn't in the keystore`);
            }
            out[name] = { identity: { keyId: key.keyId, publicKey: serializePublicKeyToBase64(key.publicKey) } };
            continue;
        }
        const value: ParamValue = { value: encodeValue(name, decl, given) };
        if (!paramTypeFits(decl, value)) {
            throw new ParamError(`param '${name}' needs a ${decl.type} value, got ${JSON.stringify(given)}`);
        }
        out[name] = value;
    }
    return out;
}

// A param value typed on a command line or at a prompt: `$me` or `$<key label>`
// for an identity, otherwise JSON, or the text itself when it isn't JSON.
export function parseParamText(name: string, decl: { type: string }, text: string): json.Literal {
    if (decl.type === 'identity') {
        if (!text.startsWith('$') || text.length < 2) {
            throw new ParamError(`param '${name}' is an identity: use $me or $<key label>, got '${text}'`);
        }
        return text;
    }
    let value: unknown;
    try {
        value = JSON.parse(text);
    } catch {
        return text;
    }
    return value === null ? text : value as json.Literal;
}
