import type { KeyId, OwnIdentity, PublicKey } from "@hyper-hyper-space/hhs3_crypto";
import type { json } from "@hyper-hyper-space/hhs3_json";
import { serializePublicKeyToBase64 } from "@hyper-hyper-space/hhs3_mvt";

import type { ValueExpr } from "../syntax/ast.js";
import type { LangBindContext, LangValue } from "./context.js";

export async function resolveValue(expr: ValueExpr, context: LangBindContext): Promise<LangValue> {
    if (expr.kind === 'literal') return expr.value;
    if (expr.kind === 'variable') {
        if (expr.field !== undefined) throw new Error('$row.<column> is only supported in allow rule predicates');
        return context.resolveVariable(expr.name);
    }
    if (expr.name === 'publicKey') {
        if (expr.args.length !== 1) throw new Error('publicKey() expects exactly one argument');
        return publicKeyString(await resolveValue(expr.args[0], context));
    }
    throw new Error(`Unknown value function '${expr.name}'`);
}

export function asJsonLiteral(value: LangValue): json.Literal {
    if (value === null) throw new Error('NULL is not a JSON literal in RDb payloads');
    if (isIdentity(value)) return value.keyId;
    if (isKeyRecord(value)) return value.keyId;
    if (isKeyIdObject(value)) return value.keyId;
    return value;
}

export function asKeyId(value: LangValue | undefined): KeyId | undefined {
    if (value === undefined || value === null) return undefined;
    if (typeof value === 'string') return value;
    if (isIdentity(value)) return value.keyId;
    if (isKeyRecord(value)) return value.keyId;
    if (isKeyIdObject(value)) return value.keyId;
    throw new Error('Expected key id or identity value');
}

export function asIdentity(value: LangValue | undefined): OwnIdentity | undefined {
    return value !== undefined && isIdentity(value) ? value : undefined;
}

export function asCreator(value: LangValue): { keyId: KeyId; publicKey: PublicKey } {
    if (isIdentity(value)) return { keyId: value.keyId, publicKey: value.publicKey };
    if (isKeyRecord(value) && value.publicKey !== undefined) {
        return { keyId: value.keyId, publicKey: value.publicKey };
    }
    throw new Error('Expected identity or creator value');
}

function publicKeyString(value: LangValue): string {
    if (isIdentity(value) || isKeyRecord(value)) {
        if (value.publicKey === undefined) throw new Error('publicKey() requires an identity or public key record');
        return serializePublicKeyToBase64(value.publicKey);
    }
    throw new Error('publicKey() requires an identity or public key record');
}

function isIdentity(value: LangValue): value is OwnIdentity {
    return typeof value === 'object' && value !== null && 'keyId' in value && 'publicKey' in value && 'secretKey' in value;
}

function isKeyRecord(value: LangValue): value is { keyId: KeyId; publicKey?: PublicKey } {
    return typeof value === 'object' && value !== null && 'keyId' in value && !('kind' in value);
}

function isKeyIdObject(value: LangValue): value is { kind: 'key-id'; keyId: KeyId } {
    return typeof value === 'object' && value !== null && 'kind' in value && value.kind === 'key-id';
}
