// Building-block helpers for types that need payload authorship (signed
// operations). Analogous to how refs.ts in MVT provides helpers for
// inter-object references.
//
// Public keys are NOT embedded in every payload. They are stored once by
// the type (e.g. in a creation payload or identity registry) and looked
// up by KeyId during verification. This keeps authored payloads small,
// which matters especially for post-quantum signing suites.

import type { KeyId, PublicKey, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { base64, getSigningSuite, serializePublicKey, deserializePublicKey, keyIdFromPublicKey, stringToUint8Array } from "@hyper-hyper-space/hhs3_crypto";
import type { HashSuite } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";

import type { Version } from "./mvt.js";

export const MAX_KEY_ID_LENGTH = 256;
export const MAX_SIGNATURE_LENGTH = 8192;

export type AuthoredFields = {
    author: string;
    signature: string;
};

export const authoredFormat: json.Format = {
    author:    [json.Type.BoundedString, MAX_KEY_ID_LENGTH],
    signature: [json.Type.BoundedString, MAX_SIGNATURE_LENGTH],
};

export type KeyLookup = (keyId: KeyId) => Promise<PublicKey | undefined>;

export function isAuthoredPayload(payload: json.Literal): payload is json.LiteralMap & AuthoredFields {
    return typeof payload === 'object'
        && !Array.isArray(payload)
        && typeof payload['author'] === 'string'
        && typeof payload['signature'] === 'string';
}

export function extractAuthor(payload: json.Literal): KeyId | undefined {
    if (typeof payload === 'object' && !Array.isArray(payload) && typeof payload['author'] === 'string') {
        return payload['author'] as KeyId;
    }
    return undefined;
}

const OP_SIGNATURE_DOMAIN = 'hhs3/op-sig/v1';

// The chain of wrapping contexts from the root object down to the object that
// owns the payload (see ScopedDag.signingScope). Empty for root objects.
export type SigningScope = json.Literal[];

// The signed message binds the payload to its insertion point: `at` must be
// exactly the prevEntryHashes of the entry that will carry the payload, so a
// signed op cannot be replayed at another position or into another object.
// `scope` binds it to its place inside that object's DAG; it is omitted when
// empty, so root-level signatures are identical to unscoped ones.
function signedMessage(payload: json.LiteralMap, at: Version, scope: SigningScope): Uint8Array {
    const stripped: json.LiteralMap = {};
    for (const key of Object.keys(payload)) {
        if (key !== 'signature') {
            stripped[key] = payload[key];
        }
    }
    const message: json.LiteralMap = {
        domain: OP_SIGNATURE_DOMAIN,
        at: json.toSet(at),
        payload: stripped,
    };
    if (scope.length > 0) message['scope'] = scope;
    return stringToUint8Array(json.toStringNormalized(message));
}

export function serializePublicKeyToBase64(pk: PublicKey): string {
    const bytes = serializePublicKey(pk);
    return base64.fromArrayBuffer(bytes.slice().buffer);
}

export function deserializePublicKeyFromBase64(b64: string): PublicKey {
    const buf = base64.toArrayBuffer(b64);
    return deserializePublicKey(new Uint8Array(buf));
}

export function computeKeyId(pk: PublicKey, hashSuite: HashSuite): KeyId {
    return keyIdFromPublicKey(pk, hashSuite);
}

export async function signPayload<T extends json.LiteralMap>(
    payload: T,
    author: OwnIdentity,
    at: Version,
    scope: SigningScope = [],
): Promise<T & AuthoredFields> {
    if (at.size === 0) {
        throw new Error('Signed payloads require a non-empty insertion point');
    }

    const suite = getSigningSuite(author.publicKey.suite);
    if (suite === undefined) {
        throw new Error(`Signing suite '${author.publicKey.suite}' not registered`);
    }

    const withAuthor = { ...payload, author: author.keyId, signature: '' };
    const message = signedMessage(withAuthor, at, scope);
    const sigBytes = await suite.sign(message, author.secretKey);
    const signature = base64.fromArrayBuffer(sigBytes.slice().buffer);

    return { ...withAuthor, signature };
}

export async function verifyPayloadSignature(
    payload: json.LiteralMap,
    at: Version,
    keyLookup: KeyLookup,
    scope: SigningScope = [],
): Promise<boolean> {
    if (!isAuthoredPayload(payload) || at.size === 0) {
        return false;
    }

    const pk = await keyLookup(payload.author as KeyId);
    if (pk === undefined) {
        return false;
    }

    const suite = getSigningSuite(pk.suite);
    if (suite === undefined) {
        return false;
    }

    const message = signedMessage(payload, at, scope);
    const sigBuf = base64.toArrayBuffer(payload.signature);
    const sigBytes = new Uint8Array(sigBuf);

    return suite.verify(message, sigBytes, pk.key);
}
