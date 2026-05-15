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

function canonicalBytesWithoutSignature(payload: json.LiteralMap): Uint8Array {
    const stripped: json.LiteralMap = {};
    for (const key of Object.keys(payload)) {
        if (key !== 'signature') {
            stripped[key] = payload[key];
        }
    }
    return stringToUint8Array(json.toStringNormalized(stripped));
}

export function serializePublicKeyToBase64(pk: PublicKey): string {
    const bytes = serializePublicKey(pk);
    return base64.fromArrayBuffer(bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength));
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
): Promise<T & AuthoredFields> {
    const suite = getSigningSuite(author.publicKey.suite);
    if (suite === undefined) {
        throw new Error(`Signing suite '${author.publicKey.suite}' not registered`);
    }

    const withAuthor = { ...payload, author: author.keyId, signature: '' };
    const message = canonicalBytesWithoutSignature(withAuthor);
    const sigBytes = await suite.sign(message, author.secretKey);
    const signature = base64.fromArrayBuffer(sigBytes.buffer.slice(sigBytes.byteOffset, sigBytes.byteOffset + sigBytes.byteLength));

    return { ...withAuthor, signature };
}

export async function verifyPayloadSignature(
    payload: json.LiteralMap,
    keyLookup: KeyLookup,
): Promise<boolean> {
    if (!isAuthoredPayload(payload)) {
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

    const message = canonicalBytesWithoutSignature(payload);
    const sigBuf = base64.toArrayBuffer(payload.signature);
    const sigBytes = new Uint8Array(sigBuf);

    return suite.verify(message, sigBytes, pk.key);
}
