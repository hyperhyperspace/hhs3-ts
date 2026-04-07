// Cryptographic identity types. A PublicKey is self-describing key material
// (suite name + raw bytes). A KeyId is a compact, suite-agnostic hash of a
// serialized PublicKey, used as a stable peer identifier across the system.

import { Hash, HashSuite, stringToUint8Array } from './hashing.js';

export type PublicKey = {
    suite: string;
    key: Uint8Array;
};

export type KeyId = Hash;

export function serializePublicKey(pk: PublicKey): Uint8Array {
    const suiteBytes = stringToUint8Array(pk.suite);
    const lenBuf = new Uint8Array(4);
    new DataView(lenBuf.buffer).setUint32(0, suiteBytes.length, false);

    const result = new Uint8Array(4 + suiteBytes.length + pk.key.length);
    result.set(lenBuf, 0);
    result.set(suiteBytes, 4);
    result.set(pk.key, 4 + suiteBytes.length);
    return result;
}

export function deserializePublicKey(bytes: Uint8Array): PublicKey {
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const suiteLen = view.getUint32(0, false);
    const suiteBytes = bytes.subarray(4, 4 + suiteLen);
    const suite = new TextDecoder().decode(suiteBytes);
    const key = bytes.subarray(4 + suiteLen);
    return { suite, key: new Uint8Array(key) };
}

export function keyIdFromPublicKey(pk: PublicKey, hash: HashSuite): KeyId {
    return hash.hash(serializePublicKey(pk));
}
