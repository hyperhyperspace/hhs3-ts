// HMAC-based Key Derivation Function (RFC 5869). Used internally by the KEM
// suites to derive shared secrets from raw DH output and to combine component
// secrets in hybrid constructions via domain-separated extract-then-expand.

import { hkdf as nobleHkdf, extract as nobleExtract, expand as nobleExpand } from '@noble/hashes/hkdf.js';
import { sha256 } from '@noble/hashes/sha2.js';

export interface KdfSuite {
    readonly name: string;
    extract(ikm: Uint8Array, salt?: Uint8Array): Uint8Array;
    expand(prk: Uint8Array, info: Uint8Array, length: number): Uint8Array;
    deriveKey(ikm: Uint8Array, salt: Uint8Array, info: Uint8Array, length: number): Uint8Array;
}

export const hkdfSha256: KdfSuite = {
    name: 'hkdf-sha256',

    extract(ikm: Uint8Array, salt?: Uint8Array): Uint8Array {
        return nobleExtract(sha256, ikm, salt);
    },

    expand(prk: Uint8Array, info: Uint8Array, length: number): Uint8Array {
        return nobleExpand(sha256, prk, info, length);
    },

    deriveKey(ikm: Uint8Array, salt: Uint8Array, info: Uint8Array, length: number): Uint8Array {
        return nobleHkdf(sha256, ikm, salt, info, length);
    }
};
