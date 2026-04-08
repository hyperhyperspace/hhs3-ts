// Cryptographic hash functions. Used for DAG entry hashing, element identity
// in replicated data types, and KeyId derivation from public keys. The primary
// hash() method returns raw bytes; hashToB64() returns a base64-encoded string
// (the B64Hash type used as a stable identifier throughout the project).

import { sha256 as nobleSha256 } from '@noble/hashes/sha2.js';
import { blake3 as nobleBlake3 } from '@noble/hashes/blake3.js';

export type B64Hash = string;

export function stringToUint8Array(str: string): Uint8Array {
    return new TextEncoder().encode(str);
}

export function uint8ArrayToString(bytes: Uint8Array): string {
    return new TextDecoder().decode(bytes);
}

function bytesToBase64(bytes: Uint8Array): string {
    let binary = '';
    for (let i = 0; i < bytes.length; i++) {
        binary += String.fromCharCode(bytes[i]);
    }
    return btoa(binary);
}

export interface HashSuite {
    readonly name: string;
    readonly digestSize: number;
    hash(input: Uint8Array): Uint8Array;
    hashToB64(input: Uint8Array): B64Hash;
}

export const sha256: HashSuite = {
    name: 'sha-256',
    digestSize: 32,

    hash(input: Uint8Array): Uint8Array {
        return nobleSha256(input);
    },

    hashToB64(input: Uint8Array): B64Hash {
        return bytesToBase64(nobleSha256(input));
    }
};

export const blake3: HashSuite = {
    name: 'blake3',
    digestSize: 32,

    hash(input: Uint8Array): Uint8Array {
        return nobleBlake3(input);
    },

    hashToB64(input: Uint8Array): B64Hash {
        return bytesToBase64(nobleBlake3(input));
    }
};
