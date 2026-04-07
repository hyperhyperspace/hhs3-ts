import { sha256 as nobleSha256 } from '@noble/hashes/sha2.js';
import { blake3 as nobleBlake3 } from '@noble/hashes/blake3.js';

export type Hash = string;

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
    hash(input: Uint8Array): Hash;
}

export const sha256: HashSuite = {
    name: 'sha-256',
    digestSize: 32,

    hash(input: Uint8Array): Hash {
        return bytesToBase64(nobleSha256(input));
    }
};

export const blake3: HashSuite = {
    name: 'blake3',
    digestSize: 32,

    hash(input: Uint8Array): Hash {
        return bytesToBase64(nobleBlake3(input));
    }
};
