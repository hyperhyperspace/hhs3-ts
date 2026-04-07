// Authenticated Encryption with Associated Data. Used for encrypting session
// traffic after key agreement. The AEAD construction binds ciphertext integrity
// to optional associated data (e.g. message headers), preventing tampering.

import { chacha20poly1305 as nobleChacha } from '@noble/ciphers/chacha.js';

export interface AeadSuite {
    readonly name: string;
    encrypt(plaintext: Uint8Array, key: Uint8Array, nonce: Uint8Array, aad?: Uint8Array): Uint8Array;
    decrypt(ciphertext: Uint8Array, key: Uint8Array, nonce: Uint8Array, aad?: Uint8Array): Uint8Array;
    readonly keySize: number;
    readonly nonceSize: number;
    readonly tagSize: number;
}

export const chacha20Poly1305: AeadSuite = {
    name: 'chacha20-poly1305',
    keySize: 32,
    nonceSize: 12,
    tagSize: 16,

    encrypt(plaintext: Uint8Array, key: Uint8Array, nonce: Uint8Array, aad?: Uint8Array): Uint8Array {
        const cipher = nobleChacha(key, nonce, aad);
        return cipher.encrypt(plaintext);
    },

    decrypt(ciphertext: Uint8Array, key: Uint8Array, nonce: Uint8Array, aad?: Uint8Array): Uint8Array {
        const cipher = nobleChacha(key, nonce, aad);
        return cipher.decrypt(ciphertext);
    }
};
