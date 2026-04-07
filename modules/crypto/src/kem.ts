import { x25519 } from '@noble/curves/ed25519.js';
import { ml_kem768 } from '@noble/post-quantum/ml-kem.js';
import { hkdfSha256 } from './hkdf.js';

export interface KemSuite {
    readonly name: string;
    generateKeyPair(): Promise<{ publicKey: Uint8Array; secretKey: Uint8Array }>;
    encapsulate(publicKey: Uint8Array): Promise<{ ciphertext: Uint8Array; sharedSecret: Uint8Array }>;
    decapsulate(ciphertext: Uint8Array, secretKey: Uint8Array): Promise<Uint8Array>;
    readonly publicKeySize: number;
    readonly secretKeySize: number;
    readonly ciphertextSize: number;
    readonly sharedSecretSize: number;
}

const DHKEM_LABEL = new TextEncoder().encode('hhs3-dhkem-x25519');
const HYBRID_LABEL = new TextEncoder().encode('hhs3-hybrid-kem-x25519-mlkem768');

function concatBytes(...arrays: Uint8Array[]): Uint8Array {
    const totalLen = arrays.reduce((sum, a) => sum + a.length, 0);
    const result = new Uint8Array(totalLen);
    let offset = 0;
    for (const a of arrays) {
        result.set(a, offset);
        offset += a.length;
    }
    return result;
}

/**
 * DHKEM construction with X25519 + HKDF-SHA256 (RFC 9180 style).
 * Encapsulate generates an ephemeral X25519 keypair, computes a DH shared
 * secret with the recipient's public key, and derives the output via HKDF.
 * The ciphertext is the ephemeral public key.
 */
export const x25519Hkdf: KemSuite = {
    name: 'x25519-hkdf',
    publicKeySize: 32,
    secretKeySize: 32,
    ciphertextSize: 32,
    sharedSecretSize: 32,

    async generateKeyPair() {
        const secretKey = x25519.utils.randomSecretKey();
        const publicKey = x25519.getPublicKey(secretKey);
        return { publicKey, secretKey };
    },

    async encapsulate(publicKey: Uint8Array) {
        const ephemeral = x25519.utils.randomSecretKey();
        const ephemeralPk = x25519.getPublicKey(ephemeral);
        const dh = x25519.getSharedSecret(ephemeral, publicKey);
        const kemContext = concatBytes(ephemeralPk, publicKey);
        const sharedSecret = hkdfSha256.deriveKey(dh, kemContext, DHKEM_LABEL, 32);
        return { ciphertext: ephemeralPk, sharedSecret };
    },

    async decapsulate(ciphertext: Uint8Array, secretKey: Uint8Array) {
        const publicKey = x25519.getPublicKey(secretKey);
        const dh = x25519.getSharedSecret(secretKey, ciphertext);
        const kemContext = concatBytes(ciphertext, publicKey);
        return hkdfSha256.deriveKey(dh, kemContext, DHKEM_LABEL, 32);
    }
};

export const mlKem768: KemSuite = {
    name: 'ml-kem-768',
    publicKeySize: 1184,
    secretKeySize: 2400,
    ciphertextSize: 1088,
    sharedSecretSize: 32,

    async generateKeyPair() {
        return ml_kem768.keygen();
    },

    async encapsulate(publicKey: Uint8Array) {
        const { cipherText, sharedSecret } = ml_kem768.encapsulate(publicKey);
        return { ciphertext: cipherText, sharedSecret };
    },

    async decapsulate(ciphertext: Uint8Array, secretKey: Uint8Array) {
        return ml_kem768.decapsulate(ciphertext, secretKey);
    }
};

/**
 * Hybrid KEM: X25519-HKDF + ML-KEM-768.
 * Both KEMs are run independently. Ciphertexts are concatenated.
 * Shared secrets are combined via HKDF with a domain separation label.
 */
export const x25519Hkdf_mlKem768: KemSuite = {
    name: 'x25519-hkdf+ml-kem-768',
    publicKeySize: x25519Hkdf.publicKeySize + mlKem768.publicKeySize,
    secretKeySize: x25519Hkdf.secretKeySize + mlKem768.secretKeySize,
    ciphertextSize: x25519Hkdf.ciphertextSize + mlKem768.ciphertextSize,
    sharedSecretSize: 32,

    async generateKeyPair() {
        const classical = await x25519Hkdf.generateKeyPair();
        const pq = await mlKem768.generateKeyPair();
        return {
            publicKey: concatBytes(classical.publicKey, pq.publicKey),
            secretKey: concatBytes(classical.secretKey, pq.secretKey),
        };
    },

    async encapsulate(publicKey: Uint8Array) {
        const classicalPk = publicKey.subarray(0, x25519Hkdf.publicKeySize);
        const pqPk = publicKey.subarray(x25519Hkdf.publicKeySize);

        const classical = await x25519Hkdf.encapsulate(classicalPk);
        const pq = await mlKem768.encapsulate(pqPk);

        const combinedSs = concatBytes(classical.sharedSecret, pq.sharedSecret);
        const sharedSecret = hkdfSha256.deriveKey(combinedSs, new Uint8Array(0), HYBRID_LABEL, 32);

        return {
            ciphertext: concatBytes(classical.ciphertext, pq.ciphertext),
            sharedSecret
        };
    },

    async decapsulate(ciphertext: Uint8Array, secretKey: Uint8Array) {
        const classicalCt = ciphertext.subarray(0, x25519Hkdf.ciphertextSize);
        const pqCt = ciphertext.subarray(x25519Hkdf.ciphertextSize);

        const classicalSk = secretKey.subarray(0, x25519Hkdf.secretKeySize);
        const pqSk = secretKey.subarray(x25519Hkdf.secretKeySize);

        const classicalSs = await x25519Hkdf.decapsulate(classicalCt, classicalSk);
        const pqSs = await mlKem768.decapsulate(pqCt, pqSk);

        const combinedSs = concatBytes(classicalSs, pqSs);
        return hkdfSha256.deriveKey(combinedSs, new Uint8Array(0), HYBRID_LABEL, 32);
    }
};
