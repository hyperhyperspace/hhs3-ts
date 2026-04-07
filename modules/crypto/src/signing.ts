// Digital signature suites for identity authentication and message integrity.
// Provides classical (Ed25519), post-quantum (ML-DSA-65 / FIPS 204), and a
// hybrid that requires both to verify, for quantum-resistant transition safety.

import { ed25519 as nobleEd25519 } from '@noble/curves/ed25519.js';
import { ml_dsa65 } from '@noble/post-quantum/ml-dsa.js';

export interface SigningSuite {
    readonly name: string;
    generateKeyPair(): Promise<{ publicKey: Uint8Array; secretKey: Uint8Array }>;
    sign(message: Uint8Array, secretKey: Uint8Array): Promise<Uint8Array>;
    verify(message: Uint8Array, signature: Uint8Array, publicKey: Uint8Array): Promise<boolean>;
    readonly publicKeySize: number;
    readonly secretKeySize: number;
    readonly signatureSize: number;
}

export const ed25519: SigningSuite = {
    name: 'ed25519',
    publicKeySize: 32,
    secretKeySize: 32,
    signatureSize: 64,

    async generateKeyPair() {
        const secretKey = nobleEd25519.utils.randomSecretKey();
        const publicKey = nobleEd25519.getPublicKey(secretKey);
        return { publicKey, secretKey };
    },

    async sign(message: Uint8Array, secretKey: Uint8Array): Promise<Uint8Array> {
        return nobleEd25519.sign(message, secretKey);
    },

    async verify(message: Uint8Array, signature: Uint8Array, publicKey: Uint8Array): Promise<boolean> {
        try {
            return nobleEd25519.verify(signature, message, publicKey);
        } catch {
            return false;
        }
    }
};

export const mlDsa65: SigningSuite = {
    name: 'ml-dsa-65',
    publicKeySize: 1952,
    secretKeySize: 4032,
    signatureSize: 3309,

    async generateKeyPair() {
        return ml_dsa65.keygen();
    },

    async sign(message: Uint8Array, secretKey: Uint8Array): Promise<Uint8Array> {
        return ml_dsa65.sign(message, secretKey);
    },

    async verify(message: Uint8Array, signature: Uint8Array, publicKey: Uint8Array): Promise<boolean> {
        try {
            return ml_dsa65.verify(signature, message, publicKey);
        } catch {
            return false;
        }
    }
};

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

export const ed25519_mlDsa65: SigningSuite = {
    name: 'ed25519+ml-dsa-65',
    publicKeySize: ed25519.publicKeySize + mlDsa65.publicKeySize,
    secretKeySize: ed25519.secretKeySize + mlDsa65.secretKeySize,
    signatureSize: ed25519.signatureSize + mlDsa65.signatureSize,

    async generateKeyPair() {
        const classical = await ed25519.generateKeyPair();
        const pq = await mlDsa65.generateKeyPair();
        return {
            publicKey: concatBytes(classical.publicKey, pq.publicKey),
            secretKey: concatBytes(classical.secretKey, pq.secretKey),
        };
    },

    async sign(message: Uint8Array, secretKey: Uint8Array): Promise<Uint8Array> {
        const classicalSk = secretKey.subarray(0, ed25519.secretKeySize);
        const pqSk = secretKey.subarray(ed25519.secretKeySize);

        const classicalSig = await ed25519.sign(message, classicalSk);
        const pqSig = await mlDsa65.sign(message, pqSk);

        return concatBytes(classicalSig, pqSig);
    },

    async verify(message: Uint8Array, signature: Uint8Array, publicKey: Uint8Array): Promise<boolean> {
        const classicalPk = publicKey.subarray(0, ed25519.publicKeySize);
        const pqPk = publicKey.subarray(ed25519.publicKeySize);

        const classicalSig = signature.subarray(0, ed25519.signatureSize);
        const pqSig = signature.subarray(ed25519.signatureSize);

        const classicalOk = await ed25519.verify(message, classicalSig, classicalPk);
        const pqOk = await mlDsa65.verify(message, pqSig, pqPk);

        return classicalOk && pqOk;
    }
};
