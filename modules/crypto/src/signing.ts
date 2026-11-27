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
    // What is wrong with `key` as a raw public key of this suite, or undefined
    // when it is one.
    publicKeyProblem(key: Uint8Array): string | undefined;
    readonly publicKeySize: number;
    readonly secretKeySize: number;
    readonly signatureSize: number;
}

function lengthProblem(suite: string, key: Uint8Array, size: number): string | undefined {
    return key.length === size ? undefined : `the key is ${key.length} bytes, where an ${suite} public key is ${size}`;
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

    // The bytes must decode, by the strict rules, to a point of the curve
    // that isn't of small order. A key pair makes only points of prime order.
    // A point with a small-order part beside its prime-order one passes: the
    // cofactored check in verify ignores that part, so it verifies only what
    // its prime-order part does.
    publicKeyProblem(key: Uint8Array): string | undefined {
        const length = lengthProblem('ed25519', key, 32);
        if (length !== undefined) return length;
        let point;
        try {
            point = nobleEd25519.Point.fromBytes(key, false);
        } catch {
            return 'the key is not a point of the ed25519 curve';
        }
        if (point.isSmallOrder()) return "the key is one of the ed25519 curve's small-order points, which no key pair makes";
        return undefined;
    },

    // Strict RFC 8032 verification. noble's default for Ed25519 is ZIP-215,
    // which also accepts non-canonical encodings and small-order public keys:
    // against a small-order key, a signature whose R is the neutral point and
    // whose S is 0 verifies for every message. Strict mode refuses both, and
    // every key pair and signature this suite makes passes it.
    async verify(message: Uint8Array, signature: Uint8Array, publicKey: Uint8Array): Promise<boolean> {
        try {
            return nobleEd25519.verify(signature, message, publicKey, { zip215: false });
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

    // FIPS 204 decodes any string of the right length as a public key, so
    // the length is the whole check.
    publicKeyProblem(key: Uint8Array): string | undefined {
        return lengthProblem('ml-dsa-65', key, 1952);
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

    publicKeyProblem(key: Uint8Array): string | undefined {
        const length = lengthProblem('ed25519+ml-dsa-65', key, ed25519.publicKeySize + mlDsa65.publicKeySize);
        if (length !== undefined) return length;
        const classical = ed25519.publicKeyProblem(key.subarray(0, ed25519.publicKeySize));
        if (classical !== undefined) return `its ed25519 half: ${classical}`;
        const pq = mlDsa65.publicKeyProblem(key.subarray(ed25519.publicKeySize));
        return pq === undefined ? undefined : `its ml-dsa-65 half: ${pq}`;
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
