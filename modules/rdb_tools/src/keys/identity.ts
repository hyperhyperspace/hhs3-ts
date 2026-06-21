import type { KeyId, OwnIdentity, PublicKey } from "@hyper-hyper-space/hhs3_crypto";

export type StoredPublicKey = {
    suite: string;
    key: string;
};

export type StoredIdentitySecret = {
    publicKey: StoredPublicKey;
    secretKey: string;
};

export function encodePublicKey(publicKey: PublicKey): StoredPublicKey {
    return {
        suite: publicKey.suite,
        key: bytesToBase64(publicKey.key),
    };
}

export function decodePublicKey(publicKey: StoredPublicKey): PublicKey {
    return {
        suite: publicKey.suite,
        key: base64ToBytes(publicKey.key),
    };
}

export function encodeIdentitySecret(identity: OwnIdentity): StoredIdentitySecret {
    return {
        publicKey: encodePublicKey(identity.publicKey),
        secretKey: bytesToBase64(identity.secretKey),
    };
}

export function decodeIdentitySecret(keyId: KeyId, secret: StoredIdentitySecret): OwnIdentity {
    return {
        keyId,
        publicKey: decodePublicKey(secret.publicKey),
        secretKey: base64ToBytes(secret.secretKey),
    };
}

export function bytesToBase64(bytes: Uint8Array): string {
    return Buffer.from(bytes).toString('base64');
}

export function base64ToBytes(value: string): Uint8Array {
    return new Uint8Array(Buffer.from(value, 'base64'));
}
