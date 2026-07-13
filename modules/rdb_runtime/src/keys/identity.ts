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
    let binary = '';
    for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]!);
    return btoa(binary);
}

export function base64ToBytes(value: string): Uint8Array {
    const binary = atob(value);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
    return bytes;
}
