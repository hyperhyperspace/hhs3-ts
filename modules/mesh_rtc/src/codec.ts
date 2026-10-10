// Delegation and possession proofs. The delegation tells a dialer that this
// signaling origin may receive ICE candidates for the listener until an
// expiry. The possession proof tells an honest server that the key holder
// is on the socket, so a copied delegation cannot take the listen slot.

import {
    base64,
    getSigningSuite,
    keyIdFromPublicKey,
    serializePublicKey,
    sha256,
    stringToUint8Array,
    type KeyId,
    type OwnIdentity,
    type PublicKey,
} from '@hyper-hyper-space/hhs3_crypto';
import { deserializePublicKey } from '@hyper-hyper-space/hhs3_crypto';

export const DELEGATE_LABEL = 'hhs3-rtc-delegate-v1';
export const POSSESS_LABEL = 'hhs3-rtc-register-v1';

export interface Delegation {
    publicKey: PublicKey;
    origin: string;
    endpointId: string;
    expiry: number;
    signature: Uint8Array;
}

export interface DelegationWire {
    publicKey: string;
    origin: string;
    endpointId: string;
    expiry: number;
    signature: string;
}

function b64encode(bytes: Uint8Array): string {
    return base64.fromArrayBuffer(bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer);
}

function b64decode(text: string): Uint8Array {
    return new Uint8Array(base64.toArrayBuffer(text));
}

export function encodePublicKey(pk: PublicKey): string {
    return b64encode(serializePublicKey(pk));
}

export function decodePublicKey(text: string): PublicKey {
    return deserializePublicKey(b64decode(text));
}

function delegationBytes(publicKey: string, origin: string, endpointId: string, expiry: number): Uint8Array {
    return stringToUint8Array([
        DELEGATE_LABEL,
        publicKey,
        origin,
        endpointId,
        String(expiry),
    ].join('\n'));
}

function possessionBytes(nonce: string, endpointId: string, origin: string): Uint8Array {
    return stringToUint8Array([
        POSSESS_LABEL,
        nonce,
        endpointId,
        origin,
    ].join('\n'));
}

export async function signDelegation(
    identity: OwnIdentity,
    origin: string,
    endpointId: string,
    expiry: number,
): Promise<Delegation> {
    const suite = getSigningSuite(identity.publicKey.suite);
    if (suite === undefined) throw new Error(`unknown signing suite: ${identity.publicKey.suite}`);
    const publicKey = encodePublicKey(identity.publicKey);
    const signature = await suite.sign(delegationBytes(publicKey, origin, endpointId, expiry), identity.secretKey);
    return { publicKey: identity.publicKey, origin, endpointId, expiry, signature };
}

export function delegationToWire(d: Delegation): DelegationWire {
    return {
        publicKey: encodePublicKey(d.publicKey),
        origin: d.origin,
        endpointId: d.endpointId,
        expiry: d.expiry,
        signature: b64encode(d.signature),
    };
}

export function delegationFromWire(wire: DelegationWire): Delegation {
    return {
        publicKey: decodePublicKey(wire.publicKey),
        origin: wire.origin,
        endpointId: wire.endpointId,
        expiry: wire.expiry,
        signature: b64decode(wire.signature),
    };
}

export interface DelegationCheck {
    origin: string;
    nowMs: number;
    skewMs?: number;
    expectedKeyId?: KeyId;
}

export async function verifyDelegation(wire: DelegationWire, check: DelegationCheck): Promise<KeyId> {
    if (typeof wire.expiry !== 'number' || !Number.isFinite(wire.expiry)) {
        throw new Error('rtc connect rejected: bad delegation');
    }
    if (wire.origin !== check.origin) {
        throw new Error('rtc connect rejected: delegation origin mismatch');
    }
    const skew = check.skewMs ?? 60_000;
    if (wire.expiry * 1000 + skew <= check.nowMs) {
        throw new Error('rtc connect rejected: delegation expired');
    }
    let delegation: Delegation;
    try {
        delegation = delegationFromWire(wire);
    } catch {
        throw new Error('rtc connect rejected: bad delegation');
    }
    const keyId = keyIdFromPublicKey(delegation.publicKey, sha256);
    if (check.expectedKeyId !== undefined && keyId !== check.expectedKeyId) {
        throw new Error('rtc connect rejected: delegation key mismatch');
    }
    const suite = getSigningSuite(delegation.publicKey.suite);
    if (suite === undefined) throw new Error('rtc connect rejected: bad delegation');
    const ok = await suite.verify(
        delegationBytes(wire.publicKey, wire.origin, wire.endpointId, wire.expiry),
        delegation.signature,
        delegation.publicKey.key,
    );
    if (!ok) throw new Error('rtc connect rejected: bad delegation');
    return keyId;
}

export async function signPossession(
    identity: OwnIdentity,
    nonce: string,
    endpointId: string,
    origin: string,
): Promise<string> {
    const suite = getSigningSuite(identity.publicKey.suite);
    if (suite === undefined) throw new Error(`unknown signing suite: ${identity.publicKey.suite}`);
    const sig = await suite.sign(possessionBytes(nonce, endpointId, origin), identity.secretKey);
    return b64encode(sig);
}

export async function verifyPossession(
    publicKey: PublicKey,
    nonce: string,
    endpointId: string,
    origin: string,
    possessionSig: string,
): Promise<boolean> {
    const suite = getSigningSuite(publicKey.suite);
    if (suite === undefined) return false;
    try {
        return await suite.verify(
            possessionBytes(nonce, endpointId, origin),
            b64decode(possessionSig),
            publicKey.key,
        );
    } catch {
        return false;
    }
}
