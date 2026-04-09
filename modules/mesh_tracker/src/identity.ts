// Tracker identity management. Handles generation, serialization, and
// persistence of signing keypairs. Follows an sshd-like model: generate once,
// persist to disk, reload on subsequent starts.

import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import type { PublicKey, SigningName } from '@hyper-hyper-space/hhs3_crypto';
import { getSigningSuite, keyIdFromPublicKey, sha256 } from '@hyper-hyper-space/hhs3_crypto';

export interface TrackerIdentity {
    publicKey: PublicKey;
    secretKey: Uint8Array;
}

interface IdentityFile {
    suite: string;
    publicKey: string;
    secretKey: string;
}

function toBase64(bytes: Uint8Array): string {
    return Buffer.from(bytes).toString('base64');
}

function fromBase64(s: string): Uint8Array {
    return new Uint8Array(Buffer.from(s, 'base64'));
}

export async function generateIdentity(signingName: SigningName): Promise<TrackerIdentity> {
    const suite = getSigningSuite(signingName);
    if (!suite) throw new Error(`signing suite not found: ${signingName}`);
    const kp = await suite.generateKeyPair();
    return {
        publicKey: { suite: signingName, key: kp.publicKey },
        secretKey: kp.secretKey,
    };
}

export async function saveIdentity(filePath: string, identity: TrackerIdentity): Promise<void> {
    const dir = path.dirname(filePath);
    await fs.mkdir(dir, { recursive: true });
    const data: IdentityFile = {
        suite: identity.publicKey.suite,
        publicKey: toBase64(identity.publicKey.key),
        secretKey: toBase64(identity.secretKey),
    };
    await fs.writeFile(filePath, JSON.stringify(data, null, 2) + '\n', 'utf-8');
}

export async function loadIdentity(filePath: string): Promise<TrackerIdentity> {
    const raw = await fs.readFile(filePath, 'utf-8');
    const data: IdentityFile = JSON.parse(raw);
    return {
        publicKey: { suite: data.suite, key: fromBase64(data.publicKey) },
        secretKey: fromBase64(data.secretKey),
    };
}

export async function loadOrCreateIdentity(
    filePath: string,
    signingName: SigningName,
): Promise<TrackerIdentity> {
    try {
        return await loadIdentity(filePath);
    } catch {
        const identity = await generateIdentity(signingName);
        await saveIdentity(filePath, identity);
        return identity;
    }
}

export function identityKeyId(identity: TrackerIdentity): string {
    return keyIdFromPublicKey(identity.publicKey, sha256);
}
