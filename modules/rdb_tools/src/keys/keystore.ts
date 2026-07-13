import { promises as fs } from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";

import {
    chacha20Poly1305,
    createIdentity,
    HashSuite,
    KeyId,
    OwnIdentity,
    random,
    SIGNING_ED25519,
    SigningName,
} from "@hyper-hyper-space/hhs3_crypto";
import { scrypt } from "@noble/hashes/scrypt.js";
import type { KeyRecord, KeyVault } from "@hyper-hyper-space/hhs3_rdb_runtime";

import {
    base64ToBytes,
    bytesToBase64,
    decodeIdentitySecret,
    decodePublicKey,
    encodeIdentitySecret,
    encodePublicKey,
    StoredPublicKey,
} from "./identity.js";

type KeystoreFile = {
    version: 1;
    keys: StoredKeyRecord[];
};

// Resolve the global keystore path. The keystore is shared across all
// workspaces and lives in the user's home directory by default. The env
// overrides exist mainly so tests never touch the real file.
export function defaultKeystorePath(): string {
    const explicit = process.env.RDB_KEYSTORE;
    if (explicit !== undefined && explicit !== '') return explicit;
    const home = process.env.RDB_HOME ?? join(homedir(), '.rdb');
    return join(home, 'keys.json');
}

export type StoredKeyRecord = KeyRecord & {
    kdf: {
        name: 'scrypt';
        salt: string;
        N: number;
        r: number;
        p: number;
        dkLen: number;
    };
    aead: {
        name: 'chacha20-poly1305';
        nonce: string;
        ciphertext: string;
    };
};

export class KeyStore implements KeyVault {
    private data: KeystoreFile = { version: 1, keys: [] };

    private constructor(private readonly path: string, private readonly hashSuite: HashSuite) {}

    static async open(path: string, hashSuite: HashSuite): Promise<KeyStore> {
        const store = new KeyStore(path, hashSuite);
        await store.load();
        return store;
    }

    list(): StoredKeyRecord[] {
        return [...this.data.keys];
    }

    // Create a new key and persist it. The returned identity is unlocked, but
    // tracking that (and selecting a default author) is the caller's concern:
    // the keystore is a pure on-disk vault and holds no session state.
    async create(label: string, passphrase: string, signingName: SigningName = SIGNING_ED25519): Promise<OwnIdentity> {
        if (this.data.keys.some((key) => key.label === label)) throw new Error(`Key label '${label}' already exists`);
        const identity = await createIdentity(signingName, this.hashSuite);
        const record = this.encryptRecord(label, identity, passphrase);
        this.data.keys.push(record);
        await this.save();
        return identity;
    }

    // Decrypt a stored key with its passphrase and return the identity. This is
    // a pure read: it does not mutate the vault or any session state.
    async unlock(labelOrPrefix: string, passphrase: string): Promise<OwnIdentity> {
        const record = this.resolveRecord(labelOrPrefix);
        const key = deriveKey(passphrase, record.kdf);
        const ciphertext = base64ToBytes(record.aead.ciphertext);
        const nonce = base64ToBytes(record.aead.nonce);
        const plaintext = chacha20Poly1305.decrypt(ciphertext, key, nonce, new TextEncoder().encode(record.keyId));
        const secret = JSON.parse(new TextDecoder().decode(plaintext));
        return decodeIdentitySecret(record.keyId, secret);
    }

    resolvePublic(labelOrPrefix: string): { keyId: KeyId; publicKey: ReturnType<typeof decodePublicKey> } {
        const record = this.resolveRecord(labelOrPrefix);
        return { keyId: record.keyId, publicKey: decodePublicKey(record.publicKey) };
    }

    resolveRecord(labelOrPrefix: string): StoredKeyRecord {
        const normalized = labelOrPrefix.startsWith('#') ? labelOrPrefix.slice(1) : labelOrPrefix;
        const labelMatch = this.data.keys.filter((key) => key.label === normalized);
        if (labelMatch.length === 1) return labelMatch[0];

        const keyMatches = this.data.keys.filter((key) => key.keyId.startsWith(normalized));
        if (keyMatches.length === 1) return keyMatches[0];
        if (keyMatches.length === 0) throw new Error(`Unknown key '${labelOrPrefix}'`);
        throw new Error(`Ambiguous key prefix '${labelOrPrefix}'`);
    }

    private async load(): Promise<void> {
        let raw: string;
        try {
            raw = await fs.readFile(this.path, 'utf8');
        } catch (e) {
            if ((e as NodeJS.ErrnoException).code === 'ENOENT') return;
            throw e;
        }
        this.data = JSON.parse(raw) as KeystoreFile;
    }

    private async save(): Promise<void> {
        // The file holds encrypted signing secrets, so keep the directory and
        // file private to the user.
        await fs.mkdir(dirname(this.path), { recursive: true, mode: 0o700 });
        await fs.writeFile(this.path, JSON.stringify(this.data, undefined, 2) + '\n', { mode: 0o600 });
    }

    private encryptRecord(label: string, identity: OwnIdentity, passphrase: string): StoredKeyRecord {
        const kdf = {
            name: 'scrypt' as const,
            salt: bytesToBase64(random.getBytes(16)),
            N: 2 ** 15,
            r: 8,
            p: 1,
            dkLen: chacha20Poly1305.keySize,
        };
        const key = deriveKey(passphrase, kdf);
        const nonce = random.getBytes(chacha20Poly1305.nonceSize);
        const plaintext = new TextEncoder().encode(JSON.stringify(encodeIdentitySecret(identity)));
        const ciphertext = chacha20Poly1305.encrypt(plaintext, key, nonce, new TextEncoder().encode(identity.keyId));
        return {
            label,
            keyId: identity.keyId,
            publicKey: encodePublicKey(identity.publicKey),
            kdf,
            aead: {
                name: 'chacha20-poly1305',
                nonce: bytesToBase64(nonce),
                ciphertext: bytesToBase64(ciphertext),
            },
        };
    }
}

function deriveKey(passphrase: string, params: StoredKeyRecord['kdf']): Uint8Array {
    return scrypt(new TextEncoder().encode(passphrase), base64ToBytes(params.salt), {
        N: params.N,
        r: params.r,
        p: params.p,
        dkLen: params.dkLen,
    });
}
