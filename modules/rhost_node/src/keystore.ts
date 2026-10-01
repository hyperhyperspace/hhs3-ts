import { promises as fs } from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";

import {
    chacha20Poly1305,
    createIdentity,
    getSigningSuite,
    HashSuite,
    KeyId,
    keyIdFromPublicKey,
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

const KEY_PAIR_PROBE = new TextEncoder().encode('hhs3 keystore: key pair check');

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

    // Add a record exported from another keystore, still encrypted with its own
    // passphrase. Importing the same key again is a no-op.
    async importRecord(record: StoredKeyRecord): Promise<void> {
        const existing = this.data.keys.find((key) => key.label === record.label);
        if (existing !== undefined) {
            if (existing.keyId === record.keyId) return;
            throw new Error(`Key label '${record.label}' already exists`);
        }
        this.data.keys.push(JSON.parse(JSON.stringify(record)) as StoredKeyRecord);
        await this.save();
    }

    // Decrypt a stored key with its passphrase and return the identity. This is
    // a pure read: it does not mutate the vault or any session state.
    async unlock(labelOrPrefix: string, passphrase: string): Promise<OwnIdentity> {
        const record = this.resolveRecord(labelOrPrefix);
        const key = deriveKey(passphrase, record.kdf);
        const ciphertext = base64ToBytes(record.aead.ciphertext);
        const nonce = base64ToBytes(record.aead.nonce);
        let plaintext: Uint8Array;
        try {
            plaintext = chacha20Poly1305.decrypt(ciphertext, key, nonce, new TextEncoder().encode(record.keyId));
        } catch {
            throw new Error(`Wrong passphrase for key '${record.label}'`);
        }
        const secret = JSON.parse(new TextDecoder().decode(plaintext));
        const identity = decodeIdentitySecret(record.keyId, secret);
        await this.checkKeyPair(record.label, identity);
        return identity;
    }

    // The keyId is the record's plaintext; the key pair is what was sealed.
    // Whoever can write the file can seal any key pair under a real keyId
    // (it is only the AEAD's associated data), so check that they agree.
    private async checkKeyPair(label: string, identity: OwnIdentity): Promise<void> {
        if (keyIdFromPublicKey(identity.publicKey, this.hashSuite) !== identity.keyId) {
            throw new Error(`Key '${label}' does not match its key id`);
        }
        const suite = getSigningSuite(identity.publicKey.suite);
        if (suite === undefined) throw new Error(`Key '${label}' uses an unknown signing suite '${identity.publicKey.suite}'`);
        let verified: boolean;
        try {
            verified = await suite.verify(KEY_PAIR_PROBE, await suite.sign(KEY_PAIR_PROBE, identity.secretKey), identity.publicKey.key);
        } catch {
            verified = false;
        }
        if (!verified) throw new Error(`Key '${label}' holds a secret key that does not match its public key`);
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
