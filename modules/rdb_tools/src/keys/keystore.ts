import { promises as fs } from "node:fs";

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
    selected?: KeyId;
    keys: StoredKeyRecord[];
};

export type StoredKeyRecord = {
    label: string;
    keyId: KeyId;
    publicKey: StoredPublicKey;
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

export class KeyStore {
    private readonly unlocked = new Map<KeyId, OwnIdentity>();
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

    selected(): OwnIdentity | undefined {
        return this.data.selected === undefined ? undefined : this.unlocked.get(this.data.selected);
    }

    isUnlocked(keyId: KeyId): boolean {
        return this.unlocked.has(keyId);
    }

    // Create and unlock a key, adding it to the active (unlocked) set. It does
    // NOT become the default author: selecting a default is a deliberate act
    // (see select / clearSelection), so authorship is never implicit.
    async create(label: string, passphrase: string, signingName: SigningName = SIGNING_ED25519): Promise<OwnIdentity> {
        if (this.data.keys.some((key) => key.label === label)) throw new Error(`Key label '${label}' already exists`);
        const identity = await createIdentity(signingName, this.hashSuite);
        const record = this.encryptRecord(label, identity, passphrase);
        this.data.keys.push(record);
        this.unlocked.set(identity.keyId, identity);
        await this.save();
        return identity;
    }

    // Unlock a key into the active set. Like create, this does NOT select it as
    // the default author.
    async unlock(labelOrPrefix: string, passphrase: string): Promise<OwnIdentity> {
        const record = this.resolveRecord(labelOrPrefix);
        const key = deriveKey(passphrase, record.kdf);
        const ciphertext = base64ToBytes(record.aead.ciphertext);
        const nonce = base64ToBytes(record.aead.nonce);
        const plaintext = chacha20Poly1305.decrypt(ciphertext, key, nonce, new TextEncoder().encode(record.keyId));
        const secret = JSON.parse(new TextDecoder().decode(plaintext));
        const identity = decodeIdentitySecret(record.keyId, secret);
        this.unlocked.set(identity.keyId, identity);
        await this.save();
        return identity;
    }

    // Set the default author. The key must already be in the active set.
    async select(labelOrPrefix: string): Promise<OwnIdentity> {
        const record = this.resolveRecord(labelOrPrefix);
        const identity = this.unlocked.get(record.keyId);
        if (identity === undefined) throw new Error(`Key '${record.label}' is locked`);
        this.data.selected = identity.keyId;
        await this.save();
        return identity;
    }

    // Clear the default author (write statements become anonymous unless they
    // carry an explicit BY clause).
    async clearSelection(): Promise<void> {
        this.data.selected = undefined;
        await this.save();
    }

    resolvePublic(labelOrPrefix: string): { keyId: KeyId; publicKey: ReturnType<typeof decodePublicKey> } {
        const record = this.resolveRecord(labelOrPrefix);
        return { keyId: record.keyId, publicKey: decodePublicKey(record.publicKey) };
    }

    resolveIdentity(labelOrPrefix: string): OwnIdentity | undefined {
        const record = this.resolveRecord(labelOrPrefix);
        return this.unlocked.get(record.keyId);
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
        await fs.writeFile(this.path, JSON.stringify(this.data, undefined, 2) + '\n');
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
