// The same sealed records as the Node keystore, in IndexedDB: scrypt and
// ChaCha20-Poly1305, with the key id as associated data. `resolvePublic`
// needs no passphrase. Loaded once; `list` and `resolveRecord` stay synchronous.

import {
    chacha20Poly1305,
    createIdentity,
    getSigningSuite,
    keyIdFromPublicKey,
    random,
    SIGNING_ED25519,
    type HashSuite,
    type KeyId,
    type OwnIdentity,
    type SigningName,
} from "@hyper-hyper-space/hhs3_crypto";
import { scrypt } from "@noble/hashes/scrypt.js";
import {
    base64ToBytes,
    bytesToBase64,
    decodeIdentitySecret,
    decodePublicKey,
    encodeIdentitySecret,
    encodePublicKey,
    type KeyRecord,
    type KeyVault,
} from "@hyper-hyper-space/hhs3_rdb_runtime";

import { idbRequest, idbTransactionDone, openDatabase } from "./idb.js";

const STORE = 'keys';
const VERSION = 1;
const KEY_PAIR_PROBE = new TextEncoder().encode('hhs3 keystore: key pair check');

type StoredKeyRecord = KeyRecord & {
    kdf: { name: 'scrypt'; salt: string; N: number; r: number; p: number; dkLen: number };
    aead: { name: 'chacha20-poly1305'; nonce: string; ciphertext: string };
};

export class IdbKeyStore implements KeyVault {
    private records: StoredKeyRecord[] = [];
    private db: IDBDatabase | undefined;

    private constructor(
        private readonly name: string,
        private readonly factory: IDBFactory,
        private readonly hashSuite: HashSuite,
    ) {}

    static async open(name: string, factory: IDBFactory, hashSuite: HashSuite): Promise<IdbKeyStore> {
        const store = new IdbKeyStore(name, factory, hashSuite);
        store.db = await openDatabase(factory, name, VERSION, (db) => {
            if (!db.objectStoreNames.contains(STORE)) db.createObjectStore(STORE, { keyPath: 'keyId' });
        });
        const tx = store.db.transaction(STORE, 'readonly');
        store.records = await idbRequest<StoredKeyRecord[]>(tx.objectStore(STORE).getAll());
        await idbTransactionDone(tx);
        return store;
    }

    close(): void {
        this.db?.close();
        this.db = undefined;
    }

    list(): KeyRecord[] {
        return this.records.map(({ label, keyId, publicKey }) => ({ label, keyId, publicKey }));
    }

    async create(label: string, passphrase: string, signingName: SigningName = SIGNING_ED25519): Promise<OwnIdentity> {
        if (this.records.some((key) => key.label === label)) throw new Error(`Key label '${label}' already exists`);
        const identity = await createIdentity(signingName, this.hashSuite);
        const record = this.encryptRecord(label, identity, passphrase);
        this.records.push(record);
        await this.persist(record);
        return identity;
    }

    async unlock(labelOrPrefix: string, passphrase: string): Promise<OwnIdentity> {
        const record = this.resolveStored(labelOrPrefix);
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

    resolvePublic(labelOrPrefix: string): { keyId: KeyId; publicKey: ReturnType<typeof decodePublicKey> } {
        const record = this.resolveStored(labelOrPrefix);
        return { keyId: record.keyId, publicKey: decodePublicKey(record.publicKey) };
    }

    resolveRecord(labelOrPrefix: string): KeyRecord {
        const { label, keyId, publicKey } = this.resolveStored(labelOrPrefix);
        return { label, keyId, publicKey };
    }

    private resolveStored(labelOrPrefix: string): StoredKeyRecord {
        const normalized = labelOrPrefix.startsWith('#') ? labelOrPrefix.slice(1) : labelOrPrefix;
        const labelMatch = this.records.filter((key) => key.label === normalized);
        if (labelMatch.length === 1) return labelMatch[0]!;
        const keyMatches = this.records.filter((key) => key.keyId.startsWith(normalized));
        if (keyMatches.length === 1) return keyMatches[0]!;
        if (keyMatches.length === 0) throw new Error(`Unknown key '${labelOrPrefix}'`);
        throw new Error(`Ambiguous key prefix '${labelOrPrefix}'`);
    }

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
            aead: { name: 'chacha20-poly1305', nonce: bytesToBase64(nonce), ciphertext: bytesToBase64(ciphertext) },
        };
    }

    private async persist(record: StoredKeyRecord): Promise<void> {
        const db = this.db ?? await openDatabase(this.factory, this.name, VERSION, () => {});
        this.db = db;
        const tx = db.transaction(STORE, 'readwrite');
        await idbRequest(tx.objectStore(STORE).put(record));
        await idbTransactionDone(tx);
    }
}

function deriveKey(passphrase: string, params: StoredKeyRecord['kdf']): Uint8Array {
    return scrypt(new TextEncoder().encode(passphrase), base64ToBytes(params.salt), {
        N: params.N, r: params.r, p: params.p, dkLen: params.dkLen,
    });
}
