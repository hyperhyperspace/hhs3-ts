import {
    createBasicCrypto,
    createIdentity,
    HASH_SHA256,
    SIGNING_ED25519,
    type HashSuite,
    type KeyId,
    type OwnIdentity,
    type SigningName,
} from "@hyper-hyper-space/hhs3_crypto";

import { decodePublicKey, encodePublicKey } from "./identity.js";
import type { KeyRecord, KeyVault } from "./key_vault.js";

type MemoryKey = {
    record: KeyRecord;
    identity: OwnIdentity;
    passphrase: string;
};

/**
 * An ephemeral key vault suitable for browser and other in-memory runtimes.
 *
 * Identities and passphrases are retained only by this instance and are lost
 * when it is discarded.
 */
export class MemoryKeyVault implements KeyVault {
    private readonly keys: MemoryKey[] = [];
    private readonly pendingLabels = new Set<string>();

    constructor(
        private readonly hashSuite: HashSuite = createBasicCrypto().hash(HASH_SHA256),
    ) {}

    list(): KeyRecord[] {
        return this.keys.map(({ record }) => copyRecord(record));
    }

    async create(
        label: string,
        passphrase: string,
        signingName: SigningName = SIGNING_ED25519,
    ): Promise<OwnIdentity> {
        if (this.hasLabel(label) || this.pendingLabels.has(label)) {
            throw new Error(`Key label '${label}' already exists`);
        }

        this.pendingLabels.add(label);
        try {
            const identity = await createIdentity(signingName, this.hashSuite);
            const record: KeyRecord = {
                label,
                keyId: identity.keyId,
                publicKey: encodePublicKey(identity.publicKey),
            };
            this.keys.push({
                record,
                identity: copyIdentity(identity),
                passphrase,
            });
            return copyIdentity(identity);
        } finally {
            this.pendingLabels.delete(label);
        }
    }

    async unlock(labelOrPrefix: string, passphrase: string): Promise<OwnIdentity> {
        const key = this.resolveKey(labelOrPrefix);
        if (key.passphrase !== passphrase) {
            throw new Error(`Wrong passphrase for key '${labelOrPrefix}'`);
        }
        return copyIdentity(key.identity);
    }

    resolvePublic(labelOrPrefix: string): {
        keyId: KeyId;
        publicKey: ReturnType<typeof decodePublicKey>;
    } {
        const record = this.resolveKey(labelOrPrefix).record;
        return {
            keyId: record.keyId,
            publicKey: decodePublicKey(record.publicKey),
        };
    }

    resolveRecord(labelOrPrefix: string): KeyRecord {
        return copyRecord(this.resolveKey(labelOrPrefix).record);
    }

    private hasLabel(label: string): boolean {
        return this.keys.some((key) => key.record.label === label);
    }

    private resolveKey(labelOrPrefix: string): MemoryKey {
        const normalized = labelOrPrefix.startsWith('#') ? labelOrPrefix.slice(1) : labelOrPrefix;
        const labelMatch = this.keys.find((key) => key.record.label === normalized);
        if (labelMatch !== undefined) return labelMatch;

        const keyMatches = this.keys.filter((key) => key.record.keyId.startsWith(normalized));
        if (keyMatches.length === 1) return keyMatches[0]!;
        if (keyMatches.length === 0) throw new Error(`Unknown key '${labelOrPrefix}'`);
        throw new Error(`Ambiguous key prefix '${labelOrPrefix}'`);
    }
}

function copyRecord(record: KeyRecord): KeyRecord {
    return {
        label: record.label,
        keyId: record.keyId,
        publicKey: { ...record.publicKey },
    };
}

function copyIdentity(identity: OwnIdentity): OwnIdentity {
    return {
        keyId: identity.keyId,
        publicKey: {
            suite: identity.publicKey.suite,
            key: new Uint8Array(identity.publicKey.key),
        },
        secretKey: new Uint8Array(identity.secretKey),
    };
}
