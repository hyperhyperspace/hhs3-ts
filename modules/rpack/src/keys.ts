// The developer's keys, as rpack needs them: public records by label (no
// passphrase), and the stand-ins that sign in the scratch runtime that
// evaluates target-catalog.sql.

import type { KeyId, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { serializePublicKeyToBase64 } from "@hyper-hyper-space/hhs3_mvt";
import type { SourceKeyLabels } from "@hyper-hyper-space/hhs3_rdb_lang";
import { decodePublicKey, MemoryKeyVault, type KeyVault } from "@hyper-hyper-space/hhs3_rdb_runtime";

// A public key as creators and rows carry it: base64 of the serialized key.
export type DevKey = { label: string; keyId: KeyId; publicKey: string };

export class KeyDirectory {
    private readonly byLabel = new Map<string, DevKey>();
    private readonly byKeyId = new Map<string, DevKey>();
    private readonly byPublicKey = new Map<string, DevKey>();

    constructor(keys: DevKey[]) {
        for (const key of keys) {
            this.byLabel.set(key.label, key);
            this.byKeyId.set(key.keyId, key);
            this.byPublicKey.set(key.publicKey, key);
        }
    }

    static fromVault(vault: KeyVault): KeyDirectory {
        return new KeyDirectory(vault.list().map((record) => ({
            label: record.label,
            keyId: record.keyId,
            publicKey: serializePublicKeyToBase64(decodePublicKey(record.publicKey)),
        })));
    }

    keys(): DevKey[] {
        return [...this.byLabel.values()];
    }

    get(label: string): DevKey | undefined {
        return this.byLabel.get(label);
    }

    labels(): SourceKeyLabels {
        return {
            keyId: (keyId) => this.byKeyId.get(keyId)?.label,
            publicKey: (publicKey) => this.byPublicKey.get(publicKey)?.label,
        };
    }
}

// One stand-in per developer key, under the same label. `replace` maps every
// stand-in key id and public key to the real one.
export type StandIns = {
    vault: MemoryKeyVault;
    passphrase: string;
    replace: Map<string, string>;
    identity(label: string): OwnIdentity;
};

const STAND_IN_PASSPHRASE = 'stand-in';

export async function createStandIns(keys: KeyDirectory): Promise<StandIns> {
    const vault = new MemoryKeyVault();
    const replace = new Map<string, string>();
    const identities = new Map<string, OwnIdentity>();
    for (const key of keys.keys()) {
        const standIn = await vault.create(key.label, STAND_IN_PASSPHRASE);
        identities.set(key.label, standIn);
        replace.set(standIn.keyId, key.keyId);
        replace.set(serializePublicKeyToBase64(standIn.publicKey), key.publicKey);
    }
    return {
        vault,
        passphrase: STAND_IN_PASSPHRASE,
        replace,
        identity: (label) => {
            const identity = identities.get(label);
            if (identity === undefined) throw new Error(`no stand-in for '${label}'`);
            return identity;
        },
    };
}

// `value` with every string that is a stand-in key replaced by the real key.
export function replaceKeys<T>(value: T, replace: Map<string, string>): T {
    if (typeof value === 'string') return (replace.get(value) ?? value) as T;
    if (Array.isArray(value)) return value.map((v) => replaceKeys(v, replace)) as T;
    if (value instanceof Map) return new Map([...value].map(([k, v]) => [k, replaceKeys(v, replace)])) as T;
    if (typeof value === 'object' && value !== null) {
        return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, replaceKeys(v, replace)])) as T;
    }
    return value;
}
