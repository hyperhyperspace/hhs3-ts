import type { KeyId, OwnIdentity, SigningName } from "@hyper-hyper-space/hhs3_crypto";

import type { StoredPublicKey } from "./identity.js";

export type KeyRecord = {
    label: string;
    keyId: KeyId;
    publicKey: StoredPublicKey;
};

export interface KeyVault {
    list(): KeyRecord[];
    create(label: string, passphrase: string, signingName?: SigningName): Promise<OwnIdentity>;
    unlock(labelOrPrefix: string, passphrase: string): Promise<OwnIdentity>;
    resolvePublic(labelOrPrefix: string): { keyId: KeyId; publicKey: ReturnType<typeof import("./identity.js").decodePublicKey> };
    resolveRecord(labelOrPrefix: string): KeyRecord;
}
