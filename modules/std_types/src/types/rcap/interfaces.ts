import { B64Hash, KeyId, PublicKey } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity, HashSuite } from "@hyper-hyper-space/hhs3_crypto";

import { Payload, RObject, SyncableObject, Version, View, ForeignDep, ValidationResult } from "@hyper-hyper-space/hhs3_mvt";

import type { RCapEvent } from "./events.js";
import type { CapDefinition } from "./payload.js";

export interface RCap extends RObject, SyncableObject {
    getEnrollCapabilityName(): string;
    getInitialCaps(): { [capName: string]: CapDefinition };
    isCreator(keyId: KeyId): boolean;
    lookupCreatorKey(keyId: KeyId): PublicKey | undefined;
    lookupKey(keyId: KeyId): Promise<PublicKey | undefined>;
    getHashSuite(): HashSuite;
    selfValidate(): boolean;

    addIdentity(keyId: KeyId, publicKey: string, author: OwnIdentity, at?: Version): Promise<B64Hash>;
    createCap(capName: string, managedBy: string[], author: OwnIdentity, at?: Version): Promise<B64Hash>;
    deleteCap(capName: string, author: OwnIdentity, at?: Version): Promise<B64Hash>;
    grant(grantee: KeyId, capName: string, author: OwnIdentity, at?: Version): Promise<B64Hash>;
    revoke(grantee: KeyId, capName: string, author: OwnIdentity, at?: Version): Promise<B64Hash>;

    validatePayload(payload: Payload, at: Version): Promise<ValidationResult>;
    applyPayload(payload: Payload, at: Version): Promise<B64Hash>;
    getView(at?: Version, from?: Version): Promise<RCapView>;
    extractForeignDeps(payload: Payload, at: Version): ForeignDep[] | undefined;
    subscribe(callback: (event: RCapEvent) => void): void;
    unsubscribe(callback: (event: RCapEvent) => void): void;

    setDeltaStrategy(strategy: "full" | "bounded"): void;
    configure(config: { meshLabel?: string }): void;
}

export interface RCapView extends View {
    getObject(): RCap;
    isIdentity(keyId: KeyId): Promise<boolean>;
    capabilityExists(capName: string): Promise<boolean>;
    hasCapability(grantee: KeyId, capName: string, visiting?: Set<string>): Promise<boolean>;
    getManagedBy(capName: string): Promise<string[]>;
    currentCapCreationVersion(capName: string): Promise<Version>;
    getCapabilities(): Promise<string[]>;
}
