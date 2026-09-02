// Public RDb interfaces.
//
// RDb is the deployment sync root: an advisory, monotonic registry of member
// RSchemas and RTableGroups. startSync subscribes to the RDb DAG and reconciles
// a fan-out of sync sessions for members and their transitive references;
// later membership ops keep that set updated until stopSync.

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity, KeyId } from "@hyper-hyper-space/hhs3_crypto";
import type { RObject, SyncableObject, Version } from "@hyper-hyper-space/hhs3_mvt";

import type { RDbRuntimeConfig } from "./rdb.js";
import type { SchemaCreator } from "./payload.js";

export interface RDb extends RObject, SyncableObject {
    // Membership writers (monotonic; optional free-form note, never resolved).
    // When the RDb declares creators, author is required and the op is signed.
    addSchema(schemaId: B64Hash, note?: string, author?: OwnIdentity, at?: Version): Promise<B64Hash>;
    addGroup(groupId: B64Hash, note?: string, author?: OwnIdentity, at?: Version): Promise<B64Hash>;

    // Create-time deployment authority (empty when unsigned / open mode).
    getCreators(): SchemaCreator[];
    isCreator(keyId: KeyId): boolean;

    // Membership resolution (add-only union by id).
    getMemberSchemas(): Promise<B64Hash[]>;
    getMemberGroups(): Promise<B64Hash[]>;

    // Tune mesh / backend label / fetch timeout used by the sync fan-out.
    setRuntimeConfig(config: RDbRuntimeConfig): void;
}
