// Public RDb interfaces.
//
// RDb is the deployment sync root: an advisory, monotonic registry of member
// RSchemas and RTableGroups, with a startSync fan-out that ensures members and
// their transitive references are present and syncing in the replica.

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { RObject, SyncableObject, Version } from "@hyper-hyper-space/hhs3_mvt";

import type { RDbRuntimeConfig } from "./rdb.js";

export interface RDb extends RObject, SyncableObject {
    // Membership writers (monotonic; optional free-form note, never resolved).
    addSchema(schemaId: B64Hash, note?: string, at?: Version): Promise<B64Hash>;
    addGroup(groupId: B64Hash, note?: string, at?: Version): Promise<B64Hash>;

    // Membership resolution (add-only union by id).
    getMemberSchemas(): Promise<B64Hash[]>;
    getMemberGroups(): Promise<B64Hash[]>;

    // Tune mesh / backend label / fetch timeout used by the startSync fan-out.
    setRuntimeConfig(config: RDbRuntimeConfig): void;
}
