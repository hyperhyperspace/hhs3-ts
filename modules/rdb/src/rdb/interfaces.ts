// Public RDb interfaces.
//
// RDb is the sync root of a database deployed from a catalog: its DAG records
// the deployed catalog releases and their params, and its member groups are
// computed from them. startSync subscribes to the RDb DAG and reconciles a
// fan-out of sync sessions for the catalog, its schemas and the member groups;
// the adoption policy admits deployed releases into the members' local gates.

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity, KeyId } from "@hyper-hyper-space/hhs3_crypto";
import type { RObject, SyncableObject, Version } from "@hyper-hyper-space/hhs3_mvt";

import type { RDbRuntimeConfig } from "./rdb.js";
import type { ParamValue, SchemaCreator } from "./payload.js";
import type { RDbResolution } from "./resolve.js";
import type { MemberFiles } from "./instantiate.js";
import type { CreateTableGroupPayload } from "../rtable_group/payload.js";
import type { RCatalog } from "../rcatalog/interfaces.js";

export interface RDb extends RObject, SyncableObject {
    // Create-time deployment authority (empty when unsigned / open mode).
    getCreators(): SchemaCreator[];
    isCreator(keyId: KeyId): boolean;

    getName(): string | undefined;
    getCatalogRef(): B64Hash;
    getCatalog(): Promise<RCatalog | undefined>;

    // Deploys another release of the catalog (any release not at or below a
    // deployed one; a concurrent release merges with the deployed ones), with
    // the params it first needs. When the RDb declares creators, author is
    // required.
    updateCatalog(release: B64Hash, params?: { [name: string]: ParamValue }, author?: OwnIdentity, note?: string, at?: Version): Promise<B64Hash>;

    resolve(at?: Version): Promise<RDbResolution>;
    getDeployedReleases(at?: Version): Promise<B64Hash[]>;
    getDeployHistory(at?: Version): Promise<B64Hash[]>;
    getParams(at?: Version): Promise<{ [name: string]: ParamValue }>;

    // Computed membership.
    getMemberGroupNames(at?: Version): Promise<Map<string, B64Hash>>;
    getMemberGroupPayloads(at?: Version): Promise<Map<B64Hash, CreateTableGroupPayload>>;
    getMemberGroups(): Promise<B64Hash[]>;
    getMemberSchemas(): Promise<B64Hash[]>;
    getMemberFiles(at?: Version): Promise<MemberFiles[]>;

    // Creates the computed member groups (and their gates) and FILES objects
    // that are absent and whose genesis deps are present. Returns the created
    // object ids.
    materializeMembers(): Promise<B64Hash[]>;

    // Adoption.
    getAdoptionRange(): Promise<string>;
    setAdoptionRange(range: string): Promise<void>;
    getAdoptedReleases(): Promise<B64Hash[]>;
    adopt(): Promise<void>;

    // Tune mesh / backend label / fetch timeout / adoption range.
    setRuntimeConfig(config: RDbRuntimeConfig): void;
}
