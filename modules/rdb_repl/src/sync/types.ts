import type { KeyId, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { IssueReporter, Mesh, PeerDiscovery } from "@hyper-hyper-space/hhs3_mesh";
import type { RDb } from "@hyper-hyper-space/hhs3_rdb";

import type { AllowSource, SyncScope } from "./parse.js";

export type SyncMeshBuildRequest = {
    scope: SyncScope;
    identity: OwnIdentity;
    trackerAddress?: string;
    trackerKeyId?: string;
    listenAddress?: string;
    report?: IssueReporter;
};

export type SyncCloseable = { close(): void | Promise<void> };

export type BuiltSyncMesh = {
    mesh: Mesh;
    discovery: PeerDiscovery;
    listenAddresses: string[];
    discoveryNotes: string[];
    closeables: SyncCloseable[];
};

export type SyncMeshFactory = (req: SyncMeshBuildRequest) => Promise<BuiltSyncMesh>;

export type SyncSessionEntry = {
    id: number;
    dbId: string;
    dbName: string;
    db: RDb;
    meshLabel: string;
    mesh: Mesh;
    identityLabel: string;
    identityKeyId: KeyId;
    scope: SyncScope;
    sources: AllowSource[];
    listenAddresses: string[];
    discoveryNotes: string[];
    closeables: SyncCloseable[];
};
