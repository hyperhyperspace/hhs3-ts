// What a device supplies to rhost: where each host's replica lives, the
// keystore, the release files the app ships, the mesh, and the projection
// target. rhost itself is browser-safe; each platform is not.

import type { BidirectionalTarget } from "@hyper-hyper-space/hhs3_rdb_adapter";
import type { FileDirectory } from "@hyper-hyper-space/hhs3_rdb_projection";
import type { KeyVault } from "@hyper-hyper-space/hhs3_rdb_runtime";
import type { DagBackend } from "@hyper-hyper-space/hhs3_replica";
import type { ReleaseFile } from "@hyper-hyper-space/hhs3_rpack";

import type { FilesMountConfig, HostKeyConfig, HostRecord, ProjectionConfig } from "./config.js";
import type { Connection } from "./serve.js";
import type { SyncMeshFactory } from "./sync.js";

export interface HostStore {
    // The published hosts, sorted by name.
    list(): Promise<string[]>;
    read(name: string): Promise<HostRecord | undefined>;
    // Builds a new host out of sight: `build` gets a fresh replica backend and
    // returns the host's record once it has closed its runtime. Only then is
    // the host published under `name`; if `build` throws, nothing is left
    // behind.
    create(name: string, build: (backend: DagBackend) => Promise<HostRecord>): Promise<void>;
    // The replica storage of a published host.
    openReplica(name: string): Promise<DagBackend>;
    remove(name: string): Promise<void>;
}

export interface HostPlatform {
    keyVault(): Promise<KeyVault>;
    // Where the keys are kept, for messages about a missing one (Node: the
    // keystore file).
    readonly keystoreLocation?: string;
    // The passphrase of a host's key, from its `passphrase` source: 'prompt',
    // 'env:<VAR>', or undefined for a key stored without one.
    passphrase(key: HostKeyConfig): Promise<string>;
    readReleases(folder: string): Promise<ReleaseFile[]>;
    hosts: HostStore;
    meshFactory: SyncMeshFactory;
    projectionTarget(host: string, config: ProjectionConfig): Promise<BidirectionalTarget>;
    // The folder of a file mount (`mount.path` resolves in the host folder).
    filesDirectory(host: string, mount: FilesMountConfig): Promise<FileDirectory>;
    // Held while a host runs, so two processes never serve it at once.
    // Returns the release function; throws when another holder is alive.
    acquireLock?(host: string): Promise<() => Promise<void>>;
    // Accepts client connections to a running host (Node: run/rhost.sock).
    // Returns the function that stops listening and closes them.
    listen?(host: string, onConnection: (connection: Connection) => void): Promise<() => Promise<void>>;
}
