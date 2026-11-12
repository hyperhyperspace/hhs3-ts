// Sync services shared by rhost and the REPL's `\sync`: the host-injected mesh
// factory, the allow-list authorizer, and starting, fetching and tearing down a
// database's sync over a mesh attached to its replica.

import type { B64Hash, KeyId, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { IssueReporter, Mesh, PeerAuthorizer, PeerDiscovery, ScopedMeshConfig } from "@hyper-hyper-space/hhs3_mesh";
import type { RObject } from "@hyper-hyper-space/hhs3_mvt";
import { RDB_TYPE_ID, type RDb } from "@hyper-hyper-space/hhs3_rdb";
import { executeText, type RdbSession } from "@hyper-hyper-space/hhs3_rdb_runtime";
import type { Replica } from "@hyper-hyper-space/hhs3_replica";

export type { ScopedMeshConfig };
export type SyncScope = ScopedMeshConfig['scope'];

export type MeshRuntime = {
    identity: OwnIdentity;
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

// Host-injected: Node builds a tracker/folder mesh, the browser a BroadcastChannel
// or WebSocket one, tests an in-memory one.
export type SyncMeshFactory = (config: ScopedMeshConfig, runtime: MeshRuntime) => Promise<BuiltSyncMesh>;

// --- Allow lists ---

export type AllowSource =
    | { type: 'everyone' }
    | { type: 'column'; group: string; table: string; column: string; where?: string };

export type ColumnSource = Extract<AllowSource, { type: 'column' }>;

export type ColumnLookup = (source: ColumnSource) => Promise<Iterable<unknown>>;

// The frontier of the group a source reads, sorted. `undefined` while that
// group is not on this replica yet. Throws when the database has no group by
// that name.
export type AllowFrontier = (source: ColumnSource) => Promise<B64Hash[] | undefined>;

// Where a host first found its own key on its allow list: the group whose
// column listed it, and that group's frontier then.
export type ListedAt = { group: string; frontier: B64Hash[] };

// Given to the authorizer of a host that is bootstrapping: one that joined its
// database and has not found its own key on its allow list yet.
export type AllowBootstrap = {
    self: KeyId;
    // Called once, when the key is first found.
    found(at: ListedAt): Promise<void>;
};

export type AllowOptions = {
    // Lets the authorizer keep the listed keys until a group it reads changes.
    frontier?: AllowFrontier;
    bootstrap?: AllowBootstrap;
};

export function allowIsEveryone(sources: AllowSource[]): boolean {
    return sources.length === 0 || sources.some((s) => s.type === 'everyone');
}

export function formatAllow(sources: AllowSource[]): string {
    if (allowIsEveryone(sources)) return 'everyone';
    const parts = sources.map(formatAllowSource);
    return parts.length === 1 ? parts[0]! : `[${parts.join(', ')}]`;
}

export function formatAllowSource(source: AllowSource): string {
    if (source.type === 'everyone') return 'everyone';
    const path = `${source.group}.${source.table}.${source.column}`;
    return source.where === undefined ? path : `${path} where ${source.where}`;
}

const ALLOW_COLUMN_RE = /^([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)(?:\s+where\s+([\s\S]+))?$/;

// One allow entry as written in a config file: `everyone`, or
// `group.table.column`, optionally followed by `where <condition>`.
export function parseAllowSource(text: string): AllowSource {
    const trimmed = text.trim();
    if (trimmed === 'everyone') return { type: 'everyone' };
    const m = ALLOW_COLUMN_RE.exec(trimmed);
    if (m === null) {
        throw new Error(`allow entry '${text}' is not 'everyone' or 'group.table.column [where <condition>]'`);
    }
    const where = m[4]?.trim();
    return {
        type: 'column', group: m[1]!, table: m[2]!, column: m[3]!,
        ...(where !== undefined && where.length > 0 ? { where } : {}),
    };
}

// A key is listed when it appears in any of the columns. `everyone` anywhere in
// the list means no authorizer at all.
//
// A host serves only listed peers, and admits only listed peers, with one
// exception: while it is bootstrapping it admits any peer, since its copy of
// the list is older than its own admission and can't tell members from
// strangers. Bootstrapping ends the first time the host's own key is listed,
// and never starts again.
export function createAllowAuthorizer(
    sources: AllowSource[],
    lookup: ColumnLookup,
    options: AllowOptions = {},
): PeerAuthorizer | undefined {
    if (allowIsEveryone(sources)) return undefined;
    const columns = sources.filter((s): s is ColumnSource => s.type === 'column');
    const { frontier } = options;
    let pending = options.bootstrap;
    let cached: { stamp: string; keys: Set<string> } | undefined;

    async function stamp(): Promise<string | undefined> {
        if (frontier === undefined) return undefined;
        const parts: string[] = [];
        for (const source of columns) {
            let at: B64Hash[] | undefined;
            try { at = await frontier(source); } catch { at = undefined; }
            parts.push(at === undefined ? '-' : at.join(','));
        }
        return parts.join('|');
    }

    async function listedKeys(): Promise<Set<string>> {
        const before = await stamp();
        if (before !== undefined && cached !== undefined && cached.stamp === before) return cached.keys;
        const keys = new Set<string>();
        let foundIn: ColumnSource | undefined;
        for (const source of columns) {
            let values: Iterable<unknown>;
            try {
                values = await lookup(source);
            } catch {
                continue;
            }
            for (const value of values) {
                const key = String(value);
                keys.add(key);
                if (foundIn === undefined && pending !== undefined && key === pending.self) foundIn = source;
            }
        }
        if (before !== undefined) cached = { stamp: before, keys };
        if (foundIn !== undefined && pending !== undefined) {
            const bootstrap = pending;
            pending = undefined;
            // Read after the lookup, so the position is at or past the one the
            // key was found at. A group that listed the key has entries; if its
            // frontier can't be read, bootstrapping goes on and the next read
            // tries again.
            let at: B64Hash[] | undefined;
            try { at = await frontier?.(foundIn); } catch { at = undefined; }
            if (at === undefined || at.length === 0) pending = bootstrap;
            else await bootstrap.found({ group: foundIn.group, frontier: at });
        }
        return keys;
    }

    return {
        async serves(keyId: KeyId): Promise<boolean> {
            return (await listedKeys()).has(keyId);
        },
        async authorize(keyId: KeyId): Promise<boolean> {
            const keys = await listedKeys();
            if (pending !== undefined) return true;
            return keys.has(keyId);
        },
    };
}

// Reads a column source with a C-SQL SELECT on the session's workspace.
export function columnLookup(session: RdbSession): ColumnLookup {
    return async (source) => {
        const sql = `SELECT ${source.column} FROM ${source.group}.${source.table}`
            + (source.where === undefined ? '' : ` WHERE ${source.where}`);
        const run = await executeText(session, sql);
        const result = run.results[0]?.result;
        if (result === undefined || result.kind !== 'select') {
            throw new Error(`allow query did not return a SELECT result`);
        }
        return result.rows.map((row) => row.values[source.column]);
    };
}

// Runs every column query once, so a typo fails before sync starts. With a
// frontier, a group the database has but this replica hasn't created yet is
// skipped: a joined host gets its groups from sync.
export async function validateAllowSources(sources: AllowSource[], lookup: ColumnLookup, frontier?: AllowFrontier): Promise<void> {
    for (const source of sources) {
        if (source.type !== 'column') continue;
        if (frontier !== undefined && await frontier(source) === undefined) continue;
        await lookup(source);
    }
}

// --- Meshes and database sync ---

export async function closeBuiltMesh(
    mesh: { close(): void; closePeers?(): void },
    closeables: SyncCloseable[],
): Promise<void> {
    // Tell swarm peers immediately. Discovery close (the tracker leave) still
    // needs the listen/dial transports, so those stay up until mesh.close().
    try {
        mesh.closePeers?.();
    } catch {
        // best-effort
    }
    // Await discovery close (tracker chain drain / leave) before mesh.close()
    // tears down transports. A short race here used to kill in-flight tracker
    // exchanges and crash the process with an unhandled rejection.
    await Promise.all(closeables.map(async (closeable) => {
        try {
            await closeable.close();
        } catch {
            // best-effort
        }
    }));
    try {
        mesh.close();
    } catch {
        // best-effort
    }
}

export type SyncPeer = { keyId: KeyId; endpoint: string; topic: string };

export type DatabaseSync = {
    readonly meshLabel: string;
    readonly mesh: Mesh;
    readonly listenAddresses: string[];
    readonly discoveryNotes: string[];
    // One row per peer and endpoint, with the first topic it was seen on.
    peers(): SyncPeer[];
    // Distinct peer keys.
    peerCount(): number;
    stop(): Promise<void>;
};

export type StartDatabaseSyncOptions = {
    replica: Replica;
    db: RDb;
    built: BuiltSyncMesh;
    meshLabel: string;
    authorizer?: PeerAuthorizer;
    report?: IssueReporter;
};

// Attaches the built mesh to the replica under `meshLabel` and starts the
// database's sync on it. If the start fails, everything is torn down before
// the error is rethrown.
export async function startDatabaseSync(options: StartDatabaseSyncOptions): Promise<DatabaseSync> {
    const { replica, db, built, meshLabel } = options;
    let stopped = false;
    const handle: DatabaseSync = {
        meshLabel,
        mesh: built.mesh,
        listenAddresses: built.listenAddresses,
        discoveryNotes: built.discoveryNotes,
        peers() {
            const seen = new Set<string>();
            const rows: SyncPeer[] = [];
            for (const swarm of built.mesh.swarms()) {
                for (const peer of swarm.peers()) {
                    const key = `${peer.keyId}@${peer.endpoint}`;
                    if (seen.has(key)) continue;
                    seen.add(key);
                    rows.push({ keyId: peer.keyId, endpoint: peer.endpoint, topic: swarm.topic });
                }
            }
            return rows;
        },
        peerCount() {
            const ids = new Set<string>();
            for (const swarm of built.mesh.swarms()) {
                for (const peer of swarm.peers()) ids.add(peer.keyId);
            }
            return ids.size;
        },
        async stop() {
            if (stopped) return;
            stopped = true;
            try {
                await db.stopSync();
            } catch {
                // already stopped
            }
            await closeBuiltMesh(built.mesh, built.closeables);
            replica.detachMesh(meshLabel);
        },
    };

    replica.attachMesh(meshLabel, built.mesh);
    db.setRuntimeConfig({ meshLabel, authorizer: options.authorizer, report: options.report });
    try {
        await db.startSync();
    } catch (err) {
        await handle.stop();
        throw err;
    }
    return handle;
}

export type FetchDatabaseOptions = {
    replica: Replica;
    id: string;
    meshFactory: SyncMeshFactory;
    config: ScopedMeshConfig;
    runtime: MeshRuntime;
    meshLabel: string;
    backendLabel?: string;
};

// Fetches a database's genesis over a temporary mesh and creates it in the
// replica. The mesh is closed and detached whatever happens.
export async function fetchDatabase(options: FetchDatabaseOptions): Promise<RObject> {
    const { replica, meshLabel } = options;
    let built: BuiltSyncMesh | undefined;
    try {
        built = await options.meshFactory(options.config, options.runtime);
        replica.attachMesh(meshLabel, built.mesh);
        const obj = await replica.fetchObject(options.id, {
            meshLabel,
            ...(options.backendLabel !== undefined ? { backendLabel: options.backendLabel } : {}),
        });
        if (obj.getType() !== RDB_TYPE_ID) {
            throw new Error(
                `Fetched object is type '${obj.getType()}', not an RDb. The RDb may already be local.`,
            );
        }
        return obj;
    } catch (err) {
        const leftover = await replica.getObject(options.id);
        const msg = err instanceof Error ? err.message : String(err);
        if (leftover !== undefined && !msg.includes('may already be local')) {
            throw new Error(`${msg} The RDb may already be local.`);
        }
        throw err;
    } finally {
        if (built !== undefined) {
            await closeBuiltMesh(built.mesh, built.closeables);
        }
        replica.detachMesh(meshLabel);
    }
}
