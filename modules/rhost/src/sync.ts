// Sync services shared by rhost and the REPL's `\sync`: the host-injected mesh
// factory, the allow-list authorizer, and starting, fetching and tearing down a
// database's sync over a mesh attached to its replica.

import type { KeyId, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { IssueReporter, Mesh, PeerAuthorizer, PeerDiscovery } from "@hyper-hyper-space/hhs3_mesh";
import type { RObject } from "@hyper-hyper-space/hhs3_mvt";
import { RDB_TYPE_ID, type RDb } from "@hyper-hyper-space/hhs3_rdb";
import { executeText, type RdbSession } from "@hyper-hyper-space/hhs3_rdb_runtime";
import type { Replica } from "@hyper-hyper-space/hhs3_replica";

const MESH_CLOSEABLE_WAIT_MS = 1_000;

export type SyncScope = 'localhost' | 'internet';

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

// Host-injected: Node builds a tracker/folder mesh, the browser a BroadcastChannel
// or WebSocket one, tests an in-memory one.
export type SyncMeshFactory = (req: SyncMeshBuildRequest) => Promise<BuiltSyncMesh>;

// --- Allow lists ---

export type AllowSource =
    | { type: 'everyone' }
    | { type: 'column'; group: string; table: string; column: string; where?: string };

export type ColumnSource = Extract<AllowSource, { type: 'column' }>;

export type ColumnLookup = (source: ColumnSource) => Promise<Iterable<unknown>>;

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

// A peer is accepted when its key appears in any of the columns. `everyone`
// anywhere in the list means no authorizer at all.
export function createAllowAuthorizer(
    sources: AllowSource[],
    lookup: ColumnLookup,
): PeerAuthorizer | undefined {
    if (allowIsEveryone(sources)) return undefined;
    const columns = sources.filter((s): s is ColumnSource => s.type === 'column');
    return {
        async authorize(keyId: KeyId): Promise<boolean> {
            for (const source of columns) {
                let values: Iterable<unknown>;
                try {
                    values = await lookup(source);
                } catch {
                    continue;
                }
                for (const value of values) {
                    if (value === keyId || String(value) === keyId) return true;
                }
            }
            return false;
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

// Runs every column query once, so a typo fails before sync starts.
export async function validateAllowSources(sources: AllowSource[], lookup: ColumnLookup): Promise<void> {
    for (const source of sources) {
        if (source.type !== 'column') continue;
        await lookup(source);
    }
}

// --- Meshes and database sync ---

export async function closeBuiltMesh(
    mesh: { close(): void },
    closeables: SyncCloseable[],
): Promise<void> {
    const closing = Promise.all(closeables.map(async (closeable) => {
        try {
            await closeable.close();
        } catch {
            // best-effort
        }
    }));
    await Promise.race([
        closing,
        new Promise<void>((resolve) => setTimeout(resolve, MESH_CLOSEABLE_WAIT_MS)),
    ]);
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
    request: SyncMeshBuildRequest;
    meshLabel: string;
    backendLabel?: string;
};

// Fetches a database's genesis over a temporary mesh and creates it in the
// replica. The mesh is closed and detached whatever happens.
export async function fetchDatabase(options: FetchDatabaseOptions): Promise<RObject> {
    const { replica, meshLabel } = options;
    let built: BuiltSyncMesh | undefined;
    try {
        built = await options.meshFactory(options.request);
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
