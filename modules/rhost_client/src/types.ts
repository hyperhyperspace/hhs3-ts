// What an app sees of one rhost host. Plain JSON-able types, so the same
// shapes cross a socket, a BroadcastChannel, or nothing at all (in process).

export type ClientRelease = { hash: string; version: string; note?: string };

export type ClientMemberState = 'ok' | 'intermediate' | 'behind' | 'ahead' | 'diverged' | 'missing';

export type ClientMember = {
    name: string;
    groupId: string;
    catalogGroupHash: string;
    schemaRef: string;
    present: boolean;
    target: string[];
    current?: string[];
    adopted?: string[];
    state: ClientMemberState;
};

export type ClientUnresolved =
    | { kind: 'missing-catalog'; catalog: string }
    | { kind: 'missing-release'; release: string }
    | { kind: 'invalid-params'; message: string };

export type ClientShipped = { name: string; version: string; release: string; tag: string };

// A param a release declares, by name and column type.
export type ClientParam = { name: string; type: string };

// A file mount of a running host. `pending` means the deployed catalog
// has no FILES with that name yet (or its objects haven't arrived).
export type FilesMountStatus = {
    name: string;
    path: string;
    state: 'mounted' | 'pending';
    files?: number;       // live files
    missing?: number;     // live files not on disk yet (incomplete)
    localOnly?: number;   // files on disk that are never synced
    waiting?: number;     // local changes not uploaded yet
    writable?: boolean;
    lastError?: string;
};

// The status rhost computes from a host's replica. When the host is behind
// (the app ships a release it hasn't deployed), `notDeployed` says why the
// next update won't deploy it, if it won't; `missingParams` lists the params
// the release needs that the app's params don't set.
export type HostStatus = {
    host: string;
    database: string;
    name?: string;
    catalog: string;
    created: boolean;
    shipped?: ClientShipped;
    adoptionRange: string;
    deployed: ClientRelease[];
    adopted: ClientRelease[];
    held: ClientRelease[];
    members: ClientMember[];
    unresolved?: ClientUnresolved;
    upgradeRequired: boolean;
    hostBehind: boolean;
    notDeployed?: string;
    missingParams?: ClientParam[];
    running: boolean;
    peers?: number;
    files?: FilesMountStatus[];
    lastError?: string;
};

// What a client outside the process knows of a host that isn't running: only
// what its files say.
export type StoppedStatus = {
    host: string;
    database: string;
    catalog: string;
    created: boolean;
    running: false;
};

export type ClientStatus = HostStatus | StoppedStatus;

export function isHostStatus(status: ClientStatus): status is HostStatus {
    return 'deployed' in status;
}

// The host's own key. `id` is its row in the projection's key table, the
// value key columns take.
export type ClientKey = { label: string; keyId: string; publicKey: string; id: number };

// A rejected local write, or a concurrent change that voided or reinstated a
// write, as logged by the projection.
export type ClientEvent = {
    id: number;
    origin: 'ingestion' | 'concurrency';
    direction: 'failure' | 'void' | 'reinstate';
    groupId: string;
    opHash: string;
    kind: string;
    table?: string;
    rowId?: string;
    localId?: number;
    author?: string;
    op?: unknown;
    reason?: unknown;
};

export interface ClientEvents {
    // Events with an id above `afterId` (default 0), oldest first.
    since(afterId?: number, limit?: number): Promise<ClientEvent[]>;
    // New events only, in batches; returns the unsubscribe function.
    watch(listener: (events: ClientEvent[]) => void): () => void;
}

export interface RhostClient {
    me(): Promise<ClientKey>;
    // Interns a public key (base64 of the serialized key) in the projection's
    // key table and returns its id.
    registerKey(publicKey: string): Promise<number>;
    readonly events: ClientEvents;
    status(): Promise<ClientStatus>;
    // Calls `listener` with a fresh status on every change; returns the
    // unsubscribe function.
    watchStatus(listener: (status: ClientStatus) => void): () => void;
    close(): Promise<void>;
}
