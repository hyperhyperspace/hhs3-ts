// Node SyncMeshFactory: ws+wss transports, folder-discovery backup, optional
// tracker (probed, never spawned). Listen bind vs announce split so 0.0.0.0 is
// never advertised.

import { createServer } from "node:net";

import {
    KEM_X25519_HKDF,
    SIGNING_ED25519,
} from "@hyper-hyper-space/hhs3_crypto";
import {
    DiscoveryStack,
    Mesh,
    createAuthenticator,
    type DiscoveryLayer,
    type NetworkAddress,
    type PeerInfo,
    type TransportProvider,
} from "@hyper-hyper-space/hhs3_mesh";
import { FolderDiscovery, defaultMeshFolderRoot } from "@hyper-hyper-space/hhs3_mesh_folder_discovery";
import { TrackerClient } from "@hyper-hyper-space/hhs3_mesh_tracker_client";
import { WsTransportProvider } from "@hyper-hyper-space/hhs3_mesh_ws";
import {
    QuietDiscovery,
    RewriteAnnounceDiscovery,
    probeTracker,
    resolveTrackerConfig,
    type BuiltSyncMesh,
    type SyncMeshFactory,
    type TrackerEnv,
} from "@hyper-hyper-space/hhs3_rdb_repl";

export type NodeSyncMeshFactoryOptions = {
    folderRoot?: string;
    env?: NodeJS.ProcessEnv;
};

export function createNodeSyncMeshFactory(opts: NodeSyncMeshFactoryOptions = {}): SyncMeshFactory {
    return async (req): Promise<BuiltSyncMesh> => {
        const env = opts.env ?? process.env;
        const tracker = resolveTrackerConfig(req.scope, {
            tracker: req.trackerAddress,
            trackerKey: req.trackerKeyId,
        }, readTrackerEnv(env));

        const bindHost = req.scope === 'internet' ? '0.0.0.0' : '127.0.0.1';
        const listenOverride = req.listenAddress ?? empty(env.RDB_SYNC_LISTEN);
        const port = await resolveListenPort(bindHost, listenOverride);
        const bindAddress: NetworkAddress = `ws://${bindHost}:${port}`;
        const announced: NetworkAddress[] = [announceAddress(port, listenOverride)];

        const authenticator = createAuthenticator({
            localKey: req.identity,
            signingName: SIGNING_ED25519,
            kemPrefs: [KEM_X25519_HKDF],
        });

        const ws = new WsTransportProvider('ws');
        const wss = new WsTransportProvider('wss');
        const trackerProvider = providerFor(tracker.address, ws, wss);

        const localPeer: PeerInfo = { keyId: req.identity.keyId, addresses: announced };
        const layers: DiscoveryLayer[] = [];
        const closeables: BuiltSyncMesh['closeables'] = [];
        const notes: string[] = [];

        const reachable = await probeTracker(trackerProvider, tracker.address);
        if (reachable) {
            const client = new TrackerClient({
                trackerAddress: tracker.address,
                trackerKeyId: tracker.keyId,
                transportProvider: trackerProvider,
                authenticator,
                localPeer,
            });
            layers.push({ source: new QuietDiscovery(client), priority: 0 });
            closeables.push(client);
            notes.push(`tracker ${tracker.address}`);
        } else {
            notes.push(`tracker ${tracker.address} unreachable`);
        }

        const folder = new FolderDiscovery({
            root: opts.folderRoot ?? defaultMeshFolderRoot(),
            self: localPeer,
        });
        layers.push({ source: folder, priority: 1 });
        closeables.push(folder);
        notes.push(`folder ${folder.root}`);

        const discovery = new RewriteAnnounceDiscovery(new DiscoveryStack(layers), announced);
        const mesh = new Mesh({
            transports: [ws, wss],
            discovery,
            authenticator,
            localKeyId: req.identity.keyId,
            listenAddresses: [bindAddress],
        });

        return {
            mesh,
            discovery,
            listenAddresses: [bindAddress],
            announcedAddresses: announced,
            discoveryNotes: notes,
            closeables,
        };
    };
}

function readTrackerEnv(env: NodeJS.ProcessEnv): TrackerEnv {
    return {
        tracker: empty(env.RDB_SYNC_TRACKER),
        trackerKey: empty(env.RDB_SYNC_TRACKER_KEY),
        listen: empty(env.RDB_SYNC_LISTEN),
    };
}

function empty(value: string | undefined): string | undefined {
    if (value === undefined || value.trim() === '') return undefined;
    return value;
}

function providerFor(address: string, ws: TransportProvider, wss: TransportProvider): TransportProvider {
    if (address.startsWith('wss://')) return wss;
    if (address.startsWith('ws://')) return ws;
    throw new Error(`unsupported tracker URL '${address}'`);
}

async function resolveListenPort(bindHost: string, listenOverride?: string): Promise<number> {
    if (listenOverride !== undefined) {
        const url = new URL(listenOverride);
        if (url.port !== '') {
            const port = Number(url.port);
            if (!Number.isInteger(port) || port <= 0) throw new Error(`invalid --listen port in ${listenOverride}`);
            return port;
        }
    }
    return pickFreePort(bindHost);
}

function announceAddress(port: number, listenOverride?: string): NetworkAddress {
    if (listenOverride !== undefined) {
        const url = new URL(listenOverride);
        if (url.port === '') url.port = String(port);
        if (url.hostname === '0.0.0.0' || url.hostname === '[::]' || url.hostname === '::') {
            url.hostname = '127.0.0.1';
        }
        return stripTrailingSlash(url.toString());
    }
    return `ws://127.0.0.1:${port}`;
}

function stripTrailingSlash(s: string): string {
    return s.endsWith('/') ? s.slice(0, -1) : s;
}

export function pickFreePort(host: string): Promise<number> {
    return new Promise((resolve, reject) => {
        const server = createServer();
        server.unref();
        server.once('error', reject);
        server.listen(0, host, () => {
            const addr = server.address();
            const port = typeof addr === 'object' && addr !== null ? addr.port : 0;
            server.close((err) => {
                if (err) reject(err);
                else if (port === 0) reject(new Error('failed to allocate a listen port'));
                else resolve(port);
            });
        });
    });
}
