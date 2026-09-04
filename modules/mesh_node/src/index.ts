// Node mesh factory: ws+wss transports, folder-discovery backup, and an
// optional tracker (probed, never spawned). One Mesh per network environment
// (MeshScope). Advertise == listen: the addresses peers dial are exactly the
// ones the Mesh listens on, so 0.0.0.0 / bind-all is rejected as a listen
// address. `internet` without an explicit listen address is dial-out only.

import { createServer } from "node:net";

import {
    KEM_X25519_HKDF,
    SIGNING_ED25519,
    type OwnIdentity,
} from "@hyper-hyper-space/hhs3_crypto";
import {
    DiscoveryStack,
    Mesh,
    QuietDiscovery,
    createAuthenticator,
    probeTracker,
    type DiscoveryLayer,
    type IssueReporter,
    type MeshScope,
    type NetworkAddress,
    type PeerDiscovery,
    type PeerInfo,
    type TransportProvider,
} from "@hyper-hyper-space/hhs3_mesh";
import { FolderDiscovery, defaultMeshFolderRoot } from "@hyper-hyper-space/hhs3_mesh_folder_discovery";
import { TrackerClient, resolveTrackerConfig } from "@hyper-hyper-space/hhs3_mesh_tracker_client";
import { WsTransportProvider } from "@hyper-hyper-space/hhs3_mesh_ws";

export type MeshCloseable = { close(): void | Promise<void> };

export type NodeMeshRequest = {
    scope: MeshScope;
    identity: OwnIdentity;
    trackerAddress?: string;
    trackerKeyId?: string;
    listenAddress?: string;
    report?: IssueReporter;
};

export type NodeMeshOptions = {
    folderRoot?: string;
};

export type BuiltMesh = {
    mesh: Mesh;
    discovery: PeerDiscovery;
    listenAddresses: NetworkAddress[];
    discoveryNotes: string[];
    closeables: MeshCloseable[];
};

export async function createNodeMesh(
    req: NodeMeshRequest,
    opts: NodeMeshOptions = {},
): Promise<BuiltMesh> {
    const tracker = resolveTrackerConfig(req.scope, {
        tracker: req.trackerAddress,
        trackerKey: req.trackerKeyId,
    });

    const listenAddresses = await resolveListenAddresses(req.scope, req.listenAddress);

    const authenticator = createAuthenticator({
        localKey: req.identity,
        signingName: SIGNING_ED25519,
        kemPrefs: [KEM_X25519_HKDF],
    });

    const ws = new WsTransportProvider('ws');
    const wss = new WsTransportProvider('wss');
    const trackerProvider = providerFor(tracker.address, ws, wss);

    // Advertise == listen. A dial-out-only mesh (empty listen set) advertises
    // nothing, so peers found via the tracker are dialed but we register no
    // presence of our own.
    const localPeer: PeerInfo = { keyId: req.identity.keyId, addresses: listenAddresses };
    const layers: DiscoveryLayer[] = [];
    const closeables: MeshCloseable[] = [];
    const notes: string[] = [];

    if (req.scope === 'internet' && listenAddresses.length === 0) {
        notes.push('dial-out only: pass --listen to accept inbound');
    }

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
    // Same priority as the tracker so DiscoveryStack merges both sources in
    // parallel; a slow or hung tracker query must not delay the backup layer.
    layers.push({ source: folder, priority: 0 });
    closeables.push(folder);
    notes.push(`folder ${folder.root}`);

    const discovery = new DiscoveryStack(layers);
    const mesh = new Mesh({
        transports: [ws, wss],
        discovery,
        authenticator,
        localKeyId: req.identity.keyId,
        listenAddresses,
        report: req.report,
    });

    return {
        mesh,
        discovery,
        listenAddresses,
        discoveryNotes: notes,
        closeables,
    };
}

async function resolveListenAddresses(
    scope: MeshScope,
    listenOverride?: string,
): Promise<NetworkAddress[]> {
    if (listenOverride !== undefined && listenOverride.trim() !== '') {
        return [await concreteListenAddress(listenOverride)];
    }
    if (scope === 'localhost') {
        const port = await pickFreePort('127.0.0.1');
        return [`ws://127.0.0.1:${port}`];
    }
    // internet without an explicit listen address: dial-out only.
    return [];
}

async function concreteListenAddress(listen: string): Promise<NetworkAddress> {
    const url = new URL(listen);
    if (url.hostname === '0.0.0.0' || url.hostname === '[::]' || url.hostname === '::') {
        throw new Error(`--listen must be a dialable address, not bind-all (${listen})`);
    }
    if (url.port === '') {
        url.port = String(await pickFreePort(url.hostname));
    } else {
        const port = Number(url.port);
        if (!Number.isInteger(port) || port <= 0) throw new Error(`invalid --listen port in ${listen}`);
    }
    return stripTrailingSlash(url.toString());
}

function stripTrailingSlash(s: string): string {
    return s.endsWith('/') ? s.slice(0, -1) : s;
}

function providerFor(address: string, ws: TransportProvider, wss: TransportProvider): TransportProvider {
    if (address.startsWith('wss://')) return wss;
    if (address.startsWith('ws://')) return ws;
    throw new Error(`unsupported tracker URL '${address}'`);
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
