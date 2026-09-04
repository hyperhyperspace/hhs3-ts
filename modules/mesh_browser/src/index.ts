// Browser mesh factory: dial-only ws/wss + BroadcastChannel transport, a
// BroadcastChannel discovery backup, and an optional tracker (probed, never
// spawned). Listen addresses are `bc://` only (browsers cannot accept inbound
// sockets). On `internet` the tracker is given an empty-address local peer so
// the public tracker is never poisoned with a tab-local `bc://` address, while
// BroadcastChannel discovery still advertises `bc://` for tab-to-tab sync.

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
    type MeshScope,
    type NetworkAddress,
    type PeerDiscovery,
    type PeerInfo,
    type TransportProvider,
} from "@hyper-hyper-space/hhs3_mesh";
import {
    BroadcastChannelDiscovery,
    BroadcastChannelTransportProvider,
    type BroadcastChannelCtor,
} from "@hyper-hyper-space/hhs3_mesh_bc";
import { TrackerClient, resolveTrackerConfig } from "@hyper-hyper-space/hhs3_mesh_tracker_client";
import {
    BrowserWsTransportProvider,
    type WebSocketCtor,
} from "@hyper-hyper-space/hhs3_mesh_ws_browser";

export type MeshCloseable = { close(): void | Promise<void> };

export type BrowserMeshRequest = {
    scope: MeshScope;
    identity: OwnIdentity;
    trackerAddress?: string;
    trackerKeyId?: string;
    listenAddress?: string;
};

export type BrowserMeshOptions = {
    BroadcastChannelCtor?: BroadcastChannelCtor;
    WebSocketCtor?: WebSocketCtor;
};

export type BuiltMesh = {
    mesh: Mesh;
    discovery: PeerDiscovery;
    listenAddresses: NetworkAddress[];
    discoveryNotes: string[];
    closeables: MeshCloseable[];
};

export async function createBrowserMesh(
    req: BrowserMeshRequest,
    opts: BrowserMeshOptions = {},
): Promise<BuiltMesh> {
    const tracker = resolveTrackerConfig(req.scope, {
        tracker: req.trackerAddress,
        trackerKey: req.trackerKeyId,
    });

    const authenticator = createAuthenticator({
        localKey: req.identity,
        signingName: SIGNING_ED25519,
        kemPrefs: [KEM_X25519_HKDF],
    });

    const bc = new BroadcastChannelTransportProvider({
        BroadcastChannelCtor: opts.BroadcastChannelCtor,
    });
    const listenAddresses: NetworkAddress[] = [bc.localAddress];

    // Advertise == listen for the tab-local BroadcastChannel network. The
    // public tracker, however, must not learn our `bc://` address (it is
    // meaningless off-machine), so on `internet` the tracker peer is empty.
    const localPeer: PeerInfo = { keyId: req.identity.keyId, addresses: listenAddresses };
    const trackerPeer: PeerInfo = req.scope === 'internet'
        ? { keyId: req.identity.keyId, addresses: [] }
        : localPeer;

    const transports: TransportProvider[] = [];
    const ws = tryBrowserWs('ws', opts.WebSocketCtor);
    const wss = tryBrowserWs('wss', opts.WebSocketCtor);
    if (ws !== undefined) transports.push(ws);
    if (wss !== undefined) transports.push(wss);
    transports.push(bc);

    const layers: DiscoveryLayer[] = [];
    const closeables: MeshCloseable[] = [];
    const notes: string[] = [];

    const trackerProvider = tracker.address.startsWith('wss://') ? wss : ws;
    if (trackerProvider !== undefined) {
        const reachable = await probeTracker(trackerProvider, tracker.address);
        if (reachable) {
            const client = new TrackerClient({
                trackerAddress: tracker.address,
                trackerKeyId: tracker.keyId,
                transportProvider: trackerProvider,
                authenticator,
                localPeer: trackerPeer,
            });
            layers.push({ source: new QuietDiscovery(client), priority: 0 });
            closeables.push(client);
            notes.push(`tracker ${tracker.address}`);
        } else {
            notes.push(`tracker ${tracker.address} unreachable`);
        }
    } else {
        notes.push(`tracker ${tracker.address} unreachable`);
    }

    const backup = new BroadcastChannelDiscovery({
        self: localPeer,
        BroadcastChannelCtor: opts.BroadcastChannelCtor,
    });
    // Same priority as the tracker so DiscoveryStack merges both sources in
    // parallel; a slow or hung tracker query must not delay the backup layer.
    layers.push({ source: backup, priority: 0 });
    closeables.push(backup);
    notes.push(`broadcast-channel ${bc.localAddress}`);

    const discovery = new DiscoveryStack(layers);
    const mesh = new Mesh({
        transports,
        discovery,
        authenticator,
        localKeyId: req.identity.keyId,
        listenAddresses,
    });

    return {
        mesh,
        discovery,
        listenAddresses,
        discoveryNotes: notes,
        closeables,
    };
}

function tryBrowserWs(scheme: string, WebSocketCtor?: WebSocketCtor): BrowserWsTransportProvider | undefined {
    try {
        return new BrowserWsTransportProvider({ scheme, WebSocketCtor });
    } catch {
        return undefined;
    }
}
