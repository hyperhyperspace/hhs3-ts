// Browser SyncMeshFactory: dial-only ws/wss + BroadcastChannel transport,
// BroadcastChannel discovery backup, optional tracker (probed, never spawned).
// listenAddresses are bc:// only.

import {
    KEM_X25519_HKDF,
    SIGNING_ED25519,
} from "@hyper-hyper-space/hhs3_crypto";
import {
    DiscoveryStack,
    Mesh,
    createAuthenticator,
    type DiscoveryLayer,
    type PeerInfo,
    type TransportProvider,
} from "@hyper-hyper-space/hhs3_mesh";
import {
    BroadcastChannelDiscovery,
    BroadcastChannelTransportProvider,
    type BroadcastChannelCtor,
} from "@hyper-hyper-space/hhs3_mesh_bc";
import { TrackerClient } from "@hyper-hyper-space/hhs3_mesh_tracker_client";
import {
    BrowserWsTransportProvider,
    type WebSocketCtor,
} from "@hyper-hyper-space/hhs3_mesh_ws_browser";
import {
    QuietDiscovery,
    RewriteAnnounceDiscovery,
    probeTracker,
    resolveTrackerConfig,
    type BuiltSyncMesh,
    type SyncMeshFactory,
} from "@hyper-hyper-space/hhs3_rdb_repl";

export type BrowserSyncMeshFactoryOptions = {
    BroadcastChannelCtor?: BroadcastChannelCtor;
    WebSocketCtor?: WebSocketCtor;
};

export function createBrowserSyncMeshFactory(opts: BrowserSyncMeshFactoryOptions = {}): SyncMeshFactory {
    return async (req): Promise<BuiltSyncMesh> => {
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
        const announced = [bc.localAddress];
        const localPeer: PeerInfo = { keyId: req.identity.keyId, addresses: announced };

        const transports: TransportProvider[] = [];
        const ws = tryBrowserWs('ws', opts.WebSocketCtor);
        const wss = tryBrowserWs('wss', opts.WebSocketCtor);
        if (ws !== undefined) transports.push(ws);
        if (wss !== undefined) transports.push(wss);
        transports.push(bc);

        const layers: DiscoveryLayer[] = [];
        const closeables: BuiltSyncMesh['closeables'] = [];
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
                    localPeer,
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
        layers.push({ source: backup, priority: 1 });
        closeables.push(backup);
        notes.push(`broadcast-channel ${bc.localAddress}`);

        const discovery = new RewriteAnnounceDiscovery(new DiscoveryStack(layers), announced);
        const mesh = new Mesh({
            transports,
            discovery,
            authenticator,
            localKeyId: req.identity.keyId,
            listenAddresses: announced,
        });

        return {
            mesh,
            discovery,
            listenAddresses: announced,
            announcedAddresses: announced,
            discoveryNotes: notes,
            closeables,
        };
    };
}

function tryBrowserWs(scheme: string, WebSocketCtor?: WebSocketCtor): BrowserWsTransportProvider | undefined {
    try {
        return new BrowserWsTransportProvider({ scheme, WebSocketCtor });
    } catch {
        return undefined;
    }
}
