// Browser mesh factory: dial-only ws/wss, a WebRTC transport, BroadcastChannel
// for tab-to-tab sync, and an optional tracker (probed, never spawned).
// Listen addresses are `bc://` plus `rtc://` when a signaling server is
// configured. On `internet` the tracker is given only the `rtc://` address
// (or none), never the tab-local `bc://` address.

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
import {
    BroadcastChannelDiscovery,
    BroadcastChannelTransportProvider,
    type BroadcastChannelCtor,
} from "@hyper-hyper-space/hhs3_mesh_bc";
import { TrackerClient, resolveTrackerConfig } from "@hyper-hyper-space/hhs3_mesh_tracker_client";
import {
    BrowserRtcTransportProvider,
    type BrowserRtcTransportProviderOptions,
} from "@hyper-hyper-space/hhs3_mesh_rtc_browser";
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
    /** Public wss URL of a signaling server, without an endpoint id. */
    signalUrl?: string;
    report?: IssueReporter;
};

export type BrowserMeshOptions = {
    BroadcastChannelCtor?: BroadcastChannelCtor;
    WebSocketCtor?: WebSocketCtor;
    createPeer?: BrowserRtcTransportProviderOptions["createPeer"];
    openSignaling?: BrowserRtcTransportProviderOptions["openSignaling"];
};

export type BuiltMesh = {
    mesh: Mesh;
    discovery: PeerDiscovery;
    listenAddresses: NetworkAddress[];
    /** Addresses the tracker is asked to store. On `internet` this omits `bc://`. */
    trackerAddresses: NetworkAddress[];
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
    const rtc = tryBrowserRtc(req, opts);
    const rtcAddress = req.signalUrl !== undefined ? rtc?.localAddress : undefined;
    if (rtcAddress !== undefined) listenAddresses.push(rtcAddress);

    // Advertise == listen for the tab-local BroadcastChannel network. The
    // public tracker must not learn `bc://`. It learns `rtc://` when we have
    // registered with a signaling server.
    const localPeer: PeerInfo = { keyId: req.identity.keyId, addresses: listenAddresses };
    const trackerAddresses: NetworkAddress[] = req.scope === 'internet'
        ? (rtcAddress !== undefined ? [rtcAddress] : [])
        : listenAddresses;
    const trackerPeer: PeerInfo = { keyId: req.identity.keyId, addresses: trackerAddresses };

    const transports: TransportProvider[] = [];
    const ws = tryBrowserWs('ws', opts.WebSocketCtor);
    const wss = tryBrowserWs('wss', opts.WebSocketCtor);
    if (ws !== undefined) transports.push(ws);
    if (wss !== undefined) transports.push(wss);
    if (rtc !== undefined) transports.push(rtc);
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
    if (rtcAddress !== undefined) notes.push(`rtc ${rtcAddress}`);

    const discovery = new DiscoveryStack(layers);
    const mesh = new Mesh({
        transports,
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
        trackerAddresses,
        discoveryNotes: notes,
        closeables,
    };
}

function tryBrowserRtc(
    req: BrowserMeshRequest,
    opts: BrowserMeshOptions,
): BrowserRtcTransportProvider | undefined {
    try {
        return new BrowserRtcTransportProvider({
            identity: req.identity,
            signalBase: req.signalUrl,
            createPeer: opts.createPeer,
            openSignaling: opts.openSignaling,
        });
    } catch (err) {
        if (req.signalUrl !== undefined) throw err;
        return undefined;
    }
}

function tryBrowserWs(scheme: string, WebSocketCtor?: WebSocketCtor): BrowserWsTransportProvider | undefined {
    try {
        return new BrowserWsTransportProvider({ scheme, WebSocketCtor });
    } catch {
        return undefined;
    }
}
