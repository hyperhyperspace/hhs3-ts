// Browser mesh factory: dial-only ws/wss, a WebRTC transport, BroadcastChannel
// for tab-to-tab sync, and trackers (probed, never spawned). Listen addresses
// are `bc://` when that transport is on, plus `rtc://` when WebRTC listens.
// On `internet` a tracker is given the public addresses only, never `bc://`.

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
    planBroadcastChannel,
    planDiscovery,
    planWebrtc,
    planWs,
    probeTracker,
    requireMeshScope,
    type DiscoveryLayer,
    type DiscoverySource,
    type IssueReporter,
    type MeshConfig,
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
import { DEFAULT_INTERNET_SIGNAL } from "@hyper-hyper-space/hhs3_mesh_rtc_browser";
import {
    BrowserRtcTransportProvider,
    type BrowserRtcTransportProviderOptions,
} from "@hyper-hyper-space/hhs3_mesh_rtc_browser";
import {
    BrowserWsTransportProvider,
    type WebSocketCtor,
} from "@hyper-hyper-space/hhs3_mesh_ws_browser";

export type MeshCloseable = { close(): void | Promise<void> };

export type BrowserMeshRuntime = {
    identity: OwnIdentity;
    report?: IssueReporter;
};

export type BrowserMeshOptions = {
    BroadcastChannelCtor?: BroadcastChannelCtor;
    WebSocketCtor?: WebSocketCtor;
    createRTCPeerConnection?: BrowserRtcTransportProviderOptions["createRTCPeerConnection"];
    openSignaling?: BrowserRtcTransportProviderOptions["openSignaling"];
};

export type BuiltMesh = {
    mesh: Mesh;
    discovery: PeerDiscovery;
    listenAddresses: NetworkAddress[];
    /** Addresses a tracker is asked to store. On `internet` this omits `bc://`. */
    trackerAddresses: NetworkAddress[];
    discoveryNotes: string[];
    closeables: MeshCloseable[];
};

export async function createBrowserMesh(
    config: MeshConfig,
    runtime: BrowserMeshRuntime,
    opts: BrowserMeshOptions = {},
): Promise<BuiltMesh> {
    const scoped = requireMeshScope(config);
    const channel = planBroadcastChannel(scoped, 'browser');
    const wsPlan = planWs(scoped, 'browser');
    const webrtc = planWebrtc(scoped, DEFAULT_INTERNET_SIGNAL);

    const authenticator = createAuthenticator({
        localKey: runtime.identity,
        signingName: SIGNING_ED25519,
        kemPrefs: [KEM_X25519_HKDF],
    });

    const listenAddresses: NetworkAddress[] = [];
    const transports: TransportProvider[] = [];
    let bc: BroadcastChannelTransportProvider | undefined;
    if (channel.install) {
        bc = new BroadcastChannelTransportProvider({
            ...(channel.base !== undefined ? { base: channel.base } : {}),
            BroadcastChannelCtor: opts.BroadcastChannelCtor,
        });
        listenAddresses.push(bc.localAddress);
        transports.push(bc);
    }

    const ws = tryBrowserWs('ws', opts.WebSocketCtor);
    const wss = tryBrowserWs('wss', opts.WebSocketCtor);
    if (wsPlan.dial && ws !== undefined) transports.push(ws);
    if (wss !== undefined) transports.push(wss);

    let rtcAddress: NetworkAddress | undefined;
    if (webrtc.install) {
        const rtc = tryBrowserRtc(webrtc, runtime.identity, opts);
        if (rtc !== undefined) {
            transports.push(rtc);
            if (webrtc.listen && rtc.localAddress !== undefined) {
                rtcAddress = rtc.localAddress;
                listenAddresses.push(rtcAddress);
            }
        }
    }

    const localPeer: PeerInfo = { keyId: runtime.identity.keyId, addresses: listenAddresses };
    const trackerAddresses = publicAddresses(scoped.scope, listenAddresses);
    const trackerPeer: PeerInfo = { keyId: runtime.identity.keyId, addresses: trackerAddresses };
    const sources = planDiscovery(scoped, 'browser', channel.install);

    const layers: DiscoveryLayer[] = [];
    const closeables: MeshCloseable[] = [];
    const notes: string[] = [];
    for (const source of sources) {
        const priority = source.priority ?? 0;
        if (source.type === 'tracker') {
            const tracker = resolveTrackerConfig(scoped.scope, {
                tracker: source.url,
                trackerKey: source.key,
            });
            const provider = tracker.address.startsWith('wss://') ? wss : ws;
            if (provider === undefined) {
                notes.push(`tracker ${tracker.address} unreachable`);
                continue;
            }
            const reachable = await probeTracker(provider, tracker.address);
            if (!reachable) {
                notes.push(`tracker ${tracker.address} unreachable`);
                continue;
            }
            const client = new TrackerClient({
                trackerAddress: tracker.address,
                trackerKeyId: tracker.keyId,
                transportProvider: provider,
                authenticator,
                localPeer: trackerPeer,
            });
            layers.push({ source: new QuietDiscovery(client), priority });
            closeables.push(client);
            notes.push(`tracker ${tracker.address}`);
        } else if (source.type === 'broadcast-channel' && bc !== undefined) {
            const backup = new BroadcastChannelDiscovery({
                self: localPeer,
                ...(channel.install && channel.base !== undefined ? { base: channel.base } : {}),
                BroadcastChannelCtor: opts.BroadcastChannelCtor,
            });
            layers.push({ source: backup, priority });
            closeables.push(backup);
            notes.push(`broadcast-channel ${bc.localAddress}`);
        }
    }
    if (rtcAddress !== undefined) notes.push(`rtc ${rtcAddress}`);
    const acceptingWebRtc = webrtc.install && webrtc.listen;
    if (scoped.scope === 'internet' && !acceptingWebRtc && wsPlan.listen === undefined) {
        notes.unshift('dial-out only');
    }

    const discovery = new DiscoveryStack(layers);
    const mesh = new Mesh({
        transports,
        discovery,
        authenticator,
        localKeyId: runtime.identity.keyId,
        listenAddresses,
        report: runtime.report,
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

function publicAddresses(scope: MeshScope, addresses: NetworkAddress[]): NetworkAddress[] {
    if (scope !== 'internet') return addresses;
    return addresses.filter(address => !address.startsWith('bc://'));
}

function tryBrowserRtc(
    webrtc: { listen: boolean; signal?: string; iceServers?: BrowserRtcTransportProviderOptions['iceServers'] },
    identity: OwnIdentity,
    opts: BrowserMeshOptions,
): BrowserRtcTransportProvider | undefined {
    try {
        return new BrowserRtcTransportProvider({
            identity,
            ...(webrtc.listen && webrtc.signal !== undefined ? { signalBase: webrtc.signal } : {}),
            ...(webrtc.iceServers !== undefined ? { iceServers: webrtc.iceServers } : {}),
            createRTCPeerConnection: opts.createRTCPeerConnection,
            openSignaling: opts.openSignaling,
        });
    } catch (err) {
        if (webrtc.listen) throw err;
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
