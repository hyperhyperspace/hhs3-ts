// Node mesh factory: ws+wss and WebRTC transports, folder discovery, and
// trackers (probed, never spawned). One Mesh per network environment.
// Advertise == listen: the addresses peers dial are the ones the Mesh listens
// on. An omitted section takes the scope default from MeshConfig.

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
    attachPoolReuse,
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
    type NetworkAddress,
    type PeerDiscovery,
    type PeerInfo,
    type TransportProvider,
} from "@hyper-hyper-space/hhs3_mesh";
import { FolderDiscovery, defaultMeshFolderRoot } from "@hyper-hyper-space/hhs3_mesh_folder_discovery";
import { TrackerClient, resolveTrackerConfig } from "@hyper-hyper-space/hhs3_mesh_tracker_client";
import { DEFAULT_INTERNET_SIGNAL } from "@hyper-hyper-space/hhs3_mesh_rtc_node";
import {
    NodeRtcTransportProvider,
    type NodeRtcTransportProviderOptions,
} from "@hyper-hyper-space/hhs3_mesh_rtc_node";
import { WsTransportProvider } from "@hyper-hyper-space/hhs3_mesh_ws";

export type MeshCloseable = { close(): void | Promise<void> };

export type NodeMeshRuntime = {
    identity: OwnIdentity;
    report?: IssueReporter;
};

export type NodeMeshOptions = {
    createRTCPeerConnection?: NodeRtcTransportProviderOptions["createRTCPeerConnection"];
    openSignaling?: NodeRtcTransportProviderOptions["openSignaling"];
};

export type BuiltMesh = {
    mesh: Mesh;
    discovery: PeerDiscovery;
    listenAddresses: NetworkAddress[];
    /** Addresses a tracker is asked to store. On Node this matches `listenAddresses`. */
    trackerAddresses: NetworkAddress[];
    discoveryNotes: string[];
    closeables: MeshCloseable[];
};

export async function createNodeMesh(
    config: MeshConfig,
    runtime: NodeMeshRuntime,
    opts: NodeMeshOptions = {},
): Promise<BuiltMesh> {
    const scoped = requireMeshScope(config);
    planBroadcastChannel(scoped, 'node');

    const listenAddresses: NetworkAddress[] = [];
    const transports: TransportProvider[] = [];
    const wsPlan = planWs(scoped, 'node');
    const ws = new WsTransportProvider('ws');
    const wss = new WsTransportProvider('wss');
    if (wsPlan.dial) transports.push(ws);
    transports.push(wss);
    if (wsPlan.listen === 'loopback') {
        const port = await pickFreePort('127.0.0.1');
        listenAddresses.push(`ws://127.0.0.1:${port}`);
    } else if (wsPlan.listen !== undefined) {
        listenAddresses.push(await concreteListenAddress(wsPlan.listen));
    }

    const webrtc = planWebrtc(scoped, DEFAULT_INTERNET_SIGNAL);
    let rtcAddress: NetworkAddress | undefined;
    if (webrtc.install) {
        const rtc = new NodeRtcTransportProvider({
            identity: runtime.identity,
            ...(webrtc.listen ? { signalBase: webrtc.signal } : {}),
            ...(webrtc.iceServers !== undefined ? { iceServers: webrtc.iceServers } : {}),
            createRTCPeerConnection: opts.createRTCPeerConnection,
            openSignaling: opts.openSignaling,
        });
        transports.push(rtc);
        if (webrtc.listen && rtc.localAddress !== undefined) {
            rtcAddress = rtc.localAddress;
            listenAddresses.push(rtcAddress);
        }
    }

    const authenticator = createAuthenticator({
        localKey: runtime.identity,
        signingName: SIGNING_ED25519,
        kemPrefs: [KEM_X25519_HKDF],
    });

    const localPeer: PeerInfo = { keyId: runtime.identity.keyId, addresses: listenAddresses };
    const sources = planDiscovery(scoped, 'node', false);
    const { layers, closeables, notes } = await buildDiscovery({
        scope: scoped.scope,
        sources,
        localPeer,
        ws,
        wss,
        authenticator,
    });

    if (!wsPlan.dial) closeables.push({ close: () => ws.close() });
    if (scoped.scope === 'internet' && listenAddresses.length === 0) {
        notes.unshift('dial-out only');
    }

    const reuse = attachPoolReuse(new DiscoveryStack(layers));
    const mesh = new Mesh({
        transports,
        discovery: reuse.discovery,
        authenticator,
        localKeyId: runtime.identity.keyId,
        listenAddresses,
        report: runtime.report,
    });
    reuse.attach(mesh.pool);

    return {
        mesh,
        discovery: reuse.discovery,
        listenAddresses,
        trackerAddresses: listenAddresses,
        discoveryNotes: notes,
        closeables,
    };
}

async function buildDiscovery(args: {
    scope: 'localhost' | 'internet';
    sources: DiscoverySource[];
    localPeer: PeerInfo;
    ws: TransportProvider;
    wss: TransportProvider;
    authenticator: ReturnType<typeof createAuthenticator>;
}): Promise<{ layers: DiscoveryLayer[]; closeables: MeshCloseable[]; notes: string[] }> {
    const layers: DiscoveryLayer[] = [];
    const closeables: MeshCloseable[] = [];
    const notes: string[] = [];
    for (const source of args.sources) {
        const priority = source.priority ?? 0;
        if (source.type === 'tracker') {
            const tracker = resolveTrackerConfig(args.scope, {
                tracker: source.url,
                trackerKey: source.key,
            });
            const provider = providerFor(tracker.address, args.ws, args.wss);
            const probe = await probeTracker(provider, tracker.address);
            if (probe.ok) {
                const client = new TrackerClient({
                    trackerAddress: tracker.address,
                    trackerKeyId: tracker.keyId,
                    transportProvider: provider,
                    authenticator: args.authenticator,
                    localPeer: args.localPeer,
                });
                layers.push({ source: new QuietDiscovery(client), priority });
                closeables.push(client);
                notes.push(`tracker ${tracker.address}`);
            } else {
                notes.push(`tracker ${tracker.address} (${probe.reason})`);
            }
        } else if (source.type === 'folder') {
            const folder = new FolderDiscovery({
                root: source.root ?? defaultMeshFolderRoot(),
                self: args.localPeer,
            });
            layers.push({ source: folder, priority });
            closeables.push(folder);
            notes.push(`folder ${folder.root}`);
        }
    }
    return { layers, closeables, notes };
}

async function concreteListenAddress(listen: string): Promise<NetworkAddress> {
    const url = new URL(listen);
    if (url.hostname === '0.0.0.0' || url.hostname === '[::]' || url.hostname === '::') {
        throw new Error(`ws listen must be a dialable address, not bind-all (${listen})`);
    }
    if (url.port === '') {
        url.port = String(await pickFreePort(url.hostname));
    } else {
        const port = Number(url.port);
        if (!Number.isInteger(port) || port <= 0) throw new Error(`invalid ws listen port in ${listen}`);
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
