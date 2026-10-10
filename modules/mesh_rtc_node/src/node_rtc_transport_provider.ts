// Node WebRTC transport. The peer connection is werift (pure TypeScript).
// Signaling uses the ws package. A later library swap stays inside this file.

import WebSocket from 'ws';
import { RTCPeerConnection, type PeerConfig, type RTCIceServer as WeriftIceServer } from 'werift';
import type { OwnIdentity } from '@hyper-hyper-space/hhs3_crypto';
import {
    RtcTransportProvider,
    type RtcCandidateInit,
    type RtcDataChannelLike,
    type RtcDescription,
    type RtcIceServer,
    type RtcPeerLike,
    type RtcTransportProviderOptions,
    type SignalSocket,
} from '@hyper-hyper-space/hhs3_mesh_rtc';

export interface NodeRtcTransportProviderOptions {
    identity: OwnIdentity;
    signalBase?: string;
    endpointId?: string;
    iceServers?: RtcIceServer[];
    connectTimeoutMs?: number;
    delegationTtlSec?: number;
    now?: () => number;
    createPeer?: RtcTransportProviderOptions['createPeer'];
    openSignaling?: RtcTransportProviderOptions['openSignaling'];
    /** Extra werift peer options. Used when the default peer connection is created. */
    werift?: Partial<PeerConfig>;
}

export class NodeRtcTransportProvider extends RtcTransportProvider {
    constructor(opts: NodeRtcTransportProviderOptions) {
        super({
            ...opts,
            createPeer: opts.createPeer ?? (ice => adaptWerift(ice, opts.werift)),
            openSignaling: opts.openSignaling ?? openNodeSignaling,
        });
    }
}

export function openNodeSignaling(url: string): Promise<SignalSocket> {
    const ws = new WebSocket(url);
    const sock = wrapNodeSocket(ws);
    return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
            try { ws.close(); } catch { /* ignore */ }
            reject(new Error('signaling socket timeout'));
        }, 10_000);
        ws.once('open', () => {
            clearTimeout(timer);
            resolve(sock);
        });
        ws.once('error', () => {
            clearTimeout(timer);
            reject(new Error(`signaling socket failed: ${url}`));
        });
    });
}

function wrapNodeSocket(ws: WebSocket): SignalSocket {
    const messageCallbacks: ((text: string) => void)[] = [];
    const closeCallbacks: (() => void)[] = [];
    const pending: string[] = [];
    let closed = false;
    ws.on('message', data => {
        const text = typeof data === 'string' ? data : data.toString('utf8');
        if (messageCallbacks.length === 0) pending.push(text);
        else for (const cb of messageCallbacks) cb(text);
    });
    ws.on('close', () => {
        closed = true;
        for (const cb of closeCallbacks) cb();
    });
    return {
        send(text: string) { ws.send(text); },
        close() { try { ws.close(); } catch { /* ignore */ } },
        onMessage(cb) {
            messageCallbacks.push(cb);
            if (pending.length === 0) return;
            const batch = pending.splice(0, pending.length);
            for (const text of batch) cb(text);
        },
        onClose(cb) {
            closeCallbacks.push(cb);
            if (closed) cb();
        },
    };
}

function adaptWerift(iceServers: RtcIceServer[], extra?: Partial<PeerConfig>): RtcPeerLike {
    const pc = new RTCPeerConnection({
        ...extra,
        iceServers: iceServers.map(toWeriftIce),
    });
    const peer: RtcPeerLike = {
        get connectionState() { return pc.connectionState; },
        createDataChannel(label: string) {
            return adaptChannel(pc.createDataChannel(label, { ordered: true }));
        },
        async createOffer(): Promise<RtcDescription> {
            const desc = await pc.createOffer();
            return { type: 'offer', sdp: desc.sdp };
        },
        async createAnswer(): Promise<RtcDescription> {
            const desc = await pc.createAnswer();
            return { type: 'answer', sdp: desc.sdp };
        },
        async setLocalDescription(desc: RtcDescription) {
            await pc.setLocalDescription({ type: desc.type, sdp: desc.sdp ?? '' });
        },
        async setRemoteDescription(desc: RtcDescription) {
            await pc.setRemoteDescription({ type: desc.type, sdp: desc.sdp ?? '' });
        },
        async addIceCandidate(candidate: RtcCandidateInit) {
            if (!candidate.candidate) return;
            await pc.addIceCandidate({
                candidate: candidate.candidate,
                sdpMid: candidate.sdpMid ?? undefined,
                sdpMLineIndex: candidate.sdpMLineIndex ?? undefined,
            });
        },
        close() { void pc.close(); },
        onicecandidate: null,
        ondatachannel: null,
        onconnectionstatechange: null,
    };
    pc.onicecandidate = ev => {
        const c = ev.candidate;
        if (c === undefined) peer.onicecandidate?.(null);
        else peer.onicecandidate?.({
            candidate: c.candidate,
            sdpMid: c.sdpMid,
            sdpMLineIndex: c.sdpMLineIndex,
        });
    };
    pc.ondatachannel = ev => {
        peer.ondatachannel?.(adaptChannel(ev.channel));
    };
    pc.onconnectionstatechange = () => {
        peer.onconnectionstatechange?.();
    };
    return peer;
}

function toWeriftIce(server: RtcIceServer): WeriftIceServer {
    const urls = Array.isArray(server.urls) ? server.urls[0] ?? '' : server.urls;
    return { urls, username: server.username, credential: server.credential };
}

function adaptChannel(channel: ReturnType<RTCPeerConnection['createDataChannel']>): RtcDataChannelLike {
    const wrapped: RtcDataChannelLike = {
        get readyState() { return channel.readyState; },
        binaryType: 'arraybuffer',
        send(data: Uint8Array) { channel.send(Buffer.from(data)); },
        close() { channel.close(); },
        onopen: null,
        onclose: null,
        onerror: null,
        onmessage: null,
    };
    channel.onopen = () => wrapped.onopen?.();
    channel.onclose = () => wrapped.onclose?.();
    channel.onerror = () => wrapped.onerror?.();
    channel.onmessage = ev => {
        const data = ev.data;
        if (typeof data === 'string') wrapped.onmessage?.(Buffer.from(data));
        else wrapped.onmessage?.(new Uint8Array(data.buffer, data.byteOffset, data.byteLength));
    };
    return wrapped;
}
