// Browser WebRTC transport. Signaling uses the platform WebSocket. The peer
// connection is the platform RTCPeerConnection unless a test injects one.

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

export interface BrowserRtcTransportProviderOptions {
    identity: OwnIdentity;
    signalBase?: string;
    endpointId?: string;
    iceServers?: RtcIceServer[];
    connectTimeoutMs?: number;
    delegationTtlSec?: number;
    now?: () => number;
    WebSocketCtor?: new (url: string) => WebSocket;
    RTCPeerConnectionCtor?: typeof RTCPeerConnection;
    createRTCPeerConnection?: RtcTransportProviderOptions['createRTCPeerConnection'];
    openSignaling?: RtcTransportProviderOptions['openSignaling'];
}

export class BrowserRtcTransportProvider extends RtcTransportProvider {
    constructor(opts: BrowserRtcTransportProviderOptions) {
        const WS = opts.WebSocketCtor
            ?? (globalThis as { WebSocket?: new (url: string) => WebSocket }).WebSocket;
        const PC = opts.RTCPeerConnectionCtor
            ?? (globalThis as { RTCPeerConnection?: typeof RTCPeerConnection }).RTCPeerConnection;
        const createRTCPeerConnection = opts.createRTCPeerConnection ?? (ice => {
            if (PC === undefined) throw new Error('no RTCPeerConnection implementation available');
            return adaptPeer(new PC({ iceServers: ice as RTCIceServer[] }));
        });
        const openSignaling = opts.openSignaling ?? (url => {
            if (WS === undefined) return Promise.reject(new Error('no WebSocket implementation available'));
            return openPlatformSocket(url, WS);
        });
        if (opts.createRTCPeerConnection === undefined && PC === undefined) {
            throw new Error('no RTCPeerConnection implementation available');
        }
        super({ ...opts, createRTCPeerConnection, openSignaling });
    }
}

function openPlatformSocket(url: string, WS: new (url: string) => WebSocket): Promise<SignalSocket> {
    const ws = new WS(url);
    const sock = wrapPlatformSocket(ws);
    return new Promise((resolve, reject) => {
        let settled = false;
        let timer: ReturnType<typeof setTimeout> | undefined;
        const finish = (err?: Error) => {
            if (settled) return;
            settled = true;
            if (timer !== undefined) clearTimeout(timer);
            if (err !== undefined) reject(err);
            else resolve(sock);
        };
        timer = setTimeout(() => {
            try { ws.close(); } catch { /* ignore */ }
            finish(new Error('signaling socket timeout'));
        }, 10_000);
        ws.addEventListener('open', () => finish());
        ws.addEventListener('error', () => finish(new Error('signaling socket failed')));
        ws.addEventListener('close', () => finish(new Error('signaling socket closed')));
    });
}

function wrapPlatformSocket(ws: WebSocket): SignalSocket {
    const messageCallbacks: ((text: string) => void)[] = [];
    const closeCallbacks: (() => void)[] = [];
    const pending: string[] = [];
    let closed = false;
    ws.addEventListener('message', ev => {
        const data = ev.data;
        const text = typeof data === 'string' ? data : '';
        if (messageCallbacks.length === 0) pending.push(text);
        else for (const cb of messageCallbacks) cb(text);
    });
    ws.addEventListener('close', () => {
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

function adaptPeer(pc: RTCPeerConnection): RtcPeerLike {
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
            await pc.setLocalDescription(desc);
        },
        async setRemoteDescription(desc: RtcDescription) {
            await pc.setRemoteDescription(desc);
        },
        async addIceCandidate(candidate: RtcCandidateInit) {
            if (!candidate.candidate) return;
            await pc.addIceCandidate(candidate);
        },
        close() { pc.close(); },
        onicecandidate: null,
        ondatachannel: null,
        onconnectionstatechange: null,
    };
    pc.onicecandidate = ev => {
        if (ev.candidate === null) peer.onicecandidate?.(null);
        else peer.onicecandidate?.({
            candidate: ev.candidate.candidate,
            sdpMid: ev.candidate.sdpMid,
            sdpMLineIndex: ev.candidate.sdpMLineIndex,
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

function adaptChannel(channel: RTCDataChannel): RtcDataChannelLike {
    channel.binaryType = 'arraybuffer';
    const wrapped: RtcDataChannelLike = {
        get readyState() { return channel.readyState; },
        get binaryType() { return channel.binaryType; },
        set binaryType(value: string) { channel.binaryType = value as BinaryType; },
        send(data: Uint8Array) { channel.send(copyBytes(data)); },
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
        wrapped.onmessage?.(toBytes(ev.data));
    };
    return wrapped;
}

function copyBytes(data: Uint8Array): ArrayBuffer {
    const copy = new Uint8Array(data.byteLength);
    copy.set(data);
    return copy.buffer;
}

function toBytes(data: unknown): Uint8Array {
    if (data instanceof ArrayBuffer) return new Uint8Array(data);
    if (ArrayBuffer.isView(data)) {
        return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
    }
    return new Uint8Array();
}
