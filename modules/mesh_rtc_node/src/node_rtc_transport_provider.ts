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
    createRTCPeerConnection?: RtcTransportProviderOptions['createRTCPeerConnection'];
    openSignaling?: RtcTransportProviderOptions['openSignaling'];
    /** Extra werift peer options. Used when the default peer connection is created. */
    werift?: Partial<PeerConfig>;
}

export class NodeRtcTransportProvider extends RtcTransportProvider {
    private readonly settling: Promise<void>[];

    constructor(opts: NodeRtcTransportProviderOptions) {
        const settling: Promise<void>[] = [];
        super({
            ...opts,
            createRTCPeerConnection: opts.createRTCPeerConnection ?? (ice => adaptWerift(ice, opts.werift, (job) => {
                settling.push(job);
            })),
            openSignaling: opts.openSignaling ?? openNodeSignaling,
        });
        this.settling = settling;
    }

    // Peer closes flush the CLOSE frame and the SCTP abort before the ICE
    // socket goes away. Mesh teardown awaits this so those packets leave the
    // process.
    flushClose(): Promise<void> {
        const pending = this.settling.splice(0);
        return Promise.allSettled(pending).then(() => {});
    }
}

export function openNodeSignaling(url: string): Promise<SignalSocket> {
    const ws = new WebSocket(url);
    const sock = wrapNodeSocket(ws);
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
        ws.once('open', () => finish());
        ws.once('error', () => finish(new Error(`signaling socket failed: ${url}`)));
        ws.once('close', () => finish(new Error('signaling socket closed')));
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

const SCTP_FLUSH_BUDGET_MS = 500;

interface FlushableChannel {
    readyState: string;
    bufferedAmount: number;
}

interface WeriftClosing {
    readonly sctpManager: { close(): Promise<void> };
    close(): Promise<void>;
}

function adaptWerift(
    iceServers: RtcIceServer[],
    extra?: Partial<PeerConfig>,
    onClosing?: (job: Promise<void>) => void,
): RtcPeerLike {
    const pc = new RTCPeerConnection({
        ...extra,
        iceServers: iceServers.map(toWeriftIce),
    });
    const channels: FlushableChannel[] = [];
    let closed = false;
    const peer: RtcPeerLike = {
        get connectionState() { return pc.connectionState; },
        createDataChannel(label: string) {
            const native = pc.createDataChannel(label, { ordered: true });
            channels.push(native);
            return adaptChannel(native);
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
        close() {
            if (closed) return;
            closed = true;
            const job = shutdownWerift(pc, channels);
            onClosing?.(job);
            void job.catch(() => {});
        },
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
        channels.push(ev.channel);
        peer.ondatachannel?.(adaptChannel(ev.channel));
    };
    pc.onconnectionstatechange = () => {
        peer.onconnectionstatechange?.();
    };
    return peer;
}

// werift's RTCPeerConnection.close() drops the ICE socket before SCTP abort,
// so the remote peer sees nothing. Flush queued data-channel bytes (the CLOSE
// frame), abort SCTP while UDP is still up, then close the peer connection.
async function shutdownWerift(pc: RTCPeerConnection, channels: FlushableChannel[]): Promise<void> {
    const peer = pc as unknown as WeriftClosing;
    const live = channels.some((channel) => channel.readyState === 'open' || channel.readyState === 'closing');
    if (live) {
        const deadline = Date.now() + SCTP_FLUSH_BUDGET_MS;
        while (channels.some((channel) => channel.bufferedAmount > 0) && Date.now() < deadline) {
            await new Promise((resolve) => setImmediate(resolve));
        }
        try { await peer.sctpManager.close(); } catch { /* best-effort */ }
    }
    try { await peer.close(); } catch { /* best-effort */ }
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
