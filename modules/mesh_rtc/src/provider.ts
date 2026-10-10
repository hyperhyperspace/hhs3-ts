// WebRTC mesh session. Signaling runs until the data channel opens; the mesh
// then sees an ordinary Transport. connect() rejects once, whether signaling
// or ICE used up the budget.

import { random } from '@hyper-hyper-space/hhs3_crypto';
import type { KeyId, OwnIdentity } from '@hyper-hyper-space/hhs3_crypto';
import type { NetworkAddress, Transport, TransportProvider } from '@hyper-hyper-space/hhs3_mesh';
import { parseRtcAddress, rtcAddressFor } from './address.js';
import { RtcByteTransport } from './byte_transport.js';
import { delegationToWire, signDelegation, signPossession, verifyDelegation } from './codec.js';
import {
    DATA_CHANNEL_LABEL,
    DEFAULT_ICE_SERVERS,
    type RtcCandidateInit,
    type RtcIceServer,
    type RtcPeerLike,
    type SignalSocket,
} from './peer.js';
import {
    encodeSignalMessage,
    parseSignalMessage,
    type SignalData,
    type SignalMessage,
} from './protocol.js';

export const DEFAULT_CONNECT_TIMEOUT_MS = 20_000;
export const DEFAULT_DELEGATION_TTL_SEC = 600;
export const DEFAULT_CLOCK_SKEW_MS = 60_000;

export function randomId(size = 16): string {
    let binary = '';
    const bytes = random.getBytes(size);
    for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]!);
    return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '');
}

export interface RtcTransportProviderOptions {
    identity: OwnIdentity;
    /** Public wss URL of the signaling server, without an endpoint id. Required to listen. */
    signalBase?: string;
    endpointId?: string;
    iceServers?: RtcIceServer[];
    connectTimeoutMs?: number;
    delegationTtlSec?: number;
    clockSkewMs?: number;
    now?: () => number;
    createPeer: (iceServers: RtcIceServer[]) => RtcPeerLike;
    openSignaling: (signalingUrl: string) => Promise<SignalSocket>;
}

interface Outbound {
    abort: (err: Error) => void;
}

interface InboundSession {
    peer: RtcPeerLike;
    pending: RtcCandidateInit[];
    remoteSet: boolean;
}

export class RtcTransportProvider implements TransportProvider {
    readonly scheme = 'rtc';
    readonly endpointId: string;
    readonly localAddress?: NetworkAddress;

    private readonly identity: OwnIdentity;
    private readonly iceServers: RtcIceServer[];
    private readonly connectTimeoutMs: number;
    private readonly delegationTtlSec: number;
    private readonly clockSkewMs: number;
    private readonly now: () => number;
    private readonly createPeer: (iceServers: RtcIceServer[]) => RtcPeerLike;
    private readonly openSignaling: (signalingUrl: string) => Promise<SignalSocket>;
    private readonly signalingUrl?: string;
    private readonly signalingOrigin?: string;

    private onConnection?: (transport: Transport) => void;
    private listenerSocket?: SignalSocket;
    private listening = false;
    private closed = false;
    private registered = false;
    private registeredWait?: Promise<void>;
    private resolveRegistered?: () => void;
    private rejectRegistered?: (err: Error) => void;
    private listenerLoop?: Promise<void>;
    private chain: Promise<void> = Promise.resolve();
    private readonly outbounds = new Map<string, Outbound>();
    private readonly sessions = new Map<string, InboundSession>();

    constructor(opts: RtcTransportProviderOptions) {
        this.identity = opts.identity;
        this.endpointId = opts.endpointId ?? randomId();
        this.iceServers = opts.iceServers ?? DEFAULT_ICE_SERVERS;
        this.connectTimeoutMs = opts.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS;
        this.delegationTtlSec = opts.delegationTtlSec ?? DEFAULT_DELEGATION_TTL_SEC;
        this.clockSkewMs = opts.clockSkewMs ?? DEFAULT_CLOCK_SKEW_MS;
        this.now = opts.now ?? (() => Date.now());
        this.createPeer = opts.createPeer;
        this.openSignaling = opts.openSignaling;
        if (opts.signalBase !== undefined) {
            this.localAddress = rtcAddressFor(opts.signalBase, this.endpointId);
            const parsed = parseRtcAddress(this.localAddress);
            this.signalingUrl = parsed.signalingUrl;
            this.signalingOrigin = parsed.signalingOrigin;
        }
    }

    async listen(address: NetworkAddress, onConnection: (transport: Transport) => void): Promise<void> {
        if (this.localAddress === undefined || this.signalingUrl === undefined) {
            throw new Error('rtc listen requires a signaling server URL');
        }
        const parsed = parseRtcAddress(address);
        if (parsed.endpointId !== this.endpointId) {
            throw new Error(`rtc listen address does not match this endpoint: ${address}`);
        }
        this.onConnection = onConnection;
        this.listening = true;
        if (this.listenerLoop === undefined) this.listenerLoop = this.runListener();
        let timer: ReturnType<typeof setTimeout> | undefined;
        try {
            await Promise.race([
                this.whenRegistered(),
                new Promise<void>((_resolve, reject) => {
                    timer = setTimeout(() => reject(new Error('rtc listen timeout: signaling')), this.connectTimeoutMs);
                }),
            ]);
        } catch (err) {
            this.listening = false;
            try { this.listenerSocket?.close(); } catch { /* ignore */ }
            throw err;
        } finally {
            if (timer !== undefined) clearTimeout(timer);
        }
    }

    async connect(remote: NetworkAddress, local?: NetworkAddress, expectedKeyId?: KeyId): Promise<Transport> {
        if (expectedKeyId === undefined) throw new Error('rtc connect requires expected key id');
        const parsed = parseRtcAddress(remote);
        if (parsed.endpointId === this.endpointId) throw new Error('rtc connect rejected: self');
        const from = local ?? this.localAddress;
        const session = randomId();

        return new Promise<Transport>((resolve, reject) => {
            let stage: 'signaling' | 'ice' = 'signaling';
            let socket: SignalSocket | undefined;
            let peer: RtcPeerLike | undefined;
            let settled = false;
            let remoteSet = false;
            const pending: RtcCandidateInit[] = [];

            const fail = (err: Error) => {
                if (settled) return;
                settled = true;
                clearTimeout(timer);
                this.outbounds.delete(parsed.endpointId);
                try { peer?.close(); } catch { /* ignore */ }
                try { socket?.close(); } catch { /* ignore */ }
                reject(err);
            };
            const succeed = (transport: Transport) => {
                if (settled) {
                    transport.close();
                    return;
                }
                settled = true;
                clearTimeout(timer);
                this.outbounds.delete(parsed.endpointId);
                try { socket?.send(encodeSignalMessage({ type: 'bye', session })); } catch { /* ignore */ }
                try { socket?.close(); } catch { /* ignore */ }
                resolve(transport);
            };
            const timer = setTimeout(() => {
                fail(new Error(`rtc connect timeout: ${stage}`));
            }, this.connectTimeoutMs);
            const send = (msg: SignalMessage) => {
                if (settled || socket === undefined) return;
                socket.send(encodeSignalMessage(msg));
            };
            const flush = async () => {
                while (pending.length > 0 && peer !== undefined) {
                    const candidate = pending.shift()!;
                    if (candidate.candidate) await peer.addIceCandidate(candidate);
                }
            };

            this.outbounds.set(parsed.endpointId, { abort: fail });

            const onText = async (text: string) => {
                const msg = parseSignalMessage(text);
                if (msg.type === 'hello') {
                    send({ type: 'dial', session, from });
                    return;
                }
                if (msg.type === 'reject') throw new Error(`rtc connect rejected: ${msg.reason}`);
                if (msg.type === 'warrant') {
                    if (msg.session !== session) return;
                    await verifyDelegation(msg.delegation, {
                        origin: parsed.signalingOrigin,
                        nowMs: this.now(),
                        skewMs: this.clockSkewMs,
                        expectedKeyId,
                    });
                    stage = 'ice';
                    peer = this.createPeer(this.iceServers);
                    peer.onicecandidate = (candidate) => {
                        if (settled || !candidate?.candidate) return;
                        send({
                            type: 'signal',
                            session,
                            data: {
                                kind: 'candidate',
                                candidate: candidate.candidate,
                                sdpMid: candidate.sdpMid,
                                sdpMLineIndex: candidate.sdpMLineIndex,
                            },
                        });
                    };
                    const channel = peer.createDataChannel(DATA_CHANNEL_LABEL);
                    const transport = new RtcByteTransport(channel, this.localAddress, parsed.address, () => {
                        try { peer?.close(); } catch { /* ignore */ }
                    });
                    void transport.opened.then(() => succeed(transport));
                    const offer = await peer.createOffer();
                    await peer.setLocalDescription(offer);
                    send({ type: 'signal', session, data: { kind: 'offer', sdp: offer.sdp ?? '' } });
                    return;
                }
                if (msg.type === 'signal' && msg.session === session && peer !== undefined) {
                    if (msg.data.kind === 'answer') {
                        await peer.setRemoteDescription({ type: 'answer', sdp: msg.data.sdp });
                        remoteSet = true;
                        await flush();
                    } else if (msg.data.kind === 'candidate') {
                        if (!remoteSet) pending.push(msg.data);
                        else if (msg.data.candidate) await peer.addIceCandidate(msg.data);
                    }
                }
            };

            this.openSignaling(parsed.signalingUrl).then(sock => {
                if (settled) {
                    sock.close();
                    return;
                }
                socket = sock;
                sock.onClose(() => fail(new Error('rtc connect rejected: signaling closed')));
                sock.onMessage(text => {
                    if (settled) return;
                    onText(text).catch(err => fail(err instanceof Error ? err : new Error(String(err))));
                });
            }).catch(err => fail(err instanceof Error ? err : new Error(String(err))));
        });
    }

    close(): void {
        this.closed = true;
        this.listening = false;
        try { this.listenerSocket?.close(); } catch { /* ignore */ }
        this.listenerSocket = undefined;
        for (const session of this.sessions.values()) {
            try { session.peer.close(); } catch { /* ignore */ }
        }
        this.sessions.clear();
        for (const outbound of this.outbounds.values()) {
            outbound.abort(new Error('rtc transport closed'));
        }
        this.outbounds.clear();
    }

    private whenRegistered(): Promise<void> {
        if (this.registered) return Promise.resolve();
        if (this.registeredWait === undefined) {
            this.registeredWait = new Promise<void>((resolve, reject) => {
                this.resolveRegistered = resolve;
                this.rejectRegistered = reject;
            });
        }
        return this.registeredWait;
    }

    private async runListener(): Promise<void> {
        while (this.listening && !this.closed) {
            let failedBeforeRegister = false;
            try {
                const socket = await this.openSignaling(this.signalingUrl!);
                if (this.closed) {
                    socket.close();
                    return;
                }
                this.listenerSocket = socket;
                await new Promise<void>((resolve) => {
                    socket.onClose(() => resolve());
                    socket.onMessage(text => {
                        this.enqueue(async () => {
                            const msg = parseSignalMessage(text);
                            if (msg.type === 'hello' || msg.type === 'renew') {
                                await this.sendRegister(socket, msg.nonce);
                                return;
                            }
                            if (msg.type === 'registered') {
                                if (!this.registered) {
                                    this.registered = true;
                                    this.resolveRegistered?.();
                                }
                                return;
                            }
                            if (msg.type === 'reject' && !this.registered) {
                                throw new Error(`rtc listen rejected: ${msg.reason}`);
                            }
                            await this.handleListenerMessage(msg);
                        }).catch(err => {
                            if (!this.registered) {
                                failedBeforeRegister = true;
                                this.rejectRegistered?.(err instanceof Error ? err : new Error(String(err)));
                                try { socket.close(); } catch { /* ignore */ }
                            }
                        });
                    });
                });
            } catch (err) {
                if (!this.registered) {
                    this.rejectRegistered?.(err instanceof Error ? err : new Error(String(err)));
                    return;
                }
            }
            if (failedBeforeRegister || this.closed || !this.listening) return;
            await delay(500);
        }
    }

    private enqueue(fn: () => Promise<void>): Promise<void> {
        const run = this.chain.then(fn, fn);
        this.chain = run.then(() => {}, () => {});
        return run;
    }

    private async sendRegister(socket: SignalSocket, nonce: string): Promise<void> {
        if (this.signalingOrigin === undefined) throw new Error('rtc listen requires a signaling server URL');
        const expiry = Math.floor(this.now() / 1000) + this.delegationTtlSec;
        const delegation = delegationToWire(await signDelegation(
            this.identity, this.signalingOrigin, this.endpointId, expiry,
        ));
        const possessionSig = await signPossession(this.identity, nonce, this.endpointId, this.signalingOrigin);
        socket.send(encodeSignalMessage({ type: 'register', delegation, possessionSig }));
    }

    private listenerSend(msg: SignalMessage): void {
        try { this.listenerSocket?.send(encodeSignalMessage(msg)); } catch { /* ignore */ }
    }

    private async handleListenerMessage(msg: SignalMessage): Promise<void> {
        if (msg.type === 'dial') {
            this.acceptDial(msg.session, msg.from);
            return;
        }
        if (msg.type === 'signal') {
            await this.onInboundSignal(msg.session, msg.data);
            return;
        }
        if ((msg.type === 'bye' || msg.type === 'reject') && msg.session !== undefined) {
            this.dropSession(msg.session);
        }
    }

    private acceptDial(session: string, from: NetworkAddress | undefined): void {
        const remoteId = from === undefined ? undefined : endpointIdOf(from);
        if (remoteId !== undefined && this.outbounds.has(remoteId)) {
            if (this.endpointId < remoteId) {
                this.outbounds.get(remoteId)?.abort(new Error('rtc connect rejected: glare'));
            } else {
                this.listenerSend({ type: 'reject', session, reason: 'glare' });
                return;
            }
        }
        const peer = this.createPeer(this.iceServers);
        const inbound: InboundSession = { peer, pending: [], remoteSet: false };
        this.sessions.set(session, inbound);
        peer.onicecandidate = (candidate) => {
            if (!candidate?.candidate) return;
            this.listenerSend({
                type: 'signal',
                session,
                data: {
                    kind: 'candidate',
                    candidate: candidate.candidate,
                    sdpMid: candidate.sdpMid,
                    sdpMLineIndex: candidate.sdpMLineIndex,
                },
            });
        };
        peer.ondatachannel = (channel) => {
            const transport = new RtcByteTransport(channel, this.localAddress, from, () => {
                try { peer.close(); } catch { /* ignore */ }
            });
            void transport.opened.then(() => this.onConnection?.(transport));
        };
    }

    private async onInboundSignal(session: string, data: SignalData): Promise<void> {
        const inbound = this.sessions.get(session);
        if (inbound === undefined) return;
        if (data.kind === 'offer') {
            await inbound.peer.setRemoteDescription({ type: 'offer', sdp: data.sdp });
            inbound.remoteSet = true;
            await this.flush(inbound);
            const answer = await inbound.peer.createAnswer();
            await inbound.peer.setLocalDescription(answer);
            this.listenerSend({ type: 'signal', session, data: { kind: 'answer', sdp: answer.sdp ?? '' } });
            return;
        }
        if (data.kind === 'candidate') {
            if (!inbound.remoteSet) inbound.pending.push(data);
            else if (data.candidate) await inbound.peer.addIceCandidate(data);
        }
    }

    private async flush(inbound: InboundSession): Promise<void> {
        while (inbound.pending.length > 0) {
            const candidate = inbound.pending.shift()!;
            if (candidate.candidate) await inbound.peer.addIceCandidate(candidate);
        }
    }

    private dropSession(session: string): void {
        const inbound = this.sessions.get(session);
        if (inbound === undefined) return;
        this.sessions.delete(session);
        try { inbound.peer.close(); } catch { /* ignore */ }
    }
}

function endpointIdOf(address: string): string | undefined {
    try {
        return parseRtcAddress(address).endpointId;
    } catch {
        return undefined;
    }
}

function delay(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}
