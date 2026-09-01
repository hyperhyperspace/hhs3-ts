// BroadcastChannel TransportProvider. One endpoint identity (bc://<id>) and
// one bus channel; connections are multiplexed by cid. listen() accepts
// inbound connect envelopes; connect() waits for accept with a bounded timeout.

import type { Transport, TransportProvider, NetworkAddress } from '@hyper-hyper-space/hhs3_mesh';
import { random } from '@hyper-hyper-space/hhs3_crypto';
import {
    BroadcastChannelCtor, BroadcastChannelLike,
    defaultBroadcastChannelCtor, DEFAULT_BC_BASE,
} from './bc_types.js';
import {
    BroadcastChannelTransport, BcEnvelope, BcEnvelopeKind,
} from './bc_transport.js';

const DEFAULT_CONNECT_TIMEOUT_MS = 2_000;

export interface BroadcastChannelTransportProviderOptions {
    base?: string;
    endpointId?: string;
    BroadcastChannelCtor?: BroadcastChannelCtor;
    connectTimeoutMs?: number;
}

function bytesToBase64url(bytes: Uint8Array): string {
    let binary = '';
    for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]!);
    return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '');
}

function randomId(): string {
    return bytesToBase64url(random.getBytes(8));
}

function parseBcAddress(addr: NetworkAddress): string {
    if (!addr.startsWith('bc://')) {
        throw new Error(`not a bc address: ${addr}`);
    }
    return addr.slice('bc://'.length);
}

function asBytes(data: unknown): Uint8Array | undefined {
    if (data instanceof Uint8Array) return data;
    if (data instanceof ArrayBuffer) return new Uint8Array(data);
    if (ArrayBuffer.isView(data)) {
        return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
    }
    return undefined;
}

interface PendingConnect {
    resolve: (t: Transport) => void;
    reject: (e: Error) => void;
    timer: ReturnType<typeof setTimeout>;
}

export class BroadcastChannelTransportProvider implements TransportProvider {

    readonly scheme = 'bc';
    readonly endpointId: string;
    readonly localAddress: NetworkAddress;

    private readonly connectTimeoutMs: number;
    private readonly channel: BroadcastChannelLike;
    private readonly transports = new Map<string, BroadcastChannelTransport>();
    private readonly pending = new Map<string, PendingConnect>();
    private onConnection?: (transport: Transport) => void;
    private seq = 0;
    private closed = false;

    constructor(opts?: BroadcastChannelTransportProviderOptions) {
        this.endpointId = opts?.endpointId ?? randomId();
        this.localAddress = `bc://${this.endpointId}`;
        this.connectTimeoutMs = opts?.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS;
        const Ctor = opts?.BroadcastChannelCtor ?? defaultBroadcastChannelCtor();
        this.channel = new Ctor(opts?.base ?? DEFAULT_BC_BASE);
        this.channel.onmessage = (ev) => this.handle(ev.data);
    }

    async listen(
        address: NetworkAddress,
        onConnection: (transport: Transport) => void,
    ): Promise<void> {
        if (this.closed) throw new Error('provider closed');
        const id = parseBcAddress(address);
        if (id !== this.endpointId) {
            throw new Error(`cannot listen on ${address}; this provider is ${this.localAddress}`);
        }
        this.onConnection = onConnection;
    }

    connect(remote: NetworkAddress, _local?: NetworkAddress): Promise<Transport> {
        if (this.closed) return Promise.reject(new Error('provider closed'));
        const dst = parseBcAddress(remote);
        if (dst === this.endpointId) {
            return Promise.reject(new Error('cannot connect to self'));
        }

        const cid = `${this.endpointId}:${++this.seq}`;
        const transport = this.makeTransport(cid, remote);

        return new Promise<Transport>((resolve, reject) => {
            const timer = setTimeout(() => {
                this.pending.delete(cid);
                transport.close();
                reject(new Error(`bc connect timeout: ${remote}`));
            }, this.connectTimeoutMs);
            this.pending.set(cid, { resolve, reject, timer });
            this.post({
                v: 1,
                cid,
                kind: 'connect',
                src: this.endpointId,
                dst,
            });
        });
    }

    close(): void {
        if (this.closed) return;
        this.onConnection = undefined;
        for (const p of this.pending.values()) {
            clearTimeout(p.timer);
            p.reject(new Error('provider closed'));
        }
        this.pending.clear();
        for (const t of [...this.transports.values()]) {
            t.close();
        }
        this.closed = true;
        try { this.channel.close(); } catch { /* ignore */ }
    }

    private handle(raw: unknown): void {
        const msg = raw as BcEnvelope;
        if (msg === null || typeof msg !== 'object') return;
        if (msg.v !== 1 || typeof msg.cid !== 'string' || typeof msg.kind !== 'string') return;
        if (msg.dst !== this.endpointId) return;

        switch (msg.kind) {
            case 'connect':
                this.onInboundConnect(msg);
                break;
            case 'accept':
                this.onAccept(msg);
                break;
            case 'data': {
                const bytes = asBytes(msg.data);
                if (bytes !== undefined) this.transports.get(msg.cid)?.deliver(bytes);
                break;
            }
            case 'close':
                this.transports.get(msg.cid)?.remoteClosed();
                break;
        }
    }

    private onInboundConnect(msg: BcEnvelope): void {
        if (this.onConnection === undefined || this.closed) return;
        if (this.transports.has(msg.cid)) return;
        const remote = `bc://${msg.src}` as NetworkAddress;
        const transport = this.makeTransport(msg.cid, remote);
        this.post({
            v: 1,
            cid: msg.cid,
            kind: 'accept',
            src: this.endpointId,
            dst: msg.src,
        });
        this.onConnection(transport);
    }

    private onAccept(msg: BcEnvelope): void {
        const pending = this.pending.get(msg.cid);
        if (pending === undefined) return;
        clearTimeout(pending.timer);
        this.pending.delete(msg.cid);
        const transport = this.transports.get(msg.cid);
        if (transport === undefined) {
            pending.reject(new Error('accept for unknown transport'));
            return;
        }
        pending.resolve(transport);
    }

    private makeTransport(cid: string, remote: NetworkAddress): BroadcastChannelTransport {
        const transport = new BroadcastChannelTransport(
            cid,
            this.localAddress,
            remote,
            (kind: BcEnvelopeKind, data?: Uint8Array) => {
                const dst = parseBcAddress(remote);
                this.post({ v: 1, cid, kind, src: this.endpointId, dst, data });
            },
            () => { this.transports.delete(cid); },
        );
        this.transports.set(cid, transport);
        return transport;
    }

    private post(env: BcEnvelope): void {
        if (this.closed) return;
        this.channel.postMessage(env);
    }
}
