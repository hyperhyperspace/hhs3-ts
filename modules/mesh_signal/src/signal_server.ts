// Reference signaling server. One WebSocket per listener, for the life of the
// address. Dialers open a short-lived socket, check the delegation the server
// forwards, then exchange opaque SDP. Private mode allowlists key ids. Public
// mode accepts any valid, unexpired delegation. Both modes check the signature
// and the possession proof.

import { WebSocketServer, WebSocket, type RawData } from 'ws';
import type { KeyId } from '@hyper-hyper-space/hhs3_crypto';
import { random } from '@hyper-hyper-space/hhs3_crypto';
import { keyIdFromPublicKey, sha256 } from '@hyper-hyper-space/hhs3_crypto';
import {
    decodePublicKey,
    encodeSignalMessage,
    parseSignalMessage,
    verifyDelegation,
    verifyPossession,
    type DelegationWire,
    type SignalMessage,
} from '@hyper-hyper-space/hhs3_mesh_rtc';

export interface SignalServerOptions {
    host?: string;
    port?: number;
    /** Exact origin listeners must sign, e.g. wss://signal.example.com:443/mount. */
    publicOrigin: string;
    publicMode?: boolean;
    allow?: readonly KeyId[];
    now?: () => number;
    clockSkewMs?: number;
    /** Ask for a new delegation when the current one is inside this lead. */
    renewLeadMs?: number;
    renewCheckMs?: number;
}

interface Listener {
    ws: WebSocket;
    delegation: DelegationWire;
    keyId: KeyId;
    nonce: string;
}

interface DialSession {
    dialer: WebSocket;
    endpointId: string;
}

const DEFAULT_RENEW_LEAD_MS = 120_000;
const DEFAULT_RENEW_CHECK_MS = 15_000;

export class SignalServer {
    private readonly host: string;
    private readonly requestedPort: number;
    private readonly publicOrigin: string;
    private readonly publicMode: boolean;
    private readonly allow: Set<string>;
    private readonly now: () => number;
    private readonly clockSkewMs: number;
    private readonly renewLeadMs: number;
    private readonly renewCheckMs: number;

    private wss?: WebSocketServer;
    private boundPort = 0;
    private readonly listeners = new Map<string, Listener>();
    private readonly sessions = new Map<string, DialSession>();
    private readonly socketNonce = new Map<WebSocket, string>();
    private renewTimer: ReturnType<typeof setInterval> | undefined;

    constructor(opts: SignalServerOptions) {
        this.host = opts.host ?? '127.0.0.1';
        this.requestedPort = opts.port ?? 0;
        this.publicOrigin = opts.publicOrigin;
        this.publicMode = opts.publicMode ?? false;
        this.allow = new Set(opts.allow ?? []);
        this.now = opts.now ?? (() => Date.now());
        this.clockSkewMs = opts.clockSkewMs ?? 60_000;
        this.renewLeadMs = opts.renewLeadMs ?? DEFAULT_RENEW_LEAD_MS;
        this.renewCheckMs = opts.renewCheckMs ?? DEFAULT_RENEW_CHECK_MS;
    }

    get port(): number { return this.boundPort; }

    registrationCount(): number { return this.listeners.size; }

    async start(): Promise<void> {
        const wss = new WebSocketServer({ host: this.host, port: this.requestedPort });
        this.wss = wss;
        await new Promise<void>((resolve, reject) => {
            wss.once('listening', () => resolve());
            wss.once('error', reject);
        });
        const addr = wss.address();
        this.boundPort = typeof addr === 'object' && addr !== null ? addr.port : this.requestedPort;
        wss.on('connection', (ws, req) => {
            this.onConnection(ws, req.url ?? '/').catch(() => {
                try { ws.close(); } catch { /* ignore */ }
            });
        });
        this.renewTimer = setInterval(() => this.requestRenewals(), this.renewCheckMs);
        if (typeof this.renewTimer === 'object' && 'unref' in this.renewTimer) {
            this.renewTimer.unref();
        }
    }

    stop(): void {
        if (this.renewTimer !== undefined) clearInterval(this.renewTimer);
        this.renewTimer = undefined;
        for (const listener of this.listeners.values()) {
            try { listener.ws.close(); } catch { /* ignore */ }
        }
        this.listeners.clear();
        this.sessions.clear();
        const wss = this.wss;
        this.wss = undefined;
        if (wss !== undefined) {
            for (const client of wss.clients) {
                try { client.terminate(); } catch { /* ignore */ }
            }
            wss.close();
        }
    }

    /** Send a fresh nonce to every listener whose delegation is inside the lead, or to all when forced. */
    requestRenewals(force = false): void {
        const now = this.now();
        for (const listener of this.listeners.values()) {
            const remaining = listener.delegation.expiry * 1000 - now;
            if (!force && remaining > this.renewLeadMs) continue;
            listener.nonce = newNonce();
            this.socketNonce.set(listener.ws, listener.nonce);
            send(listener.ws, { type: 'renew', nonce: listener.nonce });
        }
    }

    private async onConnection(ws: WebSocket, urlPath: string): Promise<void> {
        const endpointId = endpointFromPath(urlPath);
        if (endpointId === undefined) {
            send(ws, { type: 'reject', reason: 'missing endpoint' });
            ws.close();
            return;
        }
        const nonce = newNonce();
        this.socketNonce.set(ws, nonce);
        send(ws, { type: 'hello', nonce });

        ws.on('message', (data) => {
            this.onMessage(ws, endpointId, rawText(data))
                .catch(() => { try { ws.close(); } catch { /* ignore */ } });
        });
        ws.on('close', () => {
            this.socketNonce.delete(ws);
            const current = this.listeners.get(endpointId);
            if (current?.ws === ws) this.listeners.delete(endpointId);
            for (const [session, dial] of this.sessions) {
                if (dial.dialer !== ws) continue;
                this.sessions.delete(session);
                const listener = this.listeners.get(dial.endpointId);
                if (listener !== undefined) send(listener.ws, { type: 'bye', session });
            }
        });
    }

    private async onMessage(ws: WebSocket, endpointId: string, text: string): Promise<void> {
        let msg: SignalMessage;
        try {
            msg = parseSignalMessage(text);
        } catch {
            send(ws, { type: 'reject', reason: 'malformed' });
            return;
        }
        if (msg.type === 'register') {
            await this.onRegister(ws, endpointId, this.socketNonce.get(ws) ?? '', msg.delegation, msg.possessionSig);
            return;
        }
        if (msg.type === 'dial') {
            this.onDial(ws, endpointId, msg.session, msg.from);
            return;
        }
        if (msg.type === 'signal') {
            this.forwardSignal(ws, endpointId, msg);
            return;
        }
        if (msg.type === 'bye') {
            this.forwardBye(ws, endpointId, msg.session);
            return;
        }
        if (msg.type === 'reject') {
            this.forwardReject(ws, endpointId, msg.session, msg.reason);
            return;
        }
    }

    private async onRegister(
        ws: WebSocket,
        endpointId: string,
        nonce: string,
        delegation: DelegationWire,
        possessionSig: string,
    ): Promise<void> {
        if (delegation.endpointId !== endpointId) {
            send(ws, { type: 'reject', reason: 'endpoint mismatch' });
            return;
        }
        let keyId: KeyId;
        try {
            keyId = await verifyDelegation(delegation, {
                origin: this.publicOrigin,
                nowMs: this.now(),
                skewMs: this.clockSkewMs,
            });
        } catch (err) {
            send(ws, { type: 'reject', reason: err instanceof Error ? err.message : 'bad delegation' });
            return;
        }
        const publicKey = decodePublicKey(delegation.publicKey);
        if (keyIdFromPublicKey(publicKey, sha256) !== keyId) {
            send(ws, { type: 'reject', reason: 'bad delegation' });
            return;
        }
        const possessed = await verifyPossession(publicKey, nonce, endpointId, this.publicOrigin, possessionSig);
        if (!possessed) {
            send(ws, { type: 'reject', reason: 'bad possession' });
            return;
        }
        if (!this.publicMode && !this.allow.has(keyId)) {
            send(ws, { type: 'reject', reason: 'not allowed' });
            return;
        }
        const previous = this.listeners.get(endpointId);
        if (previous !== undefined && previous.ws !== ws) {
            try { previous.ws.close(); } catch { /* ignore */ }
        }
        this.listeners.set(endpointId, { ws, delegation, keyId, nonce });
        send(ws, { type: 'registered' });
    }

    private onDial(ws: WebSocket, endpointId: string, session: string, from: string | undefined): void {
        const listener = this.listeners.get(endpointId);
        if (listener === undefined || listener.ws.readyState !== WebSocket.OPEN) {
            send(ws, { type: 'reject', session, reason: 'not registered' });
            return;
        }
        this.sessions.set(session, { dialer: ws, endpointId });
        send(ws, { type: 'warrant', session, delegation: listener.delegation });
        send(listener.ws, { type: 'dial', session, from });
    }

    private forwardSignal(ws: WebSocket, endpointId: string, msg: Extract<SignalMessage, { type: 'signal' }>): void {
        const session = this.sessions.get(msg.session);
        if (session === undefined) return;
        if (ws === session.dialer) {
            const listener = this.listeners.get(session.endpointId);
            if (listener !== undefined) send(listener.ws, msg);
            return;
        }
        const listener = this.listeners.get(endpointId);
        if (listener?.ws === ws && session.endpointId === endpointId) send(session.dialer, msg);
    }

    private forwardBye(ws: WebSocket, endpointId: string, sessionId: string): void {
        const session = this.sessions.get(sessionId);
        if (session === undefined) return;
        this.sessions.delete(sessionId);
        if (ws === session.dialer) {
            const listener = this.listeners.get(session.endpointId);
            if (listener !== undefined) send(listener.ws, { type: 'bye', session: sessionId });
            return;
        }
        if (this.listeners.get(endpointId)?.ws === ws) send(session.dialer, { type: 'bye', session: sessionId });
    }

    private forwardReject(ws: WebSocket, endpointId: string, sessionId: string | undefined, reason: string): void {
        if (sessionId === undefined) return;
        const session = this.sessions.get(sessionId);
        if (session === undefined) return;
        if (this.listeners.get(endpointId)?.ws === ws) {
            this.sessions.delete(sessionId);
            send(session.dialer, { type: 'reject', session: sessionId, reason });
        }
    }
}

function send(ws: WebSocket, msg: SignalMessage): void {
    if (ws.readyState !== WebSocket.OPEN) return;
    ws.send(encodeSignalMessage(msg));
}

function rawText(data: RawData): string {
    if (typeof data === 'string') return data;
    if (data instanceof Buffer) return data.toString('utf8');
    if (data instanceof ArrayBuffer) return Buffer.from(data).toString('utf8');
    if (Array.isArray(data)) return Buffer.concat(data).toString('utf8');
    return Buffer.from(data).toString('utf8');
}

function endpointFromPath(urlPath: string): string | undefined {
    const path = urlPath.split('?')[0] ?? '';
    const parts = path.split('/').filter(s => s.length > 0);
    if (parts.length === 0) return undefined;
    try {
        return decodeURIComponent(parts[parts.length - 1]!);
    } catch {
        return undefined;
    }
}

function newNonce(): string {
    let binary = '';
    const bytes = random.getBytes(32);
    for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]!);
    return btoa(binary);
}

export function publicOriginFor(host: string, port: number, mount = ''): string {
    const h = host.includes(':') ? `[${host}]` : host;
    const base = `wss://${h}:${port}`;
    return mount.length === 0 ? base : `${base}/${mount}`;
}
