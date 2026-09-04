// WebSocket TransportProvider. Creates a WebSocketServer for listening and
// WebSocket clients for outbound connections. Tracks all sockets so close()
// can tear everything down cleanly.

import WebSocket, { WebSocketServer } from 'ws';
import type { Transport, TransportProvider, NetworkAddress } from '@hyper-hyper-space/hhs3_mesh';
import { WsTransport } from './ws_transport.js';

export const DEFAULT_CONNECT_TIMEOUT_MS = 5_000;

export type WsClientCtor = new (address: string) => WebSocket;

export interface WsTransportProviderOptions {
    connectTimeoutMs?: number;
    WebSocketCtor?: WsClientCtor;
}

export class WsTransportProvider implements TransportProvider {

    readonly scheme: string;
    private readonly connectTimeoutMs: number;
    private readonly WS: WsClientCtor;

    private server?: WebSocketServer;
    private sockets = new Set<WebSocket>();

    constructor(scheme: string = 'ws', opts?: WsTransportProviderOptions) {
        this.scheme = scheme;
        this.connectTimeoutMs = opts?.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS;
        this.WS = opts?.WebSocketCtor ?? WebSocket;
    }

    async listen(
        address: NetworkAddress,
        onConnection: (transport: Transport) => void
    ): Promise<void> {
        if (this.scheme === 'wss') {
            throw new Error('wss listen requires TLS termination; run behind a reverse proxy and listen on ws');
        }
        const url = new URL(address);
        const host = url.hostname;
        const port = parseInt(url.port, 10);

        return new Promise<void>((resolve, reject) => {
            this.server = new WebSocketServer({ host, port }, () => {
                resolve();
            });

            this.server.on('error', reject);

            this.server.on('connection', (ws) => {
                this.sockets.add(ws);
                ws.on('close', () => this.sockets.delete(ws));
                onConnection(new WsTransport(ws));
            });
        });
    }

    async connect(remote: NetworkAddress): Promise<Transport> {
        return new Promise<Transport>((resolve, reject) => {
            let settled = false;
            const ws = new this.WS(remote);
            ws.binaryType = 'nodebuffer';
            this.sockets.add(ws);

            const timer = setTimeout(() => {
                if (settled) return;
                settled = true;
                this.sockets.delete(ws);
                try { ws.close(); } catch { /* ignore */ }
                reject(new Error(`ws connect timeout: ${remote}`));
            }, this.connectTimeoutMs);

            ws.on('close', () => this.sockets.delete(ws));
            ws.on('open', () => {
                if (settled) return;
                settled = true;
                clearTimeout(timer);
                resolve(new WsTransport(ws));
            });
            ws.on('error', (err) => {
                if (settled) return;
                settled = true;
                clearTimeout(timer);
                this.sockets.delete(ws);
                reject(err);
            });
        });
    }

    close(): void {
        for (const ws of this.sockets) {
            ws.close();
        }
        this.sockets.clear();

        if (this.server) {
            this.server.close();
            this.server = undefined;
        }
    }

    serverPort(): number | undefined {
        const addr = this.server?.address();
        if (addr && typeof addr === 'object') {
            return addr.port;
        }
        return undefined;
    }
}
