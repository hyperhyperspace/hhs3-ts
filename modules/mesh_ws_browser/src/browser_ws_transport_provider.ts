// Dial-only WebSocket TransportProvider for browsers. Connects via the
// WHATWG WebSocket API; listen() is not supported. Scheme is parameterized
// so the same class can match both ws:// and wss:// addresses. The
// WebSocket constructor is injectable so tests can run under Node.

import type { Transport, TransportProvider, NetworkAddress } from '@hyper-hyper-space/hhs3_mesh';
import { BrowserWsTransport } from './browser_ws_transport.js';

export type WebSocketCtor = new (url: string) => WebSocket;

export interface BrowserWsTransportProviderOptions {
    scheme?: string;
    WebSocketCtor?: WebSocketCtor;
}

export class BrowserWsTransportProvider implements TransportProvider {

    readonly scheme: string;
    private readonly WS: WebSocketCtor;
    private sockets = new Set<WebSocket>();

    constructor(opts?: BrowserWsTransportProviderOptions) {
        this.scheme = opts?.scheme ?? 'ws';
        this.WS = opts?.WebSocketCtor
            ?? (globalThis as unknown as { WebSocket?: WebSocketCtor }).WebSocket!;
        if (this.WS === undefined) {
            throw new Error('no WebSocket implementation available');
        }
    }

    async listen(
        _address: NetworkAddress,
        _onConnection: (transport: Transport) => void,
    ): Promise<void> {
        throw new Error('BrowserWsTransportProvider is dial-only; listen() is not supported');
    }

    connect(remote: NetworkAddress, _local?: NetworkAddress): Promise<Transport> {
        return new Promise<Transport>((resolve, reject) => {
            const ws = new this.WS(remote);
            ws.binaryType = 'arraybuffer';
            this.sockets.add(ws);
            ws.addEventListener('close', () => this.sockets.delete(ws));
            ws.addEventListener('open', () => resolve(new BrowserWsTransport(ws, remote)));
            ws.addEventListener('error', () => {
                this.sockets.delete(ws);
                reject(new Error(`ws connect failed: ${remote}`));
            });
        });
    }

    close(): void {
        for (const ws of this.sockets) {
            try { ws.close(); } catch { /* ignore */ }
        }
        this.sockets.clear();
    }
}
