// WebSocket TransportProvider. Creates a WebSocketServer for listening and
// WebSocket clients for outbound connections. Tracks all sockets so close()
// can tear everything down cleanly.

import WebSocket, { WebSocketServer } from 'ws';
import type { Transport, TransportProvider, NetworkAddress } from '@hyper-hyper-space/hhs3_mesh';
import { WsTransport } from './ws_transport.js';

export class WsTransportProvider implements TransportProvider {

    readonly scheme = 'ws';

    private server?: WebSocketServer;
    private sockets = new Set<WebSocket>();

    async listen(
        address: NetworkAddress,
        onConnection: (transport: Transport) => void
    ): Promise<void> {
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
            const ws = new WebSocket(remote);
            ws.binaryType = 'nodebuffer';

            this.sockets.add(ws);
            ws.on('close', () => this.sockets.delete(ws));

            ws.on('open', () => {
                resolve(new WsTransport(ws));
            });

            ws.on('error', (err) => {
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
