// Wraps a ws WebSocket instance into the mesh Transport interface.
// Normalizes incoming messages to plain Uint8Array (ws delivers Buffer,
// which is a Uint8Array subclass but may carry extra Node.js semantics).

import WebSocket from 'ws';
import type { Transport } from '@hyper-hyper-space/hhs3_mesh';

export class WsTransport implements Transport {

    private ws: WebSocket;

    constructor(ws: WebSocket) {
        this.ws = ws;
    }

    get open(): boolean {
        return this.ws.readyState === WebSocket.OPEN;
    }

    send(message: Uint8Array): void {
        if (this.ws.readyState !== WebSocket.OPEN) {
            throw new Error('WebSocket is not open');
        }
        this.ws.send(message);
    }

    close(): void {
        if (this.ws.readyState === WebSocket.OPEN ||
            this.ws.readyState === WebSocket.CONNECTING) {
            this.ws.close();
        }
    }

    onMessage(callback: (message: Uint8Array) => void): void {
        this.ws.on('message', (data: WebSocket.RawData) => {
            if (data instanceof ArrayBuffer) {
                callback(new Uint8Array(data));
            } else if (data instanceof Buffer) {
                callback(new Uint8Array(data.buffer, data.byteOffset, data.byteLength));
            } else if (Array.isArray(data)) {
                callback(new Uint8Array(Buffer.concat(data)));
            } else {
                callback(new Uint8Array(data));
            }
        });
    }

    onClose(callback: () => void): void {
        this.ws.on('close', callback);
    }
}
