// Wraps a ws WebSocket instance into the mesh Transport interface.
// Normalizes incoming messages to plain Uint8Array (ws delivers Buffer,
// which is a Uint8Array subclass but may carry extra Node.js semantics).

import WebSocket from 'ws';
import type { Transport } from '@hyper-hyper-space/hhs3_mesh';

export class WsTransport implements Transport {

    private ws: WebSocket;
    private pending: Uint8Array[] = [];
    private messageCallbacks: ((message: Uint8Array) => void)[] = [];
    private subscribed = false;

    constructor(ws: WebSocket) {
        this.ws = ws;
        // Attach immediately. The Noise responder registers onMessage only after
        // its handshake returns, and the initiator's first frame can already be
        // sitting on the socket by then.
        this.ws.on('message', (data: WebSocket.RawData) => {
            const bytes = toBytes(data);
            if (!this.subscribed) {
                this.pending.push(bytes);
                return;
            }
            for (const cb of this.messageCallbacks) cb(bytes);
        });
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
        const first = !this.subscribed;
        this.subscribed = true;
        this.messageCallbacks.push(callback);
        if (!first) return;
        const buffered = this.pending;
        this.pending = [];
        for (const bytes of buffered) callback(bytes);
    }

    onClose(callback: () => void): void {
        this.ws.on('close', callback);
    }
}

function toBytes(data: WebSocket.RawData): Uint8Array {
    if (data instanceof ArrayBuffer) return new Uint8Array(data);
    if (data instanceof Buffer) return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
    if (Array.isArray(data)) return new Uint8Array(Buffer.concat(data));
    return new Uint8Array(data);
}
