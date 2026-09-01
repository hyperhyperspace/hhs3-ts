// Wraps a WHATWG WebSocket into the mesh Transport interface.
// Incoming frames are normalized to plain Uint8Array (browsers deliver
// ArrayBuffer when binaryType is 'arraybuffer').

import type { Transport, NetworkAddress } from '@hyper-hyper-space/hhs3_mesh';

export class BrowserWsTransport implements Transport {

    private ws: WebSocket;
    readonly remoteAddress?: NetworkAddress;

    constructor(ws: WebSocket, remoteAddress?: NetworkAddress) {
        this.ws = ws;
        this.ws.binaryType = 'arraybuffer';
        this.remoteAddress = remoteAddress;
    }

    get open(): boolean {
        return this.ws.readyState === WebSocket.OPEN;
    }

    send(message: Uint8Array): void {
        if (this.ws.readyState !== WebSocket.OPEN) {
            throw new Error('WebSocket is not open');
        }
        // Copy: the mux may hand us a view over a shared buffer.
        this.ws.send(message.slice());
    }

    close(): void {
        const s = this.ws.readyState;
        if (s === WebSocket.OPEN || s === WebSocket.CONNECTING) {
            this.ws.close();
        }
    }

    onMessage(callback: (message: Uint8Array) => void): void {
        this.ws.addEventListener('message', (ev: MessageEvent) => {
            const d = ev.data;
            if (d instanceof ArrayBuffer) {
                callback(new Uint8Array(d));
            } else if (ArrayBuffer.isView(d)) {
                callback(new Uint8Array(d.buffer, d.byteOffset, d.byteLength));
            }
            // string frames ignored: the wire protocol is binary
        });
    }

    onClose(callback: () => void): void {
        this.ws.addEventListener('close', () => callback());
    }
}
