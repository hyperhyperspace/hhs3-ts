// Wraps a WHATWG WebSocket into the mesh Transport interface.
// Incoming frames are normalized to plain Uint8Array (browsers deliver
// ArrayBuffer when binaryType is 'arraybuffer').

import type { Transport, NetworkAddress } from '@hyper-hyper-space/hhs3_mesh';

export class BrowserWsTransport implements Transport {

    private ws: WebSocket;
    readonly remoteAddress?: NetworkAddress;
    private pending: Uint8Array[] = [];
    private messageCallbacks: ((message: Uint8Array) => void)[] = [];
    private subscribed = false;

    constructor(ws: WebSocket, remoteAddress?: NetworkAddress) {
        this.ws = ws;
        this.ws.binaryType = 'arraybuffer';
        this.remoteAddress = remoteAddress;
        // Attach immediately so a frame that arrives before onMessage (the
        // first Noise message) is not dropped.
        this.ws.addEventListener('message', (ev: MessageEvent) => {
            const bytes = toBytes(ev.data);
            if (bytes === undefined) return;
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
        const first = !this.subscribed;
        this.subscribed = true;
        this.messageCallbacks.push(callback);
        if (!first) return;
        const buffered = this.pending;
        this.pending = [];
        for (const bytes of buffered) callback(bytes);
    }

    onClose(callback: () => void): void {
        this.ws.addEventListener('close', () => callback());
    }
}

function toBytes(data: unknown): Uint8Array | undefined {
    if (data instanceof ArrayBuffer) return new Uint8Array(data);
    if (ArrayBuffer.isView(data)) return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
    return undefined;
}
