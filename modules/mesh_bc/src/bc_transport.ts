// Pair-like Transport over a BroadcastChannel bus. Mirrors MemTransport:
// copy-on-send, local close posts a close envelope, remote close does not.

import type { Transport, NetworkAddress } from '@hyper-hyper-space/hhs3_mesh';

export type BcEnvelopeKind = 'connect' | 'accept' | 'data' | 'close';

export interface BcEnvelope {
    v: 1;
    cid: string;
    kind: BcEnvelopeKind;
    src: string;
    dst: string;
    data?: Uint8Array;
}

export class BroadcastChannelTransport implements Transport {

    private _open = true;
    private messageCallbacks: ((msg: Uint8Array) => void)[] = [];
    private closeCallbacks: (() => void)[] = [];

    readonly localAddress?: NetworkAddress;
    readonly remoteAddress?: NetworkAddress;
    readonly cid: string;

    private readonly post: (kind: BcEnvelopeKind, data?: Uint8Array) => void;
    private readonly onLocalClose: () => void;

    constructor(
        cid: string,
        localAddress: NetworkAddress,
        remoteAddress: NetworkAddress,
        post: (kind: BcEnvelopeKind, data?: Uint8Array) => void,
        onLocalClose: () => void,
    ) {
        this.cid = cid;
        this.localAddress = localAddress;
        this.remoteAddress = remoteAddress;
        this.post = post;
        this.onLocalClose = onLocalClose;
    }

    get open(): boolean { return this._open; }

    send(message: Uint8Array): void {
        if (!this._open) throw new Error('transport closed');
        this.post('data', new Uint8Array(message));
    }

    close(): void {
        if (!this._open) return;
        this._open = false;
        this.post('close');
        this.onLocalClose();
        for (const cb of this.closeCallbacks) cb();
    }

    /** Remote posted `close` (or local timeout). Do not re-post. */
    remoteClosed(): void {
        if (!this._open) return;
        this._open = false;
        this.onLocalClose();
        for (const cb of this.closeCallbacks) cb();
    }

    deliver(message: Uint8Array): void {
        if (!this._open) return;
        const copy = new Uint8Array(message);
        for (const cb of this.messageCallbacks) cb(copy);
    }

    onMessage(callback: (message: Uint8Array) => void): void {
        this.messageCallbacks.push(callback);
    }

    onClose(callback: () => void): void {
        this.closeCallbacks.push(callback);
    }
}
