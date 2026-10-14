import type { NetworkAddress, Transport } from '@hyper-hyper-space/hhs3_mesh';
import { FrameDecoder, encodeCloseFrame, encodeFrames, isCloseFrame } from './frame.js';
import type { RtcDataChannelLike } from './peer.js';

export class RtcByteTransport implements Transport {
    readonly localAddress?: NetworkAddress;
    readonly remoteAddress?: NetworkAddress;
    readonly opened: Promise<void>;

    private readonly channel: RtcDataChannelLike;
    private readonly decoder = new FrameDecoder();
    private readonly messageCallbacks: ((message: Uint8Array) => void)[] = [];
    private readonly closeCallbacks: (() => void)[] = [];
    private _open: boolean;
    private closedNotified = false;
    private disposed = false;

    constructor(
        channel: RtcDataChannelLike,
        localAddress?: NetworkAddress,
        remoteAddress?: NetworkAddress,
        private readonly dispose?: () => void,
    ) {
        this.channel = channel;
        this.localAddress = localAddress;
        this.remoteAddress = remoteAddress;
        this._open = channel.readyState === 'open';
        this.opened = new Promise(resolve => {
            if (this._open) resolve();
            channel.onopen = () => {
                this._open = true;
                resolve();
            };
        });
        channel.onmessage = (data) => {
            if (isCloseFrame(data)) {
                this.shutdown(false);
                return;
            }
            const payload = this.decoder.push(data);
            if (payload === undefined) return;
            const copy = new Uint8Array(payload);
            for (const cb of this.messageCallbacks) cb(copy);
        };
        channel.onclose = () => this.shutdown(false);
        channel.onerror = () => this.shutdown(false);
    }

    get open(): boolean { return this._open; }

    send(message: Uint8Array): void {
        if (!this._open) throw new Error('transport closed');
        for (const frame of encodeFrames(message)) this.channel.send(frame);
    }

    close(): void {
        this.shutdown(true);
    }

    onMessage(callback: (message: Uint8Array) => void): void {
        this.messageCallbacks.push(callback);
    }

    onClose(callback: () => void): void {
        this.closeCallbacks.push(callback);
    }

    // `notify` sends one CLOSE frame first. A received CLOSE, or a channel
    // that is already gone, must not echo it.
    private shutdown(notify: boolean): void {
        if (this.disposed) return;
        this.disposed = true;
        if (notify && this.channel.readyState === 'open') {
            try { this.channel.send(encodeCloseFrame()); } catch { /* ignore */ }
        }
        this.markClosed();
        try { this.channel.close(); } catch { /* ignore */ }
        try { this.dispose?.(); } catch { /* ignore */ }
    }

    private markClosed(): void {
        const wasOpen = this._open;
        this._open = false;
        if (!wasOpen || this.closedNotified) return;
        this.closedNotified = true;
        for (const cb of this.closeCallbacks) cb();
    }
}
