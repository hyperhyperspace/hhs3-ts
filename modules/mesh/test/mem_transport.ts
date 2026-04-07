// In-memory transport for testing. Creates paired channels that deliver
// messages synchronously within the same process. No network dependencies.

import type { Transport, TransportProvider, NetworkAddress } from '../src/transport.js';

export class MemTransport implements Transport {
    private _open = true;
    private messageCallbacks: ((msg: Uint8Array) => void)[] = [];
    private closeCallbacks: (() => void)[] = [];
    peer?: MemTransport;

    get open(): boolean { return this._open; }

    send(message: Uint8Array): void {
        if (!this._open) throw new Error('transport closed');
        if (!this.peer || !this.peer._open) throw new Error('peer closed');
        const copy = new Uint8Array(message);
        for (const cb of this.peer.messageCallbacks) cb(copy);
    }

    close(): void {
        if (!this._open) return;
        this._open = false;
        for (const cb of this.closeCallbacks) cb();
        if (this.peer && this.peer._open) {
            this.peer.close();
        }
    }

    onMessage(callback: (message: Uint8Array) => void): void {
        this.messageCallbacks.push(callback);
    }

    onClose(callback: () => void): void {
        this.closeCallbacks.push(callback);
    }
}

export function createMemTransportPair(): [MemTransport, MemTransport] {
    const a = new MemTransport();
    const b = new MemTransport();
    a.peer = b;
    b.peer = a;
    return [a, b];
}

export class MemTransportProvider implements TransportProvider {
    readonly scheme = 'mem';
    private listeners = new Map<string, (transport: Transport) => void>();

    async listen(address: NetworkAddress, onConnection: (transport: Transport) => void): Promise<void> {
        this.listeners.set(address, onConnection);
    }

    async connect(remote: NetworkAddress): Promise<Transport> {
        const listener = this.listeners.get(remote);
        if (!listener) throw new Error(`no listener at ${remote}`);
        const [client, server] = createMemTransportPair();
        listener(server);
        return client;
    }

    close(): void {
        this.listeners.clear();
    }
}
