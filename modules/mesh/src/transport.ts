// Transport abstraction for bidirectional byte channels. The mesh module
// defines only the interfaces; concrete implementations (WebSocket, WebRTC,
// etc.) live in separate modules and are injected by the application.

import type { KeyId } from '@hyper-hyper-space/hhs3_crypto';

export type NetworkAddress = string;

export interface Transport {
    readonly open: boolean;
    readonly localAddress?: NetworkAddress;
    readonly remoteAddress?: NetworkAddress;
    send(message: Uint8Array): void;
    close(): void;
    onMessage(callback: (message: Uint8Array) => void): void;
    onClose(callback: () => void): void;
}

export interface TransportProvider {
    readonly scheme: string;
    listen(address: NetworkAddress, onConnection: (transport: Transport) => void): Promise<void>;
    // expectedKeyId is the peer discovery claims to be dialing. Providers that
    // must check a credential before spending a connection (WebRTC ICE) use it.
    // Others ignore it.
    connect(remote: NetworkAddress, local?: NetworkAddress, expectedKeyId?: KeyId): Promise<Transport>;
    close(): void;
}
