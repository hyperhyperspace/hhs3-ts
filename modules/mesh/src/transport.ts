// Transport abstraction for bidirectional byte channels. The mesh module
// defines only the interfaces; concrete implementations (WebSocket, WebRTC,
// etc.) live in separate modules and are injected by the application.

export type NetworkAddress = string;

export interface Transport {
    readonly open: boolean;
    send(message: Uint8Array): void;
    close(): void;
    onMessage(callback: (message: Uint8Array) => void): void;
    onClose(callback: () => void): void;
}

export interface TransportProvider {
    readonly scheme: string;
    listen(address: NetworkAddress, onConnection: (transport: Transport) => void): Promise<void>;
    connect(remote: NetworkAddress): Promise<Transport>;
    close(): void;
}
