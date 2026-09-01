# Mesh WS Browser

Dial-only WebSocket transport for the **`mesh`** module, using the browser (WHATWG) `WebSocket` API. Implements `Transport` and `TransportProvider` with **no runtime dependencies**.

- **`BrowserWsTransport`** — Wraps a WHATWG `WebSocket` into the mesh `Transport` interface, normalizing incoming frames to plain `Uint8Array`.
- **`BrowserWsTransportProvider`** — Connects via `WebSocket`. `listen()` is not supported (browsers cannot accept inbound sockets). Tracks sockets for clean teardown on `close()`. Scheme defaults to `ws`; pass `{ scheme: 'wss' }` for TLS.

A mesh with no `listenAddresses` is already a first-class state: the browser announces no address and only initiates. Sync is still bidirectional over the connection it opened.

## Usage

```typescript
import { BrowserWsTransportProvider } from '@hyper-hyper-space/hhs3_mesh_ws_browser';

const transports = [
    new BrowserWsTransportProvider({ scheme: 'ws' }),
    new BrowserWsTransportProvider({ scheme: 'wss' }),
];

const outbound = await transports[0].connect('ws://127.0.0.1:4610');
outbound.send(new Uint8Array([1, 2, 3]));

for (const t of transports) t.close();
```

`WebSocketCtor` can be injected for tests (or non-browser runtimes) that supply a WHATWG-compatible constructor.

## Building

To build, run the following commands at the workspace level (top directory in this repo):

```
npm install
npm run build
```
