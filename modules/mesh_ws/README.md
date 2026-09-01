# Mesh WS

WebSocket transport for the **`mesh`** module. Implements the `Transport` and `TransportProvider` interfaces using the [`ws`](https://github.com/websockets/ws) library (Node.js).

- **`WsTransport`** — Wraps a `ws` WebSocket into the mesh `Transport` interface, normalizing incoming messages to plain `Uint8Array`.
- **`WsTransportProvider`** — Listens via `WebSocketServer` and connects via `WebSocket`. Tracks all sockets for clean teardown on `close()`. Scheme defaults to `ws`; pass `'wss'` to match `wss://` addresses (`connect()` handles TLS client-side; `listen()` on `wss` is not supported — terminate TLS upstream and listen on `ws`).

## Usage

```typescript
import { WsTransportProvider } from '@hyper-hyper-space/hhs3_mesh_ws';

const ws = new WsTransportProvider();
const wss = new WsTransportProvider('wss');

await ws.listen('ws://0.0.0.0:9000', (transport) => {
    // handle inbound connection
});

const outbound = await wss.connect('wss://peer-host:9000');
outbound.send(new Uint8Array([1, 2, 3]));

ws.close();
wss.close();
```

## Building

To build, run the following commands at the workspace level (top directory in this repo):

```
npm install
npm run build
```
