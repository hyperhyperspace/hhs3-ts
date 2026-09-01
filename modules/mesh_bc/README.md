# Mesh BroadcastChannel

Cross-tab `Transport` / `TransportProvider` and `PeerDiscovery` for the **`mesh`** module, using `BroadcastChannel`. This is the cross-tab analog of in-process `MemTransport`: the bus is shared, so connections are multiplexed with per-connection ids.

Works in browsers and Node 18+. `BroadcastChannelCtor` is injectable for tests.

## Transport (`bc://`)

- **`BroadcastChannelTransport`** — Pair-like byte channel over the bus (copy-on-send, `onMessage` / `onClose`).
- **`BroadcastChannelTransportProvider`** — Scheme `bc`. Owns one endpoint `bc://<endpointId>` and one channel. `listen()` accepts logical inbound `connect` envelopes; `connect()` waits for `accept` with a bounded timeout.

Envelope: `{ v, cid, kind: 'connect'|'accept'|'data'|'close', src, dst, data? }`. Binary `data` rides structured clone.

## Discovery (query/response)

`BroadcastChannelDiscovery` uses `${base}:disc`. Presence is not a lease:

- `announce()` registers a listener that replies to `query` with `presence`. Swarm already announces before `runDiscovery()`, so the listener is up before our own query.
- `leave()` (swarm `sleep` / `destroy`) stops answering. `deactivate()` does not call `leave()`, so the listener stays (passive still accepts inbound).
- `discover()` posts `query`, collects replies for a short window (~100ms), yields a finite snapshot, returns. No heartbeat, no TTL.

A BroadcastChannel does not deliver to the object that sent, so one channel is used for both listen and post (same trick as `BroadcastIdbDagStore`).

Late tabs still connect: they query, dial `bc://…`, existing tabs accept on `listen()`.

## Usage

```typescript
import {
    BroadcastChannelTransportProvider,
    BroadcastChannelDiscovery,
} from '@hyper-hyper-space/hhs3_mesh_bc';

const transport = new BroadcastChannelTransportProvider();
await transport.listen(transport.localAddress, (t) => {
    // inbound from another tab
});

const discovery = new BroadcastChannelDiscovery({
    self: { keyId: myKeyId, addresses: [transport.localAddress] },
});
await discovery.announce(topic, discovery.self);
for await (const peer of discovery.discover(topic, ['bc'])) {
    const conn = await transport.connect(peer.addresses[0]);
    conn.send(new Uint8Array([1, 2, 3]));
}
```

## Building

To build, run the following commands at the workspace level (top directory in this repo):

```
npm install
npm run build
```
