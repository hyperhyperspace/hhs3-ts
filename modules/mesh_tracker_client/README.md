# mesh_tracker_client

Tracker-based peer discovery client for HHSv3. Implements the `PeerDiscovery` interface from the `mesh` module by connecting to a remote tracker server over an authenticated Transport channel (Noise handshake).

## Protocol

All messages are JSON-encoded over an `AuthenticatedChannel`. Each message type operates on lists of topics, allowing heartbeats and bulk operations to collapse into a single round-trip.

**Client to server:**

- `ANNOUNCE` — register/refresh presence for one or more topics, with a requested TTL per topic
- `QUERY` — fetch peers for one or more topics, optionally filtered by transport scheme
- `LEAVE` — deregister from one or more topics

**Server to client:**

- `ANNOUNCE_ACK` — confirmed TTLs (the server may clamp to its configured min/max bounds)
- `QUERY_RESPONSE` — peers keyed by topic
- `LEAVE_ACK` — confirmation
- `ERROR` — error message

## Connect-on-demand

The client does not keep a persistent connection. Each operation (announce, query, leave, heartbeat) opens a fresh connection, performs a Noise handshake, exchanges one request/response, and disconnects. This keeps server resources proportional to active requests, avoids reconnection logic, and is NAT-friendly.

A periodic heartbeat re-announces all active topics in a single ANNOUNCE message to keep registrations alive before their TTL expires.

## Usage

```typescript
import { TrackerClient } from '@hyper-hyper-space/hhs3_mesh_tracker_client';

const client = new TrackerClient({
    trackerAddress: 'ws://tracker.example.com:4433',
    trackerKeyId: knownTrackerKeyId,   // optional; TOFU if omitted
    transportProvider: wsProvider,
    authenticator: noiseAuth,
    localPeer: { keyId: myKeyId, addresses: ['ws://me:1234'] },
    announceTtl: 180,                  // seconds (default 180)
    heartbeatInterval: 60_000,         // ms (default 60000)
});

await client.announce(topic, localPeer);
for await (const peer of client.discover(topic)) { /* ... */ }
await client.leave(topic, myKeyId);
await client.close();
```

## Building

```
npm install
npm run build
```

## Testing

```
npm run test
```
