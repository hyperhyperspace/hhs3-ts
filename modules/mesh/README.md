# Mesh

Peer-to-peer networking for the HHS v3 sync engine. This module handles peer discovery, authenticated connection establishment, connection pooling with topic-based multiplexing, swarm management, incoming connection handling with topic negotiation, and per-swarm authorization. It defines transport-agnostic interfaces; concrete transport implementations (WebSocket, WebRTC, etc.) live in separate modules.

## Architecture

A `Mesh` instance represents a single **network environment** (e.g. local LAN, public internet, private device sync). Each `Mesh` owns:

- A **local identity** (`localKeyId`) and optional **listen addresses** for accepting incoming connections
- A `ConnectionPool` for reusing authenticated connections across topics
- A `PeerDiscovery` service for finding peers interested in a given topic
- A `PeerAuthenticator` for verifying peer identities over raw transports
- One or more `TransportProvider`s for establishing and accepting raw connections

The `Mesh` defines the endpoints and identity for listening. Each `Swarm` independently controls its own announce/leave lifecycle based on mode transitions, and its own authorization policy via an optional `PeerAuthorizer`.

Multiple `Mesh` instances can coexist (e.g. one for LAN discovery, another for internet overlay) and operate independently.

### Layers

```
Application / Sync Engine
        │
    ┌───┴───┐
    │ Swarm │  ← per-topic peer group with lifecycle modes + authorization
    └───┬───┘
        │
 ┌──────┴───────┐
 │ TopicChannel │  ← multiplexed, topic-scoped view of a connection
 └──────┬───────┘
        │
┌───────┴────────┐
│ ConnectionPool │  ← shared authenticated connections, keyed by (KeyId, endpoint)
└───────┬────────┘    routes topic data + control messages
        │
┌───────┴──────────────┐
│ AuthenticatedChannel │  ← post-handshake encrypted channel with verified identity
└───────┬──────────────┘
        │
   ┌────┴──────┐
   │ Transport │  ← raw bidirectional byte channel (WebSocket, WebRTC, TCP, …)
   └───────────┘
```

### Connection lifecycle

For **outbound** connections (initiator side), the Mesh wraps the authenticator with a topic negotiation layer. When a swarm's discovery loop finds a peer and connects:

1. Noise handshake authenticates the transport
2. Initiator sends `topic_interest(topic)` on the control channel
3. Responder checks `swarm.wouldAccept(keyId)` and replies `topic_accept` or `topic_reject`
4. If accepted, both sides add the connection to their pool and the swarms adopt the peer

For **inbound** connections (responder side), the Mesh listens on configured addresses:

1. Accept raw transport from the listener
2. Authenticate as responder (Noise handshake)
3. Await a `topic_interest` control message
4. Look up the swarm for that topic, call `wouldAccept`
5. Reply `topic_accept` or `topic_reject`
6. If accepted, add to pool; the swarm adopts via the pool's connect event

## Core interfaces

### Transport

A raw bidirectional message channel. The mesh module defines only the interface; implementations are plugged in via `TransportProvider`.

```typescript
type NetworkAddress = string;

interface Transport {
    readonly open: boolean;
    send(message: Uint8Array): void;
    close(): void;
    onMessage(callback: (message: Uint8Array) => void): void;
    onClose(callback: () => void): void;
}

interface TransportProvider {
    readonly scheme: string;
    listen(address: NetworkAddress, onConnection: (transport: Transport) => void): Promise<void>;
    connect(remote: NetworkAddress): Promise<Transport>;
    close(): void;
}
```

### PeerDiscovery

Finds peers interested in a given topic. Returns actionable `(KeyId, addresses)` pairs. Implementations may use DHT, signalling servers, static bootstrap files, or gossip.

```typescript
type TopicId = B64Hash;

interface PeerInfo {
    keyId: KeyId;
    addresses: NetworkAddress[];
}

interface PeerDiscovery {
    discover(topic: TopicId, schemes?: string[], targetPeers?: number): AsyncIterable<PeerInfo>;
    announce(topic: TopicId, self: PeerInfo): Promise<void>;
    leave(topic: TopicId, self: KeyId): Promise<void>;
}
```

The optional `schemes` parameter filters results to peers reachable via specific transport schemes. The optional `targetPeers` parameter hints how many unique peers the caller wants; composite sources like `DiscoveryStack` use it to stop early.

### PeerAuthenticator

Verifies that the entity on the other end of a raw `Transport` holds the private key corresponding to a claimed public identity. The local signing key is bound at construction time.

```typescript
interface AuthenticatedChannel {
    readonly remotePeer: PublicKey;
    readonly remoteKeyId: KeyId;
    readonly open: boolean;
    send(message: Uint8Array): void;
    close(): void;
    onMessage(callback: (message: Uint8Array) => void): void;
    onClose(callback: () => void): void;
}

interface PeerAuthenticator {
    authenticate(
        transport: Transport,
        role: 'initiator' | 'responder',
        expectedRemote?: KeyId
    ): Promise<AuthenticatedChannel>;
}
```

A concrete implementation (`createNoiseAuthenticator`) is included and described below.

### PeerAuthorizer

Per-swarm authorization policy. If provided in the swarm config, the authorizer is consulted before accepting any peer — both for outbound discovery candidates and inbound connections via topic negotiation.

```typescript
interface PeerAuthorizer {
    authorize(keyId: KeyId): Promise<boolean>;
}
```

### ConnectionPool

Shared pool of authenticated connections, keyed by `(KeyId, NetworkAddress)` pairs so multiple devices for the same identity get separate connections. Provides topic-multiplexed channels via `openTopic()` and dispatches control-channel messages to registered handlers.

```typescript
class ConnectionPool {
    add(channel: AuthenticatedChannel, endpoint: NetworkAddress): PooledConnection;
    get(keyId: KeyId, endpoint: NetworkAddress): PooledConnection | undefined;
    getByKeyId(keyId: KeyId): PooledConnection[];
    all(): PooledConnection[];
    remove(keyId: KeyId, endpoint: NetworkAddress): void;

    openTopic(keyId: KeyId, endpoint: NetworkAddress, topic: TopicId): TopicChannel;

    onConnect(callback: (conn: PooledConnection) => void): void;
    onDisconnect(callback: (connKey: string) => void): void;
    onControlMessage(callback: (connKey: string, peerId: KeyId, endpoint: NetworkAddress, payload: Uint8Array) => void): void;

    size(): number;
    close(): void;
}
```

### TopicChannel

A multiplexed, topic-scoped view of a pooled connection. Messages are framed with a lightweight header (`[1 byte type][2 bytes topic-length BE][topic UTF-8][payload]`) so the pool can route them to the right channel. A second frame type (`0x02 = control`) carries topic-negotiation messages outside the topic-data path.

```typescript
interface TopicChannel {
    readonly topic: TopicId;
    readonly peerId: KeyId;
    readonly endpoint: NetworkAddress;
    readonly open: boolean;
    send(message: Uint8Array): void;
    onMessage(callback: (message: Uint8Array) => void): void;
    close(): void;
    onClose(callback: () => void): void;
}
```

### Swarm

Manages a peer group for a single topic. Supports three lifecycle modes:

- **`dormant`**: ignores all pool activity and discovery; `wouldAccept` returns false
- **`passive`**: adopts peers already in the pool (with authorizer check if set) but does not initiate new connections
- **`active`**: runs discovery (with authorizer filtering), actively connects to new peers, and announces via the discovery service

Mode transitions drive the announce/leave lifecycle. The `Mesh` provides a `localPeer` (identity + listen addresses) to each swarm; the swarm decides when to advertise:

- `activate()` — calls `discovery.announce(topic, localPeer)` and starts discovery
- `sleep()` — calls `discovery.leave(topic, localPeer.keyId)` and drops all peers
- `destroy()` — calls `discovery.leave(topic, localPeer.keyId)` and tears down

The `targetPeers` config caps total swarm peer count (not just outbound connections). Both `adoptPeer` and `wouldAccept` enforce this cap.

```typescript
interface Swarm {
    readonly topic: TopicId;
    readonly mode:  SwarmMode;

    activate(): void;
    deactivate(): void;
    sleep(): void;
    destroy(): void;

    peers(): SwarmPeer[];
    onPeerJoin(callback: (peer: SwarmPeer) => void): void;
    onPeerLeave(callback: (peer: SwarmPeer) => void): void;
    blockPeer(keyId: KeyId, endpoint: NetworkAddress): void;
    wouldAccept(keyId: KeyId): Promise<boolean>;
    adopt(keyId: KeyId, endpoint: NetworkAddress): boolean;
}
```

### Mesh

Top-level facade that ties everything together for a single network environment. When `localKeyId` and `listenAddresses` are provided, the Mesh listens for incoming connections, authenticates them, and runs topic negotiation before adding them to the pool. Outbound connections created through swarms are automatically wrapped with the same negotiation protocol.

The Mesh also handles `topic_interest` control messages on already-pooled connections, enabling the pool-reuse discovery pattern.

```typescript
interface MeshConfig {
    transports:             TransportProvider[];
    discovery:              PeerDiscovery;
    authenticator:          PeerAuthenticator;
    localKeyId?:            KeyId;
    listenAddresses?:       NetworkAddress[];
    negotiationTimeoutMs?:  number;   // default: 10 000
}

class Mesh {
    readonly pool: ConnectionPool;
    constructor(config: MeshConfig);

    createSwarm(topic: TopicId, opts?: {
        targetPeers?: number;
        mode?: SwarmMode;
        authorizer?: PeerAuthorizer;
    }): Swarm;
    swarms(): Swarm[];
    close(): void;
}
```

## Topic negotiation protocol

After the Noise handshake, the initiator and responder exchange a single control message to agree on a topic. The control sub-protocol is carried inside `MSG_TYPE_CONTROL` (0x02) frames:

```
Control payload: [1 byte ctrl-sub-type][topic UTF-8 bytes]
  0x01 = topic_interest
  0x02 = topic_accept
  0x03 = topic_reject
```

Each `topic_interest` names **one topic**. If the application wants to synchronize multiple topics over the same connection, it uses the opt-in `PoolReuseDiscovery` layer to send additional `topic_interest` messages on already-established connections. This keeps the negotiation simple and gives the application explicit control over whether topic cross-correlation is acceptable from a privacy standpoint.

## Discovery

The module ships three concrete `PeerDiscovery` implementations.

### StaticDiscovery

A read-only source that yields a fixed `PeerInfo[]` provided at construction, **only for matching topics**. Results are shuffled (Fisher-Yates) on each `discover()` call so that epidemic gossip converges effectively. `announce()` and `leave()` are no-ops.

```typescript
import { StaticDiscovery } from '@hyper-hyper-space/hhs3_mesh';

const bootstrap = new StaticDiscovery(
    [{ keyId: 'abc…', addresses: ['ws://seed1.example.com:9000'] }],
    [replicaRootTopic],                  // only yield peers when this topic is requested
);
```

- **Topic-scoped**: The `topics` constructor argument is required. Peers are only yielded when the requested topic is in the list, preventing bootstrap peers from leaking into unrelated per-object swarms.
- **Scheme filtering**: If `schemes` is provided to `discover()`, only peers with at least one matching address are yielded, and their address lists are filtered accordingly.

### DiscoveryStack

A priority-based composite that chains multiple `PeerDiscovery` sources. Layers are grouped by numeric priority (lower = higher priority) and processed in ascending order. Within a priority group, all sources run **in parallel** and their results are merged into a single stream.

```typescript
import { DiscoveryStack } from '@hyper-hyper-space/hhs3_mesh';

const stack = new DiscoveryStack([
    { source: replicaDiscovery, priority: 0 },   // best: synced peer set
    { source: trackerA,         priority: 10 },   // fallback: tracker constellation
    { source: trackerB,         priority: 10 },   // (parallel with trackerA)
    { source: bootstrap,        priority: 20 },   // last resort: static bootstrap
]);

// Request up to 10 unique peers
for await (const peer of stack.discover(myTopic, undefined, 10)) {
    // ...
}
```

**`discover(topic, schemes?, targetPeers?)` logic:**

- Process priority groups in ascending order
- Within a group, race all sources in parallel and yield peers as they arrive
- Deduplicate peers by `(keyId, address)` across all groups
- If `targetPeers` is given, **stop** as soon as that many unique peers have been yielded
- If the current group is exhausted but the count is below `targetPeers`, fall through to the next group
- If all groups are exhausted, return what was found — `targetPeers` is a goal, not a guarantee
- If `targetPeers` is omitted, defaults to 10

**`announce()` and `leave()`:** Broadcast to **all** layers via `Promise.allSettled`, so one failing source does not break the others.

### PoolReuseDiscovery

An opt-in `PeerDiscovery` implementation that probes **existing authenticated connections** in the `ConnectionPool` for additional topics. For each pooled connection, it sends a `topic_interest` control message and waits for `topic_accept` or `topic_reject`. Accepted peers are yielded as `PeerInfo` so the swarm can adopt them without opening a new transport connection.

```typescript
import { PoolReuseDiscovery, DiscoveryStack } from '@hyper-hyper-space/hhs3_mesh';

const reuseDiscovery = new PoolReuseDiscovery(mesh.pool);

const stack = new DiscoveryStack([
    { source: reuseDiscovery,   priority: 0 },    // prefer reusing existing connections
    { source: trackerDiscovery, priority: 10 },   // fall back to new connections
]);
```

Applications concerned about topic cross-correlation privacy should **omit** this layer. `announce()` and `leave()` are no-ops since the pool handles presence implicitly.

## Noise Authenticator

The module includes a concrete `PeerAuthenticator` implementation based on a Noise-like 3-message (1.5 RTT) handshake with **initiator identity protection**: the initiator's identity is never revealed unless the responder first proves it holds the expected key.

1. **Msg1** (initiator → responder): random session nonce + KEM preference list (anonymous -- no identity)
2. **Msg2** (responder → initiator): signing identity + chosen KEM + ephemeral KEM public key + transcript signature
3. **Msg3** (initiator → responder): KEM ciphertext + AEAD-encrypted initiator identity and transcript signature

The initiator verifies the responder's identity after Msg2. If `expectedRemote` is set and doesn't match, the initiator aborts without sending Msg3 -- its identity is never exposed. This enables a Trust-On-First-Use (TOFU) model: on first contact, omit `expectedRemote` to learn the remote's `KeyId` from the returned `AuthenticatedChannel`, then store it for future connections.

After the handshake, both sides derive independent send/receive session keys via HKDF and encrypt all subsequent traffic with ChaCha20-Poly1305.

KEM suite negotiation allows graceful upgrades: the initiator sends a ranked preference list, the responder picks the best common suite.

```typescript
interface NoiseAuthenticatorConfig {
    localKey: { publicKey: PublicKey; secretKey: Uint8Array };
    signingName: SigningName;   // e.g. 'ed25519', 'ml-dsa-65', 'ed25519+ml-dsa-65'
    kemPrefs: KemName[];        // e.g. ['x25519-hkdf+ml-kem-768', 'x25519-hkdf']
}

const authenticator = createNoiseAuthenticator(config);
```

## Usage

### Initiator only (no listening)

```typescript
import { Mesh, createNoiseAuthenticator } from '@hyper-hyper-space/hhs3_mesh';
import { ed25519, sha256, stringToUint8Array } from '@hyper-hyper-space/hhs3_crypto';

const { publicKey, secretKey } = await ed25519.generateKeyPair();
const identity = { suite: 'ed25519', key: publicKey };

const authenticator = createNoiseAuthenticator({
    localKey: { publicKey: identity, secretKey },
    signingName: 'ed25519',
    kemPrefs: ['x25519-hkdf'],
});

const mesh = new Mesh({
    transports: [wsTransportProvider],
    discovery: myDiscoveryImpl,
    authenticator,
});

const topic = sha256.hashToB64(stringToUint8Array('my-shared-document'));
const swarm = mesh.createSwarm(topic);
swarm.activate();

swarm.onPeerJoin((peer) => {
    peer.channel.onMessage((msg) => {
        // handle sync messages from this peer
    });
});

mesh.close();
```

### Full node (listening + authorization)

```typescript
import {
    Mesh, createNoiseAuthenticator, PoolReuseDiscovery, DiscoveryStack,
} from '@hyper-hyper-space/hhs3_mesh';
import {
    ed25519, sha256, stringToUint8Array, keyIdFromPublicKey,
} from '@hyper-hyper-space/hhs3_crypto';

const { publicKey, secretKey } = await ed25519.generateKeyPair();
const identity = { suite: 'ed25519', key: publicKey };
const localKeyId = keyIdFromPublicKey(identity, sha256);

const authenticator = createNoiseAuthenticator({
    localKey: { publicKey: identity, secretKey },
    signingName: 'ed25519',
    kemPrefs: ['x25519-hkdf'],
});

// Mesh listens on the given addresses
const mesh = new Mesh({
    transports: [wsTransportProvider],
    discovery: new DiscoveryStack([
        { source: new PoolReuseDiscovery(mesh.pool), priority: 0 },
        { source: trackerDiscovery,                   priority: 10 },
    ]),
    authenticator,
    localKeyId,
    listenAddresses: ['ws://0.0.0.0:9000'],
});

const topic = sha256.hashToB64(stringToUint8Array('my-shared-document'));

// Per-swarm authorization: only accept known peers
const authorizer = {
    authorize: async (keyId) => trustedPeerSet.has(keyId),
};

const swarm = mesh.createSwarm(topic, { authorizer });
swarm.activate();   // announces via discovery, starts connecting to peers

swarm.onPeerJoin((peer) => {
    peer.channel.onMessage((msg) => { /* ... */ });
});

mesh.close();
```

## Building

To build, run the following commands at the workspace level (top directory in this repo):

```
npm install
npm run build
```

## Testing

The test suite covers all layers: transport, mux framing, control channel protocol, connection pool (including control dispatch), swarm lifecycle, per-swarm authorization, announce/leave lifecycle, mesh facade, end-to-end listen with topic negotiation (including rejection and encrypted data flow), noise authenticator handshake, static discovery, discovery stack, and pool-reuse discovery. To run it, first build the workspace and then within `modules/mesh`:

```
npm run test
```
