# Mesh

Peer-to-peer networking for the HHS v3 sync engine. This module handles peer discovery, authenticated connection establishment, connection pooling with topic-based multiplexing, and swarm management. It defines transport-agnostic interfaces; concrete transport implementations (WebSocket, WebRTC, etc.) live in separate modules.

## Architecture

A `Mesh` instance represents a single **network environment** (e.g. local LAN, public internet, private device sync). Each `Mesh` owns:

- A `ConnectionPool` for reusing authenticated connections across topics
- A `PeerDiscovery` service for finding peers interested in a given topic
- A `PeerAuthenticator` for verifying peer identities over raw transports
- One or more `TransportProvider`s for establishing raw connections

Multiple `Mesh` instances can coexist (e.g. one for LAN discovery, another for internet overlay) and operate independently.

### Layers

```
Application / Sync Engine
        │
    ┌───┴───┐
    │ Swarm │  ← per-topic peer group with lifecycle modes
    └───┬───┘
        │
  ┌─────┴───────┐
  │ TopicChannel│  ← multiplexed, topic-scoped view of a connection
  └─────┬───────┘
        │
 ┌──────┴────────┐
 │ConnectionPool │  ← shared authenticated connections, keyed by (KeyId, endpoint)
 └──────┬────────┘
        │
 ┌──────┴─────────────┐
 │AuthenticatedChannel│  ← post-handshake encrypted channel with verified identity
 └──────┬─────────────┘
        │
   ┌────┴──────┐
   │ Transport │  ← raw bidirectional byte channel (WebSocket, WebRTC, TCP, …)
   └───────────┘
```

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
    discover(topic: TopicId, schemes?: string[]): AsyncIterable<PeerInfo>;
    announce(topic: TopicId, self: PeerInfo): Promise<void>;
    leave(topic: TopicId, self: KeyId): Promise<void>;
}
```

The optional `schemes` parameter filters results to peers reachable via specific transport schemes.

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
        expectedRemote?: KeyId
    ): Promise<AuthenticatedChannel>;
}
```

A concrete implementation (`createNoiseAuthenticator`) is included and described below.

### ConnectionPool

Shared pool of authenticated connections, keyed by `(KeyId, NetworkAddress)` pairs so multiple devices for the same identity get separate connections. Provides topic-multiplexed channels via `openTopic()`.

```typescript
class ConnectionPool {
    add(channel: AuthenticatedChannel, endpoint: NetworkAddress): PooledConnection;
    get(keyId: KeyId, endpoint: NetworkAddress): PooledConnection | undefined;
    getByKeyId(keyId: KeyId): PooledConnection[];
    remove(keyId: KeyId, endpoint: NetworkAddress): void;

    openTopic(topic: TopicId, keyId: KeyId, endpoint: NetworkAddress): TopicChannel;

    onConnect(callback: (conn: PooledConnection) => void): void;
    onDisconnect(callback: (connKey: string) => void): void;

    close(): void;
}
```

### TopicChannel

A multiplexed, topic-scoped view of a pooled connection. Messages are framed with a lightweight header (`[1 byte type][2 bytes topic-length BE][topic UTF-8][payload]`) so the pool can route them to the right channel.

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

- **`dormant`**: ignores all pool activity and discovery
- **`passive`**: accepts peers already in the pool but does not initiate new connections
- **`active`**: runs discovery and actively connects to new peers

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
}
```

### Mesh

Top-level facade that ties everything together for a single network environment.

```typescript
interface MeshConfig {
    transports:    TransportProvider[];
    discovery:     PeerDiscovery;
    authenticator: PeerAuthenticator;
}

class Mesh {
    readonly pool: ConnectionPool;
    constructor(config: MeshConfig);

    createSwarm(topic: TopicId, opts?: { targetPeers?: number; mode?: SwarmMode }): Swarm;
    swarms(): Swarm[];
    close(): void;
}
```

## Noise Authenticator

The module includes a concrete `PeerAuthenticator` implementation based on a Noise-like 3-message (1.5 RTT) handshake:

1. **Msg1** (initiator → responder): signing identity + KEM preference list + transcript signature
2. **Msg2** (responder → initiator): signing identity + chosen KEM + ephemeral KEM public key + transcript signature
3. **Msg3** (initiator → responder): KEM ciphertext + AEAD confirmation

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

```typescript
import { Mesh, createNoiseAuthenticator } from '@hyper-hyper-space/hhs3_mesh';
import { ed25519, sha256, stringToUint8Array } from '@hyper-hyper-space/hhs3_crypto';

// Generate identity
const { publicKey, secretKey } = await ed25519.generateKeyPair();
const identity = { suite: 'ed25519', key: publicKey };

// Create authenticator with local identity bound
const authenticator = createNoiseAuthenticator({
    localKey: { publicKey: identity, secretKey },
    signingName: 'ed25519',
    kemPrefs: ['x25519-hkdf'],
});

// Create mesh for a network environment
const mesh = new Mesh({
    transports: [wsTransportProvider],   // from mesh_ws or another transport module
    discovery: myDiscoveryImpl,           // application-provided
    authenticator,
});

// Join a topic
const topic = sha256.hashToB64(stringToUint8Array('my-shared-document'));
const swarm = mesh.createSwarm(topic, { mode: 'active' });

swarm.onPeerJoin((peer) => {
    peer.channel.onMessage((msg) => {
        // handle sync messages from this peer
    });
});

// When done
mesh.close();
```

## Building

To build, run the following commands at the workspace level (top directory in this repo):

```
npm install
npm run build
```

## Testing

The test suite covers all layers (transport, mux framing, connection pool, swarm lifecycle, mesh facade, and noise authenticator handshake). To run it, first build the workspace and then within `modules/mesh`:

```
npm run test
```
