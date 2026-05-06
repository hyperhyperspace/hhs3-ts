# Sync

For the protocol specification, see [SPECS.md](./SPECS.md).

DAG synchronization for HHS v3 replicas over the mesh layer. This module implements the sync protocol that moves DAG entries between peers using topic channels. Synchronization is **session-based**: each `SyncSession` targets a single DAG and manages all peer interactions for that DAG's topic.

## Symmetric Design

Every peer in a sync session runs **both** sides of the protocol simultaneously:

- **Provider** — serves headers and payloads from the local DAG in response to requests from other peers.
- **Synchronizer** — discovers divergence via frontier gossip and fetches missing entries from peers.

This means there is no client/server distinction. When two peers connect on a topic, each one advertises its frontier and each one can serve the other's requests.

## Architecture

```
             SyncSession (per DAG)
            ┌──────┬──────────┐
            │      │          │
      DagProvider  │   DagSynchronizer
      (serves)     │    (fetches)
            │      │          │
            └──────┴──────────┘
                   │
            Topic Channel(s)
              via Mesh Swarm
```

A `SyncSession` is created with a `SyncTarget` (DAG + replicated object + hash suite) and one or more `Swarm` instances. It:

- Tracks connected peers and their channels
- Routes incoming messages to the provider or synchronizer based on message type
- Reports peer issues (send failures, timeouts, validation errors) via callbacks
- Exposes diagnostics (active peer count, pending requests)

## Protocol Phases

1. **Frontier gossip** — peers exchange `new-frontier` messages to discover divergence. A peer broadcasts its frontier whenever the DAG grows and pushes back when it receives a differing frontier.

2. **Header fetch** — the synchronizer sends `header-request` to walk the remote DAG backward from unknown frontier entries down to its own frontier (`limits`). The provider responds with `header-response-meta` followed by `header-batch` messages.

3. **Payload fetch** — once headers are known, the synchronizer requests payloads via `payload-request`. If `autoPayload` was set on the header request and the walk completed, payloads are delivered inline.

4. **Validation & apply** — received entries are validated (hash verification, predecessor availability, type-level checks) before being applied to the local DAG.

## Usage

```typescript
import { createSyncSession } from '@hyper-hyper-space/hhs3_sync';

const session = createSyncSession(
    { dagId, dag, rObject, hashSuite },
    [swarm],
);

session.onPeerIssue((peerKey, issue) => {
    console.warn(`peer ${peerKey}: ${issue}`);
});

// Later:
session.destroy();
```

## Exports

| Export | Description |
|--------|-------------|
| `createSyncSession` | Creates a session that syncs a single DAG across swarms |
| `createDagProvider` | Lower-level: serves headers/payloads for a DAG |
| `createDagSynchronizer` | Lower-level: fetches and applies remote entries |
| `encode` / `decode` | Codec for sync protocol messages |
| `SyncMsg` | Union type of all protocol message types |
