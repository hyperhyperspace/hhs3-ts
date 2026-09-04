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

4. **Validation & apply** — received entries are validated (hash verification, predecessor availability, type-level checks) before being applied to the local DAG. If the `RObject` reports foreign dependencies via `extractForeignDeps` (entries that must exist on another object), and `SyncTarget.ctx` is provided, the synchronizer defers the entry — skipping it rather than rejecting it. Retry is driven by the referenced object's `subscribe` (history grew) and `ctx.subscribeNewObject` (the target appeared in the replica map). A foreign-dep target must be `getObject`-visible: roots via `createObject`/`fetchObject`; a non-root only if it is a **direct ref-advance target** (`registerObject`). Nested children are not registered just because they exist.

   Two things follow from missing cross-object data being a *defer*, not a reject:

   - **Throws from validate/apply defer, they do not reject.** Only a type-level `valid: false` (or a `ValidationRejectedError`) rejects an entry. Any other throw out of `extractForeignDeps`, the dep gate, `validatePayload` or `applyPayload` — a bound object not yet present, a schema version not yet resolvable, an index entry "not found" — is treated as a defer: the header is kept and retried on the next wake, and one throwing hash never aborts the rest of the validation pass. (Trade-off: a payload that throws for a non-transient reason is retried on every pass rather than dropped; the `sync.defer` trace with the error surfaces it. Retries are bounded by the header window.)

   - **Pin-or-inherit invariant.** `extractForeignDeps` only needs to list `requiredHashes` for cross-object versions that are *not* already implied by the op's own history. Every cross-object hash validation reads at a position is either pinned by the op there (a ref-advance / observe carries the version it reads) or by a causally-prior gated op (a create pins the schema genesis; same-DAG prevs are always gated, so a local hash implies its whole history is local). Ordinary row/bundle ops therefore only gate on foreign object *presence*. **Any new op type that reads cross-object state at validation must preserve this: pin the hashes it reads, or be causally after an op that did.** Genesis pins (a create's own foreign versions, e.g. an RTableGroup's pinned schema version) are the base case and are declared separately by `RObjectFactory.extractCreationForeignDeps`, gated at object materialization rather than per op.

## Usage

```typescript
import { createSyncSession } from '@hyper-hyper-space/hhs3_sync';

const session = createSyncSession(
    {
        dagId, dag, rObject, hashSuite,
        ctx,  // optional: RContext for foreign-dep lookup and wakeup
    },
    [swarm],
    {
        // optional: structured issue sink (IssueReport). Every field is
        // optional; severity hints how the mesh/swarm should react
        // (high = terminate, moderate = consider if frequent, low = ignore).
        report: (issue) => {
            console.warn(`[${issue.severity ?? 'low'}] ${issue.kind} ${issue.keyId ?? ''}`, issue);
        },
    },
);

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
