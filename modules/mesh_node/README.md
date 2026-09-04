# Mesh Node

Node mesh factory for HHSv3. Builds a `Mesh` for a single network environment (`MeshScope`) with WebSocket transports (`ws` + `wss`), a folder-discovery backup, and an optional tracker layer (probed, never spawned).

## `createNodeMesh(req, opts?)`

```typescript
import { createNodeMesh } from '@hyper-hyper-space/hhs3_mesh_node';

const built = await createNodeMesh({
    scope: 'localhost',          // 'localhost' | 'internet'
    identity,                    // OwnIdentity
    // trackerAddress?, trackerKeyId?, listenAddress?  (explicit overrides)
});
// built: { mesh, discovery, listenAddresses, discoveryNotes, closeables }
```

The request takes only explicit overrides; it reads no environment variables. Callers (e.g. a CLI) map their own flags/env onto `trackerAddress` / `trackerKeyId` / `listenAddress`.

## Listen policy (advertise == listen)

The addresses peers dial are exactly the ones the Mesh listens on. A bind-all placeholder (`0.0.0.0` / `::`) is **rejected** as a listen address.

| Scope | `listenAddress` | Result |
|-------|-----------------|--------|
| `localhost` | none | `ws://127.0.0.1:<free port>` |
| `localhost` | set | that address (free port filled in if omitted) |
| `internet` | none | dial-out only: empty listen set, a warning note, tracker still used for discovery/dial |
| `internet` | set | that address |

Tracker defaults come from `@hyper-hyper-space/hhs3_mesh_tracker_client` (`resolveTrackerConfig`): the local tracker for `localhost`, the public tracker for `internet`.

NAT traversal / port forwarding is out of scope: to accept inbound on `internet`, pass a dialable `listenAddress`.
