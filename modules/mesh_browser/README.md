# Mesh Browser

Browser mesh factory for HHSv3. Builds a `Mesh` for a single network environment (`MeshScope`) with dial-only WebSocket transports (`ws` + `wss`), a `BroadcastChannel` transport and discovery backup for tab-to-tab sync, and an optional tracker layer (probed, never spawned).

## `createBrowserMesh(req, opts?)`

```typescript
import { createBrowserMesh } from '@hyper-hyper-space/hhs3_mesh_browser';

const built = await createBrowserMesh({
    scope: 'internet',           // 'localhost' | 'internet'
    identity,                    // OwnIdentity
    // trackerAddress?, trackerKeyId?, listenAddress?
});
// built: { mesh, discovery, listenAddresses, discoveryNotes, closeables }
```

`opts` may inject a `BroadcastChannelCtor` and/or `WebSocketCtor` (used in tests / non-DOM hosts).

## Listen and advertise

Browsers cannot accept inbound sockets, so `listenAddresses` is always the tab-local `bc://` address. Within the BroadcastChannel network, advertise == listen (`bc://` is announced for tab-to-tab discovery).

On `internet`, the tracker is given an **empty-address** local peer: the public tracker learns the identity's presence but not the meaningless `bc://` address. WebSocket transports remain available for dialing peers discovered via the tracker.
