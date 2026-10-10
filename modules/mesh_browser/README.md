# Mesh Browser

Browser mesh factory for HHSv3. Builds a `Mesh` for a single network environment (`MeshScope`) with dial-only WebSocket transports (`ws` + `wss`), a WebRTC transport (`rtc`), a `BroadcastChannel` transport and discovery backup for tab-to-tab sync, and an optional tracker layer (probed, never spawned).

## `createBrowserMesh(req, opts?)`

```typescript
import { createBrowserMesh } from '@hyper-hyper-space/hhs3_mesh_browser';

const built = await createBrowserMesh({
    scope: 'internet',           // 'localhost' | 'internet'
    identity,                    // OwnIdentity
    // trackerAddress?, trackerKeyId?, listenAddress?, signalUrl?
});
// built: { mesh, discovery, listenAddresses, trackerAddresses, discoveryNotes, closeables }
```

`opts` may inject a `BroadcastChannelCtor` and/or `WebSocketCtor` (used in tests / non-DOM hosts).

## Listen and advertise

`listenAddresses` always includes the tab-local `bc://` address. A `signalUrl` (`wss://` with no endpoint id) also registers an `rtc://` listen address. The WebRTC provider is still installed without `signalUrl`, so the tab can dial peers that published one.

On `internet`, the tracker is given the `rtc://` address when `signalUrl` is set, and an empty address list otherwise. It is never given `bc://`.
