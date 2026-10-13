# Mesh Browser

Browser mesh factory for HHSv3. Builds a `Mesh` for one network environment (`MeshScope`) from a `MeshConfig`: dial-only `ws` and `wss`, WebRTC, BroadcastChannel for tab-to-tab sync, and trackers (probed, never spawned).

## `createBrowserMesh(config, runtime, options?)`

```typescript
import { createBrowserMesh } from '@hyper-hyper-space/hhs3_mesh_browser';

const built = await createBrowserMesh(
    { scope: 'internet' },
    { identity },
);
// built: { mesh, discovery, listenAddresses, trackerAddresses, discoveryNotes, closeables }
```

`options` may inject a `BroadcastChannelCtor`, a `WebSocketCtor`, `createRTCPeerConnection`, and `openSignaling`.

## Defaults

`transports.broadcastChannel` is installed unless it is `false`, and the mesh listens on `bc://<id>`. Omitted `base` is `hhs3-mesh-bc`. `transports.ws.listen` is rejected: a tab cannot bind a port. WebRTC matches the Node defaults: on `internet` it listens at `wss://signal.mypeer.net:4612`; on `localhost` it can dial and does not advertise. `webrtc: false` removes it. A `broadcast-channel` discovery entry requires the transport.

Omitted `discovery` is the scope's tracker plus `{ "type": "broadcast-channel" }`, unless BroadcastChannel was turned off. A `folder` discovery entry is rejected.

On `internet`, trackers are given the public listen addresses (`rtc://`, and `ws://` when one is configured) and never `bc://`.
