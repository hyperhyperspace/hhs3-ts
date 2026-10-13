# Mesh Node

Node mesh factory for HHSv3. Builds a `Mesh` for one network environment (`MeshScope`) from a `MeshConfig`: WebSocket (`ws` and `wss`), WebRTC (`rtc`, werift), folder discovery, and trackers (probed, never spawned).

## `createNodeMesh(config, runtime, options?)`

```typescript
import { createNodeMesh } from '@hyper-hyper-space/hhs3_mesh_node';

const built = await createNodeMesh(
    { scope: 'internet' },
    { identity },
);
// built: { mesh, discovery, listenAddresses, trackerAddresses, discoveryNotes, closeables }
```

`config` is a `MeshConfig` whose `scope` is set. `runtime` is `{ identity, report? }`. `options` may inject `createRTCPeerConnection` and `openSignaling` for tests. The factory reads no environment variables.

## Defaults

Advertise equals listen. A bind-all address (`0.0.0.0`, `::`) is rejected.

| Omitted on `internet` | Result |
| --- | --- |
| `transports.webrtc` | listen at `wss://signal.mypeer.net:4612` |
| `transports.ws` | dial `ws://` and `wss://`, do not listen |
| `discovery` | the public tracker plus folder discovery |

| Omitted on `localhost` | Result |
| --- | --- |
| `transports.webrtc` | installed so it can dial, no `rtc://` announced |
| `transports.ws` | listen at `ws://127.0.0.1:<free port>` |
| `discovery` | `ws://127.0.0.1:4610` plus folder discovery |

`transports.webrtc: false` removes WebRTC. `{ listen: false }` keeps dialing and does not advertise. `transports.ws: false` does not dial peer `ws://` addresses; a tracker still gets its own WebSocket. `transports.broadcastChannel` is rejected on Node. A `folder` discovery entry's omitted `root` is `$RDB_HOME/mesh` or `~/.rdb/mesh`. An explicit `discovery` array replaces the default. `[]` is no discovery.

When `internet` has no WebSocket listen and WebRTC listen is off, a note says `dial-out only`.
