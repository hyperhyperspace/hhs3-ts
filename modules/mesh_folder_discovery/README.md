# Mesh Folder Discovery

Node-only `PeerDiscovery` for the **`mesh`** module. Peers announce by writing a presence file under a shared directory; `discover()` returns a finite snapshot of currently-live files. There is no `file_watch` and no mesh-core change: a late process dials earlier peers from the snapshot, and those peers adopt the inbound connection.

Presence is a lease. Swarm calls `announce()` once per `activate()`, so this source keeps its own `mtime` fresh with a heartbeat (default TTL 180s, heartbeat 60s), matching `mesh_tracker_client`.

## Layout

```
<root>/<encTopic>/<encKeyId>.<instanceId>.json
```

- **`root`** defaults to `$RDB_MESH`, else `$RDB_HOME/mesh`, else `~/.rdb/mesh`.
- **`encTopic` / `encKeyId`** are base64url encodings of the topic and key id (filesystem-safe).
- **`instanceId`** is a per-process random id so the same identity in two processes does not collide.
- File content: `{ keyId, addresses, topic, updatedAt }`.

`discover()` skips the caller's own file and any file whose `mtime` is older than the TTL, scheme-filters addresses, and opportunistically deletes files older than `2 * TTL`.

## Usage

```typescript
import { FolderDiscovery } from '@hyper-hyper-space/hhs3_mesh_folder_discovery';

const discovery = new FolderDiscovery({
    self: { keyId: myKeyId, addresses: ['ws://127.0.0.1:9000'] },
});

await discovery.announce(topic, discovery.self);
for await (const peer of discovery.discover(topic, ['ws'])) {
    // connect...
}
await discovery.leave(topic, myKeyId);
await discovery.close();
```

## Building

To build, run the following commands at the workspace level (top directory in this repo):

```
npm install
npm run build
```
