# rhost

Hosts one app on one device. The app ships release files; each of its hosts is one [Rdb](../rdb) database with its own replica, signing key, mesh peer and projection. Hosts share nothing but the app's releases and settings. rhost installs the shipped releases, creates or joins databases, deploys releases where a host's key has the authority, and syncs and projects each host. [DESIGN.md](../rpack/DESIGN.md) describes the tool, and [ARCHITECTURE.md](../rpack/ARCHITECTURE.md) maps it onto the modules.

rhost is browser-safe: everything specific to the device comes from a `HostPlatform`.

- The Node platform, with `openApp(dir)`, is [rhost_node](../rhost_node), and the `rhost` bin is in [rdb_tools](../rdb_tools).
- The client interface an app programs against is [rhost_client](../rhost_client); rhost implements it in process.
- [rhost_client_node](../rhost_client_node) reaches a host that runs in another process.

## Apps and hosts

```ts
import { Rhost } from '@hyper-hyper-space/hhs3_rhost';

const app = await Rhost.open(config, platform);

const editor = await app.create('default', { key: 'me', sync: { scope: 'internet' } }); // from the shipped release
const gaming = await app.join(databaseId, 'gaming', { key: 'me', sync: { scope: 'localhost' } }); // fetched from its peers
await editor.start();                                 // or app.start() for every host

const [status] = await app.status('default');
editor.onStatus((s) => { /* deployed, held, members, peers, notDeployed, lastError */ });

await app.update();                                   // install the shipped releases, deploy when due
await editor.deploy({ params: { moderator: '$me' } }); // deploy now, whatever autoDeploy says
await app.remove('gaming');                           // this device's copy only
await app.close();
```

- **`create(name?, { key, sync, passphrase?, catalog?, name? })`** creates a database from the shipped release, with the host's key as creator and the app's params, and deploys it.
  - `key` is a label or `#<key id>` in the platform's keystore. `host.json` records its label, key id and public key, and `passphrase`, the source of its passphrase: `prompt`, `env:<VAR>`, or absent for a key stored without one.
  - `sync` is the host's network settings, written to `host.json` as given (see [Config](#config)). Two hosts of an app can't listen on the same port.
  - The host name defaults to `default`, and the database name to the host name.
- **`join(id, name?, { key, sync, passphrase? })`** fetches the database's genesis over a temporary mesh with the host's own `sync` settings, checks that the app ships its catalog, and installs that release. Sync brings the rest once the host starts.
- **`update(name?)`** installs the shipped release (a no-op when its entries are present), sets the adoption range to everything below the next major (`<(M+1).0.0`), and deploys the release when all of these hold:
  - its version is above every deployed one, and `autoDeploy` allows the bump;
  - the host's key may deploy (the database has no creators, or the key is one);
  - `params` sets every param the release newly declares.

  Otherwise the report's `notDeployed` says why, such as `1.1.0 needs a value for :moderator (identity), which params don't set`, and `missingParams` lists the params by name and type. A hold isn't an error: `start()` runs `update()`, and the host still starts and syncs on the release it has.
- **`status(name?)`** is the database's catalog status plus the shipped release and two flags: `upgradeRequired` (a held release has a higher major than the shipped one) and `hostBehind` (the shipped release is above every deployed one). A host that's behind also gets `notDeployed`, and `missingParams` when params are why. A running host adds its peers and last error.
- **`deploy({ release?, params? })`** deploys the shipped release, or the one `release` names (`M.m.p`, `M.m.p-<tag>`, `#<hash>`), whatever `autoDeploy` says. It keeps the authority check, and refuses a release that is deployed already or below a deployed one.
  - `params` gives values for the params the release newly declares, in the same forms as `app.json`. A name the release doesn't declare, or one the database already has, is refused. Nothing is written: the database keeps the values.
  - `paramNeeds({ release?, params? })` lists the params such a deploy would still need.
- **`project()`** brings the host's projection up to date and stops it again, so the target is current without the host running. A running host already projects, so it does nothing.
- **`start(name?)` and `stop(name?)`** start and stop hosts one by one; without a name they act on every host.
  - A started host holds the platform's lock for it, updates, starts sync with its `sync` settings and the authorizer from the app's `allow`, and opens its projection with the host's key as writer.
  - Last, it listens for clients when the platform can (`run/rhost.sock` on Node).
  - Neither `create` nor `join` starts a host.
- **`remove(name)`** stops the host, closes its replica and deletes it.

A host's key is unlocked once per app, by key id: relabeling it in the keystore changes nothing, and the public key `host.json` records must match. When the keystore has no such key, the error names the host, the label, the short key id and the keystore (`platform.keystoreLocation`).

## Clients

`host.client` is the [rhost_client](../rhost_client) interface in process:

- `status()` and `watchStatus(listener)` come from the host.
- `me()` is the key `host.json` records; its `id` and `registerKey(publicKey)` and `events` go through the running projection, and fail while it is stopped.

`serveClient(host, connection)` answers the same protocol over any line-based `Connection`. The platform's `listen` hands it each connection. The protocol is read-only: `status`, `watchStatus` and `unwatch`, and nothing that signs.

## Config

`app.json` holds what the app ships and how every host runs it:

```json
{
  "releases": "catalogs/",
  "params": { "admin": "$me" },
  "autoDeploy": "minor",
  "allow": ["user.identities.keyId"],
  "projection": {
    "path": "db/data.sqlite",
    "indexPub": true,
    "indexes": [
      { "name": "by_label", "group": "editor", "table": "notes", "columns": ["label"] }
    ],
    "files": [
      { "name": "media", "path": "files/media" },
      { "name": "attachments", "path": "files/attachments" }
    ]
  }
}
```

- `params`: identity params take `$me` (the host's key) or `$<label>` (a key in the keystore); value params take a JSON literal of the declared type. Every name must be a param of the shipped release.
- `autoDeploy` is `minor` (the default: patch and minor bumps), `major`, or `none`.
- `allow` is which peers every host accepts. Each entry is `everyone` or `group.table.column [where <condition>]`, and `start` checks the columns against the deployed catalog. Without `allow`, every peer is accepted.
- `projection.path` is a relative path in the host folder, without `..`; `rhost init` writes `db/data.sqlite`. On Node, missing parent folders are created when the projection opens. `projection.indexes` and `projection.indexPub` are the projection index spec, in rdb names, shared by every host. Omitting both leaves an already installed spec alone. An explicit `"indexes": []` installs that empty spec.
- `projection.files` mounts FILES members as folders (see [File mounts](#file-mounts)). Names are identifiers, unique in the list; paths follow the same rules as `projection.path`, and are unique, not inside one another, and not the projection's own path. `files/<name>` keeps them together.
- No path may be or lie inside `host.json`, `rdb` or `run`, which rhost keeps for the host record, the replica, and the running process's lock, socket and log.
- `keystore` is where the platform keeps keys, when not in its default place. On Node it's a path relative to the app folder; without it, the user's keystore.

Each host's `host.json` is written by `create` or `join`:

```json
{
  "database": "z7xH...",
  "catalog": "editor",
  "created": true,
  "key": { "label": "me", "keyId": "...", "publicKey": "...", "passphrase": "prompt" },
  "sync": { "scope": "internet", "tracker": "wss://tracker.example.org", "listen": "ws://0.0.0.0:7400" },
  "autoDeploy": "none"
}
```

- `sync.scope` is `internet` or `localhost`; `tracker`, `trackerKey` and `listen` are optional, like `\sync`'s flags. Without a `listen` port, a free one is picked when the host starts.
- A host may override `params`, `autoDeploy` and `projection`. It can't set `releases`, `keystore` or `allow`.

Both files are strict: every object in them, nested ones included, refuses a field it doesn't know and names it, and a missing required field is reported as missing. `parseAppConfig`, `parseHostRecord` and `effectiveConfig` do the checking and merging.

For each catalog, the newest release file ships. Two files with the same version for one catalog are an error.

## File mounts

A running host mounts each `projection.files` entry with [rdb_projection's file mounts](../rdb_projection#file-mounts), with the host's key as writer:

```
hosts/default/
  db/data.sqlite
  files/media/
    common/logo.png           any writer adds or removes here
    keys/1/notes.pdf          owned by rdb_keys.id 1 (read-only on this host)
    keys/4/draft.txt          this host's key: files dropped here are uploaded
    .hhs/state.json           mount state (reserved)
```

A new mount has `common/`, `keys/` and this host's `keys/<id>/`, even while the key can't write yet, so files can be dropped there right away. This host's folder stays when it empties; other key folders go away with their last file.

A FILES that a later release adds needs its entry here to be mounted. Until the deployed catalog has a FILES with the entry's name, the mount is `pending`, and it attaches by itself once a deploy brings it. `project()` mounts too, for one pass. `status` lists every mount of a running host under `files`:

```json
"files": [
  { "name": "media", "path": "files/media", "state": "mounted", "files": 42, "missing": 3, "localOnly": 1, "waiting": 0, "writable": true },
  { "name": "attachments", "path": "files/attachments", "state": "pending" }
]
```

`missing` counts files whose bytes haven't all arrived yet; `localOnly` counts files on disk that are never synced (outside `common/` and this key's folder, in another key's folder, or with a name the path rules reject). `waiting` counts the changes in `common/` and this key's folder not uploaded yet: while the key can't write, edits, deletes and new files stay as they are, and they're uploaded once it can. `rhost status` prints one `files` line per mount.

## Platform

```ts
interface HostPlatform {
    keyVault(): Promise<KeyVault>;
    readonly keystoreLocation?: string;          // named in errors about missing keys
    passphrase(key: HostKeyConfig): Promise<string>;
    readReleases(folder: string): Promise<ReleaseFile[]>;
    hosts: HostStore;   // list, read, create(name, build), openReplica(name), remove
    meshFactory: SyncMeshFactory;
    projectionTarget(host: string, config: ProjectionConfig): Promise<BidirectionalTarget>;
    filesDirectory(host: string, mount: FilesMountConfig): Promise<FileDirectory>;
    acquireLock?(host: string): Promise<() => Promise<void>>;
    listen?(host: string, onConnection: (connection: Connection) => void): Promise<() => Promise<void>>;
}
```

`hosts.create(name, build)` hands `build` a fresh replica backend and publishes the host only once `build` returns its record, so a failed create or join leaves nothing behind.

## Services

The REPL's `\sync` and `\project` use the same services as rhost:

- `SyncMeshFactory` and its request and result types;
- `createAllowAuthorizer(sources, lookup)`, `columnLookup(session)` and `parseAllowSource(text)`;
- `startDatabaseSync(...)`, which attaches a built mesh, starts the database's sync, and returns a handle with `peers()` and `stop()`;
- `fetchDatabase(...)`, which fetches a genesis over a temporary mesh;
- `openProjection(...)`, which opens an `RdbProjection`, optionally registering the writer's key, reconciling an index spec and mounting file folders.

## Test

```
npm test
```

The tests run on the catalog in [`examples/editor.sql`](../rdb/examples/editor.sql), released at 1.0.0, 1.1.0 and 2.0.0, over a memory platform and a shared in-memory mesh. A 1.1.0 that declares `:moderator` drives the param hold, and one that adds `FILES media` drives the two-host mount test, on `MemoryDirectory` folders.
