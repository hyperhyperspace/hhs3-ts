# rpack and rhost: architecture

How the tools in [DESIGN.md](./DESIGN.md) map onto the existing modules. Most of what they do already exists. The new code is the release file, the diff, the host that composes the rest, and a few changes in rdb that must land before the first release.

## Modules

```
rdb_tools (Node)          bins rdb, rpack, rhost, rkeys; the TTY prompter; staging
  ├─ rdb_repl             \sync and \project call rhost's services
  └─ rhost_node           the Node platform: SQLite stores, keystore, node mesh, lock, run/rhost.sock; openApp, initApp
       └─ rhost           the app, browser-safe: its hosts, the in-process client, the protocol dispatcher
            ├─ rpack      release files, install, the release producer, browser-safe
            │    └─ rdb_runtime, rdb_lang, rdb
            ├─ rdb_projection
            ├─ sync, mesh
            └─ rhost_client
rhost_client              the client interface and the wire protocol; no dependencies
rhost_client_node         reaches a host over run/rhost.sock and its projection (rhost_client, crypto, mvt, better-sqlite3)
rhost_browser             the browser platform: IndexedDB stores, browser mesh, OPFS file mounts, Web Lock, BroadcastChannel
rhost_client_browser      reaches a host from another tab, over that BroadcastChannel
```

The layout follows `mesh` / `mesh_node` / `mesh_browser`: a browser-safe core, and a package per platform.

- **rpack** stays a library, as the stub already is. It depends on none of sync, mesh, projection or rhost.
- **rhost** is browser-safe because apps embed it: an Electron main process, a Node server, a browser tab. Only the platform differs (store backend, projection target, mesh, keystore, config files, the client listener), and it is injected. It installs releases with rpack's library.
- **rhost_node** is the Node platform, and holds the Node pieces rdb_tools used to own: `KeyStore` and the node mesh factory. rdb_tools' REPL imports them from here. A host's replica store is a `SqliteDagDb`.
- **rdb_tools** keeps the bins and the TTY prompter. The CLIs only parse arguments and call rpack, rhost, rhost_node and rhost_client_node. Staging lives here too, because it composes both: rpack's library produces the planned release, and rhost builds and deploys the staging app.
- **rhost_client** is the interface an app programs against, with no dependencies, so an app beside a running host doesn't install the replication stack. rhost implements it in process, `rhost_client_node` over the socket, and `rhost_client_browser` over a BroadcastChannel.
- **rhost_browser** is the browser platform. A host's replica is an `IdbDagDb` (the default `level` index, the same as `SqliteDagDb`). The projection is an `IdbTarget`. File mounts are a `HandleDirectory` on the origin private file system. The mesh factory is `createBrowserMesh(config, runtime)` on the host's merged `MeshConfig`. `serve(app, host)` holds a Web Lock and answers other tabs on a BroadcastChannel.
- **rdb_repl** gave its sync and projection wiring to rhost (`sync.ts` and `projection.ts`): the `SyncMeshFactory` types, the allow authorizer and its column lookup, mesh attach and teardown (`startDatabaseSync`, `fetchDatabase`), and projection open (`openProjection`). `\sync` and `\project` call them, so the REPL and a running host set things up the same way. rdb_repl keeps the parsing, its session registries and the output.

## Changes in rdb

These change what every replica computes, so they landed before any release exists (step 1 of the order of work, done):

- **Schema versions.** The schema create and update payloads carry a strict semver `version` (`rschema/payload.ts`). Validation requires an update's version to be above every version at its position, which by induction covers its whole past; `RSchemaView.getVersions()` exposes the versions at a position. The TS API and C-SQL leave the version optional: `RSchemaImpl.create` and `CREATE SCHEMA` default to `0.0.1`, `updateSchema` and `ALTER SCHEMA` to the next patch above the versions at `AT`. `updateSchema` takes `{ version?, note?, at? }` and still accepts the positional `(note?, at?)` form. The renderers always write `VERSION`, so dumps replay exact versions.
- **Shared semver.** `semver.ts` moved to `modules/rdb/src/semver.ts` (rcatalog's module re-exports it) so rschema can use it without an import cycle; it gained `nextPatch` and `compareParsedSemver`.
- **Conflicts.** Among concurrent writes to a slot, `resolveSlot` (`rschema/resolve.ts`) picks the higher version, then the larger entry hash. When deployed definitions clash on a group name, `assignNames` (`rdb/instantiate.ts`) gives the name to the definition from the higher deployed release that holds it, then the larger definition hash.
- **Forward check.** `update-catalog` (`rdb/validate_ops.ts`) accepts any target that isn't at or below a deployed release, instead of requiring every deployed release to be below it. Resolution already merges several maximal deployed releases: each member's target is the union of their versions. So a state like "2.0.0 + 1.5.1" needs nothing else, and the planner deploys the union.
- **Adoption range.** `semver.ts` has an upper-bound range, `<3.0.0` (or `<3`), so a host admits every release below its next major. An older release can never take the data past what the code knows, whereas `^2` on a 2.x build would hold a 1.x patch.
- **Source mode.** `parseScript(text, { catalogVersionOptional: true })` accepts `CREATE CATALOG` without `VERSION`; the binder takes it from `LangBindContext.defaultCatalogVersion()`. `BY` was already optional (the context's default author signs). `CREATE SCHEMA` without `VERSION` takes `defaultSchemaVersion` when target-catalog.sql is evaluated (the catalog's version), and otherwise `0.0.1`. A parsed `CREATE CATALOG` carries `versionSpan`, the span of its `VERSION` clause, so a tool can remove the clause in place.
- **`rdb --keystore <path>`** in rdb_tools: the flag form of `RDB_KEYSTORE`.

## rpack

### Release files

A release file is a manifest plus the objects in dependency order (the schemas sorted by id, then the catalog). Each object carries its create payload and its entries in a canonical topological order. The format is in the addendum. The code is `format.ts`, `export.ts`, `install.ts` and `verify.ts` in `modules/rpack/src`.

- **Export** (`exportRelease`) takes the catalog entries in the release's causal past, declares included. For the schemas, it collects every version pinned by a release in that past (`releaseState(r).groups` over `releasesAt([release])`), per schema, and exports the entries at or below the union of those pins. A schema that is declared but not yet pinned ships its create only. Entries above the pins stay out, so schema work that was never released doesn't ship.
- **Canonical order.** Within an object, entries are emitted by repeatedly taking the smallest hash whose prevs are all emitted. Together with sorted ids, sorted `prevs` and sorted `parents`, the same release always gives the same file.
- **Serialization.** Files are pretty-printed JSON with sorted keys, a 2-space indent and a trailing newline, so they are deterministic and diff well in the catalog repository. Parsing accepts any layout.
- **Install** (`installRelease`) is the synchronizer's apply sequence without the network: `createObject` for a missing root, after checking that the factory computes the recorded id; then, for each missing entry, the hash recomputed from `payload` and `prevs` (a mismatch is rejected before the DAG is touched), `validatePayload`, and `applyPayload`. Present entries are skipped, so installing twice is a no-op. A catalog needs only its schemas present first. It needs no change to the replica. `dag.copy` can't be used, because it skips type validation.
- **Verify** (`verifyRelease`) checks the format, installs into a fresh in-memory replica, checks the manifest, and summarizes the release, its parents and each schema's versions. That replica uses SHA-256, so a catalog with another `hashAlgorithm` can't be verified yet.
- **Self-contained files.** Each file holds its release's whole past. Consecutive files overlap, which git's delta compression absorbs.
- **Name tags.** The tag in a file name is the first 8 hex digits of the release hash. Hashes are standard base64, and its `/` and `+` aren't safe in file names.
- **The `rpack` bin** in rdb_tools has `init`, `new`, `set base`, `status`, `build`, `release` and `log` over the producer below, `stage`, `verify`, and `export`, which writes a release made in the `rdb` REPL. It finds the repository and the version folder with `locate` (`rdb_tools/src/rpack/locate.ts`), walking up from the current folder or `-C` to `rpack.json`; `work/<version>/` in the path names the folder.

### The producer

- **Model.** Both sides of the diff are a `CatalogModel`: the catalog header, the params, the schemas with their tables, and the groups with their schema and bindings by name. It holds no object ids, and its keys are real. Equality compares effective values, with the defaults `RSchemaView` applies.
- **Source.** rpack parses `target-catalog.sql` in source mode and checks its forms. It then evaluates it with `executeText(session, text, { source: { catalogVersion, schemaVersion } })` in an in-memory `RdbRuntime`, with stand-in keys (a `MemoryKeyVault`) bound to its `$labels`. `catalogVersion` is the release version when one is expected, `schemaVersion` is the catalog's version, and a schema that omits `VERSION` is created at it; a stated one is kept. In the model read back, every stand-in key is replaced by the real one, looked up by label in the developer's keystore. Public keys need no passphrase. The model holds no versions: `ReadSource.schemaVersions` has each schema `VERSION` the text states, and the draft creates or updates that schema at it. A changed schema's version must be above its versions at the parents; an unchanged one may state only a version it has.
- **Released state.** `releases/*` is installed into a second in-memory workspace (`Released.open`). The release DAG comes from the manifests.
- **Parents.** A folder's `version.json` names them in `base`, as `rpack new` or `rpack set base` wrote them: the `--base` releases or, by default, the releases below the version that aren't in the past of another one. The state the diff starts from is the parents' merge: each schema at the union of the parents' pins, and the groups from `foldParents`.
- **Starting source.** `rpack new` and `rpack set base` write `target-catalog.sql` with `writeSource`: the highest-version parent's `.released/target-catalog.sql`, edited to describe the parents' merged state. `stripCatalogVersion` then removes a `CREATE CATALOG` `VERSION` equal to a parent's version; schema `VERSION`s stay. `set base` first checks the folder with `workingDifferences`, which lists what writing it fresh would lose, and refuses unless `--force`. The rules are in the addendum.
- **A released folder** is drafted in a scratch replica: `Released.open` over every release file except the folder's release and its `descendants`. Otherwise the draft would find its own version already released. `status`, `build`, `stage` and the re-release all draft there, and a folder still makes its release when the draft has the release's parents and gives `.released/update.sql` byte for byte.
- **Re-release.** `releaseVersion` with `force` drafts and signs the new release in the scratch replica, then each released descendant by version from its own folder, on its `version.json`'s base with replaced names mapped to new ones, and with its note. `produceRelease` signs into the scratch replica, so each dependent is drafted on the ones made before it. Nothing is written until every one is made and `confirm` (given a `ReleasePreview`) agrees. The writes are in the order DESIGN.md gives, with the replaced files removed last.
- **Diff.** `upgrade-manual.sql` runs first, on the parents' merged state. Its rules are compiled against the parents' views with rdb_lang's `compileMigrationRules`. Then `diffSchema` returns migration rules or refusals, and `diffCatalog` returns the catalog release (`add`, `changes`, `params`) or refusals. Both match by name. The rules are in the addendum.
- **Publish.** `RSchema.updateSchema(rules, key, { version, at })` for each changed schema, at the union of the parents' pins and with the release's version. Then `RCatalog.publishRelease(spec, key, parents)`, which chains a `declare` when the release introduces schemas. The first release creates the schemas and the catalog instead.
- **Checks.** A draft applies its rules to the parents' tables with rdb's `applyMigrationRules`, which gives the validator's reasons, and needs no key. A release is verified in a fresh replica from its file's bytes, and the model read back from it must equal `target-catalog.sql`'s.
- **Draft output.** rdb_lang's `renderMigrationRule` and its source-form renderers, with pins written `LATEST`, as `target-catalog.sql` states them.
- **Files.** Project files go through an injected interface, `RpackProject`, with paths relative to the repository; rdb_tools implements it over the file system. rpack reads no git history. `rpack build` writes `work/<version>/build/update.sql` when the draft is accepted and prints that it did, and writes nothing on a refusal. `rpack release` writes, in order: the release file; `work/<version>/.released/` with `target-catalog.sql`, `upgrade-manual.sql` and `update.sql`; and the folder's entry in `rpack.json`'s `released`. It never writes the working files. Whether a released folder's files changed since release is a byte comparison with `.released/`, plus `version.json`'s base against the release's parents and its note against the release's note.

## Staging

Staging is built in rdb_tools (`src/rpack/stage.ts`) with rhost's own machinery, for one version folder. It builds in `work/<version>/.stage.building/` and swaps that in for `stage/`, so a failed stage leaves the previous app or nothing.

- **`staging.json`.** Parsed first, strictly: `mesh`, checked as `host.json` checks it, and `allow`, `projection`, `autoDeploy` and `params` for `app.json`. Any other field is an error, and so is a param the staged release doesn't declare.
- **Order.** The releases in the staged folder's past, sorted by version then hash. A released folder that still makes its release (`describeFolder`) takes its release's past and the release. Any other folder takes its base's pasts and the base, then the planned release. Versions increase along every path, so a merge's parents are both deployed before it. Two releases with one version are refused, since an app ships one file per catalog.
- **Shipping.** Each step writes that release's file into `catalogs/` and an `app.json` with the params it declares (`keystore` is `keys.json`), then `app.create('default', { key: 'admin', mesh })` for the first step and `Host.deploy()` after. A new param takes its value from the staged folder's `staging.json`, and an identity param defaults to `$me`.
- **Rows.** Each release's `test-data.sql` comes from its folder, found through `rpack.json`'s `released`; a release with no folder gets none. It runs on the host's runtime, with the stand-in keys unlocked, `admin` selected, and ref auto-update on, so a new identity reaches the groups bound to it before they verify its signature. Only `INSERT`, `UPDATE`, `DELETE` and `BUNDLE` are accepted, and a refused row names its file and line.
- **Keys.** The app's own keystore, `stage/keys.json`, holds one stand-in per `$label` the samples use, plus `admin`, each with an empty passphrase. The host signs with `admin`, and its `host.json` key has no passphrase source.
- **The planned release.** `produceVersion` drafts and signs the folder's release in a scratch replica and returns its file, which is written only into the staging app's `catalogs/`. With releases, the developer keystore signs it. Before the first release the staging stand-ins are the creators, so no passphrase is needed. A draft with refusals stops the stage. Replaying a released, unchanged folder signs nothing.
- **Projection.** `Host.project()` opens the projection and stops it again, so `db/data.sqlite` is current and the host isn't running.
- **Report.** Around the last deploy: each member group's `computeDelta` from before the deploy to after. Row changes summarize per table (rows gone, changed, back), a dropped table's rows are counted from the schema change at the version before the drop, and ops voided or reinstated are listed with their reasons. New groups are reported as created. A deploy's problems fail the stage.

Two additions in rhost make this work: `update` and `deploy` pass values for params the release declares that the database doesn't have yet, and `project()` projects without starting the host.

## rhost

### Apps and hosts

An app (`Rhost`) is its release files and `app.json`. A host (`Host`) is a folder under `hosts/` holding its `host.json`, its replica and its projection. On Node that's `host.json`, `rdb/replica.rdb`, `db/data.sqlite` (the default `projection.path`), any file mounts (`files/<name>` by convention), and `run/` for the serving process's lock, socket and log. The config can't name a path at or inside `host.json`, `rdb` or `run`. Hosts share only the app's release files and `app.json`; the keystore is the user's unless `app.json` names one. Each host has:

- **Its own replica.** The workspace's root index names objects after their create payloads, so two joined databases that their admins both named `app` would collide in one store. Separate stores also stay small, and removing a host is deleting its folder. The cost is a copy of the catalog and schema entries per host.
- **Its own key.** `host.json`'s `key` records the label, key id and public key of the key chosen at create or join, and its passphrase source. rhost unlocks it as `#<keyId>`, so a relabeled key still works, and checks that the unlocked pair matches the recorded public key. `Rhost.identity()` caches unlocked keys by key id, so hosts that share a key unlock it once. A key missing from the keystore is an error naming the host, the label, the key id and the keystore (`platform.keystoreLocation`).
- **Its own mesh.** A swarm adopts every connection that enters its mesh's pool, whatever topic the connection was opened for (`onPoolConnect` in `mesh/src/swarm.ts`). Within one database that's intended: the catalog, the schemas and the groups share peers. Across hosts, it would let a peer met in one host learn about the others. One mesh per database is also what `\sync start` does. `host.json`'s `mesh` is a `MeshConfig`: scope, transports, and discovery. `create` and `join` refuse a WebSocket listen port another host of the app uses. The cost is one tracker connection per host, and one port when listening.
- **Its own start and stop.** The host is the unit: `Host.start()` takes the platform's lock for it, updates it, and starts its sync and projection, and `stop()` reverses that. `Rhost.start(name?)` and `stop(name?)` act on one host or on every one. With the CLI, each host also runs in its own process.

`host.json` holds the database id, the catalog, whether this device created it, the key, the mesh settings, and optionally its own `params`, `autoDeploy` or `projection`. The effective config merges `app.json`'s `mesh` with the host's (`mergeMeshConfig` in `rhost/src/config.ts`): the host's `scope` wins, each transport key on the host replaces the app's, and a host `discovery` array replaces the app's. `releases`, `keystore` and `allow` are always the app's. Both files are parsed strictly (`parseAppConfig`, `parseHostRecord`): an unknown field, at any depth, is a `ConfigError` naming the file and the field.

`create` and `join` build a host out of sight: the platform's `hosts.create(name, build)` hands `build` a fresh replica, and publishes the host only once `build` returns its record. On Node, that's a `hosts/.<name>.pending/` folder renamed into place after `host.json` is written. A failed create or join leaves nothing behind.

### Platform

The platform supplies everything specific to the device:

```ts
interface HostPlatform {
    keyVault(): Promise<KeyVault>;
    readonly keystoreLocation?: string;           // named in a missing-key error
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

The app has no store of its own: `hosts.openReplica(name)` returns the `DagBackend` of one host's replica, and rhost opens an `RdbRuntime` on it. `passphrase` turns a host key's source (`prompt`, `env:<VAR>`, or absent) into the passphrase. There's no watch: a host added later starts when asked. `listen` accepts client connections to a running host; rhost's `serveClient` answers each one.

For each catalog, rhost ships the release file with the highest version. Two files with the same version for one catalog are an error.

- **Node** (`rhost_node`):
  - `hosts/<name>/` folders, published by rename;
  - for each host, a `SqliteDagDb` on its `rdb/replica.rdb`, its own `createNodeMesh`, and a `SqliteTarget` with capture on;
  - one `KeyStore` for the app: `app.json`'s `keystore`, relative to the app folder, or else the user's (`defaultKeystorePath`: `RDB_KEYSTORE`, `$RDB_HOME/keys.json`, `~/.rdb/keys.json`), the one rdb, rpack and rkeys use;
  - `passphrase(key)` asks once per key id, so hosts sharing a key are asked once, and `rememberPassphrase` seeds that cache when the CLI has just created the key;
  - `openApp(dir)`, which reads `app.json`, and `initApp(dir, { releases, params }, prompter)`, which is `rhost init`;
  - the parent folder of every file it opens in a host (the replica, the projection, the lock, the socket, the log) is created when missing;
  - `hosts/<name>/run/rhost.lock`, holding the pid of the process serving the host. A lock whose pid is gone is taken over.
  - `hosts/<name>/run/rhost.sock`, the Unix socket `listen` serves: mode `0600` (set through the umask before binding, and checked after), created last on start, closed and removed first on stop. A stale file is removed before binding, which is safe because the lock is held. A path over the system's limit (103 bytes on macOS, 107 on Linux) is refused with the limit named, and not worked around. On Windows the same folder maps to a named pipe (`socketPathFor`, untested).
- **Browser** (`rhost_browser`):
  - host records in IndexedDB (`rhost-<app>-hosts`), published only once create or join returns;
  - one `IdbDagDb` per host (`rhost-<app>-<host>-store`), opened directly at the default `level` index;
  - one `IdbKeyStore` for the app (`rhost-<app>-keys`), the same scrypt and ChaCha20-Poly1305 records as the Node keystore;
  - `createBrowserMesh(config, runtime)` on the merged `MeshConfig`. A `ws` listen address and folder discovery fail with the browser factory's error;
  - an `IdbTarget` (`rhost-<app>-<host>-data`) with capture on. `projection.indexes` and `projection.indexPub` are installed by rhost; a declaration that sets `options` is refused;
  - each file mount is a `HandleDirectory` at `rhost/<app>/<host>/<mount path>` in the origin private file system;
  - a Web Lock per host (`rhost-<app>-<host>`). `serve` queues when it is held and takes over if the holder closes. `stop` sets a flag so the queued tabs do not start. On the serving tab the lock reenters, so `start` and `project` do not deadlock on it;
  - `listen` on a BroadcastChannel of the same name. It is not the mesh channel and not `IdbTarget`'s `hhs3-rdb-idb:` ping.

### Update, start and stop

`create(name, { key, passphrase?, mesh, catalog?, name? })` and `join(id, name, { key, passphrase?, mesh })` are the only ways a host appears, and neither starts it. Both resolve the key in the keystore and check the network settings before building anything. `create` installs the shipped release, builds the payload with `RDbImpl.create` (a seed from the session, the host's key as creator, the params resolved against the release's declarations), creates it, deploys the release with `deployCatalogRelease`, and sets the adoption range. That is what C-SQL's `CREATE DATABASE` does. `join` fetches the genesis with `fetchDatabase`, as `\sync fetch` does, matches its catalog id against the shipped files, and installs that one; sync brings the rest.

`Host.update()`:

1. Install the shipped release of the host's catalog.
2. Set the adoption range to everything below the next major, `<(M+1).0.0`, and adopt.
3. Deploy when it's due. Call `deployCatalogRelease` when all of these hold:
   - the shipped release's version is above every deployed one;
   - `autoDeploy` allows the bump from the highest deployed version;
   - the key may deploy, meaning the RDb declares no creators or `isCreator(keyId)` holds;
   - every param the release declares that the database doesn't have yet has a value in the effective `params`.

   Otherwise the report's `notDeployed` says which of these failed first, and `missingParams` lists the params without a value, with their types. The key is unlocked only to deploy. A group whose `ALLOW DEPLOY IF` still rejects the deploy is reported as an error.

`Host.start()` takes the host's lock, runs `update`, then starts two services:

- **Sync.** Build the host's mesh from its merged `mesh` config, compute the authorizer from the app's `allow` (validating each column query first), then `startDatabaseSync`, which attaches the mesh and calls `setRuntimeConfig` and `startSync`.
- **Projection.** `openProjection` with the host's key as writer, registering that key and reconciling the index spec.

Last, it listens for clients with `platform.listen`. If any step fails, whatever started is stopped and the lock released; with no name given, `Rhost.start()` records the failure as the host's `lastError` and starts the others.

`deploy({ release?, params? })` deploys the shipped release, or a release of the host's catalog named by version, version-tag or hash, whatever `autoDeploy` says. It keeps the authority check, and refuses a release that is deployed already, or below a deployed one (`isReleaseBelow` in the catalog index). The rdb planner would otherwise skip such a deploy without a word. `params` sets the params the release newly declares, in `app.json`'s forms (`parseParamText` reads the CLI's `--param` values); a name the release doesn't declare, or one the database already has, is refused, and a param still without a value fails the deploy, naming it. `paramNeeds({ release?, params? })` lists the params that would still be missing, with their types, so the CLI can ask for them first. `rhost deploy` runs it in the CLI's own process on the shared store, and a running host picks it up, as below.

The CLI runs each host in its own process. `rhost serve <name>` runs one in the foreground and stops it on SIGINT or SIGTERM. `rhost start` unlocks each host's key in the foreground with `Rhost.identity()`, so a missing key or a wrong passphrase is reported before any spawn. It then spawns a detached `rhost --app <dir> serve <name> --passphrase-stdin` with the same node and loader, writes the passphrase to its stdin, and waits for a `ready` or `error` line on fd 3. That pipe closes after the line: it only reports startup. `rhost stop` sends SIGTERM to the pid in the host's lock, and `rhost remove` does the same before deleting the folder.

Each host's status (`HostStatus`) is `catalogStatus(rdb)` plus the shipped release, the peers and the last error, recomputed on the RDb's events. Its deployed releases are the maximal ones, shown together as "2.0.0 + 1.5.1". `upgradeRequired` means a held release has a higher major than the shipped one. `hostBehind` means the shipped release is above every deployed one, and then `notDeployed` and `missingParams` say why the next update won't deploy it, as `update` reports them. For a host that isn't running, status sets the adoption range only in the runtime config, so computing it admits nothing. `rhost status` asks a running host over `run/rhost.sock`, which adds the peers and the last error that only the serving process knows. For a stopped one it computes the status from the store. It reads `running (pid N)` from the lock.

### Several processes on one host

A running host doesn't need its store to itself. Each DAG in `rdb/replica.rdb` is a `WatcherSqliteDagStore`, which watches the SQLite WAL and fires the same listeners for other processes' writes as for its own. So a row written from the REPL reaches the host's sync and projection subscriptions.

A deploy from another process, such as `UPDATE CATALOG` in the REPL, reaches it the same way. The RDb's DAG grows, its membership subscription runs a reconcile, and the reconcile materializes the new groups with `createObject`. That finds their DAGs already in the replica and loads them, which also registers them in the root index, so the root index being read only at open doesn't matter here. The projection then follows the new members and schema versions.

`[RDB_TOOLS56]` tests this end to end. The host runs in the test process and the `rdb` bin runs as a child on the same store, with the same keystore through `RDB_KEYSTORE`. A row the child writes appears in `db/data.sqlite`, a row the app writes there reaches the child's `SELECT`, and the child's deploy of 1.1.0 shows in the running host's status, with `doc_pages` gaining the new column.

## rhost_client

`RhostClient` (in `rhost_client`) is all async:

- `me()`;
- `registerKey(publicKey)`;
- `events.since` and `events.watch`;
- `status()` and `watchStatus(listener)`;
- `close()`.

Statuses and events are plain types: `HostStatus` (what rhost computes; rhost's own type is this one), `StoppedStatus`, and `ClientEvent`, which has the op-event log's fields plus the id.

### The protocol

One JSON object per line, and read-only: it carries live process state, and nothing that signs.

- Requests are `{ id, method: 'status' | 'watchStatus' }` and `{ id, method: 'unwatch', watch }`.
- The host answers `{ id, result }` or `{ id, error }`, and a watch pushes `{ id, event }`.
- `serveClient(host, connection)` in rhost answers over any `Connection { send, onLine, onClose }`. A closed connection ends its watches.

### Where each function runs

- **In process** (`host.client`, rhost):
  - `status` and `watchStatus` come from the host (`status()`, `onStatus`);
  - `me`, `registerKey` and `events` go through the running projection (`registerKey`, `opEvents`), and fail while the host is stopped;
  - `events.watch` uses `subscribeOpEvents` only as a trigger, and reads each batch back by id, so every watch keeps its own cursor.
- **Node** (`openClient(hostDir, { db? })`, `rhost_client_node`):
  - `status` and `watchStatus` use `run/rhost.sock`. Without a running process, `status` is a `StoppedStatus` from `host.json`, and the watch reports that, then reconnects.
  - `me` reads the key's label, key id and public key from `host.json` (`hostKey(hostDir)`), with no keystore, and interns it.
  - `registerKey` computes the key id with `keyIdFromPublicKey`, and interns it into `rdb_keys` the way the target does (existing id, or the next one, in one immediate transaction). The test checks that both sides give the same id.
  - `events` read `rdb_op_events`, with `watch` polling the highest id. `[RHOST_CLIENT_NODE01]` checks they equal the projection's.
  - The projection file is `projectionPath(hostDir)`: `projection.path` from `host.json` when it overrides that section, otherwise from the app's `app.json`, two folders up. It's read synchronously, on first use.
  - These three work while the host is stopped. Before its first start there is no projection file: `events` are empty, and `me` and `registerKey` fail.
- **Browser** (`openClient(app, host)`, `rhost_client_browser`):
  - `status` and `watchStatus` use the client BroadcastChannel. No answer within a second is a `StoppedStatus` from the host record, and a watch reports that, then keeps listening.
  - `me` reads the key from the host record, with no keystore, and interns it into the projection's `keys` store the way `IdbTarget` does.
  - `registerKey` and `events` use that store and `op_events`. `watch` polls. They work while the host is stopped. Before the first `project` or `start` there is no projection database: `events` are empty, and `me` and `registerKey` fail.
  - A tab that never called `serve` is only a client. The in-process client, in the tab that runs rhost, is unchanged.

## Order of work

1. The changes in rdb (done).
2. Release file export and install, with `rpack verify` (done). The catalog in `examples/editor.sql` is the first test case.
3. The rhost core and the Node platform, with hosts: `init`, `create`, `join`, `remove`, `update`, `status`, and `start`, `serve` and `stop` with one process per host. rdb_repl's sync and projection wiring moved into it, and a test runs two processes on one host's store (done). Later, apps and hosts: `app.json` and `host.json`, a key and network settings per host, keys in the user's keystore, strict config parsing, and deploys held for a missing param, with `deploy --param` (done).
4. The client and the Node split: rhost_client, the in-process client and the dispatcher in rhost, rhost_node with `run/rhost.sock`, rhost_client_node, and the commands `deploy`, `events` and `whoami` (done).
5. The producer: source mode, work folders and their bases, the diffs, `init`, `new`, `set base`, `status`, `build`, `release` with re-release, and `log` (done). The code is in `modules/rpack/src`: `project.ts`, `model.ts`, `source.ts`, `released.ts`, `schema_diff.ts`, `catalog_diff.ts`, `draft.ts`, `format_draft.ts`, `produce.ts`, `write_source.ts` and `commands.ts`, and the bin's file-system project is `rdb_tools/src/rpack/node_project.ts`.
6. Staging: `rpack stage` in a version folder replays its past through rhost, inserts each release's `test-data.sql` after deploy, and projects once (done).
7. The browser platform: rhost_browser (IndexedDB stores and keys, `IdbDagDb`, `IdbTarget`, `HandleDirectory`, `createBrowserMesh`, Web Locks, a BroadcastChannel) and rhost_client_browser (done).

After these, `rpack test` could run queries with expected results over the staging of every upgrade path.

## Addendum: release file format

```json
{
  "format": 1,
  "manifest": {
    "name": "editor",
    "catalog": "<catalog id>",
    "version": "1.2.0",
    "release": "<release hash>",
    "parents": ["<release hash>"],
    "note": "..."
  },
  "objects": [
    {
      "id": "<object id>",
      "create": { },
      "entries": [ { "hash": "<hash>", "prevs": ["<hash>"], "payload": { } } ]
    }
  ]
}
```

- **`format`** is the file format's version, `1`. A reader rejects any other.
- **Layout.** Keys are sorted at every level, with a 2-space indent and a trailing newline. `objects` holds the schemas sorted by id, then the catalog. Each object's `entries` are in canonical topological order, each entry's `prevs` are sorted, and so are the manifest's `parents`. `note` is present only when the release has one.
- **Payloads are verbatim**, in canonical JSON. Signatures are payload fields (`author`, `signature`), and rpack never re-signs. Schema creates are unsigned and content-addressed.
- **No metadata.** An entry's hash covers only its payload and predecessors (`createEntry` in `dag/src/dag.ts`). Entry metadata isn't hashed, and these objects don't use it, so it isn't exported.
- **Hashes are checked, never trusted.** The `hash` field makes the file readable and corruption visible. Install recomputes every hash and rejects a mismatch. It then checks that the manifest's catalog and release exist, and that the catalog's name and the release's version, parents and note match the manifest.

## Addendum: the diff

- **Tables**, matched by name. A new table is `add-table`, a missing one `drop-table`. For a table in both, compare its columns and settings.
- **Columns**, matched by name. A new column is `add-column`, a missing one `drop-column`. A column whose definition changed is refused; the reset goes in `upgrade-manual.sql`.
- **Table settings.** Foreign keys, restrictions and concurrent deletes each become one `set-*` rule when their effective values differ. The identity-provider flag can't change in place.
- **Rule order.** One update per schema: the rules of `upgrade-manual.sql` first, then the generated ones in five phases, each sorted by table and column:
  1. `add-column`;
  2. `add-table`, ordered so a new table comes after the new tables its foreign keys and `EXISTS` atoms name. A table on a cycle is added without them, and phase 3 sets them;
  3. `set-fks`, `set-restrictions` and `set-concurrent-deletes`, plus the settings phase 2 deferred. Dropped tables that reference each other in a cycle have their references cleared here;
  4. `drop-table`, referencing tables first;
  5. `drop-column`.

  Everything is added before anything refers to it, and after phase 3 nothing that stays refers to what phases 4 and 5 remove. The draft applies the rules with rdb's `applyMigrationRules`, and the result must equal `target-catalog.sql`'s tables.
- **Groups**, matched by name. A new group is added with its definition, after the groups it binds. A group whose definition changed, or that is missing, is refused.
- **Pins.** A schema the release updates is pinned at the new update, a new schema at its create, and any other schema at the minimal cover of its parents' pins. A group whose pin in any parent differs from its schema's pin gets a change. That covers every group the parents disagree on, which rcatalog requires.
- **Params.** New ones are declared on the release. A removed or retyped param is refused.
- **Merges.** Take 2.0.1 with parents 2.0.0 and 1.5.1. Its schema updates are appended at the union of the parents' pins, and the catalog release sets every group the parents disagree on. `rpack new` wrote the parents' merge into `target-catalog.sql`, so whatever of 1.5.1's changes the release leaves out is an edit to `target-catalog.sql`, undone like any other difference.
- **Warnings.** These don't stop the release:
  - `drop-table`, `drop-column`, and any `set-*` rule, when the bump is not major. Restrictions are included because rpack can't tell loosening from tightening.
  - new params, which a deploy then needs values for;
  - lower releases outside the new release's past.

## Addendum: writing source

`writeSource(text, target)` edits `text` until it describes `target`, touching only what differs. `rpack new` and `rpack set base` call it with the highest-version parent's `.released/target-catalog.sql` and the parents' merged model.

- **Units.** Schema statements, the tables of a schema, the columns of a table (a foreign key is part of its column, as `REFERENCES`), the options of a table, the params, and the groups. The parser's spans locate each one.
- **Edits.** An unchanged unit is kept verbatim, comments and layout included. A changed unit is replaced by its source form, rendered by rdb_lang without `VERSION`, `AT` or `BY`, with keys as `$labels` from the keystore. A new unit is inserted after its last sibling, with that sibling's indentation: new schemas go before `CREATE CATALOG`, and a new param goes into `PARAMS` or a new `PARAMS` clause. A unit that's gone is removed with its separator and the comment lines directly above it. A table whose identity provider changed is replaced whole.
- **Check.** Reading the result back must give `target`.
- **No copy.** A release without a work folder, such as one written by `rpack export`, starts from empty text and is rendered whole.
