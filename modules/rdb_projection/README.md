# Rdb Projection

Reactive **supervisor** that keeps a replica-wide relational projection of an [Rdb](../rdb) `RDb` in sync. The concrete `[MaterializationTarget](../rdb_adapter)` (SQLite, IDB, in-memory, …) is injected by the host. Built on [rdb_adapter](../rdb_adapter)'s pure planners and database-level orchestrators.

## What it does

An `RDb`'s member table groups are computed from the catalog releases deployed into it, so it is the natural unit of projection. `RdbProjection.open(rdb, ctx, target, { writer })` resolves the members the replica holds into **one shared target** and:

- **materializes** every member with group-qualified table names (`<group>_<table>`, where `<group>` is the member name from `rdb.getMemberGroupNames()`: an identifier, unique within the database, so tables from different groups never collide);
- **resolves cross-group FKs to serial ids** when the referenced group is co-projected (otherwise a `row_hash` passthrough);
- **ingests local edits in commit order**, advancing co-projected cross-group refs on demand so an observer's cross-group FKs and `exists` reads validate against sibling groups ingested in the same pass; `fkBundling` (a per-member option, default on) bundles consecutive FK-linked inserts atomically;
- **interns authors and identity keys** into a shared `rdb_keys` table as `author_key_id` / `<col>_key_id` (`registerKey` / `keyHashForId` / `publicKeyForId` on the projection);
- **stays in sync reactively** — a debounced, coalesced `syncDatabase` fires on three triggers: each member group's `subscribe` (the rdb side advanced), the target's optional `ChangeSignalSource` (local edits are waiting), and the `RDb`'s own `subscribe` (a release was deployed, possibly adding members). An explicit `sync()` and a `nudge()` fallback are also provided.
- **exposes the op-event log** as inspect (`opEvents({ afterId, beforeId, limit, order })`) plus live subscribe (`subscribeOpEvents` / `onOpEvents`). Subscribe does not replay history.
- **maintains projection-local indexes** declared in an index spec, installed with `reconcileIndexes` (below).
- **mounts FILES members as folders** with `reconcileFiles` (below). Files have no projection tables; the target is used only for `rdb_keys` ids.



## Indexes

`projection.reconcileIndexes(spec, { dryRun? })` installs a projection index spec on the shared target. The spec format, the fingerprint check, and the per-target options are described in [rdb_adapter's Indexes section](../rdb_adapter#indexes). In a replica-wide projection:

- Each declaration names its rdb group (`group.getName()`) and is built on that group's group-qualified table (`<group>_<table>`), so the same index name can be used in two groups without colliding.
- A cross-group foreign key column resolves to its `<col>_id` companion when the referenced group is co-projected, and to `<col>_row_hash` otherwise.
- A declaration for a group that is not (yet) a member of the `RDb` is reported in `report.pending`. When the group joins (a deployed release adds it), its initial projection builds it from the installed spec; no second reconcile is needed.
- `open()` takes no spec. Call `reconcileIndexes` right after `open()` as part of the app's update; it holds the database lock, so it never interleaves with a sync cycle. After that, every sync keeps the indexes current across remote schema changes.

```typescript
const projection = await RdbProjection.open(rdb, ctx, target, { writer });
const report = await projection.reconcileIndexes(indexSpec);
if (report.pending.length > 0) console.warn('index declarations not buildable yet', report.pending);
```

Known limits (detailed in [rdb_adapter](../rdb_adapter#known-limits)):

- **Unmanaged indexes block remote column drops (SQLite).** An index the app creates by hand on a projected column makes a remote drop of that column fail, stalling that group's sync until the index is removed. Declare indexes in the spec instead.
- **A stale spec costs performance, never correctness.** Declarations whose table or column was dropped or renamed upstream show up in `report.pending` and stay inactive until the spec is updated. Check it after each release.
- **Only reconcile applies spec changes.** Opening a projection does not install a new spec; until `reconcileIndexes` runs, the previously installed indexes stay in place.



## File mounts

`projection.reconcileFiles(mounts, open, opts?)` keeps each `{ name, path }` FILES member (an [RBlobStore and an RFileMap](../rdb#files)) in sync with a folder. `open(mount)` returns the folder as a `FileDirectory`: `list`, `stat`, `read` (streamed chunks), `write` (atomic replace, creating parents), `remove`, `rename`, `ensureDir`, `preserve` (folders that stay when they empty), and an optional `watch` hint. `MemoryDirectory` is here for tests; [rdb_files_node](../rdb_files_node) and [rdb_files_web](../rdb_files_web) are the real ones.

```typescript
const report = await projection.reconcileFiles([{ name: 'media', path: 'media' }], (m) => NodeDirectory.open(join(dir, m.path)));
// report.mounted, report.pending; projection.filesStatus() for counts
```

[rhost](../rhost#file-mounts) mounts the folders listed in a host's `projection.files`, and the [`rdb` REPL](../rdb_tools#repl) mounts them one at a time with `\project files <id> <name> to <path>`.

A folder has `common/` (shared) and `keys/<rdb_keys id>/` (one per owner; the id is the one `author_key_id` columns use, and the DAG keeps the base64 key id). The first pass makes `common/`, `keys/` and, when the projection has a writer, the writer's `keys/<id>/`, whether or not the key can write yet, so files can be dropped there right away. That folder is preserved: it stays when it empties. Other owners' folders appear with their files and go away with them. Each pass:

1. applies the store delta (upload chains, per-lane bytes) and the map delta;
2. names every live file: when several hashes share a path, or paths differ only in case, each gets `~<8 hex>` from its element id before its extension;
3. writes complete files; an incomplete one is `missing` and absent on disk;
4. deletes the files of removed elements, only if unchanged on disk. Nothing local in `common/` and `keys/<myId>/` is reverted: edits, deletes and new files there wait until the key can write, and while they wait they hide remote changes to the same files. In other keys' folders, deleted or edited files are restored (the edit is moved aside to `~local` first);
5. ingests, when the projection has a writer and `canWrite` holds at the group's frontier: new or changed files in `common/` and `keys/<myId>/` (by size and mtime, then hash) are uploaded on the least loaded lane (resuming an interrupted upload) and added, removing the element they replace; a deleted file is removed;
6. counts the changes still waiting in `common/` and `keys/<myId>/` (`waiting`), and reports every other file as local-only: outside the sections, in another key's folder, or with a name the path rules reject. A local-only file in a synced file's way moves to `~local`.

The mount state lives in `.hhs/state.json` (checkpoints, upload chains, lane bytes, live elements, and a record per file written or ingested), rewritten atomically when it changes. It grows with the number of files. If it is lost, the next pass rebuilds it from full deltas and adopts the files on disk that match by hash, without uploading anything again.

A mount whose FILES isn't a member yet, or whose objects haven't arrived, is `pending` and attaches on its own (membership changes, sync cycles, and a retry every `retryMs`). Passes run on DAG growth of either object, the folder's watch hint, a periodic scan (`scanIntervalMs`, default 5 s), `nudge()`, and growth of the bound group when it changes whether the writer can write; they never overlap. With a writer, a mount also waits for its bound group to arrive. `stop()` stops the mounts too; the folders stay as they are.

## Layout

- `scope.ts` — resolve members → `GroupProjection`s (member-name-qualified tables + a cross-group resolver).
- `projection.ts` — `RdbProjection` lifecycle: `open` / `sync` / `nudge` / `status` / `reconcileIndexes` / `reconcileFiles` / `filesStatus` / `stop` (waits for in-flight sync, then `target.close()` if the target implements it).
- `files/` — `FileDirectory` and `MemoryDirectory` (`directory.ts`), disk names (`names.ts`), the mount state (`state.ts`) and the mount pass (`mount.ts`).



## Test

```
npm test
```

