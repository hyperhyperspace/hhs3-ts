# Rdb Projection

Reactive **supervisor** that keeps a replica-wide relational projection of an [Rdb](../rdb) `RDb` in sync. The concrete `[MaterializationTarget](../rdb_adapter)` (SQLite, IDB, in-memory, …) is injected by the host. Built on [rdb_adapter](../rdb_adapter)'s pure planners and database-level orchestrators.

## What it does

An `RDb`'s member table groups are computed from the catalog releases deployed into it, so it is the natural unit of projection. `RdbProjection.open(rdb, ctx, target, { writer })` resolves the members the replica holds into **one shared target** and:

- **materializes** every member with group-qualified table names (`<group>_<table>`, where `<group>` is the member name from `rdb.getMemberGroupNames()`: an identifier, unique within the database, so tables from different groups never collide);
- **resolves cross-group FKs to serial ids** when the referenced group is co-projected (otherwise a `row_hash` passthrough);
- **ingests local edits in commit order**, each as its own entry at its group's checkpoint, observing co-projected groups as the pass extends them so an observer's cross-group FKs and `exists` reads validate against sibling groups ingested in the same pass (see [Ingestion and checkpoints](../rdb_adapter#ingestion-and-checkpoints));
- **interns authors and identity keys** into a shared `rdb_keys` table as `author_key_id` / `<col>_key_id` (`registerKey` / `keyHashForId` / `publicKeyForId` on the projection);
- **stays in sync reactively** — a debounced, coalesced `syncDatabase` fires on three triggers: each member group's `subscribe` (the rdb side advanced), the target's `{ source: 'local' }` commit events on `ReactiveChangeTarget` (or `ChangeSignalSource` fallback when local edits are waiting), and the `RDb`'s own `subscribe` (a release was deployed, possibly adding members). An explicit `sync()` and a `nudge()` fallback are also provided. How a cycle runs is described in [Cycles](#cycles).
- **exposes reactive queries and commit streaming** — `projection.selectFrom(table)`, `projection.monitor(queryOrSet)`, and `projection.onCommit(listener)` delegate to the [query engine](../rdb_adapter#reactive-queries) on the underlying target.
- **exposes the op-event log** as inspect (`opEvents({ afterId, beforeId, limit, order })`) plus live subscribe (`subscribeOpEvents` / `onOpEvents`). Subscribe does not replay history.
- **maintains projection-local indexes** declared in an index spec, installed with `reconcileIndexes` (below).
- **mounts FILES members as folders** with `reconcileFiles` (below). Files have no projection tables; the target is used only for `rdb_keys` ids.



## Cycles

A cycle ingests the local changes first and projects second, and it never projects over a change that is still pending. If the app writes while a projection pass computes its delta, the target refuses the apply with `PendingChangesError`. The cycle then ends quietly and schedules the next one, which ingests the change and projects after it. A cycle also ingests a second time when the outbox filled up during the first, so a burst of writes doesn't hold remote changes back for long. How each change is ingested at its group's checkpoint is the subject of [Ingestion and checkpoints](../rdb_adapter#ingestion-and-checkpoints).

### One projector per target

Cycles, single-group syncs and index reconciles take turns on a target, under one lock per target object. Two projectors on the same store, in two processes or two tabs, would move checkpoints under each other, and the projection doesn't try to repair that: when a checkpoint moved before an apply or a settle, it halts. It halts as well when the DAG appends an entry under another hash than the one ingestion recorded for it, and when the delta of an ingestion chain changes the schema, which no chain can do. A halted projection runs no more cycles. `sync()` throws the error that halted it, `onError` fires once, `haltError()` returns the error, and reads keep working.

### Errors and retries

Any other error ends the cycle, reaches `onError`, and leaves its message in `lastError()`. The projection then schedules the next cycle itself, after 1 s, doubling up to 60 s, and back to 1 s after a cycle that succeeds. The next cycle finishes whatever the failed pass stored, so a passing error, a busy database for instance, costs a delay and nothing else.

### The writer

`writer` signs what the projection ingests. At open, and whenever the members change, the projection records it in the target, with each group's mode. That record is how capture fills the empty author column of a native insert with the writer's key id, so the row shows its author before it is ingested. `autoAuthor`, true by default and settable per group through `configOverride`, decides what an empty author column means in a group with an identity provider: the writer's key, or an anonymous insert when it is false. A group with an identity provider, `autoAuthor: false` and `authorColumn: false` is refused at open: under `autoAuthor: false` an insert names its author in that column, and there is none. A group without an identity provider is recorded as anonymous whatever the option says, and needs no key.

### A group without a key

A projection opened without a writer can't ingest the changes of a group with an identity provider. A pass ingests the changes before the first such one and stops there. That change, and every change after it, waits in the outbox, and while it waits no projection applies, so remote changes of every group wait too. The projection warns once for each such group, through `onWarning` or `console.warn`, at open or when a cycle first meets the change, and schedules no cycle that would only stop there again. Opening the projection with a key ingests the rest.

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



## Reactive queries

`projection.selectFrom(table)` and `projection.monitor(queryOrSet)` run live queries over the shared target (when it implements `QueryableTarget & ReactiveChangeTarget`, as `SqliteTarget`, `IdbTarget`, and `MemoryTarget` do). Table names are the group-qualified target names (`<group>_<table>`), and foreign keys use their `<col>_id` / `<col>_key_id` companions.

```typescript
const notes = projection
  .selectFrom('editor_notes')
  .where('folder_id = :folderId', { folderId })
  .orderBy('id', 'desc')
  .limit(50);

const sub = projection.monitor(notes);
```

`projection.onCommit(listener)` forwards the target's commit events. `SqliteTarget`, `IdbTarget`, and `MemoryTarget` emit `local`, `apply`, and `settle`. [rhost](../rhost) uses it to stream commits to `watchCommits` clients. The query language, `.before(id)` / `.after(id)` keyset seeks, `.col()` query-set DAGs, and the relevance rules are described in [rdb_adapter's Reactive queries section](../rdb_adapter#reactive-queries). For React components, see [hhs3_rdb_react](../rdb_react).



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
- `projection.ts` — `RdbProjection` lifecycle: `open` / `sync` / `nudge` / `status` / `selectFrom` / `monitor` / `onCommit` / `reconcileIndexes` / `reconcileFiles` / `filesStatus` / `stop` (waits for in-flight sync, then `target.close()` if the target implements it).
- `files/` — `FileDirectory` and `MemoryDirectory` (`directory.ts`), disk names (`names.ts`), the mount state (`state.ts`) and the mount pass (`mount.ts`).



## Test

```
npm test
```

**HIPs:** [3](../../hips/0003-schema-composability.md), [5](../../hips/0005-reactive-queries.md)
