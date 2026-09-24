# Rdb Projection

Reactive **supervisor** that keeps a replica-wide relational projection of an [Rdb](../rdb) `RDb` in sync. The concrete [`MaterializationTarget`](../rdb_adapter) (SQLite, IDB, in-memory, …) is injected by the host. Built on [rdb_adapter](../rdb_adapter)'s pure planners and database-level orchestrators.

## What it does

An `RDb` already names a set of member table groups, so it is the natural unit of projection. `RdbProjection.open(rdb, ctx, target, { writer })` resolves the members into **one shared target** and:

- **materializes** every member with group-qualified table names (`<group>_<table>`, so tables from different groups never collide);
- **resolves cross-group FKs to serial ids** when the referenced group is co-projected (otherwise a `row_hash` passthrough);
- **ingests local edits in commit order**, advancing co-projected cross-group refs on demand so an observer's cross-group FKs and `exists` reads validate against sibling groups ingested in the same pass; `fkBundling` (a per-member option, default on) bundles consecutive FK-linked inserts atomically;
- **interns authors and identity keys** into a shared `rdb_keys` table as `author_key_id` / `<col>_key_id` (`registerKey` / `keyHashForId` / `publicKeyForId` on the projection);
- **stays in sync reactively** — a debounced, coalesced `syncDatabase` fires on three triggers: each member group's `subscribe` (the rdb side advanced), the target's optional `ChangeSignalSource` (local edits are waiting), and the `RDb`'s own `subscribe` (membership changed). An explicit `sync()` and a `nudge()` fallback are also provided.
- **exposes the op-event log** as inspect (`opEvents({ afterId, beforeId, limit, order })`) plus live subscribe (`subscribeOpEvents` / `onOpEvents`). Subscribe does not replay history.
- **maintains projection-local indexes** declared in an index spec, installed with `reconcileIndexes` (below).

## Indexes

`projection.reconcileIndexes(spec, { dryRun? })` installs a projection index spec on the shared target. The spec format, the version gate, and the per-target options are described in [rdb_adapter's Indexes section](../rdb_adapter#indexes). In a replica-wide projection:

- Each declaration names its rdb group (`group.getName()`) and is built on that group's group-qualified table (`<group>_<table>`), so the same index name can be used in two groups without colliding.
- A cross-group foreign key column resolves to its `<col>_id` companion when the referenced group is co-projected, and to `<col>_row_hash` otherwise.
- A declaration for a group that is not (yet) a member of the `RDb` is reported in `report.pending`. When the group joins, its initial projection builds it from the installed spec; no second reconcile is needed.
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

## Layout

- `scope.ts` — resolve members → `GroupProjection`s (group-qualified names + a cross-group resolver).
- `projection.ts` — `RdbProjection` lifecycle: `open` / `sync` / `nudge` / `status` / `reconcileIndexes` / `stop` (waits for in-flight sync, then `target.close()` if the target implements it).

## Test

```
npm test
```
