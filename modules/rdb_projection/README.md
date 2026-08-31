# rdb_projection

Reactive **supervisor** that keeps a replica-wide relational projection of an [Rdb](../rdb) `RDb` in sync — without polling. Browser-safe and engine-agnostic: the concrete [`MaterializationTarget`](../rdb_adapter) (SQLite, in-memory, …) is injected by the host. Built on [rdb_adapter](../rdb_adapter)'s pure planners and database-level orchestrators.

## What it does

An `RDb` already names a set of member table groups, so it is the natural unit of projection. `RdbProjection.open(rdb, ctx, target, { writer })` resolves the members into **one shared target** and:

- **materializes** every member with group-qualified table names (`<group>_<table>`, so tables from different groups never collide);
- **resolves cross-group FKs to serial ids** when the referenced group is co-projected (otherwise a `row_hash` passthrough);
- **ingests local edits in commit order**, advancing co-projected cross-group refs on demand so an observer's cross-group FKs and `exists` reads validate against sibling groups ingested in the same pass; `fkBundling` (a per-member option, default on) bundles consecutive FK-linked inserts atomically;
- **interns authors and identity keys** into a shared `rdb_keys` table as `author_key_id` / `<col>_key_id` (`registerKey` / `keyHashForId` / `publicKeyForId` on the projection);
- **stays in sync reactively** — a debounced, coalesced `syncDatabase` fires on three triggers: each member group's `subscribe` (the rdb side advanced), the target's optional `ChangeSignalSource` (local edits are waiting), and the `RDb`'s own `subscribe` (membership changed). An explicit `sync()` and a `nudge()` fallback are also provided.

## Layout

- `scope.ts` — resolve members → `GroupProjection`s (group-qualified names + a cross-group resolver).
- `projection.ts` — `RdbProjection` lifecycle: `open` / `sync` / `nudge` / `status` / `stop`.

## Test

```
npm test
```
