# Rdb Projection Adapter for SQLite

SQLite [`MaterializationTarget`](../rdb_adapter) for [rdb_adapter](../rdb_adapter): a self-contained, capture-provisioned backend that materializes Rdb groups into a [`better-sqlite3`](https://github.com/WiseLibs/better-sqlite3) database and captures local edits for ingestion back into Rdb.

```typescript
import Database from 'better-sqlite3';
import { projectGroup } from '@hyper-hyper-space/hhs3_rdb_adapter';
import { SqliteTarget } from '@hyper-hyper-space/hhs3_rdb_adapter_sqlite';

const path = 'my-projection.sqlite';
const db = new Database(path);
const target = new SqliteTarget(db, { captureChanges: true, dbPath: path });
await projectGroup(group, target);
// ...
target.close();  // checkpoints WAL; drops -wal/-shm when this is the last connection
```

For a whole `RDb`, [rdb_projection](../rdb_projection) is the usual supervisor: `RdbProjection.open(rdb, ctx, target, { writer })` materializes every member under group-qualified table names and keeps both directions in sync. You still read and write the SQLite `db` handle.

## What it does

- Applies schema actions, row actions, and the **per-group checkpoint** in one transaction (`SqliteTarget`), with native column affinity, sync tables for a stable local-id ↔ rowId mapping, and advisory local FKs. A co-projected cross-group FK carries an integer id but declares **no** DB-level `FOREIGN KEY` (its referenced table belongs to another group, materialized in a separate apply). Adding a required column without a default to a non-empty table is add-nullable, then a post-backfill table rebuild restores `NOT NULL` (SQLite's `ADD COLUMN` rule).
- Owns the shared `rdb_keys(id, key_hash, public_key)` side table and implements `KeyIndex`: authors, provider `key_id`, and `identity` `<col>_key_id` intern to numeric ids (advisory FKs to `rdb_keys`); the public key stays out of the app tables.
- Implements `MaterializedChangeSource` (capture triggers + an outbox) and `RowIdentityIndex`, so it is a full bidirectional backend.
- Implements `IndexTarget`: projection-local indexes declared in an index spec become real SQLite indexes (see [Indexes](#indexes)).
- Implements `ChangeSignalSource`: wakes observers when local edits are waiting, no commit hook required. Defaults to kernel-driven WAL watching (via [file_watch](../file_watch)) for a file-backed db (pass `dbPath`), and falls back to an epoch-gated, unref'd poll of the monotonic `AUTOINCREMENT` outbox id for `:memory:` / no path.
- `close()` disarms that monitor and closes the `better-sqlite3` handle (idempotent). A file-backed db checkpoints and drops `-wal`/`-shm` when this is the last connection. `RdbProjection.stop()` calls this.

## Indexes

The index spec and `reconcileIndexes` are described in [rdb_adapter](../rdb_adapter#indexes). On this target:

- Each index is a `CREATE INDEX` on the projected table, named `<table>__<name>`. It is created after the batch's rows are written.
- `options` is `{ "columns": { "<rdb column>": { "desc": true, "collate": "NOCASE" | "RTRIM" | "BINARY", "whereNotNull": true } } }`, every field optional. Without `options`, the index is plain ascending. `whereNotNull` makes a partial index; on several columns the conditions are ANDed. Any other key, or a column the declaration does not list, is rejected before anything is installed.
- The installed spec is the single row of `rdb_index_spec`; each built index is a row of `rdb_index_meta`.
- Only indexes recorded in `rdb_index_meta` are dropped ahead of a remote column drop. An index created by hand on a projected column makes that drop fail, and the group's sync stalls until the index is removed. Table rebuilds (NOT NULL tightening, FK column drops) keep hand-made indexes and skip any on the dropped column.

## Test

Runs the shared conformance + ingestion suites from [rdb_adapter_test](../rdb_adapter_test) plus SQLite-specific assertions (affinity, NOT NULL, DEFAULT rendering, transaction rollback, the change signal, and cross-group FK DDL). The index cases cover the built indexes (direction, collation, partial `WHERE`), rejected options, a remote drop of an indexed column, and table rebuilds. `npm test` includes a smoke projection-vs-rdb sweep (shared [generator](../rdb_adapter_test_gen) vs rdb as oracle, not sqlite-vs-idb); heavier profiles:

```
npm test
npm run test:projection:fast
npm run test:projection:full
```
