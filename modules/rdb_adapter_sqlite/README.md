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
- Implements `MaterializedChangeSource` (capture triggers + an outbox, see [Capture](#capture)) and `RowIdentityIndex`, so it is a full bidirectional backend. A column that is NULL in rdb is a SQL `NULL`, and a local `UPDATE ... SET col = NULL` is ingested as a null write (see [NULL values](../rdb_adapter#null-values)).
- Implements `IndexTarget`: projection-local indexes declared in an index spec become real SQLite indexes (see [Indexes](#indexes)).
- Implements `QueryableTarget`: compiles `SingleTableQueryPlan` (including `.before(id)` / `.after(id)` tuple seeks and `LIKE` via a registered deterministic `rdb_like_match` function) into parameterized `SELECT` statements and maps rows to their logical types.
- Implements `ReactiveChangeTarget` (see [rdb_adapter](../rdb_adapter#reactive-queries)): emits `local`, `apply`, and `settle` commit events. Local writes are detected by an optional in-process commit shim (`wrapDatabase(db, target)` / `target.noteLocalCommit()`) together with a coalesced kernel WAL watcher (via [file_watch](../file_watch) when `dbPath` is passed, or an epoch-gated unref'd poll for `:memory:` / no path) and the drain rule in `drainChanges()`. Announced outbox rows are tracked in `emittedOutboxIds` as a set rather than a high-water mark, so rolled-back `AUTOINCREMENT` ids and out-of-order commits never stall or drop notifications.
- `close()` disarms that monitor and closes the `better-sqlite3` handle (idempotent). A file-backed db checkpoints and drops `-wal`/`-shm` when this is the last connection. `RdbProjection.stop()` calls this.

## Capture

With `captureChanges: true`, triggers on every projected table record the app's writes in `rdb_outbox`, whichever connection makes them. The adapter's own writes don't reach the outbox: they run with `rdb_capture_config.applying` set, which the triggers check. What ingestion then does with the outbox is described in [Ingestion and checkpoints](../rdb_adapter#ingestion-and-checkpoints).

An insert is recorded from the row as stored, defaults included. When the table has an author column and the writer record (`rdb_writer_record`) says its group's mode is `on`, the insert trigger first writes the writer key's id into an empty author column, so the row reads its author straight away. A `RETURNING` clause on the insert still shows the column empty, because SQLite computes it before `AFTER` triggers run; any later read has the id.

An `UPDATE` is recorded as one outbox row per updated row, with every column the statement names and the value it set, whether it changed or not: `UPDATE notes SET body = body` is a write of `body`. A `BEFORE UPDATE` trigger closes what an earlier statement left open for the row, and an `AFTER UPDATE OF <column>` trigger per column adds its column to this statement's row. A statement can't change a row's id, with capture on or off: a trigger fails it, since the row would sit under an id rdb doesn't know.

Every local id comes from its table's own `AUTOINCREMENT` counter. A row the projection writes is inserted first and takes the id that insert produced, and a reference to a row the target doesn't hold yet reserves the next value of the table's `sqlite_sequence`, which the row takes when it arrives. A native insert therefore never collides with an id the projection is about to use, even before the insert is ingested.

The columns' checks refuse, at the statement, a boolean other than 0 or 1 and json text that isn't json or is json's `null`. Json text in another form, such as `{ "b": 1, "a": 2 }`, is accepted, and the settle that ingests it rewrites it in canonical form, the form rows from rdb have, so a query that binds a json value matches it. A column a pending change writes is left for that change's own settle.

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
