# rdb_adapter_sqlite

SQLite [`MaterializationTarget`](../rdb_adapter) for [rdb_adapter](../rdb_adapter): a self-contained, capture-provisioned backend that materializes Rdb groups into a [`better-sqlite3`](https://github.com/WiseLibs/better-sqlite3) database and captures local edits for ingestion back into Rdb.

```typescript
import Database from 'better-sqlite3';
import { projectGroup } from '@hyper-hyper-space/hhs3_rdb_adapter';
import { SqliteTarget } from '@hyper-hyper-space/hhs3_rdb_adapter_sqlite';

const path = 'my-projection.sqlite';
const db = new Database(path);
const target = new SqliteTarget(db, { captureChanges: true, dbPath: path });
await projectGroup(group, target);
```

For a whole `RDb`, [rdb_projection](../rdb_projection) is the usual supervisor: `RdbProjection.open(rdb, ctx, target, { writer })` materializes every member under group-qualified table names and keeps both directions in sync. You still read and write the SQLite `db` handle.

## What it does

- Applies schema actions, row actions, and the **per-group checkpoint** in one transaction (`SqliteTarget`), with native column affinity, sync tables for a stable local-id ↔ rowId mapping, and advisory local FKs. A co-projected cross-group FK carries an integer id but declares **no** DB-level `FOREIGN KEY` (its referenced table belongs to another group, materialized in a separate apply). Adding a required column without a default to a non-empty table is add-nullable, then a post-backfill table rebuild restores `NOT NULL` (SQLite's `ADD COLUMN` rule).
- Owns the shared `rdb_keys(id, key_hash, public_key)` side table and implements `KeyIndex`: authors, provider `key_id`, and `identity` `<col>_key_id` intern to numeric ids (advisory FKs to `rdb_keys`); the public key stays out of the app tables.
- Implements `MaterializedChangeSource` (capture triggers + an outbox) and `RowIdentityIndex`, so it is a full bidirectional backend.
- Implements `ChangeSignalSource`: wakes observers when local edits are waiting, no commit hook required. Defaults to kernel-driven WAL watching (via [file_watch](../file_watch)) for a file-backed db (pass `dbPath`), and falls back to an epoch-gated, unref'd poll of the monotonic `AUTOINCREMENT` outbox id for `:memory:` / no path.

## Test

Runs the shared conformance + ingestion suites from [rdb_adapter_test](../rdb_adapter_test) plus SQLite-specific assertions (affinity, NOT NULL, DEFAULT rendering, transaction rollback, the change signal, and cross-group FK DDL). `npm test` includes a smoke projection-vs-rdb sweep (shared [generator](../rdb_adapter_test_gen) vs rdb as oracle, not sqlite-vs-idb); heavier profiles:

```
npm test
npm run test:projection:fast
npm run test:projection:full
```
