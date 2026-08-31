# rdb_adapter_sqlite

SQLite [`MaterializationTarget`](../rdb_adapter) for [rdb_adapter](../rdb_adapter): a self-contained, capture-provisioned backend that materializes Rdb groups into a [`better-sqlite3`](https://github.com/WiseLibs/better-sqlite3) database and captures local edits for ingestion back into Rdb.

## What it does

- Applies schema actions, row actions, and the **per-group checkpoint** in one transaction (`SqliteTarget`), with native column affinity, sync tables for a stable local-id ↔ rowId mapping, and advisory local FKs. A co-projected cross-group FK carries an integer id but declares **no** DB-level `FOREIGN KEY` (its referenced table belongs to another group, materialized in a separate apply).
- Owns the shared `rdb_keys(id, key_hash, public_key)` side table and implements `KeyIndex`: authors, provider `key_id`, and `identity` `<col>_key_id` intern to numeric ids (advisory FKs to `rdb_keys`); the public key stays out of the app tables.
- Implements `MaterializedChangeSource` (capture triggers + an outbox) and `RowIdentityIndex`, so it is a full bidirectional backend.
- Implements `ChangeSignalSource`: wakes observers when local edits are waiting, no commit hook required. Defaults to kernel-driven WAL watching (via [file_watch](../file_watch)) for a file-backed db (pass `dbPath`), and falls back to an epoch-gated, unref'd poll of the monotonic `AUTOINCREMENT` outbox id for `:memory:` / no path.

## Test

Runs the shared conformance + ingestion suites from [rdb_adapter_test](../rdb_adapter_test) plus SQLite-specific assertions (affinity, NOT NULL, DEFAULT rendering, transaction rollback, the change signal, and cross-group FK DDL).

```
npm test
```
