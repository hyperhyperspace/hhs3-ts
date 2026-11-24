# SQLite target: no SQL foreign keys

- **Status:** implemented 2026-11-24, in the working tree (not committed)
- **Written:** 2026-11-24, against `3bae793` plus the uncommitted HIP-29 work
- **Modules:** rdb_adapter_sqlite, rdb_adapter (comments and README only)

## Summary

The SQLite target declared SQL foreign keys it never enforced: a local foreign key's `<col>_id` companion referenced its table's `id`, the author and key columns referenced `rdb_keys(id)`, and each sync table's `id` referenced its app table. The target turned `foreign_keys` off on its own connection. better-sqlite3 turns it on for every connection it opens, though, and on such a connection writes that rdb accepts failed with `FOREIGN KEY constraint failed`: deleting a projected row, which its sync row references; deleting a row another one references; inserting a reference to a row that is gone. The `rdb` REPL hit the first while checking the change to ingest plain row ops.

rdb's references don't work like SQL's. A reference is checked by the operation that makes it, at that operation's own position, so a live row can reference a row deleted later (rdb README, *What an operation answers for*). Sync rows outlive their app rows by design. An enforced SQL foreign key would refuse writes rdb accepts, and an unenforced one is a trap for the next connection that opens the file.

The target now declares no SQL foreign keys. The project is unreleased: a file created before this change keeps the keys it declared until its tables are recreated, and nothing here upgrades it.

## Changes

1. **`sqlite_target.ts`:**
   - `createTable` declares no `FOREIGN KEY` clause, not on the app table and not on the sync table.
   - `dropColumn` is always a plain `ALTER TABLE … DROP COLUMN`. The table rebuild it used for a column named in a foreign key existed only because SQLite refuses to drop such a column.
   - `rebuildTable` is left with one use, restoring `NOT NULL` after a relaxed `ADD COLUMN`. It no longer re-emits foreign keys or drops a column.
   - `foreign_keys = OFF` stays on the target's connection, for a new reason: a table rebuild drops and renames a projected table, which a foreign key the app declared on a table of its own, pointing into a projected one, would refuse.
2. **`rdb_adapter/src/types.ts`:** the comments on `fkIdSuffix` and `FkColumnInfo` say a companion is a plain integer column that no target declares as a database foreign key.
3. **Docs:** the SQLite README gains a *Foreign keys* section, which links to rdb's rules rather than repeating them. Its *What it does* and *Indexes* bullets, and the rdb_adapter README's *Known limits* and *Implementation details*, drop the advisory foreign keys and the foreign key column rebuild.

## Tests

- `[ADPTS-SQL05]`: no projected table declares a SQL foreign key, for a local or a co-projected cross-group companion, an author or a key column, or a sync table. The companions stay integer columns.
- `[ADPTS-SQL09]`: a row that references another table keeps its reference, the same id, through a tighten rebuild of that table. It no longer reads the declared foreign keys.
- `[ADPTS-SQL28]`: a second connection, with foreign keys on as better-sqlite3 opens it, deletes a projected post that a comment references, and the delete is ingested while the comment stays. It then inserts a comment on the deleted post, which ingestion refuses and removes. The tables match rdb at the checkpoint after each step.
- `[ADPTS-IDX04]`: a foreign key column now drops like any other column, which changes this case. Its managed index is dropped first, as the core plans it, and a hand-made index on it makes the drop fail and the apply roll back, as one on any other projected column already did (rdb_adapter README, *Known limits*). Once the hand-made index is removed, the column drops and the other indexes stay. Before, the rebuild deleted a hand-made index on a foreign key column without a word. Decided 2026-11-24. The tighten half of the case is unchanged: the rebuild keeps managed and hand-made indexes.
