# Fourth critique of HIP-20: Unified write API for projection targets

A review of [HIP-20](0020-unified-write-api.md) as revised on 2026-11-18, and of [the third response](0020-unified-write-api.response-3.md) to [the third critique](0020-unified-write-api.critique-3.md), checked by code inspection against the working tree on 2026-11-18. The revision takes the third critique and integrates `NULL` writes (`6110da1`) cleanly into the write semantics. What remains are concrete implementation traps where the revised design meets the internals of `ingest_orchestrator`, `ingest`, `SqliteTarget`, `IdbTarget`, `RdbProjection`, and `rhost`, plus a few underspecified edge cases on `RowWriter`.

Paths are relative to `modules/`.

## Verdict

**Does the revision resolve the third critique?** Yes. Every item in the third response's table is reflected in the HIP, and the pivot to `NULL` writes removes the divergence between `null` in the writer and `NULL` in rdb and SQL.

**Is the third response right where it differs?** Yes: keeping the stored author on an IndexedDB `put` avoids spurious failure events on every form save, reading the row on every SQLite update simplifies the rule, and dropping FK target tables from `TableShape` matches leaving FK validity to ingestion.

**Is anything still wrong?** Not in the high-level write semantics. The remaining issues are in the implementation contracts:

| Item | Where in the HIP |
|---|---|
| 2. `logOpEvent` on SQLite still throws on `{ a: null }` in JSON (`J1`) | *Changes* 1, "A failed rejection record no longer blocks ingestion" |
| 3. Crash-replay identity & status traps in `mintInserts` and `marks` | *Authorship*; *Changes* 1, "A native insert over an existing local id is reverted to rdb's row" |
| 4. IndexedDB writer cannot use `target.database.transaction`, and `TABLE_META` enumeration pollutes `tableNameSet` | *Changes* 4 |
| 5. Writer record & `authorship_mode` lifecycle on SQLite and `RdbProjection` | *What the projection records in the target*; *Changes* 3, 6 |
| 6. `rhost` config validation rejects `autoAuthor` | *Changes* 7, 10 |
| 7. Smaller wiring and specification gaps | *Design*; *Checks*; *Changes* 1, 2, 3, 6, 9 |

## 1. How the claims were checked

I read the 2026-11-18 revision of HIP-20, the third response, and the implementation files they cross:

- `rdb_adapter/src/ingest.ts`, `ingest_orchestrator.ts`, `memory_target.ts`, `query_builder.ts`, `query_monitor.ts`, `names.ts`, `schema_actions.ts`, `project.ts`, `types.ts`
- `rdb_adapter_sqlite/src/sqlite_target.ts`
- `rdb_adapter_idb/src/idb_facade.ts`, `idb_target.ts`, `idb_schema.ts`, `idb_env.ts`
- `rdb_projection/src/projection.ts`, `scope.ts`
- `rhost/src/host.ts`, `projection.ts`, `config.ts`, `client.ts`
- `rhost_client_node/src/client.ts`, `rhost_client_browser/src/client.ts`
- `rdb_react/src/context.ts`, `use_query.ts`, `rdb_lang/src/bind/values.ts`

No new scratch probes were run; every finding below is from reading the code in the working tree.

## 2. `logOpEvent` on SQLite still throws on `{ a: null }` in JSON (`J1`)

*Changes* 1 says: *"when recording a rejection throws, the failure becomes that change's rejection and its row is reverted. The ingest pass always reaches `commitIngest`."*

In `rdb_adapter/src/ingest_orchestrator.ts:387-409`, `recordFailure` calls `syntheticOpHash(group, f.write)` (`:392`), which throws on `{ a: null }`. Catching that throw and substituting a fallback `opHash` is not enough on SQLite: `recordFailure` also sets `event.op = bundlePayload(f.write)` (`:399`), and inside `commitIngest`, `SqliteTarget.logOpEvent` calls `json.toStringNormalized(event.op)` (`rdb_adapter_sqlite/src/sqlite_target.ts:487`). That throws `"Cannot serialize null"` inside the `commitIngest` transaction, aborting the settle and leaving the outbox intact on every retry—the exact failure mode `J1` demonstrates.

Two details follow:

- **Omit `event.op` when `f.write` cannot be normalized.** `OpEvent.op` is already optional (`rdb_adapter/src/types.ts:213`).
- **Hash a serialized form for the fallback `opHash`.** Using `f.rowId` alone as the fallback `opHash` causes a second `{ a: null }` failure on the same row in a later pass to be ignored by `UNIQUE (op_hash, direction)` on `rdb_op_events` (`sqlite_target.ts:465`). Hashing `JSON.stringify(bundlePayload(f.write))` (or including the payload text) avoids the collision while keeping `opHash` deterministic.

## 3. Crash-replay identity & status traps in `mintInserts` and `marks`

Three edge cases sit in *Changes* 1 ("A native insert over an existing local id is reverted to rdb's row") and *Authorship* (`autoAuthor` off):

1. **`mintInserts` assumes `deriveRowId(existing.uuid, signer) === existing.rowId`.** Today `mintInserts` (`rdb_adapter/src/ingest.ts:212`) derives `rowId` from `existing.uuid` and `ctx.writerKeyId`, which always matches `existing.rowId` because every local insert had one signer. With `autoAuthor` off, suppose row `1` was inserted on this device *signed* (`existing.rowId = deriveRowId('u1', writerKeyId)`) and later overwritten with `INSERT OR REPLACE` *anonymously* (`signer = undefined`). Then `deriveRowId('u1', undefined)` differs from `existing.rowId`. `reserveMint` (`ingest_orchestrator.ts:363-370`) adopts `existing.rowId` and re-plans, which runs `mintInserts` again and derives the *same* mismatched `rowId`, submitting a phantom row to rdb and colliding with `sync`'s unique `id` index in `commitIngest`.
   - **Fix:** In `mintInserts`, when `existing.uuid !== ''` and `deriveRowId(existing.uuid, signer) !== existing.rowId`, treat the insert as non-reusable (drop and revert to rdb's row), not as a crash replay.
2. **"Already applied" insert revert can poison `syncStatus` to `ingestion_failure`.** *Changes* 1 marks an insert skipped as already applied (`isWriteOnceFor`) for revert via `marks` (`ingest_orchestrator.ts:570-590`). When `marks` reconciles at end of pass (`:575-580`), if `tableView.getRow(mark.rowId)` is `undefined`, it sets `status: 'ingestion_failure'`. If the device inserted row `1`, appended the insert to rdb, crashed before `commitIngest`, and a subsequent delete in the same batch or a concurrent peer deleted the row before replay, `isWriteOnceFor` fires on replay, `getRow` returns `undefined`, and `commitIngest` marks a valid, deleted row as `ingestion_failure`.
   - **Fix:** Distinguishing "reconcile-only revert" from "failure revert" ensures an already-applied insert skipped on replay never writes `status: 'ingestion_failure'` when the row is absent in rdb.
3. **Detecting whether the local row is still present.** *Changes* 1 logs a failure op event for a dropped insert *"whose row the target still has"*, while a stale replay *"whose local row is already gone is dropped as today"*. `MaterializedChangeSource` (`rdb_adapter/src/types.ts:310-358`) exposes `resolveRow` (the sync table, where deleted rows remain), not a read of the app table. The orchestrator will need to probe via `isQueryableTarget(source)` (or a method on `MaterializedChangeSource`) to tell the two apart.

## 4. IndexedDB `openWriter` and `TABLE_META` store layout (*Changes* 4)

*Changes* 4 states: *"Each call is one `target.database` read-write transaction"* and *"A table missing from `tableNameSet` is looked up in the meta store before the writer refuses it."* Two facts in `rdb_adapter_idb` prevent `IdbTarget.openWriter()` from using `target.database.transaction(...)` as an app does:

1. **`FacadeDatabase.transaction` rejects unknown tables synchronously.** `FacadeDatabase.transaction` (`rdb_adapter_idb/src/idb_facade.ts:973-978`) checks `this.host.appTables()` (`tableNameSet`) before opening an IDB transaction and throws `NotFoundError` synchronously. A table created by another tab after open can never reach an in-transaction `TABLE_META` lookup through `target.database.transaction`.
2. **`target.database` cannot access `COUNTERS` or `TABLE_META`.** `IdbTarget.internKey` (`rdb_adapter_idb/src/idb_target.ts:435-448`) allocates new key ids from `COUNTERS` (`COUNTER_NEXT_KEY_ID`), and the writer needs `TABLE_META` (for `nullability` and stale `tableNameSet` recovery) and the writer record. `FacadeDatabase.objectStoreNames` (`idb_facade.ts:970`) exposes only `appTables()` and `rdb_keys`.

Implement `IdbTarget.openWriter()` directly on `this.env.withReadWrite(...)` (one IDB transaction over `ALL_STORES`), sharing the facade's row-write, outbox, and index-entry helpers and calling `this.onOutboxCommitted(...)` after the transaction completes.

Separately, *Changes* 4 puts *"the writer record [as] a record in the meta store."* `TABLE_META` has `keyPath: 'table'`, and `IdbTarget.init()` (`idb_target.ts:142-144`) loads every record in `TABLE_META` via `storeGetAll` and adds `m.table` to `this.tableNameSet`. Putting a writer record in `TABLE_META` will leak its key into `tableNameSet` and `appTables()` unless filtered out—or the writer record can live in `CAPTURE_CONFIG` (`idb_schema.ts:156`), which already has `keyPath: 'key'` and is part of `ALL_STORES`.

## 5. Writer record & `authorship_mode` lifecycle on SQLite and `RdbProjection`

Three ordering and caching interactions affect *What the projection records in the target* and *Changes* 3 & 6:

1. **`RdbProjection.reconfigure()` runs before `sync()`.** In `RdbProjection.open()` (`rdb_projection/src/projection.ts:154-159`) and `onMembershipChange()` (`:455-465`), `reconfigure()` runs *before* `sync()` materializes the tables in `rdb_table_meta`. If `authorship_mode` is stored per row in `rdb_table_meta` (*Changes* 3), those rows do not exist yet during `reconfigure()`.
2. **Post-open `add-table` migrations.** A schema migration (`add-table`) arriving in an already-open group triggers `sync()` (`projection.ts:435`), not `reconfigure()`. `createTable` in `SqliteTarget` would insert a default `authorship_mode` rather than the group's `autoAuthor` setting unless `RdbProjection` syncs authorship modes after `sync()` as well (or the mode lives per-group/per-table in the writer record / `SchemaAction`).
3. **`PRAGMA schema_version` does not change on DML, and `writeMeta` overwrites `rdb_table_meta`.** Changing `autoAuthor` in config updates rows in `rdb_table_meta` or the writer-record table (DML), which leaves `PRAGMA schema_version` unchanged. A client process caching `TableMeta` in `metaCache` behind `PRAGMA schema_version` will miss an `autoAuthor` change. Also, `SqliteTarget.writeMeta` (`sqlite_target.ts:575-583`) runs `INSERT OR REPLACE` during `addColumn`/`dropColumn`, which would overwrite `authorship_mode` unless `TableMeta` carries it. Reading the writer record and `authorship_mode` directly from SQLite inside each write transaction (never from `metaCache`) avoids stale authorship modes.

## 6. `rhost` config validation rejects `autoAuthor`

*Changes* 7 updates `openProjection` and `rhost/src/host.ts:823, 912`, and *Changes* 10 documents `autoAuthor` in `rhost`'s configuration. In `rhost/src/config.ts:197`, `checkProjection` validates `ProjectionConfig` against a strict allow-list:

```ts
const o = c.object(value, field, ['path', 'indexes', 'indexPub', 'files']);
```

`ProjectionConfig` and `checkProjection` in `rhost/src/config.ts` must also accept `autoAuthor` (and `host.ts` must forward it into `openProjection`), or host configs specifying `autoAuthor` will fail validation at load.

## 7. Smaller points

- **`TableShape` defaults on SQLite (*Changes* 2 & 3).** `SqliteTarget` does not currently store logical defaults in `rdb_table_meta` (only DDL `DEFAULT` fragments, which `PRAGMA table_info` returns as raw SQL strings). Either store `defaults` as JSON in `rdb_table_meta` (no upgrade path required per Response 2), or let `TableShape` hold `hasDefault` booleans for `checkWrite` while SQLite relies on DDL `DEFAULT` for omitted columns.
- **`SqliteTarget.openWriter()` and lazy `ensureBookkeeping()`.** `SqliteTarget` sets `this.capture` inside `ensureBookkeeping()` (`sqlite_target.ts:452`), not in the constructor. Calling `openWriter()` on a fresh `SqliteTarget` before any checkpoint/query call will falsely fail with `no-capture` unless `openWriter()` calls `this.ensureBookkeeping()` first.
- **`author_key_id` in `prefetchKeys` and SQLite native inserts.** `prefetchKeys` (`ingest_orchestrator.ts:95-128`) currently ignores `authorColumn`; it must prefetch `authorColumn` for captured inserts so the planner can compare `keyHash === ctx.writerKeyId`, and `ingest.ts` must reject non-numeric values in `author_key_id` (possible via SQLite `INTEGER` affinity).
- **Splitting multi-column updates when `updateMerge: false`.** `planOps` (`ingest.ts:137-168`) should emit per-column `NetChange`s when `updateMerge` is `false`, not just per-column `UpdatePart`s inside one merged `NetChange`. Also, two translate-time rejects on the same row in one pass (e.g., a native `put` touching both a readonly column and `author_key_id`) share `{ values: {} }` in `failWrite` (`ingest.ts:451`) and will collide on `UNIQUE (op_hash, direction)` in `rdb_op_events`.
- **`QuerySourceImpl` and `isWritableTarget`.** If `QuerySourceImpl` (`rdb_adapter/src/query_builder.ts:168-181`) defines `openWriter()` unconditionally on its prototype, `isWritableTarget(engine.source)` returns `true` even when `engine`'s target is read-only. Only expose `openWriter` on `QuerySourceImpl` when `isWritableTarget(target)`. On `rdb_react`, `useWriter()` should `useMemo` the writer on `[source]`, matching `useQuery` (`rdb_react/src/use_query.ts:16-19`).
- **Unspecified `RowWriter` & `checkWrite` edge cases:**
  - **`writerKeyId()` before a projection records a writer key:** should state explicitly that it throws `WriteRefusedError` with reason `no-writer-key`.
  - **`undefined` values in `RowValues`:** state whether `{ col: undefined }` counts as "not given" (matching `IdbFacade.businessValues`, `idb_facade.ts:247`) or throws `wrong-type`.
  - **`{ pk }` in `author_key_id`:** *Background -> Keys* lists `author_key_id` as a key column while *Authorship* only mentions key ids and `null`; state whether `{ pk }` is refused there (`author` / `wrong-type`) or accepted when it equals the writer key.
  - **Memory `localInsert`/`localUpdate` with `author_key_id` in `values`:** `MemoryTarget` (`memory_target.ts:264-286, 361-367`) stores anything in `values` into `row.values` and keeps `author` in `row.author`. When capturing the author column, `localInsert`/`localUpdate` must route `state.authorColumn` to `row.author` (not `row.values`), or `asView` will return a stale `author_key_id` on anonymous rows after project-back.
- **Minor citation drift after `6110da1`:**
  - `rdb_adapter/src/ingest.ts:316-342` → key-ref reversal is `:305-331`.
  - `rdb_adapter/src/ingest_orchestrator.ts:446-452` → `isWriteOnceFor` is `:455-461`; `:413-415` is `:411-415`.
  - `rdb_adapter/src/project.ts:90-91` → direction mapping is `:89-90`.
  - `rdb_adapter_sqlite/src/sqlite_target.ts:215` → `toLogical` is `:213`; `:504-517` is `:508-516`.

## 8. Recommendations

1. **In *Changes* 1 (`ingest_orchestrator.ts` & `ingest.ts`):**
   - Omit `event.op` and hash a serialized fallback when `syntheticOpHash` throws (`J1`) so `SqliteTarget.logOpEvent` does not fail `commitIngest`.
   - Treat `deriveRowId(existing.uuid, signer) !== existing.rowId` in `mintInserts` as non-reusable (revert to rdb's row), and ensure already-applied insert reverts never set `syncStatus` to `ingestion_failure`.
   - Specify how `ingest_orchestrator` checks local row presence for dropped inserts (`isQueryableTarget`), prefetch `authorColumn` in `prefetchKeys`, and handle `updateMerge: false` on multi-column native updates.
2. **In *Changes* 3, 4 & 6 (`SqliteTarget`, `IdbTarget`, `RdbProjection`):**
   - Run `IdbTarget.openWriter()` via `env.withReadWrite` (reusing facade helpers) and store the writer record in `CAPTURE_CONFIG` (or filter it from `TABLE_META` in `init()`).
   - Have `RdbProjection` update authorship modes after `sync()` as well as in `reconfigure()`, read writer/authorship state inside each write transaction on SQLite rather than caching it behind `PRAGMA schema_version`, call `ensureBookkeeping()` in `SqliteTarget.openWriter()`, and decide `defaults` storage vs `hasDefault` in `TableShape`.
3. **In *Changes* 7 & *Design*:**
   - Update `ProjectionConfig` and `checkProjection` in `rhost/src/config.ts` to allow `autoAuthor`.
   - Specify `writerKeyId()` on missing key, `undefined` in `RowValues`, `{ pk }` in `author_key_id`, `MemoryTarget` routing `authorColumn` to `row.author`, and conditional `openWriter` on `QuerySourceImpl`.
