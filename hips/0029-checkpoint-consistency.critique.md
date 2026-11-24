# Critique of HIP-29 and the coupled revisions to HIP-20

A unified review of [HIP-29: Checkpoint consistency](0029-checkpoint-consistency.md), the 2026-11-19 revisions to [HIP-20: Unified write API for projection targets](0020-unified-write-api.md), and the partial HIP-20 implementation in the working tree on 2026-11-19.

Paths are relative to `modules/`.

## Verdict

**Is spinning off HIP-29 the right call?** Yes. Trying to handle `INSERT OR REPLACE` over a locally inserted row in HIP-20 exposed the deeper flaw: ingestion appends at the DAG frontier without moving the checkpoint, relies on projection's echo to reconcile, lets projection overwrite pending changes, drops held-back reverts, and identifies crash replays by rdb's refusals. HIP-29's core invariant (*target = rdb at checkpoint + outbox on top*, ingestion chaining at the checkpoint, settle advancing the checkpoint, projection refusing over a non-empty outbox, and batch-driven crash replay) addresses the real cause.

**Is anything wrong in the new plans?** Yes. One soundness hole affects HIP-29's cross-group `observe` claim, and several edge cases sit in held-back reverts, settle side effects, `PendingChangesError` scheduling, SQLite JSON settlement parity, and HIP-20's new `tableNames(group)`:

| Item | Target HIP & Section |
|---|---|
| 1. An `observe` entry in the chain can void or reinstate rows in `C` | HIP-29: *The settle moves the checkpoint*, *Why it is sound* |
| 2. Cross-group `observe` monotonicity, unprojected members, and spurious `propagate` | HIP-29: *Ingestion at the checkpoint*, *Changes* 1 |
| 3. Held-back reverts: `projectGroupTo` early return, absent rows, and insert orphans | HIP-29: *Held-back reverts*, *Changes* 1 |
| 4. Canonical checkpoint `{tip}` vs literal `C ∪ I` | HIP-29: *Ingestion at the checkpoint*, *The settle moves the checkpoint* |
| 5. Settle author writes vs reactive queries, IndexedDB `@author` indexes, and pending deletes | HIP-29: *Settlement parity*, *Changes* 1–3 |
| 6. Lost wake-up and unbounded re-ingest loop with `PendingChangesError` | HIP-29: *No projection over a pending change*, *Changes* 1, 4 |
| 7. Crash replay: clearing the batch record and separating batch-minted mappings | HIP-29: *Crash replay* |
| 8. SQLite `CHECK (json_valid(<col>))` admits JSON `'null'` | HIP-20: *Changes* 1, "SQLite columns refuse values..." |
| 9. `tableNames(group)` breaks on read-only projections | HIP-20: *What the projection records in the target*, *Table names* |
| 10. Current worktree diff: `MemoryTarget` does not filter `state.idColumn` | Worktree (`rdb_adapter/src/memory_target.ts`) |

## 1. An `observe` entry in the chain can void or reinstate rows in `C`

HIP-29 (*The settle moves the checkpoint*) states:

> *"An entry after C changes no row it doesn't write. Nothing in C is concurrent with it, so it can void or reinstate nothing there."*

That holds for row ops and bundles, **not for an `observe` entry**.

In `rdb/src/rtable_group/group.ts:663-664` (`resolveForeignTableViewIn`, used by `diagnoseEntry` for cross-group FKs and `EXISTS` restrictions) and `:1282` (`diagnoseObserve`, for gated observes), evaluating an entry `e ∈ C_A` from horizon `from` resolves:

```ts
const foreignAt = await canonicalVersion(foreignDag, fold.live);
const foreignFrom = await canonicalVersion(foreignDag, await resolveRefVersionAtPosition(dag, groupId, from, from));
```

When `flush(A)` appends `obs = observe(B, V_B_new)` after `C_A`—including during the end-of-pass drain when `A` has no local writes of its own (`rdb_adapter/src/ingest_orchestrator.ts:571-581`)—`from` moves from `C_A` to `{obs}`, which advances `foreignFrom` for **every prior entry `e ∈ C_A`** from `V_B_old` to `V_B_new`. If `V_B_new` contains a concurrent revocation, delete, or schema drop in `B` that voided (or reinstated) a witness or FK target at `foreignAt` that `V_B_old` had not seen, `e ∈ C_A` flips verdict at `{obs}`.

If the settle moves `A`'s checkpoint from `C_A` to `{obs}` without projecting `obs`, any newly voided row stays in `A`'s target permanently (the next delta starts at `{obs}`, where `e` is already void) and no `void`/`reinstate` op event is logged.

**Change.** Whenever `flush` appends an `observe` advancing a group's tip from `prevTip` to `{obs}`:
- Compute `group.computeDelta(prevTip, {obs})` for the `observe` step and fold its `rowActions` (under the same pending-change revert gate) and `opVerdictEvents` into `commitIngest`; or
- For a group that received only end-of-pass drain `observe`s (no row ops of its own after `obs`), leave its checkpoint at `C_A` in the settle so the next projection pass projects through `{obs}`. (If the group had row ops after `obs` in the same batch, `obs`'s delta must still be applied in the settle.)

## 2. Cross-group `observe` monotonicity, unprojected members, and spurious `propagate`

Three cross-group details in `rdb_adapter/src/ingest_orchestrator.ts` interact with checkpoint-based ingestion:

1. **Monotonicity (`rdb/src/rtable_group/validate_ops.ts:604`).** `validateRefAdvanceMonotonicity` requires `newRefVersion` to extend every prior observation visible at `at`. Because `projectDatabase` projects groups in separate transactions—and `PendingChangesError` can stop it between groups—`C_A` can already observe `B` at a version `V_B_obs` ahead of or concurrent with `C_B`. If `B` appends `I_B` at `C_B` and `flush(A)` calls `A.observe(B, {I_B}, author, C_A)`, rdb rejects it as non-monotonic. `flush` must observe at least the join of what `A` already observed at its current tip with `B`'s chain tip (or skip if already covered).
2. **Unprojected members (`checkpoint === undefined`).** If a newly added member group `B` has no checkpoint yet when `ingestDatabaseChanges` runs for another group `A`, `ingestDatabaseChanges` must skip `B` rather than building a context at `undefined` or flushing `observe`s into `B`.
3. **Spurious `propagate` on failed entries.** `ingest_orchestrator.ts:568` calls `propagate(groupId)` even when `submitEntry(entry)` failed and appended nothing. Under HIP-29, `propagate` should run only when the group's tip actually advanced, avoiding redundant `observe` entries.

## 3. Held-back reverts: `projectGroupTo` early return, absent rows, and insert orphans

1. **`versionsEqual(checkpoint, to)` skips flagged rows.** In `rdb_adapter/src/project.ts:138`, `projectGroupTo` returns immediately when `versionsEqual(checkpoint, to)` (and `SqliteTarget.apply` has an empty-batch short-circuit at `rdb_adapter_sqlite/src/sqlite_target.ts:338-340`). In HIP-29's own scenario:
   - Pass 1 rejects `memo = 'much too long'`, holds back the revert because `amount = '2.00'` is pending, and flags row 1.
   - Pass 2 ingests `amount = '2.00'`, moves `checkpoint` to `{I}`, and empties the outbox.
   - With no remote entries, rdb's frontier `to` equals `checkpoint`. `projectGroupTo` hits `if (versionsEqual(checkpoint, to)) return;` and never rewrites the flagged row.
   - **Change.** Check for flagged rows before the `versionsEqual` early return in `projectGroupTo` (and ensure `apply` does not short-circuit when clearing flags).
2. **Flagged row absent in rdb.** If the pending change that held back a revert was a `delete` that succeeded in Pass 2 (or a remote delete arrived before projection), rdb returns `undefined` for that row at `F`. Projection must handle an absent flagged row by emitting `delete-row` (or no-op if already gone) and clearing the flag, rather than assuming the row is live.
3. **Insert-orphan `delete-row` reverts stay un-gated.** Clarify that `delete-row` reverts for rejected inserts are not held back by pending changes inside `commitIngest` (matching `ingest_orchestrator.ts:604-613`). Holding back an insert orphan while `settle.statuses` marks its sync row `'ingestion_failure'` would leave a poisoned local row behind.

## 4. Canonical checkpoint `{tip}` vs literal `C ∪ I`

HIP-29 writes *"whose tip I gives the version C ∪ I"* and *"the settle sets each group's checkpoint to C ∪ I"*.

Every entry in `C` is strictly in the causal past of `tip`. Storing the literal set union `new Set([...C, tip])` produces a non-canonical version:
- `versionsEqual(checkpoint, to)` (`rdb_adapter/src/types.ts:582`, called at `project.ts:138`) checks raw set equality against canonical `dag.getFrontier()`, returning `false` even when the target is at the frontier.
- Passing a non-canonical `after` to `dag.append` (`dag/src/dag.ts:76-82`) embeds redundant ancestors in `header.prevEntryHashes`.

**Change.** State explicitly that the chain's tip version and the new checkpoint stored by the settle is the canonical singleton `{tip}` (`new Set([tip])`), which represents the causal closure of `C ∪ I`.

## 5. Settle author writes vs reactive queries, IndexedDB `@author` indexes, and pending deletes

Writing the signer's key id into `author_key_id` for accepted inserts with an empty author column inside `commitIngest` (with no echo to follow) needs three target-side details:

1. **Reactive queries.** Today `commitIngest` emits `{ source: 'settle', revertedTables, events }` only when `reverts` or `events` are non-empty (`rdb_adapter/src/memory_target.ts:523-529`, `rdb_adapter_sqlite/src/sqlite_target.ts:1265`, `rdb_adapter_idb/src/idb_target.ts:655-660`). Without the echo's `apply` event, a reactive query reading or filtering on `author_key_id` (such as `WHERE author_key_id = :me`) will not update after a native insert with an empty author unless `commitIngest` includes tables with author writes in its settle commit event.
2. **IndexedDB `@author` secondary indexes.** Projections support indexes on `AUTHOR_INDEX_COLUMN` (`@author`). On IndexedDB, `commitIngest` must call `syncIndexEntries` when updating `author_key_id`, or any `@author` index drifts permanently from `ROWS`.
3. **Pending delete race.** If a pending local `delete` removed the row between drain and settle, writing `author_key_id` must be a no-op (not throw on memory or re-insert a partial record on IndexedDB).

## 6. Lost wake-up and unbounded re-ingest loop with `PendingChangesError`

1. **Lost wake-up.** HIP-29 relies on *"the change's own signal starts the next one, which ingests it first."* That can stall in two cases:
   - In `RdbProjection.open()` (`rdb_projection/src/projection.ts:155-156`), `await p.sync()` runs before `await p.arm()`. On IndexedDB (`rdb_adapter_idb/src/idb_target.ts:704-708`), `addCommitListener` opens `BroadcastChannel` without priming for an already-pending outbox, so a write from another tab during `open()` triggers `PendingChangesError` and then sits un-ingested after `arm()`.
   - In `runSyncOnce()` (`projection.ts:548-562`), `apply()` can observe a new outbox row and throw `PendingChangesError` before the WAL watcher or `BroadcastChannel` callback runs (or for a native SQLite write without `wrapDatabase`), leaving `this.rerun === false`.
   - **Change.** When `syncDatabase` exits on `PendingChangesError`, have `RdbProjection` schedule a rerun (`this.rerun = true` / `this.schedule()`), and check/prime for an already-pending outbox when `arm()` finishes (or inside `IdbTarget.addCommitListener`).
2. **Unbounded inline re-ingest loop.** *"A cycle whose outbox filled up again during ingestion ingests again before it computes the delta."* Under continuous writes (HIP-29 Open Question 1), an unbounded loop inside `syncDatabase` never returns from `sync()` and hangs `projection.stop()` waiting on `this.idle`. Bound the inline re-ingest count and yield back to the scheduler.

## 7. Crash replay: clearing the batch record and separating batch-minted mappings

1. **Clearing the batch record.** State explicitly that `commitIngest` clears the recorded batch atomically in the settle transaction.
2. **Separating batch-minted mappings from pre-existing mappings.** If one batch contains both a legitimate new insert (`id = 2`) and an `INSERT OR REPLACE` over an already-settled row (`id = 1`) and crashes after `reserveMint`, both ids have active `sync` rows on replay. Storing the pass's newly minted `SyncMapping`s on the batch record itself (writing accepted mappings into `sync` only in `commitIngest`, or distinguishing batch-minted ids from pre-existing `sync` rows) keeps replay unambiguous and avoids exposing un-settled `rowHash`es through `rowHashForLocalId`.

## 8. SQLite `CHECK (json_valid(<col>))` admits JSON `'null'` (HIP-20 *Changes* 1)

HIP-20 *Changes* 1 adds `CHECK (json_valid(<col>))` to SQLite json columns for settlement parity.

In SQLite, `json_valid('null')` (and `json_valid(' null ')`) returns `1`. When `drainChanges` reads that value, `toLogical('null', 'json')` (`rdb_adapter_sqlite/src/sqlite_target.ts:218`) calls `JSON.parse('null')` → JS `null`, which `drainChanges` (`:1182`) turns into SQL `NULL`. Ingestion then writes SQL `NULL` to rdb, while SQLite still holds the 4-character string `'null'` (`WHERE col IS NULL` is `false`).

**Change.** Declare json columns with `CHECK (json_valid(<col>) AND json_type(<col>) != 'null')`. For SQL `NULL`, `json_type(NULL)` is `NULL`, so nullable columns still accept SQL `NULL`.

Minor caveat worth noting in HIP-29 (*Settlement parity*) and HIP-20: keeping non-canonical JSON text on SQLite (`{ "b": 1, "a": 2 }`) means `TableQuery.where('data = :v', ...)` (which binds canonical JSON via `sqlParam`) will not match that row on SQLite, even though `executeQuery` reads it back as equal by value.

## 9. `tableNames(group)` breaks on read-only projections (HIP-20)

HIP-20 adds `tableNames(group)` to `QuerySource` and stores each group's member name in the writer record (*What the projection records in the target*):

> *"When a projection opens with a writer key, it records three things in the target..."*

A read-only `RdbProjection` (opened without `options.writer`) is still a `QuerySource`, and bindings call `db.tableNames(group)` to construct queries. If the record is written only when a writer key is present, `tableNames(group)` will throw on every read-only projection.

**Change.** Record each group's member name (and the replica's hash suite) on open and reconfigure even when `options.writer` is absent, leaving `writerKeyId` unset so writes still refuse with `no-writer-key`. Also make the new schema-table name field on `create-table` optional (defaulting to `table`) so existing tests that hand-craft `create-table` actions keep working.

## 10. Current worktree diff: `MemoryTarget` does not filter `state.idColumn`

In the uncommitted HIP-20 *Changes* 1 worktree (`rdb_adapter/src/memory_target.ts:274-302`), `localInsert` (and `localUpdate`) routes `state.authorColumn` to `row.author` but still copies `state.idColumn` into `row.values` if present. Ingestion ignores `state.idColumn` (`isSystemColumn`), while `asView` (`:413-420`) overlays `row.values` onto `{ id }`, letting a stray `id` in `values` overwrite the row's real local id in query results.

**Change.** Skip or reject `state.idColumn` in `MemoryTarget.localInsert` and `localUpdate`.

## 11. Recommendations

1. **HIP-29 (`observe` & cross-group):** Apply `computeDelta(prevTip, {obs})` in the settle when an `observe` is appended (or leave drain-only observers at `C`), enforce monotonicity by joining with what `at` already observed, skip unprojected members (`checkpoint === undefined`), and propagate only when a group's tip actually advanced (§1, §2).
2. **HIP-29 (reverts & checkpoints):** Check for flagged rows before the `versionsEqual(checkpoint, to)` early return in `projectGroupTo` and `SqliteTarget.apply`, handle absent rows on flagged reverts, keep insert-orphan `delete-row` reverts un-gated, and store canonical `{tip}` as the checkpoint (§3, §4).
3. **HIP-29 (settle, scheduling & replay):** Emit settle commits and sync IndexedDB `@author` indexes on settle author writes (no-op if row was deleted), guarantee a rerun on `PendingChangesError` with a bounded inline re-ingest loop, clear the batch record atomically in `commitIngest`, and distinguish batch-minted mappings from pre-existing `sync` rows (§5–§7).
4. **HIP-20 & worktree:** Use `CHECK (json_valid(<col>) AND json_type(<col>) != 'null')` on SQLite json columns, write group member names on read-only projections too so `tableNames(group)` always works, and ignore/reject `state.idColumn` in `MemoryTarget.localInsert`/`localUpdate` (§8–§10).
