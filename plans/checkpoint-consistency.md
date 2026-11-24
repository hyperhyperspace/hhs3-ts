# Implementing HIP-29: checkpoint consistency

- **Status:** planned, not started
- **Written:** 2026-11-19, revised 2026-11-21 after the third critique and 2026-11-22 after the fourth and the fifth and 2026-11-23 after the sixth, the seventh and the eighth, against `6110da1` plus the working tree, which carries HIP-20's *Changes* 1 except its step 6 (line numbers below are from that tree). Step 6, the native insert over an existing local id, now lands in P5. The user commits the working tree's *Changes* 1 before P0, so that *Expected test flips* has a fixed base
- **Reviewed:** decisions 1 to 19 were settled in discussion from 2026-11-19 to 2026-11-23 ([third critique](../hips/0029-checkpoint-consistency.critique-3.md) §3, [fourth critique](../hips/0029-checkpoint-consistency.critique-4.md) §3, [fifth critique](../hips/0029-checkpoint-consistency.critique-5.md) §3, [sixth critique](../hips/0029-checkpoint-consistency.critique-6.md) §3, the [seventh critique](../hips/0029-checkpoint-consistency.critique-7.md), the [eighth critique](../hips/0029-checkpoint-consistency.critique-8.md), and their responses). *To confirm* has nothing left. The details under *Implementation details* are proposed.
- **Design:** [HIP-29](../hips/0029-checkpoint-consistency.md), revised 2026-11-22. Its *The algorithm* gives the whole cycle as pseudo-code. This plan adds what the HIP leaves to the implementation.
- **Modules:** dag, rdb, rdb_adapter, rdb_adapter_sqlite, rdb_adapter_idb, rdb_adapter_test, rdb_adapter_test_gen, rdb_projection, rhost; tests in rdb_react, rdb_tools, rdb_repl

## Summary

HIP-29 keeps each group's tables equal to rdb's rows at the group's checkpoint, with the outbox's changes on top. Four rules do it:
- ingestion appends each captured change as one entry, in a chain that starts at the checkpoint;
- the settle moves the checkpoint to the end of the chain;
- projection never applies while a change is pending;
- a revert writes rdb's row with the row's pending changes on top.

Each change's outbox row records what became of it, and the end of a pass reads only those records, the checkpoints and rdb. So a crashed pass is finished by the next cycle.

The work goes in nine phases, each green and test first, with three stops for review. HIP-20's step 6 lands in P5. After it come HIP-20's Phases C to E.

## Decisions

1. **One entry per change.** Bundling, merging and the column fallback go. An update lands or is refused whole.
2. **Capture records every column a write names.**
   - SQLite: one outbox row per updated row, filled by per-column `UPDATE OF` triggers.
   - Memory: `localUpdate` records the columns it is given.
   - IndexedDB: a `put` over an existing record captures every column, and the author as HIP-20 has it, so `put` keeps IndexedDB's meaning of replacing the record. The facade gains `patch`, a partial `UPDATE`: it writes the fields it is given, clears the fields it lists, keeps the rest, and captures exactly the fields it names. It compares nothing; comparing is for the libraries above it (decided 2026-11-22, fifth critique 20 as revised in discussion). Its shape is `store.patch(fields, { delete?: string[] })`, with the key in `fields`, and a `patch` of a key with no record writes and captures nothing, as an `UPDATE` that matches no row (decided 2026-11-23).
   - HIP-20's writer compares only when its update is given `compare` (decided 2026-11-21).
   - Ingestion never leaves out a named column because it holds rdb's value. The readonly and author columns of decision 4 are the only exception, and foreign-key columns get none (decided 2026-11-21, fourth critique 14 and 15).
3. **Capture fills in the author** from HIP-20's writer record and the group's `autoAuthor` mode.
   - An insert captured with an empty author column in a group whose mode is `on` was written while no writer key was recorded, and ingestion refuses it, as HIP-20's writer refuses a write then (`no-writer-key`). Decided 2026-11-21, over keeping the settle's author write for this case.
   - The writer record and `autoAuthor` land in P3, ahead of HIP-20's Phase C (decided 2026-11-22): the setting in `AdapterConfig`, ingestion under `off`, `RdbProjection`'s option and its check, and rhost's config field. `openWriter`, `writerKeyId()` and the writer's own refusals stay in Phase C.
4. **Readonly and author columns** named with rdb's value for the row are set aside by ingestion. Another value refuses the update.
5. **A revert** writes rdb's row at C ∪ I with the row's pending changes on top, in the settle. No list of held rows.
6. **Each change's outbox row records what became of it**, as it happens:
   - the hashes of the observes written for it and of its entry, each written before its append (decided 2026-11-22);
   - its failure event when it is refused, which the settle moves to the event table (fourth critique, 18). A refused insert gives its local id an identity for the rest of the pass, wherever it was refused, and its failure event carries the uuid and row hash its `ingestion_failure` mapping needs, minted even when the planner refused it before its entry was built (seventh critique, §5, decided 2026-11-23);
   - a mark when it had nothing to write.

   Every change gets one of the three, and none is dropped: the planner's drops become refusals, and a change of a table no member projects gets the mark (fifth critique, 21). The records stay in the outbox rows (decided 2026-11-22). Their cost is to be measured in a browser, in HIP-20's trial app (sixth critique, 25). A change a pass doesn't reach, of a group with an identity provider and no writer key, has no record and stops the pass (decision 11).

   rdb splits `bundle` and `observe` into a step that builds, signs and validates, and a step that appends. No identity is saved before the append: `reserveMint`, the `reserved` status and claim-or-adopt go. Ingestion appends each recorded entry without looking first (decision 12).
7. **The end of a pass reads only what was stored** (fourth critique, 16). A crashed pass is finished by the next cycle, which then handles the rest of the outbox in a new pass (19).
   - A commit that returned is assumed kept, in the target and in rdb's store (decided 2026-11-22).
   - Nothing is done against a power failure that loses the last commits: no fsync, no keeping settled rows, no uuid at capture. HIP-29's *Crashes* says what such a loss does.
8. **A local op void where it stands is refused** by rdb. The second validation runs only while a gated binding's two readings differ, and ingestion's own observes follow the same rule. After each observe, ingestion compares again the readings of every gated binding of the observing group (fifth critique, 22).
9. **A moved checkpoint halts the projection,** at the apply and at the settle.
10. **The observes' effects come from one delta per group** at the end of the pass, from C to the end of the chain, for each group whose chain holds an observe. Its verdict changes give the events and the rows to rewrite (fourth critique, 17).
11. **A projection without a writer key for a group only warns** (sixth critique, 29, replacing the refusal at open of 2026-11-22). The pass stops at the first change of a group with an identity provider and no writer key, and settles the changes before it. No projection applies while it is pending, and no cycle schedules another because of it. The warning comes once per group, through a new `onWarning` option, or `console.warn` without it (decided 2026-11-23). A group without an identity provider needs no writer key: ingestion takes its changes and writes its observes anonymously (decided 2026-11-23).
12. **`Dag.append` writes nothing for an entry it already holds**, checked inside its transaction, and returns the hash. An entry's hash covers its payload and its parents, so this is an exact duplicate. It closes the race with sync too (fifth critique, *Outside this HIP*, decided 2026-11-22). The memory DAG store queues its transactions, as the SQLite and IndexedDB stores do, so the check is atomic on every store (sixth critique, 26).
13. **An error while the planner translates a change, or while rdb builds, signs or validates its entry, is the change's failure**, with the error's message as its reason, as today (sixth critique, 24). An error in an append, or from the target's own reads and writes, ends the pass as a crash does, and reaches `onError`. A pass never goes past a change without a stored record. A cycle that ends in an error schedules the next one with a backoff: 1 s, doubling up to 60 s, reset after a cycle that succeeds. A halt doesn't retry (seventh critique, §4, decided 2026-11-23).
14. **Every local id comes from its table's own counter.** On SQLite a row the projection writes is inserted first, and its sync row takes the id the insert produced. A reference to a row the target doesn't hold takes the next value of the table's `sqlite_sequence`. The sync table never makes up an id (decided 2026-11-23, in place of the seventh critique's §3 proposal).
15. **A recorded hash that differs from the appended entry's halts the projection** (seventh critique, §6, decided 2026-11-23).
16. **`prepareObserve` always validates**, and an observe that throws while it is built, or that needs a writer key the projection lacks, is declined like a refused one (seventh critique, §7, decided 2026-11-23).
17. **One lock per target object** keeps cycles and index reconciles apart, taken by `syncGroup`, `syncDatabase` and `reconcileIndexes`, in place of the lock keyed by the member groups (eighth critique, decision 30).
18. **The end of a pass builds the ingest result from the records**, and a declined observe isn't reported: the write that needed it is refused with its own reason (eighth critique, §5, decided 2026-11-23).
19. **The end's own observes are recorded beside their group's checkpoint**, in a list that every settle clears for every group, whether or not it moves that group's checkpoint, and not in an outbox row: they belong to no change (decided 2026-11-23, in place of the last outbox row with a record).
20. **The author is filled in at capture, and only there.** `RdbProjection` records the writer at open, before its first sync creates a table, so capture always knows the default author. The adapter's own calls (`projectGroup`, `ingestChanges`, `syncGroup`) are the projection's internals: they need the target set up as the projection sets it up, and the suites record the writer before they write. In a signed group, an insert whose author column is empty is ingested anonymously under `autoAuthor` off, and otherwise refused. No fallback signs an empty author with the writer key (decided 2026-11-24 during P5, in place of the earlier "no writer record: today's behavior", which left rdb's row signed and the target's author `NULL` once the echo was gone).

Deferred:
- the skew between groups ([HIP-30](../hips/0030-cross-group-skew.md));
- projection over pending changes ([HIP-31](../hips/0031-projection-over-pending-changes.md));
- explicit bundles (HIP-20's open question 1);
- a new row's hash before ingestion (HIP-20's open question 4);
- surviving the loss of the last commits, for a later design that stores the changed rows in the outbox from the triggers;
- the records in rdb's store, in the append's transaction, to be decided once HIP-20's trial app measures their cost in a browser, and cheapest before a release (HIP-29's second open question; sixth critique, 25);
- a lock on the outbox against starvation, looked at on 2026-11-22: the native write paths can't wait for it.

## To confirm

Nothing. The three items listed here were decided on 2026-11-23: `patch`'s shape and its key with no record (decision 2), and the end's own observes, recorded beside the checkpoint (decision 19).

## Background

Terms from HIP-29: C (a group's checkpoint), the chain with tip I, C ∪ I (stored as `{I}`), a change's record, the end of a pass, the settle, a revert.

Code that already exists and is reused:
- **The oracle.** `rdb_adapter_test/src/rdb_projection_oracle.ts`, a full projection at a version, is the base of the consistency check.
- **Hashes without appending.** `computeEntryHash` (`dag/src/dag.ts:20`, `:115`).
- **The two readings of a bound group.** `resolveObservedForeignVersion` and `resolveLiveObservedForeignVersion` (`rdb/src/rtable_group/group.ts:783`, `:794`).
- **A foreign read through the live observes.** `resolveSchemaForeignTableViewIn(…, fold === 'use')`, as `evaluateObserveGate` uses it (`group.ts:708-716`). The second validation reads bound groups this way.
- **A delta's verdict changes and their events.** `OpVerdictChange` with `rowId` and `writes` (`rdb/src/rtable_group/op_delta.ts:37-50`), and `opVerdictEvents` (`rdb_adapter/src/project.ts:85-101`).
- **The `applying` flag** (`rdb_adapter_sqlite/src/sqlite_target.ts:517-521`). The author-filling trigger suspends capture with it.
- **Trigger cleanup.** `dropTriggers` drops every `rdb_cap_` trigger of a table (`sqlite_target.ts:594-599`), so it also removes the old per-column triggers.
- **The `sqlite_sequence` re-seed** in `rebuildTable` (`sqlite_target.ts:969-988`), for the id allocation fix.
- **`CheckpointMovedError`** (`rdb_adapter/src/types.ts:581`). `PendingChangesError` sits beside it.

## Implementation details

1. **rdb's two steps.**
   - **The API.** `prepareBundle(writes, author?, at?, opts?)` and `prepareObserve(group, refVersion, author?, at?, opts?)` build, sign and validate, and return `{ payload, meta, at, hash }`. `appendPrepared(prepared)` appends. `bundle` and `observe` become the two steps in a row, unchanged for callers. The names are proposed.
   - **`prepareObserve` validates always.** `observe` passes its `selfValidate` condition (`rdb/src/rtable_group/group.ts:1037`), so its callers see no change.
   - **`appendPrepared` checks the hash.** It compares the hash the DAG returns with the prepared one, and throws a `PreparedHashMismatchError` when they differ (the name is proposed).
   - **The option.** `{ liveReads: true }` also validates with the bound groups read through the live observes, at the four places where row-op validation reads a bound group (`rdb/src/rtable_group/validate_ops.ts:390`, `:449`, `:514`, `:703`).
   - **The DAG's append** (`dag/src/dag.ts:98-112`). Inside the transaction, before the parents' check, `store.loadHeader(e.hash, ...tx)`. When the entry is there, it returns `{ fireListeners: false }` and `append` returns the hash. Both scoped DAGs append through it (`mvt/src/dag/dag_nesting.ts:60`, `:188`). The SQL and IndexedDB stores don't change: they run one transaction at a time (`dag_sqlite/src/sqlite_connection.ts:41-45`, `dag_idb/src/idb_env.ts:108-117`). `MemDagStorage.withTransaction` (`dag/src/store/mem_dag_store.ts:16-22`) gets a queue like SQLite's: each call chains on the previous one, and a failure doesn't break the chain. Reads outside a transaction stay as they are. Only `Dag.append` calls `withTransaction`, and listeners aren't awaited, so the queue can't deadlock.
2. **The planner** (`rdb_adapter/src/ingest.ts`).
   - **Removed.** The split and merge of updates (`:157-209`), `groupEntries` (`:613-637`) and the `fallback` field. `fkBundling` and `updateMerge` leave `AdapterConfig` (`types.ts:89-106`) and rdb_projection's `ScopeOptions` (`scope.ts:41-43`, `:127`).
   - **Readonly and author columns.** The refusals of a readonly column in an update (`:361-362`, `:387-388`, `:414-415`) apply only when the value differs from rdb's row at the tip, compared by value. An author column is compared by key hash, through the key table.
   - **An empty author column** in an insert, in a group whose mode is `on`, is refused.
   - **Nothing to write.** An update whose named columns are all set aside gives no op, and the change is marked. So is a change of a table no member projects (`:581`).
   - **No drops.** Each becomes a refusal with a kind, a table and a local id, so the orchestrator records its failure (`ingest_orchestrator.ts:442-444`):
     - an insert on a local id that already has an identity: a mapping of any status (`:251-268`), one minted earlier in the pass (`:592-596`), or an insert refused earlier in the pass. Its revert reads that identity's row, and its mapping keeps its status;
     - an update or a delete of a row whose insert was refused (`:461-467`);
     - a change of a table without an rdb table, which today has no kind (`:451-453`).
   - **`mintInserts`** mints only for a local id with no identity, and also for an insert it refuses, so that its failure event carries a row hash. The read-back of an active mapping's uuid goes (`:270-273`), and so does the `drops` map.
3. **Capture on SQLite.**
   - **The outbox** gains `open INTEGER NOT NULL DEFAULT 0` and `record TEXT`, with a partial index on the open rows. A record is JSON: `{ observes?: [{ group, hash }], entry?: hash, failure?: OpEvent, nothing?: true }`. `rdb_checkpoint` gains `end_observes TEXT`, a JSON list of hashes (`sqlite_target.ts:455`). No upgrade path: stores are unreleased.
   - **Updates.** A `BEFORE UPDATE` trigger closes the row's open outbox row. One `AFTER UPDATE OF <column>` trigger per column opens this update's outbox row if none is open, and adds its column with `json_set`. Both are gated as today.
   - **The checks of 2026-11-20** (the critique's): every named column is captured, changed or not; one statement over two rows gives one row each; `NULL` is kept; the id is never captured; two connections give two rows.
   - **Inserts.** One `AFTER INSERT` trigger:
     1. sets `applying`;
     2. writes the writer key's id into an empty author column, when the table's group mode is `on`;
     3. clears `applying`;
     4. records the insert from the stored row.

     It is installed only for tables with an author column. Checked 2026-11-21 (W1 to W13 of the fourth critique): the fill works inside a transaction, an explicit author is kept, and the mode `off` leaves the column empty. `RETURNING` shows `NULL`.
   - **Reinstalling.** The triggers embed the table's group id, read from the table meta (detail 5).
4. **Capture on memory and IndexedDB.**
   - **Memory.** `localUpdate` records the columns it is given. It still sets aside the row's own id, and throws on another (`memory_target.ts:316-337`). `localInsert` fills in the author.
   - **IndexedDB `put`.** A `put` over an existing record captures every captured column (`capturedColumns`, `rdb_adapter_idb/src/idb_facade.ts:264-267`), and `changedColumns` (`:274-286`) leaves capture. An update through HIP-20's writer, which shares this code, captures only the columns it writes. An author the record leaves out keeps the stored value and isn't captured. `add`, and a `put` of a new record, fill in the author. The facade's transaction gains the capture config store, so it can read the writer record.
   - **IndexedDB `patch`** (decision 2). A column both given and listed is a `DataError`. It reads the stored record, writes the given fields over it, clears the listed ones, keeps the rest, updates the index entries, and captures exactly the fields it names, the cleared ones as `NULL`. It compares nothing. A key with no record writes and captures nothing. It goes through the same capture path as HIP-20's writer update.
   - **The record** is a field of the outbox record on IndexedDB, and of the outbox entry on memory. The end's observes are a field of the checkpoint record on IndexedDB (`idb_schema.ts:149`), and of the checkpoint entry on memory (`memory_target.ts:91`).
5. **The writer record and `autoAuthor`**, as HIP-20's *Changes* 2, 3, 4, 6 and 7 specify them:
   - the target interface;
   - SQLite's one-row table;
   - IndexedDB's record in the capture config store;
   - memory's fields;
   - the group id and schema name in each table's meta;
   - the projection recording it at open and on each reconfigure;
   - `autoAuthor` in `AdapterConfig`, and `RdbProjection`'s option with its per-group override, an anonymous group recorded as `anonymous`, and the refusal of `autoAuthor: false` with `authorColumn: false`;
   - ingestion under `off`: each insert signed as its captured author column says, its row hash from that signer, op events naming the signer;
   - rhost: `autoAuthor` in `ProjectionConfig` and `checkProjection` (`rhost/src/config.ts:33`, `:195`), passed to `openProjection`.
6. **The settle**, target side.
   - **The steps.** HIP-29's eight, in one transaction. `IngestSettle` gains:
     - `checkpoints` (expected and next, per group);
     - `inserts` (local id, row hash, status);
     - `deletes`, marked after the inserts' mappings are written, so that an insert, a delete and an insert on one local id leave it `deleted` (sixth critique, 28);
     - `orphans` (refused inserts on a local id with no identity yet: row removed, mapping `ingestion_failure`);
     - `reverts` (table, local id, and rdb's row as a row action, or none), covering the rows of refused changes, refused inserts on a local id with an identity among them, and the rows of ops whose verdict the delta changed, without duplicates. A row the delta reinstates has no local id here: it is keyed by its row hash and written as projection writes a reinstated row (`rdb_adapter/src/types.ts:524-528`);
     - `jsonWrites`;
     - the delta's events.
   - **Failure events.** The target reads them from the handled outbox rows, and stores them before the delta's events.
   - **The schema.** The orchestrator checks that no delta has a schema action, and halts the projection if one does (detail 9).
   - **A revert.** The target writes rdb's row through its projection path, then each of the row's remaining outbox changes as capture recorded it: an update writes its columns, a delete removes the row, and an insert on the row's local id writes its whole row. An update or a delete of a row that isn't there does nothing. It reads the remaining outbox inside the transaction, indexed by table and local id.
   - **Rows named by hash.** A row the delta reinstates is written as projection writes a reinstated row. A voided delete brings its row back under the local id its mapping kept, and sets the mapping `active`. An own insert that an observe voided is found by its row hash after step 3 writes the inserts' mappings.
   - **Writing a record.** The change source gains `writeRecord(outboxId, record)`, which merges into the row's record, and a read of the records with the outbox. The target gains `recordEndObserve(groupId, hash)`, which adds to the list beside the group's checkpoint, and returns the list with the checkpoints. Every settle clears every group's list, in its transaction, also for a group whose checkpoint it doesn't move. Neither write signals a change or moves a checkpoint (the names are proposed).
7. **The ingestion core** (`ingest_orchestrator.ts`, and a new `ingest_chain.ts`), in the order of HIP-29's *The algorithm*.
   - **Per group:** the tip, the schema at C, the gated bindings, and whether each binding's two readings differ.
   - **Per change**, in commit order:
     0. if its group has an identity provider and no writer key, warn once for the group and end the pass: its end settles the changes before it, and the cycle schedules no other;
     1. write the observes due, each recorded, then appended. Decline one that the gate refuses, that is void at its own position (`evaluateObserveGate`, fold `use`), that throws while it is built, signed or validated, or that needs a writer key the projection lacks. After each, compare again the two readings of every gated binding of the observing group;
     2. `prepare…`, with `liveReads` while a binding's readings differ;
     3. on a refusal, rdb's or the planner's, or any other error while translating, building, signing or validating, record the failure event, with the error's message as its reason, and go on. The catch wraps the planner and `prepare…` only. An error from the target, such as in `writeRecord` or in a read of the mappings, ends the pass;
     4. with nothing to write, record the mark and go on;
     5. record the entry's hash, then `appendPrepared` it, which writes nothing when rdb holds it (detail 1). An error in an append, of an entry or an observe, ends the pass and reaches `onError`. The record stays.
   - **Anonymous groups.** `authorFor` checks the identity provider before it asks for the writer (`:362-373`), and so does the observes' author (`:537`). A group without an identity provider gets no author and needs no writer key.
   - **The end of a pass** (`endPass`), a function with no parameters from the pass: the handled rows; each group's chain end, the canonical version of C with the group's recorded hashes that rdb holds, from the outbox rows and from the list beside its checkpoint; the observes still due, each declined one remembered so the loop ends; a delta for each group whose chain from C to its end holds an observe; the settle, and the ingest result: an accepted change for each handled row with an entry, and a rejection for each with a failure event, with the change from the outbox row and the reason from the event. A declined observe isn't in it (decision 18). A cycle runs it first when the outbox's first row has a record. For a refused insert it reads from what is stored whether it had an identity: none when the target has no mapping for its local id and no earlier handled row is an insert on it. Otherwise its revert reads the mapping's row, or the earlier insert's, from its entry when rdb holds it and none when it was refused.
   - **The observes still due** are read from the records and rdb, not from the pass's `dirty` set (`:513`).
   - **Identities.** An insert's uuid and row hash are kept for the pass that appends it, so later changes in the pass resolve through them. The end reads them from the entry, for the mapping.
   - **Removed.** `reserveMint`, the re-plan after it (`:397-411`), claim-or-adopt (`types.ts:330-347`), the bundle fallback (`:497-509`), the write-once and not-live skips (`:475-485`), the failure list in memory (`recordFailure`, `:417-430`), the warning-only path for a refusal without a kind (`:442-444`), and the planner's drops (detail 2).
8. **Pending changes and the cycle.**
   - **`hasPendingChanges()`.** A pure read on the change source. It replaces the orchestrator's helper of that name, which drains the whole outbox (`ingest_orchestrator.ts:235-241`).
   - **`syncDatabase`** returns `{ results, refused }`. `RdbProjection.sync()` still returns the results.
   - **No writer.** `syncDatabase` ingests the members that have a writer key or no identity provider (`ingest_orchestrator.ts:646`). Writers are per group (`rdb_projection/src/scope.ts:126`). A pass that stops at a change of a group with an identity provider and no writer key says so in what `syncDatabase` returns, and `RdbProjection` then schedules no other cycle: the next one starts when rdb grows or the outbox signals (`projection.ts:550-562`).
   - **The warning.** `RdbProjectionOptions` gains `onWarning?: (message: string) => void`, with `console.warn` when it isn't given. It fires once per group: at open when a change of that group is pending, or else when a cycle first stops at one. It names the group and says that remote changes wait until a projection opens with a key for it.
   - **Errors.** A `PendingChangesError` never reaches `onError`.
   - **The lock** (decision 17). `withLock` keys by the target object, in a `WeakMap` from target to the last promise, and `syncGroup`, `syncDatabase` and `reconcileIndexes` take it (`ingest_orchestrator.ts:205-219`, `:251`, `:632-635`, `index_reconcile.ts:48`). No holder of the lock calls another, so it can't deadlock.
   - **The retry.** After a cycle that ends in an error other than a halt, `RdbProjection` schedules the next cycle itself: 1 s, doubling up to 60 s, reset after a cycle that succeeds. A pass stopped at a group without a writer key schedules none.
9. **The halt.** A moved checkpoint at the apply or at the settle, a delta with a schema action, or a `PreparedHashMismatchError` stops the cycles, makes `sync()` throw, and fires `onError` once. Reads keep working. It doesn't call `stop()`, which would wait for the running cycle. `syncWithCasRetry` goes (`projection.ts:202-212`).
10. **Memory's settle is atomic**, by clone and swap, like its `apply`.
11. **Signatures.** `ingestChanges` and `ingestDatabaseChanges` take a `BidirectionalTarget`.
12. **Local ids on SQLite** (decision 14, HIP-29's *Local ids*). `upsertRow` of a row with no sync row inserts the app row first and writes the sync row with the id it got (`RETURNING`). `allocateId` for a reference takes `UPDATE sqlite_sequence SET seq = seq + 1 WHERE name = ? RETURNING seq`, after creating the table's row at 0 when it has none. The row that arrives later is inserted with that id. The sync table's largest id plus one goes (`allocateId`, `sqlite_target.ts:1023-1031`). The keys table, `rdb_keys`, is no app table and keeps its own allocation (`internKey`, `:1033-1051`).
13. **The consistency check.** After every step, each group's rows equal the oracle's projection at the checkpoint, with the outbox applied on top in commit order: an update writes its columns, a delete removes the row, an insert writes its whole row, and an update or a delete of a missing row does nothing. There is no mask.

## Phases

Each phase keeps the tree green and is test first: every new case fails on the code before its change. New case numbers continue from:
- the shared suite: IN36;
- SQLite: SQL24;
- IndexedDB: ADPTI-09;
- memory: ADPTM04;
- the planner: ADPTI22;
- rdb_projection: RDBPROJ16;
- rhost: RHOST28;
- rdb: BUNDLE08, OBSGATE11, ENF20;
- dag: ENTRY_04.

### P0: scaffolding

- `rdb_adapter_test/src/consistency.ts`: `assertAtCheckpoints(...)` (detail 13).
- The suite wrappers `BeforeApply` and `BeforeSettle`, a hook that crashes a pass after a given change or during its end, hooks that make one append or one `writeRecord` throw, and a harness option that leaves every end of a pass to a new cycle.
- `IngestionHarness.reopen?()` for SQLite and IndexedDB.
- The IndexedDB mutator's `update` gets a switch to go through `patch` once it exists, with the fields given as `null` in its list to clear (`rdb_adapter_idb/test/idb_target_tests.ts:63-72`). P3 turns it on.
- Fixtures:
  - a signed `perm.caps` group and an anonymous `items` group whose writes need a cap;
  - the void scenario's groups: K holds a grant; G binds K behind an admin gate, with a rule form and a foreign-key form;
  - the gate-graph scenario's groups: G binds K and M, and M's gate reads K.

### P1: rdb

- **The two steps,** the `liveReads` option, the hash check, and the DAG's append (detail 1).
- **The chain sweep**, moved from P7: it tests the claim the later phases build on, a delta that starts at a chain's tip, and needs only the generator and today's projection.
  - **The generator.** `rdb_adapter_test_gen/src/history.ts` gets a `chain` op that appends two or three entries at an older checkpoint and records the tip.
  - **The sweep.** `project_parity.ts` sweeps the pairs that start at such a tip, through `checkPair`.
- **Tests:**
  - ENTRY_04, on the memory, SQL and IndexedDB stores: a second append of an entry returns its hash, writes nothing, fires no growth event, and leaves the entry's children in the frontier. Two appends of one entry at once both return its hash, and the entry is written once.
  - BUNDLE08 and OBSGATE11: the two steps give the entry and hash that one call gives. OBSGATE11 also: with `selfValidate` off, `prepareObserve` refuses an observe the gate refuses to the writer, and `observe` doesn't, as today.
  - BUNDLE09: `appendPrepared` throws `PreparedHashMismatchError` for a prepared entry whose hash the test changed, and appends nothing.
  - The chain sweep, in both adapters' parity runs: pairs that start at a chain's tip.
  - ENF20: the rule form. With the option the insert is refused, and without it it is accepted.
  - ENF21: the foreign-key form.
  - ENF22: after a live observe, and after a later observe by an admin, the insert is accepted with the option.
  - ENF23: the two readings are equal in every state but the void observe's.
  - ENF24: in the gate-graph scenario, an observe of K voids G's observe of M; M's readings then differ, and with the option a write that reads M is refused.

### P2: no projection over a pending change

- **`PendingChangesError`.** Each target's `apply` checks the outbox inside its transaction, in this order:
  1. closed;
  2. the compare-and-set;
  3. the outbox;
  4. the idle early return.

  The check is skipped without capture.
- **`hasPendingChanges()`** (detail 8).
- **`syncDatabase` and `syncGroup`:**
  - ingest once more if the outbox filled up during ingestion;
  - catch the refusal;
  - apply the no-writer rule (detail 8), and ingest the anonymous members without a writer key;
  - drop the old first-projection check (`ingest_orchestrator.ts:658-662`);
  - take the lock per target (detail 8).
- **`RdbProjection`:** reschedule after a refusal, and `arm()` primes when a change is already pending.
- **Wrappers** pass the new method through.
- **Tests:**
  - IN36: apply refuses while any change is pending.
  - IN37: a change captured during the delta is kept, and the next cycle ingests it first.
  - IN38: a change captured during ingestion is ingested in the same cycle.
  - SQL24, ADPTI-09: the refusal across connections and tabs.
  - RDBPROJ16: a refusal reschedules without a signal.
  - RDBPROJ17: a write between `open()`'s sync and `arm()` is ingested.
  - RDBPROJ18: steady writes still let `sync()` and `stop()` return.
  - RDBPROJ19: the no-writer rule. Opened without a writer key over a pending change of a group with an identity provider, the projection opens and warns once, through `onWarning`, or `console.warn` without it. No projection applies, no cycle loops, and reads work. A change captured after an open over an empty outbox gives the same, with the warning when a cycle first meets it. Opened again with the key, the change is ingested at the checkpoint it was written against, and a projection then applies.
  - RDBPROJ23: opened without a writer key, an anonymous group's pending change is ingested, and a projection then applies.
  - RDBPROJ26: a group joins while a reactive cycle is held open, and `sync()` is then called. The second cycle waits for the first, and each pending change is ingested once.
  - RDBPROJ27: a group joins while a cycle is held open before its apply, and an index reconcile is then called. The reconcile waits, the cycle applies, the spec is installed, and nothing throws.
  - RDBPROJ25: a cycle that ends in an error is retried after 1 s, then 2 s, with no other write to trigger it, and a success resets the delay. A halt isn't retried.
- **Adapt:** SQL22, REACT02.
- **STOP A.** The disappearing edit is fixed here.

### P3: capture and authorship

- **The writer record and `autoAuthor`** (detail 5).
- **SQLite's triggers and outbox columns** (detail 3), **memory and IndexedDB**, with IndexedDB's `patch` (detail 4). The shared suite's IndexedDB mutator then updates through `patch`, so its cases name the same columns on the three targets, and IN06, IN50, IN51 and IN78 hold there. `put` keeps its own cases, IN39 and ADPTI-10.
- **The planner's rules** for readonly and author columns and an empty author (detail 2).
- **The id allocation fix** (detail 12).
- **Tests:**
  - IN39: a native update that names a column at its current value records it. An IndexedDB `put` records every column, and a `patch` exactly the fields it names.
  - IN40: a native insert's author reads the writer key's id at once. Anonymous groups, and groups under `autoAuthor` off, keep `NULL`.
  - IN41: an update naming the author and a readonly column at the row's own values lands, and at other values it is refused.
  - IN42: an insert with an empty author column, captured while no writer key was recorded, is refused.
  - IN43: under `autoAuthor` off, a native insert that leaves the author empty is ingested anonymously, and one that names the writer key's id is signed. A table with `authorColumn: false` signs its inserts with the writer key.
  - ADPTI22: the planner's set-aside and refusals.
  - SQL25: the outbox shapes of detail 3.
  - SQL26: local ids. A native insert after an id was taken for a reference gets a new id. An id taken for a reference after a native insert that has no mapping yet is another one. A row the projection writes takes the id its insert produced, and a reference to a table that never had an insert works.
  - ADPTI-10: a `put` over an existing record; an author left out is kept; the `@author` index entry is written at capture.
  - ADPTI-11: a `patch` writes the fields it is given, clears the ones it lists, keeps the others and their index entries, and captures exactly the fields it names, changed or not. A `patch` of a key with no record writes and captures nothing. A field both given and listed is refused.
  - RDBPROJ20: the projection records the writer record and each group's mode at open and on reconfigure, and a second target reads it.
  - RDBPROJ21: the `autoAuthor` option and its per-group override; a group with an identity provider, `autoAuthor: false` and `authorColumn: false` is refused, and an anonymous one opens.
  - RHOST28: the projection config takes `autoAuthor`, refuses a value that isn't a boolean, and passes it to the projection.

### P4: the settle, target side

Additive: the orchestrator doesn't send the new fields yet.
- `commitIngest` in HIP-29's order, with the compare-and-set, the inserts' mappings, the reverts of detail 6, the json writes, and the failure events read from the handled rows.
- `writeRecord` on all three targets.
- Memory's atomic settle (detail 10).
- **Tests:**
  - IN44: a settle with a moved checkpoint throws and changes nothing.
  - IN45: a revert with a pending change writes rdb's row with the change on top.
  - IN46: a revert of a row rdb no longer has removes it and marks its mapping `deleted`.
  - IN47: writing each kind of record, and an end's observe beside a checkpoint, signals no change.
  - IN88: a settle clears the end's observes of every group, also of a group whose checkpoint it doesn't move, and a settle that throws leaves them all.
  - IN48: the settle stores the failure events of the handled rows, and none of the rows it keeps.
  - ADPTM04: a failed memory settle leaves nothing behind.

### P5: the ingestion core

- **The orchestrator** of detail 7, with the end of a pass. The removals of details 2 and 7 go with it, the planner's drops among them. This carries HIP-20's step 6, the native insert over an existing local id.
- **Reads** of rdb at C ∪ I. The revert gate moves into the settle.
- **The halt** of detail 9.
- **Tests** (each checks consistency after every step):
  - IN49: the deleted row, also after a reopen.
  - IN50: a refused change with a later pending change, end to end, with no projection between.
  - IN51: a first update sets `a` and `c`, a second names `a` and `b`, and the first is refused. The row keeps the second's `a`.
  - IN52: a multi-column update with one bad column is refused whole.
  - IN53: the void scenario, in both forms, gives a failure and a revert. After a live observe, the write is accepted.
  - IN54: a crash after a chain, and in the middle of the walk. The next cycle settles what was appended, with the checkpoint at the crashed pass's last entry, then ingests the rest. A change captured after the crash joins the new pass.
  - IN55: a crash between recording a hash and the append.
  - IN56: a crash after a refusal, with a later entry appended. The failure event keeps its recorded reason, and the row is reverted. Under another writer key the change stays refused and isn't validated again.
  - IN57: a crash between a permission's entry and the observe its use needs. The end writes the observe, and the use is accepted.
  - IN58: a crash after the end's observes and before its settle. The next cycle finds them beside the checkpoints, and no observe is written twice. Also a crash between recording an end's observe and appending it: it is written once, and the stale one is cleared.
  - IN59: a signed observe written for a change that rdb then refuses, with no later change of that group accepted. The end writes no second observe.
  - IN60: two harnesses make the same anonymous update. When the second crashes after taking the entry, the next cycle finds the change handled.
  - IN61: an update with nothing to write leaves the outbox at the settle, also after a crash.
  - IN62: concurrent updates of one column.
  - IN63: a comment on a deleted post is accepted, then voided.
  - IN64: an unprojected schema change.
  - SQL27: canonical json text after the settle.
  - ADPTI23: the comparison of the two readings.
  - IN71: an `INSERT OR REPLACE` over a row from rdb, and over one inserted on this device: a failure event, rdb's row back, the mapping still `active`.
  - IN72: an insert that reuses a deleted row's id, by an explicit id on SQLite and a `put` on IndexedDB (memory can't insert with an explicit id, `memory_target.ts:274-278`): a failure event, the row removed, the mapping still `deleted`.
  - IN73: on SQLite and IndexedDB, an insert, a delete and an insert of one local id in one pass: the second insert is refused, and the mapping ends `deleted`.
  - IN74: an update of a row whose insert was refused: a failure event, and nothing written.
  - IN75: a change of a table no member projects leaves the outbox with no event, and a projection then applies.
  - IN76: the gate-graph scenario through ingestion: the pass's observe of K voids G's observe of M, and a later write that reads M is refused with a revert.
  - IN77: two devices update different fields of one row concurrently, each naming only its field (`UPDATE`, `localUpdate`, `patch`), and both land.
  - IN78: an update of one field after a refused update of another lands, and the row shows rdb's value of the refused field with the new one.
  - In IN71 to IN75, the outbox empties at the settle and a projection then applies.
  - IN80: a change whose translation or entry throws an error that isn't a refusal gets a failure event with the error's message, leaves the outbox at the settle, and a projection then applies.
  - IN81: an append that throws once ends the pass and reaches `onError`. The next cycle builds the entry again and appends it once.
  - IN82: a `writeRecord` that throws once, for an entry's hash and for a failure: the pass ends and reaches `onError`, no later change gets a record, and the next cycle handles each change once.
  - IN83: a prepared entry whose appended hash differs from the recorded one halts the projection.
  - IN84: on SQLite and IndexedDB, a refused insert, then an insert on its local id, in one pass and in two, and with a crash before the settle: two failure events, no row, the mapping `ingestion_failure`.
  - IN85: a revert of a row with a pending insert on its local id: the target shows the insert's row until the insert is refused.
  - RDBPROJ22: the halt, at the apply and at the settle, and at the end when a delta has a schema action.
  - RDBPROJ24: a group without a writer key between changes of a group with one: the pass ingests the changes before its change and stops there, warns once, and no cycle loops. Opened again with the key, the rest is ingested.

### P6: observe effects in the settle

- **The end** computes one delta per group whose chain from C to its end holds an observe (decision 10).
- **Its verdict changes** become events, and the rows of their ops join the reverts.
- **Tests:**
  - IN65: a revocation concurrent with the grant an entry of the checkpoint read comes into view, voiding the row, or rewriting it under a pending change. A removal that comes after the grant voids nothing.
  - IN66: an observe mid-chain doesn't overwrite a later write; also with two observes, and when it voids the pass's own entry.
  - IN67: a crash after an observe that voids a row of the checkpoint, with a later entry appended. After the next cycle the row is gone and the void event is stored. The same when the observe voids the pass's own entry.
  - IN68: the join over an observe ahead of the checkpoint, and a member without a checkpoint.
  - IN69: an observe advances to C plus the chain.
  - IN70: a permission written natively and used right away in another group.
  - IN79: an observe that reinstates a row void at C: after the settle, the target holds the row, written by its row hash.
  - IN86: an insert captured during a pass whose settle reinstates a row that was never in the target, with the `BeforeSettle` hook: after the settle the target holds both rows, and the next pass accepts the insert.
  - IN87: an observe that voids a delete of the checkpoint: the row is back under its local id, and its mapping is `active`.
  - The whole shared suite, run once more with every end of a pass left to a new cycle (a harness option).
  - ADPTI24: the join helper.
- **STOP B (required).** The ingestion core is complete. Report the results and every flip.

### P7: the ingest fuzzer

- **The ingest fuzzer** drops its bundling and merging profiles. The chain sweep is in P1.
- **STOP C.** Report the seeds and profiles run.
- **A testing entry point** (decided 2026-11-24 at STOP B). `rdb_adapter`'s `package.json` gains `exports` for `"."` and `"./testing"`, and `leaveEndsToNewCycles`, `readingsDiffer` and `observeVersion` move from the main index to `@hyper-hyper-space/hhs3_rdb_adapter/testing`. Only the suites import them, so the main index keeps the API that apps use. The pass still uses the two helpers internally.

### P8: docs

- **READMEs:**
  - dag: a second append of an entry writes nothing.
  - rdb: the two steps and the `liveReads` option.
  - rdb_adapter: a new "Ingestion and checkpoints" section, covering the invariant, capture, the chain, the records, the end of a pass, reverts, the settle and crashes; drop `fkBundling` and `updateMerge`.
  - rdb_projection: the single projector, the wait for an empty outbox, the halt, the warning for a group without a writer key and `onWarning`, anonymous groups without a key, `autoAuthor`, and the dropped option.
  - rhost: `autoAuthor` in the host's configuration.
  - SQLite and IndexedDB: their capture, and IndexedDB's `patch`.
- **HIP-29** gets a citation pass.
- **Then** the final report, and HIP-20's Phases C to E.

## Progress

**STOP A, 2026-11-24.** P0 to P2 are done, on `e579e35`, uncommitted. Every suite is green, and `test:parity:fast` and `test:parity:full` pass.
- **Counts against the baseline:** dag 41→42, dag_sql 37→41, dag_idb 40→44, rdb 439→447, rdb_adapter_test 54→62, SQLite 85→91, IndexedDB 72→78, rdb_projection 31→39. The rest are unchanged.
- **Flips:** SQL22 and REACT02, adapted as listed. No other.
- **Fixed in place:**
  - dag_sql's test connection now queues its transactions, as dag_sqlite's does. The concurrent-append case needs it.
  - dag_test's backend suite gains `_08` and `_09`, so ENTRY_04's cases run on the SQL, SQLite and IndexedDB stores.
- **Beyond the plan's letter:**
  - `appendPrepared` checks the hash before the append as well as after it, so BUNDLE09's altered entry appends nothing.
  - `liveReads` reaches the three row-op reads. The fourth line cited, `validate_ops.ts:703`, is a deploy's `canDeploy`.
  - `syncDatabase` returns `{ results, refused, stoppedAt }`.
  - `withDatabaseLock` becomes `withTargetLock`, on a `WeakMap`.
  - `syncGroup` goes through `syncDatabase`.
  - SQLite's harness reopens from `db.serialize()`.
  - The crash hook after a given change, and the option that leaves every end to a new cycle, wait for P4 and P5: their hook points don't exist yet. `hooked()` covers any method.

**P3, 2026-11-24.** Done, uncommitted. Every suite is green, and `test:parity:fast` passes.
- **Counts against STOP A:** rdb_adapter 84→85, rdb_adapter_test 62→67, SQLite 91→98, IndexedDB 78→85, rdb_projection 39→41, rhost 27→28.
- **Flips:** IN29 and ADPTI-06 as listed, and ADPTI-07, by the same rule as ADPTI-06. The other P3 rows didn't flip: no SQLite case read per-column outbox rows or the old ids, and IN31 and IN34 record no writer, so nothing is filled.
- **Decided:** decision 20, no writer record.
- **Fixed in place:**
  - the key prefetch now looks up an update's author key too, which the set-aside needs;
  - SQL25 found its rows by `ref`, since rows project in row-hash order.
- **Beyond the plan's letter:**
  - the planner reads rdb's row for each update's subject at the tip, until P5 moves reads to C ∪ I;
  - an anonymous insert under `autoAuthor` off is never bundled with a signed op;
  - on SQLite, the author fill is an `AFTER INSERT` trigger, so an `INSERT … RETURNING` still returns the empty author (a later read has it).

**P4, 2026-11-24.** Done, uncommitted. Every suite is green, and `test:parity:fast` passes.
- **Counts against P3:** rdb_adapter 85→86, rdb_adapter_test 67→73, SQLite 98→104, IndexedDB 85→91.
- **Flips:** none.
- **Fixed in place:** IN47 lets SQLite's monitor announce the pending outbox when the listener arms, before it writes. IN88 projects its second group under a table prefix, since both fixtures have an `identities` table.
- **Beyond the plan's letter:**
  - the record is detail 3's shape, and `group` in an observe names the bound group it observes;
  - the reads are `getEndObserves(groupId)`, beside `getCheckpoint`;
  - the settle's reverts are `rowReverts` until P5 removes today's `reverts`, `statuses` and `mappings`;
  - an orphan's row is removed with its changes still in the outbox put back on top, as any revert, so a pending insert on its local id stays shown;
  - a revert that removes a row marks its mapping `deleted` only when it is `active`, so an `ingestion_failure` or `deleted` mapping keeps its status.

**P5, 2026-11-24.** Done, uncommitted, after a stop on IN36 and IN38 (resolved by decision 20 as revised). Every suite is green, and `test:parity:fast` passes.
- **Counts against P4:** rdb_adapter 86→83, rdb_adapter_test 73→99, SQLite 104→128, IndexedDB 91→116, rdb_projection 41→43.
- **Flips:** all as listed. Removed: IN03, IN03b, IN12, IN16, IN19, SQL11, SQL12, ADPTI02b, ADPTI07, ADPTI07b. Adapted: IN13, IN15, IN17, IN28, IN32, SQL17, ADPTI02, ADPTI14 to ADPTI18, ADPTI21, RDBING03, RDB_TOOLS52, REACT02.
- **Expected-text changes from decision 20:** the suites record the writer before they write (`projectAs`, in the new `ingestion_helpers.ts`), and the planner's tests plan inserts with the author capture fills in (`planCaptured`). SQL17's captured row now shows the filled author.
- **Fixed in place:** IN53's foreign-key form wrote the cross-group key under its rdb name instead of the target's, `grant_row_hash`; IN85 on IndexedDB, with no `INSERT OR REPLACE`, has its pending insert after a delete, which lands; the ingest fuzzer left out the refused replay's dependent ops.
- **Not test first:** the P5 cases were written after the code, since the stop came between the two. Each was checked to fail where the test was wrong; none was run against the P4 code, which no longer builds against the targets.
- **Beyond the plan's letter:**
  - the ingest fuzzer is cut to one profile now (P7's change);
  - `RTableGroup` exposes `resolveObservedForeignVersion`, `resolveLiveObservedForeignVersion` and `evaluateObserveGate`;
  - the mutator gains `insertAt` (SQLite, IndexedDB) and `replaceAt` (SQLite), for IN71 to IN73, IN84 and IN85;
  - RDBPROJ24 runs over a signed and an anonymous group with no key, since one projection has one writer: nothing outside the tests gives a group its own writer through `configOverride`, and the writer record holds one key.
  - the halt's third cause, a delta with a schema action, and its RDBPROJ22 part wait for P6's deltas.

**STOP B, 2026-11-24.** P6 is done, uncommitted, and the ingestion core is complete. Every suite is green, and `test:parity:fast` passes.
- **Counts against STOP A:** rdb 447, rdb_adapter 84→83, rdb_adapter_test 62→196, SQLite 91→223, IndexedDB 78→211, rdb_projection 39→43, rhost 27→28; the rest unchanged. The adapter suites' counts include the whole ingestion suite run a second time with every end of a pass in a new cycle.
- **Flips in P6:** none.
- **Beyond the plan's letter:**
  - the error for a delta with a schema action is `ChainSchemaChangeError`, next to `PendingChangesError`, and the projection halts on it. RDBPROJ22 fakes such a delta through the context the projection loads its groups from, since no real chain makes one;
  - the observe's join is `observeVersion`, exported for ADPTI24;
  - the harness option is `leaveEndsToNewCycles(on)`, a module switch of `ingest_chain.ts`, and `createIngestionSuiteWithEndsInNewCycles` runs the suite under it;
  - two new fixtures: the revocation scenario (IN65 to IN67, IN87) and the reinstatement scenario (IN79, IN86). A live observe written after an op doesn't reinstate an op that read through a void observe at its own position: only an entry of the bound group that voids the revocation does, so IN79 uses one.

**STOP C, 2026-11-24.** P7 is done, uncommitted. Every suite is green, and so are `test:parity:fast`, `test:parity:full` and `test:projection:full` on SQLite and IndexedDB. The full profiles ran seeds 1, 7, 42, 93, 1771, 9001 and 31415: the planner sweep at 60 ops and 160 pairs per seed, 28 to 33 of them from a chain's tip; the ingest fuzzer with 8 batches of 48 changes per seed; the projection runs at 100 ops per seed.
- **The testing entry point:** `rdb_adapter` has `exports` for `"."` and `"./testing"`, and `leaveEndsToNewCycles`, `readingsDiffer` and `observeVersion` moved to `@hyper-hyper-space/hhs3_rdb_adapter/testing`. Nothing deep-imported the package, and every downstream module builds.
- **Fixed in place:** the ingest fuzzer failed at seed 7 of the full profile. It left out the ops that name the refused replayed row, but not a later change of a row inserted with a foreign key into it, which rdb then didn't have. It now follows the rows it leaves out.
- **Flips:** none.

**P8, 2026-11-24.** Done, uncommitted.
- **READMEs:** rdb_adapter's new *Ingestion and checkpoints* holds the full explanation, and its intro, limits, change-capture, implementation, layout and test sections drop bundling, merging and the echo. rdb_projection gains *Cycles* (the single projector, the wait for an empty outbox, the halt, retries, the writer and `autoAuthor`, a group without a key). SQLite gains *Capture*, IndexedDB documents `patch`, what a `put` captures and the author fill, rhost documents `projection.autoAuthor`, rdb gains *Writing in two steps*, and dag says a second append of an entry writes nothing. The others link to rdb_adapter's section rather than repeat it.
- **HIP-29:** three citations drifted by a few lines and are fixed. The header names `e579e35` as the tree its references are to, since most of them describe code this HIP rewrote.
- **Left for a decision:** `rdb_adapter/src/ref_advance.ts` has no caller since the new pass, but the index still exports it; HIP-29's status and *Implemented in*.

## Expected test flips

| Test | Phase | Adaptation |
|---|---|---|
| ADPTS-SQL22 | P2 | ack the second handle's change before the next `apply` |
| REACT02 | P2 | settle the local insert before the hand-made apply |
| SQLite tests that read per-column update rows from `rdb_outbox` | P3 | one row per updated row (expected-text changes) |
| IN29 | P3 | a native update at the current value is now captured |
| SQLite tests that expect a projected row's or a reference's id to be the sync table's largest plus one | P3 | the id comes from the table's counter (expected-text changes) |
| ADPTI-06 | P3 | a `put` captures every column; the expected texts name them all, and a null `put` over an empty column captures it |
| ADPTI-07 | P3 | the same rule: its two `put`s capture every column but the author (expected-text change; added during P3) |
| IN31, IN34 | P3 | the captured insert may carry the filled author |
| IN03, IN12 and the `fkBundling` and `updateMerge` cases (`rdb_adapter/test/ingest_tests.ts`, the suite's bundling cases, the ingest fuzzer's profiles) | P5 | removed. IN52 replaces IN12 |
| IN28, IN32, ADPTI21 | P5 | the update is refused whole, with one failure event |
| IN13, IN15 | P5 | the next cycle settles what the crashed pass appended, then ingests the rest; check the DAG's entry count, and that the checkpoint equals the crashed pass's last entry |
| IN16, IN19, ADPTS-SQL11, ADPTS-SQL12 | P5 | removed: claim-or-adopt, the write-once skip, two ingesters on one file |
| IN17 | P5 | its hook moves to `BeforeSettle` |
| RDBING03 (`rdb_projection/test/ingestion_tests.ts:175`) | P5 | its assertion that a rejection names the ref-advance goes: a declined observe isn't reported. The item's own rejection keeps the rest |
| ADPTS-SQL17 ('set' case) | P5 | now accepted: translated with the schema at C |
| RDB_TOOLS52 | P5 | sync after the grant, before the second native insert |
| ADPTI14, ADPTI15, ADPTI17 | P5 | the dropped insert becomes a refusal with a kind, which the orchestrator records |
| ADPTI16 | P5 | an insert on an active mapping is refused, not planned again with its uuid |
| ADPTI18 | P5 | the update and the delete of a row whose insert was refused are refused (`rdb_adapter/test/ingest_tests.ts:258-353`) |
| test wrappers (suite, DelayCloseTarget, FlakyTarget) | P2–P5 | pass the new methods through |

Any other flip is a stop.

## Process

- **No commits or pushes.** No edits to `mb-chat`, `mb-user`, `mb-registry` or `~/.rdb/keys.json`.
- **Stop and report on a conceptual problem**, without deciding it alone. Any of these counts:
  - a HIP rule that can't be implemented as written, or two that can't both hold;
  - an existing test changing outcome, other than the flips above;
  - a case the HIP doesn't cover that needs a rule;
  - a public API, stored format or schema change the HIP doesn't name;
  - a behavior change in a module the HIP doesn't list.
- **Fixed in place and listed in the report:** bugs in new code, expected-text changes in tests, citation drift.
- **At each STOP,** report, and continue only on a go-ahead.

## Risks

- **More entries** where separate changes used to merge or bundle. Explicit bundles (HIP-20's open question 1) give apps a way to group them.
- **Cost:**
  - a target write per entry, per observe and per refused change, roughly doubling a pass's write transactions on IndexedDB (to be measured in HIP-20's trial app);
  - a comparison of the two readings of every gated binding after each observe;
  - about 4 µs more per SQLite update for the named-column triggers, and about 0.8 µs more per insert for the author fill;
  - a verdict evaluation per op while a gated binding's readings differ, and a comparison of the readings per pass and per observe;
  - a delta for each group whose chain holds an observe;
  - an outbox read in each settle.
- **`put` names every column on IndexedDB.** Code that edits a row with `put` keeps the losses of HIP-29's *Capture*: concurrent edits of different fields, a `put` after a refused one. `patch` is the way out.
- **A group with an identity provider and no writer key** holds remote changes back while a change of it is pending, with a warning. Only apps that open `RdbProjection` without a writer meet it.
- **Durability is assumed.** A power failure that loses the target's or rdb's last commits can show an insert twice, or leave a checkpoint naming entries rdb lost (HIP-29's *Crashes*). Accepted for now.
- **The writer record and `autoAuthor` land early.** They change rdb_projection's open and reconfigure, and rhost's projection config, before the rest of HIP-20's writer.
- **mb-chat** shows "sending…" while a message's author is `NULL` (`mb-chat/src/chat_server.tsx:96`). With the fill, that marker no longer shows. Not changed here.
- **Starvation under steady local writes** (HIP-29's first open question), to be measured in HIP-20's trial app.

## Verification

- **Per module.** From `modules/<m>`: `node --import ../../register.mjs ./test/run_tests.ts '<filter>'`. rdb_react, the rhost modules and rdb_files use `TS_NODE_PROJECT=./tsconfig.test.json`.
- **Type errors in tests** fail silently at load: typecheck with a temporary tsconfig that includes `test/**`.
- **Rebuild order:** dag, rdb, rdb_adapter, rdb_adapter_test, sqlite, idb, rdb_projection, rhost, then the rest.
- **At each STOP:**
  - every module's suite: dag, dag_sql, dag_idb, sync, rdb, rdb_adapter, rdb_adapter_test, sqlite, idb, rdb_projection, rdb_react, rhost, the two rhost clients, rdb_tools, rdb_repl and rdb_files;
  - `test:parity:fast`;
  - at STOP C also `test:parity:full` and `test:projection:full` in both adapters.
- **Baseline before P0.** To be taken again once the user has committed HIP-20's *Changes* 1, since cases were added after 2026-11-19 (`ea5d199`). The counts of 2026-11-19, for reference:
  - rdb_adapter 84, rdb_adapter_test 54, sqlite 85, idb 72, rdb_projection 31, rdb_react 3, rhost 26;
  - the clients 3 and 1, rdb_tools 79, rdb_repl, rdb_files 6 and 5;
  - parity full: all green.
  - the counts of rdb, dag, dag_sql, dag_idb and sync are to be taken at P0.
