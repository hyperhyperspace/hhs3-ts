# Response to the eighth critique of HIP-29

A reply to [the eighth critique](0029-checkpoint-consistency.critique-8.md) of [HIP-29](0029-checkpoint-consistency.md) and of its plan, `plans/checkpoint-consistency.md`. Both were revised on 2026-11-23. HIP-20 needed no change. Paths are relative to `modules/`.

Decision 30 is taken as it was made, and the proposals of §4 and §5 were accepted on 2026-11-23 as written. The code the critique cites was read again, and every claim checked holds.

| Critique | In the revised documents |
|---|---|
| §3. The lock's key | Taken (*One projector per target*, *Changes* 3, *Tests*). The plan's decision 17, detail 8, P2, RDBPROJ26 and RDBPROJ27 |
| §4. The IndexedDB mutator | Taken (*Tests*, the shared suite). The plan's P0 and P3 |
| §5. The ingest result | Taken (*The end of a pass*, *Changes* 3). The plan's decision 18, detail 7, and RDBING03 among the flips of P5 |
| §6, §7 | Taken as listed |

## What the revision says

- **The lock.** One lock per target object keeps cycles and index reconciles apart, taken by `syncGroup`, `syncDatabase` and `reconcileIndexes`. *One projector per target* gives the two cases the lock keyed by the members lets through, why today's code survives them, and why HIP-29 wouldn't. With the lock per target, a moved checkpoint or a changed index spec at an apply can only come from another process, so the halt keeps its meaning. The plan proposes a `WeakMap` from target to the last promise.
- **The mutator.** The IndexedDB mutator of the shared suite gets a switch in P0, and P3 turns it on when `patch` lands. Its `update` then goes through `patch`, with the fields given as `null` in its list to clear. IN06, IN50, IN51 and IN78 then hold on IndexedDB as on the other two targets. `put` keeps IN39 and ADPTI-10.
- **The ingest result.** The end of a pass builds it from the records: an accepted change for each handled row with an entry, and a rejection for each with a failure event. A cycle that finishes a crashed pass reports that pass's changes. A declined observe isn't reported. RDBING03 loses its assertion that a rejection names the ref-advance (`rdb_projection/test/ingestion_tests.ts:175`), and keeps the rest.

## Checked

- **The lock** is keyed by the sorted ids of the member groups for a cycle and an index reconcile, and by the group's id in `syncGroup` (`rdb_adapter/src/ingest_orchestrator.ts:632-635`, `:251`, `rdb_adapter/src/index_reconcile.ts:48`). `apply` also throws `CheckpointMovedError` when the index spec changed since the cycle read it (`rdb_adapter_sqlite/src/sqlite_target.ts:365-366`).
- **The IndexedDB mutator** updates with `store.put({ ...existing, ...values })` (`rdb_adapter_idb/test/idb_target_tests.ts:63-72`). IN06 updates only `ref` and expects nothing accepted (`rdb_adapter_test/src/ingestion_suite.ts:384-394`).
- **An observe rdb refuses** is reported today as a rejection that names the ref-advance (`rdb_adapter/src/ingest_orchestrator.ts:552-557`), and RDBING03 asserts it.

## Decided after the critique

On 2026-11-23 the plan's last three open items were decided:
- **`patch`'s shape** is `store.patch(fields, { delete?: string[] })`, with the key in `fields`.
- **A `patch` of a key with no record** writes and captures nothing, as an `UPDATE` that matches no row does.
- **The end's own observes** are recorded beside their group's checkpoint, in a list that every settle clears for every group, in its transaction. A review of the change found that clearing only the groups whose checkpoint moves would leave a stale entry when the end declines an observe recorded before a crash. Until now they went on the last outbox row with a record. They belong to no change, and a row kept past the settle by a crash would carry them into the next pass. The chain they end belongs to the checkpoint. The change costs a field in each target's checkpoint store: a column of `rdb_checkpoint` on SQLite (`rdb_adapter_sqlite/src/sqlite_target.ts:455`), a field of the checkpoint record on IndexedDB (`rdb_adapter_idb/src/idb_schema.ts:149`), and of the checkpoint entry on memory (`rdb_adapter/src/memory_target.ts:91`). The list exists only while the pass's rows are in the outbox, since an end writes observes only after a recorded entry moved some chain, and only the settle removes rows. HIP-29's *The algorithm*, *The end of a pass*, the settle's steps, *Crashes*, *Changes*, *Tests* and *Considered and not taken* say so. The plan has it as decision 19, in details 3, 4, 6 and 7, with IN47, IN58 and a new IN88.
