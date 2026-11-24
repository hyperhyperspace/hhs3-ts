# Response to the seventh critique of HIP-29

A reply to [the seventh critique](0029-checkpoint-consistency.critique-7.md) of [HIP-29](0029-checkpoint-consistency.md) and of its plan, `plans/checkpoint-consistency.md`. Both were revised on 2026-11-23. HIP-20 needed no change. Paths are relative to `modules/`.

Every point was discussed on 2026-11-23. All of them are taken, §3 with another fix, and §4 with a retry it didn't have. No conceptual problem was found.

| Critique | In the revised documents |
|---|---|
| §3. Ids over a pending insert | Taken, with the table's own counter (below). A new section, *Local ids*, and *Changes* 4, *Tests* and *Considered and not taken*. The plan's decision 14, detail 12, SQL26, IN86 and a flip |
| §4. Errors from the target | Taken, with a retry (below). *The algorithm*, *What became of each change*, *Crashes*, *Why it is sound*, *Changes* 3 and 6, *Tests*, *Considered and not taken*. The plan's decision 13, details 7 and 8, IN82 and RDBPROJ25 |
| §5. A refused insert's local id | Taken (*What became of each change*, *Reverts*, *Crashes*, *Changes* 3, *Tests*). The plan's decision 6, details 2, 6 and 7, and IN84 |
| §6. The recorded hash | Taken (*The algorithm*, *Crashes*, *Changes* 2, 3 and 6, *Tests*). The plan's decision 15, details 1 and 9, BUNDLE09 and IN83 |
| §7. Observes | Taken (*The end of a pass*, *The algorithm*, *Changes* 2 and 3, *Tests*). The plan's decision 16, details 1 and 7, and OBSGATE11 |
| §8. The chain sweep | Taken: it moves to P1 (*Changes* 9, *Tests*). P7 keeps the ingest fuzzer's profiles and STOP C |
| §9. Smaller points | All taken: a pending insert in a revert and in the consistency check, a voided delete (IN87), the local id of an own voided insert, an end with no parameters from the pass and a suite run with every end left to a new cycle, and the baseline taken again after the commit |

## §3: the table's own counter

Proposed in the critique: `allocateId` takes the next id above both the sync table's largest id and the app table's `sqlite_sequence`.

Decided instead: every local id comes from its table's own counter, and the sync table never makes up an id. SQLite has no sequence object, but `INTEGER PRIMARY KEY AUTOINCREMENT`, which every app table has (`rdb_adapter_sqlite/src/sqlite_target.ts:772`), keeps a counter in `sqlite_sequence` that never hands out a number twice.
- **A row the projection writes** is inserted first, and its sync row takes the id the insert produced, as a native insert's does.
- **A reference to a row the target doesn't hold**, such as a comment whose post was deleted, takes the next value of the referenced table's counter with `UPDATE sqlite_sequence SET seq = seq + 1 … RETURNING seq`. Its counter row is created at 0 first when the table has never had an insert. The row is inserted with that id when it arrives.

A placeholder row that is inserted and deleted again was looked at in discussion, for a row that was never live in this replica. Moving the counter needs none. A placeholder could also fail on a `NOT NULL` column without a default, and between its two commits it would show readers a row that doesn't exist.

This was checked on 2026-11-23 in an in-memory database, SQLite 3.53.2:
- after the top row is deleted, the next insert still gets a new id. A table without `AUTOINCREMENT` hands the deleted id out again;
- the counter, moved by hand, hands out the next id;
- an insert with an id below the counter works, and leaves the counter where it is;
- a rolled-back insert leaves the counter where it was.

IndexedDB and memory already allocate from one counter per table (the critique's §2). With this, SQLite does too.

## §4: the retry

Under the critique's proposal, an error from an append or from the target ends the pass. The next cycle then settles what came before, and builds the failed change and the rest once. But nothing scheduled that cycle. `RdbProjection` runs the next one only when rdb grows or the outbox signals (`rdb_projection/src/projection.ts:550-566`), so an edit hit by a passing `SQLITE_BUSY` could wait for the next write.

Decided on 2026-11-23: a cycle that ends in an error schedules the next one itself, after 1 s, doubling up to 60 s, and reset after a cycle that succeeds. Three cases don't retry:
- a halt, from a moved checkpoint, a delta with a schema action, or a hash mismatch;
- a refused apply, which already schedules the next cycle;
- a pass stopped at a group without a writer key, which decision 29 makes schedule nothing.

Each failed cycle still reaches `onError`.

## Details decided with the points

- **§5.** An insert the planner refuses before its entry is built still gets a uuid and a row hash. Its failure event carries them, in the fields it already has for the affected row (`rdb_adapter/src/types.ts:209-214`). Its `ingestion_failure` mapping needs them, since the sync table is keyed by row hash.
- **§9.** The local id of an own insert that an observe voided is found by the target inside the settle, once step 3 has written the inserts' mappings. Carrying it from the pass's list of inserts was the other way. The lookup keeps the settle on stored data only.
- **§6.** Discussed: a signing suite with randomness doesn't affect the guard. The entry is signed once, before its hash is computed, and `appendPrepared` appends that same payload. Building an entry twice gives another signature, and the design already handles both places where that happens: a crash between the record and the append, and finding a held entry by rebuilding it.

## Corrected in writing

- **rhost's `onError`.** HIP-29 said in two places that it fails the host. It sets the host's last error, which the host's status reports (`rhost/src/host.ts:752`, `:794-797`), and the host keeps running.

## Checked

- **`allocateId`** takes the sync table's largest id plus one, and `upsertRow` updates the app row it finds at the id (`rdb_adapter_sqlite/src/sqlite_target.ts:1023-1031`, `:1096-1105`).
- **`observe` validates only under `selfValidate`** (`rdb/src/rtable_group/group.ts:1037`), and `bundle` always (`:1077`). `observeToVersion` turns every throw into a failure (`rdb_adapter/src/ref_advance.ts:68-73`).
- **Every checkpoint the generator records is a frontier** (`rdb_adapter_test_gen/src/history.ts:581`).
- **Computing a hash and appending** wrap the payload and fill in its parents the same way (`mvt/src/dag/dag_nesting.ts:166-178`, `:198-200`).
