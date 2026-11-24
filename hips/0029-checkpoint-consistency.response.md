# Response to the critique of HIP-29

A reply to [the critique](0029-checkpoint-consistency.critique.md) of [HIP-29](0029-checkpoint-consistency.md) and of the 2026-11-19 revisions to [HIP-20](0020-unified-write-api.md). Both HIPs were revised in place on 2026-11-19. Paths are relative to `modules/`.

The revision takes every point. One half of §7.2 doesn't arise under HIP-29, and §8's caveat changed an earlier decision. Each claim was checked against the code before it was taken.

| Critique | In the revised HIPs |
|---|---|
| §1. An `observe` in the chain can void or reinstate rows in `C` | Taken, with the first option. In HIP-29 (*The settle moves the checkpoint*), a row op after C still changes no row it doesn't write, but an observe can change rows. For each observe the pass appended, the settle applies the observe's delta: its op events, and its row actions, held back like a revert where a change to the row is pending. The second option is under *Considered and not taken*. |
| §2. Monotonicity, unprojected members, spurious `propagate` | Taken, all three (HIP-29, *Ingestion at the checkpoint*, "Other groups"), each with a test. An observe is joined with what the group already observes, follows only an entry rdb took, and leaves out groups without a checkpoint. |
| §3. Held-back reverts | Taken, all three (*Held-back reverts*). A pass rewrites flagged rows even when the checkpoint is at the frontier, and removes a flagged row rdb no longer has. A refused insert's row is not held back, as today. |
| §4. Canonical `{tip}` | Taken (*Ingestion at the checkpoint*). C ∪ I is the HIP's notation, and the settle stores `{I}`. |
| §5. Settle author writes | Taken, all three (*Settlement parity*). The settle writes column by column. It skips a column a pending change writes, and a row that is gone. Its commit notice names the tables it wrote, and IndexedDB keeps the column's index entries, `@author` included. |
| §6. Lost wake-up, unbounded loop | Taken (*No projection over a pending change*). `RdbProjection` schedules the next cycle itself after a refusal, and `arm()` checks for a change already pending. A cycle ingests once more at most. |
| §7.1. Clearing the batch record | Taken (*Crash replay*). The settle clears it in its transaction. |
| §7.2. Batch-minted mappings | Taken for the exposure through `rowHash`. The replay ambiguity doesn't arise under HIP-29 (below). |
| §8. `json_valid('null')` | Taken (HIP-20, *Changes* 1). The json check also refuses json's `null`. The caveat changed the decision on json text in another form: HIP-29's settle now rewrites it in canonical form (below). |
| §9. `tableNames` on read-only projections | Taken (HIP-20, *What the projection records in the target*, *Changes* 2, *Tests*). Member names and the hash suite are recorded without a writer key, and the `create-table` field is optional. |
| §10. Memory's id column | Taken, as a HIP-20 *Changes* 1 fix: memory refuses a value for the id column. It predates the working tree's changes (below). |

## §1: the observe's delta

Only observes can do this, and the code confirms why. An entry's reads of another group resolve that group's version from the evaluation horizon (`foreignFrom`, `rdb/src/rtable_group/group.ts:663-664`). A row op after C comes after every entry of C, so it can't be concurrent with any of them. An observe moves the horizon's version of the other group, and with it the entries of that group that count as concurrent.

The first option covers every case: it applies, for each observe, the delta from the tip before it to the observe. The second, leaving a group that only observed at its old checkpoint, works only when no row op follows the observe in the same pass, so it is not taken.

The delta's row actions follow the rule of *Held-back reverts*. A row with a pending change is flagged, and the next projection rewrites it from rdb. The settle never writes over a pending change.

## §5: column by column

The author and the canonical json text are written per column, not as row rewrites, so a pending change elsewhere in the row doesn't block them. A pending change of the same column does, and needs no flag:
- **A json column.** The pending change's own settle canonicalizes what it wrote.
- **The author column.** A pending change of an existing row's author is refused at ingestion, and its revert writes the author from rdb.

## §7.2: no ambiguity, but an exposure

A replay plans the recorded batch with the same outcome as the crashed pass, even with an `INSERT OR REPLACE` in it:
- **The new insert.** The replay reads back its reserved identity, and finds its entry among rdb's by parents and payload.
- **The `INSERT OR REPLACE` over a settled row.** Both passes read the row's existing identity. rdb refuses the insert because its row exists, and it is a failure both times.

The exposure is real, though. `reserveMint` writes active mappings before the append (`rdb_adapter/src/types.ts:322-339`), so `rowHash` and `localId` would answer for an insert that rdb may still refuse. The revision keeps the reserved identities on the batch record until the settle, which writes them into the sync table. Since a pass is the only ingester, `reserveMint`'s claim-or-adopt is then no longer needed.

## §8: canonical json text

The check was confirmed on SQLite 3.53.2. `json_valid('null')` is 1, and the proposed check refuses the text `null` while it accepts SQL `NULL`.

The caveat reversed a decision of the same day: the earlier text kept json text in another form as the app wrote it, so that the row would read the same before and after ingestion. But a query binds json in canonical form (`sqlParam`, `rdb_adapter_sqlite/src/sqlite_target.ts:1503-1508`). Such a row would then miss a query that a row from rdb with the same value matches. The settle now rewrites the column in canonical form, as a second named exception of *Settlement parity*. Keeping the text is under *Considered and not taken*.

## §10: older than the working tree

The committed `localInsert` also copies every value into the row, the id included. The committed `asView` also lays a row's values over its id. The fix belongs with HIP-20's other memory fixes in *Changes* 1. Memory refuses the value rather than ignoring it, since memory allocates ids itself, and an update can't change one, as on SQLite.

Every point was taken in discussion on 2026-11-19. For §8, the decision was to rewrite json text in canonical form at the settle.
