# Eighth critique of HIP-29: Checkpoint consistency

A last review before the implementation starts, of [HIP-29](0029-checkpoint-consistency.md) as revised on 2026-11-23 after [the seventh response](0029-checkpoint-consistency.response-7.md), of its plan, `plans/checkpoint-consistency.md`, of [HIP-20](0020-unified-write-api.md), and of the code they cite. It looks only for what could disrupt the implementation. The points were first given in chat on 2026-11-23. The lock's key was decided there (§3). The other two findings come with a proposal.

Paths are relative to `modules/`. The checks are labeled L1 to L3 and Q1 to Q5, and §1 lists them.

## Verdict

**Is there a conceptual problem?** No. The four rules, the records and the end of a pass hold up against the code (§2).

**Is it ready?** After three small changes:
- the lock that keeps cycles apart is keyed by the members, so two cycles on one target can overlap, and an index install can land inside a cycle. HIP-29 removes what makes that harmless today (§3);
- the shared suite updates IndexedDB rows through `put`, which after P3 names every column (§4);
- the ingest result has no rule, and one existing test reads from it what the new end of a pass doesn't have (§5).

| Item | In HIP-29 | In the plan |
|---|---|---|
| §3. The lock's key | *One projector per target*, *Changes* 3, *Tests* | a detail, P2, two cases |
| §4. The IndexedDB mutator | | P0, P3 |
| §5. The ingest result | *The end of a pass*, *Changes* 3 | detail 7, *Expected test flips* |

## 1. How the claims were checked

**Read.** HIP-29, its seven critiques and responses, the plan, and HIP-20. In the code:
- dag and mvt: `dag/src/dag.ts`, `mvt/src/dag/dag_nesting.ts`;
- rdb: `rtable_group/group.ts` (`observe`, `bundle`, `prepareDeploy`, the verdicts, the two readings of a bound group), `op_delta.ts`, and the four bound-group reads of `validate_ops.ts`;
- rdb_adapter: `ingest_orchestrator.ts`, `project.ts`, `index_reconcile.ts`, and the `apply` of each target;
- rdb_projection: `projection.ts`;
- the tests: `rdb_adapter_test/src/ingestion_suite.ts`, the mutators of `rdb_adapter_idb/test/idb_target_tests.ts`, and `rdb_projection/test/ingestion_tests.ts`.

**Probes.** Scratch scripts, not in the repo, in `/private/tmp/claude-501/-Users-santi-Hyper-hhs3-ts/3ea498f7-3e63-4a28-9286-490fe04066a3/scratchpad/`. All were run on 2026-11-23.

*Which calls on one target exclude each other* (`lock_probe.mjs`), on a memory target, with the fixtures of rdb_adapter_test. It runs from `modules/rdb_adapter` with `node --import ../../register.mjs <file>`, against the working tree and, with `TREE=<scratchpad>/locktree`, against a copy of rdb_adapter's sources whose lock is keyed by the target (`patch_lock.mjs` makes the copy's change):

| | Scenario | Today | With the lock keyed by the target |
|---|---|---|---|
| L1 | One insert is pending. Two cycles run on the target with the same members, and the first is held open before its settle | The second waits, and finds the outbox empty | The same |
| L2 | The same, and the second cycle has one more member, as after a group joins | The second drains the insert before the first settles. rdb ends with one row | The second waits, and finds the outbox empty |
| L3 | A cycle with two members is between reading the index spec and its apply. An index reconcile then runs with one member | The spec is installed inside the cycle, and the cycle throws `CheckpointMovedError` | The reconcile waits. The cycle applies, and then the spec is installed |

*SQLite's counter* (`seq_probe.mjs`), in an in-memory database with SQLite 3.53.2, run with plain `node` from `modules/rdb_adapter_sqlite`:

| | Statements | Result |
|---|---|---|
| Q1 | A table that never had an insert. Its counter row is created at 0, and moved by one | `sqlite_sequence` had no row for it. The reference gets id 1 |
| Q2 | A native insert, then the referenced row inserted with id 1 | The native insert gets 2. The counter stays at 2, with one row for the table |
| Q3 | A native insert, then a reference | 3, then 4 |
| Q4 | A reference taken in a transaction that rolls back, then a native insert | Both get 5 |
| Q5 | An `INSERT OR REPLACE` over a row, with `recursive_triggers` off and on | Off, SQLite's default: the insert trigger alone. On: the delete trigger, then the insert trigger |

**Not run.** No test suite.

## 2. Claims that hold up

- **The two steps of `bundle` and `observe`.** A group appends through a root DAG that validates nothing at the append (`rdb/src/rtable_group/group.ts:1459-1466`, `mvt/src/dag/dag_nesting.ts:60-62`). Only a nested scope validates there (`dag_nesting.ts:181-186`), and that is a table's. Computing the hash and appending build the same header (`dag/src/dag.ts:98-118`). `prepareDeploy` already has the shape the split gives (`group.ts:871`).
- **A deploy is never void** (`group.ts:1266`). So the delta from the checkpoint to the end of a chain changes no schema, and the end's halt on a schema action can't fire.
- **The second validation.** The four places read a bound group through one resolver (`rdb/src/rtable_group/validate_ops.ts:390`, `:449`, `:514`, `:703`), and reading through the live observes is a flag of it (`group.ts:622-628`). Both readings are public (`:783`, `:794`).
- **SQLite's counter**, also for a table that never had an insert, which the check of 2026-11-23 didn't state (Q1 to Q4).
- **An `INSERT OR REPLACE` is captured as an insert alone** with `recursive_triggers` off (Q5), which the seventh critique left unchecked. No source sets the pragma. An app that turns it on in its own connection gets a delete and an insert on one local id, which *Reverts* covers.
- **P2 on today's orchestrator.** A pass can stop at the first change of a group without a writer key by cutting the drained batch short: the batch is drained once (`rdb_adapter/src/ingest_orchestrator.ts:340`), and the settle removes what it was given (`:584`).

## 3. The lock that keeps cycles apart is keyed by the members

**What the documents have.** *One projector per target*: "`RdbProjection` never overlaps its own cycles", and "A moved checkpoint means a second projector, which is not supported." On that ground the design drops the reserved identities, claim-or-adopt and the write-once skip, and halts the projection on a `CheckpointMovedError`.

**What the code does.**
- **The key.** A cycle and an index reconcile take one lock, keyed by the sorted ids of the member groups (`rdb_adapter/src/ingest_orchestrator.ts:632-635`, `:645`, `rdb_adapter/src/index_reconcile.ts:48`). `syncGroup` takes another, keyed by the group's id (`ingest_orchestrator.ts:251`). The target is in neither key.
- **The members change.** A membership change replaces the projection's members (`rdb_projection/src/projection.ts:499`). A call that starts after it takes another lock than a call still running from before.
- **Who is guarded.** Reactive cycles never overlap each other, by a flag of their own (`projection.ts:550`). `sync()` and `reconcileIndexes` have only the lock (`:183-196`, `:221-228`).
- **The second cause.** `apply` throws `CheckpointMovedError` also when the index spec changed since the cycle read it (`rdb_adapter_sqlite/src/sqlite_target.ts:365-366`, `:381-382`, `rdb_adapter_idb/src/idb_target.ts:211`, `rdb_adapter/src/memory_target.ts:241`).

**What follows.**
- **Two cycles at once** (L2). After a group joins, a `sync()` runs beside a reactive cycle that started before it, and drains the same changes. Today rdb ends with one row: the second pass adopts the identity the first one reserved, and takes rdb's refusal of its insert for its own entry (`ingest_orchestrator.ts:397-411`, `:475-482`). HIP-29 removes both. Each pass would then build its own entry for the change, with a new uuid, and rdb would hold two rows. Or the first settle moves the checkpoint, and the second finds it moved and halts.
- **An index install inside a cycle** (L3). After a group joins, an index reconcile runs beside a cycle, and the cycle's apply meets another spec than it read. Today `syncWithCasRetry` plans again (`projection.ts:202-212`). Under HIP-29 the projection halts for good.

**Who meets it.** Both need a membership change while the first call is still running.
- **`sync()`** is called by rdb_repl's projection update (`rdb_repl/src/projection/command.ts:238`), by tests, and by apps that use `RdbProjection` directly. rhost never calls it.
- **An index reconcile** is made by rhost right after it opens the projection (`rhost/src/projection.ts:38`), and by rdb_repl's index command (`command.ts:332`).

It is narrow, but what it gives changes from nothing to a second row or a projection that stops.

**Decision 30** (2026-11-23, continuing the sixth critique's numbers). The lock is keyed by the target: one lock per target object, taken by `syncGroup`, `syncDatabase` and `reconcileIndexes`.
- **Why the target.** What a cycle reads and then writes is the target's: the outbox, the checkpoints and the index spec. The members are an input.
- **What is the same target.** The object the caller passes. `RdbProjection` passes the one target it holds to both calls (`projection.ts:206`, `:225`), so `sync()`, reactive cycles and index reconciles exclude each other whatever the members are.
- **Two targets over the same groups** no longer share a lock. Today they do, since the key holds only group ids.
- **It closes both cases** (L2, L3). In chat I also proposed an error of its own for the stale index spec. It isn't needed: with this lock, a stale spec at an apply can only come from another process, which is the second projector the halt is for.

**Change.**
- HIP-29's *One projector per target*: what keeps the cycles apart is one lock per target, which index reconciles take too.
- *Changes* 3: the lock is keyed by the target (`ingest_orchestrator.ts:205-219`, `:251`, `:632-635`, `index_reconcile.ts:48`).
- *Tests*, rdb_projection: the two cases of §7.
- The plan: a detail beside detail 8, in P2, with RDBPROJ26 and RDBPROJ27.

## 4. The shared suite updates IndexedDB rows through `put`

**What the suite does.** Its IndexedDB mutator updates a row with `store.put({ ...existing, ...values })` (`rdb_adapter_idb/test/idb_target_tests.ts:63-72`). The SQLite one names only the columns it is given, and so does memory's.

**What P3 changes.** A `put` over an existing record captures every column. So every update the shared suite makes on IndexedDB names every column, while the same case on SQLite and memory names one or two.

**What follows.**
- **In P3.** IN06 updates only `ref`, a readonly column, and asserts that nothing was accepted (`rdb_adapter_test/src/ingestion_suite.ts:390-393`). Through `put` the change also names `amount` at its current value. Today's planner submits an update's columns one by one, so `amount` lands and one op is accepted. That flip is not in the plan's list, and any other flip is a stop.
- **In P5.** IN50 and IN51 can't hold on IndexedDB. In both, a second update of the row is written before the refusal of the first is settled. Through `put` it repeats the refused value, which the record still shows, and is refused too, as *A named column is a write* says of a `put`. In IN50 the target would hold the refused `memo` under the pending change, where the case expects `start`. IN78 is the same if its second update comes before the settle.

**Proposed.** From P3, the IndexedDB mutator's `update` calls `patch`, with the fields given as `null` in its list of fields to clear. Every shared case then names the same columns on the three targets. `put` keeps its own cases: IN39 and ADPTI-10.

**Change.** The plan's P3, where `patch` lands, and the harness of P0.

## 5. The ingest result has no rule

**What reads it.** `IngestResult` holds the number of accepted ops and the rejections (`rdb_adapter/src/types.ts:371-379`). `onResult` and `lastResult()` give it to apps, rdb_repl prints its rejections, and about forty assertions in the tests read it.

**What is missing.**
- **Where it comes from.** Today the pass counts as it goes. Under HIP-29 the end of a pass "takes nothing from the pass", and the documents don't say what the result is built from, or what a cycle reports when it finishes a crashed pass.
- **A declined observe.** Today an observe rdb refuses is a rejection that reads "ref-advance to bound group … failed" (`rdb_adapter/src/ingest_orchestrator.ts:552-557`). HIP-29 declines such an observe and says only that the pass doesn't throw on it. RDBING03 asserts that a rejection names the ref-advance (`rdb_projection/test/ingestion_tests.ts:175`). It is not in the plan's flips, and the plan doesn't mention that file.

**Proposed.**
- **The end builds the result from the records**: an accepted op for each handled row with an entry, and a rejection for each with a failure event, with the change from the outbox row and the reason from the event. A cycle that finishes a crashed pass then reports that pass's changes too.
- **A declined observe is not reported.** The write that needed it is refused with its own reason, which is what the user can act on. Reporting it would take one more kind of record in the outbox row, so that the end can read it.
- **RDBING03** drops that one assertion. The item's own rejection still holds its other assertions.

**Change.**
- HIP-29's *The end of a pass*: one sentence on the result. *Changes* 3, with the end of a pass.
- The plan's detail 7, and RDBING03 among the flips of P5.

## 6. The plan

| In the plan | What changes |
|---|---|
| A detail beside detail 8 | One lock per target, taken by `syncGroup`, `syncDatabase` and `reconcileIndexes` (§3) |
| Detail 7, the end of a pass | It builds the ingest result from the records. A declined observe is not in it (§5) |
| P0 | The IndexedDB mutator's `update` (§4) |
| P2 | The lock, with RDBPROJ26 and RDBPROJ27 (§3) |
| P3 | The IndexedDB mutator's `update` calls `patch` (§4) |
| *Expected test flips* | RDBING03, in P5: the assertion on the ref-advance goes (§5) |

## 7. Tests

- **rdb_projection:**
  - RDBPROJ26: a group joins while a reactive cycle is held open, and `sync()` is then called. The second cycle waits for the first, and each pending change is ingested once.
  - RDBPROJ27: a group joins while a cycle is held open before its apply, and an index reconcile is then called. The reconcile waits, the cycle applies, the spec is installed, and nothing throws.

  Both fail on today's code (L2, L3).
- **The shared suite on IndexedDB** updates through `patch`. No case changes.
- **RDBING03** without its assertion on the ref-advance.

## 8. Not checked

- No suite was run with the lock keyed by the target. L1 to L3 ran on a copy of rdb_adapter's sources. In those sources no holder of the lock calls another one.
- What HIP-29's passes do when they overlap (§3) is reasoned. Nothing of them is built.
- The flip of IN06 on IndexedDB (§4) is read from the planner and the mutator. It needs P3's capture to run.
- The other cases of `rdb_projection/test/ingestion_tests.ts`, RDBING01, 02, 04 and 05, pass as they are by my reading. They were not run against the design.
- The plan's list of flips was not audited beyond these two.
