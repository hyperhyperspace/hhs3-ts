# Seventh critique of HIP-29: Checkpoint consistency

A review of [HIP-29](0029-checkpoint-consistency.md) as revised on 2026-11-23 after [the sixth response](0029-checkpoint-consistency.response-6.md), of its plan, `plans/checkpoint-consistency.md`, and of the code they cite. It was made before the implementation starts, and looks only for conceptual problems, blockers, and missing rules that would let a defect in. The points were first given in chat on 2026-11-23. None has been discussed, so each finding comes with a proposal and not a decision.

Paths are relative to `modules/`. Nothing was run for this critique. The findings come from reading the code.

## Verdict

**Is there a conceptual problem?** None was found. The four rules and the records hold together, and the sixth response's changes are in both documents.

**Is it ready?** P0 to P2 can start, as the sixth critique said. Before P5 the documents need one fix and four rules, and one phase is better moved:
- SQLite can allocate a local id that a pending insert already holds, and the settle then writes another row over the user's (§3);
- an error from the target's own reads and writes during a pass has no rule, and the wording of decision 13 makes it the change's failure (§4);
- what a refused insert leaves on its local id for the rest of the pass isn't said, and neither is what the end reads to revert it (§5);
- nothing stops a pass whose appended entry has another hash than the recorded one, and the result repeats every cycle (§6);
- `observe` validates only when the replica is configured to, and the end's rules for an observe rely on its refusal (§7);
- the fuzzer's sweep over chains needs none of the new code, and tests the claim that P2 to P6 build on (§8).

| Item | In HIP-29 | In the plan |
|---|---|---|
| §3. Ids over a pending insert | *Changes* (the SQLite target), *Tests* | detail 12, SQL26, a shared case in P6 |
| §4. Errors from the target | *The algorithm*, *What became of each change*, *Crashes* | decision 13, detail 7 step 3, a case beside IN81 |
| §5. A refused insert's local id | *What became of each change*, *Reverts*, *Crashes* | decision 6, details 2, 6 and 7, a case in P5 |
| §6. The recorded hash | *Crashes* | details 1 and 9, a case in P5 |
| §7. Observes | *The end of a pass* | details 1 and 7, OBSGATE11 |
| §8. The chain sweep | | P1, P7 |
| §9. Smaller points | *Reverts*, *The settle moves the checkpoint*, *Tests* | details 6 and 13, P0, *Verification* |

## 1. How the claims were checked

**Read.** HIP-29, the plan, the sixth critique, and the fourth, fifth and sixth responses. The first five critiques and the first three responses were searched for each point below, not read again. In the code:
- dag: `dag/src/dag.ts`, `dag/src/store/mem_dag_store.ts`, `dag_sql/src/sql_dag_store.ts`;
- mvt: `authorship.ts`, `dag/dag_nesting.ts`, `refs.ts`;
- rdb: `rtable_group/group.ts` (`observe`, `bundle`, `diagnoseObserve`, the verdicts), `op_delta.ts`, `delta.ts`, `validate_ops.ts`, `scopes.ts`;
- rdb_adapter: `ingest_orchestrator.ts`, `ingest.ts` (the minting and the insert's refusals), `ref_advance.ts`, `project.ts`, `index_reconcile.ts`, and the id allocation of `memory_target.ts`;
- rdb_adapter_sqlite: `sqlite_target.ts` (`apply`, capture, `allocateId`, `upsertRow`, `paramForColumn`, `createTable`, the outbox);
- rdb_adapter_idb: the id allocation of `idb_target.ts` and `idb_facade.ts`, and `commitIngest`;
- rdb_projection: `projection.ts`;
- rdb_adapter_test_gen: `checkpoints.ts`, `history.ts`, and `rdb_adapter/test/planner_parity/project_parity.ts`.

**Not read.** HIP-20, beyond what HIP-29 and the plan quote from it. The rest of the IndexedDB facade and of the memory target.

**Not run.** No probes and no tests.

## 2. Claims that hold up

- **IndexedDB and memory allocate local ids from one counter.** IndexedDB's facade raises `meta.nextId` when it captures an insert (`rdb_adapter_idb/src/idb_facade.ts:542-564`), and the target allocates from the same counter (`rdb_adapter_idb/src/idb_target.ts:443-447`). Memory's `localInsert` and its `allocateId` share `nextId` (`rdb_adapter/src/memory_target.ts:279`, `:821`). So §3 is about SQLite only.
- **`bundle` validates whatever the replica's configuration says** (`rdb/src/rtable_group/group.ts:1077`). A row op's refusal doesn't depend on `selfValidate`. §7 covers `observe`.
- **A growth of rdb schedules a cycle** (`rdb_projection/src/projection.ts:507-508`). §6 relies on it.
- **The generator already branches writes off older checkpoints** (`rdb_adapter_test_gen/src/history.ts:429`, `checkpoints.ts:13-16`), and the sweep's `checkPair` compares a delta with a full projection (`rdb_adapter/test/planner_parity/project_parity.ts:65`). §8 relies on it.

## 3. SQLite allocates an id over a pending insert

**What the documents have.** The plan's detail 12: "An id allocated in a sync table without an app row raises `sqlite_sequence` (`allocateId`, `sqlite_target.ts:1023-1031`). Otherwise a native insert can take the id, and the settle's mapping then meets it." That is one direction: a native insert after an allocation.

**The other direction.** An allocation after a native insert is still open.
- **The cause.** `allocateId` gives `MAX(id) + 1` of the sync table (`rdb_adapter_sqlite/src/sqlite_target.ts:1027-1029`). It doesn't read `sqlite_sequence` or the app table. A native insert takes its id from the app table, and has no row in the sync table until its settle.
- **When.** A settle allocates an id in a table that holds an insert still in the outbox. A settle allocates in two places:
  - a row that the delta reinstates and that was never in the target (`upsertRow`, `:1081`);
  - a foreign key of a reverted row whose target row has no mapping yet (`paramForColumn`, `:1070`).

  Inserts are still in the outbox whenever the settle doesn't empty it: they were captured after the pass's drain, or the pass stopped or crashed before them.
- **Not a rare id.** Step 3 has just written the handled inserts' mappings. So `MAX(id) + 1` is the id right after the last handled insert, which is the id of the first pending one.
- **The effect.** `upsertRow` finds an app row at that id and updates it (`:1096-1105`). The user's row now holds the other row's values. At its turn the insert is on a local id that already has an identity, so it is refused, and its revert keeps the other row. The user's insert is lost, with a failure event that names a conflict the user didn't make.
- **Why it is new.** Today `reserveMint` covers this: a reservation whose local id another row hash holds adopts that row ("a projection that materialized it first", `rdb_adapter_idb/src/idb_target.ts:626-628`, and the same on SQLite). HIP-29 removes claim-or-adopt, and relies on no projection applying over a pending change. The settle is not a projection, and it allocates.

**Proposed.** `allocateId` takes the next id above both the sync table's largest id and the app table's `sqlite_sequence`. With detail 12, SQLite then has one counter per table, as the other two targets do (§2). The settle runs in a `BEGIN IMMEDIATE` transaction, so no native insert comes between the read and the write.

**Change.**
- HIP-29's *Changes*, the SQLite target (`0029-checkpoint-consistency.md:469`), and its test (`:541`): the allocation fix covers both directions.
- The plan's detail 12, and SQL26: an allocation after a native insert that has no mapping yet takes another id.
- A shared case in P6, beside IN79, with the `BeforeSettle` hook: an insert captured during a pass whose settle reinstates a row that was never in the target. After the settle the target holds both rows, and the next pass accepts the insert.

## 4. An error from the target's own reads and writes

**What the documents have.** Decision 13: "An error before the append is the change's failure". *The algorithm*: "rdb's refusals, the planner's, and any error before the append". The plan's detail 7, step 3, is narrower: "any other error while translating, building, signing or validating". Step 5: "An error in an append, of an entry or an observe, ends the pass".

**What is missing.** A pass now also reads and writes the target for each change: `writeRecord`, the mappings and keys the planner needs, and the comparison of the readings. An error there is in neither list. Examples are `SQLITE_BUSY` while the app holds a long write transaction on its own handle, and an IndexedDB quota. Read with decision 13's title, it is the change's failure. Two things then go wrong:
- **A valid write is reverted.** The change gets a failure event, and its settle writes rdb's row over it. The user loses a write that rdb would have taken a moment later.
- **A record that can't be written leaves a gap.** If `writeRecord` throws for a failure, or for a mark, and the pass goes on, later rows get records while this one has none. The handled rows end before it. The next pass builds every later change again, and an insert gets a new uuid. This is the double ingestion that the sixth critique's §8 ruled out for a change without a writer key.

**Proposed.**
- **Only the planner and `prepare…` make a failure.** The catch that turns an error into a failure wraps those two calls and nothing else. Decision 13 stays as it was decided for them.
- **An error from the target ends the pass**, as an error in an append does, and reaches `onError`. The records written so far stay, and the next cycle's end settles them.
- **A pass never goes past a change without a stored record.** This is the rule the other two follow from, and it is worth stating on its own.

**Change.**
- *The algorithm* and *What became of each change*: "any error before the append" becomes an error while the planner translates the change or rdb builds, signs or validates its entry.
- *Crashes*: an error from the target ends the pass as an error in an append does.
- The plan's decision 13 and detail 7, and a case beside IN81: a `writeRecord` that throws once, for an entry's hash and for a failure. The pass ends, no later change gets a record, and the next cycle handles each change once.

## 5. A refused insert and its local id

**What the documents have.**
- **Two kinds of refused insert.** One on a local id with no identity yet: its row is removed and its mapping marked `ingestion_failure` (the settle's steps 3 and 4, the plan's `orphans`). One on a local id that already has an identity, "a mapping of any status, or an identity minted earlier in the pass": its revert reads that identity's row (step 5).
- **What is stored.** The record of a refused change is its failure event (decision 6).
- **Identities.** "An insert's uuid is minted when its entry is built" (*Crashes*). The plan's detail 7: "An insert's uuid and row hash are kept for the pass that appends it".

**What is missing.**
- **An insert after a refused insert, in one pass.** The app inserts a row that rdb refuses, then writes the same local id again in the same drain, with SQLite's `INSERT OR REPLACE` or with a delete and an insert with the id. Whether the local id has "an identity minted earlier in the pass" depends on where the first insert was refused: after its entry was built, or by the planner before. If it has none, the second insert is accepted. The settle then has the local id among the orphans and among the inserts. Step 4 removes the row that step 3 just mapped `active`, and rdb holds a row the target doesn't. In the next pass the same insert is refused instead, since the mapping is `ingestion_failure`. So the result depends on where the pass ends.
- **What the end reads.** After a crash, the end has to tell the two kinds apart, and find the row a revert reads, from what is stored. The documents don't say from what. The stored mapping isn't enough: an identity minted earlier in the pass has no mapping until the settle. A wrong answer for IN73's second insert writes an `ingestion_failure` mapping on a local id whose mapping must end `deleted`.

**Proposed.**
- **A refused insert gives its local id an identity for the rest of the pass**, wherever it was refused, as its `ingestion_failure` mapping does for a later pass. An insert after it is refused, its revert finds no row in rdb, and the local row is removed. The result is then the same wherever the pass ends.
- **The end derives the kind from what is stored.** A refused insert had no identity when the target has no mapping for its local id and no earlier handled row is an insert on it. Otherwise its revert reads the mapping's row, or the row of the earlier insert: from its entry when rdb holds it, and none when it was refused.
- **The failure event of a refused insert carries the row hash** that its `ingestion_failure` mapping needs, when it had no identity.

**Change.**
- *What became of each change* and *Reverts*: the rule for the rest of the pass.
- *Crashes*: what the end reads for a refused insert.
- The plan's decision 6 and details 2, 6 and 7.
- A case in P5, on SQLite and IndexedDB: a refused insert, then an insert on its local id, in one pass and in two. Both give two failure events, no row, and an `ingestion_failure` mapping. Also with a crash before the settle.

## 6. The recorded hash and the appended entry

**What the documents have.** `prepare…` returns `{ payload, meta, at, hash }`, ingestion records the hash, and `appendPrepared` appends (detail 1). BUNDLE08 and OBSGATE11 check that the two steps give the hash that one call gives.

**What a mismatch does.** Say the append stores an entry with another hash than the recorded one. A cause could be an `at` that the nested scoped DAG fills in or replaces at the append (`mvt/src/dag/dag_nesting.ts:166-176`), or a payload shape the two tests don't build.
- **The end doesn't find the entry.** The record names a hash rdb doesn't hold, so the change is not handled, and neither is any change after it.
- **The next pass builds them all again.** An insert gets a new uuid, so rdb takes a second row.
- **It repeats.** Each append makes rdb grow, which schedules a cycle (§2). Every cycle adds a copy of every change from that one on, and the copies replicate.

Nothing in the documents stops it. The two tests cover the shapes of their fixtures.

**Proposed.** `appendPrepared` compares the hash the DAG returns with the prepared one, and throws when they differ. Ingestion halts the projection on that error, as on a moved checkpoint (detail 9). A halt leaves one stray entry instead of a growing number.

**Change.**
- *Crashes*: a recorded hash that differs from the appended entry's halts the projection.
- The plan's details 1 and 9, and a case in P5 with a prepared entry whose hash was changed by the test.

## 7. Observes

**What the documents have.** The plan's detail 1: `prepareObserve` builds, signs and validates, and "`bundle` and `observe` become the two steps in a row, unchanged for callers". *The end of a pass*: "An observe that the gate refuses to the writer, or that would be void where it stands, is declined, and the end doesn't ask for it again."

**What is missing.**
- **`observe` validates only under `selfValidate`** (`rdb/src/rtable_group/group.ts:1037`), while `bundle` always does (§2). The two sentences of detail 1 can't both hold for a replica with the setting off. If `prepareObserve` keeps the condition, ingestion appends an observe that the gate refuses to the writer, and the settle puts it in the checkpoint. `openWorkspace` sets it on by default (`rdb_runtime/src/workspace/workspace.ts:50`), so only a replica configured otherwise meets this.
- **An error that isn't a refusal.** Today `observeToVersion` turns every throw of `observe` into a failure and goes on (`rdb_adapter/src/ref_advance.ts:68-73`). Decision 13 gives the rule for a change, and step 5 for an observe's append. Nothing says what an error while an observe is built or validated does. If the end throws on it, the settle never runs, and every cycle throws again.
- **The end under other keys.** The end of a crashed pass can run in a projection opened with other keys (IN56). An observe can then be due for a group whose key this projection lacks. Today `flush` asks for the writer first and throws (`rdb_adapter/src/ingest_orchestrator.ts:537`).

**Proposed.**
- **`prepareObserve` always validates.** `observe` passes its `selfValidate` condition to it, so its callers see no change.
- **An error while an observe is built, signed or validated declines it**, as a refusal does, and the pass or the end goes on.
- **An observe that needs a key the projection lacks is declined.**

**Change.**
- *The end of a pass*, step 3: the two other reasons to decline.
- The plan's details 1 and 7, and OBSGATE11: with `selfValidate` off, `prepareObserve` refuses what the gate refuses.

## 8. The chain sweep can come first

**What the documents have.** P7, after the ingestion core: the generator gets a `chain` op "that appends two or three entries at an older checkpoint and records the tip", and the sweep covers "the pairs that start at such a tip, through `checkPair`".

**Why earlier.**
- **What it tests.** After a settle, a checkpoint is the tip of a chain: a version that was never rdb's frontier, with remote entries concurrent to it. Every later projection starts its delta there. Today every checkpoint the generator records is a frontier (`rdb_adapter_test_gen/src/history.ts:581`). So no test has run a delta from such a start, and the design rests on rdb's delta being right for it.
- **What it needs.** The generator and today's projection (§2). None of P1 to P6.
- **What a failure costs.** In P7 it comes after the settle, the orchestrator and their tests are written. In P1 it comes before any of them.

**Proposed.** The `chain` op and its sweep move to P1. P7 keeps the ingest fuzzer's profiles and STOP C.

**What it doesn't cover.** The generator has no bound groups. The claim that the verdict changes of a delta are enough for an observe (*The settle moves the checkpoint*) stays with the hand-built cases of IN65 to IN67 and IN79.

## 9. Smaller points

- **A revert over a pending insert.** *Reverts* and detail 6 give two kinds of pending change: "an update writes its columns, and a delete removes the row". A third is possible: an insert on the reverted row's local id, captured after the drain, from an `INSERT OR REPLACE` or from a delete and an insert with the id. It writes its whole image. The insert is refused at its turn, so the end state is the same either way, but until then the target must show it. The consistency check of detail 13 needs the same three rules, and one more: an update or a delete of a row that isn't there does nothing.
- **A delete of the checkpoint that an observe voids.** The delta's rows are described for a voided write and for a reinstated insert. A voided delete is a third: the row comes back, its mapping is `deleted`, and it has a local id. Written by its row hash through the projection path, it gets its id back and its mapping set `active` (`rdb_adapter_sqlite/src/sqlite_target.ts:1081-1085`). It needs a case beside IN79.
- **The local id of the pass's own voided insert.** The delta names the row by its hash, and detail 6 keys `reverts` by local id. The stored mappings don't hold it before step 3. It comes from the pass's `inserts`, or the target resolves the row hash inside the settle, after step 3.
- **`endPass` takes nothing from `runPass`.** If the function has no parameters from the pass, the rule that the end reads only what is stored can't be broken by accident. A stronger check: the shared suite run once with every end left to a new cycle.
- **Before P0.**
  - The working tree is not committed yet: 17 files, as the plan's header expects.
  - The baseline of *Verification* is from 2026-11-19, and `ea5d199`, from 2026-11-20, added cases: rhost's tests now name 27 cases and rdb_tools' 80, against a baseline of 26 and 79. It is to be taken again, or "any other flip is a stop" has no fixed base.

## 10. The plan

| In the plan | What changes |
|---|---|
| Decision 6 | The failure event of a refused insert without an identity carries its row hash (§5) |
| Decision 13 | It covers the planner and `prepare…`. An error from the target ends the pass (§4) |
| Detail 1 | `prepareObserve` always validates. `appendPrepared` throws when the DAG returns another hash (§6, §7) |
| Detail 2 | A refused insert gives its local id an identity for the rest of the pass (§5) |
| Detail 6 | A pending insert in a revert. The local id of an own voided insert (§9) |
| Detail 7, step 1 | An error while an observe is built, signed or validated declines it (§7) |
| Detail 7, step 3 | The catch wraps the planner and `prepare…` only. No change is passed without a stored record (§4) |
| Detail 7, the end | What it reads for a refused insert (§5). An observe that needs a missing key is declined (§7). No parameters from the pass (§9) |
| Detail 9 | A hash mismatch halts (§6) |
| Detail 12, SQL26 | Both directions (§3) |
| Detail 13 | The check applies a pending insert as its image, and skips an update or delete of a missing row (§9) |
| P1 | The `chain` op and its sweep (§8). OBSGATE11 with `selfValidate` off (§7) |
| P5 | The cases of §4, §5 and §6 |
| P6 | The cases of §3 and §9 |
| P7 | The ingest fuzzer's profiles only (§8) |
| *Verification* | The baseline taken again after the working tree is committed (§9) |

## 11. Tests

- **SQLite** (SQL26): an allocation after a native insert that has no mapping yet takes another id.
- **Shared ingestion suite:**
  - An insert captured during a pass whose settle reinstates a row that was never in the target. Both rows are in the target after the settle, and the insert is accepted.
  - A `writeRecord` that throws once, for an entry's hash and for a failure: the pass ends and reaches `onError`, no later change gets a record, and the next cycle handles each change once.
  - A refused insert, then an insert on its local id, in one pass and in two, and with a crash before the settle: two failure events, no row, the mapping `ingestion_failure`.
  - A prepared entry whose appended hash differs from the recorded one halts the projection.
  - A revert of a row with a pending insert on its local id: the target shows the insert's image until the insert is refused.
  - An observe that voids a delete of the checkpoint: the row is back under its local id, and its mapping is `active`.
- **rdb** (OBSGATE11): with `selfValidate` off, `prepareObserve` refuses an observe the gate refuses to the writer.
- **The planner-parity sweep**, in P1: pairs that start at a chain's tip.

## 12. Not checked

- The collision of §3 is read from the code. It was not reproduced.
- Whether SQLite's capture records an `INSERT OR REPLACE` as an insert alone. HIP-29 says so (*What became of each change*), and §5 and §9 take it from there.
- What the new planner does with an insert after a refused one (§5). It isn't written yet, and the two readings come from the documents.
- Whether anything can make the two steps give different hashes today (§6). The guard is for a cause nobody has found.
- The case counts of §9 are counts of test names. No suite was run.
- HIP-20's text, for what it says about the points of §3 and §5.
