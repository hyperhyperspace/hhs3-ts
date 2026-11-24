# Sixth critique of HIP-29: Checkpoint consistency

A review of [HIP-29](0029-checkpoint-consistency.md) as revised on 2026-11-22 after [the fifth response](0029-checkpoint-consistency.response-5.md), of its plan, `plans/checkpoint-consistency.md`, and of the code they cite. The points were first given in chat on 2026-11-22 and discussed the same day. The decisions taken there are in §3, and every finding below is written with its decision.

Paths are relative to `modules/`. Nothing was run for this critique. The findings come from reading the code.

## Verdict

**Does the revision resolve the fifth critique?** Yes. Each of its points is in the HIP, the plan and HIP-20 as it was decided, and the three documents agree. The fifth response's own decisions hold up: `patch` names its fields without comparing, `put` keeps IndexedDB's meaning, and a second append of an entry is an exact duplicate (§2). One of them is revised here: a projection that starts without a writer key only warns, instead of refusing to open (§8).

**Is it ready?** Yes. P0 to P2 can start. Five points remain, and none changes the design:
- the memory DAG store doesn't serialize its transactions, so `Dag.append`'s new check isn't atomic there (§4);
- the HIP's claim that projection keeps making progress needs a rule for an error that isn't a refusal (§5);
- P4's measurement can't run as planned (§6);
- memory can't insert with an explicit id, which two cases of the plan assume (§7);
- a start without a writer key that only warns needs three rules the warning alone doesn't give (§8).

| Item | In HIP-29 | In the plan |
|---|---|---|
| §4. The memory DAG store | *Changes* 1, *Tests* | detail 1, P1 |
| §5. An error that isn't a refusal | *Ingestion at the checkpoint*, *Crashes*, *Why it is sound*, *Tests* | detail 7, P5 |
| §6. The IndexedDB measurement | *Open questions* 2 | P4, *Deferred* |
| §7. Explicit ids on memory | *Tests* | IN72, IN73 |
| §8. Starting without a writer key | *The algorithm*, *No projection over a pending change*, *What changes for apps*, *Why it is sound*, *Changes* 3, 6 and 10, *Tests* | decision 11, detail 8, RDBPROJ19, *To confirm* |
| §9. Smaller points | *The settle moves the checkpoint* | detail 6 |

## 1. How the claims were checked

**Read.** HIP-29, the plan, the fifth response, and HIP-20's header and *Changes* 1. In the code:
- dag: `dag/src/dag.ts`, `dag/src/store/mem_dag_store.ts`, `dag_sqlite/src/sqlite_connection.ts`, `dag_sql/src/sql_dag_store.ts` and `dag_idb/src/idb_env.ts`;
- the memory backends: `replica/src/mem_dag_backend.ts` and `mvt/src/dag/mem_dag_storage.ts`, and their users, `rdb_runtime/src/workspace/workspace.ts` and `rdb_repl_web/src/direct_client.ts`;
- rdb_adapter: `ingest_orchestrator.ts` (`writerFor`, `authorFor`, `attempt`, `syncDatabase`) and `memory_target.ts` (`localInsert`);
- rdb_projection: `scope.ts`, and the options, `reconfigure` and the scheduling of cycles in `projection.ts`;
- the `package.json` of every module, for what runs IndexedDB in tests.

**Not run.** No probes and no tests.

## 2. Claims that hold up

- **A second append is an exact duplicate.** An entry's hash is the hash of its header, which holds the payload's hash and the parents (`dag/src/dag.ts:65-79`). The meta isn't hashed, but rdb derives it from the payload and the schema at the parents.
- **SQLite and IndexedDB make `Dag.append`'s check atomic.** SQLite queues the transactions of a connection (`dag_sqlite/src/sqlite_connection.ts:41-45`) and begins each with `BEGIN IMMEDIATE` (`:19`), which also orders them against other processes. IndexedDB queues its units of work (`dag_idb/src/idb_env.ts:108-117`). §4 covers memory.
- **Writers are set per group** (`rdb_projection/src/scope.ts:126`), and a reconfigure keeps the options given at open and only adds groups (`rdb_projection/src/projection.ts:496-503`). So one projection can hold groups with a key and groups without one, which §8 relies on.
- **rhost and rdb_repl always open with a writer key** (`rhost/src/host.ts:913`, `rdb_repl/src/projection/command.ts:187`).
- **The planner's drops**, now refusals, are where the HIP says they are.

## 3. Decisions taken in discussion

Taken on 2026-11-22. The numbers continue the fifth critique's.

| | Decision | Section |
|---|---|---|
| 24 | An error before the append is the change's failure. An error in the append ends the pass as a crash does | §5 |
| 25 | The IndexedDB measurement is dropped from the plan for now. The records stay in the outbox rows | §6 |
| 26 | `MemDagStorage.withTransaction` queues its calls, as the SQLite and IndexedDB stores do, so `Dag.append`'s check is atomic on every store | §4 |
| 27 | The cases of an insert with an explicit id run on SQLite and IndexedDB only, since memory can't insert with one | §7 |
| 28 | The settle's step 3 writes the inserts' mappings before it marks the deletes | §9 |
| 29 | A projection that starts without a writer key for a group only warns. It replaces the refusal at open of the fifth response, and the error for a change captured after the open | §8 |

**Not decided.** The first point of §9, ingesting an anonymous group without a writer key.

**To confirm.** The channel of §8's warning.

## 4. The memory DAG store runs its transactions concurrently

**What the documents have.** HIP-29's *Changes* 1: "Every store and both of rdb's scoped DAGs append through it, so the stores don't change." The plan's detail 1: "Inside the transaction, before the parents' check, `store.loadHeader(e.hash, ...tx)` … The stores don't change."

**Why memory is different.** The check and the write are atomic only on a store that runs one transaction at a time. SQLite and IndexedDB do (§2). Memory's `withTransaction` runs `await fn()` with no queue (`dag/src/store/mem_dag_store.ts:16-22`). Inside `Dag.append`'s transaction, the parents' check and `index.index` await between the new check and `store.append` (`dag/src/dag.ts:102-109`). So two appends of one entry can both find it missing, and both write it. The second puts the entry back into the frontier (`mem_dag_store.ts:24-33`), and `index.index` runs twice for it.

**Who uses it.** Every memory DAG is a `MemDagStorage`: the replica's memory backend (`replica/src/mem_dag_backend.ts:38`) and mvt's (`mvt/src/dag/mem_dag_storage.ts:16`). So is every DAG of `openMemWorkspace` (`rdb_runtime/src/workspace/workspace.ts:88-99`), which rdb_repl_web's direct client opens (`rdb_repl_web/src/direct_client.ts:75`), and of most tests. HIP-29's case of two devices that make the same anonymous update is among those tests.

**Decision 26.** `MemDagStorage.withTransaction` queues its calls, as the other two stores do. That is a few lines. Reads outside a transaction stay as they are.

**Change.**
- HIP-29's *Changes* 1: the memory store queues its transactions. "The stores don't change" holds for the SQL and IndexedDB stores.
- *Tests*, dag: two appends of one entry at once, on the three stores. Both return the hash, the entry is written once, and its children stay in the frontier. The sequential case doesn't reach the race.
- The plan's detail 1, and ENTRY_04 in P1.

## 5. An error that isn't a refusal

**What the documents have.** *Why it is sound*: "Every change a pass reaches gets a record, so the settle removes it from the outbox." *The algorithm*: "if e is refused: record(change, its failure event)".

**What is missing.** Neither says what an error that isn't a refusal does. Today `attempt` turns every error from `bundle` into a failure. A `ValidationRejectedError` gives its reason, and any other error its message (`rdb_adapter/src/ingest_orchestrator.ts:456-465`). If `prepare…` catches only refusals, a change whose entry throws anything else gets no record. That could be a bug, or a payload rdb can't build. The handled prefix ends at it, the next pass throws on it again, and no projection applies while it is pending. This is the stall of the fifth critique's §5, from another cause. The planner can throw too, and today a throw there ends the whole pass.

**Decision 24.**
- **Before the append.** An error while the planner translates a change, or while rdb builds, signs or validates its entry, is the change's failure, with the error's message as its reason, as today.
- **In the append.** An error while appending a recorded entry ends the pass, as a crash does, and reaches `onError`. Examples are an I/O error and an IndexedDB quota. The record stays. The next cycle's end finds the entry missing and the change not handled, and a new pass builds it again (*Crashes*).

So among the changes a pass can reach, the only one that holds the projection back is one whose append keeps failing, as on a full disk. `onError` reports that, and in rhost it fails the host.

**Change.**
- *Ingestion at the checkpoint*, *What became of each change*: a failure also covers any error before the append.
- *Crashes*: an error in the append ends the pass as a crash does.
- *Why it is sound*: the claim of progress names both rules.
- *Tests*, below (§11).
- The plan's detail 7, step 3, and a case in P5.

## 6. The IndexedDB measurement

**What the documents have.** The plan's P4: "Measure on IndexedDB: a pass of 1,000 inserts, with the records and with `writeRecord` stubbed out. If the records are a large share of the pass, report before P5." HIP-29's second open question: "The case to measure is a pass of 1,000 inserts on IndexedDB, with and without the records, during the implementation."

**Why it can't run as planned.**
- **No real IndexedDB.** rdb_adapter_idb and dag_idb test on fake-indexeddb (`rdb_adapter_idb/package.json:29`, `dag_idb/package.json:24`). That is an IndexedDB written in JavaScript over memory.
- **No browser.** No module runs one: no `package.json` names Playwright or Puppeteer. What a transaction costs on fake-indexeddb says nothing about a browser, where each commit goes through the browser's storage.
- **Not the new pass.** In P4 the new orchestrator doesn't exist yet. The pass measured would be today's.

**Decision 25.**
- **Dropped for now.** The plan's P4 doesn't measure, and nothing waits on a report before P5.
- **The records stay in the outbox rows**, as the fifth response decided. They are needed for correctness either way.
- **The question stays open.** HIP-29's second open question is to be measured in a browser, in HIP-20's trial app, like the first.

**When a move would be cheapest.** Moving the records into rdb's store changes the outbox and rdb's store. Neither is released now (the plan's detail 3: no upgrade path). After a release, the move needs a migration of both. If the trial app shows a cost, the move is cheapest before then.

**Change.**
- HIP-29's second open question: "during the implementation" becomes "in a browser, in HIP-20's trial app".
- The plan: P4 loses the measurement and its report. *Deferred* keeps the move into rdb's store, to be decided after the trial app.

## 7. Memory can't insert with an explicit id

**What the documents have.** HIP-29's *Tests*: "an insert that reuses a deleted row's id, by an explicit id on SQLite and memory and by a `put` on IndexedDB". The plan's IN72 says the same.

**What memory does.** `localInsert` throws on a value for the id column (`rdb_adapter/src/memory_target.ts:274-278`), as HIP-20's *Changes* 1 decided, and allocates ids that never repeat. So memory can't reuse a deleted row's id. IN73, an insert, a delete and an insert on one local id, can't be built on memory either.

**Decision 27.** Both cases run on SQLite, with an explicit id, and on IndexedDB, with a `put`. HIP-20's own case already covers memory's refusal of an id.

**Change.** HIP-29's *Tests*, and the plan's IN72 and IN73.

## 8. Starting without a writer key

**What the documents have.** *No projection over a pending change*, "Without a writer key". `RdbProjection` refuses to open while the outbox holds a change of a group without a writer key. A change captured after the open "reports an error that says so, and schedules no other". This is the fifth critique's decision 23, with the refusal at open that the fifth response took.

**Decision 29.** A projection that starts without a writer key for a group only warns. It opens, and its reads work. The refusal at open goes, and so does the error for a change captured after the open.

**What a warning alone leaves unsaid.** No pass can ingest such a change, so it gets no record. Three rules follow, and the documents have none of them, since until now such a change stopped everything.
- **The pass stops at the first such change.** It can't skip it:
  - **A later change can depend on it,** such as an insert in a group with a key that references its row.
  - **Skipping would ingest later changes twice.** The settle removes only the outbox's leading rows that have a record (*The end of a pass*). A change skipped at the front would keep every later row in the outbox, recorded. The next pass would build their entries again, and an insert would get a new uuid.

  So the pass ends at that change, and its end settles the changes before it, as it can after any change.
- **No projection applies while it is pending.** The outbox isn't empty, so every group's checkpoint stays where it is, and remote changes wait.
  - **The invariant holds.** The change stays written at the checkpoint it was made against.
  - **A later open with the key ingests it there.**
- **No cycle schedules another because of it.**
  - **The loop to avoid.** After a refused apply, `RdbProjection` schedules the next cycle itself (*A refusal ends the cycle*). A cycle that ends at such a change can't do the same, or it would loop on it.
  - **The next cycle** starts as any does: when rdb grows or the outbox signals (`rdb_projection/src/projection.ts:550-562`). It stops at the same change at once.

**The warning.**
- **When.** Once for each group without a writer key, not once per cycle: at open, if a change of it is pending, or else when a cycle first meets one.
- **What it says.** It names the group, and says that remote changes wait until a projection opens with a key for it.
- **The channel (to confirm).** `RdbProjection` has no channel for warnings today, only `onResult` and `onError` (`rdb_projection/src/projection.ts:64-66`), and in rhost `onError` fails the host. Proposed: an `onWarning` option, with `console.warn` when it isn't given.

**Who meets it.**
- **Not rhost or rdb_repl,** which always open with a key (§2).
- **An app that opens `RdbProjection` without a writer** over a target written with one.
- **An app whose groups are all anonymous** and that passes no key (§9).

**Change.**
- *The algorithm*: `runPass` ends at the first change of a group without a writer key, and a cycle that ends there schedules no other.
- *No projection over a pending change*, "Without a writer key": the rules above and the warning, in place of the refusal and the error.
- *What changes for apps*: such a projection opens with a warning, shows no remote changes while the change is pending, and ingests it once a projection opens with the key. Today it projects over the pending changes.
- *Why it is sound*, the paragraph on progress: such a change holds the projection back, with a warning, until a projection opens with its group's key.
- *Changes* 3 (`syncDatabase`), 6 (`open()` warns instead of refusing) and 10 (the rdb_projection README).
- *Tests*, rdb_projection (§11).
- The plan: decision 11, detail 8, RDBPROJ19, and the channel under *To confirm*.

## 9. Smaller points

- **Anonymous groups need a writer key (not decided).**
  - **Today.** Ingestion takes a group's author through its writer. `authorFor` calls `writerFor`, which throws for a group without one (`rdb_adapter/src/ingest_orchestrator.ts:362-373`). It does so even for a group with no identity provider, which signs nothing. `syncDatabase` ingests only the members that have a writer (`:646`).
  - **With HIP-29 and §8.** An app whose groups are all anonymous and that passes no key opens with a warning, and its pending changes hold remote changes back for good.
  - **What HIP-29 makes possible.** It records each group's mode, `anonymous` among them, so ingestion could take an anonymous group's changes with no key. The requirement is older than HIP-29.
- **The settle's mappings for one local id.**
  - **The case.** An insert, a delete and an insert on one local id in one pass (IN73).
  - **The rule.** Step 3 writes the first insert's mapping and marks the delete, so the mapping must end `deleted`. It does if step 3 applies the inserts' mappings before the deletes.
  - **Decision 28.** Step 3 writes the inserts' mappings first, then marks the deletes. HIP-29's settle step 3 says so, and so does the plan's detail 6, which carries `inserts` and `deletes` as two lists.

## 10. The plan

| In the plan | What changes |
|---|---|
| Decision 11 | A start without a writer key only warns (§8) |
| Detail 1 | `MemDagStorage.withTransaction` queues its calls. "The stores don't change" holds for SQL and IndexedDB (§4) |
| Detail 6 | Step 3 writes the inserts' mappings before it marks the deletes (§9) |
| Detail 7, step 3 | An error before the append is the change's failure. An error in the append ends the pass (§5) |
| Detail 7, per change | The pass ends at the first change of a group without a writer key (§8) |
| Detail 8, no writer | `open()` warns instead of refusing. A cycle that ends at such a change schedules no other, and the warning comes once per group (§8) |
| P1, ENTRY_04 | Two appends of one entry at once (§4) |
| P2, RDBPROJ19 | As in §11 |
| P4 | The measurement and its report go (§6) |
| P5 | IN72 and IN73 on SQLite and IndexedDB (§7). The cases of §5 |
| *Deferred* | The records in rdb's store, to be decided after HIP-20's trial app (§6) |
| *To confirm* | The channel of §8's warning |

## 11. Tests

- **dag,** on the memory, SQL and IndexedDB stores: two appends of one entry at once both return its hash, write it once, and leave its children in the frontier.
- **Shared ingestion suite:**
  - A change whose translation or entry throws an error that isn't a refusal gets a failure event with the error's message. It leaves the outbox at the settle, and a projection then applies.
  - An append that throws once ends the pass and reaches `onError`. The next cycle builds the entry again and appends it once.
  - IN72 and IN73 on SQLite and IndexedDB only.
- **rdb_projection** (RDBPROJ19):
  - Opened without a writer key for a group, over a pending change of that group, with changes of a group with a key before it and after it. The projection opens and warns once. A pass ingests the changes before it and stops there, no projection applies, and no cycle loops. Reads work.
  - A later change of that group, captured after an open over an empty outbox, gives the same, with the warning when a cycle first meets it.
  - Opened again with the key: the change, and the changes after it, are ingested at the checkpoint they were written against, and a projection then applies.

## 12. Not checked

- The race of §4 is read from the code. It was not reproduced.
- Whether memory's flat index copes with a second `index` call for one entry. With the queue of §4 it never gets one.
- What the new orchestrator's `prepare…` catches. It isn't written yet.
- How a warning reaches an app, since `RdbProjection` has no channel for one yet (§8).
