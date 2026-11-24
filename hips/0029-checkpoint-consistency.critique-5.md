# Fifth critique of HIP-29: Checkpoint consistency

A review of [HIP-29](0029-checkpoint-consistency.md) as revised on 2026-11-22 after [the fourth response](0029-checkpoint-consistency.response-4.md), of its plan, `plans/checkpoint-consistency.md`, and of the code they cite. The points were first given in chat on 2026-11-22 and discussed the same day. The decisions taken there are in §3, and every finding below is written with its decision.

Paths are relative to `modules/`. Nothing was run for this critique. The findings come from reading the code, and §12 lists what that leaves unchecked.

## Verdict

**Is the core still sound?** Yes. Each claim checked in §2 holds. The four rules, the records, the end of a pass and the one delta per group need no change.

**Is anything still wrong?** Three gaps:
- an IndexedDB `put` that captures every column turns a merge of fields into a merge of rows (§4);
- the planner still drops changes the way it did for crash replays, and a dropped change has no record, so it never leaves the outbox and holds every projection back (§5);
- after an observe, ingestion compares again only the two readings of the binding it observed, while a gate can read other bindings (§6).

Two more points: a projection opened without a writer key stalls on a pending change (§7), and the records cost a target transaction per entry, for which there is an alternative (§8).

| Item | In HIP-29 | In the plan |
|---|---|---|
| §4. An IndexedDB `put` names nothing | *Capture*, *What changes for apps*, *Considered and not taken* | decision 2, detail 4, P3 |
| §5. Every change ends with a record | *Ingestion at the checkpoint*, *Reverts*, *HIP-20*, *Changes* 2, *Tests* | details 2, 6 and 7, P5, *Expected test flips* |
| §6. Every gated binding is compared again | *Writes void where they stand* | detail 7, P5 |
| §7. A projection without a writer key | *No projection over a pending change*, *What changes for apps* | detail 8 |
| §8. What the records cost | *Open questions* 2, *Crashes* | decision 6, P1, P4, *Risks* |

## 1. How the claims were checked

**Read.** HIP-29, the plan, the four earlier critiques and their responses, and HIP-20's *Changes* 1. In the code:
- rdb: `rdb/src/rtable_group/group.ts` (bindings, `observe`, `bundle`, `diagnoseObserve`, the schema version at a position) and `op_delta.ts`;
- dag: `dag/src/dag.ts`, `dag/src/store/dag_store.ts`, `dag_idb/src/idb_dag_store.ts` and `dag_sql/src/sql_dag_store.ts`;
- sync: `sync/src/synchronizer.ts`;
- rdb_adapter: `ingest.ts`, `ingest_orchestrator.ts`, `project.ts`, `types.ts`, `memory_target.ts` and `index_reconcile.ts`;
- the targets: SQLite's types, checks, capture setup and outbox (`rdb_adapter_sqlite/src/sqlite_target.ts`), and IndexedDB's facade and schema (`rdb_adapter_idb/src/idb_facade.ts`, `idb_schema.ts`);
- rdb_projection's options, and rhost's and rdb_repl's calls of `openProjection`;
- `rdb/src/rschema/canonical.ts` and `validate.ts` for the string-carried types.

**Not run.** No probes and no tests.

## 2. Claims that hold up

- **The end's observes terminate.** A group's bindings come from its create entry (`rdb/src/rtable_group/group.ts:320-322`), and a binding names the hash of a group that already exists. So bindings can't form a cycle, and the end's loop of *The algorithm* terminates once every group that binds a moved one observes it. The loop does need a stop for an observe it declines (§9).
- **An observe in the chain changes no schema.** Schema deploys are ref-advances too, but `diagnoseObserve` returns `live` for the schema ref (`group.ts:1266`), and the schema at a position is the union of the deploys at or below it (`:428-432`). So a deploy of C never flips at C ∪ I, and the settle needs no schema actions.
- **The settle's removals don't cascade.** SQLite runs with `foreign_keys` off (`rdb_adapter_sqlite/src/sqlite_target.ts:448`). Memory and IndexedDB have no foreign keys.
- **A row the delta reinstates can be written.** A row action is keyed by row hash (`rdb_adapter/src/types.ts:524-528`). SQLite keeps a deleted row's sync row, so a reinstatement reuses its local id (`sqlite_target.ts:1170-1175`). A row void at C has never been in the target, and plan detail 6 has to allow for it (§9).
- **The string-carried types settle alike.** rdb refuses a decimal, bigint or bytes value that isn't in canonical form (`rdb/src/rschema/validate.ts:133-140`), and the adapter never normalizes one: no module under `rdb_adapter*` calls `normalizeDecimal` or `normalizeBigint`. So a native write of `2.5` into a decimal column of scale 2 is refused and reverted, and nothing differs silently.
- **Outbox ids are never reused**: `AUTOINCREMENT` on SQLite (`sqlite_target.ts:523`), `autoIncrement` on IndexedDB (`rdb_adapter_idb/src/idb_schema.ts:161`), and a counter on memory (`rdb_adapter/src/memory_target.ts:177`). §8's alternative relies on it.
- **rhost always projects with a writer key** (`rhost/src/host.ts:913`). §7 doesn't reach it.

## 3. Decisions taken in discussion

Taken on 2026-11-22. The numbers continue the fourth critique's.

| | Decision | Section |
|---|---|---|
| 20 | IndexedDB offers a `patch` that compares the fields it is given with the stored record, and captures only those whose value differs | §4 |
| 21 | Every change the planner doesn't turn into an entry gets a failure record or the mark of nothing to write. Taken because it is cheap: the planner already detects each case | §5 |
| 22 | After each observe, ingestion compares the two readings of every gated binding of the observing group | §6 |
| 23 | *What changes for apps* states that a projection without a writer key stalls on a pending change. Refusing to open without a writer key while the outbox holds a change was offered as the alternative | §7 |

**Still open:**
- **Whether `put` compares too** (§4). This critique recommends it.
- **Refusing at open** (§7). This critique recommends doing it as well as stating the stall.
- **Where the records go** (§8): in the outbox rows, as now, or in rdb's store, in the append's transaction.
- **`Dag.append` and sync** (*Outside this HIP*). In discussion the race was answered with the single projector, but it is between ingestion and sync in that one process.

## 4. An IndexedDB `put` names nothing

**What the HIP has.** *Capture*: "a `put` over an existing record captures every column of the table". Today the facade compares the record with the stored one and captures what differs (`rdb_adapter_idb/src/idb_facade.ts:274-286`).

**What capturing every column costs.**
- **Fields no longer merge.** Two devices edit different fields of one row, each with a `put`. Each change names every column. The two entries are concurrent, and for every column the larger hash wins, so one entry wins all of them and the other edit is lost whole. Today each change holds its own field, and both land. On SQLite, an `UPDATE` that sets one column still merges by column.
- **A refusal repeats.** A `put` sets `c` to a value rdb refuses. Until the settle, the record still shows that value. The app reads it and puts it back to change `b`, so the second change names `c` with the refused value and is refused too: the edit of `b` is lost. Compared, the second change holds only `b`, and it lands.
- **A row whose referenced row is gone can't be edited.** Every `put` names the foreign-key column. An update that names it is refused when the referenced row isn't live (the fourth critique's N2), and rdb keeps such rows, so the target shows them. With the referenced row deleted concurrently, the update is voided, while the same edit through SQLite lands.

**Comparing keeps the invariant.** A compared change holds the columns that differ from the stored record, which is rdb's row at C with the earlier pending changes on top. Applied in commit order, the compared changes give the record back, and a revert that applies them to rdb's row gives what they wrote. *Capture* motivates naming with a statement that names `a`: "the revert then loses the `a` that the second write named". A `put` names nothing. It repeats what the app read. Losing the `a` of a refused write is then right: that `a` was part of the write rdb refused.

**Decision 20.** IndexedDB offers a `patch` that compares the fields it is given with the stored record and captures only those whose value differs.

**Open: `put`.** A `patch` helps code written for it. Code written against the IndexedDB API only has `put`, and if `put` captures every column, that code keeps the three costs above. This critique recommends that `put` compares as it does today. "A named column is a write" then holds where a write names its columns: SQL statements, memory's `localUpdate`, and HIP-20's writer.

**Change.**
- *Capture*, IndexedDB: `patch`, and `put` as decided.
- *What changes for apps*: the line on IndexedDB's `put`.
- *Considered and not taken*, "Capture only the columns whose value changed": it holds for SQL statements and memory. On IndexedDB, `patch` compares, and so does `put` if decided, since a `put` can't name a column.
- The plan's decision 2 and detail 4: `changedColumns` stays, for `patch` and, if decided, for `put`. IN39 and ADPTI-10 follow.
- The fourth critique's decision 15 loses its IndexedDB case.

## 5. Every change ends with a record

**What the documents have.** *The end of a pass*: "The handled changes are the outbox's leading rows whose record says what became of them." *The algorithm* gives each change one of three records: an entry, a failure, or the mark of nothing to write.

**What the planner does.** Some of its outcomes are none of the three. It drops a change with a warning in the ingest result, and stores no op event and no revert:
- an insert on a local id whose mapping can't be reused: `deleted`, `ingestion_failure`, or a row projected from rdb, which has no uuid (`rdb_adapter/src/ingest.ts:251-268`);
- a second insert capture for one local id in the batch (`:592-596`);
- an update or a delete of a row whose insert was refused (`:461-467`);
- a change of a table that no member projects (`:581`).

The orchestrator also records no failure for a refusal that lacks a table, a kind or a local id (`ingest_orchestrator.ts:442-444`). The refusal for a missing rdb table carries no kind (`ingest.ts:451-453`).

These drops were written for crash replays: "an insert on an id that already has a non-reusable mapping is always stale" (`ingest.ts:256-262`). HIP-29 has no replays, and the plan's list of removals (detail 7) doesn't include them.

**What follows from them.**
- **Liveness.** A dropped change gets no record, so the handled prefix ends at it. The settle leaves it in the outbox, the next pass drops it again, and no projection applies while it is pending. One such change holds remote changes back for good.
- **The invariant.** If the implementation keeps removing it from the outbox instead, the target keeps a row that rdb doesn't have. An `INSERT OR REPLACE` over a row from rdb keeps the values it wrote. An insert that reuses a deleted row's id keeps the row. HIP-20's *Changes* 1 lists both.

**The fix.** It is cheap: the planner detects each case already, and only what it returns changes, plus one rule in *Reverts*.
- **An insert on a local id that already has an identity**, a mapping of any status or one minted earlier in the pass, is refused. Its revert reads the row of that identity: rdb's row at C ∪ I, or none, which removes the local row. The mapping keeps its status. Only a refused insert with a new identity marks its mapping `ingestion_failure`.
- **An update or a delete of a row whose insert was refused** is refused, as *Reverts* already says: "its pending changes are refused at their turn". Its revert finds no row and writes nothing.
- **A missing rdb table** is a refusal with its kind.
- **A change of a table that no member projects** gets the mark of nothing to write. A member without a writer key is not such a table: its changes are §7's case.

**Decision 21.** Taken.

**What else follows.**
- HIP-20's *Changes* 1 item "A native insert over an existing local id is reverted to rdb's row" lands with this HIP, both its cases. Its new change-source method, which tells a stale replay whose local row is gone from an insert whose row is still there, isn't needed. There are no replays: every such insert is refused, and the revert of one whose local row is already gone writes nothing.
- `mintInserts` mints only for a local id that has no identity. Its read-back of an active mapping's uuid (`ingest.ts:270-273`) goes with the replays.

**Change.**
- *Ingestion at the checkpoint*, *What became of each change*: the planner's refusals gain the insert on a local id with an identity, the change of a row whose insert was refused, and the missing table. No change is dropped.
- *Reverts*: the rule for a refused insert splits as above.
- *HIP-20*: the native insert over an existing local id lands here. HIP-20's header and *Changes* 1 follow.
- *Changes* 2, the planner: the drops become refusals.
- *Tests*: below (§11).
- The plan: details 2, 6 and 7, P5's cases, and the flips of ADPTI14 to ADPTI18 (§10).

## 6. Every gated binding is compared again after an observe

**What the documents have.** *Writes void where they stand*: "Ingestion compares the two readings of each gated binding at C, and again after each observe it appends to that group." The fourth critique's §9 reads this as the observed binding only: "After one, compare that binding's readings again."

**Why that binding isn't enough.** An observe gate can read the group's other bindings, and the group's own tables (`foreign` and `local` in `diagnoseObserve`, `rdb/src/rtable_group/group.ts:1285-1286`). Ingestion already orders its observes by the graph of what the gates read (`ingest_orchestrator.ts:538-540`). Say G binds K and M, and M's gate reads K:
1. G's checkpoint holds a live observe of M.
2. The pass writes an observe of K. It brings in an entry of K that is concurrent with what the gate of M's observe read, and that voids it.
3. M's two readings now differ, but only K's were compared again.
4. A write that reads M passes rdb's checks and is void where it stands.

**Decision 22.** After each observe, ingestion compares the two readings of every gated binding of the observing group. That is one comparison per gated binding per observe. Narrowing it to the bindings whose gates read the observed group, directly or through other gates, saves little.

**Change.** *Writes void where they stand*, "Only while the readings differ". The plan's detail 7. A test in rdb and one in the shared suite (§11).

## 7. A projection without a writer key

**What the HIP has.** "A projection with no writer key for any group can't ingest, so a pending change would refuse every apply. The cycle then reports an error that says so, and schedules no other."

**Who meets it.** Capture stays on once a target has had it (`rdb_adapter_sqlite/src/sqlite_target.ts:490-495`). A target written with a key and opened again without one, while changes are pending, then takes no remote changes, and reads show it as it was. Today such a projection is read-only and goes on projecting over the pending changes (`rdb_projection/src/projection.ts:56-57`). rhost always passes the host's key (`rhost/src/host.ts:913`), so this reaches only apps that open `RdbProjection` without a writer.

**Decision 23.** *What changes for apps* states it. Refusing to open without a writer key while the outbox holds a change was offered as the alternative:
- **Refusing at open** names the cause where the caller can act, before anything stalls. It covers only changes captured before the open: a later one still meets the cycle's error. A projection that refuses to open can't be read through `RdbProjection` either.
- **Only stating it** lets the projection open and its reads work, with remote changes held back until a projection opens with a key.

This critique recommends both: the refusal at open, and the cycle's error for a change captured later, both stated. An open without a key over pending changes is a configuration mistake, and the refusal turns a silent stall into an error at the call that made it.

**Change.** *No projection over a pending change*, "Without a writer key". *What changes for apps*. If the refusal is taken: the plan's detail 8 and an rdb_projection case.

## 8. What the records cost, and where else they could go

**What the HIP has.** Each record is a target transaction, made before its entry is appended (*Open questions* 2).

**Why the records can't be batched.** Each entry of a chain is built at the one before it. Validation reads that entry from rdb's store, and the DAG's append checks that each parent is there (`dag/src/dag.ts:98-112`). So no entry can be built before the one before it is appended, and no two records can share a write.

**What bounds the cost.** rdb appends each entry in a store transaction of its own (`Dag.append`, `dag/src/dag.ts:98-112`). The record adds one transaction, in another database, beside each one that is already there. On IndexedDB that roughly doubles a pass's write transactions. Validation's reads come on top of both. Not measured.

**An alternative: the record in rdb's store, in the append's transaction.** It would be a local table of rdb's store, never synced, keyed by the target and the outbox row's id, and written by the same store transaction that appends the entry.
- **No extra write for an entry or an observe.** A refusal, the mark, and an entry rdb already holds each still need a write of their own, and these are rare.
- **The record and the append commit together.** A hash recorded for an entry that rdb doesn't hold can't happen, so the fourth row of the table in *Crashes* goes. So does the first case of a lost commit: the target can no longer lose an insert's record while rdb keeps its entry, so a power failure can't show a row twice.
- **The rest stays.** The end of a pass reads the records from rdb's store instead of from the outbox rows. The settle, the deltas and the reverts don't change. The outbox loses its record column, and the change source its `writeRecord`.
- **What it needs.**
  - `DagStore` gains a local record written in an append's transaction, and a read of it, on the memory, SQL and IndexedDB stores (`dag/src/store/dag_store.ts`, `dag_sql`, `dag_idb`). rdb's `appendPrepared` passes the record through.
  - Each target gets a stable id, since one replica can feed several targets.
  - The settle can't remove records from rdb's store, which it doesn't write. The end of a pass removes those whose outbox id is below the outbox's first row, since outbox ids are never reused (§2).
  - rdb's store holds a little state about each target. A target that is deleted leaves its records behind.

**Not taken: building a run of entries ahead.** Building several entries against a layer of entries not yet appended would let their records share a write. But then every read that validation makes would go through that layer.

**Open.** Measure in P4, on IndexedDB: a pass of 1,000 inserts with and without the records. If the records are a large share of it, move them to rdb's store before P5, which is where they are first read. The move is also worth weighing on its own: it removes one crash case and one power-failure case, at the price of an API change in three dag modules.

**Change.** If the records move: *Ingestion at the checkpoint*, *What became of each change*; *Crashes* and its table; *Changes* 1, with dag, dag_sql and dag_idb in the header's modules; *Changes* 2 to 4 lose the outbox's record; the plan's decision 6 and details 3, 6 and 7. Either way: the measurement in P4.

## 9. Smaller points

- **The end's loop needs a stop.** In *The algorithm*, "while some group h binds a group B whose end moved, and end[h] doesn't observe end[B]" never ends when the end declines that observe: the gate refuses the writer, or the observe would be void. And `end[g]` is safer as a union than as "the last hash", since a record merges across a crash (plan detail 6):

  ```
  end[g]  = the canonical version of checkpoint[g] with the hashes recorded for g that rdb holds
  while some group h binds a group B whose end moved, end[h] doesn't observe end[B],
        and this end hasn't declined that observe:
  ```
- **The deltas.** "for each g whose recorded hashes include an observe" also counts an observe that a crashed pass left on a row the settle kept, which is in the past of the new checkpoint. The condition reads better as "whose chain from checkpoint[g] to end[g] holds an observe".
- **Reinstated rows.** Plan detail 6 keys `reverts` by local id. A row that the delta reinstates was void at C and has never been in the target, so it has no local id. Its revert is keyed by its row hash, and it has no pending changes.
- **The delta's schema.** By §2, the delta from C to C ∪ I changes no schema. The settle can check that its schema channel is empty and halt if not, at no cost.
- **HIP-20's *Changes* 1 isn't committed.** It is in the working tree: 17 files, about 1,260 lines added. Committing it before P0 gives the plan's *Expected test flips* a fixed base.
- **The plan's *To confirm*.** The end's observes recorded on the last outbox row that has a record: *The algorithm* already uses it. It changes if the records move to rdb's store (§8).

## 10. The plan

| In the plan | What changes |
|---|---|
| Decision 2, IndexedDB | `patch` compares. `put` as decided (§4) |
| Detail 2 | The drops become refusals. `mintInserts` mints only for a local id with no identity (§5) |
| Detail 4 | `changedColumns` stays for `patch`, and for `put` if decided (§4) |
| Detail 6 | A refused insert on an existing identity reverts to that identity's row and keeps its mapping (§5). A reinstated row is keyed by row hash (§9) |
| Detail 7 | Every gated binding is compared again after an observe (§6). The removals add the planner's drops and the read-back of an active mapping (§5) |
| Detail 8 | The refusal at open, if taken (§7) |
| P3 | `patch` on IndexedDB. IN39 and ADPTI-10 follow §4 |
| P4 | The measurement of §8 |
| P5 | The cases of §5 and §6 |
| *Expected test flips* | ADPTI14, ADPTI15 and ADPTI17: the drop becomes a refusal. ADPTI16: an insert on an active mapping is refused, not replanned with its uuid. ADPTI18: the update and the delete are refused (`rdb_adapter/test/ingest_tests.ts:258-353`) |
| *Risks* | A record write per entry, or the store change of §8 |

## 11. Tests

- **§4:**
  - Two devices patch different fields of one row concurrently, and both fields land. If `put` compares, the same with `put`.
  - A patch after a refused one, which carries the refused value of the refused column and a new value of another, lands with the new value.
- **§5,** each on the three targets, checking that the outbox empties at the settle and that a projection then applies:
  - an `INSERT OR REPLACE` over a row from rdb, and over one inserted on this device: a failure event, rdb's row back, and the mapping still `active`;
  - an insert that reuses a deleted row's id, by an explicit id on SQLite and a `put` on IndexedDB: a failure event, the row removed, and the mapping still `deleted`;
  - an insert, a delete and an insert of one local id in one pass: the second insert is refused;
  - an update of a row whose insert was refused: a failure event, and nothing written;
  - a change of a table no member projects leaves the outbox with no event.
- **§6.** G binds K and M, and M's gate reads K. A pass's observe of K voids G's observe of M, and a later write of the pass that reads M is refused. In rdb: with the option set from both comparisons, the write is refused.
- **§7,** if the refusal is taken: opening without a writer key over a pending change throws, and opening over an empty outbox works.
- **§8:** the measurement in P4.

## 12. Not checked

- No claim here was run. The liveness gap of §5 is read from *The algorithm* and the planner's code, and the gap of §6 from `diagnoseObserve`.
- The costs of §4's comparison, §6's comparisons and §8's records.
- What each DAG store does with a second append of one entry, beyond IndexedDB (*Outside this HIP*).
- The transaction details of §8's alternative on each store.

## Outside this HIP

**`Dag.append` isn't idempotent, and sync checks for an entry long before it appends it.** In discussion, the race of the chat review was answered with the single projector. But it doesn't involve a second projector. It is between ingestion and sync, in the one process.
- **The race.** Sync checks whether it holds an entry when the entry's header arrives (`sync/src/synchronizer.ts:796`). Then it fetches the payload, validates it and appends it (`:1301-1323`). `Dag.append` checks the entry's parents inside its transaction, but not the entry itself (`dag/src/dag.ts:98-112`). When ingestion appends the same entry in between, sync appends it a second time. HIP-29's own check, "append e unless rdb holds it", has the same gap the other way around.
- **What a second append does.** On IndexedDB every write of the append is a put (`dag_idb/src/idb_dag_store.ts:45-59`). So the second one succeeds and puts the entry back into the frontier, as it does on memory. The SQL store's frontier insert is `INSERT OR IGNORE` too (`dag_sql/src/sql_dag_store.ts:59`).
- **When it happens.** It needs two devices that make the same entry, the case *Entries rdb already holds* names: two devices at one checkpoint that make the same anonymous update. Today's append at the frontier meets it as well.
- **The fix.** `Dag.append` returns the hash without writing anything when its transaction finds the entry already there. That closes sync's second append too, and HIP-29's check becomes a plain append. It belongs to dag, which HIP-29 doesn't list.
