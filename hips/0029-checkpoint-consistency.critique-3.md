# Third critique of HIP-29: Checkpoint consistency

A review of [HIP-29](0029-checkpoint-consistency.md) as revised on 2026-11-19 after [the second response](0029-checkpoint-consistency.response-2.md), and of its plan, `plans/checkpoint-consistency.md`. The points were first given in chat on 2026-11-20 and discussed that day and the next. The decisions taken there are in §3, and every finding below is written with its decision. The code references and the probes were checked again against the working tree on 2026-11-21.

Paths are relative to `modules/`. The checks are labeled R1, D1 to D5, A1 to A3, G1 to G5, F1, C1 to C6 and T1 to T4, and §1 lists them.

## Verdict

**Does the HIP still have unnecessary complexity?** Yes, around a core that has none. The four core rules are right and should stay as they are: ingestion appends at the checkpoint, the settle moves the checkpoint, no projection applies over a pending change, and each captured change is one entry. Four mechanisms around them can go:

- the list of held rows (§4);
- the column-by-column fallback of a refused update (§5);
- reserved identities and the two ways of finding an entry in rdb (§6);
- the two behaviours of a moved checkpoint (§7).

In exchange, the outbox row records its entry's hash, rdb splits `bundle` and `observe` in two steps, and SQLite's per-column update triggers come back.

**Is it correct?** The core held in every check (§2). There is one real gap: rdb can accept an entry that is void where it stands, and the target then keeps a write that rdb doesn't have (§8). Two limits remain and are now documented on their own: a write can be checked against another version of a bound group than the target shows ([HIP-30](0030-cross-group-skew.md), §9), and remote changes wait for an empty outbox ([HIP-31](0031-projection-over-pending-changes.md), §10).

| Item | In HIP-29 | In the plan |
|---|---|---|
| §4. Held rows aren't needed | *The invariant*, *Held-back reverts*, *The settle moves the checkpoint*, *Why it is sound*, *Changes* 1 to 3, *Tests* | details 5, 6 and 12, P2, P3a |
| §5. An update is one change, refused whole | *Ingestion at the checkpoint*, *Updates on SQLite*, *HIP-20* | decision 1, details 1 and 3 |
| §6. Entries are found by a recorded hash | *Ingestion at the checkpoint*, *Reserved identities*, *Crash replay*, *Why it is sound*, *Considered and not taken* | decisions 2 and 3, details 2, 7 and 13, *Risks* |
| §7. A moved checkpoint halts in both places | *One projector per target* | detail 4 |
| §8. An accepted entry can be void where it stands | *Ingestion at the checkpoint*, *Why it is sound*, *Changes*, *Tests*, the header's modules | a new phase in rdb |
| §9. Skew between groups | *What changes for apps* | none |
| §10. The wait stays | *Considered and not taken*, *Open questions* | none |

## 1. How the claims were checked

**Read.** HIP-29, the plan, both earlier critiques and responses, and HIP-20 where HIP-29 touches it. In the code: `rdb_adapter/src/ingest.ts`, `ingest_orchestrator.ts`, `project.ts`, `types.ts`, `row_actions.ts`, `ref_advance.ts` and `memory_target.ts`; the capture triggers and the settle of `rdb_adapter_sqlite/src/sqlite_target.ts`; `rdb/src/rtable_group/group.ts`, `validate_ops.ts` and `op_delta.ts`; `dag/src/dag.ts`; `crypto/src/signing.ts`; `rdb_projection/src/projection.ts`; HIP-14 §2.3 and §4, and `mvt/MVT_DEFINITION.md` §8.

**Probes.** Scratch scripts, not in the repo, in `/private/tmp/claude-501/-Users-santi-Hyper-hhs3-ts/e4a1050f-4052-419b-8a25-72fef9ed73c5/scratchpad/`. The ones that load rdb run from `modules/rdb` or `modules/rdb_adapter` with `node --import ../../register.mjs <file>`. The SQLite ones run with plain `node`. All were run again on 2026-11-21.

*A row op appended after everything* (`own_row_only_probe.mjs`):

| | Scenario | Result |
|---|---|---|
| R1 | On 12 generated histories of 160 ops (one group, schema changes, concurrent branches), one row op is appended at a recorded version C, and the delta from C to the new entry is read. 1,275 ops: 497 inserts, 389 updates, 389 deletes | Every op was accepted. None changed a row other than its own, flipped a verdict, or was void where it stood |

*The delta from the checkpoint plus the chain* (`tip_delta_probe.mjs`, from the session of 2026-11-19). A target projected to C ∪ I and then to the frontier is compared with a full projection at the frontier:

| | Scenario | Result |
|---|---|---|
| D1 | Concurrent updates of one row | Same |
| D2 | An own insert, deleted remotely after it | Same |
| D3 | An own update, with the row deleted concurrently | Same |
| D4 | An own comment, with its post deleted concurrently | Same |
| D5 | An own chain of two entries, with remote writes concurrent with it and after it | Same |

*An accepted entry that is void where it stands* (`void_at_tip_probe.mjs`, `folds_more_probe.mjs`). A users group K holds a grant for Bob. An observer group G binds K behind a gate: only G's admins may observe. Mallory, an admin, writes the only observe that imports Bob's grant. Concurrently, the other admin removes Mallory from the admins, so her observe is void:

| | Scenario | Result |
|---|---|---|
| A1 | Bob inserts a note, which a rule allows to holders of the grant. The insert is appended after everything G holds | rdb accepts it. At its own position it is void, and rdb has no such row. The delta from the checkpoint to the insert has no row change and no verdict change |
| A2 | The same shape with a foreign key in place of the rule: a row of G references the grant's row in K | Accepted, and void at its own position |
| A3 | A1 through today's adapter, on a memory target | Ingestion reports the insert accepted. The target has the row, rdb has none, the mapping is `active`, and no event is stored. After another cycle the row is still there |

*The two readings of a bound group* (`folds_agree_probe.mjs`, `folds_more_probe.mjs`). G can read K in two ways: at the version every observe gives, or at the version the live observes give. The probe compares the two (`resolveObservedForeignVersion` and `resolveLiveObservedForeignVersion`, `rdb/src/rtable_group/group.ts:783`, `:794`) and then lets Bob insert:

| | State of G | The two readings | Bob's insert |
|---|---|---|---|
| G1 | No observe of the grant yet | Equal | Refused |
| G2 | A live observe of the grant | Equal | Accepted, live at its own position |
| G3 | That observe is void (A1's state) | Differ | Accepted, void at its own position |
| G4 | An admin observes again, after both | Equal | Accepted, live at its own position |
| G5 | A live observe, and then a row op after it that removes its author from the admins | Equal. The observe is still live | Accepted, live at its own position |

*A refused update that lands in part* (`fallback_probe.mjs`), on today's code, which has the same fallback as the HIP:

| | Scenario | Result |
|---|---|---|
| F1 | A table `grants` with `holder`, of at most 8 characters, and `role`. The row is (alice, reader). One update sets (bob-the-builder, admin) | rdb refuses the update, the fallback submits each column, and `role` lands. rdb and the target both hold (alice, admin), a row nobody wrote |

*Capturing the columns an `UPDATE` names, on SQLite 3.53.2* (`named_plain_probe.mjs`). One `BEFORE UPDATE` trigger and one `AFTER UPDATE OF <column>` trigger per column fill one outbox row per updated row:

| | Statements | Outbox |
|---|---|---|
| C1 | A first update sets `a`. A second names `a` with the same value, and `b` | `{"a":"5"}`, then `{"b":"7","a":"5"}` |
| C2 | A first update sets `a` and `c`. A second names only `b` | `{"c":"y","a":"bad"}`, then `{"b":"8"}` |
| C3 | An update that names `c` and changes nothing | `{"c":"y"}` |
| C4 | One statement over two rows, with a `NULL` | One row each: `{"c":null,"b":"9"}` |
| C5 | An update that names the id and `a` | `{"a":"k"}` |
| C6 | Two connections, one update each on the same row | `{"a":"x"}`, then `{"b":"w","a":"x"}` |

*What each capture costs* (`trigger_cost_probe.mjs`). An in-memory SQLite table of six text columns and 2,000 rows. Each row is updated once by a statement that names three columns and changes two, all in one transaction. The figure is the median of twelve rounds, less the time with no trigger (0.37 µs per update):

| | Capture | Added per update |
|---|---|---|
| T1 | One trigger per table, one outbox row with the changed columns (the plan's) | 1.1 µs |
| T2 | One trigger per column, one outbox row per changed column (today's) | 1.0 µs |
| T3 | The columns the statement names, in one outbox row | 5.1 µs |
| T4 | As T3, keeping a named column only if it changed or a pending change writes it | 4.3 µs |

## 2. Claims that hold up

- **A row op after C changes no row it doesn't write** (*The settle moves the checkpoint*). R1.
- **The delta may start from C ∪ I**, which was never rdb's frontier (*Why it is sound*). D1 to D5.
- **A replay is accepted and refused as the first pass was.** Validation reads only the past of the position it is given (`rdb/src/rtable_group/validate_ops.ts:389-452`).
- **The handling of observes in the settle is needed as it is.** I looked for a simpler form. Each one either brought the echo back or still needed the rewrite from C ∪ I.
- **The author and json exceptions, and the `sqlite_sequence` fix,** are needed as written.

## 3. Decisions taken in discussion

Taken on 2026-11-20, except the last, which is of 2026-11-21.

| | Decision | Section |
|---|---|---|
| 1 | A local op that would be void at its own position is refused before it is appended, like any validation failure | §8 |
| 2 | Ingestion compares the two readings of each gated binding once per pass, and again after each observe it appends. Only a pass where they differ runs the second check | §8 |
| 3 | An update of a row is all or nothing | §5 |
| 4 | Capture records the columns an `UPDATE` names, changed or not, at the cost measured in T3. The write library may offer a mode that compares with the row and names only what differs | §5 |
| 5 | A revert sets the row to rdb's row and puts the row's pending changes back on top | §4 |
| 6 | Ingestion writes each entry's hash into its outbox row before it appends the entry. rdb splits `bundle` and `observe` for that. Accepted with it: one target write per entry, a change appended twice when a power failure loses the recorded hash, and a signed observe written twice after a crash | §6 |
| 7 | An insert's uuid is not saved before the append. This replaces the choice, made earlier the same day, of a uuid column that ingestion fills | §6 |
| 8 | In a replay, a change that comes before the last appended one and has no entry stays refused | §6 |
| 9 | Outbox rows are removed only at the settle, after the pass's last observes | §6 |
| 10 | A moved checkpoint halts the projection at the apply as well as at the settle | §7 |
| 11 | No extra observe is written to close the skew between groups. The skew is documented in HIP-30, which is deferred | §9 |
| 12 | The stale reference to a bound group, which only a new live observe corrects, is deferred | §8 |
| 13 | HIP-29 keeps the wait for an empty outbox. Applying over pending changes is documented in HIP-31, which is deferred (2026-11-21) | §10 |

**Still open.** These are points of mine that came up on the way and were not contested, or not answered:

- **The list of held rows.** Dropping it follows from decision 5. It was explained on request and not confirmed in so many words.
- **Columns that can't be written.** With decision 4, an `UPDATE` that names a readonly column, or the author column, with the row's own value needs a rule (§5).
- **IndexedDB.** A `put` has no column list, so the facade has to keep comparing (§5).
- **The second ingestion in a cycle.** I proposed dropping it. It was not decided apart from the wait, so it stays (§10).

## 4. Held rows aren't needed

*Held-back reverts* handles one case: rdb refuses a change, and a newer change to the same row is pending. The revert would overwrite the pending change, so the row goes on a list that the target stores, and the next projection pass applies the revert. The same holds for a row that an observe touched. This is the invariant's one exception. It needs storage on three targets, and it reaches into projection: two early returns must not skip held rows (`rdb_adapter/src/project.ts:138`, `rdb_adapter_sqlite/src/sqlite_target.ts:368-370`).

The revert doesn't have to wait. The invariant already says what the row should hold: rdb's row, with the row's pending changes on top. The settle can write exactly that:

1. It takes rdb's row at C ∪ I.
2. It applies the row's changes still in the outbox, in commit order.
3. It writes the result, or removes the row when the result has none.

In the HIP's own scenario, the app sets `memo` to a value that is too long, and then changes `amount`. The first change is refused. The settle writes rdb's row, where `memo` is `start`, and puts the pending `amount` back. The target holds `start` and `2.00`, which is rdb plus the outbox.

The other cases:

- **A pending delete.** The result has no row, and the row is already gone.
- **A row rdb no longer has.** It is removed. A pending update of it is refused at its turn.
- **A refused insert.** As today: its row is removed, and pending changes to it are refused at their turn.
- **A row an observe touched.** The same write.

This is exact only if each pending change holds every column it wrote. With the plan's capture of changed values, it is not. A first update sets `a` and `c`. A second sets `a` to the same value, and `b`, so its captured change holds only `b`. When the first is refused because of `c`, the replay loses the `a` that the second named. §5 closes this.

**Change.** In *The settle moves the checkpoint* and *Held-back reverts*, a revert and the rewrite of a row an observe touched write rdb's row at C ∪ I with the row's pending changes on top. Then:

- the invariant loses its exception, and *Why it is sound* loses its held rows in steps 2 and 3;
- the list of held rows goes, with `rdb_held_rows`, the IndexedDB records, memory's set, `heldRows()` and `RowAction.release`;
- projection's two early returns stay as they are;
- the settle's step 5 no longer holds anything back;
- the held-row tests go, and the consistency test loses its mask (§14).

## 5. An update is one change: it captures the columns it names, and is refused whole

**Refused whole.** The HIP tries an update whole and then submits each column on its own (*Ingestion at the checkpoint*). F1 shows what that gives: a row that mixes the old `holder` with the new `role`, in rdb and in the target, which no one wrote. HIP-20 chose failure by column because SQLite's capture had no unit larger than a column (line 131: "as SQLite's per-column capture already gives"). HIP-29 makes a row's update one captured change, so that reason is gone. Decision 3: rdb accepts the update, or it is refused and reverted as a whole.

**The columns it names.** *Updates on SQLite* captures the columns whose value changed. Decision 4 captures the columns the statement names. The reason is the replay of §4: a change has to hold everything its statement wrote. On SQLite only `UPDATE OF <column>` triggers see which columns a statement names, so the per-column triggers come back, now filling one outbox row per updated row (C1 to C6). That costs about 5 µs per update, against 1 µs for the plan's single trigger (T1, T3).

What follows from it:

- **A named column is a write.** C3: an update that names a column without changing it is captured. In rdb it wins over a clear made concurrently on another device. HIP-20's *Unchanged columns* guards against that for a form that saves every field. Under decision 4, that comparison becomes a mode of the write library.
- **Memory.** `localUpdate` captures the columns it is given, without comparing.
- **IndexedDB (open).** A `put` passes a whole record, so the facade has to keep comparing with the stored record. To keep the replay of §4 exact, it should also keep a column that a pending change of the row writes. The write library has the same need in its comparing mode, and it needs a way to tell IndexedDB's capture which columns it names.
- **Columns that can't be written (open).** The planner refuses an update that carries a readonly column (`rdb_adapter/src/ingest.ts:361-362`, `:387-388`, `:414-415`), and HIP-20 has it refuse one that carries the author column. Today neither is captured when its value is unchanged. With decision 4, a statement that names every column would be refused whole. So ingestion has to set aside a readonly or author column that holds the row's own value, and refuse only a different one. HIP-20's *A row spread into an update* has this rule for the id and the author. It needs to cover readonly columns, and to move from the comparison to ingestion.

**Change.**

- *Ingestion at the checkpoint*: "An update is tried whole" loses its second sentence. The planned op's `fallback`, the per-column failure and the clause of *Crash replay* about falling back the same way go.
- *Updates on SQLite*: one outbox row per updated row, with every column the statement names. The triggers are those of C1 to C6. The reason given there for one change per update stays.
- *HIP-20*: the paragraph that says an update still fails column by column changes, with the HIP-20 lines in §13.
- State the rule for readonly and author columns, and what IndexedDB captures, once the two open points are decided.

## 6. Entries are found by a hash recorded in the outbox row

The HIP has three mechanisms so that a replay after a crash appends nothing twice:

- **Reserved identities.** Ingestion saves the identities it mints, in a transaction before its first append. The mappings get a `reserved` status, which `rowHash` and `localId` skip.
- **A lookup before each append,** with two methods: by computed hash for an anonymous entry, and among the tip's children, by payload, for a signed one.
- **The same entries.** A replay has to build exactly the entries of the crashed pass. That fails when the writer key or a group's config changes in between, and the replay then appends a second chain (*Crash replay*, last paragraph).

Decision 6 replaces all three. Ingestion builds and signs an entry, writes its hash into the change's outbox row, and then appends. A replay looks at each outbox row:

| The outbox row | rdb | The replay |
|---|---|---|
| Has a hash | Holds the entry | Takes the entry as appended. For an insert, it reads the uuid and the row hash from the entry |
| Has a hash | Doesn't hold the entry | The crash came between the two writes. It builds the entry again and writes the new hash over the old one |
| Has no hash | | It handles the change as any pass does |

Nothing has to be rebuilt the same way, so a changed writer key no longer adds a second chain.

**An entry rdb already holds is taken.** The same test covers the one case the HIP's lookup had beyond a replay. Two devices at one checkpoint can make the same anonymous update, and the two entries then have one hash. After building an entry, ingestion asks whether rdb holds that hash, and takes the entry if so. Only an anonymous entry can meet this, since a signature is random (`crypto/src/signing.ts:53`).

**One rule the replay needs (decision 8).** A pass handles the outbox in order. So every change before the last appended one was already handled by the crashed pass, and one that has no entry was refused. The replay keeps it refused, even if rdb would now accept it. An example with three changes:

1. The crashed pass appends `c1`, refuses `c2`, appends `c3` after `c1`, and crashes before the settle.
2. The replay finds the entries of `c1` and `c3` through their recorded hashes.
3. `c2` comes before `c3` and has no entry, so the replay refuses it.

Without the rule, a replay under another writer key could append `c2` after `c1`, beside `c3`. If both write the same column, the larger hash wins. That can be `c2`, while the target shows `c3`, the later write. In the usual case the rule costs nothing: the replay validates `c2` at the same position, rdb refuses it again, and the failure gets its reason. Only when rdb would accept it does the replay refuse on its own, with a reason that says so.

**The uuid (decision 7).** Checked at each point where a pass can crash:

- **Before the hash is written, or between that and the append.** No entry exists, so a new uuid is fine. No later change that references the row was reached yet.
- **After the append.** The replay reads the uuid and the row hash from the entry it finds.
- **A refused insert.** Nothing was appended and no event stored.

So no identity is saved before the append. The mapping of an insert is written by the settle, `active` or `ingestion_failure`. Until then the row has no mapping, so `rowHash` and `localId` have nothing to hide, and `reserved` isn't needed.

**What it needs.**

- **A split in rdb.** `bundle` signs, validates and appends in one call, and returns the hash at the end (`rdb/src/rtable_group/group.ts:1061-1085`). `observe` does the same (`:1020-1045`). Each needs a first step that builds, signs and validates, and a second that appends. The DAG already computes a hash without appending (`computeEntryHash`, `dag/src/dag.ts:20`, `:115`).
- **A target write per entry,** in place of one before the first append. Not measured. The case to measure is a pass of many inserts on IndexedDB, where each write is a transaction.
- **Observes have no outbox row.** A signed observe written just before a crash can be written again by the replay. The two name the same version, and the second changes nothing. Accepted in decision 6.
- **A power failure** that loses the recorded hash and keeps the append makes the replay append the change again. Accepted in decision 6.

**Change.**

- *Reserved identities* goes: the status, the rule for `rowHash` and `localId`, and `reserveMint`. The settle's step 3 writes the mappings of the pass's inserts.
- *Ingestion at the checkpoint*: "Entries rdb already holds" becomes the table above.
- *Crash replay* is rewritten around the recorded hash, with the rule of decision 8. Its last paragraph, on a second chain, goes.
- *Why it is sound*, the four crash states: "the next pass reuses any identities that were saved" goes, and "the replay makes the same chain" becomes "the replay finds the entries by their recorded hashes".
- *Considered and not taken*: "Record each entry before appending it" is now what the HIP does. "Derive a new row's uuid from its change" and "Tell a reserved mapping by its pending insert" have nothing left to answer.
- The header's modules gain rdb.
- The `sqlite_sequence` fix stays. Its reason changes: without it, the mapping that the settle writes can meet an id that a native insert took.

## 7. A moved checkpoint halts in both places

*One projector per target* gives a moved checkpoint two behaviours: at the settle the projection halts, and at the apply it is that cycle's error and the next cycle runs. A moved checkpoint means a second projector, which is not supported. A projection that carries on after the apply's error still has that second projector, and can meet it at the settle next, after it has appended. Decision 10: halt in both places.

**Change.** *One projector per target* states one behaviour. `CheckpointMovedError` needs no `stage`, and one of the two tests goes.

## 8. An entry rdb accepts can be void where it stands

*Why it is sound*, step 2, takes an accepted change to be in rdb at C ∪ I. A1 shows one that is not. rdb accepts Bob's insert, the insert is void in every version that holds it, and rdb never has the row. No delta reports it, because its verdict never changes. Today the row stays in the target for good (A3), and under the HIP it would too.

**Why it happens.** rdb checks a row op twice, and the two checks read a bound group differently:

| Check on a row op | When it is appended | When its verdict is read |
|---|---|---|
| Schema, types, constraints | Yes | No |
| Signature and author key | Yes | No |
| Row id never used (insert) | Yes | No |
| Row is live (update, delete) | Yes | Yes |
| `ALLOW` rules | Yes | Yes |
| Foreign keys reach live rows | Yes | Yes |

For the last three, reads of the op's own group agree in both columns (R1). Reads of a bound group don't. The first check takes the version every observe gives, void ones included. The verdict takes the version the live observes give. This is deliberate. HIP-14 §2.3 gives the reason: a revoke its author has seen binds them, whatever the verdict of the observe that imported it. HIP-14 also says that an author has to pass both checks (§4), and nothing does that for row ops today. It takes a binding with a `canObserve` gate, since only a gated observe can be void. It applies to rules (A1) and to foreign keys (A2).

**The fix (decisions 1 and 2).**

- **Refuse.** For a local author, rdb also requires the op to pass with the bound groups read as the verdict reads them. An op that fails is refused before it is appended. In the adapter it is then one more refusal: a failure event, a revert, and the tip doesn't move. Entries received through sync are checked as today.
- **Only when the readings differ.** Running both checks for every op would cost a verdict evaluation per op. The two checks differ only in the version of the bound group they read, and rdb gives both versions. G1 to G4: the insert is accepted and void exactly when the two differ. So ingestion compares them, for each gated binding, at the checkpoint, and again after each observe the pass appends to that group. A row op changes neither reading (G5, and R1 for verdicts in general). A group without a gated binding skips all of it.
- **The same for ingestion's own observes.** One that would be void at its own position is not appended. `evaluateObserveGate` already takes either reading (`rdb/src/rtable_group/group.ts:708`).

**What stays.** A refused write tells the user nothing about the cause: the target shows the grant in K's tables, and G reads K without it. Only a new live observe ends that, and the writer may not be allowed to write one. Decision 12 defers this.

**Change.**

- **rdb.** The step that builds and validates a local entry (§6) takes an option to also validate with bound groups read through the live observes. The four places where row-op validation reads a bound group are `rdb/src/rtable_group/validate_ops.ts:390`, `:449`, `:514` and `:703`.
- *Ingestion at the checkpoint*: the comparison of the two readings, and when the option is set.
- *Why it is sound*, step 2: an accepted entry is live at its own position, by this rule.
- *Tests*: A1 and A2 on the three targets, expecting a failure event and a revert. G2 and G4, expecting the write to be accepted.

## 9. A write can be checked against another version of a bound group than the target shows

The target shows a bound group B at B's checkpoint. A write of the group A that binds it is checked against B at the version A's checkpoint observes. Nothing ties the two together. The app can then show a post, and have a comment on it refused because A doesn't observe the post yet. Appending at the checkpoint makes one form of this more likely than it is today, since today's append at the frontier finds an observe as soon as it reaches rdb. This is reasoned from the code and was not run.

An observe written before A's first entry would close that form. Decision 11 declines it: the local writer may not have the right to observe, and the observe would be written because of remote entries. HIP-30 records the problem, why it can't be closed completely, and a design for later.

**Change.** *What changes for apps* gains a line: a write that reads another group is checked against the version its own group observes, which can differ from what the target shows of that group, with a pointer to HIP-30.

## 10. The wait stays

*No projection over a pending change* holds remote changes back while any local change is pending. I looked at three ways out:

- **Dropping the second ingestion in a cycle.** The refusal and the next cycle already cover a change captured during ingestion. Not decided on its own, so the rule stays.
- **Smaller batches.** They make each wait shorter and don't remove it.
- **Applying over pending changes,** with ingestion appending each change at the checkpoint it was written against. It held against rdb with a simulated target. Its price is a refusal where the wait gives a void event. HIP-31 records it.

Decision 13 keeps the HIP as it is.

**Change.** Two pointers to HIP-31: from the open question, and from the entry of *Considered and not taken* that starts "Record with each captured change the checkpoint it was written at". HIP-31 needs that record, and takes it at the apply instead of at capture.

## 11. A pass after these changes

For reference, the order that §4 to §8 give.

1. For each gated binding of a group with pending changes, compare the two readings at the checkpoint.
2. For each change, in commit order:
   1. Write the observes that are due before its group's entry. After one, compare that binding's readings again.
   2. If the outbox row has a hash and rdb holds the entry, take it and go on.
   3. Build, sign and validate the entry. A refusal is recorded for the settle.
   4. If rdb already holds an entry with that hash, take it and go on.
   5. Write the entry's hash into the outbox row.
   6. Append.
3. Write the observes still due.
4. Settle, in one transaction:
   1. check each group's checkpoint;
   2. remove the handled changes from the outbox, and read what is left;
   3. write the mappings of the pass's inserts;
   4. remove the rows of refused inserts;
   5. write each reverted row, and each row an observe touched, as rdb's row with its pending changes on top;
   6. write the author and json columns;
   7. store the op events;
   8. set the new checkpoints.

## 12. The plan

| In the plan | What changes |
|---|---|
| Decision 1, second bullet | An update is refused whole (§5) |
| Decision 1, third bullet, and detail 3 | One outbox row per updated row with the columns the statement names, by per-column triggers (§5). The checks of detail 3 are replaced by C1 to C6 |
| Decisions 2 and 3, detail 2 | Replaced by the recorded hash (§6) |
| Detail 1 | No `fallback` on a planned update |
| Detail 4 | One behaviour, no `stage` (§7) |
| Detail 5 | Goes (§4) |
| Detail 6 | The steps of §11. Its steps 4 and 6 change, and nothing is held |
| Detail 7 | Replaced by the three cases of §6 |
| Detail 11 | Stays, with the reason in §6 |
| Detail 12 | The check compares the target with a full projection at the checkpoint plus the outbox applied on top, with no mask |
| *Background* | `findForkPosition` and the `fallback` field are no longer reused |
| P2 | It is called additive, with the orchestrator unchanged, but detail 2 removes the orchestrator's re-plan. With `reserveMint` gone, the phase has to be cut again anyway |
| Phases | A new first phase in rdb: the split of `bundle` and `observe` (§6), and the second validation (§8) |
| *Risks* | "Replay determinism" goes. The fork query and the hash lookup go from the costs, and a target write per entry comes in |

## 13. HIP-20's text

- **Lines 129 and 311, *Unchanged columns* and "Changes are compared by value".** The comparison becomes a mode of the write library. Capture on SQLite and memory records what is named.
- **Line 130, *A row spread into an update*.** The id and author columns are set aside when they hold the row's own value. This has to cover readonly columns, and hold when no comparison runs (§5, open).
- **Line 131, *Failures by column*,** line 208 and line 341. An update lands or is refused as a whole.
- **Lines 200 and 201.** They describe a replay that signs the insert again and reuses the identity its pass saved. A replay now reads the signer and the identity from the entry it finds.
- **Line 322.** "A crash replay is recognized by the batch its pass recorded" was already stale after the plan's decision 1. It becomes: by the hash recorded in the change's outbox row.

## 14. Tests

Against the HIP's *Tests* and the plan's cases:

- **Go:** the held-row cases (IN41, IN44, SQL25, the holds of ADPTI-10), the reservation case (IN40), the lookup cases (ADPTI22 to 24), and the apply-stage moved checkpoint (one of RDBPROJ16 to 20).
- **Change:**
  - IN52: a multi-column update with one bad column is refused whole, and the row reads as before the update.
  - SQL27: one outbox row with every named column, changed or not.
  - IN46 and IN47: the replay finds its entries by the recorded hash.
  - IN51: the second device takes the entry rdb already holds, found by the hash of the entry it built.
  - The consistency check: no mask.
- **Add:**
  - A refused change with a later pending change to the same row: after the settle, the row is rdb's row with the pending change on top, with no projection in between. The same for a row an observe touched.
  - A first update sets `a` and `c`, a second names `a` and `b`, and the first is refused: the row keeps the `a` that the second named.
  - A crash between writing the hash and the append: the replay builds the entry again and appends it once.
  - A replay under another writer key: the entries already appended are taken, and a change the crashed pass refused stays refused (decision 8).
  - A crash after a signed observe: the replay may write a second one, and the target ends the same.
  - A1 and A2, with G2 and G4 (§8).
  - An `UPDATE` that names a readonly column with the row's own value, once that rule is decided.

## 15. Smaller points

- **The summary.** Two of its sentences describe mechanisms that go: "a revert that a pending change holds back is kept until projection applies it", and a replay that finds its entries "with nothing recorded beforehand".
- ***What changes for apps*, *Validity*.** It gives one direction: a remote change the app hadn't seen can void a write. The other direction is missing: a grant that reached rdb and not the target no longer validates a write, where today's append at the frontier accepts it.
- **The delta is computed before the apply's check.** A refused apply throws its delta away, and the delta's cost grows with the backlog. Catching up after a long time offline, in an app that keeps writing, is the case to measure for the open question.
- **The recorded hash must not wake the projection.** On SQLite it doesn't: the watcher fires only when the outbox's highest id grows (`rdb_adapter_sqlite/src/sqlite_target.ts:262-275`). I didn't check IndexedDB and memory.

## 16. Not checked

- The transaction details of the settle on IndexedDB and SQLite.
- The plan's test list, line by line, beyond §14.
- The cost of a target write per entry (§6), and of the comparison of the two readings (§8).
- §9, which is reasoned from the code.
- §4's revert and §6's replay are designs. Nothing of them is built, so what was checked is the code they rest on.

## Outside this HIP

- **The gap of §8 is in today's code** (A3), and not only in the adapter. The runtime's writes pass the same single check. For observes, HIP-14 §4 specifies a second check by the author and defers it (`rdb_runtime/src/session/ref_auto_update.ts:161` still checks one reading). For row ops nothing is specified.
- **The partial update of §5 is in today's code** (F1), through `updateMerge`'s fallback.
