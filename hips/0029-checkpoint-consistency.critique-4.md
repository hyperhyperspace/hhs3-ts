# Fourth critique of HIP-29: Checkpoint consistency

A review of [HIP-29](0029-checkpoint-consistency.md) as revised on 2026-11-21 after [the third response](0029-checkpoint-consistency.response-3.md), of its plan, `plans/checkpoint-consistency.md`, and of [HIP-20](0020-unified-write-api.md) where that revision changed it. The points were first given in chat on 2026-11-21 and discussed the same day. The decisions taken there are in §3, and every finding below is written with its decision.

Paths are relative to `modules/`. The checks are labeled W1 to W13, T5 and T6, S1 to S4 and N1 to N3, continuing the third critique's, and §1 lists them.

## Verdict

**Does the revision resolve the third critique?** Yes. Every section is taken as it was decided, and the documents agree with each other: each case in HIP-29's *Tests* has a case in the plan, and the lines of HIP-20 that the third critique listed are rewritten.

**Is the response right where it differs?** Yes, in each point (§2). Its linear replay is the right order for a replay that resumes a pass. §7 replaces resuming, and keeps what the response observed about observes.

**Is anything still wrong?** One gap. After a crash, the effects of the observes that the crashed pass wrote are lost: no void event is stored, and a voided row stays in the target for good (§4). The cause is general: the settle works from results that the pass keeps in memory. The rule decided in discussion removes the cause: what a pass does at its end depends only on what was stored while it ran (§5).

**Does the HIP still have unnecessary complexity?** In the two places the gap led to, and both get simpler:

- the settle computes one delta per group, in place of a delta for each observe and the rewrite that those deltas needed (§4);
- a crashed pass is finished by the next cycle, in place of a replay that resumes it and rebuilds what it had in memory (§7).

In exchange, a refused change records its failure in its outbox row, as an accepted one records its entry's hash (§6).

| Item | In HIP-29 | In the plan |
|---|---|---|
| §4. One delta gives the observes' effects | *The settle moves the checkpoint*, *Why it is sound*, *Changes* 2, *Tests*, *Considered and not taken* | detail 6, P6 |
| §5. The end of a pass reads only what is stored | *Ingestion at the checkpoint*, *The settle moves the checkpoint* | decision 6, details 3 and 7 |
| §6. A refusal is recorded when it happens | *Ingestion at the checkpoint*, *Crash replay*, *Changes* 2 to 4 | decisions 6 and 7, details 3, 6 and 7, P4 |
| §7. A crashed pass is finished, not resumed | the summary, *Crash replay*, *Why it is sound*, *Changes* 2, *Tests* | decision 7, detail 7, P5 |
| §8. A named column stays a write | *Considered and not taken* | none |

## 1. How the claims were checked

**Read.** HIP-29, the plan, the third response, and HIP-20's *Authorship*, *Changes* and open questions. In the code: `rdb/src/rtable_group/delta.ts`, `op_delta.ts` and `group.ts:1086-1131`; `dag/src/dag.ts:98-112` and `dag/src/store/mem_dag_store.ts:24-34`; `mvt/src/refs.ts:409-425`; `rdb_adapter/src/ingest.ts`, `ingest_orchestrator.ts:415-430` and `:511-575`, `project.ts:80-101` and `types.ts:190-256`, `:356-362`; the capture gate and the event table of `rdb_adapter_sqlite/src/sqlite_target.ts` (`:466`, `:503`, `:543`, `:1383-1400`); `rdb_adapter/src/memory_target.ts:206-212` and `rdb_adapter_idb/src/idb_target.ts:124-129`.

**Probes.** Scratch scripts, not in the repo, in `/private/tmp/claude-501/-Users-santi-Hyper-hhs3-ts/e4a1050f-4052-419b-8a25-72fef9ed73c5/scratchpad/`. The ones that load rdb run from `modules/rdb` or `modules/rdb_adapter` with `node --import ../../register.mjs <file>`. The SQLite ones run with plain `node`. All were run on 2026-11-21.

*Capture fills in the author, on SQLite* (`author_fill_probe.mjs`). One `AFTER INSERT` trigger sets `applying`, writes the writer key's id into an empty author column when the group's mode is `on`, clears `applying`, and records the insert from the stored row. Updates are captured as decided on 2026-11-20. The writer key is 7. Every case gave the same result with `recursive_triggers` off and on:

| | Statement | Result |
|---|---|---|
| W1 | An insert that leaves the author out | The row and the captured insert hold 7. `changes` is 1, and `lastInsertRowid` is the row's |
| W2 | An insert that names author 3 | 3 is kept |
| W3 | An insert that names the author as `NULL` | Filled with 7 |
| W4 | The mode is `off` | The column stays empty |
| W5 | No writer key is recorded | The column stays empty |
| W6 | One statement that inserts three rows | Three filled rows, and three captured inserts |
| W7 | `INSERT … RETURNING id, author` | It returns `NULL` for the author, while the row holds 7 |
| W8 | An insert that is rolled back | No row, and no outbox row |
| W9 | A statement of two rows whose second row fails a `CHECK` | No row, no outbox row, and `applying` is 0 |
| W10 | An insert made with `applying` set, as the adapter writes | Not filled and not captured, and the flag is still set |
| W11 | An update, an `INSERT OR REPLACE` and an update, on a row authored by 3 | The row holds 7. The outbox holds the update, an insert with author 7, and the update |
| W12 | An `INSERT … ON CONFLICT DO UPDATE` that meets the conflict | One captured update, and the author unchanged |
| W13 | Read a row just inserted, and write every column back | One captured update with every column, the author at 7 |

*What the fill costs* (`insert_fill_cost_probe.mjs`). Inserts of one row each, the median of twelve rounds of 2,000. An insert with no trigger takes 0.33 µs:

| | Capture | Added per insert |
|---|---|---|
| T5 | Today's insert trigger | 0.98 µs |
| T6 | The filling trigger | 1.78 µs |

*One delta from the checkpoint to the end of the chain* (`start_to_end_delta_probe.mjs`), on rdb alone. A group G binds a users group K, and a note in G needs its author to hold a grant in K. The checkpoint C holds a note `n0` by Bob. A pass with Bob as its writer appends a note `n1`, then an observe of K, then a row that needs no grant:

| | The observe brings in | Delta | Verdict changes | Note rows changed |
|---|---|---|---|---|
| S1 | An entry of K that is concurrent with Bob's grant and voids it | One, from C to the end of the chain | `n0` void, `n1` void, each naming its row | `n0` removed |
| S2 | The same | The observe's own, from `n1` to the observe | The same two | `n0` and `n1` removed |
| S3 | A grant the writer just gave itself in K, with the note after the observe | One, from C to the end of the chain | None | The note added |
| S4 | A removal of Bob's grant that comes after the grant in K | One, from C to the end of the chain | None. Both notes are live at the end | `n1` added |

In S1 and S4, the bounded and the full delta strategy give the same. In S1 and S3, the pass's own note read from C, which doesn't hold it, is live: rdb reads an entry's verdict at the entry's own position, as seen from the version given (`rdb/src/rtable_group/group.ts:1086-1098`).

*A named foreign-key column* (`named_fk_probe.mjs`, `dangling_fk_target_probe.mjs`). A comment references a post, and the post is then deleted. rdb keeps the comment, with its reference:

| | Update of the comment | Result |
|---|---|---|
| N1 | Names `body` | Accepted |
| N2 | Names `body`, and `post` with the row's own value | Refused: the foreign key points to a row that isn't live |
| N3 | N2 through today's memory target, after the delete was projected | The target still shows the post's id in the comment. Today's capture leaves the unchanged column out, and the update is accepted |

## 2. Claims that hold up

- **Observes are looked up by hash** (the response's first refinement). `Dag.append` builds and appends without checking for the entry (`dag/src/dag.ts:98-112`). The in-memory store's append removes the parents from the frontier and adds the hash (`dag/src/store/mem_dag_store.ts:24-34`), so a second append of a held entry puts it back in the frontier.
- **A second signed observe changes nothing.** Ingestion signs an observe only when the binding's gate reads `$author` (`rdb_adapter/src/ingest_orchestrator.ts:550`), and rdb takes an observe of a version equal to the one already observed (`mvt/src/refs.ts:409-425`).
- **Durable commits** (the third refinement). Dropping the power-failure case is right: the settle relies on the same.
- **Capture fills in the author.** W1 to W13, at about 0.8 µs more per insert than today's trigger (T5, T6). It removes the settle's author write, and the row reads the same before and after ingestion. HIP-20's *Authorship* table agrees with the refusal of an empty author column where the mode is `on`.
- **An IndexedDB `put` captures every column.** Accepted as the response has it (decision 14).
- **Recording into an outbox row wakes nothing**, on memory and IndexedDB as on SQLite (`rdb_adapter/src/memory_target.ts:206-212`, `rdb_adapter_idb/src/idb_target.ts:124-129`). §6 adds a second write of that kind.
- **The linear replay** (the second refinement). For a replay that resumes a pass it is the right order, and simpler than the third critique's. What it observed carries over to §7: an observe in the past of a recorded entry is part of the chain, and only an observe after a group's last recorded entry is written again.

## 3. Decisions taken in discussion

Taken on 2026-11-21. The numbers continue the third critique's.

| | Decision | Section |
|---|---|---|
| 14 | A column that a write names is written, changed or not, and ingestion never leaves one out because it holds rdb's value. The readonly and author columns of *Columns that can't be written* are the only exception | §8 |
| 15 | Foreign-key columns get no exception | §8 |
| 16 | What a pass does at its end depends only on what was stored while it ran: the outbox rows, the checkpoints, and rdb | §5 |
| 17 | The settle takes the effects of the pass's observes from one delta per group, from the checkpoint to the end of the chain. It stores the delta's verdict changes as events, and rewrites the rows of those ops | §4 |
| 18 | A refusal is recorded in the change's outbox row when it happens. The settle moves it to the event table | §6 |
| 19 | A pass can end after any change. A crashed pass is finished by the next cycle, which then handles the rest of the outbox in a new pass | §7 |

Decision 18 replaces the third critique's decision 8, under which a change with no entry stayed refused by inference. Decision 19 replaces the replay that resumes, in either order.

## 4. The settle takes the observes' effects from one delta

**What the documents have.** A delta for each observe, computed while the pass runs:

- *The settle moves the checkpoint*: "For each observe the pass appended, the settle computes the delta from the tip before the observe to the observe, and stores its op events."
- The plan's P6: "Per observe step, the orchestrator computes `computeDelta(before, after)`", and its events and touched rows "go into the settle".
- Such a delta gives a row as rdb has it at the observe, which a later write of the same pass may have changed. So the rows are written by a second rule: "the settle writes every row that any of the pass's observes touched as a revert does, from rdb at C ∪ I".

**The gap.** The deltas are results in the pass's memory, and a crash loses them. The outbox holds a change in K and then two changes in G, which binds K:

1. The pass appends K's entry, writes G's observe of K, appends G's first entry, and crashes.
2. The observe brought in an entry of K that voids a row of G's checkpoint, as in S1.
3. The replay takes the two entries by their recorded hashes, and carries on with G's second change. The observe is in the past of G's first entry, so the replay doesn't write it, and nothing computes its delta again.
4. The settle stores no void event and leaves the row. It moves G's checkpoint past the observe, so no later projection reports the void either.

The target keeps a row that rdb doesn't have, which is what this HIP removes.

**One delta instead.** At the end of a pass, for each group, rdb computes the delta from the checkpoint C to the end of the group's chain. The settle takes two things from it:

- **The events.** The delta's verdict changes become void and reinstate events, as in projection (`opVerdictEvents`, `rdb_adapter/src/project.ts:85-101`).
- **The rows.** A verdict change names the rows of its op (`rowId`, or `writes` for a bundle, `rdb/src/rtable_group/op_delta.ts:37-50`). The settle writes each as a revert does: rdb's row at the end of the chain, with the row's pending changes on top.

It needs nothing that the pass remembers: C is stored in the target, and the chain is in rdb. S1 gives both effects of the gap's observe, the void of `n0` and of the pass's own `n1`.

- **Not the delta's row changes.** They can't be the list of rows to write:
  - They hold the pass's own writes, which the target already has (`n1` in S4, the note in S3). Writing those would be the rewrite of every row the pass touched, which *Considered and not taken* declines.
  - They miss an own insert that an observe voided. rdb has that row at neither end, so the delta has no row change for it (`n1` in S1). This is the first scenario of *What happens today* again.
- **Why the verdict changes are enough.** After C the chain holds row ops and observes only. A row op changes no row it doesn't write (R1 of the third critique). So any other row that differs between C and the end of the chain belongs to an op whose verdict an observe changed.
- **The pass's own entries.** rdb reads an own entry's verdict from C as it stands at its own position. So an entry that was live when appended and is void at the end shows as a verdict change (S1), and an entry that needed an observe of the same pass shows none (S3).
- **No rule for the middle of a chain.** The delta ends where the reverts read rdb, so a later write of the same pass is in it.
- **Net effects.** An entry that one observe voids and a later observe of the same pass reinstates gives no event. Its row reads the same before and after.
- **Only when the chain holds an observe.** A chain of row ops changes no verdict, so its group needs no delta. The end reads this from the chain, not from the pass's memory: an accepted entry whose parent is neither the checkpoint nor the group's previous accepted entry follows an observe, and the end knows the observes it writes itself (§7).

**Change.**

- *The settle moves the checkpoint*: the bullets *The op events* and *The rows* become the rule above. Step 5 writes "each reverted row, and each row of an op whose verdict changed". Step 7 stores the failure events (§6) and the delta's events.
- *Why it is sound*, 2: "each row an observe touched" becomes "each row of an op whose verdict the pass's observes changed".
- *Changes* 2, *The settle*: `IngestSettle` carries the rows of the ops whose verdict changed and the delta's events, in place of "the rows the observes touched" and "each observe's op events".
- *Considered and not taken*: the entry "Leave a group whose pass only observed at its old checkpoint" ends "One delta from the checkpoint to the end of the chain covers every case". A new entry: a delta for each observe, computed as the pass appends it. It is lost by a crash, and needs a second rule for the rows.
- The plan's P6 computes one delta per group at the end. Detail 6 follows *Changes* 2.

## 5. The end of a pass reads only what is stored

The rule of decision 16: what a pass does at its end depends only on what was stored while it ran. The end is then the same computation whoever runs it, the process that ran the pass or the next one after a crash.

| What the end needs | In the revision | With this rule |
|---|---|---|
| Which changes the pass handled | The pass's memory. A replay rebuilds it | Each handled change's outbox row holds what became of it |
| The accepted entries, and an insert's uuid and row hash | The recorded hash, then rdb's entry | The same |
| The refusals and their reasons | The pass's memory. A replay infers a refusal, and validates again for the reason | The failure event in the outbox row (§6) |
| The effects of the observes | The pass's memory, one delta per observe | One delta from the stored checkpoint (§4) |
| The end of each group's chain | The pass's memory | The group's last accepted entry, then the observes the end writes |
| The observes still due | The pass's memory (`dirty`, `rdb_adapter/src/ingest_orchestrator.ts:513`) | Read from rdb (§7) |

**What became of a change.** Every change the pass handled has one of three records in its outbox row:

- **The hash of its entry**, written before the append, as decided.
- **Its failure event**, written when it is refused (§6).
- **A mark that it had nothing to write.** An update whose named columns were all set aside, such as a `put` of an unchanged record in a table whose columns are all readonly, makes no entry and is no failure.

A row with none of these was not handled. Neither was a row whose hash rdb doesn't hold: the crash came between the two writes.

**The hash before the lookup.** HIP-29 records the hash and then looks the entry up. The plan's detail 7 has the other order: "4. if rdb holds the hash, take the entry; 5. otherwise `recordEntryHash`, then `appendPrepared`". An entry taken because rdb already holds it then has no record, and after a crash its change looks unhandled or refused. The hash is recorded first in every case.

**Change.**

- *Ingestion at the checkpoint* states the three records beside *The hash first*, and that the end of a pass reads only the outbox rows, the checkpoints and rdb.
- The plan's detail 7 records before it looks up. Detail 3's outbox column holds any of the three records, and the change source's `recordEntryHash` becomes a write of any of them.

## 6. A refusal is recorded when it happens

**Today, and in the revision.** A refusal builds its failure event and keeps it in a list in memory (`recordFailure`, `rdb_adapter/src/ingest_orchestrator.ts:417-430`). The event table, `rdb_op_events`, is durable and append-only, and reading it by cursor removes nothing (`rdb_adapter/src/types.ts:190-197`, `:356-362`, `rdb_adapter_sqlite/src/sqlite_target.ts:466`). But a failure reaches it only at the settle.

**After a crash, the revision infers.** *Refused before the crash*: "A change before that last one that has no entry was refused by the crashed pass, and the replay keeps it refused. It validates the change again at its position for the failure's reason, and says so when rdb would now accept it." Three things follow from inferring:

- The reason is made a second time, and can differ from the one that refused the change.
- A change with no entry isn't always a refused one: an entry taken with no hash recorded, and an update with nothing to write (§5).
- The replay has to find the last recorded entry before it can tell what the changes before it are.

**Recorded instead.** When a change is refused, the pass writes its failure event into the change's outbox row. It is one target write, like the hash, and wakes nothing. The settle moves the event into the event table in the transaction that reverts the row.

- **Not straight into the event table.** The app would read the failure while the row still shows the refused write, until the pass ends. In the outbox row, the app gets the failure and the reverted row together, as today, and the settle finds the refusal by the change it belongs to.
- **Every refusal.** rdb's refusals, and the planner's: a readonly or author column with another value, an empty author column where the mode is `on`, and the write that is void where it stands.
- **The cost.** A target write per refused change.

**Change.**

- *Ingestion at the checkpoint* gains the rule, with the records of §5.
- *Crash replay* loses *Refused before the crash* and its example of `c1`, `c2` and `c3`. With the refusal recorded, `c2` stays refused without the inference.
- *Changes* 2 to 4: the change source writes a failure event into an outbox row, and the outbox of each target holds it.
- The plan's decision 6 gains the record, and decision 7 loses "A change before that one with no entry stays refused". P4 implements the write on the three targets, and its case that recording signals no change covers it.

## 7. A crashed pass is finished, not resumed

**A pass can end after any change.** The settle already copes with changes left in the outbox: a revert reads "every change captured since the drain", and a cycle whose outbox filled up during ingestion ingests once more. So the changes a settle removes can be any leading part of the outbox. The revision doesn't use this after a crash.

**The revision resumes.** *Linear*: "the replay takes every entry up to the last change whose recorded entry rdb holds, and each group's tip is the last of these in that group. Their pasts hold the observes the crashed pass wrote before them. Then it carries on with the next change." To carry on, the replay has to rebuild what the crashed pass had in memory: the tips, the identities that later changes resolve through, the observes due, the refused changes, and the observes' deltas, which §4 found missing.

**Finished instead.** A crashed pass is a pass that stopped before its end. The next cycle runs that end first, on what the outbox recorded, and then handles the rest of the outbox in a new pass from the new checkpoints. The end is the same code that ends any pass:

1. **Read the outbox.** The handled changes are its leading rows that have a record (§5).
2. **Find each chain's end**: the group's last accepted entry.
3. **Write the observes still due** (below). Each becomes the end of its group's chain.
4. **Compute the deltas** (§4).
5. **Settle**, in one transaction.

In the scenario of §4, the next cycle finds K's change and G's first change recorded. It computes G's delta from its checkpoint to G's first entry, which holds the void. The settle stores the event, removes the row, removes the two outbox rows, and moves both checkpoints. A new pass then takes G's second change.

| The outbox row has | rdb | The next cycle |
|---|---|---|
| A hash | Holds the entry | Handled, accepted. An insert's uuid and row hash are read from the entry |
| A failure event | | Handled, refused |
| The mark of nothing to write | | Handled |
| A hash | Doesn't hold the entry | Not handled. The new pass builds the entry again and records the new hash over the old one |
| Nothing | | Not handled |

**The observes still due.** A pass writes, before it settles, the observes that its entries made due and that no later entry called for. The end after a crash has to do the same. Say a permission in K and its use in G are in the outbox, and the pass crashes after K's entry. If the end only settled, K's checkpoint would hold the permission, the new pass would have no entry of its own in K, and so it would write no observe. G's change would be refused.

- **Read from rdb.** For each group with an accepted entry, each group that binds it needs an observe when its chain doesn't observe that entry yet, and so on for the groups that bind those. The rules of *Other groups* apply as they are.
- **An observe the crashed pass wrote.** It has no outbox row. One in the past of an accepted entry is part of the chain. One after its group's last accepted entry is written again: an anonymous one comes out the same and is found by its hash, and a signed one becomes a second observe of the same version. The first one stays outside the checkpoint, and the next projection applies it, which changes nothing. This is the case accepted in the third critique's decision 6.
- **A crash during the end**, after its observes and before its settle, leaves the same state, and the next cycle runs the end again.

**What goes.**

- *Linear*, and the state a replay rebuilds to carry on.
- *Refused before the crash* (§6).
- The first step of the plan's order for each change, "a recorded hash that rdb holds: take the entry". A new pass never meets a handled change, since the end removed it.

**What stays.**

- The hash recorded before the append. It is how the end knows what was appended.
- The lookup by hash before any append, for an observe written again and for two devices that make the same anonymous update.
- *No identity is saved before the append*, *Observes* and *Durable commits*.

**Change.**

- The summary: "so a replay after a crash takes what was already appended" becomes that the next cycle settles what was already appended.
- *Crash replay* is rewritten around the end of a pass: the table above, the observes still due, and what stays.
- *Why it is sound*, the crash between the first append and the settle: the next cycle finishes the pass, and then handles the rest in a new one.
- *Changes* 2, *Ingestion*: "the replay of *Crash replay*" becomes the end of a pass, run first on what the outbox recorded.
- The plan's decision 7 and detail 7 follow. An insert's identity is still kept for the pass that appends it, and the settle reads it from the entry for the mapping.

## 8. A named column stays a write

Decided, with no change to the design.

- **What I proposed.** Capture stays as decided, and ingestion leaves out of the op every named column that holds rdb's value for the row. It would have covered N2, and a form that saves every field.
- **Why it is declined** (decision 14). An app may need the columns of a write to land together, and a write turned into a patch behind its back takes that away. And the state ingestion would compare with is not final: it is rdb at the checkpoint plus the chain, not what the write meets once concurrent entries arrive. An app that wants a patch names fewer columns, or gives HIP-20's writer `compare`.
- **Foreign keys** (decision 15). N2 is real, but nobody edits a row whose referenced row was deleted. No exception is added.

**Change.** One entry in *Considered and not taken*: leave out, at ingestion, the named columns that hold rdb's value, with the two reasons above.

## 9. A pass after these changes

For reference, the order that §4 to §7 give. It replaces §11 of the third critique.

1. If the outbox's first change has a record, run the end (step 4) on what is recorded.
2. For each gated binding of a group with pending changes, compare the two readings at the checkpoint.
3. For each change, in commit order:
   1. Write the observes that are due before its group's entry, each looked up by its hash first. After one, compare that binding's readings again.
   2. Build, sign and validate the entry. On a refusal, write the failure event into the outbox row, and go on.
   3. If there is nothing to write, mark the outbox row, and go on.
   4. Write the entry's hash into the outbox row.
   5. If rdb already holds the entry, go on. Otherwise append it.
4. The end:
   1. Read the outbox. The handled changes are the leading rows with a record.
   2. Take each group's last accepted entry as the end of its chain.
   3. Write the observes still due.
   4. For each group whose chain holds an observe, compute the delta from the checkpoint to the end of the chain.
   5. Settle, in one transaction:
      1. check each group's checkpoint;
      2. remove the handled changes from the outbox, and read what is left;
      3. write the mappings of the handled inserts, and mark the accepted deletes;
      4. remove the rows of refused inserts;
      5. write each reverted row, and each row of an op whose verdict changed, as rdb's row with its pending changes on top;
      6. on SQLite, rewrite json text in canonical form;
      7. store the failure events from the outbox rows, and the delta's events;
      8. set the new checkpoints.

## 10. The plan

| In the plan | What changes |
|---|---|
| Decision 6 | Gains the failure event and the mark: every handled change has a record in its outbox row (§5, §6) |
| Decision 7 | Replaced: a crashed pass is finished by the next cycle (§7). "It assumes durable commits" stays |
| Detail 3 | The outbox holds a failure event as well as a hash |
| Detail 6 | `reverts` also carries the rows of the ops whose verdict changed. The events are the failure events read from the outbox rows and the delta's. `recordEntryHash` becomes a write of any record |
| Detail 7 | The order of §9. Its first step goes, the hash is recorded before the lookup, and "The replay" becomes the end of a pass |
| P4 | The record writes on the three targets |
| P5 | The crash cases change as in §11 |
| P6 | One delta per group at the end, in place of one per observe step (§4) |
| *Risks* | The costs gain a target write per refused change, and a delta for each group whose chain holds an observe |

## 11. Tests

Against HIP-29's *Tests* and the plan's cases:

- **Change:**
  - A crash after a chain, and in the middle of the walk (IN52): the next cycle first settles what was appended, with the checkpoint at the crashed pass's last entry, and then ingests the rest. Nothing is appended twice.
  - A replay under another writer key (IN54): the refused change stays refused, with the failure event the crashed pass recorded, and is not validated again.
  - A revocation that comes into view through the pass's observe (IN60): the entry that the observe brings in has to be concurrent, in the bound group, with what the voided entry read (S1). One that comes after it there voids nothing (S4).
  - An observe in the middle of a chain (IN61) stays as a case. It now holds by the one delta.
- **Add:**
  - A crash after an observe that voids a row of the checkpoint, with a later entry appended: after the next cycle the row is gone and the void event is stored. The same when the observe voids the pass's own earlier entry.
  - A crash after a refusal, with a later entry appended: the next cycle stores the failure event with its recorded reason and reverts the row.
  - A crash between a permission's entry and the observe that its use needs: the next cycle writes the observe when it finishes the pass, and the use is accepted.
  - A crash after the end's observes and before its settle: the next cycle settles, with the target ending the same.
  - An update with nothing to write leaves the outbox at the settle, also after a crash.
  - Two devices make the same anonymous update, and the second crashes after taking the entry: the next cycle finds the change handled.

## 12. Smaller points

Kept apart from the decisions above, as agreed. None changes the design.

- **The mode `off`.** The plan's *To confirm* says that only the writer record moves into P3. A group's mode `off` comes from the `autoAuthor` setting, which appears in no code under `modules`, `apps` or `packs`, and P3's case for the author fill covers `off` (IN40). The plan should say which: P3 writes only `on` and anonymous and `off` arrives with HIP-20's setting, or the setting lands in P3.
- **A table projected without an author column** (`authorColumn: false`, `rdb_adapter/src/names.ts:23-25`). There is nothing to fill or to read. One sentence should say that the refusal of an empty author column doesn't apply, and that its inserts are signed with the writer key as today.
- ***Capture*, SQLite.** "A `BEFORE UPDATE` trigger starts the row's outbox row". The plan's detail 3 and the checked triggers have it close the row's open outbox row, and the column triggers open the new one. As worded, `SET id = id` leaves an empty change, against the HIP's own case that an update naming only the id captures nothing.
- **HIP-20, line 212.** "HIP-29 removes both, so that a crash replay can make the same entries again" gives a reason that no longer holds. A crashed pass's entries are found by their recorded hashes.
- ***Durable commits*.** It states the assumption, and not what a lost commit does: a signed change is appended twice, which the third critique's decision 6 accepted. Neither target asks for durability today: SQLite sets no `synchronous` pragma, and IndexedDB no `durability` option.

## 13. Not checked

- The end of a pass (§5 to §7) is a design, and nothing of it is built. What was checked is rdb's delta (S1 to S4) and the code the design rests on.
- That every row an observe changes belongs to an op whose verdict changed (§4) is reasoned, and held in S1 to S4. The consistency check after every step, and the fuzzer, would find a miss.
- Reading the observes still due from rdb (§7) is reasoned from `propagate` and `flush` (`rdb_adapter/src/ingest_orchestrator.ts:520-560`).
- The cost of the delta at the end, and of the record writes.
- The transaction details of the settle on IndexedDB and SQLite.
