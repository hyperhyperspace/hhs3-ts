# HIP-29: Checkpoint consistency

- **Type:** design
- **Status:** implemented
- **Modules:** dag, rdb, rdb_adapter, rdb_adapter_sqlite, rdb_adapter_idb, rdb_adapter_test, rdb_adapter_test_gen, rdb_projection, rhost; the tests of rdb_react, rdb_tools and rdb_repl
- **Requires:** [20](0020-unified-write-api.md): its *Changes* 1 except the native insert over an existing local id, which lands with this HIP. Its writer record (*What the projection records in the target*) and its authorship setting (*Authorship*) land with this HIP too, in an earlier shape that HIP-20 changes before its writer (its *Changes*)
- **Superseded by:** —
- **Implemented in:** `3bae793`, `efa3d7e`
- **Code references:** to the tree at `e579e35`, which this HIP changes

## Summary

A projection target records a checkpoint for each group, the rdb version it was last projected to, and each projection pass applies the change from that checkpoint to rdb's frontier. That is right only while the target holds what rdb holds at the checkpoint, and local writes break it in two ways: ingestion appends them to rdb without moving the checkpoint, and projection writes over changes still waiting in the outbox. As a result, a row the app inserted stays in the target for good when another device deletes it before the next projection, a refused change can stay in the target, and an edit made while a projection runs disappears until the next cycle. Ingestion also records every local write as made after the remote changes that reached rdb but not the target, which the app never saw.

This HIP keeps each group's tables equal to rdb's rows at the checkpoint, with the outbox's changes on top. Four rules do it:
- ingestion appends each captured change as one entry, in a chain that starts at the checkpoint;
- the settle moves the checkpoint to the end of the chain, in the transaction that removes the changes from the outbox;
- projection never applies while a change is pending;
- a revert writes rdb's row with the row's pending changes on top.

A local write then counts as concurrent with the remote changes the app hadn't seen. rdb's rules for concurrent writes decide between them, and a write those changes make invalid is voided instead of refused. Projection stops applying a pass's own writes a second time, so a local write has to settle in the target as its op settles in rdb. For that, capture fills in the author column at once, and the settle rewrites SQLite's json text in canonical form.

The rest follows from the four rules. A change holds every column its write names, and an update lands or is refused whole. IndexedDB gains a `patch` that names only some fields, as an `UPDATE` does. Ingestion records in each change's outbox row what became of it: the hash of each entry it appends for the change, before the append, or the change's failure. No change is dropped. The end of a pass reads only what was stored, so after a crash the next cycle finishes the pass that crashed. The settle takes what the pass's observes changed from one delta per group. rdb refuses a local write that would be void where it stands. Ingestion stops bundling inserts and merging updates. Apps get explicit bundles in a later HIP.

## Why

### Background: projection, ingestion and checkpoints

- **Entries and versions.** rdb keeps each table group as a DAG of entries. An entry carries a row op (an insert, an update or a delete), a *bundle* of ops that apply together, or an *observe* (below). An entry is signed by its author's key, or *anonymous*. An entry's hash covers its payload, signature included, and its parents. A *version* is a set of entries and stands for them and everything before them. A group's *frontier* is its newest entries. Two entries are *concurrent* when neither is before the other.
- **Concurrent writes.** Between concurrent writes to a column, the one in the entry with the larger hash wins, except that a value wins over a concurrent `NULL`. An entry is checked against the entries before it when it is appended, and only against them. An entry that passed can later be *voided* by a concurrent one, for example when its author's permission was revoked concurrently, or when a row it references was deleted concurrently.
- **Bound groups.** A group can *bind* other groups, and its rules and foreign keys then read their rows. It reads a bound group at the version its own *observes* give: an observe is an entry that names a version of the bound group. A binding can have a gate, `canObserve`, that says who may observe, and an observe whose author lost that right concurrently is void. rdb checks an op reading the bound group through every observe before it, void ones included, and evaluates the op's verdict reading it through the live observes only (HIP-14 §2.3; `resolveObservedForeignVersion` and `resolveLiveObservedForeignVersion`, `rdb/src/rtable_group/group.ts:783`, `:794`). The two readings differ only while a gated observe is void.
- **Appends.** A table's `insert`, `update` and `delete`, and a group's `bundle` and `observe`, build an entry, sign it, validate it and append it in one call, and return its hash at the end (`rdb/src/rtable/rtable.ts:156-187`, `rdb/src/rtable_group/group.ts:1020-1045`, `:1061-1085`). The DAG checks that an entry's parents are there, but not the entry itself (`dag/src/dag.ts:98-112`). A second append of an entry puts it back into the frontier, even when it already has children, on every store (`dag/src/store/mem_dag_store.ts:24-33`, `dag_sql/src/sql_dag_store.ts:42-62`, `dag_idb/src/idb_dag_store.ts:45-59`).
- **Target.** A projection copies a replica's groups into a relational *target*: SQLite, IndexedDB or memory. Each projected row has a *local id*, specific to the device, and a *row hash*, rdb's id for the row. The target's sync table maps one to the other. Each mapping has a status: `active`, `deleted`, or `ingestion_failure` for an insert rdb refused (`rdb_adapter/src/types.ts:167-172`). HIP-20 adds `rowHash` and `localId` to the query source, which read the mapping for apps.
- **Checkpoint and projection pass.** For each group, the target stores a *checkpoint*, the version it was last projected to. A projection pass computes rdb's delta from the checkpoint to the group's frontier, and applies it together with the new checkpoint in one target transaction (`rdb_adapter/src/project.ts:130-163`). The transaction first checks that the stored checkpoint is still the one the delta started from, and throws `CheckpointMovedError` if not. The delta lists a row only if it went from live to not live or back, or if a value written to it changed (`rdb/src/rtable/delta.ts:125`).
- **Capture.** When an app writes a projected table, *capture* records the change in the target's *outbox*, one outbox for all of the target's groups, in commit order. A change holds the columns whose value changed. Memory and IndexedDB capture an update of a row as one change, and SQLite's triggers capture one change per changed column (`rdb_adapter_sqlite/src/sqlite_target.ts:566-577`).
- **Ingestion.** *Ingestion* drains the outbox, plans the changes into rdb ops, appends them, and then *settles*: in one target transaction, it removes the drained changes from the outbox, stores op events, reverts what rdb refused and updates mappings (`rdb_adapter/src/types.ts:296-355`). Today it also saves the uuids of new rows before its first append, so that a replay after a crash makes the same inserts (`ingest_orchestrator.ts:397-411`). It bundles inserts linked by a foreign key and merges updates of one row, and when rdb refuses a bundle or an update it submits the parts one by one (`rdb_adapter/src/ingest.ts:6-17`, `ingest_orchestrator.ts:497-509`). It keeps the pass's failure events in memory until the settle (`recordFailure`, `ingest_orchestrator.ts:417-430`).
- **The echo.** Ingestion doesn't move the checkpoint, so the next pass's delta still contains the pass's own entries. Projection applies them to the rows that already hold them, found through the mapping. It rewrites the columns each op wrote, as rdb has them, and the row's author, but not the defaults of columns nobody wrote (`rdb_adapter/src/row_actions.ts:77-99`). This second application of a device's own writes is the *echo*. It is not the code's echo suppression, the rule that writes made by projection and by reverts never enter the outbox, which stays as it is.
- **Reverts.** A refused change is reverted by rewriting its row from rdb, unless a newer change to the same row is pending, since the revert would then overwrite it (`ingest_orchestrator.ts:615-618`).
- **Op events.** What the UI learns about writes: a *failure* when ingestion is refused, a *void* or *reinstate* when a concurrent entry changes an entry's verdict. Each target stores them in an event table that only grows, which apps read by cursor (`rdb_adapter/src/types.ts:192-198`, `:357-362`).
- **The sync cycle.** `RdbProjection` runs cycles of ingestion followed by projection (`syncDatabase`, `ingest_orchestrator.ts:640-667`). A cycle starts when rdb grows or the outbox signals a change, and never overlaps another. In rhost, one process runs a host's projection, under a lock per host. Clients in other processes and tabs write the target directly.

### What happens today

The first four scenarios were run on 2026-11-19 against the working tree, which carries part of HIP-20's *Changes* 1; none of it touches these paths. They use the memory target and the fixtures of rdb_adapter_test: a group with a `ledger` table (`ref`, `memo` of up to 8 characters, `amount`), and one with `posts` and `comments`, whose `post` column references a post. The last two were run on 2026-11-20.

**A row deleted before its echo stays.** The target is at checkpoint P. The app inserts a row, and ingestion appends it to rdb. Before the next projection, another device deletes the row. The pass then compares rdb at P, which doesn't have the row yet, with the frontier, which no longer has it:

```
ingested: 1 mapping status: active
after project-back: local row present = true | mapping status = active
```

The delta is right about rdb: the row is live at neither end. But the target already held it, so the target keeps it for good, with a mapping to a deleted row. A remote delete has to land between ingestion's append and the projection's read of the frontier. In a cycle that is a short gap. But a host that stops in between, from a crash or a closed tab, holds it open through its downtime, and rhost starts sync before it opens the projection (`rhost/src/host.ts:896`, `:907`).

**An edit made during a projection disappears until the next cycle.** B's edit of a row reaches A's rdb. Before A projects it, an app on A edits the same row. The edit is captured after the cycle's ingestion has drained the outbox:

```
synced memo: start
outbox: [ '{"memo":"local"}' ]
after projection, local edit still pending; memo shown: remote
after the next cycle (with echo); memo shown: local
```

Projection wrote B's value over the pending edit. The next cycle ingests the edit and its echo restores it, so the user sees the edit vanish and come back.

**A refused change can stay in the target.** The app sets `memo` to a value that is too long, and then, after ingestion has drained the outbox, changes `amount` on the same row:

```
pass 1: rejected 1 | target {"memo":"much too long","amount":"2.00"}
after two more cycles: target {"memo":"much too long","amount":"2.00"} | rdb {"ref":"r1","memo":"start","amount":"2.00"}
```

The revert waits, so as not to overwrite the pending `amount`, and nothing reverts the row later.

**Writes are recorded against changes the app hadn't seen.** Ingestion translates changes with the schema at rdb's frontier (`ingest_orchestrator.ts:61-65`). It appends at the frontier, the default of `bundle` (`:459`, `rdb/src/rtable_group/group.ts:1061-1063`), and advances another group's version to that group's frontier (`ingest_orchestrator.ts:523`). The app wrote against the target, which is at the checkpoint. So rdb records each local write as made after every remote change that reached rdb before it:
- The local write wins over any remote write to the same column that the app hadn't seen.
- The local write is checked against those changes. An app shows a post, another device deletes it, and the app comments on the post before projection removes it. The comment is refused at append: `FK column 'post' in table 'comments' points to non-live row`.

**A refused update can land in part.** A table `grants` has `holder`, of at most 8 characters, and `role`. A row holds (alice, reader), and one update sets it to (bob-the-builder, admin). rdb refuses the update, ingestion submits each column on its own, and `role` lands. rdb and the target both hold (alice, admin), a row nobody wrote.

**rdb accepts a write that is void where it stands.** A group K holds a grant for Bob. A group G binds K behind a gate that lets only G's admins observe it. Mallory, an admin, writes the only observe that brings in Bob's grant. Concurrently, the other admin removes her from the admins, so her observe is void. Bob then inserts a note, which G's rules allow to holders of the grant. rdb accepts the insert, since it checks it reading K through every observe. But the insert's verdict reads K through the live observes, which don't hold the grant. So the insert is void from the start, and rdb never has the row. No delta reports it, since its verdict never changes. Through today's adapter, on a memory target, the target keeps the row for good, with an `active` mapping and no op event. The same holds with a foreign key from G into the grant's row in place of the rule.

### The cause

Projection assumes that the target is what rdb holds at the checkpoint, and both kinds of local work break that.
- **After ingestion, the target holds more.** It holds the pass's writes, which rdb has only after the checkpoint. While the next delta contains them, the echo makes up for it, until a later change cancels them inside the same delta.
- **After a projection over a pending change, it holds less.** The pending change is gone from the table, and the echo restores it only after it is ingested.
- **Refused changes are left out.** A held-back revert is never retried.

Appending at the frontier is what puts the pass's writes after the checkpoint in the first place. The last two scenarios have causes of their own: the column fallback submits parts of an update that nobody wrote alone, and rdb admits an op with a reading of a bound group that its verdict doesn't use.

## Design

### The invariant

After every target transaction, each group's tables are rdb's rows at the group's checkpoint, with the outbox's changes applied on top in commit order. A change applied on top writes the columns it holds, or removes its row.

### The algorithm

The sections below give the rules one by one. Put together, a cycle of `RdbProjection` runs as follows. A group is `g`, its stored checkpoint `checkpoint[g]`, `record(change, …)` writes into the change's outbox row, and `record beside checkpoint[h]` writes into the group's checkpoint record. The comments name the section that holds each rule.

```
cycle:                                                  # an error ends the cycle, and the next one is
                                                        #   retried with a backoff; a halt is not (Crashes)
  if the outbox's first row has a record:  endPass()    # a pass crashed before its settle (Crashes)
  if the outbox has rows:  runPass()
  if the outbox has rows, and the pass didn't stop:  runPass()   # the changes captured during the first pass
  if the outbox is empty:  project()                    # the apply refuses if a change came in meanwhile,
                                                        #   and schedules the next cycle; a pass that
                                                        #   stopped schedules none

runPass:                                                # Ingestion at the checkpoint
  tip[g] = checkpoint[g], for each group g
  compare the two readings of each gated binding, at each tip[g]   # Writes void where they stand
  for each change in the outbox, in commit order:       # never past a change without a stored record
    g = the change's group
    if g has an identity provider and no writer key:    # No projection over a pending change
      warn once for g;  stop: endPass() and return
    for each group B that g binds, whose tip moved in this pass and that tip[g] doesn't observe:
      o = prepareObserve(g, B, at tip[g])               # declined when the gate refuses the writer, when
                                                        #   o would be void where it stands, when building
                                                        #   it throws, or when it needs a key we lack
      record(change, the observe's hash)                # before the append
      append o;  tip[g] = o.hash                        # rdb's append skips an entry it holds
      compare again the two readings of every gated binding of g
    e = prepareEntry(change, at tip[g])                 # translate, build, sign, validate; while a gated
                                                        #   binding's two readings differ, also through
                                                        #   the live observes
    if e is refused:      record(change, its failure event)  # rdb's refusals, the planner's, and any error
                                                        #   while translating or building the entry; a
                                                        #   refused insert's local id keeps an identity
                                                        #   for the rest of the pass (Reverts)
    else if e is empty:   record(change, nothing to write)
    else:                 record(change, the entry's hash)    # before the append
                          append e;  tip[g] = e.hash    # halt if the DAG returns another hash
  endPass()                                             # an error from an append or from the target
                                                        #   ends the pass before this (Crashes)

endPass:                                                # The end of a pass: takes nothing from runPass
  handled = the outbox's leading rows whose record is a failure, nothing to write, or an entry rdb holds
  end[g]  = the canonical version of checkpoint[g] with the hashes recorded for g that rdb holds,
            in the outbox rows and beside checkpoint[g]
  while some group h binds a group B whose end moved, end[h] doesn't observe end[B],
        and this end hasn't declined that observe:
    o = prepareObserve(h, B, at end[h])                 # the same rules as in runPass
    if o is not written:  the end has declined it
    else:  record it beside checkpoint[h];  append o;  end[h] = o.hash   # it belongs to no change
  delta[g] = rdb's delta from checkpoint[g] to end[g], for each g whose chain to end[g] holds an observe
  halt if some delta[g] changes the schema              # never, by The settle moves the checkpoint
  settle, in one target transaction:                    # The settle moves the checkpoint
    check that each checkpoint is still checkpoint[g], or halt
    remove the handled rows from the outbox, and read the rest
    write the handled inserts' mappings, then mark the accepted deletes deleted
    remove the rows of refused inserts that had no identity yet
    for each row of a refused change, and each row of an op whose verdict delta[g] changed:
      write rdb's row at end[g], then the row's changes still in the outbox      # Reverts
    on SQLite, rewrite json text in canonical form      # Settlement parity
    store the handled rows' failure events, then delta[g]'s void and reinstate events
    checkpoint[g] = end[g]
    clear the observes recorded beside every checkpoint
```

A change of a table that no member projects gets the mark of nothing to write, and the planner's refusals and any error while translating the change or building its entry get a failure event like rdb's. So every change a pass reaches ends with a record, or the pass ends there (*What became of each change*). A group without an identity provider signs nothing, so it needs no writer key (*Ingestion at the checkpoint*).

### One projector per target

One process projects and ingests for a target. rhost already ensures it with a lock per host: a file lock on Node (`rhost_node/src/lock.ts:38`), a Web Lock in the browser (`rhost_browser/src/platform.ts:169-174`). `RdbProjection` never overlaps its own cycles. Other processes and tabs may write the target, and their writes only add to the outbox.

**One lock per target.** Within the process, what keeps cycles apart is one lock per target object, taken by `syncGroup`, `syncDatabase` and index reconciles. What a cycle reads and then writes belongs to the target: the outbox, the checkpoints and the index spec. `RdbProjection` passes the one target it holds to each call (`rdb_projection/src/projection.ts:206`, `:225`), so `sync()`, reactive cycles and index reconciles exclude each other.
- **Today** the lock is keyed by the sorted ids of the member groups, and `syncGroup` takes another, keyed by its group (`rdb_adapter/src/ingest_orchestrator.ts:632-635`, `:251`, `rdb_adapter/src/index_reconcile.ts:48`). A membership change replaces the members (`projection.ts:499`), so a call that starts after a group joins takes another lock than one still running from before.
- **Two cycles at once** then drain the same changes. Today rdb still ends with one row: the second pass adopts the identity the first reserved, and takes rdb's refusal of its insert for its own entry (`ingest_orchestrator.ts:397-411`, `:475-482`). This HIP removes both, so rdb would get two rows, or the second settle would find the checkpoint moved.
- **An index install inside a cycle** makes the cycle's apply throw `CheckpointMovedError`, since `apply` checks the index spec too (`rdb_adapter_sqlite/src/sqlite_target.ts:365-366`). Today `syncWithCasRetry` plans again.

  With the lock per target, a moved checkpoint or a changed index spec at an apply can only come from another process: the second projector the halt is for. Both cases were run on 2026-11-23 against a copy of rdb_adapter with the lock keyed by the target: the second cycle waited for the first, and the reconcile for the cycle's apply. Two targets over the same groups no longer share a lock.

The checkpoint compare-and-set stays in `apply`, and the settle gets one too. A moved checkpoint means a second projector, which is not supported. Wherever it is found, the projection halts: `RdbProjection` runs no more cycles, `sync()` throws, and `onError` fires once. Reads of the target keep working. Today `RdbProjection` retries a moved checkpoint at `apply` (`syncWithCasRetry`, `rdb_projection/src/projection.ts:202-212`). A projection that carried on would still share the target with the other projector, and could meet it at the settle, after appending.

### Capture

A change holds every column its write names, changed or not, and nothing else. A revert puts a row's pending changes back on top of rdb's row (*Reverts*), so each change has to hold everything its write set. Say a first update sets `a` and `c`, and a second sets `a` to the same value and `b`. If capture keeps only the values that changed, the second change holds only `b`. When rdb refuses the first because of `c`, the revert then loses the `a` that the second write named.

- **SQLite.** Capture writes one outbox row per updated row, with every column the statement names. A `BEFORE UPDATE` trigger closes the row's open outbox row, if an earlier statement left one. One `AFTER UPDATE OF <column>` trigger per column opens this statement's outbox row when none is open, and adds its column to it. Only `UPDATE OF` triggers see which columns a statement names. Measured on 2026-11-20, they add about 5 µs per update, against 1 µs for today's triggers. The id column is never captured: a statement that changes it fails (HIP-20 *Changes* 1), and `SET id = id` passes.
- **Memory.** `localUpdate` records the columns it is given.
- **IndexedDB.** A `put` replaces the whole record, as IndexedDB defines it, so a `put` over an existing record captures every column of the table, as an `UPDATE` that names every column would. A field the record leaves out counts as `NULL`, as today, except the author column: HIP-20 keeps the stored author and captures nothing for it. An update through HIP-20's writer, which shares the facade's write code, captures the columns it writes, not the whole record.
- **IndexedDB's `patch`.** The facade gains `patch`, a partial `UPDATE` for IndexedDB. It takes the fields to write and, separately, a list of fields to clear, and leaves every other field as stored. It captures exactly the fields it names, changed or not, the cleared ones as `NULL`. It compares nothing: a comparison is for the library that calls it, as `compare` is for HIP-20's writer. Like an `UPDATE` that matches no row, a `patch` of a key with no record writes nothing. HIP-20's writer on IndexedDB reads the row first and stops when it is missing, in the same way (HIP-20 *Changes* 4).
- **One change per updated row.** On every target, an update of a row is one change, and so one entry. With one entry per column, a rule that reads two of the columns would see the first changed without the second.

**A named column is a write.** In rdb it competes with concurrent writes to the column, and wins over a concurrent clear. A form that saves every field therefore undoes what other devices changed concurrently in fields the user didn't touch. So does an IndexedDB `put`, which names every column. Say a phone changes a row's title and a laptop its body, each with a `put`. The two entries are concurrent and both name both columns, so the one with the larger hash wins both, and the other edit is lost. With a `patch` of the field each one changed, both land. A `put` after a refused one also repeats the refused value, which the app read back before the settle, and so is refused too. HIP-20's writer can compare with the row first, when its update is given `compare`, and then names only the columns that differ.

**Columns that can't be written.** A statement that names every column, or a `put`, also names the readonly columns and the author column, usually with the row's own values. Ingestion sets such a column aside when it holds rdb's value for the row, and refuses the update when it holds another. A `patch` that names one is treated the same way.

**The author is filled in at once.** In a group with an identity provider, an insert that leaves the author column empty gets the writer key's id at capture, in the row and in the captured change, when the projection's `author` setting is `auto`, the default. Under `manual`, and in a group without an identity provider, the column stays empty, which means anonymous. Capture reads the writer key and the setting from HIP-20's writer record, and whether the table's group has an identity provider from the table's meta (HIP-20 *What the projection records in the target*, *Authorship*). The record and the setting landed with this HIP in an earlier shape, which HIP-20 changes before its writer (its *Changes*): the setting was `autoAuthor`, which `RdbProjection`'s per-group `configOverride` could change for one group, and the record held each group's name and mode, which capture read in place of the table's meta. On SQLite, an `AFTER INSERT` trigger writes the id, with capture switched off for that write as the `applying` flag does, since a `BEFORE INSERT` trigger can't change the row. Memory's `localInsert` and the IndexedDB facade set it in the record. Ingestion signs each insert as its captured author column says:
- **An empty column under `auto`**, in a group with an identity provider, means the insert was written while no writer key was recorded. It is refused, as HIP-20's writer refuses a write then (`no-writer-key`).
- **Another key's id** is refused, as HIP-20 has it. So is the id of a writer key the host no longer uses, after a change of key.

A table projected without an author column (`authorColumn: false`, `rdb_adapter/src/names.ts:23-25`) has nothing to fill in or read. Its inserts are signed with the writer key, as today, and the refusal of an empty author column doesn't apply. HIP-20's `RdbProjection` refuses a group with an identity provider configured that way under `manual`.

### Ingestion at the checkpoint

Ingestion works from what the app saw: the group's checkpoint, written C.
- **Schema.** It translates changes with the schema at C.
- **One entry per change.** Each captured change becomes one entry, in commit order. Ingestion no longer bundles inserts or merges updates.
  - **A child needs no bundle.** An insert that references a row inserted earlier in the pass is appended after that row's entry, so its reference finds the row.
  - **An update lands or is refused whole.** There is no fallback to its columns, which can land a row nobody wrote (*What happens today*).
- **A chain.** The pass's first entry for a group is appended at C, and each next one at the previous one. The pass's entries form a chain after C, with tip I. This HIP writes C ∪ I for the version that holds C and the chain. Since C is in I's past, its canonical form is `{I}`, which is what the settle stores and what `versionsEqual` compares with rdb's frontier (`rdb_adapter/src/types.ts:591`). Every entry of C is before the chain, and each entry of the chain is before the next, so no entry of the chain is concurrent with C or with another entry of the chain.
- **What became of each change.** Ingestion records it in the change's outbox row as it happens. Every change gets one of three records, and none is dropped:
  - **The hashes of its entries.** Ingestion builds each entry, signs it and validates it, writes its hash into the outbox row, and only then appends it. This covers the change's own entry and every observe written before it. rdb's row ops, `bundle` and `observe` are split into these two steps, and a change's entry is the plain row op the app wrote, the entry a native write of it makes. The DAG already computes a hash without appending (`computeEntryHash`, `dag/src/dag.ts:115`).
  - **Its failure event, when it is refused.** This covers rdb's refusals and the planner's:
    - a readonly or author column with another value, and an empty author column under `auto` in a group with an identity provider;
    - a write void where it stands;
    - an insert on a local id that already has an identity: a mapping of any status, an identity minted earlier in the pass, or an insert refused earlier in the pass. Examples are SQLite's `INSERT OR REPLACE` over a row, and an insert that reuses a deleted row's id (*Reverts*). A refused insert gives its local id an identity for the rest of the pass, wherever it was refused, as its `ingestion_failure` mapping does for later passes. So the result doesn't depend on where the pass ends;
    - an update or a delete of a row whose insert was refused;
    - a change of a table that has no rdb table;
    - any other error while the planner translates the change, or while rdb builds, signs or validates its entry, such as a bug or a payload rdb can't build. Its reason is the error's message, as today (`attempt`, `ingest_orchestrator.ts:456-465`). Without a record, the change would hold the projection back as a dropped change does. Only these two steps make a failure: an error from the target's own reads and writes, such as `writeRecord` or a read of the mappings, ends the pass (*Crashes*). Taken as the change's failure, a passing `SQLITE_BUSY` would revert a write rdb would take a moment later.

    An insert refused before its entry is built still gets a uuid and a row hash, which its failure event carries (`rowId`, `rdb_adapter/src/types.ts:209-214`). Its `ingestion_failure` mapping needs them, since the sync table is keyed by row hash.

    The settle moves the event to the event table, in the transaction that reverts the row. Written straight to the event table, it would reach the app while the row still shows the refused write.
  - **A mark that it had nothing to write.** An update whose named columns were all set aside (*Capture*) makes no entry and is no failure. An example is a `put` of an unchanged record in a table whose columns are all readonly. A change of a table that no member projects gets the mark too. A table of a member without a writer key is not such a table (*No projection over a pending change*).

  Today the planner drops some of these changes, with a warning in the ingest result or none, and records nothing for them (`rdb_adapter/src/ingest.ts:251-268`, `:461-467`, `:581`, `:592-596`, and `ingest_orchestrator.ts:442-444` for a refusal without a kind, such as a missing rdb table, `ingest.ts:451-453`). The drops were written for crash replays, which this HIP removes. A dropped change would get no record, so it would stay in the outbox for good, and no projection would apply again.

  Every entry ingestion appends is recorded before it is appended, and every refusal when it is made. A pass never goes past a change without a stored record: when a record can't be written, the pass ends there. Otherwise the changes after it would get records and entries while the handled rows ended before it, and the next pass would build them again, with a new uuid for each insert. The end of a pass reads these records, and nothing the pass kept in memory (*The end of a pass*). Writing a record signals no change (*Changes*).
- **Entries rdb already holds.** An entry made again comes out the same when it is anonymous, or signed with a suite that signs without randomness: Ed25519 does, ML-DSA doesn't (`crypto/src/signing.ts:31`, `:54`). Two devices at one checkpoint can make the same anonymous update. Most of ingestion's observes are anonymous too: it signs one only when the binding's gate reads `$author` (`observeNeedsAuthor`, `ingest_orchestrator.ts:550`). A second append of an entry would put it back into rdb's frontier (*Background*). So `Dag.append` returns the entry's hash without writing anything when its transaction finds the entry already there, and ingestion appends each recorded entry without looking first. A look before the append would race with sync, which checks for an entry when its header arrives (`sync/src/synchronizer.ts:796`) and appends it only after fetching and validating its payload (`:1301-1323`). The check inside the append closes that race for sync too.
- **Other groups.** An observe advances another group's version to that group's checkpoint plus its chain so far, not to its frontier.
  - **Joined with what is already observed.** The observe also takes in what the observing group already observes at its tip. rdb refuses an observe that doesn't extend every earlier one (`mvt/src/refs.ts:409-425`). Groups are projected one transaction at a time, so the observing group's checkpoint can already hold a remote observe of entries newer than the other group's checkpoint.
  - **Only when that tip advanced.** An observe follows an entry that rdb took, never one it refused (`ingest_orchestrator.ts:568`).
  - **Part of the chain.** Observes are part of the observing group's chain.
  - **Groups without a checkpoint are left alone.** A member group that has none yet holds no projected tables, so no change comes from it. It is neither ingested nor observed.
- **Reads of rdb.** Reverts, and every other read of rdb's rows during the pass, are made at C ∪ I.
- **Anonymous groups need no writer key.** A group without an identity provider signs nothing: its entries and its observes are anonymous, and an insert's row hash comes from its uuid alone. So ingestion takes its changes with no writer key. Today it asks for the writer anyway (`authorFor` and `writerFor`, `ingest_orchestrator.ts:362-373`, and the observes at `:537`), and `syncDatabase` ingests only the members that have one (`:646`). Under this HIP that would stop an app whose groups are all anonymous, opened without a key, at its first pending change, and remote changes would wait for good. A group with an identity provider still needs a writer key (*No projection over a pending change*).

The remote entries that reached rdb after C are concurrent with the chain. Projection applies them later, as rdb resolves them against the chain.

A permission and its use in another group, written one right after the other, need no coordination. The outbox holds them in commit order, and a drain takes every committed change, so the permission's entry comes first, in the same pass or an earlier one. Before the use's entry, its group observes the permission's group at its checkpoint plus its chain, which holds the permission.

### Writes void where they stand

An op can pass rdb's checks, which read a bound group through every observe, and fail its verdict, which reads it through the live observes only (*Background*). It is then void in every version that holds it, and no delta ever reports it (*What happens today*). HIP-14 §4 requires an author to pass both, and nothing does that for row ops today.
- **rdb refuses it.** The step that builds and validates a local entry takes an option to also validate with the bound groups read through the live observes. An op that fails is refused before it is appended. In ingestion it is one more refusal: a failure event, a revert, and the tip doesn't move. Entries that arrive through sync are checked as today.
- **Only while the readings differ.** The second validation costs a verdict evaluation. Ingestion compares the two readings of each gated binding at C, and sets the option only while they differ. A row op changes neither reading, and a group without a gated binding skips all of it.
- **Every gated binding, after each observe.** After each observe it appends to a group, ingestion compares again the two readings of every gated binding of that group, not only the one it observed. An observe gate can read the group's other bindings and its own tables (`diagnoseObserve`, `rdb/src/rtable_group/group.ts:1285-1286`), which is why ingestion already orders its observes by what the gates read (`ingest_orchestrator.ts:538-540`). Say G binds K and M, and M's gate reads K. G's checkpoint holds a live observe of M. The pass writes an observe of K, which brings in an entry of K concurrent with what the gate of M's observe read, and voids that observe. M's two readings now differ, and a later write of the pass that reads M has to be validated through the live observes too. This costs one comparison per gated binding per observe. Narrowing it to the bindings whose gates read the observed group would save little.
- **Ingestion's own observes.** One that would be void at its own position is not appended. `evaluateObserveGate` takes either reading (`rdb/src/rtable_group/group.ts:708`).

The refused write doesn't tell the user why: the target shows the grant in K's tables, while G reads K without it until someone writes a live observe. [HIP-30](0030-cross-group-skew.md) records this with the other ways a target can show a bound group at another version than its binding group reads.

### The end of a pass

What a pass does at its end depends only on what was stored while it ran: the outbox rows, the checkpoints and rdb. The end is then the same computation whoever runs it: the process that ran the pass, or the next cycle after a crash (*Crashes*). It takes nothing from the pass, so that this can't be broken by accident. A pass can end after any change. The settle already copes with changes left in the outbox, since a revert puts them back on top.
1. **The handled changes** are the outbox's leading rows whose record says what became of them: an entry rdb holds, a failure, or nothing to write.
2. **Each group's chain ends** at the canonical version of C with the hashes recorded for the group that rdb holds: the entries and observes in the outbox rows, and the end's own observes recorded beside the checkpoint (step 3). That is C ∪ I. A group with none keeps its checkpoint. Within a pass the recorded hashes form a chain, so this is the last of them. The union also covers a row that a settle kept after a crash: its record can still hold hashes from the crashed pass, which are in the past of the new checkpoint and drop out.
3. **The observes still due.** A pass writes an observe before an entry that needs it. The end writes the observes that its entries made due and that no later entry called for, with the rules of *Other groups*. For each group whose chain end moved, each group that binds it needs an observe when its own chain end doesn't observe that much. The same then holds for the groups that bind those. Before it is appended, each such observe is recorded beside its group's checkpoint, in a list of the end's observes. Every settle clears every group's list, whether or not it moves that group's checkpoint. It belongs to no change, so no outbox row holds it. It becomes the end of its group's chain. The list exists only while the pass's rows are in the outbox: an end writes observes only after a recorded entry moved some chain, and only the settle removes rows, in the transaction that clears the lists. At a settle, each entry is either in its group's chain end or one rdb doesn't hold, so clearing every list drops nothing a later pass needs. Clearing only the groups whose checkpoint moves would leave a stale entry when the end declines an observe recorded before a crash. An observe is declined when the gate refuses it to the writer, when it would be void where it stands, when building, signing or validating it throws, or when it needs a writer key this projection lacks, as the end of a crashed pass run under other keys can. The end doesn't ask for a declined observe again, and a pass doesn't throw on one. `prepareObserve` always validates: `observe` validates only when the replica's `selfValidate` is on (`rdb/src/rtable_group/group.ts:1037`), while `bundle` always does (`:1077`), and the end relies on the refusal. Bindings can't form a cycle, since a group's bindings come from its create entry and name groups that already exist (`rdb/src/rtable_group/group.ts:320-322`), so the end's observes come to an end.
4. **The deltas.** For each group whose chain from C to its end holds an observe, rdb computes the delta from C to that end (*The settle moves the checkpoint*).
5. **The settle**, in one target transaction.

**The ingest result** comes from the records too: an accepted change for each handled row with an entry, and a rejection for each with a failure event, with the change from the outbox row and the reason from the event. So a cycle that finishes a crashed pass reports that pass's changes. A declined observe isn't reported. The write that needed it is refused with its own reason, which is what the user can act on. Today the pass counts as it goes, and reports an observe rdb refused as a rejection of its own (`ingest_orchestrator.ts:552-557`).

Say a pass writes a signed observe for a change that rdb then refuses, and no later change of that group is accepted. The observe's hash is in the refused change's record, so the chain's end holds the observe, and the end doesn't write a second one.

### The settle moves the checkpoint

In the transaction that removes the handled changes from the outbox, the settle sets each group's checkpoint to C ∪ I, after checking that it is still C. A group whose chain is empty keeps its checkpoint.

There is no echo: no projection applies the chain's row ops to the target again. The target already holds what they did:
- **Each row op is the op of a change the target already applied**, and each change settled in the target as its op settled in rdb (*Settlement parity*).
- **A row op after C changes no row it doesn't write.** It comes after every entry of C, so it is concurrent with none of them, and voids or reinstates none. And rdb doesn't cascade a delete: a comment stays live when its post is deleted, in rdb and in the target.

The echo also did two things that the settle now does itself. It marks the mapping of an accepted delete `deleted`. And a refused change whose row rdb no longer has at C ∪ I is reverted by removing the row (*Reverts*).

An observe is different. rdb evaluates an entry's reads of another group from a horizon, at the version of that group the horizon observes (`rdb/src/rtable_group/group.ts:663-664`, `:1282`).
- **What an observe changes.** An observe after C moves that version for every entry before it: the entries of C, and the pass's own entries earlier in the chain. It can bring in an entry of the other group that is concurrent with what one of them read, such as a revocation or a delete, and so void or reinstate it.
- **One delta per group.** For each group whose chain holds an observe, rdb computes the delta from C to C ∪ I. The settle takes two things from it:
  - **The events.** The delta's verdict changes become void and reinstate events, as in a projection pass (`opVerdictEvents`, `rdb_adapter/src/project.ts:85-101`).
  - **The rows.** A verdict change names the rows its op writes (`rowId`, or `writes` for a bundle, `rdb/src/rtable_group/op_delta.ts:37-50`). The settle writes each as a revert does: rdb's row at C ∪ I, with the row's pending changes on top (*Reverts*). A row that the delta reinstates was void at C, so it isn't in the target. The settle writes it by its row hash, as projection writes a reinstated row (`rdb_adapter/src/types.ts:524-528`): with the local id its mapping kept, if the row was in the target once (`rdb_adapter_sqlite/src/sqlite_target.ts:1170-1175`), or with a new one from the table's own counter (*Local ids*).
  - **A voided delete.** An observe can void a delete that C holds. The row comes back: its mapping is `deleted` and kept its local id, so the settle writes the row under that id and sets the mapping `active` (`sqlite_target.ts:1081-1085`).
  - **An own insert that an observe voided** is named by its row hash only, and has no stored mapping until step 3. The target finds its local id inside the settle, after step 3 writes the inserts' mappings.
  - **No schema.** An observe never changes the schema: `diagnoseObserve` returns `live` for the schema ref (`rdb/src/rtable_group/group.ts:1266`), and the schema at a position is the union of the deploys at or below it (`:428-432`). So the delta has no schema actions. The end checks that before the settle, and halts the projection if not.
- **Not the delta's row changes.** They hold the pass's own writes, which the target already has, and writing them would be the rewrite of every row the pass touched that *Considered and not taken* declines. And they miss an own insert that an observe voided: rdb has that row at neither end, so the delta has no row change for it.
- **Why the verdict changes are enough.** After C the chain holds row ops and observes only, and a row op changes no row it doesn't write (above). So any other row that differs between C and C ∪ I belongs to an op whose verdict an observe changed.
- **The pass's own entries.** rdb reads the verdict of an entry that C doesn't hold at the entry's own position (`rdb/src/rtable_group/group.ts:1086-1098`). So an own entry that was live when it was appended and is void at the end shows as a verdict change. An own entry that needed an observe of the same pass shows none.
- **Net effects.** An entry that one observe voids and a later observe of the same pass reinstates gives no event, and its row reads the same before and after.
- **No rule for the middle of a chain.** An observe often sits in the middle of a chain, since ingestion observes before each entry of a group (`ingest_orchestrator.ts:564-569`). The delta ends where the reverts read rdb, so a later write of the same pass is in it.
- **Nothing from the pass's memory.** C is stored in the target, and the chain is in rdb, so a crash loses nothing the delta needs.

This was checked against rdb on 2026-11-21. A group G binds a users group K, and a note in G needs its author to hold a grant in K. The checkpoint holds a note by Bob, and a pass appends a second note by Bob, then an observe of K, then a row that needs no grant:
- **The observe brings in an entry of K concurrent with Bob's grant that voids it.** The delta has both notes void, the old one and the pass's own, each naming its row.
- **The grant was given in the pass, and the note comes after the observe.** No verdict changes.
- **The observe brings in a removal of the grant that comes after the grant in K.** No verdict changes, since both notes are live at the end.

The settle's steps, in its one transaction:
1. It checks each group's checkpoint.
2. It removes the handled changes from the outbox, and reads what is left.
3. It writes the mappings of the handled inserts, `active`, or `ingestion_failure` for a refused insert that had no identity yet, and then marks accepted deletes `deleted`. In that order, a pass that inserts, deletes and inserts again on one local id leaves the mapping `deleted`.
4. It removes the rows of refused inserts that had no identity yet.
5. It writes each reverted row, and each row of an op whose verdict changed (*Reverts*). A refused insert on a local id that already had an identity is one of these.
6. On SQLite, it rewrites json text in canonical form (*Settlement parity*).
7. It stores the failure events from the handled rows, then the delta's events.
8. It sets the new checkpoints, and clears the observes the end recorded beside every checkpoint.

Outbox rows are removed only here.

### Reverts

A revert sets a row to what the invariant says it holds:
1. It takes rdb's row at C ∪ I.
2. It applies the row's changes still in the outbox, in commit order. An update writes its columns, a delete removes the row, and an insert on the row's local id, from an `INSERT OR REPLACE` or a delete and an insert with the id, writes its whole row. An update or a delete of a row that isn't there does nothing.
3. It writes the result, or removes the row when there is none.

It runs inside the settle's transaction, so it reads every change captured since the drain, and never overwrites one. Each target already has both writes it needs: rdb's row as projection writes it, and then each pending change as capture recorded it. In the scenario of the refused memo, the settle writes rdb's row, with `memo` at `start`, and puts the pending `amount` back: the target holds `start` and `2.00`.
- **A pending delete.** The result has no row, and the row is gone already.
- **A row rdb no longer has.** It is removed, and its mapping marked `deleted`. A pending update of it is refused at its turn.
- **A refused insert on a local id with no identity yet.** As today, its row is removed and its mapping marked `ingestion_failure`. Its pending changes are refused at their turn, and their reverts find no row and write nothing.
- **A refused insert on a local id that already has an identity**: a mapping of any status, an identity minted earlier in the pass, or an insert refused earlier in the pass. The revert reads that identity's row, and the mapping keeps its status. After a refused insert, rdb has no row, so a second insert on its local id is refused and the local row removed, and the mapping stays `ingestion_failure`. After an `INSERT OR REPLACE` over a row, rdb's row at C ∪ I comes back. After an insert that reuses a deleted row's id, rdb has no row, and the local row is removed. So is the second insert in a pass that inserts, deletes and inserts again on one local id.

Nothing is held back for a later pass, so the invariant has no exception, and a projection pass has nothing to catch up on.

### Local ids

Every local id comes from its table's own counter, which never hands out a number twice. The sync table never makes one up. The settle writes rows while inserts can still be pending, captured after the drain or after the point where the pass stopped or crashed. So an id it takes must not be one that a pending insert already holds.
- **IndexedDB and memory** already work this way. Each table has one counter, which capture and projection share (`rdb_adapter_idb/src/idb_facade.ts:542-564`, `rdb_adapter_idb/src/idb_target.ts:443-447`, `rdb_adapter/src/memory_target.ts:279`, `:821`).
- **SQLite** has no sequence object. But each app table's `INTEGER PRIMARY KEY AUTOINCREMENT` keeps a counter in `sqlite_sequence`: the largest id the table has handed out, which it never hands out again (`rdb_adapter_sqlite/src/sqlite_target.ts:772`). Today `allocateId` takes the sync table's largest id plus one instead (`:1023-1031`). An insert still in the outbox has no sync row yet, so it can hold that id, and the settle then writes another row over the user's: `upsertRow` updates the app row it finds at the id (`:1096-1105`). With the counter:
  - **A row the projection writes** is inserted first, and its sync row takes the id the insert produced, as a native insert's does.
  - **A reference to a row the target doesn't hold**, such as a comment whose post was deleted, or a post that comes later in the same apply, takes the next value of the referenced table's counter: `UPDATE sqlite_sequence SET seq = seq + 1 WHERE name = … RETURNING seq`. A table that has never had an insert gets its counter row first, at 0. When the row arrives, it is inserted with that id, which nothing else can get. Today a sync row alone reserves such an id (`:1018-1022`).
  - **The counter** rolls back with its transaction, so an id is never kept by a transaction that didn't commit. A rebuild of the table restores it (`rebuildTable`, `:969-988`). An app can still insert with an explicit id, which *Reverts* covers.

  Checked on 2026-11-23 in an in-memory database with SQLite 3.53.2. After the top row is deleted, the next insert gets a new id, where a table without `AUTOINCREMENT` hands the deleted id out again. The counter moved by hand hands out the next id, and a later insert with an id below the counter works and leaves the counter where it is. A rolled-back insert leaves the counter where it was.

### Settlement parity

Without the echo, a local write has to settle in the target the way its op settles in rdb. Parity comes from the source: each target applies a write with rdb's rules, as far as it can express them. HIP-20's *Changes* 1 provides this for defaults, explicit `NULL` and the author rules. Today the echo hides the differences that are left. Run against the working tree on 2026-11-19, first an insert on memory that leaves the author column empty, then a native SQLite insert of `5` into a boolean column and of json text in another form:

```
after ingestion : {"id":1,"ref":"r1","memo":"empty au","amount":"1.00","author_key_id":null}
after the echo  : {"id":1,"ref":"r1","memo":"empty au","amount":"1.00","author_key_id":1}

native write      : {"flag":5,"data":"{ \"b\": 1, \"a\": 2 }"}
after ingestion   : {"flag":5,"data":"{ \"b\": 1, \"a\": 2 }"}
after the echo    : {"flag":1,"data":"{\"a\":2,\"b\":1}"}
```

Each difference is handled on its own:
- **An empty author column.** Capture fills it in (*Capture*).
- **A SQLite boolean other than 0 or 1, and json text that isn't json or is json's `null`.** HIP-20's *Changes* 1 makes SQLite refuse them at the statement.
- **Json text in another form, on SQLite.** It holds the same value as rdb's. But a query that binds a json value matches only the canonical text (`sqlParam`, `rdb_adapter_sqlite/src/sqlite_target.ts:1616-1621`), which rows from rdb have. So the settle rewrites the column in canonical form. It skips a column that a pending change writes, whose own settle canonicalizes it, and a row that is gone. Its commit notice names the tables it wrote, so the app's queries run again.

No other difference is known. The consistency test (*Tests*) would find one: on all three targets, after every step, it compares the target with a full projection.

### No projection over a pending change

A projection pass applies only when the outbox holds no change, of any group.
- **Inside the apply.** The check is made in the apply transaction, next to the checkpoint compare-and-set, so no write can land between the check and the apply. SQLite's apply holds the database's write lock (`BEGIN IMMEDIATE`, `rdb_adapter_sqlite/src/sqlite_target.ts:407-413`). IndexedDB's readwrite transaction covers every store, the outbox included (`rdb_adapter_idb/src/idb_env.ts:44`). Memory is synchronous.
- **A refusal ends the cycle.** When a change is pending, the apply throws `PendingChangesError` and changes nothing.
  - **Not an error.** The refusal never reaches `onError`, which in rhost reports an error in the host's status (`rhost/src/host.ts:752`, `:794-797`).
  - **The next cycle.** The cycle ends, and `RdbProjection` schedules the next one itself, which ingests the change first.
  - **Not the change's signal.** That signal may have fired before the refusal, or not yet: a native SQLite write outside `wrapDatabase` has none until the WAL watcher sees it.
  - **When it arms.** `arm()` also checks for a change already pending when it starts listening, since `open()` runs its first cycle before it arms (`rdb_projection/src/projection.ts:155-156`).
- **A short gap.** A cycle whose outbox filled up again during ingestion ingests once more before it computes the delta. Only a change captured after that makes the apply refuse.
- **No loop.** A cycle ingests once more at most. Under a steady stream of writes, it then ends and leaves the rest to the next cycle, so `sync()` and `stop()` return.
- **Without a writer key.** A group with an identity provider and no writer key can't ingest. Writers are set per group (`rdb_projection/src/scope.ts:126`), so one projection can hold groups with a key and groups without one. Capture stays on once a target has had it (`rdb_adapter_sqlite/src/sqlite_target.ts:490-495`), so a target written with a key and opened again without one can hold such a change. The projection opens, its reads work, and it warns. Three rules follow:
  - **The pass stops at the first such change**, and its end settles the changes before it, as it can after any change. It can't skip the change. A later change can depend on it, such as an insert in a group with a key that references its row. And the settle removes only the outbox's leading rows that have a record (*The end of a pass*), so the changes after a skipped one would stay in the outbox, recorded, and the next pass would build their entries again, with a new uuid for each insert.
  - **No projection applies while it is pending.** The outbox isn't empty, so every group's checkpoint stays where it is, and remote changes wait. The invariant holds: the change stays written at the checkpoint it was made against, and a later open with the key ingests it there.
  - **No cycle schedules another because of it.** A refused apply schedules the next cycle (*A refusal ends the cycle*). A cycle that stopped at such a change doesn't, or it would loop on it. The next cycle starts as any does, when rdb grows or the outbox signals (`rdb_projection/src/projection.ts:550-562`), and stops at the same change at once.

  **The warning** comes once for each such group, not once per cycle: at open when a change of it is pending, or else when a cycle first meets one. It names the group, and says that remote changes wait until a projection opens with a key for it. It goes to a new `onWarning` option of `RdbProjection`, or to `console.warn` when that isn't given. `RdbProjection` has only `onResult` and `onError` today (`rdb_projection/src/projection.ts:64-66`), and in rhost `onError` reports an error in the host's status.

  rhost and rdb_repl always open with a writer key (`rhost/src/host.ts:913`, `rdb_repl/src/projection/command.ts:187`). Today a projection without a writer is read-only and projects over the pending changes (`rdb_projection/src/projection.ts:56-57`). A group without an identity provider needs no key (*Ingestion at the checkpoint*).
- **One rule.** Today's refusal of a first projection over un-ingested changes (`ingest_orchestrator.ts:658-662`) becomes a case of it.

[HIP-31](0031-projection-over-pending-changes.md) records a design that applies over pending changes instead of waiting.

### Crashes

A pass that crashes before its settle has stored everything its end needs: every entry it appended is recorded before the append, and every refusal when it was made (*Ingestion at the checkpoint*). The next cycle runs that end first, as the end of any pass runs (*The end of a pass*). Then it handles the rest of the outbox in a new pass, from the new checkpoints. Nothing of the crashed pass is resumed or rebuilt.

| The outbox row holds | rdb | The next cycle |
|---|---|---|
| An entry's hash | Holds the entry | Handled and accepted. An insert's uuid and row hash are read from the entry |
| A failure event | | Handled and refused, with the reason the crashed pass recorded |
| The mark of nothing to write | | Handled |
| An entry's hash | Doesn't hold the entry | Not handled: the crash came between the record and the append. The new pass builds the entry again and records the new hash over the old one |
| Only observes' hashes, or nothing | | Not handled. A recorded observe that rdb holds is part of its group's chain |

Say the outbox holds a change in K, then two changes in G, which binds K. The pass appends K's entry, writes G's observe of K, appends G's first entry, and crashes. The observe brought in an entry of K that voids a row of G's checkpoint. The next cycle finds K's change and G's first change handled. It computes G's delta from the checkpoint to G's first entry, which holds the void. The settle stores the void event, removes the row, removes the two outbox rows, and moves both checkpoints. A new pass then takes G's second change.

- **The observes still due.** Say a permission in K and its use in G are in the outbox, and the pass crashes after K's entry. The end writes G's observe of K before it settles, as the end of any pass does. Without it, K's checkpoint would hold the permission, the new pass would have no entry of its own in K and so write no observe, and G's change would be refused.
- **A crash during the end**, after its observes and before its settle, leaves the same state. Those observes are recorded beside their groups' checkpoints, so the next cycle's end finds them and writes none of them twice. One recorded and not appended, after a crash between the two, isn't held by rdb: the end leaves it out and decides again, and the settle clears it, also when the end declines the observe this time and the group's checkpoint doesn't move.
- **An error in an append, or from the target's own reads and writes**, ends the pass, as a crash does, and reaches `onError`. Examples are an I/O error, an IndexedDB quota, and a `SQLITE_BUSY` on `writeRecord` while the app holds a long write transaction. The records written so far stay. The next cycle's end settles the changes before the failed one, and a new pass builds that one and the rest again, each once: a record is written before its append, so a record that failed to write means nothing of its change reached rdb. Only a change whose append or record keeps failing, as on a full disk, holds the projection back this way, and `onError` reports it. In rhost that sets the host's last error (`rhost/src/host.ts:794-797`).
- **The retry.** After a cycle that ends in an error, `RdbProjection` schedules the next one itself, with a backoff: 1 s, then doubling up to 60 s, reset after a cycle that succeeds. Otherwise an edit hit by a passing error would wait for the next write or remote change (`rdb_projection/src/projection.ts:550-566`). A halt doesn't retry. Neither does a pass stopped at a group without a writer key (*No projection over a pending change*). A refused apply already schedules the next cycle.
- **The recorded hash.** `appendPrepared` compares the hash the DAG returns with the one it was prepared with, and the projection halts when they differ, as on a moved checkpoint. Otherwise the record would name an entry rdb never holds, and each cycle would build that change and every later one again, with a new uuid for each insert, and the copies would replicate. No cause is known: computing the hash and appending wrap the payload and fill in its parents the same way (`mvt/src/dag/dag_nesting.ts:166-178`, `:198-200`). The signature is made once, before the hash is computed, so a signing suite with randomness doesn't affect it.
- **A refused insert** is read from what is stored. It had no identity when the target has no mapping for its local id and no earlier handled row is an insert on it. Otherwise its revert reads that identity's row: the mapping's row, or the earlier insert's, from its entry when rdb holds it and none when that insert was refused too.
- **No identity is saved before the append.** An insert's uuid is minted when its entry is built. A crash before its hash is recorded, or between that and the append, leaves no entry, and no later change that references the row has been reached, so a new uuid is fine. The settle writes the insert's mapping. Until then the row has none, so `rowHash` and `localId` have nothing to show.
- **Durable commits.** The design assumes that a commit that returned is kept, in the target and in rdb's store, as the settle already does. Neither asks for it today. The SQLite target and rdb's SQLite store run in WAL mode, which better-sqlite3 builds with `synchronous` at `NORMAL`, and IndexedDB transactions use the default durability. A crash of the process keeps every commit, but a power failure can lose the last commits of either store.
  - **The target loses an insert's record while rdb keeps the entry.** The next pass mints a new uuid, and the row shows twice once the first copy is projected.
  - **rdb loses entries that a settle already put in a checkpoint.** The checkpoint then names entries rdb doesn't have.

  Both are left to a later design (*Considered and not taken*).

An insert whose row already exists, or a delete whose row isn't live, is then a real conflict, and a failure like any other.

### What changes for apps

- **Conflicts.** A local write and a remote write that the app hadn't been shown are concurrent. The larger entry hash wins, and a value wins over a `NULL`. Today the local write always wins.
- **Validity.** A write is checked against what the app saw.
  - A remote change the app hadn't seen can make it invalid. It is then accepted and voided, its row leaves the target through projection, and the app gets a void event instead of a failure. In the scenario above, the comment on the deleted post is accepted. It is in the target at C ∪ I, gone after the next pass, and the event log holds `origin: 'concurrency', direction: 'void', kind: 'insert', table: 'comments'`.
  - A remote grant that reached rdb but not the target doesn't validate a write either, where today's append at the frontier finds it.
  - A write that reads another group is checked against the version its own group observes, which can differ from what the target shows of that group ([HIP-30](0030-cross-group-skew.md)).
  - A write that would be void where it stands is refused.
- **An update lands or is refused whole.**
- **A named column is a write.** An `UPDATE` that names a column with its current value writes it, and an IndexedDB `put` writes every column. The IndexedDB facade gains `patch`, which writes only the fields it is given and clears the ones it lists, as a partial `UPDATE` does. HIP-20's writer, given `compare`, names only the columns that differ.
- **Each change is its own entry.** Inserts linked by a foreign key no longer go into one bundle, and a parent and its child can come apart: rdb can accept the parent and refuse the child, or a concurrent change can void one of them. An app that needs writes to land together uses rdb's bundles directly (HIP-20's *Atomic writes*), until a later HIP gives the targets explicit bundles.
- **The author column** holds the writer key's id as soon as the row is written. `INSERT … RETURNING` still returns `NULL` for it, since it shows the row before `AFTER` triggers run. An app can't tell a pending row by its empty author column any more. `rowHash` tells it: it has no answer until the insert is settled.
- **Remote changes can wait.** Projection waits for an empty outbox, so while local writes keep arriving, remote changes reach the target only between them.
- **A projection without a writer key** for a group with an identity provider opens with a warning, through `onWarning` or `console.warn`, when a change of that group is pending or comes in. While it is pending, no remote change reaches the target. A projection opened with the key ingests it at the checkpoint it was written against. Today such a projection projects over the pending changes and never ingests them.
- **An anonymous group is ingested without a writer key.** Today a projection without a writer ingests no group.
- **A native insert over an existing local id is refused and reverted**, with a failure event, where today it is dropped with a warning or taken for a replay.
- **A second projector halts the projection.**
- **Json text on SQLite** reads back in canonical form once its change is ingested.
- **Fixed.** A row deleted before its echo leaves the target. An edit no longer disappears during a projection. A refused change is always reverted. No update lands in part, and rdb holds every row the target shows.

### HIP-20

HIP-20's *Changes* 1 reverts a native insert over an existing local id to rdb's row, in two cases: an insert that reuses a deleted row's id, and SQLite's `INSERT OR REPLACE` over a live row. Today's ingestion drops the first as a stale crash replay (`rdb_adapter/src/ingest.ts:251-268`). It drops the second too when the row came from rdb, and skips it as already applied when this device inserted the row, which is also how today's crash replay recognizes its own inserts. Marking such skips for a revert would not be enough either: when rdb no longer has the row, the revert is left to project-back, and the first scenario above shows that project-back can't be relied on there.

With this HIP, what a crashed pass appended is known from the hashes recorded in the outbox, never guessed from a refusal, and there are no replays. Both cases then become ordinary failures, reverted and logged like any other (*Reverts*), and both land with this HIP. HIP-20's new change-source method, which told a stale replay whose local row is gone from an insert whose row is still there, isn't needed: the revert of an insert whose local row is gone writes nothing. This HIP requires the rest of HIP-20's *Changes* 1, the source of settlement parity.

This HIP also changes five things HIP-20 describes:
- **Bundling and merging go**, and with them HIP-20's rule that a bundle joins only inserts with one signer. HIP-20's first open question, a batch of writes that becomes one bundle, is the explicit replacement.
- **An update lands or is refused whole**, where HIP-20 has each column land or fail on its own.
- **Capture records the columns a write names.** The comparison leaves capture, and becomes the `compare` option of HIP-20's writer.
- **Capture fills in the author column** from HIP-20's writer record and its `author` setting. So the record lands with this HIP, ahead of the rest of HIP-20's writer. So does the setting: `RdbProjection`'s option and its check, rhost's config field, and ingestion's signing of each insert as its captured author column says. Both landed in an earlier shape (*Capture*), which HIP-20 changes before its writer. The writer's own refusals stay with the writer.
- **`no-writer-key` covers only groups with an identity provider.** Ingestion takes an anonymous group's changes without a writer key (*Ingestion at the checkpoint*), so HIP-20's writer accepts a write to one on a projection without a key. This lands with HIP-20's writer (its *Changes* 2 to 4).

## Why it is sound

Only this process moves checkpoints and writes reverts (*One projector per target*). The invariant holds after each kind of target transaction, given that it held before:

1. **A local write** adds a change to the outbox and applies it to the table. The change holds every column the write set, so applying it on top gives the table the write left. That is what the invariant describes.
2. **The settle.** Before it, the target is rdb at C, with the pass's handled changes and then the later changes on top.
   - The chain's row ops apply the handled changes that rdb accepted after C, in commit order. Each is live at its own position (*Writes void where they stand*). Apart from the chain's observes, C ∪ I holds no other entry after C. So rdb at C ∪ I is rdb at C with those changes applied, as rdb settles them, and with the effects of the observes.
   - The target applied the same changes and settled them the same way (*Settlement parity*).
   - Each row of a refused change, and each row of an op whose verdict the pass's observes changed, is written as rdb's row at C ∪ I with the remaining changes on top (*Reverts*). Every other row differs between C and C ∪ I only by the accepted changes (*The settle moves the checkpoint*).
   - Removing the handled changes from the outbox leaves rdb at C ∪ I with the remaining changes on top.
3. **A projection pass** runs with an empty outbox, so the target is rdb at C. It applies the delta from C to the new version F. By the delta's contract, that gives rdb at F, and F is the new checkpoint.

Projection also keeps making progress. Every change a pass reaches gets a record, so the settle removes it from the outbox. An error while the change is translated or its entry built is its failure too. A pass that ends at an error from an append or from the target is retried with a backoff, and its next cycle handles each change once. Projection then waits only for changes captured since, with two exceptions, each reported. A change whose append or record keeps failing, as on a full disk, ends every pass, and `onError` reports it (*Crashes*). A change of a group with an identity provider and no writer key stops every pass, with a warning, until a projection opens with the key (*No projection over a pending change*).

The scenarios follow:
- **The deleted row.** It is live at C ∪ I and not at F, so the delta deletes it.
- **The disappearing edit.** It can't happen, since a pass doesn't apply while the edit is pending.
- **The refused change.** It is reverted in the settle, with the pending change on top.
- **The partial update.** rdb refuses the update whole, and the revert writes rdb's row.
- **The void write.** rdb refuses it, and the revert removes its row.

A crash leaves one of four states, given that a commit that returned is kept (*Crashes*):
- **Before the first append.** Nothing was appended. A recorded hash whose entry rdb doesn't hold is replaced.
- **Between the first append and the settle.** The next cycle finishes the pass from what the outbox recorded, and then handles the rest in a new pass (*Crashes*).
- **After the settle.** The target is consistent.
- **During an apply.** The transaction is atomic.

The delta now starts from C ∪ I, which was never rdb's frontier when a remote entry is concurrent with the chain. Five cases were checked. In each, a target projected to C ∪ I and then incrementally to the frontier equals a fresh full projection at the frontier:
- concurrent updates of one row;
- an own insert, deleted remotely after it;
- an own update, with the row deleted concurrently;
- an own comment, with its post deleted concurrently;
- a chain of two entries, with remote writes concurrent with it and after it.

The planner-parity fuzzer extends this (*Tests*).

## Changes

The authorship parts, here and in *Tests*, are named as they landed, in the earlier shape of *Capture*: the `autoAuthor` setting, and a writer record with each group's mode. HIP-20 changes them, and their tests, before its writer.

1. **dag:** `Dag.append` returns the entry's hash without writing anything, and fires no growth event, when its transaction finds the entry already there (`dag/src/dag.ts:98-112`). An entry's hash is the hash of its header, which holds the payload's hash and the parents' hashes (`:65-80`), so an entry with the same hash is the same entry. Its meta isn't hashed, but rdb derives it from the payload. Every store and both of rdb's scoped DAGs append through it (`mvt/src/dag/dag_nesting.ts:60`, `:188`). The check is atomic only on a store that runs one transaction at a time. The SQLite and IndexedDB stores do (`dag_sqlite/src/sqlite_connection.ts:41-45`, `dag_idb/src/idb_env.ts:108-117`), and don't change. The memory store, `MemDagStorage`, runs them at once (`dag/src/store/mem_dag_store.ts:16-22`), so two appends of one entry could both find it missing. Its `withTransaction` queues its calls instead. Only `Dag.append` calls it, and growth listeners are called without being awaited, so the queue can't deadlock.
2. **rdb:**
   - A table's row ops, `bundle` and `observe` each split into two steps: one builds, signs and validates the entry and returns it with its hash, and one appends it (`rdb/src/rtable/rtable.ts:156-187`, `rdb/src/rtable_group/group.ts:1020-1045`, `:1061-1085`). A prepared row op is appended through its table's scope, which wraps it in a row envelope.
   - The first step takes an option to also validate with the bound groups read through the live observes, at the three places where row-op validation reads a bound group (`rdb/src/rtable_group/validate_ops.ts:390`, `:449`, `:514`).
   - The first step of `observe` always validates. `observe` keeps validating only under `selfValidate` (`:1037`), so its callers see no change.
   - The second step throws when the DAG returns another hash than the prepared one.
3. **rdb_adapter:**
   - **Ingestion** (`ingest_orchestrator.ts`):
     - each group's context from the schema at its checkpoint (`buildGroupContext`, `:61-65`);
     - one entry per change, at the chain's tip, with its hash recorded before the append, and appended without a look first, since the append skips an entry rdb holds (`attempt`, `:456-465`);
     - observes at the chain's tip, to the other group's checkpoint plus its chain, joined with what the group already observes, only when that tip advanced, and never one that would be void. Each is recorded in the outbox row of the change it is written for (`propagate` and `flush`, `:520-560`, `:568`);
     - a refusal's failure event written into its change's outbox row, in place of the list in memory (`recordFailure`, `:417-430`), the planner's refusals and any error while translating the change or building its entry included, and a mark for a change with nothing to write. The catch that makes a failure wraps the planner and the first step only;
     - an error in an append or from the target ends the pass and reaches `onError`, and a pass never goes past a change without a stored record. A hash mismatch at the append halts the projection;
     - a refused insert gives its local id an identity for the rest of the pass. An insert refused before its entry is built still gets a uuid and a row hash, which its failure event carries;
     - an observe that the gate refuses, that would be void, that throws while it is built, or that needs a writer key the projection lacks is declined, in a pass and at the end;
     - an anonymous group ingested and observed with no writer key (`authorFor` and `writerFor`, `:362-373`, `:537`), and the pass stopped at the first change of a group with an identity provider and no writer key;
     - the two readings of each gated binding compared at C, and of every gated binding of a group after each observe appended to it, and the second validation asked for while they differ;
     - groups without a checkpoint left out;
     - reads of rdb at C ∪ I (`:600`);
     - the end of a pass of *The end of a pass*, a function that takes nothing from the pass, also run first in a cycle whose outbox starts with a recorded change, in place of the skips of an insert already applied and a delete of a row not live (`:475-485`). It reads what a refused insert's revert needs from what is stored (*Crashes*);
     - one delta per group whose chain holds an observe, from C to the end of the chain, with each chain's end read as the canonical version of C with the group's recorded hashes that rdb holds, and an observe the end declines not asked for again;
     - with `autoAuthor` off, each insert signed as its captured author column says, its row hash derived from that signer, and op events naming the signer (HIP-20 *Changes* 2);
     - the bundle fallback goes (`submitEntry`, `:497-509`), and so do `reserveMint`, the re-plan after it and claim-or-adopt (`:397-411`, `types.ts:330-347`).
   - **The planner** (`ingest.ts`). One op per change, with no fallback to its columns. `fkBundling` and `updateMerge` leave `AdapterConfig` (`types.ts:89-106`). A readonly or author column that holds rdb's value is set aside, and an insert with an empty author column in a group whose mode is `on` is refused. Its drops become refusals with a kind: an insert on a local id that already has an identity, a second insert of one local id in the pass, a change of a row whose insert was refused, and a change of a table without an rdb table (`:251-268`, `:592-596`, `:461-467`, `:451-453`). A change of a table no member projects gets the mark of nothing to write (`:581`). `mintInserts` mints only for a local id that has no identity, and its read-back of an active mapping's uuid goes (`:270-273`).
   - **The settle.** `IngestSettle` gains each group's new checkpoint and the one it expects, the inserts' mappings, the rows to revert, the rows of the ops whose verdict changed, the json column writes, and the delta's events. The target stores the failure events it reads from the handled outbox rows. Each target writes a reverted row inside the settle's transaction: rdb's row as projection writes it, then the row's remaining outbox changes as capture recorded them, a pending insert on its local id as its whole row. The target finds the local id of an own insert that an observe voided after it writes the inserts' mappings. A refused insert on a local id with an identity reverts to that identity's row and keeps its mapping's status. A row the delta reinstates is written by its row hash. A delta with a schema action halts the projection.
   - **The change source.** It gains `hasPendingChanges()`, and a write of a record into an outbox row: an entry's or an observe's hash, a failure event, or the mark of nothing to write. The target also gains a write of an end's observe beside a group's checkpoint, read with the checkpoints, and cleared by every settle for every group. Neither write moves a checkpoint, so `apply`'s compare-and-set doesn't see it. A record signals no change: SQLite's watcher fires only when the outbox's highest id grows (`rdb_adapter_sqlite/src/sqlite_target.ts:262-275`), and memory and IndexedDB signal only for new outbox rows (`memory_target.ts:206-212`, `rdb_adapter_idb/src/idb_target.ts:124-129`).
   - **`apply`.** It refuses while the outbox holds a change, with `PendingChangesError` next to `CheckpointMovedError` (`types.ts:581`).
   - **`syncDatabase`.** It ingests the members that have a writer key or no identity provider (`:646`), ingests once more at most when the outbox filled up during ingestion, unless the pass stopped, and ends the cycle on `PendingChangesError`. It reports a pass that stopped at a change of a group without a writer key, so that no other cycle is scheduled for it.
   - **The lock** that keeps cycles apart is keyed by the target, and taken by `syncGroup`, `syncDatabase` and `reconcileIndexes` (`ingest_orchestrator.ts:205-219`, `:251`, `:632-635`, `index_reconcile.ts:48`).
   - **The ingest result** is built by the end of a pass from the records.
   - **`ingestChanges` and `ingestDatabaseChanges`** take a target that can be projected to, since they read its checkpoints.
   - **HIP-20's writer record**, its target interface and the table meta's group id, and `autoAuthor` in `AdapterConfig` (HIP-20 *Changes* 2).
   - **`MemoryTarget`.** All of the above. Each checkpoint entry gains the list of the end's observes (`memory_target.ts:91`). `localUpdate` records the columns it is given, `localInsert` fills in the author, and the settle becomes atomic, as its `apply` is.
4. **rdb_adapter_sqlite:**
   - the outbox check inside `apply`'s transaction, and the settle's steps inside `commitIngest`;
   - capture of updates by a `BEFORE UPDATE` trigger and one `AFTER UPDATE OF` trigger per column, in place of today's per-column triggers (`:566-577`), and an `AFTER INSERT` trigger that fills in the author;
   - the outbox gains a column for a change's record, and `rdb_checkpoint` a column for the end's observes (`:455`);
   - the writer record (HIP-20 *Changes* 3);
   - every local id from its table's own counter (*Local ids*): a row the projection writes is inserted first, and its sync row takes the id the insert produced; a reference to a row the target doesn't hold takes the next value of the table's `sqlite_sequence`, whose row is created at 0 for a table that never had an insert. `allocateId` no longer takes the sync table's largest id plus one (`:1023-1031`), which a pending native insert can already hold.
5. **rdb_adapter_idb:** the same. The checkpoint records gain the end's observes (`rdb_adapter_idb/src/idb_schema.ts:149`). A `put` over an existing record captures every column, and the facade reads the writer record in its transaction. The facade's object store gains `patch(fields, { delete })`, which writes the fields it is given, clears the fields it lists, keeps the rest, and captures exactly the fields it names. `fields` holds the record's key, and a field both given and listed is a `DataError`. Today's comparison of a `put` with the stored record leaves capture (`changedColumns`, `rdb_adapter_idb/src/idb_facade.ts:274-286`). The settle updates the index entries of the columns it writes. The outbox records gain the change's record, and the database version doesn't change.
6. **rdb_projection:**
   - a moved checkpoint halts the projection, at the apply and at the settle (`syncWithCasRetry`, `projection.ts:202-212`, goes);
   - a refused projection ends the cycle without an error and schedules the next one;
   - `arm()` starts a cycle when a change is already pending;
   - a cycle that ends in an error schedules the next one, with a backoff of 1 s doubling up to 60 s, reset after a cycle that succeeds. A halt, and a pass stopped at a group without a writer key, schedule none;
   - a hash mismatch at an append halts the projection, as a moved checkpoint does;
   - an `onWarning` option, with `console.warn` when it isn't given, which warns once for each group with an identity provider and no writer key whose change is pending: at open, or when a cycle first meets one. A cycle that stopped at such a change schedules no other;
   - the projection records HIP-20's writer record at open and on each reconfigure (HIP-20 *Changes* 6);
   - the `autoAuthor` option, a default for every member that a per-group override can change. An anonymous group is recorded as `anonymous`, and a group with an identity provider, `autoAuthor: false` and `authorColumn: false` is refused at open and on reconfigure (HIP-20 *Changes* 6);
   - the scope's `fkBundling` option goes (`scope.ts:41-43`, `:127`).
7. **rhost:** `autoAuthor` joins `ProjectionConfig` and its validation (`rhost/src/config.ts:33`, `:195`), and the host passes it to `openProjection` (HIP-20 *Changes* 7, this part only).
8. **rdb_adapter_test:** the suites below.
9. **rdb_adapter_test_gen:** the planner-parity fuzzer's histories gain a chain at an older checkpoint, and its sweep runs the pairs that start at a chain's tip. It needs only the generator and today's projection, so it comes first, before the code it vouches for.
10. **Docs:**
   - the rdb_adapter README describes the invariant, ingestion at the checkpoint, capture, the end of a pass, reverts, the settle and crashes, and drops `fkBundling` and `updateMerge`;
   - the rdb_projection README describes the single projector, the wait for an empty outbox, the halt, the warning for a group without a writer key and `onWarning`, anonymous groups without a key, and `autoAuthor`;
   - the rhost README describes `autoAuthor` in the host's configuration;
   - the SQLite and IndexedDB READMEs describe their capture, and the IndexedDB one `patch`;
   - the dag README says that a second append of an entry writes nothing.

## Tests

- **dag,** on the memory, SQL and IndexedDB stores: a second append of an entry the store holds returns its hash, writes nothing and fires no growth event, and the frontier keeps the entry's children. The same for two appends of one entry at once: both return its hash, and the entry is written once.
- **rdb:**
  - In the scenario of *rdb accepts a write that is void where it stands*, with a rule and with a foreign key: with the option, the local insert is refused, and without it, it is accepted as today. After a live observe of the grant, and after a later observe by an admin, the insert is accepted with the option.
  - G binds K and M, and M's gate reads K. After an observe of K that voids G's observe of M, M's two readings differ, and with the option a write that reads M is refused.
  - The two steps of a row op, `bundle` and `observe` give the entry and the hash that one call gives today, and an ingested change's entry is the plain row op a native write makes. With `selfValidate` off, `prepareObserve` refuses an observe the gate refuses to the writer, and `observe` doesn't, as today.
- **Shared ingestion suite, on all three targets.** After every step of each case, the target equals a full projection at its checkpoint with the outbox applied on top. On IndexedDB the suite updates a row through `patch`, so that every case names the same columns on the three targets. `put` has cases of its own.
  - A row the app inserted, which another device deletes before the next pass, leaves the target, and its mapping is marked deleted. The same holds after a stop between the settle and the next pass.
  - A change captured while a delta is computed makes the apply refuse. The next cycle ingests the change and then projects, and the row never shows the remote value over the pending change.
  - A change captured during ingestion is ingested in the same cycle.
  - A refused change with a later pending change to the same row: after the settle, with no projection in between, the row is rdb's row with the pending change on top. The same holds for a row of an op whose verdict an observe changed.
  - A first update sets `a` and `c`, and a second names `a` and `b`. The first is refused, and the row keeps the second's `a`.
  - An update of several columns with one bad column is refused whole, and the row reads as before it.
  - A native insert that leaves the author column empty reads the writer key's id at once, and an anonymous one keeps `NULL`. An update that names the author and a readonly column with the row's own values lands, and one with other values is refused whole. An insert captured with an empty author column while no writer key was recorded is refused. An update whose named columns are all set aside leaves the outbox at the settle with nothing appended.
  - With `autoAuthor` off, a native insert that leaves the author column empty is anonymous, and one that names the writer key's id is signed. A table projected with `authorColumn: false` signs its inserts with the writer key.
  - On SQLite, json text in another form reads back in canonical form after the settle, and a query that binds its value finds it. When a pending change writes the column again, the settle leaves it to that change.
  - The scenario of *rdb accepts a write that is void where it stands*, with a rule and with a foreign key: a failure event and a revert. After a live observe, the write is accepted.
  - A remote revocation in another group, concurrent there with the grant that an entry of the observing group's checkpoint read, comes into view through the pass's observe. The settle removes the voided row and logs the void event, or writes it with its pending change on top. A removal of the grant that comes after it in that group voids nothing.
  - An observe in the middle of a chain voids an earlier write to a column of a row, and a later write of the same pass sets the column again. After the settle, the row holds the later write. The same holds with two observes in one pass, and when the observe voids one of the pass's own earlier entries.
  - A signed observe written for a change that rdb then refuses, with no later change of that group accepted: the end writes no second observe.
  - The same scenario as in rdb, through ingestion: a pass's observe of K voids G's observe of M, and a later write of the pass that reads M is refused, with a failure event and a revert.
  - No change is dropped. In each of these, the outbox empties at the settle, and a projection then applies:
    - an `INSERT OR REPLACE` over a row from rdb, and over one inserted on this device: a failure event, rdb's row back, and the mapping still `active`;
    - an insert that reuses a deleted row's id, by an explicit id on SQLite and by a `put` on IndexedDB: a failure event, the row removed, and the mapping still `deleted`. Memory can't insert with an explicit id (`rdb_adapter/src/memory_target.ts:274-278`);
    - an insert, a delete and an insert of one local id in one pass, on SQLite and IndexedDB: the second insert is refused, and the mapping ends `deleted`;
    - an update of a row whose insert was refused: a failure event, and nothing written;
    - a change of a table that no member projects: no event;
    - a change whose translation or entry throws an error that isn't a refusal: a failure event with the error's message.
  - An append that throws once ends the pass and reaches `onError`. The next cycle builds the entry again and appends it once.
  - A `writeRecord` that throws once, for an entry's hash and for a failure: the pass ends and reaches `onError`, no later change gets a record, and the next cycle handles each change once.
  - A prepared entry whose appended hash differs from the recorded one halts the projection, and nothing more is appended.
  - On SQLite and IndexedDB, a refused insert, then an insert on its local id, in one pass and in two, and with a crash before the settle: two failure events, no row, and the mapping `ingestion_failure`.
  - A revert of a row with a pending insert on its local id: the target shows the insert's row until the insert is refused.
  - An anonymous group's changes are ingested with no writer key.
  - Two devices update different fields of one row concurrently, each naming only its field: an `UPDATE` on SQLite, `localUpdate` on memory, a `patch` on IndexedDB. Both fields land. An update of one field after a refused update of another lands, and the row shows rdb's value of the refused field with the new one.
  - An observing group whose checkpoint already observes entries of another group newer than that group's checkpoint: the pass's observe joins the two, and rdb takes it. A member group with no checkpoint is neither ingested nor observed.
  - A permission written natively and used right away in another group is accepted, whether the two are ingested in one pass or the use in the next.
  - Crashes:
    - after a chain of several entries, and in the middle of the walk: the next cycle first settles what was appended, with the checkpoint at the crashed pass's last entry, and then ingests the rest. Nothing is appended twice. A change captured after the crash is ingested in the new pass;
    - between recording a hash and the append: the next pass builds the entry again and appends it once;
    - after a refusal, with a later entry appended: the next cycle stores the failure event with its recorded reason and reverts the row. Under another writer key, the refused change stays refused and isn't validated again;
    - after an observe that voids a row of the checkpoint, with a later entry appended: after the next cycle the row is gone and the void event is stored. The same when the observe voids the pass's own earlier entry;
    - between a permission's entry and the observe its use needs: the next cycle writes the observe when it finishes the pass, and the use is accepted;
    - after the end's observes and before its settle: the next cycle finds them beside the checkpoints, writes no observe twice, and the target ends the same. The settle clears them;
    - between recording an end's observe and appending it: the next cycle's end leaves it out, writes it again once, and the settle clears the stale one. The same when the end declines it this time: the settle clears it, and the group's checkpoint stays;
    - an update with nothing to write leaves the outbox at the settle after a crash too.
  - Two devices at the same checkpoint make the same anonymous update. The second device's pass appends the entry rdb already holds, and nothing is written twice. When it crashes after that, the next cycle finds the change handled.
  - A local and a remote update of one column, concurrent: the target ends with the value rdb resolves.
  - A comment on a post deleted remotely, which the app hadn't seen: the comment is accepted, voided, and removed by projection, with a void event and no failure event.
  - A remote schema change that hasn't been projected yet: a local write is translated with the schema at the checkpoint.
  - An observe from an ingested group advances the observed group's version to that group's checkpoint plus its chain.
  - A settle that finds a moved checkpoint throws, and changes nothing.
  - An observe that reinstates a row void at C: after the settle, the target holds the row.
  - An insert captured during a pass whose settle reinstates a row that was never in the target: after the settle the target holds both rows, and the next pass accepts the insert.
  - An observe that voids a delete of the checkpoint: the row is back under its local id, and its mapping is `active`.
  - The whole suite runs once more with every end of a pass left to a new cycle.
- **SQLite:**
  - A native `UPDATE` of several columns is one outbox row with every column it names, changed or not. One statement over two rows gives one outbox row each. An update that names only the id captures nothing.
  - Writing a record into an outbox row, or an end's observe beside a checkpoint, doesn't wake the projection.
  - A native insert after an id was taken for a reference gets a new id, and an id taken for a reference after a native insert that has no mapping yet is another one. A row the projection writes takes the id its insert produced.
- **IndexedDB.**
  - A `put` over an existing record captures every column, and one that leaves out the author keeps it.
  - A `patch` captures exactly the fields it is given, changed or not, and the fields it clears as `NULL`, and keeps the others as stored. A `patch` of a key with no record writes and captures nothing.
- **Planner-parity fuzzer (rdb_adapter).** Its histories gain chains at an older checkpoint, and its sweep checks that an incremental projection that starts at a chain's tip equals a full projection at the frontier, with remote entries concurrent with the chain and after it. Today every checkpoint it records is a frontier (`rdb_adapter_test_gen/src/history.ts:581`). The generator has no bound groups, so the claim that a delta's verdict changes are enough after an observe stays with the hand-built cases.
- **rdb_projection:**
  - a projection refused by a pending change ends the cycle, and the next cycle ingests and then projects, without another write to trigger it;
  - a change written in another tab or process while `open()` runs its first cycle is ingested once the projection is armed;
  - under a steady stream of writes, `sync()` returns, and `stop()` completes;
  - a moved checkpoint halts the projection;
  - opened without a writer key for a group with an identity provider, over a pending change of that group, with changes of a group with a key before it and after it: the projection opens and warns once, through `onWarning`, or `console.warn` without it. A pass ingests the changes before it and stops there, no projection applies, no cycle loops, and reads work;
  - a change of that group captured after an open over an empty outbox gives the same, with the warning when a cycle first meets it;
  - opened again with the key, the change and the ones after it are ingested at the checkpoint they were written against, and a projection then applies;
  - opened without a writer key, an anonymous group's pending change is ingested, and a projection then applies;
  - a group joins while a reactive cycle is held open, and `sync()` is then called: the second cycle waits for the first, and each pending change is ingested once;
  - a group joins while a cycle is held open before its apply, and an index reconcile is then called: the reconcile waits, the cycle applies, the spec is installed, and nothing throws;
  - a cycle that ends in an error is retried after 1 s, then 2 s, without another write to trigger it, and a success resets the delay. A halt isn't retried;
  - the projection records the writer record and each group's mode at open and on reconfigure, and refuses a group with an identity provider, `autoAuthor: false` and `authorColumn: false`.
- **rhost:** the projection config takes `autoAuthor`, and refuses a value that isn't a boolean.

## Considered and not taken

- **Project to the chain's tip right after ingestion, keeping appends at the frontier.** It fixes the deleted row, but rdb still records each write as made after changes the app never saw, and every ingestion takes an extra pass.
- **Rewrite every row the pass touched from rdb at C ∪ I in the settle, a narrow echo.** It would repair any difference between how the target and rdb settle a write, without knowing which ones exist. But parity at the source is the rule, and the few differences left are exceptions with their own handling. A rewrite of every row would hide a target that applies a change differently from rdb, which is a bug the consistency test should find.
- **Check only the rows the delta writes, instead of the whole outbox.** Fewer waits under steady local writes. But a pending change can reference a row the delta deletes, so the check would have to cover referenced rows too. The whole outbox is simpler. HIP-31 records a design that removes the wait.
- **Hold back a revert that would overwrite a pending change**, in a list of held rows that the next projection pass applies. An earlier draft did this. It needed storage on three targets, an exception to the invariant, and two early returns of projection that mustn't skip held rows. Writing rdb's row with the pending changes on top needs none of it.
- **Fall back to an update's columns when rdb refuses it.** It can land a row nobody wrote (*What happens today*).
- **Capture only the columns whose value changed.** A revert that puts the pending changes back would lose a column that a later write named with its current value. The comparison stays available as the `compare` option of HIP-20's writer, and a library above IndexedDB's `patch` can compare before it calls it.
- **Compare an IndexedDB `put` with the stored record, as today.** A `put` doesn't name columns: it repeats the whole record, mostly what the app read. Compared, two devices that edit different fields of a row with a `put` would both land, and a `put` after a refused one wouldn't repeat the refused value. But a `put` replaces a record, as IndexedDB defines it, and a write that replaces every column is a write of every column. The facade keeps that meaning, and `patch` gives code written for distributed updates a write that names only some fields.
- **Keep bundling and merging, made deterministic.** Each would need its grouping fixed before the walk, and a bundle that rdb refuses still falls back by trial. Explicit bundles, which the app asks for, replace both (HIP-20's first open question).
- **Find a crashed pass's entries by building them again, with nothing recorded.** An earlier draft did this. The replay had to make exactly the same entries, so ingestion saved its minted identities before the first append, under a status that hid them from apps, and looked for a signed entry among the tip's children by its payload. A change of the writer key between the crash and the replay made a second chain. The recorded hash needs none of this, for a target write per entry.
- **Resume a crashed pass.** An earlier draft's replay took the entries up to the last change whose recorded entry rdb held, and carried on from there. To carry on, it rebuilt what the crashed pass had kept in memory: the tips, the identities later changes resolve through, the observes due, and the refusals. It took a change before the last recorded one, with no entry, to be refused, and validated it again for the reason, which could differ from the first. It also lost the effects of the crashed pass's observes. Finishing the pass from its records needs none of this.
- **A delta for each observe, computed as the pass appends it.** An earlier draft did this. The deltas lived in the pass's memory, so a crash lost them: the void event was never stored, and a voided row stayed in the target. Each delta also gave rows as rdb had them at the observe, so the rows needed a second rule. One delta from the checkpoint to the end of the chain needs neither.
- **Leave out, at ingestion, a named column that holds rdb's value.** It would turn a form that saves every field into a patch. But an app may need the columns of a write to land together, and a write turned into a patch behind its back loses that. And the state ingestion compares with is not final: it is rdb at C ∪ I, not what the write meets once concurrent entries arrive. An app that wants a patch names fewer columns, with an `UPDATE`, IndexedDB's `patch`, or HIP-20's writer and `compare`. A foreign-key column gets no exception either. An update that names a comment's `post` after the post was deleted is refused, or voided when the delete was concurrent. rdb keeps such a comment, so the target shows it, and an app that edits it names only the columns it changes.
- **Survive the loss of the last commits.** A power failure can lose the last commits of the target or of rdb's store (*Crashes*). Two ways were looked at on 2026-11-22. One makes rdb's store write to disk before each settle, an fsync per pass. The other keeps settled rows in the outbox until rdb's store writes to disk now and then, and redoes the changes rdb lost. Redoing a settle that already ran has to undo its effects: the inserts' mappings, and the rows that an observe's void removed. Storing each entry in its outbox row, so that it can be appended again, would avoid that. Minting an insert's uuid at capture, so that a pass that lost the insert's record makes the same row again, was looked at too. Declined for now: the design assumes a returned commit is kept, and a later design that stores the changed rows in the outbox from the triggers would make recovery simpler.
- **Lock the outbox from the last drain to the apply**, so that steady local writes can't hold the projection back. No native write path can wait for such a lock. On SQLite it would be a write transaction held across ingestion's async work. An app writing through `wrapDatabase` would write inside it, and one on another connection in the same process would block the event loop in better-sqlite3's busy wait, so the pass couldn't finish. An IndexedDB transaction closes when it has no pending request, and the facade's `transaction()` is synchronous, so it can't wait for a Web Lock. Memory's local writes are synchronous. HIP-20's writer could wait, but the native paths stay.
- **Record the end's observes on the last outbox row that has a record.** An earlier revision did this, since the outbox rows were where the pass recorded. But such an observe belongs to no change. And when a crash leaves that row in the outbox past the settle, its record carries the observe into the next pass. The observe belongs with the chain it ends, which the checkpoint stands for.
- **Record nothing for the end's observes.** After a crash during the end, the next cycle would build the same observe again. An anonymous or Ed25519 one comes out the same, and rdb's append skips it. An ML-DSA one would be a second observe of the same version, concurrent with the first. That would be harmless, but the records otherwise keep a signed entry from being written twice.
- **Keep allocating local ids in the sync table, above both its largest id and the app table's `sqlite_sequence`.** It would close the collision of *Local ids*, with two counters kept in step by hand. The table's own counter is one counter, as on the other two targets, and the sync table then never makes up an id.
- **Reserve the id of a referenced row with a placeholder row**, inserted and deleted again. Moving the table's counter needs no row. A placeholder could fail on a `NOT NULL` column without a default, and between its two commits it would show readers a row that doesn't exist.
- **Take every error before the append as the change's failure.** An error from the target's own reads and writes would then revert a write that rdb would take a moment later, and a record that couldn't be written would leave a gap that makes the next pass ingest later changes twice. The pass ends instead, and is retried.
- **Report a group without a writer key in its `IngestResult`, through `onResult`.** It needs no new option, and rdb_repl already prints rejections from there (`formatRejectWarning`). But it would come every cycle, where `onWarning` comes once for each group.
- **Validate every local op with both readings of a bound group.** It costs a verdict evaluation per op, and the two validations differ only while a gated binding's readings differ.
- **Observe a bound group before a group's first entry**, so that a write is checked against the bound group the target shows. HIP-30 records why this was declined.
- **Make the delta delete a row that was inserted and deleted inside its window.** It fixes only the first scenario, and leaves the others.
- **Leave a group whose pass only observed at its old checkpoint**, so that the next projection applies the observe. It covers only a group with no row ops after its observe. One delta from the checkpoint to the end of the chain covers every case.
- **Keep json text as the app wrote it**, so that the row reads the same before and after ingestion. A query that binds the value would then miss the row on SQLite, while it finds the same value in a row from rdb.
- **Record with each captured change the checkpoint it was written at.** No projection applies while a change is pending, so every pending change was written at the checkpoint ingestion finds. There is nothing to record. HIP-31 needs such a record, and takes it at the apply instead of at capture.

## Open questions

1. **How long can local writes hold remote changes back?** An editor that writes on every keystroke could keep the outbox busy while the user types. A refused apply also throws its delta away, and the delta's cost grows with the backlog, so the case to measure is catching up after a long time offline while the app keeps writing. Nothing is measured for now (decided 2026-11-25). [HIP-31](0031-projection-over-pending-changes.md) removes the wait. A lock on the outbox was looked at and doesn't fit the native write paths (*Considered and not taken*).
2. **What do the records cost?** Ingestion writes the target once per entry, once per observe, and once per refused change. The records can't share a write: each entry is built at the one before it, which validation reads from rdb's store. rdb already appends each entry in a store transaction of its own, so on IndexedDB the records roughly double a pass's write transactions. The case to measure is a pass of 1,000 inserts on IndexedDB, with and without the records, in a browser. Like the first question, it isn't measured for now. The tests run on fake-indexeddb, an IndexedDB written in JavaScript over memory, which says nothing about a browser's storage.
   - **If they cost too much**, they can move into rdb's store: a local table, never synced, keyed by the target and the outbox row's id, and written in the transaction that appends the entry. An entry or an observe then needs no write of its own, and the record and the append commit together. So a recorded hash whose entry rdb doesn't hold can't happen, and neither can a lost commit that shows an insert twice (*Crashes*). It needs a record in the append's transaction on the three DAG stores, a stable id for each target, since a replica can feed several, and a cleanup by the end of a pass, which removes the records whose outbox id is below the outbox's first row (outbox ids are never reused). A deleted target leaves its records behind.
   - **When a move would be cheapest.** Neither the outbox nor rdb's store is released, so the move needs no migration now. After a release it needs one for both.
   - **Building several entries ahead** against entries not appended yet would let their records share a write, but every read that validation makes would then go through that layer.
