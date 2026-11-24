# HIP-31: Projection over pending changes

- **Type:** design
- **Status:** deferred
- **Modules:** rdb_adapter, rdb_adapter_sqlite, rdb_adapter_idb, rdb_projection
- **Requires:** [29](0029-checkpoint-consistency.md)
- **Superseded by:** —
- **Implemented in:** —

## Summary

With HIP-29, a projection pass applies only when no local change is waiting to be ingested. Remote changes wait for that, and while an app keeps writing they can wait for long. This HIP records a design that removes the wait. A pass applies over pending changes and puts them back on top of the rows it writes. Ingestion appends each change at the checkpoint it was written against, so rdb treats it as concurrent with what the pass applied. The settle then corrects the rows where that changed the outcome. The design was checked against rdb with a simulated target, and it held. Its price is that a write which depends on a pending change is refused, instead of accepted and voided, when that change loses to a remote one. Nothing is proposed for now. HIP-29 keeps the wait.

## Why

### Background: checkpoints, pending changes and the wait

- **Target and checkpoint.** A projection copies a replica's groups into a relational *target*: SQLite, IndexedDB or memory. For each group, the target stores a *checkpoint*, the version of the group it shows. A *projection pass* computes rdb's *delta* from the checkpoint to the group's newest entries. It applies the delta and the new checkpoint in one target transaction, the *apply*.
- **Pending changes.** When an app writes a projected table, the target records the change in its *outbox*, one outbox for all groups, in commit order. A change is *pending* until ingestion has handled it.
- **Ingestion and the settle.** *Ingestion* turns each pending change into an entry and appends it to rdb. With HIP-29 it appends the first entry at the group's checkpoint, the version the app saw, and each next entry after the previous one. The pass's entries form a *chain*. Ingestion then *settles*: in one target transaction, it removes the changes from the outbox, reverts what rdb refused, and moves the checkpoint so that it holds the chain. A *revert* sets a row back to what rdb holds.
- **Concurrent entries.** Two entries are *concurrent* when neither is before the other. Between concurrent writes to a column, the entry with the larger hash wins. A concurrent entry can also *void* an entry rdb accepted, for example a comment whose post was deleted concurrently. The app learns of it through an *op event*.
- **HIP-29's invariant.** After every target transaction, a group's tables are rdb's rows at the checkpoint, with the outbox's changes on top.
- **The wait.** A projection pass applies only when the outbox is empty. The check is made inside the apply transaction. When a change is pending, the apply refuses, the cycle ends, and the next cycle ingests the change before it projects.

### What the wait costs

- **Remote changes are held back.** While a local change is pending, nothing from other devices reaches the target.
- **They can starve.** A cycle ingests at most twice and then ends, so that it always returns. Under writes that never pause, the apply can be refused cycle after cycle.
- **It has its own machinery.** The refusal is an exception that must not count as an error. The cycle that meets it schedules the next one itself. A projection checks for a pending change when it starts listening. A projection that has no writer key, and so can't ingest, reports an error instead of waiting forever.

### Why HIP-29 waits

HIP-29's settle moves the checkpoint without applying the chain to the target a second time. That is right because every entry of the chain is after everything the target shows, and a row op that is after everything changes only the row it writes. A pass that applies over a pending change leaves two places to append the change, and each breaks something:

- **At the new checkpoint.** The change then counts as written after remote entries the app hadn't seen. It wins over them by position, and a write to a row they deleted is refused.
- **At the old checkpoint.** The change is then concurrent with part of what the target shows. It can lose to a remote write, be voided, or void a row the pass applied, and the target shows none of that.

This HIP takes the second, and corrects the difference in the settle.

## Design

An *overtaken* change is a pending change that a projection pass applied over. A change's *own checkpoint* is the checkpoint its group had when the change was written.

### What this assumes of HIP-29

Two revisions of HIP-29 that came out of its review on 2026-11-20, and are not in its text yet:

- **A revert replays.** A revert writes rdb's row with the row's pending changes on top, in commit order. For that, capture records every column a change wrote.
- **Entries are found by hash.** Ingestion writes an entry's hash into its outbox row before it appends the entry. A replay after a crash looks for that hash.

### 1. The apply records the checkpoint it replaces

- **The record.** An apply that finds the outbox non-empty stores, in its transaction, the outbox's highest id together with the group's checkpoint before the apply. Outbox ids follow commit order, so every pending change with an id up to that one was written before the apply.
- **A change's own checkpoint.** It is the checkpoint stored in the first record of the group whose id is at least the change's id. A change with no such record was written against the current checkpoint, and is not overtaken.
- **Two applies with no write between them.** The second finds a record with the same highest id and stores none. The changes up to that id were written against the older checkpoint.
- **Removal.** The settle removes the records up to the last id it drained.
- **Capture doesn't change.** Nothing is added to an outbox row or to the cost of a write. The records exist only while changes are pending.

### 2. The apply never refuses

- **A row with pending changes.** For each row the delta writes, the apply writes rdb's row with the row's pending changes on top, in commit order. This is the operation a revert performs.
- **A row the delta removes** stays removed. A pending update of it then does nothing.
- **A pending insert's row** is not in rdb, and no delta touches it.
- **One computation.** The shared core computes the row from rdb's row and the outbox, so the three targets write the same result. The apply reads the outbox inside its transaction, as the wait's check does today.
- **The invariant holds as written.** The tables are rdb's rows at the new checkpoint, with the outbox on top.
- **What it no longer means.** With the wait, the target also shows what rdb will hold once the pending changes are ingested, as long as rdb accepts them. For an overtaken change, that is true only if the change wins.

### 3. Ingestion appends at a change's own checkpoint

- **The chain.** The pass's first entry for a group is appended at the first change's own checkpoint. Each next entry is appended after the previous one, joined with its change's own checkpoint when that is a later one. The entries still form a chain.
- **Overtaken changes come first.** No apply runs during ingestion, and outbox ids follow commit order. So the overtaken changes are the first part of the chain, and the changes written since the last apply are the rest.
- **Schema.** An overtaken change is translated with the schema at its own checkpoint. rdb holds the schema at any version.
- **Observes of other groups.** An observe names the other group's checkpoint as of the change, found through that group's records, plus that group's chain so far.
- **The new checkpoint.** It holds the old checkpoint C and the chain, as in HIP-29. When the chain ends with an overtaken entry, that entry is not after C, and the checkpoint has more than one newest entry.

### 4. The settle corrects overtaken changes

- **One delta.** The settle computes the delta from the checkpoint C to C joined with the last overtaken entry. It covers the whole overtaken part of the chain, its observes included. The settle stores the delta's op events.
- **The rows.** The settle rewrites every row that delta touches, and every overtaken change's own row. Each is written as rdb's row at the new checkpoint with the changes still pending on top, and removed when rdb no longer has it.
- **Why the change's own row.** When an overtaken change lost or is void, rdb's row is the same at both ends of the delta, and the delta doesn't list it. The target still shows the change.
- **The void event.** An overtaken entry that is void at the end of that delta was never live inside it, so the delta reports no change of verdict for it. The settle reads the verdict of each overtaken entry at the delta's end, and stores a void event for each void one.
- **The rest of the chain** settles as in HIP-29, with no second application.
- **A pass with no overtaken change** is HIP-29's pass, unchanged.

### What goes from HIP-29

*No projection over a pending change* goes: the check in the apply, the refusal and its exception, the cycle that schedules the next one, the second ingestion in a cycle, and the check when a projection starts listening.

### What it costs

- **A refusal where the wait gives a void.** Between an overtaking apply and the settle, the target shows a pending change as if it had won. A write made in that time which depends on it is checked against rdb, where the change may have lost:
  1. The user comments on a post.
  2. A pass applies a remote delete of the post. The comment still shows, since it is pending.
  3. The user replies to the comment.
  4. Ingestion appends the comment at its own checkpoint. rdb accepts it, and it is void, since its post was deleted concurrently.
  5. Ingestion appends the reply after it. rdb refuses the reply, because the comment it references is not live.

  With the wait, step 2 happens after ingestion. Both the comment and the reply are accepted, and the next pass voids both. The rows end up the same. The app gets a failure for the reply instead of a void event.
- **An exception to HIP-29's rule.** HIP-29 checks a write against what the app saw. Here that holds except for a write that depends on an overtaken change that lost.
- **The checkpoint can have several newest entries**, where HIP-29's has one.
- **More in the settle.** It gains a delta and a rewrite of rows, for passes that have overtaken changes.

## Why it is sound

A sketch. It takes HIP-29's argument and adds the two new steps.

**An apply over pending changes.** Before it, the target is rdb at C with the outbox on top. The delta from C to the new version F lists every row that differs between the two. The apply writes each of those as rdb's row at F with its pending changes on top. Every other row is the same at C and at F, and keeps its pending changes. So the target is rdb at F with the outbox on top.

**The settle.** Write C for the checkpoint, P for the last overtaken entry, and N for C joined with the whole chain.

- **Rows the settle rewrites.** They are written as rdb has them at N, with the changes still pending on top.
- **The other rows.** No overtaken change writes them, and the delta from C to C joined with P doesn't list them. So they are the same in rdb at C and at C joined with P, and the target holds them as rdb at C does, apart from what the rest of the chain wrote. Every entry of the rest of the chain is after C and after P. By HIP-29's argument, such a row op changes only the row it writes, and the target already applied it the way rdb settles it.

So after the settle, the target is rdb at N with the outbox on top.

## What was checked

Run on 2026-11-20 against the working tree, with rdb and the posts and comments fixture of rdb_adapter_test: a comment references a post, and can reference a parent comment. The target was a map of rows that followed the rules above. None of the three targets ran. In each scenario, a remote entry is appended, the map is set to rdb's rows with the pending changes on top, the changes are appended at their own checkpoints, and the settle's rewrite is applied. The map is then compared with rdb at the new checkpoint. It was equal in every scenario.

| Pending local changes | What the pass had applied | What happened |
|---|---|---|
| A delete of a post | A remote comment on the post | The delta reports the comment voided and removes its row |
| A comment on a post | A remote delete of the post | rdb accepts the comment. It is void, and the delta reports nothing for it. The rewrite of its own row removes it |
| An update of a title, with four different values | A remote update of the same title | The local value won twice and the remote one twice |
| An update of a post | A remote delete of the post | The row is gone |
| An insert of a post. After the apply, a comment on it, and a comment on the remote post | A remote insert of another post | All three accepted. Only the overtaken insert's row is rewritten |
| An update of a title. After the apply, another update of it | A remote update of the same title | The later local update wins, being after both |
| An insert of a post and a comment on it. After the apply, an edit of the comment | A remote update of another post | All three accepted |
| A comment on a post. After the apply, a reply to the comment | A remote delete of the post | The reply is refused, as under *What it costs* |
| A delete of a post. After the apply, an edit of the remote comment | A remote comment on the post | The edit is refused: the comment is void once the delete is appended |

The rule was also run on generated histories of one group, with schema changes and concurrent branches. In 128 cases, a row op was appended at an older recorded version and compared at a newer one: 51 inserts, 36 deletes and 41 updates. A schema change lay between the two versions in 110 of them, and the op was void at the newer version in 32. In no case did a row differ between the two ends that was neither the op's own row nor a row the delta lists. Only one of the cases changed another row, so that part rests on the first scenario of the table.

Taking this HIP up starts with these scenarios as tests on the three targets.

## Considered and not taken

- **A number in each outbox row** that says which checkpoint the change was written against. Capture would read the checkpoint on every write, on each target in its own way. The record at the apply costs nothing until a pass applies over a pending change.
- **Append an overtaken change at the current checkpoint.** rdb would record it as written after remote entries the app hadn't seen, which is what HIP-29 set out to stop.
- **Smaller passes.** A pass that applies toward the newest entries in bounded steps, or an ingestion that drains a few changes at a time, makes each wait shorter. Under steady writes a change is still pending at most applies.
- **Refuse only when the delta writes a row that has a pending change.** HIP-29 lists it. It gives fewer waits and keeps all of the wait's machinery.

## Open questions

1. **The refused write.** Is a failure acceptable where the wait gives a void event? The failure could at least say that the write depended on a change that lost.
2. **The targets.** How each one writes a row with its pending changes on top, inside the apply. Nothing of this ran.
3. **Schema changes.** A pending change to a column or a table that the overtaking apply drops or renames. rdb accepts the entry at the change's own checkpoint. What the target does with the pending change is not worked out.
4. **Mappings.** A pending change to a row that the apply removed. The row's mapping is then marked deleted, and ingestion still needs its row hash.
5. **Observes.** The rule for observes of other groups in the overtaken part of a chain is reasoned, not run.
6. **Passes that can't ingest.** A projection without a writer key would apply over pending changes that never leave the outbox. And a cycle today refuses a group's first projection while any change is pending. Whether either refusal stays is open.
7. **HIP-30.** Its scenarios of skew between groups assume the wait. Here a pass can move a bound group's tables under a pending write of the group that binds it. Whether that widens the skew is not worked out.
