# HIP-30: Skew between groups in a projection target

- **Type:** design
- **Status:** deferred
- **Modules:** rdb_adapter, rdb_adapter_sqlite, rdb_adapter_idb, rdb_projection
- **Requires:** [29](0029-checkpoint-consistency.md)
- **Superseded by:** —
- **Implemented in:** —

## Summary

A projection target shows each group at its own checkpoint. A group's rules and foreign keys read another group at the version the group has observed. Nothing ties these two versions together, so an app can write against rows the target shows and have the write checked against a different version of them. The write is then refused though the target showed it as valid. This HIP records the problem, why it can't be closed completely, and a design that would close most of it: project the groups together, each to a version that agrees with what the groups reading it have observed. Nothing is proposed for now. A projection keeps the skew, and the first step, a failure that names it, can land alone.

## Why

### Background: bindings, observes and checkpoints

- **Bindings.** A group can *bind* other groups. Its rules and its foreign keys can then read their rows. Below, A is the *binding* group and B the *bound* group: comments in A, with a foreign key to posts in B.
- **Observes.** A reads B at a version, never at B's newest state. An *observe* is an entry of A that names a version of B. A write of A reads B at the version that the observes before it give. That is the version A *observes* at that point.
- **Gates.** A binding can have a gate, `canObserve`, a rule that says who may write an observe of it. An author the gate refuses can't advance the version A observes.
- **Target and checkpoint.** A projection copies a replica's groups into a relational target. For each group it stores a *checkpoint*, the version of the group the target shows. A projection pass applies rdb's changes since the checkpoint and moves it, in one target transaction per group, bound groups first (`rdb_adapter/src/ingest_orchestrator.ts:305-312`).
- **Local writes.** With HIP-29, ingestion appends a local write at its group's checkpoint, the version the app saw, and no projection pass applies while a local change is waiting to be ingested.
- **Who observes.** Ingestion writes an observe of B from A after it has itself appended to B in the same pass (`ingest_orchestrator.ts:520-560`). Other devices' observes arrive through sync. Nothing else in a projection moves the version A observes.

### The problem

The target shows B at B's checkpoint. A write of A is checked against B at the version A's checkpoint observes. The two can differ in either direction.

**The target shows more of B than A observes.**

1. A post made on another device arrives, and projection applies B. The post is in the target.
2. A's checkpoint holds no observe of it yet.
3. The user comments on the post. A change is now pending, so no projection pass applies, and ingestion appends the comment at A's checkpoint.
4. rdb checks the comment's foreign key reading B as A observes it. It finds no post and refuses the comment.

Step 2 happens in three ways:

- **Between two applies.** B's apply and A's are separate transactions. A write that lands between them is ingested before A's apply.
- **Between two syncs.** B's entries and A's observe of them are entries of different groups, and sync can deliver the first before the second.
- **Nobody observed.** The device that wrote the post does not project A, or its author fails A's gate. A then observes the post only when some other device writes an observe.

The first two pass by themselves. The third lasts until someone observes. Before HIP-29, ingestion appended at A's frontier and so found an observe as soon as it reached rdb, projected or not. Appending at the checkpoint makes the first case more likely.

**A observes more of B than the target shows.**

1. Another device deletes a post and observes the delete from A. A's observe arrives and is projected. B's delete is not projected yet.
2. The target still shows the post, and the user comments on it.
3. rdb checks the comment reading B as A observes it. The post is deleted there, and the comment is refused.

In both directions the target and rdb agree on every row. What is wrong is the refusal: the app showed the user a state in which the write was valid.

A third form involves void observes. An observe can be *void*, for example when its author lost the right to observe concurrently. rdb admits a write counting every observe and evaluates it counting only the live ones, so A has two readings of B, and a void observe leaves them apart until someone observes again. HIP-29 refuses a local write that the two readings judge differently. Which reading the target should follow is part of this problem.

These cases are reasoned from the code. None has been run. Taking this HIP up starts with a test for each.

### Why it can't be closed completely

- **A write and its observe are two entries.** B's entry and A's observe of it live in two DAGs. They are appended, synced and applied separately, so no step makes them appear together everywhere.
- **The local writer may not be allowed to observe.** With a gate on the binding, ingestion can't always bring A up to what the target shows of B.
- **The app's own writes are already shown.** A local write to B is in the target at once. When the writer can't observe it from A, A stays behind its own device's B.
- **Several groups can bind one.** When A1 and A2 bind B and observe it at different versions, one copy of B's tables can't match both.

## Design

A sketch, in the order it could land.

**1. A failure that names the skew.** When rdb refuses a write of A, ingestion compares the version of B that A observes at that point (`resolveObservedForeignVersion`, `rdb/src/rtable_group/group.ts:783`) with B's checkpoint. When they differ, the failure event says that the write was checked against an older or a newer version of the bound group than the one shown. The comparison runs only on a refusal.

**2. One transaction for a cycle's applies.** Every group's delta and checkpoint are applied in one target transaction. This removes the window between two applies and nothing else.

**3. Projecting to versions that agree.** The pass picks a version for each group before it applies anything:

- **From the binding groups down.** A group that no projected group binds is projected to its frontier.
- **The observable part.** A bound group is projected up to the version its binding group observes, at the version that group is being projected to. Its own local entries are always included, since the target already shows them.
- **Several binding groups.** The bound group is projected up to the most advanced of them.
- **Together.** All of it is applied in one transaction, as in step 2.

With one binding group per bound group, the target then shows B at exactly the version A's checkpoint observes. With HIP-29's rules, a write of A is checked against the B the app saw.

What step 3 costs and leaves open:

- **Entries nobody observed are not shown.** Normally the device that writes B observes it from A in the same pass, and the lag is short. Entries whose author never observes from A stay out of the target on every device, with no sign of it. Readers and writers of B itself then work on an older B.
- **Local writes that can't be observed.** The skew stays for them until someone with the right observes.
- **Binding groups that lag.** With several binding groups, the ones behind the most advanced keep the first form of the skew.
- **The targets.** They need an apply that spans groups. Deltas to a version that is not the frontier already exist (`computeDelta` takes any version).

## Considered and not taken

- **An observe before a group's first entry.** Ingestion would observe B at its checkpoint from A before appending to A, whenever A doesn't observe that much yet. This closes the first form exactly, because no projection pass runs while a change is pending. Declined on 2026-11-20: it writes an observe because of remote entries, the local writer may not have the right, and two devices that race write one each.
- **The least advanced binding group as the limit.** One group that is rarely written would hold B's tables back for all the others.

## Open questions

1. **Hidden entries.** Is it acceptable that a bound group with its own screens shows only what the groups binding it have observed? It may need to be a choice per binding.
2. **Who observes.** Entries whose author never observes from A stay unobserved under every design here. This meets the rule HIP-14 deferred, a replica that observes again in response to remote changes.
3. **Which reading.** With a void observe, should the target follow the version every observe gives, or the one the live observes give?
4. **Bindings in a cycle.** Step 3 orders groups from binding to bound. Two groups that bind each other have no such order.
