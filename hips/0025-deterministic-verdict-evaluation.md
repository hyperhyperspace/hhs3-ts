# HIP-25: Deterministic verdict evaluation

- **Type:** design
- **Status:** draft
- **Modules:** dag, dag_sql, dag_idb, mvt, rdb, std_types
- **Requires:** —
- **Superseded by:** —
- **Implemented in:** —

## Summary

Whether an entry is valid can depend on the verdicts of other entries, and working those out is a recursive computation with a depth limit. No verdict depends on the order in which a replica reads its DAG, but the depth the computation reaches does, and that order is local today: it comes from the DAG index the replica uses and from the order in which entries reached it. So two replicas that hold the same DAG can hit the limit on different entries, and one accepts an entry that the other cannot validate. This HIP makes the computation identical on every replica. It fixes the order of every read from the DAG alone, validates each entry in a single evaluation, and moves the depth limit into that evaluation, where reaching it rejects the entry everywhere alike. It changes no verdict, it does not choose the value of the limit, and it does not reduce what a verdict costs to compute.

## Why

### Background: how a verdict is computed

The terms below are used throughout.

- **Verdict.** An entry is *live* or *void* as seen from a position, which is a set of entries: everything at or before them is visible. An entry is void when one of its rules fails as seen from there. The usual case is an author whose permission was revoked by a concurrent entry.
- **Diagnosis.** The computation of one entry's verdict. It runs the entry's rules, and a rule may need the verdicts of other entries: was the grant that authorizes this author itself live, and is the revoke against it live? So one diagnosis starts others.
- **Evaluation.** The object that carries one such computation. It holds the diagnoses in progress and the verdicts already found. A question asked from outside, such as "is this row there?", starts a new evaluation.
- **Depth.** The number of diagnoses in progress in an evaluation at one moment.
- **Cycles.** When a diagnosis needs the verdict of an entry whose own diagnosis is in progress, the evaluation answers *undecided* and later solves the mutually dependent entries together. A computation therefore always ends. The depth limit bounds memory, not termination.
- **Cover walk.** How a diagnosis finds the entries to ask about. To know whether a row is there, it walks the DAG backwards from a position, looking for the latest entries that wrote the row. It asks the evaluation for the verdict of each candidate as it meets it, and it walks past a void candidate to what lies behind it.
- **DAG index.** The walk is done by the replica's DAG index. There are three: the flat index, used by in-memory replicas, and the topo and level indexes, used over SQLite and IndexedDB.
- **Validation.** An entry is validated when it is written locally and when it is received from a peer, at its own position: only the history of its predecessors is visible. Validation reads rows (is the author allowed, is the row there, do the foreign keys point at live rows), and each read may need verdicts.

### The order of reads is local

Three things decide the order in which an evaluation runs its diagnoses, and each depends on the replica.

**The walk order.** Each index visits candidates in its own order.

| Walk | Order of visits |
|---|---|
| Flat index ([`flat_idx.ts`](../modules/dag/src/idx/flat/flat_idx.ts)) | First in, first out, starting from the iteration order of the position it is given, then the store's order of predecessors |
| Topo and level indexes ([`topo_idx.ts`](../modules/dag/src/idx/topo/topo_idx.ts), [`level_idx.ts`](../modules/dag/src/idx/level/level_idx.ts)) | The entry with the highest arrival number first. The number is a counter the store increments as each entry is stored |
| Concurrent cover, shared by the three ([`concurrent_cover.ts`](../modules/dag/src/idx/concurrent_cover.ts)) | First in, first out, as in the flat index |

**Loops over the members of an object.** The foreign-key check visits columns in the key order of the schema object, and a `WHERE` clause visits its fields in the key order of the clause ([`predicates.ts`](../modules/rdb/src/rtable_group/predicates.ts), [`view.ts`](../modules/rdb/src/rtable/view.ts)). The key order of a payload is that of the JSON text the replica stored, and the key order of a resolved schema is the order in which the replica built it.

**Which reads share an evaluation.** Verdicts found earlier in an evaluation are not computed again, so the depth of a read depends on what the same evaluation did before. Validation opens a new table view for almost every read it makes, and each view has an evaluation of its own ([`validate_ops.ts`](../modules/rdb/src/rtable_group/validate_ops.ts)).

### Same DAG, different depth

The following was built on six replicas: the three indexes, each with two arrival orders.

A users group has nine managers from its creation. Eight entries are then written at the same position, so all eight are concurrent:

- Two managers, P and Q, each revoke a third manager, H.
- Five more managers, X1 to X5, form a chain: X2 revokes X1, X3 revokes X2, X4 revokes X3, X5 revokes X4.
- X1 revokes P, and X3 revokes Q.

Whether P's revoke of H counts depends on five other verdicts: X1's revoke of P, then the four revokes along the chain. Whether Q's counts depends on three: X3's revoke of Q and the last two of the chain. Read in full, both revokes of H are void and H remains a manager.

The six replicas hold the same eight entries, with the same hashes, and all six answer that H is a manager. The depth the evaluation reached was not the same:

| Index | P's revoke arrived before Q's | Q's revoke arrived before P's |
|---|---|---|
| Flat | 6 | 5 |
| Topo | 5 | 6 |
| Level | 5 | 6 |

The walk that visits P's revoke first goes down the whole chain, six deep. The walk that visits Q's revoke first goes four deep, and when it then reads P's revoke the end of the chain is already known.

With the limit lowered to 5 on all six replicas, H wrote one entry on a replica that reaches depth 5. That replica and two others accepted it. On the other three, the validation of the same entry failed at the limit. Among them are two in-memory replicas that differ only in arrival order, and an in-memory replica and a level-index replica with the same arrival order.

The limit is 10,000 today, so the structure needed to reach it is larger. The mechanism is the same at any value.

### The limit itself depends on load

The limit is a counter on the group object ([`group.ts`](../modules/rdb/src/rtable_group/group.ts)). It counts the diagnoses in progress in every computation the group is running, so several computations at once add up, and whether it fires depends on what else the replica is doing.

When it fires during the validation of a received entry, it is thrown as a plain error. The synchronizer reads a plain error as a transient failure, keeps the entry, and tries again on its next pass ([`synchronizer.ts`](../modules/sync/src/synchronizer.ts)). The entry is never rejected.

The counter was added for a different purpose, described under *The lost-evaluation case* below.

## Design

After this HIP, the sequence of diagnoses an evaluation runs, each with its depth, is a function of the question and of the DAG. Four rules give that.

### Rule 1: one walk order

The **height** of an entry is 0 when it has no predecessors, and otherwise one more than the greatest height among its predecessors. It is the length of the longest path from the entry back to the start of the DAG.

A cover walk always visits next, among the entries waiting, the one with the greatest height. Among entries of equal height it visits the one with the smallest entry hash.

Every index stores the height of each entry when the entry is stored. The order in which the starting position is iterated, the order of predecessors and the arrival number no longer affect a walk.

An entry is always higher than each of its predecessors, so a walk still visits an entry before anything the entry comes after. That is the order the topo and level indexes use today, with the ties between concurrent entries now broken the same way everywhere.

### Rule 2: one iteration order

Wherever a diagnosis or a validation loops over a collection and the loop can ask for a verdict, the order is fixed:

- **Members of an object** (the columns of a table, its foreign keys, the fields of a `WHERE` clause): by key, in the order the hash input sorts keys (dag spec §2.2).
- **A set of entries or of rows**: by hash or row id, ascending.
- **A list in a payload or a schema** (the writes of a bundle, the members of an `AND` or an `OR`): in list order.
- **The candidates of a walk**: in the order of Rule 1.

### Rule 3: one evaluation per validated entry

The validation of an entry creates one evaluation, and every read the validation makes uses it. The reads come in a fixed order.

For an entry that writes one row:

1. The author's key, read from the identity table the group names, which may belong to another group.
2. For an update or a delete, that the row is there. For an insert, that the row id is unused, which needs no verdict.
3. The table's restriction for this kind of write.
4. The foreign-key targets, column by column, in the order of Rule 2.

For a bundle, the author's key is read once, and steps 2 to 4 are then done for each write in the bundle's order. An observation of another group and a schema deploy keep the order of reads they have today, and the reference docs record it.

A view that an application opens keeps one evaluation for as long as the view is used, as today. Rules 1 and 2 apply to its reads too.

### Rule 4: the limit is counted in the evaluation

The depth of an evaluation counts every diagnosis in progress in it, including the ones run again while a cycle is being solved. The limit is a constant of the protocol. A diagnosis that would be one more than the limit is not started, and the evaluation fails.

- **In validation**, the entry is invalid, and the reason names the limit. A local write fails with that reason. A received entry is rejected like any other invalid entry, and is not tried again. The outcome depends only on the entry and its history, so it is the same on every replica and does not change later.
- **In a read**, the read fails with an error that names the limit.

Reaching the limit never produces a verdict. It does not make an entry live or void.

The value must be low enough that every supported runtime can reach it. A replica that runs out of memory first fails at a point that depends on the device. The value is an open question.

### The lost-evaluation case

A diagnosis must ask for other verdicts through the evaluation it was given. If some code inside a diagnosis asks through a new evaluation instead, the new one does not know which diagnoses are in progress, and on a cycle the two entries diagnose each other through an endless chain of new evaluations. No evaluation in that chain is deep, so a limit counted in the evaluation cannot see it.

This is an error in the code, not something data can cause. The group counter exists to turn it from a hang into an error. This HIP removes the counter, because it is what makes the limit depend on load, and keeps the error out in two ways:

- The rule stays in the reference doc: never create an evaluation inside a diagnosis.
- The test suites check it. While a diagnosis is running, creating an evaluation is an error. The check uses the asynchronous context tracking that Node provides, so it runs in the tests and not in browsers.

In the rdb and std_types suites today, 244,959 evaluations are created and none is created inside a diagnosis.

### Recorded runs

An evaluation can report each diagnosis it starts, with its key and depth, to a listener that tests install. The list of reports for one question is its *recorded run*. Two replicas agree when their recorded runs are equal.

A few small DAGs are kept in the repo with the recorded run of each validation. An implementation in another language has to reproduce them.

## Why it is sound

**No verdict changes.** The engine computes the same verdict whatever the order of reads: an entry met while its diagnosis is in progress is answered as undecided and solved with its cycle, and verdicts are kept only for cycles solved in full. Existing tests ask for the same verdicts in different orders and compare them. In the six-replica case above, the order of reads differed and the answer did not.

**No walk result changes.** A cover walk returns the latest accepted candidates that can be reached without passing through an accepted one. That set depends on which candidates are accepted, not on the order of visits.

**Height is the same everywhere.** It is defined from an entry's predecessors alone, and a replica that holds an entry holds its predecessors.

**The order is total.** Two entries waiting in a walk differ in height or in hash, so the next visit is always determined.

**So the run is determined.** The question determines the first diagnosis. What a diagnosis reads is determined by its entry, by the schema and rows at its position, which are themselves verdicts and values that do not depend on order, and by the iteration orders of Rules 1 and 2. The engine adds no order of its own: it follows the order in which diagnoses ask, and where it chooses, it uses that order or sorted keys. By induction over the run, the sequence of diagnoses and their depths is a function of the question and the DAG.

**Validation depends only on the entry and its history.** It reads at the entry's own position, where nothing concurrent is visible, in one new evaluation, in a fixed order. Entries the replica holds outside that history are never visited.

**The lost-evaluation case stays out.** It needs a call that the rule forbids, and the test check fails the suite when one is added.

## Changes

In the order they land.

1. **dag: height.** Each index store records the height of an entry when it indexes it: the in-memory stores of the flat, topo and level indexes, and the SQL and IndexedDB stores (`sql_schema.ts`, `sql_topo_index_store.ts`, `sql_level_index_store.ts`, `idb_schema.ts`, `idb_topo_index_store.ts`, `idb_level_index_store.ts`). The level index's `distanceToARoot` is the shortest distance and stays as it is. Stores created before the change are recreated.
2. **dag: walk order.** `findCoverWithFilterUsingFlatIndex`, `findCoverWithFilterUsingTopoIndex`, `findCoverWithMetaUsingLevelIndex` and `findConcurrentCoverUsing` take the next entry from a queue ordered by height, then hash. `ConcurrentCoverOps` gains the height lookup.
3. **mvt: the limit and the listener.** `VerdictEvaluation.run` counts depth and throws a `VerdictDepthExceeded` error past `MAX_VERDICT_DEPTH`. A listener, set by tests, receives each run and each evaluation created. The folds in `refs.ts` that build the list of pending observations iterate by Rule 2.
4. **rdb: iteration order.** Sorted keys in `explainRowOpFKReach`, `fkTargetsLive`, `resolveRow`, `rowMatch` and `query`, and in whatever else the tests of this HIP find.
5. **rdb: one evaluation in validation.** `validateTableGroupPayload` creates the evaluation, and `validateRowEnvelope`, `validateBundle`, `validateRowOpRestrictionAt`, `validateObserveGate` and `validateDeploy` build their views on it, through the constructors that diagnosis already uses. The catalog's validation does the same. A `VerdictDepthExceeded` becomes a validation failure.
6. **rdb: remove the group counter.** `_verdictInflight`, `enterVerdictFrame`, `exitVeredictFrame` and `VERDICT_MAX_INFLIGHT` go.
7. **std_types.** RCap's walks take Rule 1 from the dag module. Its validation uses one evaluation, and its loops are checked against Rule 2.
8. **Test support.** The mock context of the rdb tests takes the index to use, so a suite can run on the flat, topo or level index.
9. **Docs.** `dag/SPECS.md` §5 defines height and the walk order. `mvt/WELL_FOUNDED_VERDICTS.md` §5 and §6 and `rdb/VOID_SEMANTICS.md` §4 describe the limit, the order of reads in validation and the test check, in place of the counter.

## Tests

- **Equal runs across replicas.** The eight-entry case above, and cases with a cycle, a bundle, an observation and a foreign-key chain, are each built on the three indexes with several arrival orders. The recorded run of every validation is equal on all of them. This fails today.
- **Equal outcome under the limit.** With a low limit, the entry H writes in the case above is accepted by every replica or rejected by every replica.
- **Height.** A merge of a long branch and a short one takes the height of the long branch plus one. The height of an entry is the same in the in-memory, SQL and IndexedDB stores and for every arrival order.
- **Walks.** The existing dag suites pass unchanged, which shows the walk results did not change.
- **The limit.** A chain that reaches the limit exactly is answered, and one more level fails. A received entry whose validation reaches the limit is rejected and not retried. A read that reaches it fails with the named error. Diagnoses run again while a cycle is solved are counted.
- **Warm and new instances.** A group object that has already answered other questions gives the same recorded run as a new one. This catches a cache that outlives an evaluation.
- **The lost-evaluation check.** A test plants a diagnosis that creates an evaluation and sees the check fail it. The rdb and std_types suites run with the check on.
- **Vectors.** The stored DAGs and their recorded runs are replayed. Each vector carries the signing keys that wrote its entries, so the entry hashes are reproduced.

## Considered and not taken

- **Entry hash alone as the walk order.** It needs no stored field. But whoever writes an entry can choose where it sorts by trying variants of the payload, and so can steer the order of evaluation. Height can only be raised, and only by building a longer chain.
- **The level index's `distanceToARoot`.** It is the shortest distance to the start, it exists only in the level index, and it can be lowered at will by merging in a short branch. A merge can rank below its own predecessor.
- **A counter shared between computations,** on the group or per entry. Its result depends on how many computations run at once. A limit has to depend on one computation only.
- **Treating "too deep" as void.** Depth is counted from the question. In a test with 64 managers who each revoke the next, and a limit of 65, one question about those entries was answered at depth 64 and another failed at depth 66. As a verdict, the same entry would be void for one reader and live for another.
- **No limit.** Memory grows with depth: an evaluation held 39 MB at depth 512. Without a limit a large structure fails where the device runs out of memory, which differs between replicas.
- **Keeping validation's several evaluations and specifying where each begins.** The boundaries follow from where the code happens to open a view. Another implementation would have to copy them.
- **A check for the lost-evaluation case in production.** It needs asynchronous context tracking, which browsers do not have. The alternative is a shared counter, excluded above.

## Open questions

1. **The value of the limit.** It bounds three things at once, measured here on the in-memory store of one machine. Both cases in the table have N concurrent entries and reach a depth of about N.

   | N | N managers each revoke the next: one later write | N managers each revoke the same manager: one later write |
   |---|---|---|
   | 16 | 0.07 s | 0.24 s |
   | 64 | 0.5 s | 4.9 s |
   | 128 | 2.1 s | 33 s |
   | 256 | 6.5 s | 3 min 51 s |
   | 512 | 29 s | not measured |

   - **Time.** The table shows the time of one write, a grant, by a manager whose permission depends on the N entries. Each of the N entries is accepted when it arrives, at depth 1.
   - **Memory.** The first case held 39 MB at depth 512.
   - **Honest data.** A chain of rows that each refer to the previous one through a foreign key reaches a depth of its length plus one, so the limit is also the longest such chain.

   With these costs, a limit that keeps one write under a second is near 32. The costs are expected to change, so the value should be set after that work.
2. **Reaching the limit is still a way to block a user.** Someone with the right to write entries that are each valid alone can build a structure deeper than the limit behind another user's permission. Every later write by that user is then rejected, on every replica, for good. This HIP makes that outcome uniform and immediate. Preventing it needs cheaper verdicts or a rule that removes such structures, which is a separate proposal.
3. **[HIP-22](0022-concurrent-removals.md)** removes the read that makes concurrent removals of one target depend on each other. That read turns the N revokes of the second column into one cycle of N members, which is the likely reason that column grows so fast; this was not tested. If HIP-22 lands, the recorded runs change and the stored vectors are regenerated.
4. **[HIP-17](0017-verdict-cache.md)** proposes keeping verdicts between computations. A kept verdict shortens a later run, so the depth would depend on what the replica computed before. The cache would have to stay out of validation or record the depth of what it keeps.
5. **Code not read for this draft:** the gate of an observation and its pending observations, views of another group, the catalog's validation, RSet and RFileMap, and the two memos kept on the group object. The tests above decide what else needs Rule 2.
6. **Reads through a view.** A view keeps its evaluation, so whether one of its reads reaches the limit depends on what the view read before. The effect is local to the application. A new evaluation for each read would remove it and cost the reuse of verdicts.
