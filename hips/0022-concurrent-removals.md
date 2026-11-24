# HIP-22: Concurrent removals of one target

- **Type:** design
- **Status:** draft
- **Modules:** rdb, std_types, mvt
- **Requires:** —
- **Superseded by:** —
- **Implemented in:** —

## Summary

When two entries concurrently remove the same thing, and each one's rule reads that thing, each reads the other's removal as a concurrent barrier and fails. The two form a cycle that the well-founded solve collapses to void, so nothing is removed. An owner who deletes a row from two devices finds it live at the merge, and so does an RCap holder who revokes their own self-managed capability from two devices. With no cycle at all, the same read voids a bundle whose delete someone else did first. This HIP removes that read: a delete of row `r` reads `r` as if no concurrent entry deleted it, and a revoke of a pair reads that pair as if no concurrent revoke cleared it. Every other read keeps its barriers. A concurrent revocation of a removal's authority still voids it, and a contest, such as two admins revoking each other, is decided as today. The verdicts that change are those of removals whose target another removal also removed, and those of the entries that read them.

## Why

### What happens today

Each row below was run against the working tree on 2026-11-06. The removals in a row are concurrent, from a common base, and "both void" means void with reason `undecided-cycle`.

| # | Type and rule | Who removes | The removals | The target at the merge |
| :--- | :--- | :--- | :--- | :--- |
| D1 | rdb, the default delete rule of a group with an identity provider, `rowAuthor = $author` | the owner, from two devices | both void | live |
| D2 | the same | the owner, from three devices | all three void | live |
| D3 | rdb, `EXISTS grants WHERE grantee = $author AND resource = docs.resource` | two holders of a grant for the doc's resource | both void | live |
| D4 | rdb, the caps delete rule of `users/users.ts`: a manager, or the cap's grantee | the grantee, not a manager, from two devices | both void | live |
| D5 | rdb, `EXISTS caps WHERE label = 'manager' AND grantee = $author` as the caps delete rule | a manager deleting their own manager cap, from two devices | both void | live |
| D6 | RCap, `coadmin` managed by `coadmin` | a holder revoking their own `coadmin`, from two devices | both void | held |
| D7 | rdb, `rowAuthor = $author OR EXISTS admins WHERE grantee = $author` | an admin deletes a doc; its owner bundles a delete of the doc with an insert of a replacement | the admin's delete live, the bundle void (`row-not-live`) | deleted, and the replacement absent |

With one removal in place of two, D1 to D6 remove their target, as `[PERM16]` and `[CAP35]` check for D5 and D6.

### The cause

**rdb.** A delete's rule reads its own row in two ways. It reads it as the subject, through `rowAuthor`, a column or a `$row` term: such an atom is `and(the row is live, the atom)`. It reads it as a witness when the row is a candidate of an `exists`, as in D5. Both read the row's liveness in the delete's own views, observed from `from` (`rowState` in [`rtable/view.ts`](../modules/rdb/src/rtable/view.ts)). That liveness includes the kill by a live delete concurrent with the delete, honored when the table's `concurrentDeletes` is on, which is the default (`killedByConcurrentDelete`). Two concurrent deletes of one row each kill it in the other's views. Each is live only if the other is void, which is the even loop of [`WELL_FOUNDED_VERDICTS.md`](../modules/mvt/WELL_FOUNDED_VERDICTS.md) §2.1, and the solve collapses it to void.

**RCap.** A revoke is live when its author holds one of the managing caps at the revoke (`opVerdict` in [`rcap/view.ts`](../modules/std_types/src/types/rcap/view.ts)). A slot is held only if no live revoke of the pair is concurrent with the position (step 2 of `holdingIn`). When a cap manages itself and a holder revokes their own, the revoke's authority is the very pair it clears, and a second revoke of the pair from another device clears it at the first.

### Why it is a defect

The collapse is the rule for contests: it "returns contested state to what it was", so that "neither side of a dispute gets its way" (`WELL_FOUNDED_VERDICTS.md` §4.3). Nothing is contested in D1 to D6. The cycle has two stable models, one removal live in each, and the target is removed in both. Only the collapse keeps it.

The concurrent kill exists so that a write does not take effect on a row its author could not know was gone: an update of a deleted row, or a use of a deleted cap as a witness ([`rschema/payload.ts`](../modules/rdb/src/rschema/payload.ts), the note on `concurrentDeletes`). A delete of that row has the effect it asked for whether or not the other delete is live.

D7 has no cycle. The owner's delete fails because the admin's delete removed the row, and the bundle takes its insert down with it.

No test covers these cases. `WELL_FOUNDED_VERDICTS.md` §5.2 lists two concurrent revokes by one admin, of two different caps (`[PERM22]`).

## Design

### The rule

> A removal does not need its target to survive another removal of it.

**rdb.** In the views that evaluate a delete op's restriction, the row the op deletes is not killed by concurrent deletions. Its liveness there is "inserted, and not deleted in the op's history". This covers every read of the row in that op's rule: as the subject, and as a candidate of an `exists`. The concurrent deletions of a row are the entries `killedByConcurrentDelete` meets: delete ops, and the deletes of a schema deploy that adopts an FK.

**RCap.** In the authorization of a revoke of `(grantee, cap)`, holding that same pair ignores the concurrent revokes of it, step 2 of `holdingIn`. Step 1, the see-through cover of the pair's grants and revokes in the revoke's past, is read as today.

What does not change:

- **Every other read keeps its barriers.** A delete whose authorizing row is deleted concurrently is void, and so is a revoke whose author's managing cap is revoked concurrently.
- **Only the removal's own rule spares its target.** The other ops of a bundle read the row through the ordinary views. A bundle that grants a cap and deletes its author's manager cap is void when a concurrent delete removes that manager cap, because the grant needs it.
- **Deletions in the op's history still count.** Admission rejects a delete of a row already deleted in its past (*Considered and not taken*).
- **Updates.** An update concurrent with a live delete of its row, under a rule that reads the row, stays void (*Why it is sound*).
- **Admission.** It validates at `(at, at)`, where nothing is concurrent.

### Outcomes

The rows of *Why*, and controls, run against the working tree and against a copy of it with the change of *Changes* applied:

| # | Today | With this HIP |
| :--- | :--- | :--- |
| D1, D2, D3, D4, D5 | every removal void, the target live | every removal live, the target deleted |
| D6 | both revokes void, the cap held | both live, the cap revoked |
| D7 | the bundle void, the replacement absent | both live, the doc deleted and the replacement present |
| K1: a bundle that grants a cap and deletes its author's manager cap, while another manager deletes that cap | the bundle void (`restriction`) | unchanged |
| K2: a delete whose authorizing cap another manager deletes concurrently | void (`restriction`) | unchanged |
| K3: two managers delete each other's manager caps | both void (`undecided-cycle`) | unchanged |
| K4: an update under the default rule, concurrent with an admin's delete of its row | void (`row-not-live`) | unchanged |
| K5: a note whose rule reads doc `t` as a witness, concurrent with a delete `D` of `t` and in the past of another delete `E` of `t` | `D` and `E` void, the note live, `t` live | `D` and `E` live, the note void (`restriction`), `t` deleted |
| R1, RCap: a holder revokes their own `coadmin` while another holder revokes it too | the self-revoke void (`unauthorized`), the other live, the cap revoked | both live, the cap revoked |
| R2, RCap: two holders revoke each other (`[CAP32]`) | both void (`undecided-cycle`) | unchanged |

K5 is the one row where an entry other than a removal changes: see *Why it is sound*.

On the same copy, the rdb and std_types suites (402 and 167 tests) pass with output identical to the working tree's, so no existing test depends on a verdict this HIP changes.

## Why it is sound

**It only removes reads.** The spared liveness is the row-state read without one conjunct, and the spared holding is the slot read without step 2. Both stay read-once formulas over the entries they meet, so they stay exact, sound and monotone (`WELL_FOUNDED_VERDICTS.md` §5.5, item 3). Each reads a subset of what the full read reads, so no edge is added to the graph of reads. No cycle appears, and components can only split. Assumption A3 of [`INCREMENTAL_DELTA_BOUND.md`](../modules/rdb/INCREMENTAL_DELTA_BOUND.md) lists what a verdict may read, and it still holds.

**It does not decide whether the target is removed.** Take a removal `D` of target `t`, and another removal `E` of `t` concurrent with it. If `E` is void, it is seen through and kills nothing, so `D`'s read is today's. If `E` is live, `t` is removed at the merge whatever `D`'s verdict. Either way the target's fate is settled without `D`'s read of it, as it is in both stable models of today's cycle. A removal's verdict therefore no longer depends on the other removals of its target, and what changes is `D`'s own verdict and what follows from it:

- **Its bundle** takes effect (D7).
- **Its barrier** holds for the entries concurrent with it, which is what a lone delete does. In K5, the note is concurrent with `D` and in the past of `E`. Today the note is live only because the collapse voids `D`. With `D` live, the note used a row that was deleted concurrently, and `concurrentDeletes` voids it, as it would if `D` were the only delete.
- **Its explain output** reports no reason.

**Authority is still checked.** The spared read is of the op's own target, and only in that op's rule (K1, K2). In RCap it is only the pair the revoke clears: a revoke whose authority is another pair reads that pair with its barriers.

**Contests stay contests.** In K3 and R2, each removal's authority is the other's target, not its own, so nothing is spared, and the collapse still decides them.

**Updates are left out.** An update's effect is its values on the row, and on a row deleted concurrently they never show. Voiding the update tells its bundle that its effect did not happen. A delete's effect, the row gone, does happen. Updates also form no cycle with deletes: a delete reads its row's liveness, which updates do not change.

## Changes

1. **rdb, [`rtable/view.ts`](../modules/rdb/src/rtable/view.ts).** `RTableViewImpl` takes an optional spared rowId beside `exclude`. `rowState` skips `killedByConcurrentDelete` for that row.
2. **rdb, [`rtable_group/group.ts`](../modules/rdb/src/rtable_group/group.ts).** `diagnoseEntry` evaluates a delete op's restriction with a `getTableView` that spares the op's row in the op's table. Other ops and other tables get the ordinary views, and so does the `row-not-live` explain alias. Views are built fresh for each call today, so a spared view and an ordinary one share no state; a cache of views would have to key on the spared row.
3. **rdb, [`rtable_group/predicates.ts`](../modules/rdb/src/rtable_group/predicates.ts).** No change: `opSubject` reads through the `getTableView` it is given. The boolean path over public views runs at admission, where nothing is concurrent.
4. **std_types, [`rcap/view.ts`](../modules/std_types/src/types/rcap/view.ts).** `opVerdict` tells `holdingIn` which pair a revoke clears, and `holdingIn` skips the walk over concurrent revokes when asked about that pair.
5. **Docs.**
   - [`rdb/VOID_SEMANTICS.md`](../modules/rdb/VOID_SEMANTICS.md) §4: a section beside "An op is not its own witness or barrier", and the `$row` atom rule.
   - [`rdb/src/rschema/payload.ts`](../modules/rdb/src/rschema/payload.ts): the note on `concurrentDeletes`.
   - [`mvt/WELL_FOUNDED_VERDICTS.md`](../modules/mvt/WELL_FOUNDED_VERDICTS.md): rows in §5.2, the rule as part of item 4 of §5.5, and the tests in §6.6.
   - [`std_types/README.md`](../modules/std_types/README.md): the admissibility bullet.
6. **Stores.** Verdicts of stored entries change, and a deploy that adopts an FK derives the rows it deletes from liveness at its parents, once, when it is applied (the note on FK adoption in `rschema/payload.ts`). The protocol is unreleased, so stores written before the change are rebuilt rather than migrated.

In the copy used for *Outcomes*, items 1, 2 and 4 come to about a dozen lines.

## Tests

- **rdb, a new `test/concurrent_removal_tests.ts`:**
  - `[CRMV01]` to `[CRMV06]`: D1, D2, D3, D4, D5 and D7, each with every removal live and the target deleted, and D7's replacement present.
  - `[CRMV07]`: K1, the bundle still void.
  - `[CRMV08]`: K2, the delete still void.
  - `[CRMV09]`: K4, the update still void.
  - `[CRMV10]`: K5, the note void once `D` is live.
  - K3 is `[PERM12]` to `[PERM15]`, which must keep passing.
- **rdb, `[VREAD06]`:** the spared liveness among its read rules, checked exact, sound and monotone over every state.
- **std_types:** `[CAP45]`, D6; `[CAP46]`, R1. `[CAP32]` to `[CAP34]` must keep passing.

## Considered and not taken

- **Sparing only the subject read**: a delete's read of its own row as the subject leaves out concurrent deletes, and its read as a witness does not. It fixes D1 to D4 and D7, and misses D5 and D6, where the target is the removal's witness.
- **Sparing the target in every op of the bundle.** K1 shows why not: a grant in the same bundle needs the cap that a concurrent delete removed.
- **Sparing deletions in the op's history too.** No cycle runs through them, since a delete in an op's past never reads that op. Admission rejects a delete of a row already deleted in its past, so the case needs that earlier delete to be void at admission and live later. And "inserted" below a live delete needs a walk of its own, since the identity cover stops at the delete.
- **Sparing updates.** *Why it is sound*, last paragraph.
- **A seniority order for the core** ([HIP-16](0016-seniority-order.md)). It would make one of D1's deletes live and remove the row, but it decides by an order where nothing is contested, and it does nothing for D7, which has no cycle.
- **Recognizing cores whose stable models agree.** The solve would have to compare models, which is the enumeration the engine rules out (`VOID_SEMANTICS.md` §4, "Structure, never enumeration").
- **Merging concurrent deletes by one author.** D3 and D7 have two authors.
- **`concurrentDeletes` off by default.** It changes what every concurrent delete means, not just this case.

## Open questions

- **Deploy deletes.** The rule spares the deletes of an FK-adopting deploy along with delete ops. A deploy is never void, so it forms no cycle with a delete, and sparing it only makes a delete of a row the deploy removed live instead of void. One rule for every deletion, or a smaller change that leaves deploys out?
- **Other types.** Whether any removal in RSet or RFileMap reads its own target in its authorization. Not checked.
- **Relation to [HIP-21](0021-exists-where-or.md).** Two holders of a `'*'` grant who delete one doc concurrently, under `docs.resource = 'r1' AND EXISTS (grants WHERE grantee = $author AND resource = '*')`, void each other today. That rule reads the doc in every arm. With this HIP both deletes are live. HIP-21 meets this cycle under an earlier draft of its read (*Why it is sound*, "Why the subject row is read per leaf"). Its final read leaves the doc out of the `'*'` arm, so its example rule has no cycle (`[PERM26]`). The update case still tells the two reads apart: an update by a `'*'` holder, concurrent with a live delete of its doc. HIP-21's tests rely on it (`[PERM25]`).
