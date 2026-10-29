# Follow-up plan: fixes after Phases 1-4

**Status:** proposal.

This plan covers the issues in [`PHASE_1_TO_4_CRITIQUE.md`](./PHASE_1_TO_4_CRITIQUE.md), with the decisions made after that review. It does not reopen ideal concurrency, antichain admission, or the two-check holding rule. Those stand.

The critique's generic repair for a masked barrier (a positive-anchored search inside the past of `at`) is **not** the fix. Two narrower rules remove the cases where that search would matter:

- A capability is one slot. Grants are not partitioned by `create-cap` origin.
- Barrier-ness of a delete is a property of the set, not of the delete.

Sections are in descending importance. Sections 1-3 change replicated verdicts or validation and ship together. Sections 4-7 do not.

---

## 1. A scoped concurrent cover is decided on the root DAG

This section supersedes §2.3 and §2.4 of the critique, which proposed projecting the position onto the scope. Projection is wrong for the reasons below.

### Background: what a scoped DAG is

A nested object has no DAG of its own:

- A table's row ops are entries of the group's DAG, tagged `tables: [name]` (`TableScope.baseFilter()` in [`rtable_group/scopes.ts`](./src/rtable_group/scopes.ts)).
- A nested set element's ops are entries of the outer set's DAG, tagged `inner-elmts: [hash]` (`NestedElementScope.baseFilter()` in [`rset.ts`](../std_types/src/types/rset/rset.ts)).

`NestedScopedDag` is a lens over its parent. It wraps and unwraps payloads, it stamps the tag on writes, and it answers every query by delegating to the parent with the scope filter joined into the caller's filter. Nested scopes chain down to a `RootScopedDag`, which calls the real `Dag`.

Delegation means three things for every query:

- **Positions are root positions.** The positions the caller passes (`at`, `from`, `concurrentTo`) can hold any entry: this scope's, a sibling scope's, or the parent's.
- **The order is the root order.** Walks follow the root DAG's edges. They pass through entries that do not match the filter and continue to those entries' predecessors.
- **The filter selects results.** It decides which entries may be returned. It does not decide what is before, after, or concurrent.

`findCoverWithFilter` and `findMinimalCover` work this way, and so did `findConcurrentCoverWithFilter` before Phases 1-4. For the concurrent cover, the primitive in [`dag/src/idx/concurrent_cover.ts`](../dag/src/idx/concurrent_cover.ts) computes, over all entries, the past of `concurrentTo` and the entries after every head of it. The scope filter only picks which of the remaining entries count as matches.

### The issue

Phases 1-4 replaced that delegation with a head filter in `NestedScopedDag.findConcurrentCoverWithFilter` ([`mvt/src/dag/dag_nesting.ts`](../mvt/src/dag/dag_nesting.ts), around lines 242-258):

```text
inScope = { h in concurrentTo : entry(h) matches the scope }
heads   = inScope.size > 0 ? inScope : concurrentTo
```

The goal was that an entry of a sibling nested set should not make another set's barrier apply (`[NES04]`). But the result is a position the caller did not pass, chosen by which scopes its heads belong to. Three things follow.

**(a) It is not a function of the causal past.** Let `n1` be in scope, `s1` outside it, and `n1 < s1`. Then `{s1}` and `{n1, s1}` have the same causal past. Take an in-scope barrier `r` appended at `{n1}`, concurrent with `s1`:

| `concurrentTo` | Heads the code uses | `r` |
| :--- | :--- | :--- |
| `{s1}` | `{s1}` (no in-scope head, so the raw position) | concurrent, so it applies |
| `{n1, s1}` | `{n1}` | after `n1`, so it does not apply |

**(b) It is not monotone.** Read the table the other way: adding the head `n1` removes the barrier.

**(c) It reopens the Mixed-Cut Attack in rdb.** Every table is a `NestedScopedDag` over its group, and the at-use checks call `killedByConcurrentDelete` on the *target* table's scoped DAG. In the `itemLaterImport` fixture of [`observed_cut_tests.ts`](./test/observed_cut_tests.ts):

1. Write a second caps row `c′` at `version(g)`, concurrent with `f2`.
2. The revoker deletes the witness row at `{f1, c′}`. Call that delete `d`.
3. The item's author observes the antichain `{f2, c′}` and inserts the item.

When the item's witness check runs, the filter keeps `{c′}`, the only caps head. `d` is after `c′`, so it is not concurrent, and the item survives a delete that never saw `f2`. The `[ENF]` and `[OCUT]` tests pass today only because none of their cuts contains a head in the target table, so the filter falls back to the raw position.

### Why projecting onto the scope is not the fix either

The critique, and the first version of this section, proposed projecting `concurrentTo` onto the scope: replacing it with the maximal in-scope entries below it. That fixes (a) and (b), but it breaks every use-point check. `[ENF08]`:

```text
c1  (caps insert)
 └─ base
     ├─ d   (caps delete of c1)     branch A
     └─ i1  (items insert)          branch B
```

The witness check opens the caps view at `at = {i1}`, the op being judged:

- **Plain delegation:** `i1` is a head, and `d` is not above it, so `d` is concurrent. The witness is dead and the item is void. That is correct: the delete never saw the insert.
- **Projection:** `{i1}` projects to `{c1}`. `d` is after `c1`, so it does not apply, and the item survives.

The same happens to `[ENF05]` (an FK instead of an exists check). It also happens to `[OCUT09]`: the single head `{f2}` projects to `{g}`. And it happens to `[OCUT10]`-`[OCUT12]`, whose cuts are all `fill` entries. Under projection, a concurrent delete in the witness table never voids a dependent op in another table, unless the delete also missed some witness-table entry below that op. `concurrentDeletes: true` would behave like `false` for every cross-table FK and exists restriction. The mapping in [`CAPABILITIES.md`](./CAPABILITIES.md), from RCap's use-anchored barrier to `concurrentDeletes`, would stop holding.

The root cause is the same for both rewrites. A barrier check asks "did the barrier see the use?", and the use is often an op in another scope. Projection replaces the use with the target scope's cut below it, and the barrier usually *did* see that cut. Any rewrite of `concurrentTo` that drops or replaces out-of-scope heads fails the same way. Falling back to the raw heads when the projection is empty does not help, because `{i1}` projects to `{c1}`, which is not empty.

### The fix

Restore plain delegation, which is the code before Phases 1-4:

```ts
findConcurrentCoverWithFilter(from: Position, concurrentTo: Position, meta: EntryMetaFilter, predicate?: EntryPredicate): Promise<Position> {
    return this.dag.findConcurrentCoverWithFilter(
        from, concurrentTo, joinFilters(this.scope.baseFilter(), this.scope.wrapFilter(meta)), predicate,
    );
}
```

The positions go to the parent unchanged. The scope filter, joined with the caller's filter, decides which entries can be returned. Concurrency is decided on the root DAG.

### Why this is right

1. **It answers the question the views are defined by.** RCap's `hasCapability` and RSet's membership are documented as "would an op appended at `at` be admissible, observed from `from`?". rdb's at-use checks literally open the view at the op being judged. An op appended at `at` has every head of `at` in its past, whichever scope the head belongs to. A barrier that has not seen one of those heads has not seen that op, so it is concurrent with it. Only the full position can express that.
2. **Kill 1 and kill 2 read the same position.** `NestedScopedDag.findCoverWithFilter` has always delegated with the raw `at`; it never filtered or projected heads. With the head filter, the cover (kill 1) read one position and the concurrent cover (kill 2) read another. Plain delegation makes both checks of the two-check rule about the same `at`.
3. **It inherits the primitive's invariants.** The primitive depends only on the causal past, is monotone, and handles the empty position (design document §3.1). Delegation adds nothing but a filter on matches, which preserves all three. `[META_08]` and `[META_09]` test them at the `Dag` level, so nothing nested-specific is left to get wrong.
4. **Root and nested scopes agree.** `RootScopedDag` has always delegated plainly. With plain delegation, making an object nested, or putting a table in a group, does not change what "concurrent" means.
5. **It is the behavior that was measured.** The design document's §1 shim swapped the primitive under `RootScopedDag`, and nested scopes delegated to it unchanged. The rdb and std_types numbers in that section were measured that way.
6. **It closes the hole in (c).** The cut `{f2, c′}` keeps `f2` as a head. `d` is not after `f2`, so it is concurrent, and it voids the item.

### What changes for callers

Compared with the current uncommitted code, a result can differ only when two things hold: `concurrentTo` mixes in-scope and out-of-scope heads, and a matching barrier is in the past of `from` but not of `at`.

- **`[NES04]`: two assertions flip.** Nested sets A and B are added to the outer set, then each runs the same script in its own scope:
  1. Add `root`.
  2. Add `shared`.
  3. Branch `left` and `right`.
  4. Barrier-delete `shared` at `left`.

  Each nested set appends at its own frontier, so A's and B's inner ops are mutually concurrent. Running all of A before all of B does not order them:
  - `RSet.add` sets an omitted position to the nested set's scoped frontier before it appends ([`rset.ts`](../std_types/src/types/rset/rset.ts), line 231).
  - So `NestedScopedDag.append` always receives an explicit position, and its parent-frontier default is never used.
  - Before B has any ops, its scoped frontier is `{hashB}`, the outer add that created it. That add carries B's `inner-elmts` tag and precedes all of A's inner ops.
  - So B's first op has `hashB` as its only predecessor, and A's first op has `hashA` as its only predecessor.

  A probe that replays the fixture confirms that A's delete is not in the past of B's shared add, and that B's delete is not in the past of A's shared add. Both positions below are two-element antichains. Calling the raw primitive with the joined scope filter, which is what plain delegation calls, reports each set's delete as concurrent. The current code passes these two assertions only because the head filter drops the other set's head.
  - **B at `{A's delete, B's shared add}`, from B's frontier.** B's delete is after B's add, but it has not seen A's delete. It is concurrent with the position and visible from `from`, so `shared` is gone from B. The test expects it present (line 460).
  - **A at `{B's delete, A's shared add}`, from A's frontier.** This is the symmetric case: `shared` is gone from A. The test expects it present (line 465).

  The other assertions are unchanged. Their positions are within one scope, or the delete is in the cover. Both flips are `[SET02d]` with the two heads in different scopes. Reading B at a cut that includes A's history asks about an op that has seen A's delete, and B's delete has not seen that op. Rewrite both assertions to `assertFalse`, and say why in the messages.
- **rdb table views at mixed group cuts, with `from` wider than `at`.** A table's barrier delete that has not seen another table's head of `at` now applies. An op appended at `at` would be concurrent with that delete, so this is the same reading. At-use checks, where `at` is the single op, and views with `from = at` are unaffected.
- **Root objects:** no change.

Sibling isolation, which the head filter was trying to provide, is not part of this rule. Default reads don't need it: a nested object's `getView` sets `at` and `from` to its own frontier by default (`NestedScopedDag.getFrontier` is the scope's cover), so sibling history only enters when a caller passes a mixed cut. A product that needs "B's state at the cut B has seen" needs a separate query with that meaning. That query must not replace the concurrent cover the at-use checks rely on.

### Tests

In `mvt/test/dag_nesting_tests.ts`:

- **A use point outside the scope.** In-scope `c`, then an in-scope barrier `r` and an out-of-scope `i`, both after `c` and concurrent with each other. The scoped concurrent cover at `{i}` returns `r`. This is `[ENF08]` at the DAG level, and projection fails it.
- **Mixed heads.** An in-scope barrier after the in-scope head `n1`, but not after the out-of-scope head `s`, is concurrent with `{n1, s}`.
- **Causal past.** With `n1 < s1`, the scoped cover at `{s1}` equals the scoped cover at `{n1, s1}`.
- **Randomized.** On a DAG mixing two scopes and parent entries, the scoped call on a random `S` equals two things: the raw `Dag` call with the scope filter joined, and the scoped call on the raw `findMinimalCover(S)`.

In rdb:

- **`[OCUT14]`:** the `{f2, c′}` construction in (c). The item is void.
- **`[ENF05]`, `[ENF08]`, `[OCUT09]`-`[OCUT12]` stay.** Add a comment to `[ENF05]` and `[ENF08]` that the dependent op is a head of the position the target table is read at.

In std_types: the two `[NES04]` flips above.

---

## 2. A capability is one slot, and so is each grant of it

### The issue

`readDefinition` in [`modules/std_types/src/types/rcap/view.ts`](../std_types/src/types/rcap/view.ts) treats a capability as a set of origins. A `delete-cap` removes an origin only when the delete itself is still in the cover. A re-create on the delete's branch masks it.

At `{c1, c2}` where `c1` is a `create-cap` on one branch and `c2 > d` is a re-create after a `delete-cap` `d` concurrent with `c1`:

- The cover is `{c1, c2}`. `d` is under `c2`.
- The concurrent-barrier check does not see `d`, because `d` is in the past of `at`.
- `origins` is `{c1, c2}`. A grant recorded against `c1` still counts.

That breaks the single-slot rule. The delete was a barrier against `c1`, but the old create stays an origin because something later on the delete's own branch is not a delete.

This is also how a revoked grant comes back:

1. On branch X: `create-cap` `c1`, grant `g` to Alice with origins `{c1}`, then a revoke `r` of that grant by a non-creator manager.
2. On branch Y: `d` concurrent with `c1`, then `c2 > d`.
3. Read at `{r, c2}`.

The merge still lists `c1` as an origin, so `g` counts. `opVerdict(r)` reads `getManagedBy` at `version(r)` from the outer `from`. From there `d` is concurrent, the definition looks absent, the manager list is empty, and `r` is void as unauthorized. The cover sees through `r` to `g`. Alice holds the capability. The revoke was valid when it was written.

### The fix

Two checks, in order. No origin set.

1. **Is the capability live?** One slot, using the same two checks as a holding. A `delete-cap` in the cover turns it off. A `delete-cap` concurrent with `at` and visible from `from` turns it off. A later `create-cap` puts the delete below the cover and turns the same slot back on. `managedBy` comes from the greatest live create in that cover, and only when the slot is on.
2. **If it is live, is this (capability, grantee) pair live?** Every grant and revoke of that pair is on one plane, including grants made before the delete. `holdingIn` stops filtering grants through `hasAnySurvivingOriginIn`.

`capOrigins` stays on the grant payload as the admission record (`validateGrant` still checks it against the origins live at insert). It does not affect liveness. `currentCapCreationVersion` stays as the live create hashes of the slot when the slot is on, and is empty when it is off. Callers that only ask "does this capability exist?" are unchanged. Callers that treat two surviving creates as two generations are not: grants do not hang from a named create. When `c1` stays concurrent with `c2`, both remain in `currentCapCreationVersion`; the masked delete does not remove `c1` from that set. Grants simply do not care which create they named. Dropping a grant's origin check means a grant written against `c1` still holds after `c2` turns the slot back on, and so does a revoke written against that grant.

A delete does not start a fresh generation. It turns the slot off. A re-create turns it back on, and the old grants and revokes are still about that slot.

**Authorizing a grant or revoke.** `getManagedBy` for that decision is read at `(version(op), version(op))`, not from the outer `from`. The manager list is fixed by the definition in the operation's own past. A `delete-cap` on another branch has not happened yet, so it must not empty the list and void the revoke. A `delete-cap` that is actually before the operation is in its cover, the capability is off there, and the operation is unauthorized.

The check that the author holds that managing capability does **not** change. It still uses the outer `from`, on a view at `version(op)`. A revoke of the author's authority concurrent with the operation still voids it.

```ts
const atOp = version(hash);
const managers = (await new RCapViewImpl(this.target, atOp, atOp).getManagedBy(p.capName))
    .filter((mgr) => mgr !== 'creator');
const opView = new RCapViewImpl(this.target, atOp, this.from);
const authorized = await someTruth(managers, (mgr) => opView.holdingIn(evaluation, author, mgr, hash));
```

Creates and deletes are creator-only, so the `(op, op)` definition read stays two-valued and does not join the author's component.

### Tests

- **Old origin does not survive a masked delete.** `c1` concurrent with `d < c2`. At the merge the capability is live, and `currentCapCreationVersion` is `{c2}` only if `c2` dominates every earlier create. It is not, when `c1` is concurrent with `c2`: both creates are in the cover and the slot is on, but grants do not care which create they named. Pin that: a grant with origins `{c1}` holds, and a revoke of it on the same branch holds too.
- **Resurrection.** The setup in the issue above. Alice does not hold the capability at `{r, c2}`.
- **`managedBy` is causal.** A concurrent `delete-cap` plus re-create with a different `managedBy` does not change whether a revoke written on the other branch was authorized.
- **`[CAP40]`** changes. A grant whose recorded origins were deleted no longer fails for that reason. After a delete and a re-create it holds, unless a revoke of that pair is live.
- **`[CAP09]`** ("delete and re-create voids old grants") changes for the same reason. Update it to the single-slot outcome or replace it.

---

## 3. Barrier deletes are a property of the set

### The issue

`supportBarrierDelete` is create-time, but the payload still carries its own bit. `deleteByHash` calls `createDeletePayload(..., false)`, and on a barrier set that returns `{ barrier: false }` ([`rset.ts`](../std_types/src/types/rset/rset.ts), around lines 280-286 and 378-381). `deleteWithBarrierByHash` writes `barrier: true`. `hasByHash` only treats `barrier === true` as a barrier ([`view.ts`](../std_types/src/types/rset/view.ts), around line 72).

So a barrier set can contain both kinds. That is the membership bug:

- Add `a` on one branch.
- On the other: barrier delete `d` concurrent with `a`, then re-add `a2 > d`, then a non-barrier delete `nd > a2`.
- At `{a, nd}` the cover is `{a, nd}`. `d` is under `a2`. Kill 1 sees no barrier in the cover. Kill 2 does not see `d`, because `d` is in the past. `hasByHash` returns true. The old add-anchored search returned false, because `d` is concurrent with `a`.

### The fix

The flag means every delete. It is not a permission to set the bit.

- `createDeletePayload` writes `barrier: true` on every delete when the flag is on, and omits it when the flag is off. `deleteByHash` and `deleteWithBarrierByHash` are the same operation on a given set. Keep both methods as wrappers so callers do not break.
- `validateDeletePayload` rejects a delete whose bit disagrees with the set. A barrier bit on a non-barrier set already fails. A missing or false bit on a barrier set fails too. Signed payloads cannot be rewritten.
- `hasByHash` treats every delete as a barrier when the flag is on, and treats a `barrier: true` delete on a non-barrier set as a non-barrier delete (it should have been rejected at admission). A delete in the cover then always removes the element on a barrier set, which makes the shape above false: `nd` is a barrier and it is in the cover.

A set with the flag off is unchanged. A delete removes an element only by coming after its add. `[PSET19]` stays.

`supportBarrierAdd` is already the other mode, and the two flags stay mutually exclusive. This plan does not change adds.

### Tests

- The shape above, on a barrier set: the element is absent at `{a, nd}`.
- `deleteByHash` on a barrier set stores `barrier: true`.
- A payload with `barrier: false` on a barrier set is rejected. A payload with `barrier: true` on a non-barrier set is rejected, as today.
- `[SET02b]`, `[SET02d]`, `[SET02e]`, `[SET04]`, and `[PSET19]` stay.

---

## 4. The documents contradict the code

`[CAP08]` flipped, and the flip is from the definition read moving to the use point, not from ideal concurrency. The view is a single head: `create-cap deploy` and `delete-cap write` are concurrent, and the view is at the deploy, from the frontier. The old check asked whether the delete was concurrent with the genesis origin. The new one asks whether it is concurrent with `at`. It is, so `write` is absent. That matches "a concurrent negative operation wins."

These still say otherwise:

- [`DESIGN_AND_PLAN_FOR_ANY.md`](./DESIGN_AND_PLAN_FOR_ANY.md) §1, the §4 comparison table, and §4 reason 5 say `[CAP29]` is the only std_types test that flips. `[CAP08]` is a Phase 3 flip. Add a §3.5 row: a `delete-cap` concurrent with `at` and after the origin makes the capability absent (`[CAP08]`).
- `MODERNIZATION_v2.md` has a status note, but the body still says `[CAP28]` flips to true, `[CAP29]` stays, and the future of a multi-hash position is the union of its heads' futures (around the `[CAP28]` paragraph, the §5 test list, and the "future is the union" sentence). Rewrite those to the ideal rule and to the outcomes in the design document's §3.5 table.
- The std_types README says a `delete-cap` voids grants tied to the deleted origin. After section 2 it should say the capability is one slot: off means nobody holds it, and a re-create turns the same slot back on without clearing the grants and revokes.

Also record section 1 in the design document: a scoped concurrent cover is decided on the root DAG (plain delegation), and the scope filter only selects matches. State the two `[NES04]` flips and the reason for them, and say that sibling isolation would need a separate query.

---

## 5. Tests the implementation plan asked for and did not get

| Test | Where |
| :--- | :--- |
| Two reads whose foreign `from` differs only by a dominated ancestor share one verdict frame | rdb, next to the verdict-memo tests |
| A foreign table view's `getVersion()` and `getFromVersion()` are antichains | rdb, next to `[OCUT13]` or `[DEPLOY21]` |
| An undecided grant that dominates a live revoke | `rcap_tests`. `[CAP39]` only covers a live revoke beside a grant. This is the `hidden` branch of `slotTruth`. The public read is a boolean after the component collapses; assert that boolean and that it does not throw. |
| `[META_08]` actually tests "after both heads" | `meta_search_tests.ts`. `aboveBoth` repeats the `aboveOne` call. Use `from = {m}` and `concurrentTo = {a, b}` and expect an empty cover. |
| `[META_09]` varies `from` | Add a second seed and a few random `from` positions. The `from` walk is where a head outside the past of `from` matters. |

The schema-param barrier test stays optional and is not part of this round.

`[CAP38]` is single-replica. The tiebreak is hash order, which is the same on every replica. No second replica is required.

---

## 6. Four returned versions are still not canonical

Section 3.4 of the design document canonicalized the versions that enter verdicts and schema reads. These still return `resolveRefVersionAtPosition` unchanged, so a caller can observe a dominated ancestor:

| Site | File |
| :--- | :--- |
| `RTableGroupImpl.resolveObservedForeignVersion` | [`group.ts`](./src/rtable_group/group.ts), around line 754. This is the group view's `resolveRefVersion`, and `providerPublicKeyAt` reads it at `(at, at)`. |
| `RBlobStoreViewImpl.resolveRefVersion` | [`rblob_store/view.ts`](./src/rblob_store/view.ts), line 23 |
| `RFileMapViewImpl.resolveRefVersion` | [`rfile_map/view.ts`](./src/rfile_map/view.ts), line 38 |
| `FilesAccessControl.observedVersion` | [`rfiles/access.ts`](./src/rfiles/access.ts), line 174 |

Canonicalize each against the referenced group's causal DAG, the same way `resolveForeignTableViewIn` does. One test per site: a dominated ancestor in the folded set is not in the returned version.

`diagnoseObserve` canonicalizes `refAt` again after `resolveObserveGateRefAt`, which already returns a canonical version. Drop the second call.

---

## 7. Nits

- `version` is imported in [`rset/view.ts`](../std_types/src/types/rset/view.ts) and no longer used. Remove it.
- `[CAP18]` is still named "Candidate-based delete barriers only void concurrent capability origins." Rename it to the use-point rule it now tests.
- `[OCUT13]` sits before `[OCUT12]` in [`observed_cut_tests.ts`](./test/observed_cut_tests.ts). Swap them.
- `RCapViewImpl.definitions` caches the promise, including a rejected one, for the life of the view. Views are built for one read. Add a one-line comment that a long-lived view would need to drop a rejected entry.
- `ABSENT.origins` is one shared `Set`. Readers only read it or copy it. Freeze the object, or build a fresh empty set on each return, so a later caller cannot mutate it.

---

## Order of work

1. **Section 1**: plain delegation, the nesting tests, `[OCUT14]`, and the two `[NES04]` flips. This is the invariant break and the rdb hole, and every nested read and every table view goes through it.
2. **Section 2**, then **section 3**. Section 2 closes the resurrection. Section 3 closes the set-membership shape. Neither needs the positive-anchored search from the critique.
3. **Sections 4 and 6.** Documents, then the four canonical returns.
4. **Sections 5 and 7.** The missing tests and the cleanup.

Sections 1-3 ship to every replica in one change. A barrier set that already contains a `barrier: false` delete would start rejecting that payload on re-validation; the sets in this repository's tests write new payloads, and stored dev replicas that mixed the bit should be reset.
