# Phase 5: RSet ref-advances as gated observes

**Status:** implemented. This supersedes the Phase 5 section of [`DESIGN_AND_PLAN_FOR_ANY.md`](./DESIGN_AND_PLAN_FOR_ANY.md). Phases 1–4 are in, including the follow-up that landed with them: a scoped concurrent cover is decided on the root DAG, a capability is one slot, and on a barrier set every delete is a barrier.

A permissioned set's ref-advance is an observe of its RCap. Rdb can void an observe and, when it folds the referenced version, skip voided observes. RSet still admits the advance against the parent cut and then treats every advance as live. This phase closes that gap with the walk rdb already has. It does not add a second meaning of "concurrent", and it does not put ref-advances on `VerdictEvaluation`.

---

## What already holds

Phases 1–4 and the follow-up changed the ground under the old Phase 5 text. These stay, and this phase builds on them.

| Piece | Where it stands |
| :--- | :--- |
| Ideal concurrency | `findConcurrentCoverWithFilter` is the ideal rule. On a multi-head cut a revoke is later than the cut only when it is after every head. `VOID_SEMANTICS.md` §5.5 already says that this is the same fact as "rides a strictly dominating version". The G-upward walk needs no `∃` variant. |
| Antichain on write | `RSet.refAdvance` publishes `canonicalVersion`. Admission rejects a non-antichain (`validateRefVersionShape` in `validateRefAdvancePayload`). `[PSET29]` locks both. Shape and monotonicity stay separate calls. This phase does not introduce `validateRefAdvanceVersion`. |
| Canonical folds | `rcapViewForEntry` and `RSetViewImpl.resolveRefVersion` already run `canonicalVersion` on the RCap DAG. A strictly dominating advance already drops the older heads from a fold that contains both. |
| Element checks | `hasByHash` authorizes through `hasCapability`, which is the two-check holding rule. `[PSET17]` voids an add concurrent with a ref-advance that imports a revoke. `[PSET25]` voids an add whose `refAt` sits under a grant while a later sequential advance pulls a concurrent revoke into `refFrom`. |
| The walk | `RTableGroupImpl.resolveObserveGateRefAt` (`group.ts`, around lines 699–734) is the finished Layer 1 walk, and it already returns `canonicalVersion`. It is still private to the group. |
| The delta bound | RSet's bounded delta uses `walkRevisionBound` (`mvt/src/revision_bound.ts`). Its reads of the reference are geometric: `resolveVersion` is `RSetViewImpl.resolveRefVersion`, and the walk descends the ref-advance cover with no liveness filter. |
| Ref-advances are barriers | `prepareRefAdvance` / `createRefAdvanceMeta` tag `barrier`. `RSet.applyPayload` uses that meta. The concurrent fold already sees them. |

`hasCapability` returns a boolean. A cycle inside RCap collapses in the evaluation `hasCapability` mints, or throws if a result is read outside that evaluation (`rcap/view.ts`, the throw in `hasCapability`). An RSet ref-advance verdict that calls `hasCapability` does not join an RCap component, and it does not need its own `VerdictEvaluation`.

## What is still open

Two holes. Canonicalization does not close either.

**Admission checks the parent cut.** `validateRefAdvancePayload` resolves the RCap version at `(at, at)` and asks `hasCapability` there (`validate.ts`, around lines 167–173). Rdb checks the observe gate at `(newRefVersion, newRefVersion)` (`validateObserveGate` in `validate_ops.ts`). An author who holds `refAdvanceCaps` on the version the parents already observe, and lacks it on the version they import, is admitted. The other direction is the same bug: an author who lacks the cap on the parent cut and holds it on the imported cut is rejected.

At `(newRefVersion, newRefVersion)` the RCap view has `from == at`, so Kill 2 is empty. Admission fails only for a revoke in the past of the imported cut. A revoke concurrent with that cut still passes admission and is caught at use, once `refFrom` includes it. That split is rdb's split. Ordinary adds and deletes stay at `(at, at)` on the geometric parent version (`checkPayloadAuth`). This phase does not move them.

**A bad advance stays in the fold.** `rcapViewForEntry` calls `resolveRefVersions`, which has no `isLive`. Every advance contributes its heads. `canonicalVersion` then drops a head only when some other head in that same fold is strictly above it.

That already matches the strict-dominance case for element reads. An advance of `{v1}` concurrent with a live advance of a version strictly above `{v1}` disappears from the canonical union. `[PSET17]` and `[PSET24]` are that shape. Making the dominated advance void does not change `has` or the public `resolveRefVersion` there. The new behavior is the advance that fails its gate and still publishes a head nobody else dominates.

That is a mixed cut. The advance publishes `{v1, vAdv}`, the revoke of its author is above `v1` and concurrent with `vAdv`, and a live advance publishes the revoke. The ideal rule makes the revoke concurrent with `{v1, vAdv}`, so the gate fails once the revoke is visible from `refFrom`. The union's canonical cover keeps `vAdv`. Today an element whose grant lives only under `vAdv` stays. After this phase the void advance contributes nothing, and the element is gone.

The same hole has a causal shape. An element sits after the bad advance and before a later import of the revoke. The later import is in `refFrom`, so the gate fails, and it is not in the element's `refAt`. See-through falls through the bad advance to the live advance below it. Today the element's `refAt` is the bad version.

---

## The two layers stay distinct

Rdb keeps them distinct. This phase copies that split. Merging them is what would void a pair of benign advances.

**Layer 1, the gate's own cut.** For the advance being judged, start from the causal version at `(opPos, opPos)`. A concurrent advance widens that cut only when its published version strictly dominates the base in the RCap DAG and the widening advance is itself live. A candidate that is not strictly above stops the walk: write-time monotonicity puts everything below it at or below its own version, so nothing further down can be strictly above either. Liveness is asked only of candidates strictly above the base. Two advances that publish incomparable versions never consult each other.

**Layer 2, the fold a reader uses.** Element authorization resolves `refAt` with `isLive` set to the Layer 1 verdict. A void advance contributes no version and does not hide a live advance below it, whether or not its version dominated anything. `refFrom` is the geometric resolution at `(from, from)`. Filtering is a property of which versions participate in `at`.

The public `resolveRefVersion` and the delta bound's reads of the reference stay on the geometric fold. `[PSET24]` reads the public method and expects the concurrent barrier's hashes. `[PSET25]` depends on a later advance moving `refFrom` without being reclassified by a filtered floor.

---

## 1. Lift the walk

Move the body of `resolveObserveGateRefAt` into `mvt/src/refs.ts`. `resolveObserveGateRefAt` becomes a call. Rdb's observe verdicts do not change; the `[OCUT]` and observe-gate tests are the check.

Signature:

```ts
widenRefAtUpward(
    observerDag: ScopedDag,
    refId: B64Hash,
    opPos: Version,
    from: Version,
    referencedDag: CausalDag,
    isLive: (entryHash: B64Hash) => Promise<boolean>,
): Promise<Version>
```

What stays inside the helper, unchanged from the group method:

- The base is `resolveRefVersionAtPosition(observerDag, refId, opPos, opPos)` with no `isLive`. That is the version this advance publishes, folded with the advances in its causal past. Leave that base uncanonicalized for the dominance tests. `findForkPosition` is a function of the causal past, so the comparison already matches the minimal cover, and changing it in the lift is an unneeded risk.
- A candidate widens iff `refVersionAtOrAbove(referencedDag, candidate, base)` and not `refVersionAtOrAbove(referencedDag, base, candidate)`. Cache that per entry hash. The cover predicate and the union loop both consult the cache.
- The concurrent search is `findConcurrentRefAdvanceBarriers`. Its predicate is the one the group uses now: `(await widening(z)) === undefined || await isLive(z)`. The `||` short-circuit is the rule. `isLive` runs only when `widening` returns a version. A non-dominating candidate stays in the cover, so the walk does not descend through it, and its version is not unioned.
- The return is `canonicalVersion(referencedDag, refAt)`.

`isLive` means "this strictly-dominating candidate is a live advance". It is not the see-through predicate of Layer 2. The helper builds the cover predicate. Callers that pass Layer 2's `isLive` into `findConcurrentRefAdvanceBarriers` directly will ask liveness of incomparable advances and can void both.

Rdb's callback stays `observeVerdictLive(evaluation, z, from)`. The group's comment on why an observe is never on a cycle stays next to `observeVerdictLive`.

One mvt test, in `mvt/test/refs_tests.ts`, on a hand-built observer DAG and a hand-built referenced DAG:

- A concurrent advance whose version strictly dominates the base is unioned when `isLive` returns true, and seen through when it returns false, so a live advance below it contributes.
- A concurrent advance whose version is incomparable with the base is not unioned, and `isLive` is not called for it.

Export the helper from `mvt/src/refs.ts` (the module index already re-exports that file).

## 2. The RSet verdict

A ref-advance is live at `from` when its author would pass today's admission cap rule, evaluated at the Layer 1 cut from the geometric `refFrom`.

Anchors, both canonical on the RCap DAG:

- `refAt = widenRefAtUpward(setDag, capabilityRef, version(advanceHash), from, rcapDag, isLive)`
- `refFrom = canonicalVersion(rcapDag, resolveRefVersionAtPosition(setDag, capabilityRef, from, from))`

The author is live when either of these holds:

- `refAdvanceCreators()` is set and `rcap.isCreator(author)`, or
- some name in `refAdvanceCaps()` satisfies `hasCapability(author, name)` on `rcap.getView(refAt, refFrom)`.

That is today's two-branch admission rule, moved to the widened cut. Keep both branches as they are: the flag lets a creator through, and otherwise some listed cap must hold. `refAdvanceCaps()` defaults to `[]` and `refAdvanceCreators()` defaults to true, so a creator advance is the common case and the cap loop never runs. With the flag off, a creator still passes when the list is non-empty, because `hasCapability` is true for a creator, and fails when the list is empty. That is today's behavior too.

`isLive` for the walk is this same verdict, at the same `from`. Memoize the promise per view, keyed by the advance hash. Put the promise in the map before its first `await`; two calls that both miss an empty map would otherwise each start a computation. A second caller, including one from an unrelated `has` running concurrently on the same view, awaits the first caller's promise. Do not detect cycles with an "in progress" flag on the view: concurrent reads on one view would trip it. Pass the chain of advances being decided down the recursion instead (the walk's `isLive` callback closes over it), and throw when an advance is already on its own chain. Strict dominance makes that throw unreachable; it is the same defensive check as `observeVerdictLive`. Without it a real cycle would hang on its own promise instead of failing. Do not catch the throw from `hasCapability`. There is nothing to solve at the RSet layer.

The advance is not its own witness. The base of the walk includes the advance's own published version; the candidates are other advances.

Put the verdict next to `rcapViewForEntry` on `RSetViewImpl`, so one view's memo covers every element the view authorizes. `checkEntryAuthorization` goes through `rcapViewForEntry` and picks it up. A fresh view has a fresh memo. That matches `hasCapability` minting one evaluation per top-level read.

**Deltas do not report ref-advance flips in this phase.** `RSetDeltaAccumulator.ingest` computes `validityChanges` only for `add` and `delete`. An advance that turns void shows up through its effects: the elements it stops authorizing appear in `removed`, and their adds flip in `validityChanges`. Reporting the advance itself is a follow-on. If it is added, it must not go through `checkEntryAuthorization`: `isEntryAuthorized` treats every action other than `add` as a delete and would check `capRequirementForDelete()`. It needs its own case that reads the §2 verdict at the start and end views.

## 3. Admission

In `validateRefAdvancePayload`, replace the parent-cut lookup with a view at the imported version:

```ts
const rcapView = await rcap.getView(newRefVersion, newRefVersion);
```

`newRefVersion` is already shape-checked and canonical on write. Keep the creator short-circuit, the signature check at the set position, and the RCap key lookup where they are. Keys are not voided. Monotonicity stays the check that the imported cut is at or above every cut the parents observe, which is why the imported cut is the admission anchor.

`checkPayloadAuth` keeps `getView(at, at)` and the geometric `resolveRefVersion`. Adds and deletes are checked causally at insert, then re-checked at use.

## 4. See-through on the enforcement fold only

`rcapViewForEntry` stops calling `resolveRefVersions`. Resolve the two sides separately:

- `refAt`: `resolveRefVersionAtPosition(setDag, refId, version(entryHash), this.from, isLive)`, then `canonicalVersion`.
- `refFrom`: `resolveRefVersionAtPosition(setDag, refId, this.from, this.from)` with no `isLive`, then `canonicalVersion`.

`isLive` is the verdict from §2. Layer 2 sees through every void advance, dominating or not. That is a different predicate from the one inside `widenRefAtUpward`.

These stay geometric, with no `isLive`:

- `RSetViewImpl.resolveRefVersion`. `[PSET24]` asserts the widened public version.
- The delta bound's reads of the reference in `walkRevisionBound`: `resolveVersion` and the ref-advance descent. A void advance is not reclassified as unstable by a second, stricter fold. Its flip reaches the bound through the visitor (below).
- Admission of adds and deletes.
- The base inside `widenRefAtUpward`.

`resolveRefVersions` gains no `isLive` parameter. Callers that want a filtered `refAt` call `resolveRefVersionAtPosition`.

### The delta bound

RSet's bounded delta uses `walkRevisionBound`. The set declares its RCap as a `'horizon'` reference, and the visitor returns true for a barrier add or delete of `start` whose authorization flipped (`std_types/src/types/rset/delta.ts`). The walk already drops below every ref-advance whose published version is above RCap's bound between the versions observed at `start` and at `end`. That includes an unstable side-branch advance of `start` concurrent with an element, which widens the element's concurrent fold. Before RSet adopted the walk, the meet-based projection missed that case. It was the residual case this section used to trace by hand, and `[SET_BOUND01]` reproduces it.

This phase adds one case to the visitor: **a ref-advance candidate whose liveness differs between `start` and `end`.** An advance `Y` can flip while its own version is stable, through its widened Layer 1 cut. When it does, an entry `E` concurrent with `Y` gains or loses `vY` from its Layer 2 fold, so `E`'s authorization can flip, and nothing else makes the walk reach `E`. With the visitor reporting the flip, the bound drops below `Y`, and every entry concurrent with `Y` is walked. Read the liveness at the `start` and `end` views, as the accumulator reads authorization; the delta still does not report advance flips (§2).

Two things to check while implementing it, against the contract in `walkRevisionBound`'s comment:

- Condition 6 says a referenced object is read through views whose `at` folds the ref-advances in the reading entry's past and the concurrent ref-advance barriers. The Layer 2 fold is a subset of that geometric fold, the live advances. Its versions are each at or below RCap's bound whenever the geometric fold's are, so the argument for stable folds should carry over; confirm it, and reword the condition if it should name the subset.
- A stable advance below the bound can flip only through a candidate in its Layer 1 cut. Confirm that such a candidate is unstable or flips itself, and so is walked and lowers the bound below the advance.

---

## 5. Tests

In `permissioned_set_tests.ts`, unless a fixture is obviously an rdb observe-gate replay.

**Admission, both directions of the anchor.**

- The author holds `refAdvanceCaps` at the parent cut and a revoke of that cap is in the past of `newRefVersion`. `validatePayload` fails. The same author, importing the parent cut, succeeds.
- The author lacks the cap at the parent cut and a re-grant is in the past of `newRefVersion` only. `validatePayload` succeeds. Today this fails.
- A creator still succeeds when `refAdvanceCreators` is left at its default, including when `refAdvance` is omitted. A non-creator still fails when the list is empty.

In both flip fixtures below, the attacker is a non-creator who holds a cap listed in `refAdvance`. `holdingIn` returns true for a creator before it looks at any revoke, so a creator attacker makes both tests pass for the old reason.

**Mixed cut, the concurrent fold.** The case the canonical union does not already cover.

- RCap: `v1` above a cut where the attacker holds the ref-advance cap; `vAdv` concurrent with `v1`, carrying a grant to Bob that no other branch carries; `r` above `v1`, revoking the attacker's ref-advance cap, concurrent with `vAdv`.
- The attacker ref-advances to `{v1, vAdv}`. Admission passes: at `{v1, vAdv}` with `from == at`, `r` is not in the past.
- The admin, concurrently in the set, ref-advances to `{r}`.
- Bob adds an element after the attacker's advance and concurrent with the admin's. Admission of the add passes, because the geometric parent version includes `vAdv`.
- From the merged frontier the element is absent. The attacker's advance is void (`r` is concurrent with `{v1, vAdv}` and visible from `refFrom`), see-through drops `vAdv`, and `{r}` does not contain Bob's grant.
- The admin's advance stays live. An element granted on the `r` branch and added against that advance stays.

**Causal see-through.** The later-import shape of `[OCUT04]`.

- A live advance `L` publishes a version where Bob does not hold the cap.
- The attacker appends `Y` after `L`, publishing `vAdv`, where Bob does hold the cap and the attacker still holds the ref-advance cap. Admission of `Y` passes.
- Bob adds `E` after `Y`. Admission passes.
- In RCap, `r` revokes the attacker's ref-advance cap on a branch concurrent with `vAdv`.
- A later sequential advance, after `E`, publishes `{vAdv, r}`. Monotonicity requires it to be at or above `vAdv`, so it does contain Bob's grant. That does not matter for `E`: the advance is after `E`, so it reaches `refFrom` and not `E`'s `refAt`.
- From the frontier `E` is absent. `Y`'s Layer 1 cut is `vAdv`, because the later advance is after `Y`, not concurrent with it. `refFrom` is `{vAdv, r}`, and `r` is concurrent with `vAdv`, so Kill 2 voids `Y`. `Y` is the causal cover of `E`'s ref-advances and nothing is concurrent with `E`, so only see-through falls through `Y` to `L`, where Bob does not hold the cap.
- Today `E`'s `refAt` is `vAdv`, Bob holds the cap there, and `E` stays.

**Incomparable advances.** Two ref-advances publish incomparable RCap versions. Each author holds the ref-advance cap at their own version and at the frontier's geometric `refFrom`. Both stay live. An element authorized only on one branch stays. The read does not throw.

**Stay, as they are.**

- `[PSET17]`, `[PSET20]`, `[PSET24]`, `[PSET25]`, `[PSET26]`, `[PSET29]`.
- `[PSET25]`'s assertion stays. Its comment still describes the grant-anchored search and "B2". That search is gone; the void is Kill 2 at the entry's `refAt` from the geometric `refFrom`. Update the comment in this change so the test that guards the geometric `refFrom` does not describe the old rule.

The strict-dominance back-dated advance (the `[OCUT01]` shape: publish `{v1}`, a concurrent live advance strictly above it) is already the canonical union. Do not add a membership assertion for it and read a pass as coverage of see-through. The mixed-cut test and the causal see-through test are the ones that flip.

**Memo under concurrent reads.** On one view, run `has` for several elements with `Promise.all`, where their authorizations share a ref-advance verdict. The reads agree with the same reads run one at a time, and none throws.

**Delta parity.** Every generator ref-advances only as the creator, so no generated advance is ever void and parity cannot see this phase. Extend `rset_shaped_generate.ts` with a non-creator ref-advancer, with a `refAdvance` cap in the set's requirements, and an RCap revoke of that cap on a branch concurrent with a version the advancer publishes. Add the widening shape from §4: a non-creator advance concurrent with another live advance whose version is above the later RCap fork point, and a new revoke concurrent with that higher version only. Full and bounded deltas must agree on those histories. Show first that the extended generator fails with the visitor's ref-advance case removed.

## 6. Docs

- `std_types/README.md`, the permissioned-set section. `rcapAt` on the enforcement path is the live fold, not every ref-advance in the past and the concurrent cover. `rcapFrom` and the delta bound's reads of the reference stay the geometric resolutions the section already describes. Say that a ref-advance whose author lacks the cap at its widened cut is void and contributes no version. Keep the sentence that insertion of ordinary ops uses `(at, at)`.
- The status line of `DESIGN_AND_PLAN_FOR_ANY.md`: Phase 5 points here. Leave the old Phase 5 bullets in place; this file is the one to implement.

`MODERNIZATION.md` still says `[CAP28]` flips to true and describes this work as future. It is not the spec. Do not rewrite it in this change.

---

## What this phase does not change

- The ideal-concurrency primitive, antichain admission, the two-check holding rule, the one-slot capability, barrier-ness of set deletes, and `NestedScopedDag` deciding concurrency on the root DAG.
- Rdb observe verdicts, apart from `resolveObserveGateRefAt` calling the helper.
- `RFileMap` presence. That is Phase 6. Its add-anchored remove is a different rule; this phase does not touch `presence.ts`.
- The public reference version. The delta bound changes in one place, its visitor (§4).
- Ordinary admission of adds, deletes, grants, and revokes.

## Order

1. The helper, the rdb call, and the mvt test. Run the rdb suite and the observe-gate / `[OCUT]` tests before any RSet change. A flip here is a bad lift.
2. The verdict, the admission anchor, and the enforcement fold, with the permissioned-set tests.
3. The README paragraph and the status pointer.

Ship it as one change. Admission and the at-use fold change together: a replica that admits the new way and folds the old way, or the reverse, disagrees on which heads a set observes. The project is pre-1.0, so no migration is planned.

## Exit

- `mvt` refs tests, including the new walk test.
- `permissioned_set_tests`, `delta_set_tests`, the RSet and branchy delta parity, with the shaped generator extended to produce void ref-advances and the widening shape (§5).
- `replica/test/replica_permissioned_sync_tests.ts`.
- The rdb suite. The lift is the only rdb change; a failure there is a behavior change in the helper.
