# HIP-13: RCap and RSet adopt the bound walk

- **Type:** implementation
- **Status:** implemented
- **Modules:** std_types, mvt
- **Requires:** 12
- **Superseded by:** —
- **Implemented in:** `7c4505e`, `d3537ca`

## Summary

RCap bounded its delta at the fork meet, which misses a grant under the meet whose verdict flips when a new entry voids an old revoke on a side branch. This HIP switches RCap to mvt's `walkRevisionBound` with a host and visitors of its own, gives it `computeRevisionBound` so that RSet can treat it as a `'horizon'` reference, reports grants revived by a capability re-create, and lists every create-cap in `getCapabilities`. RSet followed in `d3537ca` without a plan of its own: its delta is bounded by the same walk, the meet-based observer bound helpers are gone, and a permissioned nested set is rejected by the bounded strategy.

The walk and its contract are in `mvt/src/revision_bound.ts`. The proof is in [`INCREMENTAL_DELTA_BOUND.md`](../modules/rdb/INCREMENTAL_DELTA_BOUND.md), whose terms this plan uses.

---

## Why

`computeDeltaBounded` in `std_types/src/types/rcap/delta.ts` uses the fork meet as its bound, and walks with `walkEntriesBackwardsToBound`, which stops only at the bound's heads. The meet is not sound for RCap, for the same reason it was not sound for RTableGroup. An old revoke on a side branch of `start` is concurrent with a grant below the meet. A new entry can void that revoke, and then the grant's verdict changes without the grant being walked.

The concrete history, traced by hand and not yet built as a test:

```text
  c ... gS - gA - G - s1 - N        start = {s1, T}, end = {N, T}
              \
               T
```

- Capabilities: `super` (managed by the creator), `admin` (managed by `super`), `write` (managed by `admin`).
- `gS`: the creator grants `super` to S. `gA`: the creator grants `admin` to A.
- `G`: A grants `write` to B. `s1`: any later entry on G's branch.
- `T`: S revokes A's `admin`. It comes after `gA` and is concurrent with `G` and `s1`.
- `N`: the creator revokes S's `super`. It comes after `s1` and is concurrent with `T`.

At `start`, `T` is live, so A does not hold `admin` at `G`. `G` is void, and B does not hold `write`. At `end`, `N` kills S's `super` at `T`'s position, so `T` is void. A then holds `admin` at `G`, `G` is live, and B holds `write`.

The meet is `{s1}`. The walk visits `N` and `T`, and follows `T` down through `gA` and below, an over-walk. But `G` is reachable only through `s1`, so the pair (`write`, B) is never collected. The full delta reports B gaining `write`, and the bounded delta does not.

With the walk, `T` is a candidate whose verdict flips. The bound drops to `{gA}`, and `G` is walked.

---

## 1. RCap's host and visitors

**References.** `references()` is empty. RCap has no ref-advance action: `applyPayload` throws on anything other than `add-identity`, `create-cap`, `delete-cap`, `grant` and `revoke`. RCap objects cannot depend on one another for now, and that is intended. When RCap stacking arrives, the host will declare those references as `'horizon'`.

**The verdict entry point.**
- `RCapViewImpl` gains a public `isOpVoid(hash)`. It mints a fresh `VerdictEvaluation`, calls the private `opVerdict`, and throws on `undecided`, as `hasCapability` does.
- `RCapImpl` gains `isEntryVoided(hash, from)`, which calls `isOpVoid` on `new RCapViewImpl(this, from, from)`. `opVerdict` reads only `from` and the op's own position, so `at` does not matter.
- `rcap/delta.ts` takes an `RCapDeltaHost = RCap & { isEntryVoided(...) }`, like rdb's `GroupDeltaHost`. The `RCap` interface does not gain `isEntryVoided`.

**Visitors.** RCap's delta has no op channel; it reports identity, capability and grant changes only. So both visitors read verdicts for candidates and nothing else:

```ts
// The delta visitor. The bound-only visitor is the same without the ingest.
async visit(entry, candidate) {
    await acc.ingest(entry);
    return candidate
        && await cap.isEntryVoided(entry.hash, start) !== await cap.isEntryVoided(entry.hash, end);
}
```

A candidate is a revoke or a delete-cap of `start`, the two actions `applyPayload` tags `barrier`.
- **A revoke** affects concurrent entries only through Kill 2, which reads only live revokes. So its effect changes exactly when its verdict does.
- **A delete-cap** is read without a liveness filter in `readDefinition`, so its effect never changes between horizons. Its verdict is always live: delete-cap is creator-only (`validateDeleteCap`), and `opVerdict` returns live for creators, whose list is fixed in the create payload. Comparing its verdict is cheap and always reports no change. It also stays right if non-creators are ever allowed to delete capabilities. That change would also need a liveness filter in `readDefinition`.

**Entry points.**
- `computeDeltaBounded` calls `requireExtendingFork`, returns early when `forkB` is empty, walks with the delta visitor, and reports the walk's bound as `revisionBound`.
- A new `computeRCapRevisionBound(cap, rawDag, start, end)` runs the bound-only visitor.
- `computeDeltaFull` stays as it is: `walkDelta` from genesis, the reference for parity.

---

## 2. `computeRevisionBound`

- **`RCapImpl.computeRevisionBound(start, end)`** canonicalizes both versions against RCap's causal DAG and calls `computeRCapRevisionBound`. It ignores the delta strategy, as rdb's does. Canonicalizing is cheap. RSet will pass canonical versions anyway, and the walk may not need them, but that has not been checked.
- **The `RCap` interface gains `computeRevisionBound`.** RSet holds its capability as an `RCap`, and its host will return that object as a `ReferencedObject`.
- **No memo.** Nothing in production calls RCap's or RSet's `computeDelta` yet. Once RSet adopts the walk, it will call `computeRevisionBound` once per RSet delta. rdb's memo is itself unproven, and a cache at another layer may serve better (INCREMENTAL_DELTA_BOUND.md §6.2). Copying it here would add a second per-type cache before anything has shown that the first one is needed.

---

## 3. The re-create gap

This bug is separate from the bound, and the meet-bounded delta has it today. A capability is one slot, and re-creating it "turns the same slot back on". The grants and revokes written before the delete are about that slot again (`readDefinition` in `rcap/view.ts`). So a pair's holding depends on its own grants and revokes and also on its capability's `create-cap` and `delete-cap` entries. In rdb, the counterpart is that a row's operations include the deploys that delete it.

The accumulator collects pairs only from walked grants and revokes. In the following history, the bounded delta walks only the re-create:
- A grant of `dyn` to B.
- A delete of `dyn` at `start`.
- A re-create of `dyn` in `end`.

The re-create adds `dyn` to the capability names, but not (`dyn`, B) to the pairs. The full delta reports B gaining `dyn`, and the bounded delta does not. Nothing in the parity sweep builds this history: `rcap_ops.ts` always creates fresh names, and only ever grants `write`.

**The fix, option 1.** In `finalize`, after computing `capabilityChanges`:
- For every capability that is absent at `start` and present at `end`, add a pair for each grant or revoke of it.
- Find those entries with `scopedDag.loadAllEntries()`, keeping grants and revokes whose `capName` matches.
- Then compare the pairs as now.

This costs a full scan, but only when a capability reappears. A pair whose entries all lie outside `H(end)` holds at neither horizon, so the superset is harmless. The other existence changes need nothing:
- A capability present at both horizons: its pairs change only through their own entries.
- A capability absent at `end`: both strategies already skip its pairs.

The filtered cover cannot be used for this scan. `findCoverWithFilter` stops at the first match on each path, so it returns only the topmost matching entries.

**Found along the way: `getCapabilities`.** It lists the create-caps through the same topmost-only cover (`findCoverWithFilter(at, { containsKeys: ['caps'] })`), so it misses any create-cap that has another capability entry above it. A probe confirmed this. After `createCap('y')` and then `createCap('z')`, it returns `["admin", "z"]`, while `capabilityExists('y')` is true. No test calls it. The fix is the same scan: walk `H(at)`, collect create-cap names, and filter them by `capabilityExists`. It is step 4 of the order below, and can be dropped from this change without touching the rest.

---

## 4. The contract, checked for RCap

These are the items in the doc comment on `walkRevisionBound`:

1. **One root.** RCap is created as a root object, and `computeDelta` reads `ctx.getDag(createOpId)`. The implementation checks that nothing creates an RCap as a nested scope.
2. **Determinism.** Verdicts come from a `VerdictEvaluation`, which solves mutually dependent verdicts together. A revoke ring that nothing outside decides is void.
3. **Cross-branch reads go through barriers.**
   - The two concurrent reads in `rcap/view.ts` filter on `barrier`: delete-caps in `readDefinition`, and revokes in Kill 2.
   - Every other view read is a cover at `at`, which reads only the past: the definition cover, the pair's slot cover, and the identity cover.
   - `lookupKey` reads the replica's frontier, but only to verify signatures during validation. No view reads it.
4. **The visitor covers a barrier's effect.** See §1. A revoke's effect is its verdict, and a delete-cap's effect never changes.
5. **Every reference is declared.** There are no ref-advances, so the walk's undeclared-reference throw can never fire.
6. **Referenced versions, 7. sound referenced bounds, 8. references read at `(v, v)`.** None apply, since RCap has no references.

`add-identity` is not a barrier, and it is permanent on purpose (`std_types/MODERNIZATION_v2.md` §4). An identity's state depends only on its own entries, so the identity channel stays complete under the smaller walk.

---

## 5. Tests

**New tests in `test/delta_cap_tests.ts`, written first.** Step 1 of the order below confirms that `[CAP_BOUND01]` and `[CAP_BOUND03]` fail on today's code. Each test compares the bounded delta with the full one through `normalizeRCapDelta`. Once `computeRevisionBound` exists, each also checks it against the delta's bound.

- `[CAP_BOUND01]` The history in "Why". B's `write` change is reported, and the bound is `{gA}`. This fails today.
- `[CAP_BOUND02]` The same history, with `N` an `add-identity` that leaves `T` alone. Nothing about (`write`, B) changes, and the bound stays at the meet `{s1}`.
- `[CAP_BOUND03]` The re-create history from §3. B's `dyn` change is reported. This fails today.
- `[CAP_BOUND04]` `computeRevisionBound` returns the bounded delta's bound. It returns the same bound when the same versions are written with a dominated hash added.
- `[CAP_BOUND05]` Only if step 4 is taken: `getCapabilities` lists a create-cap that has a later capability entry above it.

`[CAP_DELTA01–11]` stay as they are. `[CAP_DELTA06]` is named "revisionBound equals the fork-point meet". That is still true in its linear history, since there is no barrier for the walk to find.

**Parity.**
- `compareRCapDeltaStrategies` (`test/delta_parity/parity.ts`) also checks `cap.computeRevisionBound(start, end)` against the bounded delta's bound, as rdb's `assertRevisionBoundParity` does.
- `rcap_ops.ts` grows two things. It grants and revokes dynamic capabilities, not only `write`. A `recreateCap` op re-creates a deleted dynamic name.
- A shaped generator, `[DELTA_PARITY_RCAP_SHAPED]`, builds the skeleton of "Why" with randomized barriers, as rdb's `[DELTA_PARITY_SHAPED]` does.
  - The skeleton: `x - E - s1 - N` with `x - T`, where `T` is an old revoke on a side branch and `E` is a grant whose verdict reads `T`.
  - Each variant adds 0 to 2 noise entries after `E` and after `T`.
  - Usually `N` changes `T`'s verdict, either by revoking its author's manager or by revoking that manager's own grant.
  - Otherwise `N` is noise.
- Following the lesson in INCREMENTAL_DELTA_BOUND.md §6.3, the shaped sweep and the extended random one must both fail on today's code before the switch. Record how many pairs fail in each profile.

**RSet.** RSet's floor (`computeObserverRevisionBound`) reads RCap's `computeDelta(...).revisionBound`, which will sit lower in some histories. That is the sound direction. If a bound assertion in `test/delta_set_tests.ts` moves, stop and check that the cause is RCap's bound dropping on a flipped side-branch revoke. Then update the test.

**Mutations.** Run each once by hand, and do not commit them.
- A visitor that always returns false brings back the meet's behavior. It must fail `[CAP_BOUND01]` and the shaped sweep.
- Removing the re-create scan must fail `[CAP_BOUND03]`.

**Build.** This change does not touch mvt. std_types reads mvt from its `dist`, which already holds the walk.

---

## 6. Docs and comments

- **`readDefinition`:** state the rule. delete-cap and create-cap are creator-only, creators are fixed, so both are always live and the read needs no liveness filter. Allowing non-creators to delete capabilities would need the filter here, and the visitor would then see delete-cap verdicts flip.
- **`rcap/delta.ts` header:** say how each strategy works, as the rdb header does. Bounded is the walk with a verdict-only visitor and no references. Full is `walkDelta` from genesis. Also describe the re-create scan.
- **`mvt/src/delta.ts`, "On the reported revision bound":** only RSet still uses the meet helpers.
- **INCREMENTAL_DELTA_BOUND.md §6.3:** RCap now uses the walk, and RSet still uses the meet.
- **`std_types/MODERNIZATION_v2.md` is untracked.** It is the only place that writes down why delete-cap and add-identity read without liveness. Commit it with this change, or move those sentences into the code comments above.

---

## Considered and not taken

- **Indexing grants by capability** (option 2). That is a meta format change, and the entries already stored would not carry the tag.
- **A liveness filter on delete-cap now.** It would never remove anything while delete-cap is creator-only.
- **`isEntryVoided` on the `RCap` interface.** It is the delta's own entry point. A host type keeps the public interface as it is.
- **An op channel for RCap**, with verdict flip records like rdb's. Nothing consumes one.
- **A `computeRevisionBound` memo**, or lifting rdb's into mvt as a shared helper. See §2.

## What this change does not do

- It does not change RSet's bound. `computeObserverRevisionBound`, `combineObserverRevisionBounds` and `projectForeignBound` stay until RSet adopts the walk.
- It does not add RCap references. RCap stacking is out of scope.
- It does not change RCap's views, apart from making `isOpVoid` public and, in step 4, fixing `getCapabilities`.
- It does not touch Phase 5.

## Order

1. Add `[CAP_BOUND01–03]`, the `rcap_ops.ts` extensions and the shaped generator. Confirm that the three tests and the sweeps fail on today's code, and record the parity counts.
2. Switch RCap: `isOpVoid`, `isEntryVoided`, the host and visitors, `computeRCapRevisionBound`, `computeRevisionBound`, and the interface addition. Add `[CAP_BOUND04]` and the bound check in `compareRCapDeltaStrategies`.
3. The re-create scan in `finalize`.
4. Optional: fix `getCapabilities` and add `[CAP_BOUND05]`.
5. The docs and comments in §6.

Ship it as one change. Step 4 can be split off.

## Exit

- **std_types:** the suite and `npm run test:parity`, including `[CAP_BOUND01–04]` and `[DELTA_PARITY_RCAP_SHAPED]`. Any RSet test that moved has been explained.
- **mvt and rdb:** nothing changes. rdb's suite still runs, since it builds against the same mvt.

## History

This HIP was `modules/rdb/plans/RCAP_INCREMENTAL_BOUND_WALK.md`, moved here by [HIP-1](0001-hips.md) on 2026-11-04. Text above that names a file by its old name refers to it as it was. The files it was built from can be read at commit `f45f1e3` (`git show f45f1e3:<path>`).

RSet's adoption, which this plan calls "next", landed in `d3537ca` without a plan of its own.

Its status line read:

> **Status:** implemented, including the optional step 4 (`[CAP_BOUND01–05]`, `[DELTA_PARITY_RCAP_SHAPED]`). Before the switch, `[CAP_BOUND01]` and `[CAP_BOUND03]` failed, and so did these parity pairs: shaped 4 of 204 (smoke) and 14 of 881 (extended), random 7 of 1600 (extended), branchy 14 of 1600 (extended). RCap's bounded delta moves off the fork meet and onto mvt's `walkRevisionBound` ([`MVT_INCREMENTAL_BOUND_WALK.md`](0012-mvt-bound-walk.md)). RCap also gains `computeRevisionBound`, so RSet can treat it as a `'horizon'` reference when RSet adopts the walk. That RSet change is next, and it is the prerequisite for Phase 5 (`RDB_OBSERVE_FIX_PLAN_v2.md` step 7).
