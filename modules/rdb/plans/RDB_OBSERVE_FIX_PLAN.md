# Fixing the bounded-delta floor

**Status:** plan. It answers [`RDB_OBSERVE_BUG_PROOF.md`](./RDB_OBSERVE_BUG_PROOF.md). It supersedes the floor repair sketched in §4 of [`PHASE_5_PLAN_CRITIQUE_v2.md`](./PHASE_5_PLAN_CRITIQUE_v2.md).

A bounded group delta walks backward from `end` and stops at a floor. Everything below the floor is assumed to have the same verdict and the same row state at `start` and at `end`. That assumption is false today, with and without observes. This plan replaces the floor with one that is sound by construction, states the conditions it has to meet, and proves that an entry below it cannot change.

The error is in the floor, not in the observation semantics. §2 shows the evidence.

---

## 1. Three histories that break the floor

Each one is a history where the full delta reports a change and the bounded delta does not.

**History A, an observe concurrent with a row** (`RDB_OBSERVE_BUG_PROOF.md`, built by [`test/observe_floor_proof.ts`](./test/observe_floor_proof.ts)). An ungated observe `W` is a head of `start` and concurrent with an insert `E` below the meet. `W` widens `E`'s foreign cut to `vW`. A later observe `Z` puts a foreign delete `r` into the horizon. `r` is concurrent with `vW`, so `E`'s `exists` fails at `end`. The floor is the meet `s1`, `W` is never classified, and `E` is reachable only through `s1`.

**The local history, no binding and no observe** (built by [`test/local_floor_proof.ts`](./test/local_floor_proof.ts)). One group, four tables.

- `perm` holds a row `p` with `label = ok`.
- `targets` holds a row `x`. A delete of a target is restricted by `exists perm where label = ok`.
- An insert `E` into `items` writes an FK to `x`. `s1` is any op after `E`.
- A delete `T` of `x` is concurrent with `E`. `start` is `{s1, T}`.
- A delete `N` of `p` is after `s1` and concurrent with `T`. `end` is `{N, T}`.

At `start`, `T` is live, `x` is dead at `E`'s position, and `E` is void. At `end`, `N` voids `T`, `x` is alive at `E`'s position, and `E` is live. The floor is the meet `s1`. The full delta reports `T` and `E`. The bounded delta reports `T` only.

**History B, one observe voids another** (`RDB_OBSERVE_BUG_PROOF.md`, not built yet). A gated observe `Y` publishes a version at or below the foreign bound, so the projection calls it stable. A concurrent observe `C` with a higher version widens `Y`'s cut, and a revoke in the horizon voids `Y`. An insert `E` concurrent with `Y` and below `C` falls through to an older cut and flips. The floor on `C`'s branch is `C`, above `E`.

The three have one shape. An entry below the floor reads a barrier that is concurrent with it. Either that barrier's verdict changes between the two horizons, or the foreign state it carries does. The floor never looks at concurrent barriers.

---

## 2. The floor is wrong, not the semantics

Phases 1–4 changed two things: the ideal concurrency rule (an entry is after a multi-head cut only when it is after every head), and canonical folds (`canonicalVersion` on every foreign cut). The question is whether either one is the bug.

History A was run on scratch copies of the repo with each factor switched separately. The old rule is the pre-Phase-1 concurrent cover, where anything after any head of the cut is excluded. On today's code that is the line `if (count === concurrentTo.size)` in `dag/src/idx/concurrent_cover.ts` changed to `count > 0`. The raw cut is `resolveForeignTableViewIn` without `canonicalVersion` on `foreignAt` (`group.ts` line 620).

| Concurrency rule | Foreign cut | History A | Local history |
| :--- | :--- | :--- | :--- |
| ideal (today) | canonical (today) | flips, bounded misses it | flips, bounded misses it |
| old | canonical | flips, bounded misses it | flips, bounded misses it |
| ideal | raw union | flips, bounded misses it | flips, bounded misses it |
| old | raw union (HEAD~1) | no flip | flips, bounded misses it |

History A needs one thing: the row's own causal head `g` must not shield the cut `{g, vW}`. Under the old rule with a raw union, the delete `r` is after `g`, so it is not concurrent with the cut, and the row stands. The ideal rule and canonical folds each remove that shield.

Going back is not an option, for three reasons:

- **The old pair is representation-dependent.** Rows two and four ask about the same position and get opposite answers. A dominated head changes the result. That is exactly the stale-head shield the observed-cut attacks used: a published version carrying an old head makes a later revoke look later than the cut.
- **It would not fix the floor.** The local history fails in all four rows. It uses only single-head concurrency, so neither factor touches it. The floor was already unsound before Phase 1, and History A is one more way in.
- **The flip in History A is what the semantics says.** A concurrent barrier lifts the row to the barrier's cut. A delete concurrent with that cut kills the row. Nothing is wrong with the direct reads at either horizon; only the early stop is wrong.

---

## 3. Why the meet is not enough

The floor today is the meet of `fork.common`, lowered by `projectForeignBound` for each observed object (`combineObserverRevisionBounds` in `mvt/src/delta.ts`).

The meet's argument is correct as far as it goes. `fork.common` is the set of shared entries that have a successor on only one side. Every new entry, in the past of `end` and not of `start`, sits above one of them. The past of the meet is the intersection of the pasts of the common entries. So every new entry is after every entry in the past of the meet. A new entry is never concurrent with anything below the meet.

That says nothing about entries above the meet that are already in the past of `start`. Such an entry can be concurrent with an entry below the meet, and it can also be concurrent with a new entry. If it is a barrier, the entry below the meet reads it. If a new entry changes its verdict, the change reaches the entry below the meet. That is the local history: `T` is in the past of `start`, not of the meet, concurrent with `E` below and with the new `N`.

`projectForeignBound` repeats the mistake for foreign reads. It classifies only the observes in the causal past of the meet, from the ref-advance cover of the meet downward. It decides stability from the observe's own published version. An entry's foreign cut also folds observes concurrent with it, which is History A. A gated observe's verdict also reads the cuts of concurrent observes that strictly dominate it, which is History B. And it puts the lowest unstable observes in the floor itself. Floor entries are not walked, so if one of them is gated and its own verdict flips, the op channel cannot report it.

The comments that call the floor safe are wrong: the block above `computeObserverRevisionBound` in `mvt/src/delta.ts` ("provably safe for any object"), the strategy note in the header of `rtable_group/delta.ts` ("The at-use semantics make this floor exact"), and the header of `rtable/delta.ts` ("at-use semantics make this floor exact").

---

## 4. What a sound floor has to satisfy

Write $U$ for the past of the floor, including the floor's own entries. The walk visits exactly the past of `end` outside $U$.

Every concurrent read in a verdict filters on the `barrier` tag. That covers concurrent row deletes (`killedByConcurrentDelete` in `rtable/view.ts`), observe widening (`findConcurrentRefAdvanceBarriers`), and concurrent schema deploys (`group.ts`, the schema widening query). It also holds in std_types: RSet barrier adds and deletes, RCap Kill 2, and RCap delete-cap. No verdict reads a non-barrier entry across branches. In rdb every delete, observe, and schema deploy carries the tag on the outer meta key (`scopes.ts`).

The floor is sound when $U$ meets three conditions:

1. **$U$ is in the past of the meet.** In particular it is in the past of `start`, so it holds no new entry.
2. **No barrier outside $U$ is concurrent with an entry of $U$.** Every barrier in the past of `end` that is not in $U$ has all of $U$ in its strict past.
3. **Every observe in $U$ is stable.** Its published version is at or below the floor of the object it observes, and that floor is itself sound.

### Why an entry in $U$ cannot change

Take an entry $E$ in $U$. Its verdict at a horizon reads four things:

- entries in its own past;
- barriers concurrent with it that are visible from the horizon;
- foreign objects, at cuts folded from the observes it reads, from foreign horizons folded from the observes in the past of the horizon;
- recursively, the verdicts of every entry it consults while doing so.

Its own past is inside $U$, because $U$ is down-closed. By condition 2, every barrier concurrent with $E$ in the past of `end` is in $U$. By condition 1, $U$ is in the past of `start`, so the same barriers are visible from `start`. Every entry the computation consults is in $E$'s past or is one of those barriers, so it is in $U$, and the same argument applies to it. The local part of the computation runs over the same entries, with the same visibility, at both horizons.

The observes $E$ reads are in $U$, so by condition 3 their versions are at or below the observed object's floor $b_o$. A union of versions at or below $b_o$ is at or below $b_o$. The foreign horizon moves between `start` and `end`. Inside the foreign object, the same three conditions hold for $b_o$, so a read at a cut at or below $b_o$ is the same from any horizon whose past contains that of $b_o$ and is contained in the foreign version at `end`. Both foreign horizons are such horizons: observes are monotonic, and the foreign version at `start` is at or above the one at the meet. Bindings form a DAG, so this induction on observed objects is well-founded. The schema is one of the observed objects.

A foreign read at a multi-head cut $V$ at or below $b_o$ is covered too. A foreign barrier outside the foreign $U$ has every head of $V$ in its strict past, so it is after $V$ under either concurrency rule. It is not concurrent with $V$.

So $E$'s verdict is the same at `start` and at `end`. That includes the floor's own entries, which is why floor entries need not be walked.

The row channel follows. A row id that no walked entry mentions has all its ops in $U$. Their verdicts are unchanged. Any barrier that touches the row is either in $U$ or walked, and a walked one mentions the row. So `deltaRowState` agrees at both horizons.

### The greatest such floor

The down-closed sets that satisfy conditions 1–3 are closed under union. A barrier outside the union is outside each set, so if it were concurrent with an entry of one set, that set would violate condition 2. So there is a greatest sound $U$. §5 computes it: every entry it removes is one that every sound $U$ must exclude.

---

## 5. Computing the floor

Start at the meet and lower the floor until conditions 2 and 3 hold. Lowering is a meet with the predecessors of one entry: for an entry $Z$ outside $U$, $\mathrm{glb}(\mathit{floor}, \mathrm{preds}(Z))$ removes from $U$ exactly the entries that are concurrent with $Z$ or above it. Nothing in $U$ is after $Z$, because $Z$ is not in $U$.

```text
floor := meet

-- condition 3
for each observed object o:
    b_o := o.computeDelta(refAtMeet_o, refAtEnd_o).revisionBound      -- sound, by the same procedure
    for each lowest unstable observe X of o in the past of the meet:
        floor := glb(floor, preds(X))

-- condition 2
checked := {}
loop:
    region := walkEntriesBackwardsToBound(end, floor)     -- the past of end outside U
    lowered := false
    for Z in region with Z barrier and Z not in checked:
        checked += Z
        if not atOrAbove(preds(Z), floor):                  -- some entry of U is concurrent with Z
            floor := glb(floor, preds(Z))
            lowered := true
    if not lowered: return (floor, region)
```

Notes:

- **Lowest unstable observes.** This is the descent `projectForeignBound` already does, from the ref-advance cover of the meet. A stable observe stops it, and by monotonicity everything below a stable observe is stable. The change is what happens to the result: the floor goes to the observes' predecessors, not to the observes themselves. Observes above the meet need no classification here. They are barriers outside $U$, and the loop handles them.
- **A barrier that passes stays passed.** If `preds(Z)` is at or above the floor, it stays at or above every lower floor. Only barriers newly exposed by a lowering need a check, which is what `checked` is for. The floor only descends and the DAG is finite, so the loop ends.
- **`atOrAbove(preds(Z), floor)`** is `findForkPosition(floor, preds(Z)).forkA` being empty. **`glb`** is the `commonFrontier` fold `computeForkMeet` already uses. An empty floor means a full walk.
- **The last region is the walk.** `computeRTableGroupDelta` feeds it to the accumulator and to `computeOpVerdictFlips` instead of walking again.
- **Batching.** Instead of one fork query per barrier, record successors while walking the region, close each floor head upward, and intersect. A barrier in the region that is not after every floor head is a violation. This is the forward-map trick in `concurrent_cover.ts`. Do it if the per-barrier queries show up in profiles.

### The three histories

- **History A.** The meet is `s1`. `A` publishes `g`, which is at or below `b = g`, so there is nothing for condition 3. `W` is a barrier outside $U$ whose predecessors are `{A}`, and `s1` is not at or below `A`, so the floor becomes `A`. `Z`'s predecessors are `{s1}`, which is above `A`. The floor is `A`, and `E` is walked.
- **The local history.** The meet is `s1`, and there are no observes. `T` is a barrier with predecessors `{x}`, so the floor becomes `x`. `N` is above `x`. `E` is above `x`, so it is walked.
- **History B.** The meet is `M`. `C` is unstable, so the floor becomes $\mathrm{glb}(M, \{E\}) = E$. `Y` is a barrier outside $U$ with predecessors `{A}`, so the floor becomes `A`. `E` is walked. `Y` was stable; condition 2 catches it without deciding whether its verdict can flip.

### Against today's floor

The new floor is never above today's. It starts at the meet, and condition 3 already lowers it to the predecessors of the observes today's projection stops at. When no barrier outside $U$ is concurrent with anything inside, as in a history whose observes and deletes form a chain over the meet, the walk is today's walk plus the lowest unstable observes themselves. Those are walked now, so their own verdict flips reach the op channel.

---

## 6. Precision and cost

Condition 2 treats every barrier outside $U$ as possibly changed. That is conservative. A barrier above the meet that nothing new touches still drags the floor down to its fork with the branch below. On a branchy history with deletes on side branches, the floor can reach the root, and the bounded delta costs as much as the full one. That is the honest price of at-use reads of concurrent barriers. It is not a regression in correctness.

Two refinements, both deferred until the simple version has numbers:

- **Exact change set.** Compute the entries that can actually change. The sources are new entries and observes whose version is not at or below $b_o$. The set is closed upward, and closed under "concurrent with a changed barrier". Condition 2 then only needs to hold for changed barriers. The proof in §4 goes through, with the closure supplying the fact that an unchanged barrier outside $U$ really is unchanged. This keeps an untouched old side branch from lowering the floor.
- **Relevance.** A delete barrier only affects entries whose verdicts read its table: its own rows, FK targets, `exists` witnesses. An observe only affects entries that read its binding. Filtering condition 2 by the tables an entry's ops read needs the schema at the entry, so it is more machinery. Leave it for later.

Today `computeObserverRevisionBound` gets $b_o$ by running the observed object's whole bounded delta. That cost is unchanged by this plan. A `computeRevisionBound` method that skips the accumulator is an easy follow-on.

---

## 7. Composition with the observed objects, and the other types

- **Foreign groups.** A foreign group's bounded `computeDelta` returns the new floor, so condition 3 composes by induction.
- **RSchema.** Its verdicts read no concurrent entries (no concurrent cover query under `rschema/`). Conditions 2 and 3 are empty, and its meet floor (`rschema/delta.ts`, `computeForkMeet`) is sound by the same argument. Say so in a comment there. Concurrent schema deploys are read by the group, and they are barriers in the group's DAG, so the group's condition 2 covers them.
- **RSet and RCap.** These are the same problem. RCap reads concurrent barriers (Kill 2, delete-cap), so its meet floor is suspect for the same reason as the local history; this has not been built. RSet's histories in `PHASE_5_PLAN_CRITIQUE_v2.md` (H1, H1b, and the Phase 5 history H2) are instances of conditions 2 and 3. Both types should move to the same helper before Phase 5. This plan replaces critique v2's §4 repair.
- **RFileMap.** It has its own `computeDelta` and reads concurrent barrier removes (`presence.ts`). Check its floor against the same conditions as part of Phase 6.

---

## 8. Changes

**`mvt/src/refs.ts`.** Extract the descent out of `projectForeignBound` as `findLowestUnstableRefAdvances(observerDag, refId, referencedDag, localAt, foreignBound): Position`. It returns the lowest unstable ref-advances, or an empty position when none is unstable. Keep `projectForeignBound` for RSet until RSet moves; it becomes the old floor.

**`mvt/src/delta.ts`.** Add the floor computation of §5:

```ts
computeSafeFloor(
    observer: RObject,
    rawDag: dag.Dag,
    meet: Version,
    end: Version,
    referenced: RObject[],
    isBarrier: (entry: dag.Entry) => boolean,
): Promise<{ floor: Version; walked: dag.Entry[] }>
```

The observer's ref-advance queries go through its scoped DAG, as `computeObserverRevisionBound` does now. The walk goes through the raw DAG. Rewrite the comment block on the reported revision bound: the meet is a sound floor only for objects whose verdicts read no concurrent entry. Say why in the terms of §3.

**`rdb/src/rtable_group/delta.ts`.** In the bounded branch, replace `combineObserverRevisionBounds` and the second walk with `computeSafeFloor`. `isBarrier` checks the outer `barrier` meta key for `'t'`. Replace the strategy note in the header with the three conditions.

**`rdb/src/rtable/delta.ts`.** Fix the header comment (lines 13–14) that says voiding flips sit above the combined revision bound and that at-use semantics make the floor exact.

**`rdb/src/rschema/delta.ts`.** One comment: why the meet is enough here.

`Delta.getRevisionBound()` reports the new, lower floor. Check consumers of `revisionBound` for anything that assumed it equals the meet or the old projection.

---

## 9. Tests

**rdb, registered in `test/run_tests.ts`.** Each asserts that full and bounded agree on `opVerdictChanges` and on row changes, and that the direct reads flip as described, so a pass cannot come from a history that stopped flipping.

- `[FLOOR01]` History A, from `observe_floor_proof.ts`.
- `[FLOOR02]` the local history, from `local_floor_proof.ts`.
- `[FLOOR03]` History B, built from the description in the bug proof.
- `[FLOOR04]` a gated observe that today's projection puts in the floor, whose own verdict flips. Build it against today's code first to confirm the op channel misses it, then keep it as a regression.
- `[FLOOR05]` a chain history: the new floor equals the predecessors of today's projected floor, and the walk differs from today's only by those observes.

Delete the two proof scripts once `[FLOOR01]` and `[FLOOR02]` exist.

**mvt, `test/delta_tests.ts`.** Use a stub observer on hand-built DAGs, as `[DELTA_02]` does:

- a barrier above the meet concurrent with an entry below it lowers the floor to their fork;
- an unstable ref-advance below the meet lowers the floor to its predecessors, and a stable one does not;
- a lowering that exposes a second violating barrier lowers again;
- a chain gives the meet back.

**Parity.** The random sweep does not build these shapes. Extend `rdb/test/delta_parity/generate.ts` so that:

- `start` is often a multi-head frontier;
- deletes restricted by `exists` land on side branches;
- observes are appended below the frontier, concurrent with rows;
- non-creator gated observers are revoked on foreign branches concurrent with versions others observe.

While the sweep runs, assert that the new floor is at or below the old one. That is a cheap check on the implementation, and it can be removed afterward.

---

## 10. Order

1. Register `[FLOOR01]` and `[FLOOR02]` as failing tests. Build `[FLOOR03]` and `[FLOOR04]` and confirm they fail.
2. The mvt helper and its unit tests.
3. Switch rdb to it. Fix the comments. Run the rdb suite, the four floor tests, and the existing parity.
4. Extend the generator and run parity.
5. Follow-ons, in order: RCap and RSet onto the helper, which is a prerequisite for Phase 5; the RFileMap check; the exact change set from §6 if profiles ask for it.

## Exit

- `[FLOOR01]`–`[FLOOR05]` and the mvt floor tests pass.
- The rdb suite passes, including `[OCUT]`, the observe-gate tests, and `[OPDELTA]`.
- The rdb delta parity passes with the extended generator.
- No verdict and no direct read changes. Only bounded deltas change, and only by reporting what the full delta already reports.

## What this does not change

- The ideal concurrency rule, canonical folds, the observe gate's two layers, and use-before-revoke.
- Any verdict, any view, or any replicated state. The bug was never in what a replica stores or reads at a horizon.
- The full delta strategy.
