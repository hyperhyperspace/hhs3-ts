# Fixing the bounded-delta floor, v2

**Status:** execution plan. It replaces the loop in §5 and the sections §8–§10 of [`RDB_OBSERVE_FIX_PLAN.md`](./RDB_OBSERVE_FIX_PLAN.md). The definition of the floor does not change: the three conditions and the soundness argument in §4 of that plan, restated compactly in §1 of [`FLOOR_CURSOR.md`](./FLOOR_CURSOR.md). The bug is in [`RDB_OBSERVE_BUG_PROOF.md`](./RDB_OBSERVE_BUG_PROOF.md).

The cursor cache in §3 of `FLOOR_CURSOR.md` is deferred. [`FLOOR_CURSOR_OPTIMIZATION.md`](./FLOOR_CURSOR_OPTIMIZATION.md) is the landscape for making the floor cheap. This plan does its step 1, the batch round, and gates the rest on measurements.

---

## 1. What changes from v1

- **Batch only.** The floor is computed per `computeDelta` call from `(start, end)`. No state is kept between calls.
- **Rounds over exposed bands.** v1's loop walks back from `end` to the current floor once per lowering and tests every barrier with its own fork query. Here each round tests only the barriers that the last lowering exposed, with one forward map per round. The union of the bands is the walked region, and it is ingested as-is.
- **An exact region walk.** `walkEntriesBackwardsToBound` over-walks below the floor (§4). The barrier test would misfire on the extra entries, so the floor computation gets its own walk.
- **A brute-force oracle.** The mvt unit tests compare the round-based floor with a slow reference loop on random DAGs.
- **A measurement step.** Nothing in `FLOOR_CURSOR_OPTIMIZATION.md` past the batch round gets built until the numbers from §7 ask for it.

## 2. Why no cursor cache now

The math in `FLOOR_CURSOR.md` §3 holds. With the cursor fixed, the meet, each \(b_o\), and the greatest floor only move toward the origin as the head grows, and the incremental update gives the greatest floor again. Its payoff does not match the consumers that exist:

- **The projector advances its checkpoint to the end of every delta.** `projectTo` computes `group.computeDelta(checkpoint, to)` and writes `to` as the new checkpoint in the same `apply` ([`rdb_adapter/src/project.ts`](../rdb_adapter/src/project.ts), lines 144–153). The cache would be dropped and reseeded on every call. The only reuse is across the entries that arrive between two runs, and folding those in one at a time costs about the same as one round over them.
- **The cache would need to be persisted with the checkpoint.** Several projectors can share a store, and `rdb_projection/src/projection.ts` retries when another one advanced the checkpoint first. The pair \((c, G)\), and the \(b_o\) under it, would have to be written and compare-and-swapped together with the checkpoint in every target adapter.
- **A seed is rarely available.** The cache can only be seeded when the cursor equals the head. `to` is often already behind the head by the time `apply` commits.
- **It does not change the floor.** The expensive case in §6 of v1 is an old side-branch barrier that drags the floor toward the root. Every reseed pays for that again. The exact change set in §6 of v1 is the lever for it, not the cache.

Revisit the cache when either of these shows up:

- a consumer that calls `computeDelta(c, head)` repeatedly with a fixed `c` while the head grows;
- the §7 numbers show the floor computation, not the walk or the ingest, dominating the projector after steps 1–4 of `FLOOR_CURSOR_OPTIMIZATION.md` §6.

The round of §3 is exactly what the cache's incremental update runs on each new entry. Building it first leaves that path open.

---

## 3. The computation

Notation as in `FLOOR_CURSOR.md` §1. \(U\) is the past of the floor, including the floor's own entries. \(\mathrm{meet}\) is `computeForkMeet` over `fork.common`. \(\mathrm{glb}\) of several positions is the frontier of the intersection of their histories, computed today by `dag.computeMeet` folding `findForkPosition(...).commonFrontier`.

```text
computeSafeFloor(observer, rawDag, meet, end, referenced, isBarrier):

    -- condition 3: unstable observes in the past of the meet
    lower := []
    for each o in referenced:
        refAtMeet := ref version of o at meet
        refAtEnd  := ref version of o at end
        b_o := o.computeDelta(refAtMeet, refAtEnd).revisionBound
        for each X in findLowestUnstableRefAdvances(observer, o, meet, b_o):
            lower += preds(X)
    floor := glb(meet, ...lower)

    -- condition 2: rounds
    (band, forward) := walkRegionAboveFloor(rawDag, end, floor)
    region := band
    loop:
        violators := [ Z in band | isBarrier(Z) and not afterAll(floor, Z, forward) ]
        if violators is empty:
            return { floor, walked: region }
        newFloor := glb(floor, ...preds(Z) for Z in violators)
        (band, forward) := walkRegionAboveFloor(rawDag, floor, newFloor)
        region := region ∪ band
        floor := newFloor
```

`afterAll(floor, Z, forward)` is true when every head of `floor` reaches `Z` through the forward map of the band. It is the count used in [`dag/src/idx/concurrent_cover.ts`](../dag/src/idx/concurrent_cover.ts): walk upward from each head over the recorded successors and count how many heads reach each entry. One upward walk per head per round answers it for every barrier in the band. An empty floor has no heads, so every barrier passes, which matches condition 2 for an empty \(U\).

### Why this is the greatest floor

Every lowering is forced, so the result is the greatest sound floor:

- **Condition 3.** An unstable observe \(X\) in \(H(\mathrm{meet})\) has to be outside \(U\). \(X\) is a barrier, so condition 2 then puts all of \(U\) strictly before \(X\), that is \(U \subseteq H(\mathrm{preds}(X))\). Lowering to `preds(X)` removes nothing that a sound \(U\) could keep. The descent from the ref-advance cover of the meet reaches every unstable observe in \(H(\mathrm{meet})\), or one below it, because observes are monotonic: everything below a stable observe is stable.
- **Condition 2.** A violator \(Z\) is outside \(U\) and not after every head of the floor. Any sound \(U\) has \(U \subseteq H(\mathrm{preds}(Z))\). A round lowers by the glb of all its violators at once, which is the intersection of forced constraints.

### Why a round only tests the band

- **A barrier that passed stays passed.** Every head of the floor is strictly before it, and every later floor is in the past of the current one.
- **A violator passes after the lowering.** \(H(\mathit{newFloor}) \subseteq H(\mathrm{preds}(Z))\).
- **Only the band can hold new violators.** The new band \(H(\mathit{floor}) \setminus H(\mathit{newFloor})\) holds the only barriers that have not been tested.
- **The band's forward map is enough.** A path from a head of `newFloor` to a band barrier stays in the band. An entry on that path is after that head, so it is not in \(H(\mathit{newFloor})\), whose heads are an antichain. It is at or below the barrier, so it is in \(H(\mathit{floor})\).

The floor only descends and the DAG is finite, so the loop ends. Each entry of the final region is walked once, in the round that exposed it, and each barrier is tested once.

### The three histories

These are unchanged from §5 of v1, now as rounds:

- **History A.** There are no unstable observes. The first region is `{Z, W}`. Round 1 passes `Z`, whose predecessor is `s1`, and finds `W`, whose predecessors are `{A}`. The floor drops to `A`. The next band is `{E, s1}`, with no barrier, so `E` is walked.
- **The local history.** The first region is `{N, T}`. Round 1 passes `N` and finds `T`, and the floor drops to `{x}`. The next band holds `E` and `s1`, and no barrier in it violates.
- **History B.** Condition 3 lowers the floor for `C` to \(\mathrm{glb}(M, \{E\})\). Round 1 finds `Y`, and the floor drops to `A`.

---

## 4. The region walk

`walkEntriesBackwardsToBound(rawDag, from, bound)` stops only at entries that are members of `bound`. A walked entry whose predecessor is in \(H(\mathit{bound})\) but is not a head of `bound` keeps going below the floor. In the local history today, the bound is `{s1}`, `T` is a head of `end`, and its predecessor `x` is not `s1`. So the walk loads `x` and every ancestor of `x`.

For today's callers this only costs time. An over-walked entry is in the past of both horizons, so the accumulator and `computeOpVerdictFlips` see no change. For the rounds it is a bug: an over-walked barrier is inside \(U\), fails `afterAll`, and drags the floor down. The floor stays sound, but it is no longer the greatest.

The exact walk uses the fork. If a region entry has a predecessor in \(H(\mathit{floor})\), that predecessor has a successor outside \(H(\mathit{floor})\). So it is in `findForkPosition(floor, from).common`, and that set is the right stop set:

```ts
walkRegionAboveFloor(
    rawDag: dag.Dag,
    from: Version,
    floor: Version,
): Promise<{ entries: dag.Entry[]; forward: Map<B64Hash, B64Hash[]> }>
```

It returns \(H(\mathit{from}) \setminus H(\mathit{floor})\), and the successor map over its edges, including edges out of the stop set. An empty floor has an empty `common`, and the walk covers all of \(H(\mathit{from})\).

Leave `walkEntriesBackwardsToBound` and `walkDelta` as they are for RSet, RCap and RSchema. Fix the comment on `walkEntriesBackwardsToBound`, which says it returns entries "strictly above the revision bound."

---

## 5. Changes

**`mvt/src/refs.ts`.** Extract the descent out of `projectForeignBound` as:

```ts
findLowestUnstableRefAdvances(
    observerDag: ScopedDag,
    refId: B64Hash,
    referencedDag: CausalDag,
    localAt: Version,
    foreignRevisionBound: Version,
): Promise<Position>
```

It returns the set `projectForeignBound` builds in `bound`, and an empty position when no observe is unstable (not `localAt`). `projectForeignBound` becomes a wrapper with today's fallback, so RSet keeps working until it moves.

**`mvt/src/delta.ts`.**

- Add `walkRegionAboveFloor` (§4).
- Add `computeSafeFloor` (§3):

  ```ts
  computeSafeFloor(
      observer: RObject,
      rawDag: dag.Dag,
      meet: Version,
      end: Version,
      referenced: RObject[],
      isBarrier: (entry: dag.Entry) => boolean,
      stats?: FloorStats,
  ): Promise<{ floor: Version; walked: dag.Entry[] }>
  ```

  Ref-advance queries go through the observer's scoped DAG, as `computeObserverRevisionBound` does now. The walk goes through the raw DAG. `FloorStats` is an optional counter object for §7: rounds, band sizes, barriers tested, violators, glb inputs, fork queries, and time spent in each \(b_o\).
- Rewrite the comment block "On the reported revision bound." The meet is a sound floor only for objects whose verdicts read no concurrent entry. Say why, using §3 of v1.
- Mark `combineObserverRevisionBounds` and `computeObserverRevisionBound` as the old floor, kept for RSet only.

**`rdb/src/rtable_group/delta.ts`.**

- In the bounded branch, replace `combineObserverRevisionBounds` and the second walk with `computeSafeFloor`.
- Feed `walked` to the accumulator and to `computeOpVerdictFlips`.
- `isBarrier` checks that the outer `barrier` meta key contains `'t'`. That covers deletes (`scopes.ts`), and observes and schema deploys (`createRefAdvanceMeta` defaults to a barrier).
- Replace the strategy note in the header with the three conditions and a pointer to this plan.

**`rdb/src/rtable_group/group.ts`.** The comment above `getObservedObjects` (around line 1296) points to `combineObserverRevisionBounds`. Point it to `computeSafeFloor` instead.

**`rdb/src/rtable/delta.ts`.** Fix the header comment that says voiding flips sit above the combined revision bound and that at-use semantics make the floor exact.

**`rdb/src/rschema/delta.ts`.** Add one comment: RSchema verdicts read no concurrent entry, so the meet is sound here. Concurrent deploys are barriers in the group's DAG and are covered by the group's condition 2.

**Consumers of `revisionBound`.** `RTableGroupDelta.revisionBound` now reports the lower floor. The only readers outside tests are the REPL formatter (`rdb_repl/src/format/delta.ts`), which prints it, and the foreign-floor recursion itself. Neither assumes it equals the meet. `local_floor_proof.ts` prints whether it is `s1`, and that script goes away in step 1 of §8.

---

## 6. Tests

**rdb, registered in `test/run_tests.ts` as a new suite in `test/floor_tests.ts`.** Each test asserts three things:

- the full and bounded deltas agree on `opVerdictChanges` and on row changes;
- the direct reads flip as described, so a pass cannot come from a history that stopped flipping;
- `revisionBound` is the expected floor.

The tests:

- `[FLOOR01]` History A, from `observe_floor_proof.ts`. The floor is `A`.
- `[FLOOR02]` the local history, from `local_floor_proof.ts`. The floor is `{x}`.
- `[FLOOR03]` History B, built from the description in the bug proof. The floor is `A`.
- `[FLOOR04]` a gated observe that today's projection puts in the floor, and whose own verdict flips. Build it against today's code first to confirm that the op channel misses it.
- `[FLOOR05]` a chain history. The floor is the meet lowered to the predecessors of the lowest unstable observes, and the walk is today's walk plus those observes.
- `[FLOOR06]` over-walk. A barrier on a side branch that joins below a floor head, inside \(U\). The floor must not drop for it, and the walk must not load it.

Delete the two proof scripts once `[FLOOR01]` and `[FLOOR02]` exist.

**mvt, `test/delta_tests.ts`.** Use stub observers on hand-built DAGs, as `[DELTA_02]` does:

- `[DELTA_03]` `walkRegionAboveFloor` returns exactly \(H(\mathit{from}) \setminus H(\mathit{floor})\) when a side branch joins below a floor head, and the whole history for an empty floor.
- `[DELTA_04]` a barrier above the meet that is concurrent with an entry below it lowers the floor to their fork.
- `[DELTA_05]` an unstable ref-advance below the meet lowers the floor to its predecessors, and a stable one does not.
- `[DELTA_06]` a lowering that exposes a second violating barrier triggers a second round, and the first round's barriers are not tested again (checked through `FloorStats`).
- `[DELTA_07]` a chain gives back the meet.
- `[DELTA_08]` the oracle. On random DAGs with random barrier tags and random `(start, end)` pairs, compare `computeSafeFloor` with a reference loop that lives in the test file. Each iteration of the reference loop recomputes the exact region from scratch, tests every barrier in it with `findForkPosition(floor, preds(Z))`, and lowers by one violator. The floors must be equal. Seed the generator and print the seed on failure.

`refs_tests.ts` keeps `[REFS_08]` for the wrapper. Add one case for `findLowestUnstableRefAdvances` returning an empty position when everything is stable.

**Parity.** Extend `rdb/test/delta_parity/generate.ts` as in §9 of v1:

- `start` is often a multi-head frontier;
- deletes restricted by `exists` land on side branches;
- observes are appended below the frontier, concurrent with rows;
- non-creator gated observers are revoked on foreign branches concurrent with versions others observe.

While the sweep runs, assert that the new floor is at or below the old one (`combineObserverRevisionBounds`). Remove the assertion once the sweep is green.

---

## 7. Measurement

The goal is to know which step of `FLOOR_CURSOR_OPTIMIZATION.md` §6 to take next, if any. Wire `FloorStats` into the parity sweep and into one benchmark script, `rdb/test/floor_bench.ts`. Do not register the script in `run_tests.ts`.

The benchmark runs each shape at a few sizes (\(10^2\), \(10^3\), \(10^4\) entries), for the bounded delta under the new floor, under the old floor (a scratch branch, or the strategy switch kept for the benchmark), and for the full delta:

- **Chain.** A long chain with deletes and observes, where `start` is one entry behind `end`. The new floor should equal the meet, and the cost should match today's.
- **Old side branch.** A long chain plus a delete on a side branch that forked early and that `start` already includes. This is the case that drags the floor down. Record how far it drops and what that costs compared with the full delta.
- **Projector cadence.** Replay a history in batches of 1, 10 and 100 entries, computing `delta(checkpoint, head)` and advancing the checkpoint each time, the way `projectTo` does.
- **Foreign depth.** One observer, one foreign group with many observed versions, and a second level of binding.

Report, per shape and size:

- wall time for the floor, the ingest, and the op-verdict flips;
- rounds, region size, barriers tested, violators, and glb inputs;
- time spent inside the \(b_o\) recursion.

Map the results to the next step:

| What dominates | Next step (`FLOOR_CURSOR_OPTIMIZATION.md` §6) |
| :--- | :--- |
| The \(b_o\) recursion, mostly the foreign ingest | 2. A floor-only `computeRevisionBound` that skips the accumulator |
| glb folds over `common` or over violators | 3. `meetMany` |
| The level-0 walks in `walkRegionAboveFloor` and the forward map on deep regions | 4. `afterAll` |
| The final region on the old-side-branch shape | 5. The exact change set (§6 of v1) |
| Floor work at projector cadence, after the steps above | Reopen §2 of this plan, the cursor cache |

Record the numbers in a short `FLOOR_BENCH.md` next to this plan, so the next decision is made against them.

---

## 8. Order

1. **Failing tests.** Register `[FLOOR01]` and `[FLOOR02]` as failing tests. Build `[FLOOR03]`, `[FLOOR04]` and `[FLOOR06]`, and confirm the first two fail on today's code. `[FLOOR06]` passes today, because the floor does not try to lower there.
2. **The region walk.** Add `walkRegionAboveFloor` and `[DELTA_03]`.
3. **The mvt helpers.** Add `findLowestUnstableRefAdvances`, `computeSafeFloor` and `FloorStats`, with `[DELTA_04]`–`[DELTA_08]` and the refs case.
4. **Switch rdb.** Switch rdb to `computeSafeFloor` and fix the comments. Run the rdb suite, the floor tests and the existing parity.
5. **Parity.** Extend the generator and run parity with the "at or below the old floor" assertion. Then remove the assertion.
6. **Measure.** Build `floor_bench.ts` and write `FLOOR_BENCH.md`.
7. **Follow-ons.** Moving RCap and RSet onto `computeSafeFloor` comes first, because it is a prerequisite for Phase 5 (§7 of v1). Then the RFileMap check. Then whatever the §7 table points to.

## Exit

- `[FLOOR01]`–`[FLOOR06]`, `[DELTA_03]`–`[DELTA_08]` and the refs case pass.
- The rdb suite passes, including `[OCUT]`, the observe-gate tests and `[OPDELTA]`.
- The rdb delta parity passes with the extended generator.
- `FLOOR_BENCH.md` exists, and it names the next optimization or says that none is needed.
- No verdict and no direct read changes. Bounded deltas change only by reporting what the full delta already reports.

## What this does not change

- The definition of the floor, the ideal concurrency rule, canonical folds, the two layers of the observe gate, and use-before-revoke.
- Any verdict, view, or replicated state.
- The full delta strategy, and the floors of RSchema, RSet and RCap, until step 7.
- The checkpoint format, and the target adapters.
