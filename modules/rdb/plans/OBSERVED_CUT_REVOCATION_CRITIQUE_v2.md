# Critique of the Critique and of the `ANY` Plan

This is a review of [`OBSERVED_CUT_REVOCATION_CRITIQUE.md`](./OBSERVED_CUT_REVOCATION_CRITIQUE.md) and of [`DESIGN_AND_PLAN_FOR_ANY.md`](./DESIGN_AND_PLAN_FOR_ANY.md). Section numbers cite those documents. Notation follows them.

The check was a re-read of the three DAG indexes, `resolveObserveGateRefAt`, `resolveRefVersionAtPosition`, `rowState` / `killedByConcurrentDelete`, `holdingIn`, `validateRefAdvanceMonotonicity`, and `[OCUT01]`–`[OCUT12]`. Those twelve tests pass against today's engine. The critique's `normalize` and `any` shim was not re-run. The columns it reports were re-derived from the code, and they match.

---

## Verdict

The critique's diagnosis is the one that matches the code, and the plan's switch to ideal concurrency is the repair that closes the holes `[OCUT]` pins. This note is about the places where the critique over-attributes, and where the plan is incomplete or states a measurement as a bound.

What stands:

- `findConcurrentCoverWithFilter` is `ALL` in all three indexes. The dominated-ancestor bug and the Mixed-Cut Attack are real. `[OCUT02]`, `[OCUT03]`, `[OCUT05]`, `[OCUT07]`, `[OCUT10]`, `[OCUT11]`, and `[OCUT12]` are live today for the reasons the critique gives.
- The interval rule in `OBSERVED_CUT_REVOCATION.md` §4.3 is unsound under `ALL`. The counterexample in the critique §4.1 (points 1–3) is a soundness violation on its own: the rule calls `d` definitely concurrent, and `d` is deferred in the completion where `y` is live.
- `ANY` plus today's whole-cut dominance is exactly complete. The proof in the critique §3.3 and the plan §3.3 holds. Strict inclusion of ideals stays a partial order, so observes stay off cycles and `observeVerdictLive` can keep throwing.
- `[CAP29]` is the collapse-X fixture the original document draws. Under `ANY` it flips. `[CAP30]` stays, because the revoke sits above both heads. Dropping B1 loses nothing under `ANY`.

---

## 1. The critique attributes too much to the original text

### 1.1 `[OCUT07]` is not a disagreement with §1.3

The critique §5 says normalization turns Alice's observe of `{v1}` into a retroactive void "by §1.3's own reasoning," and that the ideal reading is what makes the flip correct.

After `findMinimalCover`, the widened cut is `{v2}`, and `r` is concurrent with that only head. Normalized `ALL` voids it. `ANY` voids it. §1.3 is a different shape: an antichain in which one head really does precede `r`. On that shape the two rules disagree. On `[OCUT07]` they agree.

The original document should say that normalization flips `[OCUT07]`. The flip is not evidence that normalization betrays the rule the original is defending. "Past `r`'s fork" is the wrong picture of `v2`: `v2` is the sibling of `r`, both children of `v1`.

### 1.2 "Layer 2 is already safe" is qualified, and still incomplete

The executive summary, item 3, and §3.1 say Layer 2 is safe when the honest import is concurrent in `O` with the operation. That sentence is true. `resolveRefVersionAtPosition` unions the concurrent barrier, `r` enters the foreign cut, and `rowState` rejects when any live delete is in the causal cover.

§3.2's "strictly in the causal past in both DAGs" is about the singleton `{v1}` with a later import. That is `[OCUT06]` and `[OCUT08]`, and both rules leave them live.

What the original never analyzes is a later import against a cut that already contains a head below the revoke: padding (`[OCUT10]`) and rescue by a benign widener (`[OCUT05]`, `[OCUT11]`). Those are real holes under `ALL`. They are a missing case, not a contradiction of the sentences that were written. The critique §3.2(b) should say that.

### 1.3 The engine-throw trace is not part of the soundness result

Critique §4.1 points 1–3 are enough. A definite verdict that is false in one completion is unsound, whatever the solver does next.

Point 5 was traced by hand, and the trace assumes `z` is decided void and then flips to live once `y` enters `L`. Under the rule as written, `r` lies in `↓Vmax(y) \ ↓Vmin(y)` while `z` is an undecided widener, so `y` is undecided in that pass rather than a member that simply walks into `L`. `VerdictEvaluation.fixpoint` does throw on a non-monotone rule and on a final verdict that disagrees with the well-founded value. Which of those fires, and whether either fires, depends on the order of the member loop. Leave point 5 as an unverified consequence.

The "smaller gap" in the same section, `d ∈ ↑Vmax \ ↑Vmin`, is the counterexample. `c1` is a head of `Vmax` and not of `Vmin`, and `d > c1`, so `d` is outside `↑Vmin` and inside `↑Vmax`. The rule has no separate unhandled region beyond the one the counterexample already uses.

### 1.4 Cross-advance is common, and it is not every diverging pair

Critique §4.3 says any two observers who have synced different branches of a multi-writer `G` will cross-advance. Nested cuts do not. If `↓Vy ⊂ ↓Vz`, dominance runs one way and no component forms, today or under `≻∃`.

Cross-advance is the case where each cut is strictly ahead on a different head: `{a_far, b_near}` against `{a_near, b_far}`. That is an ordinary multi-writer sync state, which is why "rare" in the original §4.3 is wrong. It is a subclass of "synced different branches," which is why "any two" is wrong. The conclusion that stands is the narrower one: such pairs are independent today, and `≻∃` puts each of them in a component.

### 1.5 §4.1 of the original is the rule it names

Critique §4.5 calls the original §4.1 a strawman, on the grounds that a working interval rule would decide fully incomparable pairs live without `≻∃`.

The original §4.1 argues against a specific rule: drop the dominance filter, and return `undecided` whenever a concurrent observe is `undecided`. That rule does put an `[OBSGATE02]` pair in a component that collapses to void. The argument against that rule holds.

`≻∃` is what the original proposal actually uses to keep incomparable pairs acyclic. An interval rule precise enough to return live when the gap does not affect the gate would also save them, and no sound rule of that kind is on offer. The useful remark is that `≻∃` limits which pairs form components. It is the correctness mechanism for `[OBSGATE02]` inside the proposal that was written.

### 1.6 Attack 2b's liveness claim is conditional in the scenario itself

The original §4.3 says Admin's cap has no delete or update in `↓Vmax \ ↓Vmin`. Under that hypothesis the Lower-loop claim holds. Critique §4.6 is a real residual of the general rule: if `a_adv` deletes Admin's witness, the dependencies are an even cycle and the well-founded outcome is void for both. It is not a counterexample to the worked scenario. Say it as a missing side condition on the rule, which the scenario happens to satisfy.

### 1.7 The shim does not wrap every call

`NestedScopedDag.findConcurrentCoverWithFilter` delegates to its parent, and a table's scoped DAG is a `NestedScopedDag` over the group's `RootScopedDag`. The caps-table probes, including every `[OCUT]` case, go through that path.

`rfile_map/presence.ts` calls `findConcurrentCoverWithFilter` on the raw `Dag`. A prototype shim on `RootScopedDag` does not see it. The call passes `version(add)`, so `ALL` and `ANY` agree and the missed call does not change the suite result. "Intercepts every query" should be "intercepts every scoped query." The raw-DAG call belongs on the plan's caller list as already single-head, which the plan §2.1 already says.

---

## 2. The plan is the right change, with three holes in the write-up

### 2.1 The empty position flips under the definition as written

For `k ≥ 1` the plan's exclusion set is right:

```text
notConcurrentTo = ↓concurrentTo ∪ ⋂_{v ∈ concurrentTo} ↑v     (within ↓from)
```

A dominated element does not change the intersection, because its future contains its dominator's future. That is why `ANY` is a function of `↓concurrentTo` and why normalization is unnecessary for correctness.

For `k = 0` the same sentence says something else. `↓∅ ⊆ ↓x` is true for every `x`, so every entry is after the empty position and nothing is concurrent. Today's implementation starts `notConcurrentTo` from `concurrentTo` and adds nothing, so every entry is concurrent. The two disagree on every node.

The plan should pin this. Either document that the empty position is unreachable and assert it, or define `k = 0` explicitly and test it. An implementation that transcribes "intersect the futures" with a vacuous intersection will silently invert today's empty-position behavior.

### 2.2 Canonicalization misses the schema version everyone reads

The plan §3.4(b) names two schema constructions: `validate_ops.ts` around line 688, and `group.ts` around line 875 (`fkAdoptionKills`). The version every schema view is built from is `resolveSchemaVersion` (`group.ts`, lines 405–411): pinned genesis unioned with `resolveRefVersionAtPosition`, returned as-is.

`RSchemaImpl.resolveAt` already minimal-covers before the state-cache key (`rschema.ts`, line 214), so a non-canonical argument does not split that cache. `getView` then stores the un-normalized set on the view (`rschema.ts`, line 207), and `RSchemaViewImpl.getVersion` returns it. Callers that compare `resolveSchemaVersion`'s return value, or that read `getVersion()`, still see the dominated ancestor.

Canonicalize the return of `resolveSchemaVersion`. The two cited sites are the deploy-admission union and a local copy inside `fkAdoptionKills`. `getView` normalizes the second for state resolution. Neither citation covers the read path.

The admission table has a related slip. Schema deploy is one `validateRefAdvanceMonotonicity` call, at line 684, inside `validateDeploy` which begins at line 633. Line 636 is `extractRefVersion`. The four sites are foreign observe (line 574), schema deploy (line 684), RFiles (`access.ts`, the monotonicity call at line 232; the presence loop is lines 228–230), and RSet (`validate.ts`, line 153).

The verdict-frame concern is real and is not the schema cache. `resolveVerdict` keys on the sorted `from` (`group.ts`, line 1071). On a foreign table view that `from` is `foreignFrom`. Two encodings of one ideal are two frames. Under `ANY` they agree, so the split is duplication. `resolveForeignTableViewIn` and `diagnoseObserve` are the calls that have to canonicalize, and the plan already lists them.

### 2.3 "Only `[CAP29]` flips" is the scope of one shim run

The critique §1 says the other `std_types` suites were not run under the shim. The critique §3.4 and the plan §4 then say `[CAP29]` is the only existing test that flips. The plan's comparison table repeats it as the blast radius.

That is the result for the RCap tests and the permissioned-set tests. `supportBarrierAdd` passes `this.at` to the primitive (`rset/view.ts`, the barrier-add search). A barrier add above one head of a multi-head `at` is "after" today and concurrent under `ANY`, so the element joins. The plan §3.5 marks this "not specified" in `MODERNIZATION_v2.md` and adds a test in Phase 1. Until simple-set and nested-set have been run under the shim, the table should say "measured so far" rather than "existing tests that flip."

Phase 1's exit criterion already runs the whole `std_types` suite. The hole is in the decision record, which treats the partial measurement as the cost. The same sentence in the critique §3.4 needs the same qualifier. It is the source of the plan's wording.

### 2.4 One slogan is stronger than the rule

Plan §4, reason 1, says the party being judged cannot influence its verdict. §3.1 is the accurate statement. Adding a head can only add barriers. Omitting heads, down to a cut entirely below the revoke, still dodges it. That remaining dodge is `[OCUT06]` and `[OCUT08]`, and both documents correctly keep those live. Reason 1 should keep the qualification, so "cannot influence" is not read as "cannot choose a cut."

---

## 3. Edits worth making before implementation

In the critique:

- §3.2(b): the Layer 2 holes are cases the original does not analyze. The concurrent-import sentence is true.
- §4.1 point 5: mark the throw as unverified. Points 1–3 carry the result. Drop "smaller gap" as a separate item.
- §4.3: cross-advance means each cut is strictly ahead on a different head.
- §4.5: the naive undecided rule really does void `[OBSGATE02]`. `≻∃` is how that proposal avoids it.
- §5, the `[OCUT07]` bullet: both rules void the normalized cut. The original's omission is that it never says the verdict flips.

In the plan:

- §3.2: define `k = 0`, and add the empty position to the hand-built `dag` tests in Phase 1.
- §3.4(b): canonicalize `resolveSchemaVersion`. Drop line 636 as an admission site. Keep the verdict-frame reason; the schema state cache is already canonical.
- §4, the comparison table and reason 5: "measured on RCap and permissioned-set; the rest of `std_types` is a Phase 1 exit check." Qualify reason 1 with the stale-cut dodge from §3.1.
