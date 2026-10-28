# Critique of `OBSERVED_CUT_REVOCATION.md`

This is a review of [`OBSERVED_CUT_REVOCATION.md`](./OBSERVED_CUT_REVOCATION.md). Section numbers like "§3.3" refer to that document unless stated otherwise. Notation follows it: $\downarrow V$ is the causal past (order ideal) of a version $V$ in the observed DAG $G$, $\hat V = \text{minCover}_G(V)$ its heads, and "Step 1 / Step 2" are the causal-cover and concurrent-barrier checks.

---

## Verdict

The document's diagnosis is mostly right, but the evidence doesn't support its prescription.

**Verified as correct:**

- `findConcurrentCoverWithFilter` implements `ALL` in all three DAG indexes.
- The dominated-ancestor bug is real. Normalizing `concurrentTo` fixes it.
- The Mixed-Cut Attack on gated observes (§3.3) is real against today's code.
- The two lemmas in §4.2 hold. Head-advancing dominance $\succ_\exists$ is exactly complete with respect to `ALL`, and the walk can still stop at non-advancing candidates under write-time monotonicity.

**Main problems:**

1. **"Step 2 must use `ALL`" is asserted, not shown.** `ALL` is the "collapse-X" modeling choice that `[CAP29]` encodes. That choice is the root cause of the Mixed-Cut gap. It also opens two holes the document does not see: a Layer 2 padding attack, and benign observes that un-revoke voided entries. The ideal-order generalization (`ANY`) closes all of these while keeping today's Layer 1 rule and creating no cycles. The document dismisses `ANY` with an argument that is circular.
2. **The 3-valued cut-interval rule (§4.3) is unsound under `ALL`.** Under `ALL`, whether a revocation is deferred is not monotone in the cut. So the interval endpoints $[V_{\min}, V_{\max}]$ do not bound the outcome. A counterexample is in §4.1 below.
3. **Several properties are overstated.** These include "Layer 2 is already safe", cross-advancing being "rare", `[OBSGATE02]` being preserved, and exactness.
4. **The normalization fix misses a source of non-antichain cuts.** A single `observe` payload can carry a dominated ancestor, which is a one-step gate bypass today. The fix also flips some verdicts in ways the document doesn't acknowledge.

---

## 1. How the claims were checked

I read the cited code:

- `flat_idx.ts`, `topo_idx.ts`, `level_idx.ts`
- `mvt/src/refs.ts`, `mvt/src/verdict_evaluation.ts`, `mvt/src/delta.ts`
- `rdb/src/rtable_group/group.ts`, `rdb/src/rtable_group/validate_ops.ts`, `rdb/src/rtable/view.ts`
- `std_types/src/types/rcap/view.ts`

I then ran a scratch probe against the real rdb engine. It used the mock context and the users schema (`caps` has `concurrentDeletes: true`), with both gated and ungated bindings.

To try each fix, I used a shim that wraps `RootScopedDag.prototype.findConcurrentCoverWithFilter`. `NestedScopedDag` delegates to its parent, so the shim intercepts every scoped query. It does not see `rfile_map/presence.ts`, which calls the raw `Dag`, but that call passes a single head, where every mode agrees. It has three modes:

- **base:** today's code.
- **normalize:** the document's proposed fix. It takes the minimal cover of `concurrentTo`, then runs the `ALL` search.
- **any:** ideal concurrency. An entry counts as concurrent iff it is outside $\downarrow V$ and not above *every* head.

In the table below, "later import" means an admin's observe of the revocation, appended causally after the evaluated entry in the observer DAG $O$. The scenarios starting with L1 test gated observes (Layer 1). Those starting with L2 test an item insert in $O$ whose restriction requires the revoked witness row, through an ungated binding (Layer 2).

| # | Scenario | base | normalize | any |
| :--- | :--- | :---: | :---: | :---: |
| L1-1 | Alice observes $\{v_1\}$; admin concurrently imports $r >_G v_1$ (the `[OBSGATE03]` shape) | void | void | void |
| L1-2 | Mixed cut $\{v_1, v_{adv}\}$; concurrent import of $r$ (§3.3) | **live** | **live** | void |
| L1-3 | **One payload** $\{v_1, v_2\}$ with $v_1 <_G v_2$, $v_1 <_G r$, $r \perp_G v_2$; concurrent import | **live** | void | void |
| L1-4 | Alice observes $\{f_2\}$ with $r \perp_G f_2$; later import of $r$ | void | void | void |
| L1-5 | L1-4 plus a benign concurrent observe by Bob of $\{w, f_1\}$ ($f_2 <_G w$, $f_1 <_G r$) | **live** | **live** | void |
| L1-6 | Alice observes $\{v_1\}$ with $v_1 <_G r$; later import (use-before-revoke) | live | live | live |
| L1-7 | L1-6 plus a benign concurrent observe by Bob of $\{v_2\}$ ($v_1 <_G v_2$, $r \perp_G v_2$) | live | void | void |
| L2-1 | Item at $\{f_1\}$ with $f_1 <_G d$; later import of the delete $d$ | live | live | live |
| L2-2 | Item at $\{f_2\}$ with $d \perp_G f_2$; later import | void | void | void |
| L2-3 | Item at a **padded** cut $\{f_2, f_1\}$; later import | **live** | **live** | void |
| L2-4 | Item at $\{f_2\}$ plus a benign concurrent observe of $\{w, f_1\}$; later import | **live** | **live** | void |
| L2-5 | Item at $\{f_1\}$ plus a concurrent observe of $\{f_3\}$ ($f_1 <_G f_3$, $d \perp_G f_3$); later import (the union case from §2) | **live** | void | void |

The honest admin's observe stayed live in every scenario and every mode.

These scenarios are saved, in table order, as `[OCUT01]`–`[OCUT12]` in [`test/observed_cut_tests.ts`](./test/observed_cut_tests.ts). They assert today's (base) verdicts. The tests named "gap" are the bolded rows, and their names say which alternative would flip them.

**Existing suites under each mode:**

- The rdb suite passes 301/301 under all three modes. I excluded the replica-sync groups, which hang in the sandbox in every mode including base, and ran the delta-parity group separately (it also passes under `any`).
- In the `std_types` RCap and permissioned-set tests, base passes. Under `any`, exactly one test fails: `[CAP29]`.
- So **no rdb test pins Step 2's semantics for multi-head cuts.** `[CAP29]` is the only test that encodes the `ALL`-vs-`ANY` choice.

**Follow-up measurement** (made while answering [`OBSERVED_CUT_REVOCATION_CRITIQUE_v2.md`](./OBSERVED_CUT_REVOCATION_CRITIQUE_v2.md)):

- **std_types:** I reran the whole suite (131 tests, including simple-set, nested-set, the delta groups and the three parity groups) under `any`. Again only `[CAP29]` fails.
- **rdb:** under `any`, 313 tests run (everything except the replica-sync groups); the only failures are the seven `[OCUT]` gap tests. The delta-parity sweeps pass on their own.
- **Empty positions:** that run's shim follows the empty-position definition in `DESIGN_AND_PLAN_FOR_ANY.md` §3.1. No call in either suite passes an empty `concurrentTo`.

---

## 2. Claims that hold up

- **§1.2.** All three indexes build `notConcurrentTo` as $\downarrow V \cup \uparrow V$ inside the past of `from`, so Step 2 is `ALL`.
- **§2.** The dominated-ancestor bug is real. Both `resolveRefVersionAtPosition` (which unions causal and concurrent ref-advances) and `resolveObserveGateRefAt` (whose `refAt` is `base ∪ Vz` with $\downarrow V_z \supsetneq \downarrow \text{base}$) routinely produce non-antichain cuts. Scenarios L1-7 and L2-5 show verdicts that change once the cut is normalized.
- **§3.3.** The Mixed-Cut Attack is real (L1-2). The trace through `refVersionAtOrAbove` and `killedByConcurrentDelete` is accurate.
- **§4.2.** Lemma 1 (exact completeness of $\succ_\exists$ with respect to `ALL`) and Lemma 3 (stopping the walk at non-advancing candidates is sound under `validateRefAdvanceMonotonicity`) are both correct.
- **§4.4.** In v1, observe verdicts never sit on a component with a local row op: gates read only the observed group, and bindings are creation-pinned.
- **§4.5.** Points 1 (a self-loop on `from`) and 3 (parity with `computeObserverRevisionBound`, which resolves foreign versions geometrically) are correct.

---

## 3. The central issue: `ALL` is a choice, and it is the root cause

### 3.1 Two generalizations of the singleton rule

Take a revocation $r \notin \downarrow V$. There are two natural ways to extend the single-head rule to multi-head cuts:

- **`ANY` (ideal concurrency):** $r$ is concurrent with $V$ iff $\downarrow V \not\subseteq \downarrow r$. In words, $r$'s author had not seen all of $V$.
- **`ALL` (collapse-X):** $r$ is concurrent with $V$ iff no head $v \in \hat V$ has $v <_G r$.

Both agree when $V$ is a single head. They differ exactly when $r$ is above some heads of $V$ but not others.

§1.3 argues that `ANY` "breaks use-before-revoke". In its example, $V = \{v_1, v_2\}$ and $r >_G v_1$, but $r$'s author never saw $v_2$. Under the ideal reading, a use at $V$ is **not** before $r$. Neither knew of the other, which is the textbook case for a barrier. "Use-before-revoke" holds at $V$ only if you have already accepted collapse-X. The `rcap/view.ts` header ("an external op u is concurrent with X iff it is concurrent with EVERY element of `this.at`"), [`MODERNIZATION_v2.md`](../std_types/MODERNIZATION_v2.md) ("A revoke downstream of even one element is later than the use point"), and `[CAP29]` all *encode* that choice. None of them *justifies* it. The argument in §1.3 is therefore circular.

### 3.2 `ALL` makes revocation non-monotone in the cut

Under `ANY`, the set of entries that affect a cut $V$ is $\{x : x \in \downarrow V \lor \downarrow V \not\subseteq \downarrow x\}$, either in its past or concurrent with it. That set only grows as $\downarrow V$ grows.

Under `ALL`, adding one head below $r$ moves $r$ from "concurrent" to "deferred". So **anyone who can add a head to the evaluated cut can un-revoke.** The document finds one instance of this. There are three:

- **(a) Mixed cut in Layer 1 (L1-2).** This is the document's §3.3 attack.
- **(b) Padding in Layer 2 (L2-3).** Alice's item at $\{f_2\}$ is voided when $d$ is imported later. The same item at $\{f_2, f_1\}$ survives, because $f_1 <_G d$ makes $d$ "deferred". This contradicts two statements in the document:
  - The Executive Summary and §3.1 say Layer 2 is "already safe". In fact, Layer 2's union only helps when the import is *concurrent* with the op in $O$.
  - §3.2 says `op_adv` "is strictly in the causal past of the revocation" in both DAGs. That holds for the singleton $\{v_1\}$ but is false in $G$ for a padded cut.

  With an ungated binding, any writer can pad their cut. With a gated binding, any observer who passes Layer 1 can, and under `ALL` a padded observe passes Layer 1.
- **(c) Rescue by benign widening (L1-5, L2-4).** An honest, unrelated concurrent observe whose cut has a head below $r$ flips a voided entry back to live. This already happens in today's code with whole-cut dominance, and neither proposed fix touches it.

  Head-advancing dominance $\succ_\exists$ makes (c) *more* reachable, because it admits wideners that whole-cut dominance rejects. For example, take $V_y = \{f_2, c_0\}$ and $V_z = \{w, f_1\}$ with $f_2 <_G w$. Here $z$ does not dominate $y$, but it does advance $f_2$, and the widened cut $\{c_0, w, f_1\}$ defers $d$ through $f_1$. This one is reasoned, not run.

### 3.3 The quantifier mismatch was fixed on the wrong side

[`VOID_SEMANTICS.md`](./VOID_SEMANTICS.md) §5.5 proves Layer 1 acyclic from this premise:

> a revoke `Rk(author(y))` that voids `y` — rides, under use-before-revoke (the revoke is causally *after* the published `Vy`), a version `Vz ⊋ Vy`.

That premise is true iff "after" means $\downarrow V_y \subseteq \downarrow r$, which is `ANY`'s notion. Step 2 uses `ALL`, so the set of revocations Step 2 defers is larger than the set that whole-cut dominance re-imports. The document spots this mismatch at the end of §3.3. It then resolves it by weakening Layer 1 to $\exists$, which gives up the stratification and forces in cycles and interval evaluation. Aligning Step 2 with the premise is the other way out:

> **Claim (`ANY` plus today's Layer 1 is exactly complete).**
>
> - **Setting.** Let $y$ be an observe with base $V_y$. Let $r \in \downarrow \text{refFrom} \setminus \downarrow V_y$ be a live revocation.
> - **Case 1.** If $\downarrow V_y \not\subseteq \downarrow r$, then Step 2 (`ANY`) finds $r$.
> - **Case 2.** Otherwise $\downarrow V_y \subsetneq \downarrow r$. Any observe $z$ that is concurrent in $O$ and has $r \in \downarrow V_z$ then satisfies $\downarrow V_z \supseteq \downarrow r \supsetneq \downarrow V_y$. That is exactly the existing strict-dominance widening condition.
> - **Why no cycles.** $\supsetneq$ on ideals is a strict partial order. So the invariant "observes are never on a cycle" survives, `observeVerdictLive` can keep throwing on `undecided`, and no interval evaluation is needed.
> - **Why no normalization.** `ANY` depends only on $\downarrow V$: the intersection $\bigcap_{v \in V} \uparrow v$ is unaffected by dominated elements. So the dominated-ancestor bug (§2) disappears too.

The `any` column of the probe table confirms this. Every gap scenario (L1-2, L1-3, L1-5, L2-3, L2-4, L2-5) closes. The use-before-revoke cases (L1-6, L2-1) stay live. The honest admin's observe stays live.

### 3.4 What `ANY` costs

These costs need to be weighed, not waved away:

- **`[CAP29]` flips.** Across the full rdb and std_types suites, it is the only existing test that does, apart from the `[OCUT]` gap tests that exist to flip (see the follow-up measurement in §1). `MODERNIZATION_v2.md`'s plan also inverts: `[CAP28]` stays false instead of flipping to true, and its "barrier delete after one head, add on the other, element stays" RSet case becomes "element gone".
- **More of a revoked principal's ops are voided.** Any op whose cut contains a head the revoker hadn't seen is voided. This is the singleton rule applied uniformly, but it is stricter than today.
- **Honest ops can be over-voided.** Suppose a witness row is deleted (a revocation, or the delete half of a rotation) on a branch that the op's cut only partly covers. The op is voided. Under `ALL` it would survive if the cut contains an ancestor of the delete. Single-head cuts already behave this way under both rules.
- **The DAG primitive changes.** The upward closure becomes an intersection over heads. That changes the meaning for every caller that passes a multi-element `concurrentTo`:
  - `killedByConcurrentDelete`
  - RCap B2
  - RSet
  - `findConcurrentRefAdvanceBarriers` on the observer side

  Either change the primitive or add a sibling function. The cost is comparable: $k$ upward closures in both cases.

The document should evaluate this alternative explicitly. If `ANY` is adopted, the machinery in §4.2 and §4.3 is unnecessary.

---

## 4. Problems inside the $\succ_\exists$ + interval proposal (if `ALL` is kept)

### 4.1 The interval rule is unsound for Step 2

The rule says: a barrier $d \in \downarrow\text{refFrom} \setminus \downarrow V_{\max}$ is *definitely* concurrent if $d \notin \uparrow V_{\min}$, and possibly concurrent if $d \in \uparrow V_{\min} \setminus \uparrow V_{\max}$. Under `ALL`, concurrency at a cut depends on that cut's **heads**. $V_{\max}$ can add heads that are absent from $V_{\min}$, and a barrier above such a new head is deferred at $V_{\max}$. So "definitely concurrent" is wrong.

**Counterexample.**

```text
G:  a0 → a1           b0 → r (Admin revokes Alice)       c0 → c1 → d (Carol revokes Bob)

O:  z (Bob):   Vz = {a0, r}
    y (Alice): Vy = {a1, b0, c1}          (concurrent in O; d imported by a later observe)
```

1. **They form a component.** $z \succ_\exists y$ because $b_0 <_G r$, and $y \succ_\exists z$ because $a_0 <_G a_1$. So $\{y, z\}$ is a Tarjan component.
2. **The rule's verdict for $z$.** For $z$, $V_{\min} = \{a_0, r\}$ and $V_{\max} = \{a_1, r, c_1\}$. Since $d \notin \downarrow V_{\max}$ and $d \notin \uparrow V_{\min}$, the rule declares $d$ definitely concurrent. That makes $z$ definitely void.
3. **The completion where $y$ is live.** The actual cut is $V_{\max}$, and $c_1 <_G d$, so $d$ is deferred and $z$ is live. A definite verdict that is false in one completion violates soundness as `VOID_SEMANTICS.md` §4 defines it.
4. **The true dependencies.** Here $y = \lnot z$ ($z$ carries $r$) and $z = y$ ($y$'s head $c_1$ rescues $z$). The well-founded value is therefore undefined for both, and both should collapse to void.
5. **What the engine would do.** The rule makes $z$ look decided. Once $y$ enters $L$, a re-run flips $z$ to live. `VerdictEvaluation` treats that as a rule violation: it throws either "became possibly live again (a rule is not monotone)" or "disagrees with its well-founded value". This step is traced by hand, not run.

**The flaw is structural.** Under `ALL`, "deferred with respect to $V$" is neither monotone nor anti-monotone in $\downarrow V$. With $k$ undecided candidates, an intermediate completion can have heads that appear in neither endpoint. So no test on the endpoints alone is sound, and a correct rule would have to evaluate each completion separately, which §4 of `VOID_SEMANTICS.md` forbids. Under `ANY` the interval *would* be sound, but there it isn't needed.

A smaller gap in the rule: it never handles $d \in \uparrow V_{\max} \setminus \uparrow V_{\min}$.

### 4.2 Precision and the "decided live in the Lower loop" claim

- **The support is too broad.** The support of a gap entry $e$ should be $\{w : e \in \downarrow V_w\}$, not all of $S_{\text{cut}}$. Over-broad supports grow the support graph and collapse more members to void.
- **Kleene evaluation treats gap entries as independent.** In reality each completion adds a whole ideal $\downarrow V_w$ per candidate. So correlated entries, such as a delete and a re-insert in the same $\downarrow V_w$, come out `undecided` even when every completion agrees.
- **Benign pairs can collapse.** Take two honest managers whose cuts cross-advance, where each gap holds a rotation of the other manager's witness. Both stay undecided in the Lower loop, and both collapse to void. Today such a pair never interacts.
- **Exactness only holds in the trivial case.** The `[VREAD06]` notion of "exact" is about the fully-decided case, where it does hold. But the document's Attack 2b resolution depends on *precision*, which it doesn't establish.

### 4.3 Benign cross-advance is common, not "rare"

Any two observers who have synced different branches of a multi-writer $G$ will cross-advance. Today such pairs are independent. Under the proposal every such pair becomes a component, solved by repeated gate evaluations in the fixpoint, and its outcome depends on the precision issues in §4.2. Property 2 in §4.2 only covers *fully incomparable* cuts.

### 4.4 Invariants and code the document doesn't list

- `VOID_SEMANTICS.md` §5.5 ("Observes are never on a cycle") would need revising.
- The assertion in `observeVerdictLive` would need revising. It throws on `undecided`, and `resolveObserveGateRefAt`'s see-through predicate calls it.
- That predicate is boolean. It would need a walk-style recorder like `RTableViewImpl.walk`.
- §4.4 and §4.6 lean on [`REFERENCE_FRAME.md`](../rpack/REFERENCE_FRAME.md)'s gate DAG, which is an unimplemented v2 design note. The v1 argument holds, but those sections should be labeled forward-looking.

### 4.5 §4.1 argues against a strawman

Once the interval rule exists, "drop dominance altogether" no longer forces benign pairs to void. What the $\succ_\exists$ filter actually does is limit how many components form, which is a matter of precision and performance, not correctness. The document should say so.

### 4.6 Attack 2b relies on $G$ voiding `a_adv`

If `a_adv` is live in $G$ and touches Admin's witness (for example, on a table without `concurrentDeletes`), then $y = \lnot z$ and $z = \lnot y$ form an even cycle, and both collapse to void. That is an edge case, but "Admin's $z$ is decided `live`" is conditional on it not happening.

---

## 5. The normalization fix (§2)

- **It is correct and worth doing if `ALL` stays.** L1-3 and L2-5 are fixed by it.
- **It misses a source of non-antichain cuts.** A single `observe` payload may carry a non-antichain `refVersion`. Neither `validateRefAdvanceMonotonicity` nor `validateObserveGate` checks for this. L1-3 is therefore a one-observe gate bypass today, with no fork needed, which is simpler than §3.3's attack. Add an antichain (minimal-cover) check at admission, or canonicalize `refVersion`, in addition to normalizing inside the DAG primitive.
- **It flips verdicts the document doesn't mention.** In L1-7, Alice's observe at $\{v_1\}$, with $v_1 <_G r$, is a use-before-revoke at its own base. It becomes void because Bob's benign concurrent observe widened it to $\{v_2\}$, past $r$'s fork. By §1.3's own reasoning this is a "retroactive void". By the ideal reading it is correct. The document should say which reading it intends and add the test.
- **It does not fix the main gaps.** L1-2, L1-5, L2-3 and L2-4 are unchanged.

---

## 6. Smaller points

- **§1.2: B1 is not `ANY`-like.** B1 is grant-anchored. In fact `ANY` subsumes both B1 and B2 for revokes outside $\downarrow at$: if $u \perp g$ for a grant $g \in \downarrow at$, then $g \notin \downarrow u$, so $\downarrow at \not\subseteq \downarrow u$.
- **§3.2's advice can't be followed.** It says to "publish $z(r)$ without placing unauthorized operations in $z$'s causal past". Importers always merge their frontier, so they can't do this. The practical mitigations are prompt import (`rdb_runtime`'s `ref_auto_update`) and fixing Step 2.
- **§3.3's severity is overstated.** The bypass is real and repeatable: Alice can back-date any number of observes concurrent with the import. But whatever she imports is still judged by $G$'s own verdicts, and her fork entries that needed revoked rights are void in $G$.
- **§4.5 point 2 only covers invalid entries.** A geometric `refFrom` also lets a *voided* observe extend $O$'s negative-evidence horizon with *valid* $G$ barriers that no live observe imported. The severity is low, since those barriers are legitimate $G$ state.
- **Delta parity is unaddressed.** The proposal changes which observes can revise each other. It should ship with `[OBSGATE06]`-style parity runs over cross-advancing pairs. Under the `any` shim, the existing parity group and `[OBSGATE06]` pass.
- **Test coverage was missing.** Apart from the new `[OCUT]` group, nothing in rdb pins multi-head Step 2 semantics.

---

## 7. Recommendations

1. **Write a decision record for `ALL` vs `ANY`**, using the scenario table in §1 and the costs in §3.4.
2. **If `ANY` is chosen:**
   - Change Step 2, either in the DAG primitive or as a sibling function.
   - Keep whole-cut dominance in `resolveObserveGateRefAt`.
   - Drop $\succ_\exists$ and the interval rule.
   - Update `[CAP29]` and `MODERNIZATION_v2.md`.
   - Flip the `[OCUT]` gap tests.
3. **If `ALL` is kept:**
   - Ship the normalization, and add an antichain check on `refVersion` at admission.
   - Either document Layer 2 padding (L2-3) and benign rescue (L1-5, L2-4) as accepted residuals, or design fixes for them.
   - Do not ship the interval rule as specified. It needs a sound 3-valued treatment of `ALL`'s non-monotone deferral, or a restriction to widenings that add no new heads.
4. **Either way, flip the matching `[OCUT]` gap tests in the same change.** Before that group was added, the suite accepted any of the three semantics.
