# Design and Plan: Ideal Concurrency (`ANY`) for Barrier Checks

**Status:** implemented (Phases 1-6; Phase 5 as in [`PHASE_5_PLAN.md`](./PHASE_5_PLAN.md)).
`rfile_map` presence takes RSet's rule (Phase 6). The counter-revocation ring is not done.
A ref-advance version is canonicalized on write and rejected at admission when
it is not an antichain. A missing referenced entry stays an infrastructure
throw, not a validation failure.

**How it relates to the other documents:**

- It replaces Step 2 and the Layer 1 redesign in [`OBSERVED_CUT_REVOCATION.md`](./OBSERVED_CUT_REVOCATION.md). The reasons are in [`OBSERVED_CUT_REVOCATION_CRITIQUE.md`](./OBSERVED_CUT_REVOCATION_CRITIQUE.md).
- It amends `MODERNIZATION_v2.md`, and absorbs the ref-advance section of `MODERNIZATION.md`. Both notes have since been folded into the std_types README and removed; the comparisons below are with them as they were.
- The scenarios it fixes are pinned as `[OCUT01]`–`[OCUT12]` in [`test/observed_cut_tests.ts`](./test/observed_cut_tests.ts).

Notation:

- $\downarrow P$ is the causal past of a position $P$, including $P$ itself.
- $\uparrow v$ is the causal future of an entry $v$, including $v$.
- $\hat P$ is the minimal cover (the heads) of $P$.

---

## 1. Summary

The plan changes what "concurrent with a position" means in the DAG primitive `findConcurrentCoverWithFilter`.

- **Today (collapse-X, or `ALL`):** an entry is concurrent with a multi-head position iff it is concurrent with *every* head. Equivalently, the future of a multi-head position is the **union** of its heads' futures.
- **Proposed (ideal concurrency, or `ANY`):** an entry is concurrent with $P$ iff it is outside $\downarrow P$ and not above *every* head. Equivalently, the future of a position is the **intersection** of its heads' futures.

Everything else follows from that one change.

- **Layer 1 stays as it is.** The strict whole-cut dominance in `resolveObserveGateRefAt` becomes *exactly complete*. Observes never form cycles, and none of the head-advancing-dominance or interval machinery is needed.
- **The rule depends only on the causal past.** The answer is the same for any set with the same past. So the dominated-ancestor bug disappears without normalization.
- **Antichain hygiene is added anyway.** A ref-advance's published version must be an antichain at admission, and computed foreign versions are canonicalized. These catch the bad shapes at their source and keep memo and cache keys canonical.
- **The std_types modernization proceeds with the ideal rule.** Its structural simplifications all stand. A few expected test outcomes reverse.

Measured on today's code with a shim that swaps the primitive on `RootScopedDag` (details in the critique, §1). The shim implements the rule exactly as §3.1 defines it, including the empty position.

- **Coverage:** the shim sees every scoped query. It does not see `rfile_map/presence.ts`, which calls the raw `Dag`, but that call passes a single head, where both rules agree.
- **Scenarios:** every scenario the critique flags is closed. Use-before-revoke cases stay live, and honest observes stay live.
- **rdb:** of 313 tests (everything except the replica-sync groups, which hang in the sandbox under base too), the only failures are the seven `[OCUT]` gap tests, which flip as intended. Both delta-parity sweeps pass when run on their own.
- **std_types:** all 131 tests run. That covers simple-set, nested-set, subscribe, authorship, RCap, permissioned-set, both delta groups and the three parity groups. Only `[CAP29]` fails under the shim. Phase 3 also flips `[CAP08]`: a `delete-cap` concurrent with `at` and after the origin makes the capability absent. That flip is the definition read moving to the use point, which the shim did not cover.
- **Empty positions:** no call in either suite passes an empty `concurrentTo`, under base or under the ideal rule.

---

## 2. The problem

### 2.1 Where "concurrent with a position" decides outcomes

A barrier op (a revoke, a row delete, a barrier set-delete, a deploy, a ref-advance) affects a read at $(at, from)$ in two ways.

1. **Step 1:** it sits in the see-through causal cover at $at$.
2. **Step 2:** it is visible from $from$ and *concurrent with $at$*.

Step 2 is computed everywhere by `findConcurrentCoverWithFilter(from, at, …)`. The callers are listed below. "Multi-head" means the call can see an `at` with more than one head.

| Caller | Barrier | Multi-head `at`? |
| :--- | :--- | :--- |
| `rdb/src/rtable/view.ts` `killedByConcurrentDelete` | row delete (negative) | yes, the foreign cut of cross-group reads and observe gates |
| `std_types/src/types/rcap/view.ts` B2 (`useRevokes`) | revoke (negative) | yes, `this.at` |
| `rcap/view.ts` B1 and the cap-origin functions | revoke, `delete-cap` | no, they pass `version(hash)` |
| `std_types/src/types/rset/view.ts` barrier deletes | set delete (negative) | no, it passes `version(add)` |
| `rset/view.ts` barrier adds | set add (positive) | yes, `this.at` |
| `rdb/src/rfile_map/presence.ts` | file remove (negative) | no, it passes `version(add)` |
| `rdb/src/rtable_group/group.ts` params resolution | schema param (positive) | yes, observer positions |
| `mvt/src/refs.ts` `findConcurrentRefAdvanceBarriers` | ref-advance and deploy widening (positive) | rarely: op checks pass `version(entry)`; only views at a multi-head `at` with `from ≠ at` |

### 2.2 What collapse-X gets wrong

On a single head the two rules agree. On a multi-head cut, collapse-X says a barrier $r$ is *later than* the cut as soon as $r$ is above **one** head. That causes four problems.

1. **The outcome is non-monotone in the cut.** Adding a head below $r$ moves $r$ from "concurrent" to "deferred". Knowing *more* of the history can therefore un-revoke an op.

2. **The party being judged controls the outcome.** The op's author chooses the cut, either through her own observes or through the published version of a gated observe. Any leniency collapse-X grants honest users is available to an attacker on the same terms. All of the following hold in today's code:

   | Scenario | Test | Today |
   | :--- | :--- | :--- |
   | Mixed-cut gated observe $\{v_1, v_{adv}\}$ survives a concurrent revoke-import (the Mixed-Cut Attack) | `[OCUT02]` | live |
   | Padding an item's cut $\{f_2\} \to \{f_2, f_1\}$ dodges a later-imported delete | `[OCUT10]` | live |
   | An unrelated benign observe with a head below $r$ rescues a voided observe | `[OCUT05]` | live |
   | The same rescue, for an item | `[OCUT11]` | live |

3. **The primitive is not a function of the causal past.** Its upward closure runs from every element of `concurrentTo`, dominated ancestors included. So a set like $\{v_1, v_2\}$ with $v_1 <_G v_2$ hides barriers that the equivalent $\{v_2\}$ would expose. This bites in three ways:

   | Source of the non-antichain set | Test |
   | :--- | :--- |
   | A single observe payload carrying a dominated ancestor | `[OCUT03]` |
   | The `base ∪ Vz` widening in Layer 1 | `[OCUT07]` |
   | The causal-plus-concurrent union in Layer 2 | `[OCUT12]` |

4. **It contradicts the Layer 1 acyclicity proof.** [`VOID_SEMANTICS.md`](./VOID_SEMANTICS.md) §5.5 proves observes never form cycles from this premise: a revoke that voids an observe $y$ "rides, under use-before-revoke (the revoke is causally *after* the published `Vy`), a version `Vz ⊋ Vy`". That premise holds only if "after" means $\downarrow V_y \subseteq \downarrow r$, which is the ideal notion. Step 2 uses collapse-X, so Step 2 defers more revokes than whole-cut dominance re-imports. That gap is exactly the Mixed-Cut Attack.

### 2.3 Why the earlier proposal is not the fix

`OBSERVED_CUT_REVOCATION.md` keeps collapse-X and weakens Layer 1 to head-advancing dominance ($\succ_\exists$). The critique gives the full argument; in short:

- $\succ_\exists$ creates cycles between observes, including benign pairs that have synced different branches.
- Those cycles need a 3-valued cut-interval rule. That rule is unsound under collapse-X, because deferral is not monotone in the cut, so the interval endpoints do not bound it.
- Even if all of that worked, `[OCUT05]`, `[OCUT10]` and `[OCUT11]` stay open.

---

## 3. Design

### 3.1 The definition

For a position $P$, read as the order ideal $\downarrow P$, and an entry $x$:

| Relation | Condition |
| :--- | :--- |
| $x$ is **in the past** of $P$ | $x \in \downarrow P$ |
| $x$ is **after** $P$ | $x \notin \downarrow P$ and $\downarrow P \subseteq \downarrow x$ (above every head) |
| $x$ is **concurrent with** $P$ | $x \notin \downarrow P$ and $\downarrow P \not\subseteq \downarrow x$ |

"After" is $\bigcap_{v \in \hat P} \uparrow v \setminus \downarrow P$: the future of a position is the intersection of its heads' futures.

**The empty position.** For $P = \emptyset$, $\downarrow P = \emptyset \subseteq \downarrow x$ for every $x$. So every entry is after the empty position, and **nothing is concurrent with it**. The intersection agrees, since an intersection over no heads is the whole DAG.

Today's implementation does the opposite. It starts the exclusion set from `concurrentTo` and adds nothing to it, so for an empty `concurrentTo` every matching entry counts as concurrent. The new primitive follows the definition, for two reasons:

- **Monotonicity.** Property 3 below only holds at the bottom under the definition. Under today's behavior, every barrier reaches the empty position, but fewer reach a one-head position.
- **It costs nothing in practice.** No call in today's rdb or std_types suites passes an empty `concurrentTo` (§1).

The empty case has to be written explicitly. An implementation that folds the heads' futures into an accumulator that starts empty computes the vacuous intersection as empty instead of as everything, which silently keeps today's behavior. Phase 1 adds the case to the hand-built `dag` tests.

The rule has three properties:

1. **Single heads are unchanged.** Every caller that passes `version(hash)` behaves exactly as today.
2. **It depends only on the causal past.** The intersection $\bigcap_{v \in P} \uparrow v$ ignores dominated elements, because a dominated element's future contains its dominator's future.
3. **It is monotone.** The set of entries that *reach* $P$, meaning those in its past or concurrent with it, is $\{x : x \in \downarrow P \lor \downarrow P \not\subseteq \downarrow x\}$, and it only grows as $\downarrow P$ grows. Widening can add barriers but never remove them, so no "rescue" is possible. Legitimate re-grants in $\downarrow P$ still work through Step 1, as today.

The invariant this buys:

> **A revoked principal's surviving ops can depend only on state the revoker had already seen.**

The only way to dodge a revoke is a cut entirely below it, which is the irreducible use-before-revoke case (`[OCUT06]`, `[OCUT08]`).

### 3.2 The primitive

`findConcurrentCoverWithFilter(from, concurrentTo, meta, predicate)` keeps its signature. Only the exclusion set changes:

```text
today:     notConcurrentTo = ↓concurrentTo ∪ ⋃_{v ∈ concurrentTo} ↑v      (within ↓from)
proposed:  notConcurrentTo = ↓concurrentTo ∪ ⋂_{v ∈ concurrentTo} ↑v      (within ↓from)
```

The forward map over $\downarrow from$ and the downward closure stay as they are. Each head gets its own upward closure in the forward map, and the closures are intersected. The see-through pre-cover walk from `from` and the final minimal cover are unchanged.

- **Cost:** $k$ upward closures over $\downarrow from$, comparable to today's single union closure, and identical when $k = 1$.
- **Empty position ($k = 0$):** return an empty cover without walking (§3.1).
- **Optional optimization:** a single topological pass carrying a $k$-bit set per node. The "after" nodes are exactly those whose bit set is full.

Implement it in all three indexes (`dag/src/idx/flat/flat_idx.ts`, `topo/topo_idx.ts`, `level/level_idx.ts`), and state the definition on the `DagIndex` and `Dag` interface (`dag/src/dag.ts`).

**The primitive changes in place; no sibling is added.** Every caller in the §2.1 table means "concurrent with this position", positive barriers included:

- A barrier set-add, schema param or deploy that is concurrent with a multi-head `at` under the ideal reading should apply at the merge, for the same reason a concurrent delete should kill.
- A sibling function would leave two meanings of "concurrent" side by side, and the next caller would pick the wrong one.

**The rdb read rules need no code change.** `killedByConcurrentDelete`, `rowState` and the observe gate inherit the new meaning from the primitive. Only comments that describe the old meaning change.

### 3.3 Layer 1 and Layer 2 stay as they are

`resolveObserveGateRefAt` keeps its rule: a concurrent observe $z$ widens $y$ iff $z$ is live and $\downarrow V_z \supsetneq \downarrow V_y$. Under the ideal rule this is exactly complete.

> **Claim.** Let $y$ be an observe whose published version is $V_y$. Let $r$ be a live barrier in $\downarrow refFrom \setminus \downarrow V_y$.
>
> 1. If $\downarrow V_y \not\subseteq \downarrow r$, then $r$ is concurrent with $V_y$, and Step 2 finds it.
> 2. Otherwise $\downarrow V_y \subsetneq \downarrow r$. Take any observe $z$ that is concurrent with $y$ in the observer DAG and has $r \in \downarrow V_z$. Then $\downarrow V_z \supseteq \downarrow r \supsetneq \downarrow V_y$. So $z$ strictly dominates $y$, which is exactly the existing widening condition.
>
> Strict $\supsetneq$ on ideals is a strict partial order. So the widening relation has no cycles.

Consequences:

- **The "observes are never on a cycle" invariant stays,** along with the throw in `observeVerdictLive` (`group.ts`, lines 635–641).
- **The `VOID_SEMANTICS.md` §5.5 premise becomes literally true.** Add one sentence saying so.
- **Neither part of the earlier proposal is adopted:** not $\succ_\exists$, not the interval rule.
- **Layer 2 is unchanged.** Its union of live concurrent observes, and the geometric `refFrom`, stay.

### 3.4 Canonical versions: antichain checks and computations

Under the ideal rule, barrier checks no longer *need* canonical versions. Two gaps remain, and closing both is cheap.

**(a) Admission.** A ref-advance's `refVersion` is accepted today with no shape check. One payload can therefore carry a dominated ancestor, which is how `[OCUT03]` works. Every admission site calls `validateRefAdvanceMonotonicity`:

| Site | Monotonicity call |
| :--- | :--- |
| Foreign-group observe | `rdb/src/rtable_group/validate_ops.ts`, line 574 |
| Schema deploy | same file, line 684, inside `validateDeploy` (which starts at line 633) |
| RFiles bound-group ref-advance | `rdb/src/rfiles/access.ts`, line 232 (the entry-presence loop is lines 228–230) |
| RSet ref-advance | `std_types/src/types/rset/validate.ts`, line 153 |

Add one check in `mvt/src/refs.ts` and call it from all four sites. The cleanest form is a `validateRefAdvanceVersion` that runs shape and monotonicity together. The shape check requires `refVersion` to be:

1. Non-empty.
2. An antichain, i.e. `findMinimalCover(refVersion)` equals `refVersion`.

A missing referenced entry is an infrastructure throw at the call site (the
sync layer defers the op), not a validation failure. `rfiles/access.ts` already
throws; the other sites throw when the foreign object or an entry is absent.

For this, widen the `CausalDag` type (`mvt/src/dag/dag_nesting.ts`, lines 42–44) with `findMinimalCover(p: Position): Promise<Position>`. Every `getCausalDag()` implementation already returns the raw `Dag`, which has that method, so this is a type change only.

Why reject rather than silently canonicalize? One version should have one encoding. That keeps signatures, entry hashes and deduplication stable, and it removes the attack shape at its source, even if the primitive ever regresses.

**(b) Computation.** Several computed versions are routinely non-antichain:

- Layer 2's union in `resolveRefVersionAtPosition`.
- Layer 1's `refAt = base ∪ Vz`.
- Geometric `refFrom` folds.
- Schema versions built as `pinned ∪ refVersion`. The one every schema read goes through is `resolveSchemaVersion` (`group.ts`, lines 405–411). Two local copies of the same union sit in `validateDeploy` (`validate_ops.ts`, around line 688) and `fkAdoptionKills` (`group.ts`, around line 875).

Under the ideal rule these are *correct*, but not canonical. Two things depend on canonical versions:

- **Verdict frames.** `resolveVerdict` keys on the sorted `from` (`group.ts`, line 1071), and so does RCap's `opVerdict`. On a foreign table view that `from` is `foreignFrom`. A non-canonical encoding therefore splits one logical verdict across several frames of the same evaluation. Under the ideal rule the frames agree, so this is duplicated work, not a wrong answer.
- **Returned versions.** Callers that compare or display versions see the dominated ancestor. The schema *state* cache is already canonical, because `RSchemaImpl.resolveAt` minimal-covers before keying (`rschema.ts`, line 214). But `getView` stores the raw `at` on the view (line 207), so `getVersion()` returns it. Several callers of `resolveSchemaVersion` minimal-cover the result themselves (`conformance.ts`, line 135; the deploy-params check in `validate_ops.ts`, lines 707–711), and others compare or store it raw:
  - `catalog_update.ts`, line 84;
  - `delta.ts`, lines 136–137;
  - the group view, in `group.ts` line 1273.

Add `canonicalVersion(dag, v)` to `mvt/src/refs.ts`: it returns `v` unchanged when `v.size <= 1`, and `findMinimalCover(v)` otherwise. Call it where a version is built from a union, since those callers hold the referenced DAG:

- `resolveForeignTableViewIn`, for `foreignAt` and `foreignFrom`.
- `diagnoseObserve`, for `refAt` and `refFrom`.
- The return value of `resolveObserveGateRefAt`.
- RSet's `resolveRefVersions` consumers.
- The return value of `resolveSchemaVersion`, against the schema DAG. Then route the two local unions in `validateDeploy` and `fkAdoptionKills` through the same helper. The callers' own minimal covers become redundant and can go.

`resolveRefVersionAtPosition` keeps its signature; it does not know the referenced DAG.

**(c) Invariant and test.** State the invariant on the `Dag` interface: *every primitive that takes a `Position` is a function of its causal past.* Then add a randomized `dag` test. It compares `findCoverWithFilter`, `findConcurrentCoverWithFilter` and `findForkPosition` on a random set $S$ against the same call on `findMinimalCover(S)`, across all three indexes. Today's `findConcurrentCoverWithFilter` fails this test, which is the class of bug behind `[OCUT03]`, `[OCUT07]` and `[OCUT12]`.

### 3.5 The std_types modernization under the ideal rule

`MODERNIZATION_v2.md` stands structurally. Adopt all of it with **Kill 2 read under the ideal rule**:

- The two checks (the see-through cover at `at`, and a live negative op concurrent with `at` visible from `from`).
- Deleting the per-grant search (B1).
- Retargeting `capabilityExists`, `getFirstSurvivingCapOrigin`, `currentCapCreationVersion` and `hasAnySurvivingOriginIn` to the two checks at `(at, from)`.
- The `getManagedBy` rewrite.
- The two-kill rewrite of RSet's `hasByHash`.
- The Kleene rule for undecided ops, and the docs steps.

One definitional paragraph changes. `MODERNIZATION_v2.md` says the heads of a multi-element `at` form one position whose "future is the union of their futures", and that "a revoke downstream of even one element is later than the use point". Both become: *the future of a multi-head position is the intersection of its heads' futures; a negative op is later than the use point only if it is downstream of every head.*

**Dropping B1 loses nothing under the ideal rule.** If a revoke $u \notin \downarrow at$ is concurrent with some grant $g \in \downarrow at$, then $g \notin \downarrow u$. So $\downarrow at \not\subseteq \downarrow u$, and Kill 2 already fires. B1 was patching collapse-X's blind spot.

Expected outcomes, compared with `MODERNIZATION_v2.md`:

| Case | `MODERNIZATION_v2.md` | Under the ideal rule | Why |
| :--- | :--- | :--- | :--- |
| `[CAP28]` (revoke after one head, grant on the other) | flips to true | **stays false** | the revoke did not see the grant head |
| `[CAP29]` (revoke after one head, grant below both) | stays true | **flips to false** | the revoke did not see the other head |
| `[CAP08]` (a `delete-cap` concurrent with `at` and after the origin) | not listed | **capability absent** | a concurrent negative op wins |
| `[CAP24]`, `[CAP26]`, `[CAP27]`, `[CAP30]` | stay | stay | |
| `[CAP32]`–`[CAP34]` (revoke rings) | stay | stay | |
| RSet twin of `[CAP28]` (barrier delete after one head, add on the other) | element stays | **element gone** | same as `[CAP28]` |
| Barrier delete concurrent with `at`, causally after the add | gone | gone | |
| `delete-cap` after one head, create on the other | create survives | **capability absent** | a concurrent negative op wins |
| Two concurrent creates, no delete | both are origins; `getManagedBy` uses the greater hash | same | |
| `[SET02b]`, `[SET04]`, `[PSET19]`, `[PSET25]` | stay | stay | |
| Barrier add after one head of `at` (`supportBarrierAdd`) | not specified | **joins the set** | positive barrier, same rule |

The ref-advance work that `MODERNIZATION_v2.md` defers ("Not this change"), and that `MODERNIZATION.md` describes, is part of this plan (Phase 5). The ideal rule makes it simpler:

- The G-upward walk lifted from `resolveObserveGateRefAt` into `mvt/src/refs.ts` is correct as written. It is whole-cut dominance, with no $\exists$ variant.
- `MODERNIZATION.md`'s bullet "a revoke merely concurrent with the published version … RCap's own use-anchored kill still applies" becomes exactly true.

`rdb/src/rfile_map/presence.ts` uses the add-anchored barrier-remove shape that the modernization removes from RSet. Aligning it is a follow-on (Phase 6).

### 3.6 A scoped concurrent cover is decided on the root DAG

`NestedScopedDag` is a lens. Positions are root positions, walks follow the root order, and the scope filter only selects which entries may be returned. `findConcurrentCoverWithFilter` delegates the caller's position unchanged and joins the scope filter into the caller's filter. It does not drop or replace out-of-scope heads.

That flips two assertions in `[NES04]`. Nested sets A and B each append at their own scoped frontier, so their inner ops are mutually concurrent even when one set's script runs entirely before the other's. Reading B at `{A's delete, B's shared add}` asks about an op that has seen A's delete. B's delete has not seen that op, so it is concurrent with the cut and `shared` is gone from B. The symmetric read of A is the same.

Sibling isolation is a different question. A nested object's default `getView` sets `at` and `from` to its own frontier, so sibling history only enters when a caller passes a mixed cut. A product that needs "B's state at the cut B has seen" needs a separate query. That query must not replace the concurrent cover the at-use checks rely on.

Two narrower rules sit on top of this:

- A capability is one slot. A `delete-cap` turns it off, and a re-create turns the same slot back on. Grants and revokes of a (capability, grantee) pair stay about that slot. The manager list for a grant or revoke is the definition in that op's own past, read at `(op, op)`.
- On a set with `supportBarrierDelete`, every delete is a barrier. The payload bit is not a choice the writer makes.

---

## 4. Why this is the best option

| | Collapse-X as is | Collapse-X plus the earlier proposal | Ideal rule (this design) |
| :--- | :--- | :--- | :--- |
| Mixed-Cut Attack (`[OCUT02]`) | open | closed | closed |
| Dominated ancestor (`[OCUT03]`, `[OCUT07]`, `[OCUT12]`) | open | closed (normalization) | closed (rule plus admission check) |
| Padding (`[OCUT10]`) | open | open | closed |
| Rescue by benign widening (`[OCUT05]`, `[OCUT11]`) | open | open, and more reachable | closed |
| Observe cycles | none | yes, for every cross-advancing pair | none |
| 3-valued interval rule | no | yes, and unsound as specified | no |
| Code changed | none | DAG primitive, both layers, verdict engine | DAG primitive (rdb read rules unchanged) |
| Existing tests that flip | none | not measured | `[CAP08]` and `[CAP29]` in std_types, plus the seven `[OCUT]` gap tests in rdb (both suites measured in full, §1). `[CAP08]` is the Phase 3 definition read, not the shim. |

Five reasons, roughly in order of weight:

1. **Knowing more never helps the party being judged.** Under the ideal rule, adding heads to a cut can only add barriers, so an op's author can only make her op more exposed. Under collapse-X she can make it safer. In a Byzantine setting, a rule whose leniency the judged party controls is not a security rule.

   She can still choose a cut. Omitting heads, down to a cut entirely below the revoke, dodges it. That is the use-before-revoke residual of §3.1 (`[OCUT06]`, `[OCUT08]`), which both rules keep live. Under the ideal rule, an op that dodges this way can use only state the revoker had already seen.
2. **It is the generalization the rest of the design already assumes.** It matches the single-head rule, it matches what `concurrentDeletes` means ("a concurrent delete wins"), and it matches the `VOID_SEMANTICS.md` §5.5 premise. Layer 1's existing proof becomes literally true instead of being patched.
3. **It removes complexity instead of adding it.** No observe cycles, no interval rule, no normalization for correctness. The fix is the definition of one set in one primitive.
4. **It is monotone and depends only on the causal past.** Those are the two properties the verdict engine and the delta floor implicitly rely on. Monotonicity means widening can never rescue, and the causal-past property means no hidden dependence on how a set happens to be encoded.
5. **The measured blast radius is small.** Across the full rdb and std_types suites, the existing tests that flip are `[CAP08]` and `[CAP29]`, apart from the `[OCUT]` gap tests that exist to flip. `[CAP08]` flips because the definition is read at the use point: a `delete-cap` concurrent with `at` and after the origin makes the capability absent. Delta parity stays green, and the use-before-revoke cases are unchanged. Phase 1's exit criteria re-check this on the real implementation, with the extended parity profile and the replica permissioned-sync tests, which the shim run did not cover.

### 4.1 Costs, and how to handle them

- **A revoked principal loses more ops.** Any op whose cut includes a head the revoker had not seen is voided. That is the purpose of a barrier, now applied consistently.
- **Honest ops can lose liveness.** Suppose a witness row, FK target or cap is deleted on a branch that an honest op's cut only partly covers. The op is voided, where collapse-X would let it survive if the cut contained an ancestor of the delete. Three points keep this acceptable:
  - Single-head cuts already behave this way under both rules. Collapse-X only spared cuts that happened to include such an ancestor, which is an accident of sync timing, not a policy.
  - Where a table should not let concurrent deletes win, the existing per-table opt-out is `concurrentDeletes: false`. It is keyed to the schema, which the group controls, not to the op's author.
  - Operational practice shrinks the window: revoke from a merged frontier, and keep observations current (`rdb_runtime`'s `ref_auto_update`).
- **Positive barriers apply more often on multi-head `at`.** This affects barrier set-adds, schema params and deploy widening at an unmerged observer frontier. This is consistent but is a behavior change; Phase 1 adds one test per positive caller.

### 4.2 When to reconsider

Reconsider only for a hard product requirement that ops at multi-head cuts survive revocations issued by stale replicas, in tables that cannot use `concurrentDeletes: false`. Even then, collapse-X needs observe cycles plus a sound 3-valued treatment of its non-monotone deferral, which does not exist yet. It would still leave `[OCUT05]`, `[OCUT10]` and `[OCUT11]` open.

---

## 5. Plan

Every phase lists its exit criteria. Phase 1 changes replicated semantics, so it ships as one change.

### Phase 0: Decision and documents

- Accept this document.
- Mark the following sections of `OBSERVED_CUT_REVOCATION.md` superseded, with a pointer to this document and the critique:
  - §1.3 and the first Executive Summary item ("Step 2 must use `ALL`").
  - §4.2 and §4.3 (head-advancing dominance and the interval rule).
  - §5 items 1 and 3.
- Keep §2 (the dominated-ancestor analysis) and §3.3 (the attack) as the problem record.

### Phase 1: The DAG primitive (atomic with its test flips)

1. Implement the ideal exclusion set (§3.2) in `flat_idx.ts`, `topo_idx.ts` and `level_idx.ts`, with the empty position handled explicitly. Document both on `DagIndex` and `Dag`.
2. **`dag` tests:**
   - Hand-built cases: a barrier above one head of two is concurrent; above both heads is after; a dominated element changes nothing; nothing is concurrent with the empty position (today, everything is).
   - Agreement across the three indexes (the existing `[META_*]` harness).
   - The causal-past randomized test from §3.4(c), for this primitive.
3. **rdb:** flip the `[OCUT]` gap tests to void (`[OCUT02]`, `[OCUT05]`, `[OCUT07]`, `[OCUT10]`, `[OCUT11]`, `[OCUT12]`). `[OCUT03]` also becomes void here, and is reworked in Phase 2. Drop the "gap" labels and the header line about pinned gaps.
4. **std_types:** flip `[CAP29]` to false, and rename it to say the revoke did not see the other head.
5. **Add one test per positive multi-head caller:**
   - an RSet barrier add after one head joins the set;
   - a schema param barrier after one head of an observer `at` applies;
   - a deploy concurrent with one head of a view's `at` (with `from ≠ at`) revises the schema there.
6. **Update the text that states collapse-X:**
   - the `holdingIn` header in `rcap/view.ts` (the "collapse-X model" paragraph and the B2 comment);
   - the B2 wording in [`CAPABILITIES.md`](./CAPABILITIES.md) ("concurrent with the whole use point", around lines 69–72 and 81). The rest of that file waits for Phase 3.

   The rdb comments (`rtable/view.ts`, `refs.ts`) say "concurrent to `at`" without defining it, so they stay accurate once the primitive documents the definition.

**Exit:**

- The `dag` small suite passes, and the medium and large suites pass.
- The rdb suite and the rdb delta parity (default and extended profiles) pass.
- The std_types suite and its delta parity pass.
- `replica/test/replica_permissioned_sync_tests.ts` passes.

### Phase 2: Canonical versions

1. Widen `CausalDag` with `findMinimalCover`.
2. Add `validateRefAdvanceVersion` (shape plus monotonicity) and `canonicalVersion` to `mvt/src/refs.ts`, with `mvt` unit tests.
3. Switch the four admission sites in §3.4(a) to `validateRefAdvanceVersion`. Each rejection gets its own failure message, e.g. "ref-advance version is not an antichain".
4. Canonicalize the computed versions in §3.4(b).
5. Add the causal-past randomized test from §3.4(c) for `findCoverWithFilter` and `findForkPosition`.
6. **Tests:**
   - Admission rejects a non-antichain `refVersion`: for a gated observe, an ungated observe, a schema deploy, an RFiles ref-advance and an RSet ref-advance.
   - `[OCUT03]` becomes "a single observe carrying a dominated ancestor is rejected at admission".
   - One memo test: two reads whose foreign `from` differ only by a dominated ancestor share a verdict frame.
   - After a deploy, `resolveSchemaVersion` and the group view's schema `getVersion()` return the deployed heads without the pinned genesis version below them.

**Exit:** all suites in Phase 1's exit, plus `mvt`.

### Phase 3: RCap holdings (`MODERNIZATION_v2.md` steps 1–2, under the ideal rule)

1. Rewrite `holdingIn` as the two checks, and delete the per-grant search. Use `RTableViewImpl.walk`'s dominated filter, with the Kleene combination and the re-grant adaptation described in `MODERNIZATION_v2.md`. Keep the creator short-circuit, `exclude`, and `opVerdict` keyed by op and `from` (now canonical after Phase 2).
2. Retarget the definition reads to the two checks at `(at, from)`: `capabilityExists`, `getFirstSurvivingCapOrigin`, `currentCapCreationVersion` and `hasAnySurvivingOriginIn`. Also rewrite `getManagedBy`: greatest live create hash among the cover maxima, with a live `delete-cap` meaning the capability is absent.
3. **Tests**, per the §3.5 table:
   - `[CAP28]` stays false. Remove its "grant-anchored B1" wording and say Kill 2 fires.
   - A re-grant causally after a revoke holds.
   - An undecided grant above a live revoke leaves the result undecided; a live revoke beside a concurrent undecided grant decides it false.
   - A `delete-cap` after one head makes the capability absent, and `capabilityExists` and `currentCapCreationVersion` agree.
   - With two concurrent creates, `getManagedBy` agrees across replicas.
4. **Docs:**
   - the admissibility paragraph in `std_types/README.md`;
   - the `holdingIn` comment;
   - [`CAPABILITIES.md`](./CAPABILITIES.md): the paragraph above the B1/B2 table, and the table itself, which now describes one use-anchored check under the ideal rule.

**Exit:** `rcap_tests`, `delta_cap_tests`, the RCap delta parity, and the Phase 1 rdb suites (rdb reads RCap only through its own caps tables, so this is a guard).

### Phase 4: RSet membership (`MODERNIZATION_v2.md` step 3, under the ideal rule)

1. Rewrite `hasByHash`: see-through cover at `at` (no live barrier delete in it), plus Kill 2 against `at` from `from`, with the existing authorization predicate on both walks. The `barrier` bit stays the honor flag. `supportBarrierAdd` stays a separate mode, and the two remain mutually exclusive.
2. **Tests:**
   - The RSet twin of `[CAP28]`: element gone.
   - A barrier delete concurrent with `at` and causally after the add: gone.
   - `[SET02b]`, `[SET04]` and `[PSET19]` stay.
   - The positive barrier-add case from Phase 1, if it isn't already covered.

**Exit:** `simple_set_tests`, `nested_set_tests`, `permissioned_set_tests`, `delta_set_tests`, the RSet and branchy delta parity, and the replica permissioned sync tests.

### Phase 5: RSet ref-advances as gated observes (`MODERNIZATION.md`)

1. **Lift the walk.** Move the G-upward walk out of `RTableGroupImpl.resolveObserveGateRefAt` into `mvt/src/refs.ts`. Its inputs are the published version, the foreign causal DAG, a liveness callback, and canonical output. Point rdb at it with no behavior change.
2. **RSet admission.** Check `refAdvanceCaps` at `(newRefVersion, newRefVersion)`. Creators stay fiat when `refAdvanceCreators` is set, and the signature is still verified at the set position.
3. **RSet at use.** Give each ref-advance a verdict: its RCap holding at the widened cut, from a geometric `refFrom`. An undecided ref-advance throws, like an undecided observe.
4. **See-through on read.** Element authorization and `resolveRefVersion` on the enforcement path fold `refAt` with `isLive`. `refFrom` and the delta floor stay geometric.
5. **Tests:**
   - Admission fails when the author lacks the cap at the imported version and holds it only at the parent cut.
   - A back-dated ref-advance is void once a strictly later live advance is co-observed, and reads see through it.
   - Two ref-advances publishing incomparable RCap versions both stay live.
   - `[PSET25]` stays.
   - The RSet analogues of `[OCUT01]`, `[OCUT02]` and `[OCUT04]`.

**Exit:** the std_types suites, the replica permissioned sync tests, and the rdb suite (the lifted walk).

### Phase 6: Follow-ons

- Align `rfile_map/presence.ts` with the two-kill rule (cover at `at`, plus Kill 2 under the ideal rule) and remove its add-anchored search, with tests modeled on the RSet ones.
- Update [`VOID_SEMANTICS.md`](./VOID_SEMANTICS.md) §5.5 with one sentence: Step 2 uses the ideal rule, which is what makes "after `Vy`" coincide with "rides a strictly dominating version".
- Update the rdb README if it describes Step 2.

### Cross-cutting concerns

- **Replicated semantics.** Phase 1 changes verdicts, and Phase 2 changes validation. Replicas on different versions would compute different views, or reject each other's ops. Ship each phase to all replicas together. The project is pre-1.0, so no migration is planned.
- **Existing data.** Normal observes publish frontiers, which are antichains. Before Phase 2 lands, scan any persisted stores you care about (for example `apps/editor/hosts/*`) for non-antichain ref-advance payloads; once the rule is in, those entries would be rejected on re-sync.
- **Performance.** The primitive costs $k$ upward closures instead of one union closure. Canonicalization costs one `findMinimalCover` per constructed foreign view, and can be memoized per evaluation. Run the rdb parity extended profile before and after Phase 1 to compare.

---

## 6. Open questions

1. **Positive barriers.** This design applies the ideal rule uniformly. Is there any positive barrier whose intended meaning is "applies only if after none of the heads"? Nothing in the current tests or documents says so.
2. **Where to canonicalize.** At the callers (proposed), or inside `resolveRefVersionAtPosition` with an extra `referencedDag` parameter? The callers already have the foreign DAG, and the shared function stays generic.
3. **Reject or canonicalize at admission?** This design rejects. Canonicalizing on write would also work for locally authored ops. Incoming payloads would still need the check, because the signed bytes cannot be rewritten.
