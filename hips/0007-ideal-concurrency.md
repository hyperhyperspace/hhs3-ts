# HIP-7: Ideal concurrency (`ANY`) for barrier checks

- **Type:** design
- **Status:** implemented
- **Modules:** dag, mvt, std_types, rdb
- **Requires:** —
- **Superseded by:** —
- **Implemented in:** `d8bbd49`, `cfce04f`, `6caf1b1`

## Summary

Changes what "concurrent with a position" means in the DAG primitive `findConcurrentCoverWithFilter`: an entry is concurrent with a multi-head position when it is outside the position's past and not above every head, so the future of a position is the intersection of its heads' futures rather than their union. With that one change the whole-cut dominance of the observe-gate widening becomes exactly complete, observes stay off cycles, the dominated-ancestor bug disappears, and use-before-revoke holds on every cut. Ref-advance versions are canonicalized on write and must be antichains at admission, and the std_types modernization proceeds under the ideal rule. The follow-up appended below fixes what the review of Phases 1–4 found: a scoped concurrent cover is decided on the root DAG, a capability and each grant of it is one slot, and barrier deletes are a property of the set. Phase 5 is [HIP-8](0008-rset-gated-ref-advances.md).

**How it relates to the other documents:**

- It replaces Step 2 and the Layer 1 redesign in [`OBSERVED_CUT_REVOCATION.md`](0006-observed-cut-revocation.md). The reasons are in [`OBSERVED_CUT_REVOCATION_CRITIQUE.md`](0006-observed-cut-revocation.md#review).
- It amends `MODERNIZATION_v2.md`, and absorbs the ref-advance section of `MODERNIZATION.md`. Both notes have since been folded into the std_types README and removed; the comparisons below are with them as they were.
- The scenarios it fixes are pinned as `[OCUT01]`–`[OCUT12]` in [`test/observed_cut_tests.ts`](../modules/rdb/test/observed_cut_tests.ts).

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

4. **It contradicts the Layer 1 acyclicity proof.** [`VOID_SEMANTICS.md`](../modules/rdb/VOID_SEMANTICS.md) §5.5 proves observes never form cycles from this premise: a revoke that voids an observe $y$ "rides, under use-before-revoke (the revoke is causally *after* the published `Vy`), a version `Vz ⊋ Vy`". That premise holds only if "after" means $\downarrow V_y \subseteq \downarrow r$, which is the ideal notion. Step 2 uses collapse-X, so Step 2 defers more revokes than whole-cut dominance re-imports. That gap is exactly the Mixed-Cut Attack.

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
  > **2026-11-04:** no longer. Observer-frame gates let an observe share a component with the observer's row ops, and the throw went. Observes alone still form no cycle: the widening above keeps the observes of one binding acyclic, and the gate graph orders the bindings ([`REFERENCE_FRAME.md`](0014-observer-frame-ref.md) §3.1).
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
   - the B2 wording in [`CAPABILITIES.md`](../modules/rdb/CAPABILITIES.md) ("concurrent with the whole use point", around lines 69–72 and 81). The rest of that file waits for Phase 3.

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
   - [`CAPABILITIES.md`](../modules/rdb/CAPABILITIES.md): the paragraph above the B1/B2 table, and the table itself, which now describes one use-anchored check under the ideal rule.

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
- Update [`VOID_SEMANTICS.md`](../modules/rdb/VOID_SEMANTICS.md) §5.5 with one sentence: Step 2 uses the ideal rule, which is what makes "after `Vy`" coincide with "rides a strictly dominating version".
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

## Follow-up: fixes after Phases 1–4

This section is the plan `PHASE_1_TO_4_FOLLOW_UP_PLAN.md`, which landed with Phases 1–4 (`d8bbd49`). It answers the review of those phases, `PHASE_1_TO_4_CRITIQUE.md`.

This plan covers the issues in `PHASE_1_TO_4_CRITIQUE.md`, with the decisions made after that review. It does not reopen ideal concurrency, antichain admission, or the two-check holding rule. Those stand.

The critique's generic repair for a masked barrier (a positive-anchored search inside the past of `at`) is **not** the fix. Two narrower rules remove the cases where that search would matter:

- A capability is one slot. Grants are not partitioned by `create-cap` origin.
- Barrier-ness of a delete is a property of the set, not of the delete.

Sections are in descending importance. Sections 1-3 change replicated verdicts or validation and ship together. Sections 4-7 do not.

---

### 1. A scoped concurrent cover is decided on the root DAG

This section supersedes §2.3 and §2.4 of the critique, which proposed projecting the position onto the scope. Projection is wrong for the reasons below.

#### Background: what a scoped DAG is

A nested object has no DAG of its own:

- A table's row ops are entries of the group's DAG, tagged `tables: [name]` (`TableScope.baseFilter()` in [`rtable_group/scopes.ts`](../modules/rdb/src/rtable_group/scopes.ts)).
- A nested set element's ops are entries of the outer set's DAG, tagged `inner-elmts: [hash]` (`NestedElementScope.baseFilter()` in [`rset.ts`](../modules/std_types/src/types/rset/rset.ts)).

`NestedScopedDag` is a lens over its parent. It wraps and unwraps payloads, it stamps the tag on writes, and it answers every query by delegating to the parent with the scope filter joined into the caller's filter. Nested scopes chain down to a `RootScopedDag`, which calls the real `Dag`.

Delegation means three things for every query:

- **Positions are root positions.** The positions the caller passes (`at`, `from`, `concurrentTo`) can hold any entry: this scope's, a sibling scope's, or the parent's.
- **The order is the root order.** Walks follow the root DAG's edges. They pass through entries that do not match the filter and continue to those entries' predecessors.
- **The filter selects results.** It decides which entries may be returned. It does not decide what is before, after, or concurrent.

`findCoverWithFilter` and `findMinimalCover` work this way, and so did `findConcurrentCoverWithFilter` before Phases 1-4. For the concurrent cover, the primitive in [`dag/src/idx/concurrent_cover.ts`](../modules/dag/src/idx/concurrent_cover.ts) computes, over all entries, the past of `concurrentTo` and the entries after every head of it. The scope filter only picks which of the remaining entries count as matches.

#### The issue

Phases 1-4 replaced that delegation with a head filter in `NestedScopedDag.findConcurrentCoverWithFilter` ([`mvt/src/dag/dag_nesting.ts`](../modules/mvt/src/dag/dag_nesting.ts), around lines 242-258):

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

**(c) It reopens the Mixed-Cut Attack in rdb.** Every table is a `NestedScopedDag` over its group, and the at-use checks call `killedByConcurrentDelete` on the *target* table's scoped DAG. In the `itemLaterImport` fixture of [`observed_cut_tests.ts`](../modules/rdb/test/observed_cut_tests.ts):

1. Write a second caps row `c′` at `version(g)`, concurrent with `f2`.
2. The revoker deletes the witness row at `{f1, c′}`. Call that delete `d`.
3. The item's author observes the antichain `{f2, c′}` and inserts the item.

When the item's witness check runs, the filter keeps `{c′}`, the only caps head. `d` is after `c′`, so it is not concurrent, and the item survives a delete that never saw `f2`. The `[ENF]` and `[OCUT]` tests pass today only because none of their cuts contains a head in the target table, so the filter falls back to the raw position.

#### Why projecting onto the scope is not the fix either

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

The same happens to `[ENF05]` (an FK instead of an exists check). It also happens to `[OCUT09]`: the single head `{f2}` projects to `{g}`. And it happens to `[OCUT10]`-`[OCUT12]`, whose cuts are all `fill` entries. Under projection, a concurrent delete in the witness table never voids a dependent op in another table, unless the delete also missed some witness-table entry below that op. `concurrentDeletes: true` would behave like `false` for every cross-table FK and exists restriction. The mapping in [`CAPABILITIES.md`](../modules/rdb/CAPABILITIES.md), from RCap's use-anchored barrier to `concurrentDeletes`, would stop holding.

The root cause is the same for both rewrites. A barrier check asks "did the barrier see the use?", and the use is often an op in another scope. Projection replaces the use with the target scope's cut below it, and the barrier usually *did* see that cut. Any rewrite of `concurrentTo` that drops or replaces out-of-scope heads fails the same way. Falling back to the raw heads when the projection is empty does not help, because `{i1}` projects to `{c1}`, which is not empty.

#### The fix

Restore plain delegation, which is the code before Phases 1-4:

```ts
findConcurrentCoverWithFilter(from: Position, concurrentTo: Position, meta: EntryMetaFilter, predicate?: EntryPredicate): Promise<Position> {
    return this.dag.findConcurrentCoverWithFilter(
        from, concurrentTo, joinFilters(this.scope.baseFilter(), this.scope.wrapFilter(meta)), predicate,
    );
}
```

The positions go to the parent unchanged. The scope filter, joined with the caller's filter, decides which entries can be returned. Concurrency is decided on the root DAG.

#### Why this is right

1. **It answers the question the views are defined by.** RCap's `hasCapability` and RSet's membership are documented as "would an op appended at `at` be admissible, observed from `from`?". rdb's at-use checks literally open the view at the op being judged. An op appended at `at` has every head of `at` in its past, whichever scope the head belongs to. A barrier that has not seen one of those heads has not seen that op, so it is concurrent with it. Only the full position can express that.
2. **Kill 1 and kill 2 read the same position.** `NestedScopedDag.findCoverWithFilter` has always delegated with the raw `at`; it never filtered or projected heads. With the head filter, the cover (kill 1) read one position and the concurrent cover (kill 2) read another. Plain delegation makes both checks of the two-check rule about the same `at`.
3. **It inherits the primitive's invariants.** The primitive depends only on the causal past, is monotone, and handles the empty position (design document §3.1). Delegation adds nothing but a filter on matches, which preserves all three. `[META_08]` and `[META_09]` test them at the `Dag` level, so nothing nested-specific is left to get wrong.
4. **Root and nested scopes agree.** `RootScopedDag` has always delegated plainly. With plain delegation, making an object nested, or putting a table in a group, does not change what "concurrent" means.
5. **It is the behavior that was measured.** The design document's §1 shim swapped the primitive under `RootScopedDag`, and nested scopes delegated to it unchanged. The rdb and std_types numbers in that section were measured that way.
6. **It closes the hole in (c).** The cut `{f2, c′}` keeps `f2` as a head. `d` is not after `f2`, so it is concurrent, and it voids the item.

#### What changes for callers

Compared with the current uncommitted code, a result can differ only when two things hold: `concurrentTo` mixes in-scope and out-of-scope heads, and a matching barrier is in the past of `from` but not of `at`.

- **`[NES04]`: two assertions flip.** Nested sets A and B are added to the outer set, then each runs the same script in its own scope:
  1. Add `root`.
  2. Add `shared`.
  3. Branch `left` and `right`.
  4. Barrier-delete `shared` at `left`.

  Each nested set appends at its own frontier, so A's and B's inner ops are mutually concurrent. Running all of A before all of B does not order them:
  - `RSet.add` sets an omitted position to the nested set's scoped frontier before it appends ([`rset.ts`](../modules/std_types/src/types/rset/rset.ts), line 231).
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

#### Tests

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

### 2. A capability is one slot, and so is each grant of it

#### The issue

`readDefinition` in [`modules/std_types/src/types/rcap/view.ts`](../modules/std_types/src/types/rcap/view.ts) treats a capability as a set of origins. A `delete-cap` removes an origin only when the delete itself is still in the cover. A re-create on the delete's branch masks it.

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

#### The fix

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

#### Tests

- **Old origin does not survive a masked delete.** `c1` concurrent with `d < c2`. At the merge the capability is live, and `currentCapCreationVersion` is `{c2}` only if `c2` dominates every earlier create. It is not, when `c1` is concurrent with `c2`: both creates are in the cover and the slot is on, but grants do not care which create they named. Pin that: a grant with origins `{c1}` holds, and a revoke of it on the same branch holds too.
- **Resurrection.** The setup in the issue above. Alice does not hold the capability at `{r, c2}`.
- **`managedBy` is causal.** A concurrent `delete-cap` plus re-create with a different `managedBy` does not change whether a revoke written on the other branch was authorized.
- **`[CAP40]`** changes. A grant whose recorded origins were deleted no longer fails for that reason. After a delete and a re-create it holds, unless a revoke of that pair is live.
- **`[CAP09]`** ("delete and re-create voids old grants") changes for the same reason. Update it to the single-slot outcome or replace it.

---

### 3. Barrier deletes are a property of the set

#### The issue

`supportBarrierDelete` is create-time, but the payload still carries its own bit. `deleteByHash` calls `createDeletePayload(..., false)`, and on a barrier set that returns `{ barrier: false }` ([`rset.ts`](../modules/std_types/src/types/rset/rset.ts), around lines 280-286 and 378-381). `deleteWithBarrierByHash` writes `barrier: true`. `hasByHash` only treats `barrier === true` as a barrier ([`view.ts`](../modules/std_types/src/types/rset/view.ts), around line 72).

So a barrier set can contain both kinds. That is the membership bug:

- Add `a` on one branch.
- On the other: barrier delete `d` concurrent with `a`, then re-add `a2 > d`, then a non-barrier delete `nd > a2`.
- At `{a, nd}` the cover is `{a, nd}`. `d` is under `a2`. Kill 1 sees no barrier in the cover. Kill 2 does not see `d`, because `d` is in the past. `hasByHash` returns true. The old add-anchored search returned false, because `d` is concurrent with `a`.

#### The fix

The flag means every delete. It is not a permission to set the bit.

- `createDeletePayload` writes `barrier: true` on every delete when the flag is on, and omits it when the flag is off. `deleteByHash` and `deleteWithBarrierByHash` are the same operation on a given set. Keep both methods as wrappers so callers do not break.
- `validateDeletePayload` rejects a delete whose bit disagrees with the set. A barrier bit on a non-barrier set already fails. A missing or false bit on a barrier set fails too. Signed payloads cannot be rewritten.
- `hasByHash` treats every delete as a barrier when the flag is on, and treats a `barrier: true` delete on a non-barrier set as a non-barrier delete (it should have been rejected at admission). A delete in the cover then always removes the element on a barrier set, which makes the shape above false: `nd` is a barrier and it is in the cover.

A set with the flag off is unchanged. A delete removes an element only by coming after its add. `[PSET19]` stays.

`supportBarrierAdd` is already the other mode, and the two flags stay mutually exclusive. This plan does not change adds.

#### Tests

- The shape above, on a barrier set: the element is absent at `{a, nd}`.
- `deleteByHash` on a barrier set stores `barrier: true`.
- A payload with `barrier: false` on a barrier set is rejected. A payload with `barrier: true` on a non-barrier set is rejected, as today.
- `[SET02b]`, `[SET02d]`, `[SET02e]`, `[SET04]`, and `[PSET19]` stay.

---

### 4. The documents contradict the code

`[CAP08]` flipped, and the flip is from the definition read moving to the use point, not from ideal concurrency. The view is a single head: `create-cap deploy` and `delete-cap write` are concurrent, and the view is at the deploy, from the frontier. The old check asked whether the delete was concurrent with the genesis origin. The new one asks whether it is concurrent with `at`. It is, so `write` is absent. That matches "a concurrent negative operation wins."

These still say otherwise:

- `DESIGN_AND_PLAN_FOR_ANY.md` §1, the §4 comparison table, and §4 reason 5 say `[CAP29]` is the only std_types test that flips. `[CAP08]` is a Phase 3 flip. Add a §3.5 row: a `delete-cap` concurrent with `at` and after the origin makes the capability absent (`[CAP08]`).
- `MODERNIZATION_v2.md` has a status note, but the body still says `[CAP28]` flips to true, `[CAP29]` stays, and the future of a multi-hash position is the union of its heads' futures (around the `[CAP28]` paragraph, the §5 test list, and the "future is the union" sentence). Rewrite those to the ideal rule and to the outcomes in the design document's §3.5 table.
- The std_types README says a `delete-cap` voids grants tied to the deleted origin. After section 2 it should say the capability is one slot: off means nobody holds it, and a re-create turns the same slot back on without clearing the grants and revokes.

Also record section 1 in the design document: a scoped concurrent cover is decided on the root DAG (plain delegation), and the scope filter only selects matches. State the two `[NES04]` flips and the reason for them, and say that sibling isolation would need a separate query.

---

### 5. Tests the implementation plan asked for and did not get

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

### 6. Four returned versions are still not canonical

Section 3.4 of the design document canonicalized the versions that enter verdicts and schema reads. These still return `resolveRefVersionAtPosition` unchanged, so a caller can observe a dominated ancestor:

| Site | File |
| :--- | :--- |
| `RTableGroupImpl.resolveObservedForeignVersion` | [`group.ts`](../modules/rdb/src/rtable_group/group.ts), around line 754. This is the group view's `resolveRefVersion`, and `providerPublicKeyAt` reads it at `(at, at)`. |
| `RBlobStoreViewImpl.resolveRefVersion` | [`rblob_store/view.ts`](../modules/rdb/src/rblob_store/view.ts), line 23 |
| `RFileMapViewImpl.resolveRefVersion` | [`rfile_map/view.ts`](../modules/rdb/src/rfile_map/view.ts), line 38 |
| `FilesAccessControl.observedVersion` | [`rfiles/access.ts`](../modules/rdb/src/rfiles/access.ts), line 174 |

Canonicalize each against the referenced group's causal DAG, the same way `resolveForeignTableViewIn` does. One test per site: a dominated ancestor in the folded set is not in the returned version.

`diagnoseObserve` canonicalizes `refAt` again after `resolveObserveGateRefAt`, which already returns a canonical version. Drop the second call.

---

### 7. Nits

- `version` is imported in [`rset/view.ts`](../modules/std_types/src/types/rset/view.ts) and no longer used. Remove it.
- `[CAP18]` is still named "Candidate-based delete barriers only void concurrent capability origins." Rename it to the use-point rule it now tests.
- `[OCUT13]` sits before `[OCUT12]` in [`observed_cut_tests.ts`](../modules/rdb/test/observed_cut_tests.ts). Swap them.
- `RCapViewImpl.definitions` caches the promise, including a rejected one, for the life of the view. Views are built for one read. Add a one-line comment that a long-lived view would need to drop a rejected entry.
- `ABSENT.origins` is one shared `Set`. Readers only read it or copy it. Freeze the object, or build a fresh empty set on each return, so a later caller cannot mutate it.

---

### Order of work

1. **Section 1**: plain delegation, the nesting tests, `[OCUT14]`, and the two `[NES04]` flips. This is the invariant break and the rdb hole, and every nested read and every table view goes through it.
2. **Section 2**, then **section 3**. Section 2 closes the resurrection. Section 3 closes the set-membership shape. Neither needs the positive-anchored search from the critique.
3. **Sections 4 and 6.** Documents, then the four canonical returns.
4. **Sections 5 and 7.** The missing tests and the cleanup.

Sections 1-3 ship to every replica in one change. A barrier set that already contains a `barrier: false` delete would start rejecting that payload on re-validation; the sets in this repository's tests write new payloads, and stored dev replicas that mixed the bit should be reset.

## History

This HIP was `modules/rdb/plans/DESIGN_AND_PLAN_FOR_ANY.md`, moved here by [HIP-1](0001-hips.md) on 2026-11-04. Text above that names a file by its old name refers to it as it was. The files it was built from can be read at commit `f45f1e3` (`git show f45f1e3:<path>`).

- `modules/rdb/plans/PHASE_1_TO_4_FOLLOW_UP_PLAN.md`: appended above as *Follow-up*.
- `modules/rdb/plans/PHASE_1_TO_4_CRITIQUE.md`: the review of the uncommitted Phases 1–4, with seven findings: nested scopes dropped out-of-scope heads, a barrier masked on its own branch lost its effect, a voided revoke and a surviving origin could resurrect a grant, `[CAP08]` flipped undocumented, planned tests were missing, some returned versions were not canonical, and nits. The follow-up plan takes each one, and declines the review's generic repair for a masked barrier in favor of two narrower rules.

In git, `d8bbd49` carries RCap and RSet under the ideal rule and the follow-up; the DAG primitive (`dag/src/idx/concurrent_cover.ts`) and canonical versions first appear in `cfce04f`, with the bounded-delta change of [HIP-11](0011-single-pass-widening-bound.md); `6caf1b1` is Phase 6, RFileMap presence under RSet's rule.

Its status line read:

> **Status:** implemented (Phases 1-6; Phase 5 as in [`PHASE_5_PLAN.md`](0008-rset-gated-ref-advances.md)).
> `rfile_map` presence takes RSet's rule (Phase 6). The counter-revocation ring is not done.
> A ref-advance version is canonicalized on write and rejected at admission when
> it is not an antichain. A missing referenced entry stays an infrastructure
> throw, not a validation failure.

The status line of `modules/rdb/plans/PHASE_1_TO_4_FOLLOW_UP_PLAN.md` read:

> **Status:** proposal.
