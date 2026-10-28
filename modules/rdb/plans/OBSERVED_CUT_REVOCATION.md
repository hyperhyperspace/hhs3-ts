# Concurrent Revocation Across Observed Cuts: `ALL` vs. `ANY`, Antichains, and 3-Valued Observation Widening

> **Superseded in part.** The conclusion that Step 2 must use `ALL` (§1.3 and
> executive-summary item 1), the head-advancing-dominance and cut-interval
> design (§4.2, §4.3), and the corresponding proposed changes (§5 items 1 and 3)
> are replaced by [`DESIGN_AND_PLAN_FOR_ANY.md`](./DESIGN_AND_PLAN_FOR_ANY.md).
> The reasons are in [`OBSERVED_CUT_REVOCATION_CRITIQUE.md`](./OBSERVED_CUT_REVOCATION_CRITIQUE.md).
> §2 (the dominated-ancestor analysis) and §3.3 (the Mixed-Cut Attack) stand as
> the problem record. The ideal-concurrency rule described there is what the
> code implements.

This document analyzes how capability revocations (and barrier row/set deletes) interact with multi-element versions $V$ produced by forks and inter-object reference advances (`ref-advance` / `observe`). It connects the designs in [`VOID_SEMANTICS.md`](./VOID_SEMANTICS.md), [`MODERNIZATION.md`](../std_types/MODERNIZATION.md), [`MODERNIZATION_v2.md`](../std_types/MODERNIZATION_v2.md), and [`REFERENCE_FRAME.md`](../rpack/REFERENCE_FRAME.md), and evaluates the exact impact of moving gated observation widening (`resolveObserveGateRefAt`) onto the 3-valued well-founded engine (`VerdictEvaluation`).

---

## Executive Summary

1. **Step 2 must use `ALL` (concurrent with every head of $V$), not `ANY`.**
   - `dag.findConcurrentCoverWithFilter(from, V)` **already** implements `ALL`: it closes $V$ both upwards and downwards and excludes both, returning only entries concurrent with *every* element of $V$.
   - `ANY` breaks **use-before-revoke** (`[CAP29]`): if a cut $V = \{v_1, v_2\}$ has an honest use on branch $v_1$, a later revocation $r >_G v_1$ (appended before merging the unrelated branch $v_2$) is concurrent with $v_2$ and would retroactively void the prior state at $V$.
2. **$V$ must be a cut (an antichain) — exposing a latent bug in `findConcurrentCoverWithFilter`.**
   - When $V$ contains a dominated ancestor ($v_1 <_G v_2$, e.g. from `resolveRefVersionAtPosition` unioning a causal `ref-advance` $\{v_1\}$ with a concurrent barrier `ref-advance` $\{v_2\}$), closing $V$ upwards in `findConcurrentCoverWithFilter` computes $\uparrow \{v_1, v_2\} = \uparrow v_1 \supsetneq \uparrow \{v_2\}$.
   - Any revocation $r$ that branched off $v_1$ concurrently with $v_2$ ($v_1 <_G r$ and $r \perp_G v_2$) is falsely placed in `notConcurrentTo` and missed by Step 2.
   - **Fix:** Normalize `concurrentTo` to its minimal cover (`findMinimalCover(concurrentTo)`) before computing the upward closure inside `findConcurrentCoverWithFilter`.
3. **Where the adversarial fork attack on `ALL` actually bites — and why.**
   - **Ordinary rights-using operations in an observer $O$ (Layer 2) are already safe:** When an honest barrier `ref-advance` $z$ (importing revocation $r$) is concurrent in $O$ with an adversary's operation `op_adv`, Layer 2 (`resolveRefVersionAtPosition`) unions $z$'s version into `op_adv`'s evaluated cut $V$, placing $r \in \downarrow V$ where **Step 1** (causal cover) catches it regardless of any adversarial fork $v_{\text{adv}} \in V$.
   - **Gated `ref-advance` / `observe` operations (Layer 1) have a real vulnerability today (the Mixed-Cut Attack):**
      relative to an antichain $V_y$, Step 2 (`ALL`) ignores $r$ as soon as $r$ is causally after **some** head of $V_y$ ($\exists v \in V_y : v <_G r$), whereas `resolveObserveGateRefAt` only widens $V_y$ with a concurrent `observe` $z$ ($V_z$) when $V_z$ dominates **all** heads of $V_y$ ($\forall v \in V_y : v \le_G V_z$). By publishing a mixed cut $V_y = \{v_1, v_{\text{adv}}\}$ ($v_1 <_G r$ and $v_{\text{adv}} \perp_G r$), a revoked adversary defeats both Layer 1 and Step 2 (`ALL`).
4. **Why naively replacing dominance in `resolveObserveGateRefAt` with 3-valued widening would break `[OBSGATE02]`, and how to do it soundly.**
   - If `resolveObserveGateRefAt` dropped its $G$-dominance filter altogether and returned `undecided` whenever a concurrent `observe` is `undecided`, two **benign** concurrent observes on incomparable $G$-versions ($V_y \perp_G V_z$, `[OBSGATE02]`) would form a 2-element Tarjan component in `VerdictEvaluation`, remain `undecided` in the alternating fixpoint's lower loop, and **both collapse to `void`**.
   - Even worse, an already-revoked adversary could publish a cross-advancing cut $V_y = \{a_{\text{adv}}, b_0\}$ against an admin's honest $V_z = \{a_0, r\}$ ($a_0 <_G a_{\text{adv}}, b_0 <_G r$) to drag the admin's honest `observe` $z$ into an `undecided-cycle` and void it (**Attack 2b**).
   - **The exact, sound fix has two parts:**
     1. **Head-Advancing Dominance ($V_z \succ_{\exists} V_y$):** Widen $\hat{V}_y = \text{minCover}_G(V_y)$ with a concurrent `observe` $z$ ($\hat{V}_z = \text{minCover}_G(V_z)$) iff $V_z$ strictly advances **at least one** head of $\hat{V}_y$:
        $$V_z \succ_{\exists} \hat{V}_y \quad:\Longleftrightarrow\quad \exists v \in \hat{V}_y \setminus \hat{V}_z \text{ such that } v \le_G \hat{V}_z$$
        This closes the exact gap with Step 2 (`ALL`), keeps benign incomparable observes ($V_y \perp_G V_z$) completely acyclic, and preserves the write-time monotonicity stopping invariant ($z' <_O z \land V_z \not\succ_{\exists} \hat{V}_y \implies V_{z'} \not\succ_{\exists} \hat{V}_y$).
     2. **3-Valued Cut-Interval Evaluation $[V_{\text{min}}, V_{\text{max}}]$ inside `VerdictEvaluation`:** In the rare case where two multi-head observes cross-advance different heads of each other ($V_z \succ_{\exists} \hat{V}_y$ and $V_y \succ_{\exists} \hat{V}_z$), evaluate the gate over the version interval $[V_{\text{min}}, V_{\text{max}}]$ induced by the `live` and `undecided` widening candidates. Because an honest admin's capability is untouched in $\downarrow V_{\text{max}}(z) \setminus \downarrow V_{\text{min}}(z)$, the admin's `observe` $z$ is decided **`live` in the Lower loop** of `VerdictEvaluation` and then voids the adversary's `observe` $y$ in the Upper loop — without enumerating completions.

---

## 1. `ALL` vs. `ANY` at a Multi-Element Version $V$

### 1.1 The Two-Step Capability / Witness Rule

Let $G$ be the capability-granting (or witness-holding) DAG. When evaluating whether a capability $C$ (or set element / witness row) holds at version $V \subseteq G$ when observed from horizon $\text{from}_G \subseteq G$ (with $\downarrow V \subseteq \downarrow \text{from}_G$):

- **Step 1 (Causal cover at $V$):** Find the see-through causal cover of live grants and revocations (or inserts and deletes) of $C$ starting from $V$ (`findCoverWithFilter(V, ...)`).
  - If there is no live grant/insert in the cover, **deny**.
  - If there is **any** live revocation/delete in the cover, **deny**.
- **Step 2 (Concurrent barriers at $(V, \text{from}_G)$):** Find any live barrier revocation/delete of $C$ in $\downarrow \text{from}_G$ that is **concurrent with $V$** (`findConcurrentCoverWithFilter(from_G, V, ...)`).
  - If there is any such live revocation/delete, **deny**.

In `Rdb` (`RTableViewImpl.rowState` and `killedByConcurrentDelete` in [`modules/rdb/src/rtable/view.ts`](./src/rtable/view.ts)), each witness grant is its own immutable `rowId` and Step 1 + Step 2 are evaluated per witness row (`EXISTS` takes the disjunction over candidate witness rows). In `RCap` and `RSet` (under [`MODERNIZATION_v2.md`](../std_types/MODERNIZATION_v2.md)), `(cap, grantee)` or `elementHash` is a single reusable slot, so a revocation in the causal cover clears all concurrent grants of that slot, while a later grant $G_2 >_G R$ places $R$ below the cover and re-grants the slot.

### 1.2 What the Codebase Implements Today

In all three DAG index implementations (`flat_idx.ts`, `topo_idx.ts`, `level_idx.ts`), `findConcurrentCoverWithFilter(from, concurrentTo, ...)` builds `notConcurrentTo` by closing `concurrentTo` ($V$) **both upwards and downwards**:

$$\text{notConcurrentTo}(V) = \Bigl(\bigcup_{v \in V} \downarrow v\Bigr) \cup \Bigl(\bigcup_{v \in V} \uparrow v\Bigr)$$

Therefore, an entry $r \in \downarrow \text{from}_G$ passes `!notConcurrentTo.has(r)` if and only if:

$$\forall v \in V,\quad r \notin \downarrow v \;\land\; r \notin \uparrow v \qquad\Longleftrightarrow\qquad \forall v \in V,\quad r \perp_G v$$

That is, **`findConcurrentCoverWithFilter` already implements `ALL`**.

Why did `std_types` previously behave like `ANY` in some scenarios?
- `Rdb` (`killedByConcurrentDelete`) only calls `findConcurrentCoverWithFilter(this.from, this.at, ...)`, which is pure `ALL`.
- `RCap` (`holdingIn` in [`modules/std_types/src/types/rcap/view.ts`](../std_types/src/types/rcap/view.ts)) previously ran **two** checks: `B2` (`findConcurrentCoverWithFilter(this.from, this.at)`, which is `ALL`) **plus** `B1` (`findConcurrentCoverWithFilter(this.from, version(grant))` per grant in the cover). And `RSet` (`hasByHash`) only ran the add-anchored check. Removing the grant/add-anchored check (as specified in `MODERNIZATION_v2.md`) aligns `RCap` and `RSet` with `Rdb` on pure `ALL`.

### 1.3 Why `ANY` Breaks Use-Before-Revoke (`[CAP29]`)

Suppose $V = \text{minCover}_G(V) = \{v_1, \dots, v_k\}$ is a cut (antichain) in $G$, and let $r \in \downarrow \text{from}_G \setminus \downarrow V$ be a live revocation not already caught by Step 1. Because $r \notin \downarrow V$, for each $v_i \in V$ either $v_i <_G r$ or $v_i \perp_G r$:

- **Under `ALL`:** $r$ voids $V$ iff $\forall v_i \in V,\; r \perp_G v_i$ (i.e. $r$ is not causally after *any* head of $V$).
- **Under `ANY`:** $r$ voids $V$ iff $\exists v_i \in V,\; r \perp_G v_i$ (i.e. $r$ voids $V$ unless $r$ is causally after *every* head of $V$).

Consider the standard multi-branch history from `[CAP29]`:

```text
G (capability DAG):
        g0 (Alice is granted C)
       /  \
     v1    v2 (unrelated op on another branch)
     |
     r (Admin revokes C from Alice after v1)
```

Suppose an operation is evaluated at the frontier $V = \{v_1, v_2\}$ (for example, after an observer merges two branches that observed $v_1$ and $v_2$, or in a view at $\{v_1, v_2\}$). Later, an admin whose replica of $G$ is at $v_1$ (and has not yet synced the unrelated branch $v_2$) appends $r$ after $v_1$ ($g_0 <_G v_1 <_G r$, with $r \perp_G v_2$).
- Causally, $v_1$ is already in the past of the use ($v_1 \in V$), and the revocation $r$ happened **after** $v_1$ ($v_1 <_G r$).
- Under **`ANY`**, because $r \perp_G v_2$, $r$ retroactively voids the state at $V = \{v_1, v_2\}$ — violating **use-before-revoke**.
- Under **`ALL`**, because $v_1 \in V$ precedes $r$ ($v_1 <_G r$), $r$ is recognized as causally later than the cut $V$, preserving use-before-revoke.

### 1.4 The Three-Region Partition of $\downarrow \text{from}_G$ Relative to an Antichain $V$

When $V$ is an antichain ($\text{minCover}_G(V) = V$), every entry $r \in \downarrow \text{from}_G$ falls into **exactly one** of three disjoint regions:

| Region of $\downarrow \text{from}_G$ | Defining condition (for antichain $V$) | How the 2-step `ALL` rule handles $r$ |
| :--- | :--- | :--- |
| **1. Causal past of $V$** ($\downarrow V$) | $\exists v \in V : r \le_G v$ | Inspected by **Step 1** (`findCoverWithFilter(V)`). |
| **2. Concurrent with all of $V$** ($\text{Conc}_{\text{ALL}}(V)$) | $\forall v \in V : r \perp_G v$ | Inspected by **Step 2 (`ALL`)** (`findConcurrentCoverWithFilter(from_G, V)`). |
| **3. Causal future of some head of $V$** ($\uparrow V \setminus \downarrow V$) | $\exists v \in V : v <_G r$ (and $r \notin \downarrow V$) | **Deferred by $G$** (causally later than $v \in V$); only voids an observer op if imported by a **concurrent barrier `ref-advance`** in the observer $O$. |

Notice why $V$ being an antichain is essential for Region 3: if $V$ is an antichain and $v <_G r$ for some $v \in V$, then $r$ cannot be in $\downarrow V$ (otherwise $v <_G r \le_G v' \in V$, contradicting that $V$ is an antichain).

---

## 2. Why $V$ Must Be a Cut (Antichain) — and the Dominated-Ancestor Bug

What happens if the set $V$ passed as `concurrentTo` into `findConcurrentCoverWithFilter(from_G, V, ...)` is **not** an antichain — i.e., $V$ contains both an ancestor $v_1$ and a descendant $v_2$ with $v_1 <_G v_2$?

```text
G (capability DAG):
        v1 (Alice holds C)
       /  \
     v2    r (Admin revokes C from Alice; v1 <_G r and r ⊥_G v2)
```

Suppose $V = \{v_1, v_2\}$ with $v_1 <_G v_2$, and $\text{from}_G = \{v_2, r\}$:
- The order ideal represented by $V$ is $\downarrow \{v_1, v_2\} = \downarrow \{v_2\}$, whose cut (minimal cover) is $\hat{V} = \{v_2\}$.
- Relative to the cut $\hat{V} = \{v_2\}$, the revocation $r$ is **concurrent with every element of $\hat{V}$** ($r \perp_G v_2$), so Step 2 (`ALL`) **must** find $r$ and void the capability.
- However, because the dominated ancestor $v_1$ is still present in the set $V = \{v_1, v_2\}$, `findConcurrentCoverWithFilter` closes **every** element of $V$ upwards:
  $$\uparrow \{v_1, v_2\} = (\uparrow v_1) \cup (\uparrow v_2) = \uparrow v_1 \supsetneq \uparrow \{v_2\}$$
  Since $v_1 <_G r$, $r \in \uparrow v_1$ is added to `notConcurrentTo`, and **`findConcurrentCoverWithFilter(from_G, {v_1, v_2})` fails to return $r$!**

### How Non-Minimal Sets $V$ Arise in `resolveRefVersionAtPosition` Today

In [`modules/mvt/src/refs.ts`](../mvt/src/refs.ts) (`resolveRefVersionAtPosition`, lines 140–166), the observer unions the `refVersion` hashes extracted from `causal` (`findRefAdvances`) and `concurrent` (`findConcurrentRefAdvanceBarriers`) into a `Set` without reducing the union to its minimal cover in the referenced DAG:
1. If an entry $e$ in the observer $O$ has a causal `ref-advance` to $\{v_1\}$ and a concurrent barrier `ref-advance` in $O$ widens it with $\{v_2\}$ (where $v_1 <_G v_2$), `resolveRefVersionAtPosition` returns $V = \{v_1, v_2\}$.
2. Similarly, if two branches in $O$ (one at `ref-advance` $\{v_1\}$ and one at `ref-advance` $\{v_2\}$ with $v_1 <_G v_2$) merge in $O$ without a new `ref-advance`, the cover of `ref-advance`s in $O$ contains both entries, so `resolveRefVersionAtPosition` returns $V = \{v_1, v_2\}$.

### The Clean Fix: Normalize `concurrentTo` in `findConcurrentCoverWithFilter`

In `flat_idx.ts`, `topo_idx.ts`, and `level_idx.ts`, `findConcurrentCoverWithFilter(from, concurrentTo, meta, predicate)` already has direct access to `findMinimalCover` on the same index:

```ts
const minConcurrentTo = concurrentTo.size > 1
    ? await findMinimalCoverUsingLevelIndex(index, concurrentTo)
    : concurrentTo;
```

Closing `minConcurrentTo` (instead of raw `concurrentTo`) upwards and downwards guarantees that:
- $\downarrow \text{minConcurrentTo} = \downarrow \text{concurrentTo}$ (downward closure is unchanged),
- $\uparrow \text{minConcurrentTo}$ only closes the **maximal heads (the cut)** of $\downarrow \text{concurrentTo}$ upwards,
- `findConcurrentCoverWithFilter(from, V)` becomes a pure function of the causal ideal $\downarrow V$, invariant to dominated ancestors, and
- Singletons (`concurrentTo.size <= 1`) pay zero overhead, and `resolveRefVersionAtPosition` does not need its signature changed to accept `referencedDag`.

---

## 3. Adversarial Fork Analysis: Observed and Observing Objects

Suppose Alice initially holds a capability $C$ at $g_0 \in G$, and an administrator revokes $C$ from Alice at $r \in G$ ($g_0 \le_G v_1 <_G r$). Alice wants to perform unauthorized actions in an observing object $O$ (or in $G$) that survive even after an honest peer imports $r$ into $O$ via a barrier `ref-advance` $z$.

To escape both Step 1 ($r \notin \downarrow V$) and Step 2 (`ALL`, $\neg \forall v \in \hat{V} : r \perp_G v$), Alice needs the cut $\hat{V}$ at which her operation is evaluated to lie in **Region 3**:
$$r \notin \downarrow \hat{V} \quad\text{and}\quad \exists v_1 \in \hat{V} \text{ such that } v_1 <_G r$$
while padding $\hat{V}$ with a concurrent fork $v_{\text{adv}} \in G$ ($v_{\text{adv}} \perp_G r$) so $\hat{V} = \{v_1, v_{\text{adv}}\}$ is a multi-element antichain.

Let us trace all three ways Alice can try to use $\hat{V} = \{v_1, v_{\text{adv}}\}$ in $O$:

### 3.1 Scenario A: Alice's Rights-Using Op `op_adv` in $O$ vs. a Concurrent Honest `ref-advance` $z$ in $O$ (Layer 2 — Safe)

```text
G (observed capability DAG):          O (observing object DAG):
        g0 (Alice has C)                        base
       /  \                                    /    \
     v1    v_adv (Alice's fork)       y: ref{v1,v_adv}  z: ref{r} (Admin's barrier ref-advance)
     |                                        |
     r (Admin revokes Alice)               op_adv (Alice's add/insert in O)
```

Suppose Alice appends `ref-advance` $y$ publishing $V_y = \{v_1, v_{\text{adv}}\}$ and then appends a rights-using operation `op_adv` (e.g. a set `add` or table `insert`) after $y$. Concurrently in $O$, an honest peer appends a barrier `ref-advance` $z$ publishing $V_z = \{r\}$.

When `op_adv` is evaluated from the merged frontier of $O$:
1. `op_adv` is a single entry in $O$, so its position in $O$ is the singleton `{op_adv}`.
2. Because $z$ is a live barrier `ref-advance` concurrent with `{op_adv}` in $O$ ($z \perp_O \text{op\_adv}$), Layer 2 (`resolveRefVersionAtPosition` in [`modules/mvt/src/refs.ts`](../mvt/src/refs.ts)) unions $V_y = \{v_1, v_{\text{adv}}\}$ with $V_z = \{r\}$:
   $$\hat{V} = \text{minCover}_G(\{v_1, v_{\text{adv}}, r\}) = \{r, v_{\text{adv}}\}$$
3. Because $r \in \hat{V}$, **$r$ is now in the causal cover of $\hat{V}$ in $G$ (Step 1)**!
4. Step 1 denies whenever **any** revocation (or delete of the witness row) is in the causal cover. Thus `op_adv` is **voided**. Alice's fork $v_{\text{adv}}$ in $G$ does not help her at all in Layer 2.

### 3.2 Scenario B: Honest `ref-advance` $z(r)$ Appended Causally *After* `op_adv` in $O$ ($\text{op\_adv} <_O z$)

What if the honest peer's `ref-advance` $z$ (importing $r >_G v_1$) is appended **causally after** `op_adv` in $O$ ($\text{op\_adv} <_O z$)?
- Notice that Alice does not even need a fork $v_{\text{adv}}$ in $G$ for this case: even with a **singleton** $V = \{v_1\}$, we have $v_1 <_G r$ in $G$ and $\text{op\_adv} <_O z$ in $O$.
- Neither `ALL` nor `ANY` voids `op_adv` here, because in **both** DAGs ($G$ and $O$), `op_adv` is strictly in the causal past of the revocation (`[PSET10]`, `[XGROUP11]`). Without external wall-clock timestamps, an operation that causally precedes both the revocation in $G$ and the observation in $O$ is structurally a legitimate **use-before-revoke**.
- An honest peer who wants a revocation $r$ to void concurrent unauthorized operations in $O$ must publish its barrier `ref-advance` $z(r)$ without placing unauthorized operations in $z$'s causal past in $O$ (or must gate who can advance references in $O$, which brings us to Scenario C).

### 3.3 Scenario C: The Real Vulnerability — The Mixed-Cut Attack on Gated Ref-Advances (`resolveObserveGateRefAt`, Layer 1)

Suppose `ref-advance` (`observe`) in $O$ is gated by a capability in $G$ (`canObserve` in `Rdb`, or `refAdvanceCaps` in `RSet`), and Alice's observation capability is revoked at $r >_G v_1$.
Consider what happens when Alice publishes a concurrent back-dated `observe` $y$ in $O$ with the **mixed cut** $V_y = \{v_1, v_{\text{adv}}\}$ ($v_1 <_G r$ and $v_{\text{adv}} \perp_G r$), while Admin concurrently publishes an honest `observe` $z$ in $O$ with $V_z = \{r\}$:

1. In [`modules/rdb/src/rtable_group/group.ts`](./src/rtable_group/group.ts) (`resolveObserveGateRefAt`, lines 706–710), Layer 1 widens $y$'s causal cut $V_y$ with a concurrent `observe` $z$ ($V_z$) **only** when $V_z$ dominates the **entire** cut $V_y$:
   ```ts
   // strictly G-above the base: vz >= base AND base !>= vz
   if (await refVersionAtOrAbove(foreignDag, candidate, base)
       && !await refVersionAtOrAbove(foreignDag, base, candidate)) {
       vz = candidate;
   }
   ```
   Here `base` is $V_y = \{v_1, v_{\text{adv}}\}$ and `candidate` is $V_z = \{r\}$. Because $v_{\text{adv}} \notin \downarrow \{r\}$, `refVersionAtOrAbove(foreignDag, {r}, {v_1, v_adv})` returns **`false`**!
   So Admin's concurrent `observe` $z$ **does not widen $y$**!
2. Next, `diagnoseObserve(y)` evaluates $y$'s gate in $G$ at `refAt` $= \{v_1, v_{\text{adv}}\}$, `refFrom` $= \{r, v_{\text{adv}}\}$:
   - **Step 1 (causal cover at $\{v_1, v_{\text{adv}}\}$):** $r \notin \downarrow \{v_1, v_{\text{adv}}\}$, so Step 1 sees no revocation.
   - **Step 2 (`ALL` at $(\{v_1, v_{\text{adv}}\}, \{r, v_{\text{adv}}\})$):** Because $v_1 <_G r$, $r$ is **not** concurrent with all of $\{v_1, v_{\text{adv}}\}$, so Step 2 returns **`false`**!
3. **Result:** Alice's back-dated `observe` $y$ **stays LIVE**!
    And why does that matter? Because if Alice's `observe` $y$ stays live, Alice can import arbitrary branches of $G$ into $O$ even after her observation capability was revoked and observed by Admin!

Look at the exact quantifier mismatch that created this hole:
- **Step 2 (`ALL`)** stops treating $r \in \downarrow V_z \setminus \downarrow V_y$ as concurrent as soon as **$\exists v \in \hat{V}_y : v <_G r$**.
- **Layer 1 (`resolveObserveGateRefAt`)** only starts widening $V_y$ with $V_z$ when **$\forall v \in \hat{V}_y : v \le_G V_z$**.
- Whenever $|\hat{V}_y| \ge 2$, the gap between $\exists v \in \hat{V}_y$ and $\forall v \in \hat{V}_y$ allows a mixed cut $V_y = \{v_1, v_{\text{adv}}\}$ to bypass both!

---

## 4. Fundamental Properties Analysis: 3-Valued Widening vs. Dominance in Layer 1

Since `resolveObserveGateRefAt` predated `VerdictEvaluation`, it is natural to ask:
> *Can we simply drop the dominance condition in `resolveObserveGateRefAt` (or replace it with a 3-valued walk in `VerdictEvaluation`), and could doing so break any fundamental property of the system?*

Let us check every fundamental invariant of `VerdictEvaluation`, `RTableGroup`, `RSet`/`RCap`, and `computeDelta`.

### 4.1 Fundamental Property 1: Benign Concurrent Observes (`[OBSGATE02]`) and Why Dropping Dominance Altogether Fails

Suppose Manager 1 and Manager 2 concurrently publish benign observations $y$ ($V_y = \{v_1\}$) and $z$ ($V_z = \{v_2\}$) in $O$, where $v_1 \perp_G v_2$ are incomparable versions in $G$ and neither manager is revoked (`[OBSGATE02]`).

What would happen if we **dropped** the $G$-dominance filter in `resolveObserveGateRefAt` so that every concurrent `observe` widens every other concurrent `observe`, and `diagnoseObserve` returned `undecided` whenever a widening `observe` was `undecided` (the way `columnValue` returns `undecided` when a non-dominated write is `undecided`)?

Trace `VerdictEvaluation.resolve` on $y$:
1. `diagnoseObserve(y)` inspects concurrent `observe` $z$, which inspects concurrent `observe` $y$ $\implies$ $\{y, z\}$ forms a 2-member Tarjan strongly connected component.
2. `VerdictEvaluation.solve` runs the alternating fixpoint (`fixpoint` in [`modules/mvt/src/verdict_evaluation.ts`](../mvt/src/verdict_evaluation.ts), lines 244–288) starting from $L = \emptyset$ (known live) and $Up = \{y, z\}$ (possibly live):
   - In the **Lower loop** (which grows $L$ with $Up = \{y, z\}$ fixed), both $y$ and $z$ are in $Up \setminus L$, so both read as **`undecided`**.
   - If `diagnoseObserve(y)` returns `undecided` whenever it meets an `undecided` concurrent `observe` $z$, then `diagnoseObserve(y)` returns `undecided` and `diagnoseObserve(z)` returns `undecided`.
   - **Neither $y$ nor $z$ can ever enter $L$!**
   - The alternating fixpoint terminates with $L = \emptyset, Up = \{y, z\}$, and the bottom-up collapse step (`bottomComponents`) **collapses both $y$ and $z$ to `void` (`undecided-cycle`)**!

Thus, **naively dropping the $G$-dominance condition and returning `undecided` on undecided concurrent observes would break `[OBSGATE02]`**, voiding all concurrent observations of incomparable $G$-versions!

Furthermore, why can't `diagnoseObserve(y)` simply test all $2^k$ subsets of $k$ undecided concurrent observes to see if $y$'s gate holds in all of them?
- Because `canObserve` predicates are **neither monotone nor anti-monotone** in `refAt`: advancing `refAt` in $G$ can both kill an existing witness row (via a `DELETE`) and introduce a new witness row (via a re-grant `INSERT` or column `UPDATE`).
- As [`VOID_SEMANTICS.md`](./VOID_SEMANTICS.md) §4 ("Structure, never enumeration") establishes, enumerating $2^k$ completions is coNP-hard and forbidden by the engine design.

### 4.2 Fundamental Property 2: Head-Advancing Dominance ($V_z \succ_{\exists} \hat{V}_y$) Closes the Mixed-Cut Gap While Preserving `[OBSGATE02]` and Stopping Monotonicity

Recall the Three-Region Partition from §1.4. Given $y$'s causal cut $\hat{V}_y = \text{minCover}_G(V_y)$ and a concurrent `observe` $z \perp_O y$ publishing $\hat{V}_z = \text{minCover}_G(V_z)$:

**Definition (Head-Advancing Dominance $\succ_{\exists}$):**
$$V_z \succ_{\exists} \hat{V}_y \quad:\Longleftrightarrow\quad \exists v \in \hat{V}_y,\; \exists w \in \hat{V}_z \text{ such that } v <_G w$$
Because $\hat{V}_y$ and $\hat{V}_z$ are antichains, for each single head $v \in \hat{V}_y$:
$$\exists w \in \hat{V}_z : v <_G w \quad\Longleftrightarrow\quad v \notin \hat{V}_z \;\land\; \text{refVersionAtOrAbove}(\text{foreignDag}, \hat{V}_z, \{v\})$$

Let us verify the three key properties of $V_z \succ_{\exists} \hat{V}_y$:

1. **Exact Completeness with Step 2 (`ALL`):**
   Suppose $V_z \not\succ_{\exists} \hat{V}_y$ ($\forall v \in \hat{V}_y, \forall w \in \hat{V}_z,\; v \not<_G w$). Then for **every** entry $r \in \downarrow V_z$:
   - $r$ cannot satisfy $v <_G r$ for any $v \in \hat{V}_y$ (otherwise $v <_G r \le_G w \in \hat{V}_z \implies v <_G w$).
   - Therefore, either $r \in \downarrow \hat{V}_y$ (already inspected by **Step 1** at $\hat{V}_y$) or $\forall v \in \hat{V}_y,\; r \perp_G v$ (already inspected by **Step 2 (`ALL`)** at $(\hat{V}_y, \text{refFrom})$!).
   - Conversely, if $r \in \downarrow V_z \setminus \downarrow \hat{V}_y$ is missed by Step 2 (`ALL`), then $\exists v \in \hat{V}_y$ with $v <_G r \le_G w \in \hat{V}_z$, so $V_z \succ_{\exists} \hat{V}_y$ **must** hold!
   - In particular, in the Mixed-Cut Attack ($V_y = \{v_1, v_{\text{adv}}\}$ and $V_z = \{r\}$ with $v_1 <_G r$ and $v_{\text{adv}} \perp_G r$), $V_z \succ_{\exists} \hat{V}_y$ is **`true`** (because $v_1 \in \hat{V}_y \setminus \{r\}$ and $v_1 \le_G \{r\}$), while $V_y \succ_{\exists} \hat{V}_z$ is **`false`**. So $z$ widens $y$ with no cycle and voids $y$!

2. **Preservation of Benign Incomparable Observes (`[OBSGATE02]`):**
   When $V_y$ and $V_z$ are concurrent observations on incomparable branches ($\forall v \in \hat{V}_y, w \in \hat{V}_z : v \perp_G w$, or equal heads), neither $V_z \succ_{\exists} \hat{V}_y$ nor $V_y \succ_{\exists} \hat{V}_z$ holds. Neither widens the other, no component forms, and both stay `live`.

3. **Preservation of Write-Time Monotonicity Walk Stopping (`[OBSGATE09]`):**
   In `resolveObserveGateRefAt`, the backward cover walk over `findConcurrentRefAdvanceBarriers` stops at any candidate $z$ that does not widen $\hat{V}_y$ (returning `true` to the cover predicate without recursing into $z$'s predecessors $z' <_O z$).
   Is stopping at $z$ when $V_z \not\succ_{\exists} \hat{V}_y$ still sound?
   **Yes:** By write-time monotonicity of `ref-advance` (`validateRefAdvanceMonotonicity`), $z' <_O z \implies \downarrow V_{z'} \subseteq \downarrow V_z$. If some predecessor $z' <_O z$ had $V_{z'} \succ_{\exists} \hat{V}_y$ ($\exists v \in \hat{V}_y, w' \in V_{z'} : v <_G w'$), then since $\downarrow V_{z'} \subseteq \downarrow V_z$ there is some $w \in V_z$ with $w' \le_G w$, giving $v <_G w' \le_G w \in V_z \implies V_z \succ_{\exists} \hat{V}_y$. Contrapositively:
   $$z' <_O z \;\land\; V_z \not\succ_{\exists} \hat{V}_y \implies V_{z'} \not\succ_{\exists} \hat{V}_y$$
   So no predecessor of a non-advancing candidate can advance any head of $\hat{V}_y$ either.

### 4.3 Fundamental Property 3: Cross-Advancing Double-Forks (Attack 2b) and 3-Valued Cut-Interval Evaluation in `VerdictEvaluation`

Unlike whole-cut dominance ($\downarrow V_y \subsetneq \downarrow V_z$, which is a strict partial order on all cuts), head-advancing dominance $\succ_{\exists}$ is a strict partial order on singletons, but on **multi-head cuts** two cuts can cross-advance different heads of each other!

Consider **Attack 2b (The Cross-Advancing Double-Fork Attack)**:
```text
G (observed capability DAG):
     a0               b0 (Alice and Admin both have manager caps)
     |                |
     a_adv (Alice)    r (Admin revokes Alice's manager cap)
```
- Admin's replica of $G$ is at $V_z = \{a_0, r\}$, and Admin publishes an honest `observe` $z$ in $O$ with $V_z = \{a_0, r\}$.
- Alice (who is revoked at $r >_G b_0$) sees $V_z = \{a_0, r\}$. She appends a valid entry $a_{\text{adv}} >_G a_0$ on the $a_0$ branch (where she was not yet revoked, or on an open table) and publishes a concurrent back-dated `observe` $y$ in $O$ with:
  $$V_y = \{a_{\text{adv}}, b_0\}$$
Look at the $\succ_{\exists}$ edges between $y$ and $z$:
- $V_z = \{a_0, r\} \succ_{\exists} V_y = \{a_{\text{adv}}, b_0\}$ because $b_0 \in V_y$ and $b_0 <_G r \in V_z$.
- $V_y = \{a_{\text{adv}}, b_0\} \succ_{\exists} V_z = \{a_0, r\}$ because $a_0 \in V_z$ and $a_0 <_G a_{\text{adv}} \in V_y$.

Thus $\{y, z\}$ forms a 2-element Tarjan component!
What must happen here?
- **Admin's `observe` $z$ must be decided `live`**, because Admin holds the manager cap at both $V_z = \{a_0, r\}$ (if $y$ is void) and $\text{minCover}_G(V_z \cup V_y) = \{a_{\text{adv}}, r\}$ (if $y$ is live)!
- And once $z$ is decided `live` in the Lower loop of `VerdictEvaluation`, $z$ widens $y$'s cut to $\{a_{\text{adv}}, r\}$, where $r$ is in the causal cover (Step 1), so **Alice's `observe` $y$ is decided `void`**!

How do we achieve this in `VerdictEvaluation` while satisfying **Exactness, Soundness, and Information-Monotonicity** (`[VREAD06]`) and **without enumerating completions**?

#### The 3-Valued Cut-Interval Rule $[V_{\text{min}}, V_{\text{max}}]$

In `resolveObserveGateRefAt(evaluation, refId, opPos, from)` for an `observe` $z$:
1. Compute the causal base cut $V_{\text{base}} = \text{minCover}_G(\text{resolveRefVersionAtPosition}(\text{dag}, \text{refId}, \text{opPos}, \text{opPos}))$.
2. Run a see-through walk over `findConcurrentRefAdvanceBarriers(dag, refId, opPos, from, predicate)` for candidates $w$ with $V_w \succ_{\exists} V_{\text{base}}$:
   - If $w$ does not satisfy $V_w \succ_{\exists} V_{\text{base}}$: stop the walk on this branch (return `true` to the cover predicate and ignore $w$).
   - If $w$ satisfies $V_w \succ_{\exists} V_{\text{base}}$, query `verdict = await this.resolveVerdict(evaluation, w, from)`:
     - If `verdict.status === 'live'`: stop the walk at $w$ (return `true`) and add $V_w$ to `liveVersions`.
     - If `verdict.status === 'void'`: see through $w$ (return `false`).
     - If `verdict.status === 'undecided'`: **see through $w$** (return `false`, so any live $w' <_O w$ below $w$ is still reached!) and record $(w, V_w, \text{verdict.support})$ in `undecidedCandidates`.
3. Why is seeing through `'undecided'` $w$ to `'live'` $w' <_O w$ below it sound in every completion?
   Because by write-time monotonicity ($w' <_O w \implies \downarrow V_{w'} \subseteq \downarrow V_w$):
   - If $w$ completes to `'void'`, the see-through cover lands on $w'$, contributing $\downarrow V_{w'}$.
   - If $w$ completes to `'live'`, the cover lands on $w$, contributing $\downarrow V_w \supseteq \downarrow V_{w'}$.
   - Thus $\downarrow V_{w'}$ is contained in $\downarrow \text{refAt}$ in **every** completion!
4. Define the lower-bound and upper-bound cuts in $G$:
   $$V_{\text{min}} = \text{minCover}_G\Bigl(V_{\text{base}} \cup \bigcup_{u \in \text{live}} V_u\Bigr)$$
   Drop any undecided candidate $w$ whose version is already dominated by $V_{\text{min}}$ ($\downarrow V_w \subseteq \downarrow V_{\text{min}}$). For the remaining undecided candidates:
   $$V_{\text{max}} = \text{minCover}_G\Bigl(V_{\text{min}} \cup \bigcup_{w \in \text{undecided}} V_w\Bigr), \qquad S_{\text{cut}} = \bigcup_{w \in \text{undecided}} S_w$$
   In every completion of the undecided candidates, the resolved cut $V$ satisfies:
   $$\downarrow V_{\text{min}} \;\subseteq\; \downarrow V \;\subseteq\; \downarrow V_{\text{max}}$$
5. **Evaluating the foreign view over $[V_{\text{min}}, V_{\text{max}}]$:**
   - When $S_{\text{cut}} = \emptyset$ ($V_{\text{min}} = V_{\text{max}}$, which holds whenever there is no cross-advancing cycle), this is the ordinary single-cut view at $(V_{\text{min}}, \text{refFrom})$.
   - When $S_{\text{cut}} \neq \emptyset$ ($V_{\text{min}} \neq V_{\text{max}}$), an entry $e \in \downarrow V_{\text{max}}$ in $G$ is:
     - **Definitely in the causal past of $\text{refAt}$** if $e \in \downarrow V_{\text{min}}$ (its truth in the causal cover walk is $\text{entryTruth}(e)$).
     - **Possibly in the causal past of $\text{refAt}$** if $e \in \downarrow V_{\text{max}} \setminus \downarrow V_{\text{min}}$ (its truth in a causal cover walk from $V_{\text{max}}$ is $\text{allTruth}([\text{entryTruth}(e), \text{undecidedOn}(S_{\text{cut}})])$: `FALSE` if $e$ is void in $G$, and `undecided` with support $\text{support}(e) \cup S_{\text{cut}}$ otherwise).
     - And for Step 2 (`killedByConcurrentDelete` / `useRevokes`), a barrier delete/revoke $d \in \downarrow \text{refFrom} \setminus \downarrow V_{\text{max}}$ is definitely concurrent with $\text{refAt}$ if $d \notin \uparrow V_{\text{min}}$, and possibly concurrent (undecided with support $S_{\text{cut}}$) if $d \in \uparrow V_{\text{min}} \setminus \uparrow V_{\text{max}}$ (any $d \in \downarrow V_{\text{max}} \setminus \downarrow V_{\text{min}}$ is already seen as an undecided delete/revoke in Step 1's causal walk from $V_{\text{max}}$!).

Let us check how this resolves **Attack 2b** ($V_y = \{a_{\text{adv}}, b_0\}$ by Alice vs. $V_z = \{a_0, r\}$ by Admin):
- In Pass 1 of the Lower loop of `VerdictEvaluation` ($L = \emptyset, Up = \{y, z\}$):
  - For Admin's $z$: $V_{\text{min}}(z) = \{a_0, r\}$, $V_{\text{max}}(z) = \{a_{\text{adv}}, r\}$. Admin's manager cap was inserted at $g_0 \in \downarrow V_{\text{min}}(z)$ and has no delete or update in $\downarrow V_{\text{max}}(z) \setminus \downarrow V_{\text{min}}(z) = \{a_{\text{adv}}\}$. Thus `diagnoseObserve(z)` returns **`{ status: 'live' }` (`TRUE`)**!
  - $z$ is added to $L$!
- In Pass 2 of the Lower loop ($L = \{z\}, Up = \{y, z\}$):
  - For Alice's $y$: since $z \in L$ is now `'live'`, $V_{\text{min}}(y) = V_{\text{max}}(y) = \{a_{\text{adv}}, r\}$.
- In the Upper loop ($L = \{z\}$):
  - At $V_{\text{min}}(y) = \{a_{\text{adv}}, r\}$, $r$ is a live delete of Alice's manager cap inside $\downarrow V_{\text{min}}(y)$ (Step 1), so `diagnoseObserve(y)` returns **`{ status: 'void' }`**, removing $y$ from $Up$!
- **Final Verdicts:** Admin's $z$ is **`live`**, and Alice's $y$ is **`void`**!

### 4.4 Fundamental Property 4: Why Layer 2 (`resolveForeignTableViewIn`) Still Always Receives Boolean Verdicts

In `RTableGroupImpl.resolveForeignTableViewIn` (Layer 2) and `RSetViewImpl`, `resolveRefVersionAtPosition(dag, groupId, at, from, isLive)` requires a **boolean** callback `isLive: (h: B64Hash) => Promise<boolean>`.
If `observe` entries can form a Tarjan component in Layer 1 (when two multi-head observes cross-advance each other), can `isObserveLive(evaluation, groupId, h, from)` ever receive `'undecided'` when called from Layer 2?

**No — never.** Here is the proof from Tarjan's algorithm in `VerdictEvaluation`:
1. Layer 2 (`resolveForeignTableViewIn`) is called while diagnosing a row/bundle op $R$ in the observer group $O$ (or from a top-level view query).
2. When `isObserveLive` calls `resolveVerdict(evaluation, h, from)` for an `observe` entry $h \in O$, $h$ is pushed onto the Tarjan stack above $R$.
3. What can `diagnoseObserve(h)` reach?
   - Other `observe` entries in $O$ (for the same binding in v1, or lower-ranked bindings in $O$'s acyclic gate dependency DAG in [`REFERENCE_FRAME.md`](../rpack/REFERENCE_FRAME.md) §2.2), and
   - Entries in bound foreign groups $G$ (which lie strictly lower in the creation-pinned group binding DAG).
4. Because `canObserve` predicates are forbidden from referencing observer-local tables (`REFERENCE_FRAME.md` §2.1 rule 3) and group bindings form a strict DAG, **no path from `diagnoseObserve(h)` can ever reach a local row/bundle op $R$ in $O$**.
5. Therefore, the lowlink of the root `observe` frame called by Layer 2 can never drop below its own stack index. Its Tarjan SCC is **guaranteed** to complete inside `resolveVerdict` and return a decided verdict (`'live'` or `'void'`) to Layer 2!

### 4.5 Fundamental Property 5: Why `refFrom` / `foreignFrom` Must Stay Geometric

In both Layer 1 (`diagnoseObserve`) and Layer 2 (`resolveForeignTableViewIn`), `refAt` filters voided observes, while `refFrom` is resolved **geometrically** at `(from, from)` without `isLive`:

```ts
const foreignAt = await resolveRefVersionAtPosition(dag, groupId, at, from, isLive);
const foreignFrom = await resolveRefVersionAtPosition(dag, groupId, from, from);
```

Why must `refFrom` stay geometric (unfiltered by `isLive`)?
1. **Avoiding self-loops on `from`:** When diagnosing an `observe` $y$ at horizon `from` (where typically $y \in \text{cover}(\text{from})$), calling `resolveRefVersionAtPosition(dag, refId, from, from, isLive)` would walk `from` and immediately call `isLive(y)` while `y` is on the stack — creating a spurious self-loop on every frontier `observe`!
2. **BFT Soundness of geometric `refFrom`:** `refFrom` only defines the negative-evidence horizon in $G$; it never adds positive witnesses to `refAt`. An attacker whose `observe` in $O$ is voided cannot inject a fake revocation into $G$ via `refFrom`, because every entry in $G$ reached from `refFrom` in Step 2 **still undergoes $G$'s own at-use verdict check (`entryStatusIn` / `opVerdict`)**. If the attacker lacked authority in $G$ (or was concurrently revoked in $G$), the attacker's revocation entry in $G$ is itself `void` in $G$ and seen through by Step 2.
3. **Exact Delta Floor Parity:** `computeObserverRevisionBound` in [`modules/mvt/src/delta.ts`](../mvt/src/delta.ts) resolves `refAtMeet` and `refAtEnd` geometrically at `(observerMeet, observerMeet)` and `(observerEnd, observerEnd)`. Keeping `refFrom` geometric ensures the view horizon and the delta revision bound use the exact same foreign frontier.

### 4.6 Fundamental Property 6: Multi-Hop Chains of Rights-Granting Objects ($G_0 \leftarrow G_1 \leftarrow \dots \leftarrow O$) and `REFERENCE_FRAME.md`

What happens when permissions are delegated across a chain of objects — e.g., $G_0$ grants capabilities governing $G_1$ (both ops in $G_1$ and `observe` of $G_0$ in $G_1$), and $G_1$ grants capabilities governing $O$?
1. **Acyclicity across objects:** Creation-time bindings form a strict DAG ($O \to G_1 \to G_0$), and within a single group $O$ under [`REFERENCE_FRAME.md`](../rpack/REFERENCE_FRAME.md) §2.2, cross-binding `canObserve` gates (`auth` $\neq$ `target`) also form a genesis-validated strict DAG. Every recursive `resolveVerdict` call strictly descends the lexicographic order:
   $$\bigl(\text{group binding DAG rank},\; \text{observer gate DAG rank},\; \text{SCC within one `(group, refId)`}\bigr)$$
2. **End-to-end defense against multi-hop forks:**
   Suppose Alice is revoked at $r_0 \in G_0$, and tries to fork $G_0$, fork $G_1$ (publishing a mixed-cut `observe` of $G_0$ or inserting a sybil grant in $G_1$), and fork $O$:
   - An honest barrier `observe` $A_{\text{honest}} \in G_1$ (importing $r_0$) and an honest barrier `observe` $B_{\text{honest}} \in O$ (importing $A_{\text{honest}}$) propagate the widening at each hop:
   - In $O$, $B_{\text{honest}}$ widens $O$'s view of $G_1$ so $A_{\text{honest}}$ is included in $G_1$'s `at` and `from`.
   - In $G_1$, $A_{\text{honest}}$ widens Alice's mixed-cut `observe` of $G_0$ via $\succ_{\exists}$ (Layer 1) and widens Alice's sybil grant in $G_1$ via Layer 2, placing $r_0 \in \downarrow V_{G_0}$ where Step 1 in $G_0$ voids both!
   - In $G_1$, the see-through walks skip Alice's voided `observe` and voided sybil grant, which in turn voids Alice's dependent `observe` and operations in $O$.
3. **Why `REFERENCE_FRAME.md` was relevant:**
   In `REFERENCE_FRAME.md` §2.1, when a gate on `target` ($H$) in $O$ reads a *different* bound capability group `auth` ($K \neq H$), it resolves $K$ via Layer 2 (`resolveSchemaForeignTableViewIn(..., filterVoided = true)`), which unions **all** live concurrent observations of $K$ in $O$ into `authAt`. Thus the $K \neq H$ path already unioned all live concurrent `auth` observations into Step 1, whereas the self-gated $K = H$ path went through `resolveObserveGateRefAt` (Layer 1) and suffered from the whole-cut dominance gap on mixed cuts. Upgrading Layer 1 to $\succ_{\exists}$ + $[V_{\text{min}}, V_{\text{max}}]$ brings the $K = H$ path to the same security level as $K \neq H$.

---

## 5. Summary of Proposed Changes

1. **`modules/dag` (`flat_idx.ts`, `topo_idx.ts`, `level_idx.ts`):**
   - In `findConcurrentCoverWithFilter(from, concurrentTo, meta, predicate)`, normalize `concurrentTo` to its minimal cover (`concurrentTo.size > 1 ? await findMinimalCover(concurrentTo) : concurrentTo`) before closing `concurrentTo` upwards into `notConcurrentTo`.
2. **`modules/std_types` (`rcap/view.ts`, `rset/view.ts`, `rset/validate.ts`):**
   - Proceed with [`MODERNIZATION_v2.md`](../std_types/MODERNIZATION_v2.md) (replacing the grant/add-anchored barrier searches with the 2-step `ALL` rule: causal cover at `at` + concurrent cover against `at` from `from`).
   - Bring permissioned `RSet` `ref-advance` onto the same admission (`(newRefVersion, newRefVersion)`) and at-use `isLive` filtering as `Rdb` (`MODERNIZATION.md` §"Ref-advances voided through RCap").
3. **`modules/mvt` & `modules/rdb` (`resolveObserveGateRefAt` / `diagnoseObserve`):**
   - Replace whole-cut dominance ($\downarrow V_y \subsetneq \downarrow V_z$) with **head-advancing dominance** ($V_z \succ_{\exists} \text{minCover}_G(V_y)$): a concurrent barrier `ref-advance` $z$ widens $y$ iff $\exists v \in \text{minCover}_G(V_y) \setminus \text{minCover}_G(V_z)$ such that `refVersionAtOrAbove(foreignDag, Vz, version(v))`.
   - Support 3-valued cut-interval evaluation $[V_{\text{min}}, V_{\text{max}}]$ when `resolveObserveGateRefAt` encounters an `'undecided'` cross-advancing `observe`, so honest observations in a cross-advancing component resolve to `'live'` in the Lower loop of `VerdictEvaluation` and void the attacker's back-dated observation.
