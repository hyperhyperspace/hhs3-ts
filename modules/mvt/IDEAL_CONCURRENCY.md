# Ideal concurrency

A view of a replicated object is read at a version `at`, observed from a later version `from`. Most of what a view shows comes from the history of `at`. The rest comes from **barriers**: entries visible from `from` that are outside the history of `at` and *concurrent* with it. A revoke the use never saw, a delete of a row the use depended on, a deploy on another branch: each one reaches a read only by being concurrent with `at`.

For a position with one head, "concurrent" has only one sensible meaning. A position with several heads needs a rule, and the choice decides which revokes void an op, which deletes kill a row, and which observes widen a foreign version. This note records the rule the system uses, **ideal concurrency**, and why it replaced the earlier one.

Section 1 defines the terms. Section 2 explains why the DAG query `findConcurrentCoverWithFilter` carries this decision for every type. Section 3 states the two rules that were considered. Section 4 argues for ideal concurrency, and Section 5 describes what it means for the rest of the system. Section 6 collects implementation notes.

---

## 1. Terms

**Entries and positions.** An object's history is a DAG of **entries**. Each entry is appended after its **predecessors**. A **position** $P$ is a set of entries, its **heads**; a version is a position. The **history** $H(P)$ is the heads together with everything reachable from them through predecessors. Histories are downward-closed. The **future** $F(x)$ of an entry $x$ is $x$ together with every entry that has $x$ in its history.

**States and encodings.** A position stands for its history: two positions with the same history describe the same state. A head that is an ancestor of another head is **dominated**, and dropping it does not change the history. The **minimal cover** of $P$ (`findMinimalCover`) drops every dominated head. A position equal to its minimal cover is an **antichain**, the canonical encoding of its history.

**Views.** `getView(at, from)` reads the state at `at`, revised by what is visible from `from`. Normally $H(\text{at}) \subseteq H(\text{from})$. With `at = from` the view is the plain state at that version.

**Barriers.** Entries tagged `barrier` in their meta. Only barriers can affect a read across branches. Some remove and some add:

- **Negative:** RCap revokes and `delete-cap`; rdb row deletes, honored across branches when the table sets `concurrentDeletes`; deletes in an RSet with `supportBarrierDelete`; file removes in an `RFileMap`.
- **Positive:** adds in an RSet with `supportBarrierAdd`; rdb schema deploys and their parameters; observes and other ref-advances.

**References.** An observer object records which version of another object it has seen through **ref-advance** entries in its own DAG: an rdb group's **observe** of a bound group, or a permissioned RSet's ref-advance of its RCap. A ref-advance publishes a version its author chooses, as long as it is at or above what its predecessors saw. An op in the observer reads the referenced object at the fold of the ref-advances in its history and the barrier ref-advances concurrent with it. In rdb this is the op's **foreign cut**.

**Use point.** The position a check is evaluated at. rdb evaluates an op's verdict at use: at the op's own position, observed from the horizon. When the op reads another group, the use point there is its foreign cut.

---

## 2. Why `findConcurrentCoverWithFilter` matters

### 2.1 The two checks

Every type reads a value at `(at, from)` with two DAG queries:

1. **The cover at `at`.** `findCoverWithFilter(at, filter, predicate)` returns the latest matching entries in $H(\text{at})$. The predicate makes the walk see-through: an entry it rejects, such as a void op, is skipped and the walk continues below it.
2. **The concurrent cover.** `findConcurrentCoverWithFilter(from, at, filter, predicate)` returns the latest matching entries in $H(\text{from})$ that are concurrent with `at`. The filter normally includes `barrier: ['t']`.

The first query never leaves $H(\text{at})$. The second is the only way an entry outside it reaches the read. This is the MVT definition of a view in the [README](./README.md): the state at `at` plus the barriers of `from` that are concurrent with `at`.

So the meaning of "concurrent with `at`" decides, for every type:

- whether a revoke voids a use of a capability it did not see (RCap);
- whether a delete of a witness row or FK target voids an op that depended on it (rdb, `concurrentDeletes`);
- whether a barrier delete removes an element, and whether a barrier add makes one appear (RSet);
- whether a deploy or a schema parameter on another branch revises the schema at a merge (rdb);
- whether a concurrent observe widens the version of a foreign group that an op reads (`resolveRefVersionAtPosition`, and rdb's observe gate).

These answers become verdicts, and verdicts feed other reads. A void revoke revokes nothing, and a void observe contributes no version. A change in the definition therefore runs through the whole verdict computation, including the parts that solve mutually dependent verdicts and bound deltas.

### 2.2 The callers

| Caller | Barrier | Can `at` have several heads? |
| :--- | :--- | :--- |
| `RTableViewImpl.killedByConcurrentDelete` (rdb `rtable/view.ts`) | row delete | yes: a foreign cut, an observe gate's version, a view at a merged frontier |
| `RTableGroupImpl.resolveGroupParams` (rdb `rtable_group/group.ts`) | schema parameter | yes |
| `findConcurrentRefAdvanceBarriers` (`src/refs.ts`), through `resolveRefVersionAtPosition` and rdb's `resolveObserveGateRefAt` | observe, deploy, ref-advance | yes, for a view at a multi-head `at` with `from ≠ at`; at-use checks pass the op's own position |
| `RCapViewImpl.readDefinition` (std_types `rcap/view.ts`) | `delete-cap` | yes, the view's `at` |
| `RCapViewImpl.holdingIn` | revoke | yes, the view's `at` |
| `RSetViewImpl.hasByHash` (std_types `rset/view.ts`) | barrier delete, barrier add | yes, the view's `at` |
| `isPresent` (rdb `rfile_map/presence.ts`) | file remove | no, it passes the add's own position |

### 2.3 Multi-head positions are ordinary

A position has several heads whenever it spans concurrent branches, which is the normal state of a replicated object:

- A replica that has merged two branches reads at a frontier with a head from each.
- An op's foreign cut folds the versions published by every observe it can see. Different writers publish different versions, so the fold is often an antichain of several heads.
- An observe or ref-advance publishes a version its author picks.
- Positions inside a nested object are positions of the root DAG. A table in a group, or a set nested in a set, is read at positions whose heads can belong to a sibling scope or to the parent.

In the second and third cases, the party being judged picks the position it is judged at. That makes the multi-head rule a security question, not a corner case.

---

## 3. Two ways to extend the single-head rule

For a single head $\{v\}$, an entry $x$ outside $H(v)$ is either after $v$ ($v$ is a proper ancestor of $x$) or concurrent with it. Now let $P$ have heads $v_1, \dots, v_k$ and take $x \notin H(P)$. Then $x$ is below no head, and for each head it is either above it or concurrent with it. There are two natural rules:

- **`ALL`.** $x$ is concurrent with $P$ when it is concurrent with **all** heads. As soon as $x$ is above one head, it is later than $P$. The future of a position is the union of its heads' futures. Older notes call this **collapse-X**, because the position is treated as one collapsed point.
- **`ANY`, or ideal concurrency.** $x$ is concurrent with $P$ when it is concurrent with **any** head. $x$ is after $P$ only when it is above every head, that is, when $H(P) \subseteq H(x)$: its author had seen all of $P$. The future of a position is the intersection of its heads' futures.

| Relation of $x$ to $P$ | `ALL` | `ANY` |
| :--- | :--- | :--- |
| in the past | $x \in H(P)$ | $x \in H(P)$ |
| later | $x \notin H(P)$ and $x \in \bigcup_i F(v_i)$ | $x \notin H(P)$ and $x \in \bigcap_i F(v_i)$ |
| concurrent | everything else | everything else |

The two agree on single heads. They differ exactly when $x$ is above some heads of $P$ and not others:

```text
g ── a ── x
 \
  b
```

At $P = \{a, b\}$, $x$ is above $a$ and concurrent with $b$. `ALL` puts $x$ after $P$. `ANY` makes it concurrent.

The system used to implement `ALL`. All three DAG indexes excluded $H(\text{concurrentTo}) \cup \bigcup_i F(v_i)$ and returned the matches that remained. They now exclude $H(\text{concurrentTo}) \cup \bigcap_i F(v_i)$.

### 3.1 The argument that was made for `ALL`

This is the shape behind `[CAP29]`:

```text
g ── a1 ── u        g    grants alice `write`
 \                  a1   an unrelated entry on one branch
  a2                a2   an unrelated entry on the other branch
                    u    revokes alice's `write`, appended after a1
```

Read at $\{a_1, a_2\}$ from $\{a_2, u\}$: does alice hold `write`? Under `ALL` she does. The revoke is after $a_1$, so it is later than the use point, and voiding the use would be retroactive, a violation of **use-before-revoke**. Under `ANY` she does not, because $u$ has not seen $a_2$.

The argument assumes its conclusion. The use point is the state that holds both branches, and any op written there has $a_2$ in its past. $u$'s author had not seen $a_2$, so $u$ has not seen that op, and the op has not seen $u$. Neither knew of the other, which is exactly the case a barrier exists for. "The use came before the revoke" is true of $a_1$, not of the state the use read.

---

## 4. Why `ANY`

### 4.1 It reads a position as a state

For a single head, $x$ is after $\{v\}$ exactly when $H(v) \subseteq H(x)$. `ANY` is the same condition applied to the whole position: a barrier is later than a use point when its author had seen everything the use point holds, and concurrent otherwise. `ALL` instead asks whether *some* head is in $H(x)$, which is a statement about one head, not about the state. Under `ALL`, multi-head positions were more lenient than single heads. Under `ANY` they are treated exactly like them.

The rule is about the position, not about any one op written at it. A barrier above every head of $P$ can still be concurrent in the DAG with an op appended at $P$, since both have the heads as ancestors. `ANY` treats that barrier as after $P$, as a single head does: its author had seen all the state the op used.

### 4.2 Knowing more never helps the party being judged

Under `ANY`, the entries that reach $P$, either in its history or concurrent with it, are $\{x : x \in H(P) \text{ or } H(P) \not\subseteq H(x)\}$. Their complement is the set of entries after $P$. If $H(P) \subseteq H(Q)$, every entry after $Q$ is also after $P$. So the set of barriers that reach a position only grows as its history grows. Widening a position can add barriers but never remove them.

Under `ALL`, adding a head below a barrier moves the barrier from concurrent to later. Whoever can add a head to a position can cancel a barrier, and Section 2.3 shows that this is often the op's own author. These tests pin the cases that `ALL` left live:

- **Mixed cut on a gated observe (`[OCUT02]`).** In the observed group $G$: $v_0 \to v_1 \to r$, where $r$ revokes alice, and $v_0 \to v_{adv}$ on another branch. Alice observes $\{v_1, v_{adv}\}$ while an admin concurrently observes $\{r\}$. The admin's version is not above $v_{adv}$, so the gate does not widen alice's cut with it. Under `ALL`, $r$ is above $v_1$, so it is later than alice's cut, and her observe stays live. She can keep importing $G$ after her revocation. Under `ANY`, $r$ has not seen $v_{adv}$, so it is concurrent with her cut and voids the observe.
- **Padding (`[OCUT10]`).** An item read at the foreign cut $\{f_2\}$ is voided by a later import of a delete $d$ of its witness row that is concurrent with $f_2$ (`[OCUT09]`). The same item at $\{f_2, f_1\}$, with $f_1$ below $d$, survived under `ALL`.
- **Rescue by an unrelated observe (`[OCUT05]`, `[OCUT11]`).** A voided observe or item at $\{f_2\}$ came back to life when an honest observer concurrently published $\{w, f_1\}$, with $w$ above $f_2$ and $f_1$ below the barrier. The widened cut had a head below the barrier.

All of these are void under `ANY`.

One dodge remains under both rules: a cut entirely below the barrier. An author who observes only $\{v_1\}$, below the revoke, and writes before the revoke is imported, keeps a live op (`[OCUT06]`, `[OCUT08]`). That is use-before-revoke, and without wall-clock time no rule can tell it apart from an honest early write. Under `ANY` an op that dodges this way can use only state the revoker had already seen.

### 4.3 It depends only on the causal past

A dominated head does not change the intersection of futures, because its future contains its dominator's. So under `ANY` the result is a function of $H(P)$, whatever encoding of the position the caller passes.

`ALL`, as implemented, closed every head upward, including dominated ones. A position $\{v_1, v_2\}$ with $v_1 < v_2$ hid barriers above $v_1$ that $\{v_2\}$ would have exposed. Three sources of non-antichain positions hit this: a single observe payload carrying a dominated ancestor (`[OCUT03]`), the observe gate's widening, which unions its base with a dominating version (`[OCUT07]`), and the fold of an op's causal and concurrent observes (`[OCUT12]`). `ALL` could have been repaired by normalizing the position first. `ANY` needs no repair.

### 4.4 It makes the observe gate's acyclicity argument exact

An rdb binding with a `canObserve` gate authorizes who may advance the observation of a foreign group $G$. An observe $y$ that publishes $V_y$ is judged in two layers:

- **Layer 1, the gate.** $y$'s gate is evaluated in $G$ at $V_y$, widened by concurrent observes that are live and publish a version strictly above it ($H(V_y) \subsetneq H(V_z)$). A revoke of $y$'s author voids $y$.
- **Layer 2, reference resolution.** An op reads $G$ at the fold of the live observes it can see.

Layer 1 only climbs to strictly larger histories, so observes never form verdict cycles ([`VOID_SEMANTICS.md`](../rdb/VOID_SEMANTICS.md) §5.5). That rule loses nothing only if every revoke that should void $y$ is either concurrent with $V_y$, where the concurrent cover finds it, or carried by an observe that strictly dominates $V_y$.

**Claim.** Under `ANY` this holds exactly. Let $r$ be a live revoke visible from the horizon, outside $H(V_y)$.

1. If $H(V_y) \not\subseteq H(r)$, then $r$ is concurrent with $V_y$.
2. Otherwise $H(V_y) \subsetneq H(r)$. Any observe $z$ that carries $r$ has $H(V_z) \supseteq H(r) \supsetneq H(V_y)$, so $z$ strictly dominates $y$, which is the widening condition.

Strict inclusion of histories is a strict partial order, so the widening has no cycles.

Under `ALL`, case 1 missed every revoke above one head of $V_y$, and case 2 only brought back revokes carried by a version above every head. The gap between "above one head" and "above every head" is the mixed-cut attack.

Keeping `ALL` would have meant widening $y$ by any observe that advances at least one head of $V_y$. That closes the mixed cut, but two honest observers that synced different branches each advance a head of the other, so they form a verdict cycle. Solving those cycles needed a three-valued rule that evaluates the gate over an interval of versions. Under `ALL` that rule is unsound: whether a revoke is later than a cut is not monotone in the cut, so the interval's endpoints do not bound the cuts between them. Padding and rescue would still be open.

| | `ALL` | `ALL` with head-advancing widening | `ANY` |
| :--- | :--- | :--- | :--- |
| Mixed cut (`[OCUT02]`) | open | closed | closed |
| Dominated heads (`[OCUT03]`, `[OCUT07]`, `[OCUT12]`) | open | closed, with normalization | closed |
| Padding (`[OCUT10]`) | open | open | closed |
| Rescue (`[OCUT05]`, `[OCUT11]`) | open | open | closed |
| Observe cycles | none | yes, for benign pairs | none |
| Code changed | none | DAG primitive, both gate layers, verdict engine | DAG primitive |

### 4.5 One meaning everywhere

Every caller in Section 2.2 asks the same question: is this barrier concurrent with this position? That includes positive barriers. A barrier add, deploy or schema parameter that has not seen one head of a merged frontier applies there, for the same reason a concurrent delete kills. `ANY` is also the reading of rdb's `concurrentDeletes` flag, "a concurrent delete wins", and of the premise in [`VOID_SEMANTICS.md`](../rdb/VOID_SEMANTICS.md) §5.5.

### 4.6 The measured cost was small

Before the primitive changed, a shim that replaced it under `RootScopedDag` ran the full suites with `ANY`. In rdb, the only failures were the seven `[OCUT]` tests written to pin the gaps above. In std_types, only `[CAP29]` failed. The delta parity sweeps passed, and no call passed an empty position.

---

## 5. What it means for the system

### 5.1 The invariant

> A revoked principal's surviving ops can depend only on state the revoker had already seen.

### 5.2 Multi-head outcomes

| Case | Outcome | Test |
| :--- | :--- | :--- |
| A revoke after one head of the use point, the grant below both | void | `[CAP29]` |
| A revoke after one head, the grant on the other | void | `[CAP28]` |
| A revoke after both heads | holds (use-before-revoke) | `[CAP30]` |
| A `delete-cap` after one head | capability absent | `[CAP37]` |
| A barrier delete after one head, the add on the other | element gone | `[SET02d]` |
| A barrier add after one head | element joins | `[SET02c]` |
| A deploy after one head of a two-head frontier | schema revised | `[DEPLOY20]` |
| The mixed cut, padding and rescue of Section 4.2 | void | `[OCUT02]`, `[OCUT05]`, `[OCUT10]`, `[OCUT11]` |
| A cut entirely below the barrier, with a later import | live | `[OCUT06]`, `[OCUT08]` |

### 5.3 Costs

- **A revoked principal loses more ops.** Any op whose position includes a head the revoker had not seen is voided. That is what a barrier is for, now applied consistently.
- **Honest ops can lose liveness.** Suppose a witness row or FK target is deleted on a branch that an op's position only partly covers. The op is voided, where `ALL` let it survive if the position also held an ancestor of the delete. Whether it did was an accident of sync timing, not a policy. Single heads already behave this way under both rules. A table that should not let concurrent deletes win sets `concurrentDeletes: false`; the flag is part of the schema, which the group controls, not the op's author. Revoking from a merged frontier, and keeping observations current (`rdb_runtime`'s `ref_auto_update`), shrink the window.
- **Positive barriers apply more often at unmerged frontiers.** A barrier add, deploy or parameter after one head of a multi-head `at` applies there.

Going back to `ALL` would only make sense for a hard requirement that ops at multi-head positions survive revokes issued by stale replicas, in tables that cannot use `concurrentDeletes: false`. Even then it needs observe cycles and a sound three-valued rule for `ALL`'s non-monotone deferral, which does not exist, and it leaves padding and rescue open.

### 5.4 Nested scopes decide concurrency on the root DAG

A nested object has no DAG of its own. A table's row ops are entries of its group's DAG, and a nested set's ops are entries of the outer set's DAG. `NestedScopedDag` is a lens over the parent: it wraps and unwraps payloads and answers queries by delegating with the scope filter joined into the caller's filter. For the concurrent cover, it passes the caller's positions to the parent unchanged. The filter decides which entries may be returned. It never decides what is before, after or concurrent.

Two ways of rewriting the position inside the lens were tried, and both are wrong:

- **Dropping out-of-scope heads.** Take $n_1$ in scope and $s_1$ outside it, with $n_1 < s_1$, and a barrier appended at $\{n_1\}$ concurrently with $s_1$. The positions $\{s_1\}$ and $\{n_1, s_1\}$ have the same history. The barrier is concurrent with the first and, once $s_1$ is dropped from the second, after it. The result is not a function of the history and not monotone. In rdb it reopened the mixed cut: an item at a foreign cut $\{f_2, c'\}$, where $c'$ is the only head in the caps table, survived a witness delete that had seen $c'$ but not $f_2$ (`[OCUT14]`).
- **Projecting the position onto the scope**, replacing it with the latest in-scope entries of its history. That breaks every check whose use point is in another scope. In `[ENF08]` a caps row $c_1$ is deleted on one branch ($d$) while an item $i_1$ that needs it is inserted on another, and the witness check reads the caps table at $\{i_1\}$. With delegation, $d$ has not seen $i_1$, so it is concurrent and the item is void. Projection turns $\{i_1\}$ into $\{c_1\}$, $d$ is after $c_1$, and the item survives. Across tables, `concurrentDeletes: true` would behave like `false`.

The root cause is the same for both: a barrier check asks whether the barrier saw the use, and the use is often an entry of another scope.

Sibling scopes can therefore interact at a mixed cut. In `[NES04]`, nested sets A and B each barrier-delete `shared`. Read at a position that holds A's delete and B's add, B's delete has not seen A's head, so it is concurrent and `shared` is gone from B. Default reads are not affected, because a nested object's `getView` uses its own scoped frontier. A product that needs "B's state at the cut B has seen" needs a separate query, and that query must not replace the concurrent cover the at-use checks rely on.

### 5.5 Versions stay canonical

Under `ANY`, barrier checks no longer need antichains. Two rules keep versions canonical anyway:

- **Admission rejects a ref-advance version that is empty or not an antichain**, before the monotonicity check. One state then has one encoding, which keeps signatures, entry hashes and deduplication stable, and removes the `[OCUT03]` shape at its source even if the primitive regresses. Writers publish the minimal cover before signing. Incoming payloads are rejected rather than canonicalized, because signed bytes cannot be rewritten.
- **Versions computed from unions are canonicalized** where they become keys or are returned. rdb's verdict memo keys on the sorted `from`, so two encodings of one history would be two frames. Under `ANY` they agree, so this saves duplicate work rather than fixing wrong answers (`[OCUT17]`). Returned versions do not show dominated heads (`[OCUT15]`, `[OCUT16]`).

### 5.6 The empty position

Nothing is concurrent with the empty position. Every entry is after it, since $H(\emptyset) = \emptyset \subseteq H(x)$, and an intersection over no heads is the whole DAG. The old code did the opposite and counted every matching entry as concurrent. The definition keeps the rule monotone all the way down. No caller passes an empty position today.

---

## 6. Implementation notes

### 6.1 Where it lives

- **[`dag/src/idx/concurrent_cover.ts`](../dag/src/idx/concurrent_cover.ts)**: `findConcurrentCoverUsing(ops, from, concurrentTo, meta, predicate)`. The flat, topo and level indexes supply `getPreds`, `loadEntry` and `minimalCover` and delegate to it. The definition is stated on `Dag.findConcurrentCoverWithFilter` in `dag/src/dag.ts`.
- **[`src/dag/dag_nesting.ts`](./src/dag/dag_nesting.ts)**: `RootScopedDag` calls the `Dag`. `NestedScopedDag.findConcurrentCoverWithFilter` passes the positions to its parent unchanged and joins the scope filter.
- **[`src/refs.ts`](./src/refs.ts)**: `findConcurrentRefAdvanceBarriers`, `canonicalVersion` (the minimal cover; a single head is returned as is) and `validateRefVersionShape` (a non-empty antichain). `CausalDag` exposes `findMinimalCover` for them.
- **Admission**, shape before monotonicity: observes and deploys in rdb `rtable_group/validate_ops.ts`, bound-group ref-advances in rdb `rfiles/access.ts`, and RSet ref-advances in std_types `rset/validate.ts`. A referenced entry missing from the replica is an infrastructure error that throws, so the sync layer defers the op. It is not a validation failure.
- **Writers that canonicalize before signing**: `RTableGroupImpl.observe` and `prepareDeploy` (which `deploy` uses), `RSet.refAdvance`, `RFileMap.refAdvance` and `RBlobStore.refAdvance`.
- **Computed versions that are canonicalized**: in rdb, `foreignAt` and `foreignFrom` in `resolveForeignTableViewIn`, the result of `resolveObserveGateRefAt`, `resolveObservedForeignVersion`, the schema version, the versions that key `computeRevisionBound`'s memo, and the observed version in `rfiles/access.ts`; in std_types, the RCap versions in `RSetViewImpl`.
- The rdb read rules (`killedByConcurrentDelete`, `rowState`, the observe gate) needed no code change. They inherit the meaning from the primitive.

### 6.2 The algorithm

```text
if concurrentTo is empty: return {}             # nothing is concurrent with it

forward  = successor map over H(from)           # walk predecessors from `from`
below    = H(concurrentTo)                      # walk predecessors from concurrentTo
for each head h of concurrentTo:
    count every node of h's upward closure through `forward`
afterAll = nodes counted |concurrentTo| times
excluded = below ∪ afterAll

walk back from `from`:
    if n is not excluded and matches meta and predicate: keep n, stop on this path
    else: continue into n's predecessors        # see-through
return minimalCover(kept)
```

- **The empty position is an explicit early return.** Without it, `afterAll` is empty, nothing is excluded, and every match counts as concurrent, which is the old behavior. Any implementation that folds futures into an accumulator has the same trap.
- **Dominated heads need no normalization.** A dominated head's upward closure contains its dominator's, so counting per head yields exactly the intersection over the maximal heads. Each head keeps its own visited set, so a node is counted at most once per head.
- **Heads outside $H(\text{from})$ are allowed.** The downward walk follows predecessors directly, so `below` is complete. The upward closures run over the forward map of $H(\text{from})$, where such a head reaches nothing. Then no entry of $H(\text{from})$ is after the position, which is correct: none of them has that head in its history. Callers normally pass $H(\text{at}) \subseteq H(\text{from})$, but the primitive does not assume it (`[META_08]`).
- **The walk is see-through.** A predicate rejection continues below the entry, so a void barrier does not hide a live one. Callers pass their verdict predicates here.
- **Cost.** $k$ upward closures over $H(\text{from})$ instead of one union closure, and the same when $k = 1$. A single topological pass with a $k$-bit set per node would do the work once; it has not been needed.

### 6.3 Details worth keeping

- **The primitive changed in place.** There is no sibling function with the old meaning. Every caller means "concurrent with this position", and two meanings side by side would let the next caller pick the wrong one.
- **Anchored searches went away, and masked barriers came back.** Under `ANY`, RCap's search for revokes concurrent with each grant, and RSet's search for deletes concurrent with each add, are redundant for barriers outside $H(\text{at})$: a revoke concurrent with a grant in $H(\text{at})$ has not seen that grant, so it is concurrent with `at`. Removing them left two checks, the cover at `at` and the concurrent cover. That exposed barriers inside $H(\text{at})$ hidden by a later entry on their own branch: a `delete-cap` followed by a re-create, and a barrier delete followed by a re-add and a non-barrier delete. The fix was two narrower rules, not the anchored searches. A capability is one slot: a `delete-cap` turns it off, a re-create turns the same slot back on, and grants do not hang off a named create. On a set with `supportBarrierDelete`, every delete is a barrier, and admission rejects a delete whose bit disagrees with the set (`[SET15]`, `[SET16]`).
- **Who manages a capability is read in the op's own past.** The manager list for a grant or revoke is read at `(op, op)`, so a `delete-cap` on another branch cannot empty the list, void a valid revoke, and bring the revoked grant back. Whether the author holds the managing capability still uses the outer `from`, so a concurrent revoke of the author's authority still applies.
- **`[CAP08]` flipped for a different reason.** Its view is a single head. The flip came from reading the capability definition at the use point instead of at its origin, which landed together with `ANY`.
- **Deployment.** The change alters replicated verdicts and validation, so every replica has to run the same version. Stored data with a non-antichain ref-advance, or a barrier set that mixed barrier and non-barrier deletes, is rejected on re-sync. Reset such dev replicas.

### 6.4 What we learned

- **Pin a semantic choice with a test the alternative would fail.** Before the `[OCUT]` group existed, the rdb suite passed under `ALL`, under normalized `ALL` and under `ANY`. `[CAP29]` was the only test that encoded the choice, and it encoded the conclusion, not a reason.
- **Measure before changing.** The shim in Section 4.6 gave the blast radius before any code moved. It replaced the method on `RootScopedDag`, so it did not see callers of the raw `Dag`. The one such caller, `rfile_map/presence.ts`, passes a single head, where the rules agree.
- **A lens must not rewrite positions.** Both rewrites in Section 5.4 looked like scope hygiene, and both broke a property the design depends on.
- **Removing a redundant check can expose a case it covered by accident.** The anchored searches were redundant for the case they were written for, and they also covered masked barriers.

### 6.5 Still open

- **RSet ref-advances are not yet judged like gated observes.** Admission checks the parent cut, and every advance counts in the fold. The plan is in [`rdb/plans/PHASE_5_PLAN.md`](../rdb/plans/PHASE_5_PLAN.md). It reuses Layer 1's walk, which under `ANY` needs no variant that widens on a single head.
- **`rfile_map/presence.ts`** still uses the add-anchored search. It passes a single head, so it agrees with `ANY`, but it does not follow the two-check rule the other types use.
- **Positive barriers** follow `ANY` uniformly. No positive barrier is known whose meaning is "applies only when after none of the heads".

### 6.6 Tests

- **dag**, `test/meta_search_tests.ts`:
  - `[META_08]`: hand-built cases on all three indexes. A barrier above one of two heads is concurrent, above both it is after, a dominated head changes nothing, nothing is concurrent with the empty position, and a head outside the history of `from` leaves the rest concurrent.
  - `[META_09]`: on random DAGs, with several `from` positions, the cover on a raw position equals the cover on its minimal cover, and the three indexes agree.
  - The index parity pairs include a two-head and a non-antichain `concurrentTo`. The `dag_test` backend conformance suite has a two-head case that every backend must pass.
- **mvt**:
  - `[DNS04]`–`[DNS07]` in `test/dag_nesting_tests.ts`: a use point outside the scope, mixed heads, a dominated head, and a randomized check that the scoped cover equals the raw call with the scope filter joined.
  - `[REFS_10]` in `test/refs_tests.ts`: `canonicalVersion` and `validateRefVersionShape`.
- **rdb**:
  - `test/observed_cut_tests.ts`: `[OCUT01]`–`[OCUT12]` are the scenarios of Section 4. `[OCUT13]` rejects a dominated ancestor at admission and checks that the writer publishes the minimal cover. `[OCUT14]` is the nested mixed cut. `[OCUT15]`–`[OCUT17]` check canonical versions and shared verdict frames.
  - `[ENF05]` and `[ENF08]` pin use points in another table. `[DEPLOY20]` pins a positive barrier at a two-head frontier, and `[DEPLOY21]` a deploy carrying a dominated ancestor. `[OBSGATE02]`–`[OBSGATE09]` pin the observe gate.
- **std_types**: `[CAP28]`–`[CAP30]`, `[CAP37]`, `[CAP38]`, `[SET02c]`–`[SET02e]`, `[SET15]`, `[SET16]`, `[NES04]` and `[PSET29]`.

### 6.7 Background

The decision record and its reviews are in `rdb/plans/`: [`DESIGN_AND_PLAN_FOR_ANY.md`](../rdb/plans/DESIGN_AND_PLAN_FOR_ANY.md), [`OBSERVED_CUT_REVOCATION.md`](../rdb/plans/OBSERVED_CUT_REVOCATION.md) (which argued for `ALL`), its two critiques, and the Phase 1–4 critique and follow-up plan. This note is the reference. Those documents record how it was reached.
