# The incremental delta bound of RTableGroup

A replica that has applied one version of a table group and wants to catch up to a later one asks for a **delta** between the two. Replaying the whole history is always correct, but it costs the whole history. The bounded delta replays only the recent part: it walks back from the later version and stops at a **bound**, trusting that nothing below the bound changed.

This note explains how `RTableGroup` chooses that bound. Section 1 defines the terms. Section 2 shows why the obvious bound, the meet of the fork, is not enough. Section 3 describes what a correct bound needs, and why the group computes it during the walk itself. Section 4 sketches the algorithm, Section 5 argues that it is correct, and Section 6 collects implementation notes and what we learned building and testing it.

---

## 1. Terms

**Entries and positions.** The group's history is a DAG of **entries**. An entry is appended after its **predecessors**. The group's `create` entry is the only entry with no predecessors. A **position** is a set of entries, its **heads**. The **history** of a position $P$, written $H(P)$, is its heads together with everything reachable from them through predecessors. Histories are downward-closed: an ancestor of an entry of $H(P)$ is in $H(P)$.

A position $P$ is **at or below** a position $Q$ when $H(P) \subseteq H(Q)$. An entry $e$ is **strictly after** $P$ when every head of $P$ is a proper ancestor of $e$, that is, when $H(P) \subseteq H(\mathrm{preds}(e))$. An entry is **concurrent** with $P$ when it is outside $H(P)$ and not strictly after $P$. The **greatest lower bound** $\mathrm{GLB}(P, Q)$ is the set of heads of $H(P) \cap H(Q)$.

**Versions and deltas.** A delta goes from a version `start` to a version `end`, where `end` extends `start`: $H(\text{start}) \subseteq H(\text{end})$. The **new entries** are those of $H(\text{end}) \setminus H(\text{start})$. The **fork** of `start` and `end` (`findForkPosition`) gives, among other things, `fork.common`: the entries of $H(\text{start})$ with a direct successor among the new entries. The **meet** is the GLB of the entries of `fork.common` (`computeForkMeet`).

**Verdicts.** Every row operation, bundle and observe has a **verdict**, live or void, at a **horizon** (a version). The verdict is evaluated at use: at the entry's own position, observed from the horizon. A delete whose rule requires a permission row is void at a horizon where a concurrent delete has removed that row. A voided entry has no effect: a voided insert never makes its row live, a voided delete deletes nothing. Verdicts are a pure function of the entry and the horizon.

**Barriers.** An entry's verdict can read entries on other branches, but only **barriers**. Barriers are tagged `barrier` in the entry meta. There are three kinds:

- **Deletes**, alone or inside a bundle. Whether a delete reaches concurrent branches is decided by the table's `concurrentDeletes` flag, resolved at the delete's own position observed from the horizon. We call this the delete's **honor flag**.
- **Schema deploys**: ref-advances that publish a version of the group's schema object. A deploy that adopts an FK also deletes the rows the FK strands. Those deletes are computed when the deploy is appended and stored in its meta, and they behave like deletes.
- **Observes**: ref-advances that publish a version of a bound foreign group. Cross-group FKs and `exists` rules read the foreign group at the observed version. A binding may be gated (`canObserve`), so an observe can itself be void.

Ref-advances are **monotone**: a ref-advance publishes a version at or above every version its predecessors see. Validation enforces this for deploys and observes alike.

**The delta.** A group delta has three channels:

- **Schema changes**, computed from the schema versions at `start` and at `end`.
- **Row changes**: rows whose liveness or written values differ between the views at `start` and at `end`.
- **Op verdict flips**: entries whose verdict differs between `start` and `end`.

The **full** delta walks all of $H(\text{end})$. The **bounded** delta with bound $B$ walks $H(\text{end}) \setminus H(B)$. Every walked entry is checked for a verdict flip, and every row a walked entry touches is diffed. A bound $B$ is **sound** when every entry of $H(B)$ has the same verdict at both horizons and every row whose operations all lie in $H(B)$ has the same state at both horizons. With a sound bound the bounded delta equals the full delta: anything that differs is touched by a walked entry.

---

## 2. Why the fork meet is not enough

Below the meet the two versions share their history, so it is tempting to use the meet as the bound. A small history shows the problem. The tables:

- `perm`, with a label column.
- `targets`. Deleting a target requires some `perm` row labelled `ok`.
- `items`, with an FK to `targets`.
- `notes`, for filler.

Deletes on `perm` and `targets` reach concurrent branches.

```text
p ── x ── E ── s1 ── N        p  inserts a perm row labelled ok
      \                       x  inserts a target
       T                      E  inserts an item pointing at x
                              s1 a note after E
                              T  deletes x (concurrent with E and s1)
                              N  deletes the perm row p
```

`start` is $\{s_1, T\}$ and `end` is $\{N, T\}$.

At `start`, `T` is live. It is concurrent with `E`, so `E` sees its target deleted, its FK fails, and `E` is void. At `end`, `N` is concurrent with `T` and removes the only `ok` row, so `T`'s rule fails and `T` is void. Now `E`'s target stands and `E` is live. The full delta reports both `T` and `E`.

The only new entry is `N`. `fork.common` is $\{s_1\}$, and the meet is $\{s_1\}$. A walk from `end` that stops at the meet visits `N` and `T` and never reaches `E`, which is only reachable through $s_1$. The bounded delta misses `E`.

The change travels from `N`, which is strictly after the meet, through `T`, an old barrier on a side branch, into `E`, which is below the meet. `T` is concurrent with both.

A variant of the same shape shows that watching the barrier's own verdict is not enough. Let `T` be an observe on the side branch that publishes a foreign version where a permission still stands. `E` is concurrent with `T`, so `E`'s view of the foreign group widens to `T`'s version. Let `N` observe a later foreign version that contains a concurrent revoke of that permission. Then `E` flips from live to void while `T` stays live at both horizons. What changed is `T`'s contribution to `E`'s foreign view. `T`'s own verdict is the same at both horizons.

The bound used before this change started from the meet and, for the schema and for each bound foreign group, moved down to the lowest observes in the meet's history whose published versions were outside the observed object's own bound. That bound was taken between the versions observed at the meet and at `end`. Neither example has such an observe in the meet's history, so the bound stayed at the meet. The walk stopped only at the bound's heads, so it sometimes walked the whole past of a side branch's attachment point and reached the affected entry anyway, but nothing guaranteed it.

---

## 3. What a sound bound needs

Write $U = H(B)$. Three conditions make $B$ sound:

1. **No new entry inside.** $U \subseteq H(\text{meet})$.
2. **Barriers next to $U$ keep their effect.** Every barrier outside $U$ that is concurrent with some entry of $U$ has the same effect at `start` and at `end`.
3. **Observes inside $U$ are stable.** Every observe in $U$ publishes a version inside the sound bound of the group it observes.

The **effect** of a barrier $T$ at a horizon is what entries on other branches read from it:

- its verdict;
- for each table it deletes rows of, its honor flag;
- for an observe of a group $F$, whether its published version lies inside $F$'s bound (it is then **stable**; otherwise **unstable**).

The schema version a deploy publishes is fixed, and deploys are never void, so a deploy's effect can change only through its honor flag.

There are two ways to satisfy condition 2.

The first works from shape alone. It lowers the bound below every barrier concurrent with it, whether or not its effect changed. That is sound and needs no verdicts, but an old side branch that `start` already contains, and that never merged into the meet, pulls the bound below itself even when the new entries do not touch it. On histories with long-lived branches the bounded delta then costs as much as the full one.

The second lowers the bound only below barriers whose effect changed. Deciding that needs each barrier's verdict at both horizons, and the delta computes exactly those verdicts for every entry it walks. Lowering the bound exposes more entries, which may hold more barriers, so the decision cannot be made once up front. This is why the bound is computed during the walk: the walk starts with the meet as the bound, checks each old barrier it meets, lowers the bound when one has changed, and carries on.

Two more questions shape the design.

**Which foreign versions.** An entry below the bound reads a foreign group $F$ through a view whose horizon is the version of $F$ observed at the delta's horizon: `refAtStart` at `start` and `refAtEnd` at `end`. A bound computed by the change-driven rule only promises agreement between the two versions it compared. Between `refAtMeet`, the version observed at the meet, and `refAtEnd`, a barrier of $F$ can be live, void at `refAtStart`, and live again. A bound for (`refAtMeet`, `refAtEnd`) then says nothing about `refAtStart`. So $F$'s bound must be computed between `refAtStart` and `refAtEnd`. Section 5.6 gives a history where the difference matters.

**The schema.** An entry reads the schema at its own position: the deploys in its history plus the deploys concurrent with it. Deploys are never void, the fold has no liveness filter, and the schema object itself has no barriers, so its view at a fixed version does not depend on any horizon. No new deploy is concurrent with an entry below the meet (Lemma 1), so an entry below the bound reads the same schema at both horizons. The schema therefore needs no bound of its own, and the group declares it to the walk as a reference read at $(v, v)$ (Section 6.1). Bounding it anyway, with RSchema's own bound between the versions at `start` and at `end`, would be sound but less precise. That bound is the meet of RSchema's fork (`rschema/delta.ts`). When `end` deploys a schema version that merges schema branches forking below a version some deploy of `start` published, that meet drops below it. The deploy would look unstable and pull the bound down for nothing.

---

## 4. The algorithm

The walk below computes the bound and the delta together. Its result is the bounded delta and its bound, or, in **bound-only** mode, just the bound.

**Phase A: the starting bound.**

1. Compute the fork of `start` and `end`. If there are no new entries, the delta is empty and the bound is `start`.
2. Set `bound = meet`.
3. For each bound foreign group $F$:
   - Let `refAtStart` and `refAtEnd` be the versions of $F$ observed at `start` and at `end`.
   - Compute $b_F$, $F$'s bound between them, with the same algorithm in bound-only mode. Bindings between groups form a DAG, so this recursion ends.
   - Start from the latest observes of $F$ in $H(\text{meet})$ and descend through the unstable ones, stopping at the first stable observe on each branch. By monotonicity, everything below a stable observe is stable.
   - For each lowest unstable observe $r$ found this way, set `bound = GLB(bound, preds(r))`. The observe itself then lies above the bound and is walked.

**Phase B: the walk.** A backward breadth-first walk from the heads of `end`, with:

- `stopSet`: the **exit border** of $H(\text{bound})$ inside $H(\text{end})$. These are the entries of $H(\text{bound})$ that are heads of `end` or have a direct successor outside $H(\text{bound})$.
- `inStart`: a set that tracks which walked entries belong to $H(\text{start})$. It is seeded with `start` and `fork.common`, and extended to the predecessors of every walked member.
- `stopped`: the entries where the walk halted at the border.
- `deployCoverMoved`: whether $H(\text{end}) \setminus H(\text{start})$ contains a schema deploy.

For each entry popped from the queue:

```text
if visited: skip
if in stopSet: add to stopped; skip           # do not enter H(bound)
mark visited; load it
if in inStart: add its predecessors to inStart
feed it to the row channel                    # not in bound-only mode
canWiden = bound non-empty and entry in inStart and entry is a barrier
if void-checkable and (not bound-only or canWiden):
    voidBefore, voidAfter = verdicts at start and at end
    record a flip if they differ              # not in bound-only mode
if canWiden and (
       voidBefore != voidAfter
    or it observes a bound group F and its version is not inside b_F
    or (deployCoverMoved and the honor flag of a table it deletes from
        differs between start and end)):
    if entry is not strictly after bound:
        bound   = GLB(bound, preds(entry))
        stopSet = exit border of the new bound
        requeue every stopped entry no longer in stopSet
enqueue its predecessors
```

**Phase C.** Finalize the row channel (diff every candidate row between the views at `start` and at `end`), sort the flips, and return the delta with `bound` as its revision bound. In bound-only mode, return `bound`.

Two cases show the walk at work on the history of Section 2.

- **`N` revives `E`.** The walk pops `N` and `T`. `T` is an old barrier whose verdict flips, and it is not strictly after $\{s_1\}$, so the bound drops to $\mathrm{GLB}(\{s_1\}, \{x\}) = \{x\}$. Then $s_1$ is walked, then `E`, whose flip is recorded. `x` stops the walk.
- **`N` changes nothing.** Say `N` only adds a note. Then `T` does not trigger, the walk stops at $s_1$ and at `x`, and only `N` and `T` are loaded. The bound stays at the meet.

---

## 5. Why it is correct

### 5.1 Assumptions

These are properties of the group implementation that the argument relies on.

- **A1. One root.** The `create` entry is the only entry without predecessors. It is in $H(\text{start})$ whenever `start` is non-empty.
- **A2. Determinism.** A verdict is a function of the entry and the horizon. Mutually dependent verdicts are solved together, deterministically.
- **A3. Cross-branch reads see only barriers.** The verdict of an entry $E$ at horizon $H$ reads:
  - entries of $H(E)$;
  - the schema and group parameters at $(\{E\}, H)$;
  - deletes concurrent with $\{E\}$ in $H$, each honored by its own honor flag;
  - foreign views at $(\text{foreignAt}, \text{foreignFrom})$;
  - for a gated observe, its gate in the foreign group's frame.

  `foreignAt` folds the observes in $H(E)$ and those concurrent with $E$, seen through voided ones. `foreignFrom` folds every observe in $H$, with no liveness filter. The gate reads the foreign group at a version widened by concurrent live observes, with the horizon version as its `from`. Views anchored elsewhere make the same kinds of reads, relative to their own anchor.
- **A4. Deploys.** A schema deploy is never void. The schema version at a position is a fold with no liveness filter. The schema object's view at a version does not depend on a horizon. The rows a deploy deletes are fixed when it is appended.
- **A5. Monotone ref-advances.** Each ref-advance publishes a version at or above the versions its predecessors see.
- **A6. Bindings form a DAG.**

### 5.2 Lemmas about the walk

**Lemma 1 (new entries are strictly after the bound).** If $H(B) \subseteq H(\text{meet})$, every new entry is strictly after $B$. In particular, no new entry is concurrent with any position whose history lies in $H(B)$.

*Proof.* By A1, a backward path from a new entry $N$ reaches $H(\text{start})$, and its first entry there is some $c \in$ `fork.common`. So $H(B) \subseteq H(\text{meet}) \subseteq H(c) \subseteq H(\mathrm{preds}(N))$. If $H(P) \subseteq H(B)$, every head of $P$ is then a proper ancestor of $N$. $\square$

Both phases only replace the bound by a GLB with something, so the bound stays at or below the meet, and Lemma 1 holds throughout.

**Lemma 2 (the exit border).** Walking backward from `end` and refusing to enter `stopSet` visits exactly $H(\text{end}) \setminus H(\text{bound})$. When the bound drops, requeueing the stopped entries that left the border restores this property for the new bound.

*Proof.* A backward path from `end` enters $H(\text{bound})$ either at a head of `end`, which is then a head of $H(\text{bound})$, or by a step from an entry outside to a predecessor inside, which then has a direct successor outside. Both cases are on the border. Conversely, no entry outside $H(\text{bound})$ has a successor inside it, since histories are downward-closed, so stopping at the border hides nothing that is outside.

When the bound drops from $B$ to $B' \le B$, take a stopped entry $s$. If $s$ is still in $H(B')$, it is still a head of `end` or still has a direct successor outside $H(B)$, hence outside $H(B')$, so it is on the new border. If $s$ left $H(B')$, it is requeued, and the walk continues below it until the new border. Entries that were walked are outside $H(B) \supseteq H(B')$ and stay outside, so nothing is walked twice. $\square$

**Lemma 3 (`inStart` is exact).** When an entry is processed, it is in `inStart` if and only if it is in $H(\text{start})$.

*Proof.* Seeds are in $H(\text{start})$, and only predecessors of members are added, so nothing outside $H(\text{start})$ is ever added. Conversely, take an entry $v \in H(\text{start})$ when it is processed. It entered the queue either as a head of `end` or as a predecessor of an entry already processed.

- *A head of `end`.* $v$ is maximal in $H(\text{end})$, hence a head of `start`, and it was seeded.
- *A predecessor of a processed entry $u$, with $u \in H(\text{start})$.* By induction $u$ was a member when it was processed, and it added $v$.
- *A predecessor of a new entry $u$.* Then $v \in$ `fork.common`, which was seeded.

This holds whatever order the queue pops entries in. $\square$

**Lemma 4 (the honor gate).** If no schema deploy is new, every entry of $H(\text{start})$ has the same honor flags at `start` and at `end`. `deployCoverMoved` is true exactly when some schema deploy is new.

*Proof.* The honor flag of $T$ comes from the schema at $(\{T\}, H)$: the deploys in $H(T)$ plus the deploys of $H$ concurrent with $T$. With no new deploy, $H(\text{start})$ and $H(\text{end})$ hold the same deploys, so the fold is the same. Now compare the cover of schema deploys (the maximal ones) at each horizon. If a new deploy exists, some maximal deploy of $H(\text{end})$ is at or above it, hence new, hence not in the cover at `start`. If none exists, the two covers are equal. $\square$

The gate cannot be the schema version at the two horizons instead. Say `start` already reached version $S_1$ through a deploy causally after $T$, which does not revise $T$, and a new deploy concurrent with $T$ also publishes $S_1$. The horizon versions are both $S_1$, while $T$'s schema moves from $S_0$ to $S_1$.

**Lemma 5 (Phase A leaves no unstable observe inside the bound).** After Phase A, no observe of a bound group $F$ in $H(\text{bound})$ is unstable. Phase B keeps it that way.

*Proof.* Let $u \in H(\text{meet})$ be an unstable observe of $F$. Some maximal observe of $F$ in $H(\text{meet})$ is at or above $u$, and by A5 its version is at or above $u$'s, so it is unstable too. The descent starts there, and at each step it moves to the maximal observes below the current one's predecessors that are still unstable. The one at or above $u$ is among them, by the same argument. So the descent reaches some lowest unstable observe $r$ with $r$ at or below $u$. Lowering the bound to $\mathrm{GLB}(\text{bound}, \mathrm{preds}(r))$ excludes $u$, because $u$ is not a proper ancestor of $r$. Phase B only lowers the bound, so it never brings an observe back inside. $\square$

**Lemma 6 (barriers next to the final bound kept their effect).** Let $B$ be the final bound and $U = H(B)$. Every barrier $T$ outside $U$ that is not strictly after $B$ has the same effect at `start` and at `end`.

*Proof.* By Lemma 1, $T \in H(\text{start})$. By Lemma 2 it was walked, and by Lemma 3 `canWiden` held for it (or the bound was already empty, and then $U$ is empty). Suppose $T$ triggered. If it was not strictly after the bound of the moment, the bound was lowered to a GLB with $\mathrm{preds}(T)$, after which $T$ was strictly after it. Being strictly after a position is preserved when the position drops: $H(B') \subseteq H(B) \subseteq H(\mathrm{preds}(T))$. So $T$ is strictly after the final $B$, a contradiction. Hence $T$ did not trigger. So:

- its verdict is the same at both horizons;
- if it observes a bound group, it is stable;
- if it deletes rows, its honor flags agree, either by the explicit check or, when `deployCoverMoved` is false, by Lemma 4.

That is its whole effect. $\square$

### 5.3 Agreement below the bound

**Theorem 1.** Let $B$ be the final bound and $U = H(B)$. Then:

- (a) every entry of $U$ has the same verdict at `start` and at `end`;
- (b) for every position $P$ with $H(P) \subseteq U$, every read of a view anchored at $P$ gives the same result at both horizons;
- (c) every row whose operations all lie in $U$ has the same state in the views at `start` and at `end`.

The operations of a row include the schema deploys that delete it.

*Proof.* Look at what the verdicts of $U$, and the reads of views anchored inside $U$, consume (A3):

- **Entries of $U$**, and their verdicts.
- **The schema and parameters at positions inside $U$.** These are the same at both horizons: by Lemma 1 no new deploy is concurrent with such a position, and by A4 the fold has no liveness filter.
- **Barriers concurrent with positions inside $U$.** A barrier concurrent with a position $P$ inside $U$ is not strictly after $P$, and so not strictly after $B$. It is either in $U$ or, by Lemma 6, outside $U$ with an unchanged effect. By Lemma 1 it is never new.
- **Foreign views.** Consider a view of $F$ anchored inside $U$. Its `foreignAt` folds observes that are either in $U$, and stable by Lemma 5, or outside $U$, not strictly after $B$, and so stable with an unchanged verdict by Lemma 6. Their verdicts are the same at both horizons (they are inside $U$, or covered by Lemma 6). So `foreignAt` is the same at both horizons and lies inside $H(b_F)$. Its `foreignFrom` is `refAtStart` at `start` and `refAtEnd` at `end`, the pair $b_F$ was computed for. By A6 we may assume the theorem for $F$: part (b) for $F$ says the view of $F$ anchored at `foreignAt` agrees at the two horizons. The observe gate reads $F$ the same way: its version is a widening by concurrent live observes, which are stable with unchanged verdicts, and its horizon version is again `refAtStart` or `refAtEnd`.

Every input the computation takes from outside $U$ is therefore the same at both horizons. By A2 the computation is deterministic, including where verdicts depend on each other, so its outputs agree. That gives (a) and (b).

For (c), the view of a row at horizon $H$ reads the row's operations, all in $U$ with the same verdicts by (a). It also reads deletes concurrent with the horizon itself, and there are none: nothing in $H$ is concurrent with $H$. Values are compared within the incarnations of the `end` horizon, as the row channel always does. Table- and column-wide schema changes travel in the schema channel. $\square$

**Theorem 2.** The bounded delta equals the full delta.

*Proof.* The schema channel is computed the same way in both. By Lemma 2, the walked entries are exactly $H(\text{end}) \setminus U$. Each is checked for a verdict flip, and the entries of $U$ do not flip, by Theorem 1(a). Every row touched by a walked entry is diffed. Every other row has all its operations in $U$ and does not change, by Theorem 1(c). $\square$

### 5.4 The walk ends, and its bound does not depend on the walk order

The bound only descends, and the DAG is finite. Each entry is loaded at most once (Lemma 2), so the walk ends.

Whether a barrier of $H(\text{start})$ triggers depends only on `start`, `end` and the foreign bounds, not on the current bound. Call a position $C$ at or below the Phase A bound **closed** when every triggering barrier outside $H(C)$ is strictly after $C$. The final bound is closed. The walk meets every barrier outside the final bound's history (Lemma 2), and the proof of Lemma 6 shows that each triggering one ends up strictly after it. It is also the greatest closed position, so it does not depend on the order in which the queue pops entries.

To see this, take any closed $C$, and suppose $H(C) \subseteq H(B)$ for the bound $B$ of the moment, as holds for the Phase A bound. When a triggering barrier $T$ lowers $B$, $T$ was walked, so it lies outside $H(B)$ and hence outside $H(C)$. Because $C$ is closed, $T$ is strictly after $C$, so $H(C) \subseteq H(\mathrm{preds}(T))$. Then $H(C) \subseteq H(B) \cap H(\mathrm{preds}(T)) = H(\mathrm{GLB}(B, \mathrm{preds}(T)))$, the new bound. So every closed position stays at or below the bound, and the final bound, being closed itself, is the greatest of them. Replicas computing the same delta agree on its bound.

### 5.5 What the bound does not promise

The bound is sound. It is not claimed to be the greatest sound bound. Some triggers are conservative:

- a bundle is checked on every table it touches, not only the ones it deletes from;
- an unstable observe lowers the bound even if no entry below it reads that binding;
- a delete lowers it even if no entry below reads its row.

Restricting each trigger to the entries that actually read the barrier would need the schema at each entry, and has not been needed so far.

### 5.6 A history where the foreign bound must use `start`

This history shows why Phase A takes $F$'s bound between `refAtStart` and `refAtEnd`.

**The foreign group $F$.**

- Deleting a target requires an `ok` perm row, and deleting a perm row requires a gate row.
- `T_F` deletes the target `x`, on a side branch.
- `E_F` inserts an item pointing at `x`. `s_1` follows it.
- `N_1` deletes the perm row and `N_3` deletes the gate row. Both follow `s_1`, concurrent with each other and with `T_F`.

Take three versions of $F$:

- At $v_1 = \{s_1, T_F\}$, `T_F` is live, so `E_F` is void.
- At $v_2 = \{N_1, T_F\}$, `N_1` removes the `ok` row under `T_F`, so `T_F` is void and `E_F` is live.
- At $v_3 = \{N_1, N_3, T_F\}$, `N_3` removes the gate row under `N_1`, so `N_1` is void, `T_F` is live again, and `E_F` is void again.

$F$'s bound from $v_1$ to $v_3$ is $\{s_1\}$, because `T_F` has the same verdict at both. It is sound for that pair and for no other.

**The observer $G$.**

```text
O1 ── E_G ── O_m ── s1G ── O3        O1  observes {s1}
        \                            E_G references the item E_F inserted
         O2                          O_m observes v1    O2 observes v2
                                     O3  observes v3
```

$G$ goes from `start` $= \{s1G, O_2\}$ to `end` $= \{O_3, O_2\}$. The meet is $\{s1G\}$, and $F$ is observed at $v_1$ there, at $v_2$ at `start`, and at $v_3$ at `end`. `E_G` reads $F$ at $(\{s_1\}, v_2)$ at `start`, where the item is live, and at $(\{s_1\}, v_3)$ at `end`, where it is void. So `E_G` flips.

With $F$'s bound taken from $v_1$, it is $\{s_1\}$:

- `O1` is stable and `O_m` is unstable, so Phase A sets the bound to $\{E_G\}$.
- `O2` and `O_m` are unstable, but both are strictly after $\{E_G\}$, so Phase B never lowers the bound.
- `E_G` is missed.

With $F$'s bound taken from $v_2$:

- `T_F` flips between $v_2$ and $v_3$, so $F$'s bound is $\{x\}$.
- `O1` is unstable, and the bound drops below it.
- `E_G` is walked.

The meet-based bound described in Section 2 also took the foreign bound from the meet. It reported `E_G` only because its heads-only stop happened to walk into `E_G` through `O2`.

---

## 6. Implementation notes

### 6.1 Where it lives

- **`mvt/src/revision_bound.ts`.** The walk of Section 4 is `walkRevisionBound`, which knows the geometry, the `barrier` tag and references, and nothing about verdicts. Phase A's descent is `lowerBelowUnstableRefAdvances`. The helpers are `computeStopSet`, `isStrictlyAfter`, `computeGLB` and `isUnstableRefAdvance`. `requireExtendingFork` computes the fork the walk takes. Its doc comment states, in terms of views and barriers, what a host and its visitor must guarantee. Those guarantees are the assumptions of Section 5.1 and the effect of Section 3.
- **`rtable_group/delta.ts`.**
  - `computeRTableGroupDelta(group, rawDag, strategy, start, end)` computes the delta. Under `'bounded'` it runs the walk with the delta visitor. That visitor ingests every walked entry, records every verdict flip, and reports a candidate as changed when its verdict or its honor flag (`didDeleteHonorFlip`) flipped. The flips become records after the walk.
  - `computeRTableGroupRevisionBound(group, rawDag, start, end)` runs the same walk with the bound-only visitor, which reads verdicts and honor flags only for candidates.
  - `revisionBoundHost` declares the group's references. Each bound group is `'horizon'`, read at the version observed at the horizon. The schema is `'at'`, read at $(v, v)$, and so never bounded.
- **`GroupDeltaHost`** is what the computation needs from the group. It adds `resolveSchemaView`, `resolveObservedForeignVersion` and `getBoundGroup` to what the accumulator already used. `getBoundGroup` returns a `BoundGroupHost`, an alias of mvt's `ReferencedObject` (`computeRevisionBound`, `getCausalDag`), and throws if the bound group is missing from the replica.
- **`RTableGroupImpl.computeRevisionBound(start, end)`** is the public bound-only entry point. Observers call it to get $b_F$. It ignores the group's delta strategy.
- **`op_delta.ts`**: `describeOpVerdictFlip` builds a flip record. The full and bounded paths share it, so they report identical records.

### 6.2 Details worth keeping

- **The full strategy stays independent.** `'full'` still walks the whole history with `walkEntriesBackwardsToBound` and computes flips with `computeOpVerdictFlips`. The parity tests compare the bounded delta against it, and a reference that shared the new walk would share its mistakes.
- **Read the foreign versions the views read.** Phase A gets `refAtStart` and `refAtEnd` from `resolveObservedForeignVersion`, the same canonical fold that `resolveForeignTableViewIn` uses for `foreignFrom` and that the observe gate uses for its horizon. A raw `resolveRefVersionAtPosition` can return a version with dominated hashes. The history would be the same, but the memo key would not.
- **Deletes can live in the meta.** A deploy that adopts an FK has a plain ref-advance payload. The rows it deletes are tagged in its meta: `tables`, plus one `t-<table>-rows` tag per row. `didDeleteHonorFlip` therefore takes the tables from `entry.meta['tables']`. Reading row deletes from the payload would miss these deploys, which are never void, so the honor flip is the only trigger that can catch them.
- **Bound-only mode evaluates little.** It skips the row channel and `explainEntryVoided`, and reads verdicts only for old barriers that can still lower the bound.
- **The memo.** `computeRevisionBound` canonicalizes both versions, keys a 256-entry LRU on their sorted hashes, and clears it in `destroy()`. Keeping it across calls is safe: hashes are content addresses, so a version names a fixed history. The bound between two fixed versions depends only on those histories and on the schema and foreign histories they pin. Appending entries never changes it.
- **Stopped entries are never visited.** An entry is stopped before it is marked visited, and walked entries never re-enter the bound's history. So the resume step just requeues the stopped entries that left the border.
- **Cost.** Each lowering recomputes the exit border with `findForkPosition(bound, end)`, which walks the DAG index over roughly the interval. Each triggering barrier also costs one `findForkPosition` for the strictly-after test. Entries are loaded and evaluated at most once.

### 6.3 What we learned

- **An over-walk can hide an unsound bound.** `walkEntriesBackwardsToBound` stops only at the bound's heads. When a side branch attaches below a head, it walks the whole past of the attachment point. That reached the affected entry in some histories (Section 5.6) and made the meet-based bound look correct there. The group now stops at the exit border, which is cheaper and hides nothing. `RSet` and `RCap` still use the shared walk with the meet as their bound. Tightening that walk should come together with a bound of this kind for them, so that the over-walk is not the only thing keeping them correct.
- **Random histories rarely build these shapes.** A failure needs an old barrier on a side branch, concurrent with an entry below the meet, whose effect a new entry changes. Generators with gated deletes, honor toggles, FK adoption and side-branch observes found no failure of the meet-based bound in about 6,300 pairs. A shaped generator found 4 failures in the smoke profile and 21 in the extended one. It builds the skeleton of Section 2 and randomizes which barriers sit on it, plus noise entries. A generator should be shown to catch a known bug before it is trusted to guard against it.
- **Check one quantity two ways.** Bound-only mode skips work the full walk does. Every parity pair checks that `computeRevisionBound` returns the delta's own bound.
- **Mutations confirm the tests bite.** Taking the foreign bound from the meet's version makes `[DBOUND04]` fail. Reading deletes only from payloads makes `[DBOUND05]` fail.

### 6.4 Tests

- **`test/delta_bound_tests.ts`** has the targeted histories. Each one checks the bounded delta against the full one and the bound-only bound against the delta's.
  - `[DBOUND01]`: the history of Section 2.
  - `[DBOUND02]`: the same shape where `N` changes nothing. The bound stays at the meet.
  - `[DBOUND03]`: the live side-branch observe.
  - `[DBOUND04]`: the history of Section 5.6.
  - `[DBOUND05]`: an FK-adopting deploy on a side branch whose honor flag a new deploy turns off.
  - `[DBOUND06]`: a side-branch schema deploy with no new deploy. The bound stays at the meet.
- **`test/delta_parity/`** has the full-against-bounded sweeps.
  - `[DELTA_PARITY_GROUP]` and `[DELTA_PARITY_XGROUP]`: random single-group and cross-group histories.
  - `[DELTA_PARITY_GATED]` and `[DELTA_PARITY_GATED_XGROUP]`: gated deletes, honor toggles, FK adoption, and observes of older foreign versions.
  - `[DELTA_PARITY_SHAPED]`: the counterexample skeleton with randomized barriers.

  `npm test` runs them with the smoke profile, and `npm run test:parity` runs them with the extended one.
