# Single-Pass Incremental-Widening Delta Bound

## 1. Overview

`BOUND_SIMPLIFIED_SUMMARY.md` showed why the geometric fork meet (`computeForkMeet(rawDag, fork.common)`) is not a sound delta bound on its own: an entry $T \in H(\text{start}) \setminus H(\text{meet})$ on a side branch can be concurrent with both a new entry $N \in H(\text{end}) \setminus H(\text{start})$ and an older entry $E \in H(\text{meet})$, transmitting a change from $N$ through $T$ into $H(\text{meet})$.

That note proposed two ways to choose the bound before running the delta walk:
1. **Shape-only bound:** pull the bound below *every* barrier not strictly after it, regardless of whether that barrier's effect changed.
2. **Concurrency-closure bound:** pull the bound below every barrier reachable from $H(\text{end}) \setminus H(\text{start})$ or an unstable observe via upward causal links and barrier-to-barrier concurrency.

Both approaches treat bound computation as a **separate pre-pass** that cannot inspect verdicts. Because of that restriction, the shape-only rule pulls the bound down on any old side-branch barrier, and the concurrency-closure rule falls back to the shape-only bound as soon as *any* new barrier appears in $H(\text{end}) \setminus H(\text{start})$.

However, in `RTableGroupDelta`, **we are already walking $H(\text{end}) \setminus H(\text{bound})$ backwards, accumulating candidate row IDs into sets, and evaluating `voidBefore` and `voidAfter` on every walked entry for `opVerdictChanges`.**

Because both the row accumulator (`RTableDeltaAccumulator`) and the op-verdict channel (`computeOpVerdictFlips`) are purely additive over the walked entries, we can **compute the bound and accumulate the delta in a single backward walk**, widening the bound on the fly **only** when a walked barrier actually changed its effect between `start` and `end` (or publishes an unstable foreign version).

---

## 2. Key Structural Invariants

Let `start` and `end` be two versions with $H(\text{start}) \subseteq H(\text{end})$, let `meet` be the greatest lower bound of `fork.common` (from `fork = findForkPosition(start, end)`), and let `bound` be any position at or below `meet` ($H(\text{bound}) \subseteq H(\text{meet})$).

### Lemma 1: Every new entry is strictly after `bound`
In a group DAG, the group's `create` entry is the unique root (the only entry with no predecessors), and it lies in $H(\text{start})$ whenever `start` is non-empty (when `start` is empty, `fork.common` is empty, `computeForkMeet` returns the empty position `version()`, and the walk is a full walk from `end`). Thus for non-empty `start`, every path backward from a new entry $N \in H(\text{end}) \setminus H(\text{start})$ reaches $H(\text{start})$ and enters it through some attachment point $c \in \text{fork.common}$. By definition of `meet`,
$$H(\text{bound}) \subseteq H(\text{meet}) \subseteq H(c) \subsetneq H(N).$$
Therefore:
1. **Every head of `bound` is a proper ancestor of every $N \in H(\text{end}) \setminus H(\text{start})$.**
2. **No new entry $N \in H(\text{end}) \setminus H(\text{start})$ is ever concurrent with any entry $E \in H(\text{bound})$.**
3. **Every entry in $H(\text{end}) \setminus H(\text{bound})$ that fails to be strictly after `bound` must lie in $H(\text{start}) \setminus H(\text{bound})$.**

### Lemma 2: The exit border of $H(\text{bound})$ stops every path from `end` into $H(\text{bound})$
Checking `bound.has(hash)` only tests whether `hash` is a **head** of `bound`. A side branch in $H(\text{end}) \setminus H(\text{bound})$ (such as $T$ attached at $x \in H(\text{bound})$ below the head $s_1 \in \text{bound}$) enters $H(\text{bound})$ at $x \notin \text{bound}$; testing only `bound.has(hash)` would miss $x$ and walk the entire past of $x$.

*(Scope note: `walkEntriesBackwardsToBound` in `modules/mvt/src/delta.ts` has that `bound.has(hash)` behavior today, and is shared by `RSet`, `RCap`, and `RSchema`. Because `RSet` and `RCap` still use the geometric meet as their bound, tightening their walk before they adopt barrier widening could expose latent meet bugs that their current over-walk masks. We therefore keep the stop-set walk inside `rtable_group/delta.ts` for this change.)*

Because $H(\text{bound}) \subseteq H(\text{end})$, let $\text{boundFork} = \text{findForkPosition}(\text{bound}, \text{end})$ and define the **stop set** of `bound` relative to `end`:
$$\text{stopSet}(\text{bound}, \text{end}) = \text{boundFork.common} \cup \text{boundFork.commonFrontier} \cup \text{bound}.$$
Then:
1. **Every entry in $\text{stopSet}(\text{bound}, \text{end})$ lies in $H(\text{bound})$.**
2. **Every backward path from `end` into $H(\text{bound})$ hits $\text{stopSet}(\text{bound}, \text{end})$ at its first node in $H(\text{bound})$:**
   - If the path starts at a head $h_0 \in \text{end} \cap H(\text{bound})$, then $h_0$ is maximal in $H(\text{bound})$, so $h_0 \in \text{boundFork.commonFrontier} \cup \text{bound} \subseteq \text{stopSet}(\text{bound}, \text{end})$.
   - If the path steps from $u \in H(\text{end}) \setminus H(\text{bound})$ to a predecessor $v \in H(\text{bound})$, then $v \in H(\text{bound}) \cap H(\text{end})$ has a direct successor $u$ only in $H(\text{end})$, so $v \in \text{boundFork.common} \subseteq \text{stopSet}(\text{bound}, \text{end})$.
3. **No entry $u \in H(\text{end}) \setminus H(\text{bound})$ has a causal successor in $H(\text{bound})$** (since $H(\text{bound})$ is downward-closed: $u \preceq w \in H(\text{bound})$ would imply $u \in H(\text{bound})$). Thus no backward path from `end` to $u$ hits $\text{stopSet}(\text{bound}, \text{end})$ before reaching $u$, and stopping at $\text{stopSet}(\text{bound}, \text{end})$ visits **exclusively and exhaustively** $H(\text{end}) \setminus H(\text{bound})$.
4. **Resuming across a bound lowering:** Suppose the walk halts on entries $s \in \text{stoppedAtBound} \subseteq \text{stopSet}(\text{bound}_{\text{old}}, \text{end})$ and `bound` is lowered to $\text{bound}_{\text{new}} \le \text{bound}_{\text{old}}$.
   - If $s \in H(\text{bound}_{\text{new}})$, then $s$ still either lies in $\text{end}$ or has a direct successor in $H(\text{end}) \setminus H(\text{bound}_{\text{old}}) \subseteq H(\text{end}) \setminus H(\text{bound}_{\text{new}})$, so **$s \in \text{stopSet}(\text{bound}_{\text{new}}, \text{end})$ automatically** and stays stopped.
   - If $s \in H(\text{bound}_{\text{old}}) \setminus H(\text{bound}_{\text{new}})$, then **$s \notin \text{stopSet}(\text{bound}_{\text{new}}, \text{end})$**; moving $s$ from `stoppedAtBound` back to `queue` walks $H(s) \setminus H(\text{bound}_{\text{new}})$ and stops at $\text{stopSet}(\text{bound}_{\text{new}}, \text{end})$.

### Lemma 3: Exact tracking of $H(\text{start})$ membership during the walk
Note that `fork.forkB` in `findForkPosition(start, end)` is only the **lower border** of $H(\text{end}) \setminus H(\text{start})$ (new entries that have a direct predecessor in `fork.common` or no predecessors at all), **not** the entire set of new entries.

However, by the same border property as Lemma 2, every backward path from `end` into $H(\text{start})$ enters $H(\text{start})$ at $\text{start} \cup \text{fork.commonFrontier} \cup \text{fork.common}$, and once in $H(\text{start})$, every predecessor also lies in $H(\text{start})$. Thus seeding:
$$\text{inStart} = \text{start} \cup \text{fork.commonFrontier} \cup \text{fork.common}$$
and propagating `if (inStart.has(hash)) inStart.add(prevHash)` when enqueuing predecessors identifies whether any popped `hash` belongs to $H(\text{start})$ in $O(1)$ time with zero additional DAG queries.

---

## 3. Exact Widening Trigger Conditions

Let $U = H(\text{bound})$ with `bound` $\le$ `meet`. Consider any entry $E \in U$ (or any row whose operations all lie in $U$). Why could $E$'s verdict or row state differ between `start` and `end`?

By inspecting every cross-branch query (`findConcurrentCoverWithFilter`) in `RTableGroup` and `RTableViewImpl`:

1. **Causal past $H(E)$:**
   Since $E \in U$ and $U$ is downward-closed, $H(E) \subseteq U$. Every non-barrier read by $E$ lies in $H(E)$ and has the same verdict at `start` and `end` by induction on $U$.

2. **Local schema and group parameters at $(at = \{E\}, from = H)$:**
   `resolveSchemaView({E}, H)` and `resolveEffectiveParams({E}, H)` read schema deploys in $H(E)$ plus schema deploys in $H$ concurrent with $E$. By Lemma 1, no new entry $N \in H(\text{end}) \setminus H(\text{start})$ is concurrent with $E \in U$. Furthermore, schema deploys are never voided at-use (`diagnoseObserve` in `group.ts:1219` returns `{ status: 'live' }` unconditionally for `refId === this.getSchemaRef()`, and `resolveSchemaVersion` in `group.ts:406` resolves geometrically without an `isLive` filter), and `RSchema` itself has no barriers (`rschema/delta.ts:1-7`). Thus `resolveSchemaView({E}, start)` and `resolveEffectiveParams({E}, start)` are **always identical** to their values at `end` for every $E \in U$.
   *(Consequence: the group's `RSchema` must **not** be included in Phase A projection or in `foreignBounds`; otherwise a side-branch schema deploy in $H(\text{start}) \setminus H(\text{meet})$ would be treated as an unstable observe and pull the bound down unnecessarily.)*

3. **Local concurrent deletes (`killedByConcurrentDelete` in `rtable/view.ts:281`):**
   When $E \in U$ (or a row write $W \in U$) checks for concurrent barrier deletes at horizon $H \in \{\text{start}, \text{end}\}$, Lemma 1 guarantees that no new entry $N \in H(\text{end}) \setminus H(\text{start})$ is concurrent with $W$. Thus every candidate concurrent delete $T$ lies in $H(\text{start})$.
   Note that `killedByConcurrentDelete` matches on `barrier: ['t']` and `t-<table>-rows`, which includes both row/bundle deletes **and schema deploys that delete stranded rows when adopting an FK** (`deriveDeployKillMeta` in `scopes.ts:111-119`).
   If $T \notin U$, then $T \in H(\text{start}) \setminus U$, and $T$ kills $W$'s target row on `table` at horizon $H$ iff:
   $$\text{activeKill}(T, \text{table}, H) \iff \neg\text{isEntryVoided}(T, H) \;\land\; \text{concurrentDeletes}(\text{table}, \{T\}, H).$$
   Therefore, a row-deleting barrier $T \in H(\text{start}) \setminus U$ can change the state of $U$ between `start` and `end` **if and only if**:
   - **$T$'s verdict flipped:** $\text{isEntryVoided}(T, \text{start}) \neq \text{isEntryVoided}(T, \text{end})$, **OR**
   - **$T$'s `concurrentDeletes` honor flag flipped:** a new schema deploy in $H(\text{end}) \setminus H(\text{start})$ concurrent with $T$ changed `resolveSchemaView({T}, H).getConcurrentDeletes(table)` between $H = \text{start}$ and $H = \text{end}$ for a table $T$ deletes from (identified via `entry.meta['tables']`).

4. **Foreign-group observations (`resolveForeignTableViewIn` in `rtable_group/group.ts:609`):**
   When $E \in U$ resolves a bound foreign group $F$ at horizon $H \in \{\text{start}, \text{end}\}$:
   - `foreignAt(E, H) = resolveRefVersionAtPosition(dag, F, {E}, H, isLive(H))` folds observes in $H(E)$ plus observes in $H$ concurrent with $E$. By Lemma 1, all such observes lie in $H(\text{start})$.
   - `foreignFrom(H) = resolveRefVersionAtPosition(dag, F, H, H)` folds all observes in $H$ (which **can** include new observes $N \in H(\text{end}) \setminus H(\text{start})$ strictly after $E$).

   Notice which pair of foreign horizons `foreignFrom(H)` actually takes:
   $$\text{refAtStart} = \text{resolveRefVersion}(F, \text{start}, \text{start}), \qquad \text{refAtEnd} = \text{resolveRefVersion}(F, \text{end}, \text{end}).$$
   Because a change-driven bound on $F$ only guarantees that verdicts and row states inside $H(b_F)$ agree between the **exact two versions it compared** (a barrier in $F$ could be live at `refAtMeet`, void at `refAtStart`, and live again at `refAtEnd`), $b_F$ **must be computed between `refAtStart` and `refAtEnd`** (not `refAtMeet` and `refAtEnd`), recursively using this same single-pass widening bound procedure on $F$.
   So long as every observe $O$ contributing to `foreignAt(E, H)` has the same `isLive` verdict at `start` and `end` **and** publishes a version $\text{refVersion}(O) \le b_F$, we have $\text{foreignAt}(E, \text{start}) = \text{foreignAt}(E, \text{end}) \le b_F$. By the soundness of $b_F$ on $F$ between `refAtStart` and `refAtEnd`, advancing `foreignFrom` from `refAtStart` to `refAtEnd` cannot change any verdict or row state in $F$ at or below $b_F$.
   Conversely, an observe $T \in H(\text{start}) \setminus U$ can change $E$'s foreign view **if and only if**:
   - **$T$'s verdict flipped:** $\text{isEntryVoided}(T, \text{start}) \neq \text{isEntryVoided}(T, \text{end})$, **OR**
   - **$T$ is unstable:** $\text{refVersion}(T) \not\le b_F$ (`!refVersionAtOrBelow(foreignDag, refVersion(T), b_F)`).

5. **Observe gates (`diagnoseObserve` in `rtable_group/group.ts:1215`):**
   A gated observe $O \in U$ evaluates its binding's `canObserve` gate in $F$'s frame at `(refAt, refFrom)`:
   - `refAt = resolveObserveGateRefAt(O, H)` (`group.ts:699`) is $O$'s own published version, widened by the concurrent observes of $F$ in $H$ that are live at $H$ and publish a version strictly above it. By Lemma 1 every such observe lies in $H(\text{start})$. One inside $U$ is stable and has the same verdict at both horizons. One in $H(\text{start}) \setminus U$ that flips its verdict or is unstable is already a trigger under item 4. It is concurrent with $O \in U$, so it is not strictly after `bound`, and it lowers `bound` below $O$.
   - `refFrom = resolveRefVersion(F, H, H)` (`group.ts:1227`) takes the same pair `refAtStart`, `refAtEnd` as `foreignFrom`, so the soundness of $b_F$ covers it.

   The gate read therefore needs no trigger beyond those of item 4.

### Summary: When is a walked barrier $T$ a **widening trigger**?

A barrier $T$ visited during the backward walk is an **active widening trigger** iff:
1. $T \in H(\text{start})$ (`inStart.has(T.hash)`) and $T$ is a barrier (`entry.meta['barrier'] !== undefined`), **and**
2. At least one of the following holds:
   - **Verdict flip:** `voidBefore !== voidAfter` (already computed for `opVerdictChanges`),
   - **Unstable observe:** $T$ is a `ref-advance` for a bound foreign group $F$ and `!refVersionAtOrBelow(dag_F, extractRefVersion(T.payload), b_F)`,
   - **Concurrent-delete honor flip:** $T$ deletes rows of `table` (via row delete, bundle delete, or FK-adopting schema deploy, all tagged in `entry.meta['tables']`) and `concurrentDeletes(table, {T}, start) !== concurrentDeletes(table, {T}, end)`. Note that this cannot be gated on `resolveSchemaVersion(start, start) !== resolveSchemaVersion(end, end)`: if `start` already reached schema $S_1$ via a deploy $D_0$ causally *after* $T$ (which does not revise $T$), and a new deploy $N \in H(\text{end}) \setminus H(\text{start})$ *concurrent* with $T$ also deploys $S_1$, the horizon schema version stays $\{S_1\}$ while $T$'s schema at $(\{T\}, H)$ moves from $S_0$ to $S_1$. Instead, we gate it on whether the group DAG's schema-deploy cover moved (`deployCoverMoved = !sameSet(await findRefAdvances(scopedDag, schemaRef, start), await findRefAdvances(scopedDag, schemaRef, end))`).

If $T$ is an active widening trigger **and** $T$ is not strictly after `bound`, then `bound` must be lowered to:
$$\text{bound} \leftarrow \text{GLB}(\text{bound}, \text{preds}(T)),$$
and `stopSet` updated to $\text{stopSet}(\text{bound}, \text{end})$.

---

## 4. Why Incremental Widening Does Zero Redundant Entry Evaluations

1. **Stopping at $\text{stopSet}(\text{bound}, \text{end})$ skips $H(\text{bound})$ on every branch:**
   By Lemma 2, when a side branch $T \in H(\text{start}) \setminus H(\text{bound})$ does not trigger widening, its predecessor $x \in H(\text{bound})$ is in `boundFork.common` $\subseteq$ `stopSet`, so the walk stops immediately at $x$ without loading $x$ or its ancestors.
2. **`RTableDeltaAccumulator` is a candidate-set collector:**
   In `modules/rdb/src/rtable/delta.ts:78`, `ingest(entry)` only inserts touched `rowId`s into `this.candidates: Set<B64Hash>`. The actual `viewStart.deltaRowState(rowId)` and `viewEnd.deltaRowState(rowId)` calls happen once in `finalize()` after the walk completes. Adding more walked entries when `bound` drops simply adds more candidate `rowId`s to `this.candidates`.
3. **`opVerdictChanges` is per-entry and horizon-fixed:**
   `voidBefore = isEntryVoided(entry.hash, start)` and `voidAfter = isEntryVoided(entry.hash, end)` depend only on `start` and `end`, never on `bound`. An entry evaluated before `bound` drops never changes its `voidBefore` or `voidAfter`.
4. **"Strictly after `bound`" is monotone as `bound` descends:**
   If a barrier $Y$ was already checked against $\text{bound}_{\text{old}}$ and passed ($H(\text{bound}_{\text{old}}) \subseteq H(\text{preds}(Y))$), then for any lowered $\text{bound}_{\text{new}} \le \text{bound}_{\text{old}}$,
   $$H(\text{bound}_{\text{new}}) \subseteq H(\text{bound}_{\text{old}}) \subseteq H(\text{preds}(Y)),$$
   so $Y$ still passes against $\text{bound}_{\text{new}}$. And if $Y$ was already lowered into $\text{bound}$ ($\text{bound} \le \text{preds}(Y)$), $Y$ is now strictly after the new `bound`.
   **Therefore, no previously visited entry ever needs to be re-loaded or have its verdict re-evaluated when `bound` drops.**
   *(Cost note: each lowering does call `findForkPosition(bound, end)` to refresh `stopSet`, which walks the DAG index over the interval; with $k$ lowerings that is $k$ index walks, with zero redundant entry loads or verdict evaluations.)*

---

## 5. Step-by-Step Algorithm

### Phase A: Initial Bound from `meet` and Bound Foreign Groups

1. Compute `fork = await rawDag.findForkPosition(start, end)`.
   - If `fork.forkB.size === 0`, return empty delta with `revisionBound = fork.commonFrontier`.
2. Compute `meet = await computeForkMeet(rawDag, fork.common)`.
3. For each bound foreign group $F$ (for `groupId` in `new Set(Object.values(group.getBindings()))`—excluding `RSchema`, see §3.2):
   - Compute `refAtStart = await group.resolveObservedForeignVersion(groupId, start, start)`.
   - Compute `refAtEnd = await group.resolveObservedForeignVersion(groupId, end, end)`.
   - `resolveObservedForeignVersion` (`group.ts:754`) is the canonical, geometric fold the verdicts read: `foreignFrom` in `resolveForeignTableViewIn` (`group.ts:621`) and `refFrom` in `diagnoseObserve` (`group.ts:1227`) compute the same version. Using it guarantees that $b_F$ is computed between exactly those versions, and it gives stable memo keys. Raw `resolveRefVersionAtPosition` can return a version that still contains dominated hashes.
   - Load $F$ with `const foreign = await group.getBoundGroup(groupId)` and compute `b_F = await foreign.computeRevisionBound(refAtStart, refAtEnd)`. This is the **bound-only entry point** of this same procedure (see "Entry Points, Host Interface and Memo" below). It skips the row channel (`acc.ingest`/`acc.finalize`) and `explainEntryVoided` on $F$, and is memoized on $F$'s instance. Bindings form a DAG, so this recursion is well-founded.
   - Record `foreignBounds.set(groupId, { dag: await foreign.getCausalDag(), bound: b_F })`.
4. **Project $b_F$ through observes in $H(\text{meet})$:**
   - Start with `bound = meet`.
   - For each bound foreign group $F$, descend from `findCoverWithFilter(meet, { containsValues: { ref: [F.id] } })` through unstable `ref-advance` entries ($r$ with $\text{refVersion}(r) \not\le b_F$).
   - Stop descending a branch at the first stable `ref-advance` (by monotonicity, everything below it is stable).
   - For each **lowest unstable** `ref-advance` $r \in H(\text{meet})$:
     - Let $\text{preds}(r) = \text{position}(...\text{prevEntryHashes}(r))$.
     - If $\text{preds}(r)$ is empty, `bound = version()` (empty bound $\Rightarrow$ full walk).
     - Otherwise, lower `bound = await computeGLB(rawDag, bound, preds(r))`.
     - *(Note: today's `projectForeignBound` in `modules/mvt/src/refs.ts:269` adds `r` itself to `bound`, which leaves `r` inside $U$ and unwalked. Stepping to `preds(r)` via `GLB` fixes this so `r` sits above `bound` and is walked.)*

### Phase B: Single-Pass Backward Walk with Incremental Widening

Initialize:
- `bound: Version` (from Phase A)
- `stopSet: Set<B64Hash> = await computeStopSet(rawDag, bound, end)`
- `inStart = new Set<B64Hash>([...start, ...fork.commonFrontier, ...fork.common])`
- `visited = new Set<B64Hash>()` (entries already popped and processed)
- `stoppedAtBound = new Set<B64Hash>()` (hashes where the backward BFS halted because `stopSet.has(hash)` was true)
- `queue: B64Hash[] = [...end]`
- `acc = boundOnly ? undefined : new RTableGroupDeltaAccumulator(group, start, end)` (`boundOnly` is `true` when computing $b_F$ for a foreign group, and `false` for the top-level delta)
- `opVerdictChanges: OpVerdictChange[] = []`
- `deployCoverMoved = !sameSet(await findRefAdvances(scopedDag, group.getSchemaRef(), start), await findRefAdvances(scopedDag, group.getSchemaRef(), end))` (true iff $H(\text{end}) \setminus H(\text{start})$ contains a schema deploy; only then can `resolveSchemaView({T}, H)` differ between `start` and `end` for $T \in H(\text{start})$).

```ts
while (queue.length > 0) {
    const hash = queue.shift()!;
    if (visited.has(hash)) continue;

    // If `hash` is on the exit border of H(bound), do not walk into H(bound);
    // record it in `stoppedAtBound` in case `bound` is lowered later.
    if (stopSet.has(hash)) {
        stoppedAtBound.add(hash);
        continue;
    }

    visited.add(hash);
    const entry = await rawDag.loadEntry(hash);
    if (entry === undefined) continue;

    const entryInStart = inStart.has(hash);
    const preds = json.fromSet(entry.header.prevEntryHashes);
    if (entryInStart) {
        for (const prevHash of preds) inStart.add(prevHash);
    }

    // 1. Row-channel candidate ingestion (skipped in bound-only mode)
    if (acc !== undefined) {
        await acc.ingest(entry);
    }

    // 2. Op-verdict evaluation
    // In full-delta mode, check every void-checkable entry and record flips with reasons.
    // In bound-only mode, `isEntryVoided` still runs on candidate widening barriers
    // (`bound.size > 0 && entryInStart && isBarrierEntry(entry)`), skipping `explainEntryVoided`
    // and skipping non-barriers / new entries that cannot widen `bound`.
    const canWiden = bound.size > 0 && entryInStart && isBarrierEntry(entry);
    let voidBefore = false;
    let voidAfter = false;
    if (isVoidCheckable(entry.payload) && (!boundOnly || canWiden)) {
        voidBefore = await group.isEntryVoided(entry.hash, start);
        voidAfter = await group.isEntryVoided(entry.hash, end);
        if (!boundOnly && voidBefore !== voidAfter) {
            const voidHorizon: OpVoidHorizon = voidAfter ? 'end' : 'start';
            const horizon = voidAfter ? end : start;
            const reason = await group.explainEntryVoided(entry.hash, horizon);
            opVerdictChanges.push({
                ...describeGroupEntry(entry, group, voidBefore, voidAfter),
                voidHorizon,
                ...(reason !== undefined ? { reason } : {}),
            });
        }
    }

    // 3. Widening check on barriers in H(start) \ H(bound)
    // By Lemma 1, entries in H(end) \ H(start) (!entryInStart) are always strictly
    // after `meet >= bound`, so only barriers with `entryInStart` can widen `bound`.
    if (canWiden) {
        const triggered =
            (voidBefore !== voidAfter) ||
            (await isUnstableObserve(entry, foreignBounds)) ||
            (deployCoverMoved && await didDeleteHonorFlip(group, entry, start, end));

        if (triggered) {
            const predPos = position(...preds);
            if (!(await isStrictlyAfter(rawDag, predPos, bound))) {
                // Lower `bound` to GLB(bound, preds(entry)) and refresh `stopSet`
                bound = predPos.size === 0
                    ? version()
                    : await computeGLB(rawDag, bound, predPos);
                stopSet = await computeStopSet(rawDag, bound, end);

                // Resume walking from any former stop-border entries no longer in H(bound)
                for (const stopped of [...stoppedAtBound]) {
                    if (!stopSet.has(stopped)) {
                        stoppedAtBound.delete(stopped);
                        if (!visited.has(stopped)) queue.push(stopped);
                    }
                }
            }
        }
    }

    // 4. Continue backward BFS
    for (const prevHash of preds) {
        if (!visited.has(prevHash)) {
            queue.push(prevHash);
        }
    }
}
```

### Phase C: Finalize
- If `boundOnly` is `true`, return `bound` directly. `computeRevisionBound` stores it in the instance memo (below).
- Otherwise:
  1. `const root = await acc!.finalize()` (diffs all candidate `rowId`s between `viewStart` and `viewEnd` and computes `schemaChanges`).
  2. `opVerdictChanges.sort((a, b) => a.entry.localeCompare(b.entry))`.
  3. `root.changes.opVerdictChanges = opVerdictChanges`.
  4. Return `new RTableGroupDelta(start, end, bound, root)`.

### Entry Points, Host Interface and Memo

The walk above is one function with a `boundOnly` flag, kept in `rtable_group/delta.ts`, with two entry points:
- `computeRTableGroupDelta(group, rawDag, strategy, start, end)` keeps its signature. `'full'` is unchanged: empty bound, whole history of `end`. `'bounded'` runs Phases A–C with `boundOnly = false`.
- A new exported `computeRTableGroupRevisionBound(group, rawDag, start, end): Promise<Version>` runs Phases A–C with `boundOnly = true`.

`RTableGroupImpl` gains one public method, `computeRevisionBound(start: Version, end: Version): Promise<Version>`:
- It canonicalizes `start` and `end` in its own causal DAG (`canonicalVersion`) and looks them up in the memo.
- On a miss it gets its raw DAG the way `computeDelta` does (`group.ts:1311-1315`), calls `computeRTableGroupRevisionBound`, and stores the result.
- It ignores the instance's delta strategy: a foreign bound is always the bounded one. Under the observer's `'full'` strategy the bound is empty and Phase A does not run, so no foreign bound is requested.

`GroupDeltaHost` changes:
- Add `resolveSchemaView(at, from)`, which `didDeleteHonorFlip` calls, and `resolveObservedForeignVersion(groupId, at, from)`, which Phase A step 3 calls. Both already exist on `RTableGroupImpl`.
- Add `getBoundGroup(groupId: B64Hash): Promise<BoundGroupHost>`, with `BoundGroupHost = { computeRevisionBound(start: Version, end: Version): Promise<Version>; getCausalDag(): Promise<CausalDag> }`. `RTableGroupImpl` implements it by wrapping the private `loadForeignGroup` (`group.ts:736`), so a missing bound object still throws.
- `getScopedDag()` and `getCausalDag()` already come from `RObject`.
- Drop `getObservedObjects()`. Its only caller is `rtable_group/delta.ts:185`, so remove the method from `RTableGroupImpl` too.

In `computeRTableGroupDelta`, delete the loop that forces `setDeltaStrategy('bounded')` on the observed objects (`rtable_group/delta.ts:186-191`). It becomes dead once foreign bounds go through `computeRevisionBound`. Also drop the imports of `combineObserverRevisionBounds` and `walkEntriesBackwardsToBound`. Both stay in `mvt/delta.ts`: `walkEntriesBackwardsToBound` still serves `walkDelta` for `RSet`, `RCap` and `RSchema`, and `combineObserverRevisionBounds` keeps its tests in `mvt/test/delta_tests.ts`.

**Memo.** A `Map<string, Version>` on the `RTableGroupImpl` instance, keyed by the sorted hashes of the canonical `start` and `end`. The group is fixed by the instance.
- *It can persist across calls.* Entry hashes are content addresses, so a version names a fixed history. The bound between two fixed versions depends only on those histories and on the schema and foreign histories they pin through ref-advance versions, which are fixed too. Appending entries never changes it.
- *Cap it and clear it.* Use a small LRU (e.g. 256 entries) and clear it in `destroy()` (`group.ts:1424`).
- *Contents.* Bound-only results go in. A `'bounded'` full-mode pass may store its final bound as well, since both modes compute the same bound (test item 6).

*(Stale comments to update alongside this implementation: `rtable_group/delta.ts:22-29`, `rtable/delta.ts:13-15`, and `mvt/delta.ts:84-109`.)*

---

## 6. Helper Definitions

### `computeStopSet(rawDag: dag.Dag, bound: Position, from: Position): Promise<Set<B64Hash>>`
Computes the exit border of $H(\text{bound})$ relative to `from` (Lemma 2), kept local to `rtable_group/delta.ts`:
```ts
async function computeStopSet(rawDag: dag.Dag, bound: Position, from: Position): Promise<Set<B64Hash>> {
    if (bound.size === 0) return new Set();
    const boundFork = await rawDag.findForkPosition(bound, from);
    return new Set<B64Hash>([...boundFork.common, ...boundFork.commonFrontier, ...bound]);
}
```

### `isBarrierEntry(entry: dag.Entry): boolean`
In `RTableGroup`, every barrier (row delete, bundle containing a delete, schema deploy, and foreign-group observe) is tagged with `meta['barrier'] = json.toSet(['t'])` on the outer group DAG entry (`scopes.ts:41`, `scopes.ts:207`, `refs.ts:62`).
```ts
function isBarrierEntry(entry: dag.Entry): boolean {
    return entry.meta['barrier'] !== undefined;
}
```

### `isStrictlyAfter(rawDag: dag.Dag, preds: Position, bound: Position): Promise<boolean>`
An entry $T$ is strictly after `bound` iff every head of `bound` lies in $H(\text{preds}(T))$:
```ts
async function isStrictlyAfter(rawDag: dag.Dag, preds: Position, bound: Position): Promise<boolean> {
    if (bound.size === 0) return true;
    if (preds.size === 0) return false;
    const fork = await rawDag.findForkPosition(bound, preds);
    return fork.forkA.size === 0;
}
```

### `computeGLB(rawDag: dag.Dag, a: Position, b: Position): Promise<Version>`
The greatest lower bound of two positions is the `commonFrontier` of their fork:
```ts
async function computeGLB(rawDag: dag.Dag, a: Position, b: Position): Promise<Version> {
    if (a.size === 0 || b.size === 0) return version();
    const fork = await rawDag.findForkPosition(a, b);
    return fork.commonFrontier;
}
```

### `isUnstableObserve(entry: dag.Entry, foreignBounds: Map<B64Hash, { dag: CausalDag; bound: Version }>): Promise<boolean>`
`foreignBounds` contains only bound foreign groups (not `RSchema`), so schema deploys return `false`:
```ts
async function isUnstableObserve(
    entry: dag.Entry,
    foreignBounds: Map<B64Hash, { dag: CausalDag; bound: Version }>,
): Promise<boolean> {
    if (!isRefAdvancePayload(entry.payload)) return false;
    const target = foreignBounds.get(entry.payload.refId);
    if (target === undefined) return false;
    const refVersion = extractRefVersion(entry.payload);
    return !(await refVersionAtOrBelow(target.dag, refVersion, target.bound));
}
```

### `didDeleteHonorFlip(group, entry, start, end): Promise<boolean>`
Only called when `deployCoverMoved` is true and `entry` is a barrier in $H(\text{start}) \setminus H(\text{bound})$ whose verdict did not already flip. Taking the touched tables from `entry.meta['tables']` covers single-row deletes, bundle deletes, **and** schema deploys that delete stranded rows when adopting an FK (`deriveDeployKillMeta` in `scopes.ts:111-119`), while observes and non-killing deploys carry no `tables` meta:
```ts
function deletedTables(entry: dag.Entry): string[] {
    const tables = entry.meta['tables'];
    return tables === undefined ? [] : json.fromSet(tables);
}

async function didDeleteHonorFlip(
    group: GroupDeltaHost,
    entry: dag.Entry,
    start: Version,
    end: Version,
): Promise<boolean> {
    const deleteTables = deletedTables(entry);
    if (deleteTables.length === 0) return false;
    const atPos = version(entry.hash);
    const schemaStart = await group.resolveSchemaView(atPos, start);
    const schemaEnd = await group.resolveSchemaView(atPos, end);
    for (const table of deleteTables) {
        const before = schemaStart.hasTable(table) && schemaStart.getConcurrentDeletes(table);
        const after = schemaEnd.hasTable(table) && schemaEnd.getConcurrentDeletes(table);
        if (before !== after) return true;
    }
    return false;
}
```

---

## 7. Trace on the Counterexamples and Common Cases

### Case 1: Unflipped side-branch barrier (`T` does not flip)
- Same DAG shape as the counterexample: `x` inserts target `x`; branch 1 has `E` $\to$ `s1` $\to$ `N`; branch 2 has `T` (attached to `x`), where `T` does **not** flip.
- `start = {s1, T}`, `end = {N, T}`, `meet = {s1}`.
- Phase A sets `bound = {s1}` and `stopSet = computeStopSet({s1}, {N, T}) = {s1, x}` (since both `s1` and `x` lie in $H(\{s_1\})$ and have direct successors `N` and `T` outside $H(\{s_1\})$).
- Phase B starts from `{N, T}`:
  - Pops `N`: `!entryInStart`, ingests `N`, enqueues `s1`.
  - Pops `T`: `entryInStart = true`, barrier. Evaluates `voidBefore === voidAfter`, `isUnstableObserve(T) = false` $\Rightarrow$ `triggered = false`. Enqueues `x`.
  - Pops `s1`: `stopSet.has(s1)` is **true** $\Rightarrow$ records `s1` in `stoppedAtBound` and stops.
  - Pops `x`: `stopSet.has(x)` is **true** $\Rightarrow$ records `x` in `stoppedAtBound` and stops.
- Queue is empty. Only `{N, T}` were loaded; `s1`, `E`, `x`, and the past of `x` are never visited, and `revisionBound` stays `{s1}`.

### Case 2: Local transitive barrier flip (`BOUND_SIMPLIFIED_SUMMARY.md` counterexample)
- Same DAG, except `N` deletes `p`, causing `T` to flip from live to void and `E` to flip from void to live.
- Phase A sets `bound = {s1}` and `stopSet = {s1, x}`.
- Phase B starts from `{N, T}`:
  - Pops `N`: ingests `p`, enqueues `s1`.
  - Pops `T`: `entryInStart = true`, barrier. Evaluates `voidBefore = false`, `voidAfter = true` $\Rightarrow$ `triggered = true`.
  - `preds(T) = {x}` is not strictly after `bound = {s1}`.
  - Updates `bound = GLB({s1}, {x}) = {x}` and `stopSet = computeStopSet({x}, {N, T}) = {x}`.
  - Enqueues `x`. (If `s1` had already been popped into `stoppedAtBound`, `!stopSet.has(s1)` moves `s1` back to `queue`.)
  - Pops `s1`: `stopSet.has(s1)` is now **false** $\Rightarrow$ loads `s1`, enqueues `E`.
  - Pops `x`: `stopSet.has(x)` is **true** $\Rightarrow$ records `x` in `stoppedAtBound` and stops.
  - Pops `E`: loads `E`, ingests item row, records `E`'s verdict flip (`voidBefore = true`, `voidAfter = false`), enqueues `x` (already stopped).
- Queue is empty. Both `T` and `E` are reported in `opVerdictChanges` and their rows in `tableChanges`, `x` and its ancestors are never loaded, and `revisionBound = {x}`.

### Case 3: Cross-group observe on a side branch (`T` stays live, `E` flips)
- `T` in $H(\text{start}) \setminus H(\text{meet})$ is an observe concurrent with `E` in $H(\text{meet})$, publishing foreign version $v_1 \not\le b_F$ (where $b_F$ was computed on $F$ between `refAtStart` and `refAtEnd` using this same procedure). `N` in $H(\text{end}) \setminus H(\text{start})$ publishes $v_2$ containing a foreign delete concurrent with $v_1$.
- Phase A sets `bound = {s1}` and `stopSet = {s1, x}`.
- Phase B pops `N` and `T`.
- At `T`, `voidBefore === voidAfter === false` (`T` stays live!), **but** `isUnstableObserve(T)` is `true` because $v_1 \not\le b_F$.
- `triggered` is true $\Rightarrow$ `bound` drops to `GLB({s1}, preds(T)) = {x}`, `stopSet` updates to `{x}`, `s1` and `E` are walked, and `E`'s flip is reported.

---

## 8. Test Plan

1. **Promote `local_floor_proof.ts` and `observe_floor_proof.ts`:**
   Register both histories as tests asserting that the `'bounded'` delta matches the `'full'` delta on both `opVerdictChanges` and `tableChanges`.
2. **Intermediate foreign-version history (`refAtMeet` vs `refAtStart`):**
   Build the two-group history where a foreign barrier $T_F$ is live at $v_1 = \text{refAtMeet}$, void at $v_2 = \text{refAtStart}$, and live again at $v_3 = \text{refAtEnd}$, and assert `'bounded'` vs `'full'` parity on the observer group $G$.
3. **FK-adopting schema-deploy kill honor flip:**
   Build the history where a side-branch schema deploy $D \in H(\text{start}) \setminus H(\text{meet})$ adopts an FK (killing a stranded row $R$ via `deriveDeployKillMeta`) and a new concurrent deploy $N \in H(\text{end}) \setminus H(\text{start})$ flips `concurrentDeletes` on $R$'s table, asserting `'bounded'` vs `'full'` parity.
4. **Unflipped side-branch barrier (Case 1):**
   Assert both delta parity **and** that `delta.revisionBound` stays at `{s1}` (the meet) without walking below `s1`.
5. **Side-branch schema deploy without honor flip:**
   When a side branch in $H(\text{start}) \setminus H(\text{meet})$ contains a schema deploy $D$ and the new entries are ordinary inserts, assert that `delta.revisionBound` stays at the meet.
6. **Bound-only mode matches the full-mode bound:**
   Wherever a test computes a `'bounded'` delta (items 1–5 and every pair of the parity sweeps), also assert that `group.computeRevisionBound(start, end)` equals `delta.revisionBound`. The two modes skip different work: bound-only skips the row channel, and it skips the verdicts of entries that cannot widen. This catches them drifting apart.
7. **Parity generator (`test/delta_parity/generate.ts`) extensions:**
   - Add table delete rules that read another table (`exists` over a permission/gate table with `concurrentDeletes: true`).
   - Add two-level barrier chains in foreign groups so foreign verdicts can flip and flip back across intermediate observed versions.
   - Add side-branch `observe` ops that publish an older foreign version than the main branch.
   - Add a shaped generator. It builds the skeleton of the counterexamples (`x`, then `E`–`s1` on one branch and `T` attached at `x` on another, `start = {s1, T}`, a new `N` after `s1`, `end = {N, T}`) and randomizes which barriers sit on it: a delete gated by another table, an FK-adopting deploy, a `concurrentDeletes` toggle, or an observe of an older or newer foreign version. The concurrency these bugs need is rare in a fully random history.
   - Before relying on an extension, show that it finds at least one pair where today's `'bounded'` delta differs from `'full'`. Today's code matches the full delta on every pair of both existing generators (extended profile, about 1,900 pairs), so those sweeps only guard against regressions.
