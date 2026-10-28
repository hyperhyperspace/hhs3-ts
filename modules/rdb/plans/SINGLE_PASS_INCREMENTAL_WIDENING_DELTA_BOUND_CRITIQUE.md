# Critique of the single-pass widening bound

This reviews [`SINGLE_PASS_INCREMENTAL_WIDENING_DELTA_BOUND.md`](./SINGLE_PASS_INCREMENTAL_WIDENING_DELTA_BOUND.md), called "the plan" below, as it stands after the corrections of the previous critique. Words are as in [`BOUND_SIMPLIFIED_SUMMARY.md`](./BOUND_SIMPLIFIED_SUMMARY.md).

The plan was checked against the code it cites. The two histories in §1 and §2 were built and run in memory against the current code (scripts and output in the appendix). The run gives the verdicts, the resolved versions, the full delta and today's bounded delta. The plan is not implemented, so its own steps on those histories are traced by hand.

---

## Verdict

Not ready to implement as written. Two triggers are computed wrongly, and each lets a bounded delta miss a flip that the full delta reports:

1. **The foreign bound is computed from the wrong starting version** (§1). Phase A takes $b_F$ between the version of $F$ observed at the meet and the version observed at `end`. The verdicts it protects read $F$ at the version observed at `start`. A change-driven bound is only sound for the exact pair of versions it compared. The fix is one line in Phase A.
2. **The concurrent-delete honor check reads only the payload** (§2). A schema deploy that adopts an FK deletes rows through its meta, and its honor flag can move while its verdict never does. The fix is in the helper.

Beyond those:

3. The schema should not be an observed object in Phase A or in the unstable-observe trigger (§3). The plan's own §3.2 proves it is not needed, and keeping it reprocesses exactly the old side branches the plan is meant to skip.
4. The stop-set walk should stay inside the group delta for this change (§4). The shared walk serves `RSet` and `RCap`, whose bounds are still the meet.
5. The plan needs a test section (§5).
6. Smaller points are in §6.

The rest holds: the stop set, `inStart`, the resume after lowering, the trigger list, and Phase A's step to the predecessors of the lowest unstable observe (see "What holds").

---

## What holds

**Corrections from the previous critique are in.** Lemma 2 item 3 now says no entry outside $H(\text{bound})$ has a causal successor inside it, which is the right direction. §3.2 cites the schema early return at `group.ts` 1219.

**Lemma 1.** With the bound at or below the meet, every new entry is strictly after every head of the bound, so no new entry is concurrent with anything inside the bound. This needs every new entry to have an ancestor in `fork.common`. That holds because the group's create entry is the only entry with no predecessors, and it is in $H(\text{start})$ whenever `start` is non-empty. With an empty `start`, `computeMeet([])` returns the empty position and the walk is full (`dag_meet.ts` 21–23). The plan should say this in one sentence (§6).

**The stop set and the resume.** The stop set is the exit border of $H(\text{bound})$ inside $H(\text{end})$. A backward path that starts inside the history starts on a head of the bound. A path that steps from outside to inside lands on `boundFork.common`. The resume argument needs the bound to stay an antichain. It does: the meet and every GLB are a `commonFrontier`.

**`inStart` is exact at pop time, whatever the walk order.** An entry is popped only because it is in `end` or because a walked successor enqueued it. If that successor is in $H(\text{start})$, it was marked when it was walked and passed the mark down. If it is new, the entry has a new successor and is in `fork.common`, which is seeded. Nothing outside $H(\text{start})$ is ever marked.

**The widening step.** `isStrictlyAfter` is `forkA` empty, which holds iff $H(\text{bound}) \subseteq H(\text{preds}(T))$. Replacing the bound by $\mathrm{GLB}(\text{bound}, \text{preds}(T))$ removes exactly the entries of $H(\text{bound})$ that are not ancestors of $T$, which are the ones concurrent with $T$. A barrier that already passed keeps passing as the bound descends.

**The final bound does not depend on walk order.** Whether a barrier triggers depends on `start`, `end` and the foreign bounds, never on the current bound. Each lowering is a GLB with the barrier's predecessors, and that GLB is a no-op when the barrier is strictly after. So the result is the GLB of the Phase A bound and the predecessors of every triggering barrier in the final interval, the least fixpoint, whatever order the queue pops them in. Replicas agree on `revisionBound`.

**The trigger list matches the cross-branch reads.** An entry inside the bound reads across branches in four places, and each is covered by a trigger:

- *Schema and parameters at its own position.* `resolveSchemaVersion` folds deploys with no liveness filter (`group.ts` 406–410). Deploys are never void (`group.ts` 1219). By Lemma 1 no new deploy is concurrent with the entry, so the fold is the same at both horizons. `resolveGroupParams` does the same over parameter-tagged deploys (`group.ts` 433–513). Nothing triggers here, correctly.
- *Concurrent deletes.* `killedByConcurrentDelete` takes the concurrent cover of live delete barriers for the row and honors each one by the schema at that delete's own position (`rtable/view.ts` 279–298). The cover can hide a lower honored delete under a live unhonored one, but its inputs are still each barrier's verdict and honor flag, which the verdict-flip and honor-flip triggers watch. New entries are skipped by the concurrent search as after every head of the position (`concurrent_cover.ts` 57–76).
- *Foreign tables.* `resolveForeignTableViewIn` reads $F$ at `foreignAt`, folded from observes in the entry's past and concurrent observes, and at `foreignFrom`, folded from every observe in the horizon (`group.ts` 620–621).
- *The observe gate.* `resolveObserveGateRefAt` widens by concurrent live observes strictly above the base (`group.ts` 699–734). The gate reads $F$ at the horizon's version (`group.ts` 1227).

§1 and §2 are not missing triggers. They are errors in how two of these triggers are computed.

**Phase A's step to predecessors.** Lowering to $\mathrm{GLB}(\text{bound}, \text{preds}(r))$ for each lowest unstable observe $r$ fixes `projectForeignBound` putting $r$ itself into the bound (`refs.ts` 269–271). Every unstable observe in $H(\text{meet})$ is above some lowest one, by monotonicity, so none stays inside the bound.

---

## 1. The foreign bound is computed from the wrong starting version

### The claim and where it breaks

§3.4 and Phase A step 3 define $b_F$ as the bound of $F$ between `refAtMeet` and `refAtEnd`:

```80:81:modules/rdb/SINGLE_PASS_INCREMENTAL_WIDENING_DELTA_BOUND.md
   Let $b_F$ be the sound revision bound of foreign group $F$ between $\text{refAtMeet} = \text{resolveRefVersion}(F, \text{meet}, \text{meet})$ and $\text{refAtEnd} = \text{resolveRefVersion}(F, \text{end}, \text{end})$, computed recursively by **this same single-pass widening bound procedure** on $F$.
   So long as every observe $O$ contributing to `foreignAt(E, H)` has the same `isLive` verdict at `start` and `end` **and** publishes a version $\text{refVersion}(O) \le b_F$, we have $\text{foreignAt}(E, \text{start}) = \text{foreignAt}(E, \text{end}) \le b_F$. By the soundness of $b_F$ on $F$, advancing `foreignFrom` from `start` to `end` cannot change any verdict or row state in $F$ at or below $b_F$.
```

The last sentence needs $b_F$ to be sound for the pair of versions that `foreignFrom` takes. That pair is not (`refAtMeet`, `refAtEnd`). The foreign view is built at the version folded from every observe in the horizon:

```620:621:modules/rdb/src/rtable_group/group.ts
        const foreignAt = await canonicalVersion(foreignDag, await resolveRefVersionAtPosition(dag, groupId, at, from, isLive));
        const foreignFrom = await canonicalVersion(foreignDag, await resolveRefVersionAtPosition(dag, groupId, from, from));
```

The observe gate reads the same version (`group.ts` 1227). Inside $F$, every verdict and row read in that view is taken at `foreignFrom`. So the pair is

$$\text{refAtStart} = \text{resolveRefVersion}(F, \text{start}, \text{start}), \qquad \text{refAtEnd} = \text{resolveRefVersion}(F, \text{end}, \text{end}).$$

`refAtMeet` is not a horizon that any verdict in this delta reads.

### Why the meet's version was fine for the shape-only bound and is not here

A shape-only bound leaves no barrier outside it concurrent with anything inside it. Verdicts inside it are then the same at every horizon between its two ends, so a bound computed from `refAtMeet` also covers `refAtStart`, which lies between `refAtMeet` and `refAtEnd`.

A change-driven bound only promises the two versions it compared. It skips a barrier whose verdict is the same at both. A barrier can be live at the first, void at an intermediate version, and live again at the last. Then entries that read it go one way and back, and a bound computed from the first version says nothing about the intermediate one. New entries of $F$ between the two versions cannot do this directly, because they are strictly after $F$'s meet. The gap is an old barrier of $F$ whose verdict moves and returns.

`computeObserverRevisionBound` (`mvt/delta.ts` 123–125) takes the meet's version today, and `BOUND_SIMPLIFIED_SUMMARY.md` defines stability the same way. That matches the shape-only rule and not the change-driven one.

### The intermediate-version history

The observed group $F$ has five tables. Deleting a `targets` row requires a `perm` row labelled `ok`. Deleting a `perm` row requires a `gate` row. `items.t` is an FK to `targets`. All three of `perm`, `gate` and `targets` honor concurrent deletes. `notes` is open.

```text
F:   p ── g ── x ── E_F ── s1 ── N1
                \            \
                 T_F          N3
```

- `p` inserts the `ok` perm row, `g` the gate row, `x` the target.
- `E_F` inserts an item pointing at `x`. `s1` is a note after it.
- `T_F` deletes `x`, with predecessor `x`.
- `N1` deletes the perm row and `N3` deletes the gate row. Both have predecessor `s1`, so they are concurrent with each other and with `T_F`.

Three versions of $F$: $v_1 = \{s_1, T_F\}$, $v_2 = \{N_1, T_F\}$, $v_3 = \{N_1, N_3, T_F\}$.

- At $v_1$ nothing concurrent touches `T_F`'s rule, so `T_F` is live and kills `x` for `E_F`, whose FK fails.
- At $v_2$ `N1` is live and concurrent with `T_F`, so `T_F`'s `exists perm ok` fails, `x` stays, and `E_F` is live.
- At $v_3$ `N3` kills the gate row for `N1`, so `N1` is void, the perm row is back, `T_F` is live again, and `E_F` is void again.

```text
F v1 T_F live E_F void N1 live
F v2 T_F void E_F live N1 live
F v3 T_F live E_F void N1 void
F full delta v1->v3 flips: [ 'N1' ]
F full delta v2->v3 flips: [ 'E_F', 'T_F', 'N1' ]
F meet(v1,v3): [ 's1' ]
```

Between $v_1$ and $v_3$, `T_F` does not flip and `N1` is new. Under the plan nothing triggers, and $F$'s bound is its meet, `{s1}`. That bound is sound for $v_1 \to v_3$: `E_F` is void at both. It is not sound for $v_2 \to v_3$.

The observer $G$ binds $F$ as `f`, ungated. `refs.item` is an FK to `f.items`. `notes` is open.

```text
G:   O1 ── E_G ── O_m ── s1G ── O3
             \
              O2
```

- `O1` observes `{s1}`.
- `E_G` inserts a ref to the item `E_F` inserted. It is valid at write: $F$ at `({s1}, {s1})` has the item live.
- `O_m` observes $v_1$ after `E_G`. `s1G` is a note after `O_m`. `O3` observes $v_3$ after `s1G`.
- `O2` observes $v_2$, with predecessor `E_G`.

`start` is `{s1G, O2}` and `end` is `{O3, O2}`. The only new entry is `O3`.

`E_G` reads $F$ at `foreignAt = {s1}`, because `O1` is the only observe in its past and the others are after it. At `start`, `foreignFrom` folds `O_m` and `O2` into $v_2$, where the item is live, so `E_G` is live. At `end` it folds into $v_3$, where the item is void, so `E_G` is void.

```text
G E_G start: live end: void
G meet: [ 's1G' ]
refAtMeet: [ 'T_F', 's1' ] refAtStart: [ 'N1', 'T_F' ] refAtEnd: [ 'N1', 'N3', 'T_F' ]
G full flips: [ 'E_G' ]
G current bounded flips: [ 'E_G' ] bound: [ 'O_m' ]
```

### The plan on this history

1. `fork.common` is `{s1G}` and the meet is `{s1G}`.
2. `refAtMeet` is $v_1$ and `refAtEnd` is $v_3$, so $b_F$ = `{s1}`.
3. Phase A: the cover of $F$ observes at the meet is `{O_m}`. $v_1 \not\le \{s_1\}$, because `T_F` is not below `s1`, so `O_m` is unstable. The cover below its predecessor `E_G` is `{O1}`, which publishes `{s1}` and is stable. `O_m` is the lowest unstable observe, and the bound becomes $\mathrm{GLB}(\{s1G\}, \{E_G\}) = \{E_G\}$.
4. The stop set of `{E_G}` relative to `end` is `{E_G}`: its successors `O_m` and `O2` are outside its history.
5. The walk from `{O3, O2}`:
   - `O3` is new.
   - `O2` is in `start`, a barrier, and unstable ($v_2 \not\le \{s_1\}$), so it triggers. Its predecessors are `{E_G}`, which is strictly after `{E_G}`, so the bound does not move.
   - `s1G` is walked.
   - `O_m` triggers the same way and does not move the bound.
   - `E_G` is reached from `O2` and from `O_m`, and stops both times.
6. The bound stays `{E_G}`. The flip of `E_G` and the change to its `refs` row are both missing.

Today's code reports `E_G`, but only by accident. Its bound is `{O_m}`, because `projectForeignBound` adds the observe itself, and `walkEntriesBackwardsToBound` stops only on a head of the bound, so it enters `E_G` through `O2`. The plan's stop set closes that path. As written, the plan regresses this history.

### Fix

Compute $b_F$ between `refAtStart` and `refAtEnd`. Change §3.4 and Phase A step 3 to say so, and drop `refAtMeet`. End extends start for this pair, because ref-advances are monotone along predecessors, so the foreign `computeDelta` precondition holds. The recursion stays consistent: inside $F$, the bound of a group $K$ that $F$ observes is taken between $K$'s versions observed at `refAtStart` and at `refAtEnd`, which are the horizons $F$'s verdicts read.

On the history above, $F$'s delta from $v_2$ to $v_3$ flips `N1`, which is strictly after `{s1}`, and `T_F`, whose predecessors `{x}` are not. The bound of $F$ drops to `{x}`, and `s1` and `E_F` are walked. In $G$, `O1` publishes `{s1}`, which is not at or below `{x}`, so it is unstable. It is now the lowest unstable observe, its predecessor is $G$'s create entry, and `E_G` is walked.

When $H(\text{start}) \setminus H(\text{meet})$ holds no observe of $F$, `refAtStart` equals `refAtMeet`, so the common case is unchanged. The definition of "stable" in `BOUND_SIMPLIFIED_SUMMARY.md` needs the same change for its change-driven variant. For its shape-only variant either version works.

---

## 2. The concurrent-delete honor check reads only the payload

### The claim and where it breaks

The helper takes the deleted tables from the payload, through a function that does not exist in the codebase:

```303:305:modules/rdb/SINGLE_PASS_INCREMENTAL_WIDENING_DELTA_BOUND.md
): Promise<boolean> {
    const deleteTables = deletedTablesFromGroupPayload(entry.payload);
    if (deleteTables.length === 0) return false;
```

The trigger summary says the same ("$T$ carries a row `delete` on `table`", line 93). A schema deploy that adopts an FK deletes the rows it strands, and those deletes are in its meta, not its payload. `deriveDeployMeta` (`group.ts` 861–870) stores the `tables` tag and one `t-<table>-rows` tag per row:

```111:119:modules/rdb/src/rtable_group/scopes.ts
export function deriveDeployKillMeta(kills: DeployKills): MetaProps {
    const meta: MetaProps = {};
    if (kills.size === 0) return meta;
    meta['tables'] = json.toSet([...kills.keys()]);
    for (const [table, { incarnation, rowIds }] of kills) {
        wrapInnerMeta(meta, table, { rows: json.toSet([...rowIds].map((rowId) => rowTag(incarnation, rowId))) });
    }
    return meta;
}
```

`killedByConcurrentDelete` matches on the barrier and row tags, so it treats such a deploy as a concurrent delete. It honors the deploy by the schema at the deploy's own position (`rtable/view.ts` 285–289). A deploy is never void, so the verdict-flip trigger cannot catch it. When the version it publishes is stable, the honor flip is the only trigger left, and the helper returns `false` for it.

### The deploy-kill history

One group with schema $S_0$: `items(t)` honoring concurrent deletes, with no FK; `targets(n)`; `refs(item)` with an FK to `items`; `notes`. In the schema DAG, $S_D$ follows $S_0$ and adds the FK `items.t → targets`. $S_N$ follows $S_D$ and turns concurrent deletes off on `items`.

```text
R ── E ── D2 ── s1 ── N
 \
  D
```

- `R` inserts an item with `t = "missing"`. That is valid, because there is no FK yet.
- `E` inserts a ref to `R`.
- `D2` deploys $S_D$ after `E`. `D` deploys $S_D$ with predecessor `R`. Each adopts the FK and deletes `R`, which points at no target. `D`'s payload is a plain ref-advance, and its meta carries `tables` and `t-items-rows`.
- `s1` is a note after `D2`. `N` deploys $S_N$ after `s1`.

`start` is `{s1, D}` and `end` is `{N, D}`. Two replicas deploying the same catalog version concurrently is the ordinary way to get `D` and `D2`.

`D` is concurrent with `E` and deletes `R`. At `start`, the schema at `D`'s position folds `D` and the concurrent `D2` into $S_D$, which honors concurrent deletes on `items`. `R` is killed, `E`'s FK fails, and `E` is void. At `end`, `N` is also concurrent with `D`, so the schema at `D` is $S_N$, which does not honor them. `R` stays, and `E` is live.

```text
D meta keys: [ 'barrier', 'ref', 't-items-rows', 'tables' ] payload action: ref-advance
E start: void end: live | D start: live end: live
honor of D at start: true at end: false
meet: [ 's1' ]
deploy cover start: [ 'D', 'D2' ] end: [ 'N', 'D' ]
schema at meet: [ 'S_D' ] at end: [ 'S_N' ]
schema bound: [ 'S_D' ]
full flips: [ 'E' ] refs rows: [ [ false, true ] ]
current bounded flips: [] bound: [ 's1' ]
```

### The plan on this history

1. The meet is `{s1}`, and the stop set is `{s1, R}`: `R`'s successor `D` is outside the history.
2. Phase A: the schema version at the meet is $\{S_D\}$ and at `end` is $\{S_N\}$, so the schema's bound is $\{S_D\}$. `D2` is stable, and the bound stays `{s1}`.
3. The walk from `{N, D}`:
   - `N` is new.
   - `D` is in `start` and a barrier, with no verdict flip.
   - `D` publishes $\{S_D\} \le \{S_D\}$, so it is stable.
   - `deployCoverMoved` is true, because the deploy cover moves from `{D, D2}` to `{N, D}`.
   - `didDeleteHonorFlip` reads `D`'s ref-advance payload, finds no row ops, and returns `false`. Nothing triggers.
   - `s1` and `R` stop.
4. `E` is never walked. Today's code misses it as well: the bound is `{s1}` and the delta reports no flip.

This history keeps the schema among the observed objects, and `D` is still stable, so the gap does not depend on §3. Dropping the schema there makes this check the only trigger for every deploy that deletes rows. It has to land first or in the same change.

### Fix

Take the tables from the entry's `tables` meta. For a row delete that is its table. For a bundle it is every table the bundle touches, which only over-approximates. For a deploy it is the tables it deletes from. Observes and deploys that delete nothing carry no `tables` tag.

```ts
function deletedTables(entry: dag.Entry): string[] {
    const tables = entry.meta['tables'];
    return tables === undefined ? [] : json.fromSet(tables);
}
```

The helper is only called on barrier entries, so a row insert never reaches it. On the history above, `items` is honored at `start` and not at `end`. `D` triggers, its predecessors `{R}` are not strictly after `{s1}`, and the bound drops to `{R}`. `s1` resumes, and `D2` and `E` are walked. Change the trigger summary and the helper's description from "row/bundle delete" to "deletes rows of `table`".

---

## 3. The schema should not be an observed object here

§3.2 of the plan proves that an entry inside the bound reads the same schema and the same parameters at both horizons, and "What holds" checks that against the code. The `RSchema` view at a fixed version does not depend on a horizon, because the schema DAG has no barriers (`rschema/delta.ts` 1–7). So no schema deploy needs a stability check, and the schema needs no bound.

Phase A still projects over `group.getObservedObjects()`, which returns the schema first (`group.ts` 1300–1306). Two things follow. Phase A lowers the bound below schema deploys in $H(\text{meet})$ whose version is above the schema's bound. Phase B triggers on side-branch deploys whose version is above it, as unstable observes.

A small history shows the cost:

- The branch through the meet has only inserts.
- A side branch has one deploy `D` that publishes $S_1$. `D` is in $H(\text{start})$, outside $H(\text{meet})$.
- The new entries are inserts.

The schema version at the meet is $S_0$ and at `end` is $S_1$, so the schema's bound is $\{S_0\}$. `D` publishes $S_1 \not\le S_0$, so it is unstable and pulls the bound down to its predecessors. Everything concurrent with `D` is reprocessed. No verdict there can change, because there is no new deploy. This is the old side-branch cost the plan's overview sets out to avoid.

Build `foreignBounds` and the Phase A projection over the bound foreign groups only (`getBindings()`), not `getObservedObjects()`. A schema deploy then reaches `isUnstableObserve` with no entry in `foreignBounds`, and it returns `false` with no other change. The one schema-sensitive cross-branch read left is the honor flag, which the trigger in §2 handles once it is fixed. Today's `combineObserverRevisionBounds` includes the schema, and the plan inherits that without saying why.

---

## 4. Keep the stop-set walk inside the group delta for now

The plan says `computeStopSet` also fixes `walkEntriesBackwardsToBound` (lines 31 and 243). That function serves `walkDelta` for `RSchema`, `RSet` and `RCap` too. `RCap`'s bound is the meet (`rcap/delta.ts` 187). `RSet`'s is the projection of it (`rset/delta.ts` 212–219). `BOUND_SIMPLIFIED_SUMMARY.md` shows that the meet is not sound for a type with barriers.

The current over-walk, which stops only on a head of the bound (`mvt/delta.ts` 48), can reach entries the meet wrongly leaves out. The history in §1 is one such case in the group, and today's code gets it right only through that path. Tightening the shared walk can turn such cases into silent misses in `RSet` and `RCap`.

Put the stop set in the group's own walk in this change. Change the shared walk when `RSet` and `RCap` get their own bound, together with full-against-bounded parity for both. `RSchema` has no barriers, so the stop set is safe there whenever it moves.

---

## 5. The plan needs a test section

There is a full-against-bounded harness in `test/delta_parity/`, run by `npm run test:parity`. Its single-group generator builds only open tables, whose restrictions are all `true` (`generate.ts` 22–26, 76–79). Its cross-group generator's foreign group has only an open `orders` table (`generate.ts` 224). Neither can produce a delete whose rule reads another row. The local history in `local_floor_proof.ts`, the observe history in `observe_floor_proof.ts`, and the history in §1 all need one.

Add:

1. `local_floor_proof.ts` and `observe_floor_proof.ts` as registered tests that assert the bounded delta equals the full one. Today they assert the miss.
2. The history in §1 (Appendix A), asserting parity on $G$'s delta.
3. The history in §2 (Appendix B), asserting parity.
4. The plan's Case 1, asserting that `revisionBound` stays `{s1}` and that the deltas agree.
5. The side-branch deploy from §3, asserting that `revisionBound` stays at the meet once the schema is dropped.
6. In the generator:
   - deletes whose rule reads another table, like `targets` and `perm` in `local_floor_proof.ts`;
   - two-level barrier chains in the foreign group, so a foreign verdict can move and return;
   - side-branch observes that publish an older foreign version than the main branch.

---

## 6. Smaller points

- **Lemma 1's root assumption.** State that every new entry reaches $H(\text{start})$ because the create entry is the only root, and that an empty `start` gives an empty meet and a full walk.
- **The cost of a lowering.** "Zero redundant entry evaluations" holds for entry loads and verdicts. Each lowering also reruns `findForkPosition(bound, end)` for the new stop set. The topo index walks roughly the whole difference to answer it (`topo_idx.ts` 103 onward), so a delta with $k$ lowerings does about $k$ index walks of the interval. That is acceptable, but the plan should say so.
- **The foreign bound computation.** Phase A needs only $b_F$. `computeObserverRevisionBound` runs the foreign group's full `computeDelta`, including the per-row diffs in `finalize`, and keeps only `revisionBound` (`mvt/delta.ts` 125). Use a bound-only path and memoize it per $(F, \text{refAtStart}, \text{refAtEnd})$. Several groups, and several levels of the recursion, ask for the same foreign group.
- **Citations.** `resolveSchemaVersion` is at `group.ts` 406, not 408. `resolveForeignTableViewIn` is at 609, not 611.
- **Comments to update in the same change.**
  - `rtable_group/delta.ts` 22–29 still describes the bound as the GLB of the meet and the projected bounds, with "at-use semantics make this floor exact".
  - `rtable/delta.ts` 13–15 says the same.
  - `mvt/delta.ts` 84–109 describes the deferred tightening that this plan implements.

---

## Appendix: reproduction scripts

Both scripts were run from `modules/rdb` as

```sh
node --import ../../register.mjs --input-type=module -e '<script>'
```

against the current tree. They write nothing to disk. They check the premises: the verdicts, the resolved versions, the full delta, and today's bounded delta. The plan's own steps are the hand traces in §1 and §2.

### A. The intermediate-version history (§1)

```js
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import { version, computeForkMeet } from "@hyper-hyper-space/hhs3_mvt";
import { createMockRContext } from "./test/mock_rcontext.js";
import { RSchemaImpl, rSchemaFactory } from "./src/rschema/rschema.js";
import { RTableGroupImpl, rTableGroupFactory } from "./src/rtable_group/group.js";
import { deriveRowId } from "./src/rtable/hash.js";

const hs = createBasicCrypto().hash(HASH_SHA256);
const ctx = createMockRContext({ selfValidate: true });
ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);
const T = { p: "true" };
const open = (name, columns, extra) => ({ name, columns, restrictions: [{ on: "all", rule: T }], ...extra });
async function makeGroup(seed, tables, bindings) {
  const c = await createIdentity(SIGNING_ED25519, hs);
  const schema = await ctx.createObject(await RSchemaImpl.create({ name: seed + "_schema", creators: [{ keyId: c.keyId, publicKey: c.publicKey }], tables }));
  return ctx.createObject(await RTableGroupImpl.create({ name: seed, seed: seed + "-group", schemaRef: schema.getId(), schemaVersion: await (await schema.getScopedDag()).getFrontier(), ...(bindings ? { bindings } : {}) }));
}

const F = await makeGroup("fgrp", [
  { name: "perm", columns: { label: { type: "string", pub: true } }, restrictions: [{ on: "insert", rule: T }, { on: "delete", rule: { p: "exists", table: "gate", where: { k: "g" } } }], concurrentDeletes: true },
  open("gate", { k: { type: "string", pub: true } }, { concurrentDeletes: true }),
  { name: "targets", columns: { n: { type: "string" } }, restrictions: [{ on: "insert", rule: T }, { on: "delete", rule: { p: "exists", table: "perm", where: { label: "ok" } } }], concurrentDeletes: true },
  open("items", { t: { type: "string" } }, { fks: { t: "targets" } }),
  open("notes", { n: { type: "string" } }),
]);
const [perm, gate, targets, items, fnotes] = await Promise.all(["perm","gate","targets","items","notes"].map((t) => F.getTable(t)));
const p = await perm.insert("p-1", { label: "ok" });
const g = await gate.insert("g-1", { k: "g" }, undefined, version(p));
const x = await targets.insert("x-1", { n: "x" }, undefined, version(g));
const EF = await items.insert("i-1", { t: deriveRowId("x-1") }, undefined, version(x));
const s1 = await fnotes.insert("n-1", { n: "s1" }, undefined, version(EF));
const TF = await targets.delete(deriveRowId("x-1"), undefined, version(x));
const N1 = await perm.delete(deriveRowId("p-1"), undefined, version(s1));
const N3 = await gate.delete(deriveRowId("g-1"), undefined, version(s1));
const v1 = version(s1, TF), v2 = version(N1, TF), v3 = version(N1, N3, TF);
const vf = async (h, v) => (await F.isEntryVoided(h, v)) ? "void" : "live";
for (const [n, v] of [["v1", v1], ["v2", v2], ["v3", v3]]) console.log("F", n, "T_F", await vf(TF, v), "E_F", await vf(EF, v), "N1", await vf(N1, v));

F.setDeltaStrategy("full");
const fFull13 = await F.computeDelta(v1, v3);
const name = (h) => ({ [TF]: "T_F", [EF]: "E_F", [N1]: "N1", [N3]: "N3", [s1]: "s1" })[h] ?? h.slice(0, 6);
console.log("F full delta v1->v3 flips:", fFull13.opVerdictChanges.map((c) => name(c.entry)));
const fFull23 = await F.computeDelta(v2, v3);
console.log("F full delta v2->v3 flips:", fFull23.opVerdictChanges.map((c) => name(c.entry)));
const fRaw = await F.getCausalDag();
const fFork = await fRaw.findForkPosition(v1, v3);
const fMeet = await computeForkMeet(fRaw, fFork.common);
console.log("F meet(v1,v3):", [...fMeet].map(name));

const G = await makeGroup("ggrp", [
  open("refs", { item: { type: "string" } }, { fks: { item: "f.items" } }),
  open("notes", { n: { type: "string" } }),
], { f: F.getId() });
const refs = await G.getTable("refs");
const gnotes = await G.getTable("notes");
const O1 = await G.observe("f", version(s1));
const EG = await refs.insert("r-1", { item: deriveRowId("i-1") }, undefined, version(O1));
const Om = await G.observe("f", v1, undefined, version(EG));
const s1G = await gnotes.insert("gn-1", { n: "s1G" }, undefined, version(Om));
const O2 = await G.observe("f", v2, undefined, version(EG));
const O3 = await G.observe("f", v3, undefined, version(s1G));
const start = version(s1G, O2), end = version(O3, O2);
const gname = (h) => ({ [EG]: "E_G", [O1]: "O1", [Om]: "O_m", [O2]: "O2", [O3]: "O3", [s1G]: "s1G" })[h] ?? h.slice(0, 6);
console.log("G E_G start:", (await G.isEntryVoided(EG, start)) ? "void" : "live", "end:", (await G.isEntryVoided(EG, end)) ? "void" : "live");
const gRaw = await G.getCausalDag();
const gFork = await gRaw.findForkPosition(start, end);
const gMeet = await computeForkMeet(gRaw, gFork.common);
console.log("G meet:", [...gMeet].map(gname));
const fmt = (v) => [...v].map(name).sort();
console.log("refAtMeet:", fmt(await G.resolveObservedForeignVersion(F.getId(), gMeet, gMeet)), "refAtStart:", fmt(await G.resolveObservedForeignVersion(F.getId(), start, start)), "refAtEnd:", fmt(await G.resolveObservedForeignVersion(F.getId(), end, end)));
G.setDeltaStrategy("full");
const gFull = await G.computeDelta(start, end);
console.log("G full flips:", gFull.opVerdictChanges.map((c) => gname(c.entry)));
G.setDeltaStrategy("bounded");
const gB = await G.computeDelta(start, end);
console.log("G current bounded flips:", gB.opVerdictChanges.map((c) => gname(c.entry)), "bound:", [...gB.revisionBound].map(gname));
```

Output:

```text
F v1 T_F live E_F void N1 live
F v2 T_F void E_F live N1 live
F v3 T_F live E_F void N1 void
F full delta v1->v3 flips: [ 'N1' ]
F full delta v2->v3 flips: [ 'E_F', 'T_F', 'N1' ]
F meet(v1,v3): [ 's1' ]
G E_G start: live end: void
G meet: [ 's1G' ]
refAtMeet: [ 'T_F', 's1' ] refAtStart: [ 'N1', 'T_F' ] refAtEnd: [ 'N1', 'N3', 'T_F' ]
G full flips: [ 'E_G' ]
G current bounded flips: [ 'E_G' ] bound: [ 'O_m' ]
```

### B. The deploy-kill history (§2)

```js
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import { version, computeForkMeet, findRefAdvances } from "@hyper-hyper-space/hhs3_mvt";
import { createMockRContext } from "./test/mock_rcontext.js";
import { RSchemaImpl, rSchemaFactory } from "./src/rschema/rschema.js";
import { RTableGroupImpl, rTableGroupFactory } from "./src/rtable_group/group.js";
import { deriveRowId } from "./src/rtable/hash.js";

const hs = createBasicCrypto().hash(HASH_SHA256);
const ctx = createMockRContext({ selfValidate: true });
ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);
const open = (name, columns, extra) => ({ name, columns, restrictions: [{ on: "all", rule: { p: "true" } }], ...extra });
const admin = await createIdentity(SIGNING_ED25519, hs);
const schema = await ctx.createObject(await RSchemaImpl.create({ name: "kill_schema", creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }], tables: [
  open("items", { t: { type: "string" } }, { concurrentDeletes: true }),
  open("targets", { n: { type: "string" } }),
  open("refs", { item: { type: "string" } }, { fks: { item: "items" } }),
  open("notes", { n: { type: "string" } }),
] }));
const pinned = await (await schema.getScopedDag()).getFrontier();
const G = await ctx.createObject(await RTableGroupImpl.create({ name: "kill", seed: "kill-group", schemaRef: schema.getId(), schemaVersion: pinned }));
const [items, refs, notes] = await Promise.all(["items", "refs", "notes"].map((t) => G.getTable(t)));

const SD = await schema.updateSchema([{ rule: "set-fks", table: "items", fks: { t: "targets" } }], admin, "adopt fk", pinned);
const SN = await schema.updateSchema([{ rule: "set-concurrent-deletes", table: "items", value: false }], admin, "cd off", version(SD));

const R = await items.insert("r-1", { t: "missing" });
const E = await refs.insert("e-1", { item: deriveRowId("r-1") }, undefined, version(R));
const Dp = await G.deploy(version(SD), undefined, version(E));
const s1 = await notes.insert("n-1", { n: "s1" }, undefined, version(Dp));
const D = await G.deploy(version(SD), undefined, version(R));
const N = await G.deploy(version(SN), undefined, version(s1));
const raw = await G.getCausalDag();
const nm = (h) => ({ [R]: "R", [E]: "E", [Dp]: "D2", [s1]: "s1", [D]: "D", [N]: "N" })[h] ?? h.slice(0, 6);
const Dentry = await raw.loadEntry(D);
console.log("D meta keys:", Object.keys(Dentry.meta).sort(), "payload action:", Dentry.payload.action);

const start = version(s1, D), end = version(N, D);
const st = async (h, v) => (await G.isEntryVoided(h, v)) ? "void" : "live";
console.log("E start:", await st(E, start), "end:", await st(E, end), "| D start:", await st(D, start), "end:", await st(D, end));
const cdAt = async (h, v) => (await G.resolveSchemaView(version(h), v)).getConcurrentDeletes("items");
console.log("honor of D at start:", await cdAt(D, start), "at end:", await cdAt(D, end));

const fork = await raw.findForkPosition(start, end);
const meet = await computeForkMeet(raw, fork.common);
console.log("meet:", [...meet].map(nm));
const sdag = await G.getScopedDag();
console.log("deploy cover start:", [...await findRefAdvances(sdag, schema.getId(), start)].map(nm), "end:", [...await findRefAdvances(sdag, schema.getId(), end)].map(nm));
const sm = (v) => [...v].map((h) => h === SD ? "S_D" : h === SN ? "S_N" : "S0");
const schemaAtMeet = await G.resolveSchemaVersion(meet, meet), schemaAtEnd = await G.resolveSchemaVersion(end, end);
console.log("schema at meet:", sm(schemaAtMeet), "at end:", sm(schemaAtEnd));
const sdelta = await schema.computeDelta(schemaAtMeet, schemaAtEnd);
console.log("schema bound:", sm(sdelta.revisionBound));

G.setDeltaStrategy("full");
const full = await G.computeDelta(start, end);
console.log("full flips:", full.opVerdictChanges.map((c) => nm(c.entry)), "refs rows:", (full.tableChanges.get(refs.getId())?.rowChanges ?? []).map((r) => [r.liveBefore, r.liveAfter]));
G.setDeltaStrategy("bounded");
const b = await G.computeDelta(start, end);
console.log("current bounded flips:", b.opVerdictChanges.map((c) => nm(c.entry)), "bound:", [...b.revisionBound].map(nm));
```

Output:

```text
D meta keys: [ 'barrier', 'ref', 't-items-rows', 'tables' ] payload action: ref-advance
E start: void end: live | D start: live end: live
honor of D at start: true at end: false
meet: [ 's1' ]
deploy cover start: [ 'D', 'D2' ] end: [ 'N', 'D' ]
schema at meet: [ 'S_D' ] at end: [ 'S_N' ]
schema bound: [ 'S_D' ]
full flips: [ 'E' ] refs rows: [ [ false, true ] ]
current bounded flips: [] bound: [ 's1' ]
```
