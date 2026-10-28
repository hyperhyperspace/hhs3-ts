# Lift the incremental bound walk into mvt

**Status:** implemented (`mvt/src/revision_bound.ts`, `[RBOUND01–10]`; rdb switched over with no test edited). This moves the bounded-delta walk of `RTableGroup` into mvt. rdb is its only caller after this change. RCap and RSet adopt it in follow-on changes, and that adoption is the prerequisite for Phase 5 ([`PHASE_5_PLAN_CRITIQUE_v2.md`](./PHASE_5_PLAN_CRITIQUE_v2.md) §2, [`RDB_OBSERVE_FIX_PLAN_v2.md`](./RDB_OBSERVE_FIX_PLAN_v2.md) step 7).

The walk, its terms and its proof are in [`INCREMENTAL_DELTA_BOUND.md`](../INCREMENTAL_DELTA_BOUND.md). This plan uses its words: history $H(P)$, strictly after, concurrent, meet, barrier, effect, stable, exit border.

---

## Why

RCap bounds its delta at the fork meet (`rcap/delta.ts`, `computeDeltaBounded`). RSet projects RCap's bound through `computeObserverRevisionBound` (`rset/delta.ts`, `computeDeltaBounded`). Both are unsound for a type whose views read barriers across branches. The local history in [`BOUND_SIMPLIFIED_SUMMARY.md`](./BOUND_SIMPLIFIED_SUMMARY.md) needs nothing but a barrier on a side branch, and H1 and H1b in the v2 critique fail on today's RSet with creator advances only.

The walk in `rtable_group/delta.ts` is the sound one. Lifting it before RCap and RSet adopt it means they run the code that `[DBOUND01–06]` and the group parity sweeps already cover, not a second and third copy.

---

## What mvt learns, and what it does not

mvt's contract (`mvt.ts`) describes objects read through views at `(at, from)` and the deltas between two versions. It does not know what a verdict is, and this change keeps it that way.

The walk knows three things:

- **Geometry.** The fork, the meet, the exit border, membership in $H(\text{start})$, the strictly-after test, and the lowering step.
- **Barriers.** An entry tagged `barrier` in its meta is one whose effect can reach entries it is concurrent with. Types already write that tag, and the concurrent-cover searches in the DAG index already filter on it.
- **References.** A ref-advance publishes a version of a referenced object (`refs.ts`). The host declares every reference along with the `from` its views read it at: the version observed at the horizon, or the published version itself. Only the first kind has a revision bound for the walk to use (`Delta.revisionBound`). The walk decides by itself whether a ref-advance's version lies within that bound.

Everything else belongs to the type, behind one callback. The walk asks one question of a barrier: did its effect on concurrent entries change between `start` and `end`? A verdict flip is one cause, and an rdb delete whose `concurrentDeletes` flag moved is another. The walk does not know which, and it never asks for a verdict.

---

## 1. The mvt module

New file `mvt/src/revision_bound.ts`, re-exported from `mvt/src/index.ts`.

```ts
// A referenced object, as the walk needs it.
export type ReferencedObject = {
    computeRevisionBound(start: Version, end: Version): Promise<Version>;
    getCausalDag(): Promise<CausalDag>;
};

// A reference of the object whose delta is bounded, by how its views read the
// referenced object at a version `v` this object observed.
export type Reference =
    // Read at getView(v, w), with w the version observed at the horizon. The
    // walk bounds it between the versions observed at `start` and at `end`.
    | {
        refId: B64Hash;
        from: 'horizon';
        getObject(): Promise<ReferencedObject>;
        // The canonical version this object observes at (horizon, horizon):
        // the `from` its views use on the referenced object.
        resolveVersion(horizon: Version): Promise<Version>;
      }
    // Read at getView(v, v): moving the horizon never changes it (§3, item 8).
    | { refId: B64Hash; from: 'at' };

// What the walk needs from the object whose delta it bounds. Fixed per object.
export type RevisionBoundHost = {
    getScopedDag(): Promise<ScopedDag>;
    // Every object this one references, each once, in a fixed order.
    references(): Reference[];
};

// One per walk. Owns everything the type does with a walked entry.
export type RevisionBoundVisitor = {
    // Called once for every walked entry, in walk order. When `candidate` is
    // true, the entry is a barrier of `start` that can lower the bound, and the
    // result says whether its effect on concurrent entries differs between
    // `start` and `end`. When it is false, the result is ignored.
    visit(entry: dag.Entry, candidate: boolean): Promise<boolean>;
};

export async function requireExtendingFork(
    rawDag: dag.Dag, start: Version, end: Version, caller: string,
): Promise<ForkPosition>;

export async function walkRevisionBound(
    rawDag: dag.Dag, host: RevisionBoundHost, visitor: RevisionBoundVisitor,
    fork: ForkPosition, start: Version, end: Version,
): Promise<Version>;
```

The walk returns the bound. Row changes, flips, or anything else a delta reports stay with the visitor and the accumulator it wraps.

`ReferencedObject` is structural. `RTableGroupImpl` already satisfies it. RObject does not gain `computeRevisionBound`.

### Calling it

Both of rdb's entry points follow this sequence, and RCap and RSet will too:

```ts
const fork = await requireExtendingFork(rawDag, start, end, "computeDelta");
if (fork.forkB.size === 0) {
    // `end` adds nothing: return an empty delta whose bound is fork.commonFrontier.
}
const acc = /* the type's accumulator */;
const bound = await walkRevisionBound(rawDag, host, /* a visitor over acc */, fork, start, end);
const changes = await acc.finalize();
// Report `bound` as the delta's revisionBound.
```

- **`requireExtendingFork`** runs `rawDag.findForkPosition(start, end)`, and throws `"<caller> requires END to extend START"` when `forkA` is non-empty. `caller` appears only in that message.
- **`fork`** must come from `requireExtendingFork` with the same `rawDag`, `start` and `end`. The walk does not recompute it, since the caller needs it anyway for the check and the early return.
- **An empty `forkB`** makes the walk return `fork.commonFrontier` without calling the visitor or any reference. This is new: today rdb returns early, so the walk never sees this case. With it, the walk's only precondition is the one above, and a caller that forgets the early return still gets the right bound. Callers keep the early return so they don't build an accumulator for nothing.
- **`rawDag`** is the root's raw DAG (§3, item 1). The walk loads entries there, reads their `barrier` tags there, and does all the geometry there. **`host.getScopedDag()`** is where Phase A searches for ref-advances, with `findCoverWithFilter` on `{ ref: [refId] }`. For a root object the two DAGs hold the same entries. The split comes from rdb as it is; whether to drop it is part of the nested-RSet question in §4.
- **The result** is the bound and nothing else. The caller reports it as `Delta.revisionBound`, or returns it from its own `computeRevisionBound`. Everything else a delta reports comes from the accumulator and the visitor.

### Why every reference is declared

If the host listed only the references to bound, leaving one out would mean "item 8 holds for it", and nothing would check that. A deliberate omission (rdb's schema) and a forgotten binding would look the same, and the forgotten binding's advances would silently read as stable. That is what `isUnstableObserve` does today for any ref id without a bound (`rtable_group/delta.ts`).

Declaring every reference with its read mode turns the exclusion into a statement the host makes, which the walk can check. The modes are stated in mvt's own terms: the `from` that the referenced view is read at.

### Why one callback

Today one loop body reads an entry's verdict at `start` and at `end`, then uses the same two booleans to report a flip and to decide the trigger. `isEntryVoided` mints a fresh evaluation on every call (`group.ts`, `topLevelVerdict`), so splitting reporting and the trigger into two callbacks would evaluate every old barrier twice in a delta: gated observes, deletes with `exists` rules, deploys. One `visit` per walked entry keeps a single evaluation with no memo. It is that loop body behind an interface.

### What moves unchanged

Every step below is what `walkWideningBound` does today, except the two declaration checks: the duplicate-id throw in Phase A, and the undeclared-id throw in Phase B.

**Phase A.**

- `references()` is read once, and a ref id declared twice throws.
- The meet is `computeForkMeet(rawDag, fork.common)`, and the bound starts there.
- For each `'horizon'` reference, in order: stop if the bound is empty. Otherwise get its object, and take its bound between `resolveVersion(start)` and `resolveVersion(end)`. Then lower the bound below each lowest unstable ref-advance of that id in $H(\text{meet})$. `'at'` references are skipped.
- The descent starts from the cover of `{ ref: [refId] }` at the meet, and stops at the first stable ref-advance on each branch. A lowest unstable ref-advance $r$ lowers the bound to $\mathrm{GLB}(\text{bound}, \mathrm{preds}(r))$, so $r$ is walked.
- `getObject()` is called lazily, inside the loop. A group missing from the replica throws only when the loop reaches it, as today. With an empty `start` the meet is empty, and nothing is fetched.

**Phase B.**

- The queue starts at `end`. `inStart` is seeded with `start`, `fork.commonFrontier` and `fork.common`, and extended to the predecessors of every walked member.
- The stop set is the exit border of $H(\text{bound})$ in $H(\text{end})$. An entry on it is recorded as stopped and not visited.
- Each visited entry is loaded, and its predecessors join `inStart` if it is a member.
- A visited ref-advance whose ref id is not declared throws, before `visit`. The check reads the raw payload and covers walked entries only; it is not an audit of the DAG.
- An entry is a **candidate** when the bound is non-empty, the entry is in `inStart`, and it carries the `barrier` tag. This is today's `canWiden`.
- The walk calls `visitor.visit(entry, candidate)` exactly once.
- A candidate triggers when `visit` returns true, or when it is a ref-advance of a `'horizon'` reference whose version is not at or below that reference's bound. A triggering entry lowers the bound only when its predecessors are not strictly after it. The stop set is then recomputed, and every stopped entry that left it is requeued.

**Trigger order.** Today it is the verdict flip, then stability, then the honor check. It becomes the visitor's answer (verdict, then honor), then stability. No entry is subject to both of the last two. The honor check only applies to entries with a `tables` meta, which observes never carry. The stability check only applies to ref-advances of `'horizon'` references, and rdb declares the schema `'at'`. The checks have no side effects, so the result is the same.

### Helpers

| Today in `rtable_group/delta.ts` | In `mvt/src/revision_bound.ts` |
| :--- | :--- |
| `walkWideningBound` | `walkRevisionBound` |
| `lowerBelowUnstableObserves` | `lowerBelowUnstableRefAdvances` |
| `isUnstableObserve` | `isUnstableRefAdvance` |
| `forkOf` | `requireExtendingFork`, exported |
| `computeStopSet`, `isStrictlyAfter`, `computeGLB`, `isBarrierEntry` | same names, module-private |
| `BoundGroupHost` | `ReferencedObject` |

`isUnstableRefAdvance` returns false for a ref-advance of an `'at'` reference, as `isUnstableObserve` does today for a schema deploy. Today's fallback also returns false for an id with no bound at all; that case becomes the Phase B throw.

`lowerBelowUnstableRefAdvances` and `projectForeignBound` are the same descent with two differences. `projectForeignBound` puts the lowest unstable advance into the bound, so that advance is not walked, and it falls back to `localAt` when nothing is unstable. Do not merge them here. `projectForeignBound` serves RSet's old floor and goes away when RSet adopts the walk.

---

## 2. The rdb side

**Host.** `rtable_group/delta.ts` builds a `RevisionBoundHost` from the group:

| Host member | From `GroupDeltaHost` |
| :--- | :--- |
| `getScopedDag` | `group.getScopedDag` |
| `references` | A `'horizon'` reference for each id in `new Set(Object.values(group.getBindings()))`, in that order, then `{ refId: group.getSchemaRef(), from: 'at' }`. The schema is `'at'` because `resolveSchemaView` reads `getView(resolved, resolved)` (INCREMENTAL_DELTA_BOUND.md §3). |
| `getObject`, on a binding | `group.getBoundGroup(refId)` |
| `resolveVersion(h)`, on a binding | `group.resolveObservedForeignVersion(refId, h, h)` |

No valid group DAG can trigger the throw for an undeclared ref id. `validateRefAdvance` rejects any ref id other than the schema and a bound group (`validate_ops.ts`), and the bindings are fixed by the create op (`getBindings`, `group.ts`).

**Visitors.** Two, one per entry point. Both compute `newDeploy`, whether a schema deploy is new between `start` and `end` (today's `deployCoverMoved`, with `sameSet`, which stays in rdb), before the walk. Moving that check ahead of Phase A changes nothing: it reads only the schema ref-advances at the two horizons.

The delta visitor ingests every entry, and reads verdicts for every entry that can have one:

```ts
async visit(entry, candidate) {
    await acc.ingest(entry);
    let flipped = false;
    if (isVoidCheckable(entry.payload)) {
        const voidBefore = await group.isEntryVoided(entry.hash, start);
        const voidAfter = await group.isEntryVoided(entry.hash, end);
        if (voidBefore !== voidAfter) { flips.push({ entry, voidBefore, voidAfter }); flipped = true; }
    }
    return candidate && (flipped || (newDeploy && await didDeleteHonorFlip(group, entry, start, end)));
}
```

The bound-only visitor does not ingest or collect flips, and reads verdicts only for a candidate. Together the two visitors reproduce today's condition: verdicts are read when there is an accumulator or the entry can lower the bound.

**Entry points.**

- `computeRTableGroupDelta`, bounded: run the walk with the delta visitor, then map the visitor's flips through `describeOpVerdictFlip` and sort by entry hash, as today. The records are the same: `explainEntryVoided` depends only on the entry and the horizon, so calling it after the walk gives the same reason.
- `computeRTableGroupRevisionBound`: run the walk with the bound-only visitor.
- `forkOf` becomes `requireExtendingFork`.
- `BoundGroupHost` stays exported as an alias of `ReferencedObject`, so `group.ts` keeps compiling.
- The header comment of `rtable_group/delta.ts` says the walk lives in mvt. The rdb-specific paragraphs stay, reworded: the schema is declared `'at'`, and the honor check is part of a barrier's effect.

---

## 3. The contract

The doc comment on `walkRevisionBound` states what a host and its visitor must guarantee for the theorem of INCREMENTAL_DELTA_BOUND.md §5 to hold. They are the assumptions A1–A6 and the effect of §3, stated in terms of views and barriers, with no mention of verdicts. RCap and RSet check this list when they adopt the walk.

1. **One root.** `rawDag` is the raw DAG of a root object, and its create entry is the only entry without predecessors.
2. **Determinism.** A view's reads at a horizon, and the visitor's answer for a candidate, are functions of the entries, `start` and `end`. Mutually dependent reads are solved together and deterministically.
3. **Cross-branch reads go through barriers.** A view at `(P, F)` reads entries outside $H(P)$ only through entries in $H(F)$ that carry the `barrier` tag. That includes every read the accumulator makes on the views at `start` and `end`.
4. **The visitor covers a barrier's effect.** For a barrier `T`, every view anchored at a position concurrent with `T` reads the same from `T` at `F = start` and at `F = end`, unless `visit(T, true)` returns true or `T` is an unstable ref-advance of a `'horizon'` reference.
5. **Every reference is declared.** Every ref id that appears in a ref-advance of the object is declared exactly once in `references()`, with the `from` its views read it at.
6. **Referenced versions.** A referenced object is read through views whose `at` folds the ref-advances in the reading entry's past and the ref-advance barriers concurrent with it. For a `'horizon'` reference, their `from` is `resolveVersion(horizon)`. Ref-advances are monotone and carry the `ref` meta tag with the ref id.
7. **Sound referenced bounds.** For a `'horizon'` reference, `ReferencedObject.computeRevisionBound` is sound for the exact pair of versions it is given. References form a DAG.
8. **References read at `(v, v)`.** An object declared `'at'` is read at a geometric fold, with no liveness filter, and its view at a version does not depend on `from`. The walk computes no bound for it and never finds its ref-advances unstable. rdb's schema is one: it has no barriers, its deploys are never void, and `resolveSchemaView` reads it at `getView(resolved, resolved)`. A new deploy's effect on a delete outside the bound reaches the walk through the visitor's honor check.

---

## 4. Will RCap and RSet fit?

This change does not touch them. Here is how each one fills the host and the visitor, to check the shape before it is fixed.

**RCap.**

- `references()` is empty. The walk reduces to the meet, lowered below every barrier of `start` concurrent with it whose effect changed.
- Revokes and delete-caps carry `barrier` (`rcap.ts`, `applyPayload`). The two concurrent reads in `rcap/view.ts` filter on that tag, so contract 3 holds on its face.
- The visitor ingests into the RCap accumulator, which only collects keys. For a candidate, it compares the barrier's verdict at the two horizons. That verdict needs a public entry point at a horizon; today it is the private `opVerdict` in `rcap/view.ts`.
- RCap gains `computeRevisionBound`, and with it the memo `group.ts` keeps. Lift the memo then, not here.

**RSet.**

- `references()` holds one `'horizon'` reference to the capability on a permissioned set, and is empty otherwise.
- Its `resolveVersion` is the canonical `resolveRefVersionAtPosition(setDag, ref, h, h)`.
- The accumulator already computes `wasValid` and `nowValid` for adds and deletes. The visitor reuses them for a candidate, so each authorization is evaluated once.
- After Phase 5, a ref-advance's effect also changes when its own verdict does. That is what lets the walk handle H2.

One open question belongs to the RSet change. Nested children append through `NestedScopedDag`, which wraps meta (`dag_nesting.ts`, `wrapMeta`). If a child's barrier does not carry the `barrier` tag on the raw entry, contract 3 fails for sets with a `contentType`. The fix belongs in the scope's meta wrapping, or in the RSet change. The walk keeps the tag test fixed. The same goes for the declaration check: it reads raw payloads, so a child's ref-advance that is wrapped on the raw entry gets past it.

---

## 5. Tests

**rdb is the check that the move changed nothing.**

- `[DBOUND01–06]` in `test/delta_bound_tests.ts`. Each checks the bounded delta against the full one, and `computeRevisionBound` against the delta's bound. That last check is also the check that the two visitors agree.
- `[DELTA_PARITY_GROUP]`, `[DELTA_PARITY_XGROUP]`, `[DELTA_PARITY_GATED]`, `[DELTA_PARITY_GATED_XGROUP]` and `[DELTA_PARITY_SHAPED]`, with the smoke profile (`npm test`) and the extended one (`npm run test:parity`).
- The rest of the rdb suite, including the `[OCUT]` and observe-gate tests.

No rdb test changes. A test that has to change means the move changed behavior.

**mvt gets its own suite**, `[RBOUND]` in `mvt/test/revision_bound_tests.ts`. It uses hand-built DAGs, like `delta_tests.ts`. The visitor is scripted: it answers from a set of entries whose effect changed, and records every `(entry, candidate)` call. The objects behind `'horizon'` references are stubs, as in `delta_tests.ts`, and they record their calls. These tests pin the walk for its next two callers, which have no parity sweep that builds these shapes yet.

- `[RBOUND01]` The history of BOUND_SIMPLIFIED_SUMMARY.md, with `T`'s effect changed. Put a non-barrier entry `T'` after `T` on the side branch, so `start` is `{s1, T'}`, and list `N` first in `end`. The walk then stops at `s1` before it reaches `T`, and only the requeue brings `s1` and `E` back. The bound is `{x}`, and `s1` and `E` are visited. Without `T'` the queue can reach `T` first, and the requeue never runs.
- `[RBOUND02]` The same history with nothing changed. The bound stays at the meet `{s1}`. The visited entries are exactly `N`, `T'` and `T`, and only `T` is a candidate. A new entry and a non-barrier are never candidates.
- `[RBOUND03]` Phase A. A `'horizon'` reference whose stub bound is below the version of a ref-advance in $H(\text{meet})$, with a stable ref-advance below it. The bound becomes the GLB of the meet and the unstable ref-advance's predecessors. The unstable ref-advance is visited, and the stable one stops the descent.
- `[RBOUND04]` The stub's `computeRevisionBound` is called with `resolveVersion(start)` and `resolveVersion(end)`, not with the version at the meet. This is the rule RSet's current helper gets wrong.
- `[RBOUND05]` Phase B. A ref-advance of `start` on a side branch, outside $H(\text{meet})$ and concurrent with an entry below it, the shape of `[DBOUND03]`. The visitor reports no change, its version is above the stub's bound, and the bound drops below it.
- `[RBOUND06]` A candidate whose effect changed but whose predecessors are already strictly after the bound does not lower it.
- `[RBOUND07]` On a history where the bound drops twice, `visit` is called exactly once per walked entry, including the requeued ones, and never with `candidate` true while the bound is empty.
- `[RBOUND08]` An `'at'` reference. A ref-advance of it on a side branch of `start`, outside $H(\text{meet})$, for which the visitor reports no change. The bound stays at the meet. The ref-advance is visited as a candidate, and nothing is asked for a bound for it.
- `[RBOUND09]` Declarations. A visited ref-advance whose ref id is not declared throws before `visit` is called for it. A ref id declared twice throws before anything is visited.
- `[RBOUND10]` With `end` equal to `start`, the walk returns `fork.commonFrontier`, and records no `visit` and no call to a reference.

**Mutations.** Run each once by hand and do not commit them.
- Taking the referenced versions from the meet must fail `[RBOUND04]` and `[DBOUND04]`.
- Dropping the requeue must fail `[RBOUND01]`. Do not count on `[DBOUND01]` for that: whether its walk stops at `s1` before it reaches `T` depends on queue order.
- Treating an `'at'` reference's ref-advances as unstable must fail `[RBOUND08]` and `[DBOUND06]`.
- Treating an undeclared ref id as stable, which is today's fallback, must fail `[RBOUND09]`.

**Build.** rdb resolves `@hyper-hyper-space/hhs3_mvt` through the package `main`, which is `dist/index.js`. Run `npm run build` in `modules/mvt` before the rdb tests or a typecheck sees the new exports.

---

## 6. Docs

- INCREMENTAL_DELTA_BOUND.md §6.1, "Where it lives": the walk and its helpers are in `mvt/src/revision_bound.ts`, and the host, the two visitors and the honor check are in `rtable_group/delta.ts`. §6.3's sentence on RSet and RCap stays true until they adopt the walk.
- INCREMENTAL_DELTA_BOUND.md §3, "The schema", last two sentences: say the schema is declared to the walk as read at `(v, v)`. Correct the claim that bounding it "would make every side-branch deploy look unstable". With the bound taken between the versions at `start` and at `end`, a deploy looks unstable only when RSchema's bound, the meet of its fork (`rschema/delta.ts`), drops below the version the deploy published. That happens when `end` deploys a schema version that merges schema branches forking below it.
- The comment "On the reported revision bound" in `mvt/src/delta.ts` points to `walkRevisionBound`, and says RSet and RCap still use the meet helpers.
- Do not edit `PHASE_5_PLAN.md` in this change.

---

## Considered and not taken

- **Two callbacks, one for the trigger and one for reporting.** That evaluates every old barrier twice in a delta, or needs a verdict memo shared between them. One `visit` per entry avoids both.
- **Verdicts in the host** (`hasVerdict`, `isEntryVoided`, a flip list returned by the walk). That puts rdb's op channel into mvt, and asks every other type to describe a barrier's effect as void or live.
- **A renamed barrier concept.** The `barrier` tag is already the convention every type writes and the DAG index filters on.
- **Listing only the references to bound** (a `referencedIds()` host member). Leaving a reference out would then mean that item 8 holds for it, and nothing could tell a deliberate omission from a forgotten one.
- **Bounding the schema as a `'horizon'` reference.** With RSchema's own bound this is sound but loses precision: that bound is RSchema's walk floor, and it drops below a deploy of `start` whenever `end` merges schema branches that fork below it (§6). A wrapper whose bound is always the version at `start` would be exact, but it would hide a read mode inside a bound.
- **Referenced objects from the view contract**, through `View.getReferences()` and `View.resolveRefVersion()`. rdb's view returns the right version for a binding at `(h, h)` (`rtable_group/view.ts`, `resolveRefVersion`). But a view does not say how each reference is read, and a group's `getView` resolves the schema eagerly. Explicit host members are a plain move.

## What this change does not do

- It does not change `walkEntriesBackwardsToBound` or `walkDelta`. RSchema, RSet and RCap still use them. The over-walk that stops only at the bound's heads is still what keeps some of their meet-bounded deltas right ([`SINGLE_PASS_INCREMENTAL_WIDENING_DELTA_BOUND_CRITIQUE.md`](./SINGLE_PASS_INCREMENTAL_WIDENING_DELTA_BOUND_CRITIQUE.md) §4).
- It does not change any bound of RSet or RCap. `computeObserverRevisionBound`, `combineObserverRevisionBounds` and `projectForeignBound` stay, and are retired when RSet adopts the walk.
- It does not move the `computeRevisionBound` memo, and does not add `computeRevisionBound` to `RObject`.
- It does not change any bound, delta or flip record of `RTableGroup`. rdb never reaches either new behavior. The throw for an undeclared ref id cannot fire on a valid group's DAG (§2), and both entry points return early before the walk sees an empty `forkB`.
- It does not remove the third evaluation behind each reported flip. `describeOpVerdictFlip` calls `explainEntryVoided` at the void horizon, while `topLevelVerdict` already returns the reason with the verdict. That is a separate rdb change.

## Order

1. Add `revision_bound.ts` with the walk and its helpers, export it, and add the `[RBOUND]` suite.
2. Switch rdb to it: the host, the two visitors, the entry points. Delete the moved code from `rtable_group/delta.ts`.
3. The two doc edits.

Ship it as one change.

## Exit

- The mvt suite, including `[RBOUND]`.
- The rdb suite and `npm run test:parity` in `modules/rdb`, with no test edited.
- The std_types suite. It does not call the walk, but it builds against the new mvt.
