# Observer-frame `ALLOW UPDATE REF`

> **Status: proposal, not implemented.** Today a binding's `ALLOW UPDATE REF`
> gate is evaluated in the observed group's frame
> ([`VOID_SEMANTICS.md`](../VOID_SEMANTICS.md) §5.5). This note was first
> written in the rpack module on 2026-10-21 and revised on 2026-10-29, after
> ideal concurrency, the bounded delta and multi-alias bindings landed.
> Section 8 lists what changed.

This note proposes evaluating `ALLOW UPDATE REF` (`canObserve`) in the observer's frame instead of the observed group's, so an observer can gate who advances its view of one bound group with capabilities held in another. Section 1 states the problem. Section 2 gives the design, and Section 3 argues it is sound. Sections 4 to 6 list the code, tests and docs it touches, and Section 7 the open questions.

---

## 1. The problem

A group `G` declares `ALLOW UPDATE REF <binding> IF <predicate>` to gate who may advance its observation of the group `H` that `<binding>` names. Today the predicate is read in `H`'s frame:

- An unqualified `t` is `H`'s table `t`, read at the candidate observed version $(\text{refAt}, \text{refFrom})$.
- A qualified `a.t` resolves `a` in `H`'s bindings and reads the table at the version `H` observes at `refAt`.

That works when `H` can see the capability table. A blog that binds `users` lets a comment group write `ALLOW UPDATE REF target IF EXISTS users.caps ...` in the blog's names.

Two layouts have no such path:

1. **A data group that binds nothing.** `G` binds `target => blog` and `auth => users`, and wants `users`' capabilities to decide who advances `target`. `blog` does not bind `users`, so no predicate in `blog`'s frame can name them.
2. **A group shared by another application.** If App B observes a group created by App A, that group knows nothing about App B's users. This layout does not exist yet: catalog groups are seeded from their own RDb (`seedSource: 'rdb'`), and shared groups are future work. It is the case that turns the observed frame from an inconvenience into a dead end.

Reading the gate in `G`'s frame, through `G`'s bindings and table maps, lets `G` define its permissions in its own namespace.

---

## 2. Design

Let `G` declare `ALLOW UPDATE REF b IF p`, with `G.bindings[b] = H`. The op being judged is an observe `y` of `H` in `G`.

### 2.1 Names

Every table in `p` is named through `G`'s bindings: `a.t`, with `a` an alias of `G` and `t` translated through `G.tableMaps[a]` (`resolveForeignTableName`). The source may write an unqualified `t` as shorthand for `b.t`. The compiler lowers it, so a stored predicate has only qualified atoms.

Lowering at compile time matters because several aliases can name one group id. `observeGateFor` (`src/rtable_group/group.ts`) conjoins the gates of every alias bound to `H`, and each alias has its own table map. A stored shorthand would have to be resolved per alias before the conjunction. A stored qualified atom carries its alias.

`G`'s own tables are not allowed. In the observer's frame an unqualified name would naturally mean a local table, so a stored observer-frame gate with an unqualified atom is invalid. The reason is a cycle. A local row op's verdict reads `G`'s observes through its foreign views. An observe whose verdict read local rows would close a loop through rows, which can be undecided, while the fold of observes (`resolveRefVersionAtPosition`'s `isLive`) needs a boolean.

### 2.2 Rules checked at creation

`G`'s bindings and gates are fixed in its create payload, so these are checked once, when the create payload is validated (`src/rtable_group/validate.ts`, `validate_ops.ts`) and when a catalog is validated (`src/rcatalog/validate.ts`):

1. Every atom of an observer-frame gate is qualified with an alias of `G`.
2. **The gate graph is a DAG.** Its nodes are bound group ids, with an edge $H \to K$ whenever a gate on an alias bound to $H$ names an alias bound to $K \neq H$. A cycle is rejected.
3. As today, a gate that reads `$author` needs `USING IDENTITIES`.

### 2.3 Versions read

An atom `a.t` with `G.bindings[a] = K` is read:

| | `K = H`, the binding being gated | `K ≠ H`, another bound group |
| :--- | :--- | :--- |
| **At use** (`diagnoseObserve`, horizon `from`) | at $(\text{refAt}, \text{refFrom})$, with `refAt` from `resolveObserveGateRefAt` (mvt's `widenRefAtUpward`), as today | through `resolveSchemaForeignTableViewIn(evaluation, a, t, {y}, from, true)`: the fold of `K`'s live observes in `y`'s history and concurrent with `y`, the read a row op's guard makes |
| **At admission** (`validateObserveGate`, position `at`) | at the imported version, (`newRefVersion`, `newRefVersion`), as today | at `G`'s observed version of `K` at (`at`, `at`), the read a row op's admission makes |

Only the binding being gated keeps the ref-advance's special contexts (`mvt/MVT_DEFINITION.md` §8, items 1 and 2). Every other binding is read like a guard reads a referenced object: in the use view at use, and at the op's own position at admission.

Admission becomes mixed. One gate reads `H` at the version being imported and `K` at the observer's own position. The std_types README calls a ref-advance [the one op admitted at another version](../../std_types/README.md#admission-and-use). For an observer-frame gate, that holds for the observed binding only.

### 2.4 Keeping today's meaning

The same text means different tables in the two frames. An unqualified name, and a qualified name that walked `H`'s aliases, would silently start reading other tables. Existing groups keep the observed frame, so the create payload records the frame:

- `canObserveFrame?: 'observer'` on `CreateTableGroupPayload` and on `CatalogGroupDef`, copied at instantiation. Absent means the observed frame, so existing payloads and catalog group hashes do not change.
- Evaluation (`evaluateObserveGateIn`), the static check (`groupViewReason`) and the compiler (`compileCatalogGroup`) branch on it.

The alternative, a new group type id, duplicates the type for one predicate's meaning.

Stored JSON and C-SQL parse back to the same payload, so C-SQL has to express both frames. Which frame a new group gets by default is open (Section 7).

---

## 3. Why it is sound

Take `G` binding `target => H` and `auth => K`, with a gate on `target` that reads `auth.caps`. `y` is an observe of `H` in `G` by author `a`, judged from horizon `from`.

### 3.1 No observe cycles

`observeVerdictLive` throws on an undecided observe verdict, so observes must never be on a verdict cycle. `y`'s verdict reads:

- observes of `H` in `G` whose versions are strictly above `y`'s in `H` (Layer 1, as today);
- observes of `K` in `G`, through the Layer 2 fold;
- entries of `H` and `K`, which are lower in the binding DAG.

An observe of `K` reads observes of `K` strictly above it, observes of bindings below `K` in the gate graph, and entries of lower groups. It never reads an observe of `H`, by rule 2 of Section 2.2, and rule 1 keeps local rows out. Every dependency therefore strictly descends the lexicographic order

$$\bigl(\text{rank of the object in the binding DAG},\ \text{rank of the binding in the gate graph},\ H(\text{observed version}) \text{ under } \subsetneq\bigr),$$

which is well-founded. Observes stay off cycles, and the folds stay boolean. This answers the open item "Observes that read each other" in `mvt/WELL_FOUNDED_VERDICTS.md` §6.5.

### 3.2 Revocations through another binding

Let $r$ be a live revoke in `K` of the capability `a` relies on, visible from `from` in `G`. Write $V$ for `y`'s version of `K` at use.

1. If $r \in H(V)$, Step 1 sees it, and `y` is void.
2. If $r \notin H(V)$ and $H(V) \not\subseteq H(r)$, then under ideal concurrency $r$ is concurrent with $V$ ([`IDEAL_CONCURRENCY.md`](../../mvt/IDEAL_CONCURRENCY.md) §4.4). Step 2 finds it at the horizon version where the capability table honors concurrent deletes, as for any guard read.
3. Otherwise $H(V) \subsetneq H(r)$. Every live observe of `K` that carries $r$ is concurrent with `y`, before it, or after it in `G`. Concurrent or before, the fold would put $r$ in $H(V)$. So each is after `y`: use-before-revoke in both DAGs, which no rule voids without clocks (`[XGROUP11]`). A void observe that carries $r$ does not count, as in every Layer 2 read.

So a revoke arriving through `auth` voids `y` exactly when it would void a row op of `G`, at `y`'s position, that reads `auth`. The case $K = H$ is the existing argument of [`VOID_SEMANTICS.md`](../VOID_SEMANTICS.md) §5.5, which ideal concurrency made exact.

The first version of this note expected the cross-binding case to need its own attack analysis. Since `ALL` was replaced it does not: the Layer 2 fold takes every live concurrent observe, not only dominating ones, so there is no mixed-cut gap to close.

Reading `K` in the use view has the consequences every guard read has:

- A grant on a branch of `K` that only a concurrent live observe imports counts for `y`. For the binding being gated it does not (`mvt/MVT_DEFINITION.md` §8, item 1).
- `y` sees through voided observes of `K`, so a back-dated import by a revoked observer of `K` contributes nothing (`[OBSGATE08]`'s rule, applied to `K`).

### 3.3 Imports used against honest observers

An observe of `K` only adds `K`'s own entries, judged by `K`'s own verdicts. It cannot forge a revoke. It voids an honest `y` only through cases 1 and 2: a live revoke of `y`'s author issued without seeing all of $V$. That is the honest-loss cost of ideal concurrency ([`IDEAL_CONCURRENCY.md`](../../mvt/IDEAL_CONCURRENCY.md) §5.3), the same one row ops pay. It holds even when `auth` is ungated and anyone who can write to `G` may observe `K`. Observing `K` before `H` (Section 4) narrows the window.

### 3.4 The bounded delta

`walkRevisionBound` (`mvt/src/revision_bound.ts`) was written for observe verdicts that read their own reference, and for the foreign views of row ops. An observer-frame verdict reads a second reference. The walk itself needs no change:

- An observe of `K` in the meet's history whose version is above `K`'s bound is handled by Phase A, which runs over every declared reference. Any `y` after it is then above the bound and walked.
- An observe of `K` concurrent with `y` is in `start`, since new entries are after the meet. It is below the bound and keeps its verdict, or it is walked. If it is unstable or flipped, the bound drops to the GLB with its predecessors, which removes `y` as well, because `y` is concurrent with it.
- The visitor records `y`'s own flip through `isEntryVoided` at both horizons.

The proof of Theorem 1 in [`INCREMENTAL_DELTA_BOUND.md`](../INCREMENTAL_DELTA_BOUND.md) covers the read of `K` as a foreign view anchored at `y`. The text has to change: assumption A3 lists "for a gated observe, its gate in the foreign group's frame", and guarantee 6 of `walkRevisionBound` describes a verdict as a read of its own reference. Both must say that a verdict may read other references through the full fold. The parity sweeps must cover observer-frame gates before this is relied on.

---

## 4. Changes

**rdb**

- `src/rtable_group/payload.ts`, `src/rcatalog/payload.ts`, `src/rdb/instantiate.ts`: the `canObserveFrame` field.
- `src/rtable_group/validate.ts`, `src/rtable_group/validate_ops.ts`, `src/rcatalog/validate.ts`: the rules of Section 2.2, for observer-frame groups.
- `src/rtable_group/group.ts`: `evaluateObserveGateIn` takes the observe's position and horizon (`opPos`, `from`) and branches on the frame. An atom on an alias bound to `H` reads `H` at (`refAt`, `refFrom`), and any other alias goes through `resolveSchemaForeignTableViewIn`. `diagnoseObserve` passes (`{y}`, `from`), and the public `evaluateObserveGate` gains `opPos`.
- `src/rtable_group/validate_ops.ts`: `validateObserveGate` passes `at`.
- `src/rcatalog/validate_ops.ts`: `groupViewReason` checks observer-frame gates through `G`'s bindings and table maps.
- `src/rdb/catalog_update.ts`: `applyCatalogPlan` observes a group's bindings in gate-graph order.

**rdb_lang**

- `src/compile/catalog.ts`: `compileCatalogGroup` resolves observer-frame gates in `G`'s scope, through its bindings and table maps, lowers shorthand per alias, and writes the marker.
- `src/reverse/source.ts`, `src/reverse/render.ts`: render the frame, so C-SQL parses back to the same payload.

**rdb_runtime**

- `src/session/ref_auto_update.ts`: observe in gate-graph order, and after `G` observes `K`, retry `G`'s observes of the bindings whose gates read `K`. Today a failed observe is retried only when the observed group changes again, so an observe of `H` refused because `G`'s view of `K` was stale stays refused.
- `src/session/authz_suggest.ts`: `evaluateObserveGateKey`, `resolveObserveAuthor` and the `update-ref` branches pass `G`'s position. `rdb_tools` re-exports them.

**mvt**: no code change (Section 3.4).

---

## 5. Tests

- `[OBSGATE]`, cross-binding:
  - a revoke in `K` imported by a concurrent observe of `K` voids a back-dated observe of `H`;
  - the same import after the observe of `H` leaves it live;
  - a grant imported only by a voided observe of `K` does not authorize;
  - benign concurrent observes of `H` stay live;
  - an observed-frame group with the same gate text keeps its old meaning;
  - two aliases of one group with different table maps.
- Creation: a gate-graph cycle, an unqualified atom and a local table are rejected.
- `[DBOUND]` and `test/delta_parity`: a side-branch observe of `K` that flips an observe of `H`, and a variant of the gated-observer generator whose gate reads a second bound group.
- rdb_runtime: ordering and retry in `ref_auto_update` and `applyCatalogPlan`.
- rdb_lang: compile, static check and C-SQL round trip in both frames.

---

## 6. Docs to update when it lands

- [`VOID_SEMANTICS.md`](../VOID_SEMANTICS.md) §5.5: the two layers for a gate that reads a second group, and the new reason observes are never on a cycle (Section 3.1). The current reason, that an observe's verdict reads only the observed group, stops being true.
- [`INCREMENTAL_DELTA_BOUND.md`](../INCREMENTAL_DELTA_BOUND.md), assumption A3, and guarantee 6 in `mvt/src/revision_bound.ts`.
- [`rpack/README.md`](../../rpack/README.md): how gates are read, replacing the paragraphs that describe this as a follow-up.
- `mvt/MVT_DEFINITION.md` §8 and the std_types README's "Admission and use": the mixed admission of Section 2.3.
- `mvt/WELL_FOUNDED_VERDICTS.md` §6.5: close "Observes that read each other".

---

## 7. Open questions

1. **The default for new groups.** Either new groups get the observer frame, and C-SQL gains syntax for the observed frame so existing groups still render, or the observed frame stays the default and the observer frame is an opt-in clause. A related choice is whether new catalogs may still create observed-frame groups at all.
2. **Today's frame reads the observed group's bindings without the void filter.** For a qualified atom, `evaluateObserveGateIn` calls `resolveForeignTableViewIn(..., refAt, refFrom, false)`, while `H` reads the same binding with the filter when it judges its own entries. A voided observe of `users` in `blog` therefore still counts for `G`'s gate. It looks low-impact and may be intended, but nothing says so. Observer-frame groups do not have it. Observed-frame groups keep it unless it is fixed separately.

---

## 8. Since the first version

- **Ideal concurrency replaced `ALL`** ([`DESIGN_AND_PLAN_FOR_ANY.md`](./DESIGN_AND_PLAN_FOR_ANY.md)). The §5.5 argument is now exact for the binding being gated, and the cross-binding case reduces to guard reads (Section 3.2). The head-advancing widening and cut intervals of [`OBSERVED_CUT_REVOCATION.md`](./OBSERVED_CUT_REVOCATION.md) are not needed.
- **Several aliases can name one group** (`aliasesForId`, `observeGateFor`). Shorthand is lowered per alias at compile time (Section 2.1).
- **The bounded delta walks with `walkRevisionBound`.** Its argument covers this design, with text changes (Section 3.4).
- **`widenRefAtUpward` moved to mvt** and is shared with RSet's gated ref-advances. Nothing here changes it.
- **Added:** the retry in `ref_auto_update`, the `authz_suggest` callers, C-SQL rendering of the frame, and the honest-loss note (Section 3.3).

---

## Implementation guardrail

If an unforeseen problem, ambiguity or architectural conflict comes up during implementation, stop and report it: the issue, the affected code paths and the options. Do not make significant design or protocol decisions unadvised.
