# Void semantics: cycles, negation, and the road to stratified resolution

This document records the reasoning behind RTableGroup's entry-voiding
computation (`isEntryVoided` / `resolveVoidDetail` / `diagnoseEntryVoidedClosure`
in [src/rtable_group/group.ts](src/rtable_group/group.ts)). It explains what the
code does today (deny the whole cycle; a memo that lives and dies with one
computation, never across computations), *why* that is the sound and
replica-convergent choice, and what a more sophisticated future implementation
would look like. The current implementation is deliberately basic; this is the
reference for anyone who later wants to make it cleverer without re-deriving the
theory.

## 1. Void computation is a logic program with negation

An entry is VOID when a row op it carries fails its restriction predicate or
writes an FK column whose target is not live, evaluated at-use at the op's own
position observed from the view's `from`. That computation recurses, and the
recursion is exactly a **normal logic program** (Datalog with negation):

- **Facts.** The genesis create entry is fiat — never voided. Ref-advances
  carry no row restrictions.
- **Positive (monotone) dependencies.** A restriction/`exists` witness: op X is
  supported because some witness row is live, whose own insert is itself gated,
  and so on. FK reach: op X is live only if its FK target row is live. More
  liveness below can only *help* X — monotone.
- **Negative (non-monotone) dependency.** A barrier delete D that revokes the
  authorizing cap of op X. D is *effective* only while D's own authorizing cap
  is live, i.e. only while D is **not** voided. So `voided(X)` depends on
  `¬voided(D)`. That negation is the whole source of difficulty.

Writing liveness in terms of the attacker's liveness, a mutual revoke is the
classic even negation loop:

```
live(capA) = ¬effective(deleteB→A) = ¬live(capB)
live(capB) = ¬effective(deleteA→B) = ¬live(capA)
```

## 2. "barrier" is a transport tag, not "negative edge"

It is tempting to equate `barrier` with "non-monotone link." Do not. `barrier`
is the generic transport mechanism for "a concurrent op may revise an at-use
verdict" (`findConcurrentCoverWithFilter` with `barrier:['t']`). Three things
are barrier-tagged, but only one is negation:

- **schema deploy** (ref-advance of the schema ref) — only *adds* restrictions /
  columns: **monotone**.
- **foreign-group observe** (ref-advance of a bound group id) — *widens* the
  observed foreign version: **monotone** as a version-mover, BUT see below.
- **delete of an authorizing cap** — removes support for a dependent op:
  **non-monotone** (the negation).

An **ungated** observe is monotone (it only widens the observed version). A
**gated** observe (a binding that declares `canObserve`) is **non-monotone**:
its own liveness depends on the negation of a concurrent revoke of the
observation's author in the observed group. That negation is resolved by a
**local stratification on the observed group's version** (§5.5) rather than by
the deny-the-whole-cycle collapse — so a gated observe is the one place the
engine treats an observe edge as negative. The polarity still comes from the
type (the binding declares the gate), never from the `barrier` tag.

Equating barrier with negative edge would over-stratify: it would drag the
monotone deploy/observe revisions into the expensive negation-resolution path
and lose the cheap least fixpoint that handles the overwhelmingly common case.
Negation is a *semantic overlay* on a specific subset of barriers; the type
classifies the edge, the engine must not guess from the tag.

## 3. Positive vs negative fragment, and cycles

- **Positive (monotone) fragment.** No negation in a cycle. Always has a unique
  least fixpoint. A positive cycle (e.g. a self-granting op that is its own
  witness, or a mutual-grant ring, `[PERM07]`) resolves to DENY: the least
  fixpoint of liveness grants nothing that isn't rooted in a genesis fiat fact.
  An FK reference cycle resolves to DENY identically.
- **Negative (non-monotone) fragment.** Negation inside a cycle. No single least
  fixpoint:
  - **even negation cycle** (2-party mutual revoke) — two stable models
    (each single-survivor);
  - **odd negation cycle** (3-party revoke ring) — no stable model at all (the
    `a ← not a` liar paradox).

This is textbook logic-programming / database-theory territory. The standard
semantics are: **well-founded semantics** (unique 3-valued model, polynomial,
odd loops come out *undefined*), **stable-model / answer-set semantics**
(2-valued, possibly many or none; existence is NP-complete), and the equivalent
**Dung abstract argumentation frameworks** (grounded ≈ well-founded, stable
extension ≈ stable model; an odd attack cycle has no stable extension).

## 4. What ships today: deny the whole cycle, no cross-computation cache

`isEntryVoided` keeps a transient cycle guard: a set of
`createOpId|entryHash|fromKey` keys carried in a per-computation `VoidClosure`
(see [src/rtable_group/void_closure.ts](src/rtable_group/void_closure.ts)),
added before recursing and removed in `finally`. On a back-edge it returns
`true` — **the entire cycle is treated as voided**, both positive and negative.
This is the conservative least fixpoint for the positive fragment and a
deliberate, safe collapse for the negative fragment. The keys are
group-namespaced (prefixed by `createOpId`) so one closure flowing across a
bound-group boundary keeps each group's marks distinct — an A→B→A ring still
DENIES without a shared cross-group guard.

Worked example — 2-party mutual revoke (`[PERM12]`): co-admins A and B
concurrently revoke each other.

```
isVoided(deleteB→A)            # is A's revoker voided?
  -> needs live(capB)
     -> live(capB) needs isVoided(deleteA→B)
        -> needs live(capA)
           -> live(capA) needs isVoided(deleteB→A)   # BACK-EDGE -> true
```

Each top-level query bottoms out on its own back-edge and voids the queried
revoke, so `hasRow(capA)` and `hasRow(capB)` **both return live**: both revokes
are nullified, both caps survive. Same outcome for the N-party ring. It is not
the "single survivor" a stable-model semantics would pick — it is the safe
all-survive (for revokes) / all-deny (for grants) collapse.

### Why there is deliberately NO cross-computation cache

A memo keyed only by `(entryHash, from)` and kept on the group instance is
**unsound for replica convergence**. With negation in a cycle, the value
computed for a shared node depends on which back-edge the traversal closed
first. A position-keyed cache serves that traversal-dependent intermediate to a
later independent query, so the final answer depends on query order — and query
order can differ across replicas. Removing the cache makes each top-level
computation self-contained and a pure function of `(entry, from)`: every
replica agrees. The guard is transient (per-computation) precisely so it can
detect a cycle *within* one computation without persisting anything *across*
computations — which is exactly why the visiting set lives in a per-computation
`VoidClosure` and not on the group instance (see §5).

A previous iteration shipped an instance-level `(entry, from)` cache plus a
2-party seniority special case (senior cap survives). It was removed because the
cache broke convergence and the special case only covered the isolated 2-cycle.
The analysis in §6 is the principled version that special case was reaching for.

### The per-computation memo: `VoidClosure.completed`

The prohibition above is about caching *across* computations. *Within* one
computation the closure memoizes finished verdicts. Diagnosing an entry reads
the DAG through the path below; those reads ask `entryVoided` of other entries.
`visiting` is a DFS stack, popped in `finally`, so a finished subtree is
forgotten the moment it completes. Unmemoized, every path through the
dependency DAG re-diagnoses shared nodes — exponential in the number of paths
(column-tag peel, `EXISTS` / FK diamonds, a query that `getRow`s many rows that
share writes). Depth stays small, so `VOID_MAX_INFLIGHT` does not catch it.

**Reads.** Liveness and column values are DAG covers, not a scan of every write
that touched a row. Entry meta carries two indexes (table-scoped keys `rows` /
`cols`, stored on the group DAG as `t-<table>-rows` / `t-<table>-cols`):

- **Identity** (`liveInsert` / `hasRow`): cover of `rows` among entries that
  are not voided. Insert and delete carry this tag; updates do not. Any leftover
  **delete** → the row is dead; otherwise the max-hash **insert**. A concurrent
  delete can still kill via `killedByConcurrentDelete` (same not-voided
  predicate).
- **Values** (`resolveColumn`): cover of `cols` among entries that are not
  voided. Insert and update carry this tag for every column they write.
  Concurrent maxima tiebreak by larger entry hash. `getRow` still starts at
  `liveInsert`; `resolveColumn` does not ask whether the row is live.

A **plain** cover (`findCoverWithFilter` with no predicate) stops at the causal
maxima that match the tag. If that maximum is voided, two things go wrong: its
write must not count, and it must not *hide* a valid write below it. The
predicate `!entryVoided` makes the cover **see through** voided matches: a
tagged entry that fails the predicate is treated like a non-match, and the walk
continues to its predecessors. The leftover maxima are the latest *valid*
writes. Example: `insert → U₁ → U₂(voided)` at `U₂`'s position — the plain
`cols` cover is `{U₂}`; the see-through cover is `{U₁}`. Tiebreaks, incarnation
scoping, and `liveRowIds` live in `view.ts`. Diagnosing an entry **is** those
`entryVoided` calls on cover candidates.

**Why diagnose recurses.** An update or delete restriction — including the
default `rowAuthor = $author` — is evaluated against the subject row, so
diagnosing \(U\) or \(D\) calls `getRow` at that op's own position. `getRow` =
identity cover + per-column covers. Each cover candidate is `entryVoided`,
which is another diagnose. `EXISTS` / FK do the same for other rows. The
dependency graph is therefore "this op's restriction / FK / `EXISTS`" → "void
verdicts of the entries those covers return," not "every prior toggle of this
row."

A **self-hit** is when `entryVoided` is asked about the entry already on top of
`visiting`:

- **Delete \(D\):** the identity cover at `version(D)` includes \(D\) (deletes
  carry the identity tag), so `entryVoided(D)` runs while \(D\) is still on the
  stack.
- **Update \(U\):** the identity cover does **not** include \(U\). The self-hit
  is the column-tag cover: \(U\) wrote `cols`, `resolveColumn` asks
  `entryVoided(U)` while \(U\) is still `visiting`, peels \(U\) as voided, and
  continues below.

Either hit is at the same structural point in every evaluation of that key.

**The memo.** `VoidClosure` carries, next to `visiting`:

- `completed: Map<key, OpVoidDetail | undefined>` — finished diagnose results
  (`undefined` = live; `has(key)` vs `get(key) === undefined` distinguishes
  uncached from diagnosed-live);
- `stack: string[]` — the same keys as `visiting`, in DFS order, so the current
  frame (`stack.top`) is known;
- `foreignCycleHits: number` — a monotonic counter, described below.

`resolveVoidDetail` (the single body behind `isEntryVoidedClosure` and
`explainEntryVoidedClosure`, so boolean and explain cannot drift) is:

```
completed.has(key)   ->  return the stored verdict            (no new frame)
visiting.has(key)    ->  DENY; if stack.top !== key then foreignCycleHits++
                                                              (never stored)
otherwise            ->  push; diagnose; store iff foreignCycleHits is
                         unchanged since the push; pop (in finally)
```

**Why it is sound.** A frame's verdict is a pure function of its key with one
exception: a descendant may ask about a key that is currently `visiting` and
receive the transient deny. `completed` hits are stack-independent by
induction, so that deny is the *only* channel through which the shape of the
stack can influence a verdict. Classify it by who is being asked about:

- *Self hit* (`stack.top === key`). The op under diagnosis asked about itself
  from inside its own subtree (identity cover for a delete; column-tag cover
  for an update — see **Reads** / **Why diagnose recurses**). This happens at
  the same structural point in *every* evaluation of that key, fresh or nested,
  so the verdict is still stack-independent and may be stored. Not storing it
  would forfeit the memo on the column-tag peel (every update that `getRow`s)
  and on delete liveness.
- *Foreign hit* (`stack.top !== key`). A strictly lower ancestor was assumed
  voided. Every frame from that ancestor up to the top now depends on the stack
  shape; all of them are open when the counter increments and all compare it at
  their end, so none of them store. Frames that finished before the hit, or
  opened after it, are unaffected and store normally.

The ancestor that was hit must be excluded too, not only the frames above it.
The mutual revoke of `[PERM12]` on a single view shows why:

```
hasRow(capA)
  isVoided(deleteB→A)                    push  [deleteB]
    live(capB)?  concurrent cover: deleteA→B
      isVoided(deleteA→B)                push  [deleteB, deleteA]
        live(capA)?  concurrent cover: deleteB→A
          isVoided(deleteB→A)            visiting; top = deleteA ≠ deleteB
                                         -> FOREIGN hit, DENY
        capA live  ->  deleteA NOT voided          (not stored)
    capB dead  ->  deleteB VOIDED                  (not stored)
  capA survives

hasRow(capB)                             symmetric, from scratch
  -> deleteA VOIDED, capB survives
```

A fresh `isEntryVoided(deleteB)` says *voided*; inside `deleteA`'s frame the
same key comes out *live*. Cycle participants are root-dependent — that is what
the least-fixpoint collapse means — so storing either verdict and reusing it
from the other side yields a single survivor and disagrees with an independent
evaluation of the same entry. (The first attempt at this memo stored every
finished verdict and failed `[PERM12]` exactly this way.)

**What is and is not stored.** Stored: every verdict in an acyclic subtree,
including the column-tag peel \(U_1 … U_j\) — \(U_j\) only sees writes at or
below its own position, so no later or concurrent write can produce a foreign
hit inside it. Not stored: the in-stack deny itself; any frame open during a
foreign hit — the cycle participants and, conservatively, every frame below
them on the stack (those verdicts are in fact root-independent; tracking the
minimum hit depth instead of a counter would let them store, but cycles are
rare and small and their acyclic children still memoize, so the simpler rule
ships); and nothing on throw, since the `set` follows the `await`. A cache hit
is not a frame: it does not call `enterVoidFrame` and does not push.

**Cost.** With the memo, one diagnose per `(group, entry, from)` per
computation. `[VOID_MEMO01]` (one authored insert, 24 authored updates, then
`getRow` + `query` on one view) is the regression net for that path. The public
`isEntryVoided` / `explainEntryVoided` still mint a fresh closure per call, so a
per-entry sweep such as the REPL's `LOG` pays \(O(n)\) per line, \(O(n^2)\)
overall — acceptable; threading one closure through such sweeps is a separate
change.

**Constraint: one closure, one sequential traversal.** `stack.top` identifies
the asker only if the closure is never shared by *interleaved* traversals.
Today every void check in `view.ts` is sequentially awaited and the rdb engine
contains no `Promise.all`; independent computations mint their own closure
(`[OBSGATE07]`). Parallelizing per-row `getRow` on a single view would already
break `visiting` (false cycles between siblings) and would additionally let a
foreign hit be misread as a self hit and stored. If parallelism is ever wanted
there, mint one closure per parallel branch.

Group delta's op channel attaches structured void reasons via `explainEntryVoided`
(a sibling of `isEntryVoided`; both go through `resolveVoidDetail`, so they share
one traversal, one memo, and cannot disagree).

## 5. Reentrancy: the per-computation VoidClosure

The visiting set is a **per-computation value**, not an instance field. An
earlier version kept it as `RTableGroupImpl._voidVisiting` (one `Set` per group
instance) and relied on the invariant "void computations are never interleaved
per group instance." That invariant was NOT enforced by the engine and did not
hold: two independent top-level evaluations on the same *cached* group instance
(e.g. a synchronizer validating a payload while another validates a gated
observe, both anchored at the same version) interleave at `await` points, and
because `Replica.getObject` returns one shared instance, one computation could
observe the other's transient visiting mark and falsely detect a cycle — voiding
a live witness row and rejecting a valid write nondeterministically.

The fix carries the visiting set in a `VoidClosure`
([src/rtable_group/void_closure.ts](src/rtable_group/void_closure.ts)) minted at
each top-level entry (`isEntryVoided` / `explainEntryVoided` / `getView` /
`resolveForeignTableView` / `evaluateObserveGate`) and threaded — mandatory —
through every `*Closure` helper and every `RTableViewImpl` those helpers build
(the constructor requires it). Interleaved computations get distinct closures,
so they cannot cross-talk; the type system enforces threading, since every
internal function and the view constructor take a non-optional `closure`.
Convention (greppable): a `*Closure` body may only call other `*Closure` helpers
or `new RTableViewImpl(..., closure)`, never a minting wrapper. The same closure
also carries the per-computation memo of §4 (`completed`, `stack`,
`foreignCycleHits`); those fields die with the computation for the same reason
the visiting set does, and they assume the closure is driven by one sequential
traversal.

The residual hole the types cannot close is internal code accidentally calling a
minting wrapper (legal TypeScript). A dropped closure does not corrupt a verdict;
on cyclic data it recurses forever through fresh closures — an unbounded
microtask chain that never yields, so a `setTimeout` test timeout would never
fire. A per-instance in-flight frame counter (`_voidInflight`, bumped on each
`isEntryVoidedClosure` / `explainEntryVoidedClosure` entry, released in
`finally`) throws past `VOID_MAX_INFLIGHT`, turning that hang into an immediate,
self-explaining error. It is a fail-safe, never a verdict: additive under
concurrency and far above any legitimate recursion depth.

## 5.5 The gated observe: stratification by the observed version

A `canObserve` gate (declared in rdb_lang as `ALLOW UPDATE REF <binding> IF ...`)
authorizes who may advance the observation of a bound foreign group `G`. It is
enforced in two layers:

- **Layer 1 — the gate (non-monotone).** Decides whether an observe op is a
  *live ref-advance*. The gate predicate is evaluated in `G`'s frame at the
  observed version the op resolves to. This is what introduces the negation: a
  concurrent revoke of the observation's author in `G` can flip the op's
  liveness.
- **Layer 2 — reference resolution (monotone).** The effective observed foreign
  version is the **union** of the live observes' versions; the cross-group view
  then asks `G` for the target's liveness at that union. This layer is unchanged
  in semantics; at view-time it merely *skips voided observes*
  (`resolveRefVersionAtPosition`'s `isLive` filter). A former principal's
  authority in `G` is neutralized here, monotonically: once any co-observed
  forward advance carries the revoke, the union carries it, and a later import
  of an older `G`-branch cannot remove it.

### Why Layer 1's recursion is acyclic (the stratifying coordinate)

The at-use gate widens the observed version **G-upward only**: a concurrent
observation barrier `z` (publishing `Vz`) widens the cut of observe `y`
(publishing `Vy`) **iff `Vz` strictly dominates `Vy` in `G`'s DAG** and `z` is
itself live. The negation we must enforce — a revoke `Rk(author(y))` that voids
`y` — rides, under **use-before-revoke** (the revoke is causally *after* the
published `Vy`), a version `Vz ⊋ Vy`. So every negative edge strictly increases
the `G`-version. A cycle would require `V₁ ⊋ V₂ ⊋ … ⊋ V₁`, impossible in a
strict partial order. Hence the dependency graph is **locally stratified by the
`G`-version**: a single sweep, evaluated implicitly latest-`G`-version-first by
the recursion, has a unique perfect model. No alternating fixpoint, no
oscillation. The reference pointer being monotone in the observer is exactly the
coordinate the general delete+barrier case lacks (a mutual revoke there has no
version pinning the negative edge to a causal direction), which is why a simple
stratification exists *here* but not in general.

### What this buys

- **Benign concurrent observes** (G-incomparable versions, no revoke): neither
  is G-above the other, so neither recurses into the other — both live, and the
  closure's visiting guard never fires. (Equating `barrier` with negative edge
  would have over-stratified and voided both — see §2.)
- **Back-dated former-principal observe** (attack 1): the legit revoke-import
  publishes a strictly-G-greater version, so it widens the back-dated op's cut
  to include the revoke; the op's own gate then fails — voided.
- **Back-dated *newer* state to void others** (attack 2): the malicious import
  is itself widened by the live forward advance above it (which carries the
  attacker's own revoke), so the malicious observe is voided and excluded from
  honest ops' anchors; the honest observe stays live.

### The residual core

The stratification only fails to *enforce* a revoke that is `G`-**concurrent**
to the version it would void (a genuinely concurrent revoke, not a
use-before-revoke). The gate declines it at Layer 1 — but this is sound, not a
gap: it is not a "former" principal (he was not yet revoked at publish time),
and Layer 2's monotone union neutralizes his authority the instant the revoke is
co-observed. A third-party concurrent cross-revoke (C revokes A, D revokes B,
cross-carried) is the one irreducible even cycle with `G`-incomparable versions;
it has no causal stratification and falls back to the existing
**deny-the-whole-cycle** collapse via the closure's visiting set (all-survive,
convergent), exactly as a mutual intra-group revoke does (§4). The guard
therefore remains as a backstop, but in the entire use-before-revoke regime it
is dormant.

### Where it lives

`diagnoseObserveVoidedClosure`, `resolveObserveGateRefAtClosure` (the G-upward
filtered widening), `evaluateObserveGateClosure` (frame rebasing into `G`), and
the `filterVoided` path of `resolveForeignTableViewClosure` in
[src/rtable_group/group.ts](src/rtable_group/group.ts). The MVT
`resolveRefVersionAtPosition` `isLive` hook ([modules/mvt/src/refs.ts](../mvt/src/refs.ts))
is the generic seam for Layer 2.

## 6. Future direction (not implemented)

The principled resolution that turns "deny the whole cycle" into "resolve each
cycle to a unique, canonical model" is **SCC-stratified well-founded evaluation
with a total value order to break negation cores**:

1. **Build the dependency graph** of the reachable entries, each edge tagged
   positive or negative (the type supplies this; the engine never reads the
   `barrier` tag to infer polarity).
2. **Condense into SCCs** (Tarjan) and process them in dependencies-first
   (reverse-topological) order. The condensation is a DAG, so this terminates
   and never re-opens a settled SCC. Disjoint cycles are separate SCCs; chained
   cycles feed verdicts upward; overlapping cycles collapse into one SCC and are
   resolved together.
3. **Within each SCC, monotone-saturate first** (least fixpoint / alternating
   fixpoint). A negative edge fires only in its already-grounded direction
   (source LIVE ⇒ target VOID; source VOID ⇒ attack inert); an attack whose
   source is still undecided contributes nothing. Saturation often shrinks or
   dissolves the SCC: a node with an independent live witness, or whose attacker
   was killed by a lower stratum, settles before any negation is consulted.
4. **Resolve the residual negation core by a total value order.** Whatever is
   still undecided after saturation is a genuine negation cycle. Select the
   canonical stable model greedily, most-senior-first: set the most-senior
   contestant LIVE, propagate (its effective deletes void the junior caps it
   targets), repeat. With a *total* order this yields a unique extension for even
   cycles and a deterministically *imposed* answer for odd cycles (which have no
   stable model). This is exactly a **value-/preference-based argumentation
   framework**: a total preference order guarantees a unique extension even in
   the presence of odd cycles.

The outcome is independent of both the DFS entry point and the order in which
SCCs are tackled — determinism comes from (a) the canonical SCC decomposition
and (b) the value order being *genuinely total*, not from any clever traversal
order. A canonical SCC processing order (e.g. min-member-hash tiebreak) is worth
pinning anyway as a reproducibility/safety belt, and every resolver must stay a
pure function of its inputs.

### The value (seniority) order

The seniority key must be the **authorizing cap's insert position** (fixed at
grant time), not the revoke op's position — otherwise an attacker who controls
how they sequence revokes could grind the result. "Ancestor wins; concurrent
breaks by entry hash" is the canonical linear extension of the causal order.

Two caveats for a general engine:

- **Cross-DAG totality.** A single DAG's `findForkPosition` totally-orders
  positions *within* that DAG. Across DAGs (cross-group FK rings) two cap inserts
  are incomparable by any one fork, so the comparator must specify an explicit
  inter-DAG tiebreak (e.g. dag-id then hash). Uniqueness for N-party / cross-type
  cycles rests entirely on this tiebreak.
- **Non-grindability is the type's obligation.** A generic engine can guarantee
  the order is total and deterministic; it cannot guarantee it is non-grindable.
  That property comes from feeding it the cap-insert position + rowId, both fixed
  at grant time and neither controlled by the later attacker.

### Where it would live

The mechanism (a causal total order `compareCausalPositions`, and a generic
tabled/stratified fixpoint engine parameterized by `deps`, `evalLocal`, and
`value` oracles) is type-agnostic and is a natural fit for the MVT library, so
every type gets deterministic cycle resolution for free. The *policy* (which
edges exist, which are negative, and what the value of a node is) is
irreducibly the type's job and stays in rdb.

## References

- Van Gelder, Ross, Schlipf — *The well-founded semantics for general logic
  programs* (alternating fixpoint; unique 3-valued model).
- Gelfond, Lifschitz — *The stable model semantics for logic programming*
  (answer sets; even loops → multiple models, odd loops → none).
- Apt, Blair, Walker / Przymusinski — stratification and the perfect model for
  (locally) stratified programs.
- Chen, Warren — SLG resolution / tabling (XSB); why naive tabling over negation
  is unsound — the failure mode the cache reproduced.
- Dung — *On the acceptability of arguments* (abstract argumentation frameworks).
- Bench-Capon — *Value-based argumentation frameworks* (a total value order
  yields a unique extension even with odd cycles).
