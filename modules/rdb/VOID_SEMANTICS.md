# Void semantics: cycles, negation, and the road to stratified resolution

This document records the reasoning behind RTableGroup's entry-voiding
computation (`isEntryVoided` / `computeEntryVoided` in
[src/rtable_group/group.ts](src/rtable_group/group.ts)). It explains what the
code does today (deny the whole cycle, no cache), *why* that is the sound and
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
- **foreign-group observe** (ref-advance of a bound group id) — only *widens*
  the observed foreign version: **monotone**.
- **delete of an authorizing cap** — removes support for a dependent op:
  **non-monotone** (the negation).

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

## 4. What ships today: deny the whole cycle, no cache

`isEntryVoided` keeps a single transient cycle guard (`_voidVisiting`, a set of
`entryHash|fromKey` keys), added before recursing and removed in `finally`. On a
back-edge it returns `true` — **the entire cycle is treated as voided**, both
positive and negative. This is the conservative least fixpoint for the positive
fragment and a deliberate, safe collapse for the negative fragment.

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

### Why there is deliberately NO cache

A memo keyed only by `(entryHash, from)` is **unsound for replica
convergence**. With negation in a cycle, the value computed for a shared node
depends on which back-edge the traversal closed first. A position-keyed cache
serves that traversal-dependent intermediate to a later independent query, so
the final answer depends on query order — and query order can differ across
replicas. Removing the cache makes each top-level computation self-contained and
a pure function of `(entry, from)`: every replica agrees. The guard is transient
(per-computation) precisely so it can detect a cycle *within* one computation
without persisting anything *across* computations.

A previous iteration shipped a `(entry, from)` cache plus a 2-party
seniority special case (senior cap survives). It was removed because the cache
broke convergence and the special case only covered the isolated 2-cycle. The
analysis below is the principled version that special case was reaching for.

## 5. Reentrancy note

`_voidVisiting` is an instance field mutated across `await` points. JS is
single-threaded, so this is not a data race, but it *is* an async-reentrancy
hazard: two interleaved void computations on the same group instance would share
one set. Today nothing on the read path fans out concurrently (predicate
evaluation is strictly sequential `for … await`; there is no `Promise.all`), so
the invariant "void computations are never interleaved per group instance"
holds. If that ever changes, carry the visiting set as a per-computation value
(e.g. stored on the `RTableViewImpl` recursion context and threaded through the
two recursive view-construction sites in `computeEntryVoided` and
`resolveForeignTableView`) instead of as an instance field.

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
