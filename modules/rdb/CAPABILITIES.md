# Capabilities in Rdb — why RCap's B1/B2 rules are not needed

This note records a design conclusion: a capability system built from ordinary
Rdb rows + at-use `exists` restrictions reproduces [RCap](../std_types/src/types/rcap)'s
grant/revoke semantics **without** RCap's two-barrier admissibility rules (the
"B1 / B2" split in [`rcap/view.ts`](../std_types/src/types/rcap/view.ts)
`hasCapability`). Those rules are an artifact of RCap's representation, not a
fundamental primitive; the Rdb write-once-witness encoding dissolves them.

The standard Users module (`src/users/users.ts`) is built on this model — do not
port RCap's B1/B2 admissibility rules over.

## Background: the two representations

**RCap** models a capability as a **mutable relation keyed by `(cap, grantee)`**.
Grant and revoke ops for the *same key* interleave over the DAG, so a view must
decide which op wins under concurrency. RCap's `hasCapability` also recursively
checks that each grant's author was itself authorized **as of that grant op's
own version** (a view pinned at `version(hash)`), so a later revoke of the
author's managing cap does not retroactively invalidate the grant
(use-before-revoke).

**Rdb** models a grant as an **immutable, write-once witness row** (rowIds are
`hash(uuid, owner)`, deletes are permanent — see [`rtable/hash.ts`](src/rtable/hash.ts)).
"Holds cap X" is the existence predicate `exists caps where owner=$author and
label='X'`, evaluated **at-use**: at the using op's own position, observed from
the view horizon. Authority to *grant* is just the caps table's `insert`
restriction (e.g. `exists manager-caps where owner=$author`).

## The key mechanism: recursive drop-on-void at use

`exists` is not a shallow check. The `exists` case of predicate evaluation calls
`view.existsMatching(...)` ([`rtable_group/predicates.ts`](src/rtable_group/predicates.ts)),
which re-checks every candidate witness through the **fully enforced** row
liveness ([`rtable/view.ts`](src/rtable/view.ts)):

```ts
const candidates = await this.candidateRowIds(where);   // index field + payload prefilter
return someTruth(candidates, (rowId) => this.candidateMatch(rowId, where));   // values, then liveness
```

Row liveness applies (1) permanent-delete liveness, (2) restriction drop-on-void
(the entry's verdict, anchored at the op's own position), and (3) FK reach. So a
witness row counts **only if its own insert restriction held at its own
position** — which is itself an `exists`-over-caps, recursively. The full chain
of grants that must be valid is therefore already enforced, decomposed across
each link's insert restriction rather than written as one monolithic predicate.

Consequence: **"only a manager may grant" is written once** as the caps table's
`insert` restriction, and transitive delegation validity falls out of the
recursive at-use re-evaluation. No richer restriction language is required to
express the chain.

## Why "the at-grant check looks like the at-use check for the grant op"

Because it *is* the same operation. A grant is itself a **use** of the
manager's authority. RCap's op verdict authorizes a grant on a view pinned at
the grant op's own version; Rdb re-evaluates each cap row's insert restriction
at that row's own position through the row's liveness, which reads the insert's
verdict. RCap writes the recursion explicitly in one function; Rdb writes it
implicitly, once per link, via drop-on-void. Same semantics, different
decomposition.

## Why B1 / B2 dissolve

RCap's two see-through barriers disambiguate concurrent grant-vs-revoke **on a
shared mutable key**:

- **B2 (use-anchored):** a revoke concurrent with the whole use point.
- **B1 (grant-anchored):** the concurrent-conflict tiebreak — when a grant and a
  revoke of the same key are mutually concurrent, **revoke wins**, even if the
  revoke is not concurrent with the entire (collapsed) use frontier.

Under the write-once-witness encoding there is no shared key to disambiguate —
each grant is its own existential witness and "holds" is a disjunction over live
witnesses — so the cases map to plain liveness:

| RCap case | Rdb mechanism |
| --- | --- |
| B1: revoke concurrent with the grant, but in `past(use)` | the revoke is a **permanent delete in `past(use)`** → the witness is simply dead at the use point. No tiebreak rule. |
| B2: revoke concurrent with the whole use | **`concurrentDeletes: true`** on the caps table → the barrier delete voids the witness at concurrent uses (proven by `[ENF07/08/09]`). Deletes are always barrier-tagged; this flag is resolved **at-use, at the delete's own position** (observed from the view `from`), and is the sole authority on whether the barrier is honored, so a flip concurrent with the delete still applies and B2 reach can even be enabled by a concurrent deploy (`[DEPLOY06]`). |
| re-grant after revoke | a **new witness row** (fresh uuid); "holds" = ∃ any live witness. |
| revoke the definition cascades to its grants | an **FK** from grant row → definition row → at-use op-voiding: a concurrent revoke of the definition voids the dependent grant at the merge; a causally-later one is inert. |
| creator roots | genesis `initialRows` (fiat, irrevocable). |

B1 in particular reduces to "a permanent/barrier delete kills that witness,"
which liveness already does. The B1/B2 distinction exists only to arbitrate
interleaved ops on a mutable relation; with immutable witnesses there is nothing
to arbitrate, so it has no Rdb counterpart.

## Design constraints (not RCap gaps)

1. **Cross-group caps use barrier observation.** Foreign-group `observe` is a
   barrier ref-advance: a foreign revoke or deploy *concurrent* with an at-use
   cross-group `exists` widens at the merged frontier and voids the use
   (`[XGROUP10]`, the cross-group analogue of `[ENF08]`), while a causally-later
   observation does not (`[XGROUP11]`, use-before-revoke). Cross-group caps
   therefore have the same B2 concurrent-revoke parity as intra-group caps.

2. **A cycle that nothing outside it decides is void.** The op-voiding
   recursion (cap A's insert needs cap B, B's needs A — and FK reach, where a
   write's FK target liveness depends on another entry's verdict) is solved as a
   logic program: entries that read each other form a component, solved to its
   well-founded fixpoint, and whatever that leaves undecided collapses to void,
   bottom-up ([VOID_SEMANTICS.md](VOID_SEMANTICS.md) §4). Restriction / `exists`
   recursion and FK reach go through the same evaluation, so a self-supporting
   grant ring or an FK reference cycle is void, and an odd revoke ring voids
   every revoke the same way an even one does. A cap that a ring member revokes
   outside the ring is decided by the ring's outcome, not collapsed with it
   (`[PERM17]`). RCap solves its grants and revokes with the same engine, keyed
   by op and `from`, so a ring's revokes do not stick there either: every holder
   on it keeps the cap.

## Beyond RCap parity: subject-row attenuation

RCap's everyday semantics are name-scoped caps and delegation chains, fully
expressible by the positive existence predicates above. Beyond that parity,
Rdb restrictions can now also reference the **subject row's own readonly
fields**, which RCap has no equivalent for:

- **`cmp` / `like` atoms** evaluate operand expressions over `$row.<col>`
  (readonly columns of the row being written), literals, exact arithmetic
  (`add` / `sub` / `mul` on `integer` / `bigint` / `decimal`, operands sharing a
  type family) and `len` — gating an op on its own immutable shape. Ordering
  comparisons are numeric for `integer` / `float` / `bigint` / `decimal` (bigint
  via `BigInt`, decimal via scaled-integer — never lexical over the canonical
  string carrier); `bytes` supports equality only. A write carrying a
  non-canonical / out-of-range / out-of-scale value is hard-rejected at write
  time (Layer-1), never rounded.
- **Correlated `exists`** — an `exists` `where` value may be `$row.<col>`, so
  "grant X only for resource R, where R is a *field of the row being written*"
  IS expressible: e.g. `exists grants where resource=$row.resource and
  owner=$author` (resource-scoped / attenuated caps; see `[PERM11]`).

References are confined to **readonly** columns deliberately: a readonly value
is fixed at insert, so the witness/operand is merge-stable (never
tiebreak-flippable by a concurrent winning write) and reading it never re-enters
the op's own value resolution — the same property that makes a `pub`+`readonly`
cap label a sound witness. The restriction grammar stays positive (no `not`); a
mutable subject-row field still cannot gate an op. The richer `not` / any-column
language lives only in the read-only `query` front-end (see the README), which
never feeds op-voiding.

## Bottom line

B1/B2 are an encoding artifact of RCap's mutable `(cap, grantee)` relation. The
write-once-witness + at-use `exists` model reproduces RCap's everyday and
use-before-revoke semantics via recursive drop-on-void, and reproduces B1/B2
intra-group via permanent deletes (`past(use)`) and `concurrentDeletes`
barriers (concurrent-with-use), with full B2 parity across the cross-group
observation boundary. Subject-row attenuation — once noted as the lone future
extension — is now implemented via readonly `$row.<col>` correlation, going a
step beyond name-scoped RCap parity.
