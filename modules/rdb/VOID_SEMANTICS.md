# Void semantics: cycles, negation, and well-founded verdicts

This document records the reasoning behind RTableGroup's entry-voiding
computation (`isEntryVoided` / `resolveVerdict` / `diagnoseEntry`
in [src/rtable_group/group.ts](src/rtable_group/group.ts)). It explains what the
code does (well-founded verdicts, solved one strongly connected component at a
time, with a memo that lives and dies with one computation), *why* that is the
sound and replica-convergent choice, and what a more opinionated future
resolution of negation cycles would look like.

## 1. Void computation is a logic program with negation

**Void** and **live** say whether an entry takes effect. **Live** and **dead**
say whether a row is present. A write that fails validation and never enters
the DAG is **rejected**. A **verdict** is an entry's computed void-or-live
outcome as seen from a `from` horizon; inside one computation it can also be
**undecided** (§4). A **verdict evaluation** is the state of one top-level
computation: its answer table plus its stack, passed through every helper that
takes part in that computation.

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
  authorizing cap of op X. D is *live* only while D's own authorizing cap
  is live, i.e. only while D is **not** voided. So `voided(X)` depends on
  `¬voided(D)`. That negation is the whole source of difficulty.

Writing liveness in terms of the attacker's liveness, a mutual revoke is the
classic even negation loop:

```
live(capA) = ¬live(deleteB→A) = ¬live(capB)
live(capB) = ¬live(deleteA→B) = ¬live(capA)
```

## 2. "barrier" is a transport tag, not "negative edge"

It is tempting to equate `barrier` with "non-monotone link." Do not. `barrier`
is the generic transport mechanism for "a concurrent op may revise an at-use
verdict" (`findConcurrentCoverWithFilter` with `barrier:['t']`). Three things
are barrier-tagged, but only one is negation:

- **schema deploy** (ref-advance of the schema ref) — *adds* restrictions /
  columns: **monotone**. A deploy that adopts an FK also deletes the rows that
  don't honor it, but those rows are fixed when the deploy is applied and a
  deploy is never void, so its deletes depend on no verdict and no cycle runs
  through them.
- **foreign-group observe** (ref-advance of a bound group id) — *widens* the
  observed foreign version: **monotone** as a version-mover, BUT see below.
- **delete of an authorizing cap** — removes support for a dependent op:
  **non-monotone** (the negation).

An **ungated** observe is monotone (it only widens the observed version). A
**gated** observe (a binding that declares `canObserve`) is **non-monotone**:
its own liveness depends on the negation of a concurrent revoke of the
observation's author in the observed group. That negation is resolved by a
**local stratification on the observed group's version** (§5.5), so a gated
observe never joins a component — it is the one place the engine treats an
observe edge as negative. The polarity still comes from the type (the binding
declares the gate), never from the `barrier` tag.

Equating barrier with negative edge would over-stratify: it would drag the
monotone deploy/observe revisions into the negation-resolution path, where
nothing is gained. Negation is a *semantic overlay* on a specific subset of
barriers; the type classifies the edge, the engine must not guess from the tag.

## 3. Positive vs negative fragment, and cycles

- **Positive (monotone) fragment.** No negation in a cycle. Always has a unique
  least fixpoint. A positive cycle (e.g. a self-granting op that is its own
  witness, or a mutual-grant ring, `[PERM07]`) is void: the least fixpoint of
  liveness grants nothing that isn't rooted in a genesis fiat fact. An FK
  reference cycle is void the same way, with reason `fk`.
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

## 4. What ships: well-founded verdicts with component completion

Every verdict is the entry's value in the **well-founded model** of the
program above, computed lazily from the entry asked about. What the
well-founded model leaves undefined is then **collapsed to void**, bottom-up:
a cycle that nothing outside it can decide denies (reason `undecided-cycle`).
For revokes that means every cap on a revoke ring survives; for grants, that a
ring of mutual grants never takes effect. The engine is generic and lives in
mvt ([../mvt/src/verdict_evaluation.ts](../mvt/src/verdict_evaluation.ts)); rdb
binds it in [src/rtable_group/verdict_evaluation.ts](src/rtable_group/verdict_evaluation.ts),
and RCap (std_types) uses the same engine with its own atoms.

### Three values inside one evaluation

Inside one top-level evaluation an entry's verdict is either stored (final:
live or void) or belongs to the component being solved, where it can also be
**undecided**. Undecided comes only from entries of that component: one whose
diagnosis is still running, one that finished and is waiting for its root, or,
during the fixpoint, one that is possibly live but not known live. A completed
component has no undecided entries, so nothing outside it ever reads
undecided, and `isEntryVoided`, `explainEntryVoided`, the view reads and
`hasCapability` stay two-valued (they throw on undecided, which only a bug can
reach).

A *completion* of a state decides every undecided entry one way or the other.
A computation (a cover walk, a row's liveness, a column read, an atom, a
predicate, an entry's diagnosis) is **undecided** in a state when its rules
give neither true nor false there.

**Structure, never enumeration.** No rule tries the ways of deciding its
undecided inputs: that would be a tautology check, coNP-hard. A result is
decided only when the rule's structure proves it from the decided inputs:

- a Kleene connective with a deciding arm (a true arm decides an `or`, a false
  arm an `and`; an undecided arm decides nothing, so the others are still
  evaluated);
- a read-once formula, as in row liveness.

Everything else that meets an undecided input is undecided, even when every
completion would agree. Every rule must be **exact** (with no undecided entry
it gives the two-valued result), **sound** (a decided result is the result in
every completion) and **monotone** (deciding more entries never flips a
decided result or makes it undecided). `[VREAD06]` checks all three for every
read rule over every state of a small DAG.

Every undecided result carries its **support**: the undecided entries it rests
on. An undecided entry read has itself as support; a rule that returns
undecided unions the supports of its undecided inputs; a decided result has
none.

### The rules

- **Entry read:** live is true, void is false, undecided is undecided.
- **`and` / `or`:** Kleene. Order never changes a value: a short-circuit only
  happens on a deciding value. Order changes which reads happen, and so which
  entries get pulled into a component.
- **Row liveness** (identity cover, see **Reads** below): inserted, not
  deleted, and not killed by a concurrent delete, each the `or` over its
  entries. Each entry appears once, so this is decided exactly when every
  completion agrees. A definite delete makes the row dead whatever else is
  undecided.
- **Concurrent kill:** a live honored delete kills, unless an undecided
  unhonored one met by the walk could hide it; otherwise the `or` over the
  undecided honored deletes.
- **Column read:** if the walk met no undecided write that a live write does
  not dominate, the result is the two-valued one (the max-hash write in the
  cover, or the default). Otherwise it is undecided, with those writes as its
  support. Sound: any other write lies below a live write on every path. So
  with the insert writing `grantee` alice and an undecided update writing bob,
  every atom on `grantee` — `grantee == carol` included — is undecided
  (`[VREAD01]`), while an undecided update that does not write `grantee` never
  meets that read (`[VREAD02]`).
- **`cmp` / `like`:** undecided if an operand is.
- **`$row` atom:** the `and` of the subject row's liveness and the atom on its
  value, so a dead subject row still makes every `$row` atom false, and an
  undecided insert makes them undecided (`[VREAD03]`). The update overlay is
  static: a column the op itself writes is never undecided.
- **`exists`:** the `or`, over candidate rows, of "every field matches and the
  row is live". Candidates come from the index field, then a payload-only
  prefilter (each other field must be carried by some insert or update of the
  row, unless its default is the searched value), then values are checked
  before liveness. So a row that no write could make match is never read, and
  its verdict never joins a component (`[PERM19]`, `[PERM22]`). An `exists`
  whose where clause uses an undecided `$row` value is undecided.
- **Diagnosis:** the `and`, over the entry's ops in bundle order, of
  restriction then FK reach. False is void with the first failing reason in
  data order (bundle order, restriction before FK, schema FK order); an
  undecided check leaves the entry undecided unless a later one fails.

### An op is not its own witness or barrier

Views built to diagnose entry E read E as void. A grantee deleting their own
manager cap removes it (`[PERM16]`): the delete's restriction reads the
grantee's cap at the delete's own position, where the delete itself is in the
cover. The same rule keeps an update from seeing its own column writes when its
restriction reads the subject row. A bundle's own inserts and deletes still
count for its later ops through `localTargetProvided`, the sequential cut
inside a bundle.

### Components (Tarjan completion)

`evaluation.resolve(key, diagnose)` runs Tarjan's algorithm over the entries
the diagnoses read. Each frame has an **index** (the order in which its
diagnosis started) and a **lowlink** (the smallest index, among frames still on
the stack, that it reaches through its own reads and its children's).

- A read of a frame still on the stack lowers the reader's lowlink to that
  frame's index and returns undecided (support: that key).
- When a child returns, the parent takes the smaller lowlink.
- A frame that finishes with lowlink equal to its index is a **root**: nothing
  it depends on reaches an earlier open frame, so it and every frame above it
  on the stack form one complete component, solved there.
- A frame that finishes without being a root keeps its conditional answer on
  the stack; conditional answers are never stored.

A read during the solve can reach an entry not seen yet; it is resolved as a
normal frame. If it reads a member, it joins the component and the solve
restarts with it (**growth**). If it reaches a frame below the root, the
component was not complete after all: the root returns undecided to its reader
(**merge**), and the larger component is solved further down.

Keys are group-namespaced (`createOpId|entryHash|fromKey`), because one
evaluation flows into bound foreign groups. Bindings are fixed at creation by
group id, and a group's id is the hash of its create payload, which contains
its bindings — so bindings form a DAG, every cross-group read points down it,
and every component stays inside one group.

### Solving a component

1. **Alternating fixpoint, from all-undecided.** L is the set of members known
   live, Up the set possibly live. The lower step grows L with Up fixed; the
   upper step rebuilds Up from L. During the fixpoint a member read returns
   live (in L), void (outside Up) or undecided. It stops when Up stops
   shrinking; this is the well-founded fixpoint of the rules (approximation
   fixpoint theory). It is not Kripke–Kleene: `p ← p` comes out void.
2. **Bottom-up collapse.** While members are undecided, build the graph from
   each undecided member to the undecided members in its support, collapse its
   bottom strongly connected components (cycles that depend on no other
   undecided entry) to void with `undecided-cycle`, and re-solve the rest. A
   dependent of a collapsed core is then decided by the core's void, instead of
   being collapsed with it.
3. **Finish.** Every member is evaluated once more, all values decided, to get
   its final reason; the component is stored and popped.

**Reasons.** A reason is the first failing check in data order under the
final verdicts. It can be a consequence of the entry's own voidness: in
`[PERM20b]` a bundle is decided void by its dead FK target, which keeps the
counter revoke of its author live, which in turn fails the bundle's first op.
The reported reason is that first op's restriction, the same in every query
order.

### Worked example — 2-party mutual revoke (`[PERM12]`)

Co-admins A and B concurrently revoke each other: `deleteA→B` deletes capB,
`deleteB→A` deletes capA.

```
hasRow(capA): concurrent kill deleteB→A
  resolve(deleteB→A)                      push, index 0
    restriction: exists manager cap of B -> liveness(capB): kill deleteA→B
      resolve(deleteA→B)                  push, index 1
        restriction: liveness(capA): kill deleteB→A
          resolve(deleteB→A)              on stack: lowlink(deleteA→B) = 0,
                                          undecided {deleteB→A}
        diagnosis: undecided {deleteB→A}
      not a root (lowlink 0): undecided {deleteA→B}; lowlink(deleteB→A) = 0
    diagnosis: undecided {deleteA→B}
  root: solve {deleteB→A, deleteA→B}
    fixpoint: L = {}, Up = {deleteB→A, deleteA→B}   (each is possibly live
              only while the other is not known live)
    collapse: deleteB→A <-> deleteA→B is a bottom component -> both void
    finish: both void, undecided-cycle
capA survives

hasRow(capB): deleteA→B is answered void -> capB survives
```

Same outcome for the N-party ring (`[PERM13]`–`[PERM15]`), and for every entry
point and query order: the fixpoint starts from all-undecided, so it discards
the first pass's conditional answers. It is not the "single survivor" a
stable-model semantics would pick (§6) — it is the safe all-survive (for
revokes) / all-deny (for grants) collapse.

### Worked example — an outsider revoked by a 3-ring member (`[PERM17]`)

Admins a0, a1, a2 form a revoke ring (`R0` a0 revokes a1, `R1` a1 revokes a2,
`R2` a2 revokes a0), and a0 concurrently revokes an outsider o (`Ro`).

```
hasRow(cap o): concurrent kill Ro
  resolve(Ro)                             push, index 0
    restriction: exists manager cap with grantee a0
      candidates: label = manager, prefiltered by grantee = a0 -> {cap a0}
      liveness(cap a0): kill R2
        resolve(R2) -> R1 -> R0 -> R2     back-edge; R2 is the root (index 1)
        solve {R2, R1, R0}: all undecided (odd ring), one bottom component
          -> all void, undecided-cycle
      cap a0 live -> exists true -> Ro live (a singleton component)
cap o is dead
```

The ring is solved as its own component: Ro reads it but nothing in it reads
Ro, so Ro is decided by the ring's void instead of being collapsed with it.
Before the prefilter, each ring member's candidate scan read the liveness of
every manager cap, o's included, which closed a spurious cycle through Ro.

### Reads

Liveness and column values are DAG covers, not a scan of every write that
touched a row. Entry meta carries two indexes (table-scoped keys `rows` /
`cols`, stored on the group DAG as `t-<table>-rows` / `t-<table>-cols`):

- **Identity** (`rowLiveness` / `hasRow`): cover of `rows` among live entries.
  Insert and delete carry this tag; updates do not. A schema deploy carries it
  for each row it deletes on FK adoption, and reads as a delete. A live **delete** in the
  cover → the row is dead; otherwise the max-hash live **insert**. A concurrent
  delete can still kill (`killedByConcurrentDelete`).
- **Values** (`columnValue`): cover of `cols` among live entries. Insert and
  update carry this tag for every column they write. Concurrent maxima
  tiebreak by larger entry hash. `getRow` starts at liveness; `columnValue`
  does not ask whether the row is live.

A **plain** cover (`findCoverWithFilter` with no predicate) stops at the causal
maxima that match the tag. If that maximum is void, two things go wrong: its
write must not count, and it must not *hide* a live write below it. The
liveness predicate makes the cover **see through** void matches: a tagged
entry that fails the predicate is treated like a non-match, and the walk
continues to its predecessors. Example: `insert → U₁ → U₂(void)` at `U₂`'s
position — the plain `cols` cover is `{U₂}`; the see-through cover is `{U₁}`.
Each read is one such walk: it treats undecided entries as void and records
them, and the rules above turn the live cover plus the recorded entries into a
three-valued result. Tiebreaks, incarnation scoping, and `liveRowIds` live in
`view.ts`.

**Why diagnose recurses.** An update or delete restriction — including the
default `rowAuthor = $author` of a group with an identity provider — is
evaluated against the subject row, so diagnosing \(U\) or \(D\) reads the
subject at that op's own position: identity cover plus per-column covers. Each
cover candidate's verdict is another diagnosis. `EXISTS` / FK do the same for
other rows. The dependency graph is therefore "this op's restriction / FK /
`EXISTS`" → "verdicts of the entries those covers meet," not "every prior
toggle of this row."

### The per-computation memo: `answers`

Within one computation the evaluation stores the verdicts of completed
components only. Unmemoized, every path through the dependency DAG re-diagnoses
shared entries — exponential in the number of paths (column-tag peel,
`EXISTS` / FK diamonds, a query that `getRow`s many rows that share writes).
With the memo, each `(group, entry, from)` is diagnosed once per computation
outside a solve. A solve re-runs its members' diagnoses: a fixpoint over a
component of n entries takes at most 2n outer rounds of at most n passes over
n members, and there are at most n collapse rounds. Nothing branches over
truth assignments. `[VOID_MEMO01]` (one authored insert, 24 authored updates,
then `getRow` + `query` on one view) is the regression net for the acyclic
path.

The memo dies with the computation. A completed verdict is a pure function of
`(group, entry, from)`, so an instance-level map of completed verdicts, kept
across `getView` / `LOG` lines, would be sound, but it is not implemented. The
public `isEntryVoided` / `explainEntryVoided` mint a fresh evaluation per call,
so a per-entry sweep such as the REPL's `LOG` pays \(O(n)\) per line,
\(O(n^2)\) overall. A cache of anything *uncompleted* — a conditional answer,
or a member's value inside a solve — would be unsound: it depends on the
traversal, and so on query order, which can differ across replicas.

Group delta's op channel attaches structured void reasons via
`explainEntryVoided` (a sibling of `isEntryVoided`; both go through
`resolveVerdict`, so they share one traversal, one memo, and cannot disagree).

**Constraint: one evaluation, one sequential traversal.** The running stack
identifies the reader of each `resolve`, so an evaluation must never be shared
by *interleaved* traversals. Every verdict read in `view.ts` is sequentially
awaited and the rdb engine contains no `Promise.all`; independent computations
mint their own evaluation (`[OBSGATE07]`). If parallelism is ever wanted, mint
one evaluation per parallel branch.

### Why it converges

- Kleene connectives are commutative, and a short-circuit only happens on a
  value that decides the connective.
- The well-founded fixpoint is unique, and the solve starts from
  all-undecided, so it discards conditional answers that depend on the
  traversal.
- The collapse graph is built from supports, which depend only on values.
- Arm order and entry point can still change which entries are pulled into a
  component. An entry pulled in through an arm that did not matter is in no
  member's support, so it cannot change any verdict.

## 5. Reentrancy: the per-computation VerdictEvaluation

The evaluation is a **per-computation value**, not an instance field. An
earlier version kept its visiting set as `RTableGroupImpl._voidVisiting` (one
`Set` per group instance) and relied on the invariant "void computations are
never interleaved per group instance." That invariant was NOT enforced by the
engine and did not hold: two independent top-level evaluations on the same
*cached* group instance (e.g. a synchronizer validating a payload while another
validates a gated observe, both anchored at the same version) interleave at
`await` points, and because `Replica.getObject` returns one shared instance, one
computation could observe the other's transient visiting mark and falsely
detect a cycle — voiding a live witness row and rejecting a valid write
nondeterministically.

The fix carries that state in a `VerdictEvaluation`
([src/rtable_group/verdict_evaluation.ts](src/rtable_group/verdict_evaluation.ts)) minted at
each top-level entry (`isEntryVoided` / `explainEntryVoided` / `getView` /
`resolveForeignTableView` / `evaluateObserveGate`) and threaded — mandatory —
through every helper that receives it and every `RTableViewImpl` those helpers
build (the constructor requires it). Interleaved computations get distinct
evaluations, so they cannot cross-talk; the type system enforces threading,
since every such helper and the view constructor take a non-optional
`evaluation`. A method that receives a `VerdictEvaluation` must pass that same
evaluation on. It must never call a minting wrapper (`isEntryVoided`,
`explainEntryVoided`, `resolveForeignTableView`, `evaluateObserveGate`, or the
table's `getView`). The evaluation also carries the memo of §4 (`answers`) and
the component stack; both die with the computation and assume the evaluation is
driven by one sequential traversal.

The residual hole the types cannot close is internal code accidentally calling a
minting wrapper (legal TypeScript). A dropped evaluation does not corrupt a verdict;
on cyclic data it recurses forever through fresh evaluations — an unbounded
microtask chain that never yields, so a `setTimeout` test timeout would never
fire. A per-instance in-flight counter (`_verdictInflight`, bumped in
`enterVerdictFrame` on each diagnose run, released in `finally`) throws past
`VERDICT_MAX_INFLIGHT`, turning that hang into an immediate, self-explaining
error. It is a fail-safe, never a verdict: additive under concurrency and far
above any legitimate recursion depth.

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
  then asks `G` for the target's liveness at that union. At view-time it *sees
  through voided observes* (`resolveRefVersionAtPosition`'s `isLive`
  predicate), like a column read sees through voided writes: a voided observe
  contributes nothing and does not hide the live observes below it
  (`[OBSGATE08]`). A former principal's authority in `G` is neutralized here,
  monotonically: once any co-observed forward advance carries the revoke, the
  union carries it, and a later import of an older `G`-branch cannot remove it.

### Why Layer 1's recursion is acyclic (the stratifying coordinate)

The at-use gate widens the observed version **G-upward only**: a concurrent
observation barrier `z` (publishing `Vz`) widens the cut of observe `y`
(publishing `Vy`) **iff `Vz` strictly dominates `Vy` in `G`'s DAG** and `z` is
itself live. The walk over concurrent barriers sees through a void candidate
to the observes below it, so a void observe cannot hide a live revoke-import
(`[OBSGATE09]`); liveness is asked only of candidates strictly above `Vy`, and
a candidate that is not stops the walk, since write-time monotonicity puts
everything below it at or below its own version. The negation we must enforce — a revoke `Rk(author(y))` that voids
`y` — rides, under **use-before-revoke** (the revoke is causally *after* the
published `Vy`), a version `Vz ⊋ Vy`. So every negative edge strictly increases
the `G`-version. A cycle would require `V₁ ⊋ V₂ ⊋ … ⊋ V₁`, impossible in a
strict partial order. Hence the dependency graph is **locally stratified by the
`G`-version**: a single sweep, evaluated implicitly latest-`G`-version-first by
the recursion, has a unique perfect model. The reference pointer being monotone
in the observer is exactly the coordinate the general delete+barrier case lacks
(a mutual revoke there has no version pinning the negative edge to a causal
direction), which is why a simple stratification exists *here* but not in
general.

**Observes are never on a cycle.** An observe's verdict reads only `G`, which
is further down the binding DAG, and observes strictly above it in `G` (the
Layer 1 widening, a strict order). Neither reaches back to the observe, so when
a fold asks for an observe's verdict, the observe's frame completes as a
component of its own, and the folds stay boolean. The engine asserts this: an
undecided observe verdict throws, because it would mean bindings stopped
forming a DAG. If bindings ever become mutable, this needs revisiting.

### What this buys

- **Benign concurrent observes** (G-incomparable versions, no revoke): neither
  is G-above the other, so neither recurses into the other — both live, with
  no component formed. (Equating `barrier` with negative edge would have
  over-stratified and voided both — see §2.)
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
cross-carried) is the one irreducible even cycle with `G`-incomparable versions.
It is a cycle inside `G`: `G`'s component solve leaves it undecided and
collapses it to void (all survive, convergent), exactly as a mutual intra-group
revoke (§4). In the entire use-before-revoke regime no such component forms.

### Where it lives

`diagnoseObserve`, `resolveObserveGateRefAt` (the G-upward
filtered widening), `evaluateObserveGateIn` (frame rebasing into `G`), and
the `filterVoided` path of `resolveForeignTableViewIn` in
[src/rtable_group/group.ts](src/rtable_group/group.ts). The MVT
`resolveRefVersionAtPosition` `isLive` hook ([modules/mvt/src/refs.ts](../mvt/src/refs.ts))
is the generic seam for Layer 2.

## 6. Future direction (not implemented): a seniority order for negation cores

What §4 collapses to void is exactly the residual negation core: entries the
well-founded model leaves undefined, in cycles that nothing outside them
decides. Collapsing them is the conservative choice (every cap on a revoke ring
survives). A more opinionated resolution would instead pick a canonical model
for each core by a **total value order**: set the most-senior contestant live,
propagate (its live deletes void the junior caps it targets), repeat. With a
*total* order this yields a unique extension for even cycles and a
deterministically *imposed* answer for odd cycles (which have no stable model).
This is exactly a **value-/preference-based argumentation framework**: a total
preference order guarantees a unique extension even in the presence of odd
cycles. It would replace step 2 of the solve; the components, the fixpoint and
the read rules stay as they are.

### The value (seniority) order

The seniority key must be the **authorizing cap's insert position** (fixed at
grant time), not the revoke op's position — otherwise an attacker who controls
how they sequence revokes could grind the result. "Ancestor wins; concurrent
breaks by entry hash" is the canonical linear extension of the causal order.

Two caveats for a general engine:

- **Cross-DAG totality.** A single DAG's `findForkPosition` totally-orders
  positions *within* that DAG. Components stay inside one group today (§4), but
  a type whose cycles can span DAGs would need an explicit inter-DAG tiebreak
  (e.g. dag-id then hash), and uniqueness for such cycles would rest entirely on
  it.
- **Non-grindability is the type's obligation.** A generic engine can guarantee
  the order is total and deterministic; it cannot guarantee it is non-grindable.
  That property comes from feeding it the cap-insert position + rowId, both fixed
  at grant time and neither controlled by the later attacker.

The order would be a `value` oracle passed to the mvt engine next to `diagnose`:
the mechanism stays type-agnostic, and the *policy* (what the value of a node
is) stays the type's job.

## References

- Van Gelder, Ross, Schlipf — *The well-founded semantics for general logic
  programs* (alternating fixpoint; unique 3-valued model).
- Denecker, Marek, Truszczyński — approximation fixpoint theory (the
  alternating fixpoint over any monotone approximator).
- Tarjan — depth-first search and strongly connected components.
- Gelfond, Lifschitz — *The stable model semantics for logic programming*
  (answer sets; even loops → multiple models, odd loops → none).
- Apt, Blair, Walker / Przymusinski — stratification and the perfect model for
  (locally) stratified programs.
- Chen, Warren — SLG resolution / tabling (XSB); completion of strongly
  connected components, and why tabling incomplete answers over negation is
  unsound.
- Dung — *On the acceptability of arguments* (abstract argumentation frameworks).
- Bench-Capon — *Value-based argumentation frameworks* (a total value order
  yields a unique extension even with odd cycles).
