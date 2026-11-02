# Well-founded verdicts

A type that voids entries decides, for each entry, whether it takes effect. An rdb row op is void when it fails its restriction or writes an FK to a row that is not live. An RCap grant or revoke is void when its author did not hold a managing capability. Both questions are asked at the entry's own position, observed from a later horizon, and both read other entries' verdicts: a witness row is live only if its insert is, and a revoke removes a capability only if the revoke itself is live. Verdicts depend on verdicts. When two admins revoke each other concurrently, they depend on each other in a cycle.

This note records how the system decides such cycles. Every verdict is the entry's value in the **well-founded model** of the type's rules, and what that model leaves undecided **collapses to void, bottom-up**. The note explains why this replaced the earlier rule, why the cheaper Kripke–Kleene fixpoint is not enough, and what the choice means for the types that use it.

Section 1 defines the terms. Section 2 shows that verdicts form a logic program with negation and where its cycles come from. Section 3 states the rules that were considered. Section 4 argues for the one that ships, and Section 5 describes what it means for the rest of the system. Section 6 collects implementation notes.

rdb's three-valued read rules (column reads, row liveness, `exists`) and the stratification of gated observes are in [`VOID_SEMANTICS.md`](../rdb/VOID_SEMANTICS.md). This note covers the engine and the semantics it implements, which rdb and RCap share.

---

## 1. Terms

**Verdicts.** An entry is **live** when it takes effect and **void** when it does not. A void entry stays in the DAG, and reads see through it as if it had not been written. A write that fails validation never enters the DAG; it is **rejected**, which is a different thing. A **verdict** is an entry's live-or-void outcome observed from a horizon `from`. It is keyed by the entry and `from`, because a barrier visible from one horizon may not be visible from another.

**Rules and reads.** A type gives every entry a **rule**, its **diagnosis**: a computation over the DAG that reads other entries' verdicts and returns live, or void with a reason. A read is **positive** when the other entry being live can only help, as with a witness insert or a grant in the op's past. It is **negative** when the other entry being live can only hurt, as with a delete or a revoke of what the op needs.

**Undecided.** While a cycle is being solved, a verdict can also be **undecided**. A rule evaluated over undecided inputs returns true, false or undecided by Kleene's three-valued logic. An undecided result carries its **support**: the undecided entries it rests on.

**Evaluations.** A `VerdictEvaluation` holds the state of one top-level computation: the verdicts it has completed and the stack of entries whose diagnosis is running. It is created at a public entry point (`isEntryVoided`, `getView`, `hasCapability`, ...) and passed through every read that computation makes.

**Components and cores.** Entries whose diagnoses read each other, directly or through other entries, form a **component**: a strongly connected component of the graph of reads. A component is solved as a unit. After the solve's fixpoint, the members still undecided form the **support graph**, with an edge from each to the undecided members in its support. A strongly connected component of the support graph with no edge leaving it is a **core**: a set of entries that nothing outside it can decide.

---

## 2. Verdicts are a logic program with negation

### 2.1 The reads

| Type | An entry is live when | Positive reads | Negative reads |
| :--- | :--- | :--- | :--- |
| rdb | each row op in it passes its restriction and FK reach at the entry's position, observed from `from` | inserts and column writes in the entry's past; `exists` witnesses and FK targets at the entry's position | deletes in the entry's past; deletes concurrent with it, where `concurrentDeletes` honors them |
| RCap | its author holds a capability that manages the one it grants or revokes, at the op's position, observed from `from` | grants in the op's past | revokes in the op's past; revokes concurrent with it |

Read as rules over verdicts, this is a **normal logic program**: Datalog with negation. Facts are the entries no rule can void, such as rdb's genesis create and the ops of an RCap creator. Positive reads are positive literals. A barrier delete or revoke is a negative literal: the op is live only if the revoke is not.

A mutual revoke between co-admins A and B is the classic even loop. Each revoke is live only if its author still holds the capability, which the other revoke removes:

```text
revokeA→B  ←  not revokeB→A
revokeB→A  ←  not revokeA→B
```

Three admins who revoke each other in a ring give an odd loop: `r0 ← not r2`, `r1 ← not r0`, `r2 ← not r1`.

`barrier` is a transport tag, not a polarity. Deploys and observes are barriers too, and they only add to what a read sees. The polarity of a read comes from the type's rule, never from the tag ([`VOID_SEMANTICS.md`](../rdb/VOID_SEMANTICS.md) §2).

### 2.2 Cycles need concurrency

Every positive read in the table lands strictly in the entry's past. The covers are taken at the entry's own position, and an entry is never its own witness (Section 5.5). Negative reads of past entries also point down the DAG. A chain of such reads descends the causal order and cannot close. So a cycle needs at least one read of a concurrent entry, and the concurrent reads in the table are barrier deletes and revokes, which are negative. The cycles the types produce run through negation, and they appear exactly when entries that have not seen each other contest the same authority.

Cycles in the data are not verdict cycles. Two tables whose rows reference each other (`[XGROUP09]`) form a cycle of rows, but each write's FK target was written before it, so the verdicts form a chain. Two caps that authorize each other in one bundle (`[PERM07]`) are rejected at validation, because bundle siblings cannot witness each other.

One rdb read does not fit the table cleanly. In `killedByConcurrentDelete`, an undecided delete that `concurrentDeletes` does not honor can sit above a live honored one and hide it. The row's liveness then depends positively on a concurrent entry. Whether this can close a loop of positive reads is open (Section 6.5).

### 2.3 What logic programming offers

A program without negation has a unique least model, in which a loop that only supports itself is false. With negation in a cycle there is no single least model. An even loop has two stable models, each with a single survivor. An odd loop has none: it is the liar, `p ← not p`. The standard semantics are:

- **Well-founded.** One three-valued model, computable in polynomial time. Even and odd loops come out undefined, and a loop that only supports itself comes out false.
- **Kripke–Kleene (Fitting).** The least fixpoint of the program's three-valued evaluation. It is weaker than well-founded: `p ← p` comes out undefined, not false.
- **Stable models (answer sets).** Two-valued, possibly several or none. Deciding whether one exists is NP-complete.
- **Abstract argumentation (Dung).** The same territory in another vocabulary: the grounded extension corresponds to the well-founded model, and stable extensions to stable models.

### 2.4 What an answer must satisfy

- **Convergence.** A verdict is a function of its key and the data. Replicas ask in different orders and enter cycles at different entries, and they must agree.
- **Two values at the boundary.** `hasRow`, `hasCapability`, validation and deltas answer yes or no. A third value cannot leave a computation.
- **Exactness.** Where there is no cycle, the answer is the plain two-valued recursion.
- **No unfounded effect.** Authority roots in a fact. A grant that supports itself, directly or through a ring, is void.
- **Polynomial cost.** No step enumerates assignments to undecided entries.
- **No grinding.** An entry's author picks its position ([`IDEAL_CONCURRENCY.md`](./IDEAL_CONCURRENCY.md) §2.3) and its payload, and so its hash. A rule that decides a cycle by an order the author can influence lets the party being judged pick the outcome.

---

## 3. The rules considered

### 3.1 Back-edge denial

From June to October 2026, each top-level query walked the reads depth-first with a set of entries in progress. A read that reached an entry still in progress returned void for it. In the 2-ring, `hasRow(capA)` asks about `revokeB→A`, reaches it again through `revokeA→B`, and reads it as void. So `revokeA→B` is live, capB is dead, and `revokeB→A` is void: capA survives. Asked about capB, the walk starts on the other side and capB survives too. That was the intended outcome, and `[PERM12]` was the only ring test.

The rule answers each query as if the entry asked about were the one that closes the cycle. Two problems follow:

- **Nested answers contradict top-level ones.** Inside the walk for capA, `revokeA→B` came out live. Asked directly, it is void. Every verdict touched by a back-edge depended on where the walk started, so none could be memoized, and every query walked the cycle again.
- **Odd rings come out wrong.** Followed literally, the void assumed at the back-edge passes through three negations and comes back live. Each revoke of a 3-ring is live when it is the one asked about, so every capability on the ring dies, and no two answers agree with each other.

### 3.2 An instance cache with a seniority case

Before June 2026, verdicts were cached on the group instance by `(entry, from)`, and a 2-party mutual revoke was decided by seniority: the senior cap survived. The cache kept answers computed inside a cycle, which depend on the traversal, and served them to later queries. The answer then depended on query order, which differs across replicas. The seniority case covered only an isolated 2-ring. Both were removed in `cf3aafe`.

### 3.3 Kripke–Kleene, then collapse

Start every member of a component undecided. Evaluate the rules with Kleene logic, mark a member live when its rule is true and void when it is false, and repeat until nothing changes. Then collapse the cores to void and repeat. This needs one monotone iteration instead of the alternating one.

Kripke–Kleene leaves a loop that only supports itself undecided instead of false, and relies on the collapse to void it. When the loop is a core of its own, that works, and only the reason changes: `undecided-cycle` instead of the rule's own reason. It fails when the loop shares a strongly connected component with an entry it would decide:

```text
u ← x ∧ v      u needs x and v live
v ← u          v needs u live
x ← not u      a live u voids x
```

`u` and `v` support only each other, so both are void and `x` is live. Kripke–Kleene leaves all three undecided. The support graph `u → {x, v}`, `v → u`, `x → u` is a single core, and the collapse voids `x` along with the loop.

### 3.4 The well-founded model, then collapse

The same, with the alternating fixpoint in place of Kripke–Kleene. In the program above, the fixpoint finds `u` and `v` unfounded, voids them with their own reason, and decides `x` live. This is the rule that ships. Section 4 argues for it.

### 3.5 A preference order

Pick a model for each core by a total order over its members: make the most senior member live, propagate, and repeat. With a total order, every even loop gets a unique model and every odd loop a deterministic answer, as in a value-based argumentation framework. It would decide revoke races instead of letting every target survive. It is not implemented. The order must be one no author can grind (Section 6.5, and [`VOID_SEMANTICS.md`](../rdb/VOID_SEMANTICS.md) §6).

### 3.6 Leaving undecided visible

Return the three-valued model and let every reader handle the third value. Validation, deltas, `hasRow` and `hasCapability` would each need a policy for it, and they could pick different ones. The collapse is that policy, stated once.

| | Back-edge denial | Kripke–Kleene, collapse | Well-founded, collapse | Preference order |
| :--- | :--- | :--- | :--- | :--- |
| A function of the data | no | yes | yes | yes |
| 2-ring | both caps survive | both survive | both survive | the senior survives |
| 3-ring | every cap dies | every cap survives | every cap survives | decided by the order |
| The program of Section 3.3 | `x` live | `x` void | `x` live | `x` live |
| Completed verdicts can be memoized | only frames no back-edge touched | yes | yes | yes |
| Can be ground | no | no | no | depends on the order |

---

## 4. Why the well-founded model with bottom-up collapse

### 4.1 It is a function of the data

- The well-founded model of a program is unique. The solve starts every member undecided, so it discards the conditional answers of the first pass, which depend on where the walk entered.
- Kleene connectives are commutative and short-circuit only on a value that decides them. A rule's value and support depend on the values it reads, not on the order it reads them.
- The support graph is built from supports. Its cores are a function of the graph, and every core is collapsed in the same round.
- The entry point and the order of arms can change which entries a component pulls in. An entry pulled in through an arm that decided nothing is in no member's support, so it cannot change a verdict (`[VERDICT04]`, `[VERDICT05]`, `[PERM18]`).

This is an argument, not a proof. Every synthetic program in the mvt tests is solved from every entry order, and the rdb ring tests run in both query orders.

### 4.2 Unfounded support never takes effect

A loop of grants that only support each other has no authority to give. Both Kripke–Kleene with collapse and the well-founded model void such a loop when it stands alone. The well-founded model also voids it inside the fixpoint, which matters when the loop shares a component with entries it would decide, as in Section 3.3. There the collapse can only void the whole component and takes `x` with it, while the well-founded fixpoint removes the unfounded part first and decides the rest.

A scratch variant of the engine, whose upper step shrinks the possibly-live set instead of rebuilding it from the known-live set, implements Kripke–Kleene. On the mvt suite it passes `[VERDICT02]`, `[VERDICT04]`, `[VERDICT05]`, `[VERDICT08]` and `[VERDICT09]`. It fails `[VERDICT01]` and `[VERDICT03]` on reasons only. It fails `[VERDICT06]` and `[VERDICT07]` on verdicts: both programs contain the shape of Section 3.3, a loop `n ← n ∧ m` in one component with an entry `y ← not n` that the loop would decide.

The shape needs a loop of positive reads. Section 2.2 shows that in rdb and RCap positive reads point into the past, with one exception to check. If no type can produce such a loop, the two rules agree on every verdict the system computes. The well-founded fixpoint is kept anyway: the engine is generic, the cost is of the same order, and it is the rule that is right when the shape appears.

### 4.3 The collapse returns contested state to what it was

No two-valued answer satisfies every rule of an odd loop: all-live and all-void both break some rule. For an even loop, all-void breaks the rules too, since each revoke's author keeps the capability that should make the revoke live. So for a core the question is which rule to override, not which model to pick. Void answers it this way:

- **Void means no effect.** Reads see through a void entry as if it had not been written. Collapsing a core voids exactly the contested entries, and the state falls back to what it was before the contest. On a revoke ring, every holder keeps the capability they had. On a grant ring, nobody gains a capability they did not have. Neither side of a dispute gets its way.
- **It extends the positive case.** A loop of grants is void because nothing founds it. A core is void because nothing decides it. In both cases an entry takes no effect without a reason.
- **It needs no order between members.** There is nothing for an author to grind.
- **It needs no choice among models.** Choosing a stable model for an even loop needs a preference order, and an odd loop has no model to choose (Section 3.5).

### 4.4 Bottom-up collapse keeps dependents out

Collapsing every undecided member at once would void entries that merely depend on a core. In `[VERDICT04]`, `a` and `b` revoke each other and `d ← not a`. All three are undecided after the fixpoint and sit in one component, because `a` reads `d` through an arm that is decided anyway. Only `{a, b}` is a core. Collapsing it and solving again decides `d` live. Collapsing all undecided members would have voided `d`.

The component boundary does the same work one level up. In `[PERM17]`, admins a0, a1 and a2 form a revoke ring, and a0 also revokes an outsider o (`Ro`). The ring reads nothing from `Ro`, so the ring is solved as its own component first and collapses: a0 keeps its capability. `Ro` is then a component of its own, decided live by the ring's outcome, and o loses the capability.

### 4.5 It costs what the cycles cost

Where there is no cycle, every component is a single entry whose diagnosis reads only completed verdicts. The engine stores the verdict without solving, and the result is the plain recursion. The evaluation's memo diagnoses each `(entry, from)` once per computation outside a solve, which keeps long update chains linear (`[VOID_MEMO01]`).

For a component of $n$ entries, the fixpoint runs at most $n + 1$ rounds, each step makes at most $n + 1$ passes over the $n$ members, and there are at most $n$ collapse rounds. Nothing enumerates truth assignments. Components are as large as the cycles in the data: rings of a few entries in every test.

---

## 5. What it means for the system

### 5.1 The invariant

> An entry takes effect only when its rule decides it live from entries that do not rest on it. Entries that only support each other, or that contest each other with nothing outside to decide between them, take no effect.

### 5.2 Outcomes

| Case | Outcome | Test |
| :--- | :--- | :--- |
| Two co-admins revoke each other concurrently | both revokes void, both keep the capability | `[PERM12]`, `[CAP32]` |
| A ring of three to five revokes | every revoke void, every holder keeps the capability | `[PERM13]`–`[PERM15]`, `[CAP33]` |
| A ring member revokes an outsider | the outsider loses the capability | `[PERM17]`, `[CAP34]` |
| An `or` with one arm through a ring and a clean arm | the clean arm decides | `[PERM18]` |
| An `exists` with a clean candidate beside one in a ring | holds | `[PERM19]` |
| A bundle that rests on a ring and has a dead FK target | void for the FK | `[PERM20]` |
| A revoke that a ring leaves undecided | void, reason `undecided-cycle` | `[PERM21]` |
| Two concurrent revokes by one admin | independent, no cycle | `[PERM22]` |
| A grantee deletes their own manager cap | removed: an op is not its own barrier | `[PERM16]` |
| A delegation chain that revisits a holder at an earlier position | not a cycle: verdicts are per op | `[CAP36]` |
| A live revoke beside a concurrent grant by a ring member | slot cleared | `[CAP39]` |
| An undecided grant that dominates a live revoke | slot open until the grant resolves | `[CAP44]` |

### 5.3 Costs

- **Revoke races end in a stalemate.** A peer who manages their revoker's capability can answer a revoke with a counter-revoke that the revoke has not seen. Authors pick their positions, so a revoked peer can always write one, at a position from before the revoke. The two revokes form a ring, both are void, and both peers keep the capability. The revoker is not removed either and can revoke again, but the same answer is always available. Without wall-clock time, no rule can tell this from an honest race, the same limit as the dodge in [`IDEAL_CONCURRENCY.md`](./IDEAL_CONCURRENCY.md) §4.2. Breaking the tie needs an order between peers (Section 3.5). Authority the peer cannot revoke, such as an RCap creator's, is not exposed.
- **Good faith does not help a member of a core.** Three admins who revoke each other concurrently, each for a good reason, all fail: every target stays in place.
- **Reasons follow the final verdicts.** A void entry reports the first check that fails once every verdict is decided, which can be a consequence of its own voidness (`[PERM20b]`). Core members report `undecided-cycle`.
- **A solve runs diagnoses again**, as often as Section 4.5 allows. Each computation pays it again, since completed components are not kept across computations (Section 6.5).

### 5.4 The read rules are part of the protocol

The well-founded model is taken over the rules as written, and the rules are deliberately structural: Kleene connectives and read-once formulas, never a search over completions ([`VOID_SEMANTICS.md`](../rdb/VOID_SEMANTICS.md) §4). A rule that decides more changes which entries are undecided, and so which cores exist. Take `p ← x ∨ not x` and `x ← not p`. Under Kleene logic `p` is undecided while `x` is, the two form a core, and both are void. A rule that recognizes the tautology makes `p` live and `x` void. A change to the precision of a read rule therefore changes replicated verdicts, like a change to the concurrency rule, and every replica has to run the same version.

### 5.5 The contract for a type that uses the engine

A type that binds `VerdictEvaluation` must:

1. **Key verdicts by everything they depend on.** A key names one entry and one horizon, and an object namespace when an evaluation crosses objects. rdb uses `createOpId|entry|from`, RCap `entry|from`. A verdict must be a pure function of its key and the data.
2. **Read verdicts only through `resolve` on the same evaluation**, and combine them with the Kleene helpers (`allTruth`, `anyTruth`, `everyTruth`, `someTruth`, `notTruth`). Cover walks use `seeThroughCover`, which sees through void and undecided matches and reports the undecided ones that no live match dominates.
3. **Keep every rule exact, sound and monotone.** Exact: with no undecided input it gives the two-valued answer. Sound: a decided result holds in every way of deciding the undecided inputs. Monotone: deciding more inputs never flips a decided result or makes it undecided. An undecided result carries the supports of the undecided inputs it rests on. The engine checks two consequences and throws if either fails: the possibly-live set never grows during the fixpoint, and the final pass agrees with the fixpoint.
4. **Never let an entry witness or bar itself.** The entry being diagnosed reads as void in its own views: rdb builds them that way (`[VREAD05]`), and RCap passes the op as `exclude`.
5. **Run one sequential traversal per evaluation.** The running stack identifies the reader of each `resolve`. Reads that share an evaluation are awaited one at a time; parallel branches each get their own evaluation.
6. **Never create an evaluation inside a diagnosis.** A fresh evaluation cannot see the open frames, so on a cycle it recurses through fresh evaluations forever. rdb keeps an in-flight counter (`VERDICT_MAX_INFLIGHT`) that turns this bug into an error instead of a hang.
7. **Keep undecided inside.** Public entry points create an evaluation and treat an undecided result as a bug: `isEntryVoided` and `hasCapability` throw.
8. **Know where cycles can run.** rdb's bindings are fixed at creation and form a DAG, so every component stays inside one group, and an observe is never on a cycle (rdb throws on an undecided observe verdict). The engine itself does not need either property, but a preference order across objects would need an order between them.

---

## 6. Implementation notes

### 6.1 Where it lives

- **[`src/verdict_evaluation.ts`](./src/verdict_evaluation.ts)**: `VerdictEvaluation` (Tarjan completion, the alternating fixpoint, bottom-up collapse), the Kleene helpers and `seeThroughCover`.
- **rdb**: `rtable_group/verdict_evaluation.ts` (`freshVerdictEvaluation`, `VERDICT_MAX_INFLIGHT`); `rtable_group/group.ts` (`isEntryVoided`, `explainEntryVoided`, `resolveVerdict`, `diagnoseEntry`, `diagnoseObserve`); `rtable/view.ts` (the three-valued reads); `rtable_group/predicates.ts` (the three-valued predicates).
- **std_types**: `rcap/view.ts` (`opVerdict`, `holdingIn`, `slotTruth`).

### 6.2 The algorithm

```text
resolve(key, diagnose):
    if key has a stored answer: return it
    if key is on the stack:                       # a back-edge
        lower the reader's lowlink to key's index
        return key's value in the running solve, or undecided {key}
    push a frame for key (index = lowlink = next index)
    run diagnose                                  # its reads lower the lowlink
    if lowlink == index:                          # root of a complete component
        if the component is this frame alone and its verdict is decided: store it
        else: solve(this frame and every frame above it)
    lower the reader's lowlink
    return the stored answer, or undecided {key}

solve(members):
    collapsed = {}
    loop:
        L = {}, Up = members not collapsed
        repeat:                                   # the alternating fixpoint
            lower: add to L every member whose rule is true, reading L live,
                   members outside Up void, the rest undecided
            upper: Up' = L; add to Up' every member whose rule is not false,
                   reading L live, members outside Up' void, the rest undecided
            until Up' == Up; else Up = Up'
        undecided = Up minus L
        if undecided is empty: break
        graph = edges from each undecided member to the undecided members in its support
        add the members of graph's cores to collapsed
    evaluate every member once more, all values decided, for its reason
    store every member's verdict and pop the component
```

Kripke–Kleene differs only in the upper step, which would start from `Up` and remove every member whose rule is false.

A read during the solve can reach an entry not seen yet. It is resolved as a normal frame. If it reads a member, it joins the component and the solve restarts with it. If it reaches a frame below the root, the component was not complete: the root returns undecided to its reader, and the larger component is solved further down (`[VERDICT06]`, `[VERDICT07]`).

### 6.3 Details worth keeping

- **Only completed components are stored.** A frame that finishes without being a root keeps its conditional answer on the stack and never stores it, because that answer depends on where the walk entered. Storing it was the mistake of the instance cache (Section 3.2).
- **The memo dies with the evaluation.** A completed verdict is a pure function of its key, so a cache across computations would be sound, but none exists. The public `isEntryVoided` creates an evaluation per call, so a per-entry sweep such as the REPL's `LOG` pays $O(n)$ per line.
- **Reasons come from a final pass.** After the collapse, every member is evaluated once more with all values decided. A member that disagrees with its fixpoint value is a bug, and the engine throws.
- **The engine checks monotonicity.** If the possibly-live set grows during the fixpoint, a rule is not monotone. The engine throws instead of looping or returning an order-dependent answer.
- **Narrow reads keep components small.** Everything a diagnosis reads can join its component. Before rdb prefiltered `exists` candidates by payload, each member of a revoke ring read every manager cap's liveness, the outsider's included, which pulled `Ro` into the ring's component in `[PERM17]`.

### 6.4 What we learned

- **Test every shape the rule is about.** Back-edge denial passed `[PERM12]`, and nothing tested an odd ring, where it kills every capability.
- **A test named for a choice should fail under the alternative on the verdict.** `[VERDICT03]` is named "well-founded, not Kripke–Kleene", and the Kripke–Kleene variant fails it only on a reason. The tests that separate the two rules on verdicts are `[VERDICT06]` and `[VERDICT07]`, which were written for growth and merge.
- **Conditional answers must not outlive their traversal.** The instance cache, the root-dependent frames of back-edge denial and the interleaving bug fixed in `fd1f60d` are versions of one mistake: state from one traversal read by another. In the last one, two validations on one cached group instance saw each other's in-progress marks and denied a live witness row (`[OBSGATE07]`).

### 6.5 Still open

- **Can a type produce a loop of positive reads?** Section 2.2 argues that rdb's and RCap's positive reads point into the past. The exception to check is an unhonored concurrent delete that hides an honored one, in `killedByConcurrentDelete` (`rdb/src/rtable/view.ts`). If no loop is possible, Kripke–Kleene and the well-founded model agree on every verdict the system computes. Running the rdb and std_types suites with the Kripke–Kleene variant, as the `ANY` shim did for concurrency, would be a cheap first measurement.
- **A test for the program of Section 3.3**, named for the choice, that fails on `x`'s verdict under Kripke–Kleene.
- **A preference order for revoke races** (Section 3.5). The key would be the insert position of the authorizing capability, fixed at grant time, not the revoke's position, so a later attacker cannot grind it. Components stay inside one group today; a type whose cycles can cross objects would need an order between them.
- **A cache of completed components across computations.** Sound, not implemented.
- **Observes that read each other.** The observer-frame `ALLOW UPDATE REF` design in [`rdb/plans/REFERENCE_FRAME.md`](../rdb/plans/REFERENCE_FRAME.md) would make one observe's verdict depend on the observer's other observes, which could put observes on cycles. rdb throws on an undecided observe verdict today ([`VOID_SEMANTICS.md`](../rdb/VOID_SEMANTICS.md) §5.5).
- **A proof of Section 4.1.** The argument and the order-sweeping tests are what exists.

### 6.6 Tests

- **mvt**, `test/verdict_evaluation_tests.ts`: `[VERDICT01]`–`[VERDICT09]`. Synthetic programs, each solved from every entry order: positive loops, even and odd rings, `p ← p; q ← not p`, a dependent of a collapsed core, arm order, growth, merge, a throwing diagnosis, and the Kleene helpers checked exact, sound and monotone.
- **rdb**:
  - `test/verdict_reads_tests.ts`: `[VREAD01]`–`[VREAD06]`. `[VREAD06]` checks every read rule exact, sound and monotone over every state of a small DAG.
  - `test/rtable_perm_tests.ts`: `[PERM12]`–`[PERM22]` and `[PERM20b]`, the cases of Section 5.2.
  - `test/rtable_void_memo_tests.ts`: `[VOID_MEMO01]`, the memo on a long update chain.
  - `test/rtable_observe_gate_tests.ts`: `[OBSGATE07]`, interleaved computations on one group instance.
- **std_types**, `test/rcap_tests.ts`: `[CAP32]`–`[CAP34]`, `[CAP36]`, `[CAP39]`, `[CAP44]`.

### 6.7 Background

- [`VOID_SEMANTICS.md`](../rdb/VOID_SEMANTICS.md): rdb's read rules, covers and per-computation evaluation, the gated-observe stratification, and the preference order in more detail. Its earlier version (before `a92dfc7`) describes back-edge denial and the caching analysis that went with it.
- [`CAPABILITIES.md`](../rdb/CAPABILITIES.md): capabilities as rows and at-use predicates.
- Van Gelder, Ross, Schlipf, *The well-founded semantics for general logic programs*: the alternating fixpoint and the unique three-valued model.
- Fitting, *A Kripke–Kleene semantics for logic programs*.
- Denecker, Marek, Truszczyński: approximation fixpoint theory, the well-founded fixpoint of any monotone approximator.
- Gelfond, Lifschitz, *The stable model semantics for logic programming*.
- Tarjan, depth-first search and strongly connected components. Chen, Warren, SLG resolution: completing strongly connected components when tabling, and why tabling incomplete answers under negation is unsound.
- Dung, *On the acceptability of arguments*. Bench-Capon, *Value-based argumentation frameworks*: a total value order yields a unique extension even with odd cycles.
