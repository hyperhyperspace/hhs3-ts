# Observer-frame `ALLOW UPDATE REF`

> **Status: proposal, not implemented.** Today a binding's `ALLOW UPDATE REF`
> gate is evaluated in the observed group's frame
> ([`VOID_SEMANTICS.md`](../VOID_SEMANTICS.md) §5.5). This note was first
> written in the rpack module on 2026-10-21, revised on 2026-10-29 after ideal
> concurrency, the bounded delta and multi-alias bindings landed, and extended
> on 2026-10-30 with gates that read the observer's own tables (Section 2.5).
> On 2026-11-01 the observer frame became the only frame (Section 2.4), and
> references written in the catalog started translating through table maps
> (Section 2.1). Section 8 lists what changed.

This note proposes evaluating `ALLOW UPDATE REF` (`canObserve`) in the observer's frame instead of the observed group's. An observer can then gate who advances its view of one bound group with capabilities held in another bound group, or in its own tables. Section 1 states the problem. Section 2 gives the design: names and rules (2.1 to 2.4), gates over the observer's own tables (2.5), and the alternatives considered for those (2.6). Section 3 argues it is sound. Sections 4 to 6 list the code, tests and docs it touches, and Section 7 the open questions.

---

## 1. The problem

A group `G` declares `ALLOW UPDATE REF <binding> IF <predicate>` to gate who may advance its observation of the group `H` that `<binding>` names. Today the predicate is read in `H`'s frame:

- An unqualified `t` is `H`'s table `t`, read at the candidate observed version $(\text{refAt}, \text{refFrom})$.
- A qualified `a.t` resolves `a` in `H`'s bindings and reads the table at the version `H` observes at `refAt`.

That works when `H` can see the capability table. A blog that binds `users` lets a comment group write `ALLOW UPDATE REF target IF EXISTS users.caps ...` in the blog's names.

Two layouts have no such path:

1. **A data group that binds nothing.** `G` binds `target => blog` and `auth => users`, and wants `users`' capabilities to decide who advances `target`. `blog` does not bind `users`, so no predicate in `blog`'s frame can name them.
2. **A group shared by another application.** If App B observes a group created by App A, that group knows nothing about App B's users. This layout does not exist yet: catalog groups are seeded from their own RDb (`seedSource: 'rdb'`), and shared groups are future work. It is the case that turns the observed frame from an inconvenience into a dead end.

Reading the gate in `G`'s frame, through `G`'s bindings and table maps, lets `G` define its permissions in its own namespace. The same reading lets a gate use `G`'s own tables, for example an `admins` table that `G` keeps itself. That case needs a mechanism of its own (Section 2.5).

---

## 2. Design

Let `G` declare `ALLOW UPDATE REF b IF p`, with `G.bindings[b] = H`. The op being judged is an observe `y` of `H` in `G`.

### 2.1 Names

Names in `p` resolve as they do in `G`'s table restrictions:

- A qualified `a.t`, with `a` an alias of `G`, is the table `t` of the group `G.bindings[a]`, translated through `G.tableMaps[a]` (`resolveForeignTableName`). The gated binding's tables are written this way too, as `b.t`.
- An unqualified `t` is `G`'s own table `t`, read as Section 2.5 describes.

The first version of this note made an unqualified `t` shorthand for `b.t` and banned `G`'s tables. Now that `G`'s tables are allowed, an unqualified name means what it means everywhere else in `G`.

Several aliases can name one group id. `observeGateFor` (`src/rtable_group/group.ts`) conjoins the gates of every alias bound to `H`, and each alias has its own table map. A stored atom carries the alias it was written with, so each atom is read through its own alias's map after the conjunction.

**Translation is new for predicates written in the catalog.** Today `canObserve`, `canDeploy` and `idProvider` name the bound group's real tables, untranslated (the comment on `resolveSchemaForeignTableView` in `group.ts`). Table maps let a bound group differ in shape from what `G`'s schema expects, so a predicate written beside that schema should use the schema's names. Observer-frame gates translate, and `canDeploy` and `idProvider` change to translate as well. A name that is not a map key passes through unchanged, so a real name still works unless it is also a key: with `posts => articles`, `a.posts` reads `articles` even if the bound group has its own `posts`. Schema rules already shadow such a table the same way. The change is unconditional; a catalog whose `canDeploy` or `idProvider` names such a table is rewritten (Section 2.4).

Translation renames a table within a bound group, by a map fixed at creation. It never changes which group is read, at which version, or whose verdicts are consulted. The gate graph's nodes are group ids, and the bound walk's references are per bound group (`revisionBoundHost` in `src/rtable_group/delta.ts`), so neither changes. `canDeploy` is read only at admission, since deploys are never void at use, and `idProvider` reads key registrations without verdicts (`resolveAuthorKey`). Neither can join a verdict cycle, whichever tables they name.

### 2.2 Rules checked at creation

`G`'s bindings and gates are fixed in its create payload, so these are checked once, when the create payload is validated (`src/rtable_group/validate.ts`, `validate_ops.ts`) and when a catalog is validated (`src/rcatalog/validate.ts`):

1. Every gate atom either is qualified with an alias of `G` or names a table of `G`'s pinned schema. If a later deploy drops a local table, the atom reads false, as any missing reference does (`src/rtable_group/predicates.ts`).
2. **The gate graph is a DAG.** Its nodes are bound group ids, with an edge $H \to K$ whenever a gate on an alias bound to $H$ names an alias bound to $K \neq H$. A cycle is rejected. `G`'s own tables are not nodes; Section 2.5 handles them.
3. As today, a gate that reads `$author` needs `USING IDENTITIES`.

Nothing is checked at deploy time. Table rules change with every deploy, and Section 2.5 does not depend on them.

### 2.3 Versions read

An atom is read:

| | `a.t`, `G.bindings[a] = H`, the binding being gated | `a.t`, `G.bindings[a] = K ≠ H`, another bound group | `t`, a table of `G` |
| :--- | :--- | :--- | :--- |
| **At use** (`diagnoseObserve`, horizon `from`) | at $(\text{refAt}, \text{refFrom})$, with `refAt` from `resolveObserveGateRefAt` (mvt's `widenRefAtUpward`), as today | through `resolveSchemaForeignTableViewIn(evaluation, a, t, {y}, from, true)`: the fold of `K`'s live observes in `y`'s history and concurrent with `y`, the read a row op's guard makes | at (`{y}`, `from`) through the gate view (Section 2.5), the read a row op's guard makes |
| **At admission** (`validateObserveGate`, position `at`) | at the imported version, (`newRefVersion`, `newRefVersion`), as today | at `G`'s observed version of `K` at (`at`, `at`), the read a row op's admission makes | at (`at`, `at`) through the gate view |

Only the binding being gated keeps the ref-advance's special contexts (`mvt/MVT_DEFINITION.md` §8, items 1 and 2). Every other binding, and `G` itself, is read the way a guard reads: in the use view at use, and at the op's own position at admission.

Admission becomes mixed. One gate reads `H` at the version being imported, and everything else at the observer's own position. The std_types README calls a ref-advance [the one op admitted at another version](../../std_types/README.md#admission-and-use). For an observer-frame gate, that holds for the observed binding only.

### 2.4 Replacing the observed frame

The project is unreleased, so the observer frame replaces the observed frame instead of sitting beside it. The payload carries no frame marker. Evaluation (`evaluateObserveGateIn`), the static check (`groupViewReason`) and the compiler (`compileCatalogGroup`) each have one path, and C-SQL needs no new syntax.

The same text means different tables in the two frames, so existing gates are rewritten:

- An unqualified `t` that meant `H`'s table becomes `b.t`. In the editor example, `ALLOW UPDATE REF user IF EXISTS caps WHERE caps.grantee = $author` becomes `EXISTS user.caps WHERE user.caps.grantee = $author`. Left alone, `caps` would name a table of `doc`, which has none, so creation rejects it (Section 2.2). Only a gate whose observer has a table of the same name would change meaning silently; the rewrite has to check for that.
- A qualified `a.t` that walked one of `H`'s aliases needs `G` to bind that group itself. It is then read at `G`'s observed version, not at the version `H` observes (Section 2.3).

Catalog group hashes change, and the editor pack is rebuilt.

**What is lost:** a gate can no longer read a group through `H`'s bindings, at the version `H` observed when it reached the imported state. A gate asks who may advance `G`'s view, and `G`'s own view of the permissions group answers that, with revocations arriving through `G`'s use view (Section 3.2) and not only once `H` has observed them. Today's quirk, that such reads skip the void filter, goes away with the frame.

### 2.5 Gates that read `G`'s own tables

#### Why they need care

A row op of `G` reads bound groups through the fold of `G`'s live observes (Layer 2), so its verdict can depend on observes. A gate that reads `G`'s rows makes an observe's verdict depend on rows. Together they can form a loop, and a loop through an observe is fatal. The fold needs each observe to be live or void, so `observeVerdictLive` throws on an undecided one.

Loops among rows are not the problem: the engine solves them in every group. The problem is a loop through one of `G`'s observes, and only `G`'s own rows can close one. A bound group's rows are judged from that group's history and the groups it binds, all below it in the binding DAG, so they never read `G`'s observes. Their loops close inside that group. The engine finishes those components, collapsing what stays undecided to void, before the gate's read returns. To the gate, a bound group's rows are already live or void, the same answer any reader of that group gets. What does depend on `G` is the version at which a bound group is read. For `H` that is `G`'s other observes of `H`, handled by the upward widening (Section 2.3). For `K` it is `G`'s observes of `K`, handled by the gate graph (Section 2.2).

Take this example (rules are schematic). `G` binds `auth => K` and keeps a table `admins`:

- `admins` honors concurrent deletes, as capability tables do, and its delete rule is `$author IN auth.caps`;
- the gate is `ALLOW UPDATE REF auth IF $author IN admins`.

Alice is an admin, and Bob holds a cap in `K`. Without seeing each other:

- Bob deletes Alice's `admins` row;
- Alice observes a newer `K` that revokes Bob's cap.

Both outcomes are consistent:

- If Alice's observe is live, Bob's cap is revoked, so his delete is void, Alice stays an admin, and her observe is live.
- If her observe is void, Bob keeps his cap, so his delete is live, Alice is no longer an admin, and her observe is void.

No rule of the well-founded engine chooses between them, so Alice's observe would be undecided.

Rejecting such schemas works badly. Table rules change with deploys, concurrent deploys merge slot by slot, and every merge rule found so far either fails open or loses changes (Section 2.6). The design below never looks at the schema's shape. It cuts the loop at the one place it passes through: the gate.

#### Local-gated bindings

A binding is *local-gated* when its gate reads a table of `G`, or names a binding that is local-gated. Gates are fixed at creation, and the gate graph is a DAG, so this is a fixed property of each binding. It is computed once, when `G` loads.

Every other binding is either ungated, or has a gate that reads only bound groups that are not local-gated. Its observes' verdicts never reach `G`'s rows. In the example, `auth` is local-gated.

#### The gate view

A gate reads `G`'s tables through the *gate view*. The gate view is the ordinary table view at the same versions, with one difference: it judges every entry by the entry's *gate verdict* instead of its verdict.

A gate verdict comes from the same diagnosis as the verdict (`diagnoseEntry`), with two changes:

- it reads `G`'s tables through the gate view too, so a gate verdict only ever reads other gate verdicts of `G`'s entries;
- a read of a local-gated binding returns *unknown*, without folding that binding's observes.

Reads of bindings that are not local-gated go through the fold as usual. A bound group's own entries are judged by that group, as usual.

So a gate verdict never asks for the verdict of a local-gated binding's observe, and that is the whole cut. The loop above needs Bob's delete to read Alice's observe. In the gate view, Bob's delete reads `auth` as unknown instead.

Gate verdicts are memoized in the same `VerdictEvaluation` under their own keys (`<group>|gate|<entry>|<from>`). They are computed only for entries some gate reads. The table view needs no new logic: it already asks its target for each entry's status (`entryStatusIn`), and the gate view's target answers with gate verdicts.

#### Unknown is not undecided

The engine already has a third value, undecided, for members of a cycle that is being solved. When a component is complete, members still undecided collapse to void (`undecided-cycle`). For rows that is right: an unsupported cycle of grants grants nothing.

Unknown must not collapse, because void is not a neutral value. It passes through negation: a void delete leaves its row in place. If Bob's unknown delete collapsed to void, Alice's row would look live to the gate. The gate would trust exactly the admin whose status it cannot judge, and the cut would fail open.

So unknown is kept until the gate's answer, and Kleene logic decides at each step whether it matters:

- `true OR unknown` is true, and `false AND unknown` is false. An unknown row does not matter when the answer is settled without it. If Alice also has a second admin row whose fate is known, `$author IN admins` is true for her.
- `NOT unknown` is unknown. A row that an unknown delete targets is unknown, whatever the delete would decide.

The predicate evaluator and the table view already work in three values (`Truth`), so unknown flows through them unchanged.

#### How an observe is decided

`diagnoseObserve` evaluates the gate as today. The outcome is:

- **true:** the observe is live;
- **false:** void, with reason `observe-gate`, as today;
- **unknown:** void, with a new reason, `observe-gate-unknown`, that names the local-gated bindings the answer depended on.

An unknown gate fails closed: the observe is refused, and ordinary views are not affected. Admission (`validateObserveGate`) applies the same rule at (`at`, `at`), so an observe that would be void at once is refused at write time.

Undecided is still an invariant violation. A gate whose truth depends on a member of an open component throws, as today; Section 3.1 argues this cannot happen.

#### Walkthrough

`G` binds `auth => K` and `target => H`, and keeps `admins`, seeded at genesis with a row for Root. `admins` honors concurrent deletes.

1. **Local rules.** `admins`' insert and delete rules are `$author IN admins`, and the gate is `ALLOW UPDATE REF target IF $author IN admins`.
   - `target` is local-gated; `auth` is not.
   - `admins`' rules read no binding, so every gate verdict equals the verdict. The gate sees what any reader of `admins` sees.
   - This is the common case, and nothing in it is unknown.
2. **Rules through `auth`.** `admins`' rules are `$author IN auth.caps`, with the same gate.
   - `auth` is ungated, so it is not local-gated, and gate verdicts read it through the fold.
   - Again the gate view equals the ordinary view.
3. **The loop.** As in 2, with one addition: `ALLOW UPDATE REF auth IF $author IN admins`. Now `auth` is local-gated. In the race above:
   - Alice's observe reads `admins` through the gate view.
   - Bob's delete is a concurrent delete of her row. Its gate verdict reads `auth`, so it is unknown.
   - So her row is unknown, the gate is unknown, and her observe is void (`observe-gate-unknown`).
   - Ordinary verdicts then follow without a loop. Bob's delete reads `auth` through the fold, which skips Alice's void observe. Bob keeps his cap, and his delete is live.
   - Alice is no longer an admin. The revoke of Bob's cap stays in `K` until another admin imports it. Bob's delete stays live after that import, because it came before the import in `G`: use before revoke.
4. **The conservative case.** As in 3, but this time Carol, also an admin, imports the revoke of Bob's cap concurrently with Bob's delete. Bob's delete is then void, and Alice is an admin in every ordinary view.
   - The gate still sees Alice's row as unknown, because judging Bob's delete needs `auth`.
   - Alice can observe neither `auth` nor `target` until she is granted again under a new row, which no delete targets.
   - Gate verdicts are shared by all gates, so `target`'s gate is affected even though it is not on the loop. Section 7 discusses a finer cut.

When an observe's gate depends on a row that is in a loop with that observe, the observe always loses. A race of two revokes between rows is resolved differently: both are void (`mvt/WELL_FOUNDED_VERDICTS.md` §3.5).

#### What it costs

- **It is conservative.** A gate verdict treats every local-gated binding as unknown, even one with no path back to the gate being evaluated. In walkthrough 4, `target`'s gate is affected by `auth`'s loop.
- **Unknown is sticky.**
  - An admitted delete whose rule reads a local-gated binding makes its target row unknown to every gate, for good, whether the delete is live or void. The remedy is a new grant under a new row.
  - An insert whose rule reads a local-gated binding makes its own row unknown, so gates cannot use such a table at all.
- **Explanations.** The refusal names the binding, and could name the entry as well (Section 7): "the gate reads `admins`; a delete of Alice's row depends on `auth`, which this gate controls". The failure is always a refused observe, never changed data.
- **Engine work** in mvt, below.
- **A second verdict** for each entry a gate reads, computed by a second pass over the same reads.

#### Changes to the engine

`VerdictEvaluation` (`mvt/src/verdict_evaluation.ts`) gains unknown inputs:

- An unknown read is an undecided value whose support holds a key that is never a member (`unknown|<group id>`).
- The alternating fixpoint is unchanged. An unknown key is never known live and is always possibly live, like an undecided member.
- The collapse to void skips every component whose undecided members reach an unknown key in the support graph.
- A member left undecided only because it reaches unknown keys is stored as undecided, with those keys as its support. Readers outside the component get that value.

No ordinary diagnosis produces an unknown key, so ordinary answers stay live or void, as today.

One property keeps the gate honest: **a decided gate verdict equals the verdict.** The gate may know less than ordinary views do, but it never disagrees with them. Kleene logic and the alternating fixpoint decide a value only when it holds for every value of the undecided inputs. Gate mode has strictly more undecided inputs, so it decides fewer entries, and never differently. The collapse acts on the same members in both modes, except those that reach an unknown key, which gate mode keeps. The collapse step is not part of the textbook well-founded semantics, so this property gets its own test (Section 5).

### 2.6 Alternatives considered

Three other ways were considered to let gates use data that `G` controls.

1. **A separate group.** Keep `admins` in its own group `A`, bind it in `G` as `adm`, and gate with `$author IN adm.admins`.
   - This is the `K ≠ H` case of Section 2.3, with no new machinery.
   - `A` can never read `G`: `A` exists before `G`, and `G`'s create payload names it.
   - `A`'s schema evolves freely.
   - The costs: an extra object, admin changes made as ops in `A`, and an observe of `A` before a new admin counts (Section 4 orders it).

   This stays available and needs nothing from Section 2.5.
2. **A fixed set of gate-readable tables.** Fix at creation a set L of `G`'s tables that gates may read. Refuse any deploy whose rules for a table in L read a binding or a table outside L.
   - It survives merges without coordination. RSchema resolves each slot by last-writer-wins among its causal maxima (`src/rschema/resolve.ts`), and a maximum after a merge was a maximum on one side. So checking every candidate write at each deploy covers every merge.
   - L cannot grow. A deploy that adds a table to L could be concurrent with one that gives that table a binding read.
   - Tables in L can read only tables in L.
   - RSchema authors, who are not `G`'s deployers, can write updates that `G` cannot deploy.
3. **Rejecting loops when deploys merge.** The check would have to cover every rule `G` ever deployed, because a row keeps the rules of its own position. Suppose one deploy makes `roles` read `auth`, a second reverts that, and a third makes `admins` read `roles`. No single schema has the loop, but old `roles` rows still read `auth`.
   - On one branch, a deploy that closes a loop can be refused.
   - When two concurrent deploys close a loop together, the merge must fall back, and every fallback is bad:
     - Reverting one rule silently undoes a change to a different table than the one it conflicted with, and fails open if the reverted rule was a tightening.
     - Reverting to deny-all voids the table's deletes and revives what they revoked.
     - Failing closed at the gate needs information per row, which is this design.
   - Every fallback is permanent, because the rows carrying the old rules remain.

Section 2.5 needs no deploy checks and no merge rule, it fails closed, and it changes nothing where no loop is possible.

---

## 3. Why it is sound

Take `G` binding `target => H` and `auth => K`, with a gate on `target` that reads `auth.caps`. `y` is an observe of `H` in `G` by author `a`, judged from horizon `from`.

### 3.1 No observe cycles

`observeVerdictLive` throws on an undecided observe verdict, so observes must never be on a verdict cycle. Sort the verdicts `G` computes into four kinds:

1. verdicts of observes of bindings that are not local-gated;
2. gate verdicts of `G`'s row ops and bundles;
3. verdicts of observes of local-gated bindings;
4. verdicts of `G`'s row ops and bundles.

Besides entries of bound groups, which are lower in the binding DAG, each kind reads only its own kind or lower ones:

- **Kind 1.** An observe of a binding that is not local-gated reads:
  - observes of its own binding whose versions are strictly above its own in the observed group (Layer 1, as today);
  - observes of bindings below it in the gate graph, none of them local-gated;
  - bound groups' entries.
- **Kind 2.** A gate verdict reads:
  - gate verdicts;
  - through the fold, observes of bindings that are not local-gated;
  - nothing of local-gated bindings, which read as unknown.
- **Kind 3.** An observe of a local-gated binding reads what kind 1 reads, plus:
  - gate verdicts, through its gate;
  - observes of local-gated bindings below it in the gate graph.
- **Kind 4.** An entry's verdict reads:
  - entries' verdicts;
  - through the fold, observes of any binding.

Within kind 1 and within kind 3, every dependency between observes strictly descends the lexicographic order

$$\bigl(\text{rank of the object in the binding DAG},\ \text{rank of the binding in the gate graph},\ H(\text{observed version}) \text{ under } \subsetneq\bigr),$$

which is well-founded. Cycles remain possible within kind 2 and within kind 4, among `G`'s entries, and the engine solves them as today. So every component the engine solves holds verdicts of a single kind. An observe never shares a component with another member, and the folds stay boolean. This answers the open item "Observes that read each other" in `mvt/WELL_FOUNDED_VERDICTS.md` §6.5.

### 3.2 Revocations through another binding

Let $r$ be a live revoke in `K` of the capability `a` relies on, visible from `from` in `G`. Write $V$ for `y`'s version of `K` at use.

1. If $r \in H(V)$, Step 1 sees it, and `y` is void.
2. If $r \notin H(V)$ and $H(V) \not\subseteq H(r)$, then under ideal concurrency $r$ is concurrent with $V$ ([`IDEAL_CONCURRENCY.md`](../../mvt/IDEAL_CONCURRENCY.md) §4.4). Step 2 finds it at the horizon version where the capability table honors concurrent deletes, as for any guard read.
3. Otherwise $H(V) \subsetneq H(r)$. Every live observe of `K` that carries $r$ is concurrent with `y`, before it, or after it in `G`. If it were concurrent or before, the fold would put $r$ in $H(V)$. So each is after `y`: use before revoke in both DAGs, which no rule voids without clocks (`[XGROUP11]`). A void observe that carries $r$ does not count, as in every Layer 2 read.

So a revoke arriving through `auth` voids `y` exactly when it would void a row op of `G` at `y`'s position that reads `auth`. The case $K = H$ is the existing argument of [`VOID_SEMANTICS.md`](../VOID_SEMANTICS.md) §5.5, which ideal concurrency made exact.

The first version of this note expected the cross-binding case to need its own attack analysis. Since `ALL` was replaced, it does not. The Layer 2 fold takes every live concurrent observe, not only dominating ones, so there is no mixed-cut gap to close.

Reading `K` in the use view has the consequences every guard read has:

- A grant on a branch of `K` that only a concurrent live observe imports counts for `y`. For the binding being gated it does not (`mvt/MVT_DEFINITION.md` §8, item 1).
- `y` sees through voided observes of `K`, so a back-dated import by a revoked observer of `K` contributes nothing (`[OBSGATE08]`'s rule, applied to `K`).

**Revocations in `G`'s own tables** follow the same rule. The gate view reads at (`{y}`, `from`), like a row op's guard. So a delete that is in `y`'s history, or concurrent with `y` in a table that honors concurrent deletes, voids `y` when its gate verdict is live. When its gate verdict is unknown, `y` is void as well (Section 2.5).

### 3.3 Imports used against honest observers

An observe of `K` only adds `K`'s own entries, judged by `K`'s own verdicts, so it cannot forge a revoke. It voids an honest `y` only through cases 1 and 2: a live revoke of `y`'s author, issued without seeing all of $V$. That is the honest-loss cost of ideal concurrency ([`IDEAL_CONCURRENCY.md`](../../mvt/IDEAL_CONCURRENCY.md) §5.3), the same one row ops pay. It holds even when `auth` is ungated and anyone who can write to `G` may observe `K`. Observing `K` before `H` (Section 4) narrows the window.

### 3.4 The bounded delta

`walkRevisionBound` (`mvt/src/revision_bound.ts`) was written for observe verdicts that read their own reference, and for the foreign views of row ops. An observer-frame verdict reads a second reference, and possibly `G`'s own tables. The walk itself needs no change:

- An observe of `K` in the meet's history whose version is above `K`'s bound is handled by Phase A, which runs over every declared reference. Any `y` after it is then above the bound and walked.
- An observe of `K` concurrent with `y` is in `start`, since new entries are after the meet. It is below the bound and keeps its verdict, or it is walked. If it is unstable or flipped, the bound drops to the GLB with its predecessors, which also removes `y`, because `y` is concurrent with it.
- A gate reads `G`'s tables at (`{y}`, `from`), as a row op's guard does. A gate verdict reads a subset of what the entry's verdict reads, with local-gated reads replaced by unknown. The walk should cover `y`'s reads of `G`'s tables the way it covers a guard's.
- The visitor records `y`'s own flip through `isEntryVoided` at both horizons.

The proof of Theorem 1 in [`INCREMENTAL_DELTA_BOUND.md`](../INCREMENTAL_DELTA_BOUND.md) covers the read of `K` as a foreign view anchored at `y`. The text still has to change. Assumption A3 lists "for a gated observe, its gate in the foreign group's frame". Guarantee 6 of `walkRevisionBound` describes a verdict as a read of its own reference. Both must say that a verdict may read other references through the full fold, and `G`'s tables through the gate view. The parity sweeps must cover observer-frame gates, including gates over `G`'s tables, before this is relied on.

---

## 4. Changes

**rdb**

- `src/rtable_group/validate.ts`, `src/rtable_group/validate_ops.ts`, `src/rcatalog/validate.ts`: the rules of Section 2.2.
- `src/rtable_group/group.ts`:
  - `evaluateObserveGateIn` takes the observe's position and horizon (`opPos`, `from`).
    - An atom on an alias bound to `H` reads `H` at (`refAt`, `refFrom`).
    - An atom on any other alias goes through `resolveSchemaForeignTableViewIn`.
    - An unqualified atom reads `G`'s table through the gate view.
  - `diagnoseObserve` passes (`{y}`, `from`), and maps an unknown gate to `observe-gate-unknown`. The public `evaluateObserveGate` gains `opPos`.
  - The local-gated classification is computed at load (Section 2.5).
  - Gate verdicts: a `resolveVerdict` variant keyed `<group>|gate|<entry>|<from>`, running `diagnoseEntry` with gate-view table reads and with unknown reads of local-gated bindings.
- `src/rtable/view.ts`: no new logic. The gate view is an `RTableViewImpl` whose target answers `entryStatusIn` with gate verdicts.
- `src/rtable_group/op_void.ts`: the reason `observe-gate-unknown`.
- `src/rtable_group/validate_ops.ts`: `validateObserveGate` passes `at` and refuses an unknown gate. The deploy check evaluates `canDeploy` through `resolveSchemaForeignTableView`, so its atoms translate (Section 2.1).
- `src/rtable_group/group.ts`: `resolveAuthorKey` translates a foreign `idProvider` table through `resolveForeignTableName`.
- `src/rcatalog/validate_ops.ts`: `groupViewReason` checks gates through `G`'s bindings, table maps and schema, and checks `canDeploy` atoms and a foreign `idProvider` (`groupIdentityReason`) against the mapped table.
- `src/rdb/catalog_update.ts`: `applyCatalogPlan` observes a group's bindings in gate-graph order.

**rdb_lang**

- `src/compile/catalog.ts`: `compileCatalogGroup` resolves gates in `G`'s scope: qualified atoms through its bindings and table maps, unqualified atoms as its own tables.

**Migration** (Section 2.4): the gates in `rdb/examples/editor.sql`, `rdb/examples/editor_with_attachs.sql`, `rpack/examples/editor_with_attachs.sql` and the editor pack sources under `packs/editor/work`, which is then rebuilt; and the test catalogs that write gates (`rdb/test`, `rdb_lang/test`, `rdb_tools/test/run_tests.ts`).

**rdb_runtime**

- `src/session/ref_auto_update.ts`: observe in gate-graph order. After `G` observes `K`, retry `G`'s observes of the bindings whose gates read `K`. Today a failed observe is retried only when the observed group changes again, so an observe of `H` refused because `G`'s view of `K` was stale stays refused. An observe refused as `observe-gate-unknown` stays refused until the rows its gate reads change.
- `src/session/authz_suggest.ts`: `evaluateObserveGateKey`, `resolveObserveAuthor` and the `update-ref` branches pass `G`'s position, and report `observe-gate-unknown` as its own outcome. `rdb_tools` re-exports them.

**mvt**

- `src/verdict_evaluation.ts`: unknown inputs (Section 2.5, "Changes to the engine").
- `walkRevisionBound`: no change (Section 3.4).

---

## 5. Tests

- `[OBSGATE]`, cross-binding:
  - a revoke in `K` imported by a concurrent observe of `K` voids a back-dated observe of `H`;
  - the same import after the observe of `H` leaves it live;
  - a grant imported only by a voided observe of `K` does not authorize;
  - benign concurrent observes of `H` stay live;
  - two aliases of one group with different table maps.
- `[OBSGATE]`, gates over `G`'s tables (the walkthrough of Section 2.5):
  - with local rules (1), and with rules through an ungated binding (2), the gate verdict equals the verdict on every entry;
  - the loop (3): the observe is void with `observe-gate-unknown` and Bob's delete is live, in every arrival order and at every horizon;
  - an unknown delete never makes its target look live to a gate;
  - a second row whose fate is known lets the observe through (`true OR unknown`);
  - a new grant under a new row restores the observe (4);
  - a delete in `G` concurrent with `y` voids it, as it would a row op's guard;
  - admission refuses an observe whose gate is unknown.
- mvt `VerdictEvaluation`:
  - unknown keys are never collapsed;
  - in a component that mixes a pure cycle with a member that reaches an unknown key, only the pure cycle collapses;
  - stored undecided answers carry unknown keys only.
- A property check: wherever a gate verdict is decided, it equals the verdict. Run it across the rdb suites, and in `test/delta_parity` with gates over `G`'s tables.
- Creation: a gate-graph cycle, an unbound alias and a local table missing from the pinned schema are rejected.
- `[DBOUND]` and `test/delta_parity`:
  - a side-branch observe of `K` that flips an observe of `H`;
  - a variant of the gated-observer generator whose gate reads a second bound group;
  - a variant whose gate reads `G`'s tables.
- rdb_runtime: ordering and retry in `ref_auto_update` and `applyCatalogPlan`.
- rdb_lang: compile, static check and C-SQL round trip of gates in `G`'s scope. The tests that pin the observed frame, `[CLANG17]` and `[CAT13]`, are rewritten to test the observer frame.
- Translation: a `canDeploy`, a foreign `idProvider` and a gate atom each read through a table map.

---

## 6. Docs to update when it lands

- [`VOID_SEMANTICS.md`](../VOID_SEMANTICS.md):
  - §4, the engine: unknown inputs, and gate verdicts.
  - §5.5: the two layers for a gate that reads a second group, gates over `G`'s tables, and the new reason observes are never on a cycle (Section 3.1). The current reason, that an observe's verdict reads only the observed group, stops being true.
- [`INCREMENTAL_DELTA_BOUND.md`](../INCREMENTAL_DELTA_BOUND.md), assumption A3, and guarantee 6 in `mvt/src/revision_bound.ts`.
- [`rpack/README.md`](../../rpack/README.md): how gates are read, replacing the paragraphs that describe this as a follow-up; and the table-map paragraph, which says that clauses written in the catalog use real table names.
- [`rdb_lang/README.md`](../../rdb_lang/README.md): the column-scope rules for gates, and the gate examples.
- `mvt/MVT_DEFINITION.md` §8 and the std_types README's "Admission and use": the mixed admission of Section 2.3.
- `mvt/WELL_FOUNDED_VERDICTS.md`:
  - a section on unknown inputs, and why they do not collapse;
  - §6.5: close "Observes that read each other".

---

## 7. Open questions

1. **A finer cut.** Gate verdicts are shared by every gate, so every local-gated binding reads as unknown. A gate on `H` only needs to cut the bindings that can reach `H`'s observes.
   - Gate verdicts could be keyed per gate, with a creation-fixed order of the local-gated bindings: a gate verdict for `H` may read bindings strictly below `H` through the fold. In walkthrough 4, that would let `target`'s gate see `auth`.
   - Keying per gate alone is not enough: two gates could each read the other's binding through gate verdicts, and the order rules that out.
2. **Reading a local-gated binding at an upper bound.** Instead of making the whole read unknown, the binding could be read at the version given by the unfiltered fold, which is at or above the true one, with every entry in it unknown. A read that finds no candidate at all would then be decided false. This needs an argument that every view the true version could give has its candidates in the upper one.
3. **Explaining unknown.** `observe-gate-unknown` names the bindings. Naming the entry whose gate verdict was unknown needs the support to carry it. The shape of `explainEntryVoided` and `authz_suggest` output for this case is open.

---

## 8. Since the first version

- **Ideal concurrency replaced `ALL`** ([`DESIGN_AND_PLAN_FOR_ANY.md`](./DESIGN_AND_PLAN_FOR_ANY.md)). The §5.5 argument is now exact for the binding being gated, and the cross-binding case reduces to guard reads (Section 3.2). The head-advancing widening and cut intervals of [`OBSERVED_CUT_REVOCATION.md`](./OBSERVED_CUT_REVOCATION.md) are not needed.
- **Several aliases can name one group** (`aliasesForId`, `observeGateFor`). Each stored atom carries its alias (Section 2.1).
- **References written in the catalog translate through table maps:** gates, `canDeploy` and `idProvider` (Section 2.1).
- **One frame.** Earlier versions kept the observed frame for existing groups, behind a payload marker. The project is unreleased, so the observer frame replaces it, and existing gates are rewritten (Section 2.4).
- **Gates may read `G`'s own tables** (Section 2.5), through gate verdicts that read local-gated bindings as unknown. The first version banned them; Section 2.6 records why the alternatives were set aside. An unqualified name now means a table of `G`, so the shorthand for `b.t` is gone.
- **The bounded delta walks with `walkRevisionBound`.** Its argument covers this design, with text changes (Section 3.4).
- **`widenRefAtUpward` moved to mvt** and is shared with RSet's gated ref-advances. Nothing here changes it.
- **Added:** the retry in `ref_auto_update`, the `authz_suggest` callers and the honest-loss note (Section 3.3).

---

## Implementation guardrail

If an unforeseen problem, ambiguity or architectural conflict comes up during implementation, stop and report it: the issue, the affected code paths and the options. Do not make significant design or protocol decisions unadvised.
