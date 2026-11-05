# HIP-14: Observer-frame `ALLOW UPDATE REF`

- **Type:** design
- **Status:** implemented
- **Modules:** rdb, rdb_lang, rdb_runtime, rdb_projection, rdb_adapter, rdb_repl, rdb_tools, rpack, mvt, dag
- **Requires:** 3, 8, 12
- **Superseded by:** —
- **Implemented in:** `f45f1e3`

## Summary

A group's `ALLOW UPDATE REF` gate was read in the observed group's frame, so an observer could not gate an import with capabilities held in another bound group or in its own tables. This HIP reads the gate in the observer's frame, like its table rules: the gated binding at the version being imported, other bindings through the live fold as a row's guard reads them, and the observer's own tables at the observe's position; gates that read each other's bindings in a cycle are rejected at creation. A gate over the observer's own tables lets an observe share a verdict component with the observer's row ops, so the fold of observes becomes three-valued and a pending observe is tested with a touch rule, and the engine solves such loops as it solves loops among rows. How the runtime should respond to observes voided by remote changes is left open.

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

Reading the gate in `G`'s frame, through `G`'s bindings and table maps, lets `G` define its permissions in its own namespace. The same reading lets a gate use `G`'s own tables, for example an `admins` table that `G` keeps itself. That makes an observe's verdict depend on `G`'s rows, which needs care (Section 2.5).

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
2. **The gate graph is a DAG.** Its nodes are bound group ids, with an edge $H \to K$ whenever a gate on an alias bound to $H$ names an alias bound to $K \neq H$. A cycle is rejected. `G`'s own tables are not nodes. The engine could solve a loop of gates (Section 2.5), but the rule keeps observes of different bindings from reading each other in a loop, so every loop through an observe passes through `G`'s rows (Section 3.1). It also gives `ref_auto_update` and `applyCatalogPlan` an order to observe in (Section 4).
3. As today, a gate that reads `$author` needs `USING IDENTITIES`.

Nothing is checked at deploy time. Table rules change with every deploy, and Section 2.5 does not depend on them.

### 2.3 Versions read

An atom is read:

| | `a.t`, `G.bindings[a] = H`, the binding being gated | `a.t`, `G.bindings[a] = K ≠ H`, another bound group | `t`, a table of `G` |
| :--- | :--- | :--- | :--- |
| **At use** (`diagnoseObserve`, horizon `from`) | at $(\text{refAt}, \text{refFrom})$, with `refAt` from `resolveObserveGateRefAt` (mvt's `widenRefAtUpward`), as today | through `resolveSchemaForeignTableViewIn(evaluation, a, t, {y}, from, true)`: the fold of `K`'s live observes in `y`'s history and concurrent with `y`, the read a row op's guard makes | at (`{y}`, `from`), on the same evaluation, the read a row op's guard makes |
| **At admission** (`validateObserveGate`, position `at`) | at the imported version, (`newRefVersion`, `newRefVersion`), as today | at `G`'s observed version of `K` at (`at`, `at`), the read a row op's admission makes | at (`at`, `at`), the read a row op's admission makes |

At use, both folds are three-valued: an observe whose verdict is still being solved is a pending widening, of `refAt` for `H` and of the fold for `K` (Section 2.5).

A second alias of `H` is the binding being gated too. The edge rule of Section 2.2 leaves it out ($K \neq H$), and its atoms read `H` at (`refAt`, `refFrom`), through that alias's own table map. `[OBSGATE10]` has two aliases of one group, each with a gate.

Admission reads every other binding through the geometric fold, which counts every observe, void ones included, as a row op's admission does (`resolveSchemaForeignTableView`, called by `validateRowOpRestrictionAt`). At use the fold sees through void observes. Each fold blocks one attack:

- **At use, a void import brings no grant.** A grant that only a void observe imported authorizes no one, so a back-dated import by a former principal contributes nothing (`[OBSGATE08]`).
- **At admission, a revoke the author has seen binds her.** Every import in the op's past counts, whatever its verdict, so an op written after an import of a revoke cannot use the authority the revoke removes.
  - The live fold would not give this, because it is not monotone in the position. An import live at one position can be void at a later one, once that position holds an entry that voids it or collapses with it, and the live fold then drops it. An author could contest an import and then write past it.
  - In Section 3.3, Alice's back-dated delete collapses with `z`, Carol's import of `r`. Read through the live fold, a second delete by Alice from a position that has seen both would be admitted. It would also be live at use: it is after `z`, so `z` stays void, and `r` is after the version `G` reads. The geometric fold holds `r` whatever `z`'s verdict, and refuses the delete.
  - So knowing more never helps the author being judged, as under ideal concurrency ([`IDEAL_CONCURRENCY.md`](../modules/mvt/IDEAL_CONCURRENCY.md) §4.2).

The cost is the other direction: a grant imported only by a void observe can pass admission and fail at use, as it can for a row op today. That is why the runtime rule of Section 4 checks both; until its at-use guard lands, the runtime checks admission only. `mvt/MVT_DEFINITION.md` §4.1 defines admission as the use rule at the op's own horizon, which would read the live fold, so this departs from the definition (Section 6).

Only the binding being gated keeps the ref-advance's special contexts (`mvt/MVT_DEFINITION.md` §8, items 1 and 2). Every other binding, and `G` itself, is read the way a guard reads: in the use view at use, and at the op's own position at admission.

Admission becomes mixed. One gate reads `H` at the version being imported, and everything else at the observer's own position. The std_types README calls a ref-advance [the one op admitted at another version](../modules/std_types/README.md#admission-and-use). For an observer-frame gate, that holds for the observed binding only.

**The horizon.** At use, every read of a bound group also has a horizon. From `from`, `G` reads a bound group from the fold of all its observes of that group in the history of `from`, void ones included: `refFrom` for the binding being gated, and `foreignFrom` for the others (`diagnoseObserve` and `resolveForeignTableViewIn` in `src/rtable_group/group.ts`). The horizon is what `G` knows of the bound group at `from`, which is why it counts every observe: a void observe brings nothing into the version `G` reads, but the entries it imported have reached `G`, and a revoke among them is known. The horizon reaches a read at a version `D` in three ways (`src/rtable/view.ts`):

- a delete that is visible from the horizon and concurrent with `D` kills a row, when honored (`killedByConcurrentDelete`);
- the schema at `D` folds the deploys that are concurrent with `D` and visible from the horizon, and so does the schema at each such delete, which decides whether it is honored;
- the bound group's own verdicts are read at the horizon (`entryTruth`), so a read of `D`'s history can change too.

Through these, a version that reaches the horizon can revoke or restore what a read at `D` sees. So every observe advances the horizon, live or void, and only a live one advances the live fold. The runtime's guard, which waits, depends on this, and `LOG`, the REPL and rpack staging report a void observe as having advanced the horizon (Section 4).

### 2.4 Replacing the observed frame

The project is unreleased, so the observer frame replaces the observed frame instead of sitting beside it. The payload carries no frame marker. Evaluation (`evaluateObserveGateIn`), the static check (`groupViewReason`) and the compiler (`compileCatalogGroup`) each have one path, and C-SQL needs no new syntax.

The same text means different tables in the two frames, so existing gates are rewritten:

- An unqualified `t` that meant `H`'s table becomes `b.t`. In the editor example, `ALLOW UPDATE REF user IF EXISTS caps WHERE caps.grantee = $author` becomes `EXISTS user.caps WHERE user.caps.grantee = $author`. Left alone, `caps` would name a table of `doc`, which has none, so creation rejects it (Section 2.2). Only a gate whose observer has a table of the same name would change meaning silently; the rewrite has to check for that.
- A qualified `a.t` that walked one of `H`'s aliases needs `G` to bind that group itself. It is then read at `G`'s observed version, not at the version `H` observes (Section 2.3).

Catalog group hashes change, and the editor pack is rebuilt.

**What is lost:** a gate can no longer read a group through `H`'s bindings, at the version `H` observed when it reached the imported state. A gate asks who may advance `G`'s view, and `G`'s own view of the permissions group answers that, with revocations arriving through `G`'s use view (Section 3.2) and not only once `H` has observed them. Today's quirk, that such reads skip the void filter, goes away with the frame.

### 2.5 Gates that read `G`'s own tables

#### Why they need care

A row op of `G` reads bound groups through the fold of `G`'s live observes (Layer 2), so its verdict can depend on observes. A gate that reads `G`'s rows makes an observe's verdict depend on rows. Together they can form a loop through an observe. Today that is fatal: the fold needs each observe to be live or void, and `observeVerdictLive` throws on an undecided one.

Loops among rows are not the problem: the engine solves them in every group. The problem is a loop through one of `G`'s observes, and only `G`'s own rows can close one. A bound group's rows are judged from that group's history and the groups it binds, all below it in the binding DAG, so they never read `G`'s observes. Their loops close inside that group. The engine finishes those components, collapsing what stays undecided to void, before the gate's read returns. To the gate, a bound group's rows are already live or void, the same answer any reader of that group gets. What does depend on `G` is the version at which a bound group is read. For `H` that is `G`'s other observes of `H`, read by the upward widening (Section 2.3). For `K` it is `G`'s observes of `K`, ordered by the gate graph (Section 2.2). Neither closes a loop by itself (Section 3.1).

Take this example (rules are schematic). `G` binds `auth => K` and keeps a table `admins`:

- `admins` honors concurrent deletes, as capability tables do, and its delete rule is `$author IN auth.caps`;
- the gate is `ALLOW UPDATE REF auth IF $author IN admins`.

Alice is an admin, and Bob holds a cap in `K`. Without seeing each other:

- Bob deletes Alice's `admins` row;
- Alice observes a newer `K` that revokes Bob's cap.

Both outcomes are consistent:

- If Alice's observe is live, Bob's cap is revoked, so his delete is void, Alice stays an admin, and her observe is live.
- If her observe is void, Bob keeps his cap, so his delete is live, Alice is no longer an admin, and her observe is void.

No rule of the well-founded engine chooses between them. Alice's observe and Bob's delete form a core, like two concurrent revokes of each other.

Rejecting such schemas works badly. Table rules change with deploys, concurrent deploys merge slot by slot, and every merge rule found so far either fails open or loses changes (Section 2.6). The design below never looks at the schema's shape. It lets the engine solve the loop as it solves loops among rows. For that, the fold of observes becomes a three-valued read.

#### A three-valued fold

Today `resolveRefVersionAtPosition` and `widenRefAtUpward` (`mvt/src/refs.ts`) take a boolean liveness. Here it becomes a `Truth`, and the fold becomes one see-through walk, like a column read (`seeThroughCover`):

- a live observe contributes its published version;
- a void observe contributes nothing and does not hide the observes below it, as today;
- an undecided observe does not hide the observes below it either, but it is not dropped: it is kept as *pending*, unless a live observe above it dominates it. Every read of the bound group checks the pending observes before it answers.

The fold returns `D`, the version its live observes give, and `U`, the pending observes, each with its published version `V_o`. Deciding a subset `S` of `U` live gives the version $D \cup \bigcup_{o \in S} V_o$. A pending observe below a live one would add nothing, since ref-advances are monotone (`validateRefAdvanceMonotonicity`).

`U` is empty unless an observe's verdict is open: while the engine is still finding its component, or solving it. Everywhere else every observe the fold meets is already decided, and the fold is today's walk. Layer 1 works the same way. A concurrent observe strictly above the gate's base, whose verdict is undecided, is a pending widening of `refAt`.

#### Reading a bound group with pending observes

A bound group is read in two ways only: `existsMatching`, for `EXISTS` atoms, and `rowLiveness`, for FK reach (`src/rtable_group/predicates.ts`). Predicates call `columnValue` only on the subject row, which is local. `existsMatching` calls it on each candidate's searched columns, and its queries below include those writes. A view of `K` reads at (`D`, `foreignFrom`). `foreignFrom`, the unfiltered fold at the horizon, already holds every `V_o`.

A pending observe `o` *touches* a read when adding `V_o` to `D` changes something the read depends on:

- **a slot the read uses.** A deploy of `K` becomes visible, and the schema entries it newly makes visible write one of those slots. A deploy already concurrent with `D` is visible at `D`: the schema at (`D`, `foreignFrom`) already folds it.
- **the entries one of its queries matches.** A read makes two kinds of DAG query. A history query (`findCoverWithFilter` at the version) is touched when an entry that matches its filter enters the history of $D \cup V_o$. A concurrent query (`findConcurrentCoverWithFilter` from `foreignFrom`) is touched when such an entry enters the history, or when it was invisible at `D`, being after every head of `D`, and becomes concurrent with $D \cup V_o$.

The slots and the queries are the read's own:

- `rowLiveness(r)` uses the table's existence slot, written when the table is added or dropped, which fixes the table's incarnation. Its queries are the history query for `r`'s identity tag, which matches the inserts and deletes of `r`, including a deploy that deletes `r`, and the concurrent query for the deletes that carry that tag (`rowState` and `killedByConcurrentDelete` in `src/rtable/view.ts`).
- `existsMatching` uses the existence slot and the slots of the searched columns, written when a column is added or dropped, which fix their pub flags, defaults and incarnations. Its queries are, for each candidate, those of `rowLiveness` and the history query for the candidate's writes to each searched column (`columnValue`). A column write that only becomes concurrent changes nothing, since `columnValue` reads the history alone.

Neither read uses a table's restrictions, foreign keys or concurrent-deletes flag at `D`. Those decide verdicts, and whether a concurrent delete is honored, and both are read at each entry's own position (`killedByConcurrentDelete` in `src/rtable/view.ts`).

Then:

- **A touched slot:** the read is undecided, supported by the touching observes. A table absent at `D` is a missing reference, read as false, only when no pending observe touches its existence slot.
- **`rowLiveness(r)`, its slot untouched:** if none of its queries is touched, it is the answer at `D`; otherwise it is undecided, supported by the touching observes.
- **`existsMatching`, its slots untouched:** candidates are gathered at $D \cup \bigcup_{o \in U} V_o$. A candidate's match is the Kleene `and` of its column reads and its row's liveness, as today, and each of those is a read of its own: untouched, it keeps its value at `D`; touched, it is undecided, supported by the touching observes. So a candidate that an untouched column rules out is decided false, whatever else is touched. A candidate that only a pending observe brings in usually has a touched read, since the write that brings it in enters the history. But candidates are found through pub values, which ignore incarnations, so that write can belong to a dropped column or table. Then no query of the candidate matches it, and the candidate keeps its answer at `D`, which is its answer in every completion. The result is the Kleene `or` over the candidates, as today.

A touched slot settles the whole read, not one candidate at a time, because the schema decides which rows are candidates. A row can match through a column default without ever writing the searched value.

- Say a rule reads `EXISTS auth.allowed WHERE user = $author AND active = true`. Bob's row in `allowed` was written with `active: false`, and a pending import brings a deploy that drops `active` and re-adds it with default true.
- The row's write belongs to the old incarnation, so with the deploy visible the row matches. Under `D`'s schema it is not a candidate.
- Touching candidates only, the read would find no candidate to touch, and would be decided false while the import is still pending. "Nothing decided is retracted", below, says what that does to a loop.

#### Computing a touch

The test runs the read's own queries again at $D \cup V_o$, one pending observe at a time, and compares each with the same query at `D`, which is the read's own. Every comparison applies the read's liveness predicate, so it compares the entries that match the query's filter and are live in `K` at `foreignFrom`. A write that is void in `K` touches nothing. The verdicts the test reads are `K`'s: `K`'s entries never read `G`, so they are complete before `G`'s read returns, and they are the same in every completion.

- **Slots.** Resolve `K`'s schema version at (`D`, `foreignFrom`) and at ($D \cup V_o$, `foreignFrom`) (`resolveSchemaVersion` in `src/rtable_group/group.ts`). The schema entries in the history of the second and not of the first are the ones `o` makes visible, and `slotsTouchedByRule` (`src/rschema/resolve.ts`) gives the slots their rules write. When the two versions are equal, no slot is touched. The test is structural: it never compares resolved values, which do not add up over observes (Section 7, item 1). `RSchema.computeDelta` compares values, so it does not fit.
- **History queries.** Compare the covers of the query's filter, with the predicate, at `D` and at $D \cup V_o$.
- **Concurrent queries.** Compare the concurrent covers of the query's filter, with the predicate, from `foreignFrom`, relative to `D` and to $D \cup V_o$, and compare the history covers of the same filter too. In `rowLiveness` the identity query's comparison already does the second, since the identity tag also matches the deletes.
- **The shortcut.** When $V_o$ is in the history of `D` (`refVersionAtOrAbove`, a fork-position query), $D \cup V_o$ has the history of `D`, and `o` touches nothing. Walkthrough 4's `y` is this case.

The slots come first, because they fix the filters: a table's incarnation is part of its rows' identity tags, and a column's incarnation is part of its write tags. A read that `o` touches is undecided, and its support is the union of the supports of the touching observes' verdicts, as the fold received them from `resolve`. The queries at `D` are shared by every pending observe, and `existsMatching` gathers its candidates once, at $D \cup \bigcup_{o \in U} V_o$. The gated binding is read the same way, at (`refAt`, `refFrom`), with the widening's pending observes.

#### Why the rule is sound

Write $H_F(X)$ for the entries of `K` that match a filter `F`, are live in `K` at `foreignFrom`, and are in the history of a version `X`. Write $C_F(X)$ for those that match `F`, are live, are visible from `foreignFrom` and are concurrent with `X`. The argument has four steps.

1. **A read depends on the live sets its queries match.** Take a read of one row: `rowLiveness`, or a column read or the liveness of one candidate in `existsMatching`. With its slots fixed, it depends on the version only through the live sets its queries match. `K`'s verdicts are decided, so a see-through walk returns the topmost members of its live set: it stops only at live matches, and it reaches every live match that no live match is above, since every entry on a path down to that match lies above it. The entries it passes on the way depend on the version, but they do not change its result. So two versions that give each query the same live set give the read the same answer. The verdicts are the same in every completion, because `K`'s entries never read `G`. They are completed before `G`'s read returns, so the answer at `D` is always decided.
2. **Equal covers mean equal sets.** Let the history of `X'` contain that of `X`, as the history of $D \cup V_o$ contains that of `D`. With the predicate, a history query at `X` returns the topmost entries of $H_F(X)$, and a concurrent query the topmost entries of $C_F(X)$.
   - **This is a contract on every DAG index**, and step 1 relies on it too. A cover holds every topmost entry of its set, which soundness needs. It holds nothing else, which the "exactly when" below needs and which keeps the test from touching for nothing. A match the predicate rejects is seen through, so a live match below it can be topmost. No index documents this today. `[TOUCH_03]` checks it for the three indexes (Section 5), and dag documents it (Section 4).
   - The history covers of `F` at `X` and `X'` are equal exactly when $H_F(X) = H_F(X')$. A match that enters the history lies below a topmost match of $H_F(X')$, and that one entered too: had it been in the history of `X`, so would everything below it.
   - When those are equal, the concurrent covers are equal exactly when $C_F(X) = C_F(X')$. No match enters the history, and a match concurrent with `X` cannot be after `X'`, since it would then be after `X`. So a change can only be a match that was after every head of `X` and is concurrent with `X'`. Every entry above it is after `X` as well, so a topmost member of $C_F(X')$ above it is new, and the cover changes.
   - A concurrent query needs the history comparison as well. A concurrent match can enter the history below one that does not, and leave the concurrent cover as it was.

   So the comparisons see every change to the live sets, and by step 1, a read whose live sets and slots do not change keeps its answer.
3. **Changes add up.** If no observe in `S` changes one of the read's live sets on its own, $D \cup \bigcup_{o \in S} V_o$ does not change it either. Whether an entry is live does not depend on the version read, so only its position matters. Histories are unions, so a match in the history of the union is in the history of `D` or of some $V_o$. An entry is after every head of a union only if it is after every head of each part, so a match after `D` that stays after each $D \cup V_o$ stays after the union. Deploys become visible the same way, so the schema entries that the union makes visible are those that some $D \cup V_o$ makes visible, and a slot that no pending observe touches has its value at `D` in every completion. With the slots fixed, so are the filters, and steps 1 and 2 apply in every completion.
4. **Candidates.** For the `or` of `existsMatching`, every completion's candidates are among those gathered at $D \cup \bigcup_{o \in U} V_o$: a row is a candidate when its writes carry the searched values or the defaults supply them, and with the slots fixed both only grow with the history.

Hence:

- **sound:** a decided result holds in every completion;
- **exact:** with `U` empty, the read is today's read at `D`;
- **monotone:** when a pending observe `o'` is decided live, `D` grows to $D' = D \cup V_{o'}$. A match that enters the history, or becomes concurrent, between $D'$ and $D' \cup V_o$ does so between `D` and $D \cup V_o$, and so does a schema entry that becomes visible, so the touches against $D'$ are a subset of those against `D`. When `o'` is decided void, it leaves `U`. A decided read stays decided and keeps its value.

The rule is structural. It compares the read's queries at two versions of `K` and never tries a completion ([`VOID_SEMANTICS.md`](../modules/rdb/VOID_SEMANTICS.md) §4, "Structure, never enumeration"). The touch test reads only `K`'s verdicts, as the read itself does. They complete before `G`'s read returns, so the test never reaches `G`'s open component.

**Nothing decided is retracted.** The engine's solve alternates two steps (`mvt/WELL_FOUNDED_VERDICTS.md` §6.2):

- The lower step marks members live. It reads the members already marked live as live, and every other member that is still possibly live as undecided, so a pending observe touches each read it could change, and no member is marked live on an answer that a pending observe could reverse.
- The upper step reads the members it has not added yet as void, so there the fold drops a pending observe and reads at `D`. That only bounds which members may be live, and the bound only shrinks.

The solve needs every read to keep its value as its inputs are decided, and to be exact once they all are, which the three properties above give. `verdict_evaluation.ts` asserts both, but the assertions catch only a result that contradicts itself. A read decided too early, such as the `auth.allowed` read above with touches applied per candidate, does one of two things, depending on the loop it is on:

- **On an even loop, the solve settles it silently.** Carol imports the deploy with `z`, gated by `$author IN admins`. Bob, having seen `z`, deletes Mallory's `moderators` row with `d`, whose rule is the `auth.allowed` read for Bob's row. Mallory, concurrently with both, deletes Carol's `admins` row with `e`, whose rule is `$author IN moderators`. Both tables honor concurrent deletes. `z` and `d` live with `e` void is consistent, and so is the opposite, so the correct result collapses all three. With `d` decided void early, the solve ends with `e` live and `z` void, and both assertions pass.
- **On an odd loop, the solve throws.** In the loop configuration of Section 3.3, let Alice's delete of Carol's `admins` row read `auth.allowed` for Alice's own row, admitted on an earlier import of the same deploy that is void at use. Carol's import `z` live makes the delete live, which voids `z`, and `z` void voids the delete, which makes `z` live. With the delete decided void early, `z` becomes live. The next lower step runs the delete again, since it re-runs every member not yet collapsed, including those outside the possibly-live set. The delete's rule now reads true, the delete is marked live, and the check after the upper step throws `became possibly live again`. Every replica that reads that history fails the same way.

The even case is the easier one to reach. An op that relies on an import is admitted only at a position that has seen it, so in the first example `d` reaches `z`'s gate only through `e`, a second delete. The two-member loop needs the extra, void import. Rules are monotone in their reads, since the grammar has no negation, but verdicts are not: a live delete kills a row whatever its rule says. Whether a loop is odd or even depends on the deletes on it, and on which way an import moves its reads, so the read-level rule is needed in both cases.

#### How an observe is decided

`diagnoseObserve` evaluates the gate in three values:

- **true:** the observe is live;
- **false:** void, with reason `observe-gate`, as today;
- **undecided:** the observe is undecided, and the engine solves its component. A member left undecided collapses to void, `undecided-cycle`, observes included.

Admission (`validateObserveGate`) evaluates the gate at (`at`, `at`) (Section 2.3). The observe being admitted is not in the DAG yet, so it is on no cycle, and every component the gate reads is complete by the time the gate reads it.

#### Why the collapse does not fail open

The previous version of this section kept a third value, *unknown*, out of the collapse. There the gate judged Bob's delete by a second verdict while ordinary views judged it live, so collapsing the gate's copy alone would have let the gate trust an admin whom every view had removed.

Here each entry has one verdict:

- Bob's delete is void for every reader, and Alice is an admin for every reader.
- No gate reads her row as live while the delete is open. Her observe is in the same component and collapses with it.

The collapse returns contested state to what it was (`mvt/WELL_FOUNDED_VERDICTS.md` §4.3). Alice keeps the admin row she had, Bob keeps the cap that `G` saw, and `G`'s live fold of `K` stays at the version it had. That is all it promises. `y` still advanced the horizon (Section 2.3), so what it imported can change `G`'s reads of `K` at that version, for every reader alike, as any import in `G`'s history can. Here it does not: `y` brings only the revoke of Bob's cap, which is after the version `G` had.

#### Walkthrough

`G` binds `auth => K` and `target => H`, and keeps `admins`, seeded at genesis with a row for Root. `admins` honors concurrent deletes.

1. **Local rules.** `admins`' insert and delete rules are `$author IN admins`, and the gate is `ALLOW UPDATE REF target IF $author IN admins`.
   - `admins`' rules read no binding, so no row the gate reads depends on an observe, and nothing is pending.
   - The gate sees what any reader of `admins` sees. This is the common case.
2. **Rules through `auth`.** `admins`' rules are `$author IN auth.caps`, with the same gate. `auth` is ungated, so its observes are always live, and the fold of `K` has nothing pending.
3. **The loop.** As in 2, with one addition: `ALLOW UPDATE REF auth IF $author IN admins`. In the race above:
   - Alice's observe `y` reads `admins` at (`{y}`, `from`). Bob's delete `d` of her row is concurrent with `y`, so her row's liveness reads `d`'s verdict.
   - `d`'s rule reads `auth.caps` through the fold. `y` is concurrent with `d` and its verdict is open, so `D` is the version both of them saw and `U = {y}`. `y` imports the delete of Bob's cap row, so it touches the read, and `d` is undecided, supported by `y`.
   - Alice's row is undecided, supported by `d`, so the gate and `y` are undecided, supported by `d`.
   - `{y, d}` is a component with no live member. Both collapse to void (`undecided-cycle`). Alice stays an admin, and `G` keeps reading `K` without the revoke, since the fold sees through `y`.
   - The next op that has seen the other side settles it, and only Alice can write one. An observe by Alice from the merge has `d` in its history and is not concurrent with it, so it is not in `d`'s fold. Her row is live, the new observe is live, and from then on `G` reads Bob's cap as revoked. A delete by Bob from the merge is refused: admission reads `auth.caps` through the geometric fold, which holds `y`'s version whatever `y`'s verdict (Section 2.3). Bob can answer only from a position that has not seen `y`, and such a delete contests Alice's imports again (Section 3.3).
   - This is how revoke races between rows end (`mvt/WELL_FOUNDED_VERDICTS.md` §5.2 and §5.3). Either side can answer the other's next op with one written before it, and honest peers settle as soon as one side's op has the other's in its history.
4. **Carol's import.** As in 3, but Carol, also an admin whose row no delete targets, has imported the revoke of Bob's cap in an observe `y_C` concurrent with Bob's delete.
   - `y_C` is live, so `d`'s fold has the revoke in `D`, and `d` is decided void.
   - If Alice's `y` imports the same version, it adds nothing to Bob's cap row and does not touch `d`'s read. `y` is live.
   - Ordinary views and the gate agree, and `target`'s gate is unaffected.
5. **An import that misses the read.** As in 3, but `y` imports changes to `K` that leave Bob's cap row and `caps`' slots alone.
   - `d`'s read is untouched, so `d` is decided live.
   - Alice's row is deleted, and `y` is void with reason `observe-gate`. Her import cannot undo Bob's removal.

#### Precision

- **The test asks whether the read's queries match different live entries, not whether its answer changes.** Two kinds of import make a concurrent delete that reads Bob's cap undecided even though they do not change its answer:
  - one that brings a live write to a searched column of his cap row that does not change its value;
  - one with a deploy that writes a slot the read uses without changing what it resolves to.

  Section 3.3 says who can cause that. Section 7 discusses a finer test (item 1) and a preference order that would remove such cores (item 3).
- **A new candidate never undoes a decided match.** A clean, untouched match decides the `or`. An import that grants Bob a second cap leaves a delete decided when his first cap decides it.
- **Writes the read does not walk touch nothing.** That covers writes to columns it does not search, rows it does not look at, column writes that only become concurrent, writes that are void in `K`, and deploys that leave the slots it uses alone, such as one that changes only `caps`' rules. `rowLiveness` walks no updates.

#### What it costs

- **No new keys.** An observe has one verdict, memoized in the evaluation like every entry's.
- **Extra work only for open observes.** `U` is non-empty only while an observe's verdict is open. Everywhere else the fold is today's walk.
- **A touch memo that is always safe.** The touch test reads only `K`'s verdicts at `foreignFrom`, which (`K`, `foreignFrom`) fix. Each comparison depends only on a filter or a slot and on (`K`, `D`, `V_o`, `foreignFrom`), versions that name fixed histories, so it can be memoized with canonical versions. A solve runs its members' diagnoses many times, so the memo matters there. It dies with the computation, like the evaluation's memo of verdicts, which bounds it (Section 4). Keeping it longer would be sound, as a cache of completed verdicts across computations would be. Neither exists, and whether one pays is unmeasured (`mvt/WELL_FOUNDED_VERDICTS.md` §6.5; [`INCREMENTAL_DELTA_BOUND.md`](../modules/rdb/INCREMENTAL_DELTA_BOUND.md) §6.2).
- **Filtered covers.** The query at `D` is the read's own. The test adds one filtered cover query per pending observe for each of the read's queries, at $D \cup V_o$. Those also compute `K`'s verdicts for matches that the read at `D` never meets, down to the first live ones. `K` would compute those verdicts anyway if the observe were live, so the extra work is at most one live fold per pending observe. The test gains directly from faster filtered covers.
- **Components stay small.**
  - An entry in `y`'s history never folds `y`.
  - `y`'s gate reaches entries outside its history only through the concurrent deletes of the rows it reads, and then through what their rules read (Section 3.1).
  - A reader that folds `y` without being touched still joins the component, because the engine groups entries by what they read. It is decided on the first pass.
- **`refAt` only when needed.** `refAt` is computed only for gates that read the gated binding, so an observe does not read the binding's other observes for nothing.
- **Polynomial solves.** A solve is polynomial in the size of its component (VOID_SEMANTICS §4).

#### Changes to the engine

None. `VerdictEvaluation` is unchanged. Observes become ordinary members of components, and the fold is a read rule like a cover walk. The throw in `observeVerdictLive` goes.

### 2.6 Alternatives considered

Six other ways were considered to let gates use data that `G` controls.

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
     - Failing closed at the gate needs information per row, which alternative 4 provides.
   - Every fallback is permanent, because the rows carrying the old rules remain.
4. **Gate verdicts that read local-gated bindings as unknown** (this section from 2026-10-30 to 2026-11-01). A binding was *local-gated* when its gate read `G`'s tables. Gates read `G`'s tables through a second verdict per entry, which read local-gated bindings as *unknown*, a third value that never collapses, and an unknown gate failed closed.
   - It needed engine changes: unknown inputs that block the collapse, and undecided answers stored as such.
   - It needed a second verdict, under its own keys, for every entry a gate reads.
   - It was conservative. In walkthrough 4 it refused Alice's observes of `auth` and `target`, although Bob's delete was void in every view. And unknown stuck to a row for good: the remedy was a new grant under a new row.
   - It settled the race of walkthrough 3 the other way: the observe was always void and the delete live, so a removal always beat a concurrent import. The three-valued fold voids both, as the engine does for any contested pair (Section 7, item 3).
5. **A fold over completions.** The first form of the three-valued fold read `K` at every version $D \cup \bigcup_{o \in S} V_o$ for the subsets `S` of `U`, and decided a read when they all agreed.
   - That tries the ways of deciding its undecided inputs, which no rule of the engine may do ([`VOID_SEMANTICS.md`](../modules/rdb/VOID_SEMANTICS.md) §4; `mvt/WELL_FOUNDED_VERDICTS.md` §5.4).
   - It is exponential in the size of `U`.
   - It decides more than a structural rule, which changes which cores exist.

   It stays as the test oracle: wherever the touch rule decides a read, the result must hold in every completion (Section 5).
6. **Ordering by the observed version.** In the observed frame, observes stay off cycles because a gate reads only `H`, and the widening only climbs `H`'s versions ([`VOID_SEMANTICS.md`](../modules/rdb/VOID_SEMANTICS.md) §5.5). A gate over `G`'s tables reads `G`'s rows at the observe's own position, and those rows read the observe back through the fold, at fixed versions. The loop never climbs a version, so no order on versions breaks it.

Section 2.5 needs no deploy checks, no merge rule and no engine change. It changes nothing where no loop is possible. Where one is, the engine solves it as it solves loops among rows.

---

## 3. Why it is sound

Take `G` binding `target => H` and `auth => K`, with a gate on `target` that reads `auth.caps`. `y` is an observe of `H` in `G` by author `a`, judged from horizon `from`.

### 3.1 Observes on cycles

Today an observe is never on a cycle, and `observeVerdictLive` throws on an undecided one ([`VOID_SEMANTICS.md`](../modules/rdb/VOID_SEMANTICS.md) §5.5). Under this design an observe can share a component with `G`'s row ops, as Alice's observe and Bob's delete do in Section 2.5. The engine needs no change for that, since an observe is an entry like any other. What needs arguing is that the new reads keep the engine's contract (`mvt/WELL_FOUNDED_VERDICTS.md` §5.5), and where the cycles can run.

**The contract.**

- **Keys.** An observe's verdict keeps its key, `<group>|<entry>|<from>`. The fold reads verdicts and stores none, so it adds no keys.
- **Reads.** The fold reads observe verdicts through `resolve`, on the reader's evaluation, with `seeThroughCover`, like a column read.
- **Rules.** The touch rule is exact, sound and monotone, with supports, and structural (Section 2.5).
- **Undecided stays inside.** The public entry points still throw on an undecided verdict. Every component is complete by the time they return.

**Where cycles run.** Every cycle through an observe passes through a row op or bundle of `G`:

- Leaving aside reads through `G`'s rows, an observe reads other observes of its own binding only through its widening, which climbs the observed group's versions strictly. It reads observes of other bindings only through the folds of the bindings its gate names, which are below its binding in the gate graph (Section 2.2). Bound groups' entries never read `G`. So every dependency between observes strictly descends the lexicographic order
  $$\bigl(\text{rank of the object in the binding DAG},\ \text{rank of the binding in the gate graph},\ H(\text{observed version}) \text{ under } \subsetneq\bigr),$$
  which is well-founded, and observes alone form no cycle.
- A cycle therefore closes through a gate's read of `G`'s tables, and through a row op whose rule reads a binding through the fold.
- An entry in `y`'s history never folds `y`, since `y` is neither in its history nor concurrent with it. So the row ops on a cycle with `y` are concurrent with `y` or after it. The gate first reaches them through the concurrent deletes of the rows it reads.

This keeps components small. It answers "Observes that read each other" in `mvt/WELL_FOUNDED_VERDICTS.md` §6.5.

**The narrow context of the ref-advance gate** (`mvt/MVT_DEFINITION.md` §8, item 1) stays. Its stated reason, that it keeps observes off verdict cycles, no longer holds once gates read `G`'s tables. What it still does is keep the observes of one binding from reading each other except up the version order. The argument of [`IDEAL_CONCURRENCY.md`](../modules/mvt/IDEAL_CONCURRENCY.md) §4.4, that no revoke is lost, is unchanged.

### 3.2 Revocations through another binding

Let $r$ be a live revoke in `K` of the capability `a` relies on, visible from `from` in `G`. Write $V$ for `y`'s version of `K` at use.

1. If $r \in H(V)$, Step 1 sees it, and `y` is void.
2. If $r \notin H(V)$ and $H(V) \not\subseteq H(r)$, then under ideal concurrency $r$ is concurrent with $V$ ([`IDEAL_CONCURRENCY.md`](../modules/mvt/IDEAL_CONCURRENCY.md) §4.4). Step 2 finds it at the horizon version where the capability table honors concurrent deletes, as for any guard read.
3. Otherwise $H(V) \subsetneq H(r)$. Every live observe of `K` that carries $r$ is concurrent with `y`, before it, or after it in `G`. If it were concurrent or before, the fold would put $r$ in $H(V)$. So each is after `y`: use before revoke in both DAGs, which no rule voids without clocks (`[XGROUP11]`). A void observe that carries $r$ does not count, as in every Layer 2 read.

So a revoke arriving through `auth` voids `y` exactly when it would void a row op of `G` at `y`'s position that reads `auth`. The case $K = H$ is the existing argument of [`VOID_SEMANTICS.md`](../modules/rdb/VOID_SEMANTICS.md) §5.5, which ideal concurrency made exact.

The first version of this note expected the cross-binding case to need its own attack analysis. Since `ALL` was replaced, it does not. The Layer 2 fold takes every live concurrent observe, not only dominating ones, so there is no mixed-cut gap to close.

Reading `K` in the use view has the consequences every guard read has:

- A grant on a branch of `K` that only a concurrent live observe imports counts for `y`. For the binding being gated it does not (`mvt/MVT_DEFINITION.md` §8, item 1).
- `y` sees through voided observes of `K`, so a back-dated import by a revoked observer of `K` contributes nothing (`[OBSGATE08]`'s rule, applied to `K`).

**Revocations in `G`'s own tables** follow the same rule. The gate reads `G`'s tables at (`{y}`, `from`), like a row op's guard. So a live delete in `y`'s history, or concurrent with `y` in a table that honors concurrent deletes, voids `y`. A delete in a core with `y` is void, and so is `y` (Section 2.5).

### 3.3 Races between imports and removals

An observe of `K` only adds `K`'s own entries, judged by `K`'s own verdicts, so it cannot forge a revoke. It voids an honest `y` only through cases 1 and 2: a live revoke of `y`'s author, issued without seeing all of $V$. That is the honest-loss cost of ideal concurrency ([`IDEAL_CONCURRENCY.md`](../modules/mvt/IDEAL_CONCURRENCY.md) §5.3), the same one row ops pay. It holds even when `auth` is ungated and anyone who can write to `G` may observe `K`. Observing `K` before `H` (Section 4) narrows the window.

**Contested removals.** An import can also put an observe in a core with a concurrent delete whose read it touches (walkthrough 3). Both are void, and the removal does not take effect until one side acts again, having seen the other. This is the stalemate of revoke races (`mvt/WELL_FOUNDED_VERDICTS.md` §5.3), with the import standing in for the counter-revoke.

The importer needs no authority in `K` for this. What they need is an entry in `K` that the delete's read walks and that `D` lacks:

- a write to the cap row the delete relies on that is live in `K`, from someone with authority over that row in `K`;
- or a deploy of `K` that writes the existence slot of that row's table, or the slot of a column the read searches, from one of `K`'s deployers.

Either party already controls the deleter's authority in `K`: the first can revoke the cap, and the second can change the table's rules or drop it. A write that is void in `K` touches nothing, because the comparisons count only live entries. So a principal of `K` whose authority over the row was revoked cannot contest the delete by writing to the row at a back-dated position, where `K` admits the write and then voids it at use. An import that leaves the delete's read untouched cannot contest it (walkthrough 5).

**Blocking the import of a revoke.** The same core works in the other direction, in the loop configuration of walkthrough 3:

1. `K` revokes Alice's cap with `r`, and Carol imports `r` with an observe `z`.
2. Alice deletes Carol's `admins` row, at a position concurrent with `z`. At that position `G` has not imported `r`, so the delete is admitted.
3. At use, the delete's read of `auth.caps` folds `z`, which is pending and touches Alice's cap row. `z`'s gate reads Carol's row, which the delete may kill.
4. Both are void. Carol stays an admin, and `G`'s live fold of `K` stays without `r`, although `r` is in the horizon (Section 2.3).

This is the counter-revoke stalemate of `mvt/WELL_FOUNDED_VERDICTS.md` §5.3: each entry is authorized by what the other cancels. Alice's delete is the dodge of [`IDEAL_CONCURRENCY.md`](../modules/mvt/IDEAL_CONCURRENCY.md) §4.2, an op below the revoke. What it contests is the one thing that defeats a dodge, a concurrent import of the revoke. It needs the same authority: Alice must be able to remove the importer, as a counter-revoker must manage the revoker's capability.

Every answer is back-dated. After the collapse, Carol observes again from the merge. That observe has the delete in its history, so it is outside the delete's fold. Its gate sees Carol's row live, since the delete is void, and it is live. Alice can contest it only with a new delete at a position that has not seen `z`. At a position that has, admission reads `auth.caps` through the geometric fold, which holds `r` whatever `z`'s verdict (Section 2.3), and refuses the delete. So each answer is a dodge written below the import it contests, and a client that writes at its frontier, having seen the import, cannot write one. Under the full rule of Section 4, each answer also makes every replica whose author passes the gate observe again after the batch that brings it: one duplicate observe of the same version per replica, as many times as the attacker answers. That part of the rule waits (Section 4, rdb_runtime; Section 7, item 8).

Two things make it worse than within a group:

- **An import is all-or-nothing.** A counter-revoke voids one revoke, but here every later import of `auth` that reaches past `r` touches Alice's read: it either holds `r` or makes `r` concurrent with the read.
  - While Alice answers each one, `G`'s view of `K` stays at the state `r`'s author had seen, and `K`'s later grants, revokes and deploys do not reach `G`'s guards and gates.
  - Imports below `r` still go through, and other bindings keep updating.
- **The decision was made elsewhere.** Within a group, peers race. Here `r` is settled in `K`, and every other reader of `K` sees it. Only `G`'s import of it is held up.

What protects against the stalemate within a group protects here: authority the attacker cannot remove is not exposed.

- An importer whose `admins` row `K`'s cap holders cannot delete always gets the import through.
- A binding outside the loop is never blocked. That covers a gate on `auth` that reads `auth.caps` itself, or one that reads a table whose rules do not read `auth`.
- A catalog lint can warn about the loop. It cannot be a rule, because deploys change table rules (Section 2.6, item 3).

A preference order for cores would remove the stalemate here and within groups alike (Section 7, item 3).

### 3.4 The bounded delta

`walkRevisionBound` (`mvt/src/revision_bound.ts`) was written for observe verdicts that read their own reference, and for the foreign views of row ops. An observer-frame verdict reads a second reference, and possibly `G`'s own tables. The walk itself needs no change:

- An observe of `K` in the meet's history whose version is above `K`'s bound is handled by Phase A, which runs over every declared reference. Any `y` after it is then above the bound and walked.
- An observe of `K` concurrent with `y` is in `start`, since new entries are after the meet. It is below the bound and keeps its verdict, or it is walked. If it is unstable or flipped, the bound drops to the GLB with its predecessors, which also removes `y`, because `y` is concurrent with it.
- A gate reads `G`'s tables at (`{y}`, `from`), as a row op's guard does. The walk covers those reads the way it covers a guard's.
- An observe void by collapse at either horizon counts as changed, like any barrier ([`INCREMENTAL_DELTA_BOUND.md`](../modules/rdb/INCREMENTAL_DELTA_BOUND.md) §5.7). Observes are barriers, so the visitors already do this.
- The visitor records `y`'s own flip through `explainEntryVoided` at both horizons (`horizonVerdicts` in `src/rtable_group/delta.ts`), which also tells a collapse apart.

The proof of Theorem 1 in [`INCREMENTAL_DELTA_BOUND.md`](../modules/rdb/INCREMENTAL_DELTA_BOUND.md) covers the read of `K` as a foreign view anchored at `y`, and assumption A2 already sets verdicts void by collapse apart.

**The touch test's reads.** An entry of `G` that reads `K` while some of `G`'s observes of `K` are pending also reads `K` beyond `D`: at $D \cup V_o$ for each pending observe `o`, and at `D` plus every pending version for the candidates of `existsMatching` (Section 2.5). With the live comparison, its verdict then depends on `K`'s verdicts for entries outside `D`. Theorem 1 covers these reads too. Let `B` be the final bound, $U = H(B)$, and `e` an entry of `G` in `U`, an observe or a row op:

1. **Every observe of `K` in `e`'s fold is stable**, whether live, void or pending. By Lemma 1 no new entry is concurrent with `e`, so the fold has the same members at both horizons. One in `e`'s history is in `U`, and by Lemma 5 no observe in `U` is unstable. One concurrent with `e` is not strictly after `B`, since `e` is in `U`, so it is in `U`, or by Lemma 6 it is stable with the same verdict at both horizons. A stable observe's version lies inside $H(b_K)$, the bound computed for `K` between `refAtStart` and `refAtEnd`. So `D`, each $D \cup V_o$ and their union all lie inside it.
2. **No new entry of `K` is concurrent with those versions**, by Lemma 1 for `K`. So the concurrent covers at those versions gain no new member, and the history covers range over histories with no new entry.
3. **Theorem 1 for `K` fixes the rest**, and A6 lets the proof assume it. Part (a) fixes the verdicts of `K`'s entries inside its bound. Part (b) fixes every read of a view anchored at those versions: the covers with their predicate, including the verdicts of concurrent barriers outside the bound, the schema versions the slot test compares, and the candidates.
4. **With its inputs fixed, the solve of `G`'s entries is deterministic**, by A2. A verdict void by collapse at either horizon counts as changed, as the visitors already do.

The text of the bound's argument still has to change, when the design lands (Section 6):
- Assumption A3 lists "for a gated observe, its gate in the foreign group's frame". Guarantee 6 of `walkRevisionBound` describes a verdict as a read of its own reference. Both must say that a verdict may read other references through the full live fold, and `G`'s tables at (`{y}`, `from`) as a guard does.
- Both must also list the touch test's reads, for every entry that reads a bound group, not only for observes.
- The proof of Theorem 1 must carry step 1 above, which is what makes the other steps apply.

Before this is relied on, the parity sweeps must cover observer-frame gates, gates over `G`'s tables, cores that hold an observe and a row op, and a side-branch observe of `K` that stays pending for an entry of `G` below the meet.

---

## 4. Changes

**rdb**

- The rules of Section 2.2:
  - Rule 2, the gate graph, is structural. The format validators check it (`src/rtable_group/validate.ts`, `src/rcatalog/validate.ts`).
  - Rule 1 needs `G`'s pinned schema, which the format validators do not have. `validateCreate` (`src/rtable_group/validate_ops.ts`) and `groupViewReason` (`src/rcatalog/validate_ops.ts`) check it.
  - Rule 3 stays where it is checked today.
- `src/rtable_group/group.ts`:
  - `evaluateObserveGateIn` takes the observe's position and horizon (`opPos`, `from`).
    - An atom on an alias bound to `H` reads `H` at (`refAt`, `refFrom`), with the widening's pending observes. `refAt` is computed only when the gate has such an atom.
    - An atom on any other alias goes through `resolveSchemaForeignTableViewIn`.
    - An unqualified atom reads `G`'s table through an ordinary view at (`{y}`, `from`), on the same evaluation. It needs no `exclude`: an observe carries no row ops.
  - `diagnoseObserve` passes (`{y}`, `from`). It already returns an undecided gate as undecided.
  - The public `evaluateObserveGate` takes the observe's position and the imported version as named options (`opPos`, `imported`). Both are a `Version`, and today's callers already confuse them (rdb_runtime, below). A third option, `fold`, says how other bindings are read: `'admission'`, through the geometric fold, as `validateObserveGate` reads them, or `'use'`, through the live fold with `opPos` as the horizon, for the runtime's guard. Under either fold, atoms on the gated binding read `H` at (`imported`, `imported`), and `G`'s tables are read at (`opPos`, `opPos`).
  - `isObserveLive` and `observeVerdictLive` return the verdict's truth. The throw on an undecided observe goes.
  - `resolveForeignTableViewIn`, with `filterVoided`, folds through the three-valued fold (mvt, below). Each observe's liveness is its verdict's truth (`observeVerdictTruth`, which replaces `observeVerdictLive` and does not throw); an ungated binding's observes are live.
    - It builds the schema at (`D`, `foreignFrom`). A table absent at `D` is a missing reference, `undefined`, only when no pending observe touches its existence slot. Otherwise it returns the view, whose reads report the touched slot.
    - FK reach reads `rowLiveness` through the same function (`src/rtable_group/predicates.ts`), so it needs nothing more.
    - Without `filterVoided`, at admission and in the public wrappers, the fold is geometric and nothing is pending.
  - `resolveObserveGateRefAt` returns `refAt` together with the widening's pending observes, and `evaluateObserveGateIn` passes them to its views of `H` at (`refAt`, `refFrom`).
- **The touch rule in code** (`src/rtable/view.ts`, with the rule itself in `src/rtable/touch.ts`). It sits in the two reads that every read of a bound group goes through, `rowState`, behind `rowLiveness`, and `columnValue`, and in the slot check and candidate gathering of `existsMatching`. `test/touch_rule_tests.ts` checks it (Section 5).
  - `RTableViewImpl` takes the pending observes as a list of `{ version, verdict }`: each observe's published version, and the `Undecided` that the fold received for it from `resolve`, whose support the touched reads carry. The list is empty for every view except a bound group's read through the live fold, so every other read is unchanged.
  - **The shortcut** comes first, per pending observe: when $V_o$ is in the history of `D` (`refVersionAtOrAbove` on `K`'s causal DAG), `o` touches nothing. Otherwise the test needs $D \cup V_o$, canonical.
  - **Slots**, once per (`K`, `D`, `V_o`, `foreignFrom`): `K`'s `resolveSchemaVersion` at (`D`, `foreignFrom`) and at ($D \cup V_o$, `foreignFrom`), the schema entries in the history of the second and not of the first, and `slotsTouchedByRule` on their migration rules. rschema exports its slot keys (`existenceSlot` and `columnSlot`, private to `src/rschema/resolve.ts` today), and a helper that lists the entries in one version's history and not in another's. The schema DAG is small, and `RSchemaImpl.resolveAt` already loads it whole.
  - **Queries.** `rowState` builds its identity filter and its delete filter from the table's incarnation at `D`, and `columnValue` its write filter from the column's. Each walk already runs at `D` with the liveness predicate, and its cover there is the left side of the comparison. The test runs the same walk at $D \cup V_o$ and compares the covers. A walk over `K` meets no undecided entry, so its cover is its whole result.
  - **Results.**
    - `rowLiveness(r)` is undecided when the existence slot, the identity query or the delete query is touched.
    - `columnValue(r, c)` is undecided when the existence slot, the column's slot or its write query is touched.
    - The support is the union of the supports of the touching observes. Otherwise each read returns its answer at `D`.
  - **`existsMatching`** checks its slots, existence and the searched columns, before anything else. That includes its early returns for an absent table or a field that is not a pub column, which hold at `D` only while those slots are untouched. It gathers its candidates at $D \cup \bigcup_{o \in U} V_o$: `candidateRowIds` and `carriers` take the version, instead of using the view's `at`. A candidate's match is today's Kleene `and` of its column reads and its liveness, each touched or not on its own (Section 2.5).
  - **Memo.** The comparisons are memoized with the computation's evaluation, keyed by `K`, the filter or slot, and the canonical (`D`, `V_o`, `foreignFrom`). The memo dies with the computation, like the evaluation's memo of verdicts, which bounds it ([`VOID_SEMANTICS.md`](../modules/rdb/VOID_SEMANTICS.md), "The per-computation memo"). It holds only comparisons, which read completed verdicts of `K`. It never holds a read's undecided answer, whose support comes from `G`'s open component. Comparisons kept across computations would be sound too (Section 7, item 5), but rdb keeps no verdict across computations today, and the comment on `freshVerdictEvaluation` (`src/rtable_group/verdict_evaluation.ts`) forbids an instance-level cache. A longer-lived cache is a change of its own ("What it costs").
- No new void reason: a collapsed observe reports `undecided-cycle`, like a collapsed row op.
- `src/rtable_group/validate_ops.ts`: `validateObserveGate` passes `at` as `opPos` and `newRefVersion` as `imported`. The deploy check evaluates `canDeploy` through `resolveSchemaForeignTableView`, so its atoms translate (Section 2.1).
- `src/rtable_group/group.ts`: `resolveAuthorKey` translates a foreign `idProvider` table through `resolveForeignTableName`.
- `src/rtable_group/group.ts`: the pin-or-inherit comment in `extractForeignDeps` says that an op reads cross-object state only through hashes that its prevs or a prior gated op pinned. An observe's gate now also reads other bound groups and `G`'s tables. The versions it reads are pinned by the observes already in the DAG, so no code changes, but the comment should say so.
- **Prerequisite:** `candidateRowIds` (`src/rtable/view.ts`) gathers the rows that match a searched value through a column default, whichever searched field drives the index (`[QRY10]`). Without that, the candidates of Section 2.5 miss such rows. It lands first, on its own.
- `src/rcatalog/validate_ops.ts`: `groupViewReason` checks gates through `G`'s bindings, table maps and schema, and checks `canDeploy` atoms and a foreign `idProvider` (`groupIdentityReason`) against the mapped table.
- `src/rdb/catalog_update.ts`: `applyCatalogPlan` observes a group's bindings in gate-graph order.

**rdb_lang**

- `src/compile/catalog.ts`: `compileCatalogGroup` resolves gates in `G`'s scope: qualified atoms through its bindings and table maps, unqualified atoms as its own tables. Its `ALLOW DEPLOY IF` scope (`deployScope`) translates qualified tables through the table maps too. Today it checks their columns against the bound group's real table, so a mapped name would be checked against the wrong table.

**Migration** (Section 2.4): the gates in `rdb/examples/editor.sql`, `rdb/examples/editor_with_attachs.sql`, `rpack/examples/editor_with_attachs.sql` and the editor pack sources under `packs/editor/work`, which is then rebuilt; and the test catalogs that write gates (`rdb/test`, `rdb_lang/test`, `rdb_tools/test/run_tests.ts`).

**rdb_runtime**

> **Decision (2026-11-04): the runtime acts on local activity only, and its response to remote voids waits.** What is implemented now:
> - **Triggers.** The check runs after local commands that write to a group: row writes, `UPDATE REF`, catalog updates. It never runs from a group's subscription after sync. A projection's ingestion keeps making its own observes of the co-projected groups its writes read (rdb_adapter), now in gate-graph order.
> - **Scope.** Each check covers every group that binds another, bound groups first, and each group's bindings in gate-graph order, so what sync brought is caught up at the next local command.
> - **The up-to-date check stays geometric**, as today. A version that only a void observe imported is not imported again automatically; an explicit `UPDATE REF` still can.
>
> The parts of the rule below that respond to observes voided by remote changes wait: the live-fold up-to-date check, the at-use guard, re-imports by any replica (which settle a collapse while its author is offline), the sync trigger, single-flight, "only what changed", and reporting a void observe in the runtime's outcome. So do their tests in Section 5. The reasons:
> - A replica should not write on its own in response to remote traffic.
> - If every replica whose author passes re-imports after a refusal or a void, each one becomes an observe per replica (Section 3.3 counts these duplicates). That needs a design of its own (Section 7, item 8).
>
> Until then, a collapse settles, and a version that only a void observe imported is imported again, only through an explicit `UPDATE REF` from the merge.

- `src/session/ref_auto_update.ts` keeps `G`'s live view of each binding at the bound group's frontier, by one rule. For each binding `b => H` of `G`: when the fold of `G`'s live observes of `H`, at `G`'s frontier, is behind `H`'s frontier, and a local author passes `b`'s gate both at admission and at use, observe `H`'s frontier.
  - **The up-to-date check reads the live fold.** Today `isAlreadyObserved` compares `H`'s frontier with `resolveRefVersion`, the geometric fold, which counts a void observe. A version that only a void observe imported is then never imported again.
  - **The at-use guard.** Before writing, the gate is evaluated at `G`'s frontier `F` as position and horizon: atoms on the gated binding at the imported version, other bindings through the live fold, and `G`'s tables at (`F`, `F`) (`evaluateObserveGate` with `fold: 'use'`, rdb above). Nothing is concurrent with an observe written at the frontier, and nothing is pending, so the answer is decided. Without the guard, an author who passes admission only, through a grant that a void observe imported, would write a new void observe on every check, for good.
  - **The guard answers before the import.** Like admission, it asks whether the author is authorized before the import, through the live fold where admission uses the geometric one. The new observe's verdict asks the same question from the observe's own horizon, which also holds the imported version (Section 2.3). Nothing else differs there, since the observe is above every entry of `F`, carries no row ops and advances one reference. But the verdicts the gate reads, of `G`'s rows and of other observes, read `H` from that horizon, so the import itself can change the answer, either way:
    - **An import that revokes its own author** passes both checks, is written, and is void with `observe-gate`. It still advances the horizon, so the revoke takes effect in `G`. That is how an honest replica delivers news that revokes its own author. A guard that predicted the verdict would withhold the import, and a replica where she is the only author who may import would never deliver it.
    - **An import that would restore its own author** fails the guard, and nothing is written. Admission does not see the import's effect either, since it reads `G`'s tables and other bindings before the import. Through those, no author imports her own re-authorization: another authorized author must import it. Through the gated binding's own atoms she can, by design, since both checks read them at the imported version.
    - **For a version already in the horizon, the guard is exact.** Once an observe of a version is in `G`'s history, live or void, the horizon holds that version, and the guard reads what a new observe of it would read. That covers a re-import after a collapse and one after a void observe of the same version.
    - **Termination.** So a replica writes at most one void observe per version of `H` that reaches it before any observe of that version is in its history. A newer version reopens the case only with new content of `H` that reaches `G`'s reads through the horizon. These observes are bounded by `H`'s activity, as the amplification of Section 3.3 is.
  - **Admission still applies.** `observe` refuses an author who fails it, as one fails when a revoke was imported only by a void observe (Section 3.3). The two folds can disagree either way, so both checks are needed.
  - **Author selection.** `resolveObserveAuthor` applies both checks to each candidate.
  - **The rule covers three cases:**
    - an observe refused because its gate read another binding at a stale version, which passes once `G` observes that binding;
    - an observe void by collapse, which a new observe from the merge settles (walkthrough 3): the new observe has the other side of the core in its history, so it is not on that cycle;
    - an observe void with `observe-gate`, written again for the same version only when the new observe would be live, since the guard is exact for a version already in the horizon.
  - **Order.** Bindings are checked in gate-graph order, so `K` is observed before an `H` whose gate reads it. After `G` observes `K`, `G`'s other bindings are checked again, since their gates may read `K`. Before 2026-11-04 a refused observe waited until the observed group changed again, and the queue after an observe moved only to the groups that observe `G`. Now every local statement checks every group that binds another, so a refused observe is tried again at the next statement.
  - **Triggers.** The check runs after a local insert, update, delete or bundle, `UPDATE REF` or `UPDATE CATALOG` (`extractRefUpdateTrigger`, called from `src/session/adapter.ts`); before 2026-11-04, only after row writes and bundles. `ALTER SCHEMA` writes only the schema, which no group reads until a catalog update deploys it, so it does not run the check. The full rule also runs it:
    - after a sync batch. rdb_runtime subscribes only to new roots today (`src/workspace/workspace.ts`). The hook is the subscription each group offers (`RTableGroupImpl.subscribe`), which fires at least once per growth of its DAG, so the check coalesces the events of a batch, or runs when the synchronizer is idle. A batch in `K` can void an observe of `H` (Section 3.2, case 2), and a batch in a group below `K` can change a cap's verdict in `K`. So after any batch, every group in the workspace that binds another is checked, in gate-graph order, and the up-to-date check decides.
  - **One check at a time.** A local commit fires the group's subscription too, possibly twice (the listener contract in `dag/src/store/dag_store.ts`). So an observe the check writes would start the check again inside itself. The check is single-flight: events that arrive while it runs are coalesced, and it runs once more afterwards if any arrived.
  - **Only what changed.** Each check of a binding reads the live fold, a verdict solve, and completed verdicts are not kept across computations (Section 7, item 4). The live fold at `G`'s frontier depends only on that frontier, since every version it reads is pinned by hash. So a binding needs checking again only when `G`'s frontier or `H`'s has moved since its last check, and the check records both per binding. When no local author passed, it also records the local identities, since a new one may pass. A check that throws, as one does when a bound group it reads is not present locally, is not recorded, since the same frontiers answer differently once the group arrives.
  - **Any replica** whose author passes may write the observe, not only the one whose observe went void, so a core settles while its author is offline. Observes of one version from several replicas are harmless duplicates.
- `src/session/authz_suggest.ts`: `evaluateObserveGateKey`, `resolveObserveAuthor` and the `update-ref` branches pass the observe's position (`bound.at`) and the imported version (`bound.version`). Today both `update-ref` branches pass `bound.at` as `refAt` and `refFrom`, which hands the observer's hashes to the observed group's DAG, so the author suggested for a refused `UPDATE REF` is wrong or missing. This fixes that. `ref_auto_update` already passes the target version, and adds `G`'s frontier as the position.

  `rdb_tools` re-exports them.

**Reporting a void observe**

`authz_suggest` cannot report a contested observe: it runs on admission failures (`VALIDATION_REJECTED`), and a collapse happens at use. Void reasons are printed through `formatOpVoidDetail` (`rdb/src/rtable_group/op_void.ts`) by `LOG` (`rdb_lang/src/exec/history.ts`), the REPL's delta output (`rdb_repl/src/format/delta.ts`) and rpack's staging (`rdb_tools/src/rpack/stage.ts`). They print `undecided-cycle` as "undecided dependency cycle (denied)". For an observe, they print it as contested, with the advice to observe again from the merge. For an observe void for either reason, they also say that its version advanced `G`'s horizon of the binding, so what it brought can still change `G`'s reads (Section 2.3). The runtime reads the verdict of each observe it writes, at the new frontier, and reports a void one the same way in its outcome (`RefUpdateOutcome` in `src/session/ref_auto_update.ts`), as happens when an import revokes its own author.

**mvt**

- `src/refs.ts`: three-valued variants of `resolveRefVersionAtPosition` and `widenRefAtUpward`. The boolean variants stay for RSet.
  - Liveness is a `Truth`. Each walk is a `seeThroughCover`: a void observe is seen through, an undecided one is seen through and kept, and a live one contributes its published version.
  - The result is `{ live, pending }`. `live` is today's result over the live observes, which is the referenced object's root when there are none. `pending` lists each kept observe with its entry, its published version and the undecided verdict read for it.
  - The causal walk and the concurrent walk each drop a pending observe that a live member of the same walk dominates. Nothing more is needed: an observe whose version the live version already holds touches nothing (the shortcut of Section 2.5), so a dominance across the two walks that goes unnoticed costs nothing.
  - In `widenRefAtUpward` the base stays geometric, as today: the observe's own version, which holds that of its past. Only the candidates strictly above the base are asked their liveness, and an undecided one is pending.
- `src/verdict_evaluation.ts`: no change.
- `walkRevisionBound`: no change (Section 3.4).

**dag**

- No new query. The touch test uses `findCoverWithFilter`, `findConcurrentCoverWithFilter` and, through `refVersionAtOrAbove`, `findForkPosition`.
- `src/dag.ts` and `src/idx/dag_idx.ts` state the contract of Section 2.5, step 2, on the two cover queries: with or without a predicate, a cover holds every topmost entry of its filtered set and nothing else, and it sees through the matches the predicate rejects. `[TOUCH_03]` is its check, and a new index must pass it.

---

## 5. Tests

- `[OBSGATE]`, cross-binding:
  - a revoke in `K` imported by a concurrent observe of `K` voids a back-dated observe of `H`;
  - the same import after the observe of `H` leaves it live;
  - a grant imported only by a voided observe of `K` does not authorize;
  - benign concurrent observes of `H` stay live;
  - two aliases of one group with different table maps.
- `[OBSGATE]`, gates over `G`'s tables (the walkthrough of Section 2.5):
  - with local rules (1), and with rules through an ungated binding (2), nothing is pending, and the gate reads what ordinary views read;
  - the loop (3): the observe and the delete are both void (`undecided-cycle`), in every arrival order and at every horizon;
  - after the loop: Alice's observe from the merge is live, and `G` then reads Bob's cap as revoked; a delete by Bob from the merge is refused at admission, and a back-dated one contests Alice's imports again;
  - Carol's import (4): the delete is void and the observe live, and `target`'s gate is unaffected;
  - an import that misses the read (5): the delete is live and the observe void with `observe-gate`;
  - an import that writes a column of Bob's cap row that the read does not search leaves the delete decided;
  - a second cap for Bob, imported by the observe, does not contest a delete that his first cap decides;
  - a pending deploy that writes `caps`' existence slot, or the slot of a column a read searches, makes that read undecided, including when `caps` is absent at `D`; one that changes only `caps`' rules leaves it decided;
  - a pending deploy that re-adds a searched column with a new default makes an `EXISTS` undecided when no candidate exists at `D`. With rules that read `EXISTS auth.allowed WHERE user = $author AND active = true`, the even loop of Section 2.5 voids all three entries (`undecided-cycle`) instead of settling on one side, and the odd loop voids both instead of throwing;
  - blocking an import (Section 3.3): a delete of the importer's row by a principal revoked in `K`, concurrent with the import, voids both, and `G` reads `K` without the revoke; the importer's next observe from the merge is live, and a new delete from the merge is refused at admission;
  - an importer whose row that principal cannot delete gets the import through, and so does a gate on `auth` that reads `auth.caps`;
  - a delete in `G` concurrent with `y` voids it, as it would a row op's guard;
  - admission reads `G`'s tables at (`at`, `at`).
- The touch rule:
  - It is checked against the completions oracle of Section 2.6, item 5, which stays in the suite. On an even loop nothing in the engine catches a read decided too early ("Nothing decided is retracted"), so the oracle is the only check there. Its configurations include the even and odd loops of that paragraph, and it checks every read their solves make. It runs over every state of a small DAG with two pending observes, in the style of `[VREAD06]`:
    - a decided read holds in every completion;
    - with nothing pending, it is today's read;
    - deciding an observe never flips a decided read or makes it undecided.
  - Include two observes that change a read only together, through deploys, each of which must touch it on its own; and a deploy that changes a searched column's default when no candidate exists at `D`.
  - Each comparison against the sets it stands for, over small DAGs: equal history covers at a version and at one whose history contains it exactly when the filtered histories are equal, and, with those equal, equal concurrent covers exactly when the filtered concurrent sets are.
  - The index contract of step 2: each cover is exactly the topmost entries of its filtered set, with and without a predicate, for every index.
  - Two prototypes checked the rule before the fold existed:
    - `[TOUCH]` (`dag/test/touch_cover_tests.ts`), which stays in the suite, checks steps 2 and 3 of "Why the rule is sound" by brute force on random DAGs, with and without a liveness predicate, for all three indexes. It also shows the case the history comparison exists for: a concurrent match that enters the history below one that does not leaves the concurrent cover unchanged. `[TOUCH_03]` checks the index contract directly, against the topmost entries of each brute-force set, from the frontier and from other horizons. Its cases include covers that must leave out matches below the topmost ones, and covers that must see through matches the predicate rejects.
    - `[TOUCHP]`, which the oracle below replaced, checked the rule's decided reads against the exact reads at every completion. Its bound group had concurrent row ops and two concurrent deploys, one that resets a searched column and one that changes only rules, and it injected verdicts as `[VREAD06]` does. It checked monotonicity and the shortcut, and that comparing every entry is sound but decides less. It also ran broken variants of the rule (no slot check, no concurrent comparison, candidates gathered at `D` only) and caught each.
  - The permanent oracle (`rdb/test/touch_rule_tests.ts`, `[TOUCHR01]` to `[TOUCHR06]`) improves on `[TOUCHP]` in three ways:
    - **It enumerates.** `[TOUCHP]` sampled its versions, and in 300 trials it caught the slot and concurrent variants only six and three times, so a change that made those cases rarer could still pass. The oracle enumerates the versions of small fixtures instead.
    - **It pins a configuration for each broken variant**, one that the variant gets wrong, so that removing a case from the fixture fails a test.
    - **It states the monotonicity condition.** A touch against $D \cup V_{o_1}$ must be a touch against `D` only when `o1` touches none of the read's slots: a touched slot makes the read undecided before any query runs. `[TOUCHP]` encoded this as equal incarnations at the two versions.
  - Cases:
    - a concurrent delete that enters the history below one that does not, which only the history comparison sees;
    - a delete that goes from after `D` to concurrent;
    - a column write that only becomes concurrent, which touches nothing;
    - a write that is void in `K`, which touches nothing, and the same write live, which touches;
    - $V_o$ in the history of `D`, which touches nothing.
- mvt: the three-valued fold drops a pending observe below a live one, walks past void and pending ones and keeps the pending ones, and widens `refAt` with pending observes strictly above it.
- Creation: a gate-graph cycle, an unbound alias and a local table missing from the pinned schema are rejected.
- `[DBOUND]` and `test/delta_parity`:
  - a side-branch observe of `K` that flips an observe of `H`;
  - a core of an observe and a delete at one horizon, which a later op settles at the other;
  - a variant of the gated-observer generator whose gate reads a second bound group;
  - a variant whose gate reads `G`'s tables, with cores of observes and row ops;
  - a side-branch observe of `K` that stays pending for an entry of `G` below the meet, so the entry's touch test reads `K` beyond `D` (Section 3.4).
- rdb_runtime, for what landed on 2026-11-04 (Section 4):
  - ordering in `ref_auto_update` and `applyCatalogPlan`, and a binding refused for a stale view of another binding, observed once `G` observes that binding (`[RDB_RT17]`, `[RDB_RT21]`, and `[RDBING05]` in rdb_projection for ingestion);
  - `UPDATE REF` runs the check (`[RDB_RT18]`);
  - a statement catches up every stale binding in the workspace, and reports a refusal, or offers to unlock a key, only for a binding whose bound group it wrote (`[RDB_RT16]`, `[RDB_RT19]`, `[RDB_RT20]`);
  - an author suggested for a refused `UPDATE REF` (`[RDB_TOOLS68]` in rdb_tools).
- rdb_runtime, waiting with the rest of the rule:
  - an author who passes admission only through a grant that a void observe imported writes no observe, however many checks run;
  - an author refused at admission because a void observe imported a revoke writes no observe, although the gate at use would pass;
  - in the configuration of walkthrough 3, an import that holds a delete of its author's cap, concurrent with the version her `admins` row's insert read: with her the only local author, the check writes it once, void with `observe-gate` and reported as having advanced the horizon; her row is dead in `G` afterwards, and later checks write nothing; once a second author whose authority the delete leaves alone is local, the check writes a live re-import;
  - the reverse: her row is dead because such a delete is honored, and the import holds a deploy, concurrent with the delete, that unhonors it. The check writes nothing, her own observe of that version is refused at admission, and once another authorized author imports it, that observe is live and her row is live again in `G`;
  - the observe a check writes does not start a second check while the first runs, and growth during a check runs it once more afterwards;
  - a binding is not checked again while neither `G`'s frontier nor `H`'s moves; after a check that no local author passed, it is checked again when a new local identity appears; a check that throws because a bound group is missing locally runs again once the group arrives;
  - a new observe after one goes void at the frontier, including when the delete that collapses it arrives later by sync, and from a replica that did not write it. rdb_runtime's tests never sync two replicas; rdb's `test/rdb_sync_tests.ts` and `test/rdb_full_sync_harness.ts` have the harness.
- rdb_lang: compile, static check and C-SQL round trip of gates in `G`'s scope. The tests that pin the observed frame, `[CLANG17]` and `[CAT13]`, are rewritten to test the observer frame.
- Translation: a `canDeploy`, a foreign `idProvider` and a gate atom each read through a table map.

---

## 6. Docs to update when it lands

Done on 2026-11-04. The `rpack/IMPLEMENTATION.md` section was deleted on 2026-11-05 by [HIP-1](0001-hips.md). These changed too: the mvt README (the three-valued helpers), `mvt/IDEAL_CONCURRENCY.md` §4.4, [`OBSERVED_CUT_REVOCATION.md`](0006-observed-cut-revocation.md) §4.4, and the rdb_tools and rdb_adapter READMEs (when the runtime checks bindings, and the order of ingestion's observes).

- [`VOID_SEMANTICS.md`](../modules/rdb/VOID_SEMANTICS.md):
  - §2: a gated observe "never joins a component" and is "the one place the engine treats an observe edge as negative". Both statements go.
  - §4, the rules: the fold of observes as a read rule, with the touch rule.
  - §5.5: the two layers for a gate that reads a second group, gates over `G`'s tables, and observes on cycles (Section 3.1). "Observes are never on a cycle" and its reason go, and "Where it lives" stops describing `evaluateObserveGateIn` as frame rebasing.
- [`INCREMENTAL_DELTA_BOUND.md`](../modules/rdb/INCREMENTAL_DELTA_BOUND.md), and guarantee 6 in `mvt/src/revision_bound.ts` (Section 3.4):
  - A3, the foreign views, for every entry and not only observes: besides `foreignAt`, the entry may read the bound group, and its schema, at `foreignAt` plus the versions of observes its fold sees through, one at a time for the touch test and several together for the candidates of `existsMatching`. Which observes those are depends on which verdicts are undecided, which is solve state and not an input, so A3 does not name them.
  - A3, the gate: a gate reads its own binding at (`refAt`, `refFrom`), and at `refAt` plus the versions of observes that can widen it, in the same way. It reads other bindings through the full live fold, and `G`'s tables at (`{y}`, `from`), as a guard does.
  - The proof of Theorem 1: step 1 of Section 3.4, that every observe in the fold of an entry inside the bound is stable, so every version the touch test reads lies inside the bound group's bound; and that the reads A3 adds are made only while those observes are undecided, which A2 covers.
  - Guarantee 6: a verdict may also read a referenced object at a version that adds, to the fold of the live ref-advances, the versions of ref-advances the fold sees through, one at a time or several together. Those versions lie inside the referenced object's bound when the ref-advances are stable.
- [`rpack/README.md`](../modules/rpack/README.md): how gates are read, replacing the paragraphs that describe this as a follow-up; and the table-map paragraph, which says that clauses written in the catalog use real table names.
- `rpack/IMPLEMENTATION.md`, "Advancing an observed version": it describes the design this note replaces, with an unqualified `t` read as `<binding>.t`, `G`'s tables kept out of gates, and a frame recorded in the create payload.
- [`rdb_lang/README.md`](../modules/rdb_lang/README.md): the column-scope rules for gates, and the gate examples.
- `mvt/MVT_DEFINITION.md` §8: the mixed admission of Section 2.3 (item 2), and the reason for item 1 (Section 3.1). The std_types README's "Admission and use": the mixed admission.
  - A new item in §8: admission reads references through the geometric fold, counting every ref-advance in the op's past, where §4.1 implies the live fold. §8 calls each departure either a clause the definition should state or a change the code should make. This one is a clause, with the reason given in Section 2.3. Row-op admission already departs this way today, so the item does not have to wait for this design.
- `mvt/WELL_FOUNDED_VERDICTS.md`:
  - §5.2: the race of an observe and a delete;
  - §5.3: an import as a counter-revoke, and a delete that holds up the import of a revoke (Section 3.3);
  - §6.5, the preference order: cores that hold an observe need an order across a binding (Section 7, item 3).
  - §5.5, item 8: observes can be on cycles, always through `G`'s rows;
  - §6.5: close "Observes that read each other".
- Comments that describe the observed frame or the acyclicity invariant: the `canObserve` comment in `src/rtable_group/payload.ts`, the `evaluateObserveGate` declaration and the observe-gate admission comment in `src/rtable_group/validate_ops.ts`, the comments on `observeVerdictLive` and `evaluateObserveGate` in `src/rtable_group/group.ts`, and the `ALLOW UPDATE REF` checks in `test/catalog_tests.ts`.
- `PHASE_5_PLAN.md` and `DESIGN_AND_PLAN_FOR_ANY.md` record that the invariant and the throw stay. They are decision records and keep their text; a dated line in each can point here.

---

## 7. Open questions

1. **Value-aware touches.** The touch test asks whether the read's queries match different entries, not whether its answer changes. Comparing the answer at `D` with the answer at $D \cup V_o$, one observe at a time, would decide more. But answers do not add up over observes the way matched entries do. Take a slot whose value comes from a write `w0`. One import brings `w1`, concurrent with `w0`, with a new value, which loses the tie-break to `w0`. Another brings `w2`, after `w0`, with `w0`'s value. Each import alone leaves the value as it was. Together, `w0` is no longer a maximum, and `w1` can win the tie-break against `w2`. So two imports that each leave the answer alone can change it together. Deciding per observe would then be unsound, and deciding over sets of observes is the enumeration of Section 2.6, item 5. A finer test needs an argument of its own.
2. **Explaining a contested observe.** A collapsed observe reports `undecided-cycle`. Naming the delete it raced needs the core in the reason, the refinement proposed in [`INCREMENTAL_DELTA_BOUND.md`](../modules/rdb/INCREMENTAL_DELTA_BOUND.md) §5.7. Until then, `authz_suggest` can only say that the observe was contested.
3. **A preference order for cores.** The fold voids both an observe and a delete that contest each other, as the engine voids both sides of a revoke race. The stalemates of Section 3.3 are those of `mvt/WELL_FOUNDED_VERDICTS.md` §5.3, so the fix planned there should carry over.
   - **The plan.** A preference order tries the most senior member of a core live, propagates, and repeats (§3.5 there; [`VOID_SEMANTICS.md`](../modules/rdb/VOID_SEMANTICS.md) §6).
   - **What it needs here.** It ranks members by the insert position of the cap that authorizes them. In these cores the two caps are in different objects: the importer's `admins` row is in `G`, and the deleter's cap is in `K`. VOID_SEMANTICS §6 already flags the need for an order across DAGs.
   - **A ranking that can be gamed.** Ranking `K`'s authority by the position in `G` where it was first imported fails: an author can back-date an import of their own grant.
   - **A rule fixed at creation cannot be gamed: authority `G` holds itself outranks authority it reads from a bound group.** An entry then cannot use its view of a binding to block an update of that binding. In the loop configuration the observe is the locally authorized member, so the import wins. That settles both cases of Section 3.3 the way `K` would:
     - walkthrough 3: Alice's observe is live and Bob's delete void;
     - blocking an import: Carol's observe is live and Alice's delete void.
   - **It also removes cores made by touches that change nothing** (Section 2.5, "Precision"). Trying a member live and checking the result evaluates `K` at the actual version, not through touches. Suppose Alice's import touches Bob's cap row without changing it:
     - trying her observe live leaves Bob's delete live, so her gate fails;
     - the order then tries Bob's delete, which is consistent: the delete is live and the observe void.

   Whether a member tried live must satisfy its own rule, and what to do when no member does, are questions for the preference order in general. Odd cycles raise them too.
4. **Costs outside this note.** Gates over `G`'s tables add reads to an observe's verdict, so these costs, which exist today, grow:
   - Admission creates an evaluation for each table view (`getView` in `src/rtable/rtable.ts`), so the atoms of one rule share no memo.
   - Completed verdicts are not kept across computations.
   - Every DAG index answers a concurrent query by walking the whole history below `from` (`dag/src/idx/concurrent_cover.ts`).
5. **Re-check before implementing: the live comparison.** On 2026-11-03 the comparisons of Section 2.5 were changed to count only entries live in `K`, instead of every entry. Comparing every entry is also sound and reads no verdicts, but then a write that is void in `K` touches, and a principal whose authority in `K` was revoked can contest a delete in `G` with a back-dated write (Section 3.3). Before implementation, re-check:
   - that every verdict of `K` the comparisons read is complete before `G`'s read returns, including those of entries that the read at `D` never meets;
   - step 1 of "Why the rule is sound" with `K`'s verdicts all decided, and steps 2 and 3 with the filter "matches `F` and is live";
   - that the memo would stay sound if it outlived a computation, since `K`'s verdicts at `foreignFrom` are fixed, although it dies with the computation (Section 4);
   - the cost of the extra verdicts.

   A review on 2026-11-03 went through each point and found that it holds. The extra verdicts cost at most one live fold per pending observe ("What it costs").
6. **One trigger.** Settled for now on 2026-11-04: the check runs inside local statements only, and the statement's result reports the observes it wrote (`item.refUpdates` in `rdb_runtime/src/session/adapter.ts`; Section 4). If the sync trigger lands (item 8), the question returns: a local commit fires the group's subscription too, so the subscription could be the only trigger. Run from the subscription alone, the check would run after the statement returns, and its observes would appear in no result unless the session collects them.
7. **Ranking authors.** When several local authors pass both checks, `resolveObserveAuthor` could prefer one whose observe will be live. That saves the void observe an author would write first when the import revokes her own authority. It needs the gate evaluated from the new observe's own horizon: a fresh evaluation in which `G`'s horizon of `H` holds the imported version wherever it is computed (`foreignFrom` and `refFrom` in `src/rtable_group/group.ts`). Nothing depends on it.
8. **Responding to remote voids.** Deferred on 2026-11-04 (Section 4, rdb_runtime). When sync voids an observe, collapses one, or leaves `G`'s live view behind, should the runtime re-import on its own, and from which replicas? The rule of Section 4 answers "any replica whose author passes, after any batch", which makes every refusal or void an observe per replica, as many times as an attacker answers (Section 3.3). Options to weigh: only the replica whose observe went void re-imports; re-imports wait for a local command (today's behavior); or a replica re-imports only when no live observe of the same version has arrived after a delay. Item 7 depends on this.

---

## 8. Since the first version

- **Ideal concurrency replaced `ALL`** ([`DESIGN_AND_PLAN_FOR_ANY.md`](0007-ideal-concurrency.md)). The §5.5 argument is now exact for the binding being gated, and the cross-binding case reduces to guard reads (Section 3.2). The head-advancing widening and cut intervals of [`OBSERVED_CUT_REVOCATION.md`](0006-observed-cut-revocation.md) are not needed.
- **Several aliases can name one group** (`aliasesForId`, `observeGateFor`). Each stored atom carries its alias (Section 2.1).
- **References written in the catalog translate through table maps:** gates, `canDeploy` and `idProvider` (Section 2.1).
- **One frame.** Earlier versions kept the observed frame for existing groups, behind a payload marker. The project is unreleased, so the observer frame replaces it, and existing gates are rewritten (Section 2.4).
- **Gates may read `G`'s own tables** (Section 2.5). The first version banned them. The version of 2026-10-30 read them through gate verdicts that treated local-gated bindings as unknown. On 2026-11-02 that was replaced by a three-valued fold, which lets the engine solve loops through observes as it solves loops among rows (Section 2.6, items 4 to 6). An unqualified name now means a table of `G`, so the shorthand for `b.t` is gone.
- **The bounded delta walks with `walkRevisionBound`.** Its argument covers this design, with text changes (Section 3.4).
- **A barrier void by collapse counts as changed in the bounded delta** ([`INCREMENTAL_DELTA_BOUND.md`](../modules/rdb/INCREMENTAL_DELTA_BOUND.md) §5.7). This landed on its own, and covers collapsed observes with no further change.
- **`widenRefAtUpward` moved to mvt** and is shared with RSet's gated ref-advances. This design adds a three-valued variant beside it.
- **Added:** the retry in `ref_auto_update`, the `authz_suggest` callers, the honest-loss note, and the races between imports and removals (Section 3.3). The touch test is now specified as a comparison of the read's own queries at `D` and $D \cup V_o$, over the entries live in `K`, with its proof (Section 2.5, "Computing a touch"). The proof names the contract it needs from every DAG index (step 2), the bounded delta's argument covers the touch test's reads (Section 3.4), the touch memo dies with the computation, and the check after sync is single-flight and skips bindings whose frontiers did not move (Section 4).
- **The runtime's guard answers before the import** (2026-11-04). It was described as giving the new observe's verdict. But it reads from the frontier's horizon, which lacks the imported version, and the verdicts it reads depend on the horizon, so the import itself can change the answer, in either direction. The guard now asks whether the author is authorized before the import, as admission does. An import that revokes its own author is written once, void, and still delivers the revoke (Section 4). The horizon and the ways it reaches a read are stated once (Section 2.3), a void observe is reported as having advanced it, and "Why the collapse does not fail open" promises only what the live fold gives (Section 2.5). Section 2.3 now gives the reason admission reads other bindings through the geometric fold: a revoke the author has seen binds her, whatever the verdict of the import that brought it.
- **The runtime acts on local activity only** (2026-11-04). The check runs after local commands, over every group that binds another, in gate-graph order, with today's geometric up-to-date check. Projection ingestion keeps its own observes, in gate-graph order. The response to observes voided by remote changes (the sync trigger, the live-fold check, the guard, re-imports by any replica) waits, for fear of an observe per replica on every refusal (Section 4, rdb_runtime; Section 7, item 8).

---

## Implementation guardrail

If an unforeseen problem, ambiguity or architectural conflict comes up during implementation, stop and report it: the issue, the affected code paths and the options. Do not make significant design or protocol decisions unadvised.

## History

This HIP was `modules/rdb/plans/REFERENCE_FRAME.md`, moved here by [HIP-1](0001-hips.md) on 2026-11-04. Text above that names a file by its old name refers to it as it was. The files it was built from can be read at commit `f45f1e3` (`git show f45f1e3:<path>`).

- `modules/rdb/plans/REFERENCE_FRAME_CRITIQUE.md` and `REFERENCE_FRAME_CRITIQUE_RESPONSE.md`: the first review, before implementation. The design held; the restriction grammar has no negation, `authz_suggest` passed the wrong position, §4 missed the compiler's deploy scope, §6 missed two docs. The response accepted all of it and showed that a too-early decision does not always fail loudly.
- `REFERENCE_FRAME_CRITIQUE_2.md` and its response: the retry in the runtime could loop forever, and the triggers did not exist as described. Both fixed in §4.
- `REFERENCE_FRAME_CRITIQUE_3.md` and its response: the four-step proof, the live comparison and the per-component match hold; the index contract was made explicit (`[TOUCH_03]`), and the touch memo now dies with the computation.
- `REFERENCE_FRAME_CRITIQUE_4.md` and its response: the at-use guard does not give the observe's verdict. The review recommended an exact guard; the response argued for the guard-before-import rule, which §8 records.
- `modules/rpack/IMPLEMENTATION.md`, "Advancing an observed version": an earlier sketch of this design, with an unqualified `t` read as `<binding>.t`, the observer's own tables kept out of gates, and a frame recorded in the create payload. [HIP-1](0001-hips.md) deletes that section rather than moving it here: this plan supersedes it on every point.

Its status line read:

> **Status: implemented on 2026-11-04, except the runtime's response to
> observes voided by remote changes, which waits (Section 4, rdb_runtime;
> Section 7, item 8).** Before it, a binding's `ALLOW UPDATE REF` gate was
> evaluated in the observed group's frame; [`VOID_SEMANTICS.md`](../modules/rdb/VOID_SEMANTICS.md)
> §5.5 now describes the observer frame. This note was first
> written in the rpack module on 2026-10-21, revised on 2026-10-29 after ideal
> concurrency, the bounded delta and multi-alias bindings landed, and extended
> on 2026-10-30 with gates that read the observer's own tables (Section 2.5).
> On 2026-11-01 the observer frame became the only frame (Section 2.4), and
> references written in the catalog started translating through table maps
> (Section 2.1). On 2026-11-02 the gate view of Section 2.5 was replaced by a
> three-valued fold of observes. Section 8 lists what changed.
