# MVT definition

This note gives an operational definition of monotone view types, in the style of Shapiro, Preguiça, Baquero and Zawirski's definition of op-based CRDTs: what a replica holds, what a read returns, what an op means, and which properties follow. It is a draft for discussion. The [README](./README.md#monotone-view-types) keeps the informal definition, and nothing here changes behavior.

The definition has two layers. The first is the mechanism every MVT has: barrier ops, and views read at a version `at` from a later version `from`. A read sees the barriers concurrent with `at`, replayed on top of it. The second layer says what an op means once concurrency is known: an op is judged at its own position, against the barriers concurrent with it. **Voiding**, the rule RCap, permissioned RSet and rdb use, is one way to do that. Other techniques could plug in at the same point.

Section 1 defines the terms. Section 2 states the definition. Sections 3 and 4 annotate the two layers. Section 5 lists the properties that follow and which layer each needs. Section 6 reads the definition against CALM, and Section 7 against other formalisms. Section 8 lists where the implementation departs from it, and Section 9 collects open questions.

---

## 1. Terms

**Entries and versions.** As in [IDEAL_CONCURRENCY.md](./IDEAL_CONCURRENCY.md) §1, an object's history is a DAG of entries. $x \prec y$ when $x$ is a proper ancestor of $y$. $H(x)$ is $x$ with its ancestors, and $\mathrm{past}(x) = H(x) \setminus \{x\}$. A **version** is a downward-closed set of entries; a position $P$ stands for the version $H(P)$. Versions here are sets, so $A \subseteq A'$ means that $A'$ holds everything $A$ holds.

**Replicas.** A replica holds a version: the entries it has received, closed under predecessors, which the hash links guarantee. Receiving an entry adds it to the set. An entry is received after its predecessors, so it arrives as a maximal element.

**Barriers.** $B$ is the set of entries tagged `barrier`. The tag says an entry may act on versions that did not see it. It says nothing about polarity: barrier deletes and revokes remove, while barrier adds, deploys and ref-advances add.

**Views.** `getView(at, from)` is written $V_F(A)$, with $A = H(\text{at})$, $F = H(\text{from})$ and $A \subseteq F$. $F$ is the **horizon**: what the reader knows.

**Plain semantics.** Each type supplies $\mathcal{S}$, a deterministic function from a finite partially ordered set of entries, with their payloads, to the value a view reports. It is the type's answer for a history with no revision: RSet's membership rule, RCap's slot rule. Burckhardt et al. specify replicated data types the same way, by a function of the graph of operations instead of a state.

---

## 2. The definition at a glance

```text
Layer 1: barriers and scoped views (every MVT)
  (1)  x ∥ A    ⇔  x ∉ A  ∧  A ⊄ past(x)
  (2)  K_F(A)   =  { b ∈ B ∩ F | b ∥ A }             A ⊕ K: A, then K
  (3)  V_F(A)   =  S( μ_F( A ⊕ K_F(A) ) )

Layer 2: what an op means
  (4)  U_F(x)   =  S( μ_F( past(x) ⊕ K_F(H(x)) ) )
       μ_F(x)   =  ι_x( U_F(x) )                      no technique: μ_F(x) = x

Voiding, the technique RCap, permissioned RSet and rdb use
  (5)  μ_F(x)   =  x if g_x( U_F(x) ), void otherwise
       solved to the well-founded model, undecided cores void
```

$\mu_F$ applied to a set of entries replaces each entry with its meaning and keeps the order. With Layer 1 alone, $\mu_F$ is the identity, and (3) reads $V_F(A) = \mathcal{S}(A \oplus K_F(A))$.

The layers differ in what concurrency is measured against. Layer 1 measures it against the version read: which barriers did the reader of $A$ not know? Layer 2 measures it against each op: which barriers did the author of $x$ not know, and which did not know $x$?

---

## 3. Layer 1: barriers and scoped views

### 3.1 Equation (1): concurrent with a version

$$x \parallel A \iff x \notin A \ \land\ A \not\subseteq \mathrm{past}(x)$$

$x$ and $A$ are concurrent when neither had seen the other. $x \notin A$ says the reader of $A$ does not know $x$. $A \not\subseteq \mathrm{past}(x)$ says $x$'s author did not know all of $A$. Every entry stands in exactly one of three relations to a version:

| Relation | Condition | Meaning |
| :--- | :--- | :--- |
| in the past | $x \in A$ | the reader knows it |
| after | $A \subseteq \mathrm{past}(x)$ | its author knew everything the reader knows |
| concurrent | otherwise | neither knew the other |

For a single head this is ordinary concurrency. At an entry's own history it is concurrency with that entry: $y \parallel H(x)$ exactly when $y \notin H(x)$ and $x \not\prec y$. For a position with several heads it is the `ANY` rule, which IDEAL_CONCURRENCY.md argues for. Because (1) is stated on the set $A$, it does not depend on how a position is encoded, and nothing is concurrent with the empty version.

### 3.2 Equation (2): the barriers that reach a read

$$K_F(A) = \{\, b \in B \cap F \mid b \parallel A \,\}$$

These are the entries the horizon knows, the reader of $A$ does not, and that may act on $A$. Three kinds of entry in $F \setminus A$ are left out, each for a reason:

- **Plain entries.** They commute with the entries of $A$. A version shows them once it contains them. This is the CRDT part of an MVT.
- **Entries after $A$.** Their authors had seen all of $A$. They are $A$'s future, not a revision of it.
- **The history of a barrier.** A barrier is carried alone. Its branch is not merged into the read.

$A \oplus K$ orders $A \cup K$ by $\prec$ inside $A$, by $\prec$ inside $K$, and with every entry of $A$ before every entry of $K$. That is a partial order, because no entry of $K$ is below an entry of $A$: $A$ is downward closed and $K$ lies outside it. It extends $\prec$ on $A \cup K$.

$A \oplus K$ replays the concurrent barriers on top of the version read. A barrier wins over what it had not seen, in the sense that $\mathcal{S}$ sees it as later. In `[SET02d]`, $x$ is added at $g$, two branches $a$ and $b$ follow, and a barrier delete $d$ of $x$ is appended after $a$. Read at $\{a, b\}$ from $\{d, b\}$: $d$ has not seen $b$, so it is concurrent with the read, it is replayed after both heads, and $x$ is gone.

### 3.3 Equation (3): the view

$$V_F(A) = \mathcal{S}\big(\mu_F(A \oplus K_F(A))\big)$$

The view of $A$ from $F$ is the state at $A$, revised by what $F$ knows that $A$ did not and that applies to it. At $A = F$ nothing is outside the read, $K$ is empty, and the view is the plain state of $F$.

The README describes the view as "the version that contains the union of all the operations in the set `at` and the barrier operations in `concurrent(at, from)`". (3) settles the two things that sentence leaves open. $A \cup K$ is not downward closed, so it is not a version. And what matters is the order between $A$ and $K$.

$\mathcal{S}$ sees one partially ordered set and does not know which entries came from $K$. The two DAG queries of IDEAL_CONCURRENCY.md §2.1 compute it. Every entry of $K$ is above every entry of $A$, so the latest matching entries of $A \oplus K$ are those of $K$ when $K$ has a match, and those of $A$ otherwise. The concurrent cover is the first case, and the cover at `at` is the second.

Every barrier in the repo acts through (3):

| Barrier | Effect through (3) | Pinned by |
| :--- | :--- | :--- |
| RSet barrier delete | the element is gone | `[SET02d]`, `[SET02e]` |
| RSet barrier add | the element joins | `[SET02c]` |
| RCap revoke, `delete-cap` | the slot is cleared, the capability absent | `[CAP29]`, `[CAP37]` |
| rdb deploy, schema parameter | the schema is revised | `[DEPLOY20]` |
| ref-advance, observe | the referenced version is the join of the latest live ones | `resolveRefVersionAtPosition` |

### 3.4 One example, two consequences

```text
g ── p1 ── b1 ── c         g        adds x
 \                         b1, b2   barrier deletes of x
  p2 ── b2 ── a            c, a     plain re-adds of x
```

Each branch deletes $x$ with a barrier and adds it back. RSet with `supportBarrierDelete` gives:

| Read | $x$ | Why |
| :--- | :--- | :--- |
| at $\{c\}$ from $\{c\}$ | present | the re-add follows the delete |
| at $\{c\}$ from $\{c, a\}$ | absent | $b_2$ is concurrent with $\{c\}$ and replayed after it; $a$ is plain and is not |
| at $\{a\}$ from $\{c, a\}$ | absent | the same, with $b_1$ |
| at $\{c, a\}$ from $\{c, a\}$ | present | both re-adds are in the version, each after its own delete |

No test pins this yet. It was checked against the implementation when this note was written.

**Barrier-wins is relative to a version.** No single order of all entries puts every barrier after the entries concurrent with it. Here it would need $a$ before $b_1$, $b_1 \prec c$, $c$ before $b_2$ and $b_2 \prec a$, which is a cycle. For any one version, $A \oplus K$ is acyclic. This is why a view takes two versions: barrier-wins is decided against the version read, not once for the whole history.

**A branch seen from the merge is not the merge.** From $\{c, a\}$, each branch shows the other branch's barrier but not its re-add, so each shows $x$ absent while the merge shows it present. Views are not monotone in $A$. Section 9 asks whether this is the intended reading.

---

## 4. Layer 2: what an op means

### 4.1 Equation (4): the use view

$$U_F(x) = \mathcal{S}\big(\mu_F(\mathrm{past}(x) \oplus K_F(H(x)))\big), \qquad \mu_F(x) = \iota_x\big(U_F(x)\big)$$

$U_F(x)$ is the state $x$'s author read, revised by the barriers that $x$ had not seen and that had not seen $x$. It is the view at $x$'s own position with $x$ removed, since an op is never its own witness or barrier ([WELL_FOUNDED_VERDICTS.md](./WELL_FOUNDED_VERDICTS.md) §5.5). This is where an op's semantics becomes a function of its prior state and of the ops concurrent with it.

$\iota_x$ is the technique: what $x$ amounts to, given its use view. With no technique, $\mu_F(x) = x$ and (4) is vacuous.

Three consequences:

- **Admission is a meaning at the op's own horizon.** At $F = H(x)$ nothing is concurrent with $x$, and $U_{H(x)}(x) = \mathcal{S}(\mu_{H(x)}(\mathrm{past}(x)))$ is the state the author saw. Admission applies the rule there, and use applies the same rule at a later horizon. An op that fails admission is rejected and never enters the DAG. An op that fails at use stays, and is void.
- **Meanings depend on the horizon.** $\mu_F(x)$ can change as $F$ grows. Layer 1 views change only through $K$. A technique adds a second source of change.
- **Meanings can depend on each other.** $\mu$ is on both sides of (4). $\mathrm{past}(x)$ only reaches down the DAG, so cycles run through concurrent barriers, as WELL_FOUNDED_VERDICTS.md §2.2 shows for verdicts.

The two layers can act on one fact. In RCap, a revoke concurrent with a read clears the slot through (3), and a revoke concurrent with a grant voids the grant through (5).

### 4.2 Equation (5): voiding

$$\mu_F(x) = \begin{cases} x & \text{if } g_x(U_F(x)) \\ \text{void} & \text{otherwise} \end{cases}$$

Meanings are two-valued: the op as written, or nothing. A void entry is dropped from every set $\mu_F$ is applied to, and the remaining entries keep their order. That is the see-through rule of the covers. The guard $g_x$ is the type's rule for $x$: RCap's managing capability, a permissioned RSet's capability requirement, rdb's restriction and FK reach. A guard may also read referenced objects, at the versions named by the ref-advances in its use view.

With $\mu$ on both sides, (5) is a normal logic program over the entries of $F$. The system takes its well-founded model and collapses undecided cores to void, bottom-up. Guards must be exact, sound and monotone in Kleene's three-valued logic (WELL_FOUNDED_VERDICTS.md §5.5).

### 4.3 Other techniques

[MVT_IDEAS.md](../../MVT_IDEAS.md) proposes alternatives to voiding. They fall in different places:

| Idea | What it changes | Where |
| :--- | :--- | :--- |
| A booking conflict is reported, and an arbiter's barrier decides | $\mathcal{S}$ reports both claims | Layer 1 |
| One of two moves of a node is shown, the other reported | $\mathcal{S}$ | Layer 1 |
| Concurrent debits are accepted greedily in a fixed order | the rule deciding live or void; stratified, no cycles | Layer 2, voiding |
| A write concurrent with closing a period moves to the next one | what the write means | Layer 2, not voiding |
| Escrow | the conflict disappears | neither |

Only voiding is built. $\iota$ stays abstract until a second technique is. A technique with non-Boolean meanings and cycles would need a fixpoint beyond the well-founded model; approximation fixpoint theory (Denecker, Marek, Truszczyński) is the generalization.

---

## 5. Properties

Each property names the layer it needs. The proofs are sketches.

**P1, convergence.** $V_F$ is a function of the partially ordered set $F$ and its payloads, so two replicas that hold the same entries give the same view of every pair of versions. Layer 1 needs $\mathcal{S}$ to be deterministic. Layer 2 also needs (4) to have one solution: the well-founded model is unique, and WELL_FOUNDED_VERDICTS.md §4.1 argues the collapse is a function of the data.

In op-based CRDTs, convergence rests on concurrent effects commuting. Here receiving an entry is a set union, which commutes, and all the non-commutation lives in $\mathcal{S}$ and $\oplus$. Commutation comes back only as an obligation of an incremental evaluator, and barriers are the entries exempt from it.

**P2, the context only grows.** If $A \subseteq A'$, then $A \cup K_F(A) \subseteq A' \cup K_F(A')$. If $F \subseteq G$, then $K_F(A) \subseteq K_G(A)$. Proof: the entries after $A$, $\{x \mid A \subseteq \mathrm{past}(x)\}$, only shrink as $A$ grows. Knowing more never removes a barrier from a read (IDEAL_CONCURRENCY.md §4.2). Layer 1.

**P3, plain entries revise nothing.** Receive a plain entry $x$. Every view of a version the replica already held is unchanged, and so is the meaning of every entry it already held: $x$ is in no $K$, in no such version, and in no entry's past. Both layers.

**P4, revision is bounded.** Receive a barrier $x$.

- Layer 1: $V(A)$ changes only if $x \parallel A$. Every $A \subseteq \mathrm{past}(x)$ is unchanged, so the README's `getScopeBound(x)` is $\mathrm{past}(x)$.
- Layer 2: let $\Delta$ be $x$ together with every entry whose meaning changed. $V(A)$ changes only if $A \cup K(A)$ meets $\Delta$, and every $A \subseteq \bigcap_{y \in \Delta} \mathrm{past}(y)$ is unchanged.

The second term is what voiding costs the bound. In `walkRevisionBound` it is the type's visitor, reporting the barriers whose effect changed.

**P5, finality is causal stability.** Suppose every entry received after $F$ has all of $F$ in its past. Then every view of a version inside $F$ is final, and so is the meaning of every entry of $F$: no later entry is concurrent with such a version or with an entry of $F$, so none enters their equations. Layer 2 also needs the solution for the entries of $F$ to ignore entries outside their equations, which is **relevance**. Stable-model semantics lacks it. Dix's classification credits the well-founded semantics with it, which this note has not checked against the source. The collapse keeps it, because components are solved bottom-up.

| | Layer 1 | Layer 2 adds |
| :--- | :--- | :--- |
| Convergence | $\mathcal{S}$ deterministic | a unique solution of (4) |
| Bound of a barrier $x$ | $\mathrm{past}(x)$ | $\bigcap_{y \in \Delta} \mathrm{past}(y)$ |
| Finality | causal stability | relevance |
| Cycles | none | the well-founded model |

---

## 6. Reading it against CALM

The [README](./README.md#introduction) frames MVTs as a monotonic transformation of non-monotonic problems, through the CALM theorem: a problem has a consistent, coordination-free solution exactly when it is monotone.

**The observations are monotone.** A replica's observations are the triples $(A, F, V_F(A))$ for the versions $A \subseteq F$ it holds. Receiving entries only adds triples, since a view is a function of its two versions and a version is a fixed set. A replica also knows by itself when it holds all of a version: it holds the heads and everything they link to. CALM's coordination is the cost of knowing that an input is complete, and a hash-linked version carries that knowledge.

**That part is cheap.** Indexing answers by the version they were computed from makes any deterministic function of history monotone in this sense. The transformation does not make the problem monotone. It names the input of each answer. What MVTs add is P3–P5: only barriers revise a view, only concurrent ones, and within a bound.

**The view itself is not monotone.** As $F$ grows, a barrier delete removes an element from $V_F(A)$. With voiding the view is not even antitone: an entry received later can void that delete, by revoking its author concurrently, and the element comes back. Finality needs P5's causal stability, and establishing it needs every writer's frontier. That is coordination, as CALM predicts for the non-monotone part. The settlement section of MVT_IDEAS.md is about this cost.

---

## 7. Relation to other formalisms

- **Op-based CRDTs** (Shapiro, Preguiça, Baquero, Zawirski). An update is prepared at its source, and its effect is applied at every replica in causal order. Concurrent effects must commute. An op's meaning is fixed at its source. Here it is fixed at use, against the barriers it had not seen.
- **Specifications by operation context** (Burckhardt, Gotsman, Yang, Zawirski). A result is a function of the graph of operations the event has seen. An op's result never depends on the ops concurrent with it, which are outside that graph. (4) adds the concurrent barriers to the context, and that is why meanings need a horizon.
- **Pure op-based CRDTs** (Baquero, Almeida, Shoker). Delivery only adds an op to a partially ordered log, and every datatype-specific rule lives in the query. Layer 1 has this shape. Their causal stability is P5.
- **Semidirect products** (Weidner, Miller, Meiklejohn). An op of one CRDT is transformed by the concurrent ops of another before it applies, so the acting ops effectively come after it. $\oplus$ is that order, taken relative to the version read.

---

## 8. Where the implementation departs from the definition

1. **Ref-advance verdicts.** A ref-advance is judged at the version it imports, widened only by live concurrent ref-advances strictly above that version (`widenRefAtUpward`, and `widenRefAtUpwardFold`, which keeps an undecided one pending). Under (4), its use view would be its past with every concurrent barrier. The narrower context keeps the ref-advances of one reference from reading each other except up the referenced object's versions, a strict order, so ref-advances alone form no cycle; IDEAL_CONCURRENCY.md §4.4 argues that no revoke is lost. It does change what can help the op: a grant on an incomparable branch would count under (4), and does not here. The context covers the reference being advanced only. An rdb observe's gate can also read the observer's other references and its own tables, and reads those as a guard does, in its use view, so an observe can share a verdict cycle with the observer's row ops (rdb's VOID_SEMANTICS.md §5.5).
2. **Ref-advance admission** is at the version the advance imports, not at its own horizon. The std_types README calls it the one op admitted at another version. In rdb, admission is mixed: the reference being advanced is read at the imported version, and everything else the gate reads at the op's own position.
3. **RCap's manager list** is read at `(op, op)`, so part of the guard reads at the op's own horizon rather than at $F$ (IDEAL_CONCURRENCY.md §6.3).
4. **Admission reads references through the geometric fold.** At the op's own position, a guard reads a referenced object at the version that every ref-advance in the op's past gives, void ones included, where §4.1 implies the live fold. rdb's row ops and the other references of an observe's gate are admitted this way. The reason is that the live fold is not monotone in the position: a ref-advance live at one position can be void at a later one, once that position holds an entry that voids it or collapses with it. Read through the live fold, an author could contest an import of a revoke, then write past it with the authority it removes. Through the geometric fold, a revoke the author has seen binds her, whatever the verdict of the ref-advance that brought it, so knowing more never helps the author being judged, as under ideal concurrency (IDEAL_CONCURRENCY.md §4.2). The cost runs the other way: a grant that only a void ref-advance imported can pass admission and fail at use.

Each is either a clause the definition should state or a change the code should make. Item 4 is a clause.

---

## 9. Open questions

1. **Is the layer boundary right?** The cut is what concurrency is measured against: the version read, or the op. The alternative is one layer, with $\mathcal{S}$ given the horizon. It is shorter, but it loses P4's Layer 1 bound, which needs a view to depend only on $A \oplus K$.
2. **Is "after" the only placement?** $\oplus$ commits every barrier to acting as if it came after the version read. Every barrier in the repo fits. Revokes act on concurrent uses through Layer 2, not by being placed before them.
3. **Barriers inside the version read.** (3) replays the barriers outside $A$. Inside $A$, barrier-wins is $\mathcal{S}$'s job. RSet removes an element when a barrier delete is among the latest ops on it, even with a concurrent add beside it, so the barrier still wins once the branches merge. The definition does not require that. Should it, and how, given that in Section 3.4 the re-adds after the barriers rightly bring the element back?
4. **A branch seen from the merge** (Section 3.4) differs from the merge. Replaying the plain entries after a barrier as well would merge the barrier's branch into the read, which is what (2) avoids. Is that difference what applications should see?
5. **Meanings beyond live and void.** Should $\iota$ stay in the definition before a second technique exists, or should Layer 2 be stated for voiding alone?
6. **The ref-advance gate** (Section 8, item 1). State its context as an exception, or move it to (4)?
7. **Finality.** P5 covers honest writers. The settlement certificates in MVT_IDEAS.md are barriers. Do they belong to Layer 1, or are they a technique?
8. **The name.** What is monotone is the context (P2) and the set of observations (Section 6), not the views.

---

## References

- Shapiro, Preguiça, Baquero, Zawirski. *Conflict-free Replicated Data Types.* SSS 2011. The op-based definition and its convergence proof are in INRIA RR-7687; the earlier comprehensive study is [RR-7506](https://hal.inria.fr/inria-00555588).
- Burckhardt, Gotsman, Yang, Zawirski. *Replicated Data Types: Specification, Verification, Optimality.* POPL 2014. [Extended version](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/replDataTypesPOPL13-complete.pdf).
- Baquero, Almeida, Shoker. *Making Operation-Based CRDTs Operation-Based.* DAIS 2014. [10.1007/978-3-662-43352-2_11](https://doi.org/10.1007/978-3-662-43352-2_11)
- Weidner, Miller, Meiklejohn. *Composing and Decomposing Op-Based CRDTs with Semidirect Products.* ICFP 2020. [arXiv:2004.04303](https://arxiv.org/abs/2004.04303)
- Hellerstein, Alvaro. *Keeping CALM: When Distributed Consistency is Easy.* [arXiv:1901.01930](https://arxiv.org/abs/1901.01930)
- Denecker, Marek, Truszczyński: approximation fixpoint theory.
- Dix. *A Classification Theory of Semantics of Normal Logic Programs*, I and II. Fundamenta Informaticae 22(3), 1995.
- Abrantes, Pereira. *Properties of Stable Model Semantics Extensions.* [arXiv:1405.3896](https://arxiv.org/abs/1405.3896)
