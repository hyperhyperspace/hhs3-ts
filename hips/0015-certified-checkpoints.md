# HIP-15: Certified checkpoints (finality)

- **Type:** design
- **Status:** draft
- **Modules:** rdb, mvt
- **Requires:** —
- **Superseded by:** —
- **Implemented in:** —

## Summary

Against a Byzantine writer no amount of waiting makes a view final: a barrier concurrent with any version can still arrive. This HIP proposes certified checkpoints for table groups: a version that a quorum of witnesses signs, after which every barrier concurrent with it is void. It states the finality claim checkpoints support, proves it, lists what they would fix in rdb today, places the design against prior work, and plans an implementation.

This note proposes **certified checkpoints** for table groups: a version that a quorum of witnesses signs, after which every barrier concurrent with it is void. It states the finality claim checkpoints support, proves it, and plans an implementation in rdb. Nothing here is implemented.

Notation follows [MVT_DEFINITION.md](../modules/mvt/MVT_DEFINITION.md): $H(x)$ is $x$ with its ancestors, $\mathrm{past}(x) = H(x) \setminus \{x\}$, a version is a downward-closed set of entries, $x \parallel A$ means $x \notin A$ and $A \not\subseteq \mathrm{past}(x)$, $K_F(A)$ is the set of barriers in $F$ concurrent with $A$, and $V_F(A)$ is the view of $A$ from $F$. Voiding is the technique rdb uses: an entry's guard is evaluated in its use view, and failing entries are void.

Section 1 states the problem. Section 2 defines checkpoints and Section 3 proves what they guarantee. Section 4 lists the costs. Section 5 lists what checkpoints would fix in rdb today, and Section 6 plans the implementation. Section 7 places the design against prior work, and Section 8 collects open questions.

---

## 1. The problem

### 1.1 Views are provisional

A view can change as the horizon grows. MVT_DEFINITION.md P4 bounds the change: a barrier revises only views of versions it is concurrent with, and with voiding, only views above the common past of the entries whose meaning it changes. P5 says when change stops: once a version is causally stable, nothing appended later is concurrent with it.

P5 holds with a weaker hypothesis. Plain entries revise nothing (P3), so it suffices that **every barrier** appended later has the version in its past. That form matters below.

### 1.2 Causal stability needs honest writers

Hash links bind an entry to its predecessors. They do not bind an author to its own earlier entries. A writer that has received a version $A$, and even acknowledged it, can still append an entry whose predecessors are an older version $P \not\supseteq A$. If that entry is a barrier, it is concurrent with $A$ ($x \notin A$ since it is new, and $A \not\subseteq P$), so it enters $K(A)$ and can revise $V(A)$.

So against a Byzantine writer, no amount of waiting or acknowledgement makes $V(A)$ final. [HIP-18](0018-mvt-ideas.md)'s settlement section reaches the same conclusion for assets and proposes certificates. This note makes certificates a property of the group rather than of one type.

### 1.3 Who can still do it

Within one history, backdating gains a writer little. An entry $x$ appended to a replica holding $F$ is new, so every barrier of $F$ is either in $\mathrm{past}(x)$ or concurrent with $x$: $x$'s use view contains every barrier of $F$, known or replayed. The barriers outside $x$'s use context are exactly those whose authors had seen $x$. A writer whose authority was removed by a barrier the reader holds cannot get a guarded op past that barrier by choosing older predecessors.

The writers who can still revise $V(A)$ are:

1. writers who still hold the authority to author a live barrier that acts on $A$;
2. writers of barriers that have no use-time guard (in rdb, schema deploys; Section 5.1);
3. writers whose authority is read from another group, at a version their own ref-advances choose (the use-before-revoke residual; Section 5.2).

The first is the purpose of authority. A checkpoint does not restrict authority. It sets a version below which nobody, authorized or not, can revise anything.

---

## 2. Checkpoints

### 2.1 Definition

**Eras.** A group with checkpoints has eras $0, 1, 2, \dots$. Era 0 is the group's create op $c_0$, whose cut is $C(c_0) = H(c_0)$. It is certified by definition.

**Witnesses.** The group declares a witness function $W$, which maps a view to a finite set of witness keys, and a threshold function, by default $q(n) = \lfloor 2n/3 \rfloor + 1$.

**Checkpoint entries.** A checkpoint is an entry $c$ whose payload carries:

- an era $e \geq 1$;
- $\mathrm{prev}(c) \in \mathrm{past}(c)$, a checkpoint of era $e - 1$ ($c_0$ when $e = 1$);
- a set $\sigma(c)$ of signatures.

Its **cut** is $C(c) = \mathrm{past}(c)$: the entry's predecessors are the heads of the version it certifies. Its witnesses are read from the previous cut, seen from itself:

$$W(c) = W\big(V_{C(\mathrm{prev}(c))}(C(\mathrm{prev}(c)))\big), \qquad n = |W(c)|.$$

Witnesses sign the message $m(c) = (\text{group}, e, \mathrm{heads}(C(c)))$. $c$ is **certified** when $\mathrm{prev}(c)$ is certified and $\sigma(c)$ contains valid signatures on $m(c)$ by at least $q(n)$ distinct members of $W(c)$.

Certification is a function of $H(c)$ alone. $W(c)$ is a view at a fixed horizon, which never changes, and the signatures are in the payload. So admission can check it completely, and a certified checkpoint is never void.

**The rule.** In a horizon $F$, a barrier $x$ is **cut off** when some certified checkpoint $c \in F$ has $x \parallel C(c)$. A cut-off barrier is void, whatever its guard.

**Honest witnesses** sign at most one message per era, and only a message $(\text{group}, e, P)$ such that $H(P)$ contains a certified checkpoint of era $e - 1$.

### 2.2 Why concurrency with the cut, and not with the checkpoint

$x \parallel C(c)$ asks whether $x$ is in the cut or after it. $x \parallel \{c\}$ would ask whether $x$ had seen the checkpoint entry itself. Two reasons favor the cut:

- **Signing takes time.** Entries written on top of the cut while witnesses sign have the whole cut in their past. They are after the cut and keep their meaning. The second rule would void every barrier written before the certificate arrived.
- **Copies are harmless.** Two checkpoint entries with the same cut and different signature subsets are different entries with the same cut. Under the rule they cut off the same barriers. Under the second rule each would void the barriers written on top of the other.

### 2.3 Why only barriers

A plain entry concurrent with the cut is in no barrier context and in no entry's past inside the cut, so it can revise nothing there (P3). Voiding it would buy no finality. It keeps its normal semantics, which already judge it against every barrier of the cut it had not seen: those barriers are concurrent with it, so they are replayed in its use view.

Some applications want more: an auction close, a poll close, the end of an accounting period. There every write concurrent with the close must be void, plain or not. That is [HIP-18](0018-mvt-ideas.md)'s seal. A **seal mode** that cuts off every entry concurrent with the cut is a strict strengthening, and every proof below goes through unchanged.

### 2.4 Stratification

Cut-off status depends on the DAG and on certification. Certification depends on views at strictly lower cuts. Neither depends on verdicts at the horizon being evaluated. Cut-off is therefore a stratum below the verdict program: in the approximator of WELL_FOUNDED_VERDICTS.md, a cut-off entry's guard is the constant false. Locality, exactness and monotonicity hold trivially. A cut-off entry is dropped by the see-through covers like any other void entry.

---

## 3. What checkpoints guarantee

An era $e$ is **safe** when fewer than a third of its witnesses are faulty. Its witness set is well defined once the cuts of era $e - 1$ are unique, which Theorem 1 establishes inductively.

### Theorem 1 (one cut per era)

If eras $1, \dots, e$ are safe, then for every $i \leq e$, all certified checkpoints of era $i$, on every replica, have the same cut $C_i$.

*Proof.* By induction on $i$. Every certified checkpoint of era $i$ names a certified checkpoint of era $i - 1$. Those all have the same cut, by induction or because era 0 is $c_0$. So they share one witness set $W$, with $n = |W|$, and one threshold $q$. Suppose two certified checkpoints of era $i$ have cuts $C \neq C'$, with signer sets $Q, Q' \subseteq W$ of size at least $q$. Then $|Q \cap Q'| \geq 2q - n > n/3$, which is more than the number of faulty witnesses. So some honest witness signed two messages for era $i$, against the rule. ∎

### Lemma 2 (cuts are nested)

Under the hypothesis of Theorem 1, $C_{i-1} \subsetneq C_i$ for every $i \leq e$. A checkpoint of era $i$ has $\mathrm{prev} \in C_i$, and $C_{i-1} = \mathrm{past}(\mathrm{prev}) \subseteq C_i$.

### Theorem 3 (finality below a checkpoint)

Suppose every era is safe. Let $c$ be a certified checkpoint with cut $C$. For every horizon $G \ni c$:

- (a) every entry of $C$ has the same meaning at $G$ as at horizon $C$;
- (b) $V_G(A) = V_C(A)$ for every version $A \subseteq C$.

*Proof.*

1. **The cut-off entries of $C$ are the same at $G$ and at $C$.** Take $y \in C$ and a certified $c' \in G$. If $c'$ is of an earlier era than $c$, its cut equals that of the checkpoint of its era on $c$'s chain, which lies in $C$, so the same test is made at horizon $C$. Otherwise $C \subseteq C(c')$ by Lemma 2, so $y \in C(c')$, and $y$ is not concurrent with $C(c')$.
2. **Every $z \in G \setminus C$ is harmless.** Either $C \subseteq \mathrm{past}(z)$, or $z \parallel C$.
   - If $z$ is after $C$, it is after every $A \subseteq C$ and above every $y \in C$. So it is in no $K_G(A)$ and in no $K_G(H(y))$.
   - If $z \parallel C$ and $z$ is a barrier, $c$ cuts it off.
   - If $z \parallel C$ and $z$ is plain, it is in no barrier context, and in the past of no entry of $C$.
3. **Splitting.** Every guard of an entry of $C$ reads entries of $C$, or cut-off entries, which are void and dropped. The splitting lemma that gives MVT_DEFINITION.md P5 its relevance gives (a). For (b), $K_G(A) \setminus K_C(A)$ consists of cut-off barriers, which $\mu$ drops. ∎

The hypothesis is about witnesses only. Writers can be arbitrarily Byzantine.

**Reading.** A certified checkpoint makes its cut causally stable with respect to live barriers: every live barrier outside the cut is after it. Voiding removes the rest. Theorem 3 is then P5 in its barriers-only form (Section 1.1), and the certificate is what makes the hypothesis true without hearing from every writer.

### Theorem 4 (accountability)

If two certified checkpoints of one era have different cuts, consider the earliest such era. By minimality, its checkpoints share one witness set $W$. More than $|W|/3$ of its members signed both messages, and the two signature sets name them.

So finality fails only if more than a third of some era's witnesses sign conflicting cuts, and then the checkpoints themselves are the evidence. This is the accountable safety of Casper FFG, obtained here from the DAG alone.

### What a reader can conclude

A replica that holds a certified checkpoint $c$ knows, without asking anyone, that every view of a version inside $C(c)$ and every verdict of an entry of $C(c)$ is final, unless more than a third of some era's witnesses have equivocated. In that case it will hold the proof once it receives the conflicting checkpoint.

---

## 4. What it costs

### 4.1 Liveness

Certification needs $q(n)$ witnesses to sign. If witnesses are offline, or split their votes, the era does not close. Nothing else stops: the group keeps accepting entries, and views stay provisional. This is CALM's division: coordination buys finality, not progress.

The one-vote-per-era rule is safe without any timing assumption, but it can stall forever on a split vote. To guarantee that eras eventually close, run a consensus protocol to choose the cut (PBFT, HotStuff, or any protocol with rounds and locking). FLP rules out doing it without timing assumptions. The MVT side only verifies the certificate, so the protocol is pluggable, provided its commit certificates satisfy Theorem 1's uniqueness.

With $n = 1$, the witness is a notary. An application's backend can be that notary. It signs eras and never sees, serializes or approves individual writes.

### 4.2 Honest losses

A barrier is cut off when its author wrote it at a version missing part of a cut that did not include it. An honest writer can hit this in two ways:

- by writing while offline across an era boundary;
- by racing a cut, writing a barrier just before receiving the last entries of a version the witnesses certified without that barrier.

Witnesses limit both by **certifying lagging cuts**: a version old enough that active writers already hold it. A checkpoint's job is to close the door behind a version, not to cut off the present. The void reason (Section 6.6) names the checkpoint, so the application can re-issue the barrier on top of it.

### 4.3 The alternative: carry-over

Instead of voiding a barrier concurrent with a certified cut, it can be moved after the cut. Define an effective order $\prec'_F$ by adding $y \prec' x$ for every $y \in C(c)$ and every $x \parallel C(c)$, for each certified $c \in F$. It is acyclic. Rank entries by the first era whose cut contains them: original edges never decrease rank, and added edges strictly increase it.

Under $\prec'$, nothing is concurrent with a certified cut. Finality is P5 on $\prec'$, and stragglers keep their meaning, judged against the certified state. ERA (Section 7) orders events after an epoch in a similar way.

The cost is in the DAG layer. Hash links give $\prec$, so every cover and concurrency query would need $\prec'$. A version containing a carried-over entry is also no longer downward closed under $\prec'$, so reads at such versions would have to include the cut. Voiding needs only a verdict. Start with voiding.

---

## 5. What checkpoints would fix in rdb today

### 5.1 Backdated deploys

A schema deploy is a barrier-tagged ref-advance. `canDeploy` is checked at admission at the deploy's own position (`validateDeploy`, `src/rtable_group/validate_ops.ts`). At use, `diagnoseObserve` returns live for the schema reference (`src/rtable_group/group.ts:1188`), and the delta walk assumes deploys are never void (`src/rtable_group/delta.ts:43-48`).

A deploy key that loses `canDeploy` can therefore still deploy at a position from before, and the deploy is replayed on every concurrent view. Under checkpoints, such a deploy is concurrent with any later certified cut and is cut off.

### 5.2 Cross-group early writes

A guard that reads another group reads it at the version named by the ref-advances in its use view. A writer who observes a version entirely below a revocation in the observed group places the revocation after that version, not concurrent with it. This is the use-before-revoke residual, `[OCUT06]` and `[OCUT08]` in [HIP-7](0007-ideal-concurrency.md) §3.1.

Once the observing group has a certified cut containing a ref-advance past the revocation, the window closes:

- a backdated observe is a barrier concurrent with the cut, and is cut off;
- a new observe must contain the advances before it (`validateRefAdvanceMonotonicity`, `mvt/src/refs.ts`), so it includes the revocation.

Witnesses who want this property should refresh the group's observes before cutting.

### 5.3 Not fixed: FILES access

FILES members check write access only at admission, and "a key that loses access later can still append at a position from before" ([README](../modules/rdb/README.md), *Access*). RFileMap adds are plain, so the barrier rule leaves them alone. Closing this needs seal mode on FILES members, or use-time access checks.

### 5.4 Cheaper deltas

When both horizons of a delta contain a certified checkpoint $c$, nothing below $C(c)$ changed (Theorem 3), so the revision bound never needs to go below the cut. This is an optimization, not a correctness requirement.

---

## 6. Implementation in rdb

### 6.1 Scope

Checkpoints are per table group. The README calls the group "the unit of atomicity, snapshot, observation, and composition", because its tables share one causal history. One cut certifies every table at once, and finality composes along observations: a group that observes another at a version inside that group's certified cut reads final data there.

A database-wide checkpoint is not needed. FILES members come later (Section 5.3).

### 6.2 Configuration

Declare checkpoints on the catalog group (`CatalogGroupDef`, `src/rcatalog/payload.ts:93`), copied into the group's create payload at instantiation (`src/rdb/instantiate.ts`), next to `canDeploy` and `canObserve`.

- **Phase 1: a fixed witness list.** An embedded key list in the create payload, like `deployKeys` (`src/rtable_group/payload.ts`), plus a threshold. $W$ is constant, $c_0$ is the create op, and witness keys are verified against the embedded list.
- **Phase 2: witnesses from rows.** $W$ is the set of keys returned by a declared query, for example capability rows labeled `witness`, evaluated at $(C_{e-1}, C_{e-1})$. It must be enumerable, unlike an `ALLOW … IF` predicate, because the threshold needs $n$. Witness changes then take effect at era boundaries, from a view that is final by Theorem 3.

C-SQL could follow the catalog's existing `ALLOW DEPLOY IF` clauses (`rdb_lang/src/syntax/parser.ts`), for example `CHECKPOINTS BY (key, …) THRESHOLD 3` in phase 1 and `CHECKPOINTS BY SELECT grantee FROM user.caps WHERE label = 'witness'` in phase 2.

### 6.3 The op

Add one group action. "Checkpoint" already names a projection target's materialized version (`getCheckpoint`, `rdb_adapter/src/types.ts`), so this note suggests the action name `era`:

```ts
type EraPayload = {
    action: 'era';
    era: number;                 // ≥ 1
    prev: B64Hash;               // era op of era - 1, or the create op
    signatures: { keyId: string; suite: string; signature: string }[];
};
// prevs of the entry = heads of the certified cut
// meta: { barrier: ['t'], era: ['t'] }
```

It is barrier-tagged because it acts on entries that did not see it. The `era` meta tag lets `findCoverWithFilter` find the latest era ops in a horizon. Witnesses sign `{domain: 'hhs3/era-sig/v1', group, era, prev, heads}` with the existing suites (`crypto/src/registry.ts`), under a new domain distinct from `hhs3/op-sig/v1` (`mvt/src/authorship.ts`). The entry needs no author: anyone holding a certificate may append it, and duplicates are harmless (Section 2.2).

Code that enumerates the group's actions and must learn the new one, from a survey of the current source:

- `TableScope.unwrapPayload` and `tableOpsFromGroupPayload` (`src/rtable_group/scopes.ts`);
- `applyPayload` (`src/rtable_group/group.ts`);
- the format validator (`src/rtable_group/validate.ts`);
- `isVoidCheckable` and `describeGroupEntry` (`src/rtable_group/op_delta.ts`);
- `renderOp` (`rdb_lang/src/reverse/render.ts`), which currently falls back to a comment.

`diagnoseEntry` already returns live for unknown actions, which is right for the era op itself.

### 6.4 Admission

Add `validateEra` to the semantic dispatch (`validateTableGroupPayload`, `src/rtable_group/validate_ops.ts:123`):

1. `prev` is in the entry's past, and is the create op with `era = 1`, or an era op with `era − 1`.
2. Compute $W$ at the previous cut. In phase 1 it is the embedded list.
3. Verify every signature against `{group, era, prev, sorted prevs}`. Count distinct valid signers in $W$, and require at least the threshold.

Since certification depends only on the entry's history, admission is complete: era ops, like the create op, are never void.

### 6.5 Witness protocol

- **Phase 1, notary.** `group.certify(notaryKey, {lag})` picks a lagging cut, signs it, and appends the era op. No consensus is needed.
- **Phase 2, committee.** A witness publishes its vote as a plain, signed `era-vote` entry carrying `{era, prev, heads}`. Votes are plain, so they are never cut off. Any party assembles the era op once a quorum of votes exists. Votes in the DAG make every equivocation visible to every replica (Theorem 4) without a separate channel. A leader per era, for example the witness at index $e \bmod n$, proposes the cut.
- **Phase 3, rounds.** Replace one vote per era with a round-based protocol whose commit certificate is what the era op carries, so that a split vote does not stall an era forever.

### 6.6 Use time

Cut-off is checked in `diagnoseEntry` (`src/rtable_group/group.ts:1084`) before anything else, including the schema-deploy shortcut in `diagnoseObserve`, which today returns live unconditionally:

1. If the entry is not barrier-tagged and the group is not in seal mode, skip.
2. Find the era ops in `from` with `findCoverWithFilter(from, {era: ['t']})`, and follow `prev` to obtain the chain. Memoize the chain per `from`.
3. Cuts are nested (Lemma 2), so "$x \in C_i$" is monotone in $i$. Binary-search for the latest cut not containing $x$, and test whether that cut is in $x$'s past. If not, $x$ is cut off. Both tests are ancestry queries. The `Dag` interface (`dag/src/dag.ts`) has no direct ancestry test, so this needs one, built from the index or `findForkPosition`.
4. If several era ops of one era are present (equivocation), test each cut. The rule is per checkpoint, so nothing else changes.

Return `{status: 'void', reason: {kind: 'checkpoint', era, checkpoint}}`. Add the variant to `OpVoidDetail` and `formatOpVoidDetail` (`src/rtable_group/op_void.ts`), to `OpVerdictKind` and `describeGroupEntry` (`src/rtable_group/op_delta.ts`), and to the adapter's `OpEvent` (`rdb_adapter/src/types.ts`).

Views see through void entries via `entryTruth` (`src/rtable/view.ts`), so cut-off deletes stop deleting with no further change. Three reads currently assume deploys are never void and must consult cut-off:

- `resolveSchemaView`;
- `resolveEffectiveParams`;
- the schema ref fold, which can use the `isLive` hook of `resolveRefVersionAtPosition` (`mvt/src/refs.ts`).

### 6.7 Delta

A newly received era op flips the verdicts of the barriers it cuts off. The visitors in `src/rtable_group/delta.ts` must report those flips, including flips of deploys, so the walk lowers the bound below a deploy whose verdict changed. That retires the "deploys are never void" assumption (`delta.ts:43-48`, INCREMENTAL_DELTA_BOUND.md §3). It holds in phase 1 only for groups without checkpoints.

Section 5.4's floor is a later optimization.

### 6.8 Reads, C-SQL and projections

- **API.** `getFinalVersion(from)` returns the latest certified cut in `from`, and `isFinal(at, from)` tests $A \subseteq C$.
- **C-SQL.** Add `FINAL` as a version expression (`VersionExpr`, `rdb_lang/src/syntax/ast.ts`; `resolveVersion`, `rdb_lang/src/bind/context.ts`), so `SELECT … AT FINAL` reads the certified cut from the current frontier. `LOG` shows the `checkpoint` void reason in its void column, and a `SHOW ERAS` statement lists the chain with signers.
- **Projections.** A projection can target the latest certified cut instead of the frontier (`projectGroupTo`, `rdb_adapter/src/project.ts`). Such a projection never retracts, so it is a safe source for side effects: an outbox worker that acts only on final rows sends each email once.

### 6.9 Tests

| Tag | Checks |
| :--- | :--- |
| `[FIN01]` | A delete written at a version missing part of a certified cut is void with reason `checkpoint`. The row stays in every view inside the cut. |
| `[FIN02]` | A plain insert concurrent with the cut stays live, and is judged against the cut's barriers it had not seen. In seal mode it is void. |
| `[FIN03]` | A delete written on top of the cut before the era op exists is not cut off. |
| `[FIN04]` | Two era ops with the same cut and different signer subsets cut off the same entries. |
| `[FIN05]` | An era op below the threshold, with a signer outside $W$, or with a bad `prev` is rejected at admission. |
| `[FIN06]` | A backdated deploy by a key that lost `canDeploy` is cut off. The schema at positions above it no longer includes it, and the delta reports the change. |
| `[FIN07]` | `[OCUT06]`'s early write is cut off once the observer's certified cut contains a ref-advance past the revocation. |
| `[FIN08]` | Two certified era ops of one era with different cuts (an equivocating notary) both apply, and the evidence API names the signers of both. |
| `[FIN09]` | Phase 2: the witness set read at $C_{e-1}$ changes certification of era $e$ only. |
| `[FIN10]` | Delta parity across an era op, in the extended profile. |

### 6.10 Phases

1. **Notary.** Fixed witness list, barrier rule, table groups only, `[FIN01]`–`[FIN08]`, `[FIN10]`.
2. **Committee.** Witnesses from rows, vote entries, the evidence API, `[FIN09]`.
3. **Later.** A round-based protocol, seal mode on FILES members, the delta floor, and snapshot hashes in the signed message, so a new replica can verify the certificate chain and start from a snapshot instead of replaying history.

---

## 7. Prior work

- **ERA** (Kegan Dougal, [arXiv:2601.22963](https://arxiv.org/abs/2601.22963), submitted to PaPoC 2026) addresses duelling admins in group-management CRDTs with epoch events from a single trusted *finality arbiter*. Epochs order events rather than void them. Two concurrent epochs from a misbehaving arbiter are detected with a fraud proof, and peers switch to the next arbiter on a list; the paper notes this does not prevent history from being rewritten. Finality is argued rather than proven. Here:
  - a quorum makes conflicting cuts impossible rather than detectable, and $n = 1$ recovers ERA's arbiter;
  - concurrency that is not certified still has defined semantics;
  - finality is Theorem 3, and equivocation is Theorem 4.
- **Matrix.** An event that fails authorization against the receiving server's current state is *soft-failed*. Current state is whatever that server has seen, so two honest servers can disagree on it ([spec](https://spec.matrix.org/latest/server-server-api/)). Jacob et al. analyse Matrix's access control as a DAG without finality (SACMAT 2020).
- **Causal stability** (Baquero, Almeida, Shoker) is the honest-writer special case: a cut every writer has acknowledged.
- **BFT consensus** (PBFT, HotStuff) is how a committee picks the cut. Checkpoints use only its output.
- **Accountable safety** (Casper FFG) is Theorem 4's guarantee.
- **[HIP-18](0018-mvt-ideas.md)'s certificates** for RAsset are per-asset checkpoints: a certified transfer is a cut of the asset's chain that voids concurrent uncertified transfers.

---

## 8. Open questions

1. **The action name.** `era` avoids the projection's `checkpoint`. Is renaming the projection's term better?
2. **Threshold policy.** Is $\lfloor 2n/3 \rfloor + 1$ fixed, or declared? A crash-only committee needs only a majority, but the group cannot know its witnesses are crash-only.
3. **Who appends.** Anyone holding a certificate, or witnesses only?
4. **Seal mode per table.** Should seal mode be per group, or per table, so a `bids` table closes at an era while `profiles` does not?
5. **Stalemates.** Two admins revoking each other concurrently collapse to both void ([VOID_SEMANTICS.md](../modules/rdb/VOID_SEMANTICS.md) §4, `[PERM12]`). Should a checkpoint also break such cores, for example by the seniority order of VOID_SEMANTICS.md §6? That would be a second technique, not voiding.
6. **Deploys and the adoption gate.** A deploy cut off after it was adopted into the replica's RDeployGate leaves the gate ahead of the group. Should the gate follow cut-off?
7. **Lag.** Should the lag be a group parameter, so readers can tell how old a final view is?
8. **Carry-over** (Section 4.3), if honest losses turn out to matter.

## History

This HIP was `modules/rdb/FINALITY.md`, moved here by [HIP-1](0001-hips.md) on 2026-11-04. Text above that names a file by its old name refers to it as it was. The files it was built from can be read at commit `4ec4661` (`git show 4ec4661:<path>`).
