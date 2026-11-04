# Fourth critique of `REFERENCE_FRAME.md`

This reviews [`REFERENCE_FRAME.md`](./REFERENCE_FRAME.md) as revised on 2026-11-04 after [`REFERENCE_FRAME_CRITIQUE_3.md`](./REFERENCE_FRAME_CRITIQUE_3.md) and [`REFERENCE_FRAME_CRITIQUE_3_RESPONSE.md`](./REFERENCE_FRAME_CRITIQUE_3_RESPONSE.md). It is a final pass for conceptual problems only. "§4 rdb_runtime" refers to the plan, "response 2 §1" and "response 3" to the responses, "critique 3 §5" to the third review. Line numbers refer to the working tree on the day of the review and will drift.

---

## Verdict

One conceptual problem, and it is the reviewer's as much as the plan's. **The at-use guard does not give the observe's verdict.** A verdict's reads of a bound group depend on the horizon through `foreignFrom`, the geometric fold at the horizon, and a new observe extends the next horizon's fold of its binding by the version it imports. So the verdicts the gate reads can change between the check and the observe (Section 2). The claim that they cannot is in the plan (§4 rdb_runtime, "The at-use guard"), in response 2 §1, and in critique 3 §5, which confirmed it and is withdrawn on that point (Section 3).

**What it does not break:** the soundness of the fold and the touch rule, the bounded-delta argument, and the termination of the retry, which holds for a different reason than the plan gives (Section 4).

**What it changes:** the guard's semantics and tests, author selection, and one sentence of "Why the collapse does not fail open" (Sections 4 and 5).

**Verified as correct:** the five corrections of response 3 §1, the per-computation memo, the single-flight check, and "Only what changed", with one input to add (Section 6).

The fix is a design decision with two options, and the exact guard is recommended (Section 5). Nothing else in the plan warrants another round.

---

## 1. How the claims were checked

I read the current plan in full and response 3 against:

- rdb: `src/rtable_group/group.ts`, `resolveForeignTableViewIn` (`:617-634`, the live fold's verdicts at `:626`, `foreignFrom` at `:629`), the observe's diagnosis (`refAt` at `:1195`, `refFrom` at `:1196`), a row op's foreign reads at use (`:1114`), `resolveObservedForeignVersion` (`:723-727`) and `resolveAuthorKey` (`:740-754`); `src/rtable/view.ts`, `killedByConcurrentDelete` (`:279-297`), the concurrent cover from `from` (`:281-284`) and `honored` (`:286-289`);
- rdb_runtime: `src/session/ref_auto_update.ts`, `isAlreadyObserved` (`:113-123`) and `observeWithResolvedAuthor` (`:129-160`);
- `INCREMENTAL_DELTA_BOUND.md`, assumption A3 (`:168-175`), and guarantees 3, 4 and 6 of `mvt/src/revision_bound.ts`.

No test was run for this round. The finding is in the semantics of the horizon, which the two prototypes do not model: `[TOUCHP]` fixes `from` per trial and reads every completion at that same `from`.

---

## 2. The horizon channel

Every verdict at horizon `from` reads a bound group at (`foreignAt`, `foreignFrom`). `foreignAt` is the fold at the entry's position, live observes only at use. `foreignFrom` is the fold at the horizon with no liveness filter: `group.ts:629` for the views a row op or a gate builds of other bindings, `:1196` for the gate's own `refFrom`, and A3 states it ("`foreignFrom` folds every observe in `H`, with no liveness filter"). Two reads consume it:

- the concurrent query of `rowState`, which kills a row through a live honored delete of `K` that is concurrent with `foreignAt` and visible from `foreignFrom` (`view.ts:281-284`);
- the honor flag of each such delete, read at the delete's own position and `foreignFrom` (`:286-289`).

So an entry's verdict changes with the horizon when the horizon's fold brings a barrier of `K` concurrent with the entry's `foreignAt`, or a deploy of `K` concurrent with such a barrier. The plan relies on this channel: §3.2, case 2, is a revoke concurrent with `y`'s version, found "at the horizon version", and the same holds for a row op's guard. The guard paragraph forgets it.

Now take a new observe `o'` of `K` written at `G`'s frontier `F`. It is concurrent with nothing, it is in no fold or widening, and it advances one reference. At its own horizon, `H(F) ∪ {o'}`, one thing differs from horizon `F`: the fold of `K` is `fold(F) ∪ V'`, with `V'` the imported version. Every other input is the same: the folds of the other references, `G`'s schema, every concurrency set, every widening. That one difference reaches every view of `K` that a verdict at the new horizon builds: the verdicts of `G`'s rows whose rules read `K`, the verdicts of other bindings' observes whose gates read `K`, and the gate's own `refFrom`.

---

## 3. The guard is not the observe's verdict

§4 rdb_runtime says the guard is evaluated "at `G`'s frontier as position and horizon" and that its answer "is the observe's verdict when written". In the configuration of walkthrough 3, where `admins`' rules are `$author IN auth.caps` and the gate on `auth` is `$author IN admins`:

1. Alice's `admins` row was inserted by `r`, which read `K` at `D_r`.
2. On a branch of `K` concurrent with `D_r`, Alice's cap is deleted, and `caps` honors concurrent deletes. `K`'s frontier `V'` holds the delete.
3. The check at `G`'s frontier `F` reads Alice's row with horizon `F`. The fold of `K` at `F` lacks `V'`, so the delete is invisible from `foreignFrom`, `r` is live, the row is live, and the guard says true. Alice's observe `o'` is written, importing `V'`.
4. At `o'`'s own horizon, `r` reads `K` from `fold(F) ∪ V'`. The delete is concurrent with `D_r` and visible, so Alice's cap is killed, `r` is void, her row is dead, and `o'` is void with `observe-gate`.

The reverse direction exists too. If at horizon `F` Alice's row is dead because such a delete is honored, and `V'` carries a deploy of `K`, concurrent with the delete, that turns `concurrentDeletes` off, then at the observe's horizon `honored` reads the deploy, the delete no longer kills, `r` is live, and the observe would be live. The guard says false and nothing is written.

**The retraction.** Response 2 §1 says the new observe "is concurrent with none of them and changes none of their verdicts. It is in none of their folds either." The folds are right; the verdicts are wrong, through `foreignFrom`. Critique 3 §5 repeated it ("no old verdict changes, since `o'` is in no old entry's history or concurrent set, hence in no old fold or widening") and is withdrawn on that point. Of its three parts, only the gated binding's `refAt` and `refFrom` survive: `refAt` is the imported version, and `refFrom` is `fold(F) ∪ V'`, which is `V'` by monotonicity. The claim that the other bindings' observes keep their verdicts is wrong for the same reason, since their gates may read `K`.

---

## 4. What it does not break

- **Soundness.** A verdict is what its horizon says. The guard only decides whether an observe is written, so a wrong guard writes a void observe or withholds a live one. It never changes what any entry means.
- **Termination.** Once `o'` is in a replica's history, the fold of `K` at every later horizon there holds `V'`. A guard at such a horizon reads what `o'`'s verdict read, so for an import of the same version its answer is the verdict: if `o'` was void with `observe-gate`, the guard says false and nothing more is written. So a replica writes at most one void observe per version of `K` that reaches it before any observe of that version is in its history, and a newer version can reopen the channel only with new content of `K`: a barrier concurrent with an existing fold, or a deploy concurrent with such a barrier. The loop is bounded by `K`'s activity, as the amplification of §3.3 is. The §4 sentence that motivates the guard, "would write a new void observe on every check, for good", is about the fold disagreement, and the guard still closes that; the two runtime tests of §5 for it still hold.
- **Collapses.** After a collapse, the guard reads the collapsed verdicts and writes the observe from the merge, which imports a version already in the fold. There the guard is exact, and the retry argument of §4 stands.
- **"Why the collapse does not fail open."** Its last sentence says `G` "keeps reading `K` at the version it had". That holds for history reads. Through the horizon, a barrier of `K` in the void import's version that is concurrent with `D` still kills rows in `G`'s reads at `D`, when honored. In walkthrough 3 the revoke of Bob's cap is after `D`, so the walkthrough is right, but the sentence is too strong in general. It is the deny direction and today's semantics (A3), so nothing fails open; the text should promise only what the fold gives.

---

## 5. Two ways to fix it

> **Note (2026-11-04):** option 2 is withdrawn. [`REFERENCE_FRAME_CRITIQUE_4_RESPONSE.md`](./REFERENCE_FRAME_CRITIQUE_4_RESPONSE.md) §2 shows that its second test cannot pass, since admission reads `G`'s tables before the import, and that an exact guard would withhold a self-revoking import, which the void observe delivers through the horizon. Option 1 stands, as the response's §3 states it. Three additions from the review of that response: the horizon reaches reads by a third path, `H`'s own verdicts at `foreignFrom` (`view.ts:239`), so history reads of `H` can change too; the plan should state once that the horizon fold counts every observe because it is what the replica knows, and report an `observe-gate` void observe as having advanced the horizon; and "no author imports her own re-authorization" holds for `G`'s tables and other bindings, not for the gated binding's own atoms, which are read at the imported version by design.

**1. Keep the guard as a heuristic.** Rewrite the paragraph: the guard reads at the frontier's horizon, whose fold of the gated binding lacks the version being imported, so the observe's verdict can differ from its answer through barriers of `K` concurrent with the folds the gate's reads use, in either direction; the loop is bounded as in Section 4. Add to §5: an observe whose own import voids its author's authorization is written once and not retried; an author kept out by an honored delete that the import's deploy unhonors writes no observe, an honest loss.

**2. Make the guard exact.** Recommended. `evaluateObserveGate` with `fold: 'use'` already takes the imported version. Let it evaluate at the horizon the observe will have: in its fresh evaluation, the horizon fold of the gated reference is `fold(frontier) ∪ imported` wherever that fold is computed, `group.ts:629` and `:1196`, best through one helper that reads the override from the evaluation. By Section 2 nothing else at that horizon differs, so the guard's reads are the observe's reads and its answer is the verdict, in both directions of Section 3. Three things make it safe:

- Verdicts computed under the override are keyed by horizon and live only in the fresh evaluation, which is discarded. Nothing reaches another computation.
- `K`'s own verdicts need no override. They are read at the horizon the view passes, `fold(frontier) ∪ imported`, which is a version of `K` like any other.
- `isAlreadyObserved`, the delta walk and every other caller of `resolveObservedForeignVersion` keep the real fold.

`resolveObserveAuthor` evaluates its candidates on the same evaluation, so author selection picks an author whose observe will be live, which §4 "Author selection" already assumes. The cost is unchanged: the override changes which versions are read, not how many reads are made. With it, §5 gains two runtime tests: an import whose version carries a delete concurrent with the author's authorization writes no observe, and one whose deploy unhonors the delete that kept the author out writes a live one.

---

## 6. The response's four questions

1. **§3.4 step 1 for every entry of `G`.** Yes. The argument uses only that `e` is in `U` and that each observe in its fold is in `U` or stable by Lemma 6. Nothing in it uses that `e` is an observe, so the touch test's reads of a row op are covered as those of an observe are.
2. **The A3 wording in §6.** Yes. A read of the bound group at a version, in A3's sense, includes the verdicts the predicate reads, which Theorem 1(b) for `K` fixes. `refVersionAtOrAbove` compares two versions inside `K`'s bound and needs no mention. The pending set is solve state, not an input, which A2 covers, and belongs in the proof's text rather than in A3.
3. **"Only what changed".** One more input: local presence. `resolveForeignTableViewIn` loads the foreign group and throws when it is missing, which the caller defers. A check that throws must not be recorded as done, since the same frontiers answer differently once the object arrives. Otherwise the two frontiers and the local identities are all, because every version the check reads is pinned by hash, and with the exact guard the imported version is `H`'s frontier, one of the two.
4. **The memo.** A per-computation memo is enough to implement with. A cache across computations is the same decision as for verdicts (`mvt/WELL_FOUNDED_VERDICTS.md` §6.5), and should be taken for both at once, once the design has landed and the cost is measured.

---

## 7. Recommendations

1. Fix the guard, preferably by making it exact (Section 5), and rewrite the guard paragraph of §4 rdb_runtime and its termination sentence.
2. Add the two runtime tests of Section 5 to §5.
3. Weaken the last sentence of "Why the collapse does not fail open" to history reads (Section 4).
4. Record a check as done only when it completed (Section 6, item 3).
5. Add dated notes to response 2 §1 and critique 3 §5. This review adds the latter.

With the first, the plan is ready to implement.
