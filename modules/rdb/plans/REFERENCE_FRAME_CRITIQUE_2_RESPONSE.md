# Response to `REFERENCE_FRAME_CRITIQUE_2.md`

This answers [`REFERENCE_FRAME_CRITIQUE_2.md`](./REFERENCE_FRAME_CRITIQUE_2.md) and lists what changed in [`REFERENCE_FRAME.md`](./REFERENCE_FRAME.md) since. "Critique 2 §4" refers to the second review, "§2.5" to the plan. Section 3 below covers changes the second review did not see. They were made after it was written, and they need a review of their own.

---

## Summary

Both main problems are right, and so is every smaller point. The loop example of critique 2 §4 needs one correction, and the guard it proposes needs three extensions (Section 1). The correction to the first response about rdb's `dist` is right: it had been rebuilt at 23:03, before the response said it had not.

| Review item | Response | Plan change |
| :--- | :--- | :--- |
| §4. The retry can loop forever | Right. The example needs `w` to be admitted (Section 1). The guard applies to every observe the runtime writes, and to author selection. | §4 rdb_runtime, rewritten as one rule; §4 rdb, a `fold` option for `evaluateObserveGate`; §5. |
| §5. The triggers do not exist as described | Right, confirmed in the code. | §4 rdb_runtime: triggers, the sync hook, the one rule over the bound group's frontier; §5: the sync harness. |
| §5. Amplification | Right. | §3.3, a sentence. |
| §2. The oracle stays in the suite, with even loops | Right. | §5. |
| §6. The lower step reads live members as live | Right. | §2.5, "Nothing decided is retracted". |
| §6. Where the odd loop throws | Right: the lower step re-runs the delete and marks it live; the check after the upper step throws. | Same paragraph. |
| §6. A dated note on critique §3 | Done, at the top of that section. | `REFERENCE_FRAME_CRITIQUE.md` |
| §6. `rpack/IMPLEMENTATION.md` | Outside the plan. Committing it or fixing the links is the owner's call. | None. |
| §3. `dist` was rebuilt | Right. The first response was wrong. | None. |

---

## 1. The retry loop

**The example needs one change.** In step 2, Mallory is not an admin, so her observe `w` of `auth` would be refused at admission: the gate on `auth` is `$author IN admins`, read at `w`'s own position. For `w` to be void at use, it must pass admission first. Mallory can be an admin whose `admins` row a concurrent delete removes, with the delete honored, or a former admin who writes `w` back-dated. With either, steps 3 and 4 hold as written, and the loop is real.

**The guard is the fix, with three extensions.**

1. **Every observe the runtime writes needs it, not only retries.** With the up-to-date check on the live fold, the first observe of a version that passes admission only is already the first turn of the loop.
2. **Author selection needs both checks.** Otherwise `resolveObserveAuthor` picks an author who passes admission only.
3. **The gate needs a mode for it.** The public `evaluateObserveGate` reads other bindings through the geometric fold, as admission does. It now takes `fold: 'use'`, which reads them through the live fold with the observe's position as the horizon (§4, rdb).

Both checks are needed, because the two folds disagree in both directions. A grant that only a void observe imported passes admission and fails at use. A revoke that only a void observe imported fails admission and passes at use, which is what makes every answer in §3.3 back-dated.

> **Note (2026-11-04):** the next paragraph is wrong. A new observe adds the version it imports to the fold at every later horizon. Verdicts read that fold through `foreignFrom`, so entries written before the observe can change verdict. The guard answers whether the author is authorized before the import, and the observe's verdict can differ by what the import itself brings. See [`REFERENCE_FRAME_CRITIQUE_4.md`](./REFERENCE_FRAME_CRITIQUE_4.md) §3 and [`REFERENCE_FRAME_CRITIQUE_4_RESPONSE.md`](./REFERENCE_FRAME_CRITIQUE_4_RESPONSE.md) §3.

The guard's answer is the new observe's verdict when written. At the frontier, nothing is concurrent with the new observe and nothing is pending, so the answer is decided. The new observe is above every entry of the frontier's history, so it is concurrent with none of them and changes none of their verdicts. It is in none of their folds either.

**The rule** is now stated once, as critique 2 §5 item 4 proposes. For each binding `b => H` of `G`: when the fold of `G`'s live observes of `H`, at `G`'s frontier, is behind `H`'s frontier, and a local author passes `b`'s gate both at admission and at use, observe `H`'s frontier. That covers three cases:
- an observe refused for a stale view of another binding;
- an observe void by collapse;
- an observe void with `observe-gate`, written again only when a new one would be live.

---

## 2. Triggers

Confirmed in the code:
- `propagateRefUpdates` is called only from `adapter.ts`, after a statement for which `extractRefUpdateTrigger` returns a trigger: an insert, an update, a delete or a bundle.
- After a successful observe, its queue moves only to the groups that observe `G`, never to `G`'s other bindings.
- The workspace subscribes only to new roots.

§4 now lists:
- **`UPDATE REF`**, with `G` as the changed group;
- **`ALTER SCHEMA` and catalog updates**;
- **sync batches**, through the subscription each group offers (`RTableGroupImpl.subscribe`, built on the DAG growth listener). The check coalesces a batch's events or runs when the synchronizer is idle. After any batch, every group in the workspace that binds another is checked, in gate-graph order, and the up-to-date check decides.

The §5 sync tests point at rdb's harness.

---

## 3. Changes the second review did not see

These were made after critique 2 was written. They are the main thing to review next.

1. **How a touch is computed** (§2.5, "Computing a touch"). A touch is now defined per query.
   - The test runs each of the read's DAG queries again at $D \cup V_o$ and compares the cover with the one at `D`.
   - A concurrent query also compares the history covers of its filter. A concurrent match can enter the history below one that stays concurrent and leave the concurrent cover unchanged.
   - Slots compare `K`'s schema versions at the two versions, and pass the newly visible schema entries' rules through `slotsTouchedByRule`. This is structural. `RSchema.computeDelta` compares resolved values, which do not add up over observes (§7 item 1).
   - The shortcut: when $V_o$ is in the history of `D`, the observe touches nothing.
2. **The proof** (§2.5, "Why the rule is sound") now has four steps:
   1. a read depends only on the live sets its queries match, not on how its walk reaches them;
   2. equal covers mean equal sets, with the history comparison covering the gap above;
   3. changes add up over observes;
   4. every completion's candidates are among those gathered at the union.
3. **Only live entries are compared.** The comparisons apply the read's liveness predicate. This was adopted on 2026-11-03, over comparing every entry, which is also sound but reads no verdicts.
   - The reason is §3.3. If every entry counts, a write that is void in `K` touches. A principal whose authority over a cap row was revoked could then write to the row at a back-dated position: `K` admits the write and voids it at use. An import that brings it would contest a delete in `G`.
   - With live entries only, the §3.3 claim holds as first written: only someone who controls the deleter's authority in `K` can contest.
   - The test now reads `K`'s verdicts. They are complete before `G`'s read returns, as the read's own are.
   - §7 item 5 lists what to re-check before implementing this.
4. **The design in code** (§4, rdb and mvt):
   - the result of the three-valued fold;
   - how the pending observes reach each reader of a bound group;
   - the missing-table rule, which also covers FK reach;
   - where the comparisons sit in `rowState` and `columnValue`;
   - the slot helpers rschema must export;
   - the memo.
   - One refinement: a candidate's match in `existsMatching` is the Kleene `and` of its column reads and its liveness, each touched or not on its own. So a candidate that an untouched column rules out is decided false.
5. **Two prototypes on today's code** (§5), both in the working tree:
   - `[TOUCH]` (`dag/test/touch_cover_tests.ts`) checks steps 2 and 3 of the proof by brute force on random DAGs, for all three indexes, with and without a liveness predicate. In 975 of its comparisons the concurrent cover stayed equal while the concurrent set changed, and the history comparison caught every one.
   - `[TOUCHP]` (`rdb/test/touch_rule_prototype_tests.ts`) checks the rule's decided reads against today's exact reads at every completion. At ten times its default trials (`TOUCHP_SCALE=10`, 4,000 trials) it decided 29,831 reads with no violation, and its 3,402 monotonicity checks and 4,614 shortcut checks held. Comparing every entry instead was sound and decided fewer reads (15,716 against 18,493 on the same trials). Each broken variant produced violations: no slot check 82, no concurrent comparison 30, candidates gathered at `D` only 2,582.

**Questions for the reviewer:**

- Does the four-step proof hold, in particular step 1, which lets the comparisons ignore how a walk reaches its matches?
- Does the live comparison hold up against the re-check list of §7 item 5?
- Is the per-component match in `existsMatching` sound as argued (step 1 applied to each component)?
- Does the guard's answer equal the observe's verdict when written, as Section 1 argues?
- Does the bounded-delta argument of §3.4 cover the touch test's reads? With the live comparison, an entry's verdict in `G` can depend on `K`'s verdicts for entries beyond `D`, in the imported part. The test reads them at `foreignFrom`, as a guard would read them if the import were live. That suggests Theorem 1 covers them as foreign reads, but this response has not checked it.
