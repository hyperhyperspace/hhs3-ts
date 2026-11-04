# Second critique of `REFERENCE_FRAME.md`

This reviews [`REFERENCE_FRAME.md`](./REFERENCE_FRAME.md) as revised on 2026-11-03 in answer to [`REFERENCE_FRAME_CRITIQUE.md`](./REFERENCE_FRAME_CRITIQUE.md) and [`REFERENCE_FRAME_CRITIQUE_RESPONSE.md`](./REFERENCE_FRAME_CRITIQUE_RESPONSE.md). "§2.5" refers to the plan, "critique §3" to the first review and "response §2" to the response. Line numbers refer to the working tree on the day of the review and will drift.

---

## Verdict

The revision holds up. The response is right on its one disagreement with the first review: a read decided too early does not always fail loudly. On an even loop the engine settles it silently, and the first review's claim to the contrary is withdrawn (Section 2). Every change the response lists is in the plan, and each matches the code (Section 3).

**Verified as correct:**

- The even-loop counterexample of response §2, traced through `VerdictEvaluation.fixpoint` and `finalVerdicts`: the solve ends with `e` live and `z` and `d` void, and no assertion fires.
- The odd-loop outcome in §2.5: the delete decided too early is marked live once `z` is, and the monotonicity assertion throws.
- The correction to walkthrough 3 and the new paragraph of §3.3: a row op's admission reads other bindings through the geometric fold, so a delete from the merge is refused.
- The §4 items added for the first review: the `authz_suggest` fix, where each §2.2 rule is checked, the `deployScope` translation, the reporting surface and the `extractForeignDeps` comment.
- The prerequisite: the rdb suite passes with the uncommitted `view.ts` change, `[QRY10]` included.

**Main problems**, both in the new rdb_runtime policy of §4:

1. **The retry can loop forever.** An observe that passes admission and is void at use is retried on every trigger, and each retry is a new void entry (Section 4).
2. **The triggers do not exist as described.** Today the check runs only after a local row write, and rdb_runtime has no sync hook. The plan should name the hook and the statements that trigger it (Section 5).

Neither changes the design. Both should be fixed in §4 before implementation starts.

---

## 1. How the claims were checked

I read the revised plan and the response against:

- mvt: `verdict_evaluation.ts` in full, for the traces of Section 2;
- rdb: `rtable_group/group.ts` (the foreign view resolution, the observe gate, `resolveObservedForeignVersion`), `rtable_group/validate_ops.ts` (`validateRowOpRestrictionAt`), `rtable_group/view.ts` (`resolveRefVersion`), `rtable_group/op_void.ts`, and the `[QRY10]` diff in `test/rtable_query_tests.ts`;
- rdb_runtime: `session/ref_auto_update.ts` in full, `session/adapter.ts` (the trigger), `session/authz_suggest.ts`, `workspace/workspace.ts`, and the test list;
- rdb_lang: `bind/bind.ts` (`BoundStatement`, `bindUpdateRef`);
- the callers of `formatOpVoidDetail` in rdb_lang, rdb_repl and rdb_tools, and the re-exports in `rdb_tools/src/session/authz_suggest.ts`;
- replica, sync and dag: `replica.ts` (the subscriptions), `synchronizer.ts` and `dag.ts` (the growth listener), for where a sync hook could attach;
- `rpack/README.md`, for the links the response mentions.

The even and odd loops were traced by hand through the engine, as in the first review. This time the rdb suite was also run: 370 tests, all passing, `[QRY10]` among them.

---

## 2. The first critique's claim on silent settlement is withdrawn

Critique §3 said that with positive atoms only, a too-coarse touch test cannot go unnoticed. Response §2 answers that rules are monotone in their reads but verdicts are not, so a loop with an even number of deletes has two consistent assignments, and a too-early decision can pick one. That is right, and the counterexample runs as the response says. The engine's loop (`verdict_evaluation.ts:273-317`):

- The lower step walks every open member and skips only the live ones (`:282-283`). Members in `possible` but not live are read as undecided, members outside `possible` as void (`interpret`, `:364`). A member whose diagnosis is live joins `live`; a false or undecided one is left as it is.
- The upper step starts `next` from `live` (`:295`), walks every member not yet in `next` (`:298-299`), reads unadded members as void and added ones as undecided, and iterates until nothing changes.
- The only checks are that `next` is within the previous `possible` (`:312`) and, once everything is decided, that each member's final diagnosis agrees with its well-founded value (`:355`).

With the per-candidate rule, round 1's upper step leaves `d` out, because it reads false whether `z` is void or undecided. Round 2's lower step reads `d` as void, marks `e` live and leaves `z` void. The possibly-live set shrinks to `{e}` and the solve converges. `finalVerdicts` then reads `d` at `D`, gets false, and agrees with `d` being void. Nothing throws. The outcome is one of the two consistent assignments, which is why the assertions pass.

The odd loop of §2.5 does throw, for the reason the plan gives. Once `z` is live, the lower step of the next round re-evaluates the delete, which is outside `possible` but still open, reads `z` as live and marks it live. The assertion after the upper step (`:312`) then finds it in `next` but not in `possible`.

The consequence for implementation is that there is no safety net on even loops. The three properties of the touch rule are what keep the fold sound, and the completions oracle of §5 is the only check that exercises them. That oracle should stay in the suite, not be a one-off, and its configurations should include even loops explicitly, since those are where a read-level bug hides.

---

## 3. The revised text matches the code

- **Walkthrough 3 and §3.3.** `validateRowOpRestrictionAt` (`validate_ops.ts:353-369`) reads other bindings through `resolveSchemaForeignTableView`, which passes `filterVoided = false` (`group.ts:593-597`). A delete from the merge reads `auth.caps` with `y`'s version in the fold, whatever `y`'s verdict, and is refused. The asymmetry §3.3 now describes follows.
- **§2.3.** `[OBSGATE10]` (`test/rtable_observe_gate_tests.ts:453`) has two aliases of one group, each gated. The admission paragraph matches the call at `validate_ops.ts:365`.
- **§4, `authz_suggest`.** `bindUpdateRef` builds `version` against the bound group and `at` against the observer (`bind.ts:683`). Both `update-ref` branches still pass `at, at` (`authz_suggest.ts:284-285`, `:330-332`). The plan now says what to pass. `rdb_tools` re-exports both helpers (`rdb_tools/src/session/authz_suggest.ts:5`, `:20`).
- **§4, reporting.** `formatOpVoidDetail` prints `undecided-cycle` as "undecided dependency cycle (denied)" (`op_void.ts:33-34`). Its callers are `rdb_lang/src/exec/history.ts:32`, `rdb_repl/src/format/delta.ts:116` and `rdb_tools/src/rpack/stage.ts:437`, as §4 lists. `isAuthRelatedFailure` matches `VALIDATION_REJECTED` only (`authz_suggest.ts:239-241`).
- **§4, the up-to-date check.** `isAlreadyObserved` (`ref_auto_update.ts:113-122`) calls the view's `resolveRefVersion`, which is `resolveObservedForeignVersion` (`rtable_group/view.ts:55-63`), which passes no liveness (`group.ts:723-727`). A void observe counts, so its version is never imported again. `observeWithResolvedAuthor` passes the target version as both anchors (`ref_auto_update.ts:141-142`).
- **§4, the fold.** `resolveForeignTableViewIn` is where the Layer 2 fold is computed with the boolean liveness (`group.ts:617-636`), and `resolveObserveGateRefAt` is Layer 1 (`:692-697`). The plan names the right functions.
- **§6.** `rpack/README.md` links to `IMPLEMENTATION.md` at lines 11, 290 and 296.
- **The prerequisite.** `[QRY10]` is in the suite and passes. One correction to response §5: it says that modules depending on rdb test against a `dist` that has not been rebuilt. It has been. `dist/rtable/view.js` is newer than the source and contains the new candidate code.

---

## 4. The retry can loop forever

§4 (rdb_runtime) retries an observe that is void at the frontier whenever the replica's author passes the gate, and response §4 argues for retrying `observe-gate` voids as well as collapses. "Passes the gate" is admission, through `resolveObserveAuthor` and `evaluateObserveGate`. §2.3 says that admission reads other bindings through the geometric fold and use through the live one, so a grant imported only by a void observe passes admission and fails at use. Put the two together for the replica's own author:

1. `G` gates `target` by `EXISTS auth.caps WHERE grantee = $author`, and gates `auth` by `$author IN admins`.
2. Mallory, not an admin, observes `K` at a version where Dave holds a cap. Her observe `w` is void with `observe-gate`.
3. Dave's replica checks `target`. The gate passes at admission, since the geometric fold counts `w`. Dave's observe is written, and it is void at use from the moment it is written, since the live fold sees through `w`.
4. On the next trigger the check finds Dave's observe void at the frontier, Dave passes admission again, and a second void observe is written. And so on, one entry per trigger, until an admin imports `w`'s version or a later one.

Today nothing loops, because a void observe counts as imported and a failed admission writes nothing. The loop is new with the retry.

**Recommendation.** Guard the retry with the gate evaluated at use, not at admission: for the local author, at the frontier as both position and horizon, through the live fold. At the frontier nothing is concurrent and nothing is pending, so the answer is decided, and it is the verdict the new observe will have when written. If it is false, do not retry. Restricting retries to collapses would also close the loop, but it loses the `observe-gate` retry the response argues for, which is sound once guarded. Add the loop to the §5 tests: an observe that passes admission and is void at use is not retried.

---

## 5. The triggers do not exist as described

§4 says the check runs "when a bound group changes, as today, and also after a sync batch that changes `G`". Today is narrower than that, and the sync part is new infrastructure:

- The only call to `propagateRefUpdates` is in the session adapter, after a local statement (`adapter.ts:221-223`). `extractRefUpdateTrigger` (`ref_auto_update.ts:41-51`) returns a trigger for an insert, update, delete or bundle, and nothing for `UPDATE REF`, `ALTER SCHEMA` or a catalog update. So a bound group that changes by sync, by a deploy, or by advancing one of its own refs triggers nothing.
- rdb_runtime subscribes to nothing but new roots (`workspace.ts:56`). The replica offers new-root and new-object subscriptions only (`replica.ts:96-109`). What the synchronizer uses is the DAG's growth listener (`dag.ts:46-49`, `synchronizer.ts:252`), at-least-once per growth; objects can also be subscribed to.

The plan should say:

1. **Where the sync hook attaches.** The DAG growth listener on the group roots is the natural point. It fires per growth, not per batch, so the check should coalesce the events of a batch, or run once the synchronizer is idle.
2. **That `UPDATE REF` is a trigger**, with the observer as the source. "After `G` observes `K`, retry `G`'s observes of the bindings whose gates read `K`" needs it, and today a manual `UPDATE REF` propagates to nothing.
3. **Which batches count.** A batch in `K` can void an observe of `H` through §3.2's second case, a revoke concurrent with it, and a batch in a group below `K` can flip a cap's verdict in `K`. The simplest rule is to re-check every observer root in the workspace after any batch, in gate-graph order, and let the per-binding comparison decide.
4. **One rule instead of two cases.** The new up-to-date check compares the live fold with the bound group's frontier, so the action after a void observe is the one taken today: observe the bound group's frontier. "Observe its version again" reads as a special path and should say the frontier. Both retry cases then reduce to one sentence: a replica whose author passes the gate at use keeps `G`'s live view of each binding at the bound group's frontier.

Two consequences for §5 and §3.3:

- **The sync tests have no harness in rdb_runtime.** Its 22 tests never sync two replicas. rdb's `test/rdb_sync_tests.ts` and `test/rdb_full_sync_tests.ts` have one to borrow.
- **Amplification.** In the stalemate of §3.3, one back-dated delete makes every replica whose author passes the gate retry after the next batch. It is bounded by the attacker's activity and covered by the mitigation already there, but §3.3 could say so in a sentence, and implementers should expect duplicate observes of one version.

---

## 6. Smaller points

- §2.5, "Nothing decided is retracted": the lower step reads members already live as live, not as undecided. "Every member that is still possibly live and not yet live" is exact.
- The same paragraph says the upper step throws on the odd loop. The lower step is what marks the delete live; the assertion after the upper step throws. The summary is fine; the mechanism is worth knowing when the error appears in a test.
- `rpack/IMPLEMENTATION.md` is untracked while the committed README links to it three times, as the response notes. That is outside the plan, but it should be committed or the links fixed, whichever comes first.
- Critique §3 still states the withdrawn claim. The response records the correction; a dated line at the top of that section pointing to response §2 and to this note would let the file stand alone.

---

## 7. Recommendations

1. Add the at-use guard to the retry in §4, and a test for an observe that passes admission and is void at use (Section 4).
2. Name the sync hook, add `UPDATE REF` to the triggers, say which batches count, and state the retry as one rule over the bound group's frontier (Section 5).
3. Point the sync tests of §5 at rdb's harness (Section 5).
4. Keep the completions oracle in the suite and include even loops in its configurations (Section 2).
5. Commit or unlink `rpack/IMPLEMENTATION.md`, and add the dated note to critique §3 (Section 6).

With the first two, the plan is ready to implement.
