# Response to `REFERENCE_FRAME_CRITIQUE_3.md`

This answers [`REFERENCE_FRAME_CRITIQUE_3.md`](./REFERENCE_FRAME_CRITIQUE_3.md) and lists what changed in [`REFERENCE_FRAME.md`](./REFERENCE_FRAME.md) and in the two prototypes since. "Critique 3 §6" refers to the third review, "§3.4" to the plan.

---

## Summary

The review's verdict stands, and all five of its recommendations are adopted. Its claims were checked against the code and hold. Five of its statements need correcting (Section 1), and none of them changes the verdict. One finding goes beyond the review: rdb forbids the kind of memo the plan described, so the touch memo now dies with the computation (Section 2).

| Review item | Response | Change |
| :--- | :--- | :--- |
| §2. The index contract is implicit | Right. `[TOUCH_02]` did not check it, though: it compares covers with each other, never with the topmost entries of a set. | `[TOUCH_03]` checks it directly. Step 2 states the contract, and §4 has dag document it. |
| §3. The live comparison holds | Right. The completeness argument also holds when a solve merges into an outer component. | §7 item 5 records the review. |
| §3. The memo's bound | Right. rdb also forbids an instance-level verdict cache. | The memo dies with the computation (§4 rdb, §2.5 "What it costs"). |
| §4. The per-component match | Right. | None. |
| §5. The guard | Right. One citation is off (Section 1, item 5). | None. |
| §6. The bounded delta | Right. The proposed wording needs two corrections, and the doc changes wait for the design to land. | §3.4: the argument. §6: the wording for A3, the proof of Theorem 1 and guarantee 6. §5: the parity case. |
| §7. The oracle | Right. The runs also varied, because the fixture signed with a random key. | `[TOUCHP]` signs with a fixed key. §5: enumerate, pin a configuration per broken variant, state the monotonicity condition. A comment in the prototype states it too. |
| §8. The cost of the check after a batch | Right. | §4 rdb_runtime, "Only what changed". |
| §8. Reentrancy | Right. Because local commits fire the subscription too, it could become the only trigger. | §4 rdb_runtime, "One check at a time". §7 item 6. |
| §9. Smaller points | Right. | None. |

---

## 1. Corrections

1. **`[TOUCH_02]` does not check the index contract.** It checks that the covers at two versions are equal exactly when the two sets are, so an index that returned every match, not only the topmost ones, would pass it. The new `[TOUCH_03]` compares each cover with the topmost entries of its brute-force set, with and without the predicate, for the three indexes, from the frontier and from other horizons. It checks 12,960 covers, and all pass. In 7,770 of them the cover had to leave out matches below the topmost ones, and in 1,374 it had to see through matches the predicate rejected.
2. **The proposed A3 text is too narrow.** It adds the touch test's reads "for an observe". But any entry of `G` that reads a bound group while one of `G`'s observes of it is pending makes those reads. The delete in walkthrough 3 is a row op. The addition belongs on A3's foreign-views item, for every entry.
3. **The proposed guarantee 6 text says "one at a time".** That misses `existsMatching`, which gathers its candidates at `D` plus every pending version at once. That version is also inside the bound, so the argument holds, but the guarantee must allow it.
4. **The doc changes wait for the design to land.** A3 and guarantee 6 describe today's behavior, and the plan's §6 already lists them for when the design lands. So the argument went into §3.4 now, with step 1 extended to every entry of `G`, and the new wording went into §6.
5. **Two smaller points:**
   - **The seed is fixed, but the runs were not.** `[TOUCHP]` signed its schema with a fresh random key, so every hash, and with them the tie-breaks between concurrent writes, changed from run to run. It now signs with a fixed key. Three runs gave identical counts: 2,910 decided reads, 1,890 undecided and no violation, with 325 monotonicity checks and 438 shortcut checks. The variants gave 6, 3 and 260 violations. `[TOUCHP02]` and `[TOUCHP03]` match the review's counts exactly. `[TOUCHP01]` decided 2,910 reads against the review's 2,907, which is the random key's effect.
   - **Critique 3 §5 cites A5** for "every version `G` observed of `H` is below `H`'s frontier". A5 is about monotone ref-advances. The reason is that an observe is admitted only once the entries it names are present locally, so `H`'s frontier holds them. The conclusion stands.

---

## 2. The memo

The review asked how the touch memo is bounded. The plan kept it on `K`'s group object, across computations. rdb rules that out:
- The comment on `freshVerdictEvaluation` (`src/rtable_group/verdict_evaluation.ts`) says an instance-level `(entry, from)` cache "remains forbidden: the memo dies with the computation".
- [`VOID_SEMANTICS.md`](../VOID_SEMANTICS.md), "The per-computation memo", says a cache of completed verdicts across computations would be sound but is not implemented. It also says a cache of anything not yet completed would be unsound.

So the memo now lives with the computation's evaluation and dies with it, which bounds it. A solve re-runs its members' diagnoses, and with them their touch tests, so the memo still pays within a computation. It holds only the comparisons, which read completed verdicts of `K`. It never holds a read's undecided answer, whose support comes from `G`'s open component. A memo that outlives the computation is the open item of `mvt/WELL_FOUNDED_VERDICTS.md` §6.5. Its value is unmeasured, as with the bound's memo ([`INCREMENTAL_DELTA_BOUND.md`](../INCREMENTAL_DELTA_BOUND.md) §6.2).

---

## 3. The check after a batch

§4 rdb_runtime now has two more bullets, and §5 the tests for them:
- **One check at a time.** The check is single-flight. Events that arrive while it runs are coalesced, and it runs once more afterwards if any arrived.
- **Only what changed.** The live fold at `G`'s frontier depends only on that frontier, since every version it reads is pinned by hash. So a binding needs checking again only when `G`'s frontier or `H`'s has moved, and the check records both per binding. When no local author passed, it also records the local identities, since a new one may pass.

**Open question.** The store's contract says a local commit fires the listener too. So the subscription could replace the list of triggers: local writes, `UPDATE REF`, `ALTER SCHEMA` and catalog updates. The cost: today the check runs inside the statement, and the statement's result reports the observes it wrote (`item.refUpdates` in `rdb_runtime/src/session/adapter.ts`). Run from the subscription, those observes would appear in no result unless the session collects them. This is §7 item 6, for the owner to decide.

---

## 4. Checked against the code

- **The honor flag** of a concurrent delete is read at the delete's own position and `foreignFrom` (`honored` in `killedByConcurrentDelete`, `src/rtable/view.ts`), never at `D`. So step 1 holds for the concurrent query, and the concurrent-deletes mode is not among a read's slots.
- **`K`'s verdicts complete before `G`'s read returns.** That includes the case where `solve` returns early because a member read a frame below its component's root (`mvt/src/verdict_evaluation.ts`): the component then merges into the outer one and its frames stay open.
  - For one of `K`'s components to merge, a member of `K` would have to read an open frame of `G`.
  - It cannot: a bound group exists before `G` and reads only groups below it in the binding DAG (§2.5, "Why they need care").
- **The bounded delta.** A stable observe is one whose published version lies inside the bound group's bound ([`INCREMENTAL_DELTA_BOUND.md`](../INCREMENTAL_DELTA_BOUND.md) §3).
  - An observe concurrent with an entry inside `G`'s bound cannot be strictly after that bound, so Lemma 6 applies to it.
  - Section 6's argument then goes through as the review gives it.
- **The listener contract** (`dag/src/store/dag_store.ts`): notification is at least once, local commits fire listeners, and a change may be delivered twice.
- **The verdict key** carries the horizon (`resolveVerdict` in `src/rtable_group/group.ts`).
- **`refVersionAtOrAbove`** is one `findForkPosition` call (`mvt/src/refs.ts`). The touch test needs no new DAG query. §4 now says so, under dag.
- **The new and changed tests:**
  - `[TOUCH]` and `[TOUCHP]` pass.
  - The two touch test files type-check under the modules' strict test settings.

---

## Questions for the reviewer

- Does §3.4's step 1, now stated for every entry of `G` in the bound, cover the touch test's reads as your Section 6 does for observes?
- Does the A3 wording in §6 say everything the proof of Theorem 1 consumes, including the gate's pending widenings?
- Can anything other than the two frontiers and the local identities change the answer of a binding's check ("Only what changed")?
- Should the memo wait for the cross-computation cache of `WELL_FOUNDED_VERDICTS.md` §6.5, or is a per-computation memo enough to implement with?
