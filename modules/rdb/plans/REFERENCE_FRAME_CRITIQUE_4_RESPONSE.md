# Response to `REFERENCE_FRAME_CRITIQUE_4.md`

This answers [`REFERENCE_FRAME_CRITIQUE_4.md`](./REFERENCE_FRAME_CRITIQUE_4.md). "Critique 4 §5" refers to the fourth review, "§2.3" to [`REFERENCE_FRAME.md`](./REFERENCE_FRAME.md).

The plan has not changed yet. The fix is a design decision, and this response argues for the option the review did not recommend. The plan changes of Section 5 wait for agreement.

> **Note (2026-11-04):** the changes of Section 5 are now in the plan, together with the three additions in the reviewer's note on critique 4 §5. The plan's Section 8 lists them.

---

## Summary

The diagnosis is right, and the code confirms it (Section 1). The recommended fix, the exact guard, has two consequences the review did not weigh: one of its tests contradicts admission, and it keeps an honest importer from delivering a revocation of her own authority (Section 2). They reverse the recommendation. This response proposes option 1, stated as a rule rather than a heuristic (Section 3). The review's other recommendations are right (Section 4).

| Review item | Response | Plan change, pending agreement |
| :--- | :--- | :--- |
| §2–3. The guard is not the observe's verdict | Right, and confirmed in the code. | §4 rdb_runtime: the guard paragraph is rewritten. |
| §3. The claim in response 2 §1 | Wrong, as the review says. | Response 2 §1 now carries a dated note. |
| §4. What the problem does not break | Right, including termination. | None. |
| §4. "Why the collapse does not fail open" | Right. | §2.5: the last sentence promises only what history reads give. |
| §5, option 2. The exact guard | Not adopted (Section 2). | None. |
| §5, option 1. The guard as written | Adopted, as a rule: the guard answers whether the author is authorized before the import (Section 3). | §4 rdb_runtime; §4 rdb, `evaluateObserveGate`; §5, two tests. |
| §6, items 1, 2 and 4 | Agreed. | §6: the A3 wording drops the pending set. |
| §6, item 3. Local presence | Right. | §4 rdb_runtime, "Only what changed". |
| §7, item 5. Dated notes | Done for response 2 §1. The review added the one in critique 3 §5. | None. |

---

## 1. The diagnosis, checked

- **The fold at the horizon counts every observe.**
  - `resolveForeignTableViewIn` (`src/rtable_group/group.ts`) computes `foreignFrom` with `resolveRefVersionAtPosition(dag, groupId, from, from)`, which takes no liveness filter.
  - `diagnoseObserve` computes `refFrom` the same way.
  - A row op's reads of a bound group pass its horizon through (`resolveSchemaForeignTableViewIn(evaluation, group, table, opPos, from, true)`).
- **So a new observe can change older verdicts.** Once an observe `o'` of `K` is in `G`'s history, the fold of `K` at every later horizon holds the version `V'` it imports. A row written before `o'` that reads `K` can then change verdict in two ways:
  - a delete in `V'` that is concurrent with the version the row read kills a row the row relied on;
  - a deploy in `V'` changes the honor flag of such a delete. The flag is read at the delete's own position and `foreignFrom`, and a concurrent deploy changes it.
- **The guard misses this.** It is evaluated before `o'` exists, at a horizon whose fold of `K` lacks `V'`. So the claim made in §4 rdb_runtime, in response 2 §1 and in critique 3 §5 is wrong.
- **Only the guard depends on the claim:** the guard paragraph of §4 rdb_runtime and the tests written for it. The three-valued fold, the touch rule and the bounded delta each read at one horizon, and within one computation `foreignFrom` already holds every pending version (§2.5).

---

## 2. Why not the exact guard

1. **Its second test cannot pass.** The test is: "one whose deploy unhonors the delete that kept the author out writes a live one".
   - Admission reads `G`'s tables at (`at`, `at`), the read a row op's admission makes (§2.3), so it sees the author's row as `G` saw it before the import.
   - In the review's example, the delete that kept the author out is honored at `at`. So her row is dead, and admission refuses the observe, whatever an exact guard says.
   - `observe` refuses an author who fails admission (§4 rdb_runtime, "Admission still applies"), so the runtime writes nothing.
   - Making admission exact as well, at the observe's own horizon, would read the import's own effect at admission. An author could then import her own re-authorization. And an import that revokes its author would be refused, which is the next point.
2. **It withholds a revocation of the importer.** Take the review's first example. Alice imports `V'`, which holds a delete of her cap that is concurrent with the version her admin row read.
   - **With the guard as written,** the guard passes, and the observe is written and is void at its own horizon. Its version is in the fold of every later horizon, void or not (A3). So the delete kills Alice's cap in the reads of her admin row, and the row is dead in `G`: the revocation lands.
   - **With the exact guard,** Alice never writes the import. Unless another local author passes, `V'` never reaches `G` from her replica. If she is the only principal who can import, `G` never learns of the revocation, and she keeps her authority in `G`.
   - A dishonest Alice would withhold the import anyway, and no runtime rule changes that. The difference matters for honest importers, the case the runtime exists for. Writing the void import is how an honest replica delivers news that revokes its own author.

---

## 3. The proposal: option 1, as a rule

The guard asks the question admission asks: **is the author authorized before the import?**
- Admission answers it through the geometric fold, and the guard through the live fold.
- Both read the gated binding's own atoms at the imported version (§2.3).
- The observe's verdict asks the question at the observe's own horizon, which also holds the import. So the guard and the verdict differ only by what the import itself brings through the horizon.

The import can change the gate's answer in one direction or the other, so every difference is one of the first two cases below:
- **An import that revokes its own author**: authorized before the import, not with it.
  - It passes both checks, is written once, and is void. Its version still reaches `G`'s horizon, so the revocation lands.
  - The next check reads at a horizon that holds that version, where the guard reads what the void observe's verdict read, so it writes nothing more.
  - A replica writes at most one such observe per version of `K` (critique 4 §4, "Termination").
- **An import that would restore its own author**: not authorized before the import, authorized with it.
  - The guard says false, and nothing is written. In the review's example admission refuses it too.
  - Another authorized author must import it: no author imports her own re-authorization.
- **Every other import**: the guard's answer is the observe's verdict.

**The guard is exact for a version already in the fold.** Two cases matter: a re-import after a collapse, and a re-import after a void observe of the same version (critique 4 §4, "Collapses"). The retry argument of §4 rdb_runtime rests on these cases, and it stands.

So the one rule of §4 rdb_runtime stands with the guard as written. Only its rationale changes. Two refinements:
- **The gated binding's own atoms are read at the imported version in the guard,** as they are at admission and at use. This is the part of critique 3 §5 that survives. The plan's description of `evaluateObserveGate` says how other bindings are read, and it should say this too.
- **Author selection may prefer an author whose observe will be live.** When several local authors pass, the review's exact evaluation (the horizon override of critique 4 §5) could rank them. A revoked author then writes no void observe when another author would write a live one. This saves one observe, it is not needed for correctness, and it can wait.

Two tests for §5:
- **An import that holds a delete concurrent with its author's authorization** is written once and is void with `observe-gate`. The author's row is dead in `G` afterwards, and later checks write nothing. A second local author whose authority the delete leaves alone then writes a live re-import.
- **An import that would restore its author,** through a deploy that unhonors the delete that kept her out, is refused at admission, and the runtime writes nothing. Another authorized author's import of the same version is live.

---

## 4. The other recommendations

- **"Why the collapse does not fail open."** Right. Its last sentence holds for history reads only.
  - A barrier of `K` in the void import's version that is concurrent with `D` still kills rows in `G`'s reads at `D`, when honored. That is today's deny direction (A3).
  - Walkthrough 3 is right, because the revoke of Bob's cap is after `D`.
  - The sentence will promise only what the fold gives.
- **Local presence.** Right. A check that throws because a bound group is missing locally is not recorded as done, since the same frontiers answer differently once the group arrives.
- **The answers to response 3's questions.** Agreed. Following the answer to the second question, the A3 wording of §6 drops the pending set.
  - A3 lists the reads: at `foreignAt` plus the version of each observe in the fold, alone, and plus all of them together.
  - The proof's text says that these reads are made only while those observes are undecided (A2).
- **Dated notes.** Response 2 §1 now has one. The review added the note in critique 3 §5.

---

## 5. Plan changes, pending agreement

- §4 rdb_runtime, "The at-use guard": rewritten as in Section 3, with the termination argument of critique 4 §4.
- §4 rdb, `evaluateObserveGate`: the gated binding's own atoms are read at the imported version under both folds.
- §4 rdb_runtime, "Only what changed": a check that throws is not recorded.
- §2.5, "Why the collapse does not fail open": the last sentence.
- §5: the two tests of Section 3. The two existing tests of the guard stay: an author who passes admission only writes no observe, and neither does an author refused at admission.
- §6: the A3 wording.

---

## Questions for the reviewer

- **Is the list of inputs complete?** This response relies on critique 4 §2's list of what differs between the frontier's horizon and the new observe's. Is the import's own effect through the horizon the only difference between the guard and the verdict?
- **Is the void import acceptable?** Is delivering a self-revoking import as a void observe acceptable as intended behavior, or should the design deliver such news some other way?
- **Is admission's order intended?** Admission reads `G`'s tables before the import (§2.3), so no author can import her own re-authorization. Is that intended? This response relies on it.
