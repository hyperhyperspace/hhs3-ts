# Response to `REFERENCE_FRAME_CRITIQUE.md`

This answers [`REFERENCE_FRAME_CRITIQUE.md`](./REFERENCE_FRAME_CRITIQUE.md) and lists what changed in [`REFERENCE_FRAME.md`](./REFERENCE_FRAME.md) in response. "§2.5" refers to the plan, and "critique §3" to the review. Every finding was checked against the code before it was accepted. Like the review's, the traces below are by hand through `VerdictEvaluation`. The three-valued fold is not implemented, so nothing here was executed except where a test is named.

---

## Summary

The review is right on its four main findings and on every smaller point, with one exception. Its claim that a too-coarse touch test always fails loudly holds for the loop it traced, but not in general. Section 2 gives a counterexample, which is also the configuration that the gap reaches most easily.

| Review item | Response | Plan change |
| :--- | :--- | :--- |
| 1. No negation in the grammar | The premise is right. The conclusion, that the engine always throws, is not (Section 2). | §2.5: the example uses `auth.allowed`, and "Nothing decided is retracted" gives both outcomes. §5: the test. |
| 2. `authz_suggest` passes the observer's position | Right, confirmed in the code. | §4: named options for `evaluateObserveGate`, and the fix in rdb_runtime. §5: a test. |
| 3.1. `deployScope` ignores table maps | Right. | §4, rdb_lang. |
| 3.2. Where the §2.2 rules are checked | Right. | §4, rdb. |
| 3.3. No trigger for re-observing after a collapse | Right. The plan now picks a policy (Section 4, item 1). | §4, rdb_runtime. §5: tests. |
| 3.4. `authz_suggest` cannot report a collapse | Right. | §4: "Reporting a contested observe". |
| 3.5. `extractForeignDeps` comment | Right. | §4, rdb. |
| 3.6. The `view.ts` change is a prerequisite | Right. | §4: "Prerequisite". |
| 4. Docs missing from §6 | Right, with a note on `rpack/IMPLEMENTATION.md` (Section 4, item 4). | §6. |
| Smaller points (critique §7) | All right. | §2.3, §2.5, §3.4, §4. |
| The admission detail after the collapse (critique §2) | Right, and it also corrects walkthrough 3 (Section 3). | §2.5 walkthrough 3, §3.3, §5. |

---

## 1. Findings accepted as written

- **No negation.** `Predicate` has `true`, `false`, `exists`, `cmp`, `like`, `and` and `or` (`rschema/payload.ts:248-255`). `lowerRestrictionPredicate` throws on `NOT` (`rdb_lang/src/compile/query.ts:170`), and `predicates.ts` says that every rule is monotone in its reads.
- **`authz_suggest`.** `bindUpdateRef` resolves `at` against the observer group and `version` against the bound group (`rdb_lang/src/bind/bind.ts:677-678`). Both `update-ref` branches pass `bound.at` as `refAt` and `refFrom` (`rdb_runtime/src/session/authz_suggest.ts:284-285` and `:330-332`), and `evaluateObserveGate` builds the gate's views on the observed group at those versions. `ref_auto_update` passes `targetVersion`, which is correct.
- **`deployScope`** resolves `alias.t` through `boundViews` and `getTable(table)`, with no table map (`rdb_lang/src/compile/catalog.ts:210-219`).
- **Validators.** `rtable_group/validate.ts` and `rcatalog/validate.ts` describe themselves as format-level. Rule 1 needs the pinned schema, so it goes in `validateCreate` and `groupViewReason`.
- **Collapse detection.** Nothing in `ref_auto_update` reads verdicts. In addition, `isAlreadyObserved` compares the target with `resolveRefVersion`, which is the geometric fold (`resolveObservedForeignVersion` passes no liveness). So a collapsed observe still counts as imported, and its version is never imported again.
- **Reporting.** `isAuthRelatedFailure` matches `VALIDATION_REJECTED` diagnostics only. The places that print void reasons go through `formatOpVoidDetail`, which prints `undecided-cycle` as "undecided dependency cycle (denied)": `LOG` (`rdb_lang/src/exec/history.ts`), the REPL's delta output and rpack's staging.
- **§6.** `VOID_SEMANTICS.md` §2 (lines 62-69) and the "Where it lives" paragraph of §5.5, `rpack/IMPLEMENTATION.md` (lines 436-446), the code comments listed, and the two decision records.
- **Smaller points.** `candidateMatch` calls `columnValue` on the foreign view. The delta visitor uses `explainEntryVoided` through `horizonVerdicts` (`rtable_group/delta.ts:293-301`). Admission reads other bindings through `resolveSchemaForeignTableView`, which passes `filterVoided = false` (`validate_ops.ts:365`). `diagnoseObserve` already returns an undecided gate as undecided (`group.ts:1201`).

---

## 2. A too-early decision does not always fail loudly

Critique §3 argues that, with positive atoms only, a read decided too early cannot go unnoticed: the solve either throws `became possibly live again` or fails `finalVerdicts`. The trace it gives for the two-member loop of §3.3 is right. The general claim is not.

Rules are monotone in their reads, but verdicts are not. A live delete kills a row whatever its rule says, so every delete is a negative edge between verdicts. A loop with an odd number of negative edges has no consistent assignment, and a too-early decision is contradicted later, so the solve throws. A loop with an even number has two consistent assignments, and a too-early decision can pick one of them. That result is consistent, so neither assertion fires.

### The counterexample

The setup is the following.

- `K` has `allowed(user pub, active pub default false)`, with Bob's row `b` written with `active: false`. `K` then deploys `δ`, which drops `active` and re-adds it, pub, with default `true`. After `δ`, `b`'s write belongs to the dead incarnation, and `b` matches `active = true` through the default.
- `G` binds `auth => K` with `ALLOW UPDATE REF auth IF $author IN admins`. It keeps `admins`, with Carol's row, and `moderators`, with Mallory's row `m`. Both tables honor concurrent deletes. The delete rule of `moderators` is `EXISTS auth.allowed WHERE user = $author AND active = true`. The delete rule of `admins` is `$author IN moderators`.
- Carol imports `δ` with `z`. Bob, having seen `z`, deletes `m` with `d`. Mallory, having seen neither, deletes Carol's `admins` row with `e`. So `z` is in `d`'s history, and `e` is concurrent with both.
- All three are admitted. Carol is an admin at `z`'s position. The geometric fold at `d`'s position holds `δ`, so Bob is allowed. `m` is live at `e`'s position.

At use, from a horizon that holds all three:

- `z`'s gate reads Carol's row at $(\{z\}, \text{from})$, where `e` is a concurrent delete.
- `e`'s rule reads `m`, where `d` is a concurrent delete.
- `d`'s rule reads `auth.allowed` through the fold. `z` is in `d`'s history with its verdict open, so $U = \{z\}$.

There are two consistent assignments. In one, `z` and `d` are live and `e` is void. In the other, `z` and `d` are void and `e` is live. The well-founded model leaves all three undecided. With the rule the plan specifies, `z` touches `d`'s read through `active`'s slot, so `d` is undecided. The support graph is the cycle `z → e → d → z`, and all three collapse.

Now the same solve with touches applied per candidate. Under `D`'s schema, `b` is not a candidate, so `d`'s read is false whenever `z` is pending.

1. **Round 1, lower step** (nothing live, all possible). `d` reads false and is void. `e` reads `d` undecided, so it is undecided. `z` reads `e` undecided, so it is undecided. Nothing is marked live.
2. **Round 1, upper step**, rebuilding from the empty set:
   - `d` reads `z` as void, so the read is false and `d` is not added.
   - `e` reads `d` as void, so `m` is live and `e` is added.
   - `z` reads `e` as undecided, so `z` is added.
   - `d` is tried again. It reads `z` as undecided, so the read is still false and `d` is not added.

   The possibly-live set shrinks to `{z, e}`.
3. **Round 2, lower step.** `d` is now outside the possibly-live set, so others read it as void. `e` is live and is marked. `z` sees Carol's row killed by `e`, so it is void. `d` still reads false.
4. **Round 2, upper step.** Starting from `{e}`, nothing is added. The possibly-live set is `{e}`, and the next round changes nothing.
5. **`finalVerdicts`**, with all values decided:
   - `d` reads `z` as void, so it reads at `D`, gets false and is void. That agrees.
   - `e` is live, and `z` is void. Both agree.

The solve ends with `e` live, and `d` and `z` void (`observe-gate`). Carol has lost her admin row, and nothing reports a problem.

### Why the even case is the easier one to reach

The gap only arises when an import makes a read true: a candidate that `D`'s schema misses, matched in the completion where the import is live. An op that relies on that read is admitted only at a position that has seen the import, because admission needs the match. So the op comes after the import, and it cannot be a concurrent delete that the import's gate reads.

In this example the op reaches `z`'s gate through `e`, a delete concurrent with `z`. It reaches `e` by being itself a delete, which `e`'s read honors concurrently. That makes two deletes, an even loop.

The two-member loop of critique §3 needs one more piece to exist. Alice's delete has to be concurrent with `z` and still be admitted. So the grant has to reach the delete's position through an earlier import of `δ` that is void at use. With that piece, the review's trace holds as written.

The plan now states both outcomes in §2.5, under "Nothing decided is retracted". On an odd loop the solve throws, on every replica that reads that history. On an even loop it settles silently. The read-level rule is needed for both.

### On stating that rules are monotone in their reads

The plan now says it. But the touch argument does not rest on it. Kleene `not` keeps a read's three properties, since it only swaps true and false and keeps undecided. What the absence of negation does not give is monotone verdicts, and the parity of a loop comes from its deletes and from which way an import moves its reads.

---

## 3. The admission detail corrects walkthrough 3

Critique §2 notes that after the collapse in §3.3, admission at the merge reads `auth.caps` through the geometric fold. That fold holds the revoke whatever the import's verdict. So the attacker can answer only from a position that has not seen the import. This is right, and it is now in §3.3, in the paragraph "Every answer is back-dated".

The same rule contradicts the plan's walkthrough 3. The plan said that if Bob deletes Alice's row again from the merge, "his delete reads `K` without `y` and is live". At admission, though, Bob's delete reads `auth.caps` through the geometric fold. That fold holds `y`'s version, so Bob's cap is revoked and the delete is refused.

The two sides are therefore not symmetric. The importer's side settles the race from the merge. The other side can only keep answering with back-dated ops. Walkthrough 3, §3.3 and the two §5 tests now say so.

---

## 4. Choices for the reviewer to evaluate

1. **Re-observing after a void verdict** (§4, rdb_runtime). The plan now specifies:
   - **When:** an observe of `G` that is void at the frontier, by collapse or otherwise, is re-observed at its version from the frontier, by any replica whose author passes the gate.
   - **Up-to-date check:** `isAlreadyObserved` compares against the fold of live observes at the frontier, instead of the geometric ref.
   - **Triggers:** the check runs when a bound group changes, as today, and also after a sync batch that changes `G`, since such a batch can void an observe after the fact.

   Two points are open to challenge:
   - **Any replica may retry, not only the author's.** Restricting retries to the author's replica would stall `G`'s view while that author is offline. Duplicate observes of one version are harmless.
   - **Observes void with `observe-gate` are retried too, not only collapses.** `K`'s entries are judged by `K`, so the version itself is sound. The void verdict was about its author, and a replica that passes the gate can import the same version legitimately.
2. **The signature.** The public `evaluateObserveGate` takes named options `{ opPos, imported }`, as the review suggests. `validateObserveGate` passes `at` and `newRefVersion`. `authz_suggest` passes `bound.at` and `bound.version`.
3. **The reporting surface** is the callers of `formatOpVoidDetail`, not `authz_suggest`. For an observe, they print `undecided-cycle` as contested, with the advice to observe again from the merge.
4. **`rpack/IMPLEMENTATION.md` is untracked**, yet the committed `rpack/README.md` links to it three times (lines 11, 290 and 296). Those links are broken in a clean checkout today, whatever this plan does. The plan lists the file in §6 without a link until it is committed.

---

## 5. Outside the plan

- The `view.ts` change of critique §5.6 is in the working tree, uncommitted. It comes with `[QRY10]` in `rdb/test/rtable_query_tests.ts`.
  - Before the change, `EXISTS` and `findRowIds` returned different answers depending on which searched field came first. `query()`'s pushdown missed rows that rely on a default.
  - The full rdb suite passes with the change: 370 tests, no failures.
  - Modules that depend on rdb test against its built `dist`, which has not been rebuilt.
- `[QRY10]`'s comment said that a missed candidate "fails open under `NOT EXISTS`". The grammar rules that out, and the comment now gives the real effect. A missed match refuses an op whose rule needs it. For a delete, the removal silently does not happen.

---

## 6. What changed in `REFERENCE_FRAME.md`

- **§2.3.** A second alias of `H` is the binding being gated, read at $(\text{refAt}, \text{refFrom})$ through its own table map. Admission reads other bindings through the geometric fold. So a grant imported only by a void observe can pass admission and fail at use, and an import counts at admission whatever its verdict.
- **§2.5.**
  - The `columnValue` sentence now names `existsMatching`'s own calls.
  - The example uses `EXISTS auth.allowed WHERE user = $author AND active = true`.
  - "Nothing decided is retracted" gives the even and odd outcomes, with the examples of Section 2, and contrasts rules that are monotone in their reads with verdicts that are not.
  - Walkthrough 3: a delete by Bob from the merge is refused, and only a back-dated one contests Alice's imports again.
- **§3.3.** A new paragraph, "Every answer is back-dated".
- **§3.4.** `explainEntryVoided` through `horizonVerdicts`, in place of `isEntryVoided`.
- **§4.**
  - Where each rule of §2.2 is checked, and that `exclude` is not needed.
  - `diagnoseObserve` already returns undecided gates as undecided.
  - Named options for `evaluateObserveGate`, which `validateObserveGate` passes.
  - The `extractForeignDeps` comment, and the `candidateRowIds` prerequisite.
  - `deployScope` translates through table maps.
  - The re-observe policy and its triggers.
  - The `authz_suggest` fix.
  - A new block, "Reporting a contested observe".
- **§5.**
  - The default-change test covers the even and odd loops.
  - The walkthrough 3 and blocking tests cover refusal at admission from the merge.
  - New rdb_runtime tests for re-observing, including after a later sync and from a replica that did not write the observe, and for the suggested author of a refused `UPDATE REF`.
- **§6.**
  - `VOID_SEMANTICS.md` §2, and "Where it lives" in §5.5.
  - `rpack/IMPLEMENTATION.md`.
  - The code comments that describe the observed frame or the acyclicity invariant.
  - The two decision records, which keep their text.
