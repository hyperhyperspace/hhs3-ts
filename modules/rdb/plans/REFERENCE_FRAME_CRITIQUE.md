# Critique of `REFERENCE_FRAME.md`

This is a review of [`REFERENCE_FRAME.md`](./REFERENCE_FRAME.md) in its revision of 2026-11-02, made as a final check before implementation. Section numbers like "§2.5" refer to that document unless stated otherwise. Line numbers refer to the working tree on the day of the review and will drift.

---

## Verdict

The design holds up. The three-valued fold of §2.5 is exact, sound and monotone for the reasons the document gives, the engine needs no change, and the bounded-delta argument of §3.4 survives against the lemmas of [`INCREMENTAL_DELTA_BOUND.md`](../INCREMENTAL_DELTA_BOUND.md). Every file, symbol, test id and document section the plan cites exists where it says, and its description of today's code is accurate.

**Verified as correct:**

- The references: every symbol in §4 lives in the file named, `resolveRefVersions` is gone, `slotsTouchedByRule` is exported from `rschema/resolve.ts`, and `[CLANG17]` and `[CAT13]` pin the observed frame as §5 says.
- The account of today's code in §2.1, §2.3 and §2.5: catalog-written references are untranslated, the observed frame skips the void filter, and an undecided observe throws.
- The touch rule's three properties (Section 2 below).
- That a bound group's components complete before the observer's read returns, and that every loop through an observe passes through the observer's rows (§2.5, §3.1).
- Walkthroughs 3, 4 and 5 of §2.5, traced through the engine's fixpoint, and the stalemate of §3.3.
- §3.4: `walkRevisionBound` needs no change, and the text changes it lists are the right ones.

**Main problems:**

1. **The `NOT EXISTS` rule of §2.5 and §5 cannot be written.** The restriction grammar has no negation. The example and the test must be rewritten with a positive atom, and the claim that a too-coarse touch test fails silently is wrong: with positive atoms only, the engine throws.
2. **The rdb_runtime item of §4 would preserve an existing bug.** `authz_suggest` passes the observer's position where the observed group's version belongs. The plan has to say that both are passed under the new signature.
3. **§4 misses the compiler's `ALLOW DEPLOY IF` scope**, which must translate like the gates, and leaves two runtime items without a detection point.
4. **§6 misses `rpack/IMPLEMENTATION.md`**, which documents the superseded design, and `VOID_SEMANTICS.md` §2.

None of these changes the design. All four should be fixed in the plan before implementation starts.

---

## 1. How the claims were checked

I read the plan against:

- rdb: `rtable_group/group.ts`, `rtable_group/validate.ts`, `rtable_group/validate_ops.ts`, `rtable_group/predicates.ts`, `rtable_group/delta.ts`, `rtable_group/op_delta.ts`, `rtable/view.ts` (with its uncommitted change), `rtable/rtable.ts`, `rschema/resolve.ts`, `rschema/payload.ts`, `rcatalog/validate.ts`, `rcatalog/validate_ops.ts`, `rdb/catalog_update.ts`;
- mvt: `refs.ts`, `verdict_evaluation.ts`, `revision_bound.ts`, `delta.ts`;
- rdb_lang: `compile/catalog.ts`, `compile/query.ts`, `bind/bind.ts`;
- rdb_runtime: `session/ref_auto_update.ts`, `session/authz_suggest.ts`;
- the documents it cites: [`VOID_SEMANTICS.md`](../VOID_SEMANTICS.md), [`INCREMENTAL_DELTA_BOUND.md`](../INCREMENTAL_DELTA_BOUND.md), `mvt/WELL_FOUNDED_VERDICTS.md`, `mvt/IDEAL_CONCURRENCY.md`, `mvt/MVT_DEFINITION.md` §8, `std_types/README.md`, `rpack/README.md`, `rpack/IMPLEMENTATION.md`, `rdb_lang/README.md`;
- the tests it names: `[CLANG17]`, `[CAT13]`, `[XGROUP11]`, `[OBSGATE08]`, `[VREAD06]`, `[DBOUND07]`, the `[OBSGATE]` suite, and `test/delta_parity/generate.ts`.

The walkthroughs of §2.5 were traced by hand through `VerdictEvaluation.fixpoint` (the lower and upper steps) and `finalVerdicts`. No probe was run and no test was executed. This is a reading, not a measurement.

---

## 2. Claims that hold up

**Today's code is as described.**

- Catalog-written references are untranslated: `validateDeploy` evaluates `canDeploy` through `resolveForeignTableView` (`validate_ops.ts:671`), `resolveAuthorKey` reads a foreign provider table by its real name (`group.ts:751`), and `evaluateObserveGateIn` reads the observed group's tables and bindings with no void filter (`group.ts:680-686`). `groupViewReason` and `groupIdentityReason` check `canDeploy` atoms and a foreign `idProvider` against the unmapped table (`rcatalog/validate_ops.ts:503` and `:561`).
- `diagnoseObserve` already returns an undecided gate as undecided (`group.ts:1201`). Only `observeVerdictLive` throws (`group.ts:653`), so "the throw goes" is the whole engine-facing change on that path.
- `isVoidCheckable` accepts ref-advances (`op_delta.ts:52-56`), so the delta visitors already count a collapsed observe as changed, as §3.4 says.
- `resolveRefVersionAtPosition` and `widenRefAtUpward` take a boolean liveness, and RSet uses both (`std_types/src/types/rset/view.ts`), so the boolean variants must stay.

**The touch rule (§2.5).** Status relative to a version decomposes under union as the document says: an entry is in $H(D \cup X)$ when it is in $H(D)$ or in $H(X)$, and it is strictly after $D \cup X$ only when it is strictly after both. So an entry that changes status between $D \cup V_o$ and $D \cup V_o \cup V_{o'}$ also changes it between $D$ and $D \cup V_{o'}$, which is the subset property behind monotonicity. Visibility of deploys follows the same rule. The slots a read uses are resolved per slot by LWW over the visible writes (`rschema/resolve.ts`), and the incarnations a read filters on are functions of the existence slot and the column slot, so "writes a slot the read uses" is the right granularity. The honor flag and every verdict of the bound group are read at the entry's own position from `foreignFrom` (`view.ts:286-289`) and do not depend on $D$. A deploy that deletes rows on FK adoption carries the rows' identity tags (`view.ts:9-35`), so the identity cover matches it, as the document notes.

**Components complete inside the read.** A bound group's diagnoses never call `resolve` on an observer key, so a frame pushed for a bound group's entry can lower its lowlink only to frames of that group pushed above it. Its component completes inside the observer's read, and the read sees decided verdicts. The claim that every loop through an observe passes through the observer's rows follows: an observe reads other observes only through the widening, strictly up the observed version, and through the folds of the bindings its gate names, strictly down the gate graph, and bound groups' entries read nothing above them.

**The walkthroughs.** Traced through the fixpoint:

- Walkthrough 3: the lower step leaves `y` and `d` undecided, the upper step keeps both possibly live, the support graph is the two-cycle `{y, d}`, and both collapse.
- Walkthrough 4: Carol's live `y_C` puts the revoke in $D$. Alice's `y` imports the same version, so it touches nothing. `d` is decided void and `y` live, each on its first evaluation.
- Walkthrough 5: `d` folds the pending `y`, so it joins `y`'s component, but its read is untouched and decides true. The lower step marks `d` live, Alice's row is dead, and `y` fails its gate. This is the "decided on the first pass" case of §2.5.
- After the collapse, Carol's observe `z2` from the merge has `d` in its history, so it is not in `d`'s fold. `d` stays in its core with `z` at every later horizon, `z2`'s gate sees through the void `d`, and `z2` is live. Alice can contest `z2` only from a position below `z`: at the merge, admission's geometric fold already shows the revoke and rejects her delete. §3.3's description of the stalemate is right, and this detail is worth adding to it.

**The bounded delta (§3.4).** Under Lemma 1 of `INCREMENTAL_DELTA_BOUND.md`, no new entry is concurrent with an entry inside the bound, so an observe of `K` concurrent with `y` is old. It is inside the bound and stable (Lemma 5), or outside, not strictly after the bound, and unchanged (Lemma 6). The observe's reads of the observer's own tables at $(\{y\}, \text{from})$ are the reads a row op's guard makes, which Theorem 1 already covers. A delete void by collapse that `y` reads lowers the bound below itself, and `y`, being concurrent with it, leaves the bound and is walked. The text changes to assumption A3 and to guarantee 6 of `walkRevisionBound` are the only changes needed.

**The engine needs no change.** The fold reads verdicts through `resolve`, which returns undecided for an open frame (`verdict_evaluation.ts:199-204`), and `seeThroughCover` already implements the see-through walk with dominance (`verdict_evaluation.ts:110-130`).

---

## 3. The restriction grammar has no negation

> **2026-11-03:** The claim below that a too-coarse touch test always fails loudly is withdrawn. On a loop with an even number of deletes the engine settles such a read silently. See [`REFERENCE_FRAME_CRITIQUE_RESPONSE.md`](./REFERENCE_FRAME_CRITIQUE_RESPONSE.md) §2 and [`REFERENCE_FRAME_CRITIQUE_2.md`](./REFERENCE_FRAME_CRITIQUE_2.md) §2.

§2.5 motivates "a touched slot settles the whole read" with a delete rule `NOT EXISTS auth.bans WHERE user = $author AND active = true`, and §5 specifies a test with the same rule. Neither can be written:

- The `Predicate` IR has `true`, `false`, `exists`, `cmp`, `like`, `and` and `or` (`rschema/payload.ts:248-255`). `not` exists only in `RowFilter`, which is query-only (`rtable/query.ts:38`).
- C-SQL parses `NOT` in any condition, but `lowerRestrictionPredicate`, which lowers gates and `ALLOW DEPLOY IF` as well as table rules, throws `NOT is not supported in schema allow rules` (`rdb_lang/src/compile/query.ts:169-170`).
- `predicates.ts:24-25` records the consequence: "the grammar has no negation, so every rule is monotone in its reads".

The point the example makes survives with a positive atom. Take a delete rule `EXISTS auth.allowed WHERE user = $author AND active = true`, Alice's row written with `active: false`, and a pending import whose deploy drops `active` and re-adds it with default `true`. At $D$ no candidate matches and the read is false. With the import live, the old write belongs to the dead column incarnation, the default supplies `true`, the row matches, and the read is true. A rule that touches candidates only finds none to touch and decides false too early.

What changes is the consequence. The document says such a decision would "settle a loop on one side, consistently and unnoticed". With positive atoms only, it cannot stay unnoticed. In the loop of §3.3, with the rule above and the wrong touch test, the solve goes like this: the first fixpoint decides the delete void and leaves the import possibly live; the next round marks the import live; the delete's rule, now reading the import live, evaluates at $D \cup V_o$ and comes out live; the upper step then finds a member outside the possibly-live set that is live again and throws `became possibly live again (a rule is not monotone)` (`verdict_evaluation.ts:311-313`). Had the early decision gone the other way, `finalVerdicts` would throw on the disagreement (`verdict_evaluation.ts:354-355`). So a too-coarse touch test fails loudly rather than silently. The correct rule still matters, because an error thrown inside a verdict is a crash on every replica that reads that history.

**Recommendation.** Rewrite the example and the §5 test with a positive `EXISTS`. The expected outcome, both entries void by collapse, is unchanged. Replace "consistently and unnoticed" with the engine's actual behavior. State once, early in §2.5, that gates and rules are monotone in their reads: a gate is an `and`/`or` of `EXISTS` atoms over `$author` and literals, and a row rule adds only `cmp`/`like` over the subject row. That property is what keeps the sound-exact-monotone argument short.

---

## 4. `authz_suggest` passes the observer's position as the observed version

`bindUpdateRef` resolves `at` against the observer group and `version` against the bound group (`rdb_lang/src/bind/bind.ts:677-678`). The two `update-ref` branches of `authz_suggest.ts` then call `evaluateObserveGateKey(group, foreignId, at, at, keyId)` with `at = bound.at` (`authz_suggest.ts:284-285` and `:330-332`). `evaluateObserveGate` takes `refAt` and `refFrom` as versions of the observed group and builds the gate's views on that group at those positions (`group.ts:681`), so today the observer's hashes are handed to the observed group's DAG, and the author suggestion for a refused `UPDATE REF` is wrong or empty. `ref_auto_update.ts` is correct: it passes `targetVersion` (`ref_auto_update.ts:141-142`).

§4 says these call sites "pass `G`'s position". Under the new signature they must pass two things: `bound.at` as the observe's position, and `bound.version` as the version imported for the gated binding. Written as the plan reads now, the rewrite would keep the bug.

**Recommendation.** Say in §4 that the `update-ref` branches pass `bound.at` as `opPos` and `bound.version` as the imported version, and that this fixes an existing mismatch. Give the new public `evaluateObserveGate` parameters that cannot be confused, for example an options object with `opPos` and `imported`, since both are a `Version`. Add a rdb_runtime test that suggests an author for a refused `UPDATE REF`, which no test covers today.

---

## 5. Gaps in Section 4

1. **The compiler's `ALLOW DEPLOY IF` scope.** §2.1 makes `canDeploy` translate through table maps, and §4 lists `groupViewReason` for the check. The rdb_lang entry mentions gates only. `compileCatalogGroup` resolves a qualified table for `canDeploy` in the bound view without the map (`rdb_lang/src/compile/catalog.ts:210-219`), so the compiler would check `ALLOW DEPLOY IF` columns against the wrong table whenever a map applies. The `deployScope` resolver needs the same translation as the gate scope.
2. **Where the creation rules of §2.2 live.** §4 lists `rtable_group/validate.ts`, `rtable_group/validate_ops.ts` and `rcatalog/validate.ts`. The first and the third are format validators with no schema view, so rule 1's "names a table of `G`'s pinned schema" cannot be checked there. The gate-graph check (rule 2) is structural and fits both format validators. Rule 1 belongs in `validateCreate` (`validate_ops.ts`) and in `groupViewReason`. The plan should say which rule goes where.
3. **Detecting a collapse in `ref_auto_update`.** "When one of its observes is void by collapse, observe again from the frontier" names no detection point. `propagateRefUpdates` reads no verdicts, and an observe is admitted before its at-use verdict exists. The natural point is `explainEntryVoided(entry, frontier)` after a successful `observe()`. The plan should also say whether a collapsed observe that arrived by sync, or that collapsed later when a concurrent delete merged, is retried, and on what trigger.
4. **Reporting a contested observe.** `authz_suggest` runs on validation diagnostics (`isAuthRelatedFailure`). A collapse is an at-use verdict and never produces one. "Reported as contested" needs a surface, probably the REPL's `LOG` or an explain path, not `authz_suggest`.
5. **`extractForeignDeps`.** Its pin-or-inherit comment (`group.ts:1334-1343`) says an op reads cross-object state only through hashes its prevs or a prior gated op pinned. An observe's gate now reads other bound groups and the observer's own tables. Presence is guaranteed by `extractCreationForeignDeps`, and the versions read are pinned by the observes already in the DAG, so no code changes, but the comment should be revisited in the same commit.
6. **The default-valued candidate change in `view.ts`**, uncommitted at the time of review, is a prerequisite. Without it, "candidates are gathered at $D \cup \bigcup V_o$" misses rows that match a searched value through a column default. It should land before or with this work.

---

## 6. Gaps in Section 6

- **`rpack/IMPLEMENTATION.md`, "Advancing an observed version"** (lines 436-446) describes the superseded design: unqualified `t` becomes `<binding>.t`, the observer's own tables stay out of the gate, and the create payload records a frame. `rpack/README.md` links to it at line 290. The file is untracked but referenced, and it is not in §6.
- **`VOID_SEMANTICS.md` §2** (lines 62-69) says a gated observe "never joins a component" and is "the one place the engine treats an observe edge as negative". §6 lists §4 and §5.5 only. The "Where it lives" paragraph of §5.5 (lines 504-511) also names `evaluateObserveGateIn` as "frame rebasing into `G`".
- **Code comments** that describe the observed frame or the acyclicity invariant: `rtable_group/payload.ts:68`, `validate_ops.ts:89` and `:588`, `group.ts:647-649` and `:658-662`, and the comment at `catalog_tests.ts:585`. These will move with the code, but §2.4's migration list could name them.
- `OBSERVED_CUT_REVOCATION.md` §4.4 item 4 is already updated in the working tree. `PHASE_5_PLAN.md:81` and `DESIGN_AND_PLAN_FOR_ANY.md:176` still say the invariant stays. They are decision records and can keep their text, but a reader following them to `observeVerdictLive` will find no throw.

---

## 7. Smaller points

- §2.5 says the subject row is "the only reader of `columnValue`". That is true of `predicates.ts`, but `candidateMatch` calls `columnValue` on the foreign view for every candidate (`view.ts:668-682`). The slots-and-covers bullet covers this, since it lists each candidate's writes to the searched columns, but the sentence could lead an implementer to apply the touch rule to identity covers only.
- §3.4 says the visitor records a flip "through `isEntryVoided`". It is `explainEntryVoided`, through `horizonVerdicts` (`delta.ts:293-301`), which is what makes the collapse check free.
- §2.2's edge rule excludes $K = H$. §2.3 then reads an alias of the gated group at $(\text{refAt}, \text{refFrom})$ even when it is a second alias with its own map. That is consistent, and worth stating in one sentence. `[OBSGATE10]` has two aliases of one group with a gate on each.
- An observe carries no row ops, so passing it as `exclude` to its local views changes nothing. The plan could say so, to spare a question during implementation.
- §2.3 could note that admission's reads of the other bindings use the geometric fold (`resolveSchemaForeignTableView` passes `filterVoided = false`), as a row op's admission does, and that the at-use fold then filters void observes. A grant imported only by a void observe can therefore pass admission and fail at use, which is today's behavior for row ops.

---

## 8. Recommendations

1. Rewrite the `auth.bans` example and the matching §5 test with a positive `EXISTS`, and correct the sentence about silent settlement (Section 3).
2. Spell out the `authz_suggest` fix in §4: `bound.at` as the observe's position, `bound.version` as the imported version, with a test (Section 4).
3. Add to §4: the compiler's `ALLOW DEPLOY IF` scope, where each §2.2 rule is checked, the detection point for a collapsed observe in `ref_auto_update`, the surface for "contested", and the `extractForeignDeps` comment (Section 5).
4. Add to §6: `rpack/IMPLEMENTATION.md` and `VOID_SEMANTICS.md` §2 (Section 6).
5. Land the default-valued candidate change in `view.ts` first.

With those changes the plan is ready to implement.
