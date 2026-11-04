# Third critique of `REFERENCE_FRAME.md`

This reviews [`REFERENCE_FRAME.md`](./REFERENCE_FRAME.md) as revised on 2026-11-03 after [`REFERENCE_FRAME_CRITIQUE_2.md`](./REFERENCE_FRAME_CRITIQUE_2.md) and [`REFERENCE_FRAME_CRITIQUE_2_RESPONSE.md`](./REFERENCE_FRAME_CRITIQUE_2_RESPONSE.md). It covers the changes the second review did not see: the per-query touch computation, the four-step proof, the live comparison, the design in code, and the two prototypes. "§2.5" refers to the plan, "response §3" to the response. Line numbers refer to the working tree on the day of the review and will drift.

---

## Verdict

The design holds. The four-step proof is right, the live comparison is sound against every item of §7 item 5, the per-component match is sound, the guard's answer is the new observe's verdict, and the bounded-delta argument covers the touch test's reads. Both prototypes run here as the response reports (Section 7).

**Verified as correct:**

- Step 1 of the proof, against each read of `src/rtable/view.ts`: the column read, the row state, the concurrent-delete check and the candidate match each depend on the version only through the live sets their queries match (Section 2).
- Steps 2 to 4, by argument and by `[TOUCH]` for all three indexes, with and without a predicate (Section 2).
- The live comparison: every verdict of `K` the comparisons read is complete before `G`'s read returns, the predicate does not depend on the version, and the memo may outlive a computation (Section 3).
- The per-component match in `existsMatching` (Section 4).
- The guard's answer equals the observe's verdict when written (Section 5).
- The bounded delta: every observe in `y`'s fold is stable, so `D` and every $D \cup V_o$ lie inside $H(b_F)$, and Theorem 1 for `F` fixes what the comparisons consume (Section 6).

**What should change before implementation**, none of it a design change:

1. **The index contract is implicit.** Step 2 relies on a filtered cover with a predicate returning exactly the topmost live matches. No index documents that; `[TOUCH]` establishes it for the three indexes. The plan should state the contract and name the test as its check (Section 2).
2. **Three text changes** in §3.4, A3 and guarantee 6, as §3.4 already says, with the argument that makes them true (Section 6).
3. **The per-batch check** recomputes the live fold of every binding of every observer, and fires on the runtime's own writes (Section 8).
4. **The oracle** should enumerate versions and pin the configurations that hit the slot and concurrent variants, and state the untouched-slots condition its monotonicity check assumes (Section 7).

---

## 1. How the claims were checked

I read the revised plan and the response against:

- rdb: `src/rtable/view.ts` (`walk`, `killedByConcurrentDelete`, `rowState`, `columnValue`, `existsMatching`, `candidateRowIds`, `carriers`, `candidateMatch`), `src/rtable_group/group.ts` (`resolveSchemaVersion`, `resolveForeignTableViewIn`, `resolveVerdict`, `subscribe`), `src/rtable_group/predicates.ts` (the two foreign reads), `src/rschema/resolve.ts` (the slot keys, `slotsTouchedByRule`), `src/rschema/incarnation.ts`, and `INCREMENTAL_DELTA_BOUND.md` (A1 to A6, Lemmas 1 to 6, Theorem 1);
- mvt: `src/verdict_evaluation.ts` (`resolve`, `lowerReader`, `complete`, `finish`), `src/refs.ts` (`refVersionAtOrAbove`), `src/revision_bound.ts` (guarantees 1 to 8), `src/mvt.ts` (`ScopedDagSubscription`);
- dag: `src/dag.ts` (the cover queries and the growth listener), `src/store/dag_store.ts` (the listener contract), `src/idx/dag_idx.ts` and the three index implementations, for what they promise about the predicate;
- the two prototypes, `dag/test/touch_cover_tests.ts` and `rdb/test/touch_rule_prototype_tests.ts`, in full.

Both prototypes were run from their module directories, with `npm test -- TOUCH` in dag and `npm test -- TOUCHP` in rdb. Running `run_tests.ts` through node directly fails to resolve `ts-node`, which `register.mjs` finds through npm's environment only.

---

## 2. The four-step proof holds

**Step 1.** Each read of a bound group is a function of the maximal live matches of its queries and of fixed per-entry data, never of the entries a walk passes:

- `columnValue` (`view.ts:383-411`) takes the largest hash among the live cover of the column's write filter, and the value that write carries.
- `rowState` (`view.ts:312-367`) returns false when the live identity cover holds a delete, otherwise takes the largest-hash live insert, and then reads `killedByConcurrentDelete`.
- `killedByConcurrentDelete` (`view.ts:279-297`) walks the live concurrent cover of the delete filter from `from`, and asks each delete's honor flag at the delete's own position and `from` (`honored`, `:286-289`). The flag depends on the delete entry and on `foreignFrom`, not on `D`.
- `candidateMatch` (`view.ts:668-682`) is a Kleene `and` of column reads, each falling back to the default at `D`, and the row's liveness.

With `K`'s verdicts decided, the `undecided` map of `walk` (`view.ts:246-264`) is empty, so each cover is the whole result. The slots the reads consume are the ones the plan lists: the table's incarnation is the hash of the base definition carried by the winning create write plus its drop generation (`incarnation.ts:34-36`), so it depends on the existence slot alone; a column's incarnation is seeded by the table's and depends otherwise on its own slot (`:42-46`). A deploy that changes only rules, or another column, touches nothing a read of this column uses, as "Precision" says.

What step 1 leaves implicit is the **index contract**. The plan says "with the predicate, a history query at `X` returns the topmost entries of $H_F(X)$". The facade documents the query as "reading a value at a specific version, by finding the last changes on that value" (`dag/src/dag.ts:33-34`), and the index interface (`dag/src/idx/dag_idx.ts`) says nothing about the predicate. The proof needs two properties: completeness, that the cover contains every maximal live match, which is what soundness needs; and exactness, that it contains nothing else, which is what the "exactly when" of step 2 needs and what keeps the test from touching for nothing. `[TOUCH_02]` checks both for the flat, topo and level indexes, with and without a predicate. The plan should state the contract next to step 2 and name that test as its check, so that a future index is held to it.

**Step 2.** The argument is right. A match entering the history lies below some topmost match of $H_F(X')$ that entered too, so the history cover changes. With equal histories, a concurrent change can only be a match after every head of `X` that is concurrent with `X'`, and a topmost member of $C_F(X')$ above it is new. The gap the history comparison closes is real: `[TOUCH_01]` builds it, and `[TOUCH_02]` met it in 975 of 3,240 comparisons here (`gapHidden`).

**Step 3.** The additivity argument is a union argument on histories, on "after every head", and on the schema entries a version makes visible. The liveness predicate does not depend on the version read, which is what lets it be carried through unchanged (Section 3). `[TOUCH_02]` checks that two additions that change nothing alone change nothing together (831 cases here), and that a change against $X \cup Y_1$ is a change against `X`, which is the monotonicity of §2.5.

**Step 4.** `candidateRowIds` (`view.ts:595-635`) gathers rows through `carriers`, which is `findAllWithFilter` at the view's version (`:645`), and through the defaults at that version. Both grow with the history, so the candidates at the union contain every completion's. The plan's change, that `candidateRowIds` and `carriers` take the version, is the right one. The gathering has no predicate, so a row whose only carrying write is void in `K` is a candidate; its match then decides false at `D` through untouched covers, which is correct (Section 4).

---

## 3. The live comparison holds up

Against the re-check list of §7 item 5:

- **Every verdict of `K` the comparisons read is complete before `G`'s read returns**, including those of entries the read at `D` never meets. In `VerdictEvaluation.resolve` (`verdict_evaluation.ts:159-183`), a key already answered is returned without touching any lowlink (`:160-161`). A new key is pushed above the running frame (`:168-171`), and it lowers its reader's lowlink only through `lowerReader` (`:181`, `:194-197`), which lowers only on a smaller index. A frame of `K` can get a lowlink below its own index only by reading an open frame, and the open frames it can reach belong to `K` or to groups below it, since `K`'s entries never read `G`. No such frame is open below the running member of `G`: a frame stays on the stack only while its component's root is open, and the chain of running diagnoses from that root to `G`'s member would have to pass through an entry of `K` reading `G`. So every frame of `K` completes before `resolve` returns to `G`'s read, and `finish` (`:220-228`) records its verdict. The verdict key carries the horizon (`group.ts:1049`), so "`K`'s verdicts at `foreignFrom`" is one key per entry.
- **Step 1 with every verdict of `K` decided** is Section 2 above. **Steps 2 and 3 with the filter "matches `F` and is live"** go through because the predicate is a function of the entry and `foreignFrom` only; `[TOUCH_02]` runs both with the `liveByHash` predicate.
- **The memo may outlive a computation.** Each comparison is a function of a filter or slot and of (`K`, `D`, `V_o`, `foreignFrom`), and `K`'s verdicts at `foreignFrom` are the same in every computation (A2). The plan does not say how the memo is bounded; `computeRevisionBound` keeps a 256-entry LRU for the same reason (`INCREMENTAL_DELTA_BOUND.md` §6.2, "The memo"), and the touch memo should say what it does.
- **The cost of the extra verdicts.** The comparison at $D \cup V_o$ computes `K`'s verdicts for matches that the read at `D` never meets, down to the first live ones, as "What it costs" says. These are verdicts `K` would compute anyway if the observe were live, so the extra work is bounded by one live fold per pending observe.

The reason for the change, §3.3's back-dated write, is handled as the plan says: a write void in `K` touches nothing, so a revoked principal cannot contest a delete in `G` with it. `[TOUCHP02]` confirms that counting every entry is sound too and decides less (1,855 against 1,588 decided reads here).

---

## 4. The per-component match is sound

`existsMatching` checks the existence slot and the searched columns' slots first, gathers candidates at the union, and takes `someTruth` over candidates of `everyTruth` over column reads and liveness. Each component is a read of one row, so step 1 applies to it on its own, and a Kleene `and` of sound three-valued reads is sound: a component decided false rules the candidate out in every completion, and a candidate decided true in every component matches in every completion. Three cases worth naming:

- A candidate ruled out by an untouched column is false in every completion, because the default it falls back to is a slot checked before the gathering.
- A candidate whose only carrying write is void in `K` has equal covers at `D` and at $D \cup V_o$, decides at `D`, and is false: `rowState` finds no live insert, or `columnValue` finds no live write and the default does not match.
- A candidate whose live insert is only in $V_o$ has a changed identity cover and is undecided, as it should be.

The early returns, a table absent at `D` or a field that is not a pub column at `D`, hold in every completion only while those slots are untouched. The plan places the slot check before them, which is right, and the prototype does the same (`touch_rule_prototype_tests.ts:295-303`).

---

## 5. The guard's answer is the observe's verdict when written

> **Note (2026-11-04):** the claim of this section is wrong. A verdict's reads of a bound group depend on the horizon through `foreignFrom`, and a new observe extends the next horizon's fold of its binding by the version it imports, so the verdicts the gate reads can change between the guard and the observe. See [`REFERENCE_FRAME_CRITIQUE_4.md`](./REFERENCE_FRAME_CRITIQUE_4.md) §3. Of the three parts below, only the gated binding's `refAt` and `refFrom` survive.

Response §1 argues it at the frontier. Three parts, each checked against the code:

- **Reads of `G`'s tables.** The new observe `o'` is at the frontier as position and horizon, so a view at (`{o'}`, `frontier`) sees the same entries as the guard's view at (`frontier`, `frontier`): nothing is concurrent with `o'`. No old verdict changes, since `o'` is in no old entry's history or concurrent set, hence in no old fold or widening.
- **Other bindings.** The guard reads them through the live fold at the frontier, and `o'` reads them through the live fold at its own position, which folds the same observes with the same verdicts.
- **The gated binding.** `refAt` is the imported version: no observe of the binding sits above the base, so `widenRefAtUpward` adds nothing. `refFrom` is the geometric fold at the new horizon, which is the imported version too, since every version `G` ever observed of `H` is below `H`'s frontier (A5). Nothing is pending, because every component completes at the frontier (Section 3).

So the guard decides, and its answer is the verdict `o'` gets. The two §5 rdb_runtime tests, an author who passes admission only writes no observe however many checks run, and an author refused at admission writes none although the gate at use would pass, cover both directions.

---

## 6. The bounded delta covers the touch test's reads

The response's worry is that, with the live comparison, an entry's verdict in `G` can depend on `K`'s verdicts for entries beyond `D`, in the imported part, and asks whether Theorem 1 covers them as foreign reads. It does, and the argument is short enough to put in §3.4:

1. Take `y` inside $U = H(B)$. Every observe in `y`'s fold is stable: one in `y`'s history is in `U`, and Lemma 5 says no unstable observe is inside the bound; one concurrent with `y` is a barrier concurrent with a position inside `U`, so it is in `U` or, by Lemma 6, stable with a decided, unchanged verdict. So `D`, the live fold at `y`, and every $D \cup V_o$ lie inside $H(b_F)$, the bound computed for `F` between `refAtStart` and `refAtEnd`.
2. Lemma 1 for `F` then says no new entry of `F` is concurrent with any position whose history lies in $H(b_F)$. So the concurrent covers at `D` and at $D \cup V_o$ gain no members, and the history covers are over histories that contain no new entry.
3. Theorem 1(a) for `F` fixes the verdicts the predicate reads, and 1(b) fixes every read of a view anchored at `D` or at $D \cup V_o$: the covers, the slot test's schema versions, and the candidates. A6 lets the theorem be assumed for `F`.
4. A2, with the rules' monotonicity, makes the solve of `G`'s entries deterministic from those inputs. A verdict void by collapse at either horizon counts as changed, which the committed visitors already do.

The text has to say this in three places, as §3.4 anticipates:

- **A3** lists a verdict's reads. It should add: for an observe, the views of each bound group at ($D \cup V_o$, `foreignFrom`) for each pending observe `o`, and the schema of that group at that version, which the touch test consumes.
- **Guarantee 6** says a verdict "may fold only some of the concurrent barriers (`widenRefAtUpward`)". It should also allow a verdict to read a referenced object at a version that adds, to the live fold, the version of a ref-advance whose verdict is undecided, one at a time, and state that those versions lie inside the referenced bound when the ref-advance is stable.
- **§3.4** should carry step 1 above, that every observe in `y`'s fold is stable, which is the sentence that makes the other two true.

Before this is relied on, the parity sweeps need the cases §3.4 lists, and in particular a side-branch observe of `K` that stays pending for an entry of `G` below the meet.

---

## 7. The prototypes

Both run here as the response reports, at the default scale:

| Test | Result |
| :--- | :--- |
| `[TOUCH_01]` | pass, all three indexes |
| `[TOUCH_02]` | 3,240 trials; `historyChanged` 3,402, `concurrentOnly` 423, `gapHidden` 975, `unchanged` 2,655, `addedUp` 831 |
| `[TOUCHP01]` | 400 trials; 2,907 decided reads, 1,893 undecided, 0 violations; 325 monotone checks, 438 shortcut checks |
| `[TOUCHP02]` | live entries 1,855 decided against 1,588 for every entry; both sound |
| `[TOUCHP03]` | no slot check 6 violations, no concurrent comparison 3, candidates gathered at `D` only 260; each caught |

Three notes for the permanent oracle of §5:

- **Enumerate, as the plan intends.** `[TOUCHP03]` catches the slot and concurrent variants six and three times in 300 trials, with a fixed seed, so a change that halves their frequency would still pass and one that removes a case might too. The oracle should enumerate the versions of a small fixture, and pin configurations that hit each variant.
- **State the monotonicity check's condition.** The prototype checks that a touch against $D \cup V_{o_1}$ is a touch against `D` only when the table's and the searched columns' incarnations agree at the two versions (`touch_rule_prototype_tests.ts:435-439`). That is the right condition, since a touch of the existence or column slot makes the read undecided before any query runs, but it is the prototype's encoding of "no pending observe touches the read's slots" and the oracle should say so.
- **The variants are the right ones.** Each broken variant removes one of the three mechanisms the proof needs: the slot check (step 3's fixed filters), the concurrent comparison (step 2's second case) and the union gathering (step 4). An oracle that keeps the three variants keeps the proof honest.

---

## 8. The check after a batch

§4 rdb_runtime now has the one rule, the at-use guard and the triggers. Two things about the sync trigger that the plan should say before it is built:

- **Cost.** After any batch, every group that binds another is checked, and each check computes the live fold of each binding at `G`'s frontier, which is a verdict solve per binding. §7 item 4 says completed verdicts are not kept across computations, so this is a fresh solve per binding per batch. Memoize the up-to-date answer per pair of frontiers (`G`'s, `H`'s), or skip a binding when neither `G` nor `H` grew since the last check.
- **Reentrancy.** The hook is `RTableGroupImpl.subscribe`, built on the DAG growth listener, and the store's contract says a local commit fires the listener too, possibly twice (`dag/src/store/dag_store.ts:40-48`). The runtime's own observe grows `G`'s DAG, so the check fires inside itself. The check must be single-flight: coalesce events while one runs, and run once more afterwards if any arrived.

---

## 9. Smaller points

- The verdict key carries the horizon (`group.ts:1049`), so the three-valued fold needs no new key for an observe's verdict at `foreignFrom`, as "No new keys" says.
- `[TOUCHP]`'s fixture has `concurrentDeletes` on, two concurrent deploys, and a row whose insert and delete are both in the sampled positions, so the delete filter's concurrent and history comparisons are both exercised. It never toggles the `concurrentDeletes` mode, which is fine: the honor flag is read at the delete's own position and `foreignFrom`, never at `D`, so the mode slot is not among a read's slots.
- The plan's statement that a bound group is read in two ways only is right: `predicates.ts` reads a foreign group through `existsMatching` (`:206`) and `rowLiveness` (`:420`).
- `[TOUCH]` and `[TOUCHP]` are registered in their runners and run under npm only, through `register.mjs`.
- The dated note on the first critique is in place, and `test/rdb_full_sync_harness.ts` exists with the name §5 gives.

---

## 10. Recommendations

1. State the index contract next to step 2 and name `[TOUCH_02]` as its check (Section 2).
2. Make the three text changes of Section 6 in §3.4, A3 and guarantee 6, with the stability sentence.
3. Say how the touch memo is bounded (Section 3).
4. Memoize the per-batch check per pair of frontiers and make it single-flight (Section 8).
5. Make the oracle enumerate versions, pin configurations for each broken variant, and state its monotonicity condition (Section 7).

With the first two, the plan is ready to implement.
