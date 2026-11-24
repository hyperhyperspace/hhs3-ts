# Critique of HIP-22: Concurrent removals of one target

A review of [HIP-22](0022-concurrent-removals.md), checked against the working tree on 2026-11-09. The defect is real, every row of the HIP's *Outcomes* reproduces, and the suites pass unchanged. But the fix lets a removal through the barrier that removed its author's authority, whenever that authority is the removal's own target. Admission checks a back-dated entry at its old position, so a party who has already been removed can still write such a removal, and with the HIP it takes effect: it voids what the remover saw and kept. In the checks below, a revoked RCap holder restores a cap they had revoked (R6), a removed manager does the same in rdb (K10), and a removed member gets her membership back (K6). That breaks the property [HIP-15](0015-certified-checkpoints.md) §1.3 states for today's rdb. What the HIP buys is smaller than its rows suggest: rhost gives each host its own key, and today's failures can be redone. I recommend not taking the HIP as written. The one rule I tried that keeps the HIP's fixes and today's results for third parties, V (§8), has a hole of its own, and the bounded delta misses its changes.

Paths are relative to `modules/`. D0 to D7, K1 to K14, H1, UD1, UD2, A1, C1 to C5, R1 to R7 and B1, B2 are the checks in §1. D1 to D7, K1 to K5, R1 and R2 are the HIP's own rows.

## Verdict

**Is it worth doing?** Not as written.

- **The cost is revocation finality** (§3). With the HIP, someone who has been removed can still undo revocations they made (R6, K10), undo their own removal when they can contest the remover (K6), and void grants they made (K9, R4). The remover may have seen all of these and kept them. Today, none of this is possible once someone else has removed the target.
- **The benefit is smaller than the rows suggest** (§9). The rows where one owner deletes from two devices (D1, D2, D4 to D6) need one key on two replicas, or a write at an old position. rhost gives each host its own key. Today's failures can be redone: a delete written again after the merge takes effect (A1).
- **There is a safe way to the same fixes.** A writer can retry a removal of its own that came out void, with no protocol change (§9).
- **No rule I tried changes the verdicts safely.** V keeps the HIP's fixes and today's results for third parties. But it lets a removed party rescue a use they made concurrently with their removal (K14, R7), and the bounded delta misses its changes (§8).

**Are there conceptual errors?** Three, and a smaller one.

- *Summary* says "a concurrent revocation of a removal's authority still voids it". That is false when the authority is the removal's target (§3).
- "K5 is the one row where an entry other than a removal changes" is false: eight more rows change, and most are the rows of §3 (§4).
- "A contest … is decided as today" is false: K6 is not (§4).
- The `row-not-live` alias reports a kill that no longer matters (§6).

**Are there missed opportunities?**

- The HIP's own argument supports a wider rule, G, which fixes the bundles of C1 to C3 and C5 (§5). G widens the problem of §3 too, so it is worth taking only inside a rule that keeps removal final.
- The two open questions can be closed (§7).
- V shows what such a rule needs, and where it still fails (§8).

| Item | Where in the HIP |
|---|---|
| 3. A removed party's back-dated self-removal takes effect | *Summary*, "A concurrent revocation of a removal's authority still voids it"; *What does not change*, "Admission"; *Why it is sound*, "Authority is still checked", "Its barrier holds" |
| 4. Eight more rows change, and one contest is not decided as today | *Outcomes*, "K5 is the one row"; *Why it is sound*, "Contests stay contests" |
| 5. The rule is narrower than its argument | *The rule*; *Why it is sound*, "It does not decide whether the target is removed"; *Changes* 1 and 2 |
| 6. The `row-not-live` alias reads the ordinary views | *Changes* 2 |
| 7. The open questions | *Open questions* 1 and 2 |
| 8. Keeping removal final: two variants | *Considered and not taken* |
| 9. What the HIP buys | *Why* |
| 10. The test plan | *Tests* |

## 1. How the claims were checked

I read the HIP against `rdb/VOID_SEMANTICS.md` §4, `mvt/WELL_FOUNDED_VERDICTS.md` §2, §4 and §5.5, assumption A3 of `rdb/INCREMENTAL_DELTA_BOUND.md` and HIP-15 §1.3, and the code it changes:

- `rdb/src/rtable/view.ts`: `rowState` (`:334`) and `killedByConcurrentDelete` (`:301`);
- `rdb/src/rtable_group/group.ts`: `diagnoseEntry` (`:1158`), its views (`:1186`) and the `row-not-live` alias (`:1215`);
- `rdb/src/rtable_group/predicates.ts`;
- `std_types/src/types/rcap/view.ts`: `holdingIn` (`:150`) and `opVerdict` (`:235`);
- for the open questions, `std_types/src/types/rset/view.ts` and `rdb/src/rfile_map/`;
- for §9, `rhost/README.md`, `rdb/src/rtable/rtable.ts` and `RTableGroupImpl.bundle`.

Then I ran checks on five copies of `rdb` and `std_types`. None is in the repo.

- **Today**: the working tree.
- **HIP**: *Changes* 1, 2 and 4 as the HIP describes them.
- **G**: HIP, plus the rule of §5 and the alias of §6. 48 changed lines with comments.
- **T** and **V**: the two variants of §8.

A Nov 6 session ran most of the K rows below against copies of the same sources, after the HIP was written. They were not in the HIP. The sources have not changed since, and every row here was run again today.

**Back-dating.** Each probe appends its entries one after another and passes the position of each explicitly, as any client can. In K3, K3s, K5, K6, K6c, K9, K10, K13, K14, C5, R3, R4, R6 and R7, the last entry is appended after the target, or its author's authority, was removed, at a position from before the removal. Admission accepts every one, because it reads `(at, at)` at that position.

**The HIP's rows.** D1 to D7, K1 to K5, R1 and R2 give exactly the results of the HIP's two tables, today and with HIP. Single-device controls (D0, D5a, D6a) leave the removal live and the target removed in every tree. Two holders of a `'*'` grant deleting one doc concurrently void each other today under the rule that reads the doc in every arm. With HIP both deletes are live. Under HIP-21's example rule written as two atoms, both are live today and with HIP.

**More symptoms of the same defect.** Rule P is `EXISTS grants WHERE grantee = $author AND resource = docs.resource`.

| # | Scenario | Today | HIP and G |
|---|---|---|---|
| D3u | rule P; Alice deletes the doc; Bob's concurrent delete arrives later | deleted after the first, live again after the second | deleted after both |
| UD1 | the owner deletes the doc; a third device bundles an update of it with a log row | the bundle void (`row-not-live`) | the same |
| UD2 | UD1 with the delete made from two devices | both deletes void, the bundle live, the doc live | as UD1 |
| A1 | D1, then a third delete written from the merge | admitted, the doc deleted | refused at admission, "rowId '…' is not live in table 'docs'" |

**Third parties and removed parties.** The caps tables are those of `rdb/src/users/users.ts`: a manager can grant, and a cap can be deleted by a manager or its grantee. RCap's `coadmin` is managed by `coadmin`.

| # | Scenario | Today | HIP and G |
|---|---|---|---|
| K5 | the HIP's: a note reads doc `t` as a witness, concurrent with a delete `D` of `t` and in the past of another delete `E` of `t` | the note live | the note void |
| K6 | docs delete rule `(rowAuthor = $author AND EXISTS members WHERE grantee = $author) OR EXISTS admins WHERE grantee = $author`; members delete rule `EXISTS docs WHERE steward = $author`. `F`: the steward of `t` deletes Alice's membership. `E`: an admin deletes `t` after seeing `F`. `D`: Alice deletes `t`, concurrent with both | `F` live, `D` void (`row-not-live`): the membership deleted | `F` and `D` void (`undecided-cycle`): the membership live |
| K6c | K6 without `E` | `F` and `D` void, the membership live | the same |
| K9 | manager M grants a cap; manager A deletes M's manager cap after seeing the grant; M deletes M's own manager cap, concurrent with both | the grant live | the grant void (`restriction`) |
| K10 | K9, with M deleting X's cap in place of the grant | X's cap deleted | M's delete of X's cap void (`restriction`), X's cap live |
| K11a | a note witnessed by `t`, concurrent with the owner's delete of `t` | the note void | the same |
| K11b | K11a plus an admin's delete of `t` after the note | the owner's delete void (`row-not-live`), the note live | the owner's delete live, the note void |
| K13 | G inserts a doc under an editor cap; Bob comments on it (FK); a manager revokes G after seeing both; G deletes G's cap, concurrent with all three | the doc and the comment present | the doc void (`restriction`), the comment void (`fk`) |
| K13c | K13 without the manager's revoke | the doc and the comment void | the same |
| K13d | K13, but after the revoke G deletes their own doc at the position right after its insert, and not their cap | the comment void (`fk`) | the same, in V too |
| K14 | A deletes M's manager cap; M's grant is concurrent with that; M deletes M's own manager cap at the position right after the grant | the grant void (`restriction`) | the same |
| K3s | K3, plus B deleting B's own manager cap from another device | all three void, both caps live | A's delete of B's cap live, B's delete of A's cap void (`restriction`) |
| R3 | RCap: R2, plus holder 2 revoking their own `coadmin` from another device | all three void, both hold | holder 2 revoked, holder 1 holds |
| R4 | RCap: h grants `coadmin` to x; h2 revokes h after seeing the grant; h revokes h's own `coadmin`, concurrent with both | the grant live, the self-revoke void (`unauthorized`) | the grant void (`unauthorized`), x revoked |
| R5 | RCap: caps `a` and `b`, each managed by the other; a holder of both revokes their own `a` and `b` concurrently | both void | the same |
| R6 | RCap: h revokes y's `coadmin`; h2 revokes h after seeing that; h revokes h's own `coadmin` at the position of the revoke of y | the self-revoke void (`unauthorized`), y revoked | h's revoke of y void (`unauthorized`), y holds `coadmin` again |
| R7 | RCap: h2 revokes h; h's grant of `coadmin` to x is concurrent with that; h revokes h's own `coadmin` at the position right after the grant | the grant void (`unauthorized`), x revoked | the same |

**A target removed with its witness.** Grants have open restrictions, so anyone may delete one. Docs have delete rule P. Alice holds grant `gA` for the doc's resource.

| # | Scenario | Today | HIP | G |
|---|---|---|---|---|
| C1 | Alice bundles [delete `gA`, delete the doc] from two devices | both void, both rows live | the same | both live, both rows deleted |
| C2 | docs with the default owner rule, comments deletable by their author or by the doc's owner (`EXISTS docs WHERE slug = $row.doc AND rowAuthor = $author`); the owner bundles [delete Bob's comment, delete the doc] from two devices | both void | the same | both live |
| C3 | device 1 bundles [delete `gA`, delete the doc]; device 2 bundles [delete the doc, insert a replacement] | both void, the doc live | device 2 void (`row-not-live`), no replacement | both live, the replacement present |
| C4 | Carol deletes `gA`; Alice bundles [delete `gA`, delete the doc] concurrently | Alice void (`restriction`), the doc live | the same | the same |
| C5 | Carol, holding her own grant, bundles [delete `gA`, delete the doc]; Alice deletes the doc | both void, the doc live | Alice void (`row-not-live`) | both live |

**Explain and deploys.**

| # | Scenario | Today | HIP | G |
|---|---|---|---|---|
| K7 | A deletes cap X; B deletes A's manager cap; C deletes X too | A's delete void (`row-not-live`) | the same | void (`restriction`) |
| K12 | an FK-adopting deploy deletes item `i`; its owner concurrently bundles a delete of `i` with a replacement | the bundle void (`row-not-live`) | live, the replacement present | the same |
| H1 | the HIP's case for not sparing history: a delete in the bundle's past, void at admission and live later | the bundle void (`row-not-live`) at the merge | the same | the same |

**The bounded delta.** B1: a note `N` witnessed by `t`, the owner's delete `D` of `t` concurrent with it. Start at `{N, D}`. End at `{E, D}`, where `E` is an admin's delete of `t` written after `N`. B2 is B1 with a note between `N` and `E`. Bounded against full `computeDelta`, with `compareGroupDeltaStrategies` from `rdb/test/delta_parity/parity.ts`:

| Tree | B1 and B2 |
|---|---|
| Today | `N` void then live, `D` live then void; both deltas report `N`; parity holds |
| HIP, G | nothing flips; parity holds |
| V | `N` void then live, `D` live at both; the full delta reports `N`, the bounded one does not |

**Suites and parity.**

- **HIP and G.** The rdb and std_types suites give output identical to the working tree's, 402 and 167 tests.
- **Instrumented HIP.** The rdb suite makes 4,766 spared reads, none with a concurrent delete of the same row other than the op itself. The std_types suite makes one spared holding, with no concurrent revoke of the pair.
- **V.** rdb fails `[DBOUND05]` (`rdb/test/delta_bound_tests.ts:272`) and passes the other 401 tests. std_types passes.
- **The removal-history generator.** The Nov 6 session wrote a generator of random histories with concurrent removals of one target, for the bounded-against-full harness. On its smoke profile, today, G and V keep parity. On its extended profile (7 seeds, 160 pairs each), G and V do. V fails parity on B1 and keeps it on every generated pair, so the generator does not produce B1's shape.

## 2. Claims that hold up

- **The cause is where the HIP says.** A delete's restriction reads its row through `rowState`, whose last conjunct is `killedByConcurrentDelete` (`rdb/src/rtable/view.ts:399`). An `exists` candidate reads it through `candidateMatch` and the same `rowState`. In RCap, step 2 of `holdingIn` (`std_types/src/types/rcap/view.ts:166`) is the walk over concurrent revokes of the pair.
- **The fix does what *Outcomes* says**, with the suites unchanged.
- **It only removes reads.** The spared read is `rowState` without one conjunct, and the spared holding is the slot without step 2. Each stays read-once, so exact, sound and monotone. Each reads a subset of "deletes concurrent with `{E}` in `H`", so A3 holds. No cycle is added, and components can only split.
- **The target's fate does not depend on the spared read**, at every horizon where both removals are visible, not only at the merge. If the other removal's kill is not honored there, no cycle existed and nothing changes.
- **Authority read from another row is still checked.** K1 and K2 are unchanged, and so is C4 with G. When the authority is the target itself, it is not (§3).
- **Updates are rightly left out.** K4 and UD1 are unchanged, and UD2 now matches UD1.
- **History is rightly not spared** (H1), **and the target is rightly not spared in other ops of the bundle** (K1).

## 3. A removed party's back-dated self-removal takes effect

Call a removal a **self-removal** when its authority is its own target. The cases are a grantee deleting their own cap under the users.ts rule, a holder revoking a self-managed RCap cap, and a manager deleting their own manager cap under D5's rule.

Today, when someone else removes the target, a self-removal concurrent with that removal fails its read of the target and is void. The HIP spares exactly that read. So for a self-removal, revoking its authority no longer voids it, because revoking the authority is removing the target. *Summary* says the opposite: "A concurrent revocation of a removal's authority still voids it."

**Back-dating turns this into a power.** Admission validates at `(at, at)`, which for a back-dated entry is its old position. Someone who has been removed can attach a self-removal to a position before their removal, where they still held the authority, and admission accepts it. Today it is void. With the HIP it is live. As a barrier, it voids every use of the target concurrent with it: everything its author did with that authority after the position they chose, including what the remover saw before removing them.

| Row | The removed party, after removal | Today | HIP |
|---|---|---|---|
| R6 | un-revokes a holder they had revoked | y revoked | y holds `coadmin` again |
| K10 | the same in rdb | X's cap deleted | X's cap live |
| K6 | undoes their own removal, by back-dating a delete of the doc the steward's authority rests on | the membership deleted | the membership live |
| K9, R4 | voids grants they made, which the remover had seen | the grants stand | the grants void |

**Most of these are new powers.** In K6, the steward cannot redo the removal afterwards: their authority was `t`, which is now deleted. K13 is the exception. Its harm is reachable today by another route: after the revoke, G back-dates a delete of their own doc and Bob's comment is void (K13d, in every tree). G's authority over the doc is ownership, which nobody removed.

**This breaks HIP-15 §1.3.** That section states, for today's rdb: "A writer whose authority was removed by a barrier the reader holds cannot get a guarded op past that barrier by choosing older predecessors." With the HIP, a self-removal is such an op. HIP-15's checkpoints would bound how far back it can be attached. They would not stop it.

**Equal to a lone self-removal, and still not benign.** In each row, the HIP's result is what a lone self-removal gives: compare K11b with K11a, and K13 with K13c. That does not make it benign. A lone self-removal is made by someone who still holds the authority, and they could undo their grants and revocations anyway, by granting again. These rows are made by someone who no longer holds it.

**Change.** Do not take the HIP's rule on its own. A rule for concurrent removals has to keep a removal by someone else final: once a live removal of `t` by another party exists, no back-dated removal of `t` may change any use's verdict. No rule I tried does this without a hole (§8). Short of a protocol change, the symptoms can be fixed at the writer (§9).

## 4. Eight more rows change, and one contest is not decided as today

A removal that was void and becomes live becomes a barrier: entries concurrent with it that read its target are void. The HIP gives one such row, K5, saying "today the note is live only because the collapse voids `D`". The same holds without a collapse wherever the second removal failed its read of the target. The first removal is then void today for that reason alone, and with the HIP it becomes live.

- **Eight more rows change.** They are K6, K9, K10, K11b and K13 in rdb, and R4 in RCap, plus K3s and R3, which change a contest. K6, K9, K10 and R4 are the rows of §3.
- **K5 and K11b are §3 with a doc in place of a cap.** The owner of `t` back-dates a delete of `t` after another party removed it, and voids notes that party had seen. Today the owner can do that only until someone else removes `t`. The stakes are lower.
- **K3s and R3 are welcome.** A side that also resigns from another device loses a contest that today collapses, and the only party it harms is the one who resigned.

**K6 is a contest, and the HIP does not decide it as today.** Alice's delete of `t` needs her membership, which the steward's `F` removes. The steward's delete needs `t`, which Alice removes. Each is the other's barrier, which is K6c. Today the admin's later delete `E` settles it. `E` kills `t` at Alice's delete, so Alice's delete is void whatever `F` is, and `F` is live. With the HIP, Alice's delete no longer reads `E`, it depends only on `F`, and the loop collapses.

So "it only removes reads" is true, and it is not the whole story. A read can be false because of an entry outside the component, and that read can be what decides an entry. Removing it can leave the entry in a loop that the read had been settling. As probed, Alice's delete is written after `F` and `E`, so K6 is also §3's self-restoration.

**Change.** Whatever rule is taken:

- list every row that changes in *Outcomes*, K6 to K14, K3s, R3, R4, R6 and R7 among them;
- replace "K5 is the one row where an entry other than a removal changes";
- replace "a contest … is decided as today" and *Contests stay contests* with what holds for that rule.

## 5. The rule is narrower than its argument

*Why it is sound* argues: "If `E` is void, it is seen through and kills nothing, so `D`'s read is today's. If `E` is live, `t` is removed at the merge whatever `D`'s verdict." The argument is about `E`, a concurrent removal of `D`'s target, as a whole. The rule takes from it only `E`'s kill of `t`. When `E` also removes a row that `D`'s rule reads as a witness, `D` still reads that kill:

- **C1 and C2.** A bundle that removes a doc together with its grant, or with its comments, from two devices collapses as D1 does. Deleting a doc with what hangs off it is the most common multi-row delete an app makes.
- **C3.** The D7 case with a witness in place of the subject: device 2's bundle is void, and its replacement is lost.
- **C5.** Alice's delete is void because Carol's bundle, which deletes the doc too, removed Alice's grant.

**G sees through the whole entry.** In the rule of a delete op of row `t`, a concurrent entry that deletes `t` is seen through, in every view of that rule. This fixes C1 to C3 and C5. Every other row of §1 is the same with G as with HIP, apart from K7's reason (§6), and both suites are unchanged. Carol's entry in C4 removes only Alice's grant, so it is not seen through, and Alice's bundle stays void.

**G also widens §3.** In C5, Carol's bundle removes Alice's authority, and with G Alice's delete is live anyway. As probed, Alice writes it after Carol's bundle, at an older position. That is §3's pattern, with authority that is not the target but was removed by an entry that also removes the target. With V's ratification the uses Carol saw stay protected (§8). On its own, G makes §3 worse.

**Implementation.** A predicate on the entry: does it delete `(table, t)`, as a row op, as a write of a bundle, or as a deploy whose kills carry `t`'s tag. It reads no verdict. The view applies it only in `killedByConcurrentDelete`, where an entry it accepts reads as void. That is the same walk over a subset of the same entries, so it stays exact, sound and monotone, and inside A3. My copy checks row ops and bundles, and keeps the spared row for the target itself, which covers a deploy's kill of the target. In RCap a revoke clears one pair, so G and the HIP's rule coincide.

**Change.** If a rule that keeps removal final is found (§8), state its rdb part as G does. Give the view the predicate beside `exclude` (*Changes* 1), and give it to every local view of a delete op's rule (*Changes* 2). Add C1 to C5 to *Tests*.

## 6. The `row-not-live` alias reads the ordinary views

*Changes* 2 keeps the ordinary views for the alias. So a void delete whose target another delete removed concurrently reports `row-not-live`, though by this HIP that kill plays no part in a delete's verdict:

- In K7, A's delete is void because B removed A's manager cap. It reports `row-not-live`, because C deleted the target.
- In C3 with HIP, device 2's bundle reports `row-not-live`. It is void because device 1 removed the grant.

With the alias reading the op's own views, K7 reports `restriction`, and nothing else in §1 changes. H1 still reports `row-not-live`: the row was deleted in the bundle's past. An update's alias is unchanged (K4). The rdb suite is identical.

**Change.** In *Changes* 2, the alias of a delete reads the views of the op's rule.

## 7. The open questions

- **Deploy deletes.** K12: with deploys spared, the owner's replacement is kept, and nothing in §1 argues for leaving them out. A deploy is never void and needs `canDeploy`, so it gives a removed party nothing. Back-dated deploys are HIP-15 §5.1's problem. Take one rule for every deletion. Under §5, a deploy whose kills carry the target is an entry that deletes it.
- **Other types.** No. An RSet entry is authorized through its author's cap in the referenced RCap (`isEntryAuthorized`, `std_types/src/types/rset/view.ts:184`), never through the element it removes. RFileMap has no authorization reads (`rdb/src/rfile_map/`).
- **HIP-21.** As the HIP says, the two `'*'` holders' deletes are both live with HIP under the rule that reads the doc in every arm. Under HIP-21's example rule they were already live.

## 8. Keeping removal final: two variants

The defect needs a removal to stop reading its target through the other removals of it. §3 needs a removal by someone else to stay final. Both hold if a removal of `t` cannot void a use of `t` that another removal of `t`, live by today's read, has already seen. I tried two rules, and neither has that last clause.

**T, two verdicts.** This is the Nov 6 session's variant. A removal takes effect by its verdict with the spared read. When another entry's read meets it as a barrier, it counts only if it is live with today's read too. T keeps K5, K6, K9, K10, K11b, K13 and R4 as today, and D1 to D7 as HIP. But in UD2 the update concurrent with two deletes is live again, while one delete voids it (UD1). The two barrier verdicts read each other's kills, so the original even loop has moved from the deletes to their barriers. C1 and C2 collapse as today, and device 2's bundle in C3 and Alice's delete in C5 are void with `undecided-cycle`.

**V, ratification.** This one is G plus a different rule for the kill. A concurrent kill of row `r` does not count for a read when a live delete of `r` that has the reader's position in its past is visible from `from`. RCap gets the same rule for revokes of a pair. Results:

- **Fixes.** V gives every result of G on the D, C, UD, A and K12 rows.
- **Third entries.** It gives today's verdict for the third entry of K5, K6, K9, K10, K11b, K13 and R4: the note, the steward's delete, the grant, the revoke, the note, the doc and comment, and the grant. In K6, Alice's delete is void with `restriction`, the right reason.
- **R6.** y stays revoked.

**V has a hole of its own: the ratifier can be the removed party's own removal.** In K14, A removes M. M's grant is concurrent with that, so today it is void. M, already removed, then writes a delete of their own manager cap at the position right after the grant. With V that self-removal is live, because its read of the target is spared. It is after the grant, so it ratifies it, and the grant is live. R7 is the same in RCap: x holds `coadmin`. Today and with the HIP, both grants are void.

So the ratifier has to be judged by today's read, not by the spared one: a self-removal that someone else's removal voids today ratifies nothing. That needs a second verdict per removal, as in T. A loop among those verdicts would only leave fewer uses ratified, which gives those uses the HIP's result. I have not built or run this variant.

**What V costs, even with that fixed:**

- **It reads a kind of entry that A3 does not list**: deletes in the reader's future. The bounded delta does not see that kind of change. In B1 and B2 the note flips between the horizons, the full delta reports it, and the bounded one misses it. V needs a new case in A3 and its proof, and in the bound walk: a new removal of a row can flip readers of that row in its past.
- **It changes a verdict a test pins.** `[DBOUND05]` has an FK-adopting deploy written after a ref insert, which ratifies the insert, so its premise fails.
- **It keeps a later removal able to revive a use.** K11a to K11b, K13c to K13. Under §3's reading that is the point: the later removal saw the use.

**Change.** Record under *Considered and not taken*:

- T, with UD2;
- V as tried, with K14, R7 and B1;
- if the HIP is rewritten around another rule, its current one, with §3.

A rule that changes these verdicts should come as a HIP of its own. It should state the property first: once another party's live removal of `t` exists, no removal of `t` written later at an older position changes any verdict. It should then check the rule against R6, K6, K9, K10, R4, K14 and R7, and carry the bounded-delta work.

## 9. What the HIP buys

**One owner from two devices is rare.**

- **Each host has its own key.** rhost runs each host "with an Rdb database with its own replica, signing key, mesh peer and projection" (`rhost/README.md:53`). Under the default owner rule, only the inserting key can delete a row.
- **Writes go at the frontier by default.** `RTableImpl` and `RTableGroupImpl.bundle` write at the frontier unless the caller passes `at` (`rdb/src/rtable/rtable.ts:155`, `rdb/src/rtable_group/group.ts:1063`).
- **So the same-key rows need unusual conditions.** D1, D2, D4 to D6, UD2, A1 and C1 to C3 need one key on two replicas, or a write at an older position.
- **The realistic rows have two authors.** D3, D3u, D7 and C5 are the ones apps will meet, and they are where §3 and §5's widening live.

**Today's failures can be redone.**

- **A delete written again works.** In A1, a delete written from the merge is admitted and takes effect.
- **A bundle can be written again.** A void bundle can be redone, with the rest of its writes.
- **§3's failures are worse.** In K6 the steward cannot redo their removal. In R6 and K10 nothing tells the remover that a revocation was undone.

**Retry at the writer.** A replica whose own removal comes out void, with `undecided-cycle`, or with `row-not-live` because another delete removed the target, writes it again at the frontier, together with the rest of its bundle where that still applies.

- **Every check still applies.** The new entry is an ordinary write, so admission and every use-time check apply, and §3 does not arise.
- **It fixes the removal rows in effect.** That covers D1 to D6, D3u and C1 to C3, and D7 and C5 when the bundle's other writes can be replayed.
- **It needs no protocol change.** I did not test it as a mechanism; A1 shows that the retried delete is admitted and works.

**Sparing only the same key.** Spare a removal only from concurrent removals of its target by the same key. Every row of §3 has a different remover, so it stays as today. By the first point, though, this fixes only the rare rows. Untested.

## 10. The test plan

| Test in the HIP | What it misses | Change |
|---|---|---|
| none | a removed party's back-dated self-removal (§3, §8) | R6, K10, K6, K9, R4, K14 and R7, each asserting that the self-removal, written after its author's removal at an older position, changes no verdict. The first five fail with the HIP's rule, K14 and R7 fail with V, and all pass today. K13d beside them, pinned as today. |
| `[CRMV01]` to `[CRMV06]` | the bundles of §5 | C1, C2, C3 and C5, and C4 as the control |
| `[CRMV10]`, K5 | the other rows of §4 | K6, K9, K10, K11b and K13, each beside its control (K6c, K11a, K13c); K3s |
| `[CRMV07]` to `[CRMV09]` | | as written |
| `[VREAD06]` | the rule's own read | the spared or seen-through walk among its read rules, and for V the ratifying read |
| `[CAP45]`, `[CAP46]` | the RCap rows of §3 and §4 | R3, R4 and R6; R5 pinned as it stands |
| none | the alias, deploys, and the user-visible symptoms | K7, K12, D3u, UD2 and A1 |
| none | the bounded delta across these verdicts | B1 and B2 as `[DBOUND]` tests; the Nov 6 generator of removal histories as a `[DELTA_PARITY]` case, extended with B1's shape |

The suites say nothing about this change. In the instrumented run, no spared read meets a concurrent delete of its row (§1). "No existing test depends on a verdict this HIP changes" is true because no existing test reaches one.

## 11. Smaller points

- **Stronger rows for *Why*.** D3u and UD2 show the defect as a user meets it: a deleted doc comes back when the second delete syncs, and an edit concurrent with two deletes survives them.
- **Admission after the merge.** A1 is admitted today and refused with the HIP: "rowId '…' is not live in table 'docs'" (`rdb/src/rtable_group/validate_ops.ts:432`, and `:550` for a bundle). A client that retries a delete after a merge would then get an error. Whatever rule is taken should say so, and §9's retry should treat that refusal as done.
- **R5 remains.** One holder resigning two caps that manage each other still collapses, because each revoke's authority is the other's target. It reads as a contest, and separating it would need the per-author reasoning the HIP sets aside.
- **The docs list in *Changes* 5.** `WELL_FOUNDED_VERDICTS.md` §5.2 should get the rows of §3 and §4. HIP-15 §1.3 would need a fourth kind of writer if the HIP's rule were taken.

## 12. Recommendations

1. **Do not take the HIP's rule as written** (§3). Mark the draft as needing revision, or withdraw it in favor of a HIP for V.
2. **Fix the symptoms at the writer first** (§9). It is safe, needs no protocol change, and covers the rows users will see.
3. **If the verdicts themselves should change, write a new HIP** that states the finality property of §8 first, checks its rule against the rows that test it, and carries A3, the bound walk and their proof. V with ratifiers judged by today's read, together with G and the alias (§5, §6), is the candidate to start from. It is untested.
4. **For any rule, correct the three claims and report every row** (§3, §4), close the open questions (§7), record T (§8), and add the tests of §10. The first of those tests is a back-dated self-removal by a removed party that changes nothing.

## 13. Outside this HIP

- **Back-dated contests restore removed parties today.** K3 and K6c are back-dated as probed. B writes the removal of A after A removed B, and Alice writes her delete after the steward removed her. In both, the collapse restores the removed party, in every tree. HIP-14 §3.3 calls such entries dodges written below what they contest. The design's answer is authority the removed party cannot contest (`[OBSFRAME20]`). HIP-15 §1.3's list of writers who can still revise a view does not name this one: its entry is void, and it revises the view by voiding the barrier that removed its author. That list could take it as a fourth item.
- **An owner keeps their rows' barriers after losing every cap.** K13d: G, revoked, back-dates a delete of their own doc and voids Bob's comment, which the manager had seen. This is HIP-15 §1.3's first kind of writer, authority still held, since ownership is not revoked. It is worth knowing for rules where ownership outlives membership.
