# Critique of the Import Patches Proposal, v3

A review of [IMPORT_PATCHES.md](./IMPORT_PATCHES.md) after v2. The parent link, the genesis `note`, the column-reset path, and §3.3 are in place. What is left is machinery that repeats the diff from `candidateView` to `wanted`. That diff is already the whole patch.

## Verdict

The plan can lose the reserved `1000+` majors and the maximal-slot walk. A local patch numbered like any other schema update, plus the planner's existing diff, covers the same upgrades. One rule has to stay, and it has to be an error rather than a semver accident: an upstream step's parents are the mapped catalog entries, never the current pin.

| Item | Cut |
| --- | --- |
| 1. Number the patch above its parents | §3.1, the `1000+` tier |
| 2. The slot warning re-derives resolution | §3.2 and §4.3 |
| 3. The reset recipes are one rule | §2.2 |

---

## 1. Number the Patch above Its Parents

§3.1 reserves `major >= 1000` so an old patch beats a later upstream version and the diff stays empty. `P(1001.0.0)` and `U(1.2.0)` are siblings. `1001.0.0 > 1.2.0`, so the patch keeps the slot and §4.3 emits nothing.

Number the patch the way `updateSchema` already numbers an update: strictly above the versions at its `at`, and nothing else. When upstream later writes that slot with a higher version, the old patch loses. `wanted` still has the local rule, because `readSource` ran the `ALTER` on the new upstream. `diffTables(candidateView, wanted)` emits a new patch on top of the merge. That is the same reconciliation §3.2 already describes for the case where the author kept the local `SET`. The empty-diff case becomes rarer. The catalog gains one schema entry on an upgrade that touches a patched slot. Catalog releases are rare enough that the entry is cheaper than the reserved range.

Deleting the range also deletes the pack-version ban, the `1000+` comments in `build/update.sql`, and the `rpack verify` annotation.

What does not get deleted is the produce rule in §4.3. An upstream step's `at` is the catalog entries §4.1 mapped, `parentStepHashes` is only for steps this draft emits, and an empty parent set is an error. `produceRelease` must not fill that hole from `pins`. With ordinary version numbers the hole fails open: `1.2.0` is above a patch numbered `1.0.1`, `validateUpdate` accepts it, and the pack step then has the local patch as an ancestor. The `1000+` major is what makes that mistake fail the semver check. An explicit error is the smaller guard, and it is required either way.

## 2. The Slot Warning Re-Derives Resolution

§3.2 and §4.3 tell the planner to find every slot whose causally maximal `1000+` write at `candidateFrontier` comes from any patch ancestor, not only the patch head, and to warn when a new upstream step wrote that slot and `patchRules` does not.

That walk is a second `resolve.ts`. It changes no entry. When `candidateView` and `wanted` differ, the diff already emits the repairing patch. When they match, the warning's only job is to say that the match is the old patch hiding an upstream edit.

Compare two views. Take the slots the new upstream steps wrote. If `candidateView` and the pure upstream view differ on one of them, and `patchRules` does not set it, the patch lineage is overriding that step. Same message, no ancestor scan. Under §1 the override usually becomes a new patch instead of a warning, and this section goes away with the reserved majors.

## 3. The Reset Recipes Are One Rule

§2.2 gives two procedures for a column whose type changes. They are one rule, applied to two kinds of column.

The `ALTER` in `target-catalog.sql` is applied to the upstream tables on every release, including a release whose `upgrade-manual.sql` is empty. `upgrade-manual.sql` is the only drop authorized against the schema already deployed.

- A column that exists in the upstream tables needs `DROP COLUMN` then `ADD COLUMN` in the `ALTER`, so the next import still applies. The release that first resets the deployed column also puts `DROP COLUMN` in `upgrade-manual.sql`.
- A column that exists only because an older patch changed its `ADD COLUMN` in the `ALTER`. The `DROP` for the deployed column is only in `upgrade-manual.sql`.

`readSource` already has the diagnostic that separates them: `ADD COLUMN t.c` failed because `t.c` is in the upstream tables. That hint is the procedure. The two recipes do not need to grow, and a generated `ALTER` in §6 should follow the same rule rather than inventing a third way to spell the drop.
