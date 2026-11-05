# Critique of the Import Patches Proposal, v2

A review of [IMPORT_PATCHES.md](./IMPORT_PATCHES.md) after it absorbed [IMPORT_PATCHES_CRITIQUE.md](./IMPORT_PATCHES_CRITIQUE.md). The replay rationale, the parent map, the union of pack files, the `produceRelease` pin check, and forbidding `PARAMS` on a local patch are in place. Parameter ranking stays ordinary semver, which is already what `rdb` does. The items below are what that revision still gets wrong.

## Verdict

Five items. Two will reject a legal upgrade or a column reset. One attaches a new upstream step to the patch. One misses a slot that an older patch still owns. One turns a column-type change into a silent reset while rewriting `target-catalog.sql`.

| Item | Where it bites |
| --- | --- |
| 1. §3.3 is already the code | `foldGroupParamWrites`, `resolveGroupParams` |
| 2. A column reset never reaches the planner | §2.2 then §4.3 |
| 3. A later upstream step is parented on the patch | §4.3 and `produceRelease` |
| 4. The slot warning looks at the patch head only | §4.3 |
| 5. `writeSource` resets a column in the generated `ALTER` | §6 |

---

## 1. §3.3 Is Already the Code

§3.3 says a causally later param write wins even when `getVersions()[0]` stays `1001.0.0`, and that semver is only a tie-break among concurrent writes. §8 then lists a change to `foldGroupParamWrites` and `resolveGroupParams`.

That order is already both functions. The catalog fold drops any write whose release is in another write's `ancestors`, and only then calls `outranksCatalogParamWrite`. The group fold reduces the deploy set with `findMinimalCover` before it compares semver, target hashes, and entry hash. A value deployed at `[P(1001.0.0), U(1.1.0)]` is a descendant of the value deployed at `[P(1001.0.0)]`, so the earlier write is dropped. The shared label `1001.0.0` is not consulted.

Two boundaries the section's wording crosses:

- A later release supersedes an earlier value only when the later release writes that parameter. Advancing the pin without a new `params` entry leaves the old write as the only maximum. The value is not retagged.
- Concurrent writes still compare raw semver. A value deployed at a `1001.0.0` pin beats a concurrent value deployed at `2.0.0`. That is the ordinary tie-break, and `rdb` should not grow a special case for it.

Drop the `modules/rdb` edit from §8.

## 2. A Column Reset Never Reaches the Planner

§2.2 applies the `ALTER` in `target-catalog.sql` to the resolved upstream state inside `readSource`, and refuses a rule that does not apply there. §4.3 applies `upgrade-manual.sql` later, on `candidateView`, and only then diffs toward `wanted`.

A type change is `DROP COLUMN` followed by `ADD COLUMN`. The drop belongs in `upgrade-manual.sql`. The scratch schema still has the old column when `readSource` runs `ADD COLUMN`, so the add is rejected and drafting never starts. `diffTables` refusing an in-place retype does not help: `wanted` was already computed without the new type.

On an inline schema this works because `target-catalog.sql` states the whole column and the manual drop runs before the diff. For an import, `readSource` has to apply `upgrade-manual.sql` to the upstream state before it applies the patch `ALTER`. Otherwise a column reset on an imported schema is inexpressible.

## 3. A Later Upstream Step Is Parented on the Patch

§4.3 queues a missing upstream step with `parentStepHashes / at`. In `produceRelease` (`modules/rpack/src/produce.ts`), `parentStepHashes` resolves only through `stepToEntry` entries created while producing this release. A parent replayed by an earlier release is not in that map. With `at` empty, the fallback is the schema's current pin:

```ts
if (atHashes.length === 0) atHashes.push(...(pins.get(step.schema) ?? base!.schemas.get(step.schema)!.at));
```

After a local patch that pin is `P(1001.0.0)`. The new `1.1.0` step is appended there. `validateUpdate` requires the new version to be above every version at `at`, and `1.1.0 > 1001.0.0` fails, so the upgrade is rejected.

Parents that §4.1 has already mapped belong in `at` as those catalog entry hashes. `parentStepHashes` is only for steps this same plan emits (the first import, where genesis and the later pack steps are created together). `produceRelease` must not fall back to the current pin for an upstream step.

## 4. The Slot Warning Looks at the Patch Head Only

§4.3 warns when a new upstream step and a surviving `1000+` patch entry wrote the same slot and `patchRules` does not rewrite it. "Surviving patch entry" reads as the patch head.

A write on an older patch stays maximal when a later patch did not touch that slot. At `[P(1001.1.0), U(1.2.0)]`, with `P(1001.1.0)` above `P(1001.0.0)` and `U(1.2.0)` not above it:

- `P(1001.0.0)` wrote `set-restrictions` on `comments`.
- `P(1001.1.0)` added a column and did not write restrictions.
- `U(1.2.0)` wrote restrictions.

The restrictions maxima are `P(1001.0.0)` and `U(1.2.0)`. `1001.0.0 > 1.2.0`, so the old patch still wins. The head `P(1001.1.0)` did not write the slot, so a check of the head emits neither a warning nor a repairing rule.

The check has to use the patch writes that are still maximal on that slot at `candidateFrontier`, not the rules of the latest patch entry.

## 5. `writeSource` Resets a Column in the Generated `ALTER`

§6, when a catalog merge cannot keep the highest parent's `ALTER`, builds one `ALTER` from the pure upstream tables to the merged schema. A column whose definition changed is emitted as `DROP COLUMN` plus whatever `diffTables` then adds.

That reproduces the tables. It is also a reset: `columnIncarnationId` includes the column's drop generation, so values written under the old column do not cross the drop. Inline editing leaves that reset in `upgrade-manual.sql`, where it is visible as a migration the author chose. A generated `ALTER` in `target-catalog.sql` hides it inside the patch the next release will treat as the desired local rules.

The same block does not cover an identity-provider change. `diffTables` still refuses those after the column drops. §6 should say so, and a generated column reset should be written to `upgrade-manual.sql` (or called out as a reset) rather than folded into the `ALTER`.

## Noted, not blocking

§4.1 compares `note` on every normal entry, including the genesis. `RSchemaImpl.create` has no `note` field, so a pack genesis step that carries a note will not match the catalog create. Compare `note` on `schema-update` entries only, unless create payloads grow a note.
