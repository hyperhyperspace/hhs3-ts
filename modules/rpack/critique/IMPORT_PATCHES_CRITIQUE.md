# Critique of the Import Patches Proposal

A review of [IMPORT_PATCHES.md](./IMPORT_PATCHES.md) against the current `rdb` and `rpack` code. The two-tier version split is sound, and pinning a group at several schema heads does merge them with the rule `RSchema` already uses. The items below are assumptions in the proposal that the code does not support.

## Verdict

Multi-version import and a local patch tier can work without changes to `semver.ts` or `validate_ops.ts`. A patch version `(1000 + catalogMajor).catalogMinor.catalogPatch` is strictly above every upstream version and every patch from an earlier catalog release, so `validateUpdate` accepts it.

What does not hold is the claim that this falls out of replaying pack steps and diffing. Identifying an existing upstream entry needs more than `payload.version`. A `1000+` head permanently beats later upstream writes to the same slot, and that semver leaks into parameter ranking. The `ALTER SCHEMA` in `target-catalog.sql` is run on a different base than the one the planner diffs. And `produceRelease` rejects both the multi-step replay and the two-head pin the examples depend on.

| Item | Where it bites |
| --- | --- |
| 1. Incarnation ids do not require bit-exact replay | §1, motivation |
| 2. A catalog entry has no step hash | §4.1, matching upstream entries |
| 3. Two versions are two pack files | §2.1, multi-version import |
| 4. A patch head outranks every future upstream write | §3, examples B and C |
| 5. The scratch `ALTER` and the planner see different bases | §2.2 and §4.3 |
| 6. A concurrent patch column survives an upstream table reset | §3.3, the sibling edges |
| 7. `produceRelease` rejects these pins | §4, after the planner |
| 8. "Imported" is reconstructed, and the reconstruction can lie | §4.1, no inline/import switch |

---

## 1. Incarnation Ids Do Not Require Bit-Exact Replay

§1 says imported steps must be replayed as-is, never flattened, because `tableIncarnationId` hashes the birth `TableDef` plus a drop count, and slot conflicts resolve by the entry's `version`.

Those ids are local to the catalog's `RSchema`. The pack never becomes that object: the catalog signs a new schema, and `schemaStepHash` does not cover incarnation ids, creators, or entry hashes. Two catalogs that both import `hhs:comments` `1.1.0` already have different schema objects. Nothing compares their incarnation ids.

Replay is load-bearing for a different reason. A later pack step was written against its parent's state, and a group that already deployed an earlier pin keeps the birth def of that pin when it moves forward. A catalog that only ever pins the imported tip can create that tip as one schema entry. Its later deploys stay consistent with that entry's birth def. They do not need the pack's intermediate entries, and they do not need those entries' incarnation ids to equal a scratch replay of the pack.

Flattening is what blocks a *later* concurrent import. Once `1.2.0` is a single entry whose past is not the pack's `1.1.0`, a pack step that forked at `1.1.0` has no parent to attach to. That is an applicability problem, not an incarnation problem. The motivation section should say that.

## 2. A Catalog Entry Has No Step Hash

§4.1 says a normal entry's step hash can be recomputed from its payload (`parents`, `version`, `params`, `tables` / `migration`, `note`) with no replica, and that this replaces `findMirroredPackSteps`.

`schemaStepHash` does include `parents`, and those are pack step hashes:

```ts
// modules/rpack/src/format.ts
{
    domain: 'hhs3-rpack-schema-step-v1',
    name,
    version: step.version,
    parents: [...step.parents].sort(),
    // params, tables, migration, note
}
```

A stored `schema-update` is a different object (`modules/rdb/src/rschema/payload.ts`): `version`, `migration`, `params`, `note`, `author`, `signature`. Its causal parents are catalog entry hashes in the DAG header. Author and signature are not part of the step hash. The pack parent hashes are not stored anywhere on the entry.

Version equality finds a candidate, because pack versions are unique. Checking that the catalog entry's `prevs` are the entries that replayed those pack parents is still a parent map. Content comparison of `migration` / `tables` / `params` / `note` can be done from the payload. The parent check cannot. Dropping the scratch replica is fair. Dropping the link between an entry and the pack step it came from is not.

## 3. Two Versions Are Two Pack Files

§2.1 treats `wanted.origin.steps` as the causal closure of every version named in one `IMPORT SCHEMA`. Each schema `.rpack` carries the past of its own `manifest.release` only. `1.0.1` and `1.1.0` forked from `1.0.0` live in two files. Neither file contains the other head.

The closure is the union of those files, deduplicated by step hash on the shared prefix. `rpack.json`'s `imports[name][version]` can lock both releases. `readLockedImports` already indexes `name@version`. The proposal should say the union, and what to do when the shared prefix's step hashes disagree. That disagreement means the two files are not one pack history, and replaying both into one `RSchema` would fork the prefix into two genesis-descendant chains.

`IMPORT SCHEMA name VERSION '1.0.0', '1.1.0'` is correctly rejected when one version is an ancestor of the other. The same check has to run on the union, not on a single file's `steps` array.

## 4. A Patch Head Outranks Every Future Upstream Write

`resolveSlot` ranks concurrent writes by semver, then entry hash (`modules/rdb/src/rschema/resolve.ts`). `1001.0.0 > 1.1.0` and `1001.0.0 > 50.0.0`. For as long as the old patch stays a head, it wins every slot it wrote, against every upstream step the reserved range still excludes.

That is the whole of example C, and it is permanent. `fks` and `restrictions` are one slot each, not a merge of rules. An upstream `1.2.0` that adds a foreign key or replaces allow-rules loses the entire map to a patch that wrote that slot at catalog `1.0.0`. Keeping the same `ALTER` text makes the planner emit nothing, so the upstream change disappears with no warning. Adopting it takes a new `1000+` entry whose rule copies the upstream value.

The same semver leaks into parameter ranking. `RSchemaView.getVersions()` is highest first. On the example B pin `[P(1001.0.0), U(1.1.0)]` the first version is `1001.0.0`. Catalog param folding stores `getVersions()[0]` as the write's schema semver (`modules/rdb/src/rcatalog/rcatalog.ts`, `recordSemver`). A deploy aimed at that two-head frontier outranks a deploy aimed at any pure `1.x` pin, including a pin from a later catalog release that did not touch the patch.

If the patch tier is going to own the top of the order, param ranking needs to use the highest *upstream* version in the pin, or the catalog release version, and the planner should warn when a new upstream step wrote a slot the surviving patch also wrote.

## 5. The Scratch `ALTER` and the Planner See Different Bases

§2.2 calls the `ALTER SCHEMA` in `target-catalog.sql` a declaration of the schema at this catalog version, and then says `readSource` *runs* those rules on a scratch schema after replaying the pack. §4.3 diffs a third state: `candidateView` at `minimalCover(baseSchema.at ∪ targetUpstreamEntries)`, which is the old patch merged *beside* the new upstream.

Those bases agree for an additive column the new upstream did not touch. That is example B. They diverge as soon as the script is re-run on the new upstream:

- `ADD COLUMN comments.flagged` fails in the scratch schema when `1.1.0` already added `flagged`. `validateUpdate` rejects a column that exists. The author cannot keep the old `ALTER` text, which is the example B edit.
- `SET` of foreign keys or allow-rules replaces the whole slot. A script written against `1.0.0`, applied causally on `1.1.0`, erases keys and rules that `1.1.0` added. The scratch `wanted` then lacks them, and the diff will not put them back.
- A parameter the old patch declared cannot be dropped or retyped. `diffSchemaParams` refuses both (`modules/rpack/src/schema_diff.ts`). If upstream later declares the same name, the scratch update rejects the redeclaration (`schema-update parameter is already declared`), and the old patch definition keeps winning because `1001 > 1.x`. The author has no edit that adopts upstream's definition.

The inline path does not have this split. `target-catalog.sql` states the whole schema, and `upgrade-manual.sql` is the only script. Here the same `ALTER` block is asked to be the desired state and a migration against whatever upstream was just imported. It can be one of those. The planner's diff already produces the migration. The file should be the desired patch against the imported heads, and a rule that does not apply on the new upstream should be a refusal with a pointer at `upgrade-manual.sql`, which §4.3 already allows for imported schemas.

§6 has the same hole on a catalog merge. When the highest parent's `ALTER` does not reproduce the merged schema, `writeSource` builds one `ALTER` via `diffTables`. A column whose type changed is a `diffTables` refusal, not a rule. The proposal does not say what `writeSource` writes then. The inline editor rewrites the full `CREATE SCHEMA`, which can represent the column, and leaves the reset for `upgrade-manual.sql`. A single generated `ALTER` cannot.

## 6. A Concurrent Patch Column Survives an Upstream Table Reset

§3.3 draws the patch as a sibling of each new upstream step, appended at the previous upstream entry. `resolve.ts` masks a subordinate write only when it is causally at or below the winning `add-table`:

```ts
const survives = (w: SlotWrite): boolean =>
    !winningAdds.some((add) =>
        w.entryHash === add.entryHash
            ? w.ruleIndex < add.ruleIndex
            : causality.ancestors[add.entryIdx].has(w.entryIdx));
```

A local `add-column` whose parent is `U(1.0.0)` is concurrent with an upstream `drop-table` / `add-table` at `1.1.0`. It is not an ancestor of that add, so it survives and lands on the reset table. The new table's birth def never mentioned the column. Rows born under the old incarnation are a different incarnation and will not see it. The proposal's examples never reset a table. The sibling edges do this anyway.

Stacking the upstream reset *on* the patch would mask the column. The proposal forbids that, correctly, because an upstream step must not have a `1000+` parent. The consequence belongs in the plan: a local column, foreign key, or allow-rule written against the old table is still in the resolved schema after upstream replaces the table, until a new patch drops it.

## 7. `produceRelease` Rejects These Pins

The planner emits several upstream steps plus at most one patch, and example B pins the schema at two heads. `produceRelease` (`modules/rpack/src/produce.ts`) publishes, then requires every emitted step's version to be the single final head:

```ts
for (const step of draft.schemas) {
    const versions = after.schemas.get(name)?.view.getVersions() ?? [];
    if (versions.length !== 1 || versions[0] !== step.version) {
        throw new ProduceError(`schema ${name} is at ${versions.join(', ') || 'no version'} ...`);
    }
}
```

A first import that creates `1.0.0` and then `1001.0.0` fails, because the genesis step's version is not the head. Example B's pin `[P_1, U_1.1.0]` fails `versions.length !== 1`. §8 lists `produce.ts` and does not mention this assertion. It runs after `publishRelease`, so the thrown release is already appended to the released replica and is not recorded in `rpack.json`.

The fixpoint should compare the resolved schema at the plan's pin with `wanted`, and accept a pin of several heads. It should not require each replayed step to be that pin.

## 8. "Imported" Is Reconstructed, and the Reconstruction Can Lie

§4.1 has no origin bit in the schema payload. "This schema was imported" is inferred by matching every `major < 1000` entry to `wanted.origin.steps`. That inference goes both ways:

- An inline genesis whose name, version, params, tables, and note hash like a pack genesis is accepted as that upstream step. Switching that inline schema to `IMPORT SCHEMA` is then allowed. The outlaw depends on the hashes differing.
- An imported step whose stored `note` or param encoding differs from the pack step fails the match and is refused as inline. Further imports of a schema this catalog already imported stop working.

A durable mark has to be something the catalog entry actually stores, or a fact kept next to the release (the `imports` lock plus the set of step hashes this catalog has replayed). Hashing the payload and hoping is the same guess `findMirroredPackSteps` makes, with a narrower key.
