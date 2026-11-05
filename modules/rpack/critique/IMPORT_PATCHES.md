# Imported Schema DAGs, Multi-Version Imports, and Local Patches

*(Updated to incorporate findings from [`IMPORT_PATCHES_CRITIQUE.md`](./IMPORT_PATCHES_CRITIQUE.md), [`IMPORT_PATCHES_CRITIQUE_v2.md`](./IMPORT_PATCHES_CRITIQUE_v2.md), and Items 2–3 of [`IMPORT_PATCHES_CRITIQUE_v3.md`](./IMPORT_PATCHES_CRITIQUE_v3.md).)*

---

## 1. Motivation & Design Summary

When a catalog imports an external schema pack (`IMPORT SCHEMA`), we want three properties to hold simultaneously:

1. **Preserving causal step history for rule applicability and branching**:
   Each step in a schema pack (`SchemaReleaseFile.steps`) was authored and validated against its parent step's table state, and concurrent upstream branches rely on their separate entries and upstream `version` numbers for per-slot LWW resolution (`modules/rdb/src/rschema/resolve.ts`). While `tableIncarnationId` (`incarnation.ts`) is local to each catalog's `RSchema` object, flattening a multi-step pack (`1.0.0 -> 1.1.0 -> 1.2.0`) into a single entry would mean a later catalog release importing a branch that forked at `1.1.0` has no `1.1.0` parent entry in the catalog's `RSchema` DAG to attach to, and a group already deployed at an earlier pin keeps that pin's birth definition as it advances. Replaying the pack's `steps` as-is into the catalog's `RSchema` preserves causal attach points and upstream branch versions.
2. **No `inline <-> import` switching (with durable origin tracking)**:
   A schema in a catalog is either **inline** (`CREATE SCHEMA`) for its entire lifetime or **imported** (`IMPORT SCHEMA`) for its entire lifetime. We outlaw converting an inline schema into an imported schema or vice versa. Rather than guessing whether a schema was imported by comparing flattened table views in a scratch replica (`findMirroredPackSteps`), `rpack` tracks imported schemas via `rpack.json`'s `"imports"` lock and verifies the causal parent map of upstream entries in `baseSchema.object`.
3. **First-class local patches (`ALTER SCHEMA` after `IMPORT SCHEMA`) and multi-version imports**:
   Instead of ejecting an imported schema into an inline `CREATE SCHEMA` to customize it, a catalog can follow `IMPORT SCHEMA` with local `ALTER SCHEMA` statement(s) in `target-catalog.sql`. Local modifications live in a reserved patch version tier (`major >= 1000`), causally layered on top of the upstream step DAG (`major < 1000`).

---

## 2. Surface Syntax in `target-catalog.sql`

### 2.1 Multi-Version `IMPORT SCHEMA` Across Pack Files

In `target-catalog.sql` (when `catalogVersionOptional` is enabled), `IMPORT SCHEMA` accepts one or more comma-separated semver strings:

```sql
IMPORT SCHEMA <name> VERSION '<v1>' [, '<v2>', ...];
```

- **Union of pack files**:
  Each `.rpack` file in `imports/` (`<name>-<ver>-<tag>.rpack`) ships only the causal past of its own `manifest.release`. When `IMPORT SCHEMA` names multiple versions (e.g. `VERSION '1.0.1', '1.1.0'`), `readSource` loads the locked `SchemaReleaseFile` for each listed version from `imports/` (`imports.get(`${name}@${v}`)`), forms the **union of their `steps` arrays deduplicated by `step.hash`**, and orders the union with `canonicalEntryOrder`.
- **Shared-history validation on the union**:
  1. **Single genesis**: The union of `steps` must contain **exactly one** genesis step (`parents.length === 0`). If two imported files for `<name>` have different genesis step hashes, `readSource` refuses:
     `imported versions of '<name>' do not share a common genesis step`.
  2. **Unique step hash per version**: No two steps in the union may have the same `step.version` with different `step.hash`es. If the shared prefix disagrees on any version's hash, `readSource` refuses:
     `imported versions of '<name>' disagree on step version '<v>'`.
  3. **Antichain check**: On the union step DAG, no listed version $v_i$ may be in the causal past of another listed version $v_j$ (e.g. `VERSION '1.0.0', '1.1.0'` when `'1.0.0'` is an ancestor of `'1.1.0'` is rejected with a hint to keep only `'1.1.0'`).
- **Merging in `readSource`**:
  `readSource` replays the union `steps` DAG into its scratch `RSchema` and evaluates `schema.getView(version(entry(v1), entry(v2), ...))` so Rdb's `RSchema` CRDT merges the concurrent upstream heads into the base `{ upstreamTables, upstreamParams }` state.

### 2.2 Local `ALTER SCHEMA` Patches in `target-catalog.sql`

After `IMPORT SCHEMA <name> VERSION ...;`, `target-catalog.sql` may contain one or more `ALTER SCHEMA <name> AS (...)` statements that declaratively specify the desired patch on top of the imported upstream heads:

```sql
IMPORT SCHEMA hhs:comments VERSION '1.0.0';

ALTER SCHEMA hhs:comments AS (
  ADD COLUMN comments.flagged boolean DEFAULT false,
  SET ALLOW RULES ON comments (
    ALLOW insert IF comments.len <= :maxLen,
    ALLOW delete IF EXISTS mod.caps WHERE mod.caps.label = 'moderator' AND mod.caps.grantee = $author
  )
);
```

**Rules for `ALTER SCHEMA` in `target-catalog.sql`**:
1. **Target must be an imported schema**: `ALTER SCHEMA <name>` in `target-catalog.sql` is only permitted when `<name>` was declared via `IMPORT SCHEMA <name>` earlier in `target-catalog.sql`. If `<name>` was declared via an inline `CREATE SCHEMA`, `readSource` refuses with:
   `schema <name> is defined inline in target-catalog.sql; edit its CREATE SCHEMA statement directly (or put migration resets in upgrade-manual.sql)`.
2. **No `VERSION`, `PARAMS`, `NOTE`, `BY`, or `AT` clauses**:
   - Local patch entries get their version numbers (`1000+`), notes, signing author, and causal `AT` parents automatically from the catalog release.
   - `PARAMS (...)` is **forbidden** on local `ALTER SCHEMA` patches in `target-catalog.sql`. Schema parameters cannot be dropped or retyped once declared (`diffSchemaParams`), so allowing a `1000+` patch to declare a schema parameter would permanently block or conflict with any future upstream parameter of the same name (`1001 > 1.x`). Because `target-catalog.sql` is already specific to one catalog, local `ALTER SCHEMA` rules can use literals or script-level `CONSTANT :name = <literal>;` directly.
3. **Evaluation against the imported upstream state in `readSource`, and the single column-reset rule**:
   - In `readSource`, the `ALTER SCHEMA <name> AS (...)` rules in `target-catalog.sql` are validated and applied sequentially on top of the resolved upstream state (`upstreamTables`, `upstreamParams`) using `applyMigrationRules` (and applied in the scratch `RdbRuntime` so subsequent `CREATE CATALOG` clauses bind against the patched schema).
   - One rule governs column type/modifier resets: **`target-catalog.sql`'s `ALTER SCHEMA` transforms `upstreamTables` into `wanted.tables` on every release, while `upgrade-manual.sql` authorizes destructive drops against the already-deployed schema (`candidateView`)**:
     - If column `t.c` exists in `upstreamTables`, replacing its definition requires `DROP COLUMN t.c, ADD COLUMN t.c <newType>` in `target-catalog.sql`'s `ALTER SCHEMA` (so `target-catalog.sql` still applies on future releases when `upgrade-manual.sql` is empty). If `t.c` exists only because an older local patch added it, `target-catalog.sql`'s `ALTER SCHEMA` simply states `ADD COLUMN t.c <newType>`.
     - In both cases, the release that drops the already-deployed column puts `ALTER SCHEMA <name> AS (DROP COLUMN t.c);` in `upgrade-manual.sql`.
     - When `ADD COLUMN t.c` in `target-catalog.sql` fails in `readSource` because `t.c` is in `upstreamTables`, the diagnostic hints to precede `ADD COLUMN t.c` with `DROP COLUMN t.c` (or, if upstream newly adopted a column that was previously in the local patch, to remove the redundant `ADD COLUMN t.c`).
   - Note that `SET FOREIGN KEYS` and `SET ALLOW RULES` in `target-catalog.sql`'s `ALTER SCHEMA` replace the entire `fks` or `restrictions` slot of that table on top of the imported upstream state. That patched result becomes `wanted.tables` for this catalog version.
   - At the end of `readSource`, `SchemaModel` holds:
     - `tables` and `params`: the final desired state (`wanted`: `upstream + local patches`).
     - `origin`:
       ```ts
       export type SchemaOrigin =
           | { kind: 'inline' }
           | {
               kind: 'imported';
               versions: string[];          // the version(s) named in IMPORT SCHEMA
               steps: SchemaReleaseStep[];  // topological union of causal closures of `versions`
               // The resolved upstream state at `versions` (before local ALTER SCHEMA).
               upstream: { tables: Map<string, TableDef>; params: Map<string, SchemaParamDecl> };
           };
       ```

---

## 3. Two-Tier Version Space, Slot Precedence, & Parameter Ranking

### 3.1 Version Space Partition (`0..999` vs `1000+`)

Standard `major.minor.patch` semver strings are partitioned by `major`:

| Tier | Semver Range | Purpose | Allowed Causal Parents (`prevs`) |
| :--- | :--- | :--- | :--- |
| **Normal (Upstream / Inline)** | `0 <= major < 1000` | Schema pack steps and inline catalog schemas | Only normal (`major < 1000`) entries |
| **Local Patch** | `major >= 1000` | Catalog-local patches on top of an imported schema | Normal (`major < 1000`) entries and/or earlier local patch (`major >= 1000`) entries |

- **Enforcement on normal versions**:
  In `schema_pack.ts` (`readSchemaSource` / `draftSchemaRelease`), `checkSchemaReleaseFile` (`format.ts`), and inline schemas in `source.ts` / `draft.ts`: any stated or release version with `major >= 1000` is rejected (`major version 1000 and above is reserved for local schema patches`).
- **Patch version formula**:
  For a catalog release with version `C = <catMajor>.<catMinor>.<catPatch>` (`catMajor < 1000`), the patch version for any local patch entry created in release `C` is:
  $$\text{patchVersion}(C) = (1000 + \text{catMajor}).\text{catMinor}.\text{catPatch}$$

### 3.2 Slot Precedence, Sibling Edges, & Planner Reconciliation

```text
Upstream lineage (< 1000):    U(1.0.0) ────────► U(1.1.0) ────────► U(1.2.0)
                                 │                  │                  │
Patch lineage (1000+):           └───► P(1001.0.0) ─┴───► P(1001.1.0) ─┴───► P(1001.2.0)
```

Because `P(1001.0.0)` (parent `U(1.0.0)`) and `U(1.1.0)` (parent `U(1.0.0)`) are **concurrent siblings** when evaluated at the join frontier `candidateFrontier = [P(1001.0.0), U(1.1.0)]`, two important `RSchema` resolution behaviors (`modules/rdb/src/rschema/resolve.ts`) occur at `candidateFrontier`:

1. **A `1000+` patch head outranks concurrent `< 1000` upstream writes on shared slots**:
   In `resolve.ts`, concurrent writes to the same slot (`fks`, `restrictions`, `concurrentDeletes`, or same-named `column` with different defs) are ranked by entry semver (`1001.0.0 > 1.1.0`). Thus, at `candidateFrontier = [P(1001.0.0), U(1.1.0)]`:
   - If `P(1001.0.0)` wrote `set-restrictions` on `comments` (`R_patch`) and `U(1.1.0)` also wrote `set-restrictions` on `comments` (`R_upstream_110`), `candidateView` at `[P(1001.0.0), U(1.1.0)]` resolves `restrictions` to `R_patch` (not `R_upstream_110`).
   - **How the planner handles this**:
     - If in `1.1.0` the developer *removed* `SET ALLOW RULES ON comments` from `target-catalog.sql`, then `wanted` has `R_upstream_110` while `candidateView` has `R_patch`. `diffTables(candidateView, wanted)` sees the difference and **emits `set-restrictions: R_upstream_110` in a new patch entry `P(1001.1.0)`** (causally after both `P(1001.0.0)` and `U(1.1.0)`), restoring upstream's restriction.
     - If in `1.1.0` the developer *kept* `SET ALLOW RULES ON comments (R_patch)` in `target-catalog.sql`, both `candidateView` and `wanted` have `R_patch`. However, because `U(1.1.0)` also modified that same slot (`r:comments`), the planner **emits a warning**:
       `schema <name>: imported 1.1.0 changes restrictions of table comments, which is overridden by the local ALTER SCHEMA patch`.
       - **How the warning is computed (without re-deriving `resolve.ts`)**: Take the slots touched by the newly added upstream steps (`slotsTouchedByRule`), remembering which of those steps touched each slot. If `wanted` differs from the pure `upstreamView` on one of those slots, the local patch — kept, edited, or added in this release — is overriding that upstream change. A removed override leaves `wanted` equal to `upstreamView` on the slot, so it does not warn. The message names only the queued versions that touched the slot.
2. **A `1000+` patch column/setting survives a concurrent upstream table reset**:
   In `resolve.ts`, a subordinate write (`add-column`, `set-fks`, `set-restrictions`) is masked by a winning `add-table` reset **only** if it is causally at or below that `add-table`. Because `P(1001.0.0)` (parent `U(1.0.0)`) is concurrent with an upstream `drop-table` + `add-table` in `U(1.1.0)`, `P(1001.0.0)`'s subordinate writes on that table are **not** masked at `candidateFrontier = [P(1001.0.0), U(1.1.0)]`—they survive onto the reset table in `candidateView`.
   - **How the planner handles this**:
     - In `readSource`, `wanted` is computed by running `target-catalog.sql`'s `ALTER SCHEMA` causally *after* `U(1.1.0)`.
     - Any column, FK, or restriction from `P(1001.0.0)` that is no longer in `wanted` will be present in `candidateView` and absent/different in `wanted`, so `diffTables(candidateView, wanted)` automatically emits `drop-column` / `set-fks` / `set-restrictions` in `P(1001.1.0)` to clean it up.
     - If the new upstream step `U(1.1.0)` dropped and re-added a table that `P(1001.0.0)` modified, the planner emits a warning noting that `U(1.1.0)` reset the table. A `drop-table` or `add-table` in a newly queued upstream step counts as touching that table's existence, mode, foreign-key, and restrictions slots (`t:`, `m:`, `f:`, `r:`) and every column slot (`c:<table>:*`) present in `candidateView`, `upstreamView`, or `wanted`, so a surviving patch column or a column added this release on the reset table is warned about even though `slotsTouchedByRule('drop-table')` names only the existence slot.

### 3.3 Causal Dominance in Group Parameter Folding (Already in `modules/rdb`)

At `[P(1001.0.0)]`, `view.getVersions()[0]` is `'1001.0.0'`. When a subsequent release advances to the two-head frontier `[P(1001.0.0), U(1.1.0)]`, `view.getVersions()` is `['1001.0.0', '1.1.0']`, so `view.getVersions()[0]` remains `'1001.0.0'`.

No changes are needed in `modules/rdb`:
- `foldGroupParamWrites` (`modules/rdb/src/rcatalog/resolve.ts`) and `resolveGroupParams` (`modules/rdb/src/rtable_group/group.ts`) **already** filter per-parameter writes to causally-maximal writes first (dropping release ancestors in `foldGroupParamWrites`, and reducing with `findMinimalCover` in `resolveGroupParams`) before calling `outranksCatalogParamWrite` / comparing `(targetSchemaSemver, targetSchemaEntryHashes, entryHash)`.
- Thus, a parameter value written in a later catalog release or group deploy at `[P(1001.0.0), U(1.1.0)]` is a causal descendant of the write at `[P(1001.0.0)]` and automatically supersedes it without consulting `1001.0.0`. (Advancing the schema pin without writing that parameter leaves the earlier write as the unique causal maximum; concurrent parameter writes across branches compare raw `targetSchemaSemver` as usual.)

---

## 4. The Planner Algorithm (`draftRelease` in `modules/rpack/src/draft.ts`)

### 4.1 Durable Origin Check & Topological Parent-Map Matching

1. **Origin check (no `inline <-> import` switching)**:
   - `rpack.json`'s `"imports"` lock records every imported schema name (`config.imports[name] !== undefined`).
   - Additionally, if `baseSchema` exists:
     - If `wanted.origin.kind === 'imported'`, `<name>` must be present in `config.imports`, and every normal (`major < 1000`) entry at or below `baseSchema.at` must match a step in `wanted.origin.steps` via the topological parent map below. `baseSchema.object` also holds entries published by installed releases outside the parents; those are mapped when they match a step and skipped otherwise, and so is any entry whose parent was skipped.
     - If `wanted.origin.kind === 'inline'`, `<name>` must **not** have any `major >= 1000` entries in `baseSchema.object` and must **not** be in `config.imports` (if it is, `draftRelease` refuses: `schema <name> was imported; keep IMPORT SCHEMA and use ALTER SCHEMA <name> in target-catalog.sql for local modifications`).

2. **Topological parent-map matching of normal (`major < 1000`) entries**:
   Because an `RSchema` entry's `prevs` are catalog entry hashes while `schemaStepHash` uses pack step hashes, we walk the normal (`major < 1000`) entries of `baseSchema.object` in topological order (`canonicalEntryOrder`):
   - Maintain two maps: `entryToStep = new Map<B64Hash, B64Hash>()` and `stepToEntry = new Map<B64Hash, B64Hash>()`.
   - For each normal entry $e$ (`major < 1000`) in topological order:
     - Every parent $p \in e.\text{prevs}$ must be in `entryToStep` (a normal entry may never have a `1000+` parent).
     - Look up the pack step $s \in \text{wanted.origin.steps}$ with `s.version === e.payload.version`. If none exists and $e$ is a genesis entry, refuse (`schema <name> was defined inline or has a version '<v>' not in the imported package history`). If none exists and $e$'s parents already mapped to imported steps, refuse (`schema <name>: imported version(s) '<v1>', ... do not have version '<v>' in their history, and a parent release imported it`). An entry outside `baseSchema.at` that does not match is skipped instead.
     - Check that `[...e.prevs].map(p => entryToStep.get(p)!).sort()` equals `[...s.parents].sort()`, and that $e$'s payload (`tables` and `params` for `create`; `migration`, `params`, and `note` for `schema-update`—`CreateRSchemaPayload` has no `note` field, so `note` is compared on `schema-update` entries only) equals $s$'s. If not, refuse.
     - Record `entryToStep.set(e.hash, s.hash)` and `stepToEntry.set(s.hash, e.hash)`.
   - Verify that every normal entry at or below `baseSchema.at` is accounted for in `wanted.origin.steps` (ensuring the new import is at or above all previously imported upstream steps of the parents).

### 4.2 Case 1: Schema `<name>` is New to the Catalog (`!base.schemas.has(name)`)

When `<name>` is not in `base.schemas` and `wanted.origin.kind === 'imported'`:

1. **Replay upstream steps (`major < 1000`) as-is**:
   For each step $s \in \text{wanted.origin.steps}$ (in topological order):
   - If `s.parents.length === 0`: emit `create` with `s.version`, `s.params`, `s.tables`, `stepHash: s.hash`.
   - If `s.parents.length > 0`: emit `update` with `s.version`, `s.params`, `s.migration`, `s.note`, `stepHash: s.hash`, `parentStepHashes: s.parents`.
2. **Run the planner on the patch section** (always; an empty diff emits nothing):
   - Let `upstreamTables` and `upstreamParams` be `wanted.origin.upstream` (the resolved state of the upstream pack at `wanted.origin.versions`, computed by `readSource`). Call that view `upstreamView`.
   - Let `targetUpstreamHeadStepHashes` be the step hashes of `wanted.origin.versions`.
   - Run `diffTables(name, upstreamTables, wanted.tables)`.
   - If `diff.rules.length > 0`:
     Emit one patch `update` step at `version = patchVersion(catalogVersion)` (`1000 + catMajor...`) with `parentStepHashes = targetUpstreamHeadStepHashes` and `rules = diff.rules`.
   - Pin `<name>` at `{ kind: 'create' }` when the only queued step is the genesis and no patch was emitted, and at `{ kind: 'update' }` otherwise. `produceRelease` starts from the create and folds each new step's hash into the pin, so no `plan.pins` entry holding scratch hashes is written.

### 4.3 Case 2: Schema `<name>` Already Exists in `base.schemas` (`wanted.origin.kind === 'imported'`)

1. **Queue missing upstream steps (`major < 1000`) with explicit upstream parents**:
   For each step $s \in \text{wanted.origin.steps}$ where `!stepToEntry.has(s.hash)`:
   - Split $s.\text{parents}$ into:
     - `existingParentEntries = s.parents.filter(p => stepToEntry.has(p)).map(p => stepToEntry.get(p)!)` (parents already in `baseSchema.object`), and
     - `newParentStepHashes = s.parents.filter(p => !stepToEntry.has(p))` (parents queued earlier in this same draft).
   - Queue an `update` step in `plan.schemas` with `s.version`, `s.params`, `s.migration`, `s.note`, `stepHash: s.hash`, `at: existingParentEntries`, and `parentStepHashes: newParentStepHashes`.
   - **Critical rule in `produceRelease`**: An upstream step (`stepHash !== undefined`) resolves its parents strictly from `step.at` (existing upstream catalog entry hashes) and `step.parentStepHashes` (in-draft upstream step hashes). It must **never** fall back to `pins.get(step.schema)` (`baseSchema.at`), which may be a `1000+` patch head! `produceRelease` throws when such a step resolves no parents, and when any `parentStepHashes` entry has no mapped catalog entry. Before signing, `produceRelease` reduces every step's parent set with `findMinimalCover`, so a redundant ancestor (a target head that is already below `baseSchema.at`) is not baked into the signed entry.

2. **Compute `candidateFrontier` and `candidateView`** (no scratch replica):
   - `targetUpstreamHeadStepHashes` are the step hashes of `wanted.origin.versions`. `upstreamView` is `wanted.origin.upstream`.
   - Build an entry list from `baseSchema.object`'s entries plus one synthetic `schema-update` entry per newly queued upstream step (its hash is the pack step hash; its `prevs` are the mapped parent entry or step hashes). `resolveSchemaState` reads only `hash`, `payload`, and `header.prevEntryHashes`.
   - `targetUpstreamEntries` are the entry hashes for `wanted.origin.versions` (a mapped catalog entry when the step is already present, otherwise the synthetic step hash).
   - Compute `candidateFrontier` as the minimal cover, over that same entry list, of `baseSchema.at` together with `targetUpstreamEntries`.
   - Evaluate `candidateView = resolveSchemaState(entries, candidateFrontier)`.

3. **Run the Planner on the Patch Section (`candidateView` $\to$ `wanted`)**:
   - Apply any manual migration rules for `<name>` from `upgrade-manual.sql` (`hand.rules.get(name) ?? []`) to a working copy of `candidateView.tables` via `applyMigrationRules`.
   - Run `diffTables(name, working, wanted.tables)`. If any refusals, record and stop.
   - Combine `patchRules = [...handRules, ...diff.rules]`.
   - Check for slot overlap warnings (§3.2): for each slot touched by the newly queued upstream steps, if `wanted` differs from `upstreamView` on that slot, add a warning to `plan.warnings` naming only the queued versions that touched it.
   - **If `patchRules.length === 0`**:
     - No new `1000+` patch entry is emitted.
   - **If `patchRules.length > 0`**:
     - Verify applicability and fixpoint against `wanted.tables`.
     - Emit **one** patch `update` step at `version = patchVersion(catalogVersion)` with:
       - `at = baseSchema.at.concat(targetUpstreamHeadStepHashes.filter(h => stepToEntry.has(h)).map(h => stepToEntry.get(h)!))` (`produceRelease` minimal-covers this before signing)
       - `parentStepHashes = targetUpstreamHeadStepHashes.filter(h => !stepToEntry.has(h))`
       - `rules = patchRules`
   - **Pin** (either branch): `{ kind: 'entries', entries: frontier }` when nothing was queued, where `frontier` is the minimal cover of `baseSchema.at` and the already-published heads (a head published by a release outside the parents is included). `{ kind: 'update', from }` when upstream steps were queued and no patch was emitted, where `from` is the minimal cover of `baseSchema.at` and the mapped heads; `produceRelease` starts from `from` and folds in each newly signed entry. `{ kind: 'update' }` when a patch was emitted: the patch's `at` already covers the mapped heads, and `produceRelease` starts from `baseSchema.at`. No pin stores scratch or synthetic hashes.

---

## 5. Fixing `produceRelease` Fixpoint Verification (`modules/rpack/src/produce.ts`)

Currently, `produceRelease` asserts that every step in `draft.schemas` is the single final head of that schema:
```ts
for (const step of draft.schemas) {
    const versions = after.schemas.get(name)?.view.getVersions() ?? [];
    if (versions.length !== 1 || versions[0] !== step.version) {
        throw new ProduceError(...);
    }
}
```
As Critique Item 7 notes, this fails both when a schema replays multiple steps in one catalog release (e.g. `1.0.0` then `1.1.0` or `1001.0.0`) and when a schema is pinned at a multi-head frontier (`[P(1001.0.0), U(1.1.0)]` or `['1.1.0', '1.0.1']`).

**Fix in `produceRelease`**:
- An upstream step (`stepHash !== undefined`) throws when it resolves no parents, and when any `parentStepHashes` entry is unmapped. The pins fallback applies only to an inline update (no `stepHash`, no `at`, no `parentStepHashes`).
- Remove the per-step `versions.length !== 1 || versions[0] !== step.version` loop.
- Instead, for each schema in `draft.desired.schemas`:
  - `modelDifferences(after.model, draft.desired)` (which already runs right before) verifies that the resolved `tables` and `params` at the published pin match `draft.desired` (`target-catalog.sql`).
  - For an inline schema that had a step in `draft.schemas`, verify `versions.length === 1 && versions[0] === step.version`.
  - For an imported schema, verify that every version in `after.schemas.get(name)!.view.getVersions()` is either one of `wanted.origin.versions` or `patchVersion(draft.version)` (or a surviving `1000+` patch version from `baseSchema`). This check is cheap and mostly duplicates `modelDifferences`; it stays as a pin-shape assertion.

---

## 6. Rewriting `target-catalog.sql` on Branch Merges (`writeSource`)

When `rpack new <version>` or `rpack set base` creates/rebases a work folder from parent catalog releases:

1. **Single parent (`parents.length === 1`)**:
   `base.model` equals the parent folder's `.released/target-catalog.sql` model, so `writeSource` returns the parent's `target-catalog.sql` verbatim.
2. **Multiple parents (`parents.length > 1`, a catalog branch merge)**:
   - **Step 1 (Update `IMPORT SCHEMA` version list)**:
     Collect all upstream versions imported for `<name>` across all parent folders. Using the union of `<name>`'s step DAGs in `imports/`, compute the **maximal upstream versions** (dropping any version in the causal past of another parent's imported version) and rewrite the `IMPORT SCHEMA <name> VERSION ...;` clause in place (e.g. `VERSION '1.0.1', '1.1.0'`).
   - **Step 2 (Reconcile local `ALTER SCHEMA` patches)**:
     Evaluate the updated `IMPORT SCHEMA` + the highest parent's existing `ALTER SCHEMA <name>` statement(s). Compare that intermediate `SchemaModel` against `base.model.schemas.get(name)`:
     - If they already match (`tableDifferences === []`), keep the highest parent's `ALTER SCHEMA <name>` statement(s) verbatim.
     - If they differ, synthesize a single `ALTER SCHEMA <name> AS (...)` block that transforms `upstreamTables` (the resolved state of the maximal upstream versions) into `base.model.schemas.get(name).tables`:
       - If any table's `idProvider` differs between `upstreamTables` and `base.model.schemas.get(name).tables` (which `ALTER SCHEMA` cannot express), `writeSource` throws a `WriteSourceError` (`schema <name> changes its IDENTITY PROVIDER, which ALTER SCHEMA cannot express`).
       - If a column `t.c` in `upstreamTables` has a different `ColumnDef` in `base.model` (`!sameColumn(uCol, bCol)`), that column was already reset (`DROP COLUMN` + `ADD COLUMN`) in one of the parent releases being merged. Because `target-catalog.sql`'s `ALTER SCHEMA` is evaluated on top of `upstreamTables`, the synthesized `ALTER SCHEMA` in `target-catalog.sql` must include `DROP COLUMN t.c` before `ADD COLUMN t.c <bCol>` (annotated with `-- column t.c was reset in a parent release`) so `target-catalog.sql` reproduces `base.model`. Note that at `candidateFrontier` (the merge of the parent releases), `t.c` already has `<bCol>`, so `draftRelease` does **not** emit another `DROP COLUMN` in the merge release itself unless the two parent branches themselves had conflicting resets of `t.c` (in which case `diffTables(candidateView, wanted)` will prompt the author to put `DROP COLUMN t.c` in `upgrade-manual.sql`).

---

## 7. Formatting & Developer Visibility of `1000+` Versions (`format_draft.ts`, `verify.ts`)

We leave `modules/rdb/src/semver.ts` untouched and use standard `major.minor.patch` semver strings with `major = 1000 + catalogMajor`.

1. **Where `1000+` is never written (`target-catalog.sql`, `upgrade-manual.sql`, `rpack.json`)**:
   - In `target-catalog.sql`, the developer writes the upstream version (`IMPORT SCHEMA <name> VERSION '<v>';`) and an unversioned `ALTER SCHEMA <name> AS (...);`.
   - Any explicit `VERSION` with `major >= 1000` in `target-catalog.sql` or `target-schema.sql` is rejected.
2. **Explanatory comments in `build/update.sql` and `rpack status` (`format_draft.ts`)**:
   - Because `build/update.sql` is a faithful C-SQL transcript of the signed `RSchema` entries, it includes `VERSION '1001.1.0'` and adds a comment explaining the `1000+` version:
     ```sql
     ALTER SCHEMA hhs:comments VERSION '1.1.0' AS (    -- imported from hhs:comments 1.1.0
       ADD COLUMN comments.pinned boolean NULL
     );

     -- Local patch for catalog 1.1.0 (major 1000+ is reserved for local patches on imported schemas)
     ALTER SCHEMA hhs:comments VERSION '1001.1.0' AS (
       ADD COLUMN comments.modNote string NULL
     );
     ```
3. **`rpack verify` output (`verify.ts`)**:
   - In `formatVerifyReport`, any schema version with `major >= 1000` is annotated as `1001.1.0 (local patch 1.1.0)` so inspection output is self-explanatory.

---

## 8. Summary of Code Changes by File

1. **`modules/rdb_lang`**:
   - `src/syntax/ast.ts`, `src/syntax/parser.ts`: Update `ImportSchemaStatement` to hold `versions: string[]` (`IMPORT SCHEMA <name> VERSION '<v1>'[, '<v2>', ...];`).
   - `src/bind/context.ts`, `src/bind/bind.ts`, `src/exec/result.ts`, `src/exec/execute.ts`: Pass `versions: string[]` to `ctx.imports(name, versions)`.
2. **`modules/rdb_runtime`**:
   - `src/session/adapter.ts`: Update `ExecuteTextOptions.source.imports` to take `(name: string, versions: string[])`; allow `ALTER SCHEMA <name> AS (...)` in source mode on imported schemas (assigning scratch `1000.0.x` versions and advancing `importedSchemaVersions`).
3. **`modules/rpack`**:
   - `src/schema_pack.ts` & `src/format.ts`: Reject `major >= 1000` in schema pack manifests and steps.
   - `src/source.ts`:
     - Union the `SchemaReleaseFile.steps` across all versions named in `IMPORT SCHEMA <name> VERSION ...`, verifying a single genesis, consistent step hashes per version, and antichain maxima.
     - Allow `ALTER SCHEMA <name> AS (...)` in `target-catalog.sql` only on imported schemas, and forbid `VERSION`, `PARAMS`, `NOTE`, `BY`, and `AT` on it.
   - `src/draft.ts` & `src/produce.ts`:
     - Replace `findMirroredPackSteps` with the topological parent-map check (§4.1) and durable `config.imports` origin check.
     - Implement the two-tier planner (§4.2–§4.3) and slot-overlap warnings (§3.2).
     - Fix `produceRelease`'s post-publish version assertion (§5) to support multi-step replay and multi-head pins.
   - `src/format_draft.ts`, `src/verify.ts`: Render `-- imported from <schema> <version>` on upstream steps, `-- Local patch for catalog <ver> (major 1000+ is reserved for local patches on imported schemas)` on `1000+` steps, and annotate `1000+` versions in `formatVerifyReport`.
   - `src/write_source.ts` & `src/commands.ts`: Update `writeSource` to merge `IMPORT SCHEMA` version lists and synthesize complete `ALTER SCHEMA` patches (including `DROP COLUMN` + `ADD COLUMN` when a column definition changed).
   - `test/composability_tests.ts`: Add tests covering multi-version `IMPORT SCHEMA` across multiple `.rpack` files, local `ALTER SCHEMA` patches across upstream upgrades, slot-overlap reconciliation and warnings, upstream table reset survival cleanup, and `inline <-> import` refusal.
