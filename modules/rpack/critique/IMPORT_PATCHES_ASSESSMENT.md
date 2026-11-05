# Assessment of the `IMPORT_PATCHES.md` Implementation

A code review of the implementation across `modules/rdb_lang`, `modules/rdb_runtime`, and `modules/rpack` against [`IMPORT_PATCHES.md`](./IMPORT_PATCHES.md).

## Overall Status

The core design from `IMPORT_PATCHES.md` is in place and all existing test suites (`modules/rpack`, `modules/rdb_lang`, `modules/rdb_runtime`, `modules/rdb_tools`) build and pass:

- **Multi-version `IMPORT SCHEMA`**: `rdb_lang` parses `IMPORT SCHEMA <name> VERSION '<v1>'[, '<v2>', ...];`, and `unionImportedSteps` (`modules/rpack/src/source.ts`) unions the `steps` across the locked `.rpack` files in `imports/`, enforcing a single genesis step, a unique step hash per version across shared history, and an antichain check on the listed versions.
- **Local `ALTER SCHEMA` patches in `target-catalog.sql`**: `checkSourceForms` (`source.ts`) permits `ALTER SCHEMA` in `target-catalog.sql` only for schemas imported earlier in the file and forbids `VERSION`, `PARAMS`, `NOTE`, `BY`, and `AT`. `executeText` (`modules/rdb_runtime/src/session/adapter.ts`) assigns sequential scratch `1000.0.x` versions and advances the imported schema pin so `CREATE CATALOG` binds against the patched schema.
- **Two-tier planner without a scratch replica**: `mapUpstreamEntries` and `planImportedUpdate` (`modules/rpack/src/draft.ts`) walk normal (`major < 1000`) entries in topological order, verify parent links and payloads against `origin.steps`, evaluate `candidateView` via `resolveSchemaState` over `baseSchema.object`'s entries plus synthetic upstream updates, and run `diffTables` on the patch section only.
- **Strict upstream parent resolution & multi-head pins**: `produceRelease` (`modules/rpack/src/produce.ts`) resolves upstream step parents strictly from `step.at` and `step.parentStepHashes` (throwing rather than falling back to `pins` / `baseSchema.at`), minimal-covers parent sets before signing, and verifies multi-step and multi-head pins.

---

## Edge Cases and Minor Bugs Found

### 1. Branch merge where a lower parent introduced a new imported schema (`modules/rpack/src/write_source.ts`, lines 506–510)

- **Where**: `writeSource` (`modules/rpack/src/write_source.ts`) and `Released.base` (`modules/rpack/src/released.ts`, line 284).
- **Issue**:
  `Released.base()` constructs each `SchemaModel` in `base.model.schemas` with `origin: { kind: 'inline' }`.
  When `rpack new` creates a merge work folder from two parent releases (say `1.1.0` and `1.0.1`), `writeFresh` (`commands.ts`) starts from the highest parent (`1.1.0`)'s `.released/target-catalog.sql` and calls `writeSource(text, base.model, keys, config.key, { imports, importedVersions })`.
  If the *lower* parent (`1.0.1`) introduced a brand-new imported schema `hhs:tags` that was not present in `1.1.0`:
  1. `hhs:tags` is in `base.model.schemas` and not in `current.schemas` (`1.1.0`), so `writeSource` treats it as a `newSchema` (line 506).
  2. `renderSchemaEntry(s, labels)` checks `s.origin?.kind === 'imported'`. Because `s` came from `base.model`, `s.origin` is `{ kind: 'inline' }`!
  3. `writeSource` therefore emits an inline `CREATE SCHEMA hhs:tags ...` into the merged `target-catalog.sql` instead of `IMPORT SCHEMA hhs:tags VERSION '...'`—even though `options.importedVersions.get('hhs:tags')` has the imported version list.
  4. Running `rpack build` on the merged work folder then fails with:
     `schema hhs:tags was imported; keep IMPORT SCHEMA and use ALTER SCHEMA hhs:tags in target-catalog.sql for local modifications`.
- **Fix**:
  In `writeSource`, pass `options.importedVersions?.get(s.name)` to `renderSchemaEntry` (or check `options.importedVersions` inside `newSchemas`): when `importedVersions` has an entry for `s.name`, render `IMPORT SCHEMA <name> VERSION ...;` and let `reconcileImportedPatches` append any local `ALTER SCHEMA` block.

---

### 2. `reconcileImportedPatches` with multiple `ALTER SCHEMA` blocks or when a merge eliminates the local patch (`modules/rpack/src/write_source.ts`, lines 422–432)

- **Where**: `reconcileImportedPatches` and `renderSchemaEntry` (`modules/rpack/src/write_source.ts`).
- **Issues**:
  1. **Non-adjacent `ALTER SCHEMA` statements**:
     When `alters.length > 1`, lines 428–430 replace a single span from `alters[0].span.start` to `statementEnd(text, alters[alters.length - 1].span)`:
     ```ts
     const start = alters[0]!.span.start;
     const end = statementEnd(text, alters[alters.length - 1]!.span);
     edits.push({ start, end, text: block });
     ```
     If another statement (for example, another `IMPORT SCHEMA` or `CREATE SCHEMA`) lies between two `ALTER SCHEMA <name>` statements in `target-catalog.sql`, this contiguous range replacement deletes the intervening statement.
  2. **Empty synthesized patch (`block === ''`)**:
     If `synthesizeImportedPatch` returns `''` (for example, on a branch merge where the newer upstream version already added the column that the highest parent's local `ALTER SCHEMA` had added, so `wanted.tables` now equals `upstream.tables`), replacing `start..end` with `''` leaves behind blank lines and any leading comment lines above `alters[0]` instead of removing the statement cleanly with `removal(...)`.
  3. **Direct calls to `renderSource(target)`**:
     `renderSchemaEntry` (lines 176–181) only renders `IMPORT SCHEMA <name> VERSION ...;` without calling `synthesizeImportedPatch`. Inside `writeSource`, `reconcileImportedPatches` runs afterward and appends the `ALTER SCHEMA` block, but if the exported `renderSource(target)` function is called directly on a `CatalogModel` whose imported schema has local patch changes, the returned SQL omits the `ALTER SCHEMA` block.
- **Fix**:
  - In `reconcileImportedPatches`, when `block === ''`, remove every statement in `alters` with `removal(text, removalStart(text, a.span.start), removalEnd(text, statementEnd(text, a.span)))`. When `block !== ''`, replace `alters[0]` with `block` and remove `alters.slice(1)` individually with `removal(...)`.
  - In `renderSchemaEntry`, when `schema.origin?.kind === 'imported'` and `tableDifferences(schema.name, schema.origin.upstream.tables, schema.tables).length > 0`, append `\n\n${synthesizeImportedPatch(schema.name, schema.origin.upstream.tables, schema.tables)}`.

---

### 3. Diagnostic message when a new `IMPORT SCHEMA` drops or misses a parent release's imported version (`modules/rpack/src/draft.ts`, lines 178–180)

- **Where**: `mapUpstreamEntries` (`modules/rpack/src/draft.ts`).
- **Issue**:
  In `mapUpstreamEntries`:
  ```ts
  const step = byVersion.get(version);
  if (step === undefined) {
      return { error: `schema ${name} was defined inline or has a version '${version}' not in the imported package history` };
  }
  ```
  If `baseSchema.object` contains an upstream entry at version `'1.0.1'` (from a parent catalog release that imported `'1.0.1'`), and the current release writes `IMPORT SCHEMA <name> VERSION '1.1.0'` (where `'1.1.0'` is a concurrent branch that does not have `'1.0.1'` in its causal past, or `'1.0.0'` is a downgrade below `'1.0.1'`), `byVersion.get('1.0.1')` is `undefined`.
  Reporting that the schema "was defined inline or has a version `'1.0.1'` not in the imported package history" is misleading when `'1.0.1'` is in fact locked in `rpack.json`'s `imports[name]`.
- **Fix**:
  Pass `origin.versions` (or `imports?.[name]`) into `mapUpstreamEntries`: if `imports?.[name]?.[version] !== undefined`, report:
  `schema ${name}: imported version(s) ${origin.versions.map((v) => `'${v}'`).join(', ')} do not include parent imported version '${version}'`.

---

### 4. `unionImportedSteps` does not filter `pack.steps` to the causal closure of `versions` (`modules/rpack/src/source.ts`, lines 256–298)

- **Where**: `unionImportedSteps` (`modules/rpack/src/source.ts`).
- **Issue**:
  `unionImportedSteps` adds every step in `pack.steps` to `byHash`. For `.rpack` files produced by `rpack release` in a schema repository, `pack.steps` is already the causal closure of `pack.manifest.version`, so this holds in standard CLI workflows.
  However, if a `SchemaReleaseFile` passed in `expect.imports` (e.g. in programmatic callers or tests) contains steps beyond the version requested in `IMPORT SCHEMA`, `unionImportedSteps` includes those later steps in `origin.steps`. Then in `readSource` (line 183):
  ```ts
  const frontier = await (await replayed.schema.getScopedDag()).getFrontier();
  ```
  `getFrontier()` on `replayed.schema` resolves at the tip of all steps in `origin.steps` rather than at `statement.versions` (when no local `ALTER SCHEMA` was run).
- **Fix**:
  In `unionImportedSteps`, filter `ordered` to the causal closure of `listed` (`listed` plus `ancestorSteps(ordered, step.hash)` for each `step` in `listed`). That guarantees `origin.steps` is strictly the causal closure of `statement.versions`.

---

### 5. Pinning at `baseSchema.at` instead of `frontier` when `queuedSomething` is false (`modules/rpack/src/draft.ts`, line 317)

- **Where**: `planImportedUpdate` (`modules/rpack/src/draft.ts`, line 317).
- **Issue**:
  Line 317 sets:
  ```ts
  plan.pins.set(name, queuedSomething ? { kind: 'update' } : { kind: 'entries', entries: baseSchema.at });
  ```
  `frontier` (`minimalCover(all, [...baseSchema.at, ...heads])`, computed on line 248) is equal to `baseSchema.at` whenever all `heads` are already in the causal past of `baseSchema.at`. However, if `baseSchema.object` already contained an upstream entry `h` (e.g. from an unreferenced or partially migrated group) that was not in `baseSchema.at`, `queued` would be empty while `frontier` would advance from `baseSchema.at` to `minimalCover([...baseSchema.at, ...heads])`.
- **Fix**:
  Use `{ kind: 'entries', entries: frontier }` when `queuedSomething` is false.

---

### 6. `warnOverriddenSlots` when the local patch is edited on the same release as an upstream change (`modules/rpack/src/draft.ts`, lines 355–385)

- **Where**: `warnOverriddenSlots` (`modules/rpack/src/draft.ts`).
- **Issue**:
  1. Line 378 skips any slot in `rewritten` (`patchRules.flatMap(slotsTouchedByRule)`):
     ```ts
     if (rewritten.has(slot) || slotSnapshot(candidate, slot) === slotSnapshot(upstream, slot)) continue;
     ```
     When `rewritten.has(slot)` is true because the developer *removed* their local override from `target-catalog.sql` (so `patchRules` resets the slot back to `upstream`, and `slotSnapshot(wanted.tables, slot) === slotSnapshot(upstream, slot)`), skipping the warning is right.
     When `rewritten.has(slot)` is true because the developer *edited* their local override in `target-catalog.sql` while upstream also changed that slot (`slotSnapshot(wanted.tables, slot) !== slotSnapshot(upstream, slot)`), the local patch is still overriding the new upstream change.
  2. Line 379 formats `queued.map((s) => s.version).join(', ')` for every slot warning, even when `queued` contains multiple upstream steps (`1.1.0`, `1.2.0`) and only one of them (`1.2.0`) touched that slot.
- **Fix**:
  Track which step version(s) in `queued` touched each slot, and warn when `slotSnapshot(candidate, slot) !== slotSnapshot(upstream, slot) && slotSnapshot(wanted.tables, slot) !== slotSnapshot(upstream, slot)`.
