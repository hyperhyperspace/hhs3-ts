# Rdb Schema Composability & Parameterization — Implementation Report

This document summarizes the changes implemented across `rdb`, `rdb_projection`, `rdb_adapter`, `rdb_lang`, `rdb_runtime`, `rdb_repl`, `rpack`, and `rdb_tools` according to the specification in [`COMPOSABILITY.md`](./COMPOSABILITY.md) (with v2 observer-frame `ALLOW UPDATE REF` documented separately in [`REFERENCE_FRAME.md`](../../rdb/plans/REFERENCE_FRAME.md)).

---

## 1. Overview of Capabilities Added

1. **Cross-group validation & fail-closed `EXISTS` evaluation (Step 1)**:
   - `RTableViewImpl.existsMatching` and `truthView` fail closed (`false`) instead of throwing when a searched `WHERE` column is missing or not `PUB` on the target table view, preserving monotonicity across concurrent schema evolution.
   - `RCatalog` validates cross-group foreign keys, `EXISTS` `PUB` columns and types, `USING IDENTITIES`, `ALLOW DEPLOY IF`, and `ALLOW UPDATE REF` on both `ADD TABLEGROUP` (`validateGroupDef`) and `UPDATE SCHEMA` (`validateChange`).
   - `rdb_lang` compiles `ALLOW UPDATE REF <alias> IF` in the observed group's frame (using the bound group's schema and table definitions) and runs `groupViewReason` with `boundViews`.

2. **Non-injective group bindings (Step 2)**:
   - Multiple schema aliases in a `TABLEGROUP` (`BIND a => g, b => g`) can bind to the same foreign `RTableGroup` ID.
   - All aliases bound to the same `refId` share a single `ref-advance` causal stream on the observer group.
   - `observeGateFor(refId)` combines the `canObserve` predicates of all aliases bound to `refId` with logical `and`.
   - Auto-observation loops in `rdb`, `rdb_adapter`, and `rdb_runtime` deduplicate by target `groupId`.

3. **Table remapping (`tableMaps`) on `BIND` (Step 3)**:
   - `CreateTableGroupPayload` and `CatalogGroupDef` carry optional `tableMaps?: { [alias: string]: { [schemaTable: string]: string } }`.
   - Schema-authored foreign keys, `EXISTS` predicates, and `fkAdoptionKills` resolve foreign tables through `resolveSchemaForeignTableView*` (applying `tableMaps[alias]?.[table] ?? table`), while catalog-authored clauses (`USING IDENTITIES`, `ALLOW DEPLOY IF`, `ALLOW UPDATE REF`) use un-remapped real table names.
   - C-SQL supports `BIND alias => group (schema_table => foreign_table, ...)` with full reverse-rendering and round-trip support.
   - `rdb_projection` and `rdb_runtime` map foreign table names through `resolveForeignTableName` and `resolveSchemaForeignTableView`.

4. **Schema `PARAMS`, default `CREATORS`, and `CONSTANT` (Step 4)**:
   - `RSchema` supports typed parameter declarations (`params?: SchemaParamDecl[]`, `{ name, type: 'string' | 'integer' | 'bigint' | 'number' | 'decimal' | 'boolean', default?: Literal }`) in genesis and update payloads, and `$param.<name>` operands in row restriction expressions (`CmpAtom`, `LikeAtom`, `RuleValueExpr`).
   - `ALTER SCHEMA` updates may add new parameters (which must declare a `DEFAULT <literal>`); existing parameters cannot be removed or altered.
   - `CreateTableGroupPayload`, schema `deploy` (`RefAdvancePayload`), `CatalogGroupDef`, and `CatalogGroupChange` carry `params?: { [name: string]: Literal }`.
   - Group `deploy` and `CatalogGroupChange` parameter writes require a schema version move (`targetVersion !== currentVersion`) and fold per-parameter across concurrent branches by `(1) target schema semver, (2) target schema entry hashes, (3) level-local entry hash`.
   - `CREATE SCHEMA` in C-SQL defaults `CREATORS` to the effective signing author when omitted.
   - C-SQL scripts support `CONSTANT :name = <literal>;` for compile-time literal substitution into `WITH PARAMS` and `WITH ROWS`.

5. **Reusable schema packs, `IMPORT SCHEMA`, and `.rpack` registry (Step 5)**:
   - Schema `.rpack` files (`manifest.kind = "schema"`) ship a content-addressed `steps` DAG (`domain: "hhs3-rpack-schema-step-v1"`), verified by replaying into a scratch `RSchema` (`replaySchemaSteps` / `verifyRelease`).
   - Catalogs import published schemas via `IMPORT SCHEMA <name> VERSION '<semver>';` in `target-catalog.sql` with step hashes locked in `rpack.json` under `"imports"` and `.rpack` files vendored in `imports/`.
   - `rpack` preserves the imported schema's step DAG when creating or upgrading the catalog's `RSchema` object (`findMirroredPackSteps` / `appendNewSchemaSteps`) and supports switching between inline `CREATE SCHEMA` and `IMPORT SCHEMA`.
   - `rpack` CLI adds `rpack init --schema <name>`, `rpack publish [<version>] [--registry <dir>]`, and `rpack import <schema> [<version>] [--registry <dir>]`.
   - `rhost` CLI adds `rhost init --catalog <name> [--version <v>] [--registry <dir>]` and `rhost update --pull [--major] [--registry <dir>]`.

---

## 2. Module-by-Module Changes

### 2.1 `modules/rdb`
- **Fail-closed `EXISTS` & truth views**:
  - `src/rtable/view.ts` (`RTableViewImpl.existsMatching`): Returns `false` when a searched column is absent from the table definition or lacks `pub: true`.
  - `src/rtable_group/predicates.ts` (`truthView`): Returns `false` if `existsMatching` throws.
- **Non-injective bindings & `tableMaps`**:
  - `src/rtable_group/payload.ts`, `src/rcatalog/payload.ts`: Added `TableMaps`, `tableMapsFormat`, `GroupParams`, `groupParamsFormat`.
  - `src/rtable_group/validate.ts`, `src/rcatalog/validate.ts`: Lifted binding injectivity checks; added `validateTableMaps` and `validateGroupParams`.
  - `src/rtable_group/group.ts`, `src/rtable_group/interfaces.ts`: Added `aliasesForId(refId)`, multi-alias `observeGateFor(refId)` (`and`-combining `canObserve` across all aliases bound to `refId`), `getTableMaps()`, `resolveForeignTableName`, `resolveSchemaForeignTableView`, and `resolveSchemaForeignTableViewIn`.
  - `src/rtable_group/op_delta.ts`, `src/rcatalog/validate_ops.ts`: Formatted multi-alias labels as `alias1/alias2`.
- **Schema `PARAMS` & group parameter folding**:
  - `src/rschema/payload.ts`, `src/rschema/interfaces.ts`, `src/rschema/rschema.ts`, `src/rschema/view.ts`, `src/rschema/resolve.ts`: Added `SchemaParamDecl` (`name`, `type`, `default?`), `getParam`, `getParams`, and union folding across schema branches.
  - `src/rschema/expr.ts`, `src/rschema/format_predicate.ts`, `src/rschema/validate.ts`, `src/rschema/validate_ops.ts`: Added `RuleOperand`, `parseParamTerm` (`$param.<name>`), type checking of `$param.<name>` in `CmpAtom`, `LikeAtom`, and `RuleValueExpr`, and mandatory `default` on `update` parameter declarations.
  - `src/rtable_group/group.ts`, `src/rtable_group/view.ts`, `src/rtable_group/validate_ops.ts`: Added `param: [name, ...]` entry meta tagging on `deploy`, version-move enforcement when `deploy.params` is present, `resolveGroupParams`, `resolveEffectiveParams`, and `params` resolution in `EvalEnv`.
  - `src/rcatalog/resolve.ts`, `src/rcatalog/validate_ops.ts`, `src/rcatalog/rcatalog.ts`: Added `foldGroupParamWrites`, `groupParamsReason`, and `boundViews` cross-group validation in both `validateGroupDef` and `validateChange`.
  - `src/rdb/instantiate.ts`, `src/rdb/catalog_update.ts`, `src/rdb/conformance.ts`: Wired `targetParams`, deploy parameter catch-up, and `differingParams` conformance checks; deduplicated auto-observation in `applyCatalogPlan`.

### 2.2 `modules/rdb_projection` & `modules/rdb_adapter`
- `modules/rdb_projection/src/scope.ts`: Wired `group.resolveForeignTableName(bindingName, table)` into `resolverFor` so cross-group foreign keys and `EXISTS` dependencies resolve through `tableMaps`.
- `modules/rdb_adapter/src/ref_advance.ts`: Deduplicated `buildObservedBy` by observer `groupId` when multiple aliases bind to the same target group.

### 2.3 `modules/rdb_lang`, `modules/rdb_runtime`, & `modules/rdb_repl`
- **AST & Parser (`modules/rdb_lang/src/syntax/ast.ts`, `parser.ts`)**:
  - Added `tableMap?: { [from: string]: string }` on `CatalogBindingNode`.
  - Added `params?: SchemaParamDeclNode[]` on `CreateSchemaStatement` and `AlterSchemaStatement` (allowing params-only `ALTER SCHEMA ... PARAMS (...)` without `AS (...)`).
  - Added `params?: CatalogGroupParamAssignNode[]` (`WITH PARAMS (:p = val, ...)`) on `CatalogGroupNode` and `CatalogUpdateSchemaItem`.
  - Added `ConstantStatement` (`CONSTANT :name = <literal>;`) and `ImportSchemaStatement` (`IMPORT SCHEMA <name> VERSION '<semver>';` in source mode).
- **Binding, Compilation & Execution (`modules/rdb_lang/src/bind/`, `compile/`, `exec/`)**:
  - Defaulted `CREATE SCHEMA` `CREATORS` to the effective signing author when omitted.
  - Bound script-level `CONSTANT` declarations and substituted them into `WITH PARAMS` and `WITH ROWS`.
  - Compiled `ALLOW UPDATE REF` in the observed group's frame and validated cross-group references with `boundViews`.
  - Added `imports` resolver hook to `LangBindContext` and `ImportSchemaLangResult`.
- **Reverse Rendering (`modules/rdb_lang/src/reverse/render.ts`, `source.ts`)**:
  - Rendered `PARAMS (...)` on `CREATE SCHEMA` / `ALTER SCHEMA`, `WITH PARAMS (...)` on `TABLEGROUP` / `UPDATE SCHEMA`, `(from => to)` on `BIND`, and `$param.<name>` as `:name` in predicates.
- **Runtime (`modules/rdb_runtime/src/session/adapter.ts`, `ref_auto_update.ts`)**:
  - Routed `resolveFkRowId` through `resolveSchemaForeignTableView`.
  - Deduplicated `findObservers` by target `groupId`.
  - Extended `ExecuteTextOptions.source` with `imports` resolver to register imported `RSchema` objects and their pinned versions in source mode.
- **REPL (`modules/rdb_repl/src/format/table.ts`)**:
  - Added table formatting for `import-schema` results.

### 2.4 `modules/rpack`
- **Project & Format (`src/project.ts`, `src/format.ts`, `src/verify.ts`)**:
  - Added `kind?: 'catalog' | 'schema'`, `registry?: string`, and `imports?: RpackImportsLock` to `rpack.json`.
  - Added `SchemaReleaseFile`, `SchemaReleaseManifest`, `SchemaReleaseStep`, `schemaStepHash` (`hhs3-rpack-schema-step-v1`), `replaySchemaSteps`, and `verifyRelease` support for schema packs.
  - Sanitized `:` to `_` in release filenames (`sanitizedReleasePrefix` / `releaseName`).
- **Model, Diffing & Source (`src/model.ts`, `src/released.ts`, `src/schema_diff.ts`, `src/catalog_diff.ts`, `src/source.ts`, `src/draft.ts`, `src/format_draft.ts`, `src/produce.ts`, `src/write_source.ts`)**:
  - Added `diffSchemaParams`, `effectiveGroupParams` (refusing removal of non-default group overrides with a hint to write the default value), `tableMaps` diffing, and warnings for unmapped new foreign table references on `UPDATE SCHEMA`.
  - Implemented DAG-preserving step replay (`findMirroredPackSteps` / `appendNewSchemaSteps`) for imported schemas and inline ↔ imported transitions.
  - Updated `writeSource` to preserve `IMPORT SCHEMA` (and update its `VERSION` on branch merges) and edit `PARAMS`, `WITH PARAMS`, and `tableMaps` in place.
- **Schema Pack Repositories & Registry (`src/schema_pack.ts`, `src/registry.ts`, `src/commands.ts`)**:
  - Implemented keyless schema repository workflows (`initProject(..., { kind: 'schema' })`, `readSchemaSource`, `ReleasedSchemas`, `draftSchemaRelease`, `produceSchemaRelease`).
  - Implemented registry operations (`publishToRegistry`, `importSchemaFromRegistry`, `readLockedImports`, `resolveCatalogInRegistry`).

### 2.5 `modules/rdb_tools`
- `bin/rpack.ts`: Added `rpack init --schema <name>`, `rpack publish [<version>] [--registry <dir>]`, `rpack import <schema> [<version>] [--registry <dir>]`, and the `rpack stage` refusal in schema repositories.
- `bin/rhost.ts`: Added `rhost init --catalog <name> [--version <v>] [--registry <dir>]` and `rhost update --pull [--major] [--registry <dir>]`.

---

## 3. New Test Suites Added

| Module | Test ID | Coverage |
| :--- | :--- | :--- |
| `modules/rdb` | `[XGROUP12]` | `existsMatching` fail-closed (`false`) on missing or non-`PUB` columns |
| `modules/rdb` | `[CAT13]` | `RCatalog` cross-group validation on `validateGroupDef` and `validateChange` |
| `modules/rdb` | `[OBSGATE10]` | Non-injective bindings (`a => g, b => g`): shared `ref-advance` stream and `and`-combined `canObserve` gates |
| `modules/rdb` | `[RDBC12]` | Auto-observation deduplication across non-injective bindings in `applyCatalogPlan` |
| `modules/rdb` | `[INST01]` | Golden hash vector for `tableMaps` in `CreateTableGroupPayload` |
| `modules/rdb` | `[XGROUP13]` | `tableMaps` remapping for cross-group FKs, `EXISTS`, and `fkAdoptionKills` while keeping gates un-remapped |
| `modules/rdb` | `[RSCHEMA16]` | Schema `params` declaration validation, `$param.<name>` expressions, and mandatory `default` on `update` |
| `modules/rdb` | `[DEPLOY19]` | Group `params` evaluation, per-parameter deploy fold, version-move requirement, and barrier voiding |
| `modules/rdb` | `[RDBC13]` | Catalog group `params` validation, folding across concurrent releases, deploy catch-up, and conformance |
| `modules/rdb_projection` | `[RDBPROJ14]` | `tableMaps` cross-group FK projection and bidirectional ingestion |
| `modules/rdb_lang` | `[CLANG17]` | Observed-frame `ALLOW UPDATE REF` compilation and cross-group checks on `ADD` / `UPDATE SCHEMA` |
| `modules/rdb_lang` | `[CLANG18]` | `BIND alias => group (from => to)` parsing, compilation, validation, and round-trip rendering |
| `modules/rdb_lang` | `[CREATORS09]` | Default `CREATORS` on `CREATE SCHEMA` from the effective signing author |
| `modules/rdb_lang` | `[CLANG19]` | Schema `PARAMS`, `WITH PARAMS`, default `CREATORS`, and `CONSTANT` end to end |
| `modules/rpack` | `[RPACK20]` | Schema pack repository lifecycle (`init --schema`, `build`, `release`, `PARAMS`, `verify`, tamper checks) |
| `modules/rpack` | `[RPACK21]` | Registry `publish` & `import`, `IMPORT SCHEMA` DAG replay, `tableMaps`, and effective group `params` |
| `modules/rpack` | `[RPACK22]` | Switching between `IMPORT SCHEMA` and inline `CREATE SCHEMA` |
| `modules/rdb_tools` | `[RDB_TOOLS67]` | CLI end-to-end: `rpack init --schema`, `rpack publish`, `rpack import`, `rhost init --catalog`, `rhost update --pull` |
