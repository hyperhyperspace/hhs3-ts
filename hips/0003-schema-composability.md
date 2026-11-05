# HIP-3: Schema composability and parameters

- **Type:** design
- **Status:** implemented
- **Modules:** rdb, rdb_lang, rdb_projection, rdb_adapter, rdb_runtime, rdb_repl, rpack, rdb_tools
- **Requires:** 2
- **Superseded by:** —
- **Implemented in:** `52bd1b9`

## Summary

One reusable schema, such as `hhs:comments`, should serve several table groups in a catalog and several catalogs in different repositories, without nominal interfaces. This HIP completes Rdb's structural composition in two places. At instantiation, a binding can remap a schema's symbolic table names onto the bound group's tables, several aliases may bind to one group, and a schema declares typed `PARAMS` whose values are chosen per group and ride on deploys. In rpack, schemas get repositories and release files of their own, a catalog imports one with `IMPORT SCHEMA`, a release is checked against the bound groups' schemas as they stand in that release, and schema and catalog packs share one registry.

How [Rdb](../modules/rdb), [C-SQL](../modules/rdb_lang), [rpack](0002-rpack-rhost.md#design) and [rhost](0002-rpack-rhost.md#hosting-with-rhost) compose reusable schemas across table groups and repositories without nominal interfaces.

## Overview

In Rdb, an `RSchema` is the specification for one `RTableGroup`. Schemas compose structurally: an `RSchema` refers to external tables by two-part symbolic names (`<group>.<table>`), and a `TABLEGROUP` in an `RCatalog` binds those names when the group is instantiated.

To let a single reusable schema — such as `hhs:comments`, `hhs:reactions`, `hhs:follows`, or `hhs:users` — be instantiated across multiple table groups in a catalog and shared across repositories, this design completes the structural model in two places:

1. **Instantiation-time parameterization in `rdb` and C-SQL:**
   - **Table remapping on `BIND`:** A group binding maps a schema's group alias to a target group and optionally remaps the schema's symbolic table names onto the target group's actual tables (`BIND target => blog (subject => posts)`). Remapping applies only to references that originate in the schema (`REFERENCES` and schema `ALLOW` rules); catalog-authored clauses (`ALLOW DEPLOY IF`, `USING IDENTITIES`, and `ALLOW UPDATE REF`) use real table names. *(Moving `ALLOW UPDATE REF` to the observer's frame is a separate v2 design; see [REFERENCE_FRAME.md](0014-observer-frame-ref.md).)*
   - **Non-injective group bindings:** Multiple group aliases in one schema (such as `source` and `target` in `hhs:follows`) may bind to the same `RTableGroup`, sharing its single `ref-advance` observation stream.
   - **Schema `PARAMS`:** An `RSchema` may declare typed scalar parameters (`PARAMS (:write_cap string, :max_len integer DEFAULT 1000)`) referenced in its `ALLOW` predicates. Values are literals chosen per `TABLEGROUP` (`WITH PARAMS (...)`) or on schema updates (`UPDATE SCHEMA ... WITH PARAMS (...)`) and written into the group's `create` and `deploy` payloads, keeping `RTableGroup` 100% autonomous. Parameter values are revisable slots ranked by target schema version, then target schema entry hashes. Optional script-level `CONSTANT :name = <literal>;` statements let C-SQL scripts share literals across groups at compile time.
   - **Default `CREATORS` on `CREATE SCHEMA`:** Omitting `CREATORS` on `CREATE SCHEMA` defaults to the signing author (matching `CREATE CATALOG`), so reusable schema source files stay author-agnostic.

2. **Reusable schema packs, imports, static checking, and the `.rpack` registry in `rpack` and `rhost`:**
   - **Schema repositories (`"kind": "schema"` in `rpack.json`):** Managed with the same `rpack` workflow (`init --schema`, `new`, `set base`, `status`, `build`, `release`, `log`, `verify`) and packaged as `.rpack` files (`manifest.kind = "schema"`) carrying the schema's version DAG and compiled `MigrationRule[]` steps.
   - **`IMPORT SCHEMA` in `target-catalog.sql`:** A catalog imports a schema release (`IMPORT SCHEMA hhs:comments VERSION '1.2.0';`), vendors the `.rpack` file in `imports/` (pinned by step hash in `rpack.json`), replays its `steps` preserving the pack's DAG structure, and signs the resulting `RSchema` entries with the catalog's key.
   - **Static cross-group checking & fail-closed runtime:** When a catalog adds or updates a `TABLEGROUP`, `RCatalog` and `rpack` check that bound foreign tables, `IDENTITY PROVIDER` designations, and `EXISTS` `PUB` columns exist in the bound groups' schemas in that release; at runtime, `existsTruth` fails closed (`false`) if a foreign column is missing or not `PUB`.
   - **Unified `.rpack` registry (`~/.rdb/registry`):** Both schema packs and catalog packs publish `.rpack` files into one namespace (`rpack publish`), consumed by `rpack import` (for schemas) and `rhost init --catalog` / `rhost update --pull` (for catalogs).

---

## 1. C-SQL Surface

### Declaring a Parameterized Schema

A schema may declare typed parameters (`PARAMS (:name type [DEFAULT <literal>], ...)`) and reference them as `:name` in table `ALLOW` rules. `CREATORS` is optional and defaults to the statement's effective author:

```sql
CREATE SCHEMA hhs:comments VERSION '1.0.0'
  PARAMS (:write_cap string, :mod_cap string)
AS (
  TABLE comments (
    item string READONLY REFERENCES target.subject,
    body string
  )
    ALLOW insert IF EXISTS auth.caps WHERE label = :write_cap AND grantee = $author
    ALLOW delete IF rowAuthor = $author
                 OR EXISTS auth.caps WHERE label = :mod_cap AND grantee = $author
);
```

An `ALTER SCHEMA` may declare new parameters before `AS` when its rules introduce references to them. Every parameter declared by `ALTER SCHEMA` **must** specify a `DEFAULT` literal, because groups created under earlier schema versions already exist and may deploy the new version without setting the new parameter:

```sql
ALTER SCHEMA hhs:comments VERSION '1.1.0'
  PARAMS (:max_len integer DEFAULT 1000)
AS (
  SET ALLOW RULES comments (
    ALLOW insert IF length(body) <= :max_len
                AND EXISTS auth.caps WHERE label = :write_cap AND grantee = $author,
    ALLOW delete IF rowAuthor = $author
                 OR EXISTS auth.caps WHERE label = :mod_cap AND grantee = $author
  )
);
```

### Importing Schemas, Constants, and Instantiating Table Groups

In `target-catalog.sql`, `IMPORT SCHEMA <name> VERSION '<semver>';` brings a released schema pack into scope before `CREATE CATALOG`, and optional `CONSTANT :name = <literal>;` statements name compile-time literals reused across groups. Each `TABLEGROUP` binds the schema's group aliases, optionally remaps foreign table names in parentheses, and supplies schema parameters with `WITH PARAMS`:

```sql
CONSTANT :default_max_len = 1000;

IMPORT SCHEMA hhs:users VERSION '1.0.0';
IMPORT SCHEMA hhs:comments VERSION '1.1.0';
IMPORT SCHEMA hhs:follows VERSION '1.0.0';

CREATE SCHEMA app:blog AS (
  TABLE posts (title string, body string),
  TABLE photos (url string, caption string)
);

CREATE CATALOG blog_app PARAMS (:admin identity) AS (
  TABLEGROUP users USING SCHEMA hhs:users
    USING IDENTITIES identities
    WITH ROWS (
      identities (keyId = :admin, publicKey = publicKey(:admin), name = 'Admin'),
      caps (label = 'manager', grantee = :admin)
    ),

  TABLEGROUP blog USING SCHEMA app:blog
    BIND users => users
    USING IDENTITIES users.identities,

  -- Bind target.subject -> blog.posts; auth tables (identities, caps) keep their names:
  TABLEGROUP post_comments USING SCHEMA hhs:comments
    BIND target => blog (subject => posts),
         auth   => users
    USING IDENTITIES auth.identities
    ALLOW DEPLOY IF EXISTS auth.caps WHERE label = 'manager' AND grantee = $author
    ALLOW UPDATE REF target IF EXISTS users.caps WHERE label = 'manager' AND grantee = $author
    WITH PARAMS (:write_cap = 'commenter', :mod_cap = 'manager', :max_len = :default_max_len),

  -- Reuse the same schema for blog.photos with its own table map and param values:
  TABLEGROUP photo_comments USING SCHEMA hhs:comments
    BIND target => blog (subject => photos),
         auth   => users
    USING IDENTITIES auth.identities
    WITH PARAMS (:write_cap = 'commenter', :mod_cap = 'manager', :max_len = 500),

  -- Both aliases (source and target) bind to the same group (blog):
  TABLEGROUP related_posts USING SCHEMA hhs:follows
    BIND source => blog (actor => posts),
         target => blog (subject => posts),
         auth   => users
    USING IDENTITIES auth.identities
);
```

And in `ALTER CATALOG`, `UPDATE SCHEMA` accepts `WITH PARAMS` to supply new schema parameters or revise existing ones. Parameter values only change together with a schema version move: `TO` must be above the group's current version, and that version must change something in the schema (here, an `hhs:comments` `1.2.0` with a new rule):

```sql
ALTER CATALOG blog_app VERSION '1.1.0' AS (
  UPDATE SCHEMA hhs:comments TO '1.2.0' ON post_comments
    WITH PARAMS (:max_len = 2000)
);
```

There are no params-only changes: fixing a botched value means releasing a schema version that changes something (for an imported schema, a new pack release) and moving the group to it with the corrected `WITH PARAMS`.

- **Group binding with table map:** `BIND alias => group (schema_table => foreign_table, ...)`.
  - Any table under `alias.<t>` not listed in `(...)` maps to `group.<t>` unchanged.
  - Remapping applies **only to schema-originated references** (`REFERENCES alias.<t>` FKs and `EXISTS alias.<t>` atoms in the schema's `ALLOW` rules).
  - **Catalog-authored references use real table names:** `ALLOW DEPLOY IF`, `USING IDENTITIES`, and `ALLOW UPDATE REF` are written in the catalog and never go through any group's `tableMaps`:

    | Clause declared on group `G` | Evaluated in | Unqualified `t` | Qualified `a.t` |
    | --- | --- | --- | --- |
    | `ALLOW DEPLOY IF` | `G` | `G`'s table `t` | `a` in `G`'s bindings, real table `t` of that group |
    | `USING IDENTITIES` | `G` | `G`'s table `t` | `a` in `G`'s bindings, real table `t` of that group |
    | `ALLOW UPDATE REF b` (`b => H`) | `H` | `H`'s real table `t` | `a` in `H`'s bindings, real table `t` of that group |

- **Group `WITH PARAMS`:** Supplies literal values for the parameters declared by the group's schema. Catalog instance params (`CREATE CATALOG ... PARAMS (:admin identity)`) are per-database values for `WITH ROWS` only and cannot be referenced in `WITH PARAMS`. Reusing an app-level literal across groups is done with `CONSTANT :name = <literal>;`, which C-SQL resolves to its literal at compile time.

---

## 2. Changes in `rdb` and `rdb_projection`

### 2.1 Table Remapping (`tableMaps`)

Both `CatalogGroupDef` (`modules/rdb/src/rcatalog/payload.ts`) and `CreateTableGroupPayload` (`modules/rdb/src/rtable_group/payload.ts`) gain an optional `tableMaps` field alongside `bindings`, fixed at group creation in v1:

```ts
export type TableMaps = {
    [alias: string]: { [schemaTable: string]: string };
};
```

- **Canonical form (anti-fungibility):**
  - `tableMaps` is omitted when empty, so groups without table remapping have byte-for-byte identical payloads and hashes.
  - Every outer key `alias` must be a key of `bindings`, and its inner map must be non-empty.
  - Every `schemaTable` and `foreignTable` must satisfy `isValidName`.
  - Identity entries (`schemaTable === foreignTable`) are rejected in format validation: omitting the entry already means the name is unchanged.
  - An entry in `tableMaps` does **not** have to be referenced by the schema at its initial pin (as long as `foreignTable` exists in the bound group), allowing a catalog to pre-map tables ahead of a schema upgrade.
- **Two resolution entry points in `RTableGroup` (`modules/rdb/src/rtable_group/group.ts`):**
  - `getTableMaps(): TableMaps` returns a copy of `createOp.tableMaps ?? {}`.
  - `resolveForeignTableName(alias: string, schemaTable: string): string` returns `this.createOp.tableMaps?.[alias]?.[schemaTable] ?? schemaTable`.
  - `resolveForeignTableViewIn(evaluation, groupName, table, at, from, filterVoided)` stays un-remapped (resolves real table names) and continues to serve catalog-authored references (`canDeploy`, `idProvider` via `resolveAuthorKey`, and `canObserve` via `evaluateObserveGateIn`).
  - `resolveSchemaForeignTableViewIn(evaluation, groupName, schemaTable, at, from, filterVoided)` maps `table = this.resolveForeignTableName(groupName, schemaTable)` and delegates to `resolveForeignTableViewIn`. It is used everywhere a reference originates from the schema:
    - row restriction evaluation (`getForeignTableView` in `evaluateRowOpRestriction` and void-time checks);
    - FK target liveness (`fkTargetsLive` in `rtable_group/validate_ops.ts`, for single row ops and bundles);
    - FK adoption on deploy (`adoptedTargetLive` in `fkAdoptionKills`, `group.ts`);
    - FK row-id resolution in `rdb_runtime/src/session/adapter.ts` (`resolveFkRowId`);
    - the projection's cross-group resolver (`resolverFor` in `rdb_projection/src/scope.ts`).
- **Instantiation (`modules/rdb/src/rdb/instantiate.ts`):**
  - `instantiateGroup` copies `def.tableMaps` to `payload.tableMaps` when present. New golden vectors in `instantiate_tests.ts` lock `tableMaps` and `params`.

### 2.2 Non-Injective Group Bindings (One Ref Pointer per Target `groupId`)

Multiple binding aliases in one `RTableGroup` (for example, `source` and `target` in `hhs:follows`) may map to the **same** foreign group ID (`bindings[source] === bindings[target]`).

- **Single `ref-advance` stream per `groupId`:**
  - In MVT (`modules/mvt/src/refs.ts` and `modules/mvt/src/delta.ts`), an observer holds one monotonically advancing version reference per observed `RObject` (`refId: B64Hash`), which `computeObserverRevisionBound` and `projectForeignBound` rely on for bounded delta soundness.
  - Therefore, all aliases bound to the same `groupId` share that group's single `ref-advance` pointer in the observer's DAG, guaranteeing snapshot cohesion across all tables of that foreign group.
- **Format validation (`modules/rdb/src/rtable_group/validate.ts` and `modules/rdb/src/rcatalog/validate.ts`):**
  - Remove the one-to-one target uniqueness rejection on `bindings`.
- **Combined observation gate (`modules/rdb/src/rtable_group/group.ts`):**
  - Because a `ref-advance` on `refId` advances the observed version for *every* alias bound to `refId`, **all** `canObserve` predicates declared for aliases bound to `refId` must hold (an alias with no `canObserve` entry is ungated, i.e. `true`).
  - `aliasesForId(refId: B64Hash): string[]` returns all binding aliases whose target is `refId` (in sorted alias order).
  - `observeGateFor(refId: B64Hash): Predicate | undefined` collects `canObserve[alias]` for each alias in `aliasesForId(refId)`: returns `undefined` if none declare a gate, the single predicate if one does, or `{ p: 'and', args: predicates }` if several do.
  - `bindingNameForId` (`group.ts`) and `bindingNameFor` (`validate_ops.ts`, `op_delta.ts`) format all aliases bound to `refId` (`aliasesForId(refId).join('/')`).
- **Deduplicating auto-observations:**
  - Loops that automatically observe bound groups must deduplicate by target `groupId` so they do not append duplicate `ref-advance` barriers when two aliases bind the same group:
    - `applyCatalogPlan` (`modules/rdb/src/rdb/catalog_update.ts`);
    - `buildObservedBy` (`modules/rdb_adapter/src/ref_advance.ts`);
    - `modules/rdb_runtime/src/session/ref_auto_update.ts`.

### 2.3 Schema `PARAMS` (Literal Values, Revisable per Group)

An `RSchema` can declare typed scalar parameters that its table `ALLOW` predicates reference.

#### Payload & Format (`modules/rdb/src/rschema/payload.ts`)

```ts
export type SchemaParamDecl = {
    name: string;
    type: ColumnType;
    default?: json.Literal;
};

export type RuleOperand =
    | Operand
    | { param: string };
```

- **`CreateRSchemaPayload`** gains `params?: SchemaParamDecl[]` (omitted when empty; max `MAX_SCHEMA_PARAMS = 64`). A parameter declared by `CREATE SCHEMA` may omit `default`; each group using the schema must then supply its value.
- **`SchemaUpdatePayload`** gains `params?: SchemaParamDecl[]` (new parameters introduced by the update; redeclaring an existing parameter name is rejected in `validate_ops.ts`).
  - Every parameter declared in `SchemaUpdatePayload.params` **must** carry a `default`, because groups created under earlier schema versions already exist and may deploy the new version without setting the new parameter.
  - A schema update must change something: `migration.length > 0 || (params?.length ?? 0) > 0` (an update with both empty is rejected).
- **Default validation:**
  - When `default` is present on `SchemaParamDecl`, it must satisfy `columnValueMatchesType(decl.default, decl.type)` and, for `string` params, must not start with `'$'` (`$author`, `$row.*`, `$param.*` are reserved terms).
- **Rule-only operand (`RuleOperand`):**
  - `Operand` in `rschema/payload.ts` is shared with `RTableView.query()` (`rtable/query.ts`). To keep `:param` out of runtime table queries, `Operand` stays unchanged and `Predicate` uses `RuleOperand = Operand | { param: string }` for `cmp` and `like` atoms.
  - In `exists.where` (`{ [field: string]: WhereValue }`): a parameter reference is encoded as the reserved `$`-prefixed string `'$param.<name>'` (`PARAM_TERM_PREFIX = '$param.'`, parsed by `parseParamTerm(s)`), parallel to `'$row.<col>'`.
  - Schema parameters are only valid in `'row'` context (table `ALLOW` rules) — never in `canDeploy`, `canObserve`, `FILES` `canWrite`, or queries.
  - Every operand walker (`rschema/expr.ts`, `rschema/format_predicate.ts`, `rschema/validate.ts`, `rtable_group/predicates.ts`, and `rdb_lang`) handles `{ param: string }` and `'$param.<name>'`.

#### Schema Resolution & Validation (`modules/rdb/src/rschema/`)

- `SchemaState` (`resolve.ts`) folds `params: Map<string, SchemaParamDecl>` across the causal past of `at`. Concurrent schema branches that declare the same parameter name converge only when both `type` and `default` are equal; otherwise the higher schema version (then larger entry hash) wins.
- `RSchemaView` (`interfaces.ts`, `view.ts`) exposes `getParams(): SchemaParamDecl[]` and `getParam(name: string): SchemaParamDecl | undefined`.
- In `validate.ts` and `validate_ops.ts`:
  - Parameter names must satisfy `isValidName(name)` and be unique within `params`.
  - Every `$param.<name>` / `{ param: '<name>' }` in `create` or `schema-update` must be declared in the schema's effective `params` at that entry, with a `ColumnType` compatible with the target column / operand in `checkPredicateColumns`.

#### Group Payloads, Validation, and Revisable LWW Resolution

To keep `RTableGroup` completely autonomous (never reading `RCatalog` or `RDb`) while sharing one `RSchema` object across groups:

1. **Literal values on `CreateTableGroupPayload` and `deploy` (`RefAdvancePayload`):**
   - `CreateTableGroupPayload` gains `params?: { [name: string]: json.Literal }` (values set at group creation).
   - A schema deploy (`ref-advance` of the group's schema ref) gains `params?: { [name: string]: json.Literal }` (values added or revised by that deploy). `RTableGroup.deploy(refVersion, author?, at?, params?)` and `prepareDeploy` gain the `params` argument.
   - A deploy that carries `params` also tags its meta with the names it writes (`param: [name, ...]`, alongside the `ref` and `barrier` values of the ref-advance meta), so the per-parameter fold in item 3 can use the DAG's indexed cover lookups.
   - *(Note on open deploys: a group with no `ALLOW DEPLOY IF` and no RDb creators has open deploys, so anyone can deploy it and write its `params`, just as they can move its schema version. An adversarial setting always gates deploys.)*
2. **Validation and read-time fallback on `RTableGroup` (`rtable_group/`):**
   - **Format validation (`rtable_group/validate.ts`):** Every parameter name in `params` must satisfy `isValidName`, and any string value starting with `'$'` is rejected.
   - **Op validation (`rtable_group/validate_ops.ts`):**
     - Any parameter name in `payload.params` that the target schema version (`create.schemaVersion` for `create`, `refVersion` for `deploy`) does not declare is rejected.
     - A deploy whose `params` is non-empty must move the version: its `refVersion` must be at or above the group's current schema version at `at` **and differ from it**. (`validateRefAdvanceMonotonicity` accepts a deploy to the current version; that stays valid for deploys without `params`, but such a deploy cannot carry values.)
     - A deploy's meta `param` tag must list exactly the names in its `params`.
   - **Read-time evaluation (`rtable_group/predicates.ts`):** A parameter's effective value at `(opPos, from)` is the group's resolved write from `resolveGroupParams(opPos, from)`, falling back to `schemaView.getParam(name)?.default`. If neither exists, or if the value does not match `schemaView.getParam(name)?.type` (e.g. after a concurrent schema branch won with a different type), every atom that reads the parameter evaluates to `false`.
3. **Revisable per-parameter slots and concurrent ranking:**
   - A parameter value is a revisable slot on the group: a later release can fix a botched value or update a limit without creating a new group, by moving the group to a higher schema version with new `params` on that deploy. There are no params-only deploys.
   - Because a deploy is a barrier, a concurrent deploy that changes a parameter value revises concurrent row ops at the merge, matching the behavior of a concurrent deploy that tightens an `ALLOW` rule.
   - **Per-parameter fold (`resolveGroupParams(at, from)`):** The latest write to `p` is not necessarily on the latest deploy (a later deploy to `1.3.0` may leave `:max_len` as a deploy to `1.2.0` set it), so the fold cannot reuse the latest-deploys cover that `resolveSchemaVersion` uses. For each parameter `p`:
     - the causal candidates are the cover of deploys writing `p` at `at` (`findCoverWithFilter(at, { containsValues: { ref: [schemaRef], param: [p] } })`);
     - the concurrent candidates are the deploy barriers writing `p` that are concurrent to `at` and visible from `from` (`findConcurrentCoverWithFilter(from, at, { containsValues: { ref: [schemaRef], barrier: ['t'], param: [p] } })`), as for the schema version's widening;
     - `create.params[p]` is the base when there is no candidate;
     - candidates causally below another candidate are dropped, and the rest are ranked by the rule below.
   - Every parameter write is tied to the target schema version (`Set<B64Hash>` of `RSchema` entry hashes) it is written with: `create.schemaVersion` at creation, `changes[hash].version` in a catalog release, and `refVersion` on a deploy.
   - **Resolution rule (applied to *writes*, not inherited parent states):** The causally latest write to parameter `p` wins. Among concurrent writes to `p`, rank by:
     1. highest schema semver among the write's target `RSchema` entries;
     2. the write's target `RSchema` entry hashes (compared as a sorted comma-joined key);
     3. level-local entry hash (deploy entry hash in the group; release entry hash in the catalog).
4. **Catalog, Creation, Catch-Up, and Status (`rcatalog/` & `rdb/`):**
   - **In `RCatalog` (`payload.ts`):** `CatalogGroupDef` and `CatalogGroupChange` carry `params?: { [name: string]: json.Literal }` (concrete literals only; no catalog-param references).
   - **Catalog resolution (`rcatalog/resolve.ts`):** `CatalogGroupState` folds the per-parameter writes in the release's causal past under the ranking rule above.
   - **Catalog validation (`rcatalog/validate_ops.ts`):**
     - `validateGroupDef` checks that `def.params` declares no unknown params, that every value fits its declared `ColumnType` at the pinned `RSchemaView`, and that every parameter without a `default` in the pinned `RSchemaView` is supplied.
     - `validateChange` keeps today's rule that a change must move the group's schema version; `change.params` is only allowed on such a change. It checks that `change.params` names only parameters of the target `RSchemaView` with values of valid types, and that the resulting folded `params` plus schema defaults cover every parameter of the target `RSchemaView`.
   - **Group creation vs. catch-up (`rdb/instantiate.ts` & `rdb/catalog_update.ts`):**
     - `instantiateGroup` copies `def.params` into `CreateTableGroupPayload.params` unchanged (never using later releases' folded values), so `groupIdOf(payload)` is invariant across releases.
     - `MemberGroup` carries `targetParams: { [name: string]: json.Literal }`, defined as the parameter values that a merge of the RDb's maximal deployed releases would inherit under the catalog ranking rule above.
     - In `planCatalogUpdate`:
       - When creating a missing group whose `member.target` differs from `def.schemaVersion`, the planner plans a catch-up `deploy` to the target version, carrying the values in `member.targetParams` that differ from `def.params`. (Values only change with version moves, so a group whose target is still `def.schemaVersion` has `def.params` as its target values.)
       - For an existing group, the planner plans a `deploy` only when the group's current schema version is below `member.target`, carrying the values in `member.targetParams` that differ from the group's resolved values at its frontier.
       - A group at its target version whose values differ from `member.targetParams` (possible only after a full tie between concurrent releases, see the ranking rule) gets no deploy: the next release that moves its version fixes it.
   - **Status (`rdb/conformance.ts`):** `catalogStatus` compares each member's resolved parameters against `member.targetParams` and reports any parameters whose values differ from the target.

### 2.4 Static Cross-Group Checking and Fail-Closed `existsTruth`

1. **Fail-closed foreign `EXISTS` at runtime (`modules/rdb/src/rtable_group/predicates.ts` & `rtable/view.ts`):**
   - Today, `candidateRowIds` in `rtable/view.ts` throws when a searched `WHERE` column is missing or not `PUB` on a table, which validation treats as an infrastructure deferral rather than `false`.
   - Fix: in `existsTruth` (or `existsMatching`), before calling `candidateRowIds`, check that every searched field in `where` (other than `rowAuthor`) is a `PUB` column of the resolved table view; if not, return `FALSE`.
2. **Static cross-group checks on both `add` and `changes` in `RCatalog` (`modules/rdb/src/rcatalog/validate_ops.ts`):**
   - Extend `groupViewReason` and run it in **both** `validateGroupDef` (when a group is added) and `validateChange` (when a group's schema version moves, with or without new params), against the bound groups' `RSchemaView`s at their versions in the same release:
     - every `tableMaps[alias][schemaTable]` entry maps to a `foreignTable` that exists in the bound group's `RSchemaView` (unused entries are permitted as long as `foreignTable` exists);
     - every qualified FK target `alias.table` and schema `EXISTS alias.table WHERE ...` atom resolves (via `tableMaps`) to an existing table in the bound group's `RSchemaView`, and every searched `WHERE` column exists, is `PUB`, and has a compatible type;
     - `ALLOW DEPLOY IF` and `USING IDENTITIES alias.table` resolve by real table name (without `tableMaps`) against the bound group's `RSchemaView` (and `IDENTITY PROVIDER` for `idProvider`);
     - `ALLOW UPDATE REF b` (`b => H`) is checked in `H`'s frame using real table names: unqualified `t` against `H`'s `RSchemaView`, and qualified `a.t` against the `RSchemaView` of `H.bindings[a]`;
     - the group's `params` are checked against the target `RSchemaView`'s parameter declarations.

---

## 3. Changes in `rdb_lang`

### 3.1 Syntax & AST (`modules/rdb_lang/src/syntax/`)

- **`CreateSchemaStatement` and `AlterSchemaStatement`:**
  - Gain `params: SchemaParamDeclExpr[]` (`{ name: string; type: ColumnTypeName; defaultValue?: ValueExpr; span: TextSpan }[]`), parsed via `PARAMS (:name type [DEFAULT <literal>], ...)` before `AS`.
- **`ImportSchemaStatement` in source mode (`target-catalog.sql`):**
  - `parseScript(sql, { catalogVersionOptional: true })` accepts `IMPORT SCHEMA <name> VERSION '<semver>';` (`{ kind: 'import-schema', name: string, version: string, span: TextSpan }`) before `CREATE CATALOG`. (`IMPORT` is a contextual keyword at statement start when followed by `SCHEMA`.)
- **`ConstantStatement` (optional follow-up after core params land):**
  - `CONSTANT :name = <literal>;` (`{ kind: 'constant', name: string, value: ValueExpr, span: TextSpan }`), accepted at top level in scripts.
  - **Rules:**
    - **Literal only:** The value must be a literal (string, number, boolean), not an expression, a catalog param, or another constant.
    - **Scope:** From its declaration onward in the script, usable wherever a literal is accepted (`WITH PARAMS`, `WITH ROWS`, and literals in `ALLOW` rules).
    - **No redeclaration or name collisions:** Declaring the same constant name twice is an error, and a constant may not share its name with a catalog param or with a schema param of any schema used in the script.
- **`CatalogBindingExpr` (on `CatalogGroupExpr`):**
  - Gains optional `tables?: { from: string; to: string; span: TextSpan }[]`, parsed after `alias => group` when followed by `(`:
    ```sql
    BIND target => blog (subject => posts, categories => tags), auth => users
    ```
- **`CatalogGroupExpr` and `CatalogChangeExpr`:**
  - `CatalogGroupExpr` gains `params: ParamAssignment[]`, parsed from `WITH PARAMS (:name = <literal>, ...)` on a `TABLEGROUP`. (`WITH` on a `TABLEGROUP` dispatches to `WITH ROWS (...)` or `WITH PARAMS (...)`.)
  - `CatalogChangeExpr`'s `update-schema` variant (`UPDATE SCHEMA s TO v ON group [WITH PARAMS (:name = <literal>, ...)]`) gains `params: ParamAssignment[]`.
- **Predicate expressions:**
  - In table `ALLOW` rules, `:name` is parsed in both `EXISTS ... WHERE col = :name` and comparison/like operands (`col = :name`, `length(col) <= :name`).

### 3.2 Binder & Compiler (`modules/rdb_lang/src/bind/` & `compile/`)

- **Default `CREATORS` on `CREATE SCHEMA` (`bindCreateSchema`):**
  - When `ast.creators` is empty, `bindCreateSchema` resolves the effective author (`context.currentAuthor()`); if present, `creators` defaults to `[{ keyId: author.keyId, publicKey: author.publicKey }]` (matching `bindCreateCatalog`).
- **`IMPORT SCHEMA` binding (source mode):**
  - The binder only knows schemas created by the script itself. An `import-schema` statement instead binds `<name>` to a schema object supplied by the caller: the execute options gain an `imports` resolver (`(name: string, version: string) => { schemaId: B64Hash; at: Version } | undefined`), which `rdb_runtime`'s `execute` passes through to the binder.
  - `IMPORT SCHEMA <name> VERSION '<v>'` resolves through it, failing with a diagnostic when the resolver has no `(name, version)`. A `TABLEGROUP ... USING SCHEMA <name>` then uses that object, pinned at the returned `at` (the catalog entries mirroring the pack step for `<v>`), instead of the source-mode version stamp.
  - The name joins the same scope as `CREATE SCHEMA` names: importing a name twice, or importing and creating the same name, is a binder error.
- **Observed-frame `ALLOW UPDATE REF` compilation fix (`compileCatalogGroup` in `compile/catalog.ts`):**
  - Today, `compileCatalogGroup` lowers `canObserve[clause.binding]` using the *observing* group's schema view as `gateScope`. Fix it to lower `canObserve[clause.binding]` against the **observed** group's schema view (at its version in the catalog being compiled) for unqualified tables, and against the observed group's own bound groups' schema views for qualified `a.t` references.
- **Schema `PARAMS` compilation (`compileCreateSchema`, `compileMigrationRules`, `lowerRestrictionPredicate`):**
  - `RuleScope` carries `params?: Map<string, ColumnType>`.
  - `:name` in `EXISTS ... WHERE` lowers to `'$param.' + name`; `:name` in an operand lowers to `{ param: name }`.
- **Catalog group & change compilation (`compileCatalogGroup`, `bindAlterCatalog`):**
  - Compiles `binding.tables` into `def.tableMaps` (rejecting duplicate `from` keys or `from === to`).
  - Compiles `WITH PARAMS` on `TABLEGROUP` and `UPDATE SCHEMA` into `def.params` / `change.params` as canonical `json.Literal`s (rejecting catalog instance params with a message explaining that schema params take literals or constants).

### 3.3 Reverse Rendering (`modules/rdb_lang/src/reverse/`)

- `renderCreateSchema`, `renderSchemaUpdate`, `renderSourceSchema`: render `PARAMS (:name type [DEFAULT <lit>], ...)` when `payload.params` is non-empty.
- `renderCatalogGroupDef` (`render.ts`): renders `BIND alias => group (from => to, ...)` from `def.tableMaps` and `WITH PARAMS (:name = ..., ...)` from `def.params` (the definition as written in that release).
- `renderSourceGroup` (`source.ts`): renders `BIND alias => group (from => to, ...)` from `SourceGroup.tableMaps` and `WITH PARAMS (:name = ..., ...)` from `SourceGroup.params` (which `rpack` fills with the group's **folded** parameter values in `BaseState`, so `rpack new` / `writeSource` seeds `target-catalog.sql` with the current values rather than stale genesis values).
- `renderCatalogRelease` (`render.ts`): renders `WITH PARAMS (...)` on `UPDATE SCHEMA` when `change.params` is non-empty.
- `renderPredicate` (`formatPredicate` in `modules/rdb/src/rschema/format_predicate.ts`): renders `'$param.<name>'` and `{ param: '<name>' }` as `:<name>`.

---

## 4. Schema Packs, `IMPORT SCHEMA`, and the Registry (`rpack` & `rhost`)

### 4.1 Schema Pack Repositories (`"kind": "schema"`)

An `rpack` repository is either a **catalog** repository (the default) or a **schema** repository:

```json
{
  "kind": "schema",
  "name": "hhs:comments",
  "released": {
    "1.0.0": "hhs_comments-1.0.0-a1b2c3d4",
    "1.1.0": "hhs_comments-1.1.0-e5f60718"
  }
}
```

- **`rpack.json`:**
  - `kind?: 'catalog' | 'schema'` (defaults to `'catalog'`; omitted when `'catalog'` so existing `rpack.json` files are unchanged).
  - `key` is required for `'catalog'` and optional/omitted for `'schema'`, since schema pack releases are unsigned templates that the consuming catalog signs.
  - `imports?: { [schemaName: string]: { [version: string]: B64Hash } }` on a catalog `rpack.json` locks the expected release step hash of each imported schema version.
  - `registry?: string` is optional on both: a path (relative to the repo root or absolute) used when `--registry` is not passed.
- **Work folder (`work/<version>/`) in a schema repository:**
  - `version.json`: `{ "base": [...], "note"?: "..." }` (identical to catalog repos).
  - `target-schema.sql`: holds a single `CREATE SCHEMA <name> [VERSION '<v>'] [PARAMS (...)] AS (...)` statement. `name` must match `rpack.json`'s `name`, and `CREATORS` must be omitted.
  - `upgrade-manual.sql`: optional `ALTER SCHEMA <name> AS (...)` steps that the diff cannot infer (such as `DROP COLUMN` for a reset).
  - `.released/`: holds `target-schema.sql`, `upgrade-manual.sql`, and `update.sql` as released.
  - `build/update.sql`: generated by `rpack build`.
- **Every step changes something:** In a schema repository, `rpack build` / `rpack release` refuses a version that produces neither migration rules nor new parameter declarations (including a merge of parent steps that adds neither).

### 4.2 Schema `.rpack` Release File Format

Both catalog releases and schema releases use the `.rpack` extension. In `rpack/src/format.ts`, `checkReleaseFile` branches on `manifest.kind` (`'catalog'` when omitted, or `'schema'`).

A schema `.rpack` file has `manifest.kind: "schema"` and carries the topological DAG of schema steps in its causal past, with **unique version strings across steps**:

```json
{
  "format": 1,
  "manifest": {
    "kind": "schema",
    "name": "hhs:comments",
    "parents": ["<parent step hash>"],
    "release": "<step hash>",
    "version": "1.1.0",
    "note": "..."
  },
  "steps": [
    {
      "hash": "<step hash>",
      "parents": [],
      "version": "1.0.0",
      "params": [{ "name": "write_cap", "type": "string" }],
      "tables": [ ]
    },
    {
      "hash": "<step hash>",
      "parents": ["<1.0.0 step hash>"],
      "version": "1.1.0",
      "params": [{ "name": "max_len", "type": "integer", "default": 1000 }],
      "migration": [ ]
    }
  ]
}
```

- **Step hashing:** Each step's `hash` is `sha256(json.toStringNormalized({ domain: 'hhs3-rpack-schema-step-v1', name, version, parents, params?, tables?, migration?, note? }))`.
- **Unique versions within a pack:** Every step in `steps` has a distinct `version` string, so `RSchema`'s per-slot resolution never falls back to entry-hash tiebreaking between concurrent steps, ensuring the pack resolves identically when mirrored into any catalog's `RSchema`.
- **Verification (`rpack verify <file.rpack>`):** Recomputes every step hash, checks topological order, unique versions, non-empty step changes, and semver monotonicity along `parents`, and replays the `steps` DAG in a scratch `RSchema` instance (preserving the exact parent links) to verify that all `TableDef`s, `SchemaParamDecl`s, and `MigrationRule[]` sequences are valid.

### 4.3 Importing Schema Packs and Pinning in `imports/`

A catalog repository that uses `IMPORT SCHEMA <name> VERSION '<version>';` keeps vendored copies of the imported `.rpack` files in its **`imports/`** directory at the repository root:

- **Manifest-based lookup and hash lock:**
  - Files in `imports/*.rpack` are indexed by parsing their `manifest` (`manifest.name`, `manifest.version`, `manifest.release`), never by parsing filenames (since replacing `:` with `_` in filenames is not one-to-one).
  - Every imported `(name, version)` used by a work folder must have its step hash locked in `rpack.json`'s `imports[name][version]`, verified whenever `imports/` is read.
  - `readSource`, `draftRelease`, and `releaseVersion` read **only** from `imports/` and never silently fetch from the external registry as a side effect of `build` or `release`. Only `rpack import` fetches from the registry into `imports/` and records the hash in `rpack.json`.

### 4.4 How `rpack` Derives `build/update.sql`, Switches Origin, and Produces Releases

#### 1. DAG-Preserving Replay and Switching Between Inline & Imported
- **Mirroring from pack genesis on first import:**
  - When a catalog first introduces `<name>` via `IMPORT SCHEMA <name> VERSION '<v>'` (even if `<v>` is `'1.2.0'`), `draftRelease` creates the catalog's `RSchema` at the pack's **genesis step** and replays every step in `<v>`'s causal past along the pack's DAG structure. That way, any future branch in the pack (such as a `1.1.1` branched from `1.1.0` and merged in `1.3.0`) always has its corresponding parent entry in the catalog's `RSchema` DAG to attach to.
- **Finding mirrors in a released catalog (by version, confirmed by content):**
  - For an existing schema in `base.schemas`, `rpack` locates the catalog `RSchema` entry(ies) mirroring a pack step by matching the step's unique `version` string and **confirming by content** that the catalog's resolved schema state (`tables` and `params`) at those entries equals the pack's resolved state at that step.
  - For each pack step in `<v>`'s past whose parents are all mirrored in `baseSchema` (in topological order), `draftRelease` emits an `update` step whose `at` position is the union of the catalog `RSchema` entry hashes mirroring that step's pack `parents`.
  - After replaying, `rpack` verifies that the resulting catalog `RSchemaView` at `<v>` equals the pack's resolved state at `<v>` (`tableDifferences` empty and `params` equal).
- **Switching from Imported → Inline ("ejecting"):**
  - Replacing `IMPORT SCHEMA <name> ...` with `CREATE SCHEMA <name> ...` in `target-catalog.sql` simply takes the inline diff path: `draftRelease` diffs `baseSchema`'s resolved tables and params against the inline `CREATE SCHEMA` and emits one `ALTER SCHEMA` step.
- **Switching from Inline → Imported:**
  - Replacing an inline `CREATE SCHEMA <name>` with `IMPORT SCHEMA <name> VERSION '<v>'` when `baseSchema`'s current version is not already a mirror of the pack works by **diffing toward the pack**: `draftRelease` diffs `baseSchema`'s state (after `upgrade-manual.sql`) against the pack's resolved state at `<v>`, emits one `ALTER SCHEMA <name> VERSION '<v>'` step, and confirms by content that the result matches the pack at `<v>`. That catalog entry becomes the mirror of `<v>`. Subsequent upgrades replay pack steps whose parents lie at or above `<v>` (refusing if a later pack step branches from an unmirrored step below `<v>`).

#### 2. File-by-File `rpack` Internals (`modules/rpack/src/`)

- **`project.ts`:** `RpackConfig` gains `kind?: 'catalog' | 'schema'`, `imports?: { [schema: string]: { [version: string]: B64Hash } }`, and `registry?: string`. `parseRpackConfig` and `formatRpackConfig` handle them; `key` is optional when `kind === 'schema'`.
- **`model.ts`:**
  - `SchemaModel` gains `params: Map<string, SchemaParamDecl>` and its origin (`inline`, or `imported` with the pack's `version` and `steps`).
  - `GroupModel` (`rdb_lang`'s `SourceGroup`) gains `tableMaps` and `params: { [name: string]: json.Literal }`.
  - `groupKey` and `groupDifferences` include `tableMaps` (fixed at creation, so a change is refused) and exclude `params` (which are revisable).
  - `modelDifferences` compares `SchemaModel.params` and the groups' **effective** parameter values (explicit value, else the group's schema `default`, as in `diffCatalog`) so the fixpoint checks in `produce.ts`, `write_source.ts`, and `commands.ts` verify them without flagging an explicit default as a difference.
- **`source.ts`:**
  - `checkSourceForms` accepts `IMPORT SCHEMA` and `CONSTANT` statements before `CREATE CATALOG`, and rejects a schema name that is imported twice or both imported and created inline.
  - `statedSchemaVersions` records the imported version for each `IMPORT SCHEMA` alongside `CREATE SCHEMA ... VERSION`.
  - Before executing the script in the scratch `RdbRuntime`, `readSource` loads each imported schema pack from `imports/` (indexed by manifest and verified against `rpack.json`'s `imports` lock) and creates its `RSchema` object in the scratch workspace by replaying the pack's `steps` DAG from its genesis up to the imported version (sharing the replay helper with `verify.ts`).
  - When the script executes, `readSource` passes an `imports` resolver (section 3.2) that maps each `(name, version)` to the scratch `RSchema` object and the entries mirroring the pack step for that version. `IMPORT SCHEMA <name> VERSION '<v>'` brings the object into scope under `<name>`, and a `TABLEGROUP` using `<name>` pins it at that step (not at the catalog-version stamp `schemaStampOf`).
  - `readSource` then reads the imported `SchemaModel` back from the scratch `RSchema` object at `<v>`, the same way it reads inline schemas.
- **`released.ts`:**
  - `BaseSchema` gains `params` and the version/content-confirmed mapping of pack steps to catalog `RSchema` entries.
  - `BaseGroup` gains `tableMaps` from its definition and the folded `params` from `CatalogGroupState`.
- **`draft.ts`:**
  - `SchemaStep` gains `params: SchemaParamDecl[]` on `create` and `update`, and `at?: B64Hash[]` on `update` (set per mirrored step to the catalog entries mirroring the step's pack parents).
  - `ReleaseDraft.changes` carries `params?: { [name: string]: json.Literal }`.
- **`schema_diff.ts`:** Diffs `SchemaParamDecl`s for inline schemas: refuses removing or retyping an existing param, and refuses an added param that has no `default`.
- **`catalog_diff.ts`:**
  - `CatalogChange` gains `params?: { [name: string]: json.Literal }`.
  - `diffCatalog` compares each existing group's **effective** parameter values: for every parameter of the target `RSchemaView`, the explicit value (in `wanted.params`, or in `baseGroup.params`) or else the schema's `default`. Writing a value equal to the effective one is not a change, so stating a default explicitly (`:max_len = 1000` when the group already uses `DEFAULT 1000`) produces nothing, and omitting an entry whose base value equals the default is not a removal.
    - If omitting a parameter from `wanted.params` changes its effective value (the base has an explicit value that differs from the default, or the param has no default), `diffCatalog` refuses with the hint: `"a param value can't be removed; to return to the schema's default, write the default's value"`.
    - Any parameter whose effective value differs is emitted in `CatalogChange.params` (as the wanted explicit value), on the change that moves the group's schema pin. If the values differ but the pin doesn't move, `diffCatalog` refuses: `the params of <group> changed, but its schema version did not; values only change with a version move` (hint: move the group to a schema version that changes something).
  - Warns when a schema update introduces a new `alias.table` reference that resolves by unmapped name in the bound group (`<alias>.<table>` binds to `<group>.<table>` without a `tableMaps` entry).
- **`produce.ts`:** Creates schemas with `params`, appends each `SchemaStep` update at its own `step.at ?? baseSchema.at` (updating the tracked pin for that schema), and writes `change.params` into `CatalogReleaseSpec.changes[defHash].params`.
- **`format_draft.ts`:** Renders `PARAMS (...)` on `CREATE SCHEMA` and `ALTER SCHEMA` steps, one `ALTER SCHEMA` statement per mirrored pack step, and `WITH PARAMS (...)` on `UPDATE SCHEMA`.
- **`write_source.ts`:** Edits `IMPORT SCHEMA ... VERSION`, `PARAMS (...)` on inline schemas, `BIND alias => group (from => to, ...)`, and `WITH PARAMS (...)` on groups (preserving `CONSTANT` statements and constant references when present).
- **`format.ts` & `verify.ts`:** Parse and serialize schema `.rpack` files (`manifest.kind === 'schema'`, `steps`), and verify them by DAG-preserving replay in a scratch `RSchema`.
- **`commands.ts`:** Supports `initProject` without a key for schema repositories, plus `publish` and `import`.

### 4.5 The `.rpack` Registry and CLI Commands

The registry is a directory on disk, resolved in order from:
1. `--registry <dir>` CLI flag,
2. `rpack.json`'s `"registry"` field (for `rpack` commands),
3. `RDB_REGISTRY` environment variable,
4. `$RDB_HOME/registry` or `~/.rdb/registry`.

Every package in the registry lives under `<registry>/<name_with_colons_as_slashes>/`:

```
~/.rdb/registry/
  editor/
    editor-1.0.0-6737fce1.rpack
  hhs/
    users/
      hhs_users-1.0.0-9c8d7e6f.rpack
    comments/
      hhs_comments-1.0.0-a1b2c3d4.rpack
      hhs_comments-1.1.0-e5f60718.rpack
```

#### `rpack` Commands

```
rpack init [--schema] <name> [--key <label>]
rpack publish [<version>] [--registry <dir>]
rpack import <schema> [<version>] [--registry <dir>]
```

- **`rpack init --schema <name>`:** Scaffolds a schema repository (`rpack.json` with `"kind": "schema"`, `releases/`, `.gitignore`), without requiring a keystore key. `rpack new <version>` writes `work/<version>/target-schema.sql` and `upgrade-manual.sql`.
- **`rpack publish [<version>] [--registry <dir>]`:**
  - Works in both schema and catalog repositories.
  - Verifies the release file(s) and copies them into `<registry>/<path>/`.
  - Refuses if `<registry>/<path>/` already holds `.rpack` files of a different `manifest.kind` (`schema` vs `catalog`) or (for a catalog) a different `manifest.catalog` genesis ID.
- **`rpack import <schema> [<version>] [--registry <dir>]`:**
  - Runs inside a catalog repository's work folder (`work/<version>/`).
  - Finds `<schema>` at `<version>` (or the highest version in `<registry>/<path>/` when omitted), refusing if `manifest.kind === 'catalog'`.
  - Copies the `.rpack` file into `imports/`, records `imports[schema][version] = manifest.release` in `rpack.json`, and inserts or updates `IMPORT SCHEMA <schema> VERSION '<version>';` in `target-catalog.sql`.

#### `rhost` Commands

```
rhost init (--release <file>... | --catalog <name> [--version <v>] [--registry <dir>]) [--param <name>=<value>]...
rhost update [<host>] [--pull [--registry <dir>]] [--yes]
```

- **`rhost init --catalog <name> [--version <v>] [--registry <dir>]`:**
  - Resolves `<name>` in `<registry>/<path>/` (refusing if `manifest.kind === 'schema'`), picks `--version` or the highest version, copies that `.rpack` file into `<app>/catalogs/`, prompts for any missing declared params, and writes `app.json`.
- **`rhost update --pull [--registry <dir>]`:**
  - Checks `<registry>/<path>/` for `.rpack` files with the same `manifest.catalog` genesis ID that descend from the installed release (within the app's adoption policy or highest descending version), copies the newest one into `<app>/catalogs/`, and runs `rhost update`.

---

## 5. Order of Work

1. **Independent fixes (`modules/rdb`, `modules/rdb_lang`):**
   - Make `existsTruth` / `existsMatching` return `FALSE` (instead of throwing in `candidateRowIds`) when a foreign table at the observed version lacks a searched `PUB` column.
   - Make `validateChange` in `rcatalog/validate_ops.ts` run `groupViewReason` and cross-group checks on target versions.
   - Fix `compileCatalogGroup` in `rdb_lang/src/compile/catalog.ts` to compile `ALLOW UPDATE REF` in the observed group's frame (against the bound group's schema and its bindings).
2. **Non-injective group bindings (`modules/rdb`, `modules/rdb_adapter`, `modules/rdb_runtime`):**
   - Lift the injectivity restriction on `bindings`; combine `canObserve` gates per `refId` with `and`; update `bindingNameForId` / `bindingNameFor`; deduplicate auto-observe loops by `groupId` in `catalog_update.ts`, `ref_advance.ts`, and `ref_auto_update.ts`.
3. **Table remapping (`tableMaps`) (`modules/rdb`, `modules/rdb_projection`, `modules/rdb_runtime`, `modules/rdb_lang`):**
   - Add `tableMaps` to `CatalogGroupDef` and `CreateTableGroupPayload`; add `resolveSchemaForeignTableViewIn` for schema-originated references while keeping `resolveForeignTableViewIn` un-remapped for catalog-authored references (`canDeploy`, `idProvider`, `canObserve`); wire `tableMaps` into `rdb_projection` (`buildScope`), `rdb_runtime` (`resolveFkRowId`), and `rdb_lang` (`BIND alias => group (from => to, ...)`).
4. **Schema `PARAMS`, default `CREATORS`, and `CONSTANT` (`modules/rdb`, `modules/rdb_lang`):**
   - Default `CREATORS` on `CREATE SCHEMA` to the effective author when omitted.
   - Add `params` (`SchemaParamDecl[]` with `default?`) and `RuleOperand` to `RSchema`; add literal `params` to `CatalogGroupDef`, `CatalogGroupChange`, `CreateTableGroupPayload`, and `deploy` (`RefAdvancePayload`); implement revisable slot resolution (the per-parameter fold over `param`-tagged deploys, ranked by target schema version, then target schema entry hashes, then level-local hash), the rule that a deploy carrying `params` must move the version, creation from `def.params` and catch-up by deploy in `planCatalogUpdate`, and `catalogStatus` reporting.
   - Parse, bind, compile, and reverse-render `PARAMS` on `CREATE SCHEMA` / `ALTER SCHEMA` and `WITH PARAMS` on `TABLEGROUP` / `UPDATE SCHEMA` (plus optional `CONSTANT :name = <literal>;` statements).
5. **Schema packs, `IMPORT SCHEMA`, registry, and CLI (`modules/rdb_lang`, `modules/rdb_runtime`, `modules/rpack`, `modules/rhost_node`, `modules/rdb_tools`):**
   - Parse and bind `IMPORT SCHEMA` through the `imports` resolver in `rdb_lang` and `rdb_runtime`'s `execute` options (section 3.2).
   - Support `"kind": "schema"` in `rpack`, DAG-preserving replay of imported schema packs (mirrors matched by version and confirmed by content, plus inline ↔ imported switching), hash-locked `imports/` in `rpack.json`, the `.rpack` registry, `rpack init --schema`, `rpack publish`, `rpack import`, `rhost init --catalog`, and `rhost update --pull`.

### Implementation Guardrail

> **Stop and Report on Unforeseen Issues:** If any unforeseen problems, ambiguities, or architectural conflicts arise during implementation, **stop immediately and present a comprehensive report** detailing the issue, the affected code paths, and the options. Do not make significant design or protocol decisions unadvised.

## Changes and tests

This section is `COMPOSABILITY_REPORT.md`, the implementation report written when the work landed (`52bd1b9`): what changed in each module, and the test suites added.

This document summarizes the changes implemented across `rdb`, `rdb_projection`, `rdb_adapter`, `rdb_lang`, `rdb_runtime`, `rdb_repl`, `rpack`, and `rdb_tools` according to the specification in `COMPOSABILITY.md` (with v2 observer-frame `ALLOW UPDATE REF` documented separately in [`REFERENCE_FRAME.md`](0014-observer-frame-ref.md)).

---

### 1. Overview of Capabilities Added

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

### 2. Module-by-Module Changes

#### 2.1 `modules/rdb`
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

#### 2.2 `modules/rdb_projection` & `modules/rdb_adapter`
- `modules/rdb_projection/src/scope.ts`: Wired `group.resolveForeignTableName(bindingName, table)` into `resolverFor` so cross-group foreign keys and `EXISTS` dependencies resolve through `tableMaps`.
- `modules/rdb_adapter/src/ref_advance.ts`: Deduplicated `buildObservedBy` by observer `groupId` when multiple aliases bind to the same target group.

#### 2.3 `modules/rdb_lang`, `modules/rdb_runtime`, & `modules/rdb_repl`
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

#### 2.4 `modules/rpack`
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

#### 2.5 `modules/rdb_tools`
- `bin/rpack.ts`: Added `rpack init --schema <name>`, `rpack publish [<version>] [--registry <dir>]`, `rpack import <schema> [<version>] [--registry <dir>]`, and the `rpack stage` refusal in schema repositories.
- `bin/rhost.ts`: Added `rhost init --catalog <name> [--version <v>] [--registry <dir>]` and `rhost update --pull [--major] [--registry <dir>]`.

---

### 3. New Test Suites Added

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

## History

This HIP was `modules/rpack/critique/COMPOSABILITY.md`, moved here by [HIP-1](0001-hips.md) on 2026-11-04. Text above that names a file by its old name refers to it as it was. The files it was built from can be read at commit `7445404` (`git show 7445404:<path>`).

- `modules/rpack/critique/COMPOSABILITY_CRITIQUE.md`, `COMPOSABILITY_CRITIQUE_v2.md`, `COMPOSABILITY_CRITIQUE_v3.md`: three reviews, each followed by a revision of the plan. The decisions they record: param values are literals that change only together with a schema version move; table maps are fixed at creation; references written in the catalog (`ALLOW DEPLOY IF`, `USING IDENTITIES`, `ALLOW UPDATE REF`) use real table names, and the observer-frame gate is deferred to a later design (both since changed by [HIP-14](0014-observer-frame-ref.md), which translates those clauses through table maps); imported packs are replayed preserving their DAG; and an imported schema must exist while `rpack` reads `target-catalog.sql` (v3 §1, the one gap that blocked schema packs).
- `modules/rpack/critique/COMPOSABILITY_REPORT.md`: folded in above as *Changes and tests*.
