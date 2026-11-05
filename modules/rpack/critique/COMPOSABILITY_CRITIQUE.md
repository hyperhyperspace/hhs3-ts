# Critique of the Composability Plan

A review of [COMPOSABILITY.md](./COMPOSABILITY.md) (table remapping, multi-alias bindings, schema `PARAMS`, schema packs and the `.rpack` registry) against the current code of `rdb`, `rdb_lang`, `rdb_projection`, `rdb_adapter`, `rdb_runtime` and `rpack`. It lists the places where the plan contradicts the code, the design risks, and the decisions taken to resolve them. Each section explains its issue from scratch, so it can be read without the plan at hand.

## Summary of Decisions

- **Schema params on updates:** `UPDATE SCHEMA <schema> TO <version> ON <group> WITH PARAMS (...)` is added to `ALTER CATALOG`. Values may be literals or forward catalog params (`:max_len = :default_max_len`), so the whole catalog stays a function of its params.
- **Params are revisable:** A param value is not write-once. A botched value is fixed by releasing again (a new catalog release that sets it). Among concurrent writes, the one written with the higher schema version wins, then the one whose target schema entries have the larger hash, at every level (catalog, RDb, group). Catalog release versions play no part.
- **Table maps are fixed at creation in v1:** `tableMaps` is part of the group's create payload and cannot change. No `REQUIRES` clause is added to schemas.
- **Catalog-authored references use the group's real names:** `ALLOW DEPLOY IF` and `USING IDENTITIES` name tables as they exist in the bound groups (`alias.<real table>`), not by the schema's symbolic names. Only references that come from the schema are remapped.
- **Imported schema packs are replayed preserving the pack's DAG**, not linearized.

---

## 1. Background

The rest of this document relies on these facts about the current model.

- An **`RSchema`** is a DAG of schema entries (`create`, then `schema-update`s), each carrying a strict semver `version` and a list of migration rules. The effective schema at a position is resolved per slot (table existence, columns, FKs, restrictions) by last-writer-wins: the causally latest write wins, and among concurrent writes the higher schema version wins, then the larger entry hash.
- An **`RTableGroup`** is the unit of data: one DAG holding the rows of all its tables. Its create payload pins an `RSchema` at a version and fixes, for its whole life:
  - `bindings`: alias → foreign group id, used to resolve qualified references `alias.table` in FKs and `EXISTS` atoms;
  - `canDeploy`, `canObserve`, `idProvider`, `deployKeys`, `initialRows`.

  The group id is the hash of this payload, so every field in it is part of the group's identity.
- A group moves to a new schema version through a **deploy**: a barrier ref-advance of its schema ref, gated by `canDeploy`. It sees a bound foreign group through an **observation**: a barrier ref-advance of that group's id, gated by `canObserve[alias]`. Both are validated when written and re-checked when read (view time), so a concurrent barrier can void an op after the fact.
- An **`RCatalog`** is a DAG of developer-signed releases. Each release adds group definitions (`CatalogGroupDef`, identified by their hash) and changes existing ones (`changes[defHash] = { schema, version }`). A merge release must set every group whose version its parents disagree on.
- An **`RDb`** is a database deployed from a catalog. Its DAG records which releases the admin deployed (`create`, `update-catalog`) and the values of the catalog's params (`ParamValue`). Its member groups are computed by the normative instantiation in `rdb/src/rdb/instantiate.ts`: group ids depend on the definition, the RDb id, and the param values used in `WITH ROWS`.
- One invariant shapes all of the param work (`rdb/src/rdb/payload.ts`):

  > RDb state never decides any group's validity: groups are fully valid and verifiable without their RDb, and never depend on it.

  A group never reads its catalog or its RDb. Anything a group's rules depend on must be in the group's own DAG or in the groups it binds.

---

## 2. Schema Params

### 2.1 How a Value Reaches a Group

A schema param (`:write_cap`, `:max_len`) is declared by the schema and read by its `ALLOW` rules, but its value is chosen per group in the catalog. Because a group cannot read its catalog (see the invariant above), the value has to travel into the group's DAG:

1. `CREATE CATALOG` / `ALTER CATALOG` sets the value in the group definition (`def.params`) or in a change (`changes[defHash].params`). The value is a literal or a reference to a catalog param.
2. `UPDATE CATALOG` deploys the release to an RDb. Catalog params get their values here, from the RDb.
3. The catalog update planner (`planCatalogUpdate`) resolves the group's values against the RDb's params.
4. The values enter the group: in the create payload (`params`, for values set at creation) or on a deploy (`params`, for values set later).
5. The group's rules read the values from its own DAG.

`ALTER CATALOG` is where values are written and versioned, and the deploy is how they reach the group. A design where groups read params from their catalog directly would break the invariant, make the catalog (and, through forwarded params, the RDb) a validation dependency of every group, and change every group id. It is rejected.

### 2.2 The `UPDATE SCHEMA ... WITH PARAMS` Construct

`ALTER CATALOG` already accepts `UPDATE SCHEMA <schema> TO <version> ON <group>` (`rdb_lang/src/syntax/parser.ts`). It gains an optional `WITH PARAMS` clause, compiled to `CatalogGroupChange.params`:

```sql
ALTER CATALOG blog_app VERSION '1.1.0' AS (
  UPDATE SCHEMA hhs:comments TO '1.1.0' ON post_comments
    WITH PARAMS (:max_len = :default_max_len),
  UPDATE SCHEMA hhs:comments TO '1.1.0' ON photo_comments
    WITH PARAMS (:max_len = 500)
);
```

Values may forward catalog params, so a catalog stays a function of its params. The cost falls on tooling: `rpack` and the catalog validator can check types and coverage, but not concrete values, since those only exist per RDb.

Values are set per group because one `RSchema` object is shared by every group that uses it (`post_comments` and `photo_comments` both use `hhs:comments`), so they cannot live in the schema.

Two typing rules need to be written down:

- **Forwarding compatibility:** a counterpart of `paramFitsColumn` (`rcatalog/validate_ops.ts`) for catalog param → schema param. For example, an `identity` catalog param can feed an `identity` or `string` schema param.
- **`publicKey(:p)`:** `WITH ROWS` accepts it; `WITH PARAMS` should accept it too (an identity catalog param feeding a `string` schema param that holds a public key) or reject it explicitly.

### 2.3 Values Are Revisable

The plan makes group params write-once: a later change may not alter a value that is already set. That cannot hold:

- Two concurrent `UPDATE CATALOG` ops can deliver two values to one group.
- RDb catalog params resolve concurrent writes by the larger op hash (`resolveParams` in `rdb/src/rdb/resolve.ts`), so a forwarded value can change after a merge, and the RDb's choice need not match whatever the group picked.
- A developer can simply get a value wrong.

**Decision:** a param value is a slot like a schema slot. A later write replaces it. A botched value is fixed by releasing again: a new catalog release whose `UPDATE SCHEMA ... WITH PARAMS` sets the right value. No special repair mechanism exists beyond that.

This requires, in the plan:

- `validateChange` (`rcatalog/validate_ops.ts`) accepts `change.params` for any param of the target schema version, including one already set. The plan's "never overwriting a settled parameter" rule is dropped.
- `diffCatalog` (`rpack/src/catalog_diff.ts`) emits a change when a group's `WITH PARAMS` differs from the base, instead of refusing it.

### 2.4 Concurrent Writes: Schema Version, Then Schema Hash

A param's value only exists at a table group. It is part of the group's effective state at a group position, resolved like the group's effective schema. The RDb holds no values of its own for schema params: the catalog and the RDb's catalog params only decide what the planner writes to the group, and the group's resolution is the only one that counts.

Every param write is tied to the schema version it is written with:

- a value set at creation, to the group's pinned version (`schemaVersion`);
- a catalog change, to its `TO` version (`changes[hash].version`);
- a deploy, to its target version.

Each of these is a set of `RSchema` entry hashes, and the same set means the same thing at every level. Catalog release versions never take part.

**The rule:** the causally latest write wins. Among concurrent writes, rank by:

1. the highest schema version (semver) among the target's entries;
2. the target's schema entry hashes (the `ALTER SCHEMA` entries, compared as a sorted set);
3. last, a level-local hash: the deploy entry hash in the group, the release hash in the catalog.

The second key is the hash of the observed schema, not of the deploy that carried the value. It is the same on every replica and at every level, so the catalog's fold ranks writes exactly as the group will. A deploy's hash, by contrast, depends on who deployed and at which group position: two planners deploying the same release produce different hashes, so the winner would depend on who happened to run first.

The rule is applied to *writes*, not to parent states. A parent that only inherits a value has not written it. For example: release `A` moves the group to schema `1.1.0` with `:p = 1`. Release `B` descends from `A` and moves the group to schema `1.2.0` without touching `:p`. Release `C` also descends from `A` and fixes `:p = 2` at schema `1.1.0`. The only write not below another is `C`'s, so a merge of `B` and `C` inherits `:p = 2`, even though `B` brings the higher schema version.

- **Catalog (merge releases).** `CatalogGroupState` folds a `params` slot per group over the writes in the causal past (`def.params`, then each `changes[hash].params`), ranked as above. Unlike version conflicts, a param conflict does not force the merge to set it: the rule picks a value, and the merge (or any later release) may override it. Values are compared as written (`{ param: 'x' }` versus `{ value: 5 }`), not resolved, so catalog validation stays independent of any RDb.
- **RDb (concurrently deployed releases).** An RDb can have several maximal deployed releases. `MemberGroup.target` is already the union of the group's versions across them. The planner's target params for a member are what a merge of those releases would inherit under the catalog rule, resolved against the RDb's catalog params.
- **Group (concurrent deploys).** The group's params slot folds the create payload's `params` and the `params` carried on deploys at or below `at` (widened by concurrent deploys visible from `from`, as schema versions already are), ranked as above.

The first two keys agree across levels. Only a full tie can make them differ: two concurrent writes that target the same schema entries with different values, for example two concurrent releases that both move the group to `1.1.0` with different `:max_len`. The group then decides by deploy entry hash and the catalog by release hash, so the catalog can't predict the group's pick. On its next run, the planner compares the group's resolved params with its target params and deploys the difference; that deploy is causally later, so it wins. If the result is still not the desired one, the fix is to release again.

Fixes that only set params (section 2.5) repeat the group's current version, so they target the same schema entries as the write they replace. They don't tie with it, because they are causally after it. Only two concurrent fixes to the same version tie.

### 2.5 Changes That Only Set Params

Fixing a value without moving the schema version is a normal case now. Three places reject or skip it today:

- `validateChange` refuses a change that doesn't move the version (`the change of group ... does not change its version`). It must accept a change whose version is unchanged and whose `params` is non-empty.
- `ParentFold` (`rcatalog/resolve.ts`) only tracks version conflicts. It needs the per-group params slot from 2.4.
- `planCatalogUpdate` (`rdb/src/rdb/catalog_update.ts`) skips any group already at or above its target version. It must also deploy when the group's params differ from the target params.

A deploy to the group's current version is already valid at the group level: `validateRefAdvanceMonotonicity` (`mvt/src/refs.ts`) accepts a new version at or above the current one. So a params-only fix is a deploy that repeats the current version and carries `params`. (A dedicated `set-params` op would also work and keep deploys purely about versions; the plan should pick one. Reusing the deploy is the smaller change.)

### 2.6 What Revisable Params Mean at Read Time

Params are resolved at `(opPos, from)` like the schema version. A deploy that changes a value is a barrier, so a row op concurrent with it is re-checked against the new value once both are visible, and may be voided. That is the same behavior as a concurrent schema deploy that tightens an `ALLOW` rule, and it is intended. It should be stated in the plan and covered by tests in `VOID_SEMANTICS.md` style.

Values set at creation (`def.params` resolved into the create payload) are part of the group id, as `WITH ROWS` values already are. If a forwarded RDb param changes after a merge, membership computes a different group id. This is existing behavior inherited from `WITH ROWS`, not new, but schema params make it more likely and the plan should say so.

### 2.7 Who Can Write Values

Values arrive on deploys, and deploys are gated by `canDeploy`. With no `ALLOW DEPLOY IF` and no RDb creators, deploys are open (`instantiate.ts`), so anyone can deploy and set `:write_cap` to a value that grants them access. The plan should either refuse schemas that declare params in groups whose deploys are open (a catalog validation rule), or document the exposure.

### 2.8 Where Params Can Appear

- Only in table `ALLOW` rules (row context). Not in `canDeploy`, `canObserve`, FILES `canWrite`, or queries.
- **The operand type:** the plan adds `{ param: string }` to `Operand`. `Operand`, `evalOperand`, `cmpTypesOk` and `likeTypesOk` are shared with query filters (`rdb/src/rtable/query.ts`), so the new operand would become valid in queries. Rules should get their own operand type (`RuleOperand = Operand | { param: string }`), leaving the query path unchanged.
- The plan resolves `'$param.<name>'` in place inside `existsTruth`, which is right. Substituting the value into the predicate first would be wrong: a string value such as `'$author'` would then be read as a term, because `$`-prefixed strings are reserved for terms in `where` values and `lit` operands (`rschema/validate.ts`). Rejecting `$`-prefixed values for string params also closes this.
- Every place that walks operands needs the new case: `rschema/expr.ts`, `rschema/format_predicate.ts`, `rschema/validate.ts`, `rtable_group/predicates.ts`, and the `rdb_lang` compiler and reverse renderer.

### 2.9 Concurrent Param Declarations in a Schema

The plan's rule stands: concurrent schema branches that declare the same param with the same type converge; with different types, the higher schema version wins, then the larger entry hash, and a rule whose param type doesn't match the winner evaluates to false. Group values are checked against the winning type, so a value valid for the losing type is ignored (the param reads as unset, and atoms over it are false).

Whether `ALTER SCHEMA` params may declare a `DEFAULT` is optional now that values can always be supplied by a later release. A default would let a release deploy a new schema version without listing every new param.

### 2.10 Status Reporting

`catalogStatus` (`rdb/src/rdb/conformance.ts`) reports per member whether the group's version matches its target. With revisable params, a group can be at the right version with the wrong values. Members need a "params differ from the target" state, listing the differing params.

---

## 3. Table Maps

### 3.1 Fixed at Creation in v1

`tableMaps` lives in `CreateTableGroupPayload` and `CatalogGroupDef`, so it is part of the group id and never changes. This is accepted for v1. Its consequence must be documented:

A schema pack that, in a later version, adds a new foreign reference (say `hhs:comments 1.2.0` adds `REFERENCES target.category`) cannot get a mapping for it in groups created before. The new name resolves unchanged, to `blog.category`. Either that table doesn't exist (the FK dangles and writes are rejected), or a table with that name exists and the reference binds to it, intended or not.

Mitigations, all without changing the payload:

- **Allow mappings for tables the schema doesn't reference yet.** The plan's check that every `tableMaps` entry is referenced by the schema at its pin should be relaxed: an entry whose target table exists in the bound group is valid even if unused (a warning at most). That lets a catalog map tables ahead of a pack upgrade.
- **Check new references on every change.** When a change moves a group to a schema version that introduces a new `alias.table` reference, catalog validation and `rpack build` check where it resolves (see 5.1). If it resolves to no table, the change is refused. If it resolves by name to an unmapped table, `rpack` warns that the reference binds to `<group>.<table>` by name.
- **The real fix is a new group.** A group that needs a different mapping is created anew (a new definition), with the usual data migration story.

### 3.2 Which References Are Remapped

There are two kinds of qualified references, and they live in different name spaces:

- **Schema-originated references:** `REFERENCES alias.t` FKs and `EXISTS alias.t` atoms in the schema's `ALLOW` rules. They use the schema's symbolic names and are remapped through `tableMaps`.
- **Catalog-authored references:** `ALLOW DEPLOY IF` (`canDeploy`) and `USING IDENTITIES alias.t` (`idProvider`). **Decision:** these use the aliased group and its real table names, the view as it is in the group being deployed, independent of the schema's symbolic names. They are never remapped.

`ALLOW UPDATE REF` (`canObserve`) is a third case. It is evaluated in the observed group's own frame (`evaluateObserveGateIn` in `rtable_group/group.ts`): its unqualified table names are the observed group's tables, as in `rdb/examples/editor.sql`, where `ALLOW UPDATE REF user IF EXISTS caps` names `user`'s `caps` table. The observing group's `tableMaps` never applies to it. The plan's line saying remapping applies to observation gates must be removed.

Because of this split, the remap cannot live inside `resolveForeignTableViewIn`, which serves both kinds: `canDeploy` is evaluated through it. If it did, `target.subject` and `target.posts` would both resolve to `blog.posts` in a catalog-written predicate, and a swapping map (`subject => posts, posts => photos`) would change what the catalog author wrote.

### 3.3 Implementation

- Keep `resolveForeignTableViewIn(evaluation, alias, table, ...)` resolving real table names, unchanged. `canDeploy` evaluation and `resolveAuthorKey` (for a qualified `idProvider`) keep calling it, without remapping. The plan's change to `resolveAuthorKey` is dropped.
- Add a schema-reference entry point, for example `resolveSchemaForeignTableViewIn(evaluation, alias, schemaTable, ...)`, which applies `tableMaps[alias][schemaTable] ?? schemaTable` and then calls the real-name resolver. Use it wherever a reference comes from the schema:
  - row restriction evaluation (`getForeignTableView` in `evaluateRowOpRestriction` and the void-time checks);
  - FK target checks (`fkTargetsLive` in `rtable_group/validate_ops.ts`, for single row ops and bundles);
  - FK adoption on deploy (`adoptedTargetLive` in `fkAdoptionKills`, `group.ts`). The deploy's deleted-row meta is protocol, but fixed maps don't change what existing groups compute;
  - FK row-id resolution in `rdb_runtime/src/session/adapter.ts` (`resolveFkRowId`);
  - the projection's cross-group resolver (`resolverFor` in `rdb_projection/src/scope.ts`).
- Static checks follow the same split: `USING IDENTITIES alias.t` checks that `t` is a real table of the bound group's schema and an `IDENTITY PROVIDER`; FK and `EXISTS` targets are checked after remapping.

---

## 4. Several Aliases Bound to One Group

The plan lifts the rule that bindings are injective, so `source` and `target` in `hhs:follows` can both bind `blog`. The core change is sound. An observation is keyed by the foreign group id, so both aliases always see the same observed version. The plan's combined gate (all `canObserve` predicates of the aliases bound to the id, joined with `and`) is the right rule.

The plan only names `observeGateFor`. Other code assumes one alias per id:

- `bindingNameForId` in `rtable_group/group.ts` (a memoized inverse map), used for void reasons and error labels;
- `bindingNameFor` in `rtable_group/validate_ops.ts` and in `rtable_group/op_delta.ts`;
- code that loops over aliases and observes each one, which would append duplicate observation barriers for the same group id:
  - `applyCatalogPlan` (`rdb/src/rdb/catalog_update.ts`);
  - `buildObservedBy` (`rdb_adapter/src/ref_advance.ts`);
  - `rdb_runtime/src/session/ref_auto_update.ts`.

Labels should list every alias bound to the id, and the observing loops should deduplicate by group id.

---

## 5. Static Checks and Fail-Closed Behavior

### 5.1 Cross-Group Checks Must Also Run on Changes

The plan's overview says cross-group checks run when a catalog adds *or updates* a group; its section 2.4 says only when one is added. Today, when a release changes a group's version, `validateChange` (`rcatalog/validate_ops.ts`) runs only `groupIdentityReason`. It doesn't even run `groupViewReason`, so a schema update can introduce `REFERENCES newalias.x` for an alias the group never binds, and the catalog accepts it.

`validateChange` should run `groupViewReason` and the new cross-group checks on the target version. That needs the versions of the bound groups in the same release, which `validateFilesBody` already computes the same way (folded versions plus this release's adds and changes).

Schema changes in a *bound* group are still not blocked (concurrent releases deployed together could bypass a single-release check anyway). `rpack build` warns when a release breaks a dependent group's references.

### 5.2 A Foreign `EXISTS` on a Missing Column Throws

The plan says missing foreign tables and columns fail closed. Tables do: an unbound alias or a missing table makes the `EXISTS` atom false. Columns don't. A foreign `EXISTS` whose `WHERE` field is missing or not `PUB` reaches `candidateRowIds` in `rdb/src/rtable/view.ts`, which throws (`'<field>' is not a pub column of table '<table>'`). During validation, a throw is treated as an infrastructure error, so the op is deferred instead of rejected, and reads that hit it fail.

Remapping onto arbitrary tables and bound schemas that drop or unpublish columns make this reachable. `existsTruth` (`rtable_group/predicates.ts`) should return false when the foreign table at the observed version lacks a searched `PUB` column. This changes validation outcomes only where they currently throw, and can ship before the rest of the plan.

---

## 6. Schema Packs and `IMPORT SCHEMA`

### 6.1 Replaying the Pack's DAG

A schema pack's history is a DAG of steps, and two concurrent steps can write the same slot (say both set the restrictions of `comments`). Inside the pack, that conflict is resolved by RSchema's rule: the higher schema version wins. The plan replays the collected steps one after another, in topological order, onto the catalog's `RSchema`. That makes them causally ordered there, so the step replayed last wins instead, and the catalog's effective schema at `<v>` can differ from the pack's.

**Decision:** replay preserving the pack's DAG. Mechanism:

- **Mirror the pack's history from its genesis.** A catalog that first imports a schema at `1.2.0` still creates the `RSchema` at the pack's genesis step and replays every step up to `1.2.0`, instead of creating it directly at `1.2.0` with flattened tables (as the plan proposes). Otherwise a later pack step whose parents include a step below `1.2.0` (a branch from `1.1.0` merged at `1.3.0`) has nothing to attach to. The cost is a few extra schema entries.
- **Map pack steps to catalog entries.** `rpack` keeps the mapping from each pack step hash to the catalog `RSchema` entry that mirrors it, recoverable from the released catalog by matching version and rules. Each step is appended at the entries mirroring its parents. `RSchema.updateSchema` already accepts an `at` position (`produceRelease` uses it with the base pin), so no new API is needed.
- **Require unique versions within a pack.** `IMPORT SCHEMA ... VERSION` addresses steps by version, and with unique versions RSchema's resolution never falls back to the entry hash between steps. Catalog entries have different hashes from pack steps (different signers), so a hash tiebreak could differ between the pack and the catalog. With unique versions it never applies, and the mirrored DAG resolves exactly like the pack.
- **Handle steps mirrored twice.** Two catalog branches can each mirror the same pack step independently. If the same key signs with deterministic signatures, the entries coincide. Otherwise the catalog has two entries with the same version and the same rules, which resolve identically but make the step → entry mapping one-to-many. The mapping must allow that, and `rpack` should reuse an existing mirror when one is in the base.
- **Check content, not just versions.** Before replaying, `rpack` checks that the catalog's schema at the mirrored base equals the pack's state there (tables and params). After replaying, it checks the result equals the pack's state at `<v>` (`tableDifferences` empty, params equal). Any difference is a refusal. The plan's current check compares version strings only.

The plan justifies hop-by-hop replay with keeping `dropGeneration` incarnation counts identical to a catalog that upgraded release by release. That reason doesn't hold: incarnation ids belong to each `RSchema` object, and each catalog has its own. The real reason is preserving the pack's semantics: resets (drop then re-add) and the pack's resolution of concurrent steps.

`rpack verify` on a schema pack must replay the same way (preserving the DAG) in a scratch `RSchema`, so a pack is checked under the resolution it will get when imported.

### 6.2 Pinning Imports

Schema packs are unsigned, `rpack import` picks the highest version found in a shared registry directory, and `readSource` fetches a missing version from the registry silently. Anyone who can write to the registry can publish a different `hhs:comments 1.1.0`.

- Record the release step hash of each import in a lock, for example `rpack.json`'s `imports: { "hhs:comments": { "1.1.0": "<step hash>" } }`, and verify it every time `imports/` is read.
- Fetch from the registry only in `rpack import`, never as a side effect of `build` or `release`.
- Optionally, let packs carry a publisher signature.

### 6.3 File Format and Naming

- The `.rpack` reader (`rpack/src/format.ts`) strictly expects `format`, `manifest` and `objects`, and a manifest with a `catalog` field. Schema packs (with `steps`) need a reader that branches on `manifest.kind`. Existing catalog `.rpack` files have no `kind`, so a missing `kind` means `catalog`.
- Replacing `:` with `_` in file names is ambiguous (`a:b_c` and `a_b:c` both give `a_b_c`). In the flat `imports/` folder, packs must be identified by their manifest, not their file name.

### 6.4 `rhost update --pull`

"A higher version with the same catalog genesis" can select a release on a concurrent branch, one that doesn't descend from the installed release. The pull should prefer descendants of the installed release, or leave the choice to the app's adoption range.

---

## 7. Compatibility With Older Replicas

`json.checkFormat` is strict by default, and the formats the plan extends are checked strictly: `createRSchemaFormat`, `schemaUpdateFormat`, `createTableGroupFormat`, `catalogGroupDefFormat`, and `catalogGroupChangeFormat`. An older replica rejects any entry that carries `params` or `tableMaps`, so in a mesh with mixed versions, replicas diverge. The plan needs a policy: bump the type ids (`hhs/rschema_v2`, ...), or gate writing the new fields on a minimum version known to all replicas. Instantiation is normative and locked by golden vectors, so `tableMaps` and `params` in group payloads need new vectors.

---

## 8. Corrections to the Plan's Text

- **`related_posts` example:** `USING IDENTITIES blog.identities` is invalid. `blog` is not an alias of `hhs:follows` (its aliases are `source` and `target`), and `app:blog` has no `identities` table. Add a binding and use the identity table by its real name, for example `BIND source => blog (actor => posts), target => blog (subject => posts), auth => users` with `USING IDENTITIES auth.identities`. Extra bindings are allowed.
- **Remapping scope:** "Remapping applies ... observation gates (`ALLOW UPDATE REF`), and `USING IDENTITIES alias.<t>`" becomes "remapping applies to FKs and `EXISTS` atoms in the schema's rules; `ALLOW DEPLOY IF` and `USING IDENTITIES` use real names; `ALLOW UPDATE REF` is evaluated in the observed group's frame" (section 3.2).
- **`resolveAuthorKey`:** drop the step that remaps the `idProvider` table.
- **Write-once params:** replace with revisable params ranked by schema version, then target schema entry hash (sections 2.3 and 2.4), at catalog, RDb and group level. Drop "the merge release's `changes[hash].params` must set it".
- **When cross-group checks run:** on adds and on changes (section 5.1).
- **Fail-closed claim:** true for missing tables, false for missing or non-`PUB` columns until `existsTruth` is fixed (section 5.2).
- **Import replay:** replace the topological, sequential replay and the flattened first import with the DAG-preserving replay of section 6.1, and drop the `dropGeneration` justification.
- **`tableMaps` entry check:** allow entries the schema doesn't reference yet (section 3.1).

---

## 9. Suggested Order of Work

1. **Independent fixes:** `existsTruth` returns false on a missing or non-`PUB` foreign column; `validateChange` runs `groupViewReason` and the cross-group checks.
2. **Several aliases per group:** the combined observe gate, labels, and observing once per group id.
3. **`tableMaps`:** fixed at creation, with the two resolution entry points of section 3.3.
4. **Schema params:** the slot model and its ranking rule (schema version, then schema entry hash) at all three levels, changes that only set params, the planner's catch-up, the rule-only operand type, and member status.
5. **Schema packs:** DAG-preserving replay with content checks and pinned imports, then the registry and the `rpack` / `rhost` commands.

The compatibility policy of section 7 should be decided before step 2 writes any new payload field.

## 10. Open Questions

- Should params-only fixes reuse the deploy (repeating the current version) or get a dedicated `set-params` op (section 2.5)?
- Should schemas that declare params be refused in groups whose deploys are open (section 2.7)?
- Should `WITH PARAMS` accept `publicKey(:p)` (section 2.2)?
