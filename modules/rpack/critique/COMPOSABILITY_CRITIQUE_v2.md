# Critique of the Composability Plan, v2

A review of the revised [COMPOSABILITY.md](./COMPOSABILITY.md) (table remapping, multi-alias bindings, schema `PARAMS`, schema packs and the `.rpack` registry), and the decisions taken after it. It follows [COMPOSABILITY_CRITIQUE.md](./COMPOSABILITY_CRITIQUE.md), and replaces it where they differ: most importantly, schema param values no longer forward catalog params, and the observer-frame `ALLOW UPDATE REF` is deferred to v2. Each section explains its issue from scratch, so it can be read without either document at hand.

## Summary of Decisions

- **Observer-frame `ALLOW UPDATE REF` is deferred to v2.** In v1, observation gates keep today's frame: they are evaluated in the observed group. The plan's section 2.3 and every step that depends on it are dropped.
- **Every catalog-authored reference uses real table names.** `ALLOW DEPLOY IF`, `USING IDENTITIES` and `ALLOW UPDATE REF` name tables as they exist in the group that holds them. Their aliases follow the bindings of the group that evaluates them, but no group's `tableMaps` is applied, at any step (section 3.2). `tableMaps` applies only to references that come from the schema. In v2, when gates move to the observer's frame, they will resolve through the observer's `tableMaps`.
- **Schema param values are literals.** A `WITH PARAMS` value can't forward a catalog param. Catalog params stay what they are today, per-database values for `WITH ROWS`. Reusing one value across groups is done with constants, declared by `CONSTANT` statements that C-SQL resolves at compile time. Constants never reach the catalog's DAG.
- **Schema params stay real.** They are declared in the `RSchema`, so one schema object serves every group that uses it, whatever its values.
- **A group is created with its definition's values.** Values written by later releases reach it by deploy, and the planner catches up a new group the same way it catches up its schema version.
- **Param validation:** a value for a param the schema doesn't declare is rejected. A missing value, or one that doesn't fit its type, makes every atom that reads it false. A param added by `ALTER SCHEMA` must declare a default. A param declared by `CREATE SCHEMA` needn't have one: each group must supply its value.
- **No params-only changes.** Values change only together with a schema version move: a deploy that carries `params` must move the group's version up, and a catalog change carries `params` only when it moves the version. Fixing a botched value means releasing a schema version that changes something (for an imported schema, a new pack release) and moving the group to it with the right values. No `set-params` op, and no deploy that repeats the current version with new values.
- **Open deploys are accepted.** An adversarial setting has no open deploys. The plan documents the exposure.
- **Compatibility with older replicas is not a concern.** The software is unreleased. Type ids are not bumped. The golden vectors are updated.
- **Every schema step changes something.** A schema update needs migration rules or new param declarations. A pack merge with neither is refused.
- **`rpack` internals** that the plan doesn't name are listed in section 6.3.

---

## 1. Background

The rest of this document relies on these facts about the current model.

- An **`RSchema`** is a DAG of schema entries (`create`, then `schema-update`s), each carrying a strict semver `version` and a list of migration rules. The effective schema at a position is resolved per slot (table existence, columns, FKs, restrictions) by last-writer-wins: the causally latest write wins, and among concurrent writes the higher schema version wins, then the larger entry hash.
- An **`RTableGroup`** is the unit of data: one DAG holding the rows of all its tables. Its create payload pins an `RSchema` at a version and fixes, for its whole life, `bindings` (alias → foreign group id), `canDeploy`, `canObserve`, `idProvider`, `deployKeys` and `initialRows`. The group id is the hash of this payload.
- A group moves to a new schema version through a **deploy**, a barrier ref-advance of its schema ref, gated by `canDeploy`. It sees a bound foreign group through an **observation**, a barrier ref-advance of that group's id, gated by `canObserve[alias]`. Both are validated when written and re-checked at read time, so a concurrent barrier can void an op after the fact.
- An **`RCatalog`** is a DAG of developer-signed releases. Each release adds group definitions (`CatalogGroupDef`) and changes existing ones (`changes[defHash] = { schema, version }`).
- An **`RDb`** is one deployment of a catalog. Its DAG records which releases its admin deployed and the values of the catalog's params (`ParamValue`), such as `:admin`. The same catalog release is deployed to many databases, each with its own values: `rhost` gives each host `:admin = $me`. Member group ids are computed by the normative instantiation (`rdb/src/rdb/instantiate.ts`) from the definition, the RDb id, and the param values used in `WITH ROWS`.
- The invariant that shapes the param work (`rdb/src/rdb/payload.ts`):

  > RDb state never decides any group's validity: groups are fully valid and verifiable without their RDb, and never depend on it.

---

## 2. What the Revised Plan Gets Right

The revision took up almost everything in the first critique: table maps fixed at creation, with unused entries allowed; two foreign-table entry points (schema references remapped, catalog references not); several aliases bound to one group, with combined gates, labels, and deduplicated observation loops; the rule-only operand type; revisable params ranked by schema version, then schema entry hashes; status reporting; cross-group checks on changes; the fail-closed `EXISTS`; DAG-preserving import replay with content checks; pinned imports; and `rhost update --pull` limited to descendants. Those parts are ready to implement as written, with the edits below.

---

## 3. `ALLOW UPDATE REF`: Observed Frame, Real Names

### 3.1 Why the Observer Frame Waits

The plan's section 2.3 would evaluate a gate on `target` in the *observer's* binding namespace, so a group that binds `target => blog` and `auth => users` could gate observations of `blog` with `auth.caps`. It is deferred because:

- **It changes a proof.** `VOID_SEMANTICS.md` §5.5 proves an observation's verdict is never undecided because it "reads only G", the observed group. Under the observer frame, a gate on `target` reads `users` at the observer's observed version, filtered by liveness, so one observation's verdict depends on the verdicts of the observer's other observations. The plan's ordering argument (gate rank, then observed version) is plausible, but §5.5's two attack analyses (a back-dated observation by a revoked principal, a back-dated import used to void others) would have to be redone for a revoke that arrives through another group.
- **It needs ordering.** A gate on `target` passes only once the observer has observed `users` past the grant, so `applyCatalogPlan` and `ref_auto_update` would have to observe in gate order. `validateObserveGate` would also need the op's own position, which it doesn't take today.
- **v1 doesn't need it.** In the observed frame, a qualified name resolves through the *observed* group's own bindings, at its observed version. In the plan's example `blog` binds `users`, so this gate already works:

  ```sql
  ALLOW UPDATE REF target IF EXISTS users.caps WHERE label = 'manager' AND grantee = $author
  ```

  The observer frame only adds something when the data group doesn't bind the group holding the capabilities.

### 3.2 The v1 Naming Rule

A table reference has two parts, and they resolve differently:

- **The alias** (`auth` in `auth.caps`) is looked up in a group's `bindings`, which leads to another group. Following bindings is how any reference reaches a foreign group.
- **The table name** (`caps`) is either used as is, or first translated through that same group's `tableMaps`. `tableMaps` never leads to another group. It only changes which table of the bound group a name means.

References also come from two authors:

- **Schema references** are `REFERENCES alias.t` FKs and `EXISTS alias.t` atoms in the schema's `ALLOW` rules. The schema author wrote them against symbolic names (`target.subject`), not knowing which groups the schema will be bound to. Their table names go through the group's `tableMaps`.
- **Catalog references** are `ALLOW DEPLOY IF`, `USING IDENTITIES`, and `ALLOW UPDATE REF`. The catalog author writes them on a `TABLEGROUP`, knowing exactly which groups are bound. Their table names are the real names of the tables in the group that holds them, and they never go through any group's `tableMaps`.

So catalog references do follow bindings, sometimes through an intermediate group, but no `tableMaps` is ever applied to them, at any step. What varies between the three is which group's bindings the alias is looked up in. That is the group whose frame evaluates the predicate:

| Reference, declared on group `G` | Evaluated in | Unqualified `t` | Qualified `a.t` |
| --- | --- | --- | --- |
| `ALLOW DEPLOY IF` | `G` | `G`'s table `t` | `a` in `G`'s bindings, table `t` of that group |
| `USING IDENTITIES` | `G` | `G`'s table `t` | `a` in `G`'s bindings, table `t` of that group |
| `ALLOW UPDATE REF b`, with `b` bound to `H` | `H` | `H`'s table `t` | `a` in `H`'s bindings, table `t` of that group |

In `rtable_group/group.ts`, `canDeploy` and the qualified `idProvider` resolve through `G`'s `resolveForeignTableView` and `resolveAuthorKey`, using `G.getBindings()`. A gate is evaluated by `evaluateObserveGateIn`. Its unqualified tables are `H.makeTable(t)`, and its qualified ones are `H.resolveForeignTableViewIn(...)`, which uses `H.getBindings()` at the observed version. Today neither path applies a map, and with this rule neither will.

**A qualified gate goes through the observed group.** A gate on `b => H` that names `a.t` reads group `K = H.bindings[a]`, at the version of `K` that `H` observes, at the version of `H` being imported. So it reaches one group beyond the one it gates. The alias `a` is `H`'s alias, as written in `H`'s `TABLEGROUP ... BIND`. It is not one of `G`'s aliases. The table `t` is `K`'s real table.

**Why `H`'s `tableMaps` doesn't apply either.** `H`'s maps translate `H`'s *schema's* symbolic names into the tables of `H`'s bound groups. A gate written in the catalog isn't part of `H`'s schema. Applying `H`'s maps would make the gate's meaning depend on how a different group was instantiated, and it would add nothing: the catalog author can always write the real name. The rule stays uniform: a catalog reference names the table as it exists in the group that holds it.

Here is the plan's example under this rule:

```sql
TABLEGROUP blog USING SCHEMA app:blog
  BIND users => users
  USING IDENTITIES users.identities,

TABLEGROUP post_comments USING SCHEMA hhs:comments
  BIND target => blog (subject => posts),
       auth   => users
  USING IDENTITIES auth.identities
  ALLOW DEPLOY IF EXISTS auth.caps WHERE label = 'manager' AND grantee = $author
  ALLOW UPDATE REF target IF EXISTS users.caps WHERE label = 'manager' AND grantee = $author
```

- `USING IDENTITIES auth.identities` and `ALLOW DEPLOY IF EXISTS auth.caps` are evaluated in `post_comments`. `auth` is `post_comments`' alias, and leads to group `users`. `identities` and `caps` are `users`' real tables.
- `ALLOW UPDATE REF target` is evaluated in `blog`. `users.caps` is `blog`'s alias `users`, then `users`' real table `caps`, at the version of `users` that `blog` observes. The gate can't say `auth.caps`, because `blog` has no `auth` alias.
- A gate on `target` that reads posts directly writes `IF EXISTS posts WHERE ...`. `subject` means nothing here: it's a name in `hhs:comments`, and `post_comments`' map for it applies only to that schema's references.
- If `blog` had table maps of its own, they wouldn't change any of this. They only affect `app:blog`'s FKs and `EXISTS` atoms.

The static checks (section 5) and the compiler (section 3.4) follow the same frames. `ALLOW DEPLOY IF` and `USING IDENTITIES` are checked against the schemas of `G`'s bound groups. A gate is checked against `H`'s schema for unqualified names, and against the schemas of `H`'s bound groups, through `H`'s bindings, for qualified ones. In every case, the names checked are real names.

In v2, if gates move to the observer's frame, a gate would be read as the observer sees its bound groups, through `G`'s bindings and `G`'s `tableMaps`. That is the time to change the rule, not before.

### 3.3 Edits to the Plan

- Drop section 2.3 (observer-frame `canObserve`), the overview bullet that announces it, and the second half of step 2 in the order of work.
- Section 1: "Remapping applies to schema-originated references ... and `ALLOW UPDATE REF` gates" becomes "remapping applies to FKs and `EXISTS` atoms in the schema's rules; `ALLOW DEPLOY IF`, `USING IDENTITIES` and `ALLOW UPDATE REF` use real names".
- Section 2.1: `resolveSchemaForeignTableViewIn` is used where a reference comes from the schema, not "or `canObserve`". Gates keep calling the real-name resolver.
- Section 2.5: the cross-group checks resolve `canObserve` atoms by real name, against the observed group's schema at its version in the release.
- Section 3.2: drop "compiles `ALLOW UPDATE REF` in the observer's binding namespace".
- The example's gate on `post_comments` uses `blog`'s binding: `IF EXISTS users.caps WHERE ...` (section 3.1).

### 3.4 A Compiler Fix That Belongs in v1

`compileCatalogGroup` (`rdb_lang/src/compile/catalog.ts`) lowers `ALLOW UPDATE REF` with the *observing* group's schema view as its scope:

```ts
const gateScope: RuleScope = { columnsOf: columnsOfFromSchemaView(view) };   // the observing group's schema
...
canObserve[clause.binding] = lowerRestrictionPredicate(clause.predicate, gateScope);
```

The engine evaluates the gate against the observed group's tables, so the compiler checks the gate's columns against the wrong schema. The gate should be lowered in the observed group's frame (section 3.2). Unqualified tables are checked against the bound group's schema at its version in the catalog being compiled. Qualified ones are checked against the schemas of that group's own bound groups, looked up through its bindings.

---

## 4. Schema Params

### 4.1 Two Kinds of Params

The design has been mixing two different things:

- **Instance params** are catalog params as they exist today: `CREATE CATALOG blog_app PARAMS (:admin identity)`. Their values are chosen per database (`rhost init --param admin=$me`), long after the catalog was released, and are stored in the RDb's DAG. Only `WITH ROWS` reads them, to seed initial rows such as the admin's identity and capabilities.
- **Schema params** are values that a reusable schema leaves open for each group: `:write_cap`, `:mod_cap`, `:max_len`. They belong to the app: `post_comments` and `photo_comments` differ in `:max_len` in every database the app is deployed to.

The first plan let schema param values forward instance params (`:max_len = :default_max_len`), which made a group's rules depend on per-database values. The decision now is that they don't. Schema param values in the catalog are literals.

**Does making catalog params syntactic sugar miss something?** Yes, if it means all catalog params. Instance params can't be sugar: a value chosen per database can't be compiled into a release shared by every database. They stay as they are and keep serving `WITH ROWS`. What can be sugar is a third, new thing, **constants**: names for app-level values that C-SQL substitutes while compiling, so the catalog's DAG only ever holds literals.

What is given up: a schema rule can't read a per-database value through a param. That is the right limit, not a loss. Per-database authority already travels through rows, with the capability pattern the examples use: `WITH ROWS` seeds `caps (label = 'manager', grantee = :admin)`, and the schema's rule checks `EXISTS auth.caps WHERE label = :mod_cap AND grantee = $author`. The label is a schema param, the holder is a row.

What is gained:

- **No RDb in the value path.** A group's values come from its catalog's literals only. Group ids no longer depend on RDb params through schema params (they still do through `WITH ROWS`, as today).
- **Full static checking.** Catalog validation and `rpack` see the concrete values, so they can check types and coverage, not just declarations.
- **Simpler types.** `CatalogGroupParamValue` (`{ value } | { param; fn? }`) becomes `json.Literal`. The forwarding typing rules and the `publicKey(:p)` question disappear.
- **No surprises after a merge.** An RDb param resolved differently after a merge can't move a schema param value, or a group id, any more.

### 4.2 Constants

A constant is a name for a literal. It is a statement of its own, not part of `CREATE CATALOG`: it belongs to the script, not to any object, because nothing about it is stored.

```sql
CONSTANT :default_max_len = 1000;
CONSTANT :commenter = 'commenter';

IMPORT SCHEMA hhs:comments VERSION '1.1.0';

CREATE CATALOG blog_app PARAMS (:admin identity) AS (
  TABLEGROUP post_comments USING SCHEMA hhs:comments
    ...
    WITH PARAMS (:write_cap = :commenter, :mod_cap = 'manager', :max_len = :default_max_len),
  TABLEGROUP photo_comments USING SCHEMA hhs:comments
    ...
    WITH PARAMS (:write_cap = :commenter, :mod_cap = 'manager', :max_len = 500),
  ...
);
```

- **Syntax:** `CONSTANT :name = <literal>;`, a top-level statement. The value is a literal of any type C-SQL accepts (string, number, boolean), not an expression and not another constant.
- **Scope:** the script it's declared in, from its declaration on. A constant can be used anywhere the script accepts a literal: `WITH PARAMS`, `WITH ROWS`, and the literals of `ALLOW` rules. Declaring the same name twice is an error.
- **Compilation:** in `rdb_lang`, the parser accepts the statement in any script, and the binder replaces every use with its literal. No payload ever holds a constant: `CatalogGroupDef.params`, `CatalogGroupChange.params`, rows and rules hold the literal.
- **Names:** constants, catalog params and schema params all use `:name`, and their meaning comes from position. In `WITH PARAMS (:max_len = :default_max_len)`, the left side is a schema param of the group's schema and the right side is a value. A constant can't share its name with a param of the catalog, or with a param of a schema used in the same script, so the right-hand side of a `WITH PARAMS` assignment is never ambiguous. A catalog param on the right side is a compile error that explains why ("`:admin` is a per-database param; schema params take literals or constants").
- **In `rpack`:** `target-catalog.sql` can declare constants before `CREATE CATALOG`, alongside the `IMPORT SCHEMA` statements. `rpack` diffs the compiled literals, so changing a constant changes every group that uses it, and `diffCatalog` emits a change for each. The generated `build/update.sql` and the reverse rendering of a release show literals. `writeSource` must keep the source's `CONSTANT` statements and constant references when it edits `target-catalog.sql` in place.
- **Not required for v1.** Literals alone are enough. Constants can follow once params work.

### 4.3 Why Schema Params Stay Real

Params could be sugar at the schema level too: C-SQL would substitute the values into the schema's rules, and each value set would get its own `RSchema` object. That is worse:

- One `RSchema` per value set: `post_comments` and `photo_comments` would need two copies of `hhs:comments`, each mirroring the whole pack.
- Changing a value would be a schema update on that copy, rewriting the rules that use it. That entry is in no pack, so the next pack step competes with it under last-writer-wins, and the import's content checks (catalog schema state equals pack state) no longer hold.
- Two groups using "the same schema" would no longer share it in any observable way.

With real params, the schema is one object per pack per catalog, the mirror checks stay exact, and a value changes without touching the schema. Hash stability across value sets is a consequence of this, not the goal.

### 4.4 Creating a Group, and Catching Up Values

A group definition is hashed into the catalog, so `def.params` never changes. Values that later releases write live in the catalog's per-group params slot (`changes[hash].params`, folded by `CatalogGroupState`). Creation and catch-up then mirror what already happens with the schema version:

- **Creation:** `instantiateGroup` copies `def.params` into the create payload unchanged. It never uses the folded values, so the group id is the same on every replica, whatever release created it.
- **Catch-up:** today, `planCatalogUpdate` creates a missing group at `def.schemaVersion` and, if the member's target version differs, plans a deploy to it (`rdb/src/rdb/catalog_update.ts`, after `plan.creates.push`). Params ride on that deploy: if the member's target params differ from the group's, the deploy to the target version carries the difference. For an existing group, the planner deploys only when its version is below the target, again carrying the differing values. Values never change without a version move (section 4.8), so a group still at `def.schemaVersion` has `def.params` as its target values.

The plan must say this in section 2.4. It currently says only that the planner "substitutes concrete values", which leaves room for the folded values to reach the create payload.

### 4.5 A Member's Target Params

An RDb can have several maximal deployed releases. `MemberGroup.target` is already the union of the group's versions across them. The plan uses `member.targetParams` without defining it. Define it the same way: the values that a merge release of those deployed releases would inherit under the catalog's ranking rule (section 4.7). With literal values, no RDb resolution is involved.

A slot can't be unset. To return to a schema default, a release writes the default's value. The planner writes only the params for which the target has an explicit value that differs from the group's resolved value.

### 4.6 Validation

**Declarations (`RSchema`).** `SchemaParamDecl` gains a default:

```ts
export type SchemaParamDecl = { name: string; type: ColumnType; default?: json.Literal };
```

- The default must fit the declared type and, for strings, must not start with `$` (a `$`-prefixed string is a reserved term in `where` values and `lit` operands).
- A param declared by `CREATE SCHEMA` needn't have a default. The group must supply its value: every group of the schema is created after the param exists, so its definition can set it. A param declared by `ALTER SCHEMA` must have a default, because groups created before it already exist and deploy the new version without necessarily setting it.
- Concurrent branches that declare the same name converge only when type and default are equal. Otherwise the higher schema version wins, then the larger entry hash, as for types today.

**Values in the group (create and deploy payloads).**

- A value for a name that the target schema version doesn't declare is rejected at validation. The target is `schemaVersion` for a create and the deploy's target version for a deploy.
- A string value starting with `$` is rejected by format validation.
- Otherwise the group accepts the value. At read time, a param's effective value is the group's resolved write, else the schema's default. If neither exists, or the value doesn't fit the type that wins in the effective schema, every atom that reads the param is false.

**Values in the catalog.** The catalog only writes literals, so `RCatalog` validation and `rpack` can be stricter than the group, and should be. On a definition or change, they reject values whose type doesn't fit the target version's declaration, and reject a group that leaves a param with no default unset (in practice, a `CREATE SCHEMA` param that the group's `WITH PARAMS` doesn't set). The group's leniency covers what the catalog can't prevent: concurrent schema branches, manual deploys, and later type conflicts.

**What a change must do.** `validateChange` (`rcatalog/validate_ops.ts`) refuses a change that doesn't move the version, and that rule stays: `change.params` is only allowed on a change that moves the version. A change that rewrites values at the version the group already has is refused, so the plan's `ALTER CATALOG` example must move the group to a higher schema version (one that changes something) to set `:max_len`.

### 4.7 Ranking Concurrent Writes

This is unchanged from the first critique, and the plan states it correctly. The causally latest write to a param wins. Among concurrent writes, rank by:

1. the highest schema version among the write's target schema entries;
2. the target schema entry hashes, as a sorted set;
3. a level-local hash: the deploy entry hash in the group, the release hash in the catalog.

The rule ranks *writes*, not inherited states, and catalog release versions play no part. With literals, the only case where catalog and group can disagree is still a full tie: two concurrent writes of different values that target the same schema entries. With no params-only deploys, the planner can't settle it at the same version. `catalogStatus` reports the difference, and the next release that moves the group's version settles it. If the result is not the wanted one, the fix is to release again.

### 4.8 Values Change Only With a Version Move

Every param write rides on a deploy that moves the group's schema version up, and that version must change something in the schema. There are no params-only deploys.

- **Group validation.** `validateRefAdvanceMonotonicity` (`mvt/src/refs.ts`) accepts a deploy to the current version. That stays valid for a deploy without `params`. A deploy whose `params` is non-empty must target a version at or above the group's current one *and different from it*, which is a new check in `validateDeploy`.
- **Catalog and `rpack`.** `validateChange` keeps requiring a version move, and `change.params` rides on it. `diffCatalog` refuses a `WITH PARAMS` edit on a group whose schema pin doesn't move, with a hint to move the group to a schema version that changes something.
- **Cost.** A value fix waits for a schema version. For an inline schema, the catalog releases one. For an imported schema, the catalog can't add versions without breaking the mirror, so the fix waits for a pack release.

A deploy is a barrier, so a row op concurrent with a deploy that changed a value is re-checked against the new value once both are visible, and may be voided, as with a concurrent deploy that tightens a rule. This should get tests in the style of `VOID_SEMANTICS.md`.

### 4.9 The Group's Per-Param Fold

The group resolves a param's value at `(at, from)` from the writes in its DAG. It can't reuse the walk that resolves the schema version: `resolveSchemaVersion` only reads the most recent deploys (`findRefAdvances`, a cover of the schema ref's ref-advances), but a param's latest write can sit on an older deploy. A deploy to `1.3.0` that doesn't touch `:max_len` leaves the value that a deploy to `1.2.0` wrote.

- A deploy that carries `params` tags its meta with the names it writes (`param: [name, ...]`, next to the ref-advance's `ref` and `barrier` values). Validation checks the tag matches `params`.
- For each param `p`, the candidates are the cover of deploys writing `p` at `at` (`findCoverWithFilter(at, { containsValues: { ref: [schemaRef], param: [p] } })`), plus the deploy barriers writing `p` that are concurrent to `at` and visible from `from` (`findConcurrentCoverWithFilter`, as for the schema version's widening). With no candidate, the create payload's value is the base.
- Candidates causally below another candidate are dropped, and the rest are ranked by the rule of section 4.7.

`RTableGroup.deploy` and `prepareDeploy` take only `(refVersion, author?, at?)` today, and need a `params` argument.

### 4.9 Open Deploys

A group with no `ALLOW DEPLOY IF` and no RDb creators has open deploys (`instantiate.ts`). Anyone can deploy it, and so anyone can write its params, as they can already move its schema. This is accepted: an adversarial setting has no open deploys. The plan should say it in one line next to the params section.

---

## 5. Table Maps and Static Checks

The revised sections 2.1, 2.2 and 2.5 hold, with the naming edits of section 3.3. Two additions:

- **Params in the checks.** With literal values, `groupViewReason` (run in both `validateGroupDef` and `validateChange`) also checks the group's values against the target version's declarations (section 4.6).
- **Golden vectors.** Instantiation is normative and locked by golden vectors. `tableMaps` and `params` in group payloads need new vectors. No type ids change (compatibility is not a concern before release).

---

## 6. Schema Packs

### 6.1 Every Step Changes Something

`RSchema` rejects an empty schema update (`rschema/validate.ts`: `schema-update migration is empty`). The plan relaxes this only to allow an update whose `params` is non-empty, and that is the whole rule: a schema update needs migration rules or new param declarations. In a schema repository, `rpack release` refuses a version that changes neither, as it already does for catalogs ("a version needs a change"). A pack merge must carry a change, so every pack step has content and every mirrored step is a valid `RSchema` entry.

### 6.2 Switching Between Inline and Imported

The plan doesn't say what happens when a catalog moves a schema from `CREATE SCHEMA` in its source to `IMPORT SCHEMA`, or back.

- **Imported → inline** needs nothing new. The inline path diffs the base schema's state against the source, and the base can be any `RSchema` state. The schema stops being a mirror, and `rpack` stops treating it as one.
- **Inline → imported** works by diffing toward the pack instead of replaying it. `rpack` diffs the base schema's state against the pack's state at the imported version and emits one `ALTER SCHEMA` step (with `upgrade-manual.sql` for what the diff can't infer, as for inline schemas), then runs the usual content check. That entry becomes the mirror of the imported pack step. Later imports replay pack steps whose parents are all mirrored. A step whose parent isn't (a branch from below the switch point) is refused, with a message naming the missing step.

  The schema's name must match the pack's (`IMPORT SCHEMA hhs:comments` over an inline `hhs:comments`).

Both cases depend on how `rpack` finds the mirror of a pack step in a released catalog, which the plan leaves unsaid. It should be **by version** (versions are unique within a pack), **confirmed by content**: the catalog's resolved schema at the candidate entries equals the pack's resolved state at that step. Matching by rules, as the first critique suggested, fails for a diffed entry, whose rules differ from the step's.

### 6.3 `rpack` Internals

The plan names `draft.ts`, `produce.ts`, `format_draft.ts`, `catalog_diff.ts` and `write_source.ts`, but not the models they share. What changes, by file:

- **`project.ts`:** `RpackConfig` gains `kind`, `imports` and `registry`. `parseRpackConfig` and `formatRpackConfig` handle them. `key` is optional when `kind` is `schema`.
- **`model.ts`:**
  - `SchemaModel` gains `params` (a map of `SchemaParamDecl`) and its origin: inline, or imported at a version and step hash.
  - `GroupModel` (which is `rdb_lang`'s `SourceGroup`, so the change starts there) gains `tableMaps` and `params`.
  - `groupKey` and `groupDifferences` include `tableMaps`, which is fixed at creation, so a difference is refused. They don't include `params`, which are revisable.
  - `modelDifferences` compares schema params and groups' folded params, so the fixpoint checks see them.
- **`source.ts`:** `readSource` parses `IMPORT SCHEMA`, reads packs only from `imports/` (indexed by manifest, checked against the lock), and builds the imported `SchemaModel` from the pack's state at the version. Constants are resolved by the C-SQL compiler it calls, so the model holds literals.
- **`released.ts`:**
  - `BaseSchema` gains `params` and, for imported schemas, the mapping from pack step to catalog entries (section 6.2).
  - `BaseGroup` gains `tableMaps` from its definition and the folded `params` from `CatalogGroupState`.
- **`draft.ts`:** `SchemaStep` gains `params` on `create` and `update`, and a position `at` on `update`, set per mirrored step to the entries mirroring the step's parents. `ReleaseDraft.changes` carries params.
- **`schema_diff.ts`:** diffs param declarations for inline schemas. It refuses removing or retyping a param, and refuses an added param without a default.
- **`catalog_diff.ts`:** `CatalogChange` gains `params`. `diffCatalog` emits changed values on the change that moves a group's pin, refuses value changes without a pin move, refuses `tableMaps` changes, and warns about new schema references that resolve by unmapped name.
- **`produce.ts`:** creates schemas with `params`, appends each update at its own `at` (today every update goes at the base pin), and writes `change.params`. Its fixpoint check (`modelDifferences(after.model, draft.desired)`) then covers params and table maps through `model.ts`.
- **`format_draft.ts`:** renders `PARAMS (...)` on schema steps, `WITH PARAMS (...)` on `UPDATE SCHEMA`, and one line per mirrored pack step.
- **`write_source.ts`:** edits `IMPORT SCHEMA ... VERSION`, `BIND alias => group (from => to)`, and `WITH PARAMS`, keeping constant references. Its own fixpoint check (`modelDifferences(check.model, target)`) needs the same model changes.
- **`format.ts` and `verify.ts`:** read schema packs (branching on `manifest.kind`) and verify them by DAG-preserving replay in a scratch `RSchema`.
- **`commands.ts`:** `initProject` without a key for schema repositories, `publish` and `import`. The release-time check that compares a re-read release with the base model (`modelDifferences` in `commands.ts`) is covered by the model changes.

---

## 7. Recommendation

Implement the plan, with the edits above, in this order:

1. **Independent fixes:** `existsTruth` returns false on a missing or non-`PUB` foreign column; `validateChange` runs `groupViewReason` and the cross-group checks; `ALLOW UPDATE REF` is compiled against the observed group's schema (section 3.4).
2. **Several aliases per group,** without the observer frame.
3. **`tableMaps`,** fixed at creation, applied to schema references only.
4. **Schema params:** declarations with defaults, literal values in the catalog, creation from the definition's values and catch-up by deploy, values only with version moves, the per-param fold over tagged deploys, the ranking rule, the planner's comparison, and status reporting. `CONSTANT` statements can follow.
5. **Schema packs:** DAG-preserving replay with mirrors found by version and confirmed by content, inline → imported by diff, pinned imports, the `rpack` internals of section 6.3, then the registry and the `rpack` / `rhost` commands.

The observer-frame `ALLOW UPDATE REF` is a v2 design, with its own `VOID_SEMANTICS.md` argument and tests.
