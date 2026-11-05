# Critique of the Composability Plan, v3

A final review of [COMPOSABILITY.md](./COMPOSABILITY.md) after it absorbed [COMPOSABILITY_CRITIQUE_v2.md](./COMPOSABILITY_CRITIQUE_v2.md) and the rule that param values change only with a schema version move. It lists what is still missing. Each item explains its issue from scratch.

## Verdict

The plan is ready to implement steps 1 to 4 of its order of work: the independent fixes, several aliases per group, `tableMaps`, and schema params. The params section is consistent after the last edits: values only ride on deploys that move the version, the per-param fold uses `param`-tagged deploys, and the planner, catalog validation and `rpack` agree on that.

One gap blocks step 5 (schema packs): how an imported schema exists while `rpack` reads `target-catalog.sql` (section 1). Three small items belong in the plan when their step is reached (sections 2 to 4). One item only concerns the v2 design note (section 5).

| Item | Blocks |
| --- | --- |
| 1. `IMPORT SCHEMA` in `readSource`'s scratch runtime | step 5 |
| 2. Removing a `WITH PARAMS` entry | step 5 (small) |
| 3. Rendering folded params, not `def.params` | steps 4 and 5 (small) |
| 4. `CONSTANT` rules | step 4's follow-up (small) |
| 5. Keeping v1 gate semantics in `REFERENCE_FRAME.md` | v2 only |

---

## 1. `IMPORT SCHEMA` in `readSource`'s Scratch Runtime

**How `rpack` reads its source today.** `readSource` (`rpack/src/source.ts`) doesn't just parse `target-catalog.sql`. It opens an in-memory `RdbRuntime` and *executes* the script there, then builds its model from the results:

```ts
const runtime = await RdbRuntime.openMemory({ keyVault: standIns.vault });
...
run = await runtime.execute(textForEval(text, script), {
    source: { catalogVersion, schemaVersion: schemaStampOf(script, catalogVersion) },
});
```

Each `CREATE SCHEMA` creates a real `RSchema` object in that scratch workspace. `CREATE CATALOG ... TABLEGROUP g USING SCHEMA app:blog` then resolves the name to that object, compiles the group's rules and gates against its view, and runs the catalog's own checks (`groupViewReason`, and with this plan the cross-group checks). Afterwards `readSource` reads each group's schema back from the scratch replica (`runtime.workspace.replica.getObject(def.schemaRef)`).

**What breaks with `IMPORT SCHEMA`.** An imported schema has no statement that creates it, so in the scratch runtime `USING SCHEMA hhs:comments` names nothing, and the catalog can't be compiled or checked. The plan only says that `rdb_lang` parses `IMPORT SCHEMA` and that `readSource` "constructs the imported `SchemaModel` from the pack's resolved state". A model alone isn't enough: compilation needs the object.

Three more places in `source.ts` assume every schema is inline:

- **`checkSourceForms`** refuses any statement other than `CREATE SCHEMA` and one `CREATE CATALOG` ("`<file>` holds only CREATE SCHEMA statements and one CREATE CATALOG"). It would refuse `IMPORT SCHEMA` and `CONSTANT`. It also detects a schema defined twice only among `CREATE SCHEMA` statements.
- **`schemaStampOf`** gives every inline schema the catalog's version as its source-mode version. An imported schema's versions are the pack's, not the catalog's.
- **`statedSchemaVersions`** only reads `CREATE SCHEMA ... VERSION`.

**What the plan should say (section 4.3 or 4.4):**

- Before executing the script, `readSource` creates each imported schema in the scratch workspace by replaying the pack's DAG from its genesis, the same replay `verify.ts` uses. The replay is shared code.
- The binder resolves `USING SCHEMA <name>` to that object, the same way it resolves a schema created by the script. `IMPORT SCHEMA` executes as "bring this already-created schema into scope under its name".
- A group using an imported schema is pinned at the import's version, the pack step with that version, not at the catalog-version stamp.
- `checkSourceForms` accepts `IMPORT SCHEMA` and `CONSTANT` statements before `CREATE CATALOG`. A name that is both imported and created inline, or imported twice, is an error.
- The imported `SchemaModel` is read back from the scratch object, like inline ones, so both kinds of schema reach the model the same way.

---

## 2. Removing a `WITH PARAMS` Entry

A param value is a slot that can be overwritten but never unset: once a group has a value for `:max_len`, there is no write that returns it to "no value".

If a developer deletes `:max_len = 500` from a group's `WITH PARAMS` in `target-catalog.sql`, the desired model has no value for `:max_len`, while the base (and every release built from it) still has `500`. `diffCatalog` emits nothing for it, since it only emits new or changed values. Then the fixpoint check (`modelDifferences(after.model, draft.desired)` in `produce.ts`) finds the difference and fails with a message about differing params that doesn't say why.

**What the plan should say (section 4.4, `catalog_diff.ts`):** `diffCatalog` refuses a removed `WITH PARAMS` entry, with a hint: "a param value can't be removed; to return to the schema's default, write the default's value".

---

## 3. Rendering Folded Params, Not `def.params`

The plan's section 3.3 says `renderCatalogGroupDef` and `renderSourceGroup` render `WITH PARAMS` from `def.params`. That is right for `renderCatalogGroupDef`, which renders one definition as written in a release. It is wrong for `renderSourceGroup`.

`renderSourceGroup` renders a `SourceGroup`, `rpack`'s model of a group in a catalog *state*. `rpack` uses it to write `target-catalog.sql` from a base state, for example when `rpack new` seeds a work folder (`renderSource` in `write_source.ts`). In a base state, later releases may have moved a group's values, so the values to render are the folded ones (`CatalogGroupState`'s params slot), not those of the definition. Rendering `def.params` would write stale values into the new work folder, and the next build would then try to change them back.

**What the plan should say (section 3.3):** `renderSourceGroup` renders `SourceGroup.params`, which `rpack` fills with the group's folded values. `renderCatalogGroupDef` keeps rendering `def.params`.

---

## 4. `CONSTANT` Rules

The plan's section 3.1 adds the statement (`CONSTANT :name = <literal>;`, accepted at top level), but not the rules that make it unambiguous:

- **Literal only.** The value is a literal (string, number, boolean). Not an expression, a catalog param, or another constant.
- **Scope.** The script it's declared in, from its declaration on. It can be used wherever the script accepts a literal: `WITH PARAMS`, `WITH ROWS`, and literals in `ALLOW` rules.
- **No redeclaration.** Declaring the same name twice is an error.
- **No shared names.** Constants, catalog params and schema params all use `:name`. A constant can't share its name with a param of the catalog, or with a param of any schema used in the script. Then the right-hand side of a `WITH PARAMS` assignment, and a `:name` in a rule, always mean one thing.
- **Instance params refused in `WITH PARAMS`.** The error should say why: "`:admin` is a per-database param; schema params take literals or constants". The plan already has this one, in section 3.2.

**What the plan should say (section 3.1 or 3.2):** these rules, next to the `ConstantStatement` definition.

---

## 5. Keeping v1 Gate Semantics in `REFERENCE_FRAME.md`

Moving `ALLOW UPDATE REF` to the observer's frame (the v2 design in [REFERENCE_FRAME.md](../../rdb/plans/REFERENCE_FRAME.md)) changes what the *same gate text* means:

- An unqualified `t` is the observed group's real table `t` in v1. In v2 it becomes `<binding>.t`, translated through the observer's `tableMaps`. With a map `subject => posts, posts => photos`, a v1 gate `IF EXISTS posts` would read `photos` under v2.
- A qualified `a.t` uses the observed group's alias `a` in v1, and the observer's alias in v2. The plan's own example writes `ALLOW UPDATE REF target IF EXISTS users.caps ...` on `post_comments`, through `blog`'s `users` binding. Under v2, `post_comments` has no `users` alias (its alias is `auth`), so the gate would read an unbound name and become false.

`canObserve` is part of the create payload, so a group's gates can't be rewritten after creation. By the time v2 lands, v1 groups will exist, possibly in released software.

**What `REFERENCE_FRAME.md` should add (section 3):** a way to tell the two semantics apart per group, so v1 groups keep v1 meaning. For example, a frame marker in `CreateTableGroupPayload` (`canObserveFrame: 'observer'`, absent meaning observed), or a type id bump for groups created under v2. The compiler and the static checks would branch on it as well.

---

## Recommendation

Start implementing steps 1 to 4 now. Add section 1 to the plan before step 5 begins, together with sections 2 and 3. Add section 4 when `CONSTANT` is implemented, and section 5 to `REFERENCE_FRAME.md` before v2 work starts.
