# rpack

Releases for [Rdb](../rdb) catalogs and reusable schemas. A repository holds one work folder per version, each describing the catalog (`target-catalog.sql`) or schema (`target-schema.sql`) as that version should leave it, and rpack turns each folder into a `.rpack` release file. A catalog release file carries a signed catalog release and its whole past (the catalog entries and the schema entries it pins), so any replica can install it offline and without keys. A schema release file carries an unsigned DAG of schema steps (`create` and `migration` rules) that catalogs import, customize, and sign under their own release key. [DESIGN.md](./DESIGN.md) describes the rpack and rhost tools this module is part of, [ARCHITECTURE.md](./ARCHITECTURE.md) maps them onto the modules, and [COMPOSABILITY.md](./COMPOSABILITY.md) and [IMPORT_PATCHES.md](./IMPORT_PATCHES.md) specify schema parameterization, imports, and local patches.

## The repository

A catalog repository:

```
editor/
  rpack.json      { name, key, registry?, imports?, released: { "1.0.0": "editor-1.0.0-c09e77f1", ... } }
  .gitignore      work/*/build/  work/*/stage/  work/*/.stage.*/
  releases/       the catalog release files, and nothing else
  imports/        imported schema .rpack files, locked by hash in rpack.json
  work/
    2.0.1/
      version.json         { "base": ["editor-2.0.0-8b21d0aa", "editor-1.5.1-c09e77f1"], "note": "..." }
      target-catalog.sql  upgrade-manual.sql  test-data.sql  staging.json
      .released/           the signed inputs and update.sql, as last released
      build/  stage/       generated
```

A schema repository (`rpack init --schema <name>`):

```
comments/
  rpack.json      { kind: "schema", name: "hhs:comments", registry?, released: { "1.0.0": "hhs_comments-1.0.0-4a19b20c", ... } }
  .gitignore      work/*/build/
  releases/       the schema release files, and nothing else
  work/
    1.0.0/
      version.json         { "base": [], "note": "..." }
      target-schema.sql   upgrade-manual.sql
      .released/           target-schema.sql, upgrade-manual.sql and update.sql, as last released
      build/               generated
```

A work folder is named after its version (`2.0.1`, or `2.0.1-b3c1` when two releases share a version). Its `version.json` says what the release is besides its SQL: `base`, the releases it builds on, its parents (`[]` for the first release), and an optional `note`, the release's note. Any other field is refused. rpack writes it one base entry per line and keeps the note when it changes the base. `rpack.json`'s `released` maps each released folder to its release name (where `:` in a namespaced name like `hhs:comments` becomes `_` in `hhs_comments-1.0.0-4a19b20c.rpack`). The commands find the repository by walking up from the current folder (or `-C <dir>`) to `rpack.json`, and the version folder from `work/<version>/` in the path.

## The producer

A release starts from its base, applies `upgrade-manual.sql` (explicit steps, such as a column reset), and diffs the result against `target-catalog.sql` (or `target-schema.sql` in a schema repository):

```
rpack new 2.0.1       # work/2.0.1/ on the releases below 2.0.1 no other one includes
cd work/2.0.1
rpack status          # the version, its base, and a count of what it would sign
rpack build           # write build/update.sql; print that it did, or the refusals
rpack release         # write releases/ and .released/ (signed in a catalog repo; keyless in a schema repo)
```

- **`rpack init [--schema] <name> [--key <label>]`** initializes the current folder as a catalog repository (with `--key`, or asking for one on a terminal) or a keyless schema repository (`--schema`).
- **`rpack new <version> [--base <release> [+ <release>]...]`** creates `work/<version>/`, anywhere in the repository. The base is `--base` or, by default, the releases below the version that aren't in the past of another one. It writes `version.json` (with no note) and the starting target SQL: for a catalog, the highest-version parent's `.released/target-catalog.sql`, verbatim, with every table, column, group, param, or imported schema version the other parents changed written in, and a `CREATE CATALOG` `VERSION` naming a parent removed; for a schema, the merged `CREATE SCHEMA` rendered into `target-schema.sql`. In a catalog folder, `staging.json` is that parent folder's, and `upgrade-manual.sql` and `test-data.sql` start empty. It refuses an existing folder, a released version, and a version that isn't above every parent. Renaming a version is `git mv`.
- **`rpack set base <release> [+ <release>]... [--force]`** puts the folder on another base and writes its starting files again. Work on the current base refuses it: an `upgrade-manual.sql` or `test-data.sql` that isn't empty, a target SQL file whose model differs from the base, or a `staging.json` that differs from the base's copy. `--force` discards that work. In a released folder the new base is a pending re-release.
- **`rpack status`** prints the catalog or schema, the version, the base, and `Released as <name>` for a released folder (with `; changed since release` when the folder no longer makes that release), then a count of the draft's rules (tables, columns, restrictions, groups, params). A refusal replaces the count. Warnings are a count. A source error still prints that header, then a blank line, `Error assembling <version>`, then the file and line.
- **`rpack build`** writes an accepted draft to `build/update.sql` and prints `Generated build/update.sql`. In a released folder it also says whether that's the `update.sql` the release was made with, and when it is, whether `version.json`'s note differs. A refusal is printed, with the fix (a redefined column needs a reset in `upgrade-manual.sql`; groups and FILES can't change or go, except a FILES that no longer fits, see Read-only FILES under [Library](#library)), and nothing is written. A source error prints `Error assembling <version>` and then the file and line.
- **`rpack release`** produces the release (signing with `rpack.json`'s key in a catalog repository, or hashing steps keylessly in a schema repository), noted with `version.json`'s note if it has one, verifies the release file in a fresh replica, checks that the release reads back to `target-catalog.sql` / `target-schema.sql`, and writes the release file, `.released/`, and the `rpack.json` entry, in that order. It never writes the working files. The same inputs give the same bytes. In a catalog repository on a terminal it first prints the same count as `status`, then a `Warning:` block when the draft has warnings, and asks `Release <catalog>-<version>? y/n`; `n` aborts with nothing written. Without a terminal it does not ask. A released folder refuses without `--force`.
- **`rpack log`** lists the releases by version with their parents and notes, marks the lower ones no higher release includes and the released folders whose files changed since release, then lists the unreleased folders with their bases.

### Target SQL forms

In `target-catalog.sql`:

- **`CREATE SCHEMA`** may omit `CREATORS (...)`; when omitted, it defaults to `rpack.json`'s signing key. In a schema repository's `target-schema.sql`, `CREATORS` must be omitted (the catalog that imports the schema supplies its own creator key).
- **Schema `PARAMS` and group `WITH PARAMS`.** A schema can declare `PARAMS (:p <type> [DEFAULT <literal>], ...)` for use in row allow rules (`:p` in `string`, `integer`, `bigint`, `number`, `decimal`, `boolean`). A new parameter added to an existing schema must have a `DEFAULT`; existing schema parameters cannot be removed or changed. A `TABLEGROUP` sets concrete values with `WITH PARAMS (:p = <literal>, ...)`. `rpack` compares each group parameter's *effective* value (`explicit ?? schemaDefault`): changing a group's effective parameter value requires its schema version to move, and removing a non-default override from `WITH PARAMS` is refused with a hint to write the default's value explicitly.
- **`CONSTANT :name = <literal>;`** defines a script-level literal constant that can be referenced in `WITH PARAMS` and `WITH ROWS` within `target-catalog.sql`.
- **Table remapping on `BIND`.** `BIND alias => group (schema_table => foreign_table, ...)` remaps table names referenced by the group's schema (foreign keys and `EXISTS` rules) to tables in the bound group; catalog-level clauses (`USING IDENTITIES`, `ALLOW DEPLOY IF`, `ALLOW UPDATE REF`) always name the bound group's real tables. Multiple aliases in one group may bind to the same foreign group. When a schema update introduces a new `alias.table` reference without a table mapping, `rpack` warns that it binds by name.

## Publishing, importing, and local patches

Both schema and catalog `.rpack` files can be published to a shared directory registry:

```
<registry>/
  hhs/
    comments/
      hhs_comments-1.0.0-4a19b20c.rpack
      hhs_comments-1.1.0-91e03d12.rpack
  editor/
    editor-1.0.0-c09e77f1.rpack
```

The registry directory is taken from `--registry <dir>`, then `$HHS_REGISTRY`, then `rpack.json`'s `"registry"` (resolved relative to the repository root), and defaults to `~/.hhs/registry`.

- **`rpack publish [<release>] [--registry <dir>]`** verifies the released `.rpack` file (or every release in `rpack.json`'s `released` when `<release>` is omitted), checks that the registry holds no package of the other kind (`schema` vs. `catalog`) or of another catalog id under that name, and writes `<registry>/<name with : as />/<file>.rpack`.
- **`rpack import <schema> [<version>] [--registry <dir>]`** runs inside a catalog's version folder (`work/<version>/`). It resolves `<schema>` in the registry (picking the highest version when `<version>` is omitted, and refusing if `<schema>` is a catalog), verifies the schema `.rpack` file, copies it into `imports/`, locks `imports[schema][version] = releaseHash` in `rpack.json`, and inserts or updates `IMPORT SCHEMA <schema> VERSION ...;` in `work/<version>/target-catalog.sql`. Importing a version that descends from one already listed in `IMPORT SCHEMA` replaces the ancestor; importing a concurrent branch lists both (`VERSION '1.0.1', '1.1.0'`).

### `IMPORT SCHEMA` and local `ALTER SCHEMA` patches

In `target-catalog.sql`, an imported schema is declared with:

```sql
IMPORT SCHEMA hhs:comments VERSION '1.1.0';
-- or, when merging concurrent upstream branches before upstream merges them:
-- IMPORT SCHEMA hhs:comments VERSION '1.0.1', '1.1.0';

ALTER SCHEMA hhs:comments AS (
  ADD COLUMN comments.flagged boolean DEFAULT false,
  SET ALLOW RULES ON comments (
    ALLOW insert IF comments.len <= :maxLen
  )
);
```

- **Replay and multi-version merge.** `readSource` takes the union of the `steps` arrays across the locked `.rpack` files for the listed versions (checking that they share a single genesis step, agree on step hashes for every shared version, and form an antichain). When multiple concurrent versions are listed, Rdb's `RSchema` CRDT merges them. During `draftRelease` and `produceRelease`, new upstream steps are replayed as-is into the catalog's `RSchema` object with their upstream versions (`major < 1000`) and causal parent links.
- **No `inline <-> import` switching.** A schema in a catalog is either inline (`CREATE SCHEMA`) or imported (`IMPORT SCHEMA`) for its entire lifetime in that catalog; switching between the two is refused.
- **Local patches (`1000+` version tier).** Zero or more `ALTER SCHEMA <name> AS (...)` statements in `target-catalog.sql` after `IMPORT SCHEMA <name>` declaratively customize the imported schema. They take no `VERSION`, `PARAMS`, `NOTE`, `BY`, or `AT`. The planner evaluates `candidateView` at the join of the catalog's previous schema frontier and the newly imported upstream head(s), applies `upgrade-manual.sql`, and diffs toward `target-catalog.sql`'s patched state. When the diff is non-empty, it emits one local patch step at version `(1000 + catalogMajor).catalogMinor.catalogPatch` (major versions `1000+` are reserved for local patches). When a newly imported upstream step touches a slot that the local patch overrides, the draft warns.
- **Column resets on imported schemas.** `target-catalog.sql`'s `ALTER SCHEMA` transforms the imported upstream tables into the desired tables on every release, while `upgrade-manual.sql` authorizes destructive drops against the already-deployed schema: resetting an upstream column writes `DROP COLUMN t.c, ADD COLUMN t.c <newType>` in `target-catalog.sql`'s `ALTER SCHEMA` (plus `DROP COLUMN t.c` in `upgrade-manual.sql` for the release that first resets the deployed column); resetting a column that only the local patch added changes its `ADD COLUMN` in `target-catalog.sql` and puts `DROP COLUMN t.c` in `upgrade-manual.sql`.

## Re-releasing

`rpack release --force` in a released folder replaces its release with one made from the folder's working files.

- **Nothing to do** when the folder still makes the release: the same base, the same `update.sql` as `.released/`, and the same note in `version.json`. A new note alone is a change, and the re-release carries it.
- **Where it's made.** The draft runs in a scratch replica of every release file except the one replaced and the releases built on it, its dependents. That is what `releases/` will hold afterwards.
- **Dependents.** Each released dependent is made again, by version, from its own folder's working files and note, on its base with the replaced names mapped to the new ones. A dependent that doesn't build fails the whole re-release, and nothing is written. A dependent ends at its own `target-catalog.sql`, so a change the re-release made that its source doesn't have is undone there, and shows in its warnings. A dependent whose files changed since its release ships those changes, and the preview says so.
- **The question.** On a terminal, the preview lists what each release replaces, each one's count and warnings, and the unreleased folders built on them, then asks. Without dependents it asks before the passphrase; with them, after, since they can only be made on the signed release. Without a terminal, a re-release with dependents needs `--yes`.
- **Writes**, in order: the new release files, each folder's `.released/`, the dependents' `version.json`, the `version.json` of each unreleased folder on a replaced release (repointed, and listed so you check it), `rpack.json`, and last the old release files are removed. An interrupted run leaves extra files, never missing ones.

A copy of a replaced file already shared stays a valid release: a replica that has it holds it next to the new one. Re-releasing the first release starts a new catalog, and replicas of the old one take none of its releases.

## Staging

`rpack stage` in a catalog repository's version folder builds an ordinary rhost app in `stage/`, with one host named `default` (schema repositories have no catalog or table groups, so `rpack stage` refuses there). It rebuilds every time, in `.stage.building/`, and swaps it in, so a failed stage leaves the previous one.

```
stage/
  app.json  keys.json  catalogs/
  hosts/default/
    host.json  rdb/replica.rdb  db/data.sqlite
```

A released folder that still makes its release replays that release, with no key. Any other folder replays its base, then signs its release in memory and deploys it; the planned file stays inside the staging app, and nothing is written to `releases/`. Before the first release the creators are stand-ins, so no passphrase is needed either.

Each step ships a release's file, deploys it through rhost, and adds that release's rows from its folder's `test-data.sql`, found through `rpack.json`; a release without a folder adds none. The app's keystore is `stage/keys.json` (`app.json`'s `keystore`): stand-ins with an empty passphrase, one per `$label` the rows use, plus `admin`, the database's admin, which the host signs with. At the end rpack projects the database once, so `hosts/default/db/data.sqlite` is current without the host running, and prints what the last deploy did to the rows.

The staged folder's `staging.json` follows the same split as rhost's files, and refuses any other field:

```json
{
  "mesh": { "scope": "localhost", "transports": { "ws": { "listen": "ws://127.0.0.1:7400" } } },
  "allow": ["user.identities.keyId"],
  "projection": { "path": "db/data.sqlite" },
  "autoDeploy": "none",
  "params": { "moderator": "$alice" }
}
```

- `mesh` is the host's network settings, written to its `host.json`; it defaults to `{ "scope": "localhost" }`.
- `allow`, `projection`, `autoDeploy` and `params` go to `app.json`. `autoDeploy` defaults to `none`, and a param the release declares and `params` leaves out defaults to `$me` when it's an identity; any other is an error. A name the staged release doesn't declare is an error too.

## Library

Browser-safe. The commands take an `RpackProject` (the repository's files) and, for catalog repositories, the developer's `KeyVault`:

```ts
import { MemoryProject, initProject, newVersion, buildVersion, releaseVersion, publishRelease, importSchema, logReleases } from '@hyper-hyper-space/hhs3_rpack';

const project = new MemoryProject();                 // rdb_tools has a file-system one
const ctx = { project, vault };                      // vault is optional for schema repositories
await initProject(project, 'editor', 'dev');         // or initProject(project, 'hhs:comments', '', { kind: 'schema' })
await newVersion(ctx, '1.0.0');                      // work/1.0.0/
await project.write('work/1.0.0/target-catalog.sql', source);
const { draft, lines } = await buildVersion(ctx, '1.0.0');
const { produced } = await releaseVersion(ctx, '1.0.0', (label) => vault.unlock(label, passphrase));
```

Each command takes the folder name: `setBase(ctx, folder, selectors, { force? })`, `statusOf`, `buildVersion`, `releaseVersion(ctx, folder, unlock?, { force?, confirm?, yes? })`, `publishRelease(project, registry, selector?)`, and `importSchema(project, registry, folder, schemaName, versionSelector?)`. `describeFolder` and `produceVersion` are what staging uses: whether a released folder still makes its release, and the folder's release signed without writing it.

The pieces underneath:

- **`readSource(text, keys, signer, file?, expect?)`** checks `target-catalog.sql`'s forms (with positions), replays any `IMPORT SCHEMA` packs from `expect.imports` into a scratch runtime with stand-in keys, evaluates `target-catalog.sql` (including any local `ALTER SCHEMA` patches on imported schemas), and returns a `CatalogModel` with the real keys. **`readSchemaSource(text, expectedName, file?, expectedVersion?)`** reads `target-schema.sql` for a schema repository. **`readNext`** compiles `upgrade-manual.sql` against the parents' views.
- **`Released.open(files)`** installs catalog release files into one in-memory replica (`resolve`, `defaultParents`, `descendants`, `outside`, and **`base(parents)`**). **`ReleasedSchemas.open(files)`** does the same for schema release files.
- **`diffTables(schema, working, desired)`** gives migration rules in five phases (add columns, add tables, settings, drop tables, drop columns), or refusals; **`diffSchemaParams`** checks added schema parameters; **`diffCatalog`** diffs the groups (including `tableMaps` and effective group `params`), FILES, changes and catalog params.
- **Read-only FILES.** `target-catalog.sql` lists only writable FILES. When a release drops a table or column a released FILES reads, the source can't list it any more (reading it fails, with a hint to remove it), so leave it out. The draft is not refused: it warns `FILES media becomes read-only in 1.1.0: ALLOW WRITE IF reads user.uploaders: schema hhs:user has no table uploaders; it stays in the catalog, and a new FILES can take its place`. Later releases don't warn again. `base(parents).files` marks it `readOnly`, and `base().model` (what `new` writes and the source is compared to) leaves it out. Listed again unchanged, once its tables are back, it is writable again; its name can't take a different definition.
- **`draftRelease`**, **`formatDraft`**, **`produceRelease`** (and **`draftSchemaRelease`**, **`formatSchemaDraft`**, **`produceSchemaRelease`** for schema packs) draft, print and produce releases; **`writeSource(text, target, keys, signer, { imports?, importedVersions? })`** edits `target-catalog.sql` to describe a target model, preserving unchanged units verbatim and reconciling `IMPORT SCHEMA` versions and local `ALTER SCHEMA` patches on branch merges; **`stripCatalogVersion(text, stale)`** removes a `CREATE CATALOG` `VERSION` naming one of `stale`.
- **Registry helpers:** **`publishToRegistry`**, **`importSchemaFromRegistry`**, **`readLockedImports`**, and **`resolveCatalogInRegistry`** (used by `rhost init --catalog` and `rhost update --pull`).

The release file:

```ts
import { exportRelease, installRelease, verifyRelease, formatVerifyReport, parseReleaseFile, parseAnyReleaseFile, serializeReleaseFile, releaseFileName } from '@hyper-hyper-space/hhs3_rpack';

const file = await exportRelease(replica, catalogId, releaseHash);
const name = releaseFileName(file.manifest.name, file.manifest.version, releaseHash);  // editor-1.1.0-e70093a9.rpack
const text = serializeReleaseFile(file);
const report = await installRelease(otherReplica, parseReleaseFile(text));    // { catalog, release, createdRoots, appliedEntries, skippedEntries }
const verdict = await verifyRelease(text);                                    // verifies catalog or schema .rpack: { ok, problems, summary? }
console.log(formatVerifyReport(verdict).join('\n'));
```

- **`exportRelease(ctx, catalogId, release)`** takes the catalog entries in the release's causal past and, for each schema that past references, the entries at or below the versions its releases pin. Schema work that was never released stays out.
- **`installRelease(ctx, file, { backendLabel? })`** creates missing roots (checking each recorded id), then, for each missing entry, recomputes its hash, validates it and applies it. A failure throws an `InstallError` naming the object and the entry. Installing twice is a no-op.
- **`verifyRelease(textOrFile)`** checks the format of a catalog or schema `.rpack` file, installs or replays its steps in a fresh SHA-256 in-memory replica, checks the manifest and step hashes, and returns a summary: the release, its parents and note, and each schema's versions (annotating `1000+` local patch versions) and entry count.
- **`parseReleaseFile(text)`**, **`parseSchemaReleaseFile(text)`**, and **`parseAnyReleaseFile(text)`** accept any JSON layout and throw a `ReleaseFileError` naming the first problem. **`serializeReleaseFile(file)`** writes pretty-printed JSON with sorted keys, so a release always gives the same bytes.

The format is in the addendum of [ARCHITECTURE.md](./ARCHITECTURE.md#addendum-release-file-format) and [COMPOSABILITY.md](./COMPOSABILITY.md#31-the-schema-rpack-format).

## Commands

The `rpack` bin lives in [rdb_tools](../rdb_tools):

```
rpack [-C <dir>] [--keystore <path>] <command>
  init [--schema] <name> [--key <label>]
  new <version> [--base <release> [+ <release>]...]
  set base <release> [+ <release>]... [--force]
  import <schema> [<version>] [--registry <dir>]
  status
  build
  stage [--passphrase-stdin]
  release [--passphrase-stdin] [--force] [--yes]
  publish [<version>] [--registry <dir>]
  log
  verify <file>
  export <store.db> <catalog> [<version> | <version>-<tag> | #hash] [--out <dir>]
```

`init` makes the current folder (or `-C`) a catalog repository or (`--schema`) a schema repository. `new`, `publish`, and `log` run anywhere in it; `set base`, `import`, `status`, `build`, `stage`, and `release` run inside a version folder; `verify` and `export` need no repository. A release is named by version, by version and tag (`2.0.0-8b21`), by name, or by `#hash`; `a + b` may be one argument or several. `status` and `build` exit 1 on refusals. `verify` prints the summary, or the problems and exits 1. `export` writes a release made in the `rdb` REPL to `<dir>/<name>-<version>-<tag>.rpack` (default `./releases`).

## Test

After building the workspace, do

```
npm test
```
