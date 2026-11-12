# rpack implementation

How the design in [README.md](./README.md) is carried out: the files a repository holds, the way a release is derived and checked, the two `.rpack` formats, the import and patch procedure, and where the code lives. Read the README first. Each section here starts from the decision it is implementing.

The `rpack` and `rhost` commands live in [rdb_tools](../rdb_tools). The libraries are browser-safe; the command line is the Node front end.

## The catalog repository

One catalog, one repository, every release present as a file. `rpack` walks up from the current directory, or from `-C`, until it finds `rpack.json`. A command about one version runs anywhere under `work/<version>/`.

```json
{
  "name": "editor",
  "key": "dev",
  "registry": "../registry",
  "imports": {
    "hhs:comments": { "1.1.0": "<step hash>" }
  },
  "released": {
    "1.5.0": "editor-1.5.0-3f9a1c2e",
    "2.0.0": "editor-2.0.0-8b21d0aa"
  }
}
```

`key` is a label in the developer's keystore (`~/.rdb/keys.json`, or `RDB_KEYSTORE`, or `--keystore`). `registry` is optional and is resolved relative to the repository. `imports` locks each vendored schema version to the hash of its pack step. `released` maps a folder name to the release file it produced, without the `.rpack` suffix. Any other field is refused, so a typo does not pass for a setting. `kind` is omitted for a catalog; a schema repository sets `"kind": "schema"` and omits `key`.

A work folder is named after its version, `2.0.0`, or `2.0.0-8b21` when two releases share a version. Renaming one is `git mv`.

```json
{ "base": ["editor-2.0.0-8b21d0aa", "editor-1.5.1-c09e77f1"], "note": "tags, and the 1.5.1 fix" }
```

`base` is the parents, `[]` for the first release. `note`, when present, is the release's note, a non-empty string. `rpack` writes one parent per line and keeps the note when it rewrites `base`.

`.released/` holds `target-catalog.sql`, `upgrade-manual.sql`, and the `update.sql` that was signed. Comparing the working files to it, and `version.json`'s base and note to the release, is how `rpack` tells whether a folder still makes its release. `build/` and `stage/` are outputs and are gitignored.

A release is named by version, by version and tag (`2.0.0-8b21`), by catalog name, or by `#hash`. Several parents are written `a + b`, as one argument or several.

```
rpack [-C <dir>] [--keystore <path>] <command>
  init [--schema] <name> [--key <label>]
  new <version> [--base <release> [+ <release>]...]
  set base <release> [+ <release>]... [--force]
  import <schema> [<version>] [--registry <dir>] [--force]
  status
  build
  stage [--passphrase-stdin]
  release [--passphrase-stdin] [--force] [--yes]
  publish [<version>] [--registry <dir>] [--force]
  log
  verify <file>
  export <store.db> <catalog> [<version>] [--out <dir>]
```

`init` makes the current folder a repository. `new`, `publish`, and `log` run anywhere in it. `set base`, `import`, `status`, `build`, `stage`, and `release` run inside a version folder. `verify` and `export` need no repository. `status` and `build` exit 1 on a refusal. `verify` prints the summary, or the problems, and exits 1. `export` writes a release made by hand in the `rdb` REPL to `<dir>/<name>-<version>-<tag>.rpack`, defaulting to `./releases`.

`rpack new` refuses an existing folder, a version already released, and a version that is not above every parent. The default parents are the releases below this version that are not in the past of another release. `--base` names them explicitly, which is how a lower release is left out on purpose. The draft then warns about those left-out releases.

`rpack status` prints the catalog, the version, the parents, and `Released as <name>` when the folder has been released, adding `changed since release` when the folder would no longer produce that file. Then a count of the draft's rules: tables, columns, restrictions, groups, parameters. A refusal replaces the count. A source error still prints the header, then `Error assembling <version>` and the file and line.

`rpack build` writes an accepted draft to `build/update.sql` and prints `Generated build/update.sql`. The file's header says the statements are generated. In a released folder it also says whether that text is the `update.sql` the release was made with, and whether the note differs. A refusal names the fix: a redefined column needs a reset in `upgrade-manual.sql`; a group or a `FILES` cannot change or disappear, with the read-only exception below.

`rpack release` on a terminal prints the status count, then a `Warning:` block, then asks `Release <catalog>-<version>?`. `n` writes nothing. Without a terminal it does not ask. A released folder refuses without `--force`. The passphrase comes from the terminal, or from `--passphrase-stdin` for the whole line.

## How a draft is built

The draft has to be the unique alters from the named parents to `target-catalog.sql`, and it has to be something Rdb will accept. `rpack build` and `rpack release` both build it. Nothing is signed until `release`.

The parents' release files are installed into an in-memory replica. The state the diff starts from is their merge: each schema at the union of the parents' pins, and the groups folded from both sides. `upgrade-manual.sql` is compiled against those views and applied first. Whatever remains between that result and `target-catalog.sql` becomes migration rules and a catalog release.

Tables and columns match by name. A new table is `add-table`, a missing one `drop-table`, and the same for columns. Foreign keys, restrictions, and concurrent deletes each become one `set-*` rule when their effective values differ. A column whose definition changed is refused, and so is a flipped identity-provider flag. The reset belongs in `upgrade-manual.sql`, as the README describes.

The generated rules of one schema update run in five phases, each sorted by table and column, after the hand-written rules:

1. `add-column`.
2. `add-table`, ordered so a new table follows the new tables its foreign keys and `EXISTS` atoms name. A table on a cycle is added without those references, and phase 3 sets them.
3. `set-fks`, `set-restrictions`, `set-concurrent-deletes`, plus the settings phase 2 deferred. Dropped tables that reference each other have those references cleared here.
4. `drop-table`, referencing tables first.
5. `drop-column`.

Everything is added before anything refers to it, and after phase 3 nothing that stays refers to what phases 4 and 5 remove. The draft applies the rules with Rdb's `applyMigrationRules`. The tables that come out have to equal `target-catalog.sql`.

Groups match by name. A new group is added after the groups it binds. A group whose definition changed, or that disappeared, is refused. `tableMaps` are part of the definition. Parameter values are not: they are revised on the change that moves the group's schema pin.

Pins: a schema this release updates is pinned at the new update, a new schema at its create, and any other schema at the minimal cover of its parents' pins. A group whose pin in any parent differs from its schema's pin gets a change. That is every group the parents disagree on, which a catalog merge requires.

Catalog parameters: new ones are declared on the release. A removed or retyped parameter is refused.

A schema this release creates or changes takes the `VERSION` stated in `target-catalog.sql`, or the release's version when the schema states none. The stated version of a changed schema must be above the versions at the parents. An unchanged schema may state only a version it has. Outside `rpack` the clause may be omitted: a create defaults to `0.0.1`, an update to the next patch.

Warnings, which still release:

- `drop-table`, `drop-column`, or any `set-*` rule, when the bump is not major.
- New parameters, which a deploy then needs values for.
- Lower releases outside the new release's past.
- A `FILES` that becomes read-only (below).
- An imported step that writes a slot the local patch still overrides (see [Local patches](#local-patches)).
- A new `alias.table` reference that binds by name because the group has no table mapping for it.

A released folder is drafted in a scratch replica of every release file except its own release and the releases built on it. Otherwise the draft would find its own version already present. A folder still makes its release when the draft has the same parents and the same `update.sql` bytes as `.released/`, and the same note.

Before signing, the release is installed into a fresh replica from the file's bytes, and the model read back from it must equal `target-catalog.sql`. A release that introduces schemas is preceded by a signed `declare` entry naming them, so a replica learns the new schema ids from the catalog before the release that pins them has to validate.

`build/update.sql` renders pins as `LATEST`, the way `target-catalog.sql` writes them.

### Where the starting source comes from

`rpack new` and `rpack set base` write `target-catalog.sql` by editing the highest-version parent's released source until it describes the parents' merged model. An unchanged stretch is kept verbatim, comments included. A changed table, column, option, parameter, or group is replaced by its rendered source. A new schema is inserted before `CREATE CATALOG`. A unit that is gone is removed with its separator and the comment lines directly above it. A `CREATE CATALOG` `VERSION` equal to a parent's version is removed, so the folder does not claim to be a parent. Schema `VERSION`s are left for the author to edit. Reading the result back must give the merged model.

`set base` refuses when the folder has work that a fresh write would lose: a non-empty `upgrade-manual.sql` or `test-data.sql`, a `target-catalog.sql` whose model differs from the base, or a `staging.json` that differs from the base's copy. `--force` discards that work. In a released folder a new base is a pending re-release.

A release that has no work folder, such as one written by `rpack export`, is rendered from scratch when a folder is later started from it.

### Making a release again

`rpack release --force` is how a file that has not been shared is replaced. When the folder still makes the release — same parents, same `update.sql`, same note — there is nothing to do. A new note alone is a change.

The new release is drafted as if the old one had never been made: every release file except the one replaced and its dependents. That set is what `releases/` will hold afterwards. No other file carries the replaced release in its past, or it would be a dependent too.

Each released dependent is then made, by version, from its own folder's working files and note, on its `base` with the replaced names mapped to the new ones. A dependent ends at its own `target-catalog.sql`, so a change the re-release introduced that the dependent's source does not have is undone there, and shows up in its warnings. A dependent whose files changed since its own release ships those changes too, and the preview says so. A dependent that does not build fails the whole re-release.

A catalog release's hash covers its signature, so only the signed release tells whether anything changed. A re-release therefore asks for the passphrase first, then makes the release and its dependents, and compares each hash with the release it replaces. When every one comes out the same — a source rewritten in a newer syntax, or an `update.sql` an older `rpack` rendered differently — nothing is replaced and nothing is asked: each folder's `.released/` is written again, so `status` and `build` read the folder as released.

Otherwise, on a terminal, the preview lists each replacement, its count and warnings, and the unreleased folders built on them, then asks once. A release that came out the same is shown as unchanged and is not replaced. The line about starting a new catalog shows only when the first release itself changed: its hash is the catalog's id. A no writes nothing. Without a terminal, a re-release that has dependents needs `--yes`, and is refused before the passphrase.

Writes, in order: the new release files; each folder's `.released/`; the dependents' `version.json`; the `version.json` of each unreleased folder on a replaced release, repointed, and listed so the author can check it; `rpack.json`; and last, removal of the replaced files.

## The catalog release file

The file is what a device installs, possibly as the first thing it has, with no keys and no network. It therefore contains the release and every catalog and schema entry in its past, signatures as they were made. Consecutive files overlap. Git's delta compression is what keeps the repository small; a device that skipped ahead still holds a complete past in the one file it copied.

```json
{
  "format": 1,
  "manifest": {
    "name": "editor",
    "catalog": "<catalog id>",
    "version": "1.2.0",
    "release": "<release hash>",
    "parents": ["<release hash>"],
    "note": "..."
  },
  "objects": [
    {
      "id": "<object id>",
      "create": { },
      "entries": [ { "hash": "<hash>", "prevs": ["<hash>"], "payload": { } } ]
    }
  ]
}
```

`format` is `1`. A reader rejects any other. Keys are sorted at every level, with a two-space indent and a trailing newline, so the same release always gives the same bytes and a diff between two files stays readable. Parsing accepts any layout. `objects` holds the schemas sorted by id, then the catalog. Inside an object, entries are in canonical topological order: repeatedly the smallest hash whose predecessors are already written. `prevs` and `parents` are sorted. `note` is present only when the release has one.

Payloads are verbatim. `rpack` does not re-sign. Schema creates are unsigned and content-addressed; catalog entries carry `author` and `signature` inside the payload. Entry metadata is not exported, because the hash covers the payload and the predecessors only, and these objects do not use metadata.

Export takes the catalog entries in the release's causal past. For each schema that past pins, it takes the entries at or below the union of those pins. A schema that is declared and not yet pinned ships its create only. Schema work that was never released stays out.

Install is the synchronizer's apply sequence without the network. A missing root is created after checking that the factory computes the recorded id. Each missing entry has its hash recomputed from the payload and predecessors, then `validatePayload`, then `applyPayload`. A mismatch is rejected before the DAG is touched. Entries already present are skipped, so installing twice does nothing. A catalog needs its schemas present first.

`verify` checks the format, installs into a fresh in-memory replica, checks that the manifest's catalog, version, parents, and note match what was installed, and summarizes the release. That replica uses SHA-256, so a catalog with another hash algorithm cannot be verified yet. Schema versions at major 1000 and above are annotated in the summary as local patches (`1001.1.0 (local patch 1.1.0)`).

The tag in the file name is the first eight hex digits of the release hash. Hashes are standard base64, and `/` and `+` are not safe in file names.

## Staging

Staging exists so each upgrade is applied to the rows the previous releases left behind, in the shape those releases had. `rpack stage` builds an ordinary rhost application in the version folder.

It builds in `.stage.building/` and swaps that in for `stage/`, so a failed stage leaves the previous application, or nothing. The result:

```
stage/
  app.json  keys.json  catalogs/
  hosts/default/
    host.json  rdb/replica.rdb  db/data.sqlite
```

The releases in the staged folder's past are taken in version order, then hash. Versions increase along every path, so a merge's parents are both deployed before it. Two releases with one version are refused, since an application ships one file per catalog. A released folder that still makes its release ends on that file. Any other folder replays its parents and then the release as it would be signed now. The planned file stays inside the staging application.

Each step ships the file, deploys it through rhost, and runs that release's `test-data.sql`, found by the folder `rpack.json` records. A release with no folder adds no rows. Only `INSERT`, `UPDATE`, `DELETE`, and `BUNDLE` are accepted, and a refused row names its file and line. In the rows, `$labels` are stand-in keys. Reference auto-update is on, so a new identity reaches the groups bound to it before they check its signature.

`stage/keys.json` holds one stand-in per `$label` the samples use, plus `admin`, each with an empty passphrase. The host signs as `admin`. Before the first release the stand-ins are the creators, so no developer passphrase is needed. A draft with refusals stops the stage.

`staging.json` is strict, and it follows the same split as a real application:

```json
{
  "mesh": { "scope": "localhost", "transports": { "ws": { "listen": "ws://127.0.0.1:7400" } } },
  "allow": ["user.identities.keyId"],
  "projection": { "path": "db/data.sqlite" },
  "autoDeploy": "none",
  "params": { "moderator": "$alice" }
}
```

`mesh` is written to `host.json` and defaults to `{ "scope": "localhost" }`. The other fields go to `app.json`. `autoDeploy` defaults to `none`. An identity parameter left out of `params` defaults to `$me`, the staging admin. Any other missing parameter is an error, and so is a name the staged release does not declare.

After the last deploy, rpack projects once and prints a row summary: per table, rows gone, changed, and back; a dropped table's rows counted from the schema as it was before the drop; operations voided or reinstated, with their reasons. New groups are reported as created. A deploy problem fails the stage.

```
rhost --app work/2.0.0/stage status
sqlite3 work/2.0.0/stage/hosts/default/db/data.sqlite
rdb work/2.0.0/stage/hosts/default/rdb/replica.rdb --keystore work/2.0.0/stage/keys.json
```

## Schema packs

A shared schema needs a history that is not any one catalog's, and a file an application can vendor and check without a signing key it does not hold. A schema repository is that history.

```
comments/
  rpack.json          { "kind": "schema", "name": "hhs:comments", "released": { ... } }
  releases/           hhs_comments-1.0.0-4a19b20c.rpack
  work/1.0.0/
    version.json
    target-schema.sql
    upgrade-manual.sql
    .released/  build/
```

`target-schema.sql` is a single `CREATE SCHEMA` whose name matches `rpack.json`. `CREATORS` is omitted. `rpack new` in a schema repository writes the merged `CREATE SCHEMA` into that file. A version that produces neither migration rules nor new parameter declarations is refused, including a merge that adds neither. Major versions 1000 and above are refused: that range belongs to catalog-local patches.

A released folder is drafted as a catalog's is: among the releases without its own and the ones built on it. A schema step is unsigned, so `rpack` knows the hash of the step a draft makes without producing the file. The folder still makes its release when that hash is the release's. The hash covers the base, the note, the tables or migration rules, and the parameters; comments, layout and the spelling of a rule do not count. `status` says `changed since release` and `build` names `rpack release --force` when it differs. `.released/` is the record of what was released and is not read for this.

`rpack release --force` in a released schema folder follows the rules of [Making a release again](#making-a-release-again): the new release is drafted without the old one and its dependents, each released dependent is made again from its own folder on the remapped base, one that has no folder or does not build fails the whole re-release, and the old files are removed last. No key is needed, so every release is made before the preview, and the preview asks once. It lists each replacement, a dependent's rules and warnings, and the unreleased folders built on them. Without a terminal, a re-release that has dependents needs `--yes`.

A re-release reaches only this repository. The preview and the result end with a warning about the copies others took: a registry that holds the old file, a catalog that imported it, and a catalog released on it. `rpack publish --force` and `rpack import --force` replace the first two, and the catalog is then re-released and published with `--force` (see [The registry](#the-registry)).

A schema `.rpack` file has `manifest.kind` set to `"schema"` and carries the steps in its causal past. Every step in the file has a distinct version string. Per-slot resolution then never falls through to an entry-hash tie between two concurrent steps of one pack, and the pack resolves the same way in every catalog that replays it.

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
      "tables": []
    },
    {
      "hash": "<step hash>",
      "parents": ["<1.0.0 step hash>"],
      "version": "1.1.0",
      "params": [{ "name": "max_len", "type": "integer", "default": 1000 }],
      "migration": []
    }
  ]
}
```

A step hash is SHA-256 of the canonical JSON of `{ domain: "hhs3-rpack-schema-step-v1", name, version, parents, params?, tables?, migration?, note? }`. `verify` recomputes every hash, checks topological order, unique versions, a non-empty change on every step, and semver increasing along parents, then replays the steps into a scratch schema, preserving the parent links.

In a file name, `:` in the schema name becomes `_`, so `hhs:comments` at `1.0.0` is `hhs_comments-1.0.0-4a19b20c.rpack`. Lookup uses the manifest, not the file name. Replacing `:` with `_` is not reversible on its own.

## The registry

Publish and pull need a place both tools can read without a service. The registry is a directory:

```
<registry>/
  hhs/comments/hhs_comments-1.1.0-91e03d12.rpack
  editor/editor-1.0.0-c09e77f1.rpack
```

`rpack` resolves it from `--registry`, then `$HHS_REGISTRY`, then `rpack.json`'s `registry` relative to the repository, then `~/.hhs/registry`. `rhost` uses the flag, then `$HHS_REGISTRY`, then `~/.hhs/registry`. It has no `rpack.json`.

`rpack publish` verifies the named release, or every release in `released` when the name is omitted, and copies the file to `<registry>/<name with colons as slashes>/`. It refuses a directory that already holds the other kind, schema or catalog, or another catalog id under that name. A re-release makes files the registry cannot hold beside the old ones: a version at another hash, or, when a catalog's first release was made again, another catalog under the name. `publish` refuses those, naming every file in the way. `publish --force` writes the new files and then removes the old ones, in that order, and says which. With a version named it replaces that version alone; without one it replaces every released version, which is what a re-release needs.

`rpack import` runs in a catalog version folder. It finds the schema in the registry, the highest version when none is named, and refuses a catalog file. It copies the file into `imports/`, sets `imports[schema][version]` to the step hash, and inserts or updates `IMPORT SCHEMA` in `target-catalog.sql`. Importing a version that descends from one already listed replaces the ancestor. Importing a concurrent branch lists both versions. A version already locked at another hash is refused before anything is written. `import --force` replaces that version's lock and its copy in `imports/`, which is how a catalog takes a re-released schema. It warns about any other locked copy made on the old history, which needs `import --force` too, and a catalog already released on the old one is then re-released itself. A catalog left on a mix of the two histories does not build, and its re-release writes nothing.

`rhost init --catalog <name> [--version <v>]` resolves a catalog file the same way, refuses a schema, copies it into `catalogs/`, asks for any parameter the flags leave out, and writes `app.json`. Identity parameters default to `$me`. `rhost init --release <file>` copies the given files and writes no keys.

`rhost update --pull` looks for a release of the same catalog id that descends from the one installed, copies the newest such file into `catalogs/`, and then runs the update. Without `--major` the candidates stay within the major already shipped.

## Local patches

A catalog that customizes an import has to keep the upstream steps as real entries, so a later branch has a parent to attach to, and has to record the local difference as a step that outranks a concurrent upstream write to the same slot. The source stays declarative. The versions and the parent links are `rpack`'s.

### What the source may say

`IMPORT SCHEMA <name> VERSION '<v1>' [, '<v2>', ...];` stands before `CREATE CATALOG`. Each version is a vendored file. `readSource` unions their `steps` by hash. The union has exactly one genesis step. No version string appears with two hashes. On that DAG, no listed version is in the past of another listed version. The scratch schema replays the union, and the view at all the listed heads is the upstream state the local `ALTER` is applied to.

`ALTER SCHEMA <name> AS (...)` after an import is the desired local difference. It is legal only for an imported name. An inline schema is edited in its `CREATE SCHEMA`; a reset of an inline schema goes in `upgrade-manual.sql`. The local `ALTER` takes no `VERSION`, `PARAMS`, `NOTE`, `BY`, or `AT`. `readSource` applies those rules on the upstream tables, in order, and the result is the schema the catalog wants. `SET FOREIGN KEYS` and `SET ALLOW RULES` replace the whole slot of that table on top of upstream.

A column reset has one rule, applied to two kinds of column. The `ALTER` in `target-catalog.sql` is what turns the upstream tables into the desired tables on every release, including a release with an empty `upgrade-manual.sql`. `upgrade-manual.sql` is the drop authorized against the schema already deployed.

- A column that exists in the upstream tables and needs a new definition has `DROP COLUMN` then `ADD COLUMN` in the `ALTER`. The release that first resets the deployed column also puts `DROP COLUMN` in `upgrade-manual.sql`.
- A column that exists only because an older patch added it changes its `ADD COLUMN` in the `ALTER`. The drop of the deployed column is only in `upgrade-manual.sql`.

If `ADD COLUMN t.c` fails because `t.c` is already in the upstream tables, the message says to precede it with `DROP COLUMN`, or to remove a redundant `ADD` when upstream has adopted a column the patch used to add.

### How the steps are emitted

The catalog's existing schema entries and the pack's steps are different hash spaces. `rpack` walks the normal entries (major below 1000) of the catalog schema in topological order and matches each to the pack step with the same version, confirming that the predecessors match under the map built so far and that the payload matches the step. A normal entry may have only normal parents. An entry at or below the parents' pin that does not match is a refusal: the schema was inline, or a parent imported a version this import's history does not contain. Entries published outside the parents are matched when they match and skipped when they do not.

A schema that `rpack.json` lists under `imports` stays imported. A schema with any local-patch entry, or an `imports` lock, is refused as an inline `CREATE SCHEMA`. An inline schema is refused as an import.

For a schema new to the catalog, every pack step is emitted as-is: a genesis `create`, then an `update` per later step, with the pack's version, parameters, migration, and parent step hashes. The local diff from the upstream tables to the desired tables, if any, is one more update at the patch version, parented on the imported heads.

For a schema already in the parents, pack steps that have no catalog entry yet are queued with their parents split in two: catalog entry hashes for parents already published, and in-draft step hashes for parents this draft is emitting. Then the candidate frontier is the minimal cover of the parents' pin together with the entries for the imported heads. The local rules are the diff from that frontier, after `upgrade-manual.sql`, to the desired tables.

The patch version of catalog `C = major.minor.patch`, with `major` below 1000, is `(1000 + major).minor.patch`. One patch entry is emitted when the combined hand-written and generated rules are non-empty. Its parents are the parents' pin plus the imported heads. When the rules are empty, no patch entry is written; the pin is the cover of the previous pin and the imported heads.

An upstream step's parents are those mapped entries and in-draft hashes, and nothing else. `produceRelease` does not fill an empty parent set from the schema's current pin. The pin may be a local patch, and an upstream step parented on a patch would put the patch underneath a version numbered below 1000. Before signing, each parent set is reduced to its minimal cover, so a head that is already under the previous pin is not baked into the entry.

The warning is a comparison of two views. Take the slots the newly queued upstream steps wrote. If the desired schema and the pure upstream view differ on one of those slots, the local patch is overriding that step. The message names the queued versions that touched the slot. A removed override leaves the desired schema equal to upstream on that slot, and does not warn. An upstream `drop-table` or `add-table` counts as touching that table's existence, mode, foreign keys, restrictions, and every column present in the candidate, the upstream view, or the desired schema, so a patch column that survived a reset is still reported. It survives because a subordinate write is hidden by a winning `add-table` only when it is causally at or below that `add-table`; a patch concurrent with the reset is not. The diff then emits the drops that bring the candidate in line with the desired schema.

`produceRelease` checks the result by the model, not by assuming one head. The resolved tables and parameters at the published pin equal `target-catalog.sql`. An inline schema that this draft updated has a single version, the one the step stated. An imported schema's versions are the imported heads, the patch version this release wrote if it wrote one, and any older patch versions still at the frontier.

### Merging parents that imported different heads

With one parent, the parent's released `target-catalog.sql` already describes the base, and it is kept verbatim. With several, `writeSource` collects the upstream versions imported for each name, drops any that sit in the past of another, and rewrites the `IMPORT SCHEMA` version list. It then applies the highest parent's local `ALTER` on that new upstream. If the result already matches the merged schema, that `ALTER` is kept verbatim. If it differs, one `ALTER` is synthesized from the upstream tables to the merged tables. A column whose definition differs was reset in a parent, so the synthesized `ALTER` includes `DROP COLUMN` before `ADD COLUMN`, annotated as a reset from a parent. The merge release itself emits another drop only when the parents reset that column differently, in which case `upgrade-manual.sql` is where the author authorizes it. An identity-provider change cannot be expressed by `ALTER SCHEMA`, and writing the source fails naming the schema.

`build/update.sql` comments an upstream step with the pack version it came from, and a patch step with the catalog version it belongs to.

## Parameter values

Schema parameters exist so each group carries the literals its rules close over, and so a later release can revise a literal by deploying a newer schema version. The group never reads the catalog to find them.

A schema create may declare up to 64 parameters, `{ name, type, default? }`, omitted from the payload when empty. A parameter on a create may omit `default`; every group using that schema then supplies a value. A parameter on an update must have a `default`, and redeclaring a name is rejected. An update has to change something: a non-empty migration, or at least one new parameter. A default has to match the parameter's type. A string default may not start with `$`, which is reserved for `$author`, `$row`, and `$param`.

In a rule, a comparison or `like` operand lowers `:name` to `{ param: name }`. An `EXISTS` comparison lowers it to the string `$param.<name>`. The table-query operand type is unchanged, so a parameter cannot appear in a query. Names in a create or update have to be declared on the schema at that entry, with a type compatible with the column they are compared to. Concurrent schema branches that declare the same name converge when type and default agree; otherwise the higher schema version wins, then the larger entry hash.

The group's create payload holds the values set at creation. A schema deploy may carry `params`, the values that deploy adds or revises, and its meta lists exactly those names under `param`, next to the `ref` and `barrier` tags, so the fold can use the DAG's indexed covers. A deploy that carries values must move the schema version: the target is at or above the group's current version and different from it. A deploy that repeats the current version stays valid and cannot carry values.

The effective value of a parameter at a view is the group's resolved write, or the schema default when the group has not written it. If neither exists, or the value does not match the type that won, every atom that reads the parameter is false.

The fold is per name, because a later deploy may move the schema and leave `:max_len` as an earlier deploy set it. Causal candidates are the cover of deploys that wrote that name. Concurrent candidates are the deploy barriers that wrote it, concurrent with the position and visible from the view's `from`, the same widening the schema version uses. The create value is the base when nothing has written it. Candidates causally below another are dropped. The rest rank by the schema version the write was aimed at (the highest semver among its target schema entries), then those entries' hashes as a sorted key, then the hash of the write itself: the deploy entry in the group, the release entry in the catalog. A causally later write wins before any of that.

Catalog definitions and changes store the same literals. A change may carry parameters only when it moves the group's schema version, and the folded values plus the schema defaults have to cover every parameter of the target view. At group creation the payload receives the definition's values, not values folded from later releases, so the group id stays fixed. The member's target values are what a merge of the database's maximal deployed releases would resolve. The planner deploys when the group's version is behind that target, carrying the target values that differ from the values resolved at the frontier. A missing group whose target version is already ahead of its definition gets a catch-up deploy with the same difference. A group that has reached the target version and still differs — possible after a full tie — gets no deploy; the next release that moves its version fixes it. `catalogStatus` lists parameters that differ from the target.

In `target-catalog.sql`, `diffCatalog` compares effective values: the explicit value, or the schema default. Writing a value equal to the effective one emits nothing, so stating a default explicitly is quiet, and omitting an entry whose base value equals the default is quiet too. Omitting an entry that changes the effective value is refused, with the hint to write the default's value. A value that differs while the schema pin does not move is refused, because values only change with a version move.

`CONSTANT :name = <literal>` is a literal — string, number, or boolean — from that line on. Declaring it twice is an error. Sharing a name with a catalog parameter or with any schema parameter in the script is an error, so `:name` on the right of `WITH PARAMS` means one thing. Using a catalog parameter there fails with the reason that schema parameters take literals or constants.

`rpack new` renders a group's `WITH PARAMS` from the folded values at the parents, so the new folder starts from the values the group has, not from the values in its original definition.

## Bindings and table maps

Table maps exist so one schema text can be instantiated on tables whose names the schema did not choose, while catalog-authored clauses keep using the names their author wrote.

`tableMaps` on a group definition and on the group create payload is `{ [alias]: { [schemaTable]: foreignTable } }`, omitted when empty, so a group with no renames keeps the bytes it had before the field existed. Every alias is a binding, every inner map is non-empty, and every name is a valid name. An entry that maps a table to itself is rejected. An entry may name a foreign table the schema does not yet reference, as long as that table exists in the bound group.

Schema-originated references resolve through the map: row restrictions, foreign-key liveness, foreign-key adoption on deploy, foreign-key row ids in the runtime, and the projection's cross-group resolver. `USING IDENTITIES`, `ALLOW DEPLOY IF`, and `ALLOW UPDATE REF` resolve the real table name, with no map. Today `ALLOW UPDATE REF <binding>` is checked and evaluated in the observed group's frame: an unqualified table is that group's table, and a qualified `a.t` uses the observed group's own bindings.

Several aliases may share a target group id. The format allows it. The observer keeps one forward-only version reference per observed object, so those aliases share one `ref-advance` stream and one snapshot of that group's tables. All `canObserve` predicates declared for aliases of that id are combined with `and`. An alias with no predicate adds none. Loops that observe bound groups automatically — the catalog planner, the adapter, and the runtime — deduplicate by target group id, so two aliases do not append two barriers. Diagnostics name the aliases joined with `/`.

`groupViewReason` runs when a group is added and when its schema version moves. It checks the map's foreign tables, schema foreign keys and `EXISTS` atoms after the map (table exists, searched columns `PUB` and of a compatible type), catalog clauses by real name, and the group's parameters against the target schema. `existsMatching` returns false when a searched column is absent or not `PUB`, and the predicate's truth view treats that as false, so a concurrent drop fails the rule closed.

## Read-only FILES

A `FILES` definition is immutable, and the bytes already stored under it have to keep their address. `target-catalog.sql` lists the writable ones. When a release drops a table or column a released `FILES` reads, the source cannot name it — reading the missing table fails, and the message says to remove the `FILES` — so the author leaves it out. The draft warns once: `FILES media becomes read-only in 1.1.0`, with the schema fact that caused it. The `FILES` stays in the catalog. Later releases do not warn again. The parents' model, which is what `new` writes and what the source is compared to, omits it. Listed again unchanged, once its tables are back, it is writable again. Its name cannot take a different definition.

At the group versions where the identity table or the write rule's tables are gone, no key resolves or the write rule is false, so reads keep working and writes and reference advances are refused. Mounts report `writable: false` and keep local changes until a key can write again.

## Running a release on a device

This is the device side of the three decisions. Network settings, file-mount layout, and the client protocol in full are in [rhost](../rhost). What follows is the part that decides whether a shipped release is installed, admitted, and deployed.

Commands act on the application in the current folder, or `--app`. From a host folder, a command with no host name acts on that host. `create` and `join` name a new host `default` when none is given. `remove` always takes the name.

```
rhost init (--release <file>... | --catalog <name> [--version <v>] [--registry <dir>]) [--param <name>=<value>]...
rhost create [<host>] [--catalog <c>] [--name <n>] [--key <label>] [--passphrase-env <VAR>] [--mesh <json>|@<file>]
rhost join <database-id> [<host>] [the same host options]
rhost remove <host>
rhost update [<host>] [--pull] [--major] [--registry <dir>] [--yes]
rhost deploy [<host>] [--version <v>] [--param <name>=<value>]...
rhost start [<host>]
rhost stop [<host>]
rhost serve [<host>] [--passphrase-stdin]
rhost status [<host>]
rhost events [<host>] [--after <id>]
rhost whoami [<host>]
```

`create` and `join` ask for a key and a network scope when the flags and `app.json` leave them out. The key prompt lists the keystore, with an option to create a key pair. The passphrase source is recorded as `prompt`, or `env:<VAR>` with `--passphrase-env`. Without a terminal, `--key`, a scope, and `--passphrase-env` are required. The public key is stored on `host.json`, so `whoami` and the client read it without the keystore. The host finds the private key by key id, so relabeling it in the keystore changes nothing. The recorded public key has to match. A missing key names the host, the label, the short key id, and the keystore.

`start` unlocks each key once, asks once per key id, and fails before any process starts when a passphrase is wrong. It passes the passphrase over a pipe and returns when each host reports that it is serving. `serve` runs one host in the foreground. A running host holds `hosts/<name>/run/rhost.lock`, logs to `rhost.log`, and listens on `rhost.sock`. `stop` signals the process in the lock. `start` does not create a database. Nothing watches `hosts/` for new folders.

On start, and on `update`: install the shipped release (a no-op when its entries are present), set the adoption range to `<(M+1).0.0` where `M` is the shipped major, adopt, and deploy when the shipped release is above every deployed one, `autoDeploy` allows the bump, the key may deploy, and every newly declared parameter has a value. Otherwise `notDeployed` says why and `missingParams` lists the gaps. A hold is not an error. The host still starts and syncs on the release it has.

`deploy` deploys the shipped release, or the one `--version` names, whatever `autoDeploy` says. It runs in its own process and unlocks the key itself. A running host picks the deploy up. The key must be allowed to deploy. A release at or below one already deployed is refused. A release that is merely concurrent with a deployed one — 1.5.1 beside 2.0.0 — is accepted here, and is not taken automatically, because automatic deploy requires the version to be above every deployed one. New parameters come from `params` or from `--param`, as `$me`, `$<label>`, or a JSON value. On a terminal the missing ones are asked; otherwise the command fails and names the flags. A parameter already set is refused. Nothing is written to `app.json`.

`status` prints deployed releases as `2.0.0 + 1.5.1`, the adoption range, held releases, member groups, and the two flags `upgrade required` and `host behind`. Peers, file mounts, and the last error come from the socket, so they appear while the host is running. A stopped host is computed from the store, and computing it admits nothing: the adoption range is set only in the runtime config. `events` lists rejected and voided writes from the projection. Both `events` and `whoami` work while the host is stopped.

`remove` stops the host and deletes this device's copy. The database lives on in every other replica.

A host's key is the unit of linkability. Separate connections still present one identity while the key is shared, including creator authority and identity rows.

The application reads `db/data.sqlite`. `rhost_client` covers the rest. In process, `me`, `registerKey`, and the event log go through the running projection. `status` and `watchStatus` come from the host. Over the Node socket, `status` is a stopped status from `host.json` when no process is running, and `me` and `registerKey` use the projection file directly, so they work while the host is stopped. Before the first start there is no projection file: events are empty, and `me` and `registerKey` fail. The socket carries `status`, `watchStatus`, and `unwatch`. A closed or slow connection drops its watches. Nothing that signs is on it.

Embedded, `openApp(dir)` from `rhost_node` uses the same folder as the command line. `create` and `join` do not start the host. An embedded host takes the same lock and socket as `serve`, so the command line reaches it and the two do not serve it at once. In the browser, `Rhost.open(config, browserPlatform({ releases, prompt }))` keeps each host's record, replica, projection, and keys in IndexedDB, and file mounts in the origin private file system. `serve(app, host)` is what a tab calls. The tab that gets the Web Lock runs the host. A tab that finds it held stays queued and takes over when the holder closes. A tab that never calls `serve` is only a client, reached through a BroadcastChannel.

`app.json` and `host.json` are strict at every depth. A host value replaces the same key from the application: `scope`, each transport, and the whole `discovery` list. On `internet`, an omitted WebRTC section listens at `wss://signal.mypeer.net:4612` and an omitted tracker is `wss://tracker.mypeer.net:4610`. On `localhost`, an omitted WebSocket listens at `ws://127.0.0.1` on a free port. Two hosts cannot share a WebSocket port. `projection.files` mounts `FILES` members as folders; a mount whose `FILES` is not in the deployed catalog yet stays pending and attaches when a deploy brings it. The path rules and the on-disk layout are in the rhost README.

## Where the code lives

The split follows `mesh`: a browser-safe core, and a package per platform. `rpack` depends on the replica, the language, and Rdb, and on none of sync, mesh, projection, or rhost. `rhost` is what applications embed, and the platform — store, projection target, mesh, keystore, config files, the client listener — is injected.

```
rdb_tools                 bins rdb, rpack, rhost, rkeys; the terminal prompter; staging
  ├─ rdb_repl             \sync and \project call rhost's services
  └─ rhost_node           SQLite stores, keystore, node mesh, lock, run/rhost.sock
       └─ rhost           the application: hosts, the in-process client, the dispatcher
            ├─ rpack      release files, install, the producer
            ├─ rdb_projection
            ├─ sync, mesh
            └─ rhost_client
rhost_client              the client interface and the wire protocol
rhost_client_node         the socket and the projection file
rhost_browser             IndexedDB, browser mesh, origin-private files, Web Lock
rhost_client_browser      other tabs, over a BroadcastChannel
```

The producer is `modules/rpack/src`: `project.ts` (the `rpack.json` shape), `model.ts` (the catalog model and the patch-version formula), `source.ts` (reading `target-catalog.sql`, including imports and local alters), `schema_pack.ts` (schema repositories), `released.ts` (the installed release files), `schema_diff.ts` and `catalog_diff.ts`, `draft.ts`, `format_draft.ts`, `produce.ts`, `write_source.ts`, `commands.ts`, `format.ts`, `export.ts`, `install.ts`, `verify.ts`, `registry.ts`. The file-system project and `stage` are `rdb_tools/src/rpack`. Callers pass an `RpackProject` and, for a catalog, a `KeyVault`. Schema commands take no vault.

```ts
const project = new MemoryProject();
const ctx = { project, vault };
await initProject(project, 'editor', 'dev');
await newVersion(ctx, '1.0.0');
await buildVersion(ctx, '1.0.0');
await releaseVersion(ctx, '1.0.0', (label) => vault.unlock(label, passphrase));
```

`describeFolder` and `produceVersion` are what staging uses: whether a released folder still makes its release, and the folder's release signed without writing it. `exportRelease`, `installRelease`, `verifyRelease`, `parseReleaseFile`, `parseSchemaReleaseFile`, `parseAnyReleaseFile`, and `serializeReleaseFile` are the file format. `publishToRegistry`, `importSchemaFromRegistry`, and `resolveCatalogInRegistry` are the registry.

Schema parameters, table maps, and non-injective bindings live in `rdb` (`rschema`, `rtable_group`, `rcatalog`, `rdb/instantiate.ts`, `rdb/catalog_update.ts`, `rdb/conformance.ts`), are parsed and rendered in `rdb_lang`, and are followed by `rdb_projection` and `rdb_runtime` where a foreign table or an automatic observation is resolved.

After a workspace build, `npm test` in the repository runs the suite. The host tests use the catalog in `modules/rdb/examples/editor.sql`.

