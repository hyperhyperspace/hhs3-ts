# rpack

Releases for [Rdb](../rdb) catalogs. A catalog repository holds one work folder per version, each describing the catalog as that version should leave it (`target-catalog.sql`), and rpack turns each folder into a signed release, written to a release file. A release file carries a signed catalog release and its whole past (the catalog entries and the schema entries it pins), so any replica can install it offline and without keys. [DESIGN.md](./DESIGN.md) describes the rpack and rhost tools this module is part of, and [ARCHITECTURE.md](./ARCHITECTURE.md) maps them onto the modules.

## The repository

```
editor/
  rpack.json      { name, key, released: { "1.0.0": "editor-1.0.0-c09e77f1", ... } }
  .gitignore      work/*/build/  work/*/stage/
  releases/       the release files, and nothing else
  work/
    2.0.1/
      version.json         { "base": ["editor-2.0.0-8b21d0aa", "editor-1.5.1-c09e77f1"], "note": "..." }
      target-catalog.sql  upgrade-manual.sql  test-data.sql  staging.json
      .released/           the signed inputs and update.sql, as last released
      build/  stage/       generated
```

A folder is named after its version (`2.0.1`, or `2.0.1-b3c1` when two releases share a version). Its `version.json` says what the release is besides its catalog: `base`, the releases it builds on, its parents (`[]` for the first release), and an optional `note`, the release's note. Any other field is refused. rpack writes it one base entry per line and keeps the note when it changes the base. `rpack.json`'s `released` maps each released folder to its release. The commands find the repository by walking up from the current folder (or `-C <dir>`) to `rpack.json`, and the version folder from `work/<version>/` in the path.

## The producer

A release starts from its base, applies `upgrade-manual.sql` (explicit steps, such as a reset), and diffs the result against `target-catalog.sql`:

```
rpack new 2.0.1       # work/2.0.1/ on the releases below 2.0.1 no other one includes
cd work/2.0.1
rpack status          # the version, its base, and a count of what it would sign
rpack build           # write build/update.sql; print that it did, or the refusals
rpack release         # sign it, write releases/ and .released/
```

- **`rpack new <version> [--base <release> [+ <release>]...]`** creates `work/<version>/`, anywhere in the repository. The base is `--base` or, by default, the releases below the version that aren't in the past of another one. It prints one line, the folder and its base, and writes `version.json` (with no note) and the starting `target-catalog.sql`: the highest-version parent's `.released/target-catalog.sql`, verbatim, with every table, column, group or param the other parents changed written in, and a `CREATE CATALOG` `VERSION` naming a parent removed. `staging.json` is that parent folder's, and `upgrade-manual.sql` and `test-data.sql` start empty. It refuses an existing folder, a released version, and a version that isn't above every parent. Renaming a version is `git mv`.
- **`rpack set base <release> [+ <release>]... [--force]`** puts the folder on another base and writes its starting files again. Work on the current base refuses it: an `upgrade-manual.sql` or `test-data.sql` that isn't empty, a `target-catalog.sql` whose model differs from the base, or a `staging.json` that differs from the base's copy. `--force` discards that work. In a released folder the new base is a pending re-release.
- **`rpack status`** prints the catalog, the version, the base, and `Released as <name>` for a released folder (with `; changed since release` when the folder no longer makes that release), then a count of the draft's rules (tables, columns, restrictions, groups, params). A refusal replaces the count. Warnings are a count. A source error still prints that header, then a blank line, `Error assembling <version>`, then the file and line.
- **`rpack build`** writes an accepted draft to `build/update.sql` and prints `Generated build/update.sql`, then any warnings. In a released folder it also says whether that's the `update.sql` the release was made with, and when it is, whether `version.json`'s note differs. A refusal is printed, with the fix (a redefined column needs a reset in `upgrade-manual.sql`; groups and FILES can't change or go, except a FILES that no longer fits, see Read-only FILES under [Library](#library)), and nothing is written. A source error prints `Error assembling <version>` and then the file and line.
- **`rpack release`** signs the draft, noted with `version.json`'s note if it has one, with `rpack.json`'s key, verifies the release file in a fresh replica, checks that the release reads back to `target-catalog.sql`, and writes the release file, `.released/` (`target-catalog.sql`, `upgrade-manual.sql` and `update.sql`), and the `rpack.json` entry, in that order. It never writes the working files. The same inputs give the same bytes. On a terminal it first prints the same count as `status`, then a `Warning:` block when the draft has warnings, and asks `Release <catalog>-<version>? y/n`; `n` aborts with nothing written. Without a terminal it does not ask. A released folder refuses without `--force`.
- **`rpack log`** lists the releases by version with their parents and notes, marks the lower ones no higher release includes and the released folders whose files changed since release, then lists the unreleased folders with their bases.

## Re-releasing

`rpack release --force` in a released folder replaces its release with one made from the folder's working files.

- **Nothing to do** when the folder still makes the release: the same base, the same `update.sql` as `.released/`, and the same note in `version.json`. A new note alone is a change, and the re-release carries it.
- **Where it's made.** The draft runs in a scratch replica of every release file except the one replaced and the releases built on it, its dependents. That is what `releases/` will hold afterwards.
- **Dependents.** Each released dependent is made again, by version, from its own folder's working files and note, on its base with the replaced names mapped to the new ones. A dependent that doesn't build fails the whole re-release, and nothing is written. A dependent ends at its own `target-catalog.sql`, so a change the re-release made that its source doesn't have is undone there, and shows in its warnings. A dependent whose files changed since its release ships those changes, and the preview says so.
- **The question.** On a terminal, the preview lists what each release replaces, each one's count and warnings, and the unreleased folders built on them, then asks. Without dependents it asks before the passphrase; with them, after, since they can only be made on the signed release. Without a terminal, a re-release with dependents needs `--yes`.
- **Writes**, in order: the new release files, each folder's `.released/`, the dependents' `version.json`, the `version.json` of each unreleased folder on a replaced release (repointed, and listed so you check it), `rpack.json`, and last the old release files are removed. An interrupted run leaves extra files, never missing ones.

A copy of a replaced file already shared stays a valid release: a replica that has it holds it next to the new one. Re-releasing the first release starts a new catalog, and replicas of the old one take none of its releases.

## Staging

`rpack stage` in a version folder builds an ordinary rhost app in `stage/`, with one host named `default`. It rebuilds every time, in `.stage.building/`, and swaps it in, so a failed stage leaves the previous one.

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
  "sync": { "scope": "localhost", "listen": "ws://127.0.0.1:7400" },
  "allow": ["user.identities.keyId"],
  "projection": { "path": "db/data.sqlite" },
  "autoDeploy": "none",
  "params": { "moderator": "$alice" }
}
```

- `sync` is the host's network settings, written to its `host.json`; it defaults to `{ "scope": "localhost" }`.
- `allow`, `projection`, `autoDeploy` and `params` go to `app.json`. `autoDeploy` defaults to `none`, and a param the release declares and `params` leaves out defaults to `$me` when it's an identity; any other is an error. A name the staged release doesn't declare is an error too.

## Library

Browser-safe. The commands take an `RpackProject` (the repository's files) and the developer's `KeyVault`:

```ts
import { MemoryProject, initProject, newVersion, buildVersion, releaseVersion, logReleases } from '@hyper-hyper-space/hhs3_rpack';

const project = new MemoryProject();                 // rdb_tools has a file-system one
const ctx = { project, vault };
await initProject(project, 'editor', 'dev');
await newVersion(ctx, '1.0.0');                      // work/1.0.0/
await project.write('work/1.0.0/target-catalog.sql', source);
const { draft, lines } = await buildVersion(ctx, '1.0.0');
const { produced } = await releaseVersion(ctx, '1.0.0', (label) => vault.unlock(label, passphrase));
```

Each command takes the folder name: `setBase(ctx, folder, selectors, { force? })`, `statusOf`, `buildVersion`, and `releaseVersion(ctx, folder, unlock, { force?, confirm?, yes? })`, whose `confirm` gets a `ReleasePreview` (`formatReleasePreview` prints it). `describeFolder` and `produceVersion` are what staging uses: whether a released folder still makes its release, and the folder's release signed without writing it.

The pieces underneath:

- **`readSource(text, keys, signer, file?, expect?)`** checks `target-catalog.sql`'s forms (with positions), evaluates it in a scratch runtime with stand-in keys, and returns a `CatalogModel` with the real keys. A stated catalog `VERSION`, `NOTE`, or `BY` must agree with `expect` (the release version, `version.json`'s note, and the signer). An omitted schema `VERSION` is created at the catalog version while the file is evaluated; a stated one is returned in `schemaVersions`, and the release creates or updates that schema at it. **`readNext`** compiles `upgrade-manual.sql` against the parents' views.
- **`Released.open(files)`** installs the release files into one in-memory replica; `resolve`, `defaultParents`, `descendants`, `outside`, and **`base(parents)`**, the parents' merged state.
- **`diffTables(schema, working, desired)`** gives migration rules in five phases (add columns, add tables, settings, drop tables, drop columns), or refusals; **`diffCatalog`** the groups, FILES, changes and params. A new `FILES` in `target-catalog.sql` becomes an `ADD FILES`; a FILES that changes under the same name is refused ("FILES definitions are immutable, use a new name"), and so is a released one missing from the source while it still fits its group's schema.
- **Read-only FILES.** `target-catalog.sql` lists only writable FILES. When a release drops a table or column a released FILES reads, the source can't list it any more (reading it fails, with a hint to remove it), so leave it out. The draft is not refused: it warns `FILES media becomes read-only in 1.1.0: ALLOW WRITE IF reads user.uploaders: schema hhs:user has no table uploaders; it stays in the catalog, and a new FILES can take its place`. Later releases don't warn again. `base(parents).files` marks it `readOnly`, and `base().model` (what `new` writes and the source is compared to) leaves it out. Listed again unchanged, once its tables are back, it is writable again; its name can't take a different definition.
- **`draftRelease`**, **`formatDraft`**, **`produceRelease`** draft, print and sign; **`writeSource(text, target, keys, signer)`** edits source to describe a model, verbatim where unchanged; **`stripCatalogVersion(text, stale)`** removes a `CREATE CATALOG` `VERSION` naming one of `stale`, as `new` does with the parents' versions.

The release file:

```ts
import { exportRelease, installRelease, verifyRelease, formatVerifyReport, parseReleaseFile, serializeReleaseFile, releaseFileName } from '@hyper-hyper-space/hhs3_rpack';

const file = await exportRelease(replica, catalogId, releaseHash);
const name = releaseFileName(file.manifest.name, file.manifest.version, releaseHash);  // editor-1.1.0-e70093a9.rpack
const text = serializeReleaseFile(file);
const report = await installRelease(otherReplica, parseReleaseFile(text));    // { catalog, release, createdRoots, appliedEntries, skippedEntries }
const verdict = await verifyRelease(text);                                    // { ok, problems, summary? }
console.log(formatVerifyReport(verdict).join('\n'));
```

- **`exportRelease(ctx, catalogId, release)`** takes the catalog entries in the release's causal past and, for each schema that past references, the entries at or below the versions its releases pin. Schema work that was never released stays out.
- **`installRelease(ctx, file, { backendLabel? })`** creates missing roots (checking each recorded id), then, for each missing entry, recomputes its hash, validates it and applies it. A failure throws an `InstallError` naming the object and the entry. Installing twice is a no-op.
- **`verifyRelease(textOrFile)`** checks the format, installs into a fresh SHA-256 in-memory replica, checks the manifest against the catalog, and returns a summary: the release, its parents and note, and each schema's versions and entry count.
- **`parseReleaseFile(text)`** accepts any JSON layout and throws a `ReleaseFileError` naming the first problem. **`serializeReleaseFile(file)`** writes pretty-printed JSON with sorted keys, so a release always gives the same bytes.

The format is in the addendum of [ARCHITECTURE.md](./ARCHITECTURE.md#addendum-release-file-format).

## Commands

The `rpack` bin lives in [rdb_tools](../rdb_tools):

```
rpack [-C <dir>] [--keystore <path>] <command>
  init <name> [--key <label>]
  new <version> [--base <release> [+ <release>]...]
  set base <release> [+ <release>]... [--force]
  status
  build
  stage [--passphrase-stdin]
  release [--passphrase-stdin] [--force] [--yes]
  log
  verify <file>
  export <store.db> <catalog> [<version> | <version>-<tag> | #hash] [--out <dir>]
```

`init` makes the current folder (or `-C`) a repository. `new` and `log` run anywhere in it; `set base`, `status`, `build`, `stage` and `release` run inside a version folder; `verify` and `export` need no repository. A release is named by version, by version and tag (`2.0.0-8b21`), by name, or by `#hash`; `a + b` may be one argument or several. `status` and `build` exit 1 on refusals. `verify` prints the summary, or the problems and exits 1. `export` writes a release made in the `rdb` REPL to `<dir>/<name>-<version>-<tag>.rpack` (default `./releases`).

## Test

Afte building the workspace, do

```
npm test
```
