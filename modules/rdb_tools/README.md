# Tooling for Rdb
## `rdb`, `rhost` , `rpack`, `rkeys`

Tools for developing applications with [Rdb](../rdb), the causal/relational distributed database from the [Hyper Hyper Space](https://www.hyperhyperspace.org) project.

This module offers four command-line tools:

**rdb**: REPL, CLI, and script runner for [Rdb](../rdb) using [C-SQL](../rdb_lang). Support for configuring [bi-directional projections](../rdb_projection) to SQLite instances, mounting Rdb files in the local filesystem and workspace/key management.

**rhost**: manage a host for an rdb-based application. Set up sync, projection, mounts, install upgrades and manage the auto-upgrade policy.

**rpack**: manage versioning for an application's catalog and schemas, and create releases that can be installed by `rhost`.

**rkeys**: manage the keys in a keystore without a workspace: list and create them, and move them between keystores.

## Build

From the monorepo root:

```
npm install
npm run build
npm link --workspace=@hyper-hyper-space/hhs3_rdb_tools
```

This compiles `src/` and the bins in `bin/` to `dist/` and links the `rdb`, `rpack`, `rhost` and `rkeys` bins into `node_modules/.bin/`.

## Run

`rdb` takes a workspace file (a SQLite DB, created if missing) as its first argument. Run it from anywhere in the repo after install + build:

```
# interactive REPL
rdb my.db

# run one statement
rdb my.db -c "SELECT * FROM g.t;"

# run a script file
rdb my.db -f script.sql

# run a script from stdin
rdb my.db < script.sql
rdb my.db -f -

# prompt for locked keys when running scripts (reads passphrases from the terminal)
rdb my.db -k -f script.sql
rdb my.db -k < script.sql

# JSON output instead of tables
rdb my.db --json

# sign with another keystore file (an app's own, a staging app's stand-ins)
rdb my.db --keystore work/1.1.0/stage/keys.json
```

This is a local workspace bin, not published to npm. Plain `npx rdb` outside the monorepo will not work until we release this package.

Scripts are C-SQL statements separated by `;`, `--` line comments, and `\` meta-commands, one per line. `-c`, `-f`, and stdin scripts use script mode (ref-auto-update off, full hash width) and exit non-zero on error. By default, scripts fail when a locked key is needed; pass `-k` (or `--prompt-keys`, or set `RDB_PROMPT_KEYS=on`) to prompt for passphrases on the terminal instead.

Keys live in a keystore at `~/.rdb/keys.json`. Override with `--keystore <path>`, `RDB_KEYSTORE` (full path) or `RDB_HOME` (dir). [`rkeys`](#rkeys) manages a keystore without a workspace.

## REPL

C-SQL statements terminate with `;` (multi-line and paste supported). Backslash meta-commands:

```
\help                         list meta-commands
\help commands [filter]       C-SQL reference
\dbs \catalogs \schemas       list roots
\groups                       list groups by database (db.group)
\dt [[db.]group] \d [db.]group.table    list / describe tables
\use database <name>          set the current database (bare group names resolve in it)
\use group <[db.]name>        set the current group (bare table names resolve in it)
\catalog [db]                 released / deployed / adopted / held releases, member states
\adopt <range> [db]           widen the adoption range (e.g. ^2, <3.0.0)
\view \frontier [group]       show view / group frontier
\key create|unlock ...        \keys \whoami \author   key + identity mgmt
\alias \aliases \unalias      name #hash prefixes
\output table|json|vertical   \hash-width \hash-labels   display
\ref-auto-update auto|self|off   auto UPDATE REF for bound observers (auto in REPL, off in scripts)
\dump schema|catalog|group|database <name> [full|schema]
\dump op [group] #hash        reverse-render one group op
\delta schema|group <name> <start> <end>
\project start <db> as <id> to <path>
\project status [<db>]  \project stop|update <id>
\project events <id> [after <n>] [before <n>] [limit <m>] [order asc|desc]
\project register-key <id> <keyHash> <publicKey>
\project resolve-key <id> <token>
\project indexes <id> <spec.json | {inline json}> [dry-run]
\project files <id> <name> to <path>
\sync start <db> as <id> [allow …] on localhost|internet
\sync fetch #<rdb-id> as <id> on localhost|internet
\sync status [<db>]  \sync stop <idx>  \sync peers <idx>
\quit
```

A database is created from a catalog release (`CREATE DATABASE ... USING CATALOG`), which creates its table groups and makes it the current database; the prompt shows `rdb:<db>[.<group>]:<author>>`. Group names are per database: `db.group` always resolves, a bare `group` resolves in the current database, and without one only when a single database has it. `UPDATE CATALOG ... ON db` deploys another release (never one at or below a deployed release; a concurrent one merges) and reports the groups it created and deployed. `\catalog` shows how the database stands against its catalog on this replica, and `\adopt ^N` (or `\adopt <N.0.0`, everything below a major) lets a held major release flow.

After a mutating write on a table group, `\ref-auto-update auto` (the REPL default) finds every loaded group that binds the written group and issues `UPDATE REF` recursively, so cross-group FK targets stay current without manual ref-advances. Each automatic ref-update prints a line like `updated ref on shop_prod to #abc…` (suppressed in `--json` output mode).

Ref-auto-update has three modes:

- **auto** — preferred authors (statement author, then `\author`), then keystore scan for any gate-satisfying identity; the REPL may prompt to unlock a matching key.
- **self** — preferred authors only (same order); prompt to unlock those identities if gated and locked; skip with a message listing tested identities if none satisfy the gate (no keystore scan).
- **off** — no automatic ref updates.

For gated `ALLOW UPDATE REF` bindings, validation failures on auth-related rules may include a `hint: BY $label` line suggesting a keystore identity that would satisfy the gate. In the interactive REPL, when a statement omits an explicit `BY` clause and a keystore identity would satisfy the auth rule, the tool may prompt to sign and retry instead of showing the validation error first; explicit `BY` (including `NOBODY` or a failing key) shows the error and hint only. The same sign-and-retry flow applies at bind time for `ALTER SCHEMA`, `ALTER CATALOG` (a catalog creator) and `UPDATE CATALOG` (a database creator) when an author is required and `BY` is omitted. Override with `RDB_REF_AUTO_UPDATE=auto|self|off` (`on` is accepted as `auto`).

`EXPLAIN LOG` adds a `reason` column for Cancelled group/table ops (void restriction, FK, observe-gate, etc.).

`\project files 1 media to media` mounts the FILES member `media` of projection 1 as a folder; the path resolves against the working directory. The folder has `common/` for shared files and `keys/<key id>/` for each owner's, as described in [rdb_projection's file mounts](../rdb_projection#file-mounts). Files put into the member are written there, and files added to `common/` or to the projection identity's `keys/` folder are uploaded, signed by the identity the projection was started `as`. While that identity can't write, the mount line says `read-only for $<label>` and counts the changes waiting, and they're uploaded once it can. A FILES the database doesn't have yet is `pending` and attaches by itself once a release deploys it. A path may not overlap another mount or a projection's target, and a mounted name stays at its path. `\project status` lists the mounts with their counts. A mount lasts as long as its projection: `\project stop` ends it, and the folder stays as it is.

## rpack

The `rpack` bin releases catalogs with [rpack](../rpack). It finds the catalog repository by walking up from the current folder, or from `-C <dir>`, to `rpack.json`, and signs with a key from your keystore (`~/.rdb/keys.json`, `RDB_KEYSTORE`, or `--keystore`). Each version is a folder, `work/<version>/`, and the commands about one version run inside it:

```
rpack init editor --key dev                # rpack.json, .gitignore, releases/ and work/
rpack new 1.0.0                            # work/1.0.0/: version.json and the starting files
rpack log                                  # the releases, their parents, and the versions under way

# inside work/1.0.0/, or with -C work/1.0.0
rpack status                               # the version, its base, and a count of the draft; exits 1 on refusals
rpack build                                # writes build/update.sql; prints Generated build/update.sql
rpack release                              # signed with version.json's "note", if any; on a terminal, asks to confirm, then for the passphrase; --passphrase-stdin reads it from stdin
rpack stage                                # a staging app for this version in stage/

# at the top of the repository
rpack new 1.0.1                            # a lower release: target-catalog.sql starts from 1.0.0's source
rpack new 1.2.0                            # 1.1.0 and 1.0.1's merge, written into target-catalog.sql
rpack new 2.1.0 --base 2.0.0               # explicit parents; several are a + b
rpack -C work/2.1.0 set base 2.0.0 + 1.0.1 --force   # another base, discarding the folder's work
rpack -C work/1.1.0 release --force        # re-release 1.1.0, and the releases built on it

# check a release file offline: prints the release, its parents and schemas, or the problems (exit 1)
rpack verify releases/editor-1.1.0-e70093a9.rpack

# write a release made in the REPL to releases/<name>-<version>-<tag>.rpack
rpack export my.db editor                  # the release no other release follows
rpack export my.db editor 1.1.0 --out dist
rpack export my.db editor 1.1.0-e700       # a tag prefix, when two releases share a version
rpack export my.db editor '#5wCT'          # a release hash prefix
```

- **`init`** takes `--key`, or asks with the same numbered key list as `rhost create`.
- **`new`** runs anywhere in the repository. Its base is `--base`, repeated or written `a + b`, or by default the releases below the version that no other one includes. It refuses an existing folder or a released version.
- **`set base`** writes the folder's starting files again on the new base, unless the folder has work; `--force` discards it.
- **`status`** counts the draft: added and dropped tables and columns, changed restrictions, foreign keys and concurrent deletes, and groups and params. A released folder says `Released as <name>`, and whether it changed since.
- **`build`** writes an accepted draft to `build/update.sql` and prints `Generated build/update.sql`, then any warnings. A refusal is printed and nothing is written.
- **`release`** on a terminal prints the status count and any warnings, then asks `Release <catalog>-<version>? y/n` before signing; `n` aborts with nothing written, and without a terminal it does not ask. It writes the release file, `.released/` and `rpack.json`'s entry, each written aside and renamed, and never the working files. In a released folder it needs `--force`, which re-releases: on a terminal it shows what each re-released version would be and asks `Re-release <name> and the N built on it? y/n`; without one, a re-release with releases built on it needs `--yes`.
- **`stage`** rebuilds `stage/` in the version's folder every time: an rhost app with stand-in keys in `keys.json` and one host, `hosts/default/`. It replays the version's past through rhost and projects once. A released folder that still makes its release needs no key; any other folder needs the release key's passphrase once releases exist.
- **`export`** refuses a missing store, and lists the candidates as `version-tag` when more than one release matches.

## rhost

The `rhost` bin runs an app's databases on this device, with [rhost](../rhost) on its Node platform, [rhost_node](../rhost_node). It acts on the app in the current folder, or in `--app <dir>`. Run from a host folder, `<app>/hosts/<name>/`, and a command with no host name acts on that host. `create` and `join` still name a new host `default` when none is given, and `remove` still requires the name. An app is a folder, and each host has its own key, network settings, replica and projection:

```
my-app/
  app.json  catalogs/
  hosts/<name>/
    host.json
    rdb/replica.rdb     the replica
    db/data.sqlite      the projection the app uses
    files/<mount>/      file mounts, at their configured paths
    run/                rhost.lock  rhost.sock  rhost.log
```

```
# a new app from the release files it ships; asks for each param the flags leave out
rhost --app my-app init --release catalogs/editor-1.1.0-e70093a9.rpack
rhost --app my-app init --release r.rpack --param 'admin=$me'

# a host; asks for the key and the scope the flags leave out
rhost --app my-app create                   # 'default', from the shipped release
rhost --app my-app create --key me --scope internet --passphrase-env EDITOR_PASS --listen ws://0.0.0.0:7400
rhost --app my-app join <database-id> gaming --key me --scope localhost

rhost --app my-app start                    # one background process per host
rhost --app my-app status                   # every host, or name one
rhost --app my-app update                   # asks before updating several; --yes skips the question
rhost --app my-app deploy --version 2.0.0   # deploy now, whatever autoDeploy says
rhost --app my-app deploy --param 'moderator=$me'   # a param the release needs and app.json lacks
rhost --app my-app events --after 12        # rejected and voided writes
rhost --app my-app whoami                   # the host's key, to send to an admin
rhost --app my-app stop gaming
rhost --app my-app serve default            # one host in the foreground, for systemd and the like
rhost --app my-app remove gaming            # this device's copy only
```

- **`init`** writes `app.json` and copies the release files into `catalogs/`. It asks for each param the releases declare (identity params default to `$me`), and refuses a `--param` they don't declare. Without a terminal, every param needs its `--param`. It writes no keys.
- **`create` and `join`** set up the host, and write its `host.json`:
  - `--key <label>` names a key of the keystore by label or `#<key id>`. Without it, they list the keystore's keys and offer "Create new key pair", which is created in that keystore.
  - `--passphrase-env <VAR>` records `env:<VAR>` as the key's passphrase source, for unattended hosts. Without it, the source is `prompt`.
  - `--scope internet|localhost` is asked when left out; `--tracker <url>`, `--tracker-key <key id>` and `--listen <address>` are optional. Two hosts can't listen on one port.
  - Without a terminal, `--key`, `--scope` and `--passphrase-env` are required.
  - They don't start the host. Run `rhost start <name>` afterwards.
- **The keystore** is the user's: `RDB_KEYSTORE`, else `$RDB_HOME/keys.json`, else `~/.rdb/keys.json`. An app that keeps its own sets `keystore` in `app.json`, relative to the app folder. A host finds its key by key id, so relabeling it changes nothing; a missing key is named with the host and the keystore.
- **`start`** unlocks each host's key, asking for each key's passphrase once, so a wrong one fails before anything starts. It hands the passphrase to each host's `serve` process over a pipe, and returns when every process reports that it's serving. Output goes to `hosts/<name>/run/rhost.log`.
- **`stop`** signals the process named in the host's `run/rhost.lock`. `remove` stops a running host before deleting it.
- **`status`** asks a running host over its `run/rhost.sock`, which adds peers and the last error. For a stopped host it reads the store. A host behind the shipped release gets a `behind` line saying why; when a param is missing, a second line says what to run.
- **`update`** prints the same reason and hint when it holds a deploy.
- **`deploy`** runs in its own process on the host's store, unlocking the key itself. A running host picks the deploy up. With no name it uses `default`, or the only host; so do `events` and `whoami`.
  - `--param <name>=<value>` sets a param the release newly declares and `app.json` doesn't, as `$me`, `$<key label>`, or a JSON value. The database keeps it.
  - On a terminal, it asks for any that are still missing.
  - Without one, it fails and names the flags: `deploying 1.1.0 needs :moderator (identity); pass --param moderator=<value> ($me, $<key label>, or a JSON value)`.
- **`events` and `whoami`** read the projection and `host.json`, so they work while the host is stopped, and need no keystore.

The other processes on a host's store work alongside a running host: `rdb hosts/<name>/rdb/replica.rdb` opens it in the REPL, with the same keystore, and the running host picks up its writes and deploys.

In code, the same folder opens with `openApp(dir, ...)` from [rhost_node](../rhost_node), and an app reaches a running host with `openClient(hostDir)` from [rhost_client_node](../rhost_client_node).

## rkeys

The `rkeys` bin manages the keys in a keystore without opening a workspace. It acts on your keystore (`~/.rdb/keys.json`, `RDB_KEYSTORE`, or `RDB_HOME`), or on the file `--keystore <path>` names, given before or after the command:

```
rkeys list                                 # each key's label and key id
rkeys create me                            # on a terminal, asks for the passphrase twice
rkeys create me --passphrase-stdin         # reads it from stdin instead
rkeys export me --out me.keys              # a keystore file holding me, still encrypted
rkeys export me dev > keys.json            # several keys, to stdout
rkeys import me.keys                       # every key in the file
rkeys import work/1.1.0/stage/keys.json alice   # only alice, from a staging app's stand-ins
rkeys --keystore my-app/keys.json list     # an app that keeps its own keystore
```

- **`create`** refuses a label the keystore already has before asking for the passphrase. Without a terminal it needs `--passphrase-stdin`.
- **`export`** writes a keystore file holding the chosen keys, still encrypted with their passphrases, so it asks for none. The file works as `--keystore`, and only the user can read it. `--out` refuses an existing file.
- **`import`** takes any keystore file, or `-` for stdin: an export, or another keystore such as a staging app's `keys.json`. With labels it brings only those keys. It checks every key before writing any. A key whose id doesn't match its public key, a label the keystore holds for another key, or a key it holds under another label refuses the whole import. A key it already holds under the same label is skipped. The sealed secret can't be checked without its passphrase: unlocking a key, in any of the tools, checks that the decrypted key pair matches its key id.

## Test

```
npm test
npm test -- RDB_TOOLS58     # only the tests whose names contain every argument
```

`[RDB_TOOLS58]` runs the `rpack` bin with `-C` through a release chain in a temporary repository (a lower release, and the merge that includes it), creates an rhost host at the lower release and deploys the merge to it, then re-releases 1.1.0, which needs `--yes` without a terminal. `[RDB_TOOLS59]` stages a released folder with no key, checks it rebuilds, refuses a `staging.json` with an unknown field or an undeclared param, stages an unreleased version through a merge, and fails a bad sample row with the previous stage kept. `[RDB_TOOLS55]` to `[RDB_TOOLS57]`, `[RDB_TOOLS63]`, `[RDB_TOOLS65]` and `[RDB_TOOLS66]` run the `rhost` bin: background hosts, a second process on a host's store, whoami, events and deploy, running from a host folder, `create` and `join`'s flags, and `deploy --param`. Their keys are in temporary keystores, named by `app.json`'s `keystore` or `RDB_KEYSTORE`, never `~/.rdb`. `[RDB_TOOLS62]` runs the bin from inside the repository, its version folders and below. Its repository is under `test-build/`, since the test loader resolves from the working folder. `[RDB_TOOLS64]` runs the `rkeys` bin on temporary keystores: create, export and import, and the imports it refuses. The rhost tests bind Unix sockets, which a sandbox may refuse.

