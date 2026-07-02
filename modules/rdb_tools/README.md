# rdb_tools

REPL, CLI, and script runner for [Rdb](../rdb). Drives an Rdb replica with [C-SQL](../rdb_lang) over a local SQLite-backed workspace, plus workspace/key management.

## Build

From the monorepo root:

```
npm install
npm run build
```

This compiles `src/` and `bin/rdb.ts` to `dist/` and links the `rdb` bin into `node_modules/.bin/`.

## Run

`rdb` takes a workspace file (a SQLite DB, created if missing) as its first argument. Run it from anywhere in the repo after install + build:

```
# interactive REPL
npx rdb my.db

# run one statement
npx rdb my.db -c "SELECT * FROM g.t;"

# run a script file
npx rdb my.db -f script.sql

# JSON output instead of tables
npx rdb my.db --json
```

This is a local workspace bin, not published to npm. Plain `npx rdb` outside the monorepo will not work until we release this package.

Scripts are C-SQL statements separated by `;`, `--` line comments, and `\` meta-commands, one per line. `-c`/`-f` exit non-zero on error.

Keys live in a keystore at `~/.rdb/keys.json`. Override with `RDB_KEYSTORE` (full path) or `RDB_HOME` (dir).

## REPL

C-SQL statements terminate with `;` (multi-line and paste supported). Backslash meta-commands:

```
\help                         list meta-commands
\help commands [filter]       C-SQL reference
\dbs \schemas \groups         list roots
\dt [group] \d group.table    list / describe tables
\use database|group <name>    set current root
\view \frontier [group]       show view / group frontier
\key create|unlock ...        \keys \whoami \author   key + identity mgmt
\alias \aliases \unalias      name #hash prefixes
\output table|json|vertical   \hash-width \hash-labels   display
\ref-auto-update on|off       auto UPDATE REF for bound observers (on in REPL, off in scripts)
\dump schema|group|database <name>
\delta schema|group <name> <start> <end>
\quit
```

After a mutating write on a table group, `\ref-auto-update on` (the REPL default) finds every loaded group that binds the written group and issues `UPDATE REF` recursively, so cross-group FK targets stay current without manual ref-advances. Each automatic ref-update prints a line like `updated ref on shop_prod to #abc…` (suppressed in `--json` output mode). For gated `ALLOW UPDATE REF` bindings, the tool scans the local keystore for an identity that satisfies the gate (read-only predicate check); the REPL may prompt to unlock a matching key. Validation failures on auth-related rules may include a `hint: BY $label` line suggesting a keystore identity that would satisfy the gate. In the interactive REPL, when a statement omits an explicit `BY` clause and a keystore identity would satisfy the auth rule, the tool may prompt to sign and retry instead of showing the validation error first; explicit `BY` (including `NOBODY` or a failing key) shows the error and hint only. The same sign-and-retry flow applies at bind time for `ALTER SCHEMA` and `ADD SCHEMA` / `ADD TABLEGROUP` when an author is required and `BY` is omitted. Override with `RDB_REF_AUTO_UPDATE=on|off`.

## Test

```
npm test
```

