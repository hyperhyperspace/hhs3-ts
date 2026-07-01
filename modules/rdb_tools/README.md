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
\dump schema|group|database <name>
\delta schema|group <name> <start> <end>
\quit
```

## Test

```
npm test
```

