# Rdb Adapter

## Intro

**[Rdb](../rdb)**'s data structures are designed for **self-verification** and **distributed state reconciliation**. To get fast querying over the last resolved & verified version, apps can use a **bi-directional projection** into a local database. **[SQLite](../rdb_adapter_sqlite/)**, **[IndexedDb](../rdb_adapter_idb/)** (for in-browser usage) and an ephemeral **memory-backed store** (packaged within this module) are available as projection targets.

This module (**rdb_adapter**) provides an abstract bi-directional projection mechanism, mapping an instance of Rdb's `RTableGroup` into a local database. The projection flattens all the history present in Rdb's DAG, omits validation metadata, and continuously exports the contents of the latest version. It includes an *outbox* construction that marshalls all the changes in the local database back into `RTableGroup` operations and the Rdb's DAG.

The export (rdb → relational) is powered by a planner, that generates abstract schema and row changes starting from an Rdb delta. These changes are then applied by a `MaterializationTarget`.

For ingesting changes (relational → rdb) an inverse planner is used, that maps the raw contents of the outbox back into Rdb operations, and optionally groups FK-related consecutive operations into bundles, providing atomicity. The job of draining the outbox into the inverse planner is done by a `MaterializedChangeSource` instance.

Hence to create a new a bi-directional projection, both `MaterializationTarget` and `MaterializedChangeSource` have to be implemented and injected into the generic adapter.

To project an entire database (`Rdb`), the module **[rdb_projection](../rdb_projection/)** acts as a reactive supervisor, creating instances of **rdb_adapter** as `RTableGroup` are added to the database, resolving group naming and managing cross-group interactions by mapping foreign key columns.

## Event monitoring

Co-transactional failures (Rdb changes reverted because of concurrency) and ingestion failures (changes in the local projection that cannot be ingested back into Rdb, either because of programming errors or concurrent Rdb changes) are reported using the `OpEvent` data structure. The app can **subscribe** to new events (`onOpEvents` at open, or `subscribeOpEvents` after) and **inspect** the durable log with `projection.opEvents({ afterId, beforeId, limit, order })`. Subscribe snapshots high-water and streams only later ids; catch-up is the app's own `afterId`. Delivery is at-least-once.


## Status and limitations

While projection is a new concept in HHS, the test suite includes a synthetic test generator and an extensive reproducible stress test runner over the generic planner and the SQLite and IndexedDB adapters. The adapters have been extensively tested using this tooling.

The forward projection planner is universal, and should be usable without limitations. The revererse planner doing change ingestion can only guess the app's bundling intent (based on FK structure). If the app has sophisticated atomicity requirements, it may be necessary to apply the changes directly at the [Rdb](../rdb/) level (or using [C-SQL](../rdb_lang/) for convenience).

Change ingestion works at the data level only. Schema changes can only be performed directly on the **Rdb** instance, since the schema change logic imposes limitations that make concurrent state reconciliation in the face of schema updates more straightforward. The forward planner then applies schema changes safely on any projections.

The projection replaces hash-based ids with database-native numeric ids, that also are used for foreign key dependencies when possible. The numeric ids are not inter-replica stable, and are meant for local usage. If the app needs to communicate off-replica, it sould use the hash-based row/key ids. They can be obtained by using the projection API (`projection.keyHashForId`, `projection.rowHashForLocalId`, etc.) or by directly inspecting the shadow tables that maintain sync state (not recommended).

## Usage

### REPL

Projection can be configured from the [CLI-based REPL](../rdb_tools/) using the `\project` meta-command:

```
\project start <db> as <id> to <path>
\project status [<db>]
\project stop|update <idx>
\project events <idx> [after <n>] [before <n>] [limit <m>] [order asc|desc]
\project indexes <idx> <spec.json | {inline json}> [dry-run]
```

The identity passed as `<id>` is used to sign the operations that are ingested back into Rdb. A SQLite databse is created on `<path>`, and can be queried and modified using standard SQLite tooling.

If changes in the projection generate any ingestion failures, or Rdb concurrency generates op cancellations, those are reported live on the REPL console while the projection is running. `\project events` pages the durable log (default: newest 50, `order desc`).

`\project indexes` installs a projection index spec (see [Indexes](#indexes)) and prints the outcome, the index actions, and any pending declarations.

While `\project` is also supported in the [web REPL demo](../rdb_repl_web/), the only supported path is `:memory:` and the contents of the projection are not inspectable at the moment. The web REPL cannot read files, so `\project indexes` takes the spec inline there.

### Library

A table group can be projected using the library:

```typescript
import Database from 'better-sqlite3';
import { projectGroup } from '@hyper-hyper-space/hhs3_rdb_adapter';
import { SqliteTarget } from '@hyper-hyper-space/hhs3_rdb_adapter_sqlite';

const target = new SqliteTarget(new Database('one-group.sqlite'));
await projectGroup(group, target);
```

For a whole `RDb`, inject the same target into [rdb_projection](../rdb_projection).

## Indexes

A projection can carry its own indexes to speed up local queries. They belong to the projection, not to the Rdb schema: other replicas never see them, and two projections of the same data can index differently. Indexes never enforce anything (there is no `UNIQUE` index); they only make reads faster.

### The index spec

An `IndexSpec` is written in **rdb names**, the names in the `RSchema`, not the projected ones:

```json
{
  "version": 3,
  "indexPub": true,
  "indexes": [
    { "name": "by_customer", "group": "orders", "table": "orders",
      "columns": ["customer", "placed_at"],
      "options": { "columns": { "placed_at": { "desc": true } } } },
    { "name": "by_title", "group": "catalog", "table": "products",
      "columns": ["title"], "options": { "columns": { "title": { "collate": "NOCASE" } } } },
    { "name": "mine", "group": "orders", "table": "orders", "columns": ["@author"] }
  ]
}
```

- `group` is the rdb group name and `table` the rdb table name. `name` must be unique within its group; the same name can be reused in another group.
- `columns` is the ordered list of columns in the index key, and it is the only part of the declaration the adapter core interprets: the index is dropped whenever one of these columns goes away. The pseudo-column `@author` stands for the row's author.
- The adapter maps rdb names to target names exactly as the schema mapper does: table renames and the group prefix (`<group>_<table>` in a replica-wide projection), column renames, foreign key companions (`<col>_id` or `<col>_row_hash`), identity columns (`<col>_key_id`), provider key-id columns (`key_id`), and `@author` to `author_key_id`.
- `options` holds everything else, per-column settings such as descending order included. Its format is defined and validated by each target (see [Targets](#targets)); the core never looks inside it. A spec is therefore written for one kind of target: an app that projects into both SQLite and IndexedDB ships one spec per target.
- `indexPub: true` adds one single-column index named `pub__<column>` for every `pub` column of every projected table. The `pub__` prefix is reserved for this.

### Pending declarations

A declaration whose group, table, or column is not projected (yet) is **pending**, not an error. It is built automatically when a later schema change adds what it needs. If something an index depends on is dropped by a schema change, the index is dropped first, in the same transaction, and the declaration goes back to pending. A provider's public-key column is never projected, so an index on it stays pending.

### Installing a spec

The target stores the installed spec, and syncing never changes it. The app installs a new spec explicitly, as a migration step when it ships a new version:

```typescript
// Replica-wide, with rdb_projection. For hand-wired members, this package's
// reconcileIndexes(members, target, spec) does the same.
const report = await projection.reconcileIndexes(spec);
console.log(report.status, report.actions, report.pending);
```

Reconciling only moves forward, by `version`:

- nothing installed yet, or a higher version: the spec is installed (`installed`);
- the same version with the same content: nothing happens (`unchanged`);
- the same version with different content: it throws, asking you to bump the version;
- a lower version: it is ignored (`skipped-older`), so an older app build sharing the same database does not flip the indexes back.

Reconcile compares the new spec with the indexes actually built, then drops, builds, and records the new spec in one transaction. A declaration that changed in any way, `options` included, is a different index: the old one is dropped and the new one built. It holds the same per-database lock as sync, so the two never interleave. `{ dryRun: true }` returns the plan without changing anything. After that, every sync keeps the installed spec up to date across remote schema changes.

On first launch, open the projection (the initial backfill runs without indexes) and then call `reconcileIndexes`, which builds them. After a restart, the installed spec is already there and nothing needs to happen until the spec changes.

### Targets

- **SQLite** builds real indexes, named `<table>__<name>`, after the batch's rows are written. Its options are `{ "columns": { "<rdb column>": { "desc": true, "collate": "NOCASE" | "RTRIM" | "BINARY", "whereNotNull": true } } }`, every key optional. `whereNotNull` makes a partial index (on several columns, the conditions are ANDed). Validation is strict: any other key, at either level, and any column not listed in `columns`, is rejected before anything is installed. The installed spec and the index records live in the `rdb_index_spec` and `rdb_index_meta` tables.
- **Memory** keeps the bookkeeping only (the spec and index records, nothing physical) and takes no options: a declaration with `options` is rejected. It exists so the lifecycle can be tested without SQLite.
- **IndexedDB** builds shadow indexes that behave like native `IDBIndex`es, read through `store.index(name)` on `target.database` (see [rdb_adapter_idb](../rdb_adapter_idb#indexes)). It takes no options: a declaration with `options` is rejected. So there is no descending order, collation, or partial index, and, as natively, `null`, booleans, and objects are not keys (a row holding one is absent from that index).

### Known limits

- **Unmanaged indexes block remote column drops (SQLite).** Only indexes recorded in `rdb_index_meta` are dropped before a `drop-column`. If the app creates its own index on a projected column and a remote schema change drops that column, SQLite's `ALTER TABLE ... DROP COLUMN` fails, the whole apply rolls back, and that group's sync stalls until the index is removed. (Table rebuilds, used for NOT NULL tightening and foreign key column drops, keep hand-made indexes and skip the ones on a dropped column instead of failing.) Declare indexes in the spec instead of creating them by hand.
- **A stale spec costs performance, never correctness.** A declaration whose table or column was dropped or renamed upstream (in rdb a rename is a drop plus an add) shows up in `report.pending` and stays inactive until the spec is updated. Check the `pending` list after each release.
- **Only reconcile applies spec changes.** A new spec does nothing until `reconcileIndexes` runs; tables that are already caught up keep their previous indexes until then.

## Layout

- `types.ts` — vocabulary + `MaterializationTarget` / `MaterializedChangeSource` / `ChangeSignalSource` / `AdapterConfig`.
- `names.ts` — target/column naming + FK resolution (`resolveFk`).
- `schema_actions.ts`, `row_actions.ts` — pure project-side planners.
- `ingest.ts` — pure inverse planner (coalesce, mint, translate, FK-consecutive bundling).
- `ref_advance.ts` — cross-group ref-advance mechanism (observed→observer index + observe wrapper).
- `project.ts`, `ingest_orchestrator.ts` — single-group and database-level orchestrators.
- `index_actions.ts` — pure index planner (spec validation, rdb → target name resolution, diff against the built indexes).
- `index_reconcile.ts` — `reconcileIndexes`: the version gate and the atomic install of a new index spec.
- `memory_target.ts` — a self-contained in-memory backend (used in tests).

## Implementation details

- **Project** (rdb → relational): pure mappers turn an `RTableGroup`'s resolved schema and row deltas into ordered `SchemaAction` / `RowAction` lists (`schema_actions.ts`, `row_actions.ts`), which a `MaterializationTarget` applies transactionally per group checkpoint (`project.ts`). A same-shape **reincarnation** (a drop+re-add whose resolved def is unchanged, see [Rdb incarnations](../rdb#schema-evolution-and-incarnations)) is a reset, not an in-place diff: a table reincarnation projects as `drop-table` + `create-table` + a live-row backfill, and a column reincarnation as `drop-column` + `add-column`, so stale cells cannot survive an incremental apply. FK columns are reshaped to a companion form: a local (or co-projected cross-group) FK becomes an integer `<col>_id` referencing the target's serial id; a non-co-projected cross-group FK becomes a text `<col>_row_hash` passthrough. Authorship projects as integer `author_key_id` into a shared `rdb_keys(id, key_hash, public_key)` side table (duplicates of a key hash collapse to one id). An identity-provider table's keyId column projects as `key_id` (same side table); its publicKey column is **not** projected — crypto material lives only in `rdb_keys`. A first-class `identity` column type likewise projects as `<col>_key_id`.
- **Ingest** (relational → rdb): the inverse planner (`ingest.ts`) replays the captured outbox in **commit order** — coalesce per row, mint rowIds, reverse-map names, rewrite FK / key-ref values (including reconstructing provider `keyId`+`publicKey` from `rdb_keys`) — then submits signed bundles via `group.bundle()` (`ingest_orchestrator.ts`). Commit order is already FK-respecting (a local FK can only be written against an already-local row), so nothing is reordered. Consecutive same-group ops joined by an explicit FK arc are bundled into one atomic entry (`fkBundling`, default on); to get parent-child atomicity, make the inserts consecutive. New keys are introduced with `KeyIndex.registerKey(domain, keyHash, publicKey)` (public key mandatory).
- **Replica-wide**: `projectDatabase` / `ingestDatabaseChanges` / `syncDatabase` materialize several groups of one `RDb` into **one shared target** so cross-group FKs resolve to serial ids; group-qualified names keep tables from colliding. Ingestion advances co-projected cross-group refs as it drains: a dirty map (`ref_advance.ts`) tracks which observed groups changed, and before an observer's write is appended it observes them to the version present at that point — so cross-group FKs **and** `exists` / restriction reads validate against freshly-ingested rows. A closing drain advances observers that never wrote, transitively.
- **Reactive inbound**: an optional `ChangeSignalSource` lets a target signal "the outbox advanced" so a runtime can ingest without polling.

## Test

The suite is driven by a small custom runner, `test/run_tests.ts`, which registers six groups and applies positional name filters plus `--profile / --seeds / --ops / --max-pairs` flags before running.

```
npm test                  # full suite; planner parity runs under the default `smoke` profile
npm run test:parity:fast  # only the planner-parity fuzzer, `fast` profile (~1.5 min)
npm run test:parity:full  # only the planner-parity fuzzer, `full` profile (deep, tens of minutes)
npm run debug             # same as `npm test` under node --inspect-brk
```

### Unit suites

Deterministic, fast checks on the pure planners (each prints under its own console tag):

- `[ADPT] rdb_adapter schema actions` (`schema_actions_tests.ts`) — project-side schema-action planner.
- `[ADPTR] rdb_adapter row planner` (`row_actions_tests.ts`) — project-side row-action planner.
- `[ADPTI] rdb_adapter inverse planner` (`ingest_tests.ts`) — ingest planner (coalesce, mint, reverse-map, FK bundling).
- `[ADPTV] rdb_adapter concurrency op-events (verdict flips)` (`verdict_events_tests.ts`).
- `[IDX] rdb_adapter projection-local indexes` (`index_actions_tests.ts`) — index planner, plus reconcile and apply-time index maintenance end to end on the memory target.

### Planner-parity fuzzer

`[PLANNER] rdb_adapter planner-parity fuzzer` (`test/planner_parity/`) is a seeded, generative cross-check with two tests:

- **project** (`project_parity.ts`, generator `@hyper-hyper-space/hhs3_rdb_adapter_test_gen`): for every extending checkpoint pair in a random schema+row history, the `full` re-projection, the `incremental` projection, and the live `rdb` projection must all agree (in-process `ActionStore`). A generated index spec covering every column of both versions rides along, and both paths must end with the same index set, equal to the spec resolved at the end version. The same generator feeds the real-target suite in [rdb_adapter_test](../rdb_adapter_test), which walks a linear checkpoint chain on a concrete backend instead of all pairs.
- **ingest** (`ingest_parity.ts`, generator `ingest_generate.ts` in this package): the `optimized` adapter config (`updateMerge` + `fkBundling`) and a `naive` config must produce equivalent live views for the same captured outbox.

Both run against a mock context with `selfValidate: true` (every mutation and view build is re-validated), which is what makes this suite heavy compared to the unit tests.

### Profiles

The fuzzer size is set by `PARITY_PROFILES` in [rdb_adapter_test_gen](../rdb_adapter_test_gen) (`seeds / ops / maxPairs / ingestBatches / ingestChanges`). Projection-vs-rdb sweeps use a separate `PROJECTION_PROFILES` table there (longer histories, linear in ops). Planner knobs:

- `smoke` — `seeds [1, 42]`, `ops 18`, `maxPairs 32`, `ingestBatches 4`, `ingestChanges 24`. Tiny; the default used by `npm test`.
- `fast` — `seeds [1, 42, 9001]`, `ops 30`, `maxPairs 60`, `ingestBatches 6`, `ingestChanges 32`. ~1.5 min; a broader pre-push sweep.
- `full` — `seeds [1, 7, 42, 93, 1771, 9001, 31415]`, `ops 60`, `maxPairs 160`, `ingestBatches 8`, `ingestChanges 48`. Deep/nightly (tens of minutes).

The project sweep dominates runtime: cost scales roughly with `seeds x maxPairs`, each pair doing full + incremental + rdb re-projections over a history of `ops` mutations.

### Filtering and overrides

Positional arguments are name filters (e.g. `PLANNER` runs only the parity tests; any substring runs a single named test), and the profile/knobs can be set explicitly:

```
node --import ../../register.mjs ./test/run_tests.ts PLANNER --profile fast --seeds 1,42
node --import ../../register.mjs ./test/run_tests.ts --ops 40 --max-pairs 80
```

`--profile`, `--seeds`, `--ops`, and `--max-pairs` override the profile's values; the profile itself can also be selected with the `PARITY_PROFILE` env var.
