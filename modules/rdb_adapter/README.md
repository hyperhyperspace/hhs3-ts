# rdb_adapter

Rdb's data structures are designed to be self-verifiable and to resolve distributed concurrency. An app will need fast querying over the last resolved and verified version, and hence a different data representation. Since this will depend on the application domain, the best solution is to use a bi-directional **projection** of the last state into a database system chosen by the application developer.

[rdb_projection](../rdb_projection) is the projection supervisor. **rdb_adapter** implements the generic machinery for bi-directional projection (plus a simple in-memory `MaterializationTarget`). Other targets are provided in derived modules: [rdb_adapter_sqlite](../rdb_adapter_sqlite), [rdb_adapter_idb](../rdb_adapter_idb). They are injected into the rdb_projection supervisor at runtime.

## What **rdb_adapter** does

- **Project** (rdb → relational): pure mappers turn an `RTableGroup`'s resolved schema and row deltas into ordered `SchemaAction` / `RowAction` lists (`schema_actions.ts`, `row_actions.ts`), which a `MaterializationTarget` applies transactionally per group checkpoint (`project.ts`). A same-shape **reincarnation** (a drop+re-add whose resolved def is unchanged, see [Rdb incarnations](../rdb#schema-evolution-and-incarnations)) is a reset, not an in-place diff: a table reincarnation projects as `drop-table` + `create-table` + a live-row backfill, and a column reincarnation as `drop-column` + `add-column`, so stale cells cannot survive an incremental apply. FK columns are reshaped to a companion form: a local (or co-projected cross-group) FK becomes an integer `<col>_id` referencing the target's serial id; a non-co-projected cross-group FK becomes a text `<col>_row_hash` passthrough. Authorship projects as integer `author_key_id` into a shared `rdb_keys(id, key_hash, public_key)` side table (duplicates of a key hash collapse to one id). An identity-provider table's keyId column projects as `key_id` (same side table); its publicKey column is **not** projected — crypto material lives only in `rdb_keys`. A first-class `identity` column type likewise projects as `<col>_key_id`.
- **Ingest** (relational → rdb): the inverse planner (`ingest.ts`) replays the captured outbox in **commit order** — coalesce per row, mint rowIds, reverse-map names, rewrite FK / key-ref values (including reconstructing provider `keyId`+`publicKey` from `rdb_keys`) — then submits signed bundles via `group.bundle()` (`ingest_orchestrator.ts`). Commit order is already FK-respecting (a local FK can only be written against an already-local row), so nothing is reordered. Consecutive same-group ops joined by an explicit FK arc are bundled into one atomic entry (`fkBundling`, default on); to get parent-child atomicity, make the inserts consecutive. New keys are introduced with `KeyIndex.registerKey(domain, keyHash, publicKey)` (public key mandatory).
- **Replica-wide**: `projectDatabase` / `ingestDatabaseChanges` / `syncDatabase` materialize several groups of one `RDb` into **one shared target** so cross-group FKs resolve to serial ids; group-qualified names keep tables from colliding. Ingestion advances co-projected cross-group refs as it drains: a dirty map (`ref_advance.ts`) tracks which observed groups changed, and before an observer's write is appended it observes them to the version present at that point — so cross-group FKs **and** `exists` / restriction reads validate against freshly-ingested rows. A closing drain advances observers that never wrote, transitively.
- **Reactive inbound**: an optional `ChangeSignalSource` lets a target signal "the outbox advanced" so a runtime can ingest without polling.

## Usage

Single group:

```typescript
import Database from 'better-sqlite3';
import { projectGroup } from '@hyper-hyper-space/hhs3_rdb_adapter';
import { SqliteTarget } from '@hyper-hyper-space/hhs3_rdb_adapter_sqlite';

const target = new SqliteTarget(new Database('one-group.sqlite'));
await projectGroup(group, target);
```

For a whole `RDb`, inject the same target into [rdb_projection](../rdb_projection).

## Layout

- `types.ts` — vocabulary + `MaterializationTarget` / `MaterializedChangeSource` / `ChangeSignalSource` / `AdapterConfig`.
- `names.ts` — target/column naming + FK resolution (`resolveFk`).
- `schema_actions.ts`, `row_actions.ts` — pure project-side planners.
- `ingest.ts` — pure inverse planner (coalesce, mint, translate, FK-consecutive bundling).
- `ref_advance.ts` — cross-group ref-advance mechanism (observed→observer index + observe wrapper).
- `project.ts`, `ingest_orchestrator.ts` — single-group and database-level orchestrators.
- `memory_target.ts` — a self-contained in-memory backend (used in tests).

## Test

The suite is driven by a small custom runner, `test/run_tests.ts`, which registers five groups and applies positional name filters plus `--profile / --seeds / --ops / --max-pairs` flags before running.

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

### Planner-parity fuzzer

`[PLANNER] rdb_adapter planner-parity fuzzer` (`test/planner_parity/`) is a seeded, generative cross-check with two tests:

- **project** (`project_parity.ts`, generator `@hyper-hyper-space/hhs3_rdb_adapter_test_gen`): for every extending checkpoint pair in a random schema+row history, the `full` re-projection, the `incremental` projection, and the live `rdb` projection must all agree (in-process `ActionStore`). The same generator feeds the real-target suite in [rdb_adapter_test](../rdb_adapter_test), which walks a linear checkpoint chain on a concrete backend instead of all pairs.
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
