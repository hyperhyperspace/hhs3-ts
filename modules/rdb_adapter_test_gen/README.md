# rdb_adapter_test_gen

Shared **pseudo-random rdb history generator** for adapter fuzzers. Depends only on [rdb](../rdb) (plus crypto/dag/mvt). It sits *beside* [rdb_adapter](../rdb_adapter) so both the planner-parity fuzzer and [rdb_adapter_test](../rdb_adapter_test) can import one generator without a cycle.

This package never imports `rdb_adapter` or `rdb_adapter_test`. The ingest-side generator (`ingest_generate.ts`) stays in [rdb_adapter](../rdb_adapter) — it emits captured outbox batches, which are an adapter type.

## Domain

`generateProjectHistory(seed, ops, onOp?)` builds an orders/lines group (optional tags/items) on an in-memory `RContext` with `selfValidate: true`. Genesis is:

- `orders.customer` — required string
- `lines.order` — required string **and** a local FK to `orders` (so a later `set-fks` flip is a required, no-default companion add)
- `lines.qty` — required integer

Row ops insert/update/delete from small pools. Schema ops add/drop defaulted columns, flip `set-fks`, drop+re-add the FK column, retype via drop+add, add/drop tables. About 30% of writes branch off an older checkpoint (`pickConcurrentAt`).

Pathological kinds are first-class menu entries **and** forced onto a seeded late-op schedule (`forcedRareAt`) so they appear even at small `ops`. After a sweep, `assertPathologicalCoverage` requires at least one accepted op of each:

- same-shape and reshaped table reincarnation
- column reincarnation
- concurrent identical / different add-column and add-table
- `concurrentDeletes` paired with a concurrent delete

`onOp` fires once per attempted op so a caller can tick progress during generation.

## Profiles

`resolveFuzzSweepOptions(argv, profiles)` (default `PARITY_PROFILES`) parses `--profile`, `--seeds`, `--ops`, `--max-pairs`, and `PARITY_PROFILE`. Two tables, because the two fuzzers spend their budget differently:

**Planner parity** (`PARITY_PROFILES`) — cost is `seeds × maxPairs` (every extending checkpoint *pair*):

- `smoke` — seeds `[1, 42]`, ops 18, maxPairs 32
- `fast` — seeds `[1, 42, 9001]`, ops 30, maxPairs 60
- `full` — seeds `[1, 7, 42, 93, 1771, 9001, 31415]`, ops 60, maxPairs 160

**Projection** (`PROJECTION_PROFILES`) — cost is linear in `seeds × ops` (one incremental `projectGroupTo` per extending checkpoint). `maxPairs` is only a cap:

- `smoke` — same seeds, ops 18, maxPairs 64
- `fast` — ops 40, maxPairs 128
- `full` — ops 100, maxPairs 256

## Contents

- `PRNG` — deterministic LCG
- checkpoint helpers (`endExtendsStart`, `pickConcurrentAt`, `collectExtendingPairs`, `subsamplePairs`, …)
- `createMockRContext` — in-memory `RContext` on public APIs
- `generateProjectHistory`, `PATHOLOGICAL_KINDS`, `assertPathologicalCoverage`, tallies
