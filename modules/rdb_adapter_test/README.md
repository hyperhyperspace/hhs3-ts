# rdb_adapter_test

Backend-agnostic **conformance suites + fixtures** for [rdb_adapter](../rdb_adapter) targets. A concrete backend package parameterizes the shared suites with a target factory and asserts behavior in logical terms; engine-specific facts stay in the per-backend package.

Histories for the generative suite come from [rdb_adapter_test_gen](../rdb_adapter_test_gen).

## Contents

- `createProjectionSuite(label, factory)` — initial materialization, incremental insert/update/delete with id stability, re-projection idempotence, `apply()` atomicity, and FK projection.
- `createIngestionSuite(label, factory)` — local-edit round-trips, coalescing, insert-then-delete cancellation, readonly rejection, FK ordering, and dangling-FK rejection.
- `createKeysSuite(label, factory)` — `rdb_keys` / `author_key_id` round-trips (`keyHashForId`, register idempotence).
- `createProjectionParitySuite(label, factory, profile?)` — seeded generative sweep (see below). Filter name is `PROJECTION`.
- A `ProjectionReader` / `LocalMutator` contract, shared group fixtures, and a `MemoryTarget`-backed harness.

Consumed by [rdb_adapter_sqlite](../rdb_adapter_sqlite) and [rdb_adapter_idb](../rdb_adapter_idb), so new targets inherit a common bar of correctness. This package's own `npm test` is a self-check against `MemoryTarget`; sqlite and idb each run the same factories in their own test process.

## Projection vs rdb (generative)

`createProjectionParitySuite` walks an **extending checkpoint chain** from [rdb_adapter_test_gen](../rdb_adapter_test_gen) and `projectGroupTo`s a real target at each step (cost linear in ops, not all checkpoint pairs). At every checkpoint it fingerprints the `ProjectionReader` read-back against the rdb live view (`rdb_projection_oracle.ts`): projected column names, rowIds as keys, FK companions reversed from serial ids to rowIds, authors reversed via `KeyIndex`. Progress prints as `gen ....` then `proj ....`.

It checks **logical row contents** against rdb as the sole oracle. It does **not** compare backends to each other, and it does **not** assert engine-native facts (affinity, `NOT NULL`, DEFAULT rendering) — those stay in the per-backend package. Planner-parity in [rdb_adapter](../rdb_adapter) is a different check: an in-process `ActionStore`, full vs incremental vs rdb, over all extending *pairs*.

Default profile is `smoke` (`PROJECTION_PROFILES`). `--profile fast|full`, `--seeds`, `--ops`, `--max-pairs`, and `PARITY_PROFILE` are the same knobs as the planner fuzzer.

## Test

```
npm test                     # conformance + smoke projection vs rdb (MemoryTarget)
npm run test:projection:fast
npm run test:projection:full
```
