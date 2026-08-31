# rdb_adapter_test

Backend-agnostic **conformance suites + fixtures** for [rdb_adapter](../rdb_adapter) targets. A concrete backend package parameterizes the shared suites with a target factory and asserts behavior in logical terms; engine-specific facts stay in the per-backend package.

## Contents

- `createProjectionSuite(label, factory)` — initial materialization, incremental insert/update/delete with id stability, re-projection idempotence, `apply()` atomicity, and FK projection.
- `createIngestionSuite(label, factory)` — local-edit round-trips, coalescing, insert-then-delete cancellation, readonly rejection, FK ordering, and dangling-FK rejection.
- A `ProjectionReader` / `LocalMutator` contract, shared group fixtures, a `MemoryTarget`-backed harness, and a mock `RContext`.

Consumed by [rdb_adapter_sqlite](../rdb_adapter_sqlite) and [rdb_adapter_idb](../rdb_adapter_idb), so new targets inherit a common bar of correctness.

## Test

```
npm test
```
