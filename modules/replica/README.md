# Replica

For the protocol specification, see [SPECS.md](./SPECS.md).

This is HHS v3 **`replica`** module. It provides:

 - Local storage for application state
 - Secure state synchronization accross application instances (BFT)
 - Resolution of concurrent updates, following app-defined rules

## Current usage

In the current architecture, `Replica` acts as an object container and resource provider (`DagBackend`s and meshes). Typical usage is:

1. Create a `Replica`.
2. Attach backend(s) and mesh(es) by label.
3. Register type factories.
4. Call `createObject(init, backendLabel?)` for known root objects (idempotent). The backend label is fixed on the object at creation.
5. Call `registerObject(obj)` for a **non-root ref-advance target** already built elsewhere (label comes from `obj.getBackendLabel()`). Nested RSets/RTables are not registered just because they exist.
6. Configure runtime sync labels in the returned object (for example, `RSet.configure({ meshLabel })`).
7. Start/stop synchronization directly on objects (`obj.startSync()` / `obj.stopSync()`).
8. Call `replica.destroy()` on shutdown to tear down roots (`obj.destroy()` then registry cleanup).

`createObject` adds objects to both the `objects` map and `rootIds`, then notifies `subscribeNewRoot` (cache miss only) and `subscribeNewObject`. `registerObject` records in `objects` and notifies `subscribeNewObject` only — not `subscribeNewRoot`. `unregisterObject(id)` is for owned objects: it calls `destroy()` then removes registry entries; it rejects root ids. Root teardown uses `replica.destroy()` instead.

`subscribeNewObject` is the replica-wide bus the sync layer uses when a deferred foreign-dep target appears. `subscribeNewRoot` stays roots-only (workspace indexing).

`DagBackend` implementations expose `getOrCreateDag(id, meta) -> { dag, created }`, letting `Replica` execute creation payloads only when a DAG is newly created.

## Data model

The data model is based on Monotone View Types (MVTs) and the State-Observation-as-Data (SOaD) architecture. See the [**mvt** module](../mvt) for the full description.

## Development Status

 - Monotone View Type support: **Completed**
 - SOaD Architecture (Causal composition for MVTs): **In progress**
 - Synchronization wiring with runtime object configuration: **Completed**

## Testing

Replica behavior tests live in [`test/`](./test):

- **Basic tests** (`replica_basic_tests.ts`): backend/mesh attachment, type registration, `createObject` idempotency, restart persistence, `registerObject`/`unregisterObject`, `subscribeNewRoot` / `subscribeNewObject`, and `destroy()` root teardown.
- **Nested tests** (`replica_nested_tests.ts`): nested `RSet`-within-`RSet` through the Replica stack.
- **Sync lifecycle tests** (`replica_sync_tests.ts`): `startSync`/`stopSync` wiring and teardown.
- **Full sync tests** (`replica_full_sync_tests.ts`): two-peer mesh sync — one-way, bidirectional, and late-join scenarios.
- **Fetch sync tests** (`replica_fetch_tests.ts`): fetch-based synchronization tests.
- **Permissioned sync tests** (`replica_permissioned_sync_tests.ts`): RCap-gated `RSet` through the full Replica + mesh stack — cross-peer writes, foreign-dep deferral, revocation propagation, and unauthorized payload rejection.

MVT type tests (RSet, RCap, authorship, permissioned sets) live in the [**std_types** module](../std_types/test). See each module's README for details.
