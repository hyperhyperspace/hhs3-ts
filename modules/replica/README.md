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
4. Call `createObject(init, backendLabel?)` for known root objects (idempotent).
5. Configure runtime sync labels in the returned object (for example, `RSet.configure(...)`).
6. Start/stop synchronization directly on objects (`obj.startSync()` / `obj.stopSync()`).
7. Call `replica.close()` on shutdown for best-effort teardown.

`DagBackend` implementations expose `getOrCreateDag(id, meta) -> { dag, created }`, letting `Replica` execute creation payloads only when a DAG is newly created.

## Data model

The data model is based on Monotone View Types (MVTs) and the State-Observation-as-Data (SOaD) architecture. See the [**mvt** module](../mvt) for the full description.

## Development Status

 - Monotone View Type support: **Completed**
 - SOaD Architecture (Causal composition for MVTs): **In progress**
 - Synchronization wiring with runtime object configuration: **Completed**

## Testing

Replica behavior tests live in the [**replica** module](./test).

Monotone View Replicable Set tests (barrier add/delete, nesting) live in the [**std_types** module](../std_types/test). See each module's README for details on running them.
