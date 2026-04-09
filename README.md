![Hyper Hyper Space](https://www.hyperhyperspace.org/logos/HHS_Logo_500px.png)

# Hyper Hyper Space Sync Engine 3.0 (TypeScript)

This is the monorepo for the TypeScript version of [Hyper Hyper Space](https://www.hyperhyperspace.org), version 3.0 [(roadmap)](https://www.hyperhyperspace.org/work-plan-2025.html).

Hyper Hyper Space is a data sync engine focused on **Authority Decentralization**. It enables applications to run locally with full autonomy, and to sync their state securely over the open Internet even in the presence of malfunctioning or adversarial peers. Furthermore, it provides support for sophisticated behavioral rules, intended to enable applications that foster cooperative and productive interactions.

This new version has two main goals:

 - **Greater modularization**. Previous versions of Hyper Hyper Space were bundled as a monolithic JavaScript app for usage in web browsers. While we still see the browser as a possible target, we're now trying to build a collection of modules that can be re-used on any platform. See below for the modules that have been ported to v3 so far.
 
 - **A new data model**. We've developed a new formalism for coordination-free replication, **Monotone View Types**, in which observations are monotonic but explicitly version-scoped, allowing historical views to be refined as additional information becomes available. MVTs are a powerful _monotonic transformation_ mechanism, that helps application developers create coordination-free approximations for applications in any domain. Learn more in the **`replica`** module [[local]](modules/replica) [[github]](https://github.com/hyperhyperspace/hhs3-ts/tree/main/modules/replica).

### Current status
 
 We're completing the data modeling layer in the **`replica`** module. While the implementation of **Monotone View Types** is complete, we're working on a concurrency model for **Monotone View Type** composition, based on the idea of **State-Observation-as-Data** (SOaD). The networking layer (**`mesh`**) now provides peer discovery, authenticated key exchange (with post-quantum options), connection pooling with topic multiplexing, and swarm management. Once the synchronizer is ported, it will bridge **`replica`** and **`mesh`** to enable live state sync. After that, adapters and tooling for using the synchronizer with existing information systems (mostly RDBMs) will be developed. Please see the **`replica`** module and the [roadmap](https://www.hyperhyperspace.org/work-plan-2025.html) for details.

### Organization

This monorepo is organized as a collection of modules. This is of course WIP.

**Data**

- `modules/replica` A replica that can synchronize Monotone View Types [[local]](modules/replica) [[github]](https://github.com/hyperhyperspace/hhs3-ts/tree/main/modules/replica)
- `modules/dag` A DAG-based append-only log with fast fork/merge & covering operations [[local]](modules/dag) [[github]](https://github.com/hyperhyperspace/hhs3-ts/tree/main/modules/dag)
- `modules/dag_sql` SQL-backed storage for DAG entries and indices, using an abstract SQL connection interface [[local]](modules/dag_sql) [[github]](https://github.com/hyperhyperspace/hhs3-ts/tree/main/modules/dag_sql)
- `modules/dag_sqlite` SQLite bindings for the SQL DAG storage layer [[local]](modules/dag_sqlite) [[github]](https://github.com/hyperhyperspace/hhs3-ts/tree/main/modules/dag_sqlite)
- `modules/dag_test` Shared test suites (backend parity, DAG creation helpers) reusable across DAG storage backends [[local]](modules/dag_test) [[github]](https://github.com/hyperhyperspace/hhs3-ts/tree/main/modules/dag_test)

**Synchronization**

- `modules/sync` Synchronizer for the replica module, using the mesh *(planned)* [[local]](modules/sync) [[github]](https://github.com/hyperhyperspace/hhs3-ts/tree/main/modules/sync)

**Networking**

- `modules/mesh` Peer discovery, authentication, connection pooling, topic multiplexing and swarm management [[local]](modules/mesh) [[github]](https://github.com/hyperhyperspace/hhs3-ts/tree/main/modules/mesh)
- `modules/mesh_ws` WebSocket transport implementation for the mesh module [[local]](modules/mesh_ws) [[github]](https://github.com/hyperhyperspace/hhs3-ts/tree/main/modules/mesh_ws)
- `modules/mesh_tracker_client` Tracker-based peer discovery client (PeerDiscovery implementation) [[local]](modules/mesh_tracker_client) [[github]](https://github.com/hyperhyperspace/hhs3-ts/tree/main/modules/mesh_tracker_client)
- `modules/mesh_tracker` Tracker server for peer discovery with identity management [[local]](modules/mesh_tracker) [[github]](https://github.com/hyperhyperspace/hhs3-ts/tree/main/modules/mesh_tracker)

**Libraries**

- `modules/crypto` Cryptographic primitives: hashing, signing, KEM, AEAD, KDF with classical, hybrid and post-quantum suites [[local]](modules/crypto) [[github]](https://github.com/hyperhyperspace/hhs3-ts/tree/main/modules/crypto)
- `modules/json` JSON module for content-based addressing data structures [[local]](modules/json) [[github]](https://github.com/hyperhyperspace/hhs3-ts/tree/main/modules/json)
- `modules/util` Collection of helper utilities used across HHS v3.0 [[local]](modules/util) [[github]](https://github.com/hyperhyperspace/hhs3-ts/tree/main/modules/util)

### Building

To build the system, run

```
npm install
npm run build
```

