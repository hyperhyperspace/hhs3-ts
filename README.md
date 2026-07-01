Hyper Hyper Space

# Hyper Hyper Space Sync Engine 3.0 (TypeScript)

This is the monorepo for the TypeScript version of [Hyper Hyper Space](https://www.hyperhyperspace.org), version 3.0 [(roadmap)](https://www.hyperhyperspace.org/work-plan-2025.html).

Hyper Hyper Space is a data sync engine focused on **Authority Decentralization**. It enables applications to run locally with full autonomy, and to sync their state securely over the open Internet even in the presence of malfunctioning or adversarial peers. Furthermore, it provides support for sophisticated behavioral rules, intended to enable applications that foster cooperative and productive interactions.

This new version has two main goals:

- **Greater modularization**. Previous versions of Hyper Hyper Space were bundled as a monolithic JavaScript app for usage in web browsers. While we still see the browser as a possible target, we're now trying to build a collection of modules that can be re-used on any platform. See below for the modules that have been ported to v3 so far.
- **A new data model**. We've developed a new formalism for coordination-free replication, **Monotone View Types**, in which observations are monotonic but explicitly version-scoped, allowing historical views to be refined as additional information becomes available. MVTs are a powerful *monotonic transformation* mechanism, that helps application developers create coordination-free approximations for applications in any domain. Learn more in the [**mvt** module](modules/mvt).



### Current status

The core sync engine is complete: the `dag` storage layer, `mvt` (Monotone View Types), `replica`, `sync` (synchronizer), and `mesh` (networking, plus transports and tracker) layers are all implemented and working together to enable live peer-to-peer state synchronization. A formal [protocol specification](modules/replica/SPECS.md) covers the full architecture — from authenticated mesh channels through DAG exchange to type-level validation. Standard replicable types (**RSet** and **RCap**, with permissioned RSet composition) are available in the `std_types` module. The **Causal/Relational database** modules `rdb`, `rdb_lang`, and `rdb_tools` are implemented (causal relational MVT model, SQL-like language, REPL/CLI). `rdb_adapter`, which keeps a local relational database in sync with an RDb replica, is in development. Please see the individual module specs and the [roadmap](https://www.hyperhyperspace.org/work-plan-2025.html) for details.

### Organization

This monorepo is organized as a collection of modules. This is of course WIP.

**Data**

- [`modules/mvt`](modules/mvt) Monotone-View Types: DAG-based replicable object type system with nesting support
- [`modules/replica`](modules/replica) A replica that orchestrates Monotone View Type instances for synchronization
- [`modules/dag`](modules/dag) A DAG-based append-only log with fast fork/merge & covering operations
- [`modules/dag_sql`](modules/dag_sql) SQL-backed storage for DAG entries and indices, using an abstract SQL connection interface
- [`modules/dag_sqlite`](modules/dag_sqlite) SQLite bindings for the SQL DAG storage layer
- [`modules/dag_test`](modules/dag_test) Shared test suites (backend parity, DAG creation helpers) reusable across DAG storage backends
- [`modules/std_types`](modules/std_types) Standard replicable types (**RSet**, **RCap**, and permissioned RSet) built on the MVT framework

**Causal/Relational database**

- [`modules/rdb`](modules/rdb) Causal/Relational database engine MVTs: RSchema, RTableGroup, RTable, and RDb
- [`modules/rdb_lang`](modules/rdb_lang) C-SQL: SQL-like language to parse, bind, compile, execute, and reverse-render RDb operations
- [`modules/rdb_tools`](modules/rdb_tools) REPL, CLI (`rdb`), workspace and key management, script runner

**Synchronization**

- [`modules/sync`](modules/sync) Synchronizer for the replica module, using the mesh

**Networking**

- [`modules/mesh`](modules/mesh) Peer discovery, authentication, connection pooling, topic multiplexing, swarm management, incoming connection handling with topic negotiation, and per-swarm authorization
- [`modules/mesh_ws`](modules/mesh_ws) WebSocket transport implementation for the mesh module
- [`modules/mesh_tracker_client`](modules/mesh_tracker_client) Tracker-based peer discovery client (PeerDiscovery implementation)
- [`modules/mesh_tracker`](modules/mesh_tracker) Tracker server for peer discovery with identity management

**Libraries**

- [`modules/crypto`](modules/crypto) Cryptographic primitives: hashing, signing, KEM, AEAD, KDF with classical, hybrid and post-quantum suites
- [`modules/json`](modules/json) JSON module for content-based addressing data structures
- [`modules/util`](modules/util) Collection of helper utilities used across HHS v3.0



### Building

To build the system, run

```
npm install
npm run build
```

The latest LTS versions of Node (v24.16.0) and NPM (11.13.0) are supported.

### Tests

To run all the test suites, run

```
npm run test
```

This automatically excludes the large tests cases from the DAG module, that take a significant amount of time / memory to run (but it does run the same tests on smaller instances).