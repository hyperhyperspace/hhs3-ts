![Hyper Hyper Space](https://www.hyperhyperspace.org/logos/HHS_Logo_500px.png)

# Hyper Hyper Space Sync Engine 3.0 (TypeScript)

This is the monorepo for the TypeScript version of [Hyper Hyper Space](https://www.hyperhyperspace.org), version 3.0 [(roadmap)](https://www.hyperhyperspace.org/work-plan-2025.html).

Hyper Hyper Space is a data sync engine focused on **Authority Decentralization**. It enables applications to run locally with full autonomy, and to sync their state securely over the open Internet even in the presence of malfunctioning or adversarial peers. Furthermore, it provides support for sophisticated behavioral rules, intended to enable applications that foster cooperative and productive interactions.

 Earlier versions were built using CRDTs over secure Merkle DAGs as the basic data type abstraction. Eventually we learned there is a fundamental tension between coordination-free systems (like the ones we want to build, that are able to run on a local device) and what is technically known as non-monotonic behaviour (these would be the *conflicts* in Conflict-free replicated data types). It turns out most non-trivial application behavior is non-monotonic: capability systems, access control systems, any kind of scheduling, moderation, anything involving currency / transactions, etc. Hyper Hyper Space used ad-hoc CRDTs extensions to work around this issue, but the resulting system was limited and hard to program.

 Version 3 of Hyper Hyper Space uses a new formalism, called **Monotone View Types** (MVTs), to solve this problem. While the CRDTs in previous versions of HHS were intuitive and meant to be used direcly, MVTs are intended to provide the sync backbone for information systems. Our plan is to create the equivalent of a database's redo-log and multi-version concurrency control systems, but designed for distributed operation in a BFT setting (essentially, the redo-log and the MVCC system are operated cooperatively by all the peers on the network). The application designer will create a schema definition -including any data invariants that must be upheld- and the system will derive an MVT that will be used by the sync engine. You can read more about this new approach in the **`replica`** module [[local]](modules/replica) [[github]](https://github.com/hyperhyperspace/hhs3-ts/tree/main/modules/replica).

 Our implementation of **Monotone View Types** needs fast solution for fork/merge, minimal cover, and metadata querying on DAGs. Support for these is provided in the **`dag`** module [[local]](modules/dag) [[github]](https://github.com/hyperhyperspace/hhs3-ts/tree/main/modules/dag).

**Current status:** We're completing the data modeling layer in the **`replica`** module. While the implementation of **Monotone View Types** is complete, we're working on a concurrency model for **Monotone View Type** composition, based on the idea of **State-Observation-as-Data** (SOaD). Once that's complete, the synchronizer will need to be ported over and adapted to work on this new model. After that, adapters and tooling for using the synchronizer with existing information systems (mostly RDBMs) will be developed. Please see the **`replica`** module and the [roadmap](https://www.hyperhyperspace.org/work-plan-2025.html) for details.

This monorepo is organized as a collection of modules. This is of course WIP.

- `modules/replica` A replica that can synchronize Monotone View Types [[local]](modules/replica) [[github]](https://github.com/hyperhyperspace/hhs3-ts/tree/main/modules/replica)
- `modules/dag` A DAG-based append-only log with fast fork/merge & covering operations [[local]](modules/dag) [[github]](https://github.com/hyperhyperspace/hhs3-ts/tree/main/modules/dag)
- `modules/crypto` Cryptographic primitives for hashing, encoding, randomness [[local]](modules/crypto) [[github]](https://github.com/hyperhyperspace/hhs3-ts/tree/main/modules/crypto)
- `modules/json` JSON module for content-based addressing data structures [[local]](modules/json) [[github]](https://github.com/hyperhyperspace/hhs3-ts/tree/main/modules/json)
- `modules/util` Collection of helper utilities used across HHS v3.0 [[local]](modules/util) [[github]](https://github.com/hyperhyperspace/hhs3-ts/tree/main/modules/util)

To build the system, run

```
npm install
npm run build
```

