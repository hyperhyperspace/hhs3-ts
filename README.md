![Hyper Hyper Space](https://www.hyperhyperspace.org/logos/HHS_Logo_500px.png)

# Hyper Hyper Space Sync Engine 3.0 (TypeScript)

This is the monorepo for the TypeScript version of [Hyper Hyper Space](https://www.hyperhyperspace.org), version 3.0 [(roadmap)](https://www.hyperhyperspace.org/work-plan-2025.html).

Hyper Hyper Space is a data sync engine focused on **Authority Decentralization**. It enables applications to run locally with full autonomy, and to sync their state securely over the open Internet even in the presence of malfunctioning or adversarial peers. Furthermore, it provides support for sophisticated behavioral rules, intended to enable applications that foster cooperative and productive interactions.

This new version has two main goals:

 - **Greater modularization**. Previous versions of Hyper Hyper Space were bundled as a monolithic JavaScript app for usage in web browsers. While we still see the browser as a possible target, we're now trying to build a collection of modules that can be re-used on any platform. See below for the modules that have been ported to v3 so far.
 
 - **A new data model**. We've developed a new formalism for coordination-free replication, **Monotone View Types**, in which observations are monotonic but explicitly version-scoped, allowing historical views to be refined as additional information becomes available. MVTs are a powerful _monotonic transformation_ mechanism, that helps application developers create coordination-free approximations for applciations in any domain. Learn more in the **`replica`** module [[local]](modules/replica) [[github]](https://github.com/hyperhyperspace/hhs3-ts/tree/main/modules/replica).

### Current status
 
 We're completing the data modeling layer in the **`replica`** module. While the implementation of **Monotone View Types** is complete, we're working on a concurrency model for **Monotone View Type** composition, based on the idea of **State-Observation-as-Data** (SOaD). Once that's complete, the synchronizer will need to be ported over and adapted to work on this new model. After that, adapters and tooling for using the synchronizer with existing information systems (mostly RDBMs) will be developed. Please see the **`replica`** module and the [roadmap](https://www.hyperhyperspace.org/work-plan-2025.html) for details.

### Organization

This monorepo is organized as a collection of modules. This is of course WIP.

- `modules/replica` A replica that can synchronize Monotone View Types [[local]](modules/replica) [[github]](https://github.com/hyperhyperspace/hhs3-ts/tree/main/modules/replica)
- `modules/dag` A DAG-based append-only log with fast fork/merge & covering operations [[local]](modules/dag) [[github]](https://github.com/hyperhyperspace/hhs3-ts/tree/main/modules/dag)
- `modules/crypto` Cryptographic primitives for hashing, encoding, randomness [[local]](modules/crypto) [[github]](https://github.com/hyperhyperspace/hhs3-ts/tree/main/modules/crypto)
- `modules/json` JSON module for content-based addressing data structures [[local]](modules/json) [[github]](https://github.com/hyperhyperspace/hhs3-ts/tree/main/modules/json)
- `modules/util` Collection of helper utilities used across HHS v3.0 [[local]](modules/util) [[github]](https://github.com/hyperhyperspace/hhs3-ts/tree/main/modules/util)

### Building

To build the system, run

```
npm install
npm run build
```

