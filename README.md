![Hyper Hyper Space](https://www.hyperhyperspace.org/logos/HHS_Logo_500px.png)

# Hyper Hyper Space Sync Engine 3.0 (TypeScript)

This is the monorepo for the TypeScript version of [Hyper Hyper Space](https://www.hyperhyperspace.org), version 3.0 [(roadmap)](https://www.hyperhyperspace.org/work-plan-2025.html)

It is organized as a collection of modules. This is WIP, the following modules have been published:

`modules/crypto` Cryptographic primitives for hashing, encoding, randomness
`modules/json` JSON module for content-based addressing data structures
`modules/dag` A DAG-based append-only log with fast fork/merge operations
`modules/util` Collection of helper utilities used across HHS v3.0

To build the system, run

```
npm install
npm run build
```

