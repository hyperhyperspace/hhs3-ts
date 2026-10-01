// rdb_projection: a reactive supervisor that keeps a replica-wide relational
// projection of an RDb in sync. Browser-safe and engine-agnostic - the concrete
// MaterializationTarget (SQLite, in-memory, IndexedDB, ...) is injected by the
// host. Built on rdb_adapter's pure planners + database-level orchestrators.
//
//   scope.ts       - resolve an RDb's members into rdb_adapter GroupProjections
//                    (group-qualified names + cross-group id resolution).
//   projection.ts  - RdbProjection: the reactive lifecycle over an RDb + target.
//   files/         - file mounts: a FILES member as a folder (FileDirectory,
//                    MemoryDirectory, the mount pass and its state).

export * from "./scope.js";
export * from "./projection.js";
export * from "./files/directory.js";
export * from "./files/names.js";
export * from "./files/state.js";
export * from "./files/mount.js";
