// rdb_adapter: project an rdb RTableGroup into a regular relational database.
// The schema/row-action vocabulary + the MaterializationTarget interface
// (types.ts), the pure planners from rdb schema/rows to ordered actions
// (schema_actions.ts / row_actions.ts), a self-contained in-memory backend
// (memory_target.ts), and the orchestrator that drives a group into a target
// (project.ts). Executing backends bound to a specific engine (SQLite,
// Postgres, IndexedDB, ...) live in their own packages.

export * from "./types.js";
export * from "./names.js";
export * from "./schema_actions.js";
export * from "./row_actions.js";
export * from "./ingest.js";
export * from "./ref_advance.js";
export * from "./ingest_orchestrator.js";
export * from "./memory_target.js";
export * from "./project.js";
