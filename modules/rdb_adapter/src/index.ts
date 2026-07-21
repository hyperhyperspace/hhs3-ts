// rdb_adapter: project an rdb RTableGroup into a regular relational database.
// The schema/row-action vocabulary + the MaterializationTarget interface
// (types.ts), the pure planners from rdb schema/rows to ordered actions
// (schema_actions.ts / row_actions.ts), the first executing backend
// (sqlite_target.ts), and the orchestrator that drives a group into a target
// (project.ts).

export * from "./types.js";
export * from "./names.js";
export * from "./schema_actions.js";
export * from "./row_actions.js";
export * from "./sqlite_target.js";
export * from "./project.js";
