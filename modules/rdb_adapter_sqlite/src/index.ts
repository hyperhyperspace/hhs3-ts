// rdb_adapter_sqlite: the SQLite-backed MaterializationTarget for the
// engine-neutral rdb_adapter. The only engine-specific artifact is
// SqliteTarget; the vocabulary, planners, and orchestrator live in
// @hyper-hyper-space/hhs3_rdb_adapter.

export * from "./sqlite_target.js";
