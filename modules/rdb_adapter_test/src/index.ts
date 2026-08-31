// Shared conformance suite + fixtures for rdb_adapter MaterializationTarget
// backends. Consumed by the concrete backend packages (and self-checked here
// against the reference MemoryTarget).

export * from "./group_fixture.js";
export * from "./projection_reader.js";
export { createProjectionSuite } from "./projection_tests.js";
export * from "./ingestion_suite.js";
export { createKeysSuite, memoryKeysSuite, memoryAuthorIdTest } from "./keys_tests.js";
