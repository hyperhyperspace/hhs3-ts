// Shared conformance suite + fixtures for rdb_adapter MaterializationTarget
// backends. Consumed by the concrete backend packages (and self-checked here
// against the reference MemoryTarget).

export * from "./group_fixture.js";
export * from "./projection_reader.js";
export { createProjectionSuite } from "./projection_tests.js";
export * from "./ingestion_suite.js";
export * from "./consistency.js";
export * from "./target_hooks.js";
export * from "./scenarios.js";
export { createKeysSuite, memoryKeysSuite, memoryAuthorIdTest } from "./keys_tests.js";
export { createProjectionParitySuite } from "./projection_parity_suite.js";
export { createWriterSuite, type BareTargetFactory } from "./writer_suite.js";
export { rdbRows, recordWriter } from "./ingestion_helpers.js";
export {
    parseTestFilters, resolveFuzzSweepOptions, PARITY_PROFILES, PROJECTION_PROFILES,
    type FuzzProfileName, type ResolvedFuzzSweepOptions,
} from "@hyper-hyper-space/hhs3_rdb_adapter_test_gen";
