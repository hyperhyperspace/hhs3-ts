// Shared conformance suite + fixtures for rdb_adapter MaterializationTarget
// backends. Consumed by the concrete backend packages (and self-checked here
// against the reference MemoryTarget).

export * from "./group_fixture.js";
export * from "./projection_reader.js";
export { createProjectionSuite } from "./projection_tests.js";
export * from "./ingestion_suite.js";
export { createKeysSuite, memoryKeysSuite, memoryAuthorIdTest } from "./keys_tests.js";
export { createProjectionParitySuite } from "./projection_parity_suite.js";
export {
    parseTestFilters, resolveFuzzSweepOptions, PARITY_PROFILES, PROJECTION_PROFILES,
    type FuzzProfileName, type ResolvedFuzzSweepOptions,
} from "@hyper-hyper-space/hhs3_rdb_adapter_test_gen";
