// Test-only entry point, @hyper-hyper-space/hhs3_rdb_adapter/testing: what the
// suites need from the ingestion pass, kept out of the main index that apps
// use. leaveEndsToNewCycles is a process-wide switch: while it is on, every
// ingestion in the process leaves each pass's end to a new cycle.
export { leaveEndsToNewCycles, observeVersion, readingsDiffer } from "./ingest_chain.js";
