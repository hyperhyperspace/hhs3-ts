// rdb_adapter_idb: the IndexedDB-backed MaterializationTarget for the
// engine-neutral rdb_adapter. The only engine-specific artifacts are IdbTarget
// and its virtual IDB facade; the vocabulary, planners, and orchestrator live
// in @hyper-hyper-space/hhs3_rdb_adapter.

export { IdbTarget, type IdbTargetOptions } from "./idb_target.js";
export { FacadeDatabase, FacadeTransaction, FacadeObjectStore, FacadeRequest } from "./idb_facade.js";
export {
    SCHEMA_VERSION, TABLE_META, CHECKPOINT, ROWS, SYNC, KEYS, COUNTERS, OUTBOX, OP_EVENTS, CAPTURE_CONFIG,
} from "./idb_schema.js";
