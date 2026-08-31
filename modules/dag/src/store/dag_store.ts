import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { Entry, Header, Position } from "../dag_defs.js";

// Store all the entries and their headers for a DAG.
// When Tx is void (the default), the tx parameter is absent — intended for
// backends where transactions are not necessary to ensure correctness (e.g.
// in-memory stores). Mutation methods require tx when Tx is non-void; read
// methods accept an optional tx so callers outside a transaction can still
// use a default connection.

// A growth notification carries the entries appended in the committing unit of
// work (for local commits) or read back from storage beyond a cursor (for
// external/cross-context growth), plus the DAG frontier after the change.
// Consumers filter `entries` to decide relevance; `frontier` is the raw version.
export type DagGrowth = { entries: readonly Entry[]; frontier: Position };
export type DagGrowthListener = (growth: DagGrowth) => void;

// The transaction callback returns a TxResult so the store knows whether to
// fire growth listeners after commit, and which entries were appended. Carrying
// the entries in the result (rather than in shared store state) keeps local
// delivery correct even if `withTransaction` calls interleave: each call fires
// from its own result. `entries` is optional; omit it (or leave empty) when the
// unit of work appended nothing.
export type TxResult = { fireListeners: boolean; entries?: readonly Entry[] };

export type DagStore<Tx = void> = {
    // The transaction callback must return a TxResult so the store knows
    // whether to fire growth listeners after commit. Listeners are only
    // invoked when the transaction commits successfully and the callback
    // returned { fireListeners: true }.
    withTransaction<T extends TxResult>(fn: (...tx: Tx extends void ? [] : [tx: Tx]) => Promise<T>): Promise<T>;
    append(entry: Entry, ...tx: Tx extends void ? [] : [tx: Tx]): Promise<void>;
    loadEntry(h: B64Hash, ...tx: Tx extends void ? [] : [tx: Tx] | []): Promise<Entry|undefined>;
    loadHeader(h: B64Hash, ...tx: Tx extends void ? [] : [tx: Tx] | []): Promise<Header|undefined>;

    getFrontier(...tx: Tx extends void ? [] : [tx: Tx] | []): Promise<Position>;
    loadAllEntries(...tx: Tx extends void ? [] : [tx: Tx] | []): AsyncIterable<Entry>; // in topo order

    // Growth events.
    //
    // Contract: at-least-once notification. For any observable change to the
    // DAG, at least one registered listener invocation is guaranteed to
    // follow after the transaction that caused the change commits. Listeners
    // MAY be invoked more than once per change, and MAY receive a superset of
    // the strictly-new entries (for example, a local commit may fire both
    // directly and again via an external observer). Listeners must therefore
    // deduplicate; the delivered `entries` are safe to over-report but never
    // under-report an observable change.
    //
    // Concurrency: this reactivity layer imposes NO serialization requirement on
    // the backend. Local delivery is per-transaction (entries travel in the
    // TxResult, not in shared state), so interleaved `withTransaction` calls
    // neither cross-deliver nor drop entries. External growth is handled by a
    // single-flight monitor per store instance whose lifecycle is epoch-gated,
    // preserving at-least-once across arm/disarm/re-arm without any global lock.
    addListener(listener: DagGrowthListener): void;
    removeListener(listener: DagGrowthListener): void;
};
