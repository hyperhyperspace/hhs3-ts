import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { Entry, Header, Position } from "../dag_defs.js";

// Store all the entries and their headers for a DAG.
// When Tx is void (the default), the tx parameter is absent — intended for
// backends where transactions are not necessary to ensure correctness (e.g.
// in-memory stores). Mutation methods require tx when Tx is non-void; read
// methods accept an optional tx so callers outside a transaction can still
// use a default connection.

export type DagGrowthListener = () => void;

export type TxResult = { fireListeners: boolean };

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
    // MAY be invoked more than once per change (for example, a local commit
    // may fire both directly and again via an external observer). Listeners
    // carry no payload -- consumers should re-read getFrontier() to compute
    // what actually changed and deduplicate.
    addListener(listener: DagGrowthListener): void;
    removeListener(listener: DagGrowthListener): void;
};
