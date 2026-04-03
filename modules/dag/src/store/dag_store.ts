import { Hash } from "@hyper-hyper-space/hhs3_crypto";
import { Entry, Header, Position } from "../dag_defs";

// Store all the entries and their headers for a DAG.
// When Tx is void (the default), the tx parameter is absent from mutation
// methods — intended for backends where transactions are not necessary to
// ensure correctness (e.g. in-memory stores).

export type DagStore<Tx = void> = {
    withTransaction<T>(fn: (...tx: Tx extends void ? [] : [tx: Tx]) => Promise<T>): Promise<T>;
    append(entry: Entry, ...tx: Tx extends void ? [] : [tx: Tx]): Promise<void>;
    loadEntry(h: Hash): Promise<Entry|undefined>;
    loadHeader(h: Hash): Promise<Header|undefined>;

    getFrontier(): Promise<Position>;
    loadAllEntries(): AsyncIterable<Entry>; // in topo order
};
