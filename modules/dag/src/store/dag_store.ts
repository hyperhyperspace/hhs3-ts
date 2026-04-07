import { Hash } from "@hyper-hyper-space/hhs3_crypto";
import { Entry, Header, Position } from "../dag_defs.js";

// Store all the entries and their headers for a DAG.
// When Tx is void (the default), the tx parameter is absent — intended for
// backends where transactions are not necessary to ensure correctness (e.g.
// in-memory stores). Mutation methods require tx when Tx is non-void; read
// methods accept an optional tx so callers outside a transaction can still
// use a default connection.

export type DagStore<Tx = void> = {
    withTransaction<T>(fn: (...tx: Tx extends void ? [] : [tx: Tx]) => Promise<T>): Promise<T>;
    append(entry: Entry, ...tx: Tx extends void ? [] : [tx: Tx]): Promise<void>;
    loadEntry(h: Hash, ...tx: Tx extends void ? [] : [tx: Tx] | []): Promise<Entry|undefined>;
    loadHeader(h: Hash, ...tx: Tx extends void ? [] : [tx: Tx] | []): Promise<Header|undefined>;

    getFrontier(...tx: Tx extends void ? [] : [tx: Tx] | []): Promise<Position>;
    loadAllEntries(...tx: Tx extends void ? [] : [tx: Tx] | []): AsyncIterable<Entry>; // in topo order
};
