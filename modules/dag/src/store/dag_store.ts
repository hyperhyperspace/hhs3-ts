import { Hash } from "@hyper-hyper-space/hhs3_crypto";
import { Entry, Header, Position } from "dag_defs";

// Store all the entries and their headers for a DAG.

export type DagStore = {
    append(entry: Entry): Promise<void>;
    loadEntry(h: Hash): Promise<Entry|undefined>;
    loadHeader(h: Hash): Promise<Header|undefined>;

    getFrontier(): Promise<Position>;
    loadAllEntries(): AsyncIterable<Entry>; // in topo order
};