import { Hash } from "@hyper-hyper-space/hhs3_crypto";
import { DagStore } from "./dag_store";
import { Entry, Header, Position } from "dag_defs";
import { json } from "@hyper-hyper-space/hhs3_json";

// In-memory implementation of the DAG store

export class MemDagStorage implements DagStore {
    private entries = new Map<Hash, Entry>();
    private headers = new Map<Hash, Header>();
    private frontier = new Set<Hash>();
    private roots = new Set<Hash>();


    async append(entry: Entry): Promise<void> {
        const { hash, header } = entry;
        this.entries.set(hash, entry);
        this.headers.set(hash, header);

        for (const prevHash of json.fromSet(header.prevEntryHashes)) {
            this.frontier.delete(prevHash);
        }

        this.frontier.add(hash);
    }

    async loadEntry(h: Hash): Promise<Readonly<Entry> | undefined> {
        return this.entries.get(h);
    }

    async loadHeader(h: Hash): Promise<Readonly<Header> | undefined> {
        return this.headers.get(h);
    }

    async getFrontier(): Promise<Position> {
        return new Set([...this.frontier]);
    }

    loadAllEntries(): AsyncIterable<Entry> {

        const entries = this.entries.values();

        return {
            async *[Symbol.asyncIterator]() {
                for (const e of entries) {
                    yield e;
                }
            } 
        }
        
    }
}