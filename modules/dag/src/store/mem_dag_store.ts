import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { DagGrowthListener, DagStore, TxResult } from "./dag_store.js";
import { Entry, Header, Position } from "../dag_defs.js";


// In-memory implementation of the DAG store

export class MemDagStorage implements DagStore {
    private entries = new Map<B64Hash, Entry>();
    private headers = new Map<B64Hash, Header>();
    private frontier = new Set<B64Hash>();
    private roots = new Set<B64Hash>();
    private listeners = new Set<DagGrowthListener>();

    async withTransaction<T extends TxResult>(fn: () => Promise<T>): Promise<T> {
        const result = await fn();
        if (result.fireListeners) this.fireListeners();
        return result;
    }

    async append(entry: Entry): Promise<void> {
        const { hash, header } = entry;
        this.entries.set(hash, entry);
        this.headers.set(hash, header);

        for (const prevHash of json.fromSet(header.prevEntryHashes)) {
            this.frontier.delete(prevHash);
        }

        this.frontier.add(hash);
    }

    async loadEntry(h: B64Hash): Promise<Readonly<Entry> | undefined> {
        return this.entries.get(h);
    }

    async loadHeader(h: B64Hash): Promise<Readonly<Header> | undefined> {
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

    addListener(listener: DagGrowthListener): void {
        this.listeners.add(listener);
    }

    removeListener(listener: DagGrowthListener): void {
        this.listeners.delete(listener);
    }

    private fireListeners(): void {
        for (const cb of this.listeners) {
            try { cb(); } catch (_e) { /* keep firing even if a listener throws */ }
        }
    }
}