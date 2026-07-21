import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { Entry, Header, Position } from "@hyper-hyper-space/hhs3_dag";
import { DagGrowthListener, DagStore, TxResult } from "@hyper-hyper-space/hhs3_dag/dist/store/dag_store.js";

import { ENTRIES, FRONTIER } from "./idb_schema.js";
import { IdbEnv, IdbReader, IdbTx } from "./idb_env.js";

// IndexedDB-backed DagStore. Abstract in the external-observer dimension: a
// subclass supplies a strategy for detecting growth from other browsing contexts
// (see BroadcastIdbDagStore). Mirrors dag_sql/src/sql_dag_store.ts.
export abstract class IdbDagStore implements DagStore<IdbTx> {

    protected env: IdbEnv;
    protected dagId: number;

    private listeners = new Set<DagGrowthListener>();
    private externalHandle: unknown = undefined;

    constructor(env: IdbEnv, dagId: number) {
        this.env = env;
        this.dagId = dagId;
    }

    async withTransaction<T extends TxResult>(fn: (tx: IdbTx) => Promise<T>): Promise<T> {
        const { result, committed } = await this.env.withUnitOfWork<T>(this.dagId, fn);
        if (result.fireListeners) {
            if (committed) this.onCommitted();
            this.fireListeners();
        }
        return result;
    }

    async append(entry: Entry, tx: IdbTx): Promise<void> {
        const { hash, header, payload, meta } = entry;

        tx.putRecord(
            ENTRIES,
            { dagId: this.dagId, hash, payload, meta, header, seq: -1 },
            [this.dagId, hash],
            { field: 'seq', counter: 'seq' }
        );

        for (const prevHash of json.fromSet(header.prevEntryHashes)) {
            tx.deleteRecord(FRONTIER, [this.dagId, prevHash]);
        }

        tx.putRecord(FRONTIER, { dagId: this.dagId, hash }, [this.dagId, hash]);
    }

    async loadEntry(h: B64Hash, ...tx: [tx: IdbTx] | []): Promise<Entry | undefined> {
        const reader: IdbReader = tx[0] ?? this.env;
        const rec = await reader.get(ENTRIES, [this.dagId, h]);
        if (rec === undefined) return undefined;
        return {
            hash: rec.hash as B64Hash,
            payload: rec.payload,
            meta: rec.meta,
            header: rec.header,
        };
    }

    async loadHeader(h: B64Hash, ...tx: [tx: IdbTx] | []): Promise<Header | undefined> {
        const reader: IdbReader = tx[0] ?? this.env;
        const rec = await reader.get(ENTRIES, [this.dagId, h]);
        if (rec === undefined) return undefined;
        return rec.header as Header;
    }

    async getFrontier(...tx: [tx: IdbTx] | []): Promise<Position> {
        const reader: IdbReader = tx[0] ?? this.env;
        const recs = await reader.getAllByPrefix(FRONTIER, null, [this.dagId]);
        return new Set(recs.map(r => r.hash as B64Hash));
    }

    loadAllEntries(...tx: [tx: IdbTx] | []): AsyncIterable<Entry> {
        const reader: IdbReader = tx[0] ?? this.env;
        const dagId = this.dagId;

        return {
            async *[Symbol.asyncIterator]() {
                const recs = await reader.getAllByPrefix(ENTRIES, 'by_seq', [dagId]);
                for (const rec of recs) {
                    yield {
                        hash: rec.hash as B64Hash,
                        payload: rec.payload,
                        meta: rec.meta,
                        header: rec.header,
                    };
                }
            }
        };
    }

    addListener(listener: DagGrowthListener): void {
        const wasEmpty = this.listeners.size === 0;
        this.listeners.add(listener);
        if (wasEmpty && this.listeners.size === 1) {
            this.externalHandle = this.startExternalObserver(() => this.fireListeners());
        }
    }

    removeListener(listener: DagGrowthListener): void {
        this.listeners.delete(listener);
        if (this.listeners.size === 0 && this.externalHandle !== undefined) {
            this.stopExternalObserver(this.externalHandle);
            this.externalHandle = undefined;
        }
    }

    protected fireListeners(): void {
        for (const cb of this.listeners) {
            try { cb(); } catch (_e) { /* keep firing even if a listener throws */ }
        }
    }

    // Subclasses implement an external observation strategy (e.g. BroadcastChannel)
    // started lazily when the first listener subscribes and stopped when the last
    // unsubscribes. `notify` may be called any time a change is possible; it is
    // safe to over-notify per the at-least-once DagStore contract.
    protected abstract startExternalObserver(notify: () => void): unknown;
    protected abstract stopExternalObserver(handle: unknown): void;

    // Called after a local unit of work commits (and requested listener firing),
    // so subclasses can notify peers in other browsing contexts. Default no-op.
    protected onCommitted(): void { /* no-op */ }
}
