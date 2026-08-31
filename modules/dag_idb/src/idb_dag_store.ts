import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { Entry, Header, Position } from "@hyper-hyper-space/hhs3_dag";
import { DagGrowth, DagGrowthListener, DagStore, TxResult } from "@hyper-hyper-space/hhs3_dag/dist/store/dag_store.js";

import { DAGS, ENTRIES, FRONTIER } from "./idb_schema.js";
import { IdbEnv, IdbReader, IdbTx } from "./idb_env.js";

// IndexedDB-backed DagStore. Abstract in the external-observer dimension: a
// subclass supplies a strategy for detecting growth from other browsing contexts
// (see BroadcastIdbDagStore). Mirrors dag_sql/src/sql_dag_store.ts.
export abstract class IdbDagStore implements DagStore<IdbTx> {

    protected env: IdbEnv;
    protected dagId: number;

    private listeners = new Set<DagGrowthListener>();
    private externalHandle: unknown = undefined;

    // Cursor (max seq) for reading entries introduced by external writers.
    private externalCursor: number | undefined = undefined;
    private externalCursorInit: Promise<void> | undefined = undefined;
    private externalRunning = false;
    // Bumped on every arm and disarm so stale in-flight async work (cursor init,
    // growth handling) can detect it no longer owns the monitor and bail out.
    private externalEpoch = 0;
    // Set when a notification arrives while the handler is already running, so
    // the running pass rescans instead of dropping the notification.
    private rescanRequested = false;

    constructor(env: IdbEnv, dagId: number) {
        this.env = env;
        this.dagId = dagId;
    }

    async withTransaction<T extends TxResult>(fn: (tx: IdbTx) => Promise<T>): Promise<T> {
        const { result, committed } = await this.env.withUnitOfWork<T>(this.dagId, fn);
        if (result.fireListeners) {
            if (committed) this.onCommitted();
            this.fireListeners({ entries: result.entries ?? [], frontier: await this.getFrontier() });
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
            const epoch = ++this.externalEpoch;
            this.externalCursor = undefined;
            this.rescanRequested = false;
            this.externalRunning = false;
            this.externalCursorInit = this.initExternalCursor(epoch);
            this.externalHandle = this.startExternalObserver(() => { void this.handleExternalGrowth(epoch); });
        }
    }

    removeListener(listener: DagGrowthListener): void {
        this.listeners.delete(listener);
        if (this.listeners.size === 0 && this.externalHandle !== undefined) {
            // Bump the epoch first so any in-flight init/handler bails out.
            ++this.externalEpoch;
            this.stopExternalObserver(this.externalHandle);
            this.externalHandle = undefined;
            this.externalCursor = undefined;
            this.externalCursorInit = undefined;
            this.rescanRequested = false;
            this.externalRunning = false;
        }
    }

    protected fireListeners(growth: DagGrowth): void {
        for (const cb of this.listeners) {
            try { cb(growth); } catch (_e) { /* keep firing even if a listener throws */ }
        }
    }

    private async initExternalCursor(epoch: number): Promise<void> {
        const rec = await this.env.get(DAGS, this.dagId);
        // Only baseline if we still own the monitor: a disarm/re-arm during the
        // read would otherwise let this stale value defeat re-baselining.
        if (epoch === this.externalEpoch && this.externalCursor === undefined) {
            this.externalCursor = rec !== undefined ? (rec.nextSeq as number) - 1 : -1;
        }
    }

    // Called by the external observer when it detects a possible change. Reads
    // the entries appended (by seq) beyond the cursor and fires listeners.
    // Single-flight per store instance: a notification arriving mid-pass sets
    // rescanRequested so the running pass loops again rather than dropping it
    // (preserving at-least-once).
    private async handleExternalGrowth(epoch: number): Promise<void> {
        if (epoch !== this.externalEpoch) return;
        if (this.externalRunning) { this.rescanRequested = true; return; }
        this.externalRunning = true;
        try {
            do {
                this.rescanRequested = false;
                await this.externalCursorInit;
                if (epoch !== this.externalEpoch) return;
                const cursor = this.externalCursor ?? -1;
                const recs = await this.env.getAllByPrefix(ENTRIES, 'by_seq', [this.dagId]);
                if (epoch !== this.externalEpoch) return;
                const fresh = recs.filter((r) => (r.seq as number) > cursor);
                if (fresh.length > 0) {
                    let maxSeq = cursor;
                    const entries: Entry[] = [];
                    for (const rec of fresh) {
                        maxSeq = Math.max(maxSeq, rec.seq as number);
                        entries.push({
                            hash: rec.hash as B64Hash,
                            payload: rec.payload,
                            meta: rec.meta,
                            header: rec.header,
                        });
                    }
                    this.externalCursor = maxSeq;
                    this.fireListeners({ entries, frontier: await this.getFrontier() });
                }
            } while (this.rescanRequested && epoch === this.externalEpoch);
        } catch (_e) {
            // ignore transient errors; the next notification will retry
        } finally {
            // Only release if we still own the monitor, so a re-arm's handler is
            // never clobbered by a stale handler completing.
            if (epoch === this.externalEpoch) this.externalRunning = false;
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
