import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { Entry, Header, Position } from "@hyper-hyper-space/hhs3_dag";
import { DagGrowth, DagGrowthListener, DagStore, TxResult } from "@hyper-hyper-space/hhs3_dag/dist/store/dag_store.js";

import { SqlConnection } from "./sql_connection.js";

export abstract class SqlDagStore implements DagStore<SqlConnection> {

    protected conn: SqlConnection;
    protected dagId: number;

    private listeners = new Set<DagGrowthListener>();
    private externalHandle: unknown = undefined;

    // Cursor (max rowid) for reading entries introduced by external writers.
    // Initialized to the current head when the observer is armed so the first
    // external notification does not re-deliver pre-existing history.
    private externalCursor: number | undefined = undefined;
    private externalCursorInit: Promise<void> | undefined = undefined;
    private externalRunning = false;
    // Bumped on every arm and disarm so stale in-flight async work (cursor init,
    // growth handling) can detect it no longer owns the monitor and bail out.
    private externalEpoch = 0;
    // Set when a notification arrives while the handler is already running, so
    // the running pass rescans instead of dropping the notification.
    private rescanRequested = false;

    constructor(conn: SqlConnection, dagId: number) {
        this.conn = conn;
        this.dagId = dagId;
    }

    async withTransaction<T extends TxResult>(fn: (tx: SqlConnection) => Promise<T>): Promise<T> {
        const result = await this.conn.transaction(fn);
        if (result.fireListeners) {
            this.fireListeners({ entries: result.entries ?? [], frontier: await this.getFrontier() });
        }
        return result;
    }

    async append(entry: Entry, tx: SqlConnection): Promise<void> {
        const c = tx;
        const { hash, header, payload, meta } = entry;

        await c.execute(
            `INSERT OR IGNORE INTO entries (dag_id, hash, payload, meta, header) VALUES (?, ?, ?, ?, ?)`,
            [this.dagId, hash, JSON.stringify(payload), JSON.stringify(meta), JSON.stringify(header)]
        );

        for (const prevHash of json.fromSet(header.prevEntryHashes)) {
            await c.execute(
                `DELETE FROM frontier WHERE dag_id = ? AND hash = ?`,
                [this.dagId, prevHash]
            );
        }

        await c.execute(
            `INSERT OR IGNORE INTO frontier (dag_id, hash) VALUES (?, ?)`,
            [this.dagId, hash]
        );
    }

    async loadEntry(h: B64Hash, ...tx: [tx: SqlConnection] | []): Promise<Entry | undefined> {
        const c = tx[0] ?? this.conn;
        const rows = await c.query(
            `SELECT hash, payload, meta, header FROM entries WHERE dag_id = ? AND hash = ?`,
            [this.dagId, h]
        );

        if (rows.length === 0) return undefined;

        const row = rows[0];
        return {
            hash: row.hash as B64Hash,
            payload: JSON.parse(row.payload as string),
            meta: JSON.parse(row.meta as string),
            header: JSON.parse(row.header as string),
        };
    }

    async loadHeader(h: B64Hash, ...tx: [tx: SqlConnection] | []): Promise<Header | undefined> {
        const c = tx[0] ?? this.conn;
        const rows = await c.query(
            `SELECT header FROM entries WHERE dag_id = ? AND hash = ?`,
            [this.dagId, h]
        );

        if (rows.length === 0) return undefined;

        return JSON.parse(rows[0].header as string);
    }

    async getFrontier(...tx: [tx: SqlConnection] | []): Promise<Position> {
        const c = tx[0] ?? this.conn;
        const rows = await c.query(
            `SELECT hash FROM frontier WHERE dag_id = ?`,
            [this.dagId]
        );
        return new Set(rows.map(r => r.hash as B64Hash));
    }

    loadAllEntries(...tx: [tx: SqlConnection] | []): AsyncIterable<Entry> {
        const c = tx[0] ?? this.conn;
        const dagId = this.dagId;

        return {
            async *[Symbol.asyncIterator]() {
                const rows = await c.query(
                    `SELECT hash, payload, meta, header FROM entries WHERE dag_id = ? ORDER BY rowid`,
                    [dagId]
                );
                for (const row of rows) {
                    yield {
                        hash: row.hash as B64Hash,
                        payload: JSON.parse(row.payload as string),
                        meta: JSON.parse(row.meta as string),
                        header: JSON.parse(row.header as string),
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

    private fireListeners(growth: DagGrowth): void {
        for (const cb of this.listeners) {
            try { cb(growth); } catch (_e) { /* keep firing even if a listener throws */ }
        }
    }

    private async initExternalCursor(epoch: number): Promise<void> {
        const rows = await this.conn.query(
            `SELECT COALESCE(MAX(rowid), 0) AS m FROM entries WHERE dag_id = ?`,
            [this.dagId]
        );
        // Only baseline if we still own the monitor: a disarm/re-arm during the
        // query would otherwise let this stale value defeat re-baselining.
        if (epoch === this.externalEpoch && this.externalCursor === undefined) {
            this.externalCursor = Number(rows[0]?.m ?? 0);
        }
    }

    // Called by the external observer when it detects a possible change. Reads
    // the entries appended beyond the cursor (a storage read, not a DAG walk)
    // and fires listeners. Safe to over-invoke: a spurious call reads no new
    // rows and fires nothing. Single-flight per store instance: a notification
    // arriving mid-pass sets rescanRequested so the running pass loops again
    // rather than dropping it (preserving at-least-once).
    private async handleExternalGrowth(epoch: number): Promise<void> {
        if (epoch !== this.externalEpoch) return;
        if (this.externalRunning) { this.rescanRequested = true; return; }
        this.externalRunning = true;
        try {
            do {
                this.rescanRequested = false;
                await this.externalCursorInit;
                if (epoch !== this.externalEpoch) return;
                const cursor = this.externalCursor ?? 0;
                const rows = await this.conn.query(
                    `SELECT rowid AS rid, hash, payload, meta, header FROM entries WHERE dag_id = ? AND rowid > ? ORDER BY rowid`,
                    [this.dagId, cursor]
                );
                if (epoch !== this.externalEpoch) return;
                if (rows.length > 0) {
                    let maxRid = cursor;
                    const entries: Entry[] = [];
                    for (const row of rows) {
                        maxRid = Math.max(maxRid, Number(row.rid));
                        entries.push({
                            hash: row.hash as B64Hash,
                            payload: JSON.parse(row.payload as string),
                            meta: JSON.parse(row.meta as string),
                            header: JSON.parse(row.header as string),
                        });
                    }
                    this.externalCursor = maxRid;
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

    // Subclasses implement these to plug in an external observation strategy
    // (e.g. polling, fs.watch on the WAL file, BroadcastChannel...). They are
    // started lazily when the first listener subscribes and stopped when the
    // last listener unsubscribes. `notify` should be called whenever the
    // subclass detects a potential change in the underlying store; it is safe
    // to over-notify, per the at-least-once contract on DagStore. The base
    // reads the new entries beyond its cursor and delivers them.
    protected abstract startExternalObserver(notify: () => void): unknown;
    protected abstract stopExternalObserver(handle: unknown): void;
}
