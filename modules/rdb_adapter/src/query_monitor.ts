// Single-table monitors over a queryable, reactive target. Local commits
// re-run every monitor of the named tables. Remote apply commits use the
// predicate, the cached ids, and the keyset window.

import type { json } from "@hyper-hyper-space/hhs3_json";
import type { ColumnType } from "@hyper-hyper-space/hhs3_rdb";

import { ColumnSetRef, QuerySourceImpl, TableQuery, type QuerySnapshot, type QuerySubscription } from "./query_builder.js";
import { compareSortTuples, evalWhere, normalizeOrder, tupleOf } from "./query_eval.js";
import type {
    BoundWhereExpr, CursorBound, OpEvent, OpEventQuery, OrderBySpec, ProjectedRow, QueryableTarget,
    ReactiveChangeTarget, SingleTableQueryPlan, StoredOpEvent, TargetCommitEvent, TargetRowChange,
} from "./types.js";

export type OpEventLog = {
    drainOpEvents(opts?: OpEventQuery | number): Promise<StoredOpEvent[]>;
};

type Store = QueryableTarget & ReactiveChangeTarget & Partial<OpEventLog>;

type RowView = { [column: string]: json.Literal | null | undefined };

export class QueryEngine {
    readonly source = new QuerySourceImpl(this);
    private readonly monitors = new Map<string, Monitor>();
    private readonly sets = new Set<QuerySet>();
    private flushQueued = false;
    private readonly onCommit = (commit: TargetCommitEvent): void => { this.receive(commit); };

    constructor(private readonly target: Store, private readonly idColumn = 'id') {
        target.addCommitListener(this.onCommit);
    }

    selectFrom(table: string): TableQuery {
        return new TableQuery(this.source, table);
    }

    monitor(query: TableQuery): QuerySubscription<ProjectedRow[]>;
    monitor(query: { [name: string]: TableQuery }): QuerySubscription<{ [name: string]: ProjectedRow[] }>;
    monitor(query: TableQuery | { [name: string]: TableQuery }): QuerySubscription<ProjectedRow[] | { [name: string]: ProjectedRow[] }> {
        if (query instanceof TableQuery) return this.open(query).subscription();
        const set = new QuerySet(this, query);
        this.sets.add(set);
        return set.subscription();
    }

    close(): void {
        this.target.removeCommitListener(this.onCommit);
    }

    open(query: TableQuery): Monitor {
        const key = query.fingerprint();
        let monitor = this.monitors.get(key);
        if (monitor === undefined) {
            monitor = new Monitor(this, query, this.target, this.idColumn);
            this.monitors.set(key, monitor);
            void monitor.start();
        }
        return monitor;
    }

    hold(monitor: Monitor): void {
        monitor.retain();
        this.monitors.set(monitor.key, monitor);
    }

    release(monitor: Monitor): void {
        monitor.release();
        if (monitor.refs === 0) this.monitors.delete(monitor.key);
    }

    keepSet(set: QuerySet): void {
        this.sets.add(set);
    }

    dropSet(set: QuerySet): void {
        this.sets.delete(set);
    }

    schedule(monitor: Monitor): void {
        monitor.dirty = true;
        if (this.flushQueued) return;
        this.flushQueued = true;
        queueMicrotask(() => { void this.flush(); });
    }

    private async flush(): Promise<void> {
        this.flushQueued = false;
        const order = topo([...this.monitors.values()]);
        for (const monitor of order) {
            if (!monitor.dirty) continue;
            await this.settle(monitor);
        }
        for (const set of this.sets) set.publish();
    }

    async settle(monitor: Monitor): Promise<void> {
        const before = this.exportedKeySnapshot(monitor);
        await monitor.run();
        if (this.exportedKeySnapshot(monitor) === before) return;
        for (const other of this.monitors.values()) {
            if (other.query.dependencies().some((ref) => ref.query.fingerprint() === monitor.key)) this.schedule(other);
        }
    }

    notifySets(): void {
        for (const set of this.sets) set.publish();
    }

    private receive(commit: TargetCommitEvent): void {
        for (const monitor of this.monitors.values()) {
            if (monitor.consider(commit)) this.schedule(monitor);
        }
    }

    private exportedKeySnapshot(monitor: Monitor): string {
        return JSON.stringify(this.exportedColumns(monitor.query).map((column) => monitor.keys(column)));
    }

    exportedColumns(query: TableQuery): string[] {
        const columns = new Set<string>();
        for (const monitor of this.monitors.values()) {
            for (const ref of monitor.query.dependencies()) {
                if (ref.query.fingerprint() === query.fingerprint()) columns.add(ref.column);
            }
        }
        return [...columns];
    }
}

class Monitor {
    readonly key: string;
    refs = 0;
    dirty = true;
    private highWater = 0;
    private armed = false;
    private rows: ProjectedRow[] = [];
    private fullRows: ProjectedRow[] = [];
    private rowById = new Map<number, ProjectedRow>();
    private events: OpEvent[] = [];
    private pendingConcurrency: OpEvent[] = [];
    private loading = true;
    private error: unknown;
    private snapshot: QuerySnapshot<ProjectedRow[]>;
    private readonly listeners = new Set<() => void>();
    private ready: (() => void) | undefined;
    private readonly readyPromise: Promise<void>;
    cachedRowIds = new Set<number>();
    private cursorTuple: { [column: string]: json.Literal | null } | undefined;
    private lastRowTuple: { [column: string]: json.Literal | null } | undefined;
    private keySets = new Map<ColumnSetRef, Array<json.Literal | null>>();

    constructor(
        private readonly engine: QueryEngine,
        readonly query: TableQuery,
        private readonly target: Store,
        private readonly idColumn: string,
    ) {
        this.key = query.fingerprint();
        this.snapshot = { data: [], events: [], loading: true };
        this.readyPromise = new Promise<void>((resolve) => { this.ready = resolve; });
    }

    retain(): void { this.refs++; }

    release(): void { this.refs = Math.max(0, this.refs - 1); }

    subscription(): QuerySubscription<ProjectedRow[]> {
        const monitor = this;
        return {
            getSnapshot: () => monitor.snapshot,
            subscribe(listener: () => void) {
                monitor.engine.hold(monitor);
                monitor.listeners.add(listener);
                return () => {
                    monitor.listeners.delete(listener);
                    monitor.engine.release(monitor);
                };
            },
            whenReady: async () => {
                await monitor.readyPromise;
                return monitor.snapshot;
            },
        };
    }

    async start(): Promise<void> {
        if (this.target.drainOpEvents !== undefined) {
            const [top] = await this.target.drainOpEvents({ order: 'desc', limit: 1 });
            this.highWater = top?.id ?? 0;
        }
        this.armed = true;
        await this.engine.settle(this);
    }

    consider(commit: TargetCommitEvent): boolean {
        if (!this.armed) return false;
        if (commit.source === 'reconnect') return true;
        if (commit.source === 'local') return commit.tables.includes(this.query.table);
        if (commit.source === 'settle') {
            this.takeEvents(commit.events);
            const dirty = commit.revertedTables.includes(this.query.table);
            if (!dirty) this.pendingConcurrency = [];
            return dirty;
        }
        if (commit.source !== 'apply') return false;
        this.takeEvents(commit.events);
        if (commit.schemaTables.includes(this.query.table)) return true;
        const where = this.boundWhere();
        const orderBy = this.order();
        for (const change of commit.changes) {
            if (change.table !== this.query.table) continue;
            if (this.changeDirties(change, where, orderBy)) return true;
        }
        this.pendingConcurrency = [];
        return false;
    }

    async run(): Promise<void> {
        this.dirty = false;
        try {
            const plan = this.plan();
            const fetched = await this.target.executeQuery(plan);
            const cursor = this.query.cursorBound(this.cursorTuple);
            if (cursor !== undefined) await this.refreshCursor(cursor, this.order());
            const delivered = strip(fetched, this.query, this.idColumn).map((row) => {
                const prev = this.rowById.get(row.id);
                return prev !== undefined && JSON.stringify(prev) === JSON.stringify(row) ? prev : row;
            });
            this.fullRows = fetched;
            this.rows = delivered;
            this.rowById = new Map(delivered.map((row) => [row.id, row]));
            this.cachedRowIds = new Set(delivered.map((row) => row.id));
            for (const event of this.pendingConcurrency) {
                if (event.localId !== undefined && this.cachedRowIds.has(event.localId)) this.events.push(event);
            }
            this.pendingConcurrency = [];
            this.lastRowTuple = delivered.length > 0 && this.queryLimit() !== undefined && delivered.length === this.queryLimit()
                ? tupleOf(fetched[fetched.length - 1]!, this.order())
                : undefined;
            const same = sameRows(this.snapshot.data, delivered) && this.events.length === 0 && !this.loading && this.error === undefined;
            if (same) return;
            this.loading = false;
            this.error = undefined;
            this.publish();
        } catch (error) {
            this.pendingConcurrency = [];
            this.loading = false;
            this.error = error;
            this.rows = [];
            this.publish();
        }
    }

    keys(column: string): Array<json.Literal | null> {
        const values = new Set<string>();
        const out: Array<json.Literal | null> = [];
        for (const row of this.fullRows) {
            const value = row[column] ?? null;
            const key = JSON.stringify(value);
            if (values.has(key)) continue;
            values.add(key);
            out.push(value);
        }
        out.sort((a, b) => JSON.stringify(a) < JSON.stringify(b) ? -1 : 1);
        return out;
    }

    private plan(): SingleTableQueryPlan {
        const extra = this.engine.exportedColumns(this.query);
        const base = this.query.plan(this.incomingKeys(), this.idColumn);
        if (base.select !== undefined) base.select = [...new Set([...base.select, ...extra, ...this.order().map((item) => item.column)])];
        const cursor = this.query.cursorBound(this.cursorTuple);
        if (cursor !== undefined) base.cursor = cursor;
        return base;
    }

    private incomingKeys(): Map<ColumnSetRef, Array<json.Literal | null>> {
        const sets = new Map<ColumnSetRef, Array<json.Literal | null>>();
        for (const ref of this.query.dependencies()) {
            // open() does not retain, so this read must not release. Releasing
            // would drop a monitor another subscription still holds.
            const upstream = this.engine.open(ref.query);
            sets.set(ref, upstream.keys(ref.column));
        }
        this.keySets = sets;
        return sets;
    }

    private boundWhere(): BoundWhereExpr | undefined {
        return this.query.whereExpr(this.keySets);
    }

    private order(): OrderBySpec[] {
        const plan = this.query.plan(this.keySets, this.idColumn);
        return normalizeOrder(plan.orderBy, this.idColumn, plan.limit !== undefined || plan.cursor !== undefined);
    }

    private queryLimit(): number | undefined {
        return this.query.plan(this.keySets, this.idColumn).limit;
    }

    private changeDirties(change: TargetRowChange, where: BoundWhereExpr | undefined, orderBy: OrderBySpec[]): boolean {
        if (change.kind !== 'delete' && change.localId === this.query.cursorBound()?.id) return true;
        if (change.kind === 'delete' && change.localId === this.query.cursorBound()?.id) return this.cursorTuple === undefined;
        if (change.kind === 'delete') return this.cachedRowIds.has(change.localId);
        const row: RowView = { ...change.values, id: change.localId };
        if (this.cachedRowIds.has(change.localId)) return true;
        if (!evalWhere(where, row, change.localId)) return false;
        const cursor = this.query.cursorBound(this.cursorTuple);
        if (cursor !== undefined && this.cursorTuple !== undefined && !rowOnCursor(row, this.cursorTuple, orderBy, cursor.side)) {
            return false;
        }
        if (this.lastRowTuple !== undefined && this.queryLimit() !== undefined && this.rows.length === this.queryLimit()
            && pastEnd(row, this.lastRowTuple, orderBy)) {
            return false;
        }
        return true;
    }

    private takeEvents(events: StoredOpEvent[]): void {
        for (const stored of events) {
            if (stored.id <= this.highWater) continue;
            this.highWater = stored.id;
            const event = stored.event;
            if (event.localId === undefined) continue;
            if (event.origin === 'ingestion') {
                if (this.cachedRowIds.has(event.localId)) this.events.push(event);
                continue;
            }
            if (event.origin !== 'concurrency') continue;
            if (this.cachedRowIds.has(event.localId)) this.events.push(event);
            else this.pendingConcurrency.push(event);
        }
    }

    private async refreshCursor(cursor: CursorBound, orderBy: OrderBySpec[]): Promise<void> {
        const found = await this.target.executeQuery({
            table: this.query.table,
            where: { kind: 'cmp', op: '=', left: { kind: 'col', name: this.idColumn }, right: { kind: 'lit', value: cursor.id } },
            select: orderBy.map((item) => item.column),
        });
        const row = found[0];
        if (row !== undefined) this.cursorTuple = tupleOf(row, orderBy);
    }

    private publish(): void {
        this.snapshot = { data: this.rows, events: this.events, loading: false, ...(this.error !== undefined ? { error: this.error } : {}) };
        this.events = [];
        this.ready?.();
        this.ready = undefined;
        for (const listener of this.listeners) listener();
        this.engine.notifySets();
    }
}

class QuerySet {
    private readonly monitors: { [name: string]: Monitor };
    private snapshot: QuerySnapshot<{ [name: string]: ProjectedRow[] }>;
    private readonly listeners = new Set<() => void>();

    constructor(private readonly engine: QueryEngine, queries: { [name: string]: TableQuery }) {
        this.monitors = {};
        const data: { [name: string]: ProjectedRow[] } = {};
        for (const [name, query] of Object.entries(queries)) {
            this.monitors[name] = engine.open(query);
            data[name] = [];
        }
        this.snapshot = { data, events: [], loading: true };
    }

    subscription(): QuerySubscription<{ [name: string]: ProjectedRow[] }> {
        const set = this;
        return {
            getSnapshot: () => set.snapshot,
            subscribe(listener: () => void) {
                set.engine.keepSet(set);
                for (const monitor of Object.values(set.monitors)) set.engine.hold(monitor);
                set.listeners.add(listener);
                return () => {
                    set.listeners.delete(listener);
                    for (const monitor of Object.values(set.monitors)) set.engine.release(monitor);
                    set.engine.dropSet(set);
                };
            },
            whenReady: () => Promise.all(Object.values(set.monitors).map((monitor) => monitor.subscription().whenReady())).then(() => set.current()),
        };
    }

    publish(): void {
        const next = this.current();
        const prev = this.snapshot;
        const names = Object.keys(next.data);
        const sameData = names.length === Object.keys(prev.data).length
            && names.every((name) => next.data[name] === prev.data[name]);
        if (sameData && next.loading === prev.loading && next.error === prev.error) return;
        this.snapshot = next;
        for (const listener of this.listeners) listener();
    }

    private current(): QuerySnapshot<{ [name: string]: ProjectedRow[] }> {
        const data: { [name: string]: ProjectedRow[] } = {};
        let loading = false;
        let error: unknown;
        for (const [name, monitor] of Object.entries(this.monitors)) {
            const snap = monitor.subscription().getSnapshot();
            data[name] = snap.data;
            loading = loading || snap.loading;
            if (snap.error !== undefined) error = snap.error;
        }
        return { data, events: [], loading, ...(error !== undefined ? { error } : {}) };
    }
}

function strip(rows: ProjectedRow[], query: TableQuery, idColumn: string): ProjectedRow[] {
    const plan = query.plan(new Map(), idColumn);
    if (plan.select === undefined) return rows;
    const requested = new Set(queryFingerprintSelect(query));
    requested.add('id');
    return rows.map((row) => {
        const out: ProjectedRow = { id: row.id };
        for (const column of requested) if (column in row) out[column] = row[column] ?? null;
        return out;
    });
}

function queryFingerprintSelect(query: TableQuery): string[] {
    const parsed = JSON.parse(query.fingerprint()) as { select: string[] | null };
    return parsed.select ?? [];
}

function sameRows(prev: ProjectedRow[], next: ProjectedRow[]): boolean {
    if (prev.length !== next.length) return false;
    for (let i = 0; i < prev.length; i++) {
        if (prev[i]!.id !== next[i]!.id) return false;
        if (JSON.stringify(prev[i]) !== JSON.stringify(next[i])) return false;
    }
    return true;
}

function rowOnCursor(row: RowView, tuple: RowView, orderBy: OrderBySpec[], side: 'before' | 'after'): boolean {
    for (const { column } of orderBy) {
        const order = compareSortTuples({ [column]: row[column] }, { [column]: tuple[column] }, [{ column, direction: 'asc' }]);
        if (order === 0) continue;
        return side === 'before' ? order < 0 : order > 0;
    }
    return false;
}

function pastEnd(row: RowView, last: RowView, orderBy: OrderBySpec[]): boolean {
    return compareSortTuples(row, last, orderBy) > 0;
}

function topo(monitors: Monitor[]): Monitor[] {
    const byKey = new Map(monitors.map((monitor) => [monitor.key, monitor]));
    const seen = new Set<string>();
    const out: Monitor[] = [];
    const visit = (monitor: Monitor): void => {
        if (seen.has(monitor.key)) return;
        seen.add(monitor.key);
        for (const ref of monitor.query.dependencies()) {
            const upstream = byKey.get(ref.query.fingerprint());
            if (upstream !== undefined) visit(upstream);
        }
        out.push(monitor);
    };
    for (const monitor of monitors) visit(monitor);
    return out;
}

export function __rowTypes(type: ColumnType | undefined): ColumnType | undefined {
    return type;
}
