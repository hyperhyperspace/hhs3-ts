// Single-table monitors over a queryable, reactive target. Local commits
// re-run every monitor of the named tables. Remote apply commits use the
// predicate, the cached ids, and the keyset window. An op-event reaches a
// monitor when a row it names, by table and local id, is in the result just
// before its commit or just after.

import type { json } from "@hyper-hyper-space/hhs3_json";
import type { ColumnType } from "@hyper-hyper-space/hhs3_rdb";

import { ColumnSetRef, QuerySourceImpl, TableQuery, type QuerySnapshot, type QuerySubscription } from "./query_builder.js";
import { compareSortTuples, evalWhere, normalizeOrder, tupleOf } from "./query_eval.js";
import {
    opEventRows,
    type BoundWhereExpr, type CursorBound, type OpEvent, type OpEventQuery, type OrderBySpec, type ProjectedRow,
    type QueryableTarget, type ReactiveChangeTarget, type SingleTableQueryPlan, type StoredOpEvent,
    type TargetCommitEvent, type TargetRowChange,
} from "./types.js";

export type OpEventLog = {
    drainOpEvents(opts?: OpEventQuery | number): Promise<StoredOpEvent[]>;
};

type Store = QueryableTarget & ReactiveChangeTarget & Partial<OpEventLog>;

type RowView = { [column: string]: json.Literal | null | undefined };

// What a query shows before its first result. One object, for a query with no
// monitor and for a monitor that has not run yet: a reader that compares
// snapshots by identity, as React does, sees no change when a monitor is made.
const LOADING: QuerySnapshot<ProjectedRow[]> = Object.freeze({
    data: Object.freeze([]) as unknown as ProjectedRow[],
    events: Object.freeze([]) as unknown as OpEvent[],
    loading: true,
});

type Listener = { listener: () => void };

export class QueryEngine {
    readonly source = new QuerySourceImpl(this);
    private readonly monitors = new Map<string, Monitor>();
    private readonly sets = new Set<QuerySet>();
    // The monitors whose count reached zero in the current synchronous code.
    private readonly idle = new Set<Monitor>();
    private sweepQueued = false;
    private flushQueued = false;
    private flushing = 0;
    private readonly onCommit = (commit: TargetCommitEvent): void => { this.receive(commit); };

    constructor(private readonly target: Store, private readonly idColumn = 'id') {
        target.addCommitListener(this.onCommit);
    }

    selectFrom(table: string): TableQuery {
        return new TableQuery(this.source, table);
    }

    monitor(query: TableQuery): QuerySubscription<ProjectedRow[]>;
    monitor(query: { [name: string]: TableQuery }): QuerySubscription<{ [name: string]: ProjectedRow[] }>;
    // Returns a handle and touches nothing. A monitor exists only while
    // something holds it: a subscribed listener, a monitor that reads it
    // through `.col()`, or a one-shot read while it waits.
    monitor(query: TableQuery | { [name: string]: TableQuery }): QuerySubscription<ProjectedRow[] | { [name: string]: ProjectedRow[] }> {
        if (!(query instanceof TableQuery)) return new QuerySet(this, query).subscription();
        const engine = this;
        const key = query.fingerprint();
        return {
            getSnapshot: () => engine.find(key)?.current() ?? LOADING,
            subscribe(listener: () => void) {
                const monitor = engine.hold(query);
                const unlisten = monitor.listen(listener);
                let held = true;
                return () => {
                    if (!held) return;
                    held = false;
                    unlisten();
                    engine.release(monitor);
                };
            },
            whenReady: async () => {
                const monitor = engine.hold(query);
                try {
                    await monitor.firstResult();
                    return monitor.current();
                } finally {
                    engine.release(monitor);
                }
            },
        };
    }

    close(): void {
        this.target.removeCommitListener(this.onCommit);
    }

    // The monitor running for a query, by fingerprint. It never makes one.
    find(key: string): Monitor | undefined {
        return this.monitors.get(key);
    }

    // Counts one more user of the query's monitor. The first one makes it: a
    // new monitor holds the monitors of the queries its `.col()` parameters
    // read, for as long as it exists, and starts its first run.
    hold(query: TableQuery): Monitor {
        const key = query.fingerprint();
        let monitor = this.monitors.get(key);
        if (monitor === undefined) {
            const upstreams = query.dependencies().map((ref) => this.hold(ref.query));
            monitor = new Monitor(this, query, this.target, this.idColumn, upstreams);
            this.monitors.set(key, monitor);
            void monitor.start();
        }
        monitor.refs++;
        return monitor;
    }

    // Counts one user less. A monitor nobody holds leaves the engine when the
    // current synchronous code ends, not at once: a subscription dropped and
    // taken again in one go, as React does within a screen update, keeps it.
    release(monitor: Monitor): void {
        monitor.refs--;
        if (monitor.refs > 0) return;
        this.idle.add(monitor);
        if (this.sweepQueued) return;
        this.sweepQueued = true;
        queueMicrotask(() => { this.sweep(); });
    }

    // Removes the idle monitors nobody took again. Removing one releases its
    // upstreams, and those left with no holder join the same pass.
    private sweep(): void {
        for (const monitor of this.idle) {
            this.idle.delete(monitor);
            if (monitor.refs > 0) continue;
            this.monitors.delete(monitor.key);
            for (const upstream of monitor.remove()) this.release(upstream);
        }
        this.sweepQueued = false;
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
        this.flushing++;
        try {
            const order = topo([...this.monitors.values()]);
            for (const monitor of order) {
                // One removed during the pass has stopped running.
                if (!monitor.dirty || monitor.removed) continue;
                await this.settle(monitor);
            }
        } finally {
            this.flushing--;
        }
        for (const set of this.sets) set.publish();
    }

    async settle(monitor: Monitor): Promise<void> {
        const before = this.exportedKeySnapshot(monitor);
        await monitor.run();
        if (monitor.removed || this.exportedKeySnapshot(monitor) === before) return;
        for (const other of this.monitors.values()) {
            if (other.query.dependencies().some((ref) => ref.query.fingerprint() === monitor.key)) this.schedule(other);
        }
    }

    // A monitor that delivers outside a flush, as on its first run, tells the
    // sets at once. In a flush, each set delivers once, at the end, with its
    // members' deliveries of the whole pass: a dependent member has re-run by
    // then, and an event two members carry is delivered once.
    notifySets(): void {
        if (this.flushing > 0) return;
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
    // How many use it: subscribed listeners, monitors that read it through
    // `.col()`, and one-shot reads in progress (QueryEngine.hold).
    refs = 0;
    // start() runs the first query; a pass runs a monitor only once a commit
    // or an upstream marks it.
    dirty = false;
    // Set when it leaves the engine. It never comes back: a later hold of the
    // same query makes a new monitor.
    removed = false;
    private highWater = 0;
    private armed = false;
    private rows: ProjectedRow[] = [];
    private fullRows: ProjectedRow[] = [];
    private rowById = new Map<number, ProjectedRow>();
    // The events for the next delivery, and those of the last one.
    private events: StoredOpEvent[] = [];
    private delivered: StoredOpEvent[] = [];
    // Events naming a row of this table that was not in the result before
    // their commit: the next run attaches each one whose row it reads.
    private pending: StoredOpEvent[] = [];
    private loading = true;
    private error: unknown;
    private snapshot: QuerySnapshot<ProjectedRow[]>;
    private readonly listeners = new Set<Listener>();
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
        // The monitors of the queries its `.col()` parameters read, in the
        // order of `query.dependencies()`. It holds them while it exists.
        readonly upstreams: Monitor[],
    ) {
        this.key = query.fingerprint();
        this.snapshot = LOADING;
        this.readyPromise = new Promise<void>((resolve) => { this.ready = resolve; });
    }

    current(): QuerySnapshot<ProjectedRow[]> { return this.snapshot; }

    // The events of the current snapshot, with their log ids.
    deliveredEvents(): StoredOpEvent[] { return this.delivered; }

    // Calls the listener on each delivery, until the returned function is called.
    listen(listener: () => void): () => void {
        const entry = { listener };
        this.listeners.add(entry);
        return () => { this.listeners.delete(entry); };
    }

    // Leaves the engine for good: its runs stop, and a run waiting for its
    // first result is let go. Returns the upstream monitors it held.
    remove(): Monitor[] {
        this.removed = true;
        this.ready?.();
        this.ready = undefined;
        return this.upstreams;
    }

    async start(): Promise<void> {
        if (this.target.drainOpEvents !== undefined) {
            const [top] = await this.target.drainOpEvents({ order: 'desc', limit: 1 });
            if (this.removed) return;
            this.highWater = top?.id ?? 0;
        }
        this.armed = true;
        await this.engine.settle(this);
    }

    firstResult(): Promise<void> { return this.readyPromise; }

    consider(commit: TargetCommitEvent): boolean {
        if (!this.armed) return false;
        if (commit.source === 'reconnect') return true;
        if (commit.source === 'local') return commit.tables.includes(this.query.table);
        if (commit.source !== 'settle' && commit.source !== 'apply') return false;
        const attached = this.takeEvents(commit.events);
        if (this.commitDirties(commit)) return true;
        // A pending event needs its row to enter the result, and this commit
        // changes nothing the query reads; a run already due checks them all.
        if (!this.dirty) this.pending = [];
        // An attached event is delivered now, even with the rows unchanged.
        return attached;
    }

    private commitDirties(commit: Extract<TargetCommitEvent, { source: 'settle' | 'apply' }>): boolean {
        if (commit.source === 'settle') return commit.revertedTables.includes(this.query.table);
        if (commit.schemaTables.includes(this.query.table)) return true;
        const where = this.boundWhere();
        const orderBy = this.order();
        return commit.changes.some((change) => change.table === this.query.table && this.changeDirties(change, where, orderBy));
    }

    async run(): Promise<void> {
        this.dirty = false;
        const pending = this.pending;
        this.pending = [];
        try {
            // A run binds the keys of the queries its `.col()` parameters
            // name, so it waits for the first result of each. It holds their
            // monitors, so it looks nothing up. A monitor removed while one
            // of its runs waits stops there, and delivers nothing.
            if (this.upstreams.length > 0) await Promise.all(this.upstreams.map((upstream) => upstream.firstResult()));
            if (this.removed) return;
            const plan = this.plan();
            const fetched = await this.target.executeQuery(plan);
            if (this.removed) return;
            const cursor = this.query.cursorBound(this.cursorTuple);
            if (cursor !== undefined) {
                await this.refreshCursor(cursor, this.order());
                if (this.removed) return;
            }
            const delivered = strip(fetched, this.query, this.idColumn).map((row) => {
                const prev = this.rowById.get(row.id);
                return prev !== undefined && JSON.stringify(prev) === JSON.stringify(row) ? prev : row;
            });
            // Rows equal to the delivered ones keep their array, also in a
            // delivery made for an event alone.
            const unchanged = !this.loading && this.error === undefined && sameRows(this.snapshot.data, delivered);
            this.fullRows = fetched;
            this.rows = unchanged ? this.snapshot.data : delivered;
            this.rowById = new Map(this.rows.map((row) => [row.id, row]));
            this.cachedRowIds = new Set(this.rows.map((row) => row.id));
            for (const stored of pending) {
                if (this.namesShownRow(stored.event)) this.events.push(stored);
            }
            this.lastRowTuple = delivered.length > 0 && this.queryLimit() !== undefined && delivered.length === this.queryLimit()
                ? tupleOf(fetched[fetched.length - 1]!, this.order())
                : undefined;
            if (unchanged && this.events.length === 0) return;
            this.loading = false;
            this.error = undefined;
            this.publish();
        } catch (error) {
            if (this.removed) return;
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
        // The upstream monitors are the ones it holds, in the same order.
        this.query.dependencies().forEach((ref, index) => {
            sets.set(ref, this.upstreams[index]!.keys(ref.column));
        });
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

    // Takes a commit's events past the high-water mark, the same way for a
    // refusal and a flip. One that names a row of the result is attached to
    // the next delivery, and one that names another row of this table waits
    // for the run (a reinstated row, or a refused delete, enters the result).
    // An event that names no row of this table is dropped, and the mark moves
    // past it either way, so an event never attaches later. Returns whether
    // one was attached.
    private takeEvents(events: StoredOpEvent[]): boolean {
        let attached = false;
        for (const stored of events) {
            if (stored.id <= this.highWater) continue;
            this.highWater = stored.id;
            if (this.namesShownRow(stored.event)) {
                this.events.push(stored);
                attached = true;
            } else if (opEventRows(stored.event).some((row) => row.table === this.query.table && row.localId !== undefined)) {
                this.pending.push(stored);
            }
        }
        return attached;
    }

    // Whether the event names a row of the result: its table is this query's
    // and its local id is among the result's ids. Local ids are per table.
    private namesShownRow(event: OpEvent): boolean {
        return opEventRows(event).some((row) =>
            row.table === this.query.table && row.localId !== undefined && this.cachedRowIds.has(row.localId));
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
        this.delivered = this.events.sort((a, b) => a.id - b.id);
        this.events = [];
        this.snapshot = {
            data: this.rows, events: this.delivered.map((stored) => stored.event), loading: false,
            ...(this.error !== undefined ? { error: this.error } : {}),
        };
        this.ready?.();
        this.ready = undefined;
        for (const { listener } of this.listeners) listener();
        this.engine.notifySets();
    }
}

// Several queries delivered as one snapshot. While it has a listener it holds
// its members' monitors, and the engine publishes it as they deliver. With no
// listener it holds nothing: it shows what its members' monitors show when
// others keep them running, and loading for a member nobody runs.
class QuerySet {
    // Each member's fingerprint, by name.
    private readonly keys: { [name: string]: string } = {};
    // The members' monitors, while the set has a listener.
    private held: { [name: string]: Monitor } | undefined;
    private snapshot: QuerySnapshot<{ [name: string]: ProjectedRow[] }> | undefined;
    private readonly listeners = new Set<Listener>();
    // Each member's snapshot as this set last read it. With a listener, a
    // member whose snapshot has changed since has events the set has not
    // delivered. Deliveries a member made before the set had a listener stay
    // with that member.
    private readonly seen = new Map<string, QuerySnapshot<ProjectedRow[]>>();

    constructor(private readonly engine: QueryEngine, private readonly queries: { [name: string]: TableQuery }) {
        for (const [name, query] of Object.entries(queries)) this.keys[name] = query.fingerprint();
    }

    subscription(): QuerySubscription<{ [name: string]: ProjectedRow[] }> {
        const set = this;
        return {
            getSnapshot: () => set.read(),
            subscribe(listener: () => void) {
                const entry = { listener };
                if (set.listeners.size === 0) set.attach();
                set.listeners.add(entry);
                let held = true;
                return () => {
                    if (!held) return;
                    held = false;
                    set.listeners.delete(entry);
                    if (set.listeners.size === 0) set.detach();
                };
            },
            whenReady: async () => {
                const members = Object.values(set.queries).map((query) => set.engine.hold(query));
                try {
                    await Promise.all(members.map((monitor) => monitor.firstResult()));
                    set.publish();
                    return set.read();
                } finally {
                    for (const monitor of members) set.engine.release(monitor);
                }
            },
        };
    }

    publish(): void {
        if (!this.refresh()) return;
        for (const { listener } of this.listeners) listener();
    }

    // The first listener: hold the members, and join the engine's sets.
    private attach(): void {
        const held: { [name: string]: Monitor } = {};
        for (const [name, query] of Object.entries(this.queries)) held[name] = this.engine.hold(query);
        // Read the members as they are now, before it counts as listening:
        // what they delivered so far is not this set's, and carries no event.
        this.refresh();
        this.held = held;
        this.engine.keepSet(this);
    }

    // The last listener left: leave the engine's sets, and let the members go.
    private detach(): void {
        const held = this.held!;
        this.held = undefined;
        this.engine.dropSet(this);
        for (const monitor of Object.values(held)) this.engine.release(monitor);
    }

    private read(): QuerySnapshot<{ [name: string]: ProjectedRow[] }> {
        // With a listener, the snapshot changes only when the engine
        // publishes the set. Without one, nothing publishes it.
        if (this.held === undefined) this.refresh();
        return this.snapshot!;
    }

    // Rebuilds the snapshot from the members' snapshots and says whether it
    // changed. The object is kept while no member's rows, loading or error
    // changed and there is no new event. With a listener, the events are those
    // of the member deliveries the set has not read, in log order, each once:
    // two members can show the same row.
    private refresh(): boolean {
        const data: { [name: string]: ProjectedRow[] } = {};
        let loading = false;
        let error: unknown;
        const fresh = new Map<number, StoredOpEvent>();
        for (const [name, key] of Object.entries(this.keys)) {
            const monitor = this.held?.[name] ?? this.engine.find(key);
            const snap = monitor?.current() ?? LOADING;
            data[name] = snap.data;
            loading = loading || snap.loading;
            if (snap.error !== undefined) error = snap.error;
            if (this.seen.get(name) === snap) continue;
            this.seen.set(name, snap);
            if (this.held === undefined || monitor === undefined) continue;
            for (const stored of monitor.deliveredEvents()) fresh.set(stored.id, stored);
        }
        const events = [...fresh.values()].sort((a, b) => a.id - b.id).map((stored) => stored.event);
        const prev = this.snapshot;
        if (prev !== undefined && events.length === 0 && prev.loading === loading && prev.error === error
            && Object.keys(data).every((name) => data[name] === prev.data[name])) return false;
        this.snapshot = { data, events, loading, ...(error !== undefined ? { error } : {}) };
        return true;
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
