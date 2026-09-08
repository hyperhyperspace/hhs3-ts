// RdbProjection: the reactive supervisor. You point it at an RDb + a shared
// target, and it keeps a replica-wide bidirectional projection in sync without
// polling:
//
//   const projection = await RdbProjection.open(rdb, ctx, target, { writer });
//   // ... app reads/writes the target; rdb changes stream in ...
//   await projection.stop();  // waits for in-flight sync, then target.close()
//
// It resolves the RDb's member groups (scope.ts), does an initial
// materialization, then drives syncDatabase (ingest local edits, then project
// rdb back) on three reactive triggers, all debounced/coalesced into one cycle:
//
//   - outbound: each member RTableGroup.subscribe fires when rdb advances;
//   - inbound: if the target is a ChangeSignalSource, its outbox monitor fires
//     when local edits are waiting (else callers use nudge() / an external poll);
//   - membership: RDb.subscribe fires when a group is added; the scope rebuilds.
//
// A single-flight guard (plus the per-database lock inside syncDatabase) ensures
// cycles never overlap; a trigger during a run schedules exactly one more.

import type { B64Hash, KeyId, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { Version, RContext } from "@hyper-hyper-space/hhs3_mvt";
import type { RDb, RTableGroup } from "@hyper-hyper-space/hhs3_rdb";
import {
    BidirectionalTarget, ChangeSignalListener, ChangeSignalSource, CheckpointMovedError,
    DEFAULT_KEY_DOMAIN, GroupProjection, IngestResult, KeyIndex, OpEvent, OpEventQuery,
    StoredOpEvent, syncDatabase,
} from "@hyper-hyper-space/hhs3_rdb_adapter";

import { buildScope, resolveMemberGroups, GroupConfigOverride } from "./scope.js";

export type OpEventListener = (events: OpEvent[]) => void;

export type RdbProjectionOptions = {
    // Default writer for every member (enables local->rdb ingestion). Absent:
    // projection is read-only (rdb -> target), local edits are never pushed.
    writer?: OwnIdentity;
    configOverride?: GroupConfigOverride;
    // Coalescing window for reactive triggers (ms). Default 50.
    debounceMs?: number;
    // uuid minter for newly-ingested rows (defaults to crypto.randomUUID).
    createUuid?: () => string;
    // Called after each successful sync cycle / on error.
    onResult?: (results: Map<B64Hash, IngestResult>) => void;
    onError?: (err: unknown) => void;
    // Live subscribe for the durable op-event log (ingestion failures + p2p
    // concurrency void/reinstate flips). Registered BEFORE the initial sync, so
    // this session sees events produced by that start. Historical rows are NOT
    // replayed — subscribe snapshots high-water, then only new ids are pushed.
    // Catch-up is the app's job via opEvents({ afterId, beforeId, limit, order }).
    onOpEvents?: OpEventListener;
    // Which authors' CONCURRENCY events to push: 'all' (default) or an allow-list
    // of KeyIds. INGESTION failures always pass (they are always the local
    // writer's own botched edits and are never filtered).
    eventAuthors?: 'all' | KeyId[];
};

function isChangeSignalSource(t: object): t is ChangeSignalSource {
    return typeof (t as ChangeSignalSource).addChangeListener === 'function';
}

function isCloseable(t: object): t is { close(): void | Promise<void> } {
    return typeof (t as { close?: unknown }).close === 'function';
}

// Whether an op-event should be pushed under an author policy. INGESTION
// failures ALWAYS pass (the local writer's own botched edits are always
// relevant). CONCURRENCY flips pass iff `eventAuthors` is 'all' (default) or
// names the op's author; a concurrency event with no known author is dropped
// under a non-'all' allow-list.
export function opEventPushable(event: OpEvent, eventAuthors: 'all' | KeyId[] = 'all'): boolean {
    if (event.origin === 'ingestion') return true;
    if (eventAuthors === 'all') return true;
    return event.author !== undefined && eventAuthors.includes(event.author);
}

function asKeyIndex(t: object): KeyIndex | undefined {
    const candidate = t as Partial<KeyIndex>;
    return typeof candidate.registerKey === 'function' ? candidate as KeyIndex : undefined;
}

export class RdbProjection {
    private members: GroupProjection[] = [];
    private groupCallbacks = new Map<B64Hash, { group: RTableGroup; cb: (v: Version) => void }>();
    private rdbCallback: ((v: Version) => void) | undefined;
    private changeListener: ChangeSignalListener | undefined;

    private timer: ReturnType<typeof setTimeout> | undefined;
    private stopped = false;
    private running = false;
    private rerun = false;
    private lastResults = new Map<B64Hash, IngestResult>();
    private lastErrorMessage: string | undefined;
    // Live-subscribe cursor: last id delivered (or high-water at arm). 0 means
    // empty log (first event id is 1). Not durable — the app keeps its own mark.
    private opEventCursor = 0;
    private opEventArmed = false;
    private opEventListeners = new Set<OpEventListener>();
    // In-flight explicit sync() + reactive runSyncOnce. stop() awaits idle
    // before closing the target so a mid-cycle apply cannot hit a closed db.
    private inFlight = 0;
    private idle: Promise<void> = Promise.resolve();
    private idleResolve: (() => void) | undefined;
    private stopPromise: Promise<void> | undefined;

    private constructor(
        private readonly rdb: RDb,
        private readonly ctx: RContext,
        private readonly target: BidirectionalTarget,
        private readonly options: RdbProjectionOptions,
    ) {}

    // Open a supervised projection: resolve members, materialize once, then arm
    // the reactive triggers. Returns once the initial sync has completed.
    static async open(
        rdb: RDb, ctx: RContext, target: BidirectionalTarget, options: RdbProjectionOptions = {},
    ): Promise<RdbProjection> {
        const p = new RdbProjection(rdb, ctx, target, options);
        try {
            await p.reconfigure();
            // Arm live subscribe BEFORE the initial sync so this session sees
            // events that start produces, without replaying the durable backlog.
            if (options.onOpEvents !== undefined) await p.subscribeOpEvents(options.onOpEvents);
            await p.sync();   // initial materialization (awaited; rethrows on failure)
            await p.arm();
            return p;
        } catch (e) {
            await p.stop();
            throw e;
        }
    }

    // The set of member group ids currently in scope.
    memberGroupIds(): B64Hash[] {
        return this.members.map((m) => m.group.getId());
    }

    // The results of the most recent sync cycle (per group).
    lastResult(): Map<B64Hash, IngestResult> {
        return this.lastResults;
    }

    // The message of the most recent thrown sync failure, cleared on the next
    // successful cycle. Ingest rejections are NOT errors and do not set this
    // (they surface through lastResult()).
    lastError(): string | undefined {
        return this.lastErrorMessage;
    }

    // Explicit, awaitable sync cycle (bypasses the debounce; still single-
    // flighted by syncDatabase's per-database lock).
    async sync(): Promise<Map<B64Hash, IngestResult>> {
        if (this.stopped) throw new Error('projection is stopped');
        this.beginWork();
        try {
            if (this.stopped) throw new Error('projection is stopped');
            const results = await this.syncWithCasRetry();
            this.lastResults = results;
            this.lastErrorMessage = undefined;
            await this.pushOpEvents();
            return results;
        } finally {
            this.endWork();
        }
    }

    // Run one syncDatabase cycle, retrying on a CheckpointMovedError: another
    // projector on the same store advanced a group's checkpoint since our delta
    // was computed, so recompute from the new checkpoint. Bounded so a pathological
    // contention loop still surfaces. Other errors propagate immediately.
    private async syncWithCasRetry(): Promise<Map<B64Hash, IngestResult>> {
        const maxAttempts = 3;
        for (let attempt = 1; ; attempt++) {
            try {
                return await syncDatabase(this.members, this.target, this.options.createUuid);
            } catch (e) {
                if (e instanceof CheckpointMovedError && attempt < maxAttempts) continue;
                throw e;
            }
        }
    }

    // Inbound trigger fallback for callers that cannot wire a ChangeSignalSource
    // (schedules a debounced cycle, like the reactive triggers do).
    nudge(): void {
        this.schedule();
    }

    // Inspect the durable op-event log. Non-destructive; independent of the
    // live-subscribe cursor. Empty when the target has no op-event log.
    // A numeric argument is `{ afterId }` (order defaults to 'asc').
    async opEvents(opts?: OpEventQuery | number): Promise<StoredOpEvent[]> {
        if (typeof this.target.drainOpEvents !== 'function') return [];
        return this.target.drainOpEvents(opts);
    }

    // Await until the live cursor is snapshotted (high-water via
    // `{ order: 'desc', limit: 1 }`, empty → 0), then receive batches of NEW
    // events after each successful sync. Subscribe first, then opEvents() for
    // leftover — at-least-once; the app dedups against its own mark.
    async subscribeOpEvents(listener: OpEventListener): Promise<void> {
        if (this.stopped) throw new Error('projection is stopped');
        if (!this.opEventArmed) {
            const tail = await this.opEvents({ order: 'desc', limit: 1 });
            this.opEventCursor = tail[0]?.id ?? 0;
            this.opEventArmed = true;
        }
        this.opEventListeners.add(listener);
    }

    unsubscribeOpEvents(listener: OpEventListener): void {
        this.opEventListeners.delete(listener);
        if (this.opEventListeners.size === 0) {
            this.opEventArmed = false;
            this.opEventCursor = 0;
        }
    }

    // -----------------------------------------------------------------------
    // KeyIndex facade (shared rdb_keys side table owned by the target).
    // Throws when the target does not implement KeyIndex.
    // -----------------------------------------------------------------------

    private requireKeys(): KeyIndex {
        const keys = asKeyIndex(this.target);
        if (keys === undefined) {
            throw new Error("projection target does not implement KeyIndex (rdb_keys)");
        }
        return keys;
    }

    // Get-or-allocate a key id for (keyHash, publicKey). Public key is
    // mandatory — required for ingest self-certification of identity rows.
    async registerKey(keyHash: string, publicKey: string, domain: string = DEFAULT_KEY_DOMAIN): Promise<number> {
        return this.requireKeys().registerKey(domain, keyHash, publicKey);
    }

    async keyHashForId(id: number, domain: string = DEFAULT_KEY_DOMAIN): Promise<string | undefined> {
        return this.requireKeys().keyHashForId(domain, id);
    }

    async publicKeyForId(id: number, domain: string = DEFAULT_KEY_DOMAIN): Promise<string | undefined> {
        return this.requireKeys().publicKeyForId(domain, id);
    }

    async idForKeyHash(keyHash: string, domain: string = DEFAULT_KEY_DOMAIN): Promise<number | undefined> {
        return this.requireKeys().idForKeyHash(domain, keyHash);
    }

    // Stop the supervisor: no new cycles, wait for any in-flight sync, then
    // close the target if it implements close() (SQLite drops WAL/SHM).
    async stop(): Promise<void> {
        this.stopPromise ??= this.doStop();
        return this.stopPromise;
    }

    private async doStop(): Promise<void> {
        this.stopped = true;
        if (this.timer !== undefined) { clearTimeout(this.timer); this.timer = undefined; }
        for (const { group, cb } of this.groupCallbacks.values()) group.unsubscribe(cb);
        this.groupCallbacks.clear();
        if (this.rdbCallback !== undefined) { this.rdb.unsubscribe(this.rdbCallback); this.rdbCallback = undefined; }
        if (this.changeListener !== undefined && isChangeSignalSource(this.target)) {
            this.target.removeChangeListener(this.changeListener);
            this.changeListener = undefined;
        }
        await this.idle;
        this.opEventListeners.clear();
        this.opEventArmed = false;
        this.opEventCursor = 0;
        if (isCloseable(this.target)) await this.target.close();
    }

    private beginWork(): void {
        this.inFlight++;
        if (this.inFlight === 1) {
            this.idle = new Promise<void>((resolve) => { this.idleResolve = resolve; });
        }
    }

    private endWork(): void {
        this.inFlight--;
        if (this.inFlight === 0) {
            this.idleResolve?.();
            this.idleResolve = undefined;
            this.idle = Promise.resolve();
        }
    }

    // Read new op-events since the live cursor and push the author-filtered
    // batch. Advances the cursor past ALL drained events (even filtered-out
    // ones) so they are not re-drained next cycle. Best-effort: a target
    // without an op-event log, or a drain error, is swallowed.
    private async pushOpEvents(): Promise<void> {
        if (this.opEventListeners.size === 0) return;
        if (typeof this.target.drainOpEvents !== 'function') return;
        let stored;
        try {
            stored = await this.target.drainOpEvents({ afterId: this.opEventCursor, order: 'asc' });
        } catch {
            return;
        }
        if (stored.length === 0) return;
        this.opEventCursor = stored[stored.length - 1].id;
        const authors = this.options.eventAuthors ?? 'all';
        const events = stored.map((s) => s.event).filter((e) => opEventPushable(e, authors));
        if (events.length === 0) return;
        for (const listener of this.opEventListeners) listener(events);
    }

    // -----------------------------------------------------------------------
    // internals
    // -----------------------------------------------------------------------

    // (Re)resolve members + rebuild the scope, subscribing any newly-present
    // group. Members already subscribed keep their callbacks (RDb membership is
    // monotonic in v1, so groups are only ever added).
    private async reconfigure(): Promise<void> {
        const groups = await resolveMemberGroups(this.rdb, this.ctx);
        this.members = await buildScope(groups, {
            writer: this.options.writer,
            configOverride: this.options.configOverride,
        });
        for (const group of groups) {
            const id = group.getId();
            if (this.groupCallbacks.has(id)) continue;
            const cb = (): void => this.schedule();
            await group.subscribe(cb);
            this.groupCallbacks.set(id, { group, cb });
        }
    }

    private async arm(): Promise<void> {
        this.rdbCallback = (): void => { void this.onMembershipChange(); };
        await this.rdb.subscribe(this.rdbCallback);
        if (isChangeSignalSource(this.target)) {
            this.changeListener = (): void => this.schedule();
            this.target.addChangeListener(this.changeListener);
        }
    }

    private async onMembershipChange(): Promise<void> {
        if (this.stopped) return;
        try {
            await this.reconfigure();
        } catch (e) {
            this.lastErrorMessage = e instanceof Error ? e.message : String(e);
            this.options.onError?.(e);
            return;
        }
        this.schedule();
    }

    // Debounced, coalesced trigger: at most one pending timer; the run itself
    // coalesces bursts via the single-flight guard.
    private schedule(): void {
        if (this.stopped || this.timer !== undefined) return;
        const delay = this.options.debounceMs ?? 50;
        this.timer = setTimeout(() => { this.timer = undefined; void this.runSyncOnce(); }, delay);
        (this.timer as unknown as { unref?: () => void }).unref?.();
    }

    // Run one (or, if a trigger arrived mid-run, exactly one more) sync cycle.
    private async runSyncOnce(): Promise<void> {
        if (this.stopped) return;
        if (this.running) { this.rerun = true; return; }
        this.running = true;
        this.beginWork();
        try {
            do {
                this.rerun = false;
                const results = await this.syncWithCasRetry();
                this.lastResults = results;
                this.lastErrorMessage = undefined;
                this.options.onResult?.(results);
                await this.pushOpEvents();
            } while (this.rerun && !this.stopped);
        } catch (e) {
            this.lastErrorMessage = e instanceof Error ? e.message : String(e);
            this.options.onError?.(e);
        } finally {
            this.running = false;
            this.endWork();
        }
    }
}
