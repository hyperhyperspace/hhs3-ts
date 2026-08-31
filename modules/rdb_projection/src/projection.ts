// RdbProjection: the reactive supervisor. You point it at an RDb + a shared
// target, and it keeps a replica-wide bidirectional projection in sync without
// polling:
//
//   const projection = await RdbProjection.open(rdb, ctx, target, { writer });
//   // ... app reads/writes the target; rdb changes stream in ...
//   await projection.stop();
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
    BidirectionalTarget, ChangeSignalListener, ChangeSignalSource, DEFAULT_KEY_DOMAIN,
    GroupProjection, IngestResult, KeyIndex, OpEvent, StoredOpEvent, syncDatabase,
} from "@hyper-hyper-space/hhs3_rdb_adapter";

import { buildScope, resolveMemberGroups, GroupConfigOverride } from "./scope.js";
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
    // Push channel for the durable op-event log (ingestion failures + p2p
    // concurrency void/reinstate flips). After each cycle the supervisor reads
    // events logged since its cursor and calls this with the (author-filtered)
    // batch, oldest first. The durable backlog is re-read from id 0 on open, so
    // an app that just started still learns of past failures/voids.
    onOpEvents?: (events: OpEvent[]) => void;
    // Which authors' CONCURRENCY events to push: 'all' (default) or an allow-list
    // of KeyIds. INGESTION failures always pass (they are always the local
    // writer's own botched edits and are never filtered).
    eventAuthors?: 'all' | KeyId[];
};

function isChangeSignalSource(t: object): t is ChangeSignalSource {
    return typeof (t as ChangeSignalSource).addChangeListener === 'function';
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
    // Push cursor into the target's durable op-event log (last id delivered).
    private opEventCursor = 0;

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
        await p.reconfigure();
        await p.sync();   // initial materialization (awaited; rethrows on failure)
        p.arm();
        return p;
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
        const results = await syncDatabase(this.members, this.target, this.options.createUuid);
        this.lastResults = results;
        this.lastErrorMessage = undefined;
        await this.pushOpEvents();
        return results;
    }

    // Inbound trigger fallback for callers that cannot wire a ChangeSignalSource
    // (schedules a debounced cycle, like the reactive triggers do).
    nudge(): void {
        this.schedule();
    }

    // The durable op-event backlog since `sinceId` (default: from the start),
    // oldest first. Non-destructive; independent of the push cursor. Empty when
    // the target has no op-event log.
    async opEvents(sinceId = 0): Promise<StoredOpEvent[]> {
        if (typeof this.target.drainOpEvents !== 'function') return [];
        return this.target.drainOpEvents(sinceId);
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

    async stop(): Promise<void> {
        this.stopped = true;
        if (this.timer !== undefined) { clearTimeout(this.timer); this.timer = undefined; }
        for (const { group, cb } of this.groupCallbacks.values()) group.unsubscribe(cb);
        this.groupCallbacks.clear();
        if (this.rdbCallback !== undefined) { this.rdb.unsubscribe(this.rdbCallback); this.rdbCallback = undefined; }
        if (this.changeListener !== undefined && isChangeSignalSource(this.target)) {
            this.target.removeChangeListener(this.changeListener);
            this.changeListener = undefined;
        }
    }

    // Read new op-events since the cursor and push the author-filtered batch.
    // Advances the cursor past ALL drained events (even filtered-out ones) so
    // they are not re-drained next cycle. Best-effort: a target without an
    // op-event log, or a drain error, is swallowed (never fails a sync cycle).
    private async pushOpEvents(): Promise<void> {
        if (this.options.onOpEvents === undefined) return;
        if (typeof this.target.drainOpEvents !== 'function') return;
        let stored;
        try {
            stored = await this.target.drainOpEvents(this.opEventCursor);
        } catch {
            return;
        }
        if (stored.length === 0) return;
        this.opEventCursor = stored[stored.length - 1].id;
        const authors = this.options.eventAuthors ?? 'all';
        const events = stored.map((s) => s.event).filter((e) => opEventPushable(e, authors));
        if (events.length > 0) this.options.onOpEvents(events);
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
            group.subscribe(cb);
            this.groupCallbacks.set(id, { group, cb });
        }
    }

    private arm(): void {
        this.rdbCallback = (): void => { void this.onMembershipChange(); };
        this.rdb.subscribe(this.rdbCallback);
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
        try {
            do {
                this.rerun = false;
                const results = await syncDatabase(this.members, this.target, this.options.createUuid);
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
        }
    }
}
