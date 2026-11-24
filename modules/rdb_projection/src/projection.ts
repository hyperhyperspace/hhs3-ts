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
import type { RBlobStore, RDb, RFileMap, RTableGroup } from "@hyper-hyper-space/hhs3_rdb";
import { PreparedHashMismatchError } from "@hyper-hyper-space/hhs3_rdb";
import {
    BidirectionalTarget, ChainSchemaChangeError, ChangeSignalListener, ChangeSignalSource, CheckpointMovedError,
    isQueryableTarget, isReactiveChangeTarget, QueryEngine, TableQuery, TargetCommitListener,
    DEFAULT_KEY_DOMAIN, GroupProjection, IndexReconcileReport, IndexSpec, IngestResult, KeyIndex, OpEvent,
    OpEventQuery, reconcileIndexes, ReconcileIndexesOptions, StoredOpEvent, syncDatabase, SyncOutcome,
    isWriterRecordStore, type AuthorshipMode, type WriterRecord,
} from "@hyper-hyper-space/hhs3_rdb_adapter";
import { serializePublicKeyToBase64 } from "@hyper-hyper-space/hhs3_mvt";

import { buildScope, resolveMemberGroups, GroupConfigOverride } from "./scope.js";
import type { FileDirectory } from "./files/directory.js";
import { FileMount, type FilesMountSpec, type FilesMountStatus } from "./files/mount.js";

export type OpEventListener = (events: OpEvent[]) => void;

// Opens the folder of a mount; called once, when the mount attaches.
export type FilesMountOpener = (spec: FilesMountSpec) => FileDirectory | Promise<FileDirectory>;

export type FilesReconcileOptions = {
    debounceMs?: number;
    // Periodic scan of each mounted folder (ms); 0 disables. Default 5000.
    scanIntervalMs?: number;
    // How often a pending mount retries attaching (ms): a joining replica
    // creates FILES objects on its own schedule. 0 disables. Default 1000.
    retryMs?: number;
};

export type FilesReconcileReport = {
    mounted: string[];
    // Mounts whose FILES is not (yet) a member with both objects present.
    pending: string[];
};

export type RdbProjectionOptions = {
    // Default writer for every member (enables local->rdb ingestion). Absent:
    // a group with an identity provider can't ingest, and an anonymous one
    // ingests without a key (HIP-29).
    writer?: OwnIdentity;
    // What an insert gets when it leaves the author column empty, for every
    // member that a configOverride doesn't set it for (HIP-20's Authorship).
    // Default on. Ignored in an anonymous group.
    autoAuthor?: boolean;
    configOverride?: GroupConfigOverride;
    // Coalescing window for reactive triggers (ms). Default 50.
    debounceMs?: number;
    // uuid minter for newly-ingested rows (defaults to crypto.randomUUID).
    createUuid?: () => string;
    // Called after each successful sync cycle / on error.
    onResult?: (results: Map<B64Hash, IngestResult>) => void;
    onError?: (err: unknown) => void;
    // Warnings that aren't errors: a group with an identity provider and no
    // writer key has a local change pending, which no pass can ingest, so
    // remote changes wait until a projection opens with a key for it (HIP-29).
    // Once per group. Without it, warnings go to console.warn.
    onWarning?: (message: string) => void;
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
    private commitListener: TargetCommitListener | undefined;
    private queryEngine: QueryEngine | undefined;

    private timer: ReturnType<typeof setTimeout> | undefined;
    // After a cycle that ends in an error, the next one is retried with a
    // backoff: 1 s, doubling up to 60 s, reset after a cycle that succeeds.
    private retryTimer: ReturnType<typeof setTimeout> | undefined;
    private retryDelayMs = 0;
    // The groups already warned about (onWarning).
    private warnedGroups = new Set<B64Hash>();
    // The error that halted the projection, if one did (HIP-29): no cycle runs
    // again, sync() throws it, and reads keep working.
    private haltedBy: unknown;
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
    // File mounts: the specs from the last reconcileFiles, and those attached.
    private fileSpecs: FilesMountSpec[] = [];
    private fileMounts = new Map<string, FileMount>();
    private fileOpener: FilesMountOpener | undefined;
    private fileOptions: FilesReconcileOptions = {};
    private fileErrors = new Map<string, string>();
    private attaching: Promise<void> = Promise.resolve();
    private attachRetry: ReturnType<typeof setTimeout> | undefined;

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
        if (this.haltedBy !== undefined) throw this.haltedBy;
        this.beginWork();
        try {
            if (this.stopped) throw new Error('projection is stopped');
            let outcome: SyncOutcome;
            try {
                outcome = await syncDatabase(this.members, this.target, this.options.createUuid);
            } catch (e) {
                if (this.halt(e)) throw e;
                this.scheduleRetry();
                throw e;
            }
            this.afterCycle(outcome);
            await this.pushOpEvents();
            return outcome.results;
        } finally {
            this.endWork();
        }
    }

    // Halts the projection when `e` says the target or rdb is not what a cycle
    // assumed (HIP-29): a checkpoint that moved under it, at the apply or at the
    // settle, which only another projector on the same store does, an entry
    // that the DAG appended under another hash than the one recorded for it, or
    // an ingestion chain whose delta changes the schema.
    // No cycle runs again, sync() throws, and onError fires once. It doesn't
    // call stop(), which would wait for the running cycle. Returns whether it
    // halted.
    private halt(e: unknown): boolean {
        if (!(e instanceof CheckpointMovedError) && !(e instanceof PreparedHashMismatchError) && !(e instanceof ChainSchemaChangeError)) return false;
        if (this.haltedBy !== undefined) return true;
        this.haltedBy = e;
        this.lastErrorMessage = e instanceof Error ? e.message : String(e);
        if (this.timer !== undefined) { clearTimeout(this.timer); this.timer = undefined; }
        if (this.retryTimer !== undefined) { clearTimeout(this.retryTimer); this.retryTimer = undefined; }
        this.options.onError?.(e);
        return true;
    }

    // The error that halted the projection, or undefined while it runs.
    haltError(): unknown {
        return this.haltedBy;
    }

    // Install a projection index spec (never triggered by sync, and open()
    // takes no spec). The same fingerprint as the installed spec is a no-op;
    // anything else is diffed against what the target materialized, at each
    // member's current checkpoint, and from then on every sync keeps it
    // maintained across schema changes. See IndexReconcileReport for the
    // outcome and the declarations still pending. Throws when the target does
    // not support indexes or the spec is invalid.
    async reconcileIndexes(spec: IndexSpec, opts: ReconcileIndexesOptions = {}): Promise<IndexReconcileReport> {
        if (this.stopped) throw new Error('projection is stopped');
        this.beginWork();
        try {
            return await reconcileIndexes(this.members, this.target, spec, opts);
        } finally {
            this.endWork();
        }
    }

    // Mount FILES members as folders (files/mount.ts). A spec whose FILES is
    // not a member yet, or whose objects are not present yet, is pending, and
    // attaches when membership gains it. Specs missing from a later call are
    // detached; their folders are left as they are.
    async reconcileFiles(
        specs: FilesMountSpec[], open: FilesMountOpener, opts: FilesReconcileOptions = {},
    ): Promise<FilesReconcileReport> {
        if (this.stopped) throw new Error('projection is stopped');
        this.requireKeys();
        this.fileSpecs = [...specs];
        this.fileOpener = open;
        this.fileOptions = opts;
        for (const [name, mount] of [...this.fileMounts]) {
            const spec = specs.find((s) => s.name === name);
            if (spec === undefined || spec.path !== mount.spec.path) {
                await mount.stop();
                this.fileMounts.delete(name);
            }
        }
        await this.attachPendingFiles();
        const mounted = specs.filter((s) => this.fileMounts.has(s.name)).map((s) => s.name);
        const pending = specs.filter((s) => !this.fileMounts.has(s.name)).map((s) => s.name);
        return { mounted, pending };
    }

    filesStatus(): FilesMountStatus[] {
        return this.fileSpecs.map((spec) => {
            const mounted = this.fileMounts.get(spec.name);
            if (mounted !== undefined) return mounted.status();
            const error = this.fileErrors.get(spec.name);
            return { name: spec.name, path: spec.path, state: 'pending', ...(error !== undefined ? { lastError: error } : {}) };
        });
    }

    fileMount(name: string): FileMount | undefined {
        return this.fileMounts.get(name);
    }

    // Serialized: reconcileFiles, membership changes and sync cycles all call it.
    private attachPendingFiles(): Promise<void> {
        const run = this.attaching.then(() => this.doAttachPendingFiles());
        this.attaching = run.catch(() => undefined);
        return run;
    }

    private async doAttachPendingFiles(): Promise<void> {
        const pending = this.fileSpecs.filter((s) => !this.fileMounts.has(s.name));
        if (pending.length === 0 || this.fileOpener === undefined || this.stopped) return;
        const members = await this.rdb.getMemberFiles();
        for (const spec of pending) {
            const member = members.find((m) => m.name === spec.name);
            if (member === undefined) continue;
            const store = await this.ctx.getObject(member.storeId);
            const map = await this.ctx.getObject(member.mapId);
            if (store === undefined || map === undefined) continue;
            // With a writer, the bound group decides access: wait for it too.
            const writer = this.options.writer;
            const group = writer === undefined ? undefined : await this.ctx.getObject((map as unknown as RFileMap).getGroupId());
            if (writer !== undefined && group === undefined) continue;
            let mount: FileMount;
            try {
                mount = await FileMount.open({
                    spec,
                    dir: await this.fileOpener(spec),
                    store: store as unknown as RBlobStore,
                    map: map as unknown as RFileMap,
                    keys: this.requireKeys(),
                    ...(writer !== undefined ? { writer, group } : {}),
                    ...(this.fileOptions.debounceMs !== undefined ? { debounceMs: this.fileOptions.debounceMs } : {}),
                    ...(this.fileOptions.scanIntervalMs !== undefined ? { scanIntervalMs: this.fileOptions.scanIntervalMs } : {}),
                    onError: (err) => this.options.onError?.(err),
                });
            } catch (err) {
                this.fileErrors.set(spec.name, err instanceof Error ? err.message : String(err));
                this.options.onError?.(err);
                continue;
            }
            this.fileErrors.delete(spec.name);
            if (this.stopped) { await mount.stop(); return; }
            this.fileMounts.set(spec.name, mount);
        }
        this.scheduleAttachRetry();
    }

    private scheduleAttachRetry(): void {
        const every = this.fileOptions.retryMs ?? 1000;
        const pending = this.fileSpecs.some((s) => !this.fileMounts.has(s.name));
        if (this.stopped || !pending || every <= 0 || this.attachRetry !== undefined) return;
        this.attachRetry = setTimeout(() => {
            this.attachRetry = undefined;
            void this.attachPendingFiles().catch((err) => this.options.onError?.(err));
        }, every);
        (this.attachRetry as unknown as { unref?: () => void }).unref?.();
    }

    // Inbound trigger fallback for callers that cannot wire a ChangeSignalSource
    // (schedules a debounced cycle, like the reactive triggers do). Mounted
    // folders get a pass too.
    nudge(): void {
        this.schedule();
        for (const mount of this.fileMounts.values()) mount.nudge();
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

    onCommit(listener: TargetCommitListener): () => void {
        if (!isReactiveChangeTarget(this.target)) return () => {};
        this.target.addCommitListener(listener);
        return () => {
            if (isReactiveChangeTarget(this.target)) this.target.removeCommitListener(listener);
        };
    }

    selectFrom(table: string): TableQuery {
        return this.queries().selectFrom(table);
    }

    monitor(query: TableQuery | { [name: string]: TableQuery }) {
        const engine = this.queries();
        return query instanceof TableQuery ? engine.monitor(query) : engine.monitor(query);
    }

    private queries(): QueryEngine {
        if (this.queryEngine !== undefined) return this.queryEngine;
        if (!isQueryableTarget(this.target) || !isReactiveChangeTarget(this.target)) {
            throw new Error('this projection target cannot host queries');
        }
        this.queryEngine = new QueryEngine(this.target);
        return this.queryEngine;
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
        if (this.retryTimer !== undefined) { clearTimeout(this.retryTimer); this.retryTimer = undefined; }
        if (this.attachRetry !== undefined) { clearTimeout(this.attachRetry); this.attachRetry = undefined; }
        for (const { group, cb } of this.groupCallbacks.values()) group.unsubscribe(cb);
        this.groupCallbacks.clear();
        if (this.rdbCallback !== undefined) { this.rdb.unsubscribe(this.rdbCallback); this.rdbCallback = undefined; }
        if (this.commitListener !== undefined && isReactiveChangeTarget(this.target)) {
            this.target.removeCommitListener(this.commitListener);
            this.commitListener = undefined;
        } else if (this.changeListener !== undefined && isChangeSignalSource(this.target)) {
            this.target.removeChangeListener(this.changeListener);
            this.changeListener = undefined;
        }
        await this.idle;
        await this.attaching;
        for (const mount of this.fileMounts.values()) await mount.stop();
        this.fileMounts.clear();
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
        const names = new Map([...(await this.rdb.getMemberGroupNames()).entries()].map(([name, id]) => [id, name]));
        const members = await buildScope(groups, {
            names,
            writer: this.options.writer,
            ...(this.options.autoAuthor !== undefined ? { autoAuthor: this.options.autoAuthor } : {}),
            configOverride: this.options.configOverride,
        });
        // autoAuthor off reads each insert's author from its author column, so a
        // signed group can't have it off without the column. Refused before
        // anything is written to the target.
        for (const m of members) {
            if (m.group.getIdProvider() !== undefined && m.config.autoAuthor === false && m.config.authorColumn === false) {
                throw new Error(`group '${names.get(m.group.getId()) ?? m.group.getName()}' has autoAuthor off and no author column: `
                    + "with autoAuthor off, an insert's author is read from its author column");
            }
        }
        this.members = members;
        await this.recordWriter(names);
        for (const group of groups) {
            const id = group.getId();
            if (this.groupCallbacks.has(id)) continue;
            const cb = (): void => this.schedule();
            await group.subscribe(cb);
            this.groupCallbacks.set(id, { group, cb });
        }
        await this.attachPendingFiles();
    }

    // Records the writer record in the target (HIP-20): the writer key, interned
    // in the key table, when there is one, the replica's hash suite, and each
    // member's name and authorship mode. Capture reads it to fill in the author
    // column. Recorded at open and on each reconfigure.
    private async recordWriter(names: Map<B64Hash, string>): Promise<void> {
        if (!isWriterRecordStore(this.target)) return;
        let writerKeyId: number | undefined;
        const writer = this.options.writer;
        const keys = asKeyIndex(this.target);
        if (writer !== undefined && keys !== undefined) {
            writerKeyId = await keys.registerKey(DEFAULT_KEY_DOMAIN, writer.keyId, serializePublicKeyToBase64(writer.publicKey));
        }
        const groups: WriterRecord['groups'] = {};
        for (const m of this.members) {
            const id = m.group.getId();
            const mode: AuthorshipMode = m.group.getIdProvider() === undefined ? 'anonymous' : m.config.autoAuthor === false ? 'off' : 'on';
            groups[id] = { name: names.get(id) ?? m.group.getName(), mode };
        }
        await this.target.recordWriter({
            ...(writerKeyId !== undefined ? { writerKeyId } : {}),
            hashSuite: this.ctx.getHashSuite().name,
            groups,
        });
    }

    private async arm(): Promise<void> {
        this.rdbCallback = (): void => { void this.onMembershipChange(); };
        await this.rdb.subscribe(this.rdbCallback);
        if (isReactiveChangeTarget(this.target)) {
            this.commitListener = (commit): void => { if (commit.source === 'local') this.schedule(); };
            this.target.addCommitListener(this.commitListener);
        } else if (isChangeSignalSource(this.target)) {
            this.changeListener = (): void => this.schedule();
            this.target.addChangeListener(this.changeListener);
        }
        // A change written between open()'s first cycle and the listeners above
        // may have signalled before anyone listened: look for it once.
        if (await this.target.hasPendingChanges()) this.schedule();
    }

    // What every cycle that ran to its end does: keep its results, warn once
    // for each group whose pending change no pass can ingest, and, when a
    // change captured after ingestion refused the projection, schedule the
    // next cycle, which ingests it first. A cycle stopped at a group without a
    // writer key schedules none: it would stop there again.
    private afterCycle(outcome: SyncOutcome): void {
        this.lastResults = outcome.results;
        this.lastErrorMessage = undefined;
        this.retryDelayMs = 0;
        for (const groupId of outcome.stoppedAt) {
            if (this.warnedGroups.has(groupId)) continue;
            this.warnedGroups.add(groupId);
            const message = `group '${groupId}' has a local change pending and no writer key to ingest it; `
                + 'remote changes wait until a projection opens with a key for it';
            if (this.options.onWarning !== undefined) this.options.onWarning(message);
            else console.warn(message);
        }
        if (outcome.refused && outcome.stoppedAt.length === 0) this.schedule();
    }

    // After a cycle that ended in an error: try again later, so a passing
    // error doesn't leave a change waiting for the next write.
    private scheduleRetry(): void {
        if (this.stopped || this.haltedBy !== undefined || this.retryTimer !== undefined) return;
        this.retryDelayMs = this.retryDelayMs === 0 ? 1000 : Math.min(this.retryDelayMs * 2, 60000);
        this.retryTimer = setTimeout(() => { this.retryTimer = undefined; void this.runSyncOnce(); }, this.retryDelayMs);
        (this.retryTimer as unknown as { unref?: () => void }).unref?.();
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
        if (this.stopped || this.haltedBy !== undefined || this.timer !== undefined) return;
        const delay = this.options.debounceMs ?? 50;
        this.timer = setTimeout(() => { this.timer = undefined; void this.runSyncOnce(); }, delay);
        (this.timer as unknown as { unref?: () => void }).unref?.();
    }

    // Run one (or, if a trigger arrived mid-run, exactly one more) sync cycle.
    private async runSyncOnce(): Promise<void> {
        if (this.stopped || this.haltedBy !== undefined) return;
        if (this.running) { this.rerun = true; return; }
        this.running = true;
        this.beginWork();
        try {
            do {
                this.rerun = false;
                const outcome = await syncDatabase(this.members, this.target, this.options.createUuid);
                this.afterCycle(outcome);
                this.options.onResult?.(outcome.results);
                await this.pushOpEvents();
                await this.attachPendingFiles();
            } while (this.rerun && !this.stopped);
        } catch (e) {
            if (this.halt(e)) return;
            this.lastErrorMessage = e instanceof Error ? e.message : String(e);
            this.options.onError?.(e);
            this.scheduleRetry();
        } finally {
            this.running = false;
            this.endWork();
        }
    }
}
