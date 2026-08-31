// Inbound orchestrator: the side-effecting counterpart of projectGroup. It
// drains captured changes from a MaterializedChangeSource, plans them with the
// pure inverse planner, reserves minted identities durably, submits each entry
// to the group via bundle() (with the two optimistic-bundling fallbacks),
// reconciles genuine failures from rdb truth, and settles atomically.
//
//   await ingestChanges(group, backend, { writer });   // local edits -> rdb
//   await syncGroup(group, backend, { writer });        // ingest THEN project
//
// Crash safety (no cross-store transaction with the rdb DAG): minted uuids are
// persisted (reserveMint) BEFORE the append walk, so a crash between append and
// settle replays deterministically - the re-appended ops are recognized as
// already-applied by their OWNED reserved identity (write-once insert /
// not-live delete) instead of duplicated. The end-of-pass settle (ack +
// op-events + reverts + statuses) is one target transaction.
//
// Failures (a change rdb rejected) are NOT retried: the row is reverted from
// rdb truth (mark-then-revert) and recorded in the durable op-event log, the
// same channel p2p concurrency void/reinstate flips flow through (projectGroup).

import type { json } from "@hyper-hyper-space/hhs3_json";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import { ValidationRejectedError, formatValidationFailure } from "@hyper-hyper-space/hhs3_mvt";
import type { ValidationFailure } from "@hyper-hyper-space/hhs3_mvt";
import type { OwnIdentity, B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { BundleWrite, RSchemaView, RTableGroup } from "@hyper-hyper-space/hhs3_rdb";

import {
    AdapterConfig, CapturedChange, CapturedChangeId, DEFAULT_KEY_DOMAIN, IngestRejection, IngestResult,
    KeyIndex, MaterializationTarget, MaterializedChangeSource, OpEvent, RowAction, SyncMapping, SyncStatusUpdate,
} from "./types.js";
import {
    KeyLookup, MappingLookup, NodeContext, PlannedEntry, PlannedOp, PlannedReject, planDatabaseEntries,
} from "./ingest.js";
import { keyRefColumnFromTarget, rdbFkColumnFromTarget, rdbTableName } from "./names.js";
import { buildObservedBy, frontierOf, observeToVersion } from "./ref_advance.js";
import { projectGroup, rowToUpsertAction } from "./project.js";

function defaultNewUuid(): string {
    const c = (globalThis as { crypto?: { randomUUID?: () => string } }).crypto;
    if (c?.randomUUID === undefined) {
        throw new Error("no crypto.randomUUID() available; provide one via config or a newer runtime");
    }
    return c.randomUUID();
}

function asKeyIndex(source: MaterializedChangeSource): KeyIndex | undefined {
    const candidate = source as MaterializedChangeSource & Partial<KeyIndex>;
    return typeof candidate.registerKey === 'function' ? candidate as KeyIndex : undefined;
}

// The per-group context an ingest pass needs: the resolved end-schema view, for
// name reversal + FK reshaping.
type GroupContext = {
    group: RTableGroup;
    config: AdapterConfig;
    schemaView: RSchemaView;
};

async function buildGroupContext(group: RTableGroup, config: AdapterConfig): Promise<GroupContext> {
    const to = await (await group.getScopedDag()).getFrontier();
    const view = await group.getView(to, to);
    return { group, config, schemaView: view.getSchemaView() };
}

// Prefetch the sync mappings the planner needs: the SUBJECT of every
// update/delete, every existing id-resolving FK TARGET, AND now every INSERT's
// own (table, localId) - so a crash-replay reads back the reserved uuid instead
// of minting a fresh one (the read-back that makes replay idempotent).
async function prefetchBatch(
    changes: CapturedChange[],
    source: MaterializedChangeSource,
    ctxFor: (table: string) => GroupContext | undefined,
    prefetched: Map<string, SyncMapping | undefined>,
): Promise<void> {
    const prefetch = async (table: string, localId: number): Promise<void> => {
        const key = table + '\u0000' + localId;
        if (!prefetched.has(key)) prefetched.set(key, await source.resolveRow(table, localId));
    };
    for (const change of changes) {
        await prefetch(change.table, change.localId);
        if (change.kind === 'delete') continue;
        const ctx = ctxFor(change.table);
        if (ctx === undefined) continue;
        const rdbTable = rdbTableName(ctx.config, change.table);
        const fks = ctx.schemaView.hasTable(rdbTable) ? ctx.schemaView.getFKs(rdbTable) : {};
        for (const targetCol of Object.keys(change.values)) {
            const fkInfo = rdbFkColumnFromTarget(ctx.config, rdbTable, fks, targetCol);
            if (fkInfo?.resolvesToId === true && fkInfo.targetTable !== undefined) {
                await prefetch(fkInfo.targetTable, change.values[targetCol] as number);
            }
        }
    }
}

// Prefetch KeyIndex entries for every key-ref value in the batch.
async function prefetchKeys(
    changes: CapturedChange[],
    keys: KeyIndex,
    ctxFor: (table: string) => GroupContext | undefined,
    cache: Map<number, { keyHash: string; publicKey?: string } | undefined>,
): Promise<void> {
    for (const change of changes) {
        if (change.kind === 'delete') continue;
        const ctx = ctxFor(change.table);
        if (ctx === undefined) continue;
        const rdbTable = rdbTableName(ctx.config, change.table);
        const provider = ctx.schemaView.hasTable(rdbTable) ? ctx.schemaView.getIdProvider?.(rdbTable) : undefined;
        const def = ctx.schemaView.getTable?.(rdbTable);
        const identityColumns = new Set<string>();
        if (def !== undefined) {
            for (const [col, colDef] of Object.entries(def.columns)) {
                if (colDef.type === 'identity') identityColumns.add(col);
            }
        }
        for (const targetCol of Object.keys(change.values)) {
            const keyRef = keyRefColumnFromTarget(ctx.config, rdbTable, provider, targetCol, identityColumns);
            if (keyRef === undefined) continue;
            const keyId = change.values[targetCol] as number;
            if (cache.has(keyId)) continue;
            const keyHash = await keys.keyHashForId(DEFAULT_KEY_DOMAIN, keyId);
            if (keyHash === undefined) {
                cache.set(keyId, undefined);
                continue;
            }
            const publicKey = await keys.publicKeyForId(DEFAULT_KEY_DOMAIN, keyId);
            const info: { keyHash: string; publicKey?: string } = { keyHash };
            if (publicKey !== undefined) info.publicKey = publicKey;
            cache.set(keyId, info);
        }
    }
}

// A backend that both materializes and captures (bidirectional).
export type BidirectionalTarget = MaterializationTarget & MaterializedChangeSource;

// Whether a validation failure chain mentions `needle` (walking parents).
function whyIncludes(why: ValidationFailure, needle: string): boolean {
    let cur: ValidationFailure | undefined = why;
    while (cur !== undefined) {
        if (cur.reason.includes(needle)) return true;
        cur = cur.parent;
    }
    return false;
}

// bundle() reported the row as already write-once (an insert we OWN a reserved
// identity for was already appended on a prior crashed pass) -> already applied.
function isWriteOnceFor(why: ValidationFailure, rowId: B64Hash): boolean {
    return whyIncludes(why, 'already exists or was deleted') && whyIncludes(why, `'${rowId}'`);
}

// bundle() reported the delete subject as not live (already deleted) -> applied.
function isNotLiveFor(why: ValidationFailure, rowId: B64Hash): boolean {
    return whyIncludes(why, 'is not live') && whyIncludes(why, `'${rowId}'`);
}

// The unsigned single-op bundle payload, for the op JSON stored in a failure
// event and for a synthetic (never-appended) op hash.
function bundlePayload(write: BundleWrite): json.Literal {
    return { action: 'bundle', writes: [{ table: write.table, op: write.op }] } as unknown as json.Literal;
}

async function syntheticOpHash(group: RTableGroup, write: BundleWrite): Promise<B64Hash> {
    const dag = await group.getScopedDag();
    // Position-free (payload) hash: stable across crash-replays regardless of
    // where in the frontier the (never-appended) op would have landed.
    return dag.computeEntryHash(bundlePayload(write));
}

// A genuinely-failed row to reconcile at end of pass.
type Mark = { groupId: B64Hash; rdbTable: string; targetTable: string; rowId: B64Hash; localId: number };

// Per-group / per-database single-flight lock: a sync cycle must not overlap
// itself (duplicate rowId minting / racing projections). Chains calls per key.
const locks = new Map<string, Promise<unknown>>();

async function withLock<T>(key: string, fn: () => Promise<T>): Promise<T> {
    const prev = locks.get(key) ?? Promise.resolve();
    const run = prev.then(fn, fn);
    const guard = run.catch(() => undefined);
    locks.set(key, guard);
    try {
        return await run;
    } finally {
        if (locks.get(key) === guard) locks.delete(key);
    }
}

// Drain, translate, submit, reconcile, and settle local changes as rdb ops.
// Single-group convenience wrapper over the replica-wide path (one member).
export async function ingestChanges(
    group: RTableGroup,
    source: MaterializedChangeSource,
    config: AdapterConfig = {},
    newUuid: () => string = defaultNewUuid,
): Promise<IngestResult> {
    if (config.writer === undefined) throw new Error("ingestChanges requires config.writer");
    const results = await ingestDatabaseChanges([{ group, config }], source, newUuid);
    return results.get(group.getId()) ?? { accepted: 0, rejected: [] };
}

// Whether the backend has un-ingested local changes waiting.
async function hasPendingChanges(backend: MaterializedChangeSource): Promise<boolean> {
    try {
        return (await backend.drainChanges()).changes.length > 0;
    } catch {
        return false;
    }
}

// The coordinated sync cycle: ingest local edits into rdb, THEN project rdb back
// into the target. Single-flighted per group.
export async function syncGroup(
    group: RTableGroup,
    backend: BidirectionalTarget,
    config: AdapterConfig = {},
    newUuid: () => string = defaultNewUuid,
): Promise<IngestResult> {
    return withLock(group.getId(), async () => {
        let result: IngestResult = { accepted: 0, rejected: [] };
        if (config.writer !== undefined) {
            result = await ingestChanges(group, backend, config, newUuid);
        }

        if ((await backend.getCheckpoint(group.getId())) === undefined && await hasPendingChanges(backend)) {
            throw new Error(
                "refusing initial projection over un-ingested changes; "
                + "configure a writer so ingestChanges drains them first");
        }

        await projectGroup(group, backend, config);
        return result;
    });
}

// ---------------------------------------------------------------------------
// Replica-wide (multi-group) orchestration
// ---------------------------------------------------------------------------

// One group of a replica-wide projection: the group plus its resolved config.
export type GroupProjection = {
    group: RTableGroup;
    config: AdapterConfig;
};

// Topologically order members so a group is projected AFTER every co-projected
// group it references (via bindings).
function orderByBindings(members: GroupProjection[]): GroupProjection[] {
    const byId = new Map<B64Hash, GroupProjection>();
    for (const m of members) byId.set(m.group.getId(), m);

    const ordered: GroupProjection[] = [];
    const done = new Set<B64Hash>();
    const active = new Set<B64Hash>();

    const visit = (m: GroupProjection): void => {
        const id = m.group.getId();
        if (done.has(id) || active.has(id)) return;
        active.add(id);
        for (const foreignId of Object.values(m.group.getBindings())) {
            const dep = byId.get(foreignId);
            if (dep !== undefined) visit(dep);
        }
        active.delete(id);
        done.add(id);
        ordered.push(m);
    };

    for (const m of members) visit(m);
    return ordered;
}

// Project every member group into the shared target, referenced groups first.
export async function projectDatabase(
    members: GroupProjection[],
    target: MaterializationTarget,
): Promise<void> {
    for (const m of orderByBindings(members)) {
        await projectGroup(m.group, target, m.config);
    }
}

// Drain the shared outbox ONCE and replay it op-by-op in COMMIT ORDER against
// each change's owning group. As the walk appends a group G's entry it
// interleaves cross-group ref-advances driven by a dirty map. A change whose
// table belongs to no member is acked-and-dropped. See the file header for the
// crash-safety / failure-reconciliation contract.
export async function ingestDatabaseChanges(
    members: GroupProjection[],
    source: MaterializedChangeSource,
    newUuid: () => string = defaultNewUuid,
): Promise<Map<B64Hash, IngestResult>> {
    const accepted = new Map<B64Hash, number>();
    const rejects = new Map<B64Hash, IngestRejection[]>();
    for (const m of members) {
        accepted.set(m.group.getId(), 0);
        rejects.set(m.group.getId(), []);
    }
    const toResults = (): Map<B64Hash, IngestResult> => {
        const out = new Map<B64Hash, IngestResult>();
        for (const m of members) {
            const id = m.group.getId();
            out.set(id, { accepted: accepted.get(id)!, rejected: rejects.get(id)! });
        }
        return out;
    };

    const batch = await source.drainChanges();
    if (batch.changes.length === 0) return toResults();

    // Build each member's context, a table -> owning group index, and lookups.
    const contexts = new Map<B64Hash, GroupContext>();
    const groupById = new Map<B64Hash, RTableGroup>();
    const writerById = new Map<B64Hash, OwnIdentity>();
    const tableToGroup = new Map<string, B64Hash>();
    for (const m of members) {
        const ctx = await buildGroupContext(m.group, m.config);
        const groupId = m.group.getId();
        contexts.set(groupId, ctx);
        groupById.set(groupId, m.group);
        if (m.config.writer !== undefined) writerById.set(groupId, m.config.writer);
        for (const rdbTable of ctx.schemaView.getTableNames()) {
            tableToGroup.set(ctx.config.tableNames?.[rdbTable] ?? rdbTable, groupId);
        }
    }
    const ctxForTable = (table: string): GroupContext | undefined => {
        const groupId = tableToGroup.get(table);
        return groupId === undefined ? undefined : contexts.get(groupId);
    };
    const writerFor = (groupId: B64Hash): OwnIdentity => {
        const writer = writerById.get(groupId);
        if (writer === undefined) throw new Error(`ingestDatabaseChanges: group '${groupId}' has no config.writer`);
        return writer;
    };

    // Global prefetch (FK targets and insert read-back may span sibling groups).
    const prefetched = new Map<string, SyncMapping | undefined>();
    await prefetchBatch(batch.changes, source, ctxForTable, prefetched);
    const lookup: MappingLookup = (table, localId) => prefetched.get(table + '\u0000' + localId);

    const keyCache = new Map<number, { keyHash: string; publicKey?: string } | undefined>();
    const keys = asKeyIndex(source);
    if (keys !== undefined) await prefetchKeys(batch.changes, keys, ctxForTable, keyCache);
    const keyLookup: KeyLookup = (keyId) => keyCache.get(keyId);

    // Plan the whole interleaved batch (order-preserving + optimistic bundlings).
    const nodeCtxFor = (table: string): NodeContext | undefined => {
        const groupId = tableToGroup.get(table);
        if (groupId === undefined) return undefined;
        return {
            groupId, schemaView: contexts.get(groupId)!.schemaView,
            config: contexts.get(groupId)!.config, writerKeyId: writerFor(groupId).keyId,
        };
    };
    const fkBundlingFor = (groupId: string): boolean => contexts.get(groupId as B64Hash)?.config.fkBundling !== false;
    const updateMergeFor = (table: string): boolean => ctxForTable(table)?.config.updateMerge !== false;
    const plan = planDatabaseEntries(batch, nodeCtxFor, lookup, keyLookup, newUuid, fkBundlingFor, updateMergeFor);

    // Durably reserve every minted identity BEFORE the append walk.
    if (plan.reservations.length > 0) await source.reserveMint(plan.reservations);

    // --- failure / recovery accumulators ---
    const failureEvents: OpEvent[] = [];
    const marks = new Map<B64Hash, Mark>();   // keyed by rowId

    const recordFailure = async (f: {
        groupId: B64Hash; kind: 'insert' | 'update' | 'delete'; targetTable: string; rdbTable: string;
        rowId?: B64Hash; localId: number; write?: BundleWrite; failure: ValidationFailure; source?: CapturedChange;
    }): Promise<void> => {
        const group = groupById.get(f.groupId)!;
        const opHash = f.write !== undefined ? await syntheticOpHash(group, f.write) : f.rowId ?? `${f.targetTable}#${f.localId}`;
        const event: OpEvent = {
            origin: 'ingestion', direction: 'failure', groupId: f.groupId, opHash, kind: f.kind,
            table: f.targetTable, localId: f.localId, author: writerFor(f.groupId).keyId,
            reason: { source: 'validation', failure: f.failure },
        };
        if (f.rowId !== undefined) event.rowId = f.rowId;
        if (f.write !== undefined) event.op = bundlePayload(f.write);
        failureEvents.push(event);
        rejects.get(f.groupId)!.push({ change: f.source, groupId: f.groupId, reason: formatValidationFailure(f.failure) });
        if (f.rowId !== undefined) {
            marks.set(f.rowId, { groupId: f.groupId, rdbTable: f.rdbTable, targetTable: f.targetTable,
                rowId: f.rowId, localId: f.localId });
        }
    };

    // Planner (translate-time) rejects are ingestion failures too.
    for (const rej of plan.rejects) {
        const groupId = rej.change !== undefined ? tableToGroup.get(rej.change.table) : undefined;
        if (groupId === undefined || rej.table === undefined || rej.kind === undefined || rej.localId === undefined) {
            if (groupId !== undefined) rejects.get(groupId)!.push({ change: rej.change, reason: rej.reason });
            continue;
        }
        await recordFailure({
            groupId, kind: rej.kind, targetTable: rej.table, rdbTable: rej.op?.table ?? rdbTableName(contexts.get(groupId)!.config, rej.table),
            rowId: rej.rowId, localId: rej.localId, write: rej.op, failure: { reason: rej.reason }, source: rej.change,
        });
    }

    // --- submit helpers ---
    const bump = (groupId: B64Hash, n: number): void => { accepted.set(groupId, accepted.get(groupId)! + n); };

    const attempt = async (group: RTableGroup, writes: BundleWrite[], writer: OwnIdentity):
        Promise<{ ok: true } | { ok: false; why: ValidationFailure }> => {
        try {
            await group.bundle(writes, writer);
            return { ok: true };
        } catch (e) {
            if (e instanceof ValidationRejectedError) return { ok: false, why: e.why };
            return { ok: false, why: { reason: e instanceof Error ? e.message : String(e) } };
        }
    };

    // Submit a single op; on reject apply the idempotent-replay skips and the
    // per-field update fallback, else record a genuine failure.
    const submitOp = async (groupId: B64Hash, op: PlannedOp): Promise<void> => {
        const group = groupById.get(groupId)!;
        const writer = writerFor(groupId);
        const rowId = op.write.op.rowId;
        const res = await attempt(group, [op.write], writer);
        if (res.ok) { bump(groupId, 1); return; }

        if (op.kind === 'insert' && op.reusedIdentity === true && isWriteOnceFor(res.why, rowId)) {
            return;   // already applied on a prior crashed pass: idempotent skip
        }
        if (op.kind === 'delete' && isNotLiveFor(res.why, rowId)) {
            return;   // already deleted: idempotent skip
        }
        if (op.kind === 'update' && op.fallback !== undefined) {
            for (const f of op.fallback) await submitOp(groupId, f);   // per-field fallback
            return;
        }
        await recordFailure({
            groupId, kind: op.kind, targetTable: op.table, rdbTable: op.rdbTable, rowId,
            localId: op.localId, write: op.write, failure: res.why, source: op.sources[op.sources.length - 1],
        });
    };

    // Submit one planned entry: try the FK bundle whole, else fall back to its
    // constituent ops individually (in order).
    const submitEntry = async (entry: PlannedEntry): Promise<void> => {
        const groupId = entry.groupId as B64Hash;
        const group = groupById.get(groupId)!;
        const writer = writerFor(groupId);
        if (entry.ops.length > 1) {
            const res = await attempt(group, entry.ops.map((o) => o.write), writer);
            if (res.ok) { bump(groupId, entry.ops.length); return; }
            for (const op of entry.ops) await submitOp(groupId, op);   // fkBundling fallback
            return;
        }
        await submitOp(groupId, entry.ops[0]);
    };

    // --- cross-group ref-advance state (unchanged interleaving) ---
    const observedBy = buildObservedBy(members);
    const dirty = new Map<B64Hash, Map<B64Hash, Version>>();
    const bindingNameFor = (observerId: B64Hash, foreignId: B64Hash): string | undefined => {
        for (const [name, fid] of Object.entries(groupById.get(observerId)!.getBindings())) {
            if (fid === foreignId) return name;
        }
        return undefined;
    };
    const propagate = async (groupId: B64Hash): Promise<void> => {
        const observers = observedBy.get(groupId);
        if (observers === undefined || observers.length === 0) return;
        const gFrontier = await frontierOf(groupById.get(groupId)!);
        for (const { observerId } of observers) {
            let pending = dirty.get(observerId);
            if (pending === undefined) { pending = new Map(); dirty.set(observerId, pending); }
            pending.set(groupId, gFrontier);
        }
    };
    const flush = async (
        observerId: B64Hash, forChanges: CapturedChange[] | undefined, guard?: Set<string>,
    ): Promise<void> => {
        const pending = dirty.get(observerId);
        if (pending === undefined || pending.size === 0) return;
        dirty.delete(observerId);
        const observer = groupById.get(observerId)!;
        const author = writerFor(observerId);
        for (const [foreignId, version] of pending) {
            const pairKey = observerId + '\u0000' + foreignId;
            if (guard !== undefined) {
                if (guard.has(pairKey)) continue;
                guard.add(pairKey);
            }
            const bindingName = bindingNameFor(observerId, foreignId);
            if (bindingName === undefined) continue;
            const failure = await observeToVersion(observer, bindingName, foreignId, version, author);
            if (failure !== undefined) {
                rejects.get(observerId)!.push({
                    change: forChanges?.[forChanges.length - 1],
                    groupId: observerId,
                    reason: `ref-advance to bound group '${foreignId}' failed: ${failure.reason}`,
                });
            }
        }
    };

    // Linear walk: for each entry, make the owning group's observations current,
    // submit the entry (with fallbacks), then propagate its new frontier.
    for (const entry of plan.entries) {
        const groupId = entry.groupId as B64Hash;
        await flush(groupId, entry.ops.flatMap((o) => o.sources));
        await submitEntry(entry);
        await propagate(groupId);
    }

    // Drain: flush observers that never wrote, transitively.
    const guard = new Set<string>();
    for (;;) {
        const pendingObservers = [...dirty.keys()].filter((o) => (dirty.get(o)?.size ?? 0) > 0);
        if (pendingObservers.length === 0) break;
        const settled = pendingObservers.find((o) =>
            ![...dirty.get(o)!.keys()].some((observed) => (dirty.get(observed)?.size ?? 0) > 0));
        const pick = settled ?? pendingObservers[0];
        await flush(pick, undefined, guard);
        await propagate(pick);
    }

    // --- mark-then-revert recovery (quiescence-gated) ---
    const consumed: CapturedChangeId[] = batch.changes.map((c) => c.id);
    const consumedSet = new Set<CapturedChangeId>(consumed);
    // A row with an un-consumed (newer) outbox change pending is left alone: a
    // later cycle ingests that write, so reverting now would clobber it.
    const pendingRows = new Set<string>();
    try {
        for (const c of (await source.drainChanges()).changes) {
            if (!consumedSet.has(c.id)) pendingRows.add(c.table + '\u0000' + c.localId);
        }
    } catch { /* not capture-provisioned: no pending rows */ }

    const reverts: RowAction[] = [];
    const statuses: SyncStatusUpdate[] = [];
    for (const mark of marks.values()) {
        if (pendingRows.has(mark.targetTable + '\u0000' + mark.localId)) continue;   // quiescence gate
        const group = groupById.get(mark.groupId)!;
        const ctx = contexts.get(mark.groupId)!;
        const to = await (await group.getScopedDag()).getFrontier();
        const view = await group.getView(to, to);
        const tableView = await view.getTableView(mark.rdbTable);
        const row = await tableView.getRow(mark.rowId);
        if (row === undefined) {
            // The op never landed (an insert orphan, or an already-deleted row):
            // remove the local app row and mark the sync record ingestion_failure.
            reverts.push({ kind: 'delete-row', table: mark.targetTable, rowId: mark.rowId });
            statuses.push({ table: mark.targetTable, rowId: mark.rowId, status: 'ingestion_failure' });
        } else {
            // The row exists in rdb: re-materialize it fully (undo the local edit).
            reverts.push(rowToUpsertAction(row, mark.rdbTable, view.getSchemaView(), ctx.config));
        }
    }

    // --- atomic settle ---
    await source.commitIngest({ consumed, events: failureEvents, reverts, statuses });

    return toResults();
}

// The replica-wide coordinated cycle: ingest all members' local edits, THEN
// project every member back into the shared target. Single-flighted across the
// whole database (the shared outbox is drained once).
export async function syncDatabase(
    members: GroupProjection[],
    backend: BidirectionalTarget,
    newUuid: () => string = defaultNewUuid,
): Promise<Map<B64Hash, IngestResult>> {
    const lockKey = members.map((m) => m.group.getId()).sort().join('|');
    return withLock('db:' + lockKey, async () => {
        const ingestable = members.filter((m) => m.config.writer !== undefined);
        let results = new Map<B64Hash, IngestResult>();
        if (ingestable.length > 0) {
            results = await ingestDatabaseChanges(ingestable, backend, newUuid);
        }

        const anyUnmaterialized = await (async (): Promise<boolean> => {
            for (const m of members) {
                if ((await backend.getCheckpoint(m.group.getId())) === undefined) return true;
            }
            return false;
        })();
        if (anyUnmaterialized && await hasPendingChanges(backend)) {
            throw new Error(
                "refusing initial projection over un-ingested changes; "
                + "configure a writer on every member so ingestDatabaseChanges drains them first");
        }

        await projectDatabase(members, backend);
        return results;
    });
}
