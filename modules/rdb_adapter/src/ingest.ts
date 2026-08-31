// Pure inverse planner: captured target-side changes -> ORDERED rdb write ops.
// The inbound sibling of row_actions.ts. No IO, no target, no signing (bundle()
// signs); given prefetched sync-mapping / key lookups it is a pure function of
// (batch, schemaView, config).
//
// FAITHFULNESS: the outbox is drained in commit order and replayed op-by-op in
// that order - one net op per captured change - with only TWO optional
// optimistic bundlings, each with a safe fallback the orchestrator applies:
//
//   1. updateMerge (config.updateMerge !== false): a run of CONSECUTIVE update
//      changes on the SAME row whose column sets are DISJOINT folds into ONE
//      rdb update op (a repeated column / a different row / a non-update breaks
//      the run). The merged op keeps its constituents as `fallback` ops so the
//      orchestrator can re-submit them per-change if the merged op is rejected.
//   2. fkBundling (config.fkBundling !== false): a run of CONSECUTIVE inserts
//      linked by an explicit same-group FK arc bundles into ONE atomic entry
//      (groupEntries). On a bundle reject the orchestrator re-submits the
//      constituent ops individually, in order.
//
// It NEVER folds an insert with a later update (an insert keeps its capture-time
// image; later edits are their own ops) and NEVER cancels an insert+delete
// (both are submitted faithfully). Identity is the HHS opId, not the rowId; a
// minted insert's uuid is REUSED from an existing reserved sync mapping when
// present (crash-replay read-back) so replays reproduce the identical payload.
//
// Validity (do the FKs/restrictions actually hold) is NOT decided here - rdb
// decides it at bundle(); rejects flow back through the orchestrator.

import type { json } from "@hyper-hyper-space/hhs3_json";
import type { KeyId } from "@hyper-hyper-space/hhs3_crypto";
import {
    BundleWrite, deriveRowId, InsertRowPayload, RowOpPayload, RSchemaView,
} from "@hyper-hyper-space/hhs3_rdb";

import {
    AdapterConfig, CapturedBatch, CapturedChange, IngestRejection, SyncMapping,
} from "./types.js";
import {
    isSystemColumn, keyRefColumnFromTarget, rdbColumnName, rdbFkColumnFromTarget, rdbTableName,
} from "./names.js";

// One translated, ready-to-submit op plus its bookkeeping. `deps` are the
// in-batch same-group local-FK row keys this op references (they drive
// FK-consecutive bundling). `mapping` is present for a minted insert.
// `reusedIdentity` marks an insert whose uuid/rowId came from an EXISTING
// reserved sync mapping (a crash-replay read-back) - so if bundle() reports the
// rowId as already-present the orchestrator treats it as already-applied
// (idempotent) rather than a duplicate. `fallback` (a merged update only) holds
// the per-change ops to re-submit if the merged op is rejected.
export type PlannedOp = {
    key: string;              // keyOf(target table, localId)
    table: string;            // target (projected) table
    localId: number;
    groupId: string;
    rdbTable: string;
    kind: 'insert' | 'update' | 'delete';
    write: BundleWrite;
    sources: CapturedChange[];
    mapping?: SyncMapping;
    reusedIdentity?: boolean;
    fallback?: PlannedOp[];
    deps: string[];
};

// One unit submitted as a single DAG entry: a bundle of >= 1 ordered ops. A
// bundle is always single-group; the orchestrator uses `groupId` to interleave
// cross-group ref-advances. `ops.length > 1` iff FK-bundled.
export type PlannedEntry = {
    groupId: string;
    ops: PlannedOp[];
};

// A translate-time reject enriched for the op-event log: the constructed
// (never-appended) op and the row it targeted, so the orchestrator can record a
// failure event (full op JSON + synthetic hash) and revert the row.
export type PlannedReject = IngestRejection & {
    op?: BundleWrite;
    table?: string;
    localId?: number;
    rowId?: string;
    kind?: 'insert' | 'update' | 'delete';
    author?: KeyId;
};

export type IngestPlan = {
    entries: PlannedEntry[];
    rejects: PlannedReject[];
    // Every minted insert identity (accepted AND rejected), for reserveMint to
    // durably persist BEFORE the append walk - so a crash-replay reads the same
    // uuid back, and a rejected insert still has a sync row to revert against.
    reservations: SyncMapping[];
};

// Resolve (target table, localId) -> the existing sync mapping, prefetched by
// the orchestrator. Used for the SUBJECT of an update/delete, for resolving an
// existing local FK target, AND for crash-replay uuid read-back of an insert.
export type MappingLookup = (table: string, localId: number) => SyncMapping | undefined;

// Prefetched KeyIndex lookups for key-ref reverse: key id -> {keyHash, publicKey}.
export type KeyLookup = (keyId: number) => { keyHash: string; publicKey?: string } | undefined;

// Everything the planner needs about the group that owns a target table.
export type NodeContext = {
    groupId: string;
    schemaView: RSchemaView;
    config: AdapterConfig;
    writerKeyId: KeyId;
};

function keyOf(table: string, localId: number): string {
    return table + '\u0000' + localId;
}

function disjoint(a: { [k: string]: unknown }, b: { [k: string]: unknown }): boolean {
    for (const k of Object.keys(b)) if (k in a) return false;
    return true;
}

// ---------------------------------------------------------------------------
// Step 1: order-preserving planning (NO reordering, NO lossy coalescing).
// ---------------------------------------------------------------------------

// One original update change folded into a (possibly merged) update net op.
type UpdatePart = { values: { [column: string]: json.Literal }; sources: CapturedChange[] };

type NetChange = {
    table: string;
    localId: number;
    kind: 'insert' | 'update' | 'delete';
    values: { [column: string]: json.Literal };
    sources: CapturedChange[];
    // Update only: the constituent per-change updates (>1 iff a merge happened),
    // used to build the merged op's per-change fallback.
    parts?: UpdatePart[];
};

// Emit one net op per captured change, in order, merging ONLY a run of
// consecutive same-row disjoint-field updates (when updateMerge is on for that
// row's group). Inserts and deletes are always standalone.
function planOps(batch: CapturedBatch, updateMergeFor: (table: string) => boolean): NetChange[] {
    const out: NetChange[] = [];
    for (const change of batch.changes) {
        if (change.kind === 'insert') {
            out.push({ table: change.table, localId: change.localId, kind: 'insert',
                values: { ...change.values }, sources: [change] });
            continue;
        }
        if (change.kind === 'delete') {
            out.push({ table: change.table, localId: change.localId, kind: 'delete',
                values: {}, sources: [change] });
            continue;
        }
        // update: merge with the immediately-preceding net op iff it is an
        // update on the same row with a disjoint column set (any other change
        // in between - a different row, an insert/delete - breaks the run).
        const last = out[out.length - 1];
        const canMerge = last !== undefined
            && last.kind === 'update'
            && last.table === change.table
            && last.localId === change.localId
            && updateMergeFor(change.table)
            && disjoint(last.values, change.values);
        if (canMerge) {
            Object.assign(last.values, change.values);
            last.sources.push(change);
            last.parts!.push({ values: { ...change.values }, sources: [change] });
            continue;
        }
        out.push({ table: change.table, localId: change.localId, kind: 'update',
            values: { ...change.values }, sources: [change],
            parts: [{ values: { ...change.values }, sources: [change] }] });
    }
    return out;
}

// ---------------------------------------------------------------------------
// Step 2: mint every insert's uuid/rowId up front (reusing a reserved uuid on
// replay), so an FK reference to a sibling inserted anywhere in the batch
// resolves regardless of commit order.
// ---------------------------------------------------------------------------

type Minted = { rowId: string; uuid: string; reused: boolean };

function mintInserts(
    nets: NetChange[],
    ctxFor: (table: string) => NodeContext | undefined,
    lookup: MappingLookup,
    newUuid: () => string,
): { minted: Map<string, Minted>; reservations: SyncMapping[] } {
    const minted = new Map<string, Minted>();
    const reservations: SyncMapping[] = [];
    for (const net of nets) {
        if (net.kind !== 'insert') continue;
        const ctx = ctxFor(net.table);
        if (ctx === undefined) continue;
        const existing = lookup(net.table, net.localId);
        const reusable = existing !== undefined && existing.uuid !== ''
            && (existing.status === undefined || existing.status === 'active');
        const uuid = reusable ? existing!.uuid : newUuid();
        const rowId = deriveRowId(uuid, ctx.writerKeyId);
        minted.set(keyOf(net.table, net.localId), { uuid, rowId, reused: reusable });
        reservations.push({ table: net.table, localId: net.localId, rowId, uuid,
            author: ctx.writerKeyId, status: 'active' });
    }
    return { minted, reservations };
}

// ---------------------------------------------------------------------------
// Step 3: reverse a set of target columns to rdb columns/values, resolving FK
// and key-ref values. Returns the rdb values + same-group FK deps, or the first
// offending column's reject reason.
// ---------------------------------------------------------------------------

type Reversed = { ok: true; values: { [column: string]: json.Literal }; deps: string[] } | { ok: false; reason: string };

// Reverse a set of target columns into rdb columns/values for `rdbTable`.
function reverseCols(
    rdbTable: string,
    targetValues: { [column: string]: json.Literal },
    ctx: NodeContext,
    isUpdate: boolean,
    lookup: MappingLookup,
    keyLookup: KeyLookup,
    minted: Map<string, Minted>,
): Reversed {
    const { schemaView, config } = ctx;
    const def = schemaView.getTable?.(rdbTable);
    const fks = schemaView.getFKs?.(rdbTable) ?? {};
    const provider = schemaView.getIdProvider?.(rdbTable);
    const identityColumns = new Set<string>();
    if (def !== undefined) {
        for (const [col, colDef] of Object.entries(def.columns)) {
            if (colDef.type === 'identity') identityColumns.add(col);
        }
    }

    const values: { [column: string]: json.Literal } = {};
    const deps: string[] = [];
    for (const targetCol of Object.keys(targetValues)) {
        const fkInfo = rdbFkColumnFromTarget(config, rdbTable, fks, targetCol);
        if (fkInfo !== undefined) {
            const rdbCol = fkInfo.rdbColumn;
            if (isUpdate && def?.columns?.[rdbCol]?.readonly) {
                return { ok: false, reason: `cannot update readonly column '${rdbTable}.${rdbCol}'` };
            }
            if (fkInfo.resolvesToId) {
                const targetTable = fkInfo.targetTable!;
                const localRef = targetValues[targetCol] as number;
                const tKey = keyOf(targetTable, localRef);
                const targetRowId = minted.get(tKey)?.rowId ?? lookup(targetTable, localRef)?.rowId;
                if (targetRowId === undefined) {
                    return { ok: false, reason: `dangling FK '${rdbTable}.${rdbCol}' -> ${targetTable}#${localRef}` };
                }
                values[rdbCol] = targetRowId;
                if (!fkInfo.crossGroup) deps.push(tKey);
            } else {
                values[rdbCol] = targetValues[targetCol];
            }
            continue;
        }

        const keyRef = keyRefColumnFromTarget(config, rdbTable, provider, targetCol, identityColumns);
        if (keyRef !== undefined) {
            const rdbCol = keyRef.rdbColumn;
            if (isUpdate && def?.columns?.[rdbCol]?.readonly) {
                return { ok: false, reason: `cannot update readonly column '${rdbTable}.${rdbCol}'` };
            }
            const keyId = targetValues[targetCol] as number;
            const info = keyLookup(keyId);
            if (info === undefined) {
                return { ok: false, reason: `unknown key id ${keyId} for '${rdbTable}.${rdbCol}'` };
            }
            if (keyRef.isProviderKeyId) {
                if (info.publicKey === undefined) {
                    return { ok: false, reason:
                        `key id ${keyId} has no public key; call registerKey before inserting into `
                        + `identity-provider table '${rdbTable}'` };
                }
                values[provider!.keyIdColumn] = info.keyHash;
                values[provider!.publicKeyColumn] = info.publicKey;
            } else {
                values[rdbCol] = info.keyHash;
            }
            continue;
        }

        if (isSystemColumn(config, targetCol)) continue;
        const rdbCol = rdbColumnName(config, rdbTable, targetCol);
        if (isUpdate && def?.columns?.[rdbCol]?.readonly) {
            return { ok: false, reason: `cannot update readonly column '${rdbTable}.${rdbCol}'` };
        }
        values[rdbCol] = targetValues[targetCol];
    }
    return { ok: true, values, deps };
}

function translateNet(
    net: NetChange,
    ctx: NodeContext,
    lookup: MappingLookup,
    keyLookup: KeyLookup,
    minted: Map<string, Minted>,
): { op?: PlannedOp; rejects: PlannedReject[] } {
    const { config, writerKeyId, groupId } = ctx;
    const key = keyOf(net.table, net.localId);
    const rdbTable = rdbTableName(config, net.table);
    const lastSource = net.sources[net.sources.length - 1];

    if (!ctx.schemaView.hasTable(rdbTable)) {
        return { rejects: [{ change: lastSource, table: net.table, localId: net.localId,
            reason: `no rdb table '${rdbTable}' for target table '${net.table}'` }] };
    }

    // Subject of an update/delete: a row inserted EARLIER in this same batch
    // (minted) resolves first (faithful insert-then-update / insert-then-delete),
    // else an already-materialized row via the prefetched lookup.
    const subjectRowId = (): string | undefined =>
        minted.get(key)?.rowId ?? lookup(net.table, net.localId)?.rowId;

    if (net.kind === 'delete') {
        const rowId = subjectRowId();
        if (rowId === undefined) {
            return { rejects: [{ change: lastSource, table: net.table, localId: net.localId, kind: 'delete',
                reason: `cannot delete unknown local row ${net.table}#${net.localId}` }] };
        }
        const op: RowOpPayload = { action: 'delete', rowId };
        return { op: { key, groupId, rdbTable, table: net.table, localId: net.localId, kind: 'delete',
            write: { table: rdbTable, op }, sources: net.sources, deps: [] }, rejects: [] };
    }

    if (net.kind === 'insert') {
        const rev = reverseCols(rdbTable, net.values, ctx, false, lookup, keyLookup, minted);
        const m = minted.get(key)!;
        if (!rev.ok) {
            const failWrite: BundleWrite = { table: rdbTable,
                op: { action: 'insert', rowId: m.rowId, uuid: m.uuid, values: {} } };
            return { rejects: [{ change: lastSource, table: net.table, localId: net.localId, rowId: m.rowId,
                kind: 'insert', author: writerKeyId, op: failWrite, reason: rev.reason }] };
        }
        const op: InsertRowPayload = { action: 'insert', rowId: m.rowId, uuid: m.uuid, values: rev.values };
        const mapping: SyncMapping = { table: net.table, localId: net.localId, rowId: m.rowId,
            uuid: m.uuid, author: writerKeyId, status: 'active' };
        return { op: { key, groupId, rdbTable, table: net.table, localId: net.localId, kind: 'insert',
            write: { table: rdbTable, op }, sources: net.sources, mapping, reusedIdentity: m.reused,
            deps: rev.deps }, rejects: [] };
    }

    // update: translate each constituent part; a part that reverses OK
    // contributes its columns to the merged op AND becomes a fallback op; a part
    // that reverses to a reject (readonly / dangling / unknown key) is recorded
    // as its own failure (so the good parts still land).
    const rowId = subjectRowId();
    if (rowId === undefined) {
        return { rejects: [{ change: lastSource, table: net.table, localId: net.localId, kind: 'update',
            reason: `cannot update unknown local row ${net.table}#${net.localId}` }] };
    }
    const parts = net.parts ?? [{ values: net.values, sources: net.sources }];
    const rejects: PlannedReject[] = [];
    const okParts: { values: { [c: string]: json.Literal }; deps: string[]; sources: CapturedChange[] }[] = [];
    for (const part of parts) {
        const rev = reverseCols(rdbTable, part.values, ctx, true, lookup, keyLookup, minted);
        if (!rev.ok) {
            const src = part.sources[part.sources.length - 1];
            const failWrite: BundleWrite = { table: rdbTable,
                op: { action: 'update', rowId, values: {} } };
            rejects.push({ change: src, table: net.table, localId: net.localId, rowId,
                kind: 'update', op: failWrite, reason: rev.reason });
            continue;
        }
        if (Object.keys(rev.values).length === 0) continue;   // only system cols touched
        okParts.push({ values: rev.values, deps: rev.deps, sources: part.sources });
    }

    if (okParts.length === 0) return { rejects };

    const mergedValues: { [c: string]: json.Literal } = {};
    const mergedDeps: string[] = [];
    const mergedSources: CapturedChange[] = [];
    for (const p of okParts) {
        Object.assign(mergedValues, p.values);
        mergedDeps.push(...p.deps);
        mergedSources.push(...p.sources);
    }
    const mergedWrite: BundleWrite = { table: rdbTable, op: { action: 'update', rowId, values: mergedValues } };

    // Fallback ops (per surviving part) only when a merge actually happened.
    let fallback: PlannedOp[] | undefined;
    if (okParts.length > 1) {
        fallback = okParts.map((p) => ({
            key, groupId, rdbTable, table: net.table, localId: net.localId, kind: 'update' as const,
            write: { table: rdbTable, op: { action: 'update' as const, rowId, values: p.values } },
            sources: p.sources, deps: p.deps,
        }));
    }

    const op: PlannedOp = { key, groupId, rdbTable, table: net.table, localId: net.localId, kind: 'update',
        write: mergedWrite, sources: mergedSources, deps: mergedDeps };
    if (fallback !== undefined) op.fallback = fallback;
    return { op, rejects };
}

// ---------------------------------------------------------------------------
// Step 2 + 3 driver.
// ---------------------------------------------------------------------------

function planNodes(
    nets: NetChange[],
    ctxFor: (targetTable: string) => NodeContext | undefined,
    lookup: MappingLookup,
    keyLookup: KeyLookup,
    newUuid: () => string,
): { ops: PlannedOp[]; rejects: PlannedReject[]; reservations: SyncMapping[] } {
    const { minted, reservations } = mintInserts(nets, ctxFor, lookup, newUuid);

    const ops: PlannedOp[] = [];
    const rejects: PlannedReject[] = [];
    for (const net of nets) {
        const ctx = ctxFor(net.table);
        if (ctx === undefined) continue;   // acked-and-dropped (untranslatable)
        const result = translateNet(net, ctx, lookup, keyLookup, minted);
        rejects.push(...result.rejects);
        if (result.op !== undefined) ops.push(result.op);
    }
    return { ops, rejects, reservations };
}

// ---------------------------------------------------------------------------
// Step 4: FK-consecutive bundling (a single commit-order pass, no reordering).
// A node joins the open bundle iff bundling is enabled for its group AND it
// targets the SAME group AND it has an explicit FK arc into the open bundle.
// ---------------------------------------------------------------------------

function groupEntries(ops: PlannedOp[], fkBundlingFor: (groupId: string) => boolean): PlannedEntry[] {
    const entries: PlannedEntry[] = [];
    let open: PlannedOp[] = [];
    const openKeys = new Set<string>();

    const close = (): void => {
        if (open.length === 0) return;
        entries.push({ groupId: open[0].groupId, ops: open });
        open = [];
        openKeys.clear();
    };

    for (const op of ops) {
        const joins = open.length > 0
            && open[0].groupId === op.groupId
            && op.kind === 'insert'
            && fkBundlingFor(op.groupId)
            && op.deps.some((d) => openKeys.has(d));
        if (!joins) close();
        open.push(op);
        openKeys.add(op.key);
    }
    close();
    return entries;
}

// ---------------------------------------------------------------------------
// Entry points
// ---------------------------------------------------------------------------

// Plan a (possibly cross-group / interleaved) batch into commit-ordered entries.
export function planDatabaseEntries(
    batch: CapturedBatch,
    ctxFor: (targetTable: string) => NodeContext | undefined,
    lookup: MappingLookup,
    keyLookup: KeyLookup,
    newUuid: () => string,
    fkBundlingFor: (groupId: string) => boolean,
    updateMergeFor: (targetTable: string) => boolean = () => true,
): IngestPlan {
    const nets = planOps(batch, updateMergeFor);
    const { ops, rejects, reservations } = planNodes(nets, ctxFor, lookup, keyLookup, newUuid);
    return { entries: groupEntries(ops, fkBundlingFor), rejects, reservations };
}

// Single-group convenience wrapper (one fixed context for every table).
export function changesToEntries(
    batch: CapturedBatch,
    schemaView: RSchemaView,
    lookup: MappingLookup,
    config: AdapterConfig,
    writerKeyId: KeyId,
    newUuid: () => string,
    keyLookup: KeyLookup = () => undefined,
): IngestPlan {
    const ctx: NodeContext = { groupId: '', schemaView, config, writerKeyId };
    const fkBundling = config.fkBundling !== false;
    const updateMerge = config.updateMerge !== false;
    return planDatabaseEntries(batch, () => ctx, lookup, keyLookup, newUuid, () => fkBundling, () => updateMerge);
}
