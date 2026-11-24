// Ingestion at the checkpoint (HIP-29): a pass over the outbox that appends
// each change's entry at its group's checkpoint plus the chain so far, and the
// end of a pass, which settles what the pass stored.
//
// runPass, for each change in commit order:
//   - a change of a table no member projects gets the mark of nothing to write;
//   - at a change of a group with an identity provider and no writer key the
//     pass stops, and its end settles the changes before it;
//   - the observes the change's group needs of the groups it binds whose tip
//     moved in this pass, each recorded in the change's outbox row, then
//     appended (an observe the gate refuses, that would be void, that throws
//     or that needs a key the projection lacks is declined);
//   - the entry, a plain row op: planned, built, signed and validated (also through the live
//     observes while a gated binding's two readings differ). A refusal, the
//     planner's or rdb's, or any error while planning or preparing, records a
//     failure event; nothing to write records the mark; otherwise the entry's
//     hash is recorded, and then the entry is appended.
// An error from the target, or from an append, ends the pass there: whatever
// it stored stays, and the next cycle's end settles it. A record that can't
// be written means nothing of its change reached rdb.
//
// endPass reads only the outbox rows, the checkpoints and rdb, so the next
// cycle runs the same end after a crash: the handled rows, each group's chain
// end (its checkpoint with the hashes recorded for it that rdb holds), the
// observes still due, then one settle transaction, and the ingest result from
// the records.

import { json } from "@hyper-hyper-space/hhs3_json";
import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import {
    canonicalVersion, formatValidationFailure, refVersionAtOrAbove, ValidationRejectedError,
} from "@hyper-hyper-space/hhs3_mvt";
import type { CausalDag, ValidationFailure, Version } from "@hyper-hyper-space/hhs3_mvt";
import { deriveRowId, gateGraph, gateGraphOrder } from "@hyper-hyper-space/hhs3_rdb";
import type { BundleWrite, PreparedEntry, RowEnvelopePayload, RowOpPayload, RSchemaView, RTableGroup, RTableGroupDelta } from "@hyper-hyper-space/hhs3_rdb";

import {
    AdapterConfig, BidirectionalTarget, CapturedChange, ChainSchemaChangeError, DEFAULT_KEY_DOMAIN, IngestRejection, IngestResult,
    KeyIndex, OpEvent, RowRevert, SyncMapping, versionsEqual,
} from "./types.js";
import { schemaDeltaActions } from "./schema_actions.js";
import { ChangePlan, KeyLookup, MappingLookup, NodeContext, planChange, PlannedOp, PlannedReject, SubjectRow } from "./ingest.js";
import { authorColumn, keyRefColumnFromTarget, rdbColumnName, rdbFkColumnFromTarget, rdbTableName, targetTableName } from "./names.js";
import { opVerdictEvents, rowToUpsertAction } from "./project.js";

// One member of a pass: a group of the projection that has a checkpoint.
// Groups without one hold no projected tables, and are neither ingested nor
// observed.
type PassMember = {
    group: RTableGroup;
    groupId: B64Hash;
    config: AdapterConfig;
    writer: OwnIdentity | undefined;
    signed: boolean;
    checkpoint: Version;
    schemaView: RSchemaView;   // the schema at the checkpoint
    causalDag: CausalDag;
    gated: B64Hash[];          // the bound groups whose binding has a canObserve gate
};

export type IngestMember = { group: RTableGroup; config: AdapterConfig };

// What one ingestion pass did: each member's result, and the group whose
// change stopped the pass, when one did (a group with an identity provider
// and no writer key).
export type IngestPassOutcome = { results: Map<B64Hash, IngestResult>; stoppedAt?: B64Hash };

async function loadMembers(members: IngestMember[], target: BidirectionalTarget): Promise<Map<B64Hash, PassMember>> {
    const loaded = new Map<B64Hash, PassMember>();
    for (const { group, config } of members) {
        const groupId = group.getId();
        const checkpoint = await target.getCheckpoint(groupId);
        if (checkpoint === undefined) continue;
        const view = await group.getView(checkpoint, checkpoint);
        const bindings = group.getBindings();
        const gated = [...new Set(Object.keys(group.getCanObserve() ?? {})
            .map((alias) => bindings[alias]).filter((id): id is B64Hash => id !== undefined))];
        loaded.set(groupId, {
            group, groupId, config, writer: config.writer, signed: group.getIdProvider() !== undefined,
            checkpoint, schemaView: view.getSchemaView(), causalDag: await group.getCausalDag(), gated,
        });
    }
    return loaded;
}

// Each projected table's member, from the schema at its checkpoint.
function tableIndex(members: Map<B64Hash, PassMember>): Map<string, PassMember> {
    const index = new Map<string, PassMember>();
    for (const m of members.values()) {
        for (const rdbTable of m.schemaView.getTableNames()) index.set(targetTableName(m.config, rdbTable), m);
    }
    return index;
}

function emptyResults(members: IngestMember[]): Map<B64Hash, IngestResult> {
    return new Map(members.map((m) => [m.group.getId(), { accepted: 0, rejected: [] as IngestRejection[] }]));
}

export function mergeResults(into: Map<B64Hash, IngestResult>, more: Map<B64Hash, IngestResult>): void {
    for (const [id, r] of more) {
        const prev = into.get(id);
        into.set(id, prev === undefined ? r : { accepted: prev.accepted + r.accepted, rejected: [...prev.rejected, ...r.rejected] });
    }
}

// The signer of a member's entries: its writer, or none in a group without an
// identity provider, whose ops are anonymous.
function authorFor(m: PassMember): OwnIdentity | undefined {
    return m.signed ? m.writer : undefined;
}

function keyOf(table: string, localId: number): string {
    return table + '\u0000' + localId;
}

function messageOf(e: unknown): string {
    return e instanceof Error ? e.message : String(e);
}

// Whether the two readings of any gated binding of `m` differ at `at`: through
// every observe, and through the live observes only (HIP-29's Writes void
// where they stand). A row op changes neither, so a pass compares them at the
// checkpoint and again after each observe it appends.
export async function readingsDiffer(group: RTableGroup, at: Version): Promise<boolean> {
    const bindings = group.getBindings();
    const gated = new Set(Object.keys(group.getCanObserve() ?? {}).map((alias) => bindings[alias]).filter((id) => id !== undefined));
    for (const foreignId of gated) {
        const all = await group.resolveObservedForeignVersion(foreignId, at, at);
        const live = await group.resolveLiveObservedForeignVersion(foreignId, at);
        if (!versionsEqual(all, live)) return true;
    }
    return false;
}

// The member's bindings to other members, once per bound group, in gate-graph
// order: a group an observe gate reads is observed before the binding that
// gate guards, so the gate sees its new version.
function boundMembers(m: PassMember, members: Map<B64Hash, PassMember>): { binding: string; foreign: PassMember }[] {
    const order = gateGraphOrder(gateGraph(m.group.getBindings(), m.group.getCanObserve() ?? {}));
    const rank = (id: B64Hash): number => { const i = order.indexOf(id); return i < 0 ? order.length : i; };
    const seen = new Set<B64Hash>();
    const out: { binding: string; foreign: PassMember }[] = [];
    for (const [binding, foreignId] of Object.entries(m.group.getBindings())) {
        const foreign = members.get(foreignId);
        if (foreign === undefined || seen.has(foreignId) || foreignId === m.groupId) continue;
        seen.add(foreignId);
        out.push({ binding, foreign });
    }
    return out.sort((a, b) => rank(a.foreign.groupId) - rank(b.foreign.groupId));
}

// The observe `m` needs at `at` of a bound group whose chain ends at
// `foreignAt`: to that version joined with what `m` already observes there.
// `covered` when `m` already observes that much. `declined` when the gate
// refuses the writer, when it would be void where it stands, when building,
// signing or validating it throws, or when it needs a writer key `m` lacks.
// The version an observe of a bound group advances to (HIP-29): the bound
// group's chain end, joined with what the observer already observes, since rdb
// refuses an observe that doesn't extend every earlier one. Undefined when the
// observer already observes that much.
export async function observeVersion(foreignDag: CausalDag, observed: Version, chainEnd: Version): Promise<Version | undefined> {
    if (await refVersionAtOrAbove(foreignDag, observed, chainEnd)) return undefined;
    return canonicalVersion(foreignDag, new Set([...observed, ...chainEnd]));
}

async function dueObserve(m: PassMember, binding: string, foreign: PassMember, foreignAt: Version, at: Version):
    Promise<PreparedEntry | 'covered' | 'declined'> {
    const observed = await m.group.resolveObservedForeignVersion(foreign.groupId, at, at);
    const version = await observeVersion(foreign.causalDag, observed, foreignAt);
    if (version === undefined) return 'covered';
    const needsAuthor = m.group.observeNeedsAuthor(binding);
    if (needsAuthor && authorFor(m) === undefined) return 'declined';
    const author = needsAuthor ? authorFor(m) : undefined;
    let prepared: PreparedEntry;
    try {
        prepared = await m.group.prepareObserve(binding, version, author, at);
        if (m.gated.includes(foreign.groupId)
            && !await m.group.evaluateObserveGate(foreign.groupId, author?.keyId, { opPos: at, imported: version, fold: 'use' })) {
            return 'declined';   // it would be void where it stands
        }
    } catch {
        return 'declined';
    }
    return prepared;
}

// The unsigned row envelope of a refused op, as the entry would have held it:
// the op JSON stored in its failure event, and the base of a synthetic
// (never-appended) op hash.
function rowPayload(write: BundleWrite): json.Literal {
    return { action: 'row', table: write.table, op: write.op } as unknown as json.Literal;
}

// The hash of a failure event, and the op JSON it may carry. The hash covers the
// failed (never-appended) op, the ids of the captured changes it came from and,
// for a write refused before rdb saw it, the refused column: the event log
// keeps one event per (hash, direction), so a hash of the op alone would drop
// every later refusal of the same row. When the op can't be hashed (it holds a
// value that isn't a json literal), the hash and the event leave it out, and
// name the row instead.
async function failureHash(
    group: RTableGroup,
    f: { write?: BundleWrite; sources: CapturedChange[]; column?: string; table: string; localId: number; rowId?: B64Hash },
): Promise<{ opHash: B64Hash; op?: json.Literal }> {
    const dag = await group.getScopedDag();
    const extra: { [key: string]: json.Literal } = { changes: f.sources.map((c) => c.id) };
    if (f.column !== undefined) extra.column = f.column;
    if (f.write !== undefined) {
        const op = rowPayload(f.write);
        if (json.literalProblem(op) === undefined) {
            try {
                return { opHash: await dag.computeEntryHash({ failed: op, ...extra }), op };
            } catch { /* recorded without its op, below */ }
        }
    }
    const row: { [key: string]: json.Literal } = { table: f.table, localId: f.localId };
    if (f.rowId !== undefined) row.rowId = f.rowId;
    return { opHash: await dag.computeEntryHash({ failed: row, ...extra }) };
}

async function failureEvent(m: PassMember, f: {
    change: CapturedChange; rowId?: B64Hash; write?: BundleWrite; failure: ValidationFailure; column?: string; author?: B64Hash;
}): Promise<OpEvent> {
    const { change } = f;
    const { opHash, op } = await failureHash(m.group, {
        write: f.write, sources: [change], column: f.column, table: change.table, localId: change.localId, rowId: f.rowId,
    });
    const event: OpEvent = {
        origin: 'ingestion', direction: 'failure', groupId: m.groupId, opHash, kind: change.kind,
        table: change.table, localId: change.localId, reason: { source: 'validation', failure: f.failure },
    };
    if (f.author !== undefined) event.author = f.author;
    if (f.rowId !== undefined) event.rowId = f.rowId;
    if (op !== undefined) event.op = op;
    return event;
}

function asKeyIndex(target: BidirectionalTarget): KeyIndex | undefined {
    const candidate = target as BidirectionalTarget & Partial<KeyIndex>;
    return typeof candidate.registerKey === 'function' ? candidate as KeyIndex : undefined;
}

// The stored mappings a change's plan reads: its own row's, and each local FK
// target's.
async function prefetchMappings(
    change: CapturedChange, m: PassMember, target: BidirectionalTarget, cache: Map<string, SyncMapping | undefined>,
): Promise<void> {
    const prefetch = async (table: string, localId: number): Promise<void> => {
        const key = keyOf(table, localId);
        if (!cache.has(key)) cache.set(key, await target.resolveRow(table, localId));
    };
    await prefetch(change.table, change.localId);
    if (change.kind === 'delete') return;
    const rdbTable = rdbTableName(m.config, change.table);
    const fks = m.schemaView.hasTable(rdbTable) ? m.schemaView.getFKs(rdbTable) : {};
    for (const [targetCol, value] of Object.entries(change.values)) {
        if (value === null) continue;   // a NULL foreign key points at no row: nothing to look up
        const fkInfo = rdbFkColumnFromTarget(m.config, rdbTable, fks, targetCol);
        if (fkInfo?.resolvesToId === true && fkInfo.targetTable !== undefined && typeof value === 'number') {
            await prefetch(fkInfo.targetTable, value);
        }
    }
}

// The key table entries a change's plan reads: its key-ref columns' and its
// author column's.
async function prefetchKeys(
    change: CapturedChange, m: PassMember, keys: KeyIndex, cache: Map<number, { keyHash: string; publicKey?: string } | undefined>,
): Promise<void> {
    if (change.kind === 'delete') return;
    const rdbTable = rdbTableName(m.config, change.table);
    const provider = m.schemaView.hasTable(rdbTable) ? m.schemaView.getIdProvider?.(rdbTable) : undefined;
    const def = m.schemaView.getTable?.(rdbTable);
    const identityColumns = new Set<string>();
    for (const [col, colDef] of Object.entries(def?.columns ?? {})) {
        if (colDef.type === 'identity') identityColumns.add(col);
    }
    const author = authorColumn(m.config);
    for (const [targetCol, value] of Object.entries(change.values)) {
        const isAuthor = targetCol === author;
        if (!isAuthor && keyRefColumnFromTarget(m.config, rdbTable, provider, targetCol, identityColumns) === undefined) continue;
        if (typeof value !== 'number' || cache.has(value)) continue;   // a NULL names no key; a non-number is refused
        const keyHash = await keys.keyHashForId(DEFAULT_KEY_DOMAIN, value);
        if (keyHash === undefined) { cache.set(value, undefined); continue; }
        const publicKey = await keys.publicKeyForId(DEFAULT_KEY_DOMAIN, value);
        cache.set(value, { keyHash, ...(publicKey !== undefined ? { publicKey } : {}) });
    }
}

// rdb's row for an update's subject at `at`, for the planner's set-aside.
async function subjectAt(m: PassMember, rdbTable: string, rowId: B64Hash, at: Version): Promise<SubjectRow | undefined> {
    const view = await m.group.getView(at, at);
    if (!view.getSchemaView().hasTable(rdbTable)) return undefined;
    const row = await (await view.getTableView(rdbTable)).getRow(rowId);
    return row === undefined ? undefined : { values: row.values, ...(row.author !== undefined ? { author: row.author } : {}) };
}

// ---------------------------------------------------------------------------
// The pass
// ---------------------------------------------------------------------------

// For the suites: when on, every pass leaves its end to the next cycle, as a
// crash right after its last record would, and an ingestion runs that end as
// a new cycle does. The end reads only what is stored, so nothing changes.
let endsInNewCycle = false;
export function leaveEndsToNewCycles(on: boolean): void {
    endsInNewCycle = on;
}

export async function runPass(
    members: IngestMember[], target: BidirectionalTarget, newUuid: () => string,
): Promise<IngestPassOutcome> {
    const loaded = await loadMembers(members, target);
    const tables = tableIndex(loaded);
    const tip = new Map<B64Hash, Version>();
    const moved = new Set<B64Hash>();
    const differ = new Map<B64Hash, boolean>();
    for (const m of loaded.values()) {
        tip.set(m.groupId, m.checkpoint);
        differ.set(m.groupId, m.gated.length > 0 && await readingsDiffer(m.group, m.checkpoint));
    }
    const keys = asKeyIndex(target);
    const stored = new Map<string, SyncMapping | undefined>();
    // The identities the pass gives local ids: an insert's, appended or refused.
    const given = new Map<string, SyncMapping>();
    const lookup: MappingLookup = (table, localId) => given.get(keyOf(table, localId)) ?? stored.get(keyOf(table, localId));
    const keyCache = new Map<number, { keyHash: string; publicKey?: string } | undefined>();
    const keyLookup: KeyLookup = (keyId) => keyCache.get(keyId);

    const append = async (m: PassMember, prepared: PreparedEntry): Promise<void> => {
        await m.group.appendPrepared(prepared);
        tip.set(m.groupId, new Set([prepared.hash]));
        moved.add(m.groupId);
    };
    // A refused insert gives its local id an identity for the rest of the pass.
    const refusedInsert = (change: CapturedChange, rowId: B64Hash | undefined, uuid: string | undefined): void => {
        if (change.kind !== 'insert' || rowId === undefined || lookup(change.table, change.localId) !== undefined) return;
        given.set(keyOf(change.table, change.localId),
            { table: change.table, localId: change.localId, rowId, uuid: uuid ?? '', status: 'ingestion_failure' });
    };

    let stoppedAt: B64Hash | undefined;
    for (const change of (await target.drainChanges()).changes) {
        const m = tables.get(change.table);
        if (m === undefined) {
            await target.writeRecord(change.id, { nothing: true });
            continue;
        }
        if (m.signed && m.writer === undefined) {
            stoppedAt = m.groupId;
            break;
        }

        // The observes the change's group needs first.
        for (const { binding, foreign } of boundMembers(m, loaded)) {
            if (!moved.has(foreign.groupId)) continue;
            const observe = await dueObserve(m, binding, foreign, tip.get(foreign.groupId)!, tip.get(m.groupId)!);
            if (observe === 'covered' || observe === 'declined') continue;
            await target.writeRecord(change.id, { observes: [{ group: foreign.groupId, hash: observe.hash }] });
            await append(m, observe);
            if (m.gated.length > 0) differ.set(m.groupId, await readingsDiffer(m.group, tip.get(m.groupId)!));
        }

        const at = tip.get(m.groupId)!;
        await prefetchMappings(change, m, target, stored);
        if (keys !== undefined) await prefetchKeys(change, m, keys, keyCache);
        const rdbTable = rdbTableName(m.config, change.table);
        const subjectId = change.kind === 'update' ? lookup(change.table, change.localId)?.rowId : undefined;
        const subject = subjectId === undefined ? undefined : await subjectAt(m, rdbTable, subjectId, at);
        const ctx: NodeContext = {
            groupId: m.groupId, schemaView: m.schemaView, config: m.config, writerKeyId: authorFor(m)?.keyId,
            subjectRow: (table, rowId) => table === rdbTable && rowId === subjectId ? subject : undefined,
            ...(m.signed && m.config.autoAuthor === false ? { autoAuthorOff: true } : {}),
        };

        let planned: ChangePlan;
        try {
            planned = planChange(change, ctx, lookup, keyLookup, newUuid);
        } catch (e) {
            // An error while translating is the change's failure. An insert still
            // gets a uuid and a row hash, which its failure event carries.
            const uuid = change.kind === 'insert' && lookup(change.table, change.localId) === undefined ? newUuid() : undefined;
            const reject: PlannedReject = { change, table: change.table, localId: change.localId, kind: change.kind,
                reason: messageOf(e), ...(uuid !== undefined ? { uuid, rowId: deriveRowId(uuid, authorFor(m)?.keyId) } : {}) };
            planned = { kind: 'refused', reject };
        }

        if (planned.kind === 'nothing') {
            await target.writeRecord(change.id, { nothing: true });
            continue;
        }
        if (planned.kind === 'refused') {
            const r = planned.reject;
            const event = await failureEvent(m, { change, rowId: r.rowId, write: r.op, failure: { reason: r.reason }, column: r.column,
                author: change.kind === 'insert' ? r.author : authorFor(m)?.keyId });
            await target.writeRecord(change.id, { failure: event });
            refusedInsert(change, r.rowId, r.uuid);
            continue;
        }

        const op: PlannedOp = planned.op;
        const author = op.anonymous === true ? undefined : authorFor(m);
        let prepared: PreparedEntry;
        try {
            prepared = await m.group.prepareRowOp(op.write.table, op.write.op, author, at, { liveReads: differ.get(m.groupId) === true });
        } catch (e) {
            const why: ValidationFailure = e instanceof ValidationRejectedError ? e.why : { reason: messageOf(e) };
            const event = await failureEvent(m, { change, rowId: op.write.op.rowId, write: op.write, failure: why, author: author?.keyId });
            await target.writeRecord(change.id, { failure: event });
            refusedInsert(change, op.mapping?.rowId, op.mapping?.uuid);
            continue;
        }
        await target.writeRecord(change.id, { entry: prepared.hash });
        await append(m, prepared);
        if (op.mapping !== undefined) given.set(keyOf(change.table, change.localId), op.mapping);
    }

    const results = endsInNewCycle ? emptyResults(members) : await endPass(members, target);
    return { results, ...(stoppedAt !== undefined ? { stoppedAt } : {}) };
}

// ---------------------------------------------------------------------------
// The end of a pass
// ---------------------------------------------------------------------------

function uuidOfFailure(event: OpEvent): string {
    const op = (event.op as { op?: { uuid?: unknown } } | undefined)?.op;
    return typeof op?.uuid === 'string' ? op.uuid : '';
}

function reasonOf(event: OpEvent): string {
    return event.reason?.source === 'validation' ? formatValidationFailure(event.reason.failure) : 'refused';
}

// rdb's row at `at`, as projection writes it, or undefined when rdb has none.
async function rdbRowAt(m: PassMember, table: string, rowId: B64Hash, at: Version): Promise<RowRevert['row']> {
    const view = await m.group.getView(at, at);
    const rdbTable = rdbTableName(m.config, table);
    if (!view.getSchemaView().hasTable(rdbTable)) return undefined;
    const row = await (await view.getTableView(rdbTable)).getRow(rowId);
    if (row === undefined) return undefined;
    return rowToUpsertAction(row, rdbTable, view.getSchemaView(), m.config) as RowRevert['row'];
}

// The json columns a change writes, which SQLite's settle rewrites in canonical form.
function jsonColumns(m: PassMember, change: CapturedChange): string[] {
    if (change.kind === 'delete') return [];
    const rdbTable = rdbTableName(m.config, change.table);
    const def = m.schemaView.getTable?.(rdbTable);
    return Object.keys(change.values).filter((col) => def?.columns?.[rdbColumnName(m.config, rdbTable, col)]?.type === 'json');
}

export async function endPass(members: IngestMember[], target: BidirectionalTarget): Promise<Map<B64Hash, IngestResult>> {
    const loaded = await loadMembers(members, target);
    const tables = tableIndex(loaded);
    const results = emptyResults(members);
    const { changes } = await target.drainChanges();
    const holds = async (m: PassMember, hash: B64Hash): Promise<boolean> =>
        (await (await m.group.getScopedDag()).loadEntry(hash)) !== undefined;

    // 1. The handled changes: the leading rows whose record says what became
    // of them.
    const handled: CapturedChange[] = [];
    for (const change of changes) {
        const record = change.record;
        const m = tables.get(change.table);
        const done = record?.failure !== undefined || record?.nothing === true
            || (record?.entry !== undefined && m !== undefined && await holds(m, record.entry));
        if (!done) break;
        handled.push(change);
    }

    // 2. Each group's chain end: its checkpoint with the hashes recorded for it
    // that rdb holds, in the outbox rows and beside the checkpoint. A chain that
    // holds an observe gets a delta (step 4).
    const recordedFor = new Map<B64Hash, { hash: B64Hash; observe: boolean }[]>();
    for (const m of loaded.values()) {
        recordedFor.set(m.groupId, (await target.getEndObserves(m.groupId)).map((hash) => ({ hash, observe: true })));
    }
    for (const change of changes) {
        const m = tables.get(change.table);
        if (m === undefined || change.record === undefined) continue;
        const list = recordedFor.get(m.groupId)!;
        for (const o of change.record.observes ?? []) list.push({ hash: o.hash, observe: true });
        if (change.record.entry !== undefined) list.push({ hash: change.record.entry, observe: false });
    }
    const end = new Map<B64Hash, Version>();
    const observed = new Set<B64Hash>();   // the groups whose chain holds an observe
    for (const m of loaded.values()) {
        const held: B64Hash[] = [];
        for (const { hash, observe } of recordedFor.get(m.groupId)!) {
            if (!await holds(m, hash)) continue;
            held.push(hash);
            if (observe) observed.add(m.groupId);
        }
        end.set(m.groupId, held.length === 0 ? m.checkpoint : await canonicalVersion(m.causalDag, new Set([...m.checkpoint, ...held])));
    }

    // 3. The observes still due: for each group whose chain end moved, each
    // group that binds it and doesn't observe that much, transitively. Each is
    // recorded beside its group's checkpoint, then appended. A declined one
    // isn't asked for again.
    const declined = new Set<string>();
    for (let progress = true; progress;) {
        progress = false;
        for (const h of loaded.values()) {
            for (const { binding, foreign } of boundMembers(h, loaded)) {
                const pair = h.groupId + '\u0000' + foreign.groupId;
                if (declined.has(pair) || versionsEqual(end.get(foreign.groupId), foreign.checkpoint)) continue;
                const observe = await dueObserve(h, binding, foreign, end.get(foreign.groupId)!, end.get(h.groupId)!);
                if (observe === 'covered') continue;
                if (observe === 'declined') { declined.add(pair); continue; }
                await target.recordEndObserve(h.groupId, observe.hash);
                await h.group.appendPrepared(observe);
                end.set(h.groupId, new Set([observe.hash]));
                observed.add(h.groupId);
                progress = true;
            }
        }
    }

    // 4. The deltas: for each group whose chain holds an observe, rdb's delta
    // from its checkpoint to its chain's end. An observe can bring in an entry
    // of the bound group that voids or reinstates an op the group read through
    // it, an op of the checkpoint or one of the pass's own. Each such verdict
    // change gives an event, and the rows its op writes are reverted to rdb's at
    // the end. Its row changes are the pass's own writes, which the target has.
    // A chain changes no schema, so a delta that does halts the projection.
    const events: OpEvent[] = [];
    const verdictRows: { m: PassMember; table: string; rowId: B64Hash }[] = [];
    for (const m of loaded.values()) {
        if (!observed.has(m.groupId)) continue;
        const to = end.get(m.groupId)!;
        const delta = (await m.group.computeDelta(m.checkpoint, to)) as RTableGroupDelta;
        const endSchema = (await m.group.getView(to, to)).getSchemaView();
        if (schemaDeltaActions(delta.schemaChanges, endSchema, m.schemaView, m.config).length > 0) {
            throw new ChainSchemaChangeError(m.groupId);
        }
        events.push(...opVerdictEvents(delta.opVerdictChanges, m.groupId, m.config));
        for (const c of delta.opVerdictChanges) {
            if (c.voidBefore === c.voidAfter) continue;
            const writes = c.writes ?? (c.table !== undefined && c.rowId !== undefined ? [{ table: c.table, rowId: c.rowId }] : []);
            for (const w of writes) verdictRows.push({ m, table: targetTableName(m.config, w.table), rowId: w.rowId });
        }
    }

    // 5. The settle, and the ingest result from the records.
    const inserts: SyncMapping[] = [];
    const deletes: { table: string; rowId: B64Hash }[] = [];
    const orphans: { table: string; localId: number }[] = [];
    const reverts = new Map<string, RowRevert>();
    const jsonWrites: { table: string; localId: number; column: string }[] = [];
    // The identity each handled insert gave its local id.
    const earlier = new Map<string, SyncMapping>();
    const identityOf = async (table: string, localId: number): Promise<SyncMapping | undefined> =>
        earlier.get(keyOf(table, localId)) ?? await target.resolveRow(table, localId);

    for (const change of handled) {
        const m = tables.get(change.table);
        const record = change.record!;
        if (m === undefined || record.nothing === true) continue;
        const result = results.get(m.groupId)!;
        if (record.entry !== undefined) {
            result.accepted++;
            const entry = await (await m.group.getScopedDag()).loadEntry(record.entry);
            // An ingested change's entry is a row op of its table (prepareRowOp).
            const op = (entry!.payload as unknown as RowEnvelopePayload).op as unknown as RowOpPayload & { author?: B64Hash };
            if (change.kind === 'insert' && op.action === 'insert') {
                const mapping: SyncMapping = { table: change.table, localId: change.localId, rowId: op.rowId, uuid: op.uuid,
                    ...(op.author !== undefined ? { author: op.author } : {}), status: 'active' };
                inserts.push(mapping);
                earlier.set(keyOf(change.table, change.localId), mapping);
            } else if (change.kind === 'delete') {
                deletes.push({ table: change.table, rowId: op.rowId });
            }
            for (const column of jsonColumns(m, change)) jsonWrites.push({ table: change.table, localId: change.localId, column });
            continue;
        }

        const event = record.failure!;
        result.rejected.push({ change, groupId: m.groupId, reason: reasonOf(event) });
        const identity = await identityOf(change.table, change.localId);
        if (change.kind === 'insert' && identity === undefined) {
            // A refused insert on a local id with no identity: its row is
            // removed, and its local id keeps the refused identity.
            orphans.push({ table: change.table, localId: change.localId });
            if (event.rowId !== undefined) {
                const mapping: SyncMapping = { table: change.table, localId: change.localId, rowId: event.rowId,
                    uuid: uuidOfFailure(event), status: 'ingestion_failure' };
                inserts.push(mapping);
                earlier.set(keyOf(change.table, change.localId), mapping);
            }
            continue;
        }
        if (identity === undefined) continue;   // a row with no identity: nothing of rdb's to put back
        const key = keyOf(change.table, change.localId);
        if (reverts.has(key)) continue;
        reverts.set(key, { table: change.table, rowId: identity.rowId, localId: change.localId,
            row: await rdbRowAt(m, change.table, identity.rowId, end.get(m.groupId)!) });
    }

    // The rows of the ops whose verdict an observe changed, named by row hash:
    // the target finds their local ids, an own insert's among them, after it
    // writes the inserts' mappings, and gives a reinstated row new to it one.
    const revertedRows = new Set([...reverts.values()].map((r) => r.table + '\u0000' + r.rowId));
    for (const { m, table, rowId } of verdictRows) {
        const key = table + '\u0000' + rowId;
        if (revertedRows.has(key)) continue;
        revertedRows.add(key);
        reverts.set(key, { table, rowId, row: await rdbRowAt(m, table, rowId, end.get(m.groupId)!) });
    }

    await target.commitIngest({
        consumed: handled.map((c) => c.id),
        checkpoints: [...loaded.values()].map((m) => ({ groupId: m.groupId, expected: m.checkpoint, next: end.get(m.groupId)! })),
        inserts, deletes, orphans, reverts: [...reverts.values()], jsonWrites, events,
    });
    return results;
}

// One ingestion: the end of a crashed pass first, when the outbox's first row
// has a record, then a pass over the rest.
export async function ingestOnce(
    members: IngestMember[], target: BidirectionalTarget, newUuid: () => string,
): Promise<IngestPassOutcome> {
    const results = emptyResults(members);
    if (!await target.hasPendingChanges()) return { results };
    const [first] = (await target.drainChanges()).changes;
    if (first?.record !== undefined) mergeResults(results, await endPass(members, target));
    if (!await target.hasPendingChanges()) return { results };
    const pass = await runPass(members, target, newUuid);
    mergeResults(results, pass.results);
    if (endsInNewCycle) {
        const [next] = (await target.drainChanges()).changes;
        if (next?.record !== undefined || (await Promise.all(members.map((m) => target.getEndObserves(m.group.getId())))).some((l) => l.length > 0)) {
            mergeResults(results, await endPass(members, target));
        }
    }
    return { results, ...(pass.stoppedAt !== undefined ? { stoppedAt: pass.stoppedAt } : {}) };
}
