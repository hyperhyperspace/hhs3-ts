// Pure inverse planner: one captured target-side change -> one rdb write op
// (HIP-29). The inbound sibling of row_actions.ts. No IO, no target, no
// signing (prepareBundle signs); given prefetched sync-mapping / key lookups
// it is a pure function of (change, schemaView, config).
//
// Each change becomes one op, or a refusal, or nothing to write: an update
// whose named columns are all set aside. Nothing is bundled or merged: an
// insert that references a row inserted earlier in the pass finds it, since
// its entry comes after that row's, and an update lands or is refused whole.
// Nothing is dropped either: every change the planner can't take is a
// refusal with a kind, so the orchestrator records its failure.
//
// Validity (do the FKs/restrictions actually hold) is NOT decided here - rdb
// decides it when it builds the entry; rejects flow back through the
// orchestrator.

import { json } from "@hyper-hyper-space/hhs3_json";
import type { KeyId } from "@hyper-hyper-space/hhs3_crypto";
import {
    BundleWrite, deriveRowId, InsertRowPayload, RowOpPayload, RowWrite, rowWriteParts, RSchemaView, TableDef,
} from "@hyper-hyper-space/hhs3_rdb";

import {
    AdapterConfig, CapturedBatch, CapturedChange, CapturedValues, IngestRejection, SyncMapping,
} from "./types.js";
import {
    authorColumn, isSystemColumn, keyRefColumnFromTarget, rdbColumnName, rdbFkColumnFromTarget, rdbTableName,
} from "./names.js";

// One translated, ready-to-submit op. `mapping` is the identity of an
// insert: its row hash, uuid and signer. `anonymous` marks an insert signed by
// no one (autoAuthor off with an empty author column).
export type PlannedOp = {
    table: string;            // target (projected) table
    localId: number;
    groupId: string;
    rdbTable: string;
    kind: 'insert' | 'update' | 'delete';
    write: BundleWrite;
    change: CapturedChange;
    mapping?: SyncMapping;
    anonymous?: true;
};

// The entries of a planned batch, one op each, in commit order.
export type PlannedEntry = {
    groupId: string;
    ops: PlannedOp[];
};

// A refusal, enriched for the op-event log: the constructed (never-appended)
// op and the row it targeted, so the orchestrator can record a failure event
// (op JSON + a synthetic hash) and revert the row. `sources` are the captured
// changes the refused write came from, and `column` the target column
// refused: the failure's hash covers both. A refused insert carries a row hash
// and uuid: those of its local id's identity, or ones minted for it, which its
// failure event carries.
export type PlannedReject = IngestRejection & {
    op?: BundleWrite;
    table?: string;
    localId?: number;
    rowId?: string;
    uuid?: string;
    kind?: 'insert' | 'update' | 'delete';
    author?: KeyId;
    sources?: CapturedChange[];
    column?: string;
};

// What the planner makes of one change.
export type ChangePlan =
    | { kind: 'op'; op: PlannedOp }
    | { kind: 'refused'; reject: PlannedReject }
    | { kind: 'nothing' };

// A planned batch (planDatabaseEntries): the ops, the refusals, and the
// changes with nothing to write, among them those of a table no member projects.
export type IngestPlan = {
    entries: PlannedEntry[];
    rejects: PlannedReject[];
    nothing: CapturedChange[];
};


// Resolve (target table, localId) -> the local id's identity: its stored sync
// mapping, or one the pass gave it, as the orchestrator keeps them. Used for
// the subject of an update/delete, for an insert on a local id that already
// has an identity, and for a local FK target.
export type MappingLookup = (table: string, localId: number) => SyncMapping | undefined;

// Prefetched KeyIndex lookups for key-ref reverse: key id -> {keyHash, publicKey}.
export type KeyLookup = (keyId: number) => { keyHash: string; publicKey?: string } | undefined;

// Everything the planner needs about the group that owns a target table.
// `writerKeyId` is undefined when the group's ops are anonymous (no idProvider).
export type NodeContext = {
    groupId: string;
    schemaView: RSchemaView;
    config: AdapterConfig;
    writerKeyId: KeyId | undefined;
    // rdb's row for an update's subject, to set aside a readonly or author
    // column that holds the row's own value (HIP-29). Absent: none set aside.
    subjectRow?: (rdbTable: string, rowId: string) => SubjectRow | undefined;
    // autoAuthor off in a group with an identity provider: an insert is signed
    // as its captured author column says, and is anonymous when it is empty.
    autoAuthorOff?: boolean;
};

// rdb's values of a row, and its author's key hash, as an update finds it.
export type SubjectRow = { values: { [column: string]: json.Literal }; author?: string };

// Whether a value written to a column is the value rdb's row has there. NULL
// and an absent column are the same.
function sameRdbValue(a: json.Literal | null | undefined, b: json.Literal | null | undefined): boolean {
    const x = a ?? null;
    const y = b ?? null;
    if (x === null || y === null) return x === y;
    return json.eq(x, y);
}

function keyOf(table: string, localId: number): string {
    return table + '\u0000' + localId;
}

// ---------------------------------------------------------------------------
// Reverse a set of target columns to rdb columns/values, resolving FK and
// key-ref values. Returns the rdb values, or the first offending column's
// reject reason.
// ---------------------------------------------------------------------------

type Reversed = { ok: true; values: RowWrite } | { ok: false; reason: string; column: string };

// Reverse a set of target columns into rdb columns/values for `rdbTable`. A
// null maps to a null of the rdb column: in each branch the readonly check
// runs first and the null is mapped next, before any lookup, since a NULL
// foreign key or key id resolves to nothing.
function reverseCols(
    rdbTable: string,
    targetValues: CapturedValues,
    ctx: NodeContext,
    isUpdate: boolean,
    lookup: MappingLookup,
    keyLookup: KeyLookup,
    unreadable?: { [column: string]: string },
    subject?: SubjectRow,
): Reversed {
    // A column the target could not read back is refused before anything
    // else: its value never reached the planner.
    const [unread] = Object.entries(unreadable ?? {});
    if (unread !== undefined) {
        const [column, reason] = unread;
        return { ok: false, reason: `column '${column}' of '${rdbTable}' holds a value the target can't read: ${reason}`, column };
    }
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

    const authorCol = authorColumn(config);
    const values: RowWrite = {};
    for (const targetCol of Object.keys(targetValues)) {
        const raw = targetValues[targetCol];
        const refuse = (reason: string): Reversed => ({ ok: false, reason, column: targetCol });
        // A value rdb can't hold, such as a json value with a null inside it, is
        // refused here: sent to rdb, its op could not even be hashed, and the
        // failure could not be recorded.
        const problem = raw === null ? undefined : json.literalProblem(raw);
        if (problem !== undefined) {
            return refuse(`column '${targetCol}' of '${rdbTable}' holds a value rdb can't store: it contains ${problem}`);
        }
        // The author column is not an rdb column: a row's author is the signer
        // of its insert, and no op changes it. An insert may name the writer
        // key, which signs it, or no author. In a group without an identity
        // provider, where every op is anonymous, it may name none.
        if (targetCol === authorCol) {
            if (isUpdate) {
                // Named with the row's own author, it is set aside (HIP-29).
                // An id the key table doesn't know names no key, so it never matches.
                const named = raw === null ? null : typeof raw === 'number' ? keyLookup(raw)?.keyHash : undefined;
                if (subject !== undefined && named !== undefined && named === (subject.author ?? null)) continue;
                return refuse(`the author of a row of '${rdbTable}' can't be changed`);
            }
            if (raw === null) continue;
            if (ctx.writerKeyId === undefined) {
                return refuse(`'${rdbTable}' is in a group without an identity provider, so an insert can't name an author`);
            }
            const info = typeof raw === 'number' ? keyLookup(raw) : undefined;
            if (info === undefined) {
                return refuse(`the author column of an insert into '${rdbTable}' holds ${JSON.stringify(raw)}, which is no key id of the key table`);
            }
            if (info.keyHash !== ctx.writerKeyId) {
                return refuse(`an insert into '${rdbTable}' names another key as its author; it can only be signed with the writer key`);
            }
            continue;
        }
        // A readonly column an update names with rdb's value for the row is set
        // aside, compared once translated to rdb's value; another value is
        // refused (HIP-29).
        const readonlyUpdate = (rdbCol: string): boolean => isUpdate && def?.columns?.[rdbCol]?.readonly === true;
        const keepsValue = (rdbCol: string, value: json.Literal | null): boolean =>
            subject !== undefined && sameRdbValue(value, subject.values[rdbCol]);
        const fkInfo = rdbFkColumnFromTarget(config, rdbTable, fks, targetCol);
        if (fkInfo !== undefined) {
            const rdbCol = fkInfo.rdbColumn;
            let value: json.Literal | null = raw;
            if (raw !== null && fkInfo.resolvesToId) {
                const targetTable = fkInfo.targetTable!;
                const localRef = targetValues[targetCol] as number;
                const targetRowId = lookup(targetTable, localRef)?.rowId;
                if (targetRowId === undefined) {
                    if (readonlyUpdate(rdbCol)) return refuse(`cannot update readonly column '${rdbTable}.${rdbCol}'`);
                    return refuse(`dangling FK '${rdbTable}.${rdbCol}' -> ${targetTable}#${localRef}`);
                }
                value = targetRowId;
            }
            if (readonlyUpdate(rdbCol)) {
                if (keepsValue(rdbCol, value)) continue;
                return refuse(`cannot update readonly column '${rdbTable}.${rdbCol}'`);
            }
            values[rdbCol] = value;
            continue;
        }

        const keyRef = keyRefColumnFromTarget(config, rdbTable, provider, targetCol, identityColumns);
        if (keyRef !== undefined) {
            const rdbCol = keyRef.rdbColumn;
            if (raw === null) {
                if (readonlyUpdate(rdbCol)) {
                    if (keepsValue(rdbCol, null)) continue;
                    return refuse(`cannot update readonly column '${rdbTable}.${rdbCol}'`);
                }
                values[rdbCol] = null;
                continue;
            }
            const keyId = raw as number;
            const info = keyLookup(keyId);
            if (info === undefined) {
                if (readonlyUpdate(rdbCol)) return refuse(`cannot update readonly column '${rdbTable}.${rdbCol}'`);
                return refuse(`unknown key id ${keyId} for '${rdbTable}.${rdbCol}'`);
            }
            if (readonlyUpdate(rdbCol)) {
                if (keepsValue(rdbCol, info.keyHash)) continue;
                return refuse(`cannot update readonly column '${rdbTable}.${rdbCol}'`);
            }
            if (keyRef.isProviderKeyId) {
                if (info.publicKey === undefined) {
                    return refuse(`key id ${keyId} has no public key; call registerKey before inserting into `
                        + `identity-provider table '${rdbTable}'`);
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
        if (readonlyUpdate(rdbCol)) {
            if (keepsValue(rdbCol, raw)) continue;
            return refuse(`cannot update readonly column '${rdbTable}.${rdbCol}'`);
        }
        values[rdbCol] = raw;
    }
    // Capture fills in an empty author column with the writer key from the
    // writer record the projection keeps in the target. So an insert in a
    // signed group that still leaves it empty was written while no writer key
    // was recorded, and is refused, unless autoAuthor is off, where it is
    // anonymous (HIP-29). HIP-20's writer refuses such a write too
    // (no-writer-key).
    if (!isUpdate && authorCol !== undefined && ctx.writerKeyId !== undefined && ctx.autoAuthorOff !== true
        && (targetValues[authorCol] ?? null) === null) {
        return { ok: false, column: authorCol,
            reason: `an insert into '${rdbTable}' leaves its author empty: it was written while no writer key was recorded` };
    }
    return { ok: true, values };
}

// The columns an insert op carries, from the reversed columns of a captured
// insert. A value is kept. A null is kept only where the column has a default,
// where it means "NULL, not the default". Everywhere else it is left out: a
// target cannot tell an explicit NULL from an empty column (SQLite captures an
// insert as the whole row, with a NULL for every empty column), and in rdb the
// two read the same. That covers a column rdb's schema does not have: a deploy
// that drops a column can arrive between the local insert and its ingestion,
// and the insert asked for no value there.
function insertWrite(reversed: RowWrite, def: TableDef | undefined): RowWrite {
    const write: RowWrite = {};
    for (const [column, value] of Object.entries(reversed)) {
        if (value === null && def?.columns?.[column]?.default === undefined) continue;
        write[column] = value;
    }
    return write;
}


// The signer of an insert: the writer key, or no one under autoAuthor off when
// its author column is empty (HIP-20).
function insertSigner(change: CapturedChange, ctx: NodeContext): { signer: KeyId | undefined; anonymous: boolean } {
    const author = authorColumn(ctx.config);
    const values = change.kind === 'delete' ? {} : change.values;
    const anonymous = ctx.autoAuthorOff === true && author !== undefined && (values[author] ?? null) === null;
    return { signer: anonymous ? undefined : ctx.writerKeyId, anonymous };
}

// Plan one captured change (HIP-29). `lookup` gives each local id's identity,
// the stored mapping or one the pass gave it: an insert on a local id that has
// one is refused, as is an update or a delete of a row whose insert was
// refused. An insert is minted a uuid and a row hash, also when it is refused,
// so its failure event carries them.
export function planChange(
    change: CapturedChange,
    ctx: NodeContext,
    lookup: MappingLookup,
    keyLookup: KeyLookup,
    newUuid: () => string,
): ChangePlan {
    const { config, writerKeyId, groupId } = ctx;
    const table = change.table;
    const localId = change.localId;
    const rdbTable = rdbTableName(config, table);
    const sources = [change];
    const refused = (reject: Omit<PlannedReject, 'change' | 'table' | 'localId' | 'kind' | 'sources'>): ChangePlan =>
        ({ kind: 'refused', reject: { change, table, localId, kind: change.kind, sources, ...reject } });

    if (!ctx.schemaView.hasTable(rdbTable)) {
        return refused({ reason: `no rdb table '${rdbTable}' for target table '${table}'` });
    }
    const identity = lookup(table, localId);

    if (change.kind === 'insert') {
        if (identity !== undefined) {
            // An INSERT OR REPLACE over a row, an insert that reuses a deleted
            // row's id, or a second insert of one local id: its revert reads
            // that identity's row.
            return refused({ rowId: identity.rowId, uuid: identity.uuid, author: identity.author,
                op: { table: rdbTable, op: { action: 'insert', rowId: identity.rowId, uuid: identity.uuid, values: {} } },
                reason: `local row ${table}#${localId} already has an rdb identity (status ${identity.status ?? 'active'}); `
                    + 'an insert on its id is refused' });
        }
        const { signer, anonymous } = insertSigner(change, ctx);
        const uuid = newUuid();
        const rowId = deriveRowId(uuid, signer);
        const rev = reverseCols(rdbTable, change.values, ctx, false, lookup, keyLookup, change.unreadable);
        if (!rev.ok) {
            return refused({ rowId, uuid, author: signer, column: rev.column, reason: rev.reason,
                op: { table: rdbTable, op: { action: 'insert', rowId, uuid, values: {} } } });
        }
        const op: InsertRowPayload = {
            action: 'insert', rowId, uuid,
            ...rowWriteParts(insertWrite(rev.values, ctx.schemaView.getTable?.(rdbTable))),
        };
        const mapping: SyncMapping = { table, localId, rowId, uuid, ...(signer !== undefined ? { author: signer } : {}), status: 'active' };
        return { kind: 'op', op: { groupId, rdbTable, table, localId, kind: 'insert', write: { table: rdbTable, op }, change, mapping,
            ...(anonymous ? { anonymous: true as const } : {}) } };
    }

    if (identity === undefined) {
        return refused({ reason: `cannot ${change.kind} unknown local row ${table}#${localId}` });
    }
    if (identity.status === 'ingestion_failure') {
        return refused({ rowId: identity.rowId, reason: `local row ${table}#${localId}'s insert was refused; its ${change.kind} is refused` });
    }
    const rowId = identity.rowId;

    if (change.kind === 'delete') {
        const op: RowOpPayload = { action: 'delete', rowId };
        return { kind: 'op', op: { groupId, rdbTable, table, localId, kind: 'delete', write: { table: rdbTable, op }, change } };
    }

    // An update lands or is refused whole, and keeps every null: it names only
    // columns the app wrote, so a NULL there is a clear.
    const subject = ctx.subjectRow?.(rdbTable, rowId);
    const rev = reverseCols(rdbTable, change.values, ctx, true, lookup, keyLookup, change.unreadable, subject);
    if (!rev.ok) {
        return refused({ rowId, column: rev.column, reason: rev.reason, op: { table: rdbTable, op: { action: 'update', rowId, values: {} } } });
    }
    if (Object.keys(rev.values).length === 0) return { kind: 'nothing' };   // every named column set aside
    return { kind: 'op', op: { groupId, rdbTable, table, localId, kind: 'update',
        write: { table: rdbTable, op: { action: 'update', rowId, ...rowWriteParts(rev.values) } }, change } };
}

// ---------------------------------------------------------------------------
// Batch entry points (tests and the planner fuzzer): plan each change in
// order, as a pass does, with the identities the batch gives its local ids.
// ---------------------------------------------------------------------------

export function planDatabaseEntries(
    batch: CapturedBatch,
    ctxFor: (targetTable: string) => NodeContext | undefined,
    lookup: MappingLookup,
    keyLookup: KeyLookup,
    newUuid: () => string,
): IngestPlan {
    const given = new Map<string, SyncMapping>();
    const passLookup: MappingLookup = (table, localId) => given.get(keyOf(table, localId)) ?? lookup(table, localId);
    const plan: IngestPlan = { entries: [], rejects: [], nothing: [] };
    for (const change of batch.changes) {
        const ctx = ctxFor(change.table);
        if (ctx === undefined) { plan.nothing.push(change); continue; }
        const planned = planChange(change, ctx, passLookup, keyLookup, newUuid);
        const key = keyOf(change.table, change.localId);
        if (planned.kind === 'nothing') { plan.nothing.push(change); continue; }
        if (planned.kind === 'op') {
            plan.entries.push({ groupId: planned.op.groupId, ops: [planned.op] });
            if (planned.op.mapping !== undefined) given.set(key, planned.op.mapping);
            continue;
        }
        plan.rejects.push(planned.reject);
        // A refused insert gives its local id an identity for the rest of the pass.
        const r = planned.reject;
        if (change.kind === 'insert' && passLookup(change.table, change.localId) === undefined && r.rowId !== undefined) {
            given.set(key, { table: change.table, localId: change.localId, rowId: r.rowId, uuid: r.uuid ?? '', status: 'ingestion_failure' });
        }
    }
    return plan;
}

// Single-group convenience wrapper (one fixed context for every table).
export function changesToEntries(
    batch: CapturedBatch,
    schemaView: RSchemaView,
    lookup: MappingLookup,
    config: AdapterConfig,
    writerKeyId: KeyId | undefined,
    newUuid: () => string,
    keyLookup: KeyLookup = () => undefined,
): IngestPlan {
    const ctx: NodeContext = { groupId: '', schemaView, config, writerKeyId };
    return planDatabaseEntries(batch, () => ctx, lookup, keyLookup, newUuid);
}
