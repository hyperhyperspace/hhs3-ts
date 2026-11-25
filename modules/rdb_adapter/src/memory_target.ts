// MemoryTarget: a self-contained, in-memory MaterializationTarget. A shipped
// artifact (not test-only): it projects an rdb RTableGroup into plain
// in-process state, so a browser / worker / test can materialize without a
// native SQLite dependency. It mirrors the semantics SqliteTarget defines:
//
//   - per-table meta (system-column names, sync table, {col: ColumnType});
//   - the content-addressed rowId maps to a projection-local serial `id`
//     through a per-table sync map, allocated on first sight and KEPT across
//     delete so a void-flip reinstatement reuses the same `id`;
//   - a shared `rdb_keys` map (key hash -> {id, publicKey?}) that authors and
//     key-ref columns intern into;
//   - apply() runs schema actions THEN row actions THEN the checkpoint commit
//     atomically: it works on a clone and swaps it in only if the whole batch
//     succeeds, so a throw never leaves a partially-applied state or a
//     checkpoint the target does not reflect.
//   - projection-local indexes as BOOKKEEPING only (the installed spec plus the
//     materialized index records; nothing is physically indexed), so the core
//     index lifecycle can be exercised without SQLite. It has no options
//     format, so a declaration carrying `options` is refused.
//
// Values are stored as their logical json.Literal (no engine coercion), so the
// read accessors return exactly what was projected.

import { json } from "@hyper-hyper-space/hhs3_json";
import type { B64Hash, KeyId } from "@hyper-hyper-space/hhs3_crypto";
import type { ColumnType } from "@hyper-hyper-space/hhs3_rdb";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";

import { compareSortTuples, evalWhere, normalizeOrder, projectColumns, rowOnSide } from "./query_eval.js";
import {
    CapturedBatch, CapturedChange, CapturedValues, ChangeSignalListener, ChangeSignalSource,
    CheckpointMovedError, PendingChangesError, IndexDecl, IndexSpec, IndexState, IndexTarget, IngestSettle, KeyIndex,
    MaterializationTarget, MaterializedChangeSource, OpEvent, OpEventQuery, pageOpEvents,
    ProjectedRow, QueryableTarget, ReactiveChangeTarget, ResolvedIndex,
    RowAction, RowIdentityIndex, SchemaAction, SingleTableQueryPlan, StoredOpEvent,
    SyncMapping, SyncStatus, TargetCommitEvent, TargetCommitListener, TargetRowChange, versionsEqual,
    CapturedChangeId, ChangeRecord, mergeChangeRecord, opEventRows, withLocalIds,
    WriterRecord, WriterRecordStore,
} from "./types.js";

type RowValues = { [column: string]: json.Literal };

// A materialized app row: its author key id (when the author column is
// projected) and its logical column values. Author is a numeric id into
// rdb_keys, not a raw KeyId.
export type MemoryRow = {
    author?: number;
    values: RowValues;
};

type KeyEntry = { id: number; keyHash: string; publicKey?: string };

// Per-table state, the in-memory analogue of SqliteTarget's app table + sync
// table + rdb_table_meta row.
type TableState = {
    idColumn: string;
    authorColumn: string | undefined;
    syncTable: string;
    columnTypes: { [column: string]: ColumnType };
    // rowId -> projection-local serial id. Allocated on first sight, kept
    // across delete (id stability across void-flip reinstatement).
    sync: Map<string, number>;
    // rowId -> lifecycle status (active / deleted / ingestion_failure). Kept
    // alongside `sync` so a sync record survives delete AND records why.
    syncStatus: Map<string, SyncStatus>;
    nextId: number;
    // live app rows by their projection-local id (a deleted row is removed
    // here but its sync entry survives).
    rows: Map<number, MemoryRow>;
    // Inbound only: local id -> its rdb identity, written back by commitIngest
    // for rows minted during ingestion (carries the uuid a bare `sync` lacks).
    syncInfo: Map<number, { rowId: string; uuid: string; author?: KeyId }>;
    // Local FK columns (projected `<col>_id` names) -> the target table they
    // reference. On upsert their rowId value is translated to the target's local
    // id; cross-group `_row_hash` columns are absent (passthrough).
    fkColumns: { [column: string]: { targetTable: string } };
    // Key-ref columns (author_key_id is separate; these are business key_id /
    // identity `<col>_key_id`). Values arriving as key hashes are interned.
    keyRefColumns: Set<string>;
    // Per-column schema defaults (only columns that declare one; always plain
    // columns - FK / key-ref companions never carry a default). The rdb delta
    // channel omits defaulted-but-unwritten values, so the target materializes
    // them: add-column backfills existing rows and a new-row upsert fills any
    // defaulted column the insert omitted (mirroring SQLite's DDL DEFAULT).
    defaults: { [column: string]: json.Literal };
    // The group the table was created for, and its name in that group's
    // schema (HIP-20): capture reads the group's mode from the writer record.
    groupId?: B64Hash;
    schemaName?: string;
};

type Store = {
    tables: Map<string, TableState>;
    // Per-group materialized version (a shared target holds several groups of
    // one RDb). Table/sync names are globally unique, so only the checkpoint is
    // per-group.
    checkpoints: Map<B64Hash, Version>;
    // The observes an end of a pass recorded beside each group's checkpoint
    // (HIP-29). Every settle clears them all.
    endObserves: Map<B64Hash, B64Hash[]>;
    // Shared keys side table: keyHash -> entry. nextKeyId allocates new ids.
    keysByHash: Map<string, KeyEntry>;
    keysById: Map<number, KeyEntry>;
    nextKeyId: number;
    // Installed index spec and the materialized index records, keyed by
    // (table, name). Records are immutable, so a shallow map copy clones.
    indexSpec: { spec: IndexSpec; fingerprint: string } | undefined;
    indexes: Map<string, ResolvedIndex>;
};

function indexKey(table: string, name: string): string {
    return table + '\u0000' + name;
}

function cloneRow(row: MemoryRow): MemoryRow {
    const clone: MemoryRow = { values: { ...row.values } };
    if (row.author !== undefined) clone.author = row.author;
    return clone;
}

function cloneTable(state: TableState): TableState {
    const rows = new Map<number, MemoryRow>();
    for (const [id, row] of state.rows) rows.set(id, cloneRow(row));
    return {
        idColumn: state.idColumn,
        authorColumn: state.authorColumn,
        syncTable: state.syncTable,
        columnTypes: { ...state.columnTypes },
        sync: new Map(state.sync),
        syncStatus: new Map(state.syncStatus),
        nextId: state.nextId,
        rows,
        syncInfo: new Map(state.syncInfo),
        fkColumns: { ...state.fkColumns },
        keyRefColumns: new Set(state.keyRefColumns),
        defaults: { ...state.defaults },
        ...(state.groupId !== undefined ? { groupId: state.groupId } : {}),
        ...(state.schemaName !== undefined ? { schemaName: state.schemaName } : {}),
    };
}

function cloneStore(store: Store): Store {
    const tables = new Map<string, TableState>();
    for (const [name, state] of store.tables) tables.set(name, cloneTable(state));
    const checkpoints = new Map<B64Hash, Version>();
    for (const [groupId, v] of store.checkpoints) checkpoints.set(groupId, new Set(v));
    const endObserves = new Map<B64Hash, B64Hash[]>();
    for (const [groupId, list] of store.endObserves) endObserves.set(groupId, [...list]);
    const keysByHash = new Map<string, KeyEntry>();
    const keysById = new Map<number, KeyEntry>();
    for (const [hash, entry] of store.keysByHash) {
        const clone: KeyEntry = { id: entry.id, keyHash: entry.keyHash };
        if (entry.publicKey !== undefined) clone.publicKey = entry.publicKey;
        keysByHash.set(hash, clone);
        keysById.set(clone.id, clone);
    }
    return {
        tables, checkpoints, endObserves, keysByHash, keysById, nextKeyId: store.nextKeyId,
        indexSpec: store.indexSpec, indexes: new Map(store.indexes),
    };
}

export class MemoryTarget implements MaterializationTarget, MaterializedChangeSource, ChangeSignalSource,
    ReactiveChangeTarget, QueryableTarget, RowIdentityIndex, KeyIndex, IndexTarget, WriterRecordStore {
    private store: Store = {
        tables: new Map(), checkpoints: new Map(), endObserves: new Map(),
        keysByHash: new Map(), keysById: new Map(), nextKeyId: 1,
        indexSpec: undefined, indexes: new Map(),
    };
    private changeListeners = new Set<ChangeSignalListener>();
    private commitListeners = new Set<TargetCommitListener>();
    private emittedOutboxIds = new Set<number>();

    // Two-level capture config (mirrors the SQLite backend):
    //   - provisioning: whether capture exists at all (opt-in, default off);
    //   - runtime enable: whether provisioned capture is currently recording.
    // Capture state lives on the instance (not the swapped store) so a pending
    // outbox survives apply()'s clone/swap. apply() never records (it does no
    // local mutations), so echo suppression is trivially satisfied here.
    private readonly captureProvisioned: boolean;
    private captureEnabled: boolean;
    private outbox: CapturedChange[] = [];
    private outboxNextId = 1;

    // Durable op-event log (ingestion failures + concurrency flips). Instance
    // state (like the outbox) so it survives apply()'s clone/swap. Idempotent by
    // (opHash, direction): a void and a later reinstate of the same op are
    // distinct rows, each logged once.
    private opEvents: StoredOpEvent[] = [];
    private opEventNextId = 1;
    private opEventKeys = new Set<string>();

    // The writer record (HIP-20), instance state like the outbox.
    private writerRecord: WriterRecord | undefined;

    constructor(opts: { captureChanges?: boolean } = {}) {
        this.captureProvisioned = opts.captureChanges === true;
        this.captureEnabled = this.captureProvisioned;
    }

    // Called once the transaction's rows are in the store, so each row an
    // event names by hash gets the local id its mapping holds.
    private logEvents(events: OpEvent[] | undefined): StoredOpEvent[] {
        const fresh: StoredOpEvent[] = [];
        if (events === undefined) return fresh;
        for (const event of events) {
            const key = event.opHash + '\u0000' + event.direction;
            if (this.opEventKeys.has(key)) continue;
            this.opEventKeys.add(key);
            const filled = withLocalIds(event, (table, rowId) => this.store.tables.get(table)?.sync.get(rowId));
            const stored: StoredOpEvent = { id: this.opEventNextId++, event: filled };
            this.opEvents.push(stored);
            fresh.push(stored);
        }
        return fresh;
    }

    private emit(commit: TargetCommitEvent): void {
        if (commit.source === 'local') {
            if (commit.tables.length === 0) return;
            for (const listener of [...this.changeListeners]) listener({});
        }
        for (const listener of [...this.commitListeners]) listener(commit);
    }

    private announce(ids: number[]): void {
        const wanted = new Set(ids);
        const fresh = this.outbox.filter((row) => typeof row.id === 'number' && wanted.has(row.id) && !this.emittedOutboxIds.has(row.id));
        for (const row of fresh) if (typeof row.id === 'number') this.emittedOutboxIds.add(row.id);
        const live = new Set(this.outbox.map((row) => row.id).filter((id): id is number => typeof id === 'number'));
        this.emittedOutboxIds = new Set([...this.emittedOutboxIds].filter((id) => live.has(id)));
        if (fresh.length === 0) return;
        this.emit({ source: 'local', tables: [...new Set(fresh.map((row) => row.table))] });
    }

    async getCheckpoint(groupId: B64Hash): Promise<Version | undefined> {
        const v = this.store.checkpoints.get(groupId);
        return v === undefined ? undefined : new Set(v);
    }

    async apply(
        groupId: B64Hash, schemaActions: SchemaAction[], rowActions: RowAction[],
        checkpoint: Version, events?: OpEvent[], expectFrom?: Version | null,
        expectIndexSpec?: string | null,
    ): Promise<void> {
        // Compare-and-set guard (concurrent projectors): reject if the stored
        // checkpoint is not what this delta was computed against. Nothing mutates.
        if (expectFrom !== undefined
            && !versionsEqual(this.store.checkpoints.get(groupId), expectFrom ?? undefined)) {
            throw new CheckpointMovedError(groupId);
        }
        if (expectIndexSpec !== undefined && (this.store.indexSpec?.fingerprint ?? null) !== expectIndexSpec) {
            throw new CheckpointMovedError(groupId);
        }
        // No projection over a pending change (HIP-29).
        if (this.captureProvisioned && this.outbox.length > 0) throw new PendingChangesError(groupId);
        // Atomicity: mutate a clone, swap in only on full success.
        const working = cloneStore(this.store);
        for (const action of schemaActions) applySchemaAction(working, action, groupId);
        const changes: TargetRowChange[] = [];
        for (const action of rowActions) {
            const change = applyRowAction(working, action);
            if (change !== undefined) changes.push(change);
        }
        working.checkpoints.set(groupId, new Set(checkpoint));
        this.store = working;
        // Concurrency void/reinstate flips ride the same checkpoint advance.
        const logged = this.logEvents(events);
        const schemaTables = [...new Set(schemaActions.map(schemaActionTable))];
        if (changes.length > 0 || logged.length > 0 || schemaTables.length > 0) {
            this.emit({ source: 'apply', groupId, schemaTables, changes, events: logged });
        }
    }

    // -----------------------------------------------------------------------
    // Local mutation API (simulates the app writing directly to the projected
    // tables): mutates rows AND records to the outbox when capture is enabled.
    // -----------------------------------------------------------------------

    // Insert a fresh local row (no sync mapping yet - its rdb identity is minted
    // at ingestion). Returns the projection-local id. `author` here is the
    // numeric key id (into rdb_keys), matching the projected author_key_id column.
    // A null sets the column to NULL: the row keeps no key for it (as for a
    // column that was never set), and the change is recorded with the null.
    // The author column, given in `values` or as `author`, is kept as the row's
    // author, not among its values, and recorded: ingestion compares it with
    // the writer key. A value for the id column throws: memory allocates ids.
    localInsert(table: string, values: CapturedValues, author?: number): number {
        const state = requireTable(this.store, table);
        if (state.idColumn in values) {
            throw new Error(`an insert into '${table}' can't give the id column '${state.idColumn}' a value: memory allocates ids`);
        }
        const id = state.nextId++;
        const row: MemoryRow = { values: {} };
        const recorded: CapturedValues = {};
        for (const [column, value] of Object.entries(values)) {
            if (column === state.authorColumn) {
                if (typeof value === 'number') row.author = value;
                recorded[column] = value;
                continue;
            }
            if (value !== null) row.values[column] = value;
            recorded[column] = value;
        }
        if (state.authorColumn !== undefined && author !== undefined) {
            row.author = author;
            recorded[state.authorColumn] = author;
        }
        // An empty author column gets the writer key's id, in the row and in the
        // recorded change, where the table's group mode is `on` (HIP-29), as
        // SQLite's insert trigger does. Only while capture is on, as there.
        const filled = this.fillAuthor(state);
        if (state.authorColumn !== undefined && row.author === undefined && filled !== undefined) {
            row.author = filled;
            recorded[state.authorColumn] = filled;
        }
        // A column the insert leaves out gets its default, in the row and in the
        // recorded change, as SQLite's DEFAULT does. A null is not left out: it
        // is an explicit NULL.
        for (const [column, def] of Object.entries(state.defaults)) {
            if (column in values) continue;
            row.values[column] = def;
            recorded[column] = def;
        }
        state.rows.set(id, row);
        this.record({ kind: 'insert', table, localId: id, values: recorded });
        return id;
    }

    // Every column it is given is written and recorded, changed or not (HIP-29:
    // a named column is a write). An update that names no column but the id
    // records nothing. An update of a row the table doesn't have does nothing
    // and returns false, since rdb accepts an update only of a live row. The id
    // can't change, as on SQLite: a value for the id column is set aside when it
    // is the row's own id, and throws otherwise.
    localUpdate(table: string, localId: number, values: CapturedValues): boolean {
        const state = requireTable(this.store, table);
        const row = state.rows.get(localId);
        if (row === undefined) return false;
        if (state.idColumn in values && values[state.idColumn] !== localId) {
            throw new Error(`the id of row ${localId} of '${table}' can't change (an update gives '${state.idColumn}' the value ${JSON.stringify(values[state.idColumn])})`);
        }
        const changed: CapturedValues = {};
        for (const [column, value] of Object.entries(values)) {
            if (column === state.idColumn) continue;
            if (column === state.authorColumn) {
                // recorded: ingestion sets it aside when it is the row's own
                // author, and refuses another
                if (typeof value === 'number') row.author = value;
                else delete row.author;
                changed[column] = value;
                continue;
            }
            if (value === null) delete row.values[column];
            else row.values[column] = value;
            changed[column] = value;
        }
        if (Object.keys(changed).length > 0) this.record({ kind: 'update', table, localId, values: changed });
        return true;
    }

    // A delete of a row the table doesn't have does nothing, records nothing,
    // and returns false: rdb accepts a delete only of a live row.
    localDelete(table: string, localId: number): boolean {
        const state = requireTable(this.store, table);
        if (!state.rows.delete(localId)) return false;
        this.record({ kind: 'delete', table, localId });
        return true;
    }

    // Runtime enable/disable (level 2). No-op unless provisioned.
    setCaptureEnabled(on: boolean): void {
        if (this.captureProvisioned) this.captureEnabled = on;
    }

    // The key id capture fills an empty author column with: the writer key's,
    // where the writer record says the table's group mode is `on`, and capture
    // is on. Otherwise none: with no writer record, nothing is filled (HIP-29).
    private fillAuthor(state: TableState): number | undefined {
        if (!this.captureProvisioned || !this.captureEnabled) return undefined;
        const record = this.writerRecord;
        if (record === undefined || record.writerKeyId === undefined || state.groupId === undefined) return undefined;
        return record.groups[state.groupId]?.mode === 'on' ? record.writerKeyId : undefined;
    }

    private record(change:
        | { kind: 'insert'; table: string; localId: number; values: CapturedValues }
        | { kind: 'update'; table: string; localId: number; values: CapturedValues }
        | { kind: 'delete'; table: string; localId: number }): void {
        if (!this.captureProvisioned || !this.captureEnabled) return;
        const id = this.outboxNextId++;
        this.outbox.push({ id, ...change });
        this.announce([id]);
    }

    // -----------------------------------------------------------------------
    // ChangeSignalSource (optional inbound reactivity). In-process capture fires
    // listeners inline from record(), so no external monitor is needed here.
    // -----------------------------------------------------------------------

    addChangeListener(listener: ChangeSignalListener): void {
        this.changeListeners.add(listener);
    }

    removeChangeListener(listener: ChangeSignalListener): void {
        this.changeListeners.delete(listener);
    }

    addCommitListener(listener: TargetCommitListener): void {
        this.commitListeners.add(listener);
    }

    removeCommitListener(listener: TargetCommitListener): void {
        this.commitListeners.delete(listener);
    }

    // In-memory only. Never writes rows and never forwards the event.
    notifyExternalCommit(commit: TargetCommitEvent): void {
        for (const listener of [...this.commitListeners]) listener(commit);
    }

    async executeQuery(plan: SingleTableQueryPlan): Promise<ProjectedRow[]> {
        const state = this.store.tables.get(plan.table);
        if (state === undefined) return [];
        const needsOrder = plan.limit !== undefined || plan.cursor !== undefined;
        const orderBy = normalizeOrder(plan.orderBy, state.idColumn, needsOrder);
        const typeOf = (column: string): ColumnType | undefined =>
            column === state.idColumn ? 'integer'
                : column === state.authorColumn ? 'integer'
                : state.columnTypes[column];
        let rows = [...state.rows.entries()].map(([id, row]) => this.asView(state, id, row));
        const direct = directIds(plan.where, state.idColumn);
        if (direct !== undefined) {
            const wanted = new Set(direct);
            rows = rows.filter((row) => wanted.has(row.id));
        }
        rows = rows.filter((row) => evalWhere(plan.where, row, row.id));
        if (plan.cursor !== undefined && orderBy.length > 0) {
            const anchor = this.anchorView(state, plan.cursor.id, plan.cursor.cachedValues);
            if (anchor === undefined) return [];
            rows = rows.filter((row) => rowOnSide(row, anchor, orderBy, plan.cursor!.side, typeOf));
        }
        if (orderBy.length > 0) rows.sort((a, b) => compareSortTuples(a, b, orderBy, typeOf));
        if (plan.limit !== undefined) rows = rows.slice(0, plan.limit);
        return rows.map((row) => projectColumns(row, plan.select, state.idColumn));
    }

    // A query row: memory keeps a NULL as a missing key, and a row gives null
    // for each column of the table it has no value in, the author column
    // included, as SQLite and IndexedDB do. It shows only the table's columns,
    // so no stored key can stand in for the id.
    private asView(state: TableState, id: number, row: MemoryRow): ProjectedRow {
        const view: ProjectedRow = { id };
        if (state.idColumn !== 'id') view[state.idColumn] = id;
        for (const column of Object.keys(state.columnTypes)) view[column] = row.values[column] ?? null;
        if (state.authorColumn !== undefined) view[state.authorColumn] = row.author ?? null;
        return view;
    }

    private anchorView(
        state: TableState, id: number,
        cached: { [column: string]: json.Literal | null } | undefined,
    ): ProjectedRow | undefined {
        const row = state.rows.get(id);
        if (row !== undefined) return this.asView(state, id, row);
        if (cached === undefined) return undefined;
        return { id, ...cached };
    }

    // -----------------------------------------------------------------------
    // MaterializedChangeSource (inbound). Meaningful only when provisioned.
    // -----------------------------------------------------------------------

    async hasPendingChanges(): Promise<boolean> {
        return this.captureProvisioned && this.outbox.length > 0;
    }

    async recordWriter(record: WriterRecord): Promise<void> {
        this.writerRecord = { ...record, groups: { ...record.groups } };
    }

    async readWriterRecord(): Promise<WriterRecord | undefined> {
        return this.writerRecord === undefined ? undefined : { ...this.writerRecord, groups: { ...this.writerRecord.groups } };
    }

    // The table's group and its name in the schema, from the table's meta.
    tableGroup(table: string): { groupId?: B64Hash; schemaName?: string } | undefined {
        const state = this.store.tables.get(table);
        if (state === undefined) return undefined;
        return { ...(state.groupId !== undefined ? { groupId: state.groupId } : {}),
            ...(state.schemaName !== undefined ? { schemaName: state.schemaName } : {}) };
    }

    async drainChanges(): Promise<CapturedBatch> {
        this.requireCapture();
        this.announce(this.outbox.map((row) => row.id).filter((id): id is number => typeof id === 'number'));
        return { changes: [...this.outbox] };
    }

    async resolveRow(table: string, localId: number): Promise<SyncMapping | undefined> {
        this.requireCapture();
        const state = this.store.tables.get(table);
        if (state === undefined) return undefined;
        const info = state.syncInfo.get(localId);
        if (info !== undefined) {
            const m: SyncMapping = { table, localId, rowId: info.rowId, uuid: info.uuid,
                status: state.syncStatus.get(info.rowId) ?? 'active' };
            if (info.author !== undefined) m.author = info.author;
            return m;
        }
        // A projected (rdb-originated) row has a bare sync entry but no uuid.
        for (const [rowId, id] of state.sync) {
            if (id === localId) return { table, localId, rowId, uuid: '', status: state.syncStatus.get(rowId) ?? 'active' };
        }
        return undefined;
    }

    async writeRecord(id: CapturedChangeId, record: ChangeRecord): Promise<void> {
        this.requireCapture();
        // A new entry, so a batch an earlier drain returned keeps what it read.
        this.outbox = this.outbox.map((row) => row.id === id ? { ...row, record: mergeChangeRecord(row.record, record) } : row);
    }

    async recordEndObserve(groupId: B64Hash, hash: B64Hash): Promise<void> {
        this.requireCapture();
        this.store.endObserves.set(groupId, [...(this.store.endObserves.get(groupId) ?? []), hash]);
    }

    async getEndObserves(groupId: B64Hash): Promise<B64Hash[]> {
        return [...(this.store.endObserves.get(groupId) ?? [])];
    }

    // The settle, in HIP-29's order (see IngestSettle). It is atomic, as apply
    // is: it works on a clone of the store and swaps it in, with the outbox and
    // the events, only when every step succeeded.
    async commitIngest(settle: IngestSettle): Promise<void> {
        this.requireCapture();
        // 1. Each group's checkpoint is still the one the pass found.
        for (const { groupId, expected } of settle.checkpoints ?? []) {
            if (!versionsEqual(this.store.checkpoints.get(groupId), expected)) throw new CheckpointMovedError(groupId);
        }
        // 2. The handled rows leave the outbox; the rest is read.
        const consumedSet = new Set(settle.consumed);
        const handled = this.outbox.filter((c) => consumedSet.has(c.id));
        const remaining = this.outbox.filter((c) => !consumedSet.has(c.id));
        const working = cloneStore(this.store);
        // 3. The handled inserts' mappings, then the accepted deletes.
        for (const m of settle.inserts ?? []) {
            const state = requireTable(working, m.table);
            state.sync.set(m.rowId, m.localId);
            const info: { rowId: string; uuid: string; author?: KeyId } = { rowId: m.rowId, uuid: m.uuid };
            if (m.author !== undefined) info.author = m.author;
            state.syncInfo.set(m.localId, info);
            state.syncStatus.set(m.rowId, m.status ?? 'active');
        }
        for (const d of settle.deletes ?? []) {
            const state = requireTable(working, d.table);
            if (state.sync.has(d.rowId)) state.syncStatus.set(d.rowId, 'deleted');
        }
        // 4. The rows of refused inserts that had no identity, and 5. the reverts,
        // each with its changes still in the outbox on top.
        const reverted = new Set<string>();
        for (const o of settle.orphans ?? []) {
            revertRow(working, { table: o.table, localId: o.localId }, remaining, false);
            reverted.add(o.table);
        }
        for (const r of settle.reverts ?? []) {
            revertRow(working, r, remaining, true);
            reverted.add(r.table);
        }
        // 6. Memory keeps json values, not text: nothing to rewrite.
        // 8. The new checkpoints, and every group's end observes cleared.
        for (const { groupId, next } of settle.checkpoints ?? []) working.checkpoints.set(groupId, new Set(next));
        working.endObserves = new Map();

        this.store = working;
        this.outbox = remaining;
        this.emittedOutboxIds = new Set(remaining.map((row) => row.id).filter((id): id is number => typeof id === 'number'));
        // 7. The handled rows' failure events, then the settle's own
        // (idempotent by (opHash, direction)).
        const failures = handled.map((c) => c.record?.failure).filter((e): e is OpEvent => e !== undefined);
        const logged = this.logEvents([...failures, ...(settle.events ?? [])]);
        const revertedTables = [...new Set([
            ...reverted,
            ...logged.flatMap((stored) => opEventRows(stored.event).map((row) => row.table)),
        ])];
        if (reverted.size > 0 || logged.length > 0) {
            this.emit({ source: 'settle', revertedTables, events: logged });
        }
    }

    async drainOpEvents(opts?: OpEventQuery | number): Promise<StoredOpEvent[]> {
        // No requireCapture: concurrency void/reinstate events are logged by
        // apply() even on a read-only projection, so the log is always readable.
        return pageOpEvents(this.opEvents, opts);
    }

    private requireCapture(): void {
        if (!this.captureProvisioned) {
            throw new Error("this MemoryTarget was not provisioned for change capture (new MemoryTarget({ captureChanges: true }))");
        }
    }

    // -----------------------------------------------------------------------
    // Read accessors (the projection's read side; wrapped by the test suite's
    // ProjectionReader, and usable directly by in-memory consumers).
    // -----------------------------------------------------------------------

    hasTable(table: string): boolean {
        return this.store.tables.has(table);
    }

    listTables(): string[] {
        return [...this.store.tables.keys()];
    }

    columnTypes(table: string): { [column: string]: ColumnType } | undefined {
        const state = this.store.tables.get(table);
        return state === undefined ? undefined : { ...state.columnTypes };
    }

    // The live rowIds of a table (a deleted row is absent, even though its sync
    // entry survives).
    getRowIds(table: string): string[] {
        const state = this.store.tables.get(table);
        if (state === undefined) return [];
        const ids: string[] = [];
        for (const [rowId, id] of state.sync) {
            if (state.rows.has(id)) ids.push(rowId);
        }
        return ids;
    }

    getRowByRowId(table: string, rowId: string): MemoryRow | undefined {
        const state = this.store.tables.get(table);
        if (state === undefined) return undefined;
        const id = state.sync.get(rowId);
        if (id === undefined) return undefined;
        const row = state.rows.get(id);
        return row === undefined ? undefined : cloneRow(row);
    }

    // The projection-local serial id for a rowId, or undefined when the rowId
    // was never seen. Survives delete (id stability).
    syncId(table: string, rowId: string): number | undefined {
        return this.store.tables.get(table)?.sync.get(rowId);
    }

    // -----------------------------------------------------------------------
    // RowIdentityIndex (local id <-> content hash)
    // -----------------------------------------------------------------------

    async rowHashForLocalId(table: string, id: number): Promise<string | undefined> {
        const state = this.store.tables.get(table);
        if (state === undefined) return undefined;
        for (const [rowId, localId] of state.sync) {
            if (localId === id) return rowId;
        }
        return undefined;
    }

    async localIdForRowHash(table: string, rowHash: string): Promise<number | undefined> {
        return this.store.tables.get(table)?.sync.get(rowHash);
    }

    // -----------------------------------------------------------------------
    // KeyIndex (rdb_keys: key hash + public key <-> numeric id)
    // -----------------------------------------------------------------------

    async registerKey(_domain: string, keyHash: string, publicKey: string): Promise<number> {
        return internKey(this.store, keyHash, publicKey);
    }

    async keyHashForId(_domain: string, id: number): Promise<string | undefined> {
        return this.store.keysById.get(id)?.keyHash;
    }

    async publicKeyForId(_domain: string, id: number): Promise<string | undefined> {
        return this.store.keysById.get(id)?.publicKey;
    }

    async idForKeyHash(_domain: string, keyHash: string): Promise<number | undefined> {
        return this.store.keysByHash.get(keyHash)?.id;
    }

    // -----------------------------------------------------------------------
    // IndexTarget (bookkeeping only)
    // -----------------------------------------------------------------------

    async getIndexState(): Promise<IndexState> {
        const state: IndexState = { materialized: [...this.store.indexes.values()] };
        if (this.store.indexSpec !== undefined) {
            state.spec = this.store.indexSpec.spec;
            state.specFingerprint = this.store.indexSpec.fingerprint;
        }
        return state;
    }

    validateIndexOptions(decl: IndexDecl): string | undefined {
        return decl.options === undefined ? undefined : 'the memory target takes no index options';
    }

    async installIndexSpec(
        spec: IndexSpec, specFingerprint: string, actions: SchemaAction[],
        expect: { specFingerprint: string | undefined; checkpoints: Map<B64Hash, Version> },
    ): Promise<void> {
        if (this.store.indexSpec?.fingerprint !== expect.specFingerprint) {
            throw new CheckpointMovedError([...expect.checkpoints.keys()][0] ?? '');
        }
        for (const [groupId, cp] of expect.checkpoints) {
            if (!versionsEqual(this.store.checkpoints.get(groupId), cp)) throw new CheckpointMovedError(groupId);
        }
        const working = cloneStore(this.store);
        for (const action of actions) applySchemaAction(working, action);
        working.indexSpec = { spec, fingerprint: specFingerprint };
        this.store = working;
    }
}

// ---------------------------------------------------------------------------
// Schema channel
// ---------------------------------------------------------------------------

function applySchemaAction(store: Store, action: SchemaAction, groupId?: B64Hash): void {
    switch (action.kind) {
        case 'create-table': return createTable(store, action, groupId);
        case 'drop-table': return dropTable(store, action);
        case 'add-column': return addColumn(store, action);
        case 'drop-column': return dropColumn(store, action);
        case 'ensure-index': return ensureIndex(store, action.index);
        case 'drop-index':
            store.indexes.delete(indexKey(action.table, action.name));
            return;
    }
}

// Mirrors a real CREATE INDEX: the table and every column must exist, and the
// name must be free (a surviving identical index is never re-ensured).
function ensureIndex(store: Store, index: ResolvedIndex): void {
    const state = requireTable(store, index.table);
    for (const c of index.columns) {
        if (state.columnTypes[c.target] === undefined && c.target !== state.authorColumn) {
            throw new Error(`cannot index '${index.table}.${c.target}': no such column`);
        }
    }
    const key = indexKey(index.table, index.name);
    if (store.indexes.has(key)) throw new Error(`index '${index.name}' already exists on '${index.table}'`);
    store.indexes.set(key, index);
}

function requireTable(store: Store, table: string): TableState {
    const state = store.tables.get(table);
    if (state === undefined) throw new Error(`no materialized metadata for table '${table}'`);
    return state;
}

function createTable(store: Store, action: Extract<SchemaAction, { kind: 'create-table' }>, groupId?: B64Hash): void {
    for (const index of store.indexes.values()) {
        if (index.table === action.table) {
            throw new Error(`cannot recreate '${action.table}': index '${index.name}' is still recorded on it`);
        }
    }
    const columnTypes: { [column: string]: ColumnType } = {};
    const fkColumns: { [column: string]: { targetTable: string } } = {};
    const keyRefColumns = new Set<string>();
    const defaults: { [column: string]: json.Literal } = {};
    for (const c of action.columns) {
        columnTypes[c.name] = c.def.type;
        if (c.fk !== undefined) fkColumns[c.name] = { targetTable: c.fk.targetTable };
        if (c.keyRef === true) keyRefColumns.add(c.name);
        if (c.def.default !== undefined) defaults[c.name] = c.def.default;
    }
    store.tables.set(action.table, {
        idColumn: action.primaryKey,
        authorColumn: action.authorColumn,
        syncTable: action.syncTable,
        columnTypes,
        sync: new Map(),
        syncStatus: new Map(),
        nextId: 1,
        rows: new Map(),
        syncInfo: new Map(),
        fkColumns,
        keyRefColumns,
        defaults,
        ...(groupId !== undefined ? { groupId } : {}),
        schemaName: action.schemaName ?? action.table,
    });
}

function dropTable(store: Store, action: Extract<SchemaAction, { kind: 'drop-table' }>): void {
    store.tables.delete(action.table);
    // A dropped table takes its indexes with it (as DROP TABLE does in SQL).
    for (const [key, index] of store.indexes) {
        if (index.table === action.table) store.indexes.delete(key);
    }
}

function addColumn(store: Store, action: Extract<SchemaAction, { kind: 'add-column' }>): void {
    const state = requireTable(store, action.table);
    state.columnTypes[action.column] = action.def.type;
    if (action.fk !== undefined) state.fkColumns[action.column] = { targetTable: action.fk.targetTable };
    if (action.keyRef === true) state.keyRefColumns.add(action.column);
    // The rdb delta channel never enumerates a defaulted column for existing
    // rows, so backfill them here (as SQLite's ALTER ADD COLUMN ... DEFAULT
    // does). Only live rows are present in `state.rows`.
    if (action.def.default !== undefined) {
        state.defaults[action.column] = action.def.default;
        for (const row of state.rows.values()) {
            if (row.values[action.column] === undefined) row.values[action.column] = action.def.default;
        }
    }
}

function dropColumn(store: Store, action: Extract<SchemaAction, { kind: 'drop-column' }>): void {
    const state = requireTable(store, action.table);
    // As SQLite: an indexed column cannot be dropped (the planner drops the
    // index first).
    for (const index of store.indexes.values()) {
        if (index.table === action.table && index.columns.some((c) => c.target === action.column)) {
            throw new Error(`cannot drop '${action.table}.${action.column}': index '${index.name}' uses it`);
        }
    }
    delete state.columnTypes[action.column];
    delete state.fkColumns[action.column];
    state.keyRefColumns.delete(action.column);
    delete state.defaults[action.column];
    for (const row of state.rows.values()) delete row.values[action.column];
}

// ---------------------------------------------------------------------------
// Row channel
// ---------------------------------------------------------------------------

function directIds(where: SingleTableQueryPlan['where'], idColumn: string): number[] | undefined {
    if (where === undefined) return undefined;
    if (where.kind === 'cmp' && where.op === '=' && where.left.kind === 'col' && where.left.name === idColumn
        && where.right.kind === 'lit' && typeof where.right.value === 'number') {
        return [where.right.value];
    }
    if (where.kind === 'in' && !where.not && where.arg.kind === 'col' && where.arg.name === idColumn) {
        const ids = where.values.filter((value): value is number => typeof value === 'number');
        return ids.length === where.values.length ? ids : undefined;
    }
    return undefined;
}

function schemaActionTable(action: SchemaAction): string {
    return action.kind === 'ensure-index' ? action.index.table : action.table;
}

function applyRowAction(store: Store, action: RowAction): TargetRowChange | undefined {
    if (action.kind === 'upsert-row') return upsertRow(store, action);
    return deleteRow(store, action);
}

function fullImage(state: TableState, id: number, row: MemoryRow): { [column: string]: json.Literal | null } {
    const values: { [column: string]: json.Literal | null } = { [state.idColumn]: id };
    for (const column of Object.keys(state.columnTypes)) {
        values[column] = row.values[column] ?? null;
    }
    if (state.authorColumn !== undefined) values[state.authorColumn] = row.author ?? null;
    return values;
}

// Allocate (or reuse) the serial id for a rowId in the sync map.
function allocateId(state: TableState, rowId: string): number {
    const existing = state.sync.get(rowId);
    if (existing !== undefined) return existing;
    const id = state.nextId++;
    state.sync.set(rowId, id);
    return id;
}

// Intern a key hash into rdb_keys, optionally backfilling public_key.
function internKey(store: Store, keyHash: string, publicKey?: string): number {
    const existing = store.keysByHash.get(keyHash);
    if (existing !== undefined) {
        if (publicKey !== undefined && existing.publicKey === undefined) {
            existing.publicKey = publicKey;
        }
        return existing.id;
    }
    const id = store.nextKeyId++;
    const entry: KeyEntry = { id, keyHash };
    if (publicKey !== undefined) entry.publicKey = publicKey;
    store.keysByHash.set(keyHash, entry);
    store.keysById.set(id, entry);
    return id;
}

// The stored value for a projected column: a local FK column carries an rdb
// rowId, translated to the referenced table's local id; a key-ref column
// carries a key hash, interned into rdb_keys (with optional public key from
// keyMaterial); plain columns pass through.
function valueForColumn(
    store: Store, state: TableState, column: string, value: json.Literal,
    keyMaterial?: { [keyHash: string]: string },
): json.Literal {
    const fk = state.fkColumns[column];
    if (fk !== undefined) {
        const targetState = requireTable(store, fk.targetTable);
        return allocateId(targetState, value as string);
    }
    if (state.keyRefColumns.has(column)) {
        const keyHash = value as string;
        return internKey(store, keyHash, keyMaterial?.[keyHash]);
    }
    return value;
}

function upsertRow(store: Store, action: Extract<RowAction, { kind: 'upsert-row' }>): TargetRowChange {
    const state = requireTable(store, action.table);
    const id = allocateId(state, action.rowId);
    const existed = state.rows.has(id);
    // A (re)materialized row is live: active (reinstates a prior deleted /
    // ingestion_failure record for the same rowId).
    state.syncStatus.set(action.rowId, 'active');
    const setAuthor = state.authorColumn !== undefined && action.author !== undefined;
    const authorId = setAuthor ? internKey(store, action.author!) : undefined;

    // Apply any keyMaterial first so subsequent key-ref interns see the pubkey.
    if (action.keyMaterial !== undefined) {
        for (const [hash, pk] of Object.entries(action.keyMaterial)) {
            internKey(store, hash, pk);
        }
    }

    // A null in the action means NULL, which this target keeps as a missing
    // key: no id is allocated and no key interned for it.
    const existing = state.rows.get(id);
    if (existing !== undefined) {
        for (const [column, value] of Object.entries(action.values)) {
            if (value === null) delete existing.values[column];
            else existing.values[column] = valueForColumn(store, state, column, value, action.keyMaterial);
        }
        // An action without an author is for a row rdb has none for: the author
        // column is cleared, whatever a local write left in it.
        if (setAuthor) existing.author = authorId;
        else delete existing.author;
        return { kind: 'update', table: action.table, localId: id, values: fullImage(state, id, existing) };
    }

    const values: RowValues = {};
    for (const [column, value] of Object.entries(action.values)) {
        if (value !== null) values[column] = valueForColumn(store, state, column, value, action.keyMaterial);
    }
    // Fill schema defaults the insert omitted (as SQLite's create-table DEFAULT
    // does on INSERT). New-row only: an update never re-defaults. Defaults are
    // plain-column values, stored directly (no FK / key-ref translation). The
    // test is whether the action names the column, not whether the row has
    // the key: a column the action sets to NULL has no key and takes no default.
    for (const [column, def] of Object.entries(state.defaults)) {
        if (!(column in action.values)) values[column] = def;
    }
    const row: MemoryRow = { values };
    if (setAuthor) row.author = authorId;
    state.rows.set(id, row);
    return { kind: existed ? 'update' : 'insert', table: action.table, localId: id, values: fullImage(state, id, row) };
}

// Sets a row to rdb's row, or to none, then puts its changes still in the
// outbox back on top, as capture recorded them (HIP-29's Reverts). A row named
// only by its row hash is found through the sync map. `markDeleted`: a row
// removed this way has its active mapping marked deleted.
function revertRow(
    store: Store, revert: { table: string; rowId?: B64Hash; localId?: number; row?: Extract<RowAction, { kind: 'upsert-row' }> },
    remaining: CapturedChange[], markDeleted: boolean,
): void {
    const state = requireTable(store, revert.table);
    let id = revert.localId ?? (revert.rowId !== undefined ? state.sync.get(revert.rowId) : undefined);
    if (revert.row !== undefined) {
        id = upsertRow(store, revert.row).localId;
    } else if (id !== undefined) {
        state.rows.delete(id);
        const rowId = revert.rowId;
        if (markDeleted && rowId !== undefined && state.sync.has(rowId) && (state.syncStatus.get(rowId) ?? 'active') === 'active') {
            state.syncStatus.set(rowId, 'deleted');
        }
    }
    if (id === undefined) return;
    for (const change of remaining) {
        if (change.table === revert.table && change.localId === id) applyCaptured(state, id, change);
    }
}

// Writes one captured change back into the table, as the app wrote it: an
// update writes its columns where the row is, a delete removes the row, and
// an insert writes its whole row.
function applyCaptured(state: TableState, id: number, change: CapturedChange): void {
    if (change.kind === 'delete') {
        state.rows.delete(id);
        return;
    }
    let row = state.rows.get(id);
    if (change.kind === 'insert') {
        row = { values: {} };
        state.rows.set(id, row);
    }
    if (row === undefined) return;
    for (const [column, value] of Object.entries(change.values)) {
        if (column === state.idColumn) continue;
        if (column === state.authorColumn) {
            if (typeof value === 'number') row.author = value;
            else delete row.author;
            continue;
        }
        if (value === null) delete row.values[column];
        else row.values[column] = value;
    }
}

function deleteRow(store: Store, action: Extract<RowAction, { kind: 'delete-row' }>): TargetRowChange | undefined {
    const state = requireTable(store, action.table);
    // Remove only the app row; the sync entry is intentionally kept so a later
    // void-flip reinstatement reuses the same serial id. Status -> deleted (the
    // settle overrides to ingestion_failure for a reverted insert orphan).
    const id = state.sync.get(action.rowId);
    if (id !== undefined) state.rows.delete(id);
    if (state.sync.has(action.rowId)) state.syncStatus.set(action.rowId, 'deleted');
    return id === undefined ? undefined : { kind: 'delete', table: action.table, localId: id };
}
