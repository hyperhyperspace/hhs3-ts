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
//
// Values are stored as their logical json.Literal (no engine coercion), so the
// read accessors return exactly what was projected.

import type { json } from "@hyper-hyper-space/hhs3_json";
import type { B64Hash, KeyId } from "@hyper-hyper-space/hhs3_crypto";
import type { ColumnType } from "@hyper-hyper-space/hhs3_rdb";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";

import {
    CapturedBatch, CapturedChange, ChangeSignalListener, ChangeSignalSource,
    IngestSettle, KeyIndex, MaterializationTarget, MaterializedChangeSource, OpEvent, RowAction,
    RowIdentityIndex, SchemaAction, StoredOpEvent, SyncMapping, SyncStatus,
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
};

type Store = {
    tables: Map<string, TableState>;
    // Per-group materialized version (a shared target holds several groups of
    // one RDb). Table/sync names are globally unique, so only the checkpoint is
    // per-group.
    checkpoints: Map<B64Hash, Version>;
    // Shared keys side table: keyHash -> entry. nextKeyId allocates new ids.
    keysByHash: Map<string, KeyEntry>;
    keysById: Map<number, KeyEntry>;
    nextKeyId: number;
};

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
    };
}

function cloneStore(store: Store): Store {
    const tables = new Map<string, TableState>();
    for (const [name, state] of store.tables) tables.set(name, cloneTable(state));
    const checkpoints = new Map<B64Hash, Version>();
    for (const [groupId, v] of store.checkpoints) checkpoints.set(groupId, new Set(v));
    const keysByHash = new Map<string, KeyEntry>();
    const keysById = new Map<number, KeyEntry>();
    for (const [hash, entry] of store.keysByHash) {
        const clone: KeyEntry = { id: entry.id, keyHash: entry.keyHash };
        if (entry.publicKey !== undefined) clone.publicKey = entry.publicKey;
        keysByHash.set(hash, clone);
        keysById.set(clone.id, clone);
    }
    return { tables, checkpoints, keysByHash, keysById, nextKeyId: store.nextKeyId };
}

export class MemoryTarget implements MaterializationTarget, MaterializedChangeSource, ChangeSignalSource, RowIdentityIndex, KeyIndex {
    private store: Store = {
        tables: new Map(), checkpoints: new Map(),
        keysByHash: new Map(), keysById: new Map(), nextKeyId: 1,
    };
    private changeListeners = new Set<ChangeSignalListener>();

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

    constructor(opts: { captureChanges?: boolean } = {}) {
        this.captureProvisioned = opts.captureChanges === true;
        this.captureEnabled = this.captureProvisioned;
    }

    private logEvents(events: OpEvent[] | undefined): void {
        if (events === undefined) return;
        for (const event of events) {
            const key = event.opHash + '\u0000' + event.direction;
            if (this.opEventKeys.has(key)) continue;
            this.opEventKeys.add(key);
            this.opEvents.push({ id: this.opEventNextId++, event });
        }
    }

    async getCheckpoint(groupId: B64Hash): Promise<Version | undefined> {
        const v = this.store.checkpoints.get(groupId);
        return v === undefined ? undefined : new Set(v);
    }

    async apply(
        groupId: B64Hash, schemaActions: SchemaAction[], rowActions: RowAction[],
        checkpoint: Version, events?: OpEvent[],
    ): Promise<void> {
        // Atomicity: mutate a clone, swap in only on full success.
        const working = cloneStore(this.store);
        for (const action of schemaActions) applySchemaAction(working, action);
        for (const action of rowActions) applyRowAction(working, action);
        working.checkpoints.set(groupId, new Set(checkpoint));
        this.store = working;
        // Concurrency void/reinstate flips ride the same checkpoint advance.
        this.logEvents(events);
    }

    // -----------------------------------------------------------------------
    // Local mutation API (simulates the app writing directly to the projected
    // tables): mutates rows AND records to the outbox when capture is enabled.
    // -----------------------------------------------------------------------

    // Insert a fresh local row (no sync mapping yet - its rdb identity is minted
    // at ingestion). Returns the projection-local id. `author` here is the
    // numeric key id (into rdb_keys), matching the projected author_key_id column.
    localInsert(table: string, values: RowValues, author?: number): number {
        const state = requireTable(this.store, table);
        const id = state.nextId++;
        const row: MemoryRow = { values: { ...values } };
        if (state.authorColumn !== undefined && author !== undefined) row.author = author;
        state.rows.set(id, row);
        this.record({ kind: 'insert', table, localId: id, values: { ...values } });
        return id;
    }

    localUpdate(table: string, localId: number, values: RowValues): void {
        const state = requireTable(this.store, table);
        const row = state.rows.get(localId);
        if (row === undefined) throw new Error(`no local row ${table}#${localId}`);
        for (const [column, value] of Object.entries(values)) row.values[column] = value;
        this.record({ kind: 'update', table, localId, values: { ...values } });
    }

    localDelete(table: string, localId: number): void {
        const state = requireTable(this.store, table);
        state.rows.delete(localId);
        this.record({ kind: 'delete', table, localId });
    }

    // Runtime enable/disable (level 2). No-op unless provisioned.
    setCaptureEnabled(on: boolean): void {
        if (this.captureProvisioned) this.captureEnabled = on;
    }

    private record(change:
        | { kind: 'insert'; table: string; localId: number; values: RowValues }
        | { kind: 'update'; table: string; localId: number; values: RowValues }
        | { kind: 'delete'; table: string; localId: number }): void {
        if (!this.captureProvisioned || !this.captureEnabled) return;
        this.outbox.push({ id: this.outboxNextId++, ...change });
        for (const listener of [...this.changeListeners]) listener({});
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

    // -----------------------------------------------------------------------
    // MaterializedChangeSource (inbound). Meaningful only when provisioned.
    // -----------------------------------------------------------------------

    async drainChanges(): Promise<CapturedBatch> {
        this.requireCapture();
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

    async reserveMint(reservations: SyncMapping[]): Promise<void> {
        this.requireCapture();
        for (const m of reservations) {
            const state = this.store.tables.get(m.table);
            if (state === undefined) continue;
            state.sync.set(m.rowId, m.localId);
            const info: { rowId: string; uuid: string; author?: KeyId } = { rowId: m.rowId, uuid: m.uuid };
            if (m.author !== undefined) info.author = m.author;
            state.syncInfo.set(m.localId, info);
            if (!state.syncStatus.has(m.rowId)) state.syncStatus.set(m.rowId, m.status ?? 'active');
        }
    }

    async commitIngest(settle: IngestSettle): Promise<void> {
        this.requireCapture();
        // Persist any (redundant, idempotent) mappings.
        for (const m of settle.mappings ?? []) {
            const state = this.store.tables.get(m.table);
            if (state === undefined) continue;
            state.sync.set(m.rowId, m.localId);
            const info: { rowId: string; uuid: string; author?: KeyId } = { rowId: m.rowId, uuid: m.uuid };
            if (m.author !== undefined) info.author = m.author;
            state.syncInfo.set(m.localId, info);
        }
        // Reverts (echo-free: applyRowAction never records to the outbox).
        for (const action of settle.reverts ?? []) applyRowAction(this.store, action);
        // Sync status transitions (after reverts).
        for (const s of settle.statuses ?? []) {
            const state = this.store.tables.get(s.table);
            if (state !== undefined) state.syncStatus.set(s.rowId, s.status);
        }
        // Op-events (idempotent by (opHash, direction)).
        this.logEvents(settle.events);
        // Ack: drop consumed outbox rows.
        const consumedSet = new Set(settle.consumed);
        this.outbox = this.outbox.filter((c) => !consumedSet.has(c.id));
    }

    async drainOpEvents(sinceId?: number): Promise<StoredOpEvent[]> {
        // No requireCapture: concurrency void/reinstate events are logged by
        // apply() even on a read-only projection, so the log is always readable.
        const since = sinceId ?? 0;
        return this.opEvents.filter((e) => e.id > since).map((e) => ({ id: e.id, event: e.event }));
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
}

// ---------------------------------------------------------------------------
// Schema channel
// ---------------------------------------------------------------------------

function applySchemaAction(store: Store, action: SchemaAction): void {
    switch (action.kind) {
        case 'create-table': return createTable(store, action);
        case 'drop-table': return dropTable(store, action);
        case 'add-column': return addColumn(store, action);
        case 'drop-column': return dropColumn(store, action);
    }
}

function requireTable(store: Store, table: string): TableState {
    const state = store.tables.get(table);
    if (state === undefined) throw new Error(`no materialized metadata for table '${table}'`);
    return state;
}

function createTable(store: Store, action: Extract<SchemaAction, { kind: 'create-table' }>): void {
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
    });
}

function dropTable(store: Store, action: Extract<SchemaAction, { kind: 'drop-table' }>): void {
    store.tables.delete(action.table);
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
    delete state.columnTypes[action.column];
    delete state.fkColumns[action.column];
    state.keyRefColumns.delete(action.column);
    delete state.defaults[action.column];
    for (const row of state.rows.values()) delete row.values[action.column];
}

// ---------------------------------------------------------------------------
// Row channel
// ---------------------------------------------------------------------------

function applyRowAction(store: Store, action: RowAction): void {
    if (action.kind === 'upsert-row') return upsertRow(store, action);
    return deleteRow(store, action);
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

function upsertRow(store: Store, action: Extract<RowAction, { kind: 'upsert-row' }>): void {
    const state = requireTable(store, action.table);
    const id = allocateId(state, action.rowId);
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

    const existing = state.rows.get(id);
    if (existing !== undefined) {
        for (const [column, value] of Object.entries(action.values)) {
            existing.values[column] = valueForColumn(store, state, column, value, action.keyMaterial);
        }
        if (setAuthor) existing.author = authorId;
        return;
    }

    const values: RowValues = {};
    for (const [column, value] of Object.entries(action.values)) {
        values[column] = valueForColumn(store, state, column, value, action.keyMaterial);
    }
    // Fill schema defaults the insert omitted (as SQLite's create-table DEFAULT
    // does on INSERT). New-row only: an update never re-defaults. Defaults are
    // plain-column values, stored directly (no FK / key-ref translation).
    for (const [column, def] of Object.entries(state.defaults)) {
        if (values[column] === undefined) values[column] = def;
    }
    const row: MemoryRow = { values };
    if (setAuthor) row.author = authorId;
    state.rows.set(id, row);
}

function deleteRow(store: Store, action: Extract<RowAction, { kind: 'delete-row' }>): void {
    const state = requireTable(store, action.table);
    // Remove only the app row; the sync entry is intentionally kept so a later
    // void-flip reinstatement reuses the same serial id. Status -> deleted (the
    // settle overrides to ingestion_failure for a reverted insert orphan).
    const id = state.sync.get(action.rowId);
    if (id !== undefined) state.rows.delete(id);
    if (state.sync.has(action.rowId)) state.syncStatus.set(action.rowId, 'deleted');
}
