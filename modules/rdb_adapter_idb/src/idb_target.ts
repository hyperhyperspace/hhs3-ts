// IdbTarget: an executing MaterializationTarget for the rdb_adapter. A
// self-contained IndexedDB backend - no dag_idb, no SQL. Logical app tables
// live as documents in a fixed set of object stores (see idb_schema.ts).
//
// App reads/writes go through a duck-typed IDB facade (`database`). Those
// writes are the capture path (outbox + BroadcastChannel). apply() /
// commitIngest reverts talk to the physical stores directly and never write
// the outbox, so materialization cannot echo.
//
// Projection-local indexes (IndexTarget) are shadow entries in the fixed
// `index_entries` store, maintained in the same transaction as the rows they
// index (apply(), commitIngest reverts, and facade writes). A native
// createIndex on `rows` would need a versionchange, which cannot run inside
// apply(). Entries behave like a native IDBIndex over the table's documents
// (key path, sparse keys, native order) and are read through the facade's
// `index()`. The target takes no index options.

import type { json } from "@hyper-hyper-space/hhs3_json";
import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import type { ColumnType } from "@hyper-hyper-space/hhs3_rdb";
import {
    CapturedBatch, CapturedChange, CheckpointMovedError, DEFAULT_KEY_TABLE, PendingChangesError,
    IndexDecl, IndexSpec, IndexState, IndexTarget, IngestSettle,
    QueryableTarget, ReactiveChangeTarget, SingleTableQueryPlan, TargetCommitEvent, TargetCommitListener, TargetRowChange,
    KeyIndex, MaterializationTarget, MaterializedChangeSource, OpEvent, OpEventQuery, OpEventReason,
    pageOpEvents, ResolvedIndex, RowAction, RowIdentityIndex, SchemaAction, StoredOpEvent, SyncMapping,
    versionsEqual, WriterRecord, WriterRecordStore,
    CapturedChangeId, ChangeRecord, mergeChangeRecord, RowRevert, opEventRows, withLocalIds,
} from "@hyper-hyper-space/hhs3_rdb_adapter";
import { compareSortTuples, evalWhere, normalizeOrder, projectColumns, rowOnSide } from "@hyper-hyper-space/hhs3_rdb_adapter";

import { FacadeDatabase, type FacadeHost } from "./idb_facade.js";
import {
    deletePrefix, IdbEnv, indexGet, storeAdd, storeCount, storeDelete, storeGet, storeGetAll,
    storeGetAllPrefix, storePut,
} from "./idb_env.js";
import {
    deleteRange, indexKeyOf, syncEntryRequests, wholeIndexRange, wholeTableRange,
} from "./idb_index_keys.js";
import {
    CAPTURE_CONFIG, CAPTURE_KEY, CHECKPOINT, COUNTER_NEXT_KEY_ID, COUNTERS,
    INDEX_ENTRIES, INDEX_META, INDEX_SPEC, KEYS, OP_EVENTS, OUTBOX, ROWS, SYNC, TABLE_META,
    type CaptureConfigRecord, type CheckpointRecord, type CounterRecord, type IndexEntryRecord,
    type IndexSpecRecord, type KeyRecord, type OpEventRecord, type OutboxRecord, type RowRecord,
    type SyncRecord, type TableMetaRecord, type WriterRecordRecord, WRITER_RECORD_KEY, reqToPromise,
} from "./idb_schema.js";

export type IdbTargetOptions = {
    captureChanges?: boolean;
    indexedDB?: IDBFactory;
};

type ApplyCtx = {
    tx: IDBTransaction;
    meta: Map<string, TableMetaRecord>;
    // Per table, the materialized indexes (read from index_meta inside tx).
    indexes: Map<string, ResolvedIndex[]>;
    changes: TargetRowChange[];
    events: StoredOpEvent[];
};

function newCtx(tx: IDBTransaction): ApplyCtx {
    return { tx, meta: new Map(), indexes: new Map(), changes: [], events: [] };
}

const NO_INDEX_OPTIONS = 'the indexeddb target takes no index options';

function logicalRow(meta: TableMetaRecord, rec: RowRecord): import("@hyper-hyper-space/hhs3_rdb_adapter").ProjectedRow {
    const values: import("@hyper-hyper-space/hhs3_rdb_adapter").ProjectedRow = { id: rec.id };
    if (meta.idColumn !== 'id') values[meta.idColumn] = rec.id;
    for (const column of Object.keys(meta.columnTypes)) {
        const value = rec[column];
        values[column] = value === undefined ? null : value as json.Literal;
    }
    if (meta.authorColumn !== undefined) {
        const author = rec[meta.authorColumn];
        values[meta.authorColumn] = author === undefined ? null : author as json.Literal;
    }
    return values;
}

export class IdbTarget implements MaterializationTarget, MaterializedChangeSource,
    ReactiveChangeTarget, QueryableTarget, RowIdentityIndex, KeyIndex, IndexTarget, FacadeHost, WriterRecordStore {

    readonly name: string;
    readonly env: IdbEnv;
    private readonly captureRequested: boolean;
    private capture = false;
    private captureEnabled = false;
    private tableNameSet = new Set<string>();
    // table -> its indexes, sorted by name. Only feeds the facade's synchronous
    // `indexNames` / `index()`; reads and writes consult index_meta in-tx.
    private indexCache = new Map<string, ResolvedIndex[]>();
    private commitListeners = new Set<TargetCommitListener>();
    // Cache refreshes and deliveries run in order on one chain: a commit that
    // changes tables is delivered only once the caches have read them, so a
    // listener can open a new table through the facade at once, and no delivery
    // overtakes another (a query monitor keeps op events by a high-water mark).
    private chain: Promise<void> = Promise.resolve();
    private queued = 0;
    private emittedOutboxIds = new Set<number>();
    private observerChannel: BroadcastChannel | undefined;
    private readonly channelName: string;
    private closed = false;
    private _database: FacadeDatabase | undefined;

    private constructor(env: IdbEnv, name: string, opts: IdbTargetOptions) {
        this.env = env;
        this.name = name;
        this.captureRequested = opts.captureChanges === true;
        this.channelName = `hhs3-rdb-idb:${name}`;
    }

    get db(): IDBDatabase { return this.env.db; }

    appTables(): string[] { return [...this.tableNameSet]; }

    cachedIndexes(table: string): ResolvedIndex[] { return this.indexCache.get(table) ?? []; }

    cmp(a: unknown, b: unknown): number { return this.env.factory.cmp(a, b); }

    isCaptureOn(): boolean { return this.capture && this.captureEnabled; }

    onOutboxCommitted(records: OutboxRecord[] = []): void {
        const fresh = records.filter((row) => row.id !== undefined && !this.emittedOutboxIds.has(row.id));
        for (const row of fresh) if (row.id !== undefined) this.emittedOutboxIds.add(row.id);
        if (fresh.length === 0) return;
        this.deliver({ source: 'local', tables: [...new Set(fresh.map((row) => row.table))] }, true);
    }

    get database(): FacadeDatabase {
        this.ensureOpen();
        if (this._database === undefined) this._database = new FacadeDatabase(this);
        return this._database;
    }

    static async open(name: string, opts: IdbTargetOptions = {}): Promise<IdbTarget> {
        const factory = opts.indexedDB ?? globalThis.indexedDB;
        if (factory === undefined) {
            throw new Error('No IndexedDB factory available; pass opts.indexedDB in non-browser environments');
        }
        const env = await IdbEnv.open(name, factory);
        const target = new IdbTarget(env, name, opts);
        await target.init();
        return target;
    }

    private async init(): Promise<void> {
        await this.refreshCaches();

        const cfg = await this.env.withRead(CAPTURE_CONFIG, (tx) =>
            storeGet<CaptureConfigRecord>(tx, CAPTURE_CONFIG, CAPTURE_KEY));
        const provisioned = cfg !== undefined;
        this.capture = this.captureRequested || provisioned;
        this.captureEnabled = cfg?.enabled ?? this.capture;
        if (this.capture && !provisioned) {
            await this.env.withReadWrite(async (tx) => {
                await storePut(tx, CAPTURE_CONFIG, { key: CAPTURE_KEY, enabled: true } satisfies CaptureConfigRecord);
            });
            this.captureEnabled = true;
        }
    }

    close(): void {
        if (this.closed) return;
        this.closed = true;
        if (this.observerChannel !== undefined) {
            try { this.observerChannel.close(); } catch { /* ignore */ }
            this.observerChannel = undefined;
        }
        this.env.close();
    }

    private ensureOpen(): void {
        if (this.closed) throw new Error('IdbTarget is closed');
    }

    // -----------------------------------------------------------------------
    // MaterializationTarget
    // -----------------------------------------------------------------------

    async getCheckpoint(groupId: B64Hash): Promise<Version | undefined> {
        this.ensureOpen();
        const rec = await this.env.withRead(CHECKPOINT, (tx) =>
            storeGet<CheckpointRecord>(tx, CHECKPOINT, groupId));
        if (rec?.version === undefined) return undefined;
        return new Set(rec.version);
    }

    async apply(
        groupId: B64Hash, schemaActions: SchemaAction[], rowActions: RowAction[],
        checkpoint: Version, events?: OpEvent[], expectFrom?: Version | null,
        expectIndexSpec?: string | null,
    ): Promise<void> {
        this.ensureOpen();
        let ctxChanges: TargetRowChange[] = [];
        let ctxEvents: StoredOpEvent[] = [];
        await this.env.withReadWrite(async (tx) => {
            const ctx = newCtx(tx);
            // Compare-and-set guard (concurrent projectors): read the stored
            // checkpoint inside this same readwrite transaction and reject if it is
            // not what the delta was computed against. Throwing aborts the tx, so
            // nothing is applied.
            const prior = await storeGet<CheckpointRecord>(tx, CHECKPOINT, groupId);
            if (expectFrom !== undefined) {
                const rec = prior;
                const stored = rec?.version === undefined ? undefined : new Set(rec.version);
                if (!versionsEqual(stored, expectFrom ?? undefined)) throw new CheckpointMovedError(groupId);
            }
            if (expectIndexSpec !== undefined) {
                const rec = await storeGet<IndexSpecRecord>(tx, INDEX_SPEC, 1);
                if ((rec?.fingerprint ?? null) !== expectIndexSpec) throw new CheckpointMovedError(groupId);
            }
            // No projection over a pending change (HIP-29). The readwrite
            // transaction covers the outbox, so no write lands in between.
            if (this.capture && await storeCount(tx, OUTBOX) > 0) throw new PendingChangesError(groupId);
            for (const action of schemaActions) await this.applySchemaAction(ctx, action, groupId);
            for (const action of rowActions) await this.applyRowAction(ctx, action);
            // The end's observes beside the checkpoint stay: only a settle clears them.
            await storePut(tx, CHECKPOINT, {
                ...(prior?.endObserves !== undefined ? { endObserves: prior.endObserves } : {}),
                groupId, version: [...checkpoint],
            } satisfies CheckpointRecord);
            if (events !== undefined) {
                for (const e of events) {
                    const stored = await this.logOpEvent(tx, e);
                    if (stored !== undefined) ctx.events.push(stored);
                }
            }
            ctxChanges = ctx.changes;
            ctxEvents = ctx.events;
        });
        const schemaTables = [...new Set(schemaActions.map((action) => action.kind === 'ensure-index' ? action.index.table : action.table))];
        const commit: TargetCommitEvent = { source: 'apply', groupId, schemaTables, changes: ctxChanges, events: ctxEvents };
        const announce = ctxChanges.length > 0 || ctxEvents.length > 0 || schemaTables.length > 0;
        if (schemaActions.length === 0) {
            if (announce) this.deliver(commit, true);
            return;
        }
        // The facade's tables and indexes come from the caches: they are read
        // again before the commit is delivered, and before apply() returns.
        await this.enqueue(async () => {
            try {
                await this.refreshCaches();
            } finally {
                if (announce) this.deliverNow(commit, true);
            }
        });
    }

    // -----------------------------------------------------------------------
    // Schema channel
    // -----------------------------------------------------------------------

    private async applySchemaAction(ctx: ApplyCtx, action: SchemaAction, groupId?: B64Hash): Promise<void> {
        switch (action.kind) {
            case 'create-table': return this.createTable(ctx, action, groupId);
            case 'drop-table': return this.dropTable(ctx, action);
            case 'add-column': return this.addColumn(ctx, action);
            case 'drop-column': return this.dropColumn(ctx, action);
            case 'ensure-index': return this.ensureIndex(ctx, action.index);
            case 'drop-index': return this.dropIndex(ctx, action.table, action.name);
        }
    }

    private async createTable(ctx: ApplyCtx, action: Extract<SchemaAction, { kind: 'create-table' }>, groupId?: B64Hash): Promise<void> {
        const existing = await storeGet<TableMetaRecord>(ctx.tx, TABLE_META, action.table);
        if (existing !== undefined) {
            ctx.meta.set(action.table, existing);
            return;
        }
        const columnTypes: { [column: string]: ColumnType } = {};
        const fkColumns: { [column: string]: { targetTable: string } } = {};
        const keyRefColumns: string[] = [];
        const defaults: { [column: string]: json.Literal } = {};
        for (const c of action.columns) {
            columnTypes[c.name] = c.def.type;
            if (c.fk !== undefined) fkColumns[c.name] = { targetTable: c.fk.targetTable };
            if (c.keyRef === true) keyRefColumns.push(c.name);
            if (c.def.default !== undefined) defaults[c.name] = c.def.default;
        }
        const meta: TableMetaRecord = {
            table: action.table,
            idColumn: action.primaryKey,
            authorColumn: action.authorColumn,
            syncTable: action.syncTable,
            columnTypes,
            fkColumns,
            keyRefColumns,
            defaults,
            nextId: 1,
            ...(groupId !== undefined ? { groupId } : {}),
            schemaName: action.schemaName ?? action.table,
        };
        await storePut(ctx.tx, TABLE_META, meta);
        ctx.meta.set(action.table, meta);
    }

    private async dropTable(ctx: ApplyCtx, action: Extract<SchemaAction, { kind: 'drop-table' }>): Promise<void> {
        await storeDelete(ctx.tx, TABLE_META, action.table);
        ctx.meta.delete(action.table);
        await deletePrefix(ctx.tx, ROWS, [action.table], (r: RowRecord) => [r.table, r.id]);
        await deletePrefix(ctx.tx, SYNC, [action.table], (r: SyncRecord) => [r.table, r.rowHash]);
        // The planner drops the table's indexes first; this only guarantees a
        // table drop never leaves entries behind.
        await deleteRange(ctx.tx, INDEX_ENTRIES, wholeTableRange(action.table));
        await deleteRange(ctx.tx, INDEX_META, wholeTableRange(action.table));
        ctx.indexes.delete(action.table);
    }

    private async addColumn(ctx: ApplyCtx, action: Extract<SchemaAction, { kind: 'add-column' }>): Promise<void> {
        const meta = await this.loadMeta(ctx, action.table);
        meta.columnTypes[action.column] = action.def.type;
        if (action.fk !== undefined) meta.fkColumns[action.column] = { targetTable: action.fk.targetTable };
        if (action.keyRef === true) meta.keyRefColumns.push(action.column);
        if (action.def.default !== undefined) {
            meta.defaults[action.column] = action.def.default;
            const rows = await storeGetAllPrefix<RowRecord>(ctx.tx, ROWS, [action.table]);
            for (const row of rows) {
                if (row[action.column] === undefined) {
                    row[action.column] = action.def.default;
                    await storePut(ctx.tx, ROWS, row);
                }
            }
        }
        await storePut(ctx.tx, TABLE_META, meta);
    }

    private async dropColumn(ctx: ApplyCtx, action: Extract<SchemaAction, { kind: 'drop-column' }>): Promise<void> {
        const meta = await this.loadMeta(ctx, action.table);
        // As SQLite: an indexed column cannot be dropped (the planner drops the
        // index first).
        for (const index of await this.tableIndexes(ctx, action.table)) {
            if (index.columns.some((c) => c.target === action.column)) {
                throw new Error(`cannot drop '${action.table}.${action.column}': index '${index.name}' uses it`);
            }
        }
        delete meta.columnTypes[action.column];
        delete meta.fkColumns[action.column];
        meta.keyRefColumns = meta.keyRefColumns.filter((c) => c !== action.column);
        delete meta.defaults[action.column];
        const rows = await storeGetAllPrefix<RowRecord>(ctx.tx, ROWS, [action.table]);
        for (const row of rows) {
            if (action.column in row) {
                delete row[action.column];
                await storePut(ctx.tx, ROWS, row);
            }
        }
        await storePut(ctx.tx, TABLE_META, meta);
    }

    // -----------------------------------------------------------------------
    // Index channel (ensure-index / drop-index, entry maintenance)
    // -----------------------------------------------------------------------

    private async tableIndexes(ctx: ApplyCtx, table: string): Promise<ResolvedIndex[]> {
        const cached = ctx.indexes.get(table);
        if (cached !== undefined) return cached;
        const list = await storeGetAll<ResolvedIndex>(ctx.tx, INDEX_META, wholeTableRange(table));
        ctx.indexes.set(table, list);
        return list;
    }

    // (Re)build an index from the table's current documents. Idempotent: any
    // existing entries of the same (table, name) are replaced.
    private async ensureIndex(ctx: ApplyCtx, index: ResolvedIndex): Promise<void> {
        const meta = await this.loadMeta(ctx, index.table);
        for (const c of index.columns) {
            if (meta.columnTypes[c.target] === undefined && c.target !== meta.authorColumn) {
                throw new Error(`cannot index '${index.table}.${c.target}': no such column`);
            }
        }
        const others = (await this.tableIndexes(ctx, index.table)).filter((i) => i.name !== index.name);
        await deleteRange(ctx.tx, INDEX_ENTRIES, wholeIndexRange(index.table, index.name));
        const rows = await storeGetAllPrefix<RowRecord>(ctx.tx, ROWS, [index.table]);
        const entries = ctx.tx.objectStore(INDEX_ENTRIES);
        const puts: Promise<unknown>[] = [];
        for (const row of rows) {
            const key = indexKeyOf(row, index);
            if (key === undefined) continue;
            const rec: IndexEntryRecord = { table: index.table, name: index.name, key, id: row.id };
            puts.push(reqToPromise(entries.put(rec)));
        }
        await Promise.all(puts);
        await storePut(ctx.tx, INDEX_META, index);
        ctx.indexes.set(index.table, [...others, index]);
    }

    private async dropIndex(ctx: ApplyCtx, table: string, name: string): Promise<void> {
        const remaining = (await this.tableIndexes(ctx, table)).filter((i) => i.name !== name);
        await deleteRange(ctx.tx, INDEX_ENTRIES, wholeIndexRange(table, name));
        await storeDelete(ctx.tx, INDEX_META, [table, name]);
        ctx.indexes.set(table, remaining);
    }

    private async syncIndexEntries(
        ctx: ApplyCtx, table: string, id: number, oldRow: RowRecord | undefined, newRow: RowRecord | undefined,
    ): Promise<void> {
        const indexes = await this.tableIndexes(ctx, table);
        if (indexes.length === 0) return;
        const reqs = syncEntryRequests(ctx.tx.objectStore(INDEX_ENTRIES), indexes, id, oldRow, newRow);
        await Promise.all(reqs.map((r) => reqToPromise(r)));
    }

    // Read the table names and indexes the facade answers from synchronously, in
    // one transaction, so the two agree. index_meta is keyed [table, name], so
    // each table's list comes out sorted.
    private async refreshCaches(): Promise<void> {
        const { metas, indexes } = await this.env.withRead([TABLE_META, INDEX_META], async (tx) => ({
            metas: await storeGetAll<TableMetaRecord>(tx, TABLE_META),
            indexes: await storeGetAll<ResolvedIndex>(tx, INDEX_META),
        }));
        this.tableNameSet = new Set(metas.map((m) => m.table));
        const byTable = new Map<string, ResolvedIndex[]>();
        for (const m of indexes) byTable.set(m.table, [...(byTable.get(m.table) ?? []), m]);
        this.indexCache = byTable;
    }

    // Run a step after every refresh and delivery queued before it.
    private enqueue(step: () => Promise<void> | void): Promise<void> {
        this.queued++;
        const run = this.chain.then(async () => {
            try {
                await step();
            } finally {
                this.queued--;
            }
        });
        this.chain = run.catch(() => undefined);
        return run;
    }

    private async loadMeta(ctx: ApplyCtx, table: string): Promise<TableMetaRecord> {
        const cached = ctx.meta.get(table);
        if (cached !== undefined) return cached;
        const rec = await storeGet<TableMetaRecord>(ctx.tx, TABLE_META, table);
        if (rec === undefined) throw new Error(`no materialized metadata for table '${table}'`);
        ctx.meta.set(table, rec);
        return rec;
    }

    // -----------------------------------------------------------------------
    // Row channel
    // -----------------------------------------------------------------------

    private async applyRowAction(ctx: ApplyCtx, action: RowAction): Promise<void> {
        if (action.kind === 'upsert-row') return this.upsertRow(ctx, action);
        return this.deleteRow(ctx, action);
    }

    private async allocateId(ctx: ApplyCtx, table: string, rowId: string): Promise<number> {
        const existing = await storeGet<SyncRecord>(ctx.tx, SYNC, [table, rowId]);
        if (existing !== undefined) return existing.id;
        const meta = await this.loadMeta(ctx, table);
        const id = meta.nextId++;
        await storePut(ctx.tx, TABLE_META, meta);
        await storePut(ctx.tx, SYNC, {
            table, rowHash: rowId, id, uuid: '', status: 'active',
        } satisfies SyncRecord);
        return id;
    }

    private async internKey(tx: IDBTransaction, keyHash: string, publicKey?: string): Promise<number> {
        const existing = await indexGet<KeyRecord>(tx, KEYS, 'by_hash', keyHash);
        if (existing !== undefined) {
            if (publicKey !== undefined && existing.publicKey === undefined) {
                existing.publicKey = publicKey;
                await storePut(tx, KEYS, existing);
            }
            return existing.id;
        }
        const counter = await storeGet<CounterRecord>(tx, COUNTERS, COUNTER_NEXT_KEY_ID);
        const id = counter?.value ?? 1;
        await storePut(tx, COUNTERS, { key: COUNTER_NEXT_KEY_ID, value: id + 1 } satisfies CounterRecord);
        const rec: KeyRecord = { id, keyHash };
        if (publicKey !== undefined) rec.publicKey = publicKey;
        await storePut(tx, KEYS, rec);
        return id;
    }

    private async valueForColumn(
        ctx: ApplyCtx, meta: TableMetaRecord, column: string, value: json.Literal,
        keyMaterial?: { [keyHash: string]: string },
    ): Promise<json.Literal> {
        const fk = meta.fkColumns[column];
        if (fk !== undefined) {
            return this.allocateId(ctx, fk.targetTable, value as string);
        }
        if (meta.keyRefColumns.includes(column)) {
            const keyHash = value as string;
            return this.internKey(ctx.tx, keyHash, keyMaterial?.[keyHash]);
        }
        return value;
    }

    private async upsertRow(ctx: ApplyCtx, action: Extract<RowAction, { kind: 'upsert-row' }>): Promise<void> {
        const meta = await this.loadMeta(ctx, action.table);
        const id = await this.allocateId(ctx, action.table, action.rowId);
        const sync = await storeGet<SyncRecord>(ctx.tx, SYNC, [action.table, action.rowId]);
        if (sync !== undefined) {
            sync.status = 'active';
            await storePut(ctx.tx, SYNC, sync);
        }

        if (action.keyMaterial !== undefined) {
            for (const [hash, pk] of Object.entries(action.keyMaterial)) {
                await this.internKey(ctx.tx, hash, pk);
            }
        }

        const existing = await storeGet<RowRecord>(ctx.tx, ROWS, [action.table, id]);
        const setAuthor = meta.authorColumn !== undefined && action.author !== undefined;
        const authorId = setAuthor ? await this.internKey(ctx.tx, action.author!) : undefined;

        // A null in the action means NULL, which this target keeps as a missing
        // key: no id is allocated and no key interned for it.
        if (existing !== undefined) {
            const before: RowRecord = { ...existing };
            for (const [column, value] of Object.entries(action.values)) {
                if (value === null) delete existing[column];
                else existing[column] = await this.valueForColumn(ctx, meta, column, value, action.keyMaterial);
            }
            // An action without an author is for a row rdb has none for: the
            // author column is cleared (its index entries with it), whatever a
            // local write left in it.
            if (meta.authorColumn !== undefined) {
                if (setAuthor) existing[meta.authorColumn] = authorId;
                else delete existing[meta.authorColumn];
            }
            await storePut(ctx.tx, ROWS, existing);
            await this.syncIndexEntries(ctx, action.table, id, before, existing);
            ctx.changes.push({ kind: 'update', table: action.table, localId: id, values: logicalRow(meta, existing) });
            return;
        }

        const rec: RowRecord = { table: action.table, id };
        rec[meta.idColumn] = id;
        for (const [column, value] of Object.entries(action.values)) {
            if (value !== null) rec[column] = await this.valueForColumn(ctx, meta, column, value, action.keyMaterial);
        }
        // A default goes to a column the action leaves out. The test is whether
        // the action names the column, not whether the record has the key: a
        // column the action sets to NULL has no key and takes no default.
        for (const [column, def] of Object.entries(meta.defaults)) {
            if (!(column in action.values)) rec[column] = def;
        }
        if (setAuthor && meta.authorColumn !== undefined) rec[meta.authorColumn] = authorId;
        await storePut(ctx.tx, ROWS, rec);
        await this.syncIndexEntries(ctx, action.table, id, undefined, rec);
        ctx.changes.push({ kind: 'insert', table: action.table, localId: id, values: logicalRow(meta, rec) });
    }

    private async deleteRow(ctx: ApplyCtx, action: Extract<RowAction, { kind: 'delete-row' }>): Promise<void> {
        const sync = await storeGet<SyncRecord>(ctx.tx, SYNC, [action.table, action.rowId]);
        if (sync !== undefined) {
            const old = await storeGet<RowRecord>(ctx.tx, ROWS, [action.table, sync.id]);
            await storeDelete(ctx.tx, ROWS, [action.table, sync.id]);
            await this.syncIndexEntries(ctx, action.table, sync.id, old, undefined);
            sync.status = 'deleted';
            await storePut(ctx.tx, SYNC, sync);
            ctx.changes.push({ kind: 'delete', table: action.table, localId: sync.id });
        }
    }

    // Append one op-event (idempotent by (opHash, direction)), with the local
    // id of each row it names by hash. Called inside the transaction, after
    // its row writes.
    private async logOpEvent(tx: IDBTransaction, logged: OpEvent): Promise<StoredOpEvent | undefined> {
        const existing = await indexGet<OpEventRecord>(tx, OP_EVENTS, 'by_op', [logged.opHash, logged.direction]);
        if (existing !== undefined) return undefined;
        // The sync records are read first: the shared fill-in looks them up
        // without awaiting.
        const ids = new Map<string, number>();
        for (const row of opEventRows(logged)) {
            if (row.localId !== undefined || row.rowId === undefined) continue;
            const sync = await storeGet<SyncRecord>(tx, SYNC, [row.table, row.rowId]);
            if (sync !== undefined) ids.set(row.table + '\u0000' + row.rowId, sync.id);
        }
        const event = withLocalIds(logged, (table, rowId) => ids.get(table + '\u0000' + rowId));
        const rec: OpEventRecord = { ...event };
        const id = Number(await storeAdd(tx, OP_EVENTS, rec));
        return { id, event };
    }

    // -----------------------------------------------------------------------
    // MaterializedChangeSource
    // -----------------------------------------------------------------------

    async executeQuery(plan: SingleTableQueryPlan): Promise<import("@hyper-hyper-space/hhs3_rdb_adapter").ProjectedRow[]> {
        this.ensureOpen();
        const meta = await this.env.withRead(TABLE_META, (tx) => storeGet<TableMetaRecord>(tx, TABLE_META, plan.table));
        if (meta === undefined) return [];
        const records = await this.env.withRead(ROWS, (tx) => storeGetAllPrefix<RowRecord>(tx, ROWS, [plan.table]));
        const needsOrder = plan.limit !== undefined || plan.cursor !== undefined;
        const order = normalizeOrder(plan.orderBy, meta.idColumn, needsOrder);
        let rows = records.map((rec) => logicalRow(meta, rec));
        rows = rows.filter((row) => evalWhere(plan.where, row, Number(row.id)));
        if (plan.cursor !== undefined && order.length > 0) {
            const anchor = rows.find((row) => row.id === plan.cursor!.id) ?? plan.cursor.cachedValues;
            if (anchor === undefined) return [];
            rows = rows.filter((row) => rowOnSide(row, anchor, order, plan.cursor!.side));
        }
        if (order.length > 0) rows.sort((a, b) => compareSortTuples(a, b, order));
        if (plan.limit !== undefined) rows = rows.slice(0, plan.limit);
        return rows.map((row) => projectColumns(row, plan.select, meta.idColumn));
    }

    async hasPendingChanges(): Promise<boolean> {
        this.ensureOpen();
        if (!this.capture) return false;
        return await this.env.withRead(OUTBOX, (tx) => storeCount(tx, OUTBOX)) > 0;
    }

    async recordWriter(record: WriterRecord): Promise<void> {
        this.ensureOpen();
        await this.env.withReadWrite(async (tx) => {
            await storePut(tx, CAPTURE_CONFIG, { key: WRITER_RECORD_KEY, ...record } satisfies WriterRecordRecord);
        });
    }

    async readWriterRecord(): Promise<WriterRecord | undefined> {
        this.ensureOpen();
        const rec = await this.env.withRead(CAPTURE_CONFIG, (tx) => storeGet<WriterRecordRecord>(tx, CAPTURE_CONFIG, WRITER_RECORD_KEY));
        if (rec === undefined) return undefined;
        const { key: _key, ...record } = rec;
        return record;
    }

    // The table's group and its name in the schema, from the table's meta.
    async tableGroup(table: string): Promise<{ groupId?: B64Hash; schemaName?: string } | undefined> {
        this.ensureOpen();
        const meta = await this.env.withRead(TABLE_META, (tx) => storeGet<TableMetaRecord>(tx, TABLE_META, table));
        if (meta === undefined) return undefined;
        return { ...(meta.groupId !== undefined ? { groupId: meta.groupId } : {}),
            ...(meta.schemaName !== undefined ? { schemaName: meta.schemaName } : {}) };
    }

    async drainChanges(): Promise<CapturedBatch> {
        this.requireCapture();
        const rows = await this.env.withRead(OUTBOX, (tx) => storeGetAll<OutboxRecord>(tx, OUTBOX));
        rows.sort((a, b) => (a.id ?? 0) - (b.id ?? 0));
        const fresh = rows.filter((row) => row.id !== undefined && !this.emittedOutboxIds.has(row.id));
        this.emittedOutboxIds = new Set(rows.map((row) => row.id).filter((id): id is number => id !== undefined));
        if (fresh.length > 0) this.deliver({ source: 'local', tables: [...new Set(fresh.map((row) => row.table))] }, true);
        const changes: CapturedChange[] = [];
        for (const row of rows) {
            const record = row.record === undefined ? {} : { record: row.record };
            if (row.op === 'delete') {
                changes.push({ id: row.id!, kind: 'delete', table: row.table, localId: row.localId, ...record });
                continue;
            }
            changes.push({
                id: row.id!, kind: row.op, table: row.table, localId: row.localId,
                values: row.changed ?? {}, ...record,
            });
        }
        return { changes };
    }

    async resolveRow(table: string, localId: number): Promise<SyncMapping | undefined> {
        this.requireCapture();
        const rec = await this.env.withRead(SYNC, (tx) =>
            indexGet<SyncRecord>(tx, SYNC, 'by_local_id', [table, localId]));
        if (rec === undefined) return undefined;
        return {
            table, localId, rowId: rec.rowHash, uuid: rec.uuid ?? '',
            status: rec.status,
        };
    }

    async writeRecord(id: CapturedChangeId, record: ChangeRecord): Promise<void> {
        this.requireCapture();
        await this.env.withReadWrite(async (tx) => {
            const row = await storeGet<OutboxRecord>(tx, OUTBOX, id);
            if (row === undefined) return;
            await storePut(tx, OUTBOX, { ...row, record: mergeChangeRecord(row.record, record) } satisfies OutboxRecord);
        });
    }

    async recordEndObserve(groupId: B64Hash, hash: B64Hash): Promise<void> {
        this.requireCapture();
        await this.env.withReadWrite(async (tx) => {
            const rec = await storeGet<CheckpointRecord>(tx, CHECKPOINT, groupId);
            await storePut(tx, CHECKPOINT, { ...(rec ?? { groupId }), endObserves: [...(rec?.endObserves ?? []), hash] } satisfies CheckpointRecord);
        });
    }

    async getEndObserves(groupId: B64Hash): Promise<B64Hash[]> {
        this.ensureOpen();
        const rec = await this.env.withRead(CHECKPOINT, (tx) => storeGet<CheckpointRecord>(tx, CHECKPOINT, groupId));
        return [...(rec?.endObserves ?? [])];
    }

    // The settle, in HIP-29's order (see IngestSettle), in one readwrite
    // transaction: a step that throws aborts every one.
    async commitIngest(settle: IngestSettle): Promise<void> {
        this.requireCapture();
        let logged: StoredOpEvent[] = [];
        const written = new Set<string>();
        await this.env.withReadWrite(async (tx) => {
            const ctx = newCtx(tx);
            // 1. Each group's checkpoint is still the one the pass found.
            for (const { groupId, expected } of settle.checkpoints ?? []) {
                const rec = await storeGet<CheckpointRecord>(tx, CHECKPOINT, groupId);
                const stored = rec?.version === undefined ? undefined : new Set(rec.version);
                if (!versionsEqual(stored, expected)) throw new CheckpointMovedError(groupId);
            }
            // 2. The handled rows' failure events are read, and the rows leave
            // the outbox; the rest is read, by table and local id.
            const failures: OpEvent[] = [];
            for (const id of settle.consumed) {
                const row = await storeGet<OutboxRecord>(tx, OUTBOX, id);
                if (row?.record?.failure !== undefined) failures.push(row.record.failure);
            }
            for (const id of settle.consumed) await storeDelete(tx, OUTBOX, id);
            const pending = new Map<string, OutboxRecord[]>();
            const remaining = await storeGetAll<OutboxRecord>(tx, OUTBOX);
            remaining.sort((a, b) => (a.id ?? 0) - (b.id ?? 0));
            for (const row of remaining) {
                const key = row.table + '\u0000' + row.localId;
                pending.set(key, [...(pending.get(key) ?? []), row]);
            }
            // 3. The handled inserts' mappings, then the accepted deletes.
            for (const m of settle.inserts ?? []) {
                await storePut(tx, SYNC, {
                    table: m.table, rowHash: m.rowId, id: m.localId, uuid: m.uuid, status: m.status ?? 'active',
                } satisfies SyncRecord);
                const meta = await this.loadMeta(ctx, m.table);
                if (m.localId >= meta.nextId) {
                    meta.nextId = m.localId + 1;
                    await storePut(tx, TABLE_META, meta);
                }
            }
            for (const d of settle.deletes ?? []) {
                const rec = await storeGet<SyncRecord>(tx, SYNC, [d.table, d.rowId]);
                if (rec !== undefined) await storePut(tx, SYNC, { ...rec, status: 'deleted' } satisfies SyncRecord);
            }
            // 4. The rows of refused inserts that had no identity, and 5. the
            // reverts, each with its changes still in the outbox on top.
            for (const o of settle.orphans ?? []) {
                await this.revertRow(ctx, { table: o.table, localId: o.localId }, pending, false);
                written.add(o.table);
            }
            for (const r of settle.reverts ?? []) {
                await this.revertRow(ctx, r, pending, true);
                written.add(r.table);
            }
            // 6. IndexedDB keeps json values, not text: nothing to rewrite.
            // 7. The handled rows' failure events, then the settle's own.
            for (const e of [...failures, ...(settle.events ?? [])]) {
                const stored = await this.logOpEvent(tx, e);
                if (stored !== undefined) logged.push(stored);
            }
            // 8. The new checkpoints, and every group's end observes cleared.
            for (const { groupId, next } of settle.checkpoints ?? []) {
                const rec = await storeGet<CheckpointRecord>(tx, CHECKPOINT, groupId);
                await storePut(tx, CHECKPOINT, { ...(rec ?? { groupId }), version: [...next] } satisfies CheckpointRecord);
            }
            for (const rec of await storeGetAll<CheckpointRecord>(tx, CHECKPOINT)) {
                if (rec.endObserves === undefined) continue;
                const { endObserves: _cleared, ...kept } = rec;
                await storePut(tx, CHECKPOINT, kept satisfies CheckpointRecord);
            }
            logged = logged.concat(ctx.events);
        });
        for (const id of settle.consumed) if (typeof id === 'number') this.emittedOutboxIds.delete(id);
        if (written.size > 0 || logged.length > 0) {
            const revertedTables = [...new Set([
                ...written,
                ...logged.flatMap((stored) => opEventRows(stored.event).map((row) => row.table)),
            ])];
            this.deliver({ source: 'settle', revertedTables, events: logged }, true);
        }
    }

    // Sets a row to rdb's row, or to none, then puts its changes still in the
    // outbox back on top, as capture recorded them (HIP-29's Reverts), with
    // their index entries. A row named only by its row hash is found through
    // the sync store, after the settle wrote the inserts' mappings.
    // `markDeleted`: a row removed this way has its active mapping marked deleted.
    private async revertRow(
        ctx: ApplyCtx, revert: Pick<RowRevert, 'table' | 'localId' | 'row'> & { rowId?: B64Hash },
        pending: Map<string, OutboxRecord[]>, markDeleted: boolean,
    ): Promise<void> {
        const { table } = revert;
        let id = revert.localId;
        if (id === undefined && revert.rowId !== undefined) id = (await storeGet<SyncRecord>(ctx.tx, SYNC, [table, revert.rowId]))?.id;
        if (revert.row !== undefined) {
            await this.upsertRow(ctx, revert.row);
            id = (await storeGet<SyncRecord>(ctx.tx, SYNC, [table, revert.row.rowId]))?.id;
        } else if (id !== undefined) {
            const old = await storeGet<RowRecord>(ctx.tx, ROWS, [table, id]);
            if (old !== undefined) {
                await storeDelete(ctx.tx, ROWS, [table, id]);
                await this.syncIndexEntries(ctx, table, id, old, undefined);
            }
            if (markDeleted && revert.rowId !== undefined) {
                const sync = await storeGet<SyncRecord>(ctx.tx, SYNC, [table, revert.rowId]);
                if (sync?.status === 'active') await storePut(ctx.tx, SYNC, { ...sync, status: 'deleted' } satisfies SyncRecord);
            }
        }
        if (id === undefined) return;
        for (const change of pending.get(table + '\u0000' + id) ?? []) await this.applyCaptured(ctx, table, id, change);
    }

    // Writes one outbox change back into its table, as capture recorded it: an
    // update writes its fields where the record is, a delete removes the
    // record, and an insert writes its whole record under its local id.
    private async applyCaptured(ctx: ApplyCtx, table: string, id: number, change: OutboxRecord): Promise<void> {
        const meta = await this.loadMeta(ctx, table);
        const old = await storeGet<RowRecord>(ctx.tx, ROWS, [table, id]);
        if (change.op === 'delete') {
            if (old === undefined) return;
            await storeDelete(ctx.tx, ROWS, [table, id]);
            await this.syncIndexEntries(ctx, table, id, old, undefined);
            return;
        }
        let rec: RowRecord;
        if (change.op === 'insert') {
            rec = { table, id };
            rec[meta.idColumn] = id;
        } else {
            if (old === undefined) return;
            rec = { ...old };
        }
        for (const [column, value] of Object.entries(change.changed ?? {})) {
            if (column === meta.idColumn || column === 'table' || column === 'id') continue;
            if (value === null) delete rec[column];
            else rec[column] = value;
        }
        await storePut(ctx.tx, ROWS, rec);
        await this.syncIndexEntries(ctx, table, id, old, rec);
    }

    async drainOpEvents(opts?: OpEventQuery | number): Promise<StoredOpEvent[]> {
        this.ensureOpen();
        const rows = await this.env.withRead(OP_EVENTS, (tx) => storeGetAll<OpEventRecord>(tx, OP_EVENTS));
        const stored: StoredOpEvent[] = rows.map((r) => {
            const { id, ...rest } = r;
            const event: OpEvent = {
                origin: rest.origin, direction: rest.direction, groupId: rest.groupId,
                opHash: rest.opHash, kind: rest.kind,
            };
            if (rest.table !== undefined) event.table = rest.table;
            if (rest.rowId !== undefined) event.rowId = rest.rowId;
            if (rest.localId !== undefined) event.localId = rest.localId;
            if (rest.writes !== undefined) event.writes = rest.writes;
            if (rest.author !== undefined) event.author = rest.author;
            if (rest.op !== undefined) event.op = rest.op;
            if (rest.reason !== undefined) event.reason = rest.reason as OpEventReason;
            return { id: id!, event };
        });
        return pageOpEvents(stored, opts);
    }

    async setCaptureEnabled(on: boolean): Promise<void> {
        this.ensureOpen();
        if (!this.capture) return;
        this.captureEnabled = on;
        await this.env.withReadWrite(async (tx) => {
            await storePut(tx, CAPTURE_CONFIG, { key: CAPTURE_KEY, enabled: on } satisfies CaptureConfigRecord);
        });
    }

    private requireCapture(): void {
        this.ensureOpen();
        if (!this.capture) {
            throw new Error("this IdbTarget was not provisioned for change capture (IdbTarget.open(name, { captureChanges: true }))");
        }
    }

    // -----------------------------------------------------------------------
    // ChangeSignalSource (BroadcastChannel)
    // -----------------------------------------------------------------------

    addCommitListener(listener: TargetCommitListener): void {
        this.ensureOpen();
        this.commitListeners.add(listener);
        if (this.commitListeners.size === 1) this.armChannel();
    }

    removeCommitListener(listener: TargetCommitListener): void {
        this.commitListeners.delete(listener);
        if (this.commitListeners.size === 0) this.disarmChannel();
    }

    // Another target's commit can change tables, and so can anything missed
    // while disconnected: the caches are read again before such a commit is
    // delivered. A failed read still delivers, since listeners read rows anew.
    notifyExternalCommit(commit: TargetCommitEvent): void {
        const changesTables = (commit.source === 'apply' && commit.schemaTables.length > 0) || commit.source === 'reconnect';
        if (!changesTables) {
            this.deliver(commit, false);
            return;
        }
        void this.enqueue(async () => {
            try { await this.refreshCaches(); } catch { /* delivered anyway */ }
            this.deliverNow(commit, false);
        });
    }

    private armChannel(): void {
        const channel = new BroadcastChannel(this.channelName);
        channel.onmessage = (ev: MessageEvent) => {
            const data = ev.data as TargetCommitEvent | undefined;
            if (data !== undefined && typeof data === 'object' && 'source' in data) this.notifyExternalCommit(data);
        };
        this.observerChannel = channel;
        // Commits made while the channel was closed were not heard.
        void this.enqueue(async () => {
            try { await this.refreshCaches(); } catch { /* the next commit reads them again */ }
        });
    }

    private disarmChannel(): void {
        if (this.observerChannel !== undefined) {
            try { this.observerChannel.close(); } catch { /* ignore */ }
            this.observerChannel = undefined;
        }
    }

    // Deliver now, or after what is queued, so deliveries keep their order.
    private deliver(commit: TargetCommitEvent, broadcast: boolean): void {
        if (this.queued === 0) this.deliverNow(commit, broadcast);
        else void this.enqueue(() => this.deliverNow(commit, broadcast));
    }

    private deliverNow(commit: TargetCommitEvent, broadcast: boolean): void {
        if (this.closed) return;
        for (const listener of [...this.commitListeners]) listener(commit);
        if (!broadcast) return;
        const channel = this.observerChannel ?? new BroadcastChannel(this.channelName);
        channel.postMessage(commit);
        if (this.observerChannel === undefined) {
            setTimeout(() => { try { channel.close(); } catch { /* ignore */ } }, 0);
        }
    }

    // -----------------------------------------------------------------------
    // IndexTarget
    // -----------------------------------------------------------------------

    async getIndexState(): Promise<IndexState> {
        this.ensureOpen();
        return this.env.withRead([INDEX_SPEC, INDEX_META], async (tx) => {
            const state: IndexState = { materialized: await storeGetAll<ResolvedIndex>(tx, INDEX_META) };
            const rec = await storeGet<IndexSpecRecord>(tx, INDEX_SPEC, 1);
            if (rec !== undefined) {
                state.spec = rec.spec;
                state.specFingerprint = rec.fingerprint;
            }
            return state;
        });
    }

    validateIndexOptions(decl: IndexDecl): string | undefined {
        return decl.options === undefined ? undefined : NO_INDEX_OPTIONS;
    }

    async installIndexSpec(
        spec: IndexSpec, specFingerprint: string, actions: SchemaAction[],
        expect: { specFingerprint: string | undefined; checkpoints: Map<B64Hash, Version> },
    ): Promise<void> {
        this.ensureOpen();
        await this.env.withReadWrite(async (tx) => {
            const ctx = newCtx(tx);
            const installed = await storeGet<IndexSpecRecord>(tx, INDEX_SPEC, 1);
            if (installed?.fingerprint !== expect.specFingerprint) {
                throw new CheckpointMovedError([...expect.checkpoints.keys()][0] ?? '');
            }
            for (const [groupId, cp] of expect.checkpoints) {
                const rec = await storeGet<CheckpointRecord>(tx, CHECKPOINT, groupId);
                const stored = rec?.version === undefined ? undefined : new Set(rec.version);
                if (!versionsEqual(stored, cp)) throw new CheckpointMovedError(groupId);
            }
            for (const action of actions) {
                if (action.kind === 'drop-index') await this.dropIndex(ctx, action.table, action.name);
                else if (action.kind === 'ensure-index') await this.ensureIndex(ctx, action.index);
                else throw new Error(`installIndexSpec accepts only index actions, got '${action.kind}'`);
            }
            await storePut(tx, INDEX_SPEC, { id: 1, spec, fingerprint: specFingerprint } satisfies IndexSpecRecord);
        });
        await this.enqueue(() => this.refreshCaches());
    }

    // -----------------------------------------------------------------------
    // RowIdentityIndex
    // -----------------------------------------------------------------------

    async rowHashForLocalId(table: string, id: number): Promise<string | undefined> {
        this.ensureOpen();
        const rec = await this.env.withRead(SYNC, (tx) =>
            indexGet<SyncRecord>(tx, SYNC, 'by_local_id', [table, id]));
        return rec?.rowHash;
    }

    async localIdForRowHash(table: string, rowHash: string): Promise<number | undefined> {
        this.ensureOpen();
        const rec = await this.env.withRead(SYNC, (tx) =>
            storeGet<SyncRecord>(tx, SYNC, [table, rowHash]));
        return rec?.id;
    }

    // -----------------------------------------------------------------------
    // KeyIndex
    // -----------------------------------------------------------------------

    async registerKey(_domain: string, keyHash: string, publicKey: string): Promise<number> {
        this.ensureOpen();
        return this.env.withReadWrite((tx) => this.internKey(tx, keyHash, publicKey));
    }

    async keyHashForId(_domain: string, id: number): Promise<string | undefined> {
        this.ensureOpen();
        const rec = await this.env.withRead(KEYS, (tx) => storeGet<KeyRecord>(tx, KEYS, id));
        return rec?.keyHash;
    }

    async publicKeyForId(_domain: string, id: number): Promise<string | undefined> {
        this.ensureOpen();
        const rec = await this.env.withRead(KEYS, (tx) => storeGet<KeyRecord>(tx, KEYS, id));
        return rec?.publicKey;
    }

    async idForKeyHash(_domain: string, keyHash: string): Promise<number | undefined> {
        this.ensureOpen();
        const rec = await this.env.withRead(KEYS, (tx) =>
            indexGet<KeyRecord>(tx, KEYS, 'by_hash', keyHash));
        return rec?.id;
    }

    // -----------------------------------------------------------------------
    // Read helpers (ProjectionReader / tests)
    // -----------------------------------------------------------------------

    async hasTable(table: string): Promise<boolean> {
        this.ensureOpen();
        const rec = await this.env.withRead(TABLE_META, (tx) =>
            storeGet<TableMetaRecord>(tx, TABLE_META, table));
        return rec !== undefined;
    }

    async getRowIds(table: string): Promise<string[]> {
        this.ensureOpen();
        return this.env.withRead([SYNC, ROWS], async (tx) => {
            const syncs = await storeGetAllPrefix<SyncRecord>(tx, SYNC, [table]);
            const ids: string[] = [];
            for (const s of syncs) {
                const row = await storeGet<RowRecord>(tx, ROWS, [table, s.id]);
                if (row !== undefined) ids.push(s.rowHash);
            }
            return ids;
        });
    }

    async getRowByRowId(table: string, rowId: string): Promise<{ author?: number; values: { [column: string]: json.Literal } } | undefined> {
        this.ensureOpen();
        return this.env.withRead([TABLE_META, SYNC, ROWS], async (tx) => {
            const meta = await storeGet<TableMetaRecord>(tx, TABLE_META, table);
            if (meta === undefined) return undefined;
            const sync = await storeGet<SyncRecord>(tx, SYNC, [table, rowId]);
            if (sync === undefined) return undefined;
            const rec = await storeGet<RowRecord>(tx, ROWS, [table, sync.id]);
            if (rec === undefined) return undefined;
            const values: { [column: string]: json.Literal } = {};
            for (const col of Object.keys(meta.columnTypes)) {
                if (rec[col] !== undefined) values[col] = rec[col] as json.Literal;
            }
            const result: { author?: number; values: { [column: string]: json.Literal } } = { values };
            if (meta.authorColumn !== undefined && rec[meta.authorColumn] !== undefined) {
                result.author = rec[meta.authorColumn] as number;
            }
            return result;
        });
    }

    async syncId(table: string, rowId: string): Promise<number | undefined> {
        return this.localIdForRowHash(table, rowId);
    }

    async columnType(table: string, column: string): Promise<ColumnType | undefined> {
        this.ensureOpen();
        const meta = await this.env.withRead(TABLE_META, (tx) =>
            storeGet<TableMetaRecord>(tx, TABLE_META, table));
        return meta?.columnTypes[column];
    }
}
