// IdbTarget: an executing MaterializationTarget for the rdb_adapter. A
// self-contained IndexedDB backend - no dag_idb, no SQL. Logical app tables
// live as documents in a fixed set of object stores (see idb_schema.ts).
//
// App reads/writes go through a duck-typed IDB facade (`database`). Those
// writes are the capture path (outbox + BroadcastChannel). apply() /
// commitIngest reverts talk to the physical stores directly and never write
// the outbox, so materialization cannot echo.
//
// Post-projection extras (secondary indexes, query helpers, ...) belong on
// IdbTarget, not MaterializationTarget, and are out of scope for v1.

import type { json } from "@hyper-hyper-space/hhs3_json";
import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import type { ColumnType } from "@hyper-hyper-space/hhs3_rdb";
import {
    CapturedBatch, CapturedChange, ChangeSignalListener, ChangeSignalSource,
    DEFAULT_KEY_TABLE, IngestSettle, KeyIndex, MaterializationTarget,
    MaterializedChangeSource, OpEvent, OpEventReason, RowAction, RowIdentityIndex,
    SchemaAction, StoredOpEvent, SyncMapping,
} from "@hyper-hyper-space/hhs3_rdb_adapter";

import { FacadeDatabase, type FacadeHost } from "./idb_facade.js";
import {
    deletePrefix, IdbEnv, indexGet, storeAdd, storeDelete, storeGet, storeGetAll,
    storeGetAllPrefix, storePut,
} from "./idb_env.js";
import {
    CAPTURE_CONFIG, CAPTURE_KEY, CHECKPOINT, COUNTER_NEXT_KEY_ID, COUNTERS,
    KEYS, OP_EVENTS, OUTBOX, ROWS, SYNC, TABLE_META,
    type CaptureConfigRecord, type CheckpointRecord, type CounterRecord,
    type KeyRecord, type OpEventRecord, type OutboxRecord, type RowRecord,
    type SyncRecord, type TableMetaRecord,
} from "./idb_schema.js";

export type IdbTargetOptions = {
    captureChanges?: boolean;
    indexedDB?: IDBFactory;
};

type ApplyCtx = {
    tx: IDBTransaction;
    meta: Map<string, TableMetaRecord>;
};

export class IdbTarget implements MaterializationTarget, MaterializedChangeSource,
    ChangeSignalSource, RowIdentityIndex, KeyIndex, FacadeHost {

    readonly name: string;
    readonly env: IdbEnv;
    private readonly captureRequested: boolean;
    private capture = false;
    private captureEnabled = false;
    private tableNameSet = new Set<string>();
    private changeListeners = new Set<ChangeSignalListener>();
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

    isCaptureOn(): boolean { return this.capture && this.captureEnabled; }

    onOutboxCommitted(): void { this.postChangeSignal(); }

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
        const metas = await this.env.withRead(TABLE_META, (tx) => storeGetAll<TableMetaRecord>(tx, TABLE_META));
        for (const m of metas) this.tableNameSet.add(m.table);

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
        if (rec === undefined) return undefined;
        return new Set(rec.version);
    }

    async apply(
        groupId: B64Hash, schemaActions: SchemaAction[], rowActions: RowAction[],
        checkpoint: Version, events?: OpEvent[],
    ): Promise<void> {
        this.ensureOpen();
        await this.env.withReadWrite(async (tx) => {
            const ctx: ApplyCtx = { tx, meta: new Map() };
            for (const action of schemaActions) await this.applySchemaAction(ctx, action);
            for (const action of rowActions) await this.applyRowAction(ctx, action);
            await storePut(tx, CHECKPOINT, {
                groupId, version: [...checkpoint],
            } satisfies CheckpointRecord);
            if (events !== undefined) {
                for (const e of events) await this.logOpEvent(tx, e);
            }
        });
        for (const action of schemaActions) {
            if (action.kind === 'create-table') this.tableNameSet.add(action.table);
            if (action.kind === 'drop-table') this.tableNameSet.delete(action.table);
        }
    }

    // -----------------------------------------------------------------------
    // Schema channel
    // -----------------------------------------------------------------------

    private async applySchemaAction(ctx: ApplyCtx, action: SchemaAction): Promise<void> {
        switch (action.kind) {
            case 'create-table': return this.createTable(ctx, action);
            case 'drop-table': return this.dropTable(ctx, action);
            case 'add-column': return this.addColumn(ctx, action);
            case 'drop-column': return this.dropColumn(ctx, action);
        }
    }

    private async createTable(ctx: ApplyCtx, action: Extract<SchemaAction, { kind: 'create-table' }>): Promise<void> {
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
        };
        await storePut(ctx.tx, TABLE_META, meta);
        ctx.meta.set(action.table, meta);
    }

    private async dropTable(ctx: ApplyCtx, action: Extract<SchemaAction, { kind: 'drop-table' }>): Promise<void> {
        await storeDelete(ctx.tx, TABLE_META, action.table);
        ctx.meta.delete(action.table);
        await deletePrefix(ctx.tx, ROWS, [action.table], (r: RowRecord) => [r.table, r.id]);
        await deletePrefix(ctx.tx, SYNC, [action.table], (r: SyncRecord) => [r.table, r.rowHash]);
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

        if (existing !== undefined) {
            for (const [column, value] of Object.entries(action.values)) {
                existing[column] = await this.valueForColumn(ctx, meta, column, value, action.keyMaterial);
            }
            if (setAuthor && meta.authorColumn !== undefined) existing[meta.authorColumn] = authorId;
            await storePut(ctx.tx, ROWS, existing);
            return;
        }

        const rec: RowRecord = { table: action.table, id };
        rec[meta.idColumn] = id;
        for (const [column, value] of Object.entries(action.values)) {
            rec[column] = await this.valueForColumn(ctx, meta, column, value, action.keyMaterial);
        }
        for (const [column, def] of Object.entries(meta.defaults)) {
            if (rec[column] === undefined) rec[column] = def;
        }
        if (setAuthor && meta.authorColumn !== undefined) rec[meta.authorColumn] = authorId;
        await storePut(ctx.tx, ROWS, rec);
    }

    private async deleteRow(ctx: ApplyCtx, action: Extract<RowAction, { kind: 'delete-row' }>): Promise<void> {
        const sync = await storeGet<SyncRecord>(ctx.tx, SYNC, [action.table, action.rowId]);
        if (sync !== undefined) {
            await storeDelete(ctx.tx, ROWS, [action.table, sync.id]);
            sync.status = 'deleted';
            await storePut(ctx.tx, SYNC, sync);
        }
    }

    private async logOpEvent(tx: IDBTransaction, event: OpEvent): Promise<void> {
        const existing = await indexGet<OpEventRecord>(tx, OP_EVENTS, 'by_op', [event.opHash, event.direction]);
        if (existing !== undefined) return;
        const rec: OpEventRecord = { ...event };
        await storeAdd(tx, OP_EVENTS, rec);
    }

    // -----------------------------------------------------------------------
    // MaterializedChangeSource
    // -----------------------------------------------------------------------

    async drainChanges(): Promise<CapturedBatch> {
        this.requireCapture();
        const rows = await this.env.withRead(OUTBOX, (tx) => storeGetAll<OutboxRecord>(tx, OUTBOX));
        rows.sort((a, b) => (a.id ?? 0) - (b.id ?? 0));
        const changes: CapturedChange[] = [];
        for (const row of rows) {
            if (row.op === 'delete') {
                changes.push({ id: row.id!, kind: 'delete', table: row.table, localId: row.localId });
                continue;
            }
            changes.push({
                id: row.id!, kind: row.op, table: row.table, localId: row.localId,
                values: row.changed ?? {},
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

    async reserveMint(reservations: SyncMapping[]): Promise<void> {
        this.requireCapture();
        await this.env.withReadWrite(async (tx) => {
            const ctx: ApplyCtx = { tx, meta: new Map() };
            for (const m of reservations) {
                const existing = await storeGet<SyncRecord>(tx, SYNC, [m.table, m.rowId]);
                if (existing !== undefined) continue;
                const meta = await this.loadMeta(ctx, m.table);
                if (m.localId >= meta.nextId) {
                    meta.nextId = m.localId + 1;
                    await storePut(tx, TABLE_META, meta);
                }
                await storePut(tx, SYNC, {
                    table: m.table, rowHash: m.rowId, id: m.localId,
                    uuid: m.uuid, status: m.status ?? 'active',
                } satisfies SyncRecord);
            }
        });
    }

    async commitIngest(settle: IngestSettle): Promise<void> {
        this.requireCapture();
        await this.env.withReadWrite(async (tx) => {
            const ctx: ApplyCtx = { tx, meta: new Map() };
            for (const m of settle.mappings ?? []) {
                const existing = await storeGet<SyncRecord>(tx, SYNC, [m.table, m.rowId]);
                const rec: SyncRecord = existing ?? {
                    table: m.table, rowHash: m.rowId, id: m.localId, uuid: m.uuid, status: 'active',
                };
                rec.id = m.localId;
                rec.uuid = m.uuid;
                await storePut(tx, SYNC, rec);
                const meta = await this.loadMeta(ctx, m.table);
                if (m.localId >= meta.nextId) {
                    meta.nextId = m.localId + 1;
                    await storePut(tx, TABLE_META, meta);
                }
            }
            for (const action of settle.reverts ?? []) await this.applyRowAction(ctx, action);
            for (const s of settle.statuses ?? []) {
                const rec = await storeGet<SyncRecord>(tx, SYNC, [s.table, s.rowId]);
                if (rec !== undefined) {
                    rec.status = s.status;
                    await storePut(tx, SYNC, rec);
                }
            }
            for (const e of settle.events ?? []) await this.logOpEvent(tx, e);
            for (const id of settle.consumed) {
                await storeDelete(tx, OUTBOX, id);
            }
        });
    }

    async drainOpEvents(sinceId?: number): Promise<StoredOpEvent[]> {
        this.ensureOpen();
        const rows = await this.env.withRead(OP_EVENTS, (tx) => storeGetAll<OpEventRecord>(tx, OP_EVENTS));
        const since = sinceId ?? 0;
        return rows
            .filter((r) => (r.id ?? 0) > since)
            .sort((a, b) => (a.id ?? 0) - (b.id ?? 0))
            .map((r) => {
                const { id, ...rest } = r;
                const event: OpEvent = {
                    origin: rest.origin, direction: rest.direction, groupId: rest.groupId,
                    opHash: rest.opHash, kind: rest.kind,
                };
                if (rest.table !== undefined) event.table = rest.table;
                if (rest.rowId !== undefined) event.rowId = rest.rowId;
                if (rest.localId !== undefined) event.localId = rest.localId;
                if (rest.author !== undefined) event.author = rest.author;
                if (rest.op !== undefined) event.op = rest.op;
                if (rest.reason !== undefined) event.reason = rest.reason as OpEventReason;
                return { id: id!, event };
            });
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

    addChangeListener(listener: ChangeSignalListener): void {
        this.ensureOpen();
        this.changeListeners.add(listener);
        if (this.changeListeners.size === 1) this.armChannel();
    }

    removeChangeListener(listener: ChangeSignalListener): void {
        this.changeListeners.delete(listener);
        if (this.changeListeners.size === 0) this.disarmChannel();
    }

    private armChannel(): void {
        const channel = new BroadcastChannel(this.channelName);
        channel.onmessage = () => this.notifyLocal();
        this.observerChannel = channel;
        this.notifyLocal();   // prime: already-pending outbox
    }

    private disarmChannel(): void {
        if (this.observerChannel !== undefined) {
            try { this.observerChannel.close(); } catch { /* ignore */ }
            this.observerChannel = undefined;
        }
    }

    private notifyLocal(): void {
        if (!this.capture) return;
        for (const l of [...this.changeListeners]) l({});
    }

    private postChangeSignal(): void {
        if (this.observerChannel !== undefined) {
            this.observerChannel.postMessage(1);
            return;
        }
        const channel = new BroadcastChannel(this.channelName);
        channel.postMessage(1);
        setTimeout(() => {
            try { channel.close(); } catch { /* ignore */ }
        }, 0);
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
