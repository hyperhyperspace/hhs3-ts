// SqliteTarget: an executing MaterializationTarget for the rdb_adapter. A
// self-contained SQLite backend on better-sqlite3 - no dag_sql, no dialect
// abstraction. All SQL here is SQLite-specific and written inline; a future
// Postgres target is its own full class (there are deeper per-engine nuances
// best kept isolated).
//
// State it owns, beyond the app + sync tables the schema actions create:
//   - rdb_checkpoint(version TEXT): the single materialized group Version,
//     stored as JSON.stringify([...Version]); drives initial-vs-delta.
//   - rdb_table_meta(...): one row per materialized app table recording its
//     system-column names, its sync table, and a {col: ColumnType} map. Read at
//     row-application time (and after a restart) so the target never introspects.
//
// Row identity: the content-addressed rowId maps to the projection-local serial
// `id` through the per-table sync table (`<table>_sync`, keyed by `row_hash`).
// The sync row is allocated on first sight of a rowId and KEPT across delete, so
// a void-flip reinstatement reuses the same `id` (stable local identity).
//
// Foreign keys are ADVISORY: local FK columns project as integer `<col>_id`
// referencing the target table's `id`, and the sync table's `id` references its
// app table - both declared, but PRAGMA foreign_keys is left OFF (SQLite's
// default) so they are never enforced. This is deliberate: rdb permits dangling
// references and keeps id mappings across deletes, which enforced FKs would
// reject. Callers must not enable foreign_keys on this connection.
//
// Atomicity: apply() runs schema actions THEN row actions THEN the checkpoint
// commit inside ONE better-sqlite3 transaction; a throw rolls the whole batch
// back, so the target never claims a checkpoint it does not reflect.

import { json } from "@hyper-hyper-space/hhs3_json";
import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { ColumnDef, ColumnType } from "@hyper-hyper-space/hhs3_rdb";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import { watchFile, FileWatchHandle } from "@hyper-hyper-space/hhs3_file_watch";
import Database from "better-sqlite3";

import {
    CapturedBatch, CapturedChange, ChangeSignalListener, ChangeSignalSource,
    DEFAULT_KEY_TABLE, IngestSettle, KeyIndex, MaterializationTarget, MaterializedChangeSource, OpEvent,
    OpEventReason, RowAction, RowIdentityIndex, SchemaAction, StoredOpEvent, SyncMapping,
} from "@hyper-hyper-space/hhs3_rdb_adapter";

// Per-table bookkeeping, mirrored in rdb_table_meta. Cached in memory within a
// process; lazily reloaded from the table after a restart. `fkColumns` maps each
// LOCAL foreign-key column (already the projected `<col>_id` name) to the target
// table it references, so upsert can translate an incoming rowId to that table's
// serial id (cross-group `<col>_row_hash` columns carry no entry - passthrough).
// `keyRefColumns` lists columns whose values are key hashes to intern into
// rdb_keys (provider key_id / identity `<col>_key_id`).
type TableMeta = {
    idColumn: string;
    authorColumn: string | undefined;
    syncTable: string;
    columnTypes: { [column: string]: ColumnType };
    fkColumns: { [column: string]: { targetTable: string } };
    keyRefColumns: string[];
};

// ---------------------------------------------------------------------------
// SQLite literal / identifier / type helpers (inline, not a shared dialect)
// ---------------------------------------------------------------------------

// Double-quote an identifier, escaping embedded quotes.
function quoteId(name: string): string {
    return '"' + name.replace(/"/g, '""') + '"';
}

// Single-quote a text literal, escaping embedded quotes (used for DDL DEFAULTs).
function quoteText(value: string): string {
    return "'" + value.replace(/'/g, "''") + "'";
}

// rdb ColumnType -> SQLite column affinity. SQLite's dynamic typing preserves
// exact stored values, so bigint/decimal/bytes keep rdb's canonical string
// carriers verbatim under TEXT.
function sqliteType(type: ColumnType): string {
    switch (type) {
        case 'integer': return 'INTEGER';
        case 'float': return 'REAL';
        case 'boolean': return 'INTEGER';
        case 'string': return 'TEXT';
        case 'json': return 'TEXT';
        case 'bigint': return 'TEXT';
        case 'decimal': return 'TEXT';
        case 'bytes': return 'TEXT';
        case 'identity': return 'TEXT';  // rdb carrier; projection retypes to integer `<col>_key_id`
    }
}

// A stored parameter value for a column of the given type: booleans as 0/1,
// json as canonical text, everything else (numbers + canonical string carriers)
// bound as-is.
function toParam(value: json.Literal, type: ColumnType): number | string {
    if (type === 'boolean') return value ? 1 : 0;
    if (type === 'json') return json.toStringNormalized(value);
    return value as number | string;
}

// The ` DEFAULT <literal>` fragment for a column decl, or '' when none.
function defaultFragment(def: ColumnDef): string {
    if (def.default === undefined) return '';
    const value = def.default;
    if (def.type === 'boolean') return ' DEFAULT ' + (value ? '1' : '0');
    if (def.type === 'integer' || def.type === 'float') return ' DEFAULT ' + String(value);
    if (def.type === 'json') return ' DEFAULT ' + quoteText(json.toStringNormalized(value));
    // string / bigint / decimal / bytes: canonical string carriers.
    return ' DEFAULT ' + quoteText(String(value));
}

// A business-column declaration: `"name" TYPE [NOT NULL] [DEFAULT x]`. rdb
// guarantees a non-nullable column added later carries a default, satisfying
// SQLite's ADD COLUMN rule.
function columnDecl(name: string, def: ColumnDef): string {
    const nn = def.nullable ? '' : ' NOT NULL';
    return `${quoteId(name)} ${sqliteType(def.type)}${nn}${defaultFragment(def)}`;
}

// ---------------------------------------------------------------------------

// Convert a stored SQLite value back to its logical rdb value, inverting
// toParam: booleans from 0/1, json from canonical text, everything else as-is.
// Used only on the inbound path (draining captured changes).
function toLogical(value: unknown, type: ColumnType): json.Literal | undefined {
    if (value === null || value === undefined) return undefined;
    if (type === 'boolean') return value !== 0;
    if (type === 'json') return JSON.parse(value as string);
    return value as json.Literal;
}

export class SqliteTarget implements MaterializationTarget, MaterializedChangeSource, ChangeSignalSource, RowIdentityIndex, KeyIndex {
    private db: Database.Database;
    private metaCache = new Map<string, TableMeta>();
    private bookkeepingReady = false;

    // ChangeSignalSource: an external outbox monitor (the app writes through its
    // OWN db handle, and better-sqlite3 exposes no commit hook, so we watch for
    // the outbox to advance - the same "external monitor" shape dag's DagStore
    // uses for cross-context growth). Two strategies, selected once at
    // construction:
    //   - WAL watch (default for a file-backed db): fs.watch on `${dbPath}-wal`
    //     via the generic file_watch helper. Kernel-driven, so the process
    //     sleeps when idle. Requires a real file path (see dbPath below);
    //     ensureBookkeeping puts the db in WAL journal mode so the file exists.
    //   - poll (fallback): a same-process interval poll of the monotonic
    //     AUTOINCREMENT outbox id. Used for `:memory:` (no WAL file) and when no
    //     dbPath is supplied, and on platforms where fs.watch is unreliable.
    // Either way the signal is opaque (ChangeSignal = {}): observers react by
    // re-draining authoritatively, so over-notifying is harmless.
    // Lazily armed on the first listener, disarmed (epoch-bumped) on the last.
    private readonly pollMs: number;
    private readonly walPath: string | undefined;
    private changeListeners = new Set<ChangeSignalListener>();
    private monitorTimer: ReturnType<typeof setInterval> | undefined;
    private walHandle: FileWatchHandle | undefined;
    private monitorEpoch = 0;
    private lastOutboxId = 0;

    // Two-level change capture:
    //   - provisioning (constructor `captureChanges`, PERSISTED by the presence
    //     of rdb_capture_config): whether the outbox + triggers exist AT ALL. A
    //     non-capturing target pays zero overhead (no config, no outbox, no
    //     triggers). Persisted so a reopened db keeps capturing.
    //   - runtime enable (rdb_capture_config.enabled): whether the always-present
    //     triggers actually record, toggled without DDL.
    // Echo suppression uses rdb_capture_config.applying, set for the duration of
    // apply()'s transaction so the adapter's own writes never re-enter the outbox
    // (safe because SQLite serializes writers).
    private readonly captureRequested: boolean;
    private capture = false;

    constructor(db: Database.Database, opts: { captureChanges?: boolean; pollMs?: number; dbPath?: string } = {}) {
        this.db = db;
        this.captureRequested = opts.captureChanges === true;
        this.pollMs = opts.pollMs ?? 250;
        // WAL watching needs a real on-disk file: an in-memory db has no WAL.
        // Only arm it when a concrete file path is supplied (the `:memory:` and
        // empty cases fall back to polling).
        const dbPath = opts.dbPath;
        this.walPath = dbPath !== undefined && dbPath !== '' && dbPath !== ':memory:'
            ? `${dbPath}-wal` : undefined;
    }

    async getCheckpoint(groupId: B64Hash): Promise<Version | undefined> {
        this.ensureBookkeeping();
        const row = this.db.prepare('SELECT version FROM rdb_checkpoint WHERE group_id = ?').get(groupId) as
            { version: string } | undefined;
        if (row === undefined) return undefined;
        return new Set(JSON.parse(row.version) as string[]);
    }

    async apply(
        groupId: B64Hash, schemaActions: SchemaAction[], rowActions: RowAction[],
        checkpoint: Version, events?: OpEvent[],
    ): Promise<void> {
        this.ensureBookkeeping();
        const run = this.db.transaction(() => {
            // Echo suppression: mark the whole batch as adapter-authored so the
            // capture triggers no-op on our own materialization writes.
            if (this.capture) this.setApplying(true);
            for (const action of schemaActions) this.applySchemaAction(action);
            for (const action of rowActions) this.applyRowAction(action);
            this.persistCheckpoint(groupId, checkpoint);
            // Concurrency void/reinstate flips ride the same checkpoint advance.
            if (events !== undefined) for (const e of events) this.logOpEvent(e);
            if (this.capture) this.setApplying(false);
        });
        run();
    }

    // -----------------------------------------------------------------------
    // Bookkeeping + checkpoint
    // -----------------------------------------------------------------------

    private ensureBookkeeping(): void {
        if (this.bookkeepingReady) return;
        // FKs are ADVISORY: turn enforcement OFF on this connection so our
        // declared FKs (local `<col>_id` -> target id; sync id -> app id) are
        // never checked. rdb permits dangling references and keeps id mappings
        // across deletes, both of which enforced FKs would reject. (Some SQLite
        // bindings enable foreign_keys by default, so we force it off here.)
        this.db.pragma('foreign_keys = OFF');
        // When we detect local edits by watching the WAL file, the db must
        // actually be in WAL journal mode (better-sqlite3 defaults to a rollback
        // journal, which never creates a `-wal` file). Enabling it here keeps the
        // caller from having to know about the change-signal strategy.
        if (this.walPath !== undefined) this.db.pragma('journal_mode = WAL');
        this.db.exec(
            'CREATE TABLE IF NOT EXISTS rdb_checkpoint (group_id TEXT PRIMARY KEY, version TEXT);'
            + 'CREATE TABLE IF NOT EXISTS rdb_table_meta ('
            + '"table" TEXT PRIMARY KEY, id_column TEXT NOT NULL, author_column TEXT, '
            + "sync_table TEXT NOT NULL, column_types TEXT NOT NULL, fk_columns TEXT NOT NULL DEFAULT '{}', "
            + "key_ref_columns TEXT NOT NULL DEFAULT '[]');"
            + `CREATE TABLE IF NOT EXISTS ${quoteId(DEFAULT_KEY_TABLE)} (`
            + 'id INTEGER PRIMARY KEY, key_hash TEXT NOT NULL UNIQUE, public_key TEXT);'
            // The durable op-event log (ingestion failures + concurrency flips).
            // Present on ANY target: concurrency void/reinstate flips are logged
            // by apply() even on a read-only projection. Append-only; idempotent
            // by (op_hash, direction).
            + 'CREATE TABLE IF NOT EXISTS rdb_op_events ('
            + 'id INTEGER PRIMARY KEY AUTOINCREMENT, origin TEXT NOT NULL, direction TEXT NOT NULL, '
            + 'group_id TEXT NOT NULL, op_hash TEXT NOT NULL, op_json TEXT, kind TEXT NOT NULL, '
            + '"table" TEXT, row_hash TEXT, local_id INTEGER, author TEXT, reason TEXT, '
            + "created_at TEXT NOT NULL DEFAULT (datetime('now')), UNIQUE (op_hash, direction));");

        // Migration: older sync tables predate the `status` column. Add it where
        // missing so upsert/delete/reserve can record lifecycle status.
        const metaRows = this.db.prepare('SELECT sync_table FROM rdb_table_meta').all() as { sync_table: string }[];
        for (const { sync_table } of metaRows) {
            const cols = this.db.prepare(`SELECT name FROM pragma_table_info(?)`).all(sync_table) as { name: string }[];
            if (!cols.some((c) => c.name === 'status')) {
                this.db.exec(`ALTER TABLE ${quoteId(sync_table)} ADD COLUMN status TEXT NOT NULL DEFAULT 'active'`);
            }
        }

        // Provisioning is sticky: once the config table exists, this db captures
        // (even if a later handle omits the flag), so triggers/outbox survive.
        const provisioned = this.db.prepare(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='rdb_capture_config'").get() !== undefined;
        this.capture = this.captureRequested || provisioned;
        if (this.capture) this.ensureCaptureInfra();

        this.bookkeepingReady = true;
    }

    // Append one op-event (idempotent by (op_hash, direction)).
    private logOpEvent(event: OpEvent): void {
        this.db.prepare(
            'INSERT OR IGNORE INTO rdb_op_events '
            + '(origin, direction, group_id, op_hash, op_json, kind, "table", row_hash, local_id, author, reason) '
            + 'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)')
            .run(
                event.origin, event.direction, event.groupId, event.opHash,
                event.op === undefined ? null : json.toStringNormalized(event.op),
                event.kind, event.table ?? null, event.rowId ?? null,
                event.localId ?? null, event.author ?? null,
                event.reason === undefined ? null : JSON.stringify(event.reason));
    }

    // Provision the capture infrastructure (idempotent). The config row holds
    // both level-2 flags: `enabled` (record?) and `applying` (adapter write in
    // progress? -> suppress).
    private ensureCaptureInfra(): void {
        this.db.exec(
            'CREATE TABLE IF NOT EXISTS rdb_capture_config (enabled INTEGER NOT NULL, applying INTEGER NOT NULL);'
            + 'CREATE TABLE IF NOT EXISTS rdb_outbox ('
            + 'id INTEGER PRIMARY KEY AUTOINCREMENT, "table" TEXT NOT NULL, '
            + 'local_id INTEGER NOT NULL, op TEXT NOT NULL, changed TEXT);');
        const has = this.db.prepare('SELECT 1 FROM rdb_capture_config LIMIT 1').get() !== undefined;
        if (!has) this.db.prepare('INSERT INTO rdb_capture_config (enabled, applying) VALUES (1, 0)').run();
    }

    private setApplying(on: boolean): void {
        this.db.prepare('UPDATE rdb_capture_config SET applying = ?').run(on ? 1 : 0);
    }

    // Install (or reinstall) the capture triggers for a table from its current
    // columns: one AFTER INSERT (full row image), one AFTER DELETE, and one
    // AFTER UPDATE OF <col> per business column so an UPDATE captures ONLY the
    // columns it actually changed (net-image updates would falsely include
    // readonly columns). Every trigger body is gated on enabled=1 AND applying=0.
    private installTriggers(table: string, meta: TableMeta): void {
        // Drop any prior capture triggers for this table (column set may have changed).
        const existing = this.db.prepare(
            "SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name = ? AND name LIKE 'rdb_cap_%'")
            .all(table) as { name: string }[];
        for (const t of existing) this.db.exec(`DROP TRIGGER ${quoteId(t.name)}`);

        const gate = '(SELECT enabled FROM rdb_capture_config) = 1 AND (SELECT applying FROM rdb_capture_config) = 0';
        const cols = Object.keys(meta.columnTypes);
        const tableLit = quoteText(table);
        const id = quoteId(meta.idColumn);

        const jsonObject = (ref: 'NEW' | 'OLD'): string => {
            if (cols.length === 0) return "json_object()";
            const pairs = cols.map((c) => `${quoteText(c)}, ${ref}.${quoteId(c)}`).join(', ');
            return `json_object(${pairs})`;
        };

        this.db.exec(
            `CREATE TRIGGER ${quoteId('rdb_cap_ins_' + table)} AFTER INSERT ON ${quoteId(table)} BEGIN `
            + `INSERT INTO rdb_outbox ("table", local_id, op, changed) `
            + `SELECT ${tableLit}, NEW.${id}, 'insert', ${jsonObject('NEW')} WHERE ${gate}; END`);

        this.db.exec(
            `CREATE TRIGGER ${quoteId('rdb_cap_del_' + table)} AFTER DELETE ON ${quoteId(table)} BEGIN `
            + `INSERT INTO rdb_outbox ("table", local_id, op, changed) `
            + `SELECT ${tableLit}, OLD.${id}, 'delete', NULL WHERE ${gate}; END`);

        cols.forEach((col, i) => {
            this.db.exec(
                `CREATE TRIGGER ${quoteId('rdb_cap_upd_' + table + '_' + i)} `
                + `AFTER UPDATE OF ${quoteId(col)} ON ${quoteId(table)} BEGIN `
                + `INSERT INTO rdb_outbox ("table", local_id, op, changed) `
                + `SELECT ${tableLit}, NEW.${id}, 'update', json_object(${quoteText(col)}, NEW.${quoteId(col)}) `
                + `WHERE ${gate}; END`);
        });
    }

    private requireCapture(): void {
        this.ensureBookkeeping();
        if (!this.capture) {
            throw new Error("this SqliteTarget was not provisioned for change capture (new SqliteTarget(db, { captureChanges: true }))");
        }
    }

    // Runtime enable/disable (level 2). No-op unless provisioned.
    setCaptureEnabled(on: boolean): void {
        this.ensureBookkeeping();
        if (this.capture) this.db.prepare('UPDATE rdb_capture_config SET enabled = ?').run(on ? 1 : 0);
    }

    private persistCheckpoint(groupId: B64Hash, checkpoint: Version): void {
        this.db.prepare('INSERT OR REPLACE INTO rdb_checkpoint (group_id, version) VALUES (?, ?)')
            .run(groupId, JSON.stringify([...checkpoint]));
    }

    private writeMeta(table: string, meta: TableMeta): void {
        this.db.prepare(
            'INSERT OR REPLACE INTO rdb_table_meta '
            + '("table", id_column, author_column, sync_table, column_types, fk_columns, key_ref_columns) '
            + 'VALUES (?, ?, ?, ?, ?, ?, ?)')
            .run(table, meta.idColumn, meta.authorColumn ?? null, meta.syncTable,
                JSON.stringify(meta.columnTypes), JSON.stringify(meta.fkColumns),
                JSON.stringify(meta.keyRefColumns));
        this.metaCache.set(table, meta);
    }

    private loadMeta(table: string): TableMeta {
        const cached = this.metaCache.get(table);
        if (cached !== undefined) return cached;

        const row = this.db.prepare(
            'SELECT id_column, author_column, sync_table, column_types, fk_columns, key_ref_columns '
            + 'FROM rdb_table_meta WHERE "table" = ?')
            .get(table) as
            { id_column: string; author_column: string | null; sync_table: string;
                column_types: string; fk_columns: string | null; key_ref_columns: string | null }
            | undefined;
        if (row === undefined) throw new Error(`no materialized metadata for table '${table}'`);

        const meta: TableMeta = {
            idColumn: row.id_column,
            authorColumn: row.author_column ?? undefined,
            syncTable: row.sync_table,
            columnTypes: JSON.parse(row.column_types) as { [column: string]: ColumnType },
            fkColumns: JSON.parse(row.fk_columns ?? '{}') as { [column: string]: { targetTable: string } },
            keyRefColumns: JSON.parse(row.key_ref_columns ?? '[]') as string[],
        };
        this.metaCache.set(table, meta);
        return meta;
    }

    // -----------------------------------------------------------------------
    // Schema channel
    // -----------------------------------------------------------------------

    private applySchemaAction(action: SchemaAction): void {
        switch (action.kind) {
            case 'create-table': return this.createTable(action);
            case 'drop-table': return this.dropTable(action);
            case 'add-column': return this.addColumn(action);
            case 'drop-column': return this.dropColumn(action);
        }
    }

    private createTable(action: Extract<SchemaAction, { kind: 'create-table' }>): void {
        // The app table OWNS the serial `id` (INTEGER PRIMARY KEY). Ids are set
        // explicitly (allocated via the sync table) so a void-flip reinstatement
        // reuses the same id; no AUTOINCREMENT needed. Local FK columns declare an
        // advisory (UNENFORCED) DB FK to the target's id - unenforced because rdb
        // permits dangling references and keeps id mappings across deletes
        // (PRAGMA foreign_keys is left OFF; see the class header note). The author
        // column is an integer key-ref into rdb_keys, emitted LAST so SELECT *
        // reads as id, business columns, then author_key_id.
        const cols: string[] = [`${quoteId(action.primaryKey)} INTEGER PRIMARY KEY`];
        for (const c of action.columns) cols.push(columnDecl(c.name, c.def));
        if (action.authorColumn !== undefined) {
            cols.push(`${quoteId(action.authorColumn)} INTEGER`);
        }
        // Table-level FK constraints AFTER all column definitions (SQLite is
        // picky about interleaving in some versions / quoting contexts).
        if (action.authorColumn !== undefined) {
            cols.push(
                `FOREIGN KEY (${quoteId(action.authorColumn)}) `
                + `REFERENCES ${quoteId(DEFAULT_KEY_TABLE)} (id)`);
        }
        for (const c of action.columns) {
            if (c.fk !== undefined && c.fk.crossGroup !== true) {
                cols.push(
                    `FOREIGN KEY (${quoteId(c.name)}) REFERENCES ${quoteId(c.fk.targetTable)} (${quoteId(action.primaryKey)})`);
            }
            if (c.keyRef === true) {
                cols.push(
                    `FOREIGN KEY (${quoteId(c.name)}) REFERENCES ${quoteId(DEFAULT_KEY_TABLE)} (id)`);
            }
        }

        this.db.exec(`CREATE TABLE ${quoteId(action.table)} (${cols.join(', ')})`);
        // Sync table keyed by row_hash, mapping to the app table's serial id via an
        // advisory FK. `uuid` is the minted uuid for ingested rows (null otherwise).
        // `status` is the row lifecycle (active / deleted / ingestion_failure);
        // the sync record survives a delete so status records WHY it is gone.
        this.db.exec(
            `CREATE TABLE ${quoteId(action.syncTable)} (`
            + `"row_hash" TEXT PRIMARY KEY, "id" INTEGER, "uuid" TEXT, `
            + `"status" TEXT NOT NULL DEFAULT 'active', `
            + `FOREIGN KEY ("id") REFERENCES ${quoteId(action.table)} (${quoteId(action.primaryKey)}))`);

        const columnTypes: { [column: string]: ColumnType } = {};
        const fkColumns: { [column: string]: { targetTable: string } } = {};
        const keyRefColumns: string[] = [];
        for (const c of action.columns) {
            columnTypes[c.name] = c.def.type;
            if (c.fk !== undefined) fkColumns[c.name] = { targetTable: c.fk.targetTable };
            if (c.keyRef === true) keyRefColumns.push(c.name);
        }
        const meta: TableMeta = {
            idColumn: action.primaryKey,
            authorColumn: action.authorColumn,
            syncTable: action.syncTable,
            columnTypes,
            fkColumns,
            keyRefColumns,
        };
        this.writeMeta(action.table, meta);
        if (this.capture) this.installTriggers(action.table, meta);
    }

    private dropTable(action: Extract<SchemaAction, { kind: 'drop-table' }>): void {
        this.db.exec(`DROP TABLE ${quoteId(action.table)}`);
        this.db.exec(`DROP TABLE ${quoteId(action.syncTable)}`);
        this.db.prepare('DELETE FROM rdb_table_meta WHERE "table" = ?').run(action.table);
        this.metaCache.delete(action.table);
    }

    private addColumn(action: Extract<SchemaAction, { kind: 'add-column' }>): void {
        // SQLite cannot ADD COLUMN with an inline table-level FK constraint, so a
        // local FK / key-ref added later is tracked in meta (for upsert translation)
        // but its advisory DB FK is not retrofitted - acceptable since FKs are
        // advisory.
        this.db.exec(`ALTER TABLE ${quoteId(action.table)} ADD COLUMN ${columnDecl(action.column, action.def)}`);
        const meta = this.loadMeta(action.table);
        meta.columnTypes[action.column] = action.def.type;
        if (action.fk !== undefined) meta.fkColumns[action.column] = { targetTable: action.fk.targetTable };
        if (action.keyRef === true) meta.keyRefColumns.push(action.column);
        this.writeMeta(action.table, meta);
        if (this.capture) this.installTriggers(action.table, meta);   // pick up the new column
    }

    private dropColumn(action: Extract<SchemaAction, { kind: 'drop-column' }>): void {
        // A plain-column drop is a direct ALTER. But SQLite REFUSES to DROP a
        // column named in a table-level FOREIGN KEY clause (a projected FK
        // companion `<col>_id`, or a key-ref column) - it would leave a dangling
        // FK definition. Such a column (e.g. an FK-ness flip reverting a
        // `<col>_id` companion to a plain column) is dropped via a table rebuild
        // that also removes its now-obsolete FK constraint.
        const fkList = this.db.prepare(`PRAGMA foreign_key_list(${quoteId(action.table)})`)
            .all() as { id: number; seq: number; table: string; from: string; to: string }[];
        if (fkList.some((f) => f.from === action.column)) {
            this.rebuildTableWithoutColumn(action.table, action.column, fkList);
        } else {
            this.db.exec(`ALTER TABLE ${quoteId(action.table)} DROP COLUMN ${quoteId(action.column)}`);
        }
        const meta = this.loadMeta(action.table);
        delete meta.columnTypes[action.column];
        delete meta.fkColumns[action.column];
        meta.keyRefColumns = meta.keyRefColumns.filter((c) => c !== action.column);
        this.writeMeta(action.table, meta);
        if (this.capture) this.installTriggers(action.table, meta);   // drop the column's trigger
    }

    // Rebuild `table` without `column` (and without any FK constraint that
    // referenced it), preserving every surviving column's EXACT definition and
    // the surviving advisory FK constraints. Used when a plain DROP COLUMN would
    // be rejected for naming a column in a FOREIGN KEY clause. Column defs are
    // read live via PRAGMA table_info, so no NOT NULL / DEFAULT detail is lost
    // (meta records only ColumnType). Runs inside apply()'s transaction; because
    // foreign_keys is OFF, dropping the old table does not trip inbound
    // (sync-table) references, and the RENAME - by the unchanged table name -
    // leaves those references intact.
    private rebuildTableWithoutColumn(
        table: string, column: string,
        fkList: { id: number; from: string; to: string; table: string }[],
    ): void {
        const meta = this.loadMeta(table);
        const info = this.db.prepare(`PRAGMA table_info(${quoteId(table)})`)
            .all() as { name: string; type: string; notnull: number; dflt_value: string | null; pk: number }[];
        const keep = info.filter((c) => c.name !== column);

        // The id column is `INTEGER PRIMARY KEY` (owned serial); every other
        // column re-renders its stored type + NOT NULL + DEFAULT verbatim.
        const decls: string[] = keep.map((c) => c.name === meta.idColumn
            ? `${quoteId(c.name)} INTEGER PRIMARY KEY`
            : `${quoteId(c.name)} ${c.type}${c.notnull ? ' NOT NULL' : ''}`
                + `${c.dflt_value !== null ? ' DEFAULT ' + c.dflt_value : ''}`);

        // Re-emit every surviving FK constraint (grouped by its id), skipping the
        // group that referenced the dropped column. Projected FKs are single-column.
        const byId = new Map<number, { from: string; to: string; table: string }[]>();
        for (const f of fkList) byId.set(f.id, [...(byId.get(f.id) ?? []), f]);
        for (const group of byId.values()) {
            if (group.some((f) => f.from === column)) continue;
            const froms = group.map((f) => quoteId(f.from)).join(', ');
            const tos = group.map((f) => quoteId(f.to)).join(', ');
            decls.push(`FOREIGN KEY (${froms}) REFERENCES ${quoteId(group[0].table)} (${tos})`);
        }

        const tmp = table + '__rebuild';
        const cols = keep.map((c) => quoteId(c.name)).join(', ');
        this.db.exec(`CREATE TABLE ${quoteId(tmp)} (${decls.join(', ')})`);
        this.db.exec(`INSERT INTO ${quoteId(tmp)} (${cols}) SELECT ${cols} FROM ${quoteId(table)}`);
        this.db.exec(`DROP TABLE ${quoteId(table)}`);
        this.db.exec(`ALTER TABLE ${quoteId(tmp)} RENAME TO ${quoteId(table)}`);
    }

    // -----------------------------------------------------------------------
    // Row channel
    // -----------------------------------------------------------------------

    private applyRowAction(action: RowAction): void {
        if (action.kind === 'upsert-row') return this.upsertRow(action);
        return this.deleteRow(action);
    }

    // Allocate (or reuse) the serial id for a rowId in a sync table. The sync
    // table is keyed by row_hash; ids are handed out monotonically as MAX+1
    // (safe because SQLite serializes writers). Allocating for a not-yet-seen
    // target rowId (e.g. an FK reference whose target row is not yet projected)
    // reserves the id without an app row; the app row later reuses it.
    private allocateId(syncTable: string, rowId: string): number {
        const existing = this.db.prepare(`SELECT "id" FROM ${quoteId(syncTable)} WHERE "row_hash" = ?`)
            .get(rowId) as { id: number } | undefined;
        if (existing !== undefined) return existing.id;
        const next = (this.db.prepare(`SELECT COALESCE(MAX("id"), 0) + 1 AS n FROM ${quoteId(syncTable)}`)
            .get() as { n: number }).n;
        this.db.prepare(`INSERT INTO ${quoteId(syncTable)} ("row_hash", "id") VALUES (?, ?)`).run(rowId, next);
        return next;
    }

    // Intern a key hash into rdb_keys, optionally backfilling public_key.
    private internKey(keyHash: string, publicKey?: string): number {
        const existing = this.db.prepare(
            `SELECT id, public_key AS pk FROM ${quoteId(DEFAULT_KEY_TABLE)} WHERE key_hash = ?`)
            .get(keyHash) as { id: number; pk: string | null } | undefined;
        if (existing !== undefined) {
            if (publicKey !== undefined && existing.pk === null) {
                this.db.prepare(
                    `UPDATE ${quoteId(DEFAULT_KEY_TABLE)} SET public_key = ? WHERE id = ?`)
                    .run(publicKey, existing.id);
            }
            return existing.id;
        }
        const next = (this.db.prepare(
            `SELECT COALESCE(MAX(id), 0) + 1 AS n FROM ${quoteId(DEFAULT_KEY_TABLE)}`)
            .get() as { n: number }).n;
        this.db.prepare(
            `INSERT INTO ${quoteId(DEFAULT_KEY_TABLE)} (id, key_hash, public_key) VALUES (?, ?, ?)`)
            .run(next, keyHash, publicKey ?? null);
        return next;
    }

    // The stored parameter for one projected column value. A local FK column
    // carries an rdb rowId (row_hash); translate it to the referenced table's
    // serial id (allocating on demand). A key-ref column carries a key hash;
    // intern it into rdb_keys. Cross-group `_row_hash` columns are not in
    // fkColumns and pass through as text.
    private paramForColumn(
        meta: TableMeta, col: string, value: json.Literal,
        keyMaterial?: { [keyHash: string]: string },
    ): number | string {
        const fk = meta.fkColumns[col];
        if (fk !== undefined) {
            const targetMeta = this.loadMeta(fk.targetTable);
            return this.allocateId(targetMeta.syncTable, value as string);
        }
        if (meta.keyRefColumns.includes(col)) {
            const keyHash = value as string;
            return this.internKey(keyHash, keyMaterial?.[keyHash]);
        }
        return toParam(value, meta.columnTypes[col] ?? 'string');
    }

    private upsertRow(action: Extract<RowAction, { kind: 'upsert-row' }>): void {
        const meta = this.loadMeta(action.table);
        const id = this.allocateId(meta.syncTable, action.rowId);
        // A (re)materialized row is live: active (reinstates a prior deleted /
        // ingestion_failure record for the same rowId).
        this.db.prepare(`UPDATE ${quoteId(meta.syncTable)} SET "status" = 'active' WHERE "row_hash" = ?`)
            .run(action.rowId);

        // Apply keyMaterial first so subsequent key-ref interns see the pubkey.
        if (action.keyMaterial !== undefined) {
            for (const [hash, pk] of Object.entries(action.keyMaterial)) {
                this.internKey(hash, pk);
            }
        }

        const exists = this.db.prepare(
            `SELECT 1 FROM ${quoteId(action.table)} WHERE ${quoteId(meta.idColumn)} = ?`).get(id) !== undefined;

        const columns = Object.keys(action.values);
        const setAuthor = meta.authorColumn !== undefined && action.author !== undefined;
        const authorId = setAuthor ? this.internKey(action.author!) : undefined;

        if (exists) {
            const assignments: string[] = [];
            const params: (number | string)[] = [];
            for (const col of columns) {
                assignments.push(`${quoteId(col)} = ?`);
                params.push(this.paramForColumn(meta, col, action.values[col], action.keyMaterial));
            }
            if (setAuthor) {
                assignments.push(`${quoteId(meta.authorColumn!)} = ?`);
                params.push(authorId!);
            }
            if (assignments.length === 0) return;   // nothing to update
            params.push(id);
            this.db.prepare(
                `UPDATE ${quoteId(action.table)} SET ${assignments.join(', ')} `
                + `WHERE ${quoteId(meta.idColumn)} = ?`).run(...params);
            return;
        }

        const insertCols: string[] = [meta.idColumn];
        const insertParams: (number | string)[] = [id];
        for (const col of columns) {
            insertCols.push(col);
            insertParams.push(this.paramForColumn(meta, col, action.values[col], action.keyMaterial));
        }
        if (setAuthor) {
            insertCols.push(meta.authorColumn!);
            insertParams.push(authorId!);
        }
        const placeholders = insertParams.map(() => '?').join(', ');
        this.db.prepare(
            `INSERT INTO ${quoteId(action.table)} (${insertCols.map(quoteId).join(', ')}) `
            + `VALUES (${placeholders})`).run(...insertParams);
    }

    private deleteRow(action: Extract<RowAction, { kind: 'delete-row' }>): void {
        const meta = this.loadMeta(action.table);
        // Delete only the app row; the sync row is intentionally kept so a
        // later void-flip reinstatement reuses the same serial id. Status ->
        // deleted (the settle overrides to ingestion_failure for a reverted
        // insert orphan).
        this.db.prepare(
            `DELETE FROM ${quoteId(action.table)} WHERE ${quoteId(meta.idColumn)} = `
            + `(SELECT "id" FROM ${quoteId(meta.syncTable)} WHERE "row_hash" = ?)`).run(action.rowId);
        this.db.prepare(`UPDATE ${quoteId(meta.syncTable)} SET "status" = 'deleted' WHERE "row_hash" = ?`)
            .run(action.rowId);
    }

    // -----------------------------------------------------------------------
    // MaterializedChangeSource (inbound). Requires provisioned capture.
    // -----------------------------------------------------------------------

    async drainChanges(): Promise<CapturedBatch> {
        this.requireCapture();
        const rows = this.db.prepare(
            'SELECT id, "table" AS tbl, local_id, op, changed FROM rdb_outbox ORDER BY id')
            .all() as { id: number; tbl: string; local_id: number; op: string; changed: string | null }[];

        const changes: CapturedChange[] = [];
        for (const row of rows) {
            if (row.op === 'delete') {
                changes.push({ id: row.id, kind: 'delete', table: row.tbl, localId: row.local_id });
                continue;
            }
            const meta = this.loadMeta(row.tbl);
            const stored = (row.changed === null ? {} : JSON.parse(row.changed)) as { [column: string]: unknown };
            const values: { [column: string]: json.Literal } = {};
            for (const [col, raw] of Object.entries(stored)) {
                const logical = toLogical(raw, meta.columnTypes[col] ?? 'string');
                if (logical !== undefined) values[col] = logical;
            }
            changes.push({ id: row.id, kind: row.op === 'insert' ? 'insert' : 'update', table: row.tbl, localId: row.local_id, values });
        }
        return { changes };
    }

    async resolveRow(table: string, localId: number): Promise<SyncMapping | undefined> {
        this.requireCapture();
        const meta = this.loadMeta(table);
        const row = this.db.prepare(
            `SELECT "row_hash" AS rh, "uuid" AS uuid, "status" AS status FROM ${quoteId(meta.syncTable)} WHERE "id" = ?`)
            .get(localId) as { rh: string; uuid: string | null; status: string } | undefined;
        if (row === undefined) return undefined;
        return { table, localId, rowId: row.rh, uuid: row.uuid ?? '', status: row.status as SyncMapping['status'] };
    }

    // Durably persist minted identities (row_hash, id, uuid, status='active')
    // BEFORE the append walk, in their own transaction. Idempotent: an existing
    // reserved row (a crash-replay read-back) is kept as-is (ON CONFLICT DO
    // NOTHING) so its uuid/status survive.
    async reserveMint(reservations: SyncMapping[]): Promise<void> {
        this.requireCapture();
        const run = this.db.transaction(() => {
            for (const m of reservations) {
                const meta = this.loadMeta(m.table);
                this.db.prepare(
                    `INSERT INTO ${quoteId(meta.syncTable)} ("row_hash", "id", "uuid", "status") `
                    + `VALUES (?, ?, ?, ?) ON CONFLICT("row_hash") DO NOTHING`)
                    .run(m.rowId, m.localId, m.uuid === '' ? null : m.uuid, m.status ?? 'active');
            }
        });
        run();
    }

    async commitIngest(settle: IngestSettle): Promise<void> {
        this.requireCapture();
        const run = this.db.transaction(() => {
            // Echo suppression: reverts materialize rows; gate the capture triggers
            // so they don't re-enter the outbox.
            this.setApplying(true);
            for (const m of settle.mappings ?? []) {
                const meta = this.loadMeta(m.table);
                this.db.prepare(
                    `INSERT INTO ${quoteId(meta.syncTable)} ("row_hash", "id", "uuid") VALUES (?, ?, ?) `
                    + `ON CONFLICT("row_hash") DO UPDATE SET "id" = excluded."id", "uuid" = excluded."uuid"`)
                    .run(m.rowId, m.localId, m.uuid === '' ? null : m.uuid);
            }
            for (const action of settle.reverts ?? []) this.applyRowAction(action);
            for (const s of settle.statuses ?? []) {
                const meta = this.loadMeta(s.table);
                this.db.prepare(`UPDATE ${quoteId(meta.syncTable)} SET "status" = ? WHERE "row_hash" = ?`)
                    .run(s.status, s.rowId);
            }
            for (const e of settle.events ?? []) this.logOpEvent(e);
            this.setApplying(false);
            for (const id of settle.consumed) {
                this.db.prepare('DELETE FROM rdb_outbox WHERE id = ?').run(id);
            }
        });
        run();
    }

    async drainOpEvents(sinceId?: number): Promise<StoredOpEvent[]> {
        this.ensureBookkeeping();
        const rows = this.db.prepare(
            'SELECT id, origin, direction, group_id, op_hash, op_json, kind, "table" AS tbl, '
            + 'row_hash, local_id, author, reason FROM rdb_op_events WHERE id > ? ORDER BY id')
            .all(sinceId ?? 0) as {
                id: number; origin: string; direction: string; group_id: string; op_hash: string;
                op_json: string | null; kind: string; tbl: string | null; row_hash: string | null;
                local_id: number | null; author: string | null; reason: string | null;
            }[];
        return rows.map((r) => {
            const event: OpEvent = {
                origin: r.origin as OpEvent['origin'], direction: r.direction as OpEvent['direction'],
                groupId: r.group_id as B64Hash, opHash: r.op_hash as B64Hash, kind: r.kind as OpEvent['kind'],
            };
            if (r.tbl !== null) event.table = r.tbl;
            if (r.row_hash !== null) event.rowId = r.row_hash as B64Hash;
            if (r.local_id !== null) event.localId = r.local_id;
            if (r.author !== null) event.author = r.author as OpEvent['author'];
            if (r.op_json !== null) event.op = JSON.parse(r.op_json) as json.Literal;
            if (r.reason !== null) event.reason = JSON.parse(r.reason) as OpEventReason;
            return { id: r.id, event };
        });
    }

    // -----------------------------------------------------------------------
    // ChangeSignalSource (optional inbound reactivity). External outbox monitor:
    // the app writes through its own db handle, so we detect the outbox
    // advancing either by watching the WAL file (default, file-backed db) or by
    // polling the monotonic AUTOINCREMENT outbox id (fallback: `:memory:`, no
    // path, or unreliable fs.watch). Both paths are epoch-gated so a disarm/
    // re-arm can never let a stale notification fire.
    // -----------------------------------------------------------------------

    addChangeListener(listener: ChangeSignalListener): void {
        this.changeListeners.add(listener);
        if (this.changeListeners.size === 1) this.armMonitor();
    }

    removeChangeListener(listener: ChangeSignalListener): void {
        this.changeListeners.delete(listener);
        if (this.changeListeners.size === 0) this.disarmMonitor();
    }

    private armMonitor(): void {
        this.ensureBookkeeping();
        if (this.walPath !== undefined) this.armWalWatch();
        else this.armPoll();
    }

    // Kernel-driven wake-up: fs.watch on the WAL file. Unlike the poll, the WAL
    // signal carries no id, so we fire on any notification (observers re-drain
    // authoritatively); a prime fire on arm wakes an observer for an
    // already-pending outbox promptly.
    private armWalWatch(): void {
        const epoch = ++this.monitorEpoch;
        const notify = (): void => {
            if (epoch !== this.monitorEpoch) return;   // disarmed / re-armed: stale
            if (!this.capture) return;
            for (const l of [...this.changeListeners]) l({});
        };
        this.walHandle = watchFile(this.walPath!, notify);
        notify();   // prime: wake an observer for an already-pending outbox promptly
    }

    private armPoll(): void {
        const epoch = ++this.monitorEpoch;
        this.lastOutboxId = 0;   // fire once for any already-pending rows

        const tick = (): void => {
            if (epoch !== this.monitorEpoch) return;   // disarmed / re-armed: stale
            if (!this.capture) return;
            let maxId = 0;
            try {
                maxId = (this.db.prepare('SELECT COALESCE(MAX(id), 0) AS m FROM rdb_outbox').get() as { m: number }).m;
            } catch {
                return;   // outbox not present yet
            }
            if (maxId > this.lastOutboxId) {
                this.lastOutboxId = maxId;
                for (const l of [...this.changeListeners]) l({});
            }
        };

        this.monitorTimer = setInterval(tick, this.pollMs);
        (this.monitorTimer as unknown as { unref?: () => void }).unref?.();
        tick();   // prime: wake an observer for an already-pending outbox promptly
    }

    private disarmMonitor(): void {
        this.monitorEpoch++;
        if (this.monitorTimer !== undefined) {
            clearInterval(this.monitorTimer);
            this.monitorTimer = undefined;
        }
        if (this.walHandle !== undefined) {
            this.walHandle.close();
            this.walHandle = undefined;
        }
    }

    // -----------------------------------------------------------------------
    // RowIdentityIndex (local id <-> content hash)
    // -----------------------------------------------------------------------

    async rowHashForLocalId(table: string, id: number): Promise<string | undefined> {
        this.ensureBookkeeping();
        const meta = this.loadMeta(table);
        const row = this.db.prepare(`SELECT "row_hash" AS rh FROM ${quoteId(meta.syncTable)} WHERE "id" = ?`)
            .get(id) as { rh: string } | undefined;
        return row?.rh;
    }

    async localIdForRowHash(table: string, rowHash: string): Promise<number | undefined> {
        this.ensureBookkeeping();
        const meta = this.loadMeta(table);
        const row = this.db.prepare(`SELECT "id" AS id FROM ${quoteId(meta.syncTable)} WHERE "row_hash" = ?`)
            .get(rowHash) as { id: number } | undefined;
        return row?.id;
    }

    // -----------------------------------------------------------------------
    // KeyIndex (rdb_keys: key hash + public key <-> numeric id)
    // -----------------------------------------------------------------------

    async registerKey(_domain: string, keyHash: string, publicKey: string): Promise<number> {
        this.ensureBookkeeping();
        return this.internKey(keyHash, publicKey);
    }

    async keyHashForId(_domain: string, id: number): Promise<string | undefined> {
        this.ensureBookkeeping();
        const row = this.db.prepare(
            `SELECT key_hash AS kh FROM ${quoteId(DEFAULT_KEY_TABLE)} WHERE id = ?`)
            .get(id) as { kh: string } | undefined;
        return row?.kh;
    }

    async publicKeyForId(_domain: string, id: number): Promise<string | undefined> {
        this.ensureBookkeeping();
        const row = this.db.prepare(
            `SELECT public_key AS pk FROM ${quoteId(DEFAULT_KEY_TABLE)} WHERE id = ?`)
            .get(id) as { pk: string | null } | undefined;
        return row?.pk ?? undefined;
    }

    async idForKeyHash(_domain: string, keyHash: string): Promise<number | undefined> {
        this.ensureBookkeeping();
        const row = this.db.prepare(
            `SELECT id FROM ${quoteId(DEFAULT_KEY_TABLE)} WHERE key_hash = ?`)
            .get(keyHash) as { id: number } | undefined;
        return row?.id;
    }
}
