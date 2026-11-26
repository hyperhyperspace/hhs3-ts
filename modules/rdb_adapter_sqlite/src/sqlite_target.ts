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
//   - rdb_index_spec(...): the single INSTALLED projection index spec (written
//     by installIndexSpec) and rdb_index_meta(...): one row per index this
//     target materialized, with its resolved definition. The core diffs
//     against these; physical indexes are named `<table>__<name>`.
//
// Row identity: the content-addressed rowId maps to the projection-local serial
// `id` through the per-table sync table (`<table>_sync`, keyed by `row_hash`).
// The sync row is allocated on first sight of a rowId and KEPT across delete, so
// a void-flip reinstatement reuses the same `id` (stable local identity).
//
// No SQL foreign keys: a local FK column projects as an integer `<col>_id`
// holding the referenced row's `id`, and authors and key columns as integer ids
// into rdb_keys, but no table declares a FOREIGN KEY. rdb's references are
// checked by the operation that makes them, at its own position, so a live row
// can reference a row deleted later, and a sync row outlives its app row; an
// enforced SQL foreign key would refuse writes rdb accepts, and better-sqlite3
// enforces them on every connection it opens.
//
// Indexes: only indexes recorded in rdb_index_meta (declared in the installed
// spec) are dropped ahead of a remote drop-column. An index the app created by
// hand on a projected column makes that `ALTER TABLE ... DROP COLUMN` fail, so
// the whole apply() rolls back and the group's sync stalls until the index is
// removed. (Table rebuilds preserve hand-made indexes, skipping any on the
// dropped column.) Declare indexes in the spec rather than creating them.
//
// Atomicity: apply() runs schema actions THEN row actions THEN any NOT NULL
// tighten rebuilds THEN deferred index ensures (built after the data is
// loaded) THEN the checkpoint commit inside ONE better-sqlite3 transaction; a
// throw rolls the whole batch back, so the target never claims a checkpoint
// it does not reflect.

import { json } from "@hyper-hyper-space/hhs3_json";
import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { likeMatch, type ColumnDef, type ColumnType } from "@hyper-hyper-space/hhs3_rdb";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import { watchFile, FileWatchHandle } from "@hyper-hyper-space/hhs3_file_watch";
import Database from "better-sqlite3";

import {
    BoundWhereExpr, BoundOperand, CapturedBatch, CapturedChange, OrderBySpec, ProjectedRow,
    QueryableTarget, ReactiveChangeTarget, SingleTableQueryPlan,
    TargetCommitEvent, TargetCommitListener, TargetRowChange,
    CheckpointMovedError, PendingChangesError, DEFAULT_KEY_TABLE, IndexDecl, IndexSpec, IndexState, IndexTarget, IngestSettle,
    KeyIndex, MaterializationTarget, MaterializedChangeSource, OpEvent, OpEventQuery, OpEventReason,
    ResolvedIndex, resolveOpEventQuery, RowAction, RowIdentityIndex, SchemaAction, StoredOpEvent, SyncMapping,
    versionsEqual, WriterRecord, WriterRecordStore,
    CapturedChangeId, ChangeRecord, mergeChangeRecord, RowRevert, opEventRows, withLocalIds, type OpEventWrite,
    checkWritable, checkWrite, memberTableNames, recordedWriterKeyId, WriteRefusedError,
    type CheckedWrite, type RowValues as WriteValues, type RowWriter, type TableNameIndex, type TableShape,
    type WritableTarget, type WriteContext,
} from "@hyper-hyper-space/hhs3_rdb_adapter";
import { normalizeOrder } from "@hyper-hyper-space/hhs3_rdb_adapter";

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
    // The group the table was created for, its name in that group's schema,
    // and whether the group has no identity provider (HIP-20). Capture fills
    // in the author only in a table whose group has one.
    groupId?: string;
    schemaName: string;
    anonymous: boolean;
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

// A writer's parameter for a column: NULL, or the stored form of its value.
// The author column, which column types leave out, holds a key id.
function writeParam(meta: TableMeta, column: string, value: json.Literal | null): number | string | null {
    return value === null ? null : toParam(value, meta.columnTypes[column] ?? 'integer');
}

// An outbox row as stored, for the settle's reverts.
type RawOutboxRow = { tbl: string; local_id: number; op: string; changed: string | null };

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

// The ` CHECK (...)` fragment that keeps a native write to values rdb settles
// the same way, or '' when the type needs none. A boolean column holds 0 or 1:
// capture reads any other value as true, while the row keeps it. A json column
// holds json text other than `null`: capture parses the text, and the text
// `null` would reach rdb as SQL NULL. SQL NULL passes both, since each check is
// then unknown. A column constraint names only its column, so it goes when the
// column is dropped.
function checkFragment(name: string, type: ColumnType | undefined): string {
    if (type === 'boolean') return ` CHECK (${quoteId(name)} IN (0, 1))`;
    if (type === 'json') return ` CHECK (json_valid(${quoteId(name)}) AND json_type(${quoteId(name)}) != 'null')`;
    return '';
}

// A business-column declaration: `"name" TYPE [NOT NULL] [DEFAULT x] [CHECK (...)]`.
// SQLite rejects ALTER TABLE ADD COLUMN ... NOT NULL with no non-NULL default
// on a non-empty table. addColumn relaxes those adds to nullable and
// applyTightens restores NOT NULL after the row backfill (same transaction).
function columnDecl(name: string, def: ColumnDef): string {
    const nn = def.nullable ? '' : ' NOT NULL';
    return `${quoteId(name)} ${sqliteType(def.type)}${nn}${defaultFragment(def)}${checkFragment(name, def.type)}`;
}

// ---------------------------------------------------------------------------
// Index options: the SQLite format for the opaque IndexDecl `options`,
// validated strictly (any other key, at either level, is refused):
//   { columns?: { [rdbColumn]: { desc?: boolean,
//                                collate?: 'NOCASE' | 'RTRIM' | 'BINARY',
//                                whereNotNull?: boolean } } }
// Only columns the index lists may be named: the core drops an index before
// one of its listed columns dies, and SQLite refuses to drop a column an index
// still uses. `whereNotNull` columns form a partial index (ANDed).
// ---------------------------------------------------------------------------

type SqliteColumnOptions = { desc?: boolean; collate?: 'NOCASE' | 'RTRIM' | 'BINARY'; whereNotNull?: boolean };
type SqliteIndexOptions = { columns?: { [rdbColumn: string]: SqliteColumnOptions } };

const COLLATIONS = new Set(['NOCASE', 'RTRIM', 'BINARY']);
const COLUMN_OPTION_KEYS = new Set(['desc', 'collate', 'whereNotNull']);

function isPlainObject(v: json.Literal | undefined): v is json.LiteralMap {
    return typeof v === 'object' && v !== null && !Array.isArray(v);
}

// A reason when `options` is not valid for an index over `listed` columns.
function sqliteIndexOptionsProblem(options: json.Literal | undefined, listed: Set<string>): string | undefined {
    if (options === undefined) return undefined;
    if (!isPlainObject(options)) return 'sqlite index options must be an object';
    for (const key of Object.keys(options)) {
        if (key !== 'columns') return `unknown sqlite index option '${key}'`;
    }
    const columns = options.columns;
    if (columns === undefined) return undefined;
    if (!isPlainObject(columns)) return "'columns' must map column names to their options";
    for (const [column, colOpts] of Object.entries(columns)) {
        if (!listed.has(column)) return `'columns' names '${column}', which the index does not list`;
        if (!isPlainObject(colOpts)) return `the options for '${column}' must be an object`;
        for (const key of Object.keys(colOpts)) {
            if (!COLUMN_OPTION_KEYS.has(key)) return `unknown sqlite column option '${key}' on '${column}'`;
        }
        for (const flag of ['desc', 'whereNotNull'] as const) {
            if (colOpts[flag] !== undefined && typeof colOpts[flag] !== 'boolean') {
                return `'${flag}' on '${column}' must be true or false`;
            }
        }
        const collation = colOpts.collate;
        if (collation !== undefined && (typeof collation !== 'string' || !COLLATIONS.has(collation))) {
            return `unknown collation '${String(collation)}' for '${column}'`;
        }
    }
    return undefined;
}

function physicalIndexName(table: string, name: string): string {
    return `${table}__${name}`;
}

// CREATE INDEX for a resolved index. Plain CREATE INDEX (no IF NOT EXISTS): a
// name collision with an index we do not own must fail, not be adopted.
function createIndexSql(index: ResolvedIndex): string {
    const problem = sqliteIndexOptionsProblem(index.options, new Set(index.columns.map((c) => c.rdb)));
    if (problem !== undefined) throw new Error(`index '${index.name}' on '${index.table}': ${problem}`);
    const perColumn = ((index.options ?? {}) as SqliteIndexOptions).columns ?? {};
    const cols = index.columns.map((c) => {
        const o = perColumn[c.rdb];
        return quoteId(c.target) + (o?.collate !== undefined ? ` COLLATE ${o.collate}` : '') + (o?.desc === true ? ' DESC' : '');
    });
    let sql = `CREATE INDEX ${quoteId(physicalIndexName(index.table, index.name))} `
        + `ON ${quoteId(index.table)} (${cols.join(', ')})`;
    const notNull = index.columns.filter((c) => perColumn[c.rdb]?.whereNotNull === true);
    if (notNull.length > 0) {
        sql += ' WHERE ' + notNull.map((c) => `${quoteId(c.target)} IS NOT NULL`).join(' AND ');
    }
    return sql;
}

// ---------------------------------------------------------------------------

// Convert a stored SQLite value back to its logical rdb value, inverting
// toParam: booleans from 0/1, json from canonical text, everything else as-is.
function toLogical(value: unknown, type: ColumnType): json.Literal | undefined {
    if (value === null || value === undefined) return undefined;
    if (type === 'boolean') return value !== 0;
    if (type === 'json') return JSON.parse(value as string);
    return value as json.Literal;
}

// toLogical that says why a value can't be read instead of throwing. Text that
// isn't json in a json column can't be: the column's check refuses it, but a
// table can still hold it with SQLite's checks switched off.
type LogicalRead = { ok: true; value: json.Literal | undefined } | { ok: false; reason: string };
function readLogical(value: unknown, type: ColumnType): LogicalRead {
    if (type === 'json' && typeof value === 'string') {
        try {
            return { ok: true, value: JSON.parse(value) as json.Literal };
        } catch {
            const shown = value.length > 40 ? value.slice(0, 40) + '...' : value;
            return { ok: false, reason: `its text is not json: ${JSON.stringify(shown)}` };
        }
    }
    return { ok: true, value: toLogical(value, type) };
}

export class SqliteTarget implements MaterializationTarget, MaterializedChangeSource,
    ReactiveChangeTarget, QueryableTarget, RowIdentityIndex, KeyIndex, IndexTarget, WriterRecordStore,
    WritableTarget, TableNameIndex {
    private db: Database.Database;
    private metaCache = new Map<string, TableMeta>();
    // The schema version the cache was read at (see freshMeta).
    private metaVersion: number | undefined;
    // The writer's PRAGMA table_info, per table, at the schema version it was read at.
    private columnInfoCache = new Map<string, { version: number | undefined; columns: Map<string, { notNull: boolean; hasDefault: boolean }> }>();
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
    // The WAL is a hint: we only fire when rdb_outbox's MAX(id) grew (same
    // filter as the poll). Adapter writes (apply checkpoint, applying flags)
    // dirty the WAL without advancing the outbox; signalling those would
    // schedule idle projection cycles. The signal is opaque (ChangeSignal = {}):
    // observers re-drain authoritatively.
    // Lazily armed on the first listener, disarmed (epoch-bumped) on the last.
    private readonly pollMs: number;
    private readonly walPath: string | undefined;
    private commitListeners = new Set<TargetCommitListener>();
    private emittedOutboxIds = new Set<number>();
    private watchPending = false;
    private watchAgain = false;
    private pendingApply: TargetRowChange[] | undefined;
    private pendingEvents: StoredOpEvent[] | undefined;
    private monitorTimer: ReturnType<typeof setInterval> | undefined;
    private walHandle: FileWatchHandle | undefined;
    private monitorEpoch = 0;
    private likeReady = false;
    // Resolves once the armed change monitor is guaranteed live: for the WAL
    // watch, once the kernel file watcher is registered (see FileWatchHandle.
    // ready); for the poll, immediately (the interval is live on arm). Undefined
    // while disarmed. Awaited by observers/tests that need a write's wake to be
    // deterministic rather than racing kernel registration.
    private monitorReady: Promise<void> | undefined;

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
    private closed = false;

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
        return this.readCheckpoint(groupId);
    }

    // Release this connection: disarm the WAL/poll monitor and close the
    // better-sqlite3 handle (checkpoints and drops -wal/-shm when this is
    // the last connection). Idempotent. RdbProjection.stop() calls this.
    close(): void {
        if (this.closed) return;
        this.closed = true;
        this.disarmMonitor();
        try { this.db.pragma('wal_checkpoint(TRUNCATE)'); } catch { /* already closed */ }
        try { this.db.close(); } catch { /* already closed by the caller */ }
    }

    private ensureOpen(): void {
        if (this.closed) throw new Error('SqliteTarget is closed');
    }

    // Synchronous checkpoint read (assumes bookkeeping is ready). Used inside the
    // apply() transaction for the compare-and-set guard.
    private readCheckpoint(groupId: B64Hash): Version | undefined {
        const row = this.db.prepare('SELECT version FROM rdb_checkpoint WHERE group_id = ?').get(groupId) as
            { version: string | null } | undefined;
        if (row === undefined || row.version === null) return undefined;
        return new Set(JSON.parse(row.version) as string[]);
    }

    async apply(
        groupId: B64Hash, schemaActions: SchemaAction[], rowActions: RowAction[],
        checkpoint: Version, events?: OpEvent[], expectFrom?: Version | null,
        expectIndexSpec?: string | null,
    ): Promise<void> {
        this.ensureBookkeeping();
        // CAS on a read so a stale/null expectFrom still throws without opening a
        // write transaction. An empty batch already at `checkpoint` is a no-op:
        // rewriting the same row + toggling applying would dirty the WAL and
        // wake the change monitor. A catch-up that advances the stored version
        // with empty actions still persists (below).
        const stored = this.readCheckpoint(groupId);
        if (expectFrom !== undefined && !versionsEqual(stored, expectFrom ?? undefined)) {
            throw new CheckpointMovedError(groupId);
        }
        if (expectIndexSpec !== undefined && (this.readIndexSpecFingerprint() ?? null) !== expectIndexSpec) {
            throw new CheckpointMovedError(groupId);
        }
        // No projection over a pending change (HIP-29), checked again inside
        // the transaction, where no write can land between the check and the apply.
        if (this.outboxHasRows()) throw new PendingChangesError(groupId);
        const empty = schemaActions.length === 0 && rowActions.length === 0
            && (events === undefined || events.length === 0);
        if (empty && versionsEqual(stored, checkpoint)) return;

        const run = this.db.transaction(() => {
            this.freshMeta();
            // Compare-and-set guard (concurrent projectors). BEGIN IMMEDIATE took
            // the write lock, so this read of the stored checkpoint cannot race a
            // committing writer: exactly one projector wins. On mismatch throw so
            // better-sqlite3 rolls the whole transaction back (nothing applied).
            if (expectFrom !== undefined && !versionsEqual(this.readCheckpoint(groupId), expectFrom ?? undefined)) {
                throw new CheckpointMovedError(groupId);
            }
            if (expectIndexSpec !== undefined && (this.readIndexSpecFingerprint() ?? null) !== expectIndexSpec) {
                throw new CheckpointMovedError(groupId);
            }
            if (this.outboxHasRows()) throw new PendingChangesError(groupId);
            // Echo suppression: mark the whole batch as adapter-authored so the
            // capture triggers no-op on our own materialization writes.
            const tighten: { table: string; column: string }[] = [];
            const ensures: ResolvedIndex[] = [];
            if (this.capture) this.setApplying(true);
            for (const action of schemaActions) this.applySchemaAction(action, tighten, ensures, groupId);
            for (const action of rowActions) this.applyRowAction(action);
            this.applyTightens(tighten);
            // Indexes are built last: over the loaded rows (faster than
            // maintaining them per insert) and after any tighten rebuild.
            for (const index of ensures) this.createIndex(index);
            this.persistCheckpoint(groupId, checkpoint);
            // Concurrency void/reinstate flips ride the same checkpoint advance.
            const logged: StoredOpEvent[] = [];
            if (events !== undefined) for (const e of events) {
                const stored = this.logOpEvent(e);
                if (stored !== undefined) logged.push(stored);
            }
            this.pendingEvents = logged;
            if (this.capture) this.setApplying(false);
            // The cache now holds this transaction's own schema changes.
            this.metaVersion = this.schemaVersion();
        });
        // BEGIN IMMEDIATE: this transaction reads (the checkpoint CAS compare)
        // and then writes, so it must take the write lock up front. A deferred
        // transaction would take a read snapshot and fail with SQLITE_BUSY_SNAPSHOT
        // when it later tried to upgrade while another connection had written.
        this.pendingApply = [];
        try {
            run.immediate();
        } catch (err) {
            this.pendingApply = undefined;
            this.pendingEvents = undefined;
            // The rollback undid the schema changes, but not the cache edits
            // made with them; the schema version is back where it was, so the
            // version check alone wouldn't notice.
            this.metaCache.clear();
            this.metaVersion = undefined;
            throw err;
        }
        const changes = this.pendingApply;
        const logged = this.pendingEvents ?? [];
        this.pendingApply = undefined;
        this.pendingEvents = undefined;
        const schemaTables = [...new Set(schemaActions.map((action) =>
            action.kind === 'ensure-index' ? action.index.table : action.table))];
        if (changes.length > 0 || logged.length > 0 || schemaTables.length > 0) {
            const commit: TargetCommitEvent = { source: 'apply', groupId, schemaTables, changes, events: logged };
            for (const listener of [...this.commitListeners]) listener(commit);
        }
    }

    // -----------------------------------------------------------------------
    // Bookkeeping + checkpoint
    // -----------------------------------------------------------------------

    private ensureBookkeeping(): void {
        this.ensureOpen();
        if (this.bookkeepingReady) return;
        // The projection declares no foreign keys, but the app may, on tables
        // of its own that reference projected ones: a table rebuild drops and
        // renames a projected table, which an enforced reference would refuse.
        // better-sqlite3 enables foreign_keys by default, so force it off here.
        this.db.pragma('foreign_keys = OFF');
        // When we detect local edits by watching the WAL file, the db must
        // actually be in WAL journal mode (better-sqlite3 defaults to a rollback
        // journal, which never creates a `-wal` file). Enabling it here keeps the
        // caller from having to know about the change-signal strategy.
        if (this.walPath !== undefined) this.db.pragma('journal_mode = WAL');
        this.db.exec(
            // `end_observes`: the observes an end of a pass recorded beside the
            // checkpoint, a json array, cleared by every settle (HIP-29). A
            // group with a list and no checkpoint yet has a NULL version.
            'CREATE TABLE IF NOT EXISTS rdb_checkpoint (group_id TEXT PRIMARY KEY, version TEXT, end_observes TEXT);'
            + 'CREATE TABLE IF NOT EXISTS rdb_table_meta ('
            + '"table" TEXT PRIMARY KEY, id_column TEXT NOT NULL, author_column TEXT, '
            + "sync_table TEXT NOT NULL, column_types TEXT NOT NULL, fk_columns TEXT NOT NULL DEFAULT '{}', "
            + "key_ref_columns TEXT NOT NULL DEFAULT '[]', group_id TEXT, schema_name TEXT NOT NULL, anonymous INTEGER NOT NULL);"
            // The writer record (HIP-20): one row, which the projection replaces.
            // `author` is the projection's author setting, `auto` or `manual`.
            + 'CREATE TABLE IF NOT EXISTS rdb_writer_record ('
            + 'id INTEGER PRIMARY KEY CHECK (id = 1), writer_key_id INTEGER, hash_suite TEXT NOT NULL, author TEXT NOT NULL);'
            + `CREATE TABLE IF NOT EXISTS ${quoteId(DEFAULT_KEY_TABLE)} (`
            + 'id INTEGER PRIMARY KEY, key_hash TEXT NOT NULL UNIQUE, public_key TEXT);'
            // The durable op-event log (ingestion failures + concurrency flips).
            // Present on ANY target: concurrency void/reinstate flips are logged
            // by apply() even on a read-only projection. Append-only; idempotent
            // by (op_hash, direction).
            + 'CREATE TABLE IF NOT EXISTS rdb_op_events ('
            + 'id INTEGER PRIMARY KEY AUTOINCREMENT, origin TEXT NOT NULL, direction TEXT NOT NULL, '
            + 'group_id TEXT NOT NULL, op_hash TEXT NOT NULL, op_json TEXT, kind TEXT NOT NULL, '
            + '"table" TEXT, row_hash TEXT, local_id INTEGER, writes TEXT, author TEXT, reason TEXT, '
            + "created_at TEXT NOT NULL DEFAULT (datetime('now')), UNIQUE (op_hash, direction));"
            // Projection-local indexes: the installed spec (one row) and the
            // indexes actually materialized (resolved definition as JSON).
            + 'CREATE TABLE IF NOT EXISTS rdb_index_spec ('
            + 'id INTEGER PRIMARY KEY CHECK (id = 1), spec TEXT NOT NULL, '
            + 'fingerprint TEXT NOT NULL);'
            + 'CREATE TABLE IF NOT EXISTS rdb_index_meta ('
            + '"table" TEXT NOT NULL, name TEXT NOT NULL, group_id TEXT NOT NULL, def TEXT NOT NULL, '
            + 'fingerprint TEXT NOT NULL, PRIMARY KEY ("table", name));');

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

    // Append one op-event (idempotent by (op_hash, direction)), with the local
    // id of each row it names by hash. Called inside the transaction, after
    // its row writes.
    private logOpEvent(logged: OpEvent): StoredOpEvent | undefined {
        const event = withLocalIds(logged, (table, rowId) => this.localIdIn(table, rowId));
        const result = this.db.prepare(
            'INSERT OR IGNORE INTO rdb_op_events '
            + '(origin, direction, group_id, op_hash, op_json, kind, "table", row_hash, local_id, writes, author, reason) '
            + 'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)')
            .run(
                event.origin, event.direction, event.groupId, event.opHash,
                event.op === undefined ? null : json.toStringNormalized(event.op),
                event.kind, event.table ?? null, event.rowId ?? null,
                event.localId ?? null, event.writes === undefined ? null : JSON.stringify(event.writes),
                event.author ?? null,
                event.reason === undefined ? null : JSON.stringify(event.reason));
        if (result.changes === 0) return undefined;
        return { id: Number(result.lastInsertRowid), event };
    }

    // The local id a row has in a projected table, or undefined when the table
    // isn't projected here or its sync table has no row for the hash.
    private localIdIn(table: string, rowId: B64Hash): number | undefined {
        const meta = this.db.prepare('SELECT sync_table FROM rdb_table_meta WHERE "table" = ?').get(table) as
            { sync_table: string } | undefined;
        return meta === undefined ? undefined : this.syncIdOf(meta.sync_table, rowId);
    }

    // Provision the capture infrastructure (idempotent). The config row holds
    // both level-2 flags: `enabled` (record?) and `applying` (adapter write in
    // progress? -> suppress).
    private ensureCaptureInfra(): void {
        this.db.exec(
            'CREATE TABLE IF NOT EXISTS rdb_capture_config (enabled INTEGER NOT NULL, applying INTEGER NOT NULL);'
            // `open`: the update row a statement is filling, one column trigger at
            // a time (HIP-29). `record`: what ingestion recorded of the change.
            + 'CREATE TABLE IF NOT EXISTS rdb_outbox ('
            + 'id INTEGER PRIMARY KEY AUTOINCREMENT, "table" TEXT NOT NULL, '
            + 'local_id INTEGER NOT NULL, op TEXT NOT NULL, changed TEXT, '
            + 'open INTEGER NOT NULL DEFAULT 0, record TEXT);'
            + 'CREATE INDEX IF NOT EXISTS rdb_outbox_open ON rdb_outbox ("table", local_id) WHERE open = 1;');
        const has = this.db.prepare('SELECT 1 FROM rdb_capture_config LIMIT 1').get() !== undefined;
        if (!has) this.db.prepare('INSERT INTO rdb_capture_config (enabled, applying) VALUES (1, 0)').run();
    }

    private setApplying(on: boolean): void {
        this.db.prepare('UPDATE rdb_capture_config SET applying = ?').run(on ? 1 : 0);
    }

    // Install (or reinstall) the capture triggers for a table from its current
    // columns (HIP-29): an AFTER INSERT, an AFTER DELETE, and for updates a
    // BEFORE UPDATE that closes the row's open outbox row and one AFTER UPDATE
    // OF <col> per business column, which opens this statement's row when none
    // is open and adds its column. So an UPDATE is one outbox row per updated
    // row, with every column the statement names, changed or not: only UPDATE
    // OF triggers see which columns a statement names. Every capture body is
    // gated on enabled=1 AND applying=0. A BEFORE UPDATE guard, gated on
    // applying=0 only, fails a change of the id.
    private installTriggers(table: string, meta: TableMeta): void {
        // Drop any prior capture triggers for this table (column set may have changed).
        this.dropTriggers(table);

        const gate = '(SELECT enabled FROM rdb_capture_config) = 1 AND (SELECT applying FROM rdb_capture_config) = 0';
        // The business columns and the author column: ingestion compares an
        // insert's author with the writer key, and refuses a change of it.
        const cols = [...Object.keys(meta.columnTypes), ...(meta.authorColumn !== undefined ? [meta.authorColumn] : [])];
        const tableLit = quoteText(table);
        const id = quoteId(meta.idColumn);

        const jsonObject = (ref: 'NEW' | 'OLD'): string => {
            if (cols.length === 0) return "json_object()";
            const pairs = cols.map((c) => `${quoteText(c)}, ${ref}.${quoteId(c)}`).join(', ');
            return `json_object(${pairs})`;
        };

        if (meta.authorColumn !== undefined && !meta.anonymous) {
            // An empty author column gets the writer key's id where the writer
            // record says `auto`, in a table whose group has an identity
            // provider. A BEFORE INSERT trigger can't change the row, so this one
            // writes it, with capture off for that write as `applying` does, then
            // records the stored row.
            const author = quoteId(meta.authorColumn);
            const record = 'SELECT writer_key_id FROM rdb_writer_record WHERE id = 1';
            const setting = 'SELECT author FROM rdb_writer_record WHERE id = 1';
            const stored = (): string => {
                if (cols.length === 0) return "json_object()";
                return `json_object(${cols.map((c) => `${quoteText(c)}, r.${quoteId(c)}`).join(', ')})`;
            };
            this.db.exec(
                `CREATE TRIGGER ${quoteId('rdb_cap_ins_' + table)} AFTER INSERT ON ${quoteId(table)} WHEN ${gate} BEGIN `
                + `UPDATE rdb_capture_config SET applying = 1; `
                + `UPDATE ${quoteId(table)} SET ${author} = (${record}) `
                + `WHERE ${id} = NEW.${id} AND ${author} IS NULL AND (${record}) IS NOT NULL AND (${setting}) = 'auto'; `
                + `UPDATE rdb_capture_config SET applying = 0; `
                + `INSERT INTO rdb_outbox ("table", local_id, op, changed) `
                + `SELECT ${tableLit}, NEW.${id}, 'insert', ${stored()} FROM ${quoteId(table)} AS r WHERE r.${id} = NEW.${id}; END`);
        } else {
            this.db.exec(
                `CREATE TRIGGER ${quoteId('rdb_cap_ins_' + table)} AFTER INSERT ON ${quoteId(table)} BEGIN `
                + `INSERT INTO rdb_outbox ("table", local_id, op, changed) `
                + `SELECT ${tableLit}, NEW.${id}, 'insert', ${jsonObject('NEW')} WHERE ${gate}; END`);
        }

        this.db.exec(
            `CREATE TRIGGER ${quoteId('rdb_cap_del_' + table)} AFTER DELETE ON ${quoteId(table)} BEGIN `
            + `INSERT INTO rdb_outbox ("table", local_id, op, changed) `
            + `SELECT ${tableLit}, OLD.${id}, 'delete', NULL WHERE ${gate}; END`);

        // An update of a row is one outbox row: the BEFORE UPDATE closes what an
        // earlier statement left open, and each named column's trigger adds its
        // column to the open row, opening it first when none is.
        const openRow = `"table" = ${tableLit} AND local_id = NEW.${id} AND open = 1`;
        this.db.exec(
            `CREATE TRIGGER ${quoteId('rdb_cap_updopen_' + table)} BEFORE UPDATE ON ${quoteId(table)} WHEN ${gate} BEGIN `
            + `UPDATE rdb_outbox SET open = 0 WHERE "table" = ${tableLit} AND local_id = OLD.${id} AND open = 1; END`);
        cols.forEach((col, i) => {
            const path = quoteText('$.' + JSON.stringify(col));
            this.db.exec(
                `CREATE TRIGGER ${quoteId('rdb_cap_upd_' + table + '_' + i)} `
                + `AFTER UPDATE OF ${quoteId(col)} ON ${quoteId(table)} WHEN ${gate} BEGIN `
                + `UPDATE rdb_outbox SET changed = json_set(changed, ${path}, NEW.${quoteId(col)}) WHERE ${openRow}; `
                + `INSERT INTO rdb_outbox ("table", local_id, op, changed, open) `
                + `SELECT ${tableLit}, NEW.${id}, 'update', json_object(${quoteText(col)}, NEW.${quoteId(col)}), 1 `
                + `WHERE NOT EXISTS (SELECT 1 FROM rdb_outbox WHERE ${openRow}); END`);
        });

        // A native statement may not change a row's id: the row would sit under
        // an id with no rdb identity, and project-back would insert it again
        // under the old one. The trigger names no column (an UPDATE OF <id>
        // trigger misses `SET rowid = ...`), and fails the statement whether or
        // not capture is on. Only the adapter's own writes pass, and they never
        // change an id.
        this.db.exec(
            `CREATE TRIGGER ${quoteId('rdb_cap_pk_' + table)} BEFORE UPDATE ON ${quoteId(table)} `
            + `WHEN NEW.${id} IS NOT OLD.${id} AND (SELECT applying FROM rdb_capture_config) = 0 BEGIN `
            + `SELECT RAISE(ABORT, ${quoteText(`can't change the id of a row of '${table}': it is the row's local id in the projection`)}); END`);
    }

    // Remove a table's capture triggers. They name the table's columns, and
    // SQLite refuses to drop a column while a trigger names it, so a column
    // drop removes them first and installs them again from the new columns.
    private dropTriggers(table: string): void {
        const existing = this.db.prepare(
            "SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name = ? AND name LIKE 'rdb_cap_%'")
            .all(table) as { name: string }[];
        for (const t of existing) this.db.exec(`DROP TRIGGER ${quoteId(t.name)}`);
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

    // Keeps the end's observes recorded beside the checkpoint: only a settle clears them.
    private persistCheckpoint(groupId: B64Hash, checkpoint: Version): void {
        this.db.prepare('INSERT INTO rdb_checkpoint (group_id, version) VALUES (?, ?) '
            + 'ON CONFLICT(group_id) DO UPDATE SET version = excluded.version')
            .run(groupId, JSON.stringify([...checkpoint].sort()));
    }

    private writeMeta(table: string, meta: TableMeta): void {
        this.db.prepare(
            'INSERT OR REPLACE INTO rdb_table_meta '
            + '("table", id_column, author_column, sync_table, column_types, fk_columns, key_ref_columns, group_id, schema_name, anonymous) '
            + 'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)')
            .run(table, meta.idColumn, meta.authorColumn ?? null, meta.syncTable,
                JSON.stringify(meta.columnTypes), JSON.stringify(meta.fkColumns),
                JSON.stringify(meta.keyRefColumns), meta.groupId ?? null, meta.schemaName, meta.anonymous ? 1 : 0);
        this.metaCache.set(table, meta);
    }

    // The meta cache holds what this handle read from rdb_table_meta. Another
    // handle on the same file can change a table, as the host does while a client
    // reads, so the cache is kept only while the schema version it was read at
    // holds: every DDL statement bumps PRAGMA schema_version. Each entry point
    // that reads or writes with column types calls this first, inside its
    // transaction, so the version and the reads that follow agree.
    private freshMeta(): void {
        const version = this.schemaVersion();
        if (version !== this.metaVersion) {
            this.metaCache.clear();
            this.metaVersion = version;
        }
    }

    private schemaVersion(): number {
        return this.db.pragma('schema_version', { simple: true }) as number;
    }

    private loadMeta(table: string): TableMeta {
        const cached = this.metaCache.get(table);
        if (cached !== undefined) return cached;

        const row = this.db.prepare(
            'SELECT id_column, author_column, sync_table, column_types, fk_columns, key_ref_columns, group_id, schema_name, anonymous '
            + 'FROM rdb_table_meta WHERE "table" = ?')
            .get(table) as
            { id_column: string; author_column: string | null; sync_table: string;
                column_types: string; fk_columns: string | null; key_ref_columns: string | null;
                group_id: string | null; schema_name: string; anonymous: number }
            | undefined;
        if (row === undefined) throw new Error(`no materialized metadata for table '${table}'`);

        const meta: TableMeta = {
            idColumn: row.id_column,
            authorColumn: row.author_column ?? undefined,
            syncTable: row.sync_table,
            columnTypes: JSON.parse(row.column_types) as { [column: string]: ColumnType },
            fkColumns: JSON.parse(row.fk_columns ?? '{}') as { [column: string]: { targetTable: string } },
            keyRefColumns: JSON.parse(row.key_ref_columns ?? '[]') as string[],
            ...(row.group_id !== null ? { groupId: row.group_id } : {}),
            schemaName: row.schema_name,
            anonymous: row.anonymous === 1,
        };
        this.metaCache.set(table, meta);
        return meta;
    }

    // -----------------------------------------------------------------------
    // Schema channel
    // -----------------------------------------------------------------------

    private applySchemaAction(
        action: SchemaAction, tighten: { table: string; column: string }[], ensures: ResolvedIndex[], groupId?: B64Hash,
    ): void {
        switch (action.kind) {
            case 'create-table': return this.createTable(action, groupId);
            case 'drop-table': return this.dropTable(action);
            case 'add-column': return this.addColumn(action, tighten);
            case 'drop-column': return this.dropColumn(action);
            case 'drop-index': return this.dropIndex(action.table, action.name);
            case 'ensure-index': ensures.push(action.index); return;
        }
    }

    // -----------------------------------------------------------------------
    // Projection-local indexes
    // -----------------------------------------------------------------------

    private readIndexSpecFingerprint(): string | undefined {
        const row = this.db.prepare('SELECT fingerprint FROM rdb_index_spec WHERE id = 1').get() as
            { fingerprint: string } | undefined;
        return row?.fingerprint;
    }

    private createIndex(index: ResolvedIndex): void {
        this.db.exec(createIndexSql(index));
        this.db.prepare(
            'INSERT INTO rdb_index_meta ("table", name, group_id, def, fingerprint) VALUES (?, ?, ?, ?, ?)')
            .run(index.table, index.name, index.groupId, JSON.stringify(index), index.fingerprint);
    }

    // IF EXISTS: a DROP TABLE (or a rebuild skipping it) may already have
    // removed the physical index; the record goes either way.
    private dropIndex(table: string, name: string): void {
        this.db.exec(`DROP INDEX IF EXISTS ${quoteId(physicalIndexName(table, name))}`);
        this.db.prepare('DELETE FROM rdb_index_meta WHERE "table" = ? AND name = ?').run(table, name);
    }

    async getIndexState(): Promise<IndexState> {
        this.ensureBookkeeping();
        const rows = this.db.prepare('SELECT def FROM rdb_index_meta ORDER BY "table", name').all() as
            { def: string }[];
        const state: IndexState = { materialized: rows.map((r) => JSON.parse(r.def) as ResolvedIndex) };
        const spec = this.db.prepare('SELECT spec, fingerprint FROM rdb_index_spec WHERE id = 1').get() as
            { spec: string; fingerprint: string } | undefined;
        if (spec !== undefined) {
            state.spec = JSON.parse(spec.spec) as IndexSpec;
            state.specFingerprint = spec.fingerprint;
        }
        return state;
    }

    validateIndexOptions(decl: IndexDecl): string | undefined {
        return sqliteIndexOptionsProblem(decl.options, new Set(decl.columns));
    }

    async installIndexSpec(
        spec: IndexSpec, specFingerprint: string, actions: SchemaAction[],
        expect: { specFingerprint: string | undefined; checkpoints: Map<B64Hash, Version> },
    ): Promise<void> {
        this.ensureBookkeeping();
        const run = this.db.transaction(() => {
            if (this.readIndexSpecFingerprint() !== expect.specFingerprint) {
                throw new CheckpointMovedError([...expect.checkpoints.keys()][0] ?? '');
            }
            for (const [groupId, cp] of expect.checkpoints) {
                if (!versionsEqual(this.readCheckpoint(groupId), cp)) throw new CheckpointMovedError(groupId);
            }
            const ensures: ResolvedIndex[] = [];
            for (const action of actions) {
                if (action.kind === 'drop-index') this.dropIndex(action.table, action.name);
                else if (action.kind === 'ensure-index') ensures.push(action.index);
                else throw new Error(`installIndexSpec accepts only index actions, got '${action.kind}'`);
            }
            for (const index of ensures) this.createIndex(index);
            this.db.prepare(
                'INSERT OR REPLACE INTO rdb_index_spec (id, spec, fingerprint) VALUES (1, ?, ?)')
                .run(JSON.stringify(spec), specFingerprint);
        });
        run.immediate();
    }

    private createTable(action: Extract<SchemaAction, { kind: 'create-table' }>, groupId?: B64Hash): void {
        // The app table OWNS the serial `id` (INTEGER PRIMARY KEY AUTOINCREMENT).
        // Explicit ids (allocated via the sync table) still work - SQLite raises
        // sqlite_sequence to any larger explicit rowid, so a void-flip
        // reinstatement reuses the same id AND an app-side (e.g. CLI) insert with
        // no id NEVER reuses an id freed by a delete/revert. That monotonicity is
        // the contract "a local id is never reused" the rdb_adapter relies on; a
        // reused id would let a stale insert image collide with a genuinely new
        // row. A local FK column holds the referenced row's id, and the author
        // column an id into rdb_keys, with no FOREIGN KEY declared (see the class
        // header note). The author column is emitted LAST so SELECT * reads as
        // id, business columns, then author_key_id.
        const cols: string[] = [`${quoteId(action.primaryKey)} INTEGER PRIMARY KEY AUTOINCREMENT`];
        for (const c of action.columns) cols.push(columnDecl(c.name, c.def));
        if (action.authorColumn !== undefined) {
            cols.push(`${quoteId(action.authorColumn)} INTEGER`);
        }

        this.db.exec(`CREATE TABLE ${quoteId(action.table)} (${cols.join(', ')})`);
        // Sync table keyed by row_hash, mapping to the app table's serial id.
        // `uuid` is the minted uuid for ingested rows (null otherwise).
        // `status` is the row lifecycle (active / deleted / ingestion_failure);
        // the sync record survives a delete so status records WHY it is gone.
        this.db.exec(
            `CREATE TABLE ${quoteId(action.syncTable)} (`
            + `"row_hash" TEXT PRIMARY KEY, "id" INTEGER, "uuid" TEXT, `
            + `"status" TEXT NOT NULL DEFAULT 'active')`);
        // A local id maps to exactly one rdb identity: two row_hashes can never
        // share an id, which keeps resolveRow unambiguous. Mirrors the idb
        // target's unique (table, id) index.
        this.db.exec(
            `CREATE UNIQUE INDEX ${quoteId(action.syncTable + '_id_uk')} `
            + `ON ${quoteId(action.syncTable)} ("id")`);

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
            ...(groupId !== undefined ? { groupId } : {}),
            schemaName: action.schemaName,
            anonymous: action.anonymous,
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
        // DROP TABLE took the table's indexes with it.
        this.db.prepare('DELETE FROM rdb_index_meta WHERE "table" = ?').run(action.table);
        this.metaCache.delete(action.table);
    }

    private addColumn(
        action: Extract<SchemaAction, { kind: 'add-column' }>,
        tighten: { table: string; column: string }[],
    ): void {
        // A local FK / key-ref column is tracked in meta, for upsert translation.
        //
        // SQLite rejects ADD COLUMN ... NOT NULL with no non-NULL default on
        // a non-empty table. Relax those adds to nullable here; applyTightens
        // restores NOT NULL after the row backfill, in the same transaction.
        const relax = action.def.nullable !== true
            && action.def.default === undefined
            && this.tableHasRows(action.table);
        const def = relax ? { ...action.def, nullable: true } : action.def;
        this.db.exec(`ALTER TABLE ${quoteId(action.table)} ADD COLUMN ${columnDecl(action.column, def)}`);
        const meta = this.loadMeta(action.table);
        meta.columnTypes[action.column] = action.def.type;
        if (action.fk !== undefined) meta.fkColumns[action.column] = { targetTable: action.fk.targetTable };
        if (action.keyRef === true) meta.keyRefColumns.push(action.column);
        this.writeMeta(action.table, meta);
        if (this.capture) this.installTriggers(action.table, meta);   // pick up the new column
        if (relax) tighten.push({ table: action.table, column: action.column });
    }

    private tableHasRows(table: string): boolean {
        return this.db.prepare(`SELECT 1 FROM ${quoteId(table)} LIMIT 1`).get() !== undefined;
    }

    private dropColumn(action: Extract<SchemaAction, { kind: 'drop-column' }>): void {
        // The capture triggers name the column, which would make the ALTER
        // fail. They are installed again below, once the meta is written. This
        // runs inside apply()'s transaction, with capture gated off, so no local
        // write falls between the two.
        if (this.capture) this.dropTriggers(action.table);
        this.db.exec(`ALTER TABLE ${quoteId(action.table)} DROP COLUMN ${quoteId(action.column)}`);
        const meta = this.loadMeta(action.table);
        delete meta.columnTypes[action.column];
        delete meta.fkColumns[action.column];
        meta.keyRefColumns = meta.keyRefColumns.filter((c) => c !== action.column);
        this.writeMeta(action.table, meta);
        if (this.capture) this.installTriggers(action.table, meta);   // from the columns that are left
    }

    // Rebuild `table` as CREATE tmp / INSERT...SELECT / DROP / RENAME, forcing
    // named columns to NOT NULL: applyTightens uses it to restore NOT NULL after
    // a relaxed ADD COLUMN, since SQLite can't add NOT NULL to a column. Column
    // defs are read live via PRAGMA table_info, so no NOT NULL / DEFAULT detail
    // is lost. table_info carries no CHECK, so a column's check is rendered
    // again from its ColumnType in meta. Runs inside apply()'s transaction;
    // because foreign_keys is OFF, dropping the old table does not trip a
    // reference the app declared into it.
    //
    // Trigger reinstall is the caller's job: DROP TABLE removes capture triggers.
    private rebuildTable(table: string, forceNotNull: ReadonlySet<string>): void {
        const meta = this.loadMeta(table);
        const keep = this.db.prepare(`PRAGMA table_info(${quoteId(table)})`)
            .all() as { name: string; type: string; notnull: number; dflt_value: string | null; pk: number }[];

        // The id column is `INTEGER PRIMARY KEY AUTOINCREMENT` (owned serial, never
        // reused); every other column re-renders its stored type + NOT NULL +
        // DEFAULT verbatim, with forceNotNull overriding a currently-nullable
        // column after backfill. The AUTOINCREMENT must survive the rebuild or the
        // first NOT NULL tighten would silently drop the never-reuse guarantee.
        const decls: string[] = keep.map((c) => {
            if (c.name === meta.idColumn) return `${quoteId(c.name)} INTEGER PRIMARY KEY AUTOINCREMENT`;
            const nn = c.notnull || forceNotNull.has(c.name) ? ' NOT NULL' : '';
            return `${quoteId(c.name)} ${c.type}${nn}`
                + `${c.dflt_value !== null ? ' DEFAULT ' + c.dflt_value : ''}`
                + checkFragment(c.name, meta.columnTypes[c.name]);
        });

        // DROP TABLE removes every index on the table (projection-managed and
        // hand-made alike), so capture their DDL and replay it on the rebuilt
        // table.
        const indexes = this.db.prepare(
            "SELECT name, sql FROM sqlite_master WHERE type = 'index' AND tbl_name = ? AND sql IS NOT NULL")
            .all(table) as { name: string; sql: string }[];

        const tmp = table + '__rebuild';
        const cols = keep.map((c) => quoteId(c.name)).join(', ');
        this.db.exec(`CREATE TABLE ${quoteId(tmp)} (${decls.join(', ')})`);
        this.db.exec(`INSERT INTO ${quoteId(tmp)} (${cols}) SELECT ${cols} FROM ${quoteId(table)}`);
        this.db.exec(`DROP TABLE ${quoteId(table)}`);
        this.db.exec(`ALTER TABLE ${quoteId(tmp)} RENAME TO ${quoteId(table)}`);
        for (const ix of indexes) this.db.exec(ix.sql);

        // DROP TABLE cleared this table's sqlite_sequence row, and the copy above
        // only re-seeded it to MAX(app.id) among surviving rows. The sync table is
        // the ledger of every id ever handed out (rows survive deletes), so re-seed
        // to at least MAX(sync.id) - otherwise a subsequent auto id could reuse an
        // id whose app row was deleted but whose sync mapping still lives.
        // sqlite_sequence exists (the AUTOINCREMENT CREATE TABLE above ensures it)
        // but has no UNIQUE(name), so upsert by hand rather than ON CONFLICT.
        const maxSync = (this.db.prepare(
            `SELECT COALESCE(MAX("id"), 0) AS n FROM ${quoteId(meta.syncTable)}`).get() as { n: number }).n;
        if (maxSync > 0) {
            const cur = this.db.prepare('SELECT seq FROM sqlite_sequence WHERE name = ?')
                .get(table) as { seq: number } | undefined;
            if (cur === undefined) {
                this.db.prepare('INSERT INTO sqlite_sequence (name, seq) VALUES (?, ?)').run(table, maxSync);
            } else if (maxSync > cur.seq) {
                this.db.prepare('UPDATE sqlite_sequence SET seq = ? WHERE name = ?').run(maxSync, table);
            }
        }
    }

    // Restore NOT NULL on columns that addColumn had to add as nullable (SQLite
    // ADD COLUMN rule). Grouped per table so one rebuild covers every column
    // tightened in this apply. The rebuild's NOT NULL is also a backfill-
    // completeness assertion: a live row still NULL throws and rolls the batch.
    private applyTightens(specs: { table: string; column: string }[]): void {
        if (specs.length === 0) return;
        const grouped = new Map<string, Set<string>>();
        for (const s of specs) {
            const cols = grouped.get(s.table);
            if (cols === undefined) grouped.set(s.table, new Set([s.column]));
            else cols.add(s.column);
        }
        for (const [table, cols] of grouped) {
            this.rebuildTable(table, cols);
            if (this.capture) this.installTriggers(table, this.loadMeta(table));
        }
    }

    // -----------------------------------------------------------------------
    // Row channel
    // -----------------------------------------------------------------------

    private applyRowAction(action: RowAction): void {
        if (action.kind === 'upsert-row') return this.upsertRow(action);
        return this.deleteRow(action);
    }

    // The serial id a rowId has in a sync table, if it has one.
    private syncIdOf(syncTable: string, rowId: string): number | undefined {
        return (this.db.prepare(`SELECT "id" FROM ${quoteId(syncTable)} WHERE "row_hash" = ?`)
            .get(rowId) as { id: number } | undefined)?.id;
    }

    // The serial id for a reference to a row of `table` (HIP-29's Local ids):
    // the id its sync row has, or else the next value of the table's own
    // counter, sqlite_sequence, reserved in a sync row with no app row. Neither
    // a native insert nor a row the projection writes later can get that id:
    // both take theirs from the same counter, and the row that arrives later
    // is inserted with the reserved id. The sync table never makes up an id.
    private allocateId(table: string, syncTable: string, rowId: string): number {
        const existing = this.syncIdOf(syncTable, rowId);
        if (existing !== undefined) return existing;
        const next = this.nextSequenceValue(table);
        this.db.prepare(`INSERT INTO ${quoteId(syncTable)} ("row_hash", "id") VALUES (?, ?)`).run(rowId, next);
        return next;
    }

    // Takes the next value of a table's AUTOINCREMENT counter, creating the
    // table's row at 0 when it never had an insert.
    private nextSequenceValue(table: string): number {
        const bumped = this.db.prepare('UPDATE sqlite_sequence SET seq = seq + 1 WHERE name = ? RETURNING seq')
            .get(table) as { seq: number } | undefined;
        if (bumped !== undefined) return bumped.seq;
        this.db.prepare('INSERT INTO sqlite_sequence (name, seq) VALUES (?, 0)').run(table);
        return (this.db.prepare('UPDATE sqlite_sequence SET seq = seq + 1 WHERE name = ? RETURNING seq')
            .get(table) as { seq: number }).seq;
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
    // A null is bound as NULL: no id is allocated, no key interned and no value
    // converted for it.
    private paramForColumn(
        meta: TableMeta, col: string, value: json.Literal | null,
        keyMaterial?: { [keyHash: string]: string },
    ): number | string | null {
        if (value === null) return null;
        const fk = meta.fkColumns[col];
        if (fk !== undefined) {
            const targetMeta = this.loadMeta(fk.targetTable);
            return this.allocateId(fk.targetTable, targetMeta.syncTable, value as string);
        }
        if (meta.keyRefColumns.includes(col)) {
            const keyHash = value as string;
            return this.internKey(keyHash, keyMaterial?.[keyHash]);
        }
        return toParam(value, meta.columnTypes[col] ?? 'string');
    }

    private upsertRow(action: Extract<RowAction, { kind: 'upsert-row' }>): void {
        const meta = this.loadMeta(action.table);
        // A row with no sync row yet has no id: it is inserted first and takes
        // the id its insert produces (HIP-29's Local ids). One with a sync row,
        // a reference's reservation or an earlier incarnation, keeps its id.
        const id = this.syncIdOf(meta.syncTable, action.rowId);
        // A (re)materialized row is live: active (reinstates a prior deleted /
        // ingestion_failure record for the same rowId).
        const activate = (): void => {
            this.db.prepare(`UPDATE ${quoteId(meta.syncTable)} SET "status" = 'active' WHERE "row_hash" = ?`)
                .run(action.rowId);
        };
        if (id !== undefined) activate();

        // Apply keyMaterial first so subsequent key-ref interns see the pubkey.
        if (action.keyMaterial !== undefined) {
            for (const [hash, pk] of Object.entries(action.keyMaterial)) {
                this.internKey(hash, pk);
            }
        }

        // The stored author is read with the row, so that an action without an
        // author clears it only when it holds something.
        const stored = id === undefined ? undefined : this.db.prepare(
            `SELECT ${meta.authorColumn !== undefined ? quoteId(meta.authorColumn) : '1'} AS author `
            + `FROM ${quoteId(action.table)} WHERE ${quoteId(meta.idColumn)} = ?`).get(id) as { author: unknown } | undefined;
        const exists = stored !== undefined;

        const columns = Object.keys(action.values);
        const setAuthor = meta.authorColumn !== undefined && action.author !== undefined;
        const authorId = setAuthor ? this.internKey(action.author!) : undefined;

        if (exists) {
            const assignments: string[] = [];
            const params: (number | string | null)[] = [];
            for (const col of columns) {
                assignments.push(`${quoteId(col)} = ?`);
                params.push(this.paramForColumn(meta, col, action.values[col], action.keyMaterial));
            }
            if (setAuthor) {
                assignments.push(`${quoteId(meta.authorColumn!)} = ?`);
                params.push(authorId!);
            } else if (meta.authorColumn !== undefined && stored.author !== null) {
                // An action without an author is for a row rdb has none for: the
                // author column is cleared, whatever a local write left in it.
                assignments.push(`${quoteId(meta.authorColumn)} = NULL`);
            }
            if (assignments.length === 0) return;   // nothing to update
            params.push(id!);
            this.db.prepare(
                `UPDATE ${quoteId(action.table)} SET ${assignments.join(', ')} `
                + `WHERE ${quoteId(meta.idColumn)} = ?`).run(...params);
            this.rememberApply(meta, action.table, id!, 'update');
            return;
        }

        // A column the action sets to NULL is named with NULL, so the table's
        // DEFAULT does not apply to it; a column the action leaves out takes it.
        const insertCols: string[] = id !== undefined ? [meta.idColumn] : [];
        const insertParams: (number | string | null)[] = id !== undefined ? [id] : [];
        for (const col of columns) {
            insertCols.push(col);
            insertParams.push(this.paramForColumn(meta, col, action.values[col], action.keyMaterial));
        }
        if (setAuthor) {
            insertCols.push(meta.authorColumn!);
            insertParams.push(authorId!);
        }
        const placeholders = insertParams.map(() => '?').join(', ');
        const inserted = this.db.prepare(
            insertCols.length === 0
                ? `INSERT INTO ${quoteId(action.table)} DEFAULT VALUES RETURNING ${quoteId(meta.idColumn)} AS id`
                : `INSERT INTO ${quoteId(action.table)} (${insertCols.map(quoteId).join(', ')}) `
                    + `VALUES (${placeholders}) RETURNING ${quoteId(meta.idColumn)} AS id`).get(...insertParams) as { id: number };
        if (id === undefined) {
            this.db.prepare(`INSERT INTO ${quoteId(meta.syncTable)} ("row_hash", "id") VALUES (?, ?)`).run(action.rowId, inserted.id);
            activate();
        }
        this.rememberApply(meta, action.table, inserted.id, 'insert');
    }

    private rememberApply(meta: TableMeta, table: string, id: number, kind: 'insert' | 'update'): void {
        if (this.pendingApply === undefined) return;
        this.pendingApply.push({ kind, table, localId: id, values: this.readLogicalRow(meta, table, id) });
    }

    private readLogicalRow(meta: TableMeta, table: string, id: number): { [column: string]: json.Literal | null } {
        const cols = [meta.idColumn, ...Object.keys(meta.columnTypes)];
        if (meta.authorColumn !== undefined && !cols.includes(meta.authorColumn)) cols.push(meta.authorColumn);
        const selected = cols.map((col) => `${quoteId(col)} AS ${quoteId(col)}`).join(', ');
        const row = this.db.prepare(
            `SELECT ${selected} FROM ${quoteId(table)} WHERE ${quoteId(meta.idColumn)} = ?`).get(id) as { [column: string]: unknown } | undefined;
        // A value that can't be read is reported as null: this runs inside
        // apply() and the settle, which must not fail on a row that a write
        // with SQLite's checks switched off left behind.
        const values: { [column: string]: json.Literal | null } = {};
        for (const col of cols) {
            const read = row === undefined ? undefined : readLogical(row[col], meta.columnTypes[col] ?? (col === meta.idColumn || col === meta.authorColumn ? 'integer' : 'string'));
            values[col] = read === undefined || !read.ok || read.value === undefined ? null : read.value;
        }
        return values;
    }

    private deleteRow(action: Extract<RowAction, { kind: 'delete-row' }>): void {
        const meta = this.loadMeta(action.table);
        // Delete only the app row; the sync row is intentionally kept so a
        // later void-flip reinstatement reuses the same serial id. Status ->
        // deleted (the settle overrides to ingestion_failure for a reverted
        // insert orphan).
        const local = this.db.prepare(`SELECT "id" AS id FROM ${quoteId(meta.syncTable)} WHERE "row_hash" = ?`)
            .get(action.rowId) as { id: number } | undefined;
        this.db.prepare(
            `DELETE FROM ${quoteId(action.table)} WHERE ${quoteId(meta.idColumn)} = `
            + `(SELECT "id" FROM ${quoteId(meta.syncTable)} WHERE "row_hash" = ?)`).run(action.rowId);
        if (local !== undefined && this.pendingApply !== undefined) {
            this.pendingApply.push({ kind: 'delete', table: action.table, localId: local.id });
        }
        this.db.prepare(`UPDATE ${quoteId(meta.syncTable)} SET "status" = 'deleted' WHERE "row_hash" = ?`)
            .run(action.rowId);
    }

    // -----------------------------------------------------------------------
    // MaterializedChangeSource (inbound). Requires provisioned capture.
    // -----------------------------------------------------------------------

    async executeQuery(plan: SingleTableQueryPlan): Promise<ProjectedRow[]> {
        this.ensureBookkeeping();
        this.ensureLike();
        // One read transaction, so the meta the rows are read with belongs to
        // the schema they are read under: another handle may change it.
        return this.db.transaction(() => this.readQuery(plan))();
    }

    private readQuery(plan: SingleTableQueryPlan): ProjectedRow[] {
        this.freshMeta();
        const meta = this.loadMeta(plan.table);
        const needsOrder = plan.limit !== undefined || plan.cursor !== undefined;
        const order = normalizeOrder(plan.orderBy, meta.idColumn, needsOrder);
        const params: Array<number | string | null> = [];
        let whereSql = plan.where === undefined ? '1' : compileWhere(plan.where, params);
        if (plan.cursor !== undefined && order.length > 0) {
            const anchor = this.anchorValues(meta, plan.table, plan.cursor.id, plan.cursor.cachedValues);
            if (anchor === undefined) return [];
            const cols = order.map((item) => quoteId(item.column)).join(', ');
            const marks = order.map(() => '?').join(', ');
            whereSql = `(${whereSql}) AND (${cols}) ${plan.cursor.side === 'before' ? '<' : '>'} (${marks})`;
            for (const item of order) params.push(sqlParam(anchor[item.column] ?? null));
        }
        const columns = plan.select ?? [meta.idColumn, ...Object.keys(meta.columnTypes), ...(meta.authorColumn ? [meta.authorColumn] : [])];
        const select = [...new Set(columns)].map((col) => quoteId(col)).join(', ');
        const orderSql = order.length === 0 ? '' : ` ORDER BY ${order.map((item) => `${quoteId(item.column)} ${item.direction.toUpperCase()}`).join(', ')}`;
        const limitSql = plan.limit === undefined ? '' : ` LIMIT ${plan.limit}`;
        const rows = this.db.prepare(`SELECT ${select} FROM ${quoteId(plan.table)} WHERE ${whereSql}${orderSql}${limitSql}`).all(...params) as Array<{ [column: string]: unknown }>;
        return rows.map((row) => {
            const projected: ProjectedRow = { id: Number(row[meta.idColumn]) };
            for (const col of new Set(columns)) {
                const read = readLogical(row[col], meta.columnTypes[col] ?? 'integer');
                if (!read.ok) throw new Error(`column '${col}' of row ${projected.id} of '${plan.table}' can't be read: ${read.reason}`);
                projected[col] = read.value === undefined ? null : read.value;
            }
            return projected;
        });
    }

    private ensureLike(): void {
        if (this.likeReady) return;
        this.db.function('rdb_like_match', { deterministic: true }, (value: unknown, pattern: unknown) => {
            if (typeof value !== 'string' || typeof pattern !== 'string') return null;
            return likeMatch(value, pattern) ? 1 : 0;
        });
        this.likeReady = true;
    }

    private anchorValues(meta: TableMeta, table: string, id: number, cached: { [column: string]: json.Literal | null } | undefined): { [column: string]: json.Literal | null } | undefined {
        const exists = this.db.prepare(`SELECT 1 FROM ${quoteId(table)} WHERE ${quoteId(meta.idColumn)} = ?`).get(id);
        if (exists !== undefined) return this.readLogicalRow(meta, table, id);
        return cached;
    }

    async hasPendingChanges(): Promise<boolean> {
        this.ensureBookkeeping();
        return this.outboxHasRows();
    }

    async recordWriter(record: WriterRecord): Promise<void> {
        this.ensureBookkeeping();
        this.db.prepare('INSERT OR REPLACE INTO rdb_writer_record (id, writer_key_id, hash_suite, author) VALUES (1, ?, ?, ?)')
            .run(record.writerKeyId ?? null, record.hashSuite, record.author);
    }

    async readWriterRecord(): Promise<WriterRecord | undefined> {
        this.ensureBookkeeping();
        return this.writerRecordNow();
    }

    private writerRecordNow(): WriterRecord | undefined {
        const row = this.db.prepare('SELECT writer_key_id, hash_suite, author FROM rdb_writer_record WHERE id = 1').get() as
            { writer_key_id: number | null; hash_suite: string; author: WriterRecord['author'] } | undefined;
        if (row === undefined) return undefined;
        return {
            ...(row.writer_key_id !== null ? { writerKeyId: row.writer_key_id } : {}),
            hashSuite: row.hash_suite, author: row.author,
        };
    }

    // HIP-20's writer. Each call is one immediate transaction, since it reads
    // before it writes: a deferred one would fail once another connection, such
    // as the host's, committed in between. Inside it the call reads what it is
    // checked against, at the current schema version: the table's meta, its
    // nullability and defaults, the writer record, the capture switch, the key
    // table and the row. Then the shared checks, the keys interned last, and
    // the statement, which the capture triggers record as a native write.
    openWriter(): RowWriter {
        this.ensureBookkeeping();
        if (!this.capture) {
            throw new WriteRefusedError('no-capture', 'this SQLite target is not set up for capture, so its writes would never reach rdb');
        }
        return {
            insert: async (table, values) => this.writerCall(() => this.writerInsert(table, values)),
            update: async (table, id, values, options) =>
                this.writerCall(() => this.writerUpdate(table, id, values, options?.compare === true)),
            delete: async (table, id) => this.writerCall(() => this.writerDelete(table, id)),
            writerKeyId: async () => {
                this.ensureBookkeeping();
                return recordedWriterKeyId(this.writerRecordNow());
            },
        };
    }

    private writerCall<T>(call: () => T): T {
        this.ensureBookkeeping();
        const result = this.db.transaction(call).immediate();
        this.noteLocalCommit();
        return result;
    }

    private writerInsert(table: string, values: WriteValues): number {
        const { meta, shape } = this.writeShape(table);
        const written = this.interned(checkWrite(shape, this.writeContext(), { kind: 'insert', values }));
        const cols = Object.keys(written);
        const info = cols.length === 0
            ? this.db.prepare(`INSERT INTO ${quoteId(table)} DEFAULT VALUES`).run()
            : this.db.prepare(`INSERT INTO ${quoteId(table)} (${cols.map(quoteId).join(', ')}) VALUES (${cols.map(() => '?').join(', ')})`)
                .run(...cols.map((c) => writeParam(meta, c, written[c])));
        return Number(info.lastInsertRowid);
    }

    private writerUpdate(table: string, id: number, values: WriteValues, compare: boolean): boolean {
        const { meta, shape } = this.writeShape(table);
        const ctx = this.writeContext();
        checkWritable(shape, ctx);
        if (this.db.prepare(`SELECT 1 FROM ${quoteId(table)} WHERE ${quoteId(meta.idColumn)} = ?`).get(id) === undefined) return false;
        const row = this.readLogicalRow(meta, table, id);
        const author = meta.authorColumn === undefined ? undefined : row[meta.authorColumn];
        const written = this.interned(checkWrite(shape, ctx, {
            kind: 'update', values, compare, row: { id, values: row, author: typeof author === 'number' ? author : undefined },
        }));
        const cols = Object.keys(written);
        if (cols.length > 0) {
            this.db.prepare(`UPDATE ${quoteId(table)} SET ${cols.map((c) => `${quoteId(c)} = ?`).join(', ')} WHERE ${quoteId(meta.idColumn)} = ?`)
                .run(...cols.map((c) => writeParam(meta, c, written[c])), id);
        }
        return true;
    }

    // Resolves from the statement's count of changed rows, which leaves out
    // the rows its capture triggers write.
    private writerDelete(table: string, id: number): boolean {
        const { meta, shape } = this.writeShape(table);
        checkWritable(shape, this.writeContext());
        return this.db.prepare(`DELETE FROM ${quoteId(table)} WHERE ${quoteId(meta.idColumn)} = ?`).run(id).changes > 0;
    }

    // What the checks know of a table: its meta, read again when the schema
    // version changed, and its nullability and defaults from PRAGMA table_info.
    private writeShape(table: string): { meta: TableMeta; shape: TableShape } {
        this.freshMeta();
        if (this.db.prepare('SELECT 1 FROM rdb_table_meta WHERE "table" = ?').get(table) === undefined) {
            throw new WriteRefusedError('unknown-table', `this target has no table '${table}'`);
        }
        const meta = this.loadMeta(table);
        const info = this.columnInfo(table);
        const columns: TableShape['columns'] = {};
        for (const [column, type] of Object.entries(meta.columnTypes)) {
            const c = info.get(column);
            columns[column] = {
                type, nullable: c === undefined || !c.notNull, hasDefault: c?.hasDefault ?? false, key: meta.keyRefColumns.includes(column),
            };
        }
        return {
            meta,
            shape: {
                table, idColumn: meta.idColumn, ...(meta.authorColumn !== undefined ? { authorColumn: meta.authorColumn } : {}),
                columns, anonymous: meta.anonymous,
            },
        };
    }

    // Each column's nullability and whether it has a default, kept while the
    // schema version the meta was read at holds.
    private columnInfo(table: string): Map<string, { notNull: boolean; hasDefault: boolean }> {
        const cached = this.columnInfoCache.get(table);
        if (cached !== undefined && cached.version === this.metaVersion) return cached.columns;
        const rows = this.db.prepare('SELECT name, "notnull" AS nn, dflt_value AS dflt FROM pragma_table_info(?)').all(table) as
            { name: string; nn: number; dflt: string | null }[];
        const columns = new Map(rows.map((r) => [r.name, { notNull: r.nn === 1, hasDefault: r.dflt !== null }]));
        this.columnInfoCache.set(table, { version: this.metaVersion, columns });
        return columns;
    }

    private writeContext(): WriteContext {
        const config = this.db.prepare('SELECT enabled FROM rdb_capture_config').get() as { enabled: number } | undefined;
        return {
            record: this.writerRecordNow(),
            captureOn: this.capture && config?.enabled === 1,
            keys: {
                hasId: (id) => this.db.prepare(`SELECT 1 FROM ${quoteId(DEFAULT_KEY_TABLE)} WHERE id = ?`).get(id) !== undefined,
                idOf: (keyHash) => (this.db.prepare(`SELECT id FROM ${quoteId(DEFAULT_KEY_TABLE)} WHERE key_hash = ?`).get(keyHash) as
                    { id: number } | undefined)?.id,
            },
        };
    }

    // A checked write's values, with its keys interned.
    private interned(checked: CheckedWrite): { [column: string]: json.Literal | null } {
        const values = { ...checked.values };
        for (const k of checked.intern) values[k.column] = this.internKey(k.keyHash, k.publicKey);
        return values;
    }

    async tableNames(member: string): Promise<{ [schemaName: string]: string }> {
        this.ensureBookkeeping();
        const rows = this.db.prepare('SELECT "table" AS t, group_id AS g, schema_name AS s FROM rdb_table_meta').all() as
            { t: string; g: string | null; s: string }[];
        return memberTableNames(member, rows.map((r) => ({ table: r.t, schemaName: r.s, ...(r.g !== null ? { groupId: r.g } : {}) })));
    }

    // The table's group, its name in the schema, and whether the group has no
    // identity provider, from the table's meta.
    tableGroup(table: string): { groupId?: B64Hash; schemaName: string; anonymous: boolean } | undefined {
        this.ensureBookkeeping();
        this.freshMeta();
        if (this.db.prepare('SELECT 1 FROM rdb_table_meta WHERE "table" = ?').get(table) === undefined) return undefined;
        const meta = this.loadMeta(table);
        return { ...(meta.groupId !== undefined ? { groupId: meta.groupId } : {}),
            schemaName: meta.schemaName, anonymous: meta.anonymous };
    }

    // Whether the outbox holds a change; false without capture.
    private outboxHasRows(): boolean {
        if (!this.capture) return false;
        return this.db.prepare('SELECT 1 FROM rdb_outbox LIMIT 1').get() !== undefined;
    }

    async drainChanges(): Promise<CapturedBatch> {
        this.requireCapture();
        // One read transaction, so the meta each change is read with belongs to
        // the schema it was written under: another handle may change it.
        const { rows, changes } = this.db.transaction(() => this.readOutbox())();
        const fresh = rows.filter((row) => !this.emittedOutboxIds.has(row.id));
        this.emittedOutboxIds = new Set(rows.map((row) => row.id));
        if (fresh.length > 0) {
            const tables = [...new Set(fresh.map((row) => row.tbl))];
            for (const listener of [...this.commitListeners]) listener({ source: 'local', tables });
        }
        return { changes };
    }

    private readOutbox(): { rows: { id: number; tbl: string }[]; changes: CapturedChange[] } {
        this.freshMeta();
        const rows = this.db.prepare(
            'SELECT id, "table" AS tbl, local_id, op, changed, record FROM rdb_outbox ORDER BY id')
            .all() as { id: number; tbl: string; local_id: number; op: string; changed: string | null; record: string | null }[];
        const changes: CapturedChange[] = [];
        for (const row of rows) {
            const record = row.record === null ? {} : { record: JSON.parse(row.record) as ChangeRecord };
            if (row.op === 'delete') {
                changes.push({ id: row.id, kind: 'delete', table: row.tbl, localId: row.local_id, ...record });
                continue;
            }
            // A table dropped since the write has no meta: its columns can't be
            // read, and ingestion drops or refuses the change.
            let meta: TableMeta | undefined;
            try { meta = this.loadMeta(row.tbl); } catch { meta = undefined; }
            const stored = (row.changed === null ? {} : JSON.parse(row.changed)) as { [column: string]: unknown };
            // A NULL is kept as a null: an update to NULL is a change, and an
            // insert's captured row names every empty column with one, which
            // the planner sorts out. A value that can't be read is left out of
            // the values and named in `unreadable`, so the drain never throws.
            const values: { [column: string]: json.Literal | null } = {};
            const unreadable: { [column: string]: string } = {};
            for (const [col, raw] of Object.entries(stored)) {
                if (meta === undefined) {
                    unreadable[col] = `the table '${row.tbl}' is no longer in the projection`;
                    continue;
                }
                const read = readLogical(raw, meta.columnTypes[col] ?? 'string');
                if (read.ok) values[col] = read.value ?? null;
                else unreadable[col] = read.reason;
            }
            changes.push({ id: row.id, kind: row.op === 'insert' ? 'insert' : 'update', table: row.tbl, localId: row.local_id, values,
                ...(Object.keys(unreadable).length > 0 ? { unreadable } : {}), ...record });
        }
        return { rows, changes };
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

    async writeRecord(id: CapturedChangeId, record: ChangeRecord): Promise<void> {
        this.requireCapture();
        const run = this.db.transaction(() => {
            const row = this.db.prepare('SELECT record FROM rdb_outbox WHERE id = ?').get(id) as { record: string | null } | undefined;
            if (row === undefined) return;
            const prev = row.record === null ? undefined : JSON.parse(row.record) as ChangeRecord;
            this.db.prepare('UPDATE rdb_outbox SET record = ? WHERE id = ?').run(JSON.stringify(mergeChangeRecord(prev, record)), id);
        });
        run.immediate();
    }

    async recordEndObserve(groupId: B64Hash, hash: B64Hash): Promise<void> {
        this.requireCapture();
        const run = this.db.transaction(() => {
            const list = [...this.readEndObserves(groupId), hash];
            this.db.prepare('INSERT INTO rdb_checkpoint (group_id, version, end_observes) VALUES (?, NULL, ?) '
                + 'ON CONFLICT(group_id) DO UPDATE SET end_observes = excluded.end_observes')
                .run(groupId, JSON.stringify(list));
        });
        run.immediate();
    }

    async getEndObserves(groupId: B64Hash): Promise<B64Hash[]> {
        this.ensureBookkeeping();
        return this.readEndObserves(groupId);
    }

    private readEndObserves(groupId: B64Hash): B64Hash[] {
        const row = this.db.prepare('SELECT end_observes AS list FROM rdb_checkpoint WHERE group_id = ?').get(groupId) as
            { list: string | null } | undefined;
        return row?.list == null ? [] : JSON.parse(row.list) as B64Hash[];
    }

    // The settle, in HIP-29's order (see IngestSettle), in one transaction:
    // a step that throws rolls back every one.
    async commitIngest(settle: IngestSettle): Promise<void> {
        this.requireCapture();
        const written = new Set<string>();
        const run = this.db.transaction(() => {
            this.freshMeta();
            // 1. Each group's checkpoint is still the one the pass found.
            for (const { groupId, expected } of settle.checkpoints ?? []) {
                if (!versionsEqual(this.readCheckpoint(groupId), expected)) throw new CheckpointMovedError(groupId);
            }
            // 2. The handled rows' failure events are read, and the rows leave
            // the outbox; the rest is read, by table and local id.
            const failures: OpEvent[] = [];
            const recordOf = this.db.prepare('SELECT record FROM rdb_outbox WHERE id = ?');
            for (const id of settle.consumed) {
                const row = recordOf.get(id) as { record: string | null } | undefined;
                const failure = row?.record == null ? undefined : (JSON.parse(row.record) as ChangeRecord).failure;
                if (failure !== undefined) failures.push(failure);
            }
            for (const id of settle.consumed) {
                this.db.prepare('DELETE FROM rdb_outbox WHERE id = ?').run(id);
            }
            const pending = new Map<string, RawOutboxRow[]>();
            for (const row of this.db.prepare('SELECT "table" AS tbl, local_id, op, changed FROM rdb_outbox ORDER BY id').all() as RawOutboxRow[]) {
                const key = row.tbl + '\u0000' + row.local_id;
                pending.set(key, [...(pending.get(key) ?? []), row]);
            }
            // Echo suppression: reverts materialize rows; gate the capture triggers
            // so they don't re-enter the outbox.
            this.setApplying(true);
            // 3. The handled inserts' mappings, then the accepted deletes.
            for (const m of settle.inserts ?? []) {
                const meta = this.loadMeta(m.table);
                this.db.prepare(
                    `INSERT INTO ${quoteId(meta.syncTable)} ("row_hash", "id", "uuid", "status") VALUES (?, ?, ?, ?) `
                    + `ON CONFLICT("row_hash") DO UPDATE SET "id" = excluded."id", "uuid" = excluded."uuid", "status" = excluded."status"`)
                    .run(m.rowId, m.localId, m.uuid === '' ? null : m.uuid, m.status ?? 'active');
            }
            for (const d of settle.deletes ?? []) {
                const meta = this.loadMeta(d.table);
                this.db.prepare(`UPDATE ${quoteId(meta.syncTable)} SET "status" = 'deleted' WHERE "row_hash" = ?`).run(d.rowId);
            }
            // 4. The rows of refused inserts that had no identity, and 5. the
            // reverts, each with its changes still in the outbox on top.
            for (const o of settle.orphans ?? []) {
                this.revertRow({ table: o.table, localId: o.localId }, pending, false);
                written.add(o.table);
            }
            for (const r of settle.reverts ?? []) {
                this.revertRow(r, pending, true);
                written.add(r.table);
            }
            // 6. Json text in canonical form, as rows from rdb have it: a query
            // that binds a json value matches only that text.
            for (const w of settle.jsonWrites ?? []) {
                if (this.canonicalizeJson(w, pending)) written.add(w.table);
            }
            // 7. The handled rows' failure events, then the settle's own.
            const logged: StoredOpEvent[] = [];
            for (const e of [...failures, ...(settle.events ?? [])]) {
                const stored = this.logOpEvent(e);
                if (stored !== undefined) logged.push(stored);
            }
            this.pendingEvents = logged;
            // 8. The new checkpoints, and every group's end observes cleared.
            for (const { groupId, next } of settle.checkpoints ?? []) this.persistCheckpoint(groupId, next);
            this.db.prepare('UPDATE rdb_checkpoint SET end_observes = NULL WHERE end_observes IS NOT NULL').run();
            this.setApplying(false);
        });
        run.immediate();
        const logged = this.pendingEvents ?? [];
        this.pendingEvents = undefined;
        for (const id of settle.consumed) this.emittedOutboxIds.delete(typeof id === 'number' ? id : Number.NaN);
        if (written.size > 0 || logged.length > 0) {
            const revertedTables = [...new Set([
                ...written,
                ...logged.flatMap((stored) => opEventRows(stored.event).map((row) => row.table)),
            ])];
            for (const listener of [...this.commitListeners]) listener({ source: 'settle', revertedTables, events: logged });
        }
    }

    // Sets a row to rdb's row, or to none, then puts its changes still in the
    // outbox back on top, as capture recorded them (HIP-29's Reverts). A row
    // named only by its row hash is found through the sync table, after the
    // settle wrote the inserts' mappings. `markDeleted`: a row removed this way
    // has its active mapping marked deleted. Runs with capture gated off.
    private revertRow(revert: Pick<RowRevert, 'table' | 'localId' | 'row'> & { rowId?: B64Hash },
        pending: Map<string, RawOutboxRow[]>, markDeleted: boolean): void {
        const meta = this.loadMeta(revert.table);
        const table = quoteId(revert.table);
        const idCol = quoteId(meta.idColumn);
        let id = revert.localId ?? (revert.rowId !== undefined ? this.syncIdOf(meta.syncTable, revert.rowId) : undefined);
        if (revert.row !== undefined) {
            this.upsertRow(revert.row);
            id = this.syncIdOf(meta.syncTable, revert.row.rowId);
        } else if (id !== undefined) {
            this.db.prepare(`DELETE FROM ${table} WHERE ${idCol} = ?`).run(id);
            if (markDeleted && revert.rowId !== undefined) {
                this.db.prepare(`UPDATE ${quoteId(meta.syncTable)} SET "status" = 'deleted' WHERE "row_hash" = ? AND "status" = 'active'`)
                    .run(revert.rowId);
            }
        }
        if (id === undefined) return;
        for (const change of pending.get(revert.table + '\u0000' + id) ?? []) this.applyCaptured(meta, revert.table, id, change);
    }

    // Writes one outbox change back into its table, as capture recorded it: an
    // update writes its columns where the row is, a delete removes the row, and
    // an insert writes its whole row under its local id. The stored values are
    // read back from the outbox's json as they were written (json_extract), so
    // each keeps its SQLite type. A column the table no longer has is skipped.
    private applyCaptured(meta: TableMeta, tableName: string, id: number, change: RawOutboxRow): void {
        const table = quoteId(tableName);
        const idCol = quoteId(meta.idColumn);
        if (change.op === 'delete') {
            this.db.prepare(`DELETE FROM ${table} WHERE ${idCol} = ?`).run(id);
            return;
        }
        const known = new Set([...Object.keys(meta.columnTypes), ...(meta.authorColumn !== undefined ? [meta.authorColumn] : [])]);
        const columns = Object.keys(change.changed === null ? {} : JSON.parse(change.changed) as object).filter((c) => known.has(c));
        const extract = (c: string): string => `json_extract(?, ${quoteText('$.' + JSON.stringify(c))})`;
        if (change.op === 'insert') {
            this.db.prepare(
                `INSERT OR REPLACE INTO ${table} (${[idCol, ...columns.map(quoteId)].join(', ')}) `
                + `VALUES (${['?', ...columns.map(extract)].join(', ')})`)
                .run(id, ...columns.map(() => change.changed));
            return;
        }
        if (columns.length === 0) return;
        this.db.prepare(`UPDATE ${table} SET ${columns.map((c) => `${quoteId(c)} = ${extract(c)}`).join(', ')} WHERE ${idCol} = ?`)
            .run(...columns.map(() => change.changed), id);
    }

    // Rewrites a json column of a handled change in canonical form. It skips a
    // column a change still in the outbox writes, whose own settle does it, and
    // a row that is gone. Returns whether it wrote.
    private canonicalizeJson(w: { table: string; localId: number; column: string }, pending: Map<string, RawOutboxRow[]>): boolean {
        const meta = this.loadMeta(w.table);
        if (meta.columnTypes[w.column] !== 'json') return false;
        for (const change of pending.get(w.table + '\u0000' + w.localId) ?? []) {
            if (change.op !== 'update') return false;
            if (change.changed !== null && w.column in (JSON.parse(change.changed) as object)) return false;
        }
        const row = this.db.prepare(`SELECT ${quoteId(w.column)} AS v FROM ${quoteId(w.table)} WHERE ${quoteId(meta.idColumn)} = ?`)
            .get(w.localId) as { v: unknown } | undefined;
        if (row === undefined || typeof row.v !== 'string') return false;
        let canonical: string;
        try { canonical = json.toStringNormalized(JSON.parse(row.v) as json.Literal); } catch { return false; }
        if (canonical === row.v) return false;
        this.db.prepare(`UPDATE ${quoteId(w.table)} SET ${quoteId(w.column)} = ? WHERE ${quoteId(meta.idColumn)} = ?`).run(canonical, w.localId);
        return true;
    }

    async drainOpEvents(opts?: OpEventQuery | number): Promise<StoredOpEvent[]> {
        this.ensureBookkeeping();
        const q = resolveOpEventQuery(opts);
        const dir = q.order === 'desc' ? 'DESC' : 'ASC';
        const where = q.beforeId === undefined ? 'id > ?' : 'id > ? AND id < ?';
        const sql = 'SELECT id, origin, direction, group_id, op_hash, op_json, kind, "table" AS tbl, '
            + 'row_hash, local_id, writes, author, reason FROM rdb_op_events WHERE ' + where
            + ' ORDER BY id ' + dir
            + (q.limit !== undefined ? ' LIMIT ?' : '');
        const params: number[] = q.beforeId === undefined ? [q.afterId] : [q.afterId, q.beforeId];
        if (q.limit !== undefined) params.push(q.limit);
        const rows = this.db.prepare(sql).all(...params) as {
                id: number; origin: string; direction: string; group_id: string; op_hash: string;
                op_json: string | null; kind: string; tbl: string | null; row_hash: string | null;
                local_id: number | null; writes: string | null; author: string | null; reason: string | null;
            }[];
        return rows.map((r) => {
            const event: OpEvent = {
                origin: r.origin as OpEvent['origin'], direction: r.direction as OpEvent['direction'],
                groupId: r.group_id as B64Hash, opHash: r.op_hash as B64Hash, kind: r.kind as OpEvent['kind'],
            };
            if (r.tbl !== null) event.table = r.tbl;
            if (r.row_hash !== null) event.rowId = r.row_hash as B64Hash;
            if (r.local_id !== null) event.localId = r.local_id;
            if (r.writes !== null) event.writes = JSON.parse(r.writes) as OpEventWrite[];
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
    // path, or unreliable fs.watch). Both paths fire only when MAX(id) grew, and
    // are epoch-gated so a disarm/re-arm can never let a stale notification fire.
    // -----------------------------------------------------------------------

    addCommitListener(listener: TargetCommitListener): void {
        this.commitListeners.add(listener);
        if (this.commitListeners.size === 1) this.armMonitor();
    }

    removeCommitListener(listener: TargetCommitListener): void {
        this.commitListeners.delete(listener);
        if (this.commitListeners.size === 0) this.disarmMonitor();
    }

    notifyExternalCommit(commit: TargetCommitEvent): void {
        // Another handle's commit can change tables: the meta of the tables an
        // apply names is read again, and all of it after a reconnect.
        if (commit.source === 'apply') for (const table of commit.schemaTables) this.metaCache.delete(table);
        else if (commit.source === 'reconnect') this.metaCache.clear();
        for (const listener of [...this.commitListeners]) listener(commit);
    }

    noteLocalCommit(): void {
        if (this.db.inTransaction) return;
        this.fireIfOutboxGrew(this.monitorEpoch);
    }

    private armMonitor(): void {
        this.ensureBookkeeping();
        if (this.walPath !== undefined) this.armWalWatch();
        else this.armPoll();
    }

    // Kernel-driven wake-up: fs.watch on the WAL file as a hint, then the same
    // MAX(id) filter as the poll. A prime on arm (and a second after kernel
    // registration) wakes an observer for an already-pending outbox; empty
    // outbox primes are silent.
    private armWalWatch(): void {
        const epoch = ++this.monitorEpoch;
        this.emittedOutboxIds.clear();
        const notify = (): void => { this.queueWatch(epoch); };
        this.walHandle = watchFile(this.walPath!, notify);
        notify();   // prime: wake an observer for an already-pending outbox promptly
        // Second prime once the kernel watcher is guaranteed registered. This
        // closes the arm-to-registration window where a write produces no vnode
        // event (an edge-triggered kqueue filter misses writes issued before its
        // EV_ADD): an observer that primes, then writes, then awaits is otherwise
        // never woken for that write. The epoch guard makes it a no-op if we were
        // disarmed in the meantime.
        this.monitorReady = this.walHandle.ready.then(notify);
    }

    private armPoll(): void {
        const epoch = ++this.monitorEpoch;
        this.emittedOutboxIds.clear();
        const tick = (): void => { this.queueWatch(epoch); };

        this.monitorTimer = setInterval(tick, this.pollMs);
        (this.monitorTimer as unknown as { unref?: () => void }).unref?.();
        tick();   // prime: wake an observer for an already-pending outbox promptly
        this.monitorReady = Promise.resolve();   // the interval is live on arm
    }

    private queueWatch(epoch: number): void {
        if (this.watchPending) { this.watchAgain = true; return; }
        this.watchPending = true;
        queueMicrotask(() => {
            this.watchPending = false;
            this.fireIfOutboxGrew(epoch);
            if (this.watchAgain) { this.watchAgain = false; this.queueWatch(epoch); }
        });
    }

    // Announce outbox rows this process has not announced yet. The remembered
    // set is only ids still in the outbox, so a burned AUTOINCREMENT id never
    // stalls the next row and a late commit is still announced.
    private fireIfOutboxGrew(epoch: number): void {
        if (epoch !== this.monitorEpoch) return;
        if (!this.capture) return;
        const rows = this.readOutboxIds();
        const fresh = rows.filter((row) => !this.emittedOutboxIds.has(row.id));
        this.emittedOutboxIds = new Set(rows.map((row) => row.id));
        if (fresh.length === 0) return;
        const tables = [...new Set(fresh.map((row) => row.tbl))];
        for (const listener of [...this.commitListeners]) listener({ source: 'local', tables });
    }

    private readOutboxIds(): { id: number; tbl: string }[] {
        try {
            return this.db.prepare('SELECT id, "table" AS tbl FROM rdb_outbox ORDER BY id').all() as { id: number; tbl: string }[];
        } catch {
            return [];
        }
    }

    private outboxMaxId(): number {
        try {
            return (this.db.prepare('SELECT COALESCE(MAX(id), 0) AS m FROM rdb_outbox').get() as { m: number }).m;
        } catch {
            return 0;   // outbox not present yet
        }
    }

    private disarmMonitor(): void {
        this.monitorEpoch++;
        this.monitorReady = undefined;
        if (this.monitorTimer !== undefined) {
            clearInterval(this.monitorTimer);
            this.monitorTimer = undefined;
        }
        if (this.walHandle !== undefined) {
            this.walHandle.close();
            this.walHandle = undefined;
        }
    }

    // Resolves once the currently-armed change monitor is guaranteed live (the
    // WAL file watcher is registered with the kernel, or immediately for the
    // poll). Resolves immediately when no monitor is armed. Await this after
    // addChangeListener, before a local write whose wake must be observed
    // deterministically - it removes the need to sleep past kernel-registration
    // latency (see armWalWatch / FileWatchHandle.ready).
    async whenChangeMonitorReady(): Promise<void> {
        await this.monitorReady;
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

function compileWhere(expr: BoundWhereExpr, params: Array<number | string | null>): string {
    switch (expr.kind) {
        case 'not': return `(NOT ${compileWhere(expr.arg, params)})`;
        case 'and': return `(${expr.args.map((arg) => compileWhere(arg, params)).join(' AND ')})`;
        case 'or': return `(${expr.args.map((arg) => compileWhere(arg, params)).join(' OR ')})`;
        case 'isNull': return `(${compileOperand(expr.arg, params)} IS ${expr.not ? 'NOT ' : ''}NULL)`;
        case 'like': return `(rdb_like_match(${compileOperand(expr.arg, params)}, ${compileOperand(expr.pattern, params)}) = 1)`;
        case 'in': {
            if (expr.values.length === 0) return expr.not ? '1' : '0';
            const arg = compileOperand(expr.arg, params);
            const marks = expr.values.map((value) => { params.push(sqlParam(value)); return '?'; });
            return `(${arg} ${expr.not ? 'NOT ' : ''}IN (${marks.join(', ')}))`;
        }
        case 'cmp': return `(${compileOperand(expr.left, params)} ${expr.op === '!=' ? '<>' : expr.op} ${compileOperand(expr.right, params)})`;
    }
}

function sqlParam(value: json.Literal | null): number | string | null {
    if (value === null) return null;
    if (typeof value === 'boolean') return value ? 1 : 0;
    if (typeof value === 'number' || typeof value === 'string') return value;
    return json.toStringNormalized(value);
}

function compileOperand(operand: BoundOperand, params: Array<number | string | null>): string {
    switch (operand.kind) {
        case 'col': return quoteId(operand.name);
        case 'lit': {
            if (operand.value === null) return 'NULL';
            params.push(sqlParam(operand.value));
            return '?';
        }
        case 'length': return `length(${compileOperand(operand.arg, params)})`;
        case 'arith': return `(${compileOperand(operand.left, params)} ${operand.op} ${compileOperand(operand.right, params)})`;
    }
}

// The commit hook better-sqlite3 does not expose. A successful outermost
// statement tells the target to look at the outbox. Nested statements and
// rollbacks leave `inTransaction` set or add no outbox row.
export function wrapDatabase(db: Database.Database, target: SqliteTarget): Database.Database {
    const wrapStatement = (statement: Database.Statement): Database.Statement => new Proxy(statement, {
        get(obj, prop, receiver) {
            if (prop === 'run') return (...args: unknown[]) => {
                const result = obj.run(...args);
                target.noteLocalCommit();
                return result;
            };
            return Reflect.get(obj, prop, receiver);
        },
    });
    return new Proxy(db, {
        get(obj, prop, receiver) {
            if (prop === 'prepare') return (sql: string) => wrapStatement(obj.prepare(sql));
            if (prop === 'exec') return (sql: string) => {
                const result = obj.exec(sql);
                target.noteLocalCommit();
                return result;
            };
            if (prop === 'transaction') return (fn: (...args: unknown[]) => unknown) => {
                const wrapped = obj.transaction(fn);
                return (...args: unknown[]) => {
                    const result = wrapped(...args);
                    target.noteLocalCommit();
                    return result;
                };
            };
            return Reflect.get(obj, prop, receiver);
        },
    });
}
