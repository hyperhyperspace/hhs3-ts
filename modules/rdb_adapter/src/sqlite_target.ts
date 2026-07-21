// SqliteTarget: the first executing MaterializationTarget. A self-contained
// SQLite backend on better-sqlite3 - no dag_sql, no dialect abstraction. All
// SQL here is SQLite-specific and written inline; a future Postgres target is
// its own full class (there are deeper per-engine nuances best kept isolated).
//
// State it owns, beyond the app + sync tables the schema actions create:
//   - rdb_checkpoint(version TEXT): the single materialized group Version,
//     stored as JSON.stringify([...Version]); drives initial-vs-delta.
//   - rdb_table_meta(...): one row per materialized app table recording its
//     system-column names, its sync table, and a {col: ColumnType} map. Read at
//     row-application time (and after a restart) so the target never introspects.
//
// Row identity: the content-addressed rowId maps to the projection-local serial
// `id` through the per-table sync table (`<table>_sync`). The sync row is
// allocated on first sight of a rowId and KEPT across delete, so a void-flip
// reinstatement reuses the same `id` (stable local identity).
//
// Atomicity: apply() runs schema actions THEN row actions THEN the checkpoint
// commit inside ONE better-sqlite3 transaction; a throw rolls the whole batch
// back, so the target never claims a checkpoint it does not reflect.

import { json } from "@hyper-hyper-space/hhs3_json";
import type { ColumnDef, ColumnType } from "@hyper-hyper-space/hhs3_rdb";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import Database from "better-sqlite3";

import { MaterializationTarget, RowAction, SchemaAction, SchemaActionColumn } from "./types.js";

// Per-table bookkeeping, mirrored in rdb_table_meta. Cached in memory within a
// process; lazily reloaded from the table after a restart.
type TableMeta = {
    idColumn: string;
    authorColumn: string | undefined;
    syncTable: string;
    columnTypes: { [column: string]: ColumnType };
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

export class SqliteTarget implements MaterializationTarget {
    private db: Database.Database;
    private metaCache = new Map<string, TableMeta>();
    private bookkeepingReady = false;

    constructor(db: Database.Database) {
        this.db = db;
    }

    async getCheckpoint(): Promise<Version | undefined> {
        this.ensureBookkeeping();
        const row = this.db.prepare('SELECT version FROM rdb_checkpoint LIMIT 1').get() as
            { version: string } | undefined;
        if (row === undefined) return undefined;
        return new Set(JSON.parse(row.version) as string[]);
    }

    async apply(schemaActions: SchemaAction[], rowActions: RowAction[], checkpoint: Version): Promise<void> {
        this.ensureBookkeeping();
        const run = this.db.transaction(() => {
            for (const action of schemaActions) this.applySchemaAction(action);
            for (const action of rowActions) this.applyRowAction(action);
            this.persistCheckpoint(checkpoint);
        });
        run();
    }

    // -----------------------------------------------------------------------
    // Bookkeeping + checkpoint
    // -----------------------------------------------------------------------

    private ensureBookkeeping(): void {
        if (this.bookkeepingReady) return;
        this.db.exec(
            'CREATE TABLE IF NOT EXISTS rdb_checkpoint (version TEXT);'
            + 'CREATE TABLE IF NOT EXISTS rdb_table_meta ('
            + '"table" TEXT PRIMARY KEY, id_column TEXT NOT NULL, author_column TEXT, '
            + 'sync_table TEXT NOT NULL, column_types TEXT NOT NULL);');
        this.bookkeepingReady = true;
    }

    private persistCheckpoint(checkpoint: Version): void {
        this.db.prepare('DELETE FROM rdb_checkpoint').run();
        this.db.prepare('INSERT INTO rdb_checkpoint (version) VALUES (?)')
            .run(JSON.stringify([...checkpoint]));
    }

    private writeMeta(table: string, meta: TableMeta): void {
        this.db.prepare(
            'INSERT OR REPLACE INTO rdb_table_meta '
            + '("table", id_column, author_column, sync_table, column_types) VALUES (?, ?, ?, ?, ?)')
            .run(table, meta.idColumn, meta.authorColumn ?? null, meta.syncTable,
                JSON.stringify(meta.columnTypes));
        this.metaCache.set(table, meta);
    }

    private loadMeta(table: string): TableMeta {
        const cached = this.metaCache.get(table);
        if (cached !== undefined) return cached;

        const row = this.db.prepare(
            'SELECT id_column, author_column, sync_table, column_types FROM rdb_table_meta WHERE "table" = ?')
            .get(table) as
            { id_column: string; author_column: string | null; sync_table: string; column_types: string }
            | undefined;
        if (row === undefined) throw new Error(`no materialized metadata for table '${table}'`);

        const meta: TableMeta = {
            idColumn: row.id_column,
            authorColumn: row.author_column ?? undefined,
            syncTable: row.sync_table,
            columnTypes: JSON.parse(row.column_types) as { [column: string]: ColumnType },
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
        const cols: string[] = [`${quoteId(action.primaryKey)} INTEGER PRIMARY KEY AUTOINCREMENT`];
        if (action.authorColumn !== undefined) cols.push(`${quoteId(action.authorColumn)} TEXT`);
        for (const c of action.columns) cols.push(columnDecl(c.name, c.def));

        this.db.exec(`CREATE TABLE ${quoteId(action.table)} (${cols.join(', ')})`);
        this.db.exec(
            `CREATE TABLE ${quoteId(action.syncTable)} (`
            + `"id" INTEGER PRIMARY KEY AUTOINCREMENT, `
            + `"row_hash" TEXT NOT NULL UNIQUE, "uuid" TEXT)`);

        const columnTypes: { [column: string]: ColumnType } = {};
        for (const c of action.columns) columnTypes[c.name] = c.def.type;
        this.writeMeta(action.table, {
            idColumn: action.primaryKey,
            authorColumn: action.authorColumn,
            syncTable: action.syncTable,
            columnTypes,
        });
    }

    private dropTable(action: Extract<SchemaAction, { kind: 'drop-table' }>): void {
        this.db.exec(`DROP TABLE ${quoteId(action.table)}`);
        this.db.exec(`DROP TABLE ${quoteId(action.syncTable)}`);
        this.db.prepare('DELETE FROM rdb_table_meta WHERE "table" = ?').run(action.table);
        this.metaCache.delete(action.table);
    }

    private addColumn(action: Extract<SchemaAction, { kind: 'add-column' }>): void {
        this.db.exec(`ALTER TABLE ${quoteId(action.table)} ADD COLUMN ${columnDecl(action.column, action.def)}`);
        const meta = this.loadMeta(action.table);
        meta.columnTypes[action.column] = action.def.type;
        this.writeMeta(action.table, meta);
    }

    private dropColumn(action: Extract<SchemaAction, { kind: 'drop-column' }>): void {
        this.db.exec(`ALTER TABLE ${quoteId(action.table)} DROP COLUMN ${quoteId(action.column)}`);
        const meta = this.loadMeta(action.table);
        delete meta.columnTypes[action.column];
        this.writeMeta(action.table, meta);
    }

    // -----------------------------------------------------------------------
    // Row channel
    // -----------------------------------------------------------------------

    private applyRowAction(action: RowAction): void {
        if (action.kind === 'upsert-row') return this.upsertRow(action);
        return this.deleteRow(action);
    }

    // Allocate (or reuse) the serial id for a rowId in the sync table.
    private allocateId(syncTable: string, rowId: string): number {
        this.db.prepare(`INSERT OR IGNORE INTO ${quoteId(syncTable)} ("row_hash") VALUES (?)`).run(rowId);
        const row = this.db.prepare(`SELECT "id" FROM ${quoteId(syncTable)} WHERE "row_hash" = ?`)
            .get(rowId) as { id: number };
        return row.id;
    }

    private upsertRow(action: Extract<RowAction, { kind: 'upsert-row' }>): void {
        const meta = this.loadMeta(action.table);
        const id = this.allocateId(meta.syncTable, action.rowId);

        const exists = this.db.prepare(
            `SELECT 1 FROM ${quoteId(action.table)} WHERE ${quoteId(meta.idColumn)} = ?`).get(id) !== undefined;

        const columns = Object.keys(action.values);
        const setAuthor = meta.authorColumn !== undefined && action.author !== undefined;

        if (exists) {
            const assignments: string[] = [];
            const params: (number | string)[] = [];
            for (const col of columns) {
                assignments.push(`${quoteId(col)} = ?`);
                params.push(toParam(action.values[col], meta.columnTypes[col] ?? 'string'));
            }
            if (setAuthor) {
                assignments.push(`${quoteId(meta.authorColumn!)} = ?`);
                params.push(action.author!);
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
        if (setAuthor) {
            insertCols.push(meta.authorColumn!);
            insertParams.push(action.author!);
        }
        for (const col of columns) {
            insertCols.push(col);
            insertParams.push(toParam(action.values[col], meta.columnTypes[col] ?? 'string'));
        }
        const placeholders = insertParams.map(() => '?').join(', ');
        this.db.prepare(
            `INSERT INTO ${quoteId(action.table)} (${insertCols.map(quoteId).join(', ')}) `
            + `VALUES (${placeholders})`).run(...insertParams);
    }

    private deleteRow(action: Extract<RowAction, { kind: 'delete-row' }>): void {
        const meta = this.loadMeta(action.table);
        // Delete only the app row; the sync row is intentionally kept so a
        // later void-flip reinstatement reuses the same serial id.
        this.db.prepare(
            `DELETE FROM ${quoteId(action.table)} WHERE ${quoteId(meta.idColumn)} = `
            + `(SELECT "id" FROM ${quoteId(meta.syncTable)} WHERE "row_hash" = ?)`).run(action.rowId);
    }
}
