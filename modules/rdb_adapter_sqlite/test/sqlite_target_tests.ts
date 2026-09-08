import * as os from "node:os";
import * as path from "node:path";
import * as fs from "node:fs";
import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { json } from "@hyper-hyper-space/hhs3_json";
import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { ColumnType } from "@hyper-hyper-space/hhs3_rdb";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import Database from "better-sqlite3";

import {
    CheckpointMovedError, ingestChanges, projectGroup, SchemaAction, RowAction, SyncMapping,
} from "@hyper-hyper-space/hhs3_rdb_adapter";
import {
    createGroup, sameVersion,
    TargetHarness, ProjectionReader, ReadRow, RowValues,
    IngestionHarness, LocalMutator,
} from "@hyper-hyper-space/hhs3_rdb_adapter_test";
import type { Row, RTableGroup } from "@hyper-hyper-space/hhs3_rdb";

import { SqliteTarget } from "../src/sqlite_target.js";

// Live rows of an rdb group table (what ingestion/projection actually wrote).
async function groupRows(group: RTableGroup, table: string): Promise<Row[]> {
    const view = await (await group.getTable(table)).getView();
    return view.query({});
}

function quoteId(name: string): string {
    return '"' + name.replace(/"/g, '""') + '"';
}

type MetaRow = {
    id_column: string;
    author_column: string | null;
    sync_table: string;
    column_types: string;
};

function toLogical(value: unknown, type: ColumnType): json.Literal | undefined {
    if (value === null || value === undefined) return undefined;
    if (type === 'boolean') return value !== 0;
    if (type === 'json') return JSON.parse(value as string);
    return value as json.Literal;
}

// A ProjectionReader over a SQLite db handle: it inverts stored values to
// logical values using the column types recorded in rdb_table_meta.
function sqliteReader(db: Database.Database): ProjectionReader {
    const meta = (table: string): MetaRow | undefined =>
        db.prepare('SELECT id_column, author_column, sync_table, column_types FROM rdb_table_meta WHERE "table" = ?')
            .get(table) as MetaRow | undefined;

    return {
        hasTable: (table) =>
            db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(table) !== undefined,

        listTables: () => {
            const rows = db.prepare('SELECT "table" AS t FROM rdb_table_meta').all() as { t: string }[];
            return rows.map((r) => r.t);
        },

        getRowIds: (table) => {
            const m = meta(table);
            if (m === undefined) return [];
            const rows = db.prepare(
                `SELECT s."row_hash" AS rh FROM ${quoteId(m.sync_table)} s `
                + `JOIN ${quoteId(table)} t ON t.${quoteId(m.id_column)} = s."id"`).all() as { rh: string }[];
            return rows.map((r) => r.rh);
        },

        getRow: (table, rowId) => {
            const m = meta(table);
            if (m === undefined) return undefined;
            const sync = db.prepare(`SELECT "id" FROM ${quoteId(m.sync_table)} WHERE "row_hash" = ?`)
                .get(rowId) as { id: number } | undefined;
            if (sync === undefined) return undefined;
            const appRow = db.prepare(`SELECT * FROM ${quoteId(table)} WHERE ${quoteId(m.id_column)} = ?`)
                .get(sync.id) as Record<string, unknown> | undefined;
            if (appRow === undefined) return undefined;

            const columnTypes = JSON.parse(m.column_types) as { [column: string]: ColumnType };
            const values: RowValues = {};
            for (const [col, type] of Object.entries(columnTypes)) {
                const logical = toLogical(appRow[col], type);
                if (logical !== undefined) values[col] = logical;
            }
            const result: ReadRow = { values };
            if (m.author_column !== null && appRow[m.author_column] != null) {
                result.author = appRow[m.author_column] as number;
            }
            return result;
        },

        syncId: (table, rowId) => {
            const m = meta(table);
            if (m === undefined) return undefined;
            const sync = db.prepare(`SELECT "id" FROM ${quoteId(m.sync_table)} WHERE "row_hash" = ?`)
                .get(rowId) as { id: number } | undefined;
            return sync?.id;
        },

        columnType: (table, column) => {
            const m = meta(table);
            if (m === undefined) return undefined;
            const columnTypes = JSON.parse(m.column_types) as { [column: string]: ColumnType };
            return columnTypes[column];
        },
    };
}

// The SQLite harness for the shared conformance suite: a fresh in-memory db +
// SqliteTarget + a reader over the same handle, closed after each test.
export function sqliteHarness(): TargetHarness {
    const db = new Database(':memory:');
    return { target: new SqliteTarget(db), read: sqliteReader(db), cleanup: () => { db.close(); } };
}

function toParam(value: json.Literal, type: ColumnType): number | string {
    if (type === 'boolean') return value ? 1 : 0;
    if (type === 'json') return json.toStringNormalized(value);
    return value as number | string;
}

// The app-side write surface for the ingestion suite: raw SQL writes to the
// projected tables, exactly as the host application would issue them, so the
// capture triggers observe genuine local mutations.
function sqliteLocal(db: Database.Database, target: SqliteTarget): LocalMutator {
    const meta = (table: string): MetaRow =>
        db.prepare('SELECT id_column, author_column, sync_table, column_types FROM rdb_table_meta WHERE "table" = ?')
            .get(table) as MetaRow;

    return {
        insert: (table, values, author) => {
            const m = meta(table);
            const ct = JSON.parse(m.column_types) as { [column: string]: ColumnType };
            const cols: string[] = [];
            const params: (number | string)[] = [];
            if (author !== undefined && m.author_column !== null) { cols.push(m.author_column); params.push(author); }
            for (const [c, v] of Object.entries(values)) { cols.push(c); params.push(toParam(v, ct[c] ?? 'string')); }
            const placeholders = params.map(() => '?').join(', ');
            const info = db.prepare(
                `INSERT INTO ${quoteId(table)} (${cols.map(quoteId).join(', ')}) VALUES (${placeholders})`).run(...params);
            return Number(info.lastInsertRowid);
        },
        update: (table, localId, values) => {
            const m = meta(table);
            const ct = JSON.parse(m.column_types) as { [column: string]: ColumnType };
            const sets: string[] = [];
            const params: (number | string)[] = [];
            for (const [c, v] of Object.entries(values)) { sets.push(`${quoteId(c)} = ?`); params.push(toParam(v, ct[c] ?? 'string')); }
            params.push(localId);
            db.prepare(`UPDATE ${quoteId(table)} SET ${sets.join(', ')} WHERE ${quoteId(m.id_column)} = ?`).run(...params);
        },
        delete: (table, localId) => {
            const m = meta(table);
            db.prepare(`DELETE FROM ${quoteId(table)} WHERE ${quoteId(m.id_column)} = ?`).run(localId);
        },
        setCaptureEnabled: (on) => target.setCaptureEnabled(on),
    };
}

// The SQLite harness for the shared change-ingestion suite: a capture-
// provisioned SqliteTarget plus a raw-SQL local mutator over the same handle.
export function sqliteIngestionHarness(): IngestionHarness {
    const db = new Database(':memory:');
    const target = new SqliteTarget(db, { captureChanges: true });
    return { target, read: sqliteReader(db), local: sqliteLocal(db, target), cleanup: () => { db.close(); } };
}

// ---------------------------------------------------------------------------
// SQLite-specific assertions (native affinity, NOT NULL, DEFAULT rendering,
// pragma introspection, better-sqlite3 transaction rollback) - the facts the
// backend-agnostic suite intentionally does not cover.
// ---------------------------------------------------------------------------

type ColInfo = { name: string; type: string; notnull: number; pk: number; dflt_value: string | null };
function tableInfo(db: Database.Database, table: string): ColInfo[] {
    return db.prepare('SELECT name, type, "notnull", pk, dflt_value FROM pragma_table_info(?)').all(table) as ColInfo[];
}

function tableExists(db: Database.Database, table: string): boolean {
    return db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(table) !== undefined;
}

function tmpDbPath(label: string): string {
    return path.join(os.tmpdir(), `hhs3-adpts-${label}-${Date.now()}-${Math.random().toString(36).slice(2)}.sqlite`);
}

function cleanupDb(p: string): void {
    for (const suffix of ['', '-wal', '-shm']) {
        try { fs.unlinkSync(p + suffix); } catch (_e) { /* ignore */ }
    }
}

export const sqliteSpecificTests = {
    title: '[ADPTS] rdb_adapter SQLite target (engine-specific)',
    tests: [
        {
            name: '[ADPTS-SQL01] native column affinity, NOT NULL, and id/author shape',
            invoke: async () => {
                const { group, admin } = await createGroup();
                const ledger = await group.getTable('ledger');
                await ledger.insert('l1', { ref: 'R-1', amount: '10.00', memo: 'paid' }, admin);

                const db = new Database(':memory:');
                const target = new SqliteTarget(db);
                await projectGroup(group, target);

                const cols = tableInfo(db, 'ledger');
                const byName = new Map(cols.map((c) => [c.name, c]));
                assertTrue(byName.get('id')?.pk === 1 && byName.get('id')?.type === 'INTEGER',
                    'id is the INTEGER primary key');
                assertTrue(byName.has('author_key_id'), 'author_key_id system column materialized');
                assertEquals(byName.get('author_key_id')?.type, 'INTEGER', 'author_key_id is INTEGER');
                assertEquals(cols[cols.length - 1]?.name, 'author_key_id',
                    'author_key_id is the last column');
                assertEquals(byName.get('ref')?.type, 'TEXT', 'string maps to TEXT');
                assertEquals(byName.get('amount')?.type, 'TEXT', 'decimal maps to TEXT (canonical carrier)');
                assertTrue(byName.get('amount')?.notnull === 1, 'non-nullable decimal is NOT NULL');
                assertTrue(byName.get('memo')?.notnull === 0, 'nullable string is not NOT NULL');
                assertTrue(tableExists(db, 'tags') && tableExists(db, 'tags_sync'),
                    'empty table gets its app + sync table');
                db.close();
            },
        },
        {
            name: '[ADPTS-SQL02] DDL via hand-fed actions: add-column (default), drop-column, drop-table (+sync)',
            invoke: async () => {
                const db = new Database(':memory:');
                const target = new SqliteTarget(db);
                const gid = 'g';
                const v1: Version = new Set(['v1']);

                const create: SchemaAction = {
                    kind: 'create-table', table: 'acct', syncTable: 'acct_sync', primaryKey: 'id',
                    authorColumn: 'author_key_id',
                    columns: [
                        { name: 'ref', def: { type: 'string' } },
                        { name: 'balance', def: { type: 'decimal', constraints: { scale: 2 } } },
                    ],
                };
                await target.apply(gid, [create], [], v1);
                assertTrue(tableExists(db, 'acct') && tableExists(db, 'acct_sync'), 'create-table makes both tables');

                // add-column with a NOT NULL + DEFAULT (SQLite ADD COLUMN rule).
                const v2: Version = new Set(['v2']);
                const addCol: SchemaAction = {
                    kind: 'add-column', table: 'acct', column: 'status',
                    def: { type: 'string', default: 'open' },
                };
                await target.apply(gid, [addCol], [], v2);
                const status = tableInfo(db, 'acct').find((c) => c.name === 'status');
                assertTrue(status !== undefined && status.type === 'TEXT' && status.notnull === 1,
                    'added column present, NOT NULL');
                assertEquals(status!.dflt_value ?? undefined, "'open'", 'default literal quoted as TEXT');

                // drop-column.
                const v3: Version = new Set(['v3']);
                await target.apply(gid, [{ kind: 'drop-column', table: 'acct', column: 'balance' }], [], v3);
                assertTrue(!tableInfo(db, 'acct').some((c) => c.name === 'balance'), 'dropped column gone');

                // drop-table removes the app + sync tables.
                const v4: Version = new Set(['v4']);
                await target.apply(gid, [{ kind: 'drop-table', table: 'acct', syncTable: 'acct_sync' }], [], v4);
                assertTrue(!tableExists(db, 'acct') && !tableExists(db, 'acct_sync'),
                    'drop-table removes app and sync tables');
                assertTrue(sameVersion(await target.getCheckpoint(gid), v4), 'checkpoint tracks each DDL apply');
                db.close();
            },
        },
        {
            name: '[ADPTS-SQL03] atomicity via better-sqlite3 transaction rollback',
            invoke: async () => {
                const db = new Database(':memory:');
                const target = new SqliteTarget(db);
                const v1: Version = new Set(['v1']);

                const create: SchemaAction = {
                    kind: 'create-table', table: 'acct', syncTable: 'acct_sync', primaryKey: 'id',
                    columns: [{ name: 'ref', def: { type: 'string' } }],
                };
                const badRow = { kind: 'upsert-row' as const, table: 'ghost', rowId: 'r1', values: { ref: 'x' } };

                let threw = false;
                try {
                    await target.apply('g', [create], [badRow], v1);
                } catch {
                    threw = true;
                }
                assertTrue(threw, 'apply throws when a row action references an unknown table');
                assertTrue(!tableExists(db, 'acct'), 'create-table rolled back with the failed batch');
                assertEquals(await target.getCheckpoint('g'), undefined, 'checkpoint not advanced on rollback');
                db.close();
            },
        },
        {
            name: '[ADPTS-SQL04] ChangeSignalSource fires when a local write lands in the outbox',
            invoke: async () => {
                const { group, admin } = await createGroup();
                const ledger = await group.getTable('ledger');
                await ledger.insert('l1', { ref: 'R-1', amount: '10.00' }, admin);

                const db = new Database(':memory:');
                const target = new SqliteTarget(db, { captureChanges: true, pollMs: 10 });
                await projectGroup(group, target);

                const signalled = new Promise<void>((resolve, reject) => {
                    const timer = setTimeout(() => reject(new Error('no change signal within timeout')), 2000);
                    target.addChangeListener(() => { clearTimeout(timer); resolve(); });
                });

                // A genuine local write on the same handle (as the host app would).
                db.prepare('INSERT INTO tags (code) VALUES (?)').run('urgent');

                await signalled;
                target.removeChangeListener(() => undefined);   // disarm path (no-op listener id)
                db.close();
            },
        },
        {
            name: '[ADPTS-SQL04b] ChangeSignalSource via WAL watch fires on a local write (file-backed db)',
            invoke: async () => {
                const { group, admin } = await createGroup();
                const ledger = await group.getTable('ledger');
                await ledger.insert('l1', { ref: 'R-1', amount: '10.00' }, admin);

                const dbPath = tmpDbPath('walsig');
                const db = new Database(dbPath);
                // dbPath -> WAL watching (not polling). The target enables WAL
                // journal mode itself so the `-wal` file exists to watch.
                const target = new SqliteTarget(db, { captureChanges: true, dbPath });
                let listener = (): void => undefined;
                try {
                    await projectGroup(group, target);

                    let fires = 0;
                    listener = (): void => { fires++; };
                    target.addChangeListener(listener);
                    // Empty outbox: arm primes are silent (MAX(id) filter). Await
                    // kernel registration so the INSERT below is not lost in the
                    // attach window.
                    await target.whenChangeMonitorReady();
                    assertEquals(fires, 0, 'empty outbox does not prime the WAL watcher');
                    const baseline = fires;

                    // A genuine local write on the same handle (as the host app would).
                    db.prepare('INSERT INTO tags (code) VALUES (?)').run('urgent');

                    // With the watcher registered, the write must produce an
                    // fs.watch wake. Delivery is inherently asynchronous, so wait
                    // (bounded) for the fire count to advance past the baseline.
                    const deadline = Date.now() + 3000;
                    while (fires <= baseline && Date.now() < deadline) {
                        await new Promise((resolve) => setTimeout(resolve, 20));
                    }
                    assertTrue(fires > baseline,
                        `a local write should wake the WAL watcher (baseline ${baseline}, got ${fires})`);
                } finally {
                    target.removeChangeListener(listener);
                    db.close();
                    cleanupDb(dbPath);
                }
            },
        },
        {
            name: '[ADPTS-SQL04c] empty apply at the current checkpoint is a no-op (no sqlite writes)',
            invoke: async () => {
                const { group, admin } = await createGroup();
                const ledger = await group.getTable('ledger');
                await ledger.insert('l1', { ref: 'R-1', amount: '10.00' }, admin);

                const db = new Database(':memory:');
                const target = new SqliteTarget(db, { captureChanges: true });
                await projectGroup(group, target);

                const gid = group.getId();
                const current = await target.getCheckpoint(gid);
                assertTrue(current !== undefined, 'checkpoint exists after projection');

                const before = (db.prepare('SELECT total_changes() AS n').get() as { n: number }).n;
                await target.apply(gid, [], [], current!, undefined, current);
                const after = (db.prepare('SELECT total_changes() AS n').get() as { n: number }).n;
                assertEquals(after, before, 'empty apply at the current checkpoint writes nothing');

                await projectGroup(group, target);
                const afterIdle = (db.prepare('SELECT total_changes() AS n').get() as { n: number }).n;
                assertEquals(afterIdle, before, 'idle projectGroup does not apply');
                db.close();
            },
        },
        {
            name: '[ADPTS-SQL04d] WAL watch ignores checkpoint writes; fires only when the outbox grows',
            invoke: async () => {
                const { group, admin } = await createGroup();
                const ledger = await group.getTable('ledger');
                await ledger.insert('l1', { ref: 'R-1', amount: '10.00' }, admin);

                const dbPath = tmpDbPath('waloutbox');
                const db = new Database(dbPath);
                const target = new SqliteTarget(db, { captureChanges: true, dbPath });
                let listener = (): void => undefined;
                try {
                    await projectGroup(group, target);

                    let fires = 0;
                    listener = (): void => { fires++; };
                    target.addChangeListener(listener);
                    await target.whenChangeMonitorReady();
                    assertEquals(fires, 0, 'empty outbox on arm does not signal');

                    await projectGroup(group, target);
                    await new Promise((resolve) => setTimeout(resolve, 50));
                    assertEquals(fires, 0, 'idle projectGroup does not signal');

                    // Catch-up persist of a new checkpoint with empty actions still
                    // writes WAL (setApplying + INSERT OR REPLACE) but must not signal.
                    const gid = group.getId();
                    const current = await target.getCheckpoint(gid);
                    const ahead = new Set([...(current ?? []), 'idle-catchup']);
                    await target.apply(gid, [], [], ahead, undefined, current);
                    const deadlineQuiet = Date.now() + 400;
                    while (Date.now() < deadlineQuiet) {
                        await new Promise((resolve) => setTimeout(resolve, 20));
                    }
                    assertEquals(fires, 0, 'checkpoint-only apply does not signal (outbox unchanged)');

                    db.prepare('INSERT INTO tags (code) VALUES (?)').run('urgent');
                    const deadline = Date.now() + 3000;
                    while (fires === 0 && Date.now() < deadline) {
                        await new Promise((resolve) => setTimeout(resolve, 20));
                    }
                    assertTrue(fires > 0, `an app INSERT should signal (got ${fires})`);
                } finally {
                    target.removeChangeListener(listener);
                    db.close();
                    cleanupDb(dbPath);
                }
            },
        },
        {
            name: '[ADPTS-SQL04e] close() checkpoints WAL/SHM and is idempotent',
            invoke: async () => {
                const { group, admin } = await createGroup();
                const ledger = await group.getTable('ledger');
                await ledger.insert('l1', { ref: 'R-1', amount: '10.00' }, admin);

                const dbPath = tmpDbPath('closewal');
                const db = new Database(dbPath);
                const target = new SqliteTarget(db, { captureChanges: true, dbPath });
                try {
                    await projectGroup(group, target);
                    target.close();
                    assertTrue(!fs.existsSync(dbPath + '-wal'), 'WAL file is gone after close');
                    assertTrue(!fs.existsSync(dbPath + '-shm'), 'SHM file is gone after close');
                    target.close();  // second close is a no-op

                    let threw = false;
                    try {
                        await target.apply(group.getId(), [], [], new Set(['x']));
                    } catch (e) {
                        threw = e instanceof Error && e.message === 'SqliteTarget is closed';
                    }
                    assertTrue(threw, 'apply after close throws a clear error');
                    assertTrue(fs.existsSync(dbPath), 'the main db file remains');
                } finally {
                    try { target.close(); } catch { /* already closed */ }
                    cleanupDb(dbPath);
                }
            },
        },
        {
            name: '[ADPTS-SQL05] a co-projected cross-group FK declares no DB-level FOREIGN KEY',
            invoke: async () => {
                const db = new Database(':memory:');
                const target = new SqliteTarget(db);
                const v1: Version = new Set(['v1']);

                // Parent (foreign group) table first.
                await target.apply('gB', [{
                    kind: 'create-table', table: 'archive_entries', syncTable: 'archive_entries_sync',
                    primaryKey: 'id', columns: [{ name: 'title', def: { type: 'string' } }],
                }], [], v1);

                // Child with a co-projected cross-group FK to it (crossGroup: true).
                await target.apply('gA', [{
                    kind: 'create-table', table: 'comments', syncTable: 'comments_sync', primaryKey: 'id',
                    columns: [
                        { name: 'body', def: { type: 'string' } },
                        { name: 'origin_id', def: { type: 'integer', nullable: true },
                            fk: { targetTable: 'archive_entries', crossGroup: true } },
                    ],
                }], [], v1);

                const fkList = db.prepare('SELECT * FROM pragma_foreign_key_list(?)').all('comments');
                assertEquals(fkList.length, 0, 'a co-projected cross-group FK declares no DB-level foreign key');
                assertTrue(tableInfo(db, 'comments').some((c) => c.name === 'origin_id' && c.type === 'INTEGER'),
                    'the cross-group FK still projects as an integer id companion');
                db.close();
            },
        },
        {
            name: '[ADPTS-SQL06] ADD COLUMN required no-default on an empty table is direct NOT NULL',
            invoke: async () => {
                const db = new Database(':memory:');
                const target = new SqliteTarget(db);
                const gid = 'g';
                await target.apply(gid, [{
                    kind: 'create-table', table: 'acct', syncTable: 'acct_sync', primaryKey: 'id',
                    columns: [{ name: 'ref', def: { type: 'string' } }],
                }], [], new Set(['v1']));

                await target.apply(gid, [{
                    kind: 'add-column', table: 'acct', column: 'status',
                    def: { type: 'string' },
                }], [], new Set(['v2']));

                const status = tableInfo(db, 'acct').find((c) => c.name === 'status');
                assertTrue(status !== undefined && status.type === 'TEXT' && status.notnull === 1,
                    'empty-table ADD COLUMN required no-default is NOT NULL');
                assertTrue(!tableExists(db, 'acct__rebuild'), 'no leftover rebuild table');
                db.close();
            },
        },
        {
            name: '[ADPTS-SQL07] ADD COLUMN required no-default on a non-empty table tightens to NOT NULL',
            invoke: async () => {
                const db = new Database(':memory:');
                const target = new SqliteTarget(db);
                const gid = 'g';
                const insert: RowAction = {
                    kind: 'upsert-row', table: 'acct', rowId: 'r1', values: { ref: 'a', extra: 'x' },
                };
                await target.apply(gid, [{
                    kind: 'create-table', table: 'acct', syncTable: 'acct_sync', primaryKey: 'id',
                    columns: [
                        { name: 'ref', def: { type: 'string' } },
                        { name: 'extra', def: { type: 'string' } },
                    ],
                }], [insert], new Set(['v1']));

                // Drop+add of a required no-default column (the set-fks flip shape)
                // with a backfill upsert supplying the new cell.
                const backfill: RowAction = {
                    kind: 'upsert-row', table: 'acct', rowId: 'r1', values: { status: 'open' },
                };
                await target.apply(gid, [
                    { kind: 'drop-column', table: 'acct', column: 'extra' },
                    { kind: 'add-column', table: 'acct', column: 'status', def: { type: 'string' } },
                ], [backfill], new Set(['v2']));

                const cols = tableInfo(db, 'acct');
                const status = cols.find((c) => c.name === 'status');
                assertTrue(status !== undefined && status.type === 'TEXT' && status.notnull === 1,
                    'tightened column is NOT NULL after backfill');
                assertTrue(!cols.some((c) => c.name === 'extra'), 'dropped column is gone');
                const row = db.prepare('SELECT status FROM acct').get() as { status: string };
                assertEquals(row.status, 'open', 'backfill value survived the tighten rebuild');
                assertTrue(!tableExists(db, 'acct__rebuild'), 'no leftover rebuild table');
                db.close();
            },
        },
        {
            name: '[ADPTS-SQL08] tighten throws when backfill leaves a live row NULL (batch rolls back)',
            invoke: async () => {
                const db = new Database(':memory:');
                const target = new SqliteTarget(db);
                const gid = 'g';
                const v1: Version = new Set(['v1']);
                await target.apply(gid, [{
                    kind: 'create-table', table: 'acct', syncTable: 'acct_sync', primaryKey: 'id',
                    columns: [{ name: 'ref', def: { type: 'string' } }],
                }], [{ kind: 'upsert-row', table: 'acct', rowId: 'r1', values: { ref: 'a' } }], v1);

                const v2: Version = new Set(['v2']);
                let threw = false;
                try {
                    await target.apply(gid, [{
                        kind: 'add-column', table: 'acct', column: 'status',
                        def: { type: 'string' },
                    }], [], v2);
                } catch {
                    threw = true;
                }
                assertTrue(threw, 'apply throws when a required no-default add is not backfilled');
                assertTrue(!tableInfo(db, 'acct').some((c) => c.name === 'status'),
                    'failed ADD COLUMN rolled back');
                assertTrue(sameVersion(await target.getCheckpoint(gid), v1),
                    'checkpoint not advanced on tighten failure');
                db.close();
            },
        },
        {
            name: '[ADPTS-SQL09] inbound FK on another table survives a tighten rebuild of its target',
            invoke: async () => {
                const db = new Database(':memory:');
                const target = new SqliteTarget(db);
                const gid = 'g';
                await target.apply(gid, [{
                    kind: 'create-table', table: 'orders', syncTable: 'orders_sync', primaryKey: 'id',
                    columns: [{ name: 'customer', def: { type: 'string' } }],
                }], [{
                    kind: 'upsert-row', table: 'orders', rowId: 'o1', values: { customer: 'acme' },
                }], new Set(['v1']));

                await target.apply(gid, [{
                    kind: 'create-table', table: 'lines', syncTable: 'lines_sync', primaryKey: 'id',
                    columns: [
                        { name: 'qty', def: { type: 'integer' } },
                        { name: 'order_id', def: { type: 'integer' }, fk: { targetTable: 'orders' } },
                    ],
                }], [{
                    kind: 'upsert-row', table: 'lines', rowId: 'l1',
                    values: { qty: 2, order_id: 'o1' },
                }], new Set(['v2']));

                const before = db.prepare('SELECT "table" AS t, "from", "to" FROM pragma_foreign_key_list(?)')
                    .all('lines') as { t: string; from: string; to: string }[];
                assertTrue(before.some((f) => f.from === 'order_id' && f.t === 'orders' && f.to === 'id'),
                    'lines.order_id references orders.id before tighten');

                await target.apply(gid, [{
                    kind: 'add-column', table: 'orders', column: 'note',
                    def: { type: 'string' },
                }], [{
                    kind: 'upsert-row', table: 'orders', rowId: 'o1', values: { note: 'rush' },
                }], new Set(['v3']));

                const note = tableInfo(db, 'orders').find((c) => c.name === 'note');
                assertTrue(note !== undefined && note.notnull === 1,
                    'tightened column on the FK target is NOT NULL');
                const after = db.prepare('SELECT "table" AS t, "from", "to" FROM pragma_foreign_key_list(?)')
                    .all('lines') as { t: string; from: string; to: string }[];
                assertTrue(after.some((f) => f.from === 'order_id' && f.t === 'orders' && f.to === 'id'),
                    'lines.order_id still references orders.id after the target rebuild');
                const child = db.prepare('SELECT qty FROM lines').get() as { qty: number };
                assertEquals(child.qty, 2, 'referencing row survived the target rebuild');
                db.close();
            },
        },
        {
            name: '[ADPTS-SQL10] the app PK is AUTOINCREMENT: a local id is never reused after a delete, and sync id is unique',
            invoke: async () => {
                const { group } = await createGroup();
                const db = new Database(':memory:');
                const target = new SqliteTarget(db, { captureChanges: true });
                await projectGroup(group, target);

                // The DDL declares AUTOINCREMENT on the PK and a UNIQUE index on the
                // sync table's id (the never-reuse contract the adapter relies on).
                const tagsSql = (db.prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name='tags'")
                    .get() as { sql: string }).sql;
                assertTrue(/AUTOINCREMENT/i.test(tagsSql), 'the app PK is INTEGER PRIMARY KEY AUTOINCREMENT');
                const syncIdx = db.prepare(
                    "SELECT 1 FROM sqlite_master WHERE type='index' AND tbl_name='tags_sync' AND sql LIKE '%UNIQUE%'").get();
                assertTrue(syncIdx !== undefined, 'the sync table has a unique index on id');

                // Insert, delete, insert: the second insert must NOT reuse id 1.
                const first = Number(db.prepare('INSERT INTO tags (code) VALUES (?)').run('a').lastInsertRowid);
                assertEquals(first, 1, 'first local id is 1');
                db.prepare('DELETE FROM tags WHERE id = ?').run(first);
                const second = Number(db.prepare('INSERT INTO tags (code) VALUES (?)').run('b').lastInsertRowid);
                assertEquals(second, 2, 'the reused-after-delete id is 2, never 1');
                db.close();
            },
        },
        {
            name: '[ADPTS-SQL11] reserveMint is claim-or-adopt: a second identity for one local id adopts the first',
            invoke: async () => {
                const { group } = await createGroup();
                const db = new Database(':memory:');
                const target = new SqliteTarget(db, { captureChanges: true });
                await projectGroup(group, target);

                const first: SyncMapping = { table: 'tags', localId: 1, rowId: 'ROWA', uuid: 'ua', status: 'active' };
                const claimed = await target.reserveMint([first]);
                assertEquals(claimed.length, 1, 'one effective mapping returned');
                assertEquals(claimed[0].rowId, 'ROWA', 'the free id is claimed by the caller');

                // A DIFFERENT identity for the same id: INSERT OR IGNORE hits the
                // unique-id index and the read-back returns the incumbent (adopt).
                const second: SyncMapping = { table: 'tags', localId: 1, rowId: 'ROWB', uuid: 'ub', status: 'active' };
                const adopted = await target.reserveMint([second]);
                assertEquals(adopted[0].rowId, 'ROWA', 'the second minter adopts the incumbent identity');
                assertEquals(adopted[0].uuid, 'ua', 'and its uuid');

                const rows = db.prepare('SELECT "row_hash" AS rh, "id" FROM tags_sync WHERE "id" = 1').all() as
                    { rh: string; id: number }[];
                assertEquals(rows.length, 1, 'exactly one sync row for the id (no duplicate)');
                assertEquals(rows[0].rh, 'ROWA', 'the incumbent row_hash is kept');
            },
        },
        {
            name: '[ADPTS-SQL12] two handles ingesting one file converge to a single rdb row per local insert',
            invoke: async () => {
                const { group, admin } = await createGroup();
                const dbPath = tmpDbPath('ingest-race');
                const dbA = new Database(dbPath); dbA.pragma('busy_timeout = 5000');
                const targetA = new SqliteTarget(dbA, { captureChanges: true, dbPath });
                try {
                    await projectGroup(group, targetA);   // schema + capture triggers on the file
                    dbA.prepare('INSERT INTO tags (code) VALUES (?)').run('x');   // one local insert

                    // A second handle/target on the SAME file, same in-memory group.
                    const dbB = new Database(dbPath); dbB.pragma('busy_timeout = 5000');
                    const targetB = new SqliteTarget(dbB, { captureChanges: true, dbPath });

                    const [rA, rB] = await Promise.all([
                        ingestChanges(group, targetA, { writer: admin }),
                        ingestChanges(group, targetB, { writer: admin }),
                    ]);

                    // Claim-or-adopt makes both submit the same identity. The
                    // second append is usually a write-once skip, but two
                    // concurrent bundle()s on the same in-memory group can both
                    // pass validation and still converge (same incarnation).
                    // The guarantee is one rdb row, not one accepted count.
                    assertEquals(rA.rejected.length + rB.rejected.length, 0, 'no rejections');

                    const rows = (await groupRows(group, 'tags')).filter((r) => r.values.code === 'x');
                    assertEquals(rows.length, 1, 'exactly one rdb tag row, not two');

                    const syncRows = dbA.prepare('SELECT "id" FROM tags_sync').all() as { id: number }[];
                    assertEquals(syncRows.length, 1, 'one sync mapping (ids are unique)');
                    assertEquals((dbA.prepare('SELECT COUNT(*) AS n FROM rdb_outbox').get() as { n: number }).n, 0,
                        'the outbox is acked by both passes');
                    assertEquals((dbA.prepare("SELECT COUNT(*) AS n FROM rdb_op_events WHERE direction='failure'")
                        .get() as { n: number }).n, 0, 'no ingestion/failure events');
                    dbB.close();
                } finally {
                    dbA.close();
                    cleanupDb(dbPath);
                }
            },
        },
        {
            name: '[ADPTS-SQL13] two handles projecting one file: exactly one applies the delta, the other gets CheckpointMovedError',
            invoke: async () => {
                const { group, admin } = await createGroup();
                const dbPath = tmpDbPath('project-race');
                const dbA = new Database(dbPath); dbA.pragma('busy_timeout = 5000');
                const targetA = new SqliteTarget(dbA, { dbPath });
                try {
                    await projectGroup(group, targetA);   // initial: creates tables + checkpoint v1

                    // Advance the rdb group so both handles have a real delta to apply.
                    await (await group.getTable('tags')).insert('t1', { code: 'y' }, admin);

                    const dbB = new Database(dbPath); dbB.pragma('busy_timeout = 5000');
                    const targetB = new SqliteTarget(dbB, { dbPath });

                    const outcomes = await Promise.allSettled([
                        projectGroup(group, targetA),
                        projectGroup(group, targetB),
                    ]);
                    const fulfilled = outcomes.filter((o) => o.status === 'fulfilled');
                    const rejected = outcomes.filter((o) => o.status === 'rejected') as PromiseRejectedResult[];
                    assertEquals(fulfilled.length, 1, 'exactly one projector applied the delta');
                    assertEquals(rejected.length, 1, 'the other was rejected');
                    assertTrue(rejected[0].reason instanceof CheckpointMovedError,
                        'the loser got a CheckpointMovedError (compare-and-set)');

                    // Final state is correct and applied exactly once.
                    const read = sqliteReader(dbA);
                    const rowIds = await read.getRowIds('tags');
                    assertEquals(rowIds.length, 1, 'the new tag is materialized exactly once');
                    const cp = await targetA.getCheckpoint(group.getId());
                    const frontier = await (await group.getScopedDag()).getFrontier();
                    assertTrue(sameVersion(cp, frontier), 'the file checkpoint advanced to the group frontier');
                    dbB.close();
                } finally {
                    dbA.close();
                    cleanupDb(dbPath);
                }
            },
        },
        {
            name: '[ADPTS-SQL14] a table rebuild preserves AUTOINCREMENT and re-seeds the sequence from the sync table',
            invoke: async () => {
                const db = new Database(':memory:');
                const target = new SqliteTarget(db);
                const gid = 'g';
                // Three rows materialized (sync ids 1..3), then two deleted: the sync
                // ledger keeps ids 1..3, the app table keeps only id 1.
                await target.apply(gid, [{
                    kind: 'create-table', table: 'acct', syncTable: 'acct_sync', primaryKey: 'id',
                    columns: [{ name: 'ref', def: { type: 'string' } }],
                }], [
                    { kind: 'upsert-row', table: 'acct', rowId: 'r1', values: { ref: 'a' } },
                    { kind: 'upsert-row', table: 'acct', rowId: 'r2', values: { ref: 'b' } },
                    { kind: 'upsert-row', table: 'acct', rowId: 'r3', values: { ref: 'c' } },
                ], new Set(['v1']));
                await target.apply(gid, [], [
                    { kind: 'delete-row', table: 'acct', rowId: 'r2' },
                    { kind: 'delete-row', table: 'acct', rowId: 'r3' },
                ], new Set(['v2']));

                const maxSync = (db.prepare('SELECT MAX("id") AS n FROM acct_sync').get() as { n: number }).n;
                assertEquals(maxSync, 3, 'the sync ledger still holds ids 1..3 (rows survive delete)');

                // A NOT NULL add on the (non-empty) table forces a table rebuild;
                // backfill the surviving row so the tighten passes.
                await target.apply(gid, [{
                    kind: 'add-column', table: 'acct', column: 'note', def: { type: 'string' },
                }], [{ kind: 'upsert-row', table: 'acct', rowId: 'r1', values: { note: 'n' } }], new Set(['v3']));

                // AUTOINCREMENT survived the rebuild AND the sequence was re-seeded
                // from MAX(sync.id)=3, so a fresh app insert gets id 4 - never 2,
                // which the surviving-but-deleted sync mapping still occupies.
                const acctSql = (db.prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name='acct'")
                    .get() as { sql: string }).sql;
                assertTrue(/AUTOINCREMENT/i.test(acctSql), 'the rebuilt table keeps AUTOINCREMENT');
                const nextId = Number(db.prepare('INSERT INTO acct (ref, note) VALUES (?, ?)').run('d', 'm').lastInsertRowid);
                assertEquals(nextId, 4, 'the next auto id is 4 (re-seeded from the sync ledger), not a reused 2');
                db.close();
            },
        },
        {
            name: '[ADPTS-SQL15] drainOpEvents pages with exclusive afterId / beforeId',
            invoke: async () => {
                const db = new Database(':memory:');
                const target = new SqliteTarget(db);
                const gid = 'g';
                const events = [1, 2, 3, 4, 5].map((i) => ({
                    origin: 'concurrency' as const, direction: 'void' as const,
                    groupId: gid as B64Hash, opHash: `H${i}`, kind: 'insert' as const,
                }));
                await target.apply(gid, [], [], new Set(['v1']), events);

                const asc = await target.drainOpEvents({ limit: 2 });
                assertEquals(asc.length, 2, 'default order page size');
                assertEquals(asc[0].event.opHash, 'H1', 'default order is asc');
                assertEquals(asc[1].event.opHash, 'H2', 'asc second');
                const desc = await target.drainOpEvents({ order: 'desc', limit: 1 });
                assertEquals(desc[0]?.event.opHash, 'H5', 'desc limit 1 is the high id');
                const low = await target.drainOpEvents({ order: 'asc', limit: 1 });
                assertEquals(low[0]?.id, 1, 'asc limit 1 is the first event id');
                const mid = await target.drainOpEvents({ afterId: 2, limit: 2, order: 'asc' });
                assertEquals(mid.length, 2, 'mid page size');
                assertEquals(mid[0].id, 3, 'afterId is exclusive');
                assertEquals(mid[1].id, 4, 'asc pages forward');
                const older = await target.drainOpEvents({ beforeId: 5, limit: 2, order: 'desc' });
                assertEquals(older[0]?.id, 4, 'beforeId is exclusive');
                assertEquals(older[1]?.id, 3, 'desc older page');
                const window = await target.drainOpEvents({ afterId: 1, beforeId: 5, order: 'asc' });
                assertEquals(window.length, 3, 'after+before window size');
                assertEquals(window[0].id, 2, 'window after 1');
                assertEquals(window[2].id, 4, 'window before 5');
                db.close();
            },
        },
    ],
};
