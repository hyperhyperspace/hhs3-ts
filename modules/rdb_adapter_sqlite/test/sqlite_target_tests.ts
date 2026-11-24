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
    CheckpointMovedError, ingestChanges, PendingChangesError, projectGroup, reconcileIndexes, SchemaAction, RowAction, SyncMapping, syncGroup,
    type GroupProjection, type IndexDecl, type IndexSpec, type ResolvedIndex,
} from "@hyper-hyper-space/hhs3_rdb_adapter";
import {
    assertAtCheckpoints, beforeSettle, createFkGroup, createGroup, createTypedGroup, projectAs, sameVersion,
    TargetHarness, ProjectionReader, ReadRow, RowValues,
    IngestionHarness, LocalMutator, LocalValues,
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

function toParam(value: json.Literal | null, type: ColumnType): number | string | null {
    if (value === null) return null;   // a NULL, of any column type
    if (type === 'boolean') return value ? 1 : 0;
    // A value that isn't a json literal (a null inside it, say) is written as
    // plain JSON text, as an app writing raw SQL would.
    if (type === 'json') return json.literalProblem(value) === undefined ? json.toStringNormalized(value) : JSON.stringify(value);
    return value as number | string;
}

// The app-side write surface for the ingestion suite: raw SQL writes to the
// projected tables, exactly as the host application would issue them, so the
// capture triggers observe genuine local mutations.
function sqliteLocal(db: Database.Database, target: SqliteTarget): LocalMutator {
    const meta = (table: string): MetaRow =>
        db.prepare('SELECT id_column, author_column, sync_table, column_types FROM rdb_table_meta WHERE "table" = ?')
            .get(table) as MetaRow;

    const insertWithId = (verb: string, table: string, localId: number, values: LocalValues): void => {
        const m = meta(table);
        const ct = JSON.parse(m.column_types) as { [column: string]: ColumnType };
        const cols: string[] = [m.id_column];
        const params: (number | string | null)[] = [localId];
        for (const [c, v] of Object.entries(values)) { cols.push(c); params.push(toParam(v, ct[c] ?? 'string')); }
        db.prepare(`${verb} INTO ${quoteId(table)} (${cols.map(quoteId).join(', ')}) VALUES (${params.map(() => '?').join(', ')})`).run(...params);
    };

    return {
        insert: (table, values, author) => {
            const m = meta(table);
            const ct = JSON.parse(m.column_types) as { [column: string]: ColumnType };
            const cols: string[] = [];
            const params: (number | string | null)[] = [];
            if (author !== undefined && m.author_column !== null) { cols.push(m.author_column); params.push(author); }
            for (const [c, v] of Object.entries(values)) { cols.push(c); params.push(toParam(v, ct[c] ?? 'string')); }
            const placeholders = params.map(() => '?').join(', ');
            const info = db.prepare(
                `INSERT INTO ${quoteId(table)} (${cols.map(quoteId).join(', ')}) VALUES (${placeholders})`).run(...params);
            return Number(info.lastInsertRowid);
        },
        insertAt: (table, localId, values) => insertWithId('INSERT', table, localId, values),
        replaceAt: (table, localId, values) => insertWithId('INSERT OR REPLACE', table, localId, values),
        update: (table, localId, values) => {
            const m = meta(table);
            const ct = JSON.parse(m.column_types) as { [column: string]: ColumnType };
            const sets: string[] = [];
            const params: (number | string | null)[] = [];
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
    return sqliteIngestionHarnessOn(new Database(':memory:'));
}

// A harness over `db`. Its reopen closes the database and opens a copy of what
// it had committed, so a restart is simulated without a file.
function sqliteIngestionHarnessOn(db: Database.Database): IngestionHarness {
    const target = new SqliteTarget(db, { captureChanges: true });
    return {
        target, read: sqliteReader(db), local: sqliteLocal(db, target),
        cleanup: () => { if (db.open) db.close(); },
        reopen: async () => {
            const image = db.serialize();
            db.close();
            return sqliteIngestionHarnessOn(new Database(image));
        },
    };
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

// Physical (non-autoindex) indexes on `table`, as name -> CREATE sql.
function physicalIndexes(db: Database.Database, table: string): Map<string, string> {
    const rows = db.prepare(
        "SELECT name, sql FROM sqlite_master WHERE type = 'index' AND tbl_name = ? AND sql IS NOT NULL ORDER BY name")
        .all(table) as { name: string; sql: string }[];
    return new Map(rows.map((r) => [r.name, r.sql]));
}

type IndexXInfo = { name: string | null; desc: number; coll: string; key: number };
function indexKeyColumns(db: Database.Database, index: string): IndexXInfo[] {
    return (db.prepare('SELECT name, "desc", coll, "key" FROM pragma_index_xinfo(?)').all(index) as IndexXInfo[])
        .filter((c) => c.key === 1);
}

function indexMetaNames(db: Database.Database): string[] {
    return (db.prepare('SELECT "table" || \'.\' || name AS n FROM rdb_index_meta ORDER BY n').all() as { n: string }[])
        .map((r) => r.n);
}

function financeDecl(name: string, table: string, columns: IndexDecl['columns'], options?: IndexDecl['options']): IndexDecl {
    const d: IndexDecl = { name, group: 'finance-prod', table, columns };
    if (options !== undefined) d.options = options;
    return d;
}

function handIndex(name: string, table: string, rdb: string, fingerprint = name): ResolvedIndex {
    return { name, groupId: 'g', table, columns: [{ rdb, target: rdb }], fingerprint };
}

async function expectThrows(fn: () => Promise<unknown>, match: string | typeof CheckpointMovedError, why: string) {
    let err: unknown;
    try { await fn(); } catch (e) { err = e; }
    assertTrue(err !== undefined, `${why}: expected a throw`);
    if (typeof match === 'string') assertTrue(err instanceof Error && err.message.includes(match), `${why}: got ${String(err)}`);
    else assertTrue(err instanceof match, `${why}: got ${String(err)}`);
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
            name: '[ADPTS-SQL04] a local write announces the table',
            invoke: async () => {
                const { group, admin } = await createGroup();
                const ledger = await group.getTable('ledger');
                await ledger.insert('l1', { ref: 'R-1', amount: '10.00' }, admin);

                const db = new Database(':memory:');
                const target = new SqliteTarget(db, { captureChanges: true, pollMs: 10 });
                await projectGroup(group, target);

                const signalled = new Promise<void>((resolve, reject) => {
                    const timer = setTimeout(() => reject(new Error('no change signal within timeout')), 2000);
                    target.addCommitListener((commit) => {
                        if (commit.source !== 'local') return;
                        clearTimeout(timer);
                        resolve();
                    });
                });

                // A genuine local write on the same handle (as the host app would).
                db.prepare('INSERT INTO tags (code) VALUES (?)').run('urgent');

                await signalled;
                target.removeCommitListener(() => undefined);   // disarm path (no-op listener id)
                db.close();
            },
        },
        {
            name: '[ADPTS-SQL04b] WAL watch announces a local write (file-backed db)',
            invoke: async () => {
                const { group, admin } = await createGroup();
                const ledger = await group.getTable('ledger');
                await ledger.insert('l1', { ref: 'R-1', amount: '10.00' }, admin);

                const dbPath = tmpDbPath('walsig');
                const db = new Database(dbPath);
                // dbPath -> WAL watching (not polling). The target enables WAL
                // journal mode itself so the `-wal` file exists to watch.
                const target = new SqliteTarget(db, { captureChanges: true, dbPath });
                let listener = (_commit: { source: string }): void => undefined;
                try {
                    await projectGroup(group, target);

                    let fires = 0;
                    listener = (commit): void => { if (commit.source === 'local') fires++; };
                    target.addCommitListener(listener);
                    // Empty outbox: arm primes are silent. Await
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
                    target.removeCommitListener(listener);
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
                let listener = (_commit: { source: string }): void => undefined;
                try {
                    await projectGroup(group, target);

                    let fires = 0;
                    listener = (commit): void => { if (commit.source === 'local') fires++; };
                    target.addCommitListener(listener);
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
                    target.removeCommitListener(listener);
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
            name: '[ADPTS-SQL05] no projected table declares a SQL foreign key: not a local or a cross-group companion, an author or key column, nor a sync table',
            invoke: async () => {
                const db = new Database(':memory:');
                const target = new SqliteTarget(db);
                const v1: Version = new Set(['v1']);

                // A table of another group, for a co-projected cross-group reference.
                await target.apply('gB', [{
                    kind: 'create-table', table: 'archive_entries', syncTable: 'archive_entries_sync',
                    primaryKey: 'id', columns: [{ name: 'title', def: { type: 'string' } }],
                }], [], v1);
                await target.apply('gA', [{
                    kind: 'create-table', table: 'posts', syncTable: 'posts_sync',
                    primaryKey: 'id', columns: [{ name: 'title', def: { type: 'string' } }],
                }, {
                    kind: 'create-table', table: 'comments', syncTable: 'comments_sync', primaryKey: 'id',
                    authorColumn: 'author_key_id',
                    columns: [
                        { name: 'body', def: { type: 'string' } },
                        { name: 'post_id', def: { type: 'integer', nullable: true }, fk: { targetTable: 'posts' } },
                        { name: 'origin_id', def: { type: 'integer', nullable: true },
                            fk: { targetTable: 'archive_entries', crossGroup: true } },
                        { name: 'reviewer_key_id', def: { type: 'integer', nullable: true }, keyRef: true },
                    ],
                }], [], v1);

                const tables = (db.prepare("SELECT name FROM sqlite_master WHERE type = 'table'").all() as { name: string }[])
                    .map((r) => r.name);
                assertTrue(['comments', 'comments_sync', 'posts_sync'].every((t) => tables.includes(t)), 'the tables are there');
                for (const table of tables) {
                    const fkList = db.prepare('SELECT * FROM pragma_foreign_key_list(?)').all(table);
                    assertEquals(fkList.length, 0, `${table} declares no foreign key`);
                }
                const info = tableInfo(db, 'comments');
                for (const column of ['post_id', 'origin_id', 'reviewer_key_id', 'author_key_id']) {
                    assertTrue(info.some((c) => c.name === column && c.type === 'INTEGER'), `${column} is an integer column`);
                }
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
            name: '[ADPTS-SQL09] a row that references another table keeps its reference through a tighten rebuild of that table',
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

                const orderId = (db.prepare('SELECT id FROM orders_sync WHERE row_hash = ?').get('o1') as { id: number }).id;

                await target.apply(gid, [{
                    kind: 'add-column', table: 'orders', column: 'note',
                    def: { type: 'string' },
                }], [{
                    kind: 'upsert-row', table: 'orders', rowId: 'o1', values: { note: 'rush' },
                }], new Set(['v3']));

                const note = tableInfo(db, 'orders').find((c) => c.name === 'note');
                assertTrue(note !== undefined && note.notnull === 1,
                    'tightened column on the FK target is NOT NULL');
                const child = db.prepare('SELECT qty, order_id FROM lines').get() as { qty: number; order_id: number };
                assertEquals(child.qty, 2, 'referencing row survived the target rebuild');
                assertEquals(child.order_id, orderId, 'and still holds the id of the order it references');
                assertEquals((db.prepare('SELECT id FROM orders').get() as { id: number }).id, orderId,
                    'which the rebuilt order kept');
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
        {
            name: '[ADPTS-SQL16] json columns, json DEFAULTs and op events store JSON text that reads back unchanged',
            invoke: async () => {
                const db = new Database(':memory:');
                const target = new SqliteTarget(db, { captureChanges: true });
                const gid = 'g';
                const long = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
                const doc = { tags: ['line\nbreak', "it's", 'tab\there'], seq: long };
                const op = { action: 'insert', values: { body: doc } };
                await target.apply(gid, [{
                    kind: 'create-table', table: 'docs', syncTable: 'docs_sync', primaryKey: 'id',
                    columns: [
                        { name: 'body', def: { type: 'json' } },
                        { name: 'extra', def: { type: 'json', default: long } },
                    ],
                }], [{ kind: 'upsert-row', table: 'docs', rowId: 'r1', values: { body: doc, extra: long } }], new Set(['v1']), [{
                    origin: 'concurrency', direction: 'void', groupId: gid as B64Hash, opHash: 'H1', kind: 'insert', op,
                }]);
                const same = (a: unknown, b: unknown) =>
                    json.toStringNormalized(a as json.Literal) === json.toStringNormalized(b as json.Literal);

                const stored = db.prepare('SELECT id, body, extra FROM docs').get() as { id: number; body: string; extra: string };
                assertTrue(same(JSON.parse(stored.body), doc), `projected json reads back unchanged (${stored.body})`);
                assertEquals(JSON.stringify(JSON.parse(stored.extra)), JSON.stringify(long), 'a 12-element array keeps its order');
                const dflt = tableInfo(db, 'docs').find((c) => c.name === 'extra')!.dflt_value!;
                assertEquals(JSON.stringify(JSON.parse(dflt.slice(1, -1).replace(/''/g, "'"))), JSON.stringify(long),
                    `json DEFAULT keeps array order (${dflt})`);

                const events = await target.drainOpEvents({});
                assertTrue(same(events[0]!.event.op, op), 'a stored op event with control characters reads back unchanged');

                const edited = { note: 'multi\nline', seq: [...long].reverse() };
                db.prepare('UPDATE docs SET body = ? WHERE id = ?').run(json.toStringNormalized(edited), stored.id);
                const batch = await target.drainChanges();
                const change = batch.changes.find((c) => c.kind === 'update');
                assertTrue(change !== undefined && change.kind === 'update' && same(change.values['body'], edited),
                    'a captured local json edit drains unchanged');
                db.close();
            },
        },
        {
            name: '[ADPTS-SQL17] an insert captured before a column drop arrives is translated with the schema at the checkpoint: with a NULL or a value in the dropped column it is accepted, and the column goes with the next projection',
            invoke: async () => {
                // A sync ingests first and projects after, so a captured insert can be
                // planned under a schema that already dropped a column the target
                // still has. The captured row names every column of the table.
                const run = async (sql: string, params: string[]) => {
                    const { group, schema, admin } = await createGroup();
                    const db = new Database(':memory:');
                    const target = new SqliteTarget(db, { captureChanges: true });
                    try {
                        await projectAs(group, target, admin);
                        await syncGroup(group, target, { writer: admin });
                        db.prepare(sql).run(...params);
                        const captured = (db.prepare('SELECT changed FROM rdb_outbox').all() as { changed: string }[]).map((r) => r.changed);

                        await schema.updateSchema([{ rule: 'drop-column', table: 'ledger', column: 'memo' }], admin, 'drop memo');
                        await group.deploy(await (await schema.getScopedDag()).getFrontier());
                        const result = await syncGroup(group, target, { writer: admin });

                        return {
                            captured, result,
                            inRdb: (await groupRows(group, 'ledger')).length,
                            inSqlite: (db.prepare('SELECT * FROM ledger').all() as Record<string, unknown>[]),
                        };
                    } finally {
                        db.close();
                    }
                };

                // the app never wrote to memo: its captured NULL is not an error
                const empty = await run('INSERT INTO ledger (ref, amount) VALUES (?, ?)', ['r1', '1.00']);
                assertEquals(empty.captured.join(';'), '{"ref":"r1","memo":null,"amount":"1.00","author_key_id":1}',
                    'the captured row names the empty column, and the author capture filled in');
                assertEquals(empty.result.accepted, 1, 'the insert is accepted');
                assertEquals(empty.result.rejected.length, 0, 'nothing is rejected');
                assertEquals(empty.inRdb, 1, 'the row is in rdb');
                assertEquals(empty.inSqlite.length, 1, 'and stays in SQLite');
                assertTrue(!('memo' in empty.inSqlite[0]), 'the column is gone from the table after the projection');

                // the app wrote a value to memo: the schema at the checkpoint has the
                // column, so the insert is accepted, concurrent with the drop (HIP-29)
                const set = await run('INSERT INTO ledger (ref, memo, amount) VALUES (?, ?, ?)', ['r1', 'hi', '1.00']);
                assertEquals(set.result.accepted, 1, 'an insert with a value in the dropped column is accepted');
                assertEquals(set.result.rejected.length, 0, 'nothing is rejected');
                assertEquals(set.inRdb, 1, 'the row is in rdb');
                assertEquals(set.inSqlite.length, 1, 'and stays in SQLite');
                assertTrue(!('memo' in set.inSqlite[0]), 'the column is gone from the table after the projection');
            },
        },
        {
            name: '[ADPTS-SQL18] a native statement that changes the id of a projected row fails, also through rowid and with capture paused; the same id passes',
            invoke: async () => {
                const { group, admin } = await createGroup();
                const ledger = await group.getTable('ledger');
                await ledger.insert('l1', { ref: 'R-1', amount: '1.00', memo: 'a' }, admin);
                const db = new Database(':memory:');
                const target = new SqliteTarget(db, { captureChanges: true });
                try {
                    await syncGroup(group, target, { writer: admin });
                    const id = (db.prepare('SELECT id FROM ledger').get() as { id: number }).id;
                    const rowId = (await target.resolveRow('ledger', id))!.rowId;
                    const outbox = () => (db.prepare('SELECT COUNT(*) AS n FROM rdb_outbox').get() as { n: number }).n;
                    const refused = (sql: string, why: string) => {
                        let err: unknown;
                        try { db.prepare(sql).run(); } catch (e) { err = e; }
                        assertTrue(err instanceof Error && err.message.includes("can't change"), `${why}: expected the statement to fail, got ${String(err)}`);
                    };

                    refused(`UPDATE ledger SET id = 999 WHERE id = ${id}`, 'a change of the id column');
                    refused(`UPDATE ledger SET rowid = 999 WHERE id = ${id}`, 'a change through rowid');
                    refused(`UPDATE ledger SET memo = 'b', id = 999 WHERE id = ${id}`, 'a change of the id together with another column');
                    target.setCaptureEnabled(false);
                    refused(`UPDATE ledger SET id = 999 WHERE id = ${id}`, 'a change of the id while capture is paused');
                    target.setCaptureEnabled(true);
                    assertEquals(JSON.stringify(db.prepare('SELECT id, memo FROM ledger').all()), JSON.stringify([{ id, memo: 'a' }]),
                        'the row keeps its id and its values');
                    assertEquals(outbox(), 0, 'and nothing is captured');

                    db.prepare(`UPDATE ledger SET id = id WHERE id = ${id}`).run();
                    assertEquals(outbox(), 0, 'setting the id to itself passes and captures nothing');

                    db.prepare(`UPDATE ledger SET memo = 'c' WHERE id = ${id}`).run();
                    const result = await syncGroup(group, target, { writer: admin });
                    assertEquals(result.accepted, 1, 'a native update of another column is ingested');
                    await ledger.update(rowId, { memo: 'd' }, admin);
                    await syncGroup(group, target, { writer: admin });
                    assertEquals(JSON.stringify(db.prepare('SELECT id, memo FROM ledger').all()), JSON.stringify([{ id, memo: 'd' }]),
                        'a remote update lands on the same local id, in one row');
                } finally {
                    db.close();
                }
            },
        },
        {
            name: '[ADPTS-SQL19] a native write of a boolean other than 0 or 1, or of json text that isn\'t json or is json\'s null, fails at the statement; NULL and json text in another form sync',
            invoke: async () => {
                const { group, admin } = await createTypedGroup();
                const db = new Database(':memory:');
                const target = new SqliteTarget(db, { captureChanges: true });
                try {
                    await projectAs(group, target, admin);
                    await syncGroup(group, target, { writer: admin });
                    const outbox = () => (db.prepare('SELECT COUNT(*) AS n FROM rdb_outbox').get() as { n: number }).n;
                    const refused = (sql: string, params: (string | number)[], why: string) => {
                        let err: unknown;
                        try { db.prepare(sql).run(...params); } catch (e) { err = e; }
                        assertTrue(err instanceof Error && err.message.includes('CHECK constraint failed'), `${why}: expected a CHECK failure, got ${String(err)}`);
                    };

                    refused('INSERT INTO things (label, flag) VALUES (?, ?)', ['five', 5], 'a boolean of 5');
                    refused('INSERT INTO things (label, data) VALUES (?, ?)', ['bad', 'nope'], 'json text that is not json');
                    refused('INSERT INTO things (label, data) VALUES (?, ?)', ['null', 'null'], 'the json text null');
                    refused('INSERT INTO things (label, data) VALUES (?, ?)', ['spaced null', ' null '], 'the json text null with spaces');
                    db.prepare('INSERT INTO things (label, flag) VALUES (?, ?)').run('one', 1);
                    const one = (db.prepare("SELECT id FROM things WHERE label = 'one'").get() as { id: number }).id;
                    refused('UPDATE things SET flag = ? WHERE id = ?', [2, one], 'an update to a boolean of 2');
                    assertEquals(outbox(), 1, 'only the valid insert is captured');

                    db.prepare('INSERT INTO things (label, data) VALUES (?, ?)').run('nulled', null);
                    db.prepare('INSERT INTO things (label, data) VALUES (?, ?)').run('spaced', '{ "b": 1, "a": 2 }');
                    db.prepare('INSERT INTO things (label, flag) VALUES (?, ?)').run('text one', '1');
                    const result = await syncGroup(group, target, { writer: admin });
                    assertEquals(result.accepted, 4, 'the four valid inserts are accepted');
                    assertEquals(result.rejected.length, 0, 'and none is refused');
                    const byLabel = new Map((await groupRows(group, 'things')).map((r) => [r.values.label, r.values]));
                    assertEquals(byLabel.get('one')?.flag, true, 'a 1 syncs as true');
                    assertEquals(byLabel.get('text one')?.flag, true, "the text '1' is stored as 1 and syncs as true");
                    assertEquals(byLabel.get('nulled')?.data ?? null, null, 'SQL NULL syncs as NULL');
                    const spaced = byLabel.get('spaced')?.data;
                    assertTrue(spaced !== undefined && spaced !== null && json.toStringNormalized(spaced) === json.toStringNormalized({ a: 2, b: 1 }),
                        `json text in another form syncs as its value, got ${JSON.stringify(spaced)}`);
                } finally {
                    db.close();
                }
            },
        },
        {
            name: '[ADPTS-SQL20] the boolean and json checks and the id guard survive an added column, a tighten rebuild, a foreign-key column drop and a plain column drop',
            invoke: async () => {
                const db = new Database(':memory:');
                const target = new SqliteTarget(db, { captureChanges: true });
                const gid = 'g';
                await target.apply(gid, [
                    { kind: 'create-table', table: 'parent', syncTable: 'parent_sync', primaryKey: 'id',
                        columns: [{ name: 'title', def: { type: 'string' } }] },
                    { kind: 'create-table', table: 'acct', syncTable: 'acct_sync', primaryKey: 'id',
                        columns: [
                            { name: 'ref', def: { type: 'string' } },
                            { name: 'parent_id', def: { type: 'integer', nullable: true }, fk: { targetTable: 'parent' } },
                            { name: 'flag', def: { type: 'boolean', nullable: true } },
                            { name: 'data', def: { type: 'json', nullable: true } },
                        ] },
                ], [{ kind: 'upsert-row', table: 'acct', rowId: 'r1', values: { ref: 'a', flag: true, data: { x: 1 } } }], new Set(['v1']));
                const id = (db.prepare('SELECT id FROM acct').get() as { id: number }).id;
                const fails = (sql: string, params: (string | number)[], match: string, why: string) => {
                    let err: unknown;
                    try { db.prepare(sql).run(...params); } catch (e) { err = e; }
                    assertTrue(err instanceof Error && err.message.includes(match), `${why}: expected '${match}', got ${String(err)}`);
                };
                const checks = (step: string, booleans: string[], jsons: string[]) => {
                    for (const col of booleans) fails(`UPDATE acct SET ${col} = ? WHERE id = ?`, [5, id], 'CHECK constraint failed', `${step}: boolean ${col}`);
                    for (const col of jsons) fails(`UPDATE acct SET ${col} = ? WHERE id = ?`, ['nope', id], 'CHECK constraint failed', `${step}: json ${col}`);
                    fails('UPDATE acct SET id = ? WHERE id = ?', [99, id], "can't change", `${step}: the id guard`);
                };

                checks('after the create', ['flag'], ['data']);
                await target.apply(gid, [
                    { kind: 'add-column', table: 'acct', column: 'flag2', def: { type: 'boolean', default: true } },
                    { kind: 'add-column', table: 'acct', column: 'data2', def: { type: 'json', nullable: true } },
                ], [], new Set(['v2']));
                checks('after adding columns to a non-empty table', ['flag', 'flag2'], ['data', 'data2']);
                await target.apply(gid, [{ kind: 'add-column', table: 'acct', column: 'status', def: { type: 'string' } }],
                    [{ kind: 'upsert-row', table: 'acct', rowId: 'r1', values: { status: 'open' } }], new Set(['v3']));
                checks('after a tighten rebuild', ['flag', 'flag2'], ['data', 'data2']);
                await target.apply(gid, [{ kind: 'drop-column', table: 'acct', column: 'parent_id' }], [], new Set(['v4']));
                checks('after a foreign-key column drop', ['flag', 'flag2'], ['data', 'data2']);
                await target.apply(gid, [{ kind: 'drop-column', table: 'acct', column: 'data2' }], [], new Set(['v5']));
                checks('after a plain column drop', ['flag', 'flag2'], ['data']);
                assertEquals(JSON.stringify(db.prepare('SELECT ref, flag, data, flag2, status FROM acct').get()),
                    JSON.stringify({ ref: 'a', flag: 1, data: '{"x":1}', flag2: 1, status: 'open' }), 'the row came through every step');
                db.close();
            },
        },
        {
            name: '[ADPTS-SQL21] with SQLite\'s checks switched off, a value the drain can\'t read is refused with a failure event instead of stopping the drain, and the pass goes on',
            invoke: async () => {
                const { group, admin } = await createTypedGroup();
                const things = await group.getTable('things');
                await things.insert('t1', { label: 't1', data: { x: 1 } }, admin);
                const db = new Database(':memory:');
                const target = new SqliteTarget(db, { captureChanges: true });
                try {
                    await projectAs(group, target, admin);
                    await syncGroup(group, target, { writer: admin });
                    const t1 = (db.prepare("SELECT id FROM things WHERE label = 't1'").get() as { id: number }).id;
                    db.pragma('ignore_check_constraints = ON');
                    db.prepare('INSERT INTO things (label, data) VALUES (?, ?)').run('bad', 'nope');
                    db.prepare('UPDATE things SET data = ? WHERE id = ?').run('nope', t1);
                    db.pragma('ignore_check_constraints = OFF');
                    db.prepare('INSERT INTO things (label) VALUES (?)').run('good');

                    const batch = await target.drainChanges();
                    const marked = batch.changes.filter((c) => c.kind !== 'delete' && c.unreadable?.data !== undefined);
                    assertEquals(marked.length, 2, 'the drain marks both changes it cannot read, instead of throwing');
                    assertTrue(marked.every((c) => c.kind !== 'delete' && !('data' in c.values)), 'and leaves the column out of their values');

                    const result = await ingestChanges(group, target, { writer: admin });
                    assertEquals(result.accepted, 1, 'the valid insert is accepted');
                    assertEquals(result.rejected.length, 2, 'both changes it could not read are refused');
                    const failures = (await target.drainOpEvents()).filter((e) => e.event.direction === 'failure');
                    assertEquals(failures.length, 2, 'each with a failure event');
                    assertTrue(failures[0].event.opHash !== failures[1].event.opHash, 'of its own');
                    assertEquals((db.prepare('SELECT COUNT(*) AS n FROM rdb_outbox').get() as { n: number }).n, 0, 'the outbox is emptied');

                    await syncGroup(group, target, { writer: admin });
                    const local = db.prepare('SELECT label, data FROM things ORDER BY label').all();
                    assertEquals(JSON.stringify(local), JSON.stringify([{ label: 'good', data: null }, { label: 't1', data: '{"x":1}' }]),
                        'the refused insert is gone and the refused update reverted');
                    const inRdb = (await groupRows(group, 'things')).map((r) => r.values.label).sort();
                    assertEquals(inRdb.join(','), 'good,t1', 'rdb has the same rows');
                } finally {
                    db.close();
                }
            },
        },
        {
            name: '[ADPTS-SQL22] a second handle on the same file follows the first one\'s schema changes: a new json column drains as json, and added and dropped columns show in its queries',
            invoke: async () => {
                const dbPath = tmpDbPath('meta-follows');
                const dbA = new Database(dbPath); dbA.pragma('busy_timeout = 5000');
                const dbB = new Database(dbPath); dbB.pragma('busy_timeout = 5000');
                const a = new SqliteTarget(dbA, { captureChanges: true });
                const b = new SqliteTarget(dbB, { captureChanges: true });
                const query = (t: SqliteTarget, table = 'acct') =>
                    t.executeQuery({ table, orderBy: [{ column: 'id', direction: 'asc' }], limit: 10 });
                try {
                    const gid = 'g';
                    await a.apply(gid, [{ kind: 'create-table', table: 'acct', syncTable: 'acct_sync', primaryKey: 'id',
                        columns: [{ name: 'ref', def: { type: 'string' } }] }],
                    [{ kind: 'upsert-row', table: 'acct', rowId: 'r1', values: { ref: 'a' } }], new Set(['v1']));
                    assertEquals((await query(b)).length, 1, 'the second handle reads the table');

                    await a.apply(gid, [{ kind: 'add-column', table: 'acct', column: 'data', def: { type: 'json', nullable: true } }], [], new Set(['v2']));
                    dbB.prepare('UPDATE acct SET data = ? WHERE id = 1').run('{"k":1}');
                    const update = (await b.drainChanges()).changes.find((c) => c.kind === 'update');
                    const drained = update !== undefined && update.kind === 'update' ? update.values.data : undefined;
                    assertTrue(drained !== null && typeof drained === 'object' && json.toStringNormalized(drained) === '{"k":1}',
                        `the second handle drains the new json column as json, got ${JSON.stringify(drained)}`);
                    // Settle it, as ingestion would: a projection doesn't apply over a pending change (HIP-29).
                    await b.commitIngest({ consumed: [update!.id] });

                    await a.apply(gid, [{ kind: 'add-column', table: 'acct', column: 'note', def: { type: 'string', nullable: true } }],
                        [{ kind: 'upsert-row', table: 'acct', rowId: 'r1', values: { note: 'n' } }], new Set(['v3']));
                    assertEquals((await query(b))[0]?.note, 'n', 'an added column shows in the second handle\'s queries');

                    await a.apply(gid, [{ kind: 'drop-column', table: 'acct', column: 'ref' }], [], new Set(['v4']));
                    const after = await query(b);
                    assertTrue(after.length === 1 && !('ref' in after[0]), 'a dropped column is gone from them, without an error');

                    await a.apply(gid, [{ kind: 'create-table', table: 'other', syncTable: 'other_sync', primaryKey: 'id',
                        columns: [{ name: 'title', def: { type: 'string' } }] }], [], new Set(['v5']));
                    assertEquals((await query(b, 'other')).length, 0, 'and a new table can be queried');
                } finally {
                    dbA.close(); dbB.close(); cleanupDb(dbPath);
                }
            },
        },
        {
            name: '[ADPTS-SQL24] a change written through a second connection makes the first one\'s projection refuse, until a cycle ingests it',
            invoke: async () => {
                const dbPath = tmpDbPath('pending-across');
                const dbA = new Database(dbPath); dbA.pragma('busy_timeout = 5000');
                const dbB = new Database(dbPath); dbB.pragma('busy_timeout = 5000');
                const a = new SqliteTarget(dbA, { captureChanges: true });
                new SqliteTarget(dbB, { captureChanges: true });
                try {
                    const { group, admin } = await createGroup();
                    const ledger = await group.getTable('ledger');
                    await ledger.insert('l1', { ref: 'R-1', amount: '1.00' }, admin);
                    await projectGroup(group, a);

                    await ledger.insert('l2', { ref: 'R-2', amount: '2.00' }, admin);
                    dbB.prepare('INSERT INTO tags (code) VALUES (?)').run('from-b');
                    let refused = false;
                    try { await projectGroup(group, a); } catch (e) { refused = e instanceof PendingChangesError; }
                    assertTrue(refused, "the first connection's projection refuses over the second's pending change");
                    assertTrue(await a.hasPendingChanges(), 'the first connection sees the change pending');

                    await syncGroup(group, a, { writer: admin });
                    assertTrue(!await a.hasPendingChanges(), 'a cycle ingests it');
                    assertTrue(sameVersion(await a.getCheckpoint(group.getId()), await (await group.getScopedDag()).getFrontier()),
                        'and then projects');
                } finally {
                    dbA.close(); dbB.close(); cleanupDb(dbPath);
                }
            },
        },
        {
            name: '[ADPTS-SQL28] a connection with foreign keys on, as better-sqlite3 opens one, can delete a referenced row and insert a dangling reference; ingestion decides',
            invoke: async () => {
                const dbPath = tmpDbPath('fk-on');
                const db = new Database(dbPath); db.pragma('busy_timeout = 5000');
                const target = new SqliteTarget(db, { captureChanges: true });
                const app = new Database(dbPath); app.pragma('busy_timeout = 5000');
                try {
                    assertEquals(app.pragma('foreign_keys', { simple: true }), 1, "the app's connection enforces foreign keys");
                    const { group, admin } = await createFkGroup();
                    await projectAs(group, target, admin);
                    const postId = Number(db.prepare("INSERT INTO posts (title) VALUES ('p')").run().lastInsertRowid);
                    db.prepare("INSERT INTO comments (body, post_id) VALUES ('c', ?)").run(postId);
                    await syncGroup(group, target, { writer: admin });
                    await assertAtCheckpoints(target, [{ group }], 'after the post and its comment');
                    assertEquals((await groupRows(group, 'comments')).length, 1, 'the comment is in rdb');

                    app.prepare('DELETE FROM posts WHERE id = ?').run(postId);
                    let result = await syncGroup(group, target, { writer: admin });
                    assertEquals(result.accepted, 1, 'the delete of the referenced post is ingested');
                    assertEquals((await groupRows(group, 'posts')).length, 0, 'the post is gone from rdb');
                    assertEquals((await groupRows(group, 'comments')).length, 1, 'its comment stays, as rdb allows');
                    await assertAtCheckpoints(target, [{ group }], 'after the delete');

                    app.prepare("INSERT INTO comments (body, post_id) VALUES ('dangling', ?)").run(postId);
                    result = await syncGroup(group, target, { writer: admin });
                    assertEquals(result.accepted, 0, 'a comment on the deleted post is not ingested');
                    assertEquals(result.rejected.length, 1, 'it is refused');
                    assertEquals((app.prepare("SELECT COUNT(*) AS n FROM comments WHERE body = 'dangling'").get() as { n: number }).n, 0,
                        'and removed from the table');
                    await assertAtCheckpoints(target, [{ group }], 'after the refused comment');
                } finally {
                    app.close(); db.close(); cleanupDb(dbPath);
                }
            },
        },
        {
            name: '[ADPTS-SQL25] a native UPDATE is one outbox row per updated row, with every column it names, changed or not; NULL is kept, the id is never captured, and two connections give two rows',
            invoke: async () => {
                const dbPath = tmpDbPath('outbox-shapes');
                const dbA = new Database(dbPath); dbA.pragma('busy_timeout = 5000');
                const dbB = new Database(dbPath); dbB.pragma('busy_timeout = 5000');
                const a = new SqliteTarget(dbA, { captureChanges: true });
                new SqliteTarget(dbB, { captureChanges: true });
                const outbox = (): string[] => (dbA.prepare('SELECT op, changed FROM rdb_outbox ORDER BY id').all() as { op: string; changed: string | null }[])
                    .map((r) => `${r.op} ${r.changed === null ? '' : JSON.stringify(Object.fromEntries(Object.entries(JSON.parse(r.changed) as object).sort()))}`);
                const clear = (): void => { dbA.prepare('DELETE FROM rdb_outbox').run(); };
                try {
                    const { group, admin } = await createGroup();
                    const ledger = await group.getTable('ledger');
                    await ledger.insert('l1', { ref: 'R-1', memo: 'm', amount: '1.00' }, admin);
                    await ledger.insert('l2', { ref: 'R-2', amount: '2.00' }, admin);
                    await projectGroup(group, a);
                    const idOf = (ref: string): number => (dbA.prepare('SELECT id FROM ledger WHERE ref = ?').get(ref) as { id: number }).id;
                    const [id1, id2] = [idOf('R-1'), idOf('R-2')];

                    dbA.prepare('UPDATE ledger SET memo = memo, amount = ? WHERE id = ?').run('1.00', id1);
                    assertEquals(outbox().join(' | '), 'update {"amount":"1.00","memo":"m"}', 'every named column, unchanged ones too, in one row');
                    clear();

                    dbA.prepare("UPDATE ledger SET memo = 'x'").run();
                    assertEquals(outbox().join(' | '), 'update {"memo":"x"} | update {"memo":"x"}', 'one statement over two rows gives one row each');
                    clear();

                    dbA.prepare('UPDATE ledger SET memo = NULL WHERE id = ?').run(id2);
                    assertEquals(outbox().join(' | '), 'update {"memo":null}', 'a NULL is kept');
                    clear();

                    dbA.prepare('UPDATE ledger SET id = id WHERE id = ?').run(id1);
                    assertEquals(outbox().length, 0, 'an update that names only the id captures nothing');

                    dbA.prepare("UPDATE ledger SET memo = 'a' WHERE id = ?").run(id1);
                    dbB.prepare("UPDATE ledger SET amount = '3.00' WHERE id = ?").run(id1);
                    assertEquals(outbox().join(' | '), 'update {"memo":"a"} | update {"amount":"3.00"}', 'two statements, on two connections, give two rows');
                } finally {
                    dbA.close(); dbB.close(); cleanupDb(dbPath);
                }
            },
        },
        {
            name: "[ADPTS-SQL26] every local id comes from its table's own counter: a reference's id, a native insert's and a projected row's never meet",
            invoke: async () => {
                const db = new Database(':memory:');
                const target = new SqliteTarget(db);
                try {
                    const { group } = await createFkGroup();
                    await projectGroup(group, target);
                    const groupId = group.getId();
                    const apply = async (actions: RowAction[]): Promise<void> =>
                        target.apply(groupId, [], actions, (await target.getCheckpoint(groupId))!);
                    const comment = (rowId: string, post: string): RowAction =>
                        ({ kind: 'upsert-row', table: 'comments', rowId, values: { body: rowId, post_id: post } });
                    const postIdOf = (rowId: string): number =>
                        (db.prepare('SELECT post_id AS p FROM comments WHERE id = (SELECT id FROM comments_sync WHERE row_hash = ?)')
                            .get(rowId) as { p: number }).p;
                    const syncId = (table: string, rowId: string): number | undefined =>
                        (db.prepare(`SELECT id FROM ${table}_sync WHERE row_hash = ?`).get(rowId) as { id: number } | undefined)?.id;

                    await apply([comment('C1', 'P-GHOST')]);
                    assertEquals(postIdOf('C1'), 1, 'a reference to a table that never had an insert takes its first id');
                    assertEquals((db.prepare('SELECT COUNT(*) AS n FROM posts').get() as { n: number }).n, 0, 'and writes no row there');

                    const native = Number(db.prepare("INSERT INTO posts (title) VALUES ('native')").run().lastInsertRowid);
                    assertEquals(native, 2, 'a native insert after a reference took an id gets a new one');

                    await apply([comment('C2', 'P-GHOST-2')]);
                    assertTrue(postIdOf('C2') !== native && postIdOf('C2') === 3,
                        `a reference after a native insert with no mapping yet takes another id, got ${postIdOf('C2')}`);

                    await apply([{ kind: 'upsert-row', table: 'posts', rowId: 'P-NEW', values: { title: 'new' } }]);
                    const newId = syncId('posts', 'P-NEW')!;
                    assertEquals(newId, 4, 'a row the projection writes takes the id its insert produced');
                    assertEquals((db.prepare('SELECT title FROM posts WHERE id = ?').get(newId) as { title: string }).title, 'new', 'and its sync row names it');

                    await apply([{ kind: 'upsert-row', table: 'posts', rowId: 'P-GHOST', values: { title: 'late' } }]);
                    assertEquals((db.prepare('SELECT title FROM posts WHERE id = 1').get() as { title: string }).title, 'late',
                        'the referenced row, arriving later, is inserted with the reserved id');
                    assertEquals(Number(db.prepare("INSERT INTO posts (title) VALUES ('again')").run().lastInsertRowid), 5,
                        'and the counter goes on past every id handed out');
                } finally {
                    db.close();
                }
            },
        },
        {
            name: "[ADPTS-SQL27] the settle rewrites an ingested change's json text in canonical form, as rdb's rows have it, and skips a column a pending change writes",
            invoke: async () => {
                const db = new Database(':memory:');
                const target = new SqliteTarget(db, { captureChanges: true });
                try {
                    const { group, admin } = await createTypedGroup();
                    await projectAs(group, target, admin);
                    const text = (id: number): string => (db.prepare('SELECT data FROM things WHERE id = ?').get(id) as { data: string }).data;
                    const id = Number(db.prepare("INSERT INTO things (label, data) VALUES ('a', ?)").run('{ "b": 1, "a": 2 }').lastInsertRowid);
                    await ingestChanges(group, target, { writer: admin });
                    assertEquals(text(id), '{"a":2,"b":1}', 'the ingested text is canonical');
                    assertEquals((db.prepare('SELECT COUNT(*) AS n FROM things WHERE data = ?').get('{"a":2,"b":1}') as { n: number }).n, 1,
                        'so a query that binds the value finds it');

                    // A change of the column captured during the pass keeps its own text until its own settle.
                    db.prepare("UPDATE things SET data = ? WHERE id = ?").run('{ "y": 1 }', id);
                    const pending = (): void => { db.prepare("UPDATE things SET data = ? WHERE id = ?").run('{ "z": 1 }', id); };
                    let once = false;
                    const hooked = beforeSettle(target, () => { if (!once) { once = true; pending(); } });
                    await ingestChanges(group, hooked, { writer: admin });
                    assertEquals(text(id), '{ "z": 1 }', 'a column a pending change writes is left as written');
                    await ingestChanges(group, target, { writer: admin });
                    assertEquals(text(id), '{"z":1}', 'and its own settle rewrites it');
                } finally {
                    db.close();
                }
            },
        },
        {
            name: '[ADPTS-SQL23] a rolled-back apply leaves no meta behind: the table reads as it was before',
            invoke: async () => {
                const db = new Database(':memory:');
                const target = new SqliteTarget(db);
                const query = () => target.executeQuery({ table: 'acct', orderBy: [{ column: 'id', direction: 'asc' }], limit: 10 });
                await target.apply('g', [{ kind: 'create-table', table: 'acct', syncTable: 'acct_sync', primaryKey: 'id',
                    columns: [{ name: 'ref', def: { type: 'string' } }] }],
                [{ kind: 'upsert-row', table: 'acct', rowId: 'r1', values: { ref: 'a' } }], new Set(['v1']));
                assertEquals((await query()).length, 1, 'the table is read once');

                let threw = false;
                try {
                    await target.apply('g', [{ kind: 'add-column', table: 'acct', column: 'x', def: { type: 'string', nullable: true } }],
                        [{ kind: 'upsert-row', table: 'ghost', rowId: 'g1', values: {} }], new Set(['v2']));
                } catch { threw = true; }
                assertTrue(threw, 'an apply that writes to a table that does not exist fails, and rolls back');
                const rows = await query();
                assertTrue(rows.length === 1 && !('x' in rows[0]), 'the table reads as it was, without the column of the failed apply');
                db.close();
            },
        },
        {
            name: '[ADPTS-IDX01] reconcile builds real indexes: columns, DESC, COLLATE, partial WHERE, author, query plan',
            invoke: async () => {
                const { group, admin } = await createGroup();
                await (await group.getTable('ledger')).insert('l1', { ref: 'R-1', amount: '10.00', memo: 'x' }, admin);
                const db = new Database(':memory:');
                const target = new SqliteTarget(db);
                await projectGroup(group, target);
                const members: GroupProjection[] = [{ group, config: {} }];

                const report = await reconcileIndexes(members, target, {
                    indexes: [
                        financeDecl('by_memo', 'ledger', ['memo'], { columns: { memo: { collate: 'NOCASE', whereNotNull: true } } }),
                        financeDecl('by_amount', 'ledger', ['amount', '@author'], { columns: { amount: { desc: true } } }),
                    ],
                });
                assertEquals(report.status, 'installed', 'spec installed');

                const physical = physicalIndexes(db, 'ledger');
                assertEquals([...physical.keys()].join(','), 'ledger__by_amount,ledger__by_memo',
                    'physical names are <table>__<name>');
                assertTrue(physical.get('ledger__by_memo')!.includes('WHERE "memo" IS NOT NULL'), 'partial index WHERE clause');
                const memo = indexKeyColumns(db, 'ledger__by_memo');
                assertEquals(memo.length, 1, 'by_memo has one key column');
                assertEquals(memo[0]!.name, 'memo', 'by_memo column');
                assertEquals(memo[0]!.coll, 'NOCASE', 'by_memo collation');
                const amount = indexKeyColumns(db, 'ledger__by_amount');
                assertEquals(amount.map((c) => `${c.name}:${c.desc}`).join(','), 'amount:1,author_key_id:0',
                    'DESC column + @author resolved to author_key_id');
                assertEquals(indexMetaNames(db).join(','), 'ledger.by_amount,ledger.by_memo', 'index records kept');

                const plan = db.prepare('EXPLAIN QUERY PLAN SELECT id FROM ledger WHERE amount = ?').all('10.00') as
                    { detail: string }[];
                assertTrue(plan.some((p) => p.detail.includes('ledger__by_amount')),
                    `the planner uses the index (plan: ${plan.map((p) => p.detail).join(' | ')})`);

                // State survives reopening the database with a fresh target.
                const reopened = await new SqliteTarget(db).getIndexState();
                assertEquals(reopened.spec?.indexes.map((d) => d.name).sort().join(','), 'by_amount,by_memo',
                    'installed spec persisted');
                assertEquals(reopened.materialized.map((m) => m.name).sort().join(','), 'by_amount,by_memo',
                    'materialized records persisted');
                db.close();
            },
        },
        {
            name: '[ADPTS-IDX02] invalid sqlite options are refused before anything is installed',
            invoke: async () => {
                const { group } = await createGroup();
                const db = new Database(':memory:');
                const target = new SqliteTarget(db);
                await projectGroup(group, target);
                const members: GroupProjection[] = [{ group, config: {} }];
                const attempt = (options: IndexDecl['options']) =>
                    reconcileIndexes(members, target, { indexes: [financeDecl('ix', 'ledger', ['memo'], options)] });

                await expectThrows(() => attempt({ columns: { memo: { collate: 'FANCY' } } }), 'unknown collation', 'bad collation');
                await expectThrows(() => attempt({ columns: { ref: { collate: 'NOCASE' } } }), 'does not list',
                    'options on an unlisted column');
                await expectThrows(() => attempt({ columns: { memo: { unique: true } } }),
                    "unknown sqlite column option 'unique' on 'memo'", 'unknown column option');
                await expectThrows(() => attempt({ unique: true }), "unknown sqlite index option 'unique'", 'unknown top-level key');
                await expectThrows(() => attempt({ collate: { memo: 'NOCASE' } }), "unknown sqlite index option 'collate'",
                    'the old top-level format is refused');
                await expectThrows(() => attempt({ columns: { memo: { desc: 'yes' } } }), 'must be true or false', 'non-boolean desc');
                await expectThrows(() => attempt({ columns: { memo: 'x' } }), "options for 'memo' must be an object",
                    'non-object column value');
                await expectThrows(() => attempt({ columns: ['memo'] }), "'columns' must map", 'non-object columns');
                await expectThrows(() => attempt([1, 2]), 'must be an object', 'non-object options');
                assertEquals((await target.getIndexState()).spec, undefined, 'no spec was installed');
                assertEquals(physicalIndexes(db, 'ledger').size, 0, 'no index was built');

                assertEquals(target.validateIndexOptions(financeDecl('ix', 'ledger', ['memo'], {})), undefined, '{} is no options');
                assertEquals(target.validateIndexOptions(financeDecl('ix', 'ledger', ['memo'], { columns: {} })), undefined,
                    '{ columns: {} } is no options');
                db.close();
            },
        },
        {
            name: '[ADPTS-IDX03] a remote drop-column on an indexed column succeeds (index dropped first) and rebuilds on re-add',
            invoke: async () => {
                const { schema, group, admin } = await createGroup();
                await (await group.getTable('ledger')).insert('l1', { ref: 'R-1', amount: '10.00', memo: 'x' }, admin);
                const db = new Database(':memory:');
                const target = new SqliteTarget(db);
                await projectGroup(group, target);
                const members: GroupProjection[] = [{ group, config: {} }];
                await reconcileIndexes(members, target, {
                    indexes: [financeDecl('by_memo', 'ledger', ['memo', 'ref']), financeDecl('by_ref', 'ledger', ['ref'])],
                });

                const deploy = async (rules: Parameters<typeof schema.updateSchema>[0]) => {
                    await schema.updateSchema(rules, admin, 'migrate');
                    await group.deploy(await (await schema.getScopedDag()).getFrontier());
                    await projectGroup(group, target);
                };
                await deploy([{ rule: 'drop-column', table: 'ledger', column: 'memo' }]);
                assertTrue(!tableInfo(db, 'ledger').some((c) => c.name === 'memo'), 'memo is gone');
                assertEquals([...physicalIndexes(db, 'ledger').keys()].join(','), 'ledger__by_ref', 'only by_ref remains');
                assertEquals(indexMetaNames(db).join(','), 'ledger.by_ref', 'by_memo record removed');

                await deploy([{ rule: 'add-column', table: 'ledger', column: 'memo', def: { type: 'string', nullable: true } }]);
                assertEquals([...physicalIndexes(db, 'ledger').keys()].join(','), 'ledger__by_memo,ledger__by_ref',
                    'the declaration completes again when memo returns');
                db.close();
            },
        },
        {
            name: '[ADPTS-IDX04] a tighten rebuild keeps managed and hand-made indexes; a foreign key column drops like any other: its managed index goes first, and a hand-made one blocks the drop until it is removed',
            invoke: async () => {
                const db = new Database(':memory:');
                const target = new SqliteTarget(db);
                const gid = 'g';
                const create: SchemaAction = {
                    kind: 'create-table', table: 'acct', syncTable: 'acct_sync', primaryKey: 'id',
                    columns: [
                        { name: 'ref', def: { type: 'string' } },
                        { name: 'parent_id', def: { type: 'integer', nullable: true }, fk: { targetTable: 'acct' } },
                    ],
                };
                const row: RowAction = { kind: 'upsert-row', table: 'acct', rowId: 'r1', values: { ref: 'a' } };
                await target.apply(gid, [create,
                    { kind: 'ensure-index', index: handIndex('by_ref', 'acct', 'ref') },
                    { kind: 'ensure-index', index: handIndex('by_parent', 'acct', 'parent_id') }],
                    [row], new Set(['v1']));
                db.exec('CREATE INDEX hand_ref ON acct (ref)');
                db.exec('CREATE INDEX hand_parent ON acct (parent_id)');

                // A required column added to a non-empty table: relaxed on ADD, then
                // tightened by a table rebuild after the backfill.
                await target.apply(gid,
                    [{ kind: 'add-column', table: 'acct', column: 'status', def: { type: 'string' } }],
                    [{ kind: 'upsert-row', table: 'acct', rowId: 'r1', values: { status: 'open' } }],
                    new Set(['v2']));
                assertEquals(tableInfo(db, 'acct').find((c) => c.name === 'status')?.notnull, 1, 'sanity: status tightened');
                assertEquals([...physicalIndexes(db, 'acct').keys()].join(','), 'acct__by_parent,acct__by_ref,hand_parent,hand_ref',
                    'the tighten rebuild kept every index');

                // The drop of the foreign key column, as the core plans it: the managed
                // index on it is dropped first. The hand-made one makes the drop fail.
                const dropParent: SchemaAction[] = [
                    { kind: 'drop-index', table: 'acct', name: 'by_parent' },
                    { kind: 'drop-column', table: 'acct', column: 'parent_id' },
                ];
                await expectThrows(() => target.apply(gid, dropParent, [], new Set(['v3'])), 'hand_parent',
                    'a hand-made index on the foreign key column blocks its drop');
                assertTrue(tableInfo(db, 'acct').some((c) => c.name === 'parent_id'), 'the apply rolled back: parent_id is there');
                assertEquals([...physicalIndexes(db, 'acct').keys()].join(','), 'acct__by_parent,acct__by_ref,hand_parent,hand_ref',
                    'and so is every index');
                assertTrue(sameVersion(await target.getCheckpoint(gid), new Set(['v2'])), 'and the checkpoint did not move');

                db.exec('DROP INDEX hand_parent');
                await target.apply(gid, dropParent, [], new Set(['v3']));
                assertTrue(!tableInfo(db, 'acct').some((c) => c.name === 'parent_id'), 'with it removed, the column drops');
                assertEquals([...physicalIndexes(db, 'acct').keys()].join(','), 'acct__by_ref,hand_ref',
                    'and the other indexes stay');
                assertEquals(indexMetaNames(db).join(','), 'acct.by_ref', 'the managed record of the dropped index is gone');
                db.close();
            },
        },
        {
            name: '[ADPTS-IDX05] installIndexSpec is atomic and compare-and-set guarded; apply checks expectIndexSpec',
            invoke: async () => {
                const db = new Database(':memory:');
                const target = new SqliteTarget(db);
                const gid = 'g';
                const v1: Version = new Set(['v1']);
                await target.apply(gid, [{
                    kind: 'create-table', table: 'acct', syncTable: 'acct_sync', primaryKey: 'id',
                    columns: [{ name: 'ref', def: { type: 'string' } }],
                }], [], v1);
                const spec: IndexSpec = { indexes: [] };
                const expect = { specFingerprint: undefined, checkpoints: new Map([[gid, v1]]) };

                await expectThrows(() => target.installIndexSpec(spec, 'fp1', [
                    { kind: 'ensure-index', index: handIndex('by_ref', 'acct', 'ref') },
                    { kind: 'ensure-index', index: handIndex('by_ghost', 'acct', 'ghost') },
                ], expect), 'ghost', 'an unbuildable ensure fails the install');
                assertEquals(physicalIndexes(db, 'acct').size, 0, 'the valid ensure was rolled back');
                assertEquals((await target.getIndexState()).spec, undefined, 'no spec was recorded');

                await expectThrows(() => target.installIndexSpec(spec, 'fp1', [],
                    { specFingerprint: undefined, checkpoints: new Map([[gid, new Set(['elsewhere'])]]) }),
                    CheckpointMovedError, 'a moved checkpoint aborts the install');
                await target.installIndexSpec(spec, 'fp1',
                    [{ kind: 'ensure-index', index: handIndex('by_ref', 'acct', 'ref') }], expect);
                assertEquals((await target.getIndexState()).specFingerprint, 'fp1', 'installed');
                await expectThrows(() => target.installIndexSpec(spec, 'fp2', [], expect),
                    CheckpointMovedError, 'a stale installed-spec expectation aborts the install');

                await expectThrows(() => target.apply(gid, [], [], new Set(['v2']), undefined, v1, null),
                    CheckpointMovedError, 'apply planned against no spec is refused once one is installed');
                await target.apply(gid, [], [], new Set(['v2']), undefined, v1, 'fp1');
                assertTrue(sameVersion(await target.getCheckpoint(gid), new Set(['v2'])), 'apply with the right spec lands');

                // Collisions with an unmanaged index are refused, never adopted.
                db.exec('CREATE INDEX "acct__by_hand" ON acct (ref)');
                await expectThrows(() => target.apply(gid,
                    [{ kind: 'ensure-index', index: handIndex('by_hand', 'acct', 'ref') }], [], new Set(['v3'])),
                    'already exists', 'a name collision with an unmanaged index fails the apply');
                db.close();
            },
        },
    ],
};
