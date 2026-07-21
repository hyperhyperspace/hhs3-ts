import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import Database from "better-sqlite3";

import { createMockRContext } from "../../rdb/test/mock_rcontext.js";
import {
    RSchemaImpl, rSchemaFactory, RTableGroupImpl, rTableGroupFactory,
    TableDef, deriveRowId,
} from "@hyper-hyper-space/hhs3_rdb";

import { SchemaAction } from "../src/types.js";
import { SqliteTarget } from "../src/sqlite_target.js";
import { projectGroup } from "../src/project.js";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

// Two tables exercising precise types: `ref` readonly pub string, `memo`
// nullable bounded string, `amount` a scale-2 decimal (canonical string carrier).
function baseTables(): TableDef[] {
    return [
        {
            name: 'ledger',
            columns: {
                ref: { type: 'string', pub: true, readonly: true },
                memo: { type: 'string', nullable: true, constraints: { maxLength: 8 } },
                amount: { type: 'decimal', constraints: { scale: 2 } },
            },
            restrictions: [{ on: 'all', rule: { p: 'true' } }],
        },
        {
            name: 'tags',
            columns: {
                code: { type: 'string', pub: true },
            },
            restrictions: [{ on: 'all', rule: { p: 'true' } }],
        },
    ];
}

async function createGroup() {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);

    const admin = await makeIdentity();
    const schemaInit = await RSchemaImpl.create({
        name: 'finance',
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        tables: baseTables(),
    });
    const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
    const pinned = await (await schema.getScopedDag()).getFrontier();

    const groupInit = await RTableGroupImpl.create({
        name: 'finance-prod', seed: 'finance-prod', schemaRef: schema.getId(), schemaVersion: pinned,
    });
    const group = (await ctx.createObject(groupInit)) as RTableGroupImpl;
    return { ctx, schema, group, admin };
}

async function frontier(group: RTableGroupImpl): Promise<Version> {
    return (await group.getScopedDag()).getFrontier();
}

function sameVersion(a: Version | undefined, b: Version): boolean {
    if (a === undefined) return false;
    if (a.size !== b.size) return false;
    for (const h of b) if (!a.has(h)) return false;
    return true;
}

type ColInfo = { name: string; type: string; notnull: number; pk: number; dflt_value: string | null };
function tableInfo(db: Database.Database, table: string): ColInfo[] {
    return db.prepare('SELECT name, type, "notnull", pk, dflt_value FROM pragma_table_info(?)').all(table) as ColInfo[];
}

function tableExists(db: Database.Database, table: string): boolean {
    return db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(table) !== undefined;
}

export const sqliteTargetTests = {
    title: '[ADPTS] rdb_adapter SQLite target',
    tests: [
        {
            name: '[ADPTS01] initial materialization: schema shape, row values + author, sync mapping, checkpoint',
            invoke: async () => {
                const { group, admin } = await createGroup();
                const ledger = await group.getTable('ledger');
                await ledger.insert('l1', { ref: 'R-1', amount: '10.00', memo: 'paid' }, admin);
                await ledger.insert('l2', { ref: 'R-2', amount: '20.00' }, admin);

                const to = await frontier(group);
                const db = new Database(':memory:');
                const target = new SqliteTarget(db);
                await projectGroup(group, target);

                // App table shape: id INTEGER PK, author, business cols with mapped types.
                const info = tableInfo(db, 'ledger');
                const byName = new Map(info.map((c) => [c.name, c]));
                assertTrue(byName.get('id')?.pk === 1 && byName.get('id')?.type === 'INTEGER',
                    'id is the INTEGER primary key');
                assertTrue(byName.has('author'), 'author system column materialized');
                assertEquals(byName.get('ref')?.type, 'TEXT', 'string maps to TEXT');
                assertEquals(byName.get('amount')?.type, 'TEXT', 'decimal maps to TEXT (canonical carrier)');
                assertTrue(byName.get('amount')?.notnull === 1, 'non-nullable decimal is NOT NULL');
                assertTrue(byName.get('memo')?.notnull === 0, 'nullable string is not NOT NULL');

                // Rows readable with correct values + author.
                const rows = db.prepare('SELECT id, author, ref, memo, amount FROM ledger ORDER BY ref').all() as
                    Array<{ id: number; author: string; ref: string; memo: string | null; amount: string }>;
                assertEquals(rows.length, 2, 'both rows materialized');
                assertEquals(rows[0].ref, 'R-1', 'first row ref');
                assertEquals(rows[0].amount, '10.00', 'canonical decimal preserved verbatim');
                assertEquals(rows[0].memo, 'paid', 'memo value');
                assertEquals(rows[0].author, admin.keyId, 'author materialized in-row');
                assertEquals(rows[1].memo, null, 'omitted nullable column is NULL');

                // Sync table maps row_hash -> the app row id.
                const l1Hash = deriveRowId('l1', admin.keyId);
                const syncRow = db.prepare('SELECT id, row_hash FROM ledger_sync WHERE row_hash=?').get(l1Hash) as
                    { id: number; row_hash: string } | undefined;
                assertTrue(syncRow !== undefined, 'sync row present for l1');
                const appL1 = db.prepare('SELECT id FROM ledger WHERE ref=?').get('R-1') as { id: number };
                assertEquals(syncRow!.id, appL1.id, 'sync id equals the app row id');

                // Checkpoint persisted at the projected frontier.
                assertTrue(sameVersion(await target.getCheckpoint(), to), 'checkpoint equals the projected version');

                // The empty `tags` table still gets its table + sync table.
                assertTrue(tableExists(db, 'tags') && tableExists(db, 'tags_sync'), 'empty table still materialized');
                db.close();
            },
        },
        {
            name: '[ADPTS02] incremental insert/update/delete: id stability, in-place update, sync row survives delete',
            invoke: async () => {
                const { group, admin } = await createGroup();
                const ledger = await group.getTable('ledger');
                await ledger.insert('l1', { ref: 'R-1', amount: '10.00' }, admin);

                const db = new Database(':memory:');
                const target = new SqliteTarget(db);
                await projectGroup(group, target);   // initial

                const cp1 = await target.getCheckpoint();
                const l1Id = (db.prepare('SELECT id FROM ledger WHERE ref=?').get('R-1') as { id: number }).id;

                // Incremental: add a row and update the existing one.
                const l1 = deriveRowId('l1', admin.keyId);
                await ledger.insert('l2', { ref: 'R-2', amount: '20.00' }, admin);
                await ledger.update(l1, { memo: 'note' }, admin);
                await projectGroup(group, target);

                const l1After = db.prepare('SELECT id, memo FROM ledger WHERE ref=?').get('R-1') as
                    { id: number; memo: string };
                assertEquals(l1After.id, l1Id, 'update keeps the same projection-local id (stable)');
                assertEquals(l1After.memo, 'note', 'update mutates in place');
                const l2 = db.prepare('SELECT id, amount FROM ledger WHERE ref=?').get('R-2') as
                    { id: number; amount: string };
                assertTrue(l2.id !== l1Id, 'the new row gets its own id');
                assertEquals(l2.amount, '20.00', 'new row value materialized');
                assertTrue(!sameVersion(cp1, await frontier(group)), 'sanity: frontier advanced');
                assertTrue(sameVersion(await target.getCheckpoint(), await frontier(group)),
                    'checkpoint advanced to the new frontier');

                // Delete: the app row goes, the sync row survives (id-stability mechanism).
                await ledger.delete(l1, admin);
                await projectGroup(group, target);
                assertTrue(db.prepare('SELECT 1 FROM ledger WHERE ref=?').get('R-1') === undefined,
                    'deleted row removed from the app table');
                const syncSurvivor = db.prepare('SELECT id FROM ledger_sync WHERE row_hash=?').get(l1) as
                    { id: number } | undefined;
                assertTrue(syncSurvivor !== undefined && syncSurvivor.id === l1Id,
                    'sync row is kept with the same id after delete');
                db.close();
            },
        },
        {
            name: '[ADPTS03] DDL via hand-fed actions: add-column (default), drop-column, drop-table (+sync)',
            invoke: async () => {
                const db = new Database(':memory:');
                const target = new SqliteTarget(db);
                const v1: Version = new Set(['v1']);

                const create: SchemaAction = {
                    kind: 'create-table', table: 'acct', syncTable: 'acct_sync', primaryKey: 'id',
                    authorColumn: 'author',
                    columns: [
                        { name: 'ref', def: { type: 'string' } },
                        { name: 'balance', def: { type: 'decimal', constraints: { scale: 2 } } },
                    ],
                };
                await target.apply([create], [], v1);
                assertTrue(tableExists(db, 'acct') && tableExists(db, 'acct_sync'), 'create-table makes both tables');

                // add-column with a NOT NULL + DEFAULT (SQLite ADD COLUMN rule).
                const v2: Version = new Set(['v2']);
                const addCol: SchemaAction = {
                    kind: 'add-column', table: 'acct', column: 'status',
                    def: { type: 'string', default: 'open' },
                };
                await target.apply([addCol], [], v2);
                const status = tableInfo(db, 'acct').find((c) => c.name === 'status');
                assertTrue(status !== undefined && status.type === 'TEXT' && status.notnull === 1,
                    'added column present, NOT NULL');
                assertEquals(status!.dflt_value ?? undefined, "'open'", 'default literal quoted as TEXT');

                // drop-column.
                const v3: Version = new Set(['v3']);
                await target.apply([{ kind: 'drop-column', table: 'acct', column: 'balance' }], [], v3);
                assertTrue(!tableInfo(db, 'acct').some((c) => c.name === 'balance'), 'dropped column gone');

                // drop-table removes the app + sync tables.
                const v4: Version = new Set(['v4']);
                await target.apply([{ kind: 'drop-table', table: 'acct', syncTable: 'acct_sync' }], [], v4);
                assertTrue(!tableExists(db, 'acct') && !tableExists(db, 'acct_sync'),
                    'drop-table removes app and sync tables');
                assertTrue(sameVersion(await target.getCheckpoint(), v4), 'checkpoint tracks each DDL apply');
                db.close();
            },
        },
        {
            name: '[ADPTS04] atomicity: a throwing apply rolls back schema, rows, and the checkpoint',
            invoke: async () => {
                const db = new Database(':memory:');
                const target = new SqliteTarget(db);
                const v1: Version = new Set(['v1']);

                const create: SchemaAction = {
                    kind: 'create-table', table: 'acct', syncTable: 'acct_sync', primaryKey: 'id',
                    columns: [{ name: 'ref', def: { type: 'string' } }],
                };
                // A row action targeting an un-materialized table throws (loadMeta),
                // AFTER the valid create-table ran in the same transaction.
                const badRow = { kind: 'upsert-row' as const, table: 'ghost', rowId: 'r1', values: { ref: 'x' } };

                let threw = false;
                try {
                    await target.apply([create], [badRow], v1);
                } catch {
                    threw = true;
                }
                assertTrue(threw, 'apply throws when a row action references an unknown table');
                assertTrue(!tableExists(db, 'acct'), 'create-table rolled back with the failed batch');
                assertEquals(await target.getCheckpoint(), undefined, 'checkpoint not advanced on rollback');
                db.close();
            },
        },
    ],
};
