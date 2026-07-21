import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "../../rdb/test/mock_rcontext.js";
import {
    RSchemaImpl, rSchemaFactory, RTableGroupImpl, rTableGroupFactory,
    RSchemaDelta, RSchemaChanges, TableDef, MigrationRule,
} from "@hyper-hyper-space/hhs3_rdb";

import { AdapterConfig, MaterializationTarget, RowAction, SchemaAction } from "../src/types.js";
import { initialSchemaActions, schemaDeltaActions } from "../src/schema_actions.js";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

// A base schema with two tables exercising precise types + constraints.
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

async function createSchema(tables: TableDef[]) {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);

    const admin = await makeIdentity();
    const schemaInit = await RSchemaImpl.create({
        name: 'finance',
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        tables,
    });
    const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
    return { ctx, schema, admin };
}

async function createGroup() {
    const { ctx, schema, admin } = await createSchema(baseTables());
    const pinned = await (await schema.getScopedDag()).getFrontier();
    const groupInit = await RTableGroupImpl.create({
        name: 'finance-prod', seed: 'finance-prod', schemaRef: schema.getId(), schemaVersion: pinned,
    });
    const group = (await ctx.createObject(groupInit)) as RTableGroupImpl;
    return { ctx, schema, group, admin };
}

async function frontier(schema: RSchemaImpl): Promise<Version> {
    return (await schema.getScopedDag()).getFrontier();
}

// Apply a migration to the schema and return the RSchemaChanges + end view for
// the mapper (schema-level: the group delegates its schema channel to exactly
// this schema.computeDelta, so this is the same input the group produces).
async function migrate(schema: RSchemaImpl, admin: OwnIdentity, migration: MigrationRule[]) {
    const before = await frontier(schema);
    await schema.updateSchema(migration, admin);
    const after = await frontier(schema);
    const delta = (await schema.computeDelta(before, after)) as RSchemaDelta;
    const endView = await schema.getView(after, after);
    return { changes: delta.changes as RSchemaChanges, endView };
}

class RecordingTarget implements MaterializationTarget {
    readonly batches: { schemaActions: SchemaAction[]; rowActions: RowAction[]; checkpoint: Version }[] = [];
    private checkpoint: Version | undefined;

    async apply(schemaActions: SchemaAction[], rowActions: RowAction[], checkpoint: Version): Promise<void> {
        this.batches.push({ schemaActions, rowActions, checkpoint });
        this.checkpoint = checkpoint;
    }

    async getCheckpoint(): Promise<Version | undefined> {
        return this.checkpoint;
    }
}

function actionOfKind(actions: SchemaAction[], kind: SchemaAction['kind']): SchemaAction[] {
    return actions.filter((a) => a.kind === kind);
}

function expectThrow(fn: () => void, why: string) {
    let threw = false;
    try { fn(); } catch { threw = true; }
    assertTrue(threw, why);
}

export const schemaActionsTests = {
    title: '[ADPT] rdb_adapter schema actions',
    tests: [
        {
            name: '[ADPT01] initial materialization end-to-end preserves precise types + advances checkpoint',
            invoke: async () => {
                const { group } = await createGroup();
                const target = new RecordingTarget();

                assertEquals(await target.getCheckpoint(), undefined, 'fresh target has no checkpoint');

                const view = (await group.getView()).getSchemaView();
                const to = await (await group.getScopedDag()).getFrontier();
                const actions = initialSchemaActions(view, {});
                await target.apply(actions, [], to);

                const creates = actionOfKind(actions, 'create-table');
                assertEquals(creates.length, 2, 'one create-table per table');

                const ledger = creates.find((a) => a.kind === 'create-table' && a.table === 'ledger');
                assertTrue(ledger !== undefined && ledger.kind === 'create-table', 'ledger create-table present');
                if (ledger === undefined || ledger.kind !== 'create-table') return;
                assertEquals(ledger.primaryKey, 'id', 'default id primary key');
                assertEquals(ledger.syncTable, 'ledger_sync', 'default sync-table name derived from table + suffix');
                assertEquals(ledger.authorColumn, 'author', 'default author system column named');
                assertTrue(!ledger.columns.some((c) => c.name === 'id' || c.name === 'author'),
                    'system columns are not emitted as business columns');
                const amount = ledger.columns.find((c) => c.name === 'amount');
                assertTrue(amount?.def.type === 'decimal' && amount?.def.constraints?.scale === 2,
                    'decimal type + scale preserved verbatim');
                const memo = ledger.columns.find((c) => c.name === 'memo');
                assertTrue(memo?.def.constraints?.maxLength === 8, 'string maxLength preserved verbatim');

                assertEquals(await target.getCheckpoint(), to, 'checkpoint advanced to the materialized version');
            },
        },
        {
            name: '[ADPT02] delta: add-table and add-column map to create-table / add-column',
            invoke: async () => {
                const { schema, admin } = await createGroup();
                const { changes, endView } = await migrate(schema, admin, [
                    { rule: 'add-table', def: {
                        name: 'audit',
                        columns: { seq: { type: 'bigint', pub: true } },
                        restrictions: [{ on: 'all', rule: { p: 'true' } }],
                    } },
                    { rule: 'add-column', table: 'ledger', column: 'note', def: { type: 'string', nullable: true } },
                ]);

                const actions = schemaDeltaActions(changes, endView, {});
                const creates = actionOfKind(actions, 'create-table');
                const adds = actionOfKind(actions, 'add-column');

                assertEquals(creates.length, 1, 'one create-table for the added table');
                assertTrue(creates[0].kind === 'create-table' && creates[0].table === 'audit', 'audit table created');
                assertEquals(adds.length, 1, 'one add-column');
                assertTrue(adds[0].kind === 'add-column' && adds[0].table === 'ledger' && adds[0].column === 'note',
                    'ledger.note added');
            },
        },
        {
            name: '[ADPT03] delta: drop-table and drop-column map to drop actions',
            invoke: async () => {
                const { schema, admin } = await createGroup();
                const { changes, endView } = await migrate(schema, admin, [
                    { rule: 'drop-table', table: 'tags' },
                    { rule: 'drop-column', table: 'ledger', column: 'memo' },
                ]);

                const actions = schemaDeltaActions(changes, endView, {});
                assertEquals(actionOfKind(actions, 'drop-table').length, 1, 'one drop-table');
                assertEquals(actionOfKind(actions, 'drop-column').length, 1, 'one drop-column');
                const dropTable = actions.find((a) => a.kind === 'drop-table');
                assertTrue(dropTable?.kind === 'drop-table' && dropTable.table === 'tags', 'tags dropped');
                const dropCol = actions.find((a) => a.kind === 'drop-column');
                assertTrue(dropCol?.kind === 'drop-column' && dropCol.table === 'ledger' && dropCol.column === 'memo',
                    'ledger.memo dropped');
            },
        },
        {
            name: '[ADPT04] delta: a changed column becomes drop-column then add-column (ordered)',
            invoke: async () => {
                const { schema, admin } = await createGroup();
                // drop + re-add the same column name with a different type: a new
                // incarnation, which the schema delta reports as before+after.
                const { changes, endView } = await migrate(schema, admin, [
                    { rule: 'drop-column', table: 'ledger', column: 'memo' },
                    { rule: 'add-column', table: 'ledger', column: 'memo', def: { type: 'integer', nullable: true } },
                ]);

                const actions = schemaDeltaActions(changes, endView, {});
                const memoActions = actions.filter((a) =>
                    (a.kind === 'drop-column' || a.kind === 'add-column') && a.table === 'ledger' && a.column === 'memo');
                assertEquals(memoActions.length, 2, 'changed column yields two actions');
                assertEquals(memoActions[0].kind, 'drop-column', 'drop precedes add');
                assertEquals(memoActions[1].kind, 'add-column', 'add follows drop');
                assertTrue(memoActions[1].kind === 'add-column' && memoActions[1].def.type === 'integer',
                    'the re-added column carries the new type');
            },
        },
        {
            name: '[ADPT05] renames: table / column / id / author overrides are applied',
            invoke: async () => {
                const { group } = await createGroup();
                const config: AdapterConfig = {
                    tableNames: { ledger: 'accounts' },
                    columnNames: { ledger: { memo: 'note' } },
                    idColumn: 'pk',
                    authorColumn: 'owner',
                };
                const view = (await group.getView()).getSchemaView();
                const actions = initialSchemaActions(view, config);

                const accounts = actions.find((a) => a.kind === 'create-table' && a.table === 'accounts');
                assertTrue(accounts !== undefined && accounts.kind === 'create-table', 'ledger renamed to accounts');
                if (accounts === undefined || accounts.kind !== 'create-table') return;
                assertEquals(accounts.primaryKey, 'pk', 'custom id column name applied');
                assertEquals(accounts.authorColumn, 'owner', 'custom author column name applied');
                assertTrue(accounts.columns.some((c) => c.name === 'note'), 'memo renamed to note');
                assertTrue(!accounts.columns.some((c) => c.name === 'memo'), 'original column name not emitted');

                // authorColumn: false omits authorship entirely.
                const noAuthor = initialSchemaActions(view, { authorColumn: false });
                const ledger = noAuthor.find((a) => a.kind === 'create-table' && a.table === 'ledger');
                assertTrue(ledger?.kind === 'create-table' && ledger.authorColumn === undefined,
                    'authorColumn: false omits the author column');
            },
        },
        {
            name: '[ADPT06] collisions are rejected (dupes, system columns, sync-name reservation)',
            invoke: async () => {
                const { group } = await createGroup();
                const view = (await group.getView()).getSchemaView();

                expectThrow(() => initialSchemaActions(view, { columnNames: { ledger: { memo: 'ref' } } }),
                    'two columns mapping to the same name should throw');
                expectThrow(() => initialSchemaActions(view, { columnNames: { ledger: { memo: 'id' } } }),
                    'a column colliding with the id column should throw');
                expectThrow(() => initialSchemaActions(view, { columnNames: { ledger: { memo: 'author' } } }),
                    'a column colliding with the author column should throw');
                expectThrow(() => initialSchemaActions(view, { tableNames: { ledger: 'x', tags: 'x' } }),
                    'two tables mapping to the same name should throw');
                // a real table named like another table's sync table must be rejected.
                expectThrow(() => initialSchemaActions(view, { tableNames: { tags: 'ledger_sync' } }),
                    'a table colliding with a sync-table name should throw');
                // custom suffix reservation.
                expectThrow(() => initialSchemaActions(view, {
                    syncTableSuffix: '__s', tableNames: { tags: 'ledger__s' },
                }), 'a table colliding with a custom sync-table suffix should throw');
            },
        },
        {
            name: '[ADPT07] empty delta yields no actions; ordering is drops-before-adds across a mixed migration',
            invoke: async () => {
                const { schema, admin } = await createGroup();

                // empty delta (start == end)
                const v = await frontier(schema);
                const emptyDelta = (await schema.computeDelta(v, v)) as RSchemaDelta;
                const emptyView = await schema.getView(v, v);
                assertEquals(schemaDeltaActions(emptyDelta.changes as RSchemaChanges, emptyView, {}).length, 0,
                    'no schema changes -> no actions');

                // mixed migration: drop-table, add-table, add-column, drop-column
                const { changes, endView } = await migrate(schema, admin, [
                    { rule: 'drop-table', table: 'tags' },
                    { rule: 'add-table', def: {
                        name: 'audit', columns: { x: { type: 'integer' } },
                        restrictions: [{ on: 'all', rule: { p: 'true' } }],
                    } },
                    { rule: 'add-column', table: 'ledger', column: 'note', def: { type: 'string', nullable: true } },
                    { rule: 'drop-column', table: 'ledger', column: 'amount' },
                ]);
                const actions = schemaDeltaActions(changes, endView, {});
                const kinds = actions.map((a) => a.kind);

                const lastDropTable = kinds.lastIndexOf('drop-table');
                const firstCreateTable = kinds.indexOf('create-table');
                const lastDropColumn = kinds.lastIndexOf('drop-column');
                const firstAddColumn = kinds.indexOf('add-column');
                assertTrue(lastDropTable < firstCreateTable, 'drop-table before create-table');
                assertTrue(firstCreateTable < lastDropColumn, 'tables before columns');
                assertTrue(lastDropColumn < firstAddColumn, 'drop-column before add-column');
            },
        },
    ],
};
