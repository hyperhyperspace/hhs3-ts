import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "@hyper-hyper-space/hhs3_rdb_adapter_test_gen";
import {
    RSchemaImpl, rSchemaFactory, RTableGroupImpl, rTableGroupFactory,
    RSchemaDelta, RSchemaChanges, RSchemaView, TableDef, MigrationRule,
} from "@hyper-hyper-space/hhs3_rdb";

import { AdapterConfig, MaterializationTarget, RowAction, SchemaAction } from "../src/types.js";
import { initialSchemaActions, reprojectedTables, schemaDeltaActions } from "../src/schema_actions.js";

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

// A schema whose `comments` table carries a self-referential local FK
// (`parent` -> comments) and a cross-table local FK (`post` -> posts).
function localFkTables(): TableDef[] {
    return [
        {
            name: 'posts',
            columns: { title: { type: 'string' } },
            restrictions: [{ on: 'all', rule: { p: 'true' } }],
        },
        {
            name: 'comments',
            columns: {
                body: { type: 'string' },
                post: { type: 'string', nullable: true },
                parent: { type: 'string', nullable: true },
            },
            fks: { post: 'posts', parent: 'comments' },
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
    const startView = await schema.getView(before, before);
    return { changes: delta.changes as RSchemaChanges, endView, startView };
}

class RecordingTarget implements MaterializationTarget {
    readonly batches: { groupId: string; schemaActions: SchemaAction[]; rowActions: RowAction[]; checkpoint: Version }[] = [];
    private checkpoints = new Map<string, Version>();

    async apply(groupId: string, schemaActions: SchemaAction[], rowActions: RowAction[], checkpoint: Version): Promise<void> {
        this.batches.push({ groupId, schemaActions, rowActions, checkpoint });
        this.checkpoints.set(groupId, checkpoint);
    }

    async getCheckpoint(groupId: string): Promise<Version | undefined> {
        return this.checkpoints.get(groupId);
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
                const groupId = group.getId();

                assertEquals(await target.getCheckpoint(groupId), undefined, 'fresh target has no checkpoint');

                const view = (await group.getView()).getSchemaView();
                const to = await (await group.getScopedDag()).getFrontier();
                const actions = initialSchemaActions(view, {});
                await target.apply(groupId, actions, [], to);

                const creates = actionOfKind(actions, 'create-table');
                assertEquals(creates.length, 2, 'one create-table per table');

                const ledger = creates.find((a) => a.kind === 'create-table' && a.table === 'ledger');
                assertTrue(ledger !== undefined && ledger.kind === 'create-table', 'ledger create-table present');
                if (ledger === undefined || ledger.kind !== 'create-table') return;
                assertEquals(ledger.primaryKey, 'id', 'default id primary key');
                assertEquals(ledger.syncTable, 'ledger_sync', 'default sync-table name derived from table + suffix');
                assertEquals(ledger.authorColumn, 'author_key_id', 'default author system column named');
                assertTrue(!ledger.columns.some((c) => c.name === 'id' || c.name === 'author_key_id'),
                    'system columns are not emitted as business columns');
                const amount = ledger.columns.find((c) => c.name === 'amount');
                assertTrue(amount?.def.type === 'decimal' && amount?.def.constraints?.scale === 2,
                    'decimal type + scale preserved verbatim');
                const memo = ledger.columns.find((c) => c.name === 'memo');
                assertTrue(memo?.def.constraints?.maxLength === 8, 'string maxLength preserved verbatim');

                assertEquals(await target.getCheckpoint(groupId), to, 'checkpoint advanced to the materialized version');
            },
        },
        {
            name: '[ADPT02] delta: add-table and add-column map to create-table / add-column',
            invoke: async () => {
                const { schema, admin } = await createGroup();
                const { changes, endView, startView } = await migrate(schema, admin, [
                    { rule: 'add-table', def: {
                        name: 'audit',
                        columns: { seq: { type: 'bigint', pub: true } },
                        restrictions: [{ on: 'all', rule: { p: 'true' } }],
                    } },
                    { rule: 'add-column', table: 'ledger', column: 'note', def: { type: 'string', nullable: true } },
                ]);

                const actions = schemaDeltaActions(changes, endView, startView, {});
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
                const { changes, endView, startView } = await migrate(schema, admin, [
                    { rule: 'drop-table', table: 'tags' },
                    { rule: 'drop-column', table: 'ledger', column: 'memo' },
                ]);

                const actions = schemaDeltaActions(changes, endView, startView, {});
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
            name: '[ADPT16] delta: dropping a local FK column drops the reshaped <col>_id companion',
            invoke: async () => {
                const { schema, admin } = await createSchema(localFkTables());
                // The rdb model requires clearing an FK before its backing column
                // may be dropped; both land in the same delta, so at the START
                // version `post` is still an FK and reshapes to `post_id`.
                const { changes, endView, startView } = await migrate(schema, admin, [
                    { rule: 'set-fks', table: 'comments', fks: { parent: 'comments' } },
                    { rule: 'drop-column', table: 'comments', column: 'post' },
                ]);

                const actions = schemaDeltaActions(changes, endView, startView, {});
                const drops = actionOfKind(actions, 'drop-column');
                const postDrop = drops.find((a) => a.kind === 'drop-column' && a.table === 'comments');
                assertTrue(postDrop?.kind === 'drop-column' && postDrop.column === 'post_id',
                    'dropped FK column names the reshaped post_id companion, not the raw post');
            },
        },
        {
            name: '[ADPT17] delta: dropping an identity column drops the reshaped <col>_key_id companion',
            invoke: async () => {
                const { schema, admin } = await createSchema([
                    {
                        name: 'caps',
                        columns: {
                            label: { type: 'string' },
                            grantee: { type: 'identity', nullable: true, readonly: true },
                        },
                        restrictions: [{ on: 'all', rule: { p: 'true' } }],
                    },
                ]);
                const { changes, endView, startView } = await migrate(schema, admin, [
                    { rule: 'drop-column', table: 'caps', column: 'grantee' },
                ]);

                const actions = schemaDeltaActions(changes, endView, startView, {});
                const drops = actionOfKind(actions, 'drop-column');
                assertEquals(drops.length, 1, 'one drop-column');
                assertTrue(drops[0].kind === 'drop-column' && drops[0].table === 'caps'
                    && drops[0].column === 'grantee_key_id',
                    'dropped identity column names the reshaped grantee_key_id companion, not the raw grantee');
            },
        },
        {
            name: '[ADPT18] delta: dropping a plain column still drops the plain name (regression)',
            invoke: async () => {
                const { schema, admin } = await createGroup();
                const { changes, endView, startView } = await migrate(schema, admin, [
                    { rule: 'drop-column', table: 'ledger', column: 'memo' },
                ]);

                const actions = schemaDeltaActions(changes, endView, startView, {});
                const drops = actionOfKind(actions, 'drop-column');
                assertEquals(drops.length, 1, 'one drop-column');
                assertTrue(drops[0].kind === 'drop-column' && drops[0].table === 'ledger'
                    && drops[0].column === 'memo',
                    'plain column drops by its own name');
            },
        },
        {
            name: '[ADPT19] delta: set-fks turning a plain column into a local FK reprojects <col> -> <col>_id',
            invoke: async () => {
                const { schema, admin } = await createSchema([
                    { name: 'posts', columns: { title: { type: 'string' } },
                        restrictions: [{ on: 'all', rule: { p: 'true' } }] },
                    { name: 'comments', columns: { body: { type: 'string' }, post: { type: 'string', nullable: true } },
                        restrictions: [{ on: 'all', rule: { p: 'true' } }] },
                ]);
                // Pure set-fks: no columnChanges entry, only fksChanged. The
                // column's projection nonetheless flips plain -> integer FK.
                const { changes, endView, startView } = await migrate(schema, admin, [
                    { rule: 'set-fks', table: 'comments', fks: { post: 'posts' } },
                ]);

                const actions = schemaDeltaActions(changes, endView, startView, {});
                const drops = actionOfKind(actions, 'drop-column');
                const adds = actionOfKind(actions, 'add-column');
                assertEquals(drops.length, 1, 'exactly one drop-column');
                assertTrue(drops[0].kind === 'drop-column' && drops[0].table === 'comments'
                    && drops[0].column === 'post', 'the raw plain post column is dropped');
                assertEquals(adds.length, 1, 'exactly one add-column');
                const postId = adds[0];
                assertTrue(postId.kind === 'add-column' && postId.table === 'comments'
                    && postId.column === 'post_id' && postId.def.type === 'integer',
                    'the integer post_id companion is added');
                assertTrue(postId.kind === 'add-column' && postId.fk?.targetTable === 'posts',
                    'post_id carries fk metadata targeting posts');
            },
        },
        {
            name: '[ADPT20] delta: set-fks removing a local FK reprojects <col>_id -> plain <col>',
            invoke: async () => {
                const { schema, admin } = await createSchema(localFkTables());
                // Drop only post from the fk map (keep parent); post reverts to a
                // plain nullable string column.
                const { changes, endView, startView } = await migrate(schema, admin, [
                    { rule: 'set-fks', table: 'comments', fks: { parent: 'comments' } },
                ]);

                const actions = schemaDeltaActions(changes, endView, startView, {});
                const drops = actionOfKind(actions, 'drop-column');
                const adds = actionOfKind(actions, 'add-column');
                assertEquals(drops.length, 1, 'exactly one drop-column');
                assertTrue(drops[0].kind === 'drop-column' && drops[0].column === 'post_id',
                    'the post_id companion is dropped');
                assertEquals(adds.length, 1, 'exactly one add-column');
                const post = adds[0];
                assertTrue(post.kind === 'add-column' && post.column === 'post'
                    && post.def.type === 'string', 'the plain post column is re-added as string');
                assertTrue(post.kind === 'add-column' && post.fk === undefined && post.keyRef !== true,
                    'the reverted column carries no fk / keyRef metadata');
            },
        },
        {
            name: '[ADPT21] delta: set-fks retargeting a local FK keeps <col>_id but drops+adds with the new target',
            invoke: async () => {
                const { schema, admin } = await createSchema([
                    { name: 'posts', columns: { title: { type: 'string' } },
                        restrictions: [{ on: 'all', rule: { p: 'true' } }] },
                    { name: 'tags', columns: { label: { type: 'string' } },
                        restrictions: [{ on: 'all', rule: { p: 'true' } }] },
                    { name: 'comments', columns: { body: { type: 'string' }, post: { type: 'string', nullable: true } },
                        fks: { post: 'posts' },
                        restrictions: [{ on: 'all', rule: { p: 'true' } }] },
                ]);
                const { changes, endView, startView } = await migrate(schema, admin, [
                    { rule: 'set-fks', table: 'comments', fks: { post: 'tags' } },
                ]);

                const actions = schemaDeltaActions(changes, endView, startView, {});
                const drops = actionOfKind(actions, 'drop-column');
                const adds = actionOfKind(actions, 'add-column');
                assertEquals(drops.length, 1, 'the old post_id is dropped');
                assertTrue(drops[0].kind === 'drop-column' && drops[0].column === 'post_id', 'drops post_id');
                assertEquals(adds.length, 1, 'the retargeted post_id is added');
                assertTrue(adds[0].kind === 'add-column' && adds[0].column === 'post_id'
                    && adds[0].fk?.targetTable === 'tags',
                    'post_id is re-added with fk now targeting tags');
            },
        },
        {
            name: '[ADPT22] delta: a column changing BOTH def and FK-ness in one delta yields exactly one drop + one add',
            invoke: async () => {
                const { schema, admin } = await createSchema([
                    { name: 'posts', columns: { title: { type: 'string' } },
                        restrictions: [{ on: 'all', rule: { p: 'true' } }] },
                    { name: 'comments', columns: { body: { type: 'string' }, post: { type: 'string', nullable: true } },
                        restrictions: [{ on: 'all', rule: { p: 'true' } }] },
                ]);
                // In one migration: retype post (string -> integer, a new
                // incarnation = drop+add) AND make it an FK. The column appears in
                // BOTH columnChanges and fksChanged; the unified diff must emit a
                // single drop/add, not double-count.
                const { changes, endView, startView } = await migrate(schema, admin, [
                    { rule: 'drop-column', table: 'comments', column: 'post' },
                    { rule: 'add-column', table: 'comments', column: 'post', def: { type: 'integer', nullable: true } },
                    { rule: 'set-fks', table: 'comments', fks: { post: 'posts' } },
                ]);

                const actions = schemaDeltaActions(changes, endView, startView, {});
                const drops = actionOfKind(actions, 'drop-column');
                const adds = actionOfKind(actions, 'add-column');
                assertEquals(drops.length, 1, 'exactly one drop-column (no double-count)');
                assertTrue(drops[0].kind === 'drop-column' && drops[0].column === 'post',
                    'the old plain post is dropped once');
                assertEquals(adds.length, 1, 'exactly one add-column (no double-count)');
                assertTrue(adds[0].kind === 'add-column' && adds[0].column === 'post_id'
                    && adds[0].def.type === 'integer' && adds[0].fk?.targetTable === 'posts',
                    'the integer post_id FK companion is added once');
            },
        },
        {
            name: '[ADPT23] delta: a table-level flip with no projection impact (set-concurrent-deletes) yields no actions',
            invoke: async () => {
                const { schema, admin } = await createGroup();
                const { changes, endView, startView } = await migrate(schema, admin, [
                    { rule: 'set-concurrent-deletes', table: 'ledger', value: true },
                ]);

                const actions = schemaDeltaActions(changes, endView, startView, {});
                assertEquals(actions.length, 0, 'a pure at-use-semantics flip projects to no DDL');
            },
        },
        {
            name: '[ADPT24] reprojectedTables: a set-fks FK->plain flip flags the table (def-unchanged reprojection)',
            invoke: async () => {
                const { schema, admin } = await createSchema(localFkTables());
                // Drop only post from the fk map (keep parent). post's def is
                // unchanged but its projection flips post_id -> plain post, so the
                // pre-existing rows need a live-view backfill.
                const { changes, endView, startView } = await migrate(schema, admin, [
                    { rule: 'set-fks', table: 'comments', fks: { parent: 'comments' } },
                ]);

                const flipped = reprojectedTables(changes, endView, startView, {});
                assertTrue(flipped.has('comments'), 'comments is flagged as reprojected');
                assertEquals(flipped.size, 1, 'only the flipped table is flagged');
            },
        },
        {
            name: '[ADPT25] reprojectedTables: a set-fks plain->FK flip flags the table',
            invoke: async () => {
                const { schema, admin } = await createSchema([
                    { name: 'posts', columns: { title: { type: 'string' } },
                        restrictions: [{ on: 'all', rule: { p: 'true' } }] },
                    { name: 'comments', columns: { body: { type: 'string' }, post: { type: 'string', nullable: true } },
                        restrictions: [{ on: 'all', rule: { p: 'true' } }] },
                ]);
                // Pure set-fks (no columnChanges): post flips plain -> post_id FK.
                const { changes, endView, startView } = await migrate(schema, admin, [
                    { rule: 'set-fks', table: 'comments', fks: { post: 'posts' } },
                ]);

                const flipped = reprojectedTables(changes, endView, startView, {});
                assertTrue(flipped.has('comments'), 'comments is flagged as reprojected');
                assertEquals(flipped.size, 1, 'only the flipped table is flagged');
            },
        },
        {
            name: '[ADPT26] reprojectedTables: pure add / drop / type-change do NOT flag (no over-trigger)',
            invoke: async () => {
                // Pure add-column: the new column is not on both sides.
                {
                    const { schema, admin } = await createGroup();
                    const { changes, endView, startView } = await migrate(schema, admin, [
                        { rule: 'add-column', table: 'ledger', column: 'note', def: { type: 'string', nullable: true } },
                    ]);
                    assertEquals(reprojectedTables(changes, endView, startView, {}).size, 0,
                        'a pure add-column does not flag a reprojection');
                }
                // Pure drop-column: the dropped column is not on both sides.
                {
                    const { schema, admin } = await createGroup();
                    const { changes, endView, startView } = await migrate(schema, admin, [
                        { rule: 'drop-column', table: 'ledger', column: 'memo' },
                    ]);
                    assertEquals(reprojectedTables(changes, endView, startView, {}).size, 0,
                        'a pure drop-column does not flag a reprojection');
                }
                // Type change (drop+add same name, new type): a def change is a new
                // incarnation whose live value is empty for old rows anyway.
                {
                    const { schema, admin } = await createGroup();
                    const { changes, endView, startView } = await migrate(schema, admin, [
                        { rule: 'drop-column', table: 'ledger', column: 'memo' },
                        { rule: 'add-column', table: 'ledger', column: 'memo', def: { type: 'integer', nullable: true } },
                    ]);
                    assertEquals(reprojectedTables(changes, endView, startView, {}).size, 0,
                        'a type change does not flag a reprojection (tracked as a def change)');
                }
            },
        },
        {
            name: '[ADPT04] delta: a changed column becomes drop-column then add-column (ordered)',
            invoke: async () => {
                const { schema, admin } = await createGroup();
                // drop + re-add the same column name with a different type: a new
                // incarnation, which the schema delta reports as before+after.
                const { changes, endView, startView } = await migrate(schema, admin, [
                    { rule: 'drop-column', table: 'ledger', column: 'memo' },
                    { rule: 'add-column', table: 'ledger', column: 'memo', def: { type: 'integer', nullable: true } },
                ]);

                const actions = schemaDeltaActions(changes, endView, startView, {});
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
                expectThrow(() => initialSchemaActions(view, { columnNames: { ledger: { memo: 'author_key_id' } } }),
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
            name: '[ADPT08] local FK columns project as integer <col>_id with fk metadata (self + cross-table)',
            invoke: async () => {
                const { schema } = await createSchema(localFkTables());
                const view = await schema.getView();
                const actions = initialSchemaActions(view, {});

                const comments = actions.find((a) => a.kind === 'create-table' && a.table === 'comments');
                assertTrue(comments?.kind === 'create-table', 'comments create-table present');
                if (comments === undefined || comments.kind !== 'create-table') return;

                const postId = comments.columns.find((c) => c.name === 'post_id');
                assertTrue(postId?.def.type === 'integer', 'cross-table FK projects as integer post_id');
                assertEquals(postId?.fk?.targetTable, 'posts', 'post_id fk targets posts');
                const parentId = comments.columns.find((c) => c.name === 'parent_id');
                assertTrue(parentId?.def.type === 'integer', 'self FK projects as integer parent_id');
                assertEquals(parentId?.fk?.targetTable, 'comments', 'parent_id fk targets comments (self)');

                assertTrue(!comments.columns.some((c) => c.name === 'post' || c.name === 'parent'),
                    'raw FK column names are not projected');
                assertTrue(parentId?.def.nullable === true, 'nullable carries onto the companion');
            },
        },
        {
            name: '[ADPT09] cross-group FK projects as text <col>_row_hash with no fk metadata',
            invoke: async () => {
                // A cross-group FK needs a bound foreign group to create a real
                // schema, so drive the mapper with a minimal resolved-view stub.
                const mockView = {
                    getTableNames: () => ['comments'],
                    getTable: (n: string) => n === 'comments'
                        ? { name: 'comments', columns: {
                            body: { type: 'string' },
                            origin: { type: 'string', nullable: true },
                        } }
                        : undefined,
                    getFKs: (n: string) => n === 'comments' ? { origin: 'archive.entries' } : {},
                } as unknown as RSchemaView;

                const actions = initialSchemaActions(mockView, {});
                const create = actions.find((a) => a.kind === 'create-table' && a.table === 'comments');
                assertTrue(create?.kind === 'create-table', 'comments create-table present');
                if (create === undefined || create.kind !== 'create-table') return;

                const origin = create.columns.find((c) => c.name === 'origin_row_hash');
                assertTrue(origin?.def.type === 'string', 'cross-group FK projects as a text row_hash column');
                assertEquals(origin?.fk, undefined, 'cross-group FK carries no fk metadata (passthrough)');
                assertTrue(!create.columns.some((c) => c.name === 'origin_id' || c.name === 'origin'),
                    'no integer id companion and not the raw name for a cross-group FK');
            },
        },
        {
            name: '[ADPT10] co-projected cross-group FK resolves to integer <col>_id with crossGroup fk metadata',
            invoke: async () => {
                const mockView = {
                    getTableNames: () => ['comments'],
                    getTable: (n: string) => n === 'comments'
                        ? { name: 'comments', columns: {
                            body: { type: 'string' },
                            origin: { type: 'string', nullable: true },
                        } }
                        : undefined,
                    getFKs: (n: string) => n === 'comments' ? { origin: 'archive.entries' } : {},
                } as unknown as RSchemaView;

                // A crossGroup resolver that co-projects 'archive.entries' as the
                // group-qualified target table 'archive_entries'.
                const config: AdapterConfig = {
                    crossGroup: (ref) => ref === 'archive.entries' ? 'archive_entries' : undefined,
                };
                const actions = initialSchemaActions(mockView, config);
                const create = actions.find((a) => a.kind === 'create-table' && a.table === 'comments');
                assertTrue(create?.kind === 'create-table', 'comments create-table present');
                if (create === undefined || create.kind !== 'create-table') return;

                const origin = create.columns.find((c) => c.name === 'origin_id');
                assertTrue(origin?.def.type === 'integer', 'co-projected cross-group FK projects as an integer id');
                assertEquals(origin?.fk?.targetTable, 'archive_entries', 'fk targets the co-projected foreign table');
                assertEquals(origin?.fk?.crossGroup, true, 'fk is flagged cross-group (no DB-level FK declared)');
                assertTrue(!create.columns.some((c) => c.name === 'origin_row_hash'),
                    'no row_hash passthrough column when the ref is co-projected');
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
                assertEquals(schemaDeltaActions(emptyDelta.changes as RSchemaChanges, emptyView, emptyView, {}).length, 0,
                    'no schema changes -> no actions');

                // mixed migration: drop-table, add-table, add-column, drop-column
                const { changes, endView, startView } = await migrate(schema, admin, [
                    { rule: 'drop-table', table: 'tags' },
                    { rule: 'add-table', def: {
                        name: 'audit', columns: { x: { type: 'integer' } },
                        restrictions: [{ on: 'all', rule: { p: 'true' } }],
                    } },
                    { rule: 'add-column', table: 'ledger', column: 'note', def: { type: 'string', nullable: true } },
                    { rule: 'drop-column', table: 'ledger', column: 'amount' },
                ]);
                const actions = schemaDeltaActions(changes, endView, startView, {});
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
        {
            name: '[ADPT11] identity-typed column projects as integer <col>_key_id with keyRef (not an FK)',
            invoke: async () => {
                const mockView = {
                    getTableNames: () => ['caps'],
                    getTable: (n: string) => n === 'caps'
                        ? { name: 'caps', columns: {
                            label: { type: 'string' },
                            grantee: { type: 'identity', nullable: true, readonly: true },
                        } }
                        : undefined,
                    getFKs: () => ({}),
                } as unknown as RSchemaView;

                const actions = initialSchemaActions(mockView, {});
                const create = actions.find((a) => a.kind === 'create-table' && a.table === 'caps');
                assertTrue(create?.kind === 'create-table', 'caps create-table present');
                if (create === undefined || create.kind !== 'create-table') return;

                const grantee = create.columns.find((c) => c.name === 'grantee_key_id');
                assertTrue(grantee?.def.type === 'integer', 'identity column projects as integer grantee_key_id');
                assertEquals(grantee?.keyRef, true, 'identity column is a key-ref into rdb_keys');
                assertEquals(grantee?.fk, undefined, 'identity column is not a row-serial FK');
                assertTrue(grantee?.def.nullable === true && grantee?.def.readonly === true,
                    'nullable and readonly carry onto the key-ref companion');
                assertTrue(!create.columns.some((c) => c.name === 'grantee' || c.name === 'grantee_id'),
                    'raw identity name and FK-style _id suffix are not projected');
            },
        },
        {
            name: '[ADPT12] identity-provider keyIdColumn projects as key_id; publicKeyColumn is dropped',
            invoke: async () => {
                const mockView = {
                    getTableNames: () => ['identities'],
                    getTable: (n: string) => n === 'identities'
                        ? { name: 'identities', columns: {
                            keyId: { type: 'string', pub: true, readonly: true },
                            publicKey: { type: 'string', pub: true, readonly: true },
                            name: { type: 'string', nullable: true },
                        } }
                        : undefined,
                    getFKs: () => ({}),
                    getIdProvider: (n: string) => n === 'identities'
                        ? { keyIdColumn: 'keyId', publicKeyColumn: 'publicKey' }
                        : undefined,
                } as unknown as RSchemaView;

                const actions = initialSchemaActions(mockView, {});
                const create = actions.find((a) => a.kind === 'create-table' && a.table === 'identities');
                assertTrue(create?.kind === 'create-table', 'identities create-table present');
                if (create === undefined || create.kind !== 'create-table') return;

                const keyId = create.columns.find((c) => c.name === 'key_id');
                assertTrue(keyId?.def.type === 'integer' && keyId?.keyRef === true,
                    'provider keyIdColumn projects as integer key_id');
                assertTrue(!create.columns.some((c) => c.name === 'keyId' || c.name === 'keyId_key_id'),
                    'provider keyId is not suffixed like an identity business column');
                assertTrue(!create.columns.some((c) => c.name === 'publicKey' || c.name === 'public_key'),
                    'provider publicKeyColumn is dropped');
                assertTrue(create.columns.some((c) => c.name === 'name'), 'plain columns still project');
            },
        },
        {
            name: '[ADPT13] keyRefSuffix overrides the identity companion suffix',
            invoke: async () => {
                const mockView = {
                    getTableNames: () => ['caps'],
                    getTable: (n: string) => n === 'caps'
                        ? { name: 'caps', columns: { grantee: { type: 'identity' } } }
                        : undefined,
                    getFKs: () => ({}),
                } as unknown as RSchemaView;

                const actions = initialSchemaActions(mockView, { keyRefSuffix: '_kid' });
                const create = actions.find((a) => a.kind === 'create-table' && a.table === 'caps');
                assertTrue(create?.kind === 'create-table', 'caps create-table present');
                if (create === undefined || create.kind !== 'create-table') return;
                assertTrue(create.columns.some((c) => c.name === 'grantee_kid' && c.keyRef === true),
                    'custom keyRefSuffix applied');
                assertTrue(!create.columns.some((c) => c.name === 'grantee_key_id'),
                    'default _key_id suffix is not used when overridden');
            },
        },
        {
            name: '[ADPT14] identity column named author collides with the author_key_id system column',
            invoke: async () => {
                const mockView = {
                    getTableNames: () => ['caps'],
                    getTable: (n: string) => n === 'caps'
                        ? { name: 'caps', columns: { author: { type: 'identity' } } }
                        : undefined,
                    getFKs: () => ({}),
                } as unknown as RSchemaView;

                expectThrow(() => initialSchemaActions(mockView, {}),
                    'identity column author mapping to author_key_id should collide with the author system column');
            },
        },
        {
            name: '[ADPT15] identity foo and FK foo_key both mapping to foo_key_id collide',
            invoke: async () => {
                const mockView = {
                    getTableNames: () => ['caps', 'other'],
                    getTable: (n: string) => n === 'caps'
                        ? { name: 'caps', columns: {
                            foo: { type: 'identity' },
                            foo_key: { type: 'string' },
                        } }
                        : n === 'other' ? { name: 'other', columns: { x: { type: 'string' } } }
                        : undefined,
                    getFKs: (n: string) => n === 'caps' ? { foo_key: 'other' } : {},
                } as unknown as RSchemaView;

                expectThrow(() => initialSchemaActions(mockView, {}),
                    'identity foo and FK foo_key both projecting as foo_key_id should collide');
            },
        },
        {
            name: '[ADPT27] same-shape column reincarnation emits drop+add (no reprojection backfill)',
            invoke: async () => {
                const { schema, admin } = await createGroup();
                // Drop then re-add `memo` (nullable, so a plain add-column can
                // re-add it) with a BYTE-IDENTICAL def: the resolved projection is
                // unchanged, only the incarnation moves. The delta flags it
                // reincarnated; the mapper must still clear + re-add the column so
                // stale cells cannot survive an incremental apply.
                const memoDef = { type: 'string' as const, nullable: true, constraints: { maxLength: 8 } };
                const { changes, endView, startView } = await migrate(schema, admin, [
                    { rule: 'drop-column', table: 'ledger', column: 'memo' },
                    { rule: 'add-column', table: 'ledger', column: 'memo', def: memoDef },
                ]);

                const actions = schemaDeltaActions(changes, endView, startView, {});
                const memoActions = actions.filter((a) =>
                    (a.kind === 'drop-column' || a.kind === 'add-column') && a.table === 'ledger' && a.column === 'memo');
                assertEquals(memoActions.length, 2, 'reincarnated column yields two actions');
                assertEquals(memoActions[0].kind, 'drop-column', 'drop precedes add');
                assertEquals(memoActions[1].kind, 'add-column', 'add follows drop');

                // A same-shape reincarnation is handled by DDL, not a live-view
                // row backfill, so it must not flag the table as reprojected.
                assertEquals(reprojectedTables(changes, endView, startView, {}).size, 0,
                    'a reincarnation does not trigger a row backfill');
            },
        },
        {
            name: '[ADPT28] table reincarnation emits drop-table + create-table (full reset), incl. a required no-default column',
            invoke: async () => {
                const { schema, admin } = await createGroup();
                // Reincarnate `tags` (drop + re-add) with a NEW required, no-default
                // column `kind`. A required-no-default column is only sound because
                // the target table is freshly (re)created and backfilled from rdb
                // truth: it never ALTERs a populated table with a NOT NULL column.
                const { changes, endView, startView } = await migrate(schema, admin, [
                    { rule: 'drop-table', table: 'tags' },
                    { rule: 'add-table', def: {
                        name: 'tags',
                        columns: { code: { type: 'string', pub: true }, kind: { type: 'string' } },
                        restrictions: [{ on: 'all', rule: { p: 'true' } }],
                    } },
                ]);

                const tagsChange = changes.tableChanges.find((c) => c.table === 'tags');
                assertTrue(tagsChange !== undefined && tagsChange.reincarnated,
                    'the delta flags tags as reincarnated');

                const actions = schemaDeltaActions(changes, endView, startView, {});
                const drops = actions.filter((a) => a.kind === 'drop-table' && a.table === 'tags');
                const creates = actions.filter((a) => a.kind === 'create-table' && a.table === 'tags');
                assertEquals(drops.length, 1, 'reincarnated table is dropped');
                assertEquals(creates.length, 1, 'reincarnated table is recreated');
                // global ordering is drops-before-creates
                assertTrue(actions.indexOf(drops[0]) < actions.indexOf(creates[0]),
                    'drop-table precedes create-table');

                // no per-column DDL for a table handled by a full reset
                const perColumn = actions.filter((a) =>
                    (a.kind === 'add-column' || a.kind === 'drop-column')
                    && a.table === 'tags');
                assertEquals(perColumn.length, 0, 'a reset table emits no per-column actions');

                const create = creates[0];
                if (create.kind !== 'create-table') return;
                const kind = create.columns.find((c) => c.name === 'kind');
                assertTrue(kind !== undefined && kind.def.type === 'string'
                    && kind.def.default === undefined && (kind.def.nullable ?? false) === false,
                    'the required no-default column is created as-is on the fresh table');
            },
        },
    ],
};
