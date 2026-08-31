// Backend-agnostic conformance suite for a MaterializationTarget. Parameterized
// by a TargetFactory (target + reader); the suite builds one rdb group fixture,
// drives it through projectGroup, and asserts the projection via the reader in
// LOGICAL terms only. Engine-specific facts live in the per-backend package.
//
// Distilled from the original SqliteTarget tests: initial materialization,
// incremental insert/update/delete with id stability, re-projection
// idempotence, and apply() atomicity.

import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import { deriveRowId } from "@hyper-hyper-space/hhs3_rdb";
import {
    DEFAULT_KEY_DOMAIN, KeyIndex, projectGroup, SchemaAction, RowAction,
} from "@hyper-hyper-space/hhs3_rdb_adapter";

import { createFkGroup, createFlipGroup, createGroup, frontier, sameVersion } from "./group_fixture.js";
import { TargetFactory } from "./projection_reader.js";

type NamedTest = { name: string; invoke: () => Promise<void> };

async function authorKeyHash(target: object, authorId: number | undefined): Promise<string | undefined> {
    if (authorId === undefined) return undefined;
    const keys = target as KeyIndex;
    return keys.keyHashForId(DEFAULT_KEY_DOMAIN, authorId);
}

export function createProjectionSuite(label: string, factory: TargetFactory): { title: string; tests: NamedTest[] } {
    return {
        title: `[${label}] rdb_adapter projection conformance`,
        tests: [
            {
                name: `[${label}01] initial materialization: tables, row values + author, sync mapping, checkpoint`,
                invoke: async () => {
                    const { group, admin } = await createGroup();
                    const ledger = await group.getTable('ledger');
                    await ledger.insert('l1', { ref: 'R-1', amount: '10.00', memo: 'paid' }, admin);
                    await ledger.insert('l2', { ref: 'R-2', amount: '20.00' }, admin);

                    const to = await frontier(group);
                    const { target, read, cleanup } = await factory();
                    try {
                        await projectGroup(group, target);

                        // Both tables materialized; the empty `tags` table too.
                        assertTrue(await read.hasTable('ledger'), 'ledger table materialized');
                        assertTrue(await read.hasTable('tags'), 'empty tags table still materialized');
                        assertEquals((await read.getRowIds('ledger')).length, 2, 'both ledger rows present');
                        assertEquals((await read.getRowIds('tags')).length, 0, 'tags has no rows');

                        // Row values + author, by content-addressed rowId.
                        const l1 = deriveRowId('l1', admin.keyId);
                        const row1 = await read.getRow('ledger', l1);
                        assertTrue(row1 !== undefined, 'l1 row present');
                        assertEquals(row1!.values.ref, 'R-1', 'ref value');
                        assertEquals(row1!.values.amount, '10.00', 'canonical decimal preserved verbatim');
                        assertEquals(row1!.values.memo, 'paid', 'memo value');
                        assertEquals(await authorKeyHash(target, row1!.author), admin.keyId, 'author materialized as key id');

                        // Omitted nullable column is simply absent.
                        const l2 = deriveRowId('l2', admin.keyId);
                        const row2 = await read.getRow('ledger', l2);
                        assertTrue(row2 !== undefined, 'l2 row present');
                        assertEquals(row2!.values.memo, undefined, 'omitted nullable column absent');

                        // Sync mapping allocated; checkpoint at the projected frontier.
                        assertTrue(await read.syncId('ledger', l1) !== undefined, 'sync id allocated for l1');
                        assertTrue(sameVersion(await target.getCheckpoint(group.getId()), to), 'checkpoint equals projected version');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}02] incremental insert/update/delete: id stability, in-place update, sync survives delete`,
                invoke: async () => {
                    const { group, admin } = await createGroup();
                    const ledger = await group.getTable('ledger');
                    await ledger.insert('l1', { ref: 'R-1', amount: '10.00' }, admin);

                    const { target, read, cleanup } = await factory();
                    try {
                        await projectGroup(group, target);   // initial

                        const l1 = deriveRowId('l1', admin.keyId);
                        const l1Id = await read.syncId('ledger', l1);
                        assertTrue(l1Id !== undefined, 'l1 id allocated');
                        const cp1 = await target.getCheckpoint(group.getId());

                        // Incremental: add a row and update the existing one.
                        await ledger.insert('l2', { ref: 'R-2', amount: '20.00' }, admin);
                        await ledger.update(l1, { memo: 'note' }, admin);
                        await projectGroup(group, target);

                        const l2 = deriveRowId('l2', admin.keyId);
                        const row1 = await read.getRow('ledger', l1);
                        assertEquals(await read.syncId('ledger', l1), l1Id, 'update keeps the same local id (stable)');
                        assertEquals(row1!.values.memo, 'note', 'update applied in place');
                        assertEquals(row1!.values.ref, 'R-1', 'unchanged column preserved');
                        assertEquals(row1!.values.amount, '10.00', 'unchanged column preserved');
                        assertTrue(await read.syncId('ledger', l2) !== l1Id, 'the new row gets its own id');
                        assertEquals((await read.getRowIds('ledger')).length, 2, 'both rows live');
                        assertTrue(!sameVersion(cp1, await frontier(group)), 'sanity: frontier advanced');
                        assertTrue(sameVersion(await target.getCheckpoint(group.getId()), await frontier(group)),
                            'checkpoint advanced to the new frontier');

                        // Delete: app row goes, sync mapping survives.
                        await ledger.delete(l1, admin);
                        await projectGroup(group, target);
                        assertEquals(await read.getRow('ledger', l1), undefined, 'deleted row no longer live');
                        assertTrue(!(await read.getRowIds('ledger')).includes(l1), 'deleted row absent from live ids');
                        assertEquals((await read.getRowIds('ledger')).length, 1, 'only the surviving row remains');
                        assertEquals(await read.syncId('ledger', l1), l1Id, 'sync id survives delete (same id)');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}03] re-projection is idempotent (no changes -> no effect)`,
                invoke: async () => {
                    const { group, admin } = await createGroup();
                    const ledger = await group.getTable('ledger');
                    await ledger.insert('l1', { ref: 'R-1', amount: '10.00', memo: 'paid' }, admin);

                    const { target, read, cleanup } = await factory();
                    try {
                        await projectGroup(group, target);

                        const l1 = deriveRowId('l1', admin.keyId);
                        const idBefore = await read.syncId('ledger', l1);
                        const rowsBefore = (await read.getRowIds('ledger')).length;
                        const cpBefore = await target.getCheckpoint(group.getId());

                        // Re-project with no intervening group changes.
                        await projectGroup(group, target);

                        assertEquals((await read.getRowIds('ledger')).length, rowsBefore, 'row count unchanged');
                        assertEquals(await read.syncId('ledger', l1), idBefore, 'local id unchanged');
                        assertEquals((await read.getRow('ledger', l1))!.values.memo, 'paid', 'row value unchanged');
                        assertTrue(sameVersion(await target.getCheckpoint(group.getId()),
                            cpBefore as Version), 'checkpoint unchanged');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}05] FK projection: local FKs become integer <col>_id (self + cross-table)`,
                invoke: async () => {
                    const { group, admin } = await createFkGroup();
                    const posts = await group.getTable('posts');
                    const comments = await group.getTable('comments');
                    await posts.insert('p1', { title: 'Hello' }, admin);
                    const p1 = deriveRowId('p1', admin.keyId);
                    await comments.insert('c1', { body: 'first', post: p1 }, admin);
                    const c1 = deriveRowId('c1', admin.keyId);
                    await comments.insert('c2', { body: 'reply', post: p1, parent: c1 }, admin);
                    const c2 = deriveRowId('c2', admin.keyId);

                    const { target, read, cleanup } = await factory();
                    try {
                        await projectGroup(group, target);

                        // FK companion SHAPE: local FKs -> integer <col>_id; the
                        // raw rdb FK column name is never projected.
                        assertTrue(await read.hasTable('comments'), 'comments materialized');
                        assertEquals(await read.columnType('comments', 'post_id'), 'integer', 'cross-table FK is an integer id');
                        assertEquals(await read.columnType('comments', 'parent_id'), 'integer', 'self FK is an integer id');
                        assertEquals(await read.columnType('comments', 'post'), undefined, 'raw FK column name not projected');
                        assertEquals(await read.columnType('comments', 'parent'), undefined, 'raw self-FK column name not projected');

                        // FK VALUES: a local FK stores the referenced row's local id.
                        const postLocal = await read.syncId('posts', p1);
                        assertTrue(postLocal !== undefined, 'posts row has a local id');
                        const row1 = await read.getRow('comments', c1);
                        assertEquals(row1!.values.post_id, postLocal, 'cross-table FK stored as the target local id');
                        assertEquals(row1!.values.parent_id, undefined, 'omitted self FK is absent');

                        const row2 = await read.getRow('comments', c2);
                        assertEquals(row2!.values.post_id, postLocal, 'c2 cross-table FK stored as the target local id');
                        assertEquals(row2!.values.parent_id, await read.syncId('comments', c1),
                            'self FK stored as the referenced comment local id');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}04] atomicity: a throwing apply rolls back schema, rows, and the checkpoint`,
                invoke: async () => {
                    const { target, read, cleanup } = await factory();
                    try {
                        const v1: Version = new Set(['v1']);
                        const create: SchemaAction = {
                            kind: 'create-table', table: 'acct', syncTable: 'acct_sync', primaryKey: 'id',
                            columns: [{ name: 'ref', def: { type: 'string' } }],
                        };
                        // A row action targeting an un-materialized table throws,
                        // AFTER the valid create-table ran in the same batch.
                        const badRow: RowAction = {
                            kind: 'upsert-row', table: 'ghost', rowId: 'r1', values: { ref: 'x' },
                        };

                        const gid = 'g-atomicity-test';
                        let threw = false;
                        try {
                            await target.apply(gid, [create], [badRow], v1);
                        } catch {
                            threw = true;
                        }
                        assertTrue(threw, 'apply throws when a row action references an unknown table');
                        assertTrue(!await read.hasTable('acct'), 'create-table rolled back with the failed batch');
                        assertEquals(await target.getCheckpoint(gid), undefined, 'checkpoint not advanced on rollback');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}06] add-column with a default backfills existing rows`,
                invoke: async () => {
                    const { target, read, cleanup } = await factory();
                    try {
                        const gid = 'g-default-backfill';
                        const create: SchemaAction = {
                            kind: 'create-table', table: 'items', syncTable: 'items_sync', primaryKey: 'id',
                            columns: [{ name: 'label', def: { type: 'string' } }],
                        };
                        const insert: RowAction = {
                            kind: 'upsert-row', table: 'items', rowId: 'r1', values: { label: 'x' },
                        };
                        await target.apply(gid, [create], [insert], new Set(['v1']));

                        // The delta channel never enumerates a defaulted column for
                        // an existing row; the target backfills it from the schema.
                        const addCol: SchemaAction = {
                            kind: 'add-column', table: 'items', column: 'status',
                            def: { type: 'string', default: 'active' },
                        };
                        await target.apply(gid, [addCol], [], new Set(['v2']));

                        const row = await read.getRow('items', 'r1');
                        assertTrue(row !== undefined, 'r1 still live after the migration');
                        assertEquals(row!.values.status, 'active', 'existing row backfilled with the column default');
                        assertEquals(row!.values.label, 'x', 'pre-existing value untouched');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}07] a new-row insert omitting a defaulted column materializes the default`,
                invoke: async () => {
                    const { target, read, cleanup } = await factory();
                    try {
                        const gid = 'g-default-insert';
                        const create: SchemaAction = {
                            kind: 'create-table', table: 'items', syncTable: 'items_sync', primaryKey: 'id',
                            columns: [
                                { name: 'label', def: { type: 'string' } },
                                { name: 'status', def: { type: 'string', default: 'active' } },
                                { name: 'note', def: { type: 'string', nullable: true } },
                            ],
                        };
                        await target.apply(gid, [create], [], new Set(['v1']));

                        // Incremental insert carries only the written column; the
                        // delta omits the defaulted `status` and the absent `note`.
                        const insert: RowAction = {
                            kind: 'upsert-row', table: 'items', rowId: 'r1', values: { label: 'x' },
                        };
                        await target.apply(gid, [], [insert], new Set(['v1', 'v2']));

                        const row = await read.getRow('items', 'r1');
                        assertTrue(row !== undefined, 'r1 present');
                        assertEquals(row!.values.status, 'active', 'omitted defaulted column filled with its default');
                        assertEquals(row!.values.label, 'x', 'written column preserved');
                        assertEquals(row!.values.note, undefined, 'omitted nullable column with no default stays absent');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}08] set-fks FK->plain flip backfills the pre-existing row's reinterpreted column`,
                invoke: async () => {
                    const { schema, group, admin } = await createFkGroup();
                    const posts = await group.getTable('posts');
                    const comments = await group.getTable('comments');
                    await posts.insert('p1', { title: 'Hello' }, admin);
                    const p1 = deriveRowId('p1', admin.keyId);
                    await comments.insert('c1', { body: 'first', post: p1 }, admin);
                    const c1 = deriveRowId('c1', admin.keyId);

                    const { target, read, cleanup } = await factory();
                    try {
                        await projectGroup(group, target);   // initial: post_id = syncId(posts, p1)
                        const postLocal = await read.syncId('posts', p1);
                        assertTrue(postLocal !== undefined, 'posts row has a local id');
                        assertEquals((await read.getRow('comments', c1))!.values.post_id, postLocal,
                            'sanity: the FK companion holds the target local id before the flip');

                        // Deploy a set-fks that drops post's FK (keeps parent), so
                        // post reverts to a plain string carrying the verbatim rowId.
                        await schema.updateSchema([
                            { rule: 'set-fks', table: 'comments', fks: { parent: 'comments' } },
                        ], admin, 'drop post fk');
                        const v2 = await (await schema.getScopedDag()).getFrontier();
                        await group.deploy(v2);

                        await projectGroup(group, target);   // incremental: the flip triggers the backfill

                        assertEquals(await read.columnType('comments', 'post'), 'string', 'post reverts to a plain string column');
                        assertEquals(await read.columnType('comments', 'post_id'), undefined, 'the post_id companion is gone');
                        const row = await read.getRow('comments', c1);
                        assertTrue(row !== undefined, 'c1 still live after the flip');
                        assertEquals(row!.values.post, p1,
                            'the reinterpreted plain column is backfilled with the verbatim rowId (gold projection)');
                        assertEquals(row!.values.post_id, undefined, 'the old integer companion no longer holds a value');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}09] set-fks plain->FK flip backfills the pre-existing row's FK companion`,
                invoke: async () => {
                    const { schema, group, admin } = await createFlipGroup();
                    const posts = await group.getTable('posts');
                    const comments = await group.getTable('comments');
                    await posts.insert('p1', { title: 'Hello' }, admin);
                    const p1 = deriveRowId('p1', admin.keyId);
                    // The plain value honors the FUTURE FK (the target rowId) so the
                    // add-fk deploy prerequisite passes (no stranded existing row).
                    await comments.insert('c1', { body: 'first', post: p1 }, admin);
                    const c1 = deriveRowId('c1', admin.keyId);

                    const { target, read, cleanup } = await factory();
                    try {
                        await projectGroup(group, target);   // initial: post plain === p1
                        assertEquals(await read.columnType('comments', 'post'), 'string', 'sanity: post starts plain');
                        assertEquals((await read.getRow('comments', c1))!.values.post, p1, 'sanity: plain value is the rowId');

                        // Deploy a set-fks that makes post an FK -> posts, flipping it
                        // into an integer post_id companion resolved to the serial id.
                        await schema.updateSchema([
                            { rule: 'set-fks', table: 'comments', fks: { post: 'posts' } },
                        ], admin, 'add post fk');
                        const v2 = await (await schema.getScopedDag()).getFrontier();
                        await group.deploy(v2);

                        await projectGroup(group, target);   // incremental: the flip triggers the backfill

                        assertEquals(await read.columnType('comments', 'post_id'), 'integer', 'post projects as an integer companion');
                        assertEquals(await read.columnType('comments', 'post'), undefined, 'the raw plain post column is gone');
                        const postLocal = await read.syncId('posts', p1);
                        assertTrue(postLocal !== undefined, 'posts row has a local id');
                        const row = await read.getRow('comments', c1);
                        assertTrue(row !== undefined, 'c1 still live after the flip');
                        assertEquals(row!.values.post_id, postLocal,
                            'the FK companion is backfilled with the referenced local id (gold projection)');
                        assertEquals(row!.values.post, undefined, 'the old plain column no longer holds a value');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
        ],
    };
}
