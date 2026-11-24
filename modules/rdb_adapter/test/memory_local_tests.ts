// MemoryTarget's local writes and query rows, where they differ from the other
// targets only in how memory keeps a row: a missing row, and a column the row
// has no value in.

import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import { MemoryTarget } from "../src/memory_target.js";

async function notesTarget(): Promise<MemoryTarget> {
    const target = new MemoryTarget({ captureChanges: true });
    await target.apply('g', [{
        kind: 'create-table', table: 'notes', syncTable: 'notes_sync', primaryKey: 'id', authorColumn: 'author_key_id',
        columns: [
            { name: 'title', def: { type: 'string', nullable: true, pub: true, readonly: false } },
            { name: 'memo', def: { type: 'string', nullable: true, pub: true, readonly: false } },
        ],
    }], [], new Set(['c']));
    return target;
}

export const memoryLocalTests = {
    title: 'MemoryTarget local writes and query rows',
    tests: [
        {
            name: '[ADPTM01] an update or delete of a missing row does nothing, records nothing, and returns false',
            invoke: async () => {
                const target = await notesTarget();
                const id = target.localInsert('notes', { title: 'a' });
                await target.drainChanges();
                const before = (await target.drainChanges()).changes.length;

                assertEquals(target.localUpdate('notes', 999, { title: 'x' }), false, 'an update of a missing row returns false');
                assertEquals(target.localDelete('notes', 999), false, 'a delete of a missing row returns false');
                assertEquals((await target.drainChanges()).changes.length, before, 'neither is recorded');

                assertEquals(target.localUpdate('notes', id, { title: 'b' }), true, 'an update of a row the table has returns true');
                assertEquals(target.localUpdate('notes', id, { title: 'b' }), true, 'also when it changes nothing');
                assertEquals(target.localDelete('notes', id), true, 'a delete of a row the table has returns true');
            },
        },
        {
            name: '[ADPTM02] a query row has null for each column the row has no value in, the author column included',
            invoke: async () => {
                const target = await notesTarget();
                target.localInsert('notes', { title: 'a' });
                const [row] = await target.executeQuery({ table: 'notes', orderBy: [{ column: 'id', direction: 'asc' }], limit: 10 });
                assertTrue(row !== undefined, 'the row is returned');
                assertTrue('memo' in row && row.memo === null, 'a column the row never set is null');
                assertTrue('author_key_id' in row && row.author_key_id === null, 'so is an empty author column');
                assertEquals(row.title, 'a', 'a set column has its value');
            },
        },
        {
            name: '[ADPTM03] memory refuses a value for the id column: on an insert always, on an update unless it is the row\'s own id',
            invoke: async () => {
                const target = await notesTarget();
                const id = target.localInsert('notes', { title: 'a' });
                const pending = async () => (await target.drainChanges()).changes;
                const recorded = (await pending()).length;

                let refused = false;
                try { target.localInsert('notes', { id: 99, title: 'x' }); } catch { refused = true; }
                assertTrue(refused, 'an insert that gives the id column a value throws');
                assertEquals((await pending()).length, recorded, 'and records nothing');
                const next = target.localInsert('notes', { title: 'b' });
                assertEquals(next, id + 1, 'the next insert gets the next id');

                const beforeUpdate = (await pending()).length;
                refused = false;
                try { target.localUpdate('notes', id, { id: id + 5, title: 'y' }); } catch { refused = true; }
                assertTrue(refused, 'an update that changes the id throws');
                assertEquals((await pending()).length, beforeUpdate, 'and records nothing');

                assertEquals(target.localUpdate('notes', id, { id, title: 'c' }), true, 'an update that carries the row\'s own id is taken');
                const last = (await pending()).at(-1);
                const lastValues = last !== undefined && last.kind === 'update' ? last.values : undefined;
                assertTrue(lastValues !== undefined, 'the update is recorded');
                assertEquals(JSON.stringify(lastValues), JSON.stringify({ title: 'c' }), 'with only the columns other than the id');

                const rows = await target.executeQuery({ table: 'notes', orderBy: [{ column: 'id', direction: 'asc' }], limit: 10 });
                assertEquals(JSON.stringify(rows.map((r) => [r.id, r.title])), JSON.stringify([[id, 'c'], [next, 'b']]),
                    'query rows show the real ids and the update');
            },
        },
    ],
};
