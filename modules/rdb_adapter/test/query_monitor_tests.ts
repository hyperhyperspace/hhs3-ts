import { assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import { QueryEngine } from "../src/query_monitor.js";
import { MemoryTarget } from "../src/memory_target.js";

async function table(target: MemoryTarget): Promise<void> {
    await target.apply('g', [{
        kind: 'create-table', table: 'notes', syncTable: 'notes_sync', primaryKey: 'id',
        columns: [
            { name: 'title', def: { type: 'string', nullable: true, pub: true, readonly: false } },
            { name: 'folder_id', def: { type: 'integer', nullable: true, pub: true, readonly: false } },
        ],
    }, {
        kind: 'create-table', table: 'folders', syncTable: 'folders_sync', primaryKey: 'id',
        columns: [{ name: 'name', def: { type: 'string', nullable: false, pub: true, readonly: false } }],
    }], [], new Set(['c']));
}

export const queryMonitorTests = {
    title: 'Query monitors',
    tests: [
        {
            name: '[ADPTQ10] a local write on another table does not re-query',
            invoke: async () => {
                const target = new MemoryTarget({ captureChanges: true });
                await table(target);
                let queries = 0;
                const original = target.executeQuery.bind(target);
                target.executeQuery = async (plan) => { queries++; return original(plan); };
                const engine = new QueryEngine(target);
                const notes = engine.selectFrom('notes').where('folder_id = :id', { id: 1 });
                const sub = engine.monitor(notes);
                const ready = sub.whenReady();
                sub.subscribe(() => {});
                await ready;
                const before = queries;
                target.localInsert('folders', { name: 'box' });
                await new Promise((resolve) => setTimeout(resolve, 20));
                assertEquals(queries, before, 'other table');
                target.localInsert('notes', { title: 'n', folder_id: 2 });
                await new Promise((resolve) => setTimeout(resolve, 20));
                assertEquals(queries > before, true, 'same table re-queries');
                const rows = sub.getSnapshot().data;
                assertEquals(rows.length, 0, 'folder 2 is outside the predicate');
            },
        },
        {
            name: '[ADPTQ11] select strips order columns but the page still seeks',
            invoke: async () => {
                const target = new MemoryTarget({ captureChanges: true });
                await table(target);
                const first = target.localInsert('notes', { title: 'a', folder_id: 1 });
                target.localInsert('notes', { title: 'b', folder_id: 1 });
                const engine = new QueryEngine(target);
                const page = engine.selectFrom('notes').select('title').where('folder_id = :id', { id: 1 })
                    .orderBy('id', 'desc').before(first + 1).limit(10);
                const sub = engine.monitor(page);
                sub.subscribe(() => {});
                const snapshot = await sub.whenReady();
                assertEquals(snapshot.data.map((row) => row.title).join(','), 'a', 'older page');
                assertEquals('folder_id' in snapshot.data[0]!, false, 'filter column stripped');
            },
        },
    ],
};
