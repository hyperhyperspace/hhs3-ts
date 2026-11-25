import { assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import { QueryEngine } from "../src/query_monitor.js";
import { MemoryTarget } from "../src/memory_target.js";
import type { QuerySnapshot, QuerySubscription } from "../src/query_builder.js";
import type { OpEvent, RowAction } from "../src/types.js";

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

// Rows as a projection writes them, by row hash, so a flip finds their local
// ids through the sync map. Each call is a new checkpoint.
let checkpoint = 0;
async function project(target: MemoryTarget, rows: RowAction[], events?: OpEvent[]): Promise<void> {
    await target.apply('g', [], rows, new Set([`v${++checkpoint}`]), events);
}

function note(rowId: string, title: string): RowAction {
    return { kind: 'upsert-row', table: 'notes', rowId, values: { title, folder_id: null } };
}

function folder(rowId: string, name: string): RowAction {
    return { kind: 'upsert-row', table: 'folders', rowId, values: { name } };
}

function gone(table: string, rowId: string): RowAction {
    return { kind: 'delete-row', table, rowId };
}

// A flip as projection derives it from rdb: rows named by hash only.
function flip(direction: 'void' | 'reinstate', opHash: string, rows: Partial<OpEvent>): OpEvent {
    return { origin: 'concurrency', direction, groupId: 'g', opHash, kind: 'update', ...rows };
}

// Every snapshot a subscription delivers from now on (after its first result).
function deliveries<T>(sub: QuerySubscription<T>): QuerySnapshot<T>[] {
    const seen: QuerySnapshot<T>[] = [];
    sub.subscribe(() => { seen.push(sub.getSnapshot()); });
    return seen;
}

const ids = (rows: { id: number }[]): string => rows.map((row) => row.id).join(',');
const eventIds = (events: OpEvent[]): string => events.map((e) => `${e.direction} ${e.opHash}`).join('; ');
const pause = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 20));

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
        {
            name: '[ADPTQ12] an op-event reaches the queries showing its row, by table and local id, even when no row changes',
            invoke: async () => {
                const target = new MemoryTarget();
                await table(target);
                await project(target, [note('N1', 'a'), folder('F1', 'box')]);
                const engine = new QueryEngine(target);
                const notes = engine.monitor(engine.selectFrom('notes'));
                const folders = engine.monitor(engine.selectFrom('folders'));
                await Promise.all([notes.whenReady(), folders.whenReady()]);
                const toNotes = deliveries(notes);
                const toFolders = deliveries(folders);
                assertEquals(`${ids(notes.getSnapshot().data)} ${ids(folders.getSnapshot().data)}`, '1 1', 'each table shows its row 1');
                const shown = folders.getSnapshot().data;

                // A void of an update a later one had overwritten: the row is unchanged.
                await project(target, [], [flip('void', 'OP1', { table: 'folders', rowId: 'F1' })]);
                await pause();
                const [logged] = await target.drainOpEvents();
                assertEquals(logged?.event.localId, 1, 'the target filled in the local id of the row the flip names');
                assertEquals(toFolders.length, 1, 'the folders query delivers at once');
                assertEquals(eventIds(toFolders[0]!.events), 'void OP1', 'with the event');
                assertEquals(toFolders[0]!.data[0], shown[0], 'and the row unchanged');
                assertEquals(toNotes.length, 0, 'the notes query, which shows row 1 of another table, gets nothing');
                engine.close();
            },
        },
        {
            name: "[ADPTQ13] a bundle's event reaches each query showing a row it writes, and a reinstate arrives with the rows that come back",
            invoke: async () => {
                const target = new MemoryTarget();
                await table(target);
                await project(target, [note('N1', 'a'), note('N2', 'b'), folder('F1', 'box')]);
                const engine = new QueryEngine(target);
                const notes = engine.monitor(engine.selectFrom('notes'));
                const folders = engine.monitor(engine.selectFrom('folders'));
                await Promise.all([notes.whenReady(), folders.whenReady()]);
                const toNotes = deliveries(notes);
                const toFolders = deliveries(folders);

                const writes: OpEvent['writes'] = [
                    { table: 'notes', action: 'insert', rowId: 'N2' },
                    { table: 'folders', action: 'insert', rowId: 'F1' },
                ];
                await project(target, [gone('notes', 'N2'), gone('folders', 'F1')], [flip('void', 'B1', { kind: 'bundle', writes })]);
                await pause();
                assertEquals(`${ids(toNotes[0]!.data)} | ${eventIds(toNotes[0]!.events)}`, '1 | void B1', 'notes loses row 2 and gets the void');
                assertEquals(`${ids(toFolders[0]!.data)} | ${eventIds(toFolders[0]!.events)}`, ' | void B1', 'folders loses row 1 and gets it too');
                assertEquals(JSON.stringify(toNotes[0]!.events[0]!.writes?.map((w) => `${w.table} ${w.localId}`)), '["notes 2","folders 1"]',
                    'each row the bundle writes, with its local id in its own table');

                await project(target, [note('N2', 'b'), folder('F1', 'box')], [flip('reinstate', 'B1', { kind: 'bundle', writes })]);
                await pause();
                assertEquals(`${ids(toNotes[1]!.data)} | ${eventIds(toNotes[1]!.events)}`, '1,2 | reinstate B1', 'row 2 comes back with the reinstate');
                assertEquals(`${ids(toFolders[1]!.data)} | ${eventIds(toFolders[1]!.events)}`, '1 | reinstate B1', 'and so does folders row 1');

                // A bundle that writes folders row 1 only is not about notes row 1.
                await project(target, [gone('folders', 'F1')], [flip('void', 'B2', { kind: 'bundle', writes: [writes[1]!] })]);
                await pause();
                assertEquals(eventIds(toFolders[2]!.events), 'void B2', 'folders gets it');
                assertEquals(toNotes.length, 2, 'notes, which shows its own row 1, does not');
                engine.close();
            },
        },
        {
            name: "[ADPTQ14] a query set delivers its members' events once per pass, each once, in log order",
            invoke: async () => {
                const target = new MemoryTarget();
                await table(target);
                await project(target, [note('N1', 'a'), note('N2', 'b'), folder('F1', 'box')]);
                const engine = new QueryEngine(target);
                const set = engine.monitor({
                    all: engine.selectFrom('notes'),
                    first: engine.selectFrom('notes').where('title = :t', { t: 'a' }),
                    folders: engine.selectFrom('folders'),
                });
                const ready = await set.whenReady();
                const toSet = deliveries(set);
                assertEquals(`${ids(ready.data.all!)} ${ids(ready.data.first!)} ${ids(ready.data.folders!)}`, '1,2 1 1', 'each member shows its rows');

                // Note 1 is in two members; the folder's event is logged first.
                await project(target, [], [
                    flip('void', 'OP1', { table: 'folders', rowId: 'F1' }),
                    flip('void', 'OP2', { table: 'notes', rowId: 'N1' }),
                ]);
                await pause();
                assertEquals(toSet.length, 1, 'one delivery for the pass');
                assertEquals(eventIds(toSet[0]!.events), 'void OP1; void OP2', 'both events, once each, in log order');
                assertEquals(toSet[0]!.data.all, ready.data.all, "a member's rows keep their array when they did not change");

                // A later pass with no event for the set delivers none again.
                await project(target, [note('N2', 'b2')]);
                await pause();
                assertEquals(`${toSet.length} ${eventIds(toSet[1]!.events)}`, '2 ', 'the next delivery carries no event');
                engine.close();
            },
        },
        {
            name: '[ADPTQ15] an event for a row off the page is not delivered when the row enters the page later',
            invoke: async () => {
                const target = new MemoryTarget();
                await table(target);
                await project(target, [note('N1', 'a'), note('N2', 'b')]);
                const engine = new QueryEngine(target);
                const page = engine.monitor(engine.selectFrom('notes').orderBy('id').limit(1));
                await page.whenReady();
                const toPage = deliveries(page);
                assertEquals(ids(page.getSnapshot().data), '1', 'the page shows row 1');

                await project(target, [note('N2', 'b2')], [flip('void', 'OP1', { table: 'notes', rowId: 'N2' })]);
                await pause();
                assertEquals(toPage.length, 0, 'row 2 is off the page: nothing is delivered');
                await project(target, [gone('notes', 'N1')]);
                await pause();
                assertEquals(`${ids(toPage[0]!.data)} | ${eventIds(toPage[0]!.events)}`, '2 | ', 'row 2 enters the page without the old event');
                engine.close();
            },
        },
        {
            name: '[ADPTQ16] a one-shot read leaves no monitor behind, nor the upstream ones it read through',
            invoke: async () => {
                const target = new MemoryTarget({ captureChanges: true });
                await table(target);
                const box = target.localInsert('folders', { name: 'box' });
                target.localInsert('notes', { title: 'a', folder_id: box });
                let queries = 0;
                const original = target.executeQuery.bind(target);
                target.executeQuery = async (plan) => { queries++; return original(plan); };
                const engine = new QueryEngine(target);
                const notes = engine.selectFrom('notes');
                const folders = engine.selectFrom('folders').where('id IN :ids', { ids: notes.col('folder_id') });

                for (let i = 0; i < 5; i++) {
                    assertEquals((await notes.where('title = :t', { t: `t${i}` }).results()).length, 0, 'no note has that title');
                }
                assertEquals((await folders.results()).map((row) => row.name).join(','), 'box', "the folder of a note, read through notes' folder ids");
                const before = queries;
                target.localInsert('notes', { title: 'b', folder_id: box });
                target.localInsert('folders', { name: 'bag' });
                await pause();
                assertEquals(queries, before, 'later writes to both tables run no query: nothing the reads opened is left');
                engine.close();
            },
        },
        {
            name: '[ADPTQ17] a one-shot read keeps a monitor a subscription holds, directly or as the upstream of the one it holds',
            invoke: async () => {
                const target = new MemoryTarget({ captureChanges: true });
                await table(target);
                const box = target.localInsert('folders', { name: 'box' });
                target.localInsert('notes', { title: 'a', folder_id: box });
                const engine = new QueryEngine(target);
                const notes = engine.selectFrom('notes');
                const folders = engine.selectFrom('folders').where('id IN :ids', { ids: notes.col('folder_id') });
                const sub = engine.monitor(folders);
                assertEquals((await sub.whenReady()).data.map((row) => row.name).join(','), 'box',
                    'its first result already reads the folder ids of the notes');
                const names: string[] = [];
                sub.subscribe(() => { names.push(sub.getSnapshot().data.map((row) => row.name).join(',')); });

                await notes.results();
                await folders.results();
                const bag = target.localInsert('folders', { name: 'bag' });
                await pause();
                target.localInsert('notes', { title: 'c', folder_id: bag });
                await pause();
                assertEquals(JSON.stringify(names), JSON.stringify(['box,bag']),
                    'the subscription delivers once, when a note points at the new folder, with no step through empty keys');
                engine.close();
            },
        },
        {
            name: "[ADPTQ18] a subscription dropped and taken again at once, as React's Strict Mode does, never runs its query on empty keys",
            invoke: async () => {
                const target = new MemoryTarget({ captureChanges: true });
                await table(target);
                const box = target.localInsert('folders', { name: 'box' });
                target.localInsert('notes', { title: 'a', folder_id: box });
                const engine = new QueryEngine(target);
                const notes = engine.selectFrom('notes');
                const sub = engine.monitor(engine.selectFrom('folders').where('id IN :ids', { ids: notes.col('folder_id') }));
                await sub.whenReady();
                const names: string[] = [];
                const listener = () => { names.push(sub.getSnapshot().data.map((row) => row.name).join(',')); };

                sub.subscribe(listener)();   // mounted and unmounted: both monitors leave the engine
                sub.subscribe(listener);     // mounted again: the notes monitor is a new one
                target.localInsert('folders', { name: 'bag' });   // a write before that one has run
                await pause();
                assertEquals(JSON.stringify(names), '[]', "nothing changed on screen, so nothing is delivered, not even a step through no folder");
                assertEquals(sub.getSnapshot().data.map((row) => row.name).join(','), 'box', 'the query still shows the folder of the note');
                engine.close();
            },
        },
    ],
};
