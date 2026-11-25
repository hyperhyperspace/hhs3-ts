// Unit tests for opVerdictEvents (project.ts): the mapping of a group delta's
// op-verdict flips into durable concurrency op-events. A flip to voided is a
// 'void'; a flip back to live is a 'reinstate'; both are surfaced. The op hash
// is the REAL entry hash (fetchable via loadEntry); the structured reason and
// the (rename-mapped) table travel through. A non-flip is never emitted. A
// target fills in the local ids of the rows an event names (withLocalIds).

import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import type { OpVerdictChange } from "@hyper-hyper-space/hhs3_rdb";

import { opVerdictEvents } from "../src/project.js";
import { withLocalIds, type OpEvent } from "../src/types.js";

const GROUP = 'group-id' as unknown as Parameters<typeof opVerdictEvents>[1];

export const verdictEventsTests = {
    title: '[ADPTV] rdb_adapter concurrency op-events (verdict flips)',
    tests: [
        {
            name: '[ADPTV01] a void flip becomes a concurrency/void event (real op hash, mapped table, reason)',
            invoke: async () => {
                const changes: OpVerdictChange[] = [{
                    entry: 'ENTRYHASH1',
                    kind: 'update',
                    voidBefore: false,
                    voidAfter: true,
                    table: 'ledger',
                    rowId: 'ROW1',
                    author: 'AUTHOR1',
                    reason: { kind: 'row-not-live', table: 'ledger', action: 'update', rowId: 'ROW1' },
                }];
                const events = opVerdictEvents(changes, GROUP, { tableNames: { ledger: 'ledger_t' } });

                assertEquals(events.length, 1, 'one event');
                const e = events[0];
                assertEquals(e.origin, 'concurrency', 'origin concurrency');
                assertEquals(e.direction, 'void', 'a void flip');
                assertEquals(e.opHash, 'ENTRYHASH1', 'carries the REAL entry hash (loadEntry resolves it)');
                assertEquals(e.kind, 'update', 'op kind preserved');
                assertEquals(e.table, 'ledger_t', 'table name is rename-mapped to the target name');
                assertEquals(e.rowId, 'ROW1', 'row id preserved');
                assertEquals(e.author, 'AUTHOR1', 'author preserved (for author-filtered push)');
                assertTrue(e.reason !== undefined && e.reason.source === 'void', 'structured void reason attached');
                assertTrue(e.op === undefined, 'no op JSON: the op is in rdb, fetched on demand');
            },
        },
        {
            name: '[ADPTV02] a reinstate flip becomes a concurrency/reinstate event',
            invoke: async () => {
                const changes: OpVerdictChange[] = [{
                    entry: 'ENTRYHASH2',
                    kind: 'insert',
                    voidBefore: true,
                    voidAfter: false,
                    table: 'tags',
                    rowId: 'ROW2',
                }];
                const events = opVerdictEvents(changes, GROUP, {});

                assertEquals(events.length, 1, 'one event');
                assertEquals(events[0].direction, 'reinstate', 'a reinstate flip (a previously-voided op is live again)');
                assertEquals(events[0].opHash, 'ENTRYHASH2', 'the entry hash');
            },
        },
        {
            name: '[ADPTV03] a non-flip is not emitted',
            invoke: async () => {
                const changes: OpVerdictChange[] = [
                    { entry: 'E3', kind: 'update', voidBefore: false, voidAfter: false },
                    { entry: 'E4', kind: 'update', voidBefore: true, voidAfter: true },
                ];
                const events = opVerdictEvents(changes, GROUP, {});
                assertEquals(events.length, 0, 'neither steady-state change is an event');
            },
        },
        {
            name: '[ADPTV04] a bundle flip names each row the bundle writes, by its target table',
            invoke: async () => {
                const changes: OpVerdictChange[] = [{
                    entry: 'BUNDLE1', kind: 'bundle', voidBefore: false, voidAfter: true,
                    writes: [
                        { table: 'caps', action: 'delete', rowId: 'ROW1' },
                        { table: 'items', action: 'insert', rowId: 'ROW2' },
                    ],
                }];
                const [event] = opVerdictEvents(changes, GROUP, { tableNames: { caps: 'caps_t' } });
                assertEquals(event?.table, undefined, 'a bundle has no row of its own');
                assertEquals(JSON.stringify(event?.writes),
                    JSON.stringify([{ table: 'caps_t', action: 'delete', rowId: 'ROW1' }, { table: 'items', action: 'insert', rowId: 'ROW2' }]),
                    'each write, in order, with its table renamed as the target names it');
            },
        },
        {
            name: '[ADPTV05] withLocalIds fills in the local id of each row named by hash, and only of those',
            invoke: async () => {
                const ids = new Map([['items\u0000ROW1', 7], ['notes\u0000ROW2', 3]]);
                const localIdOf = (table: string, rowId: string) => ids.get(table + '\u0000' + rowId);
                const flip: OpEvent = {
                    origin: 'concurrency', direction: 'void', groupId: GROUP, opHash: 'B1', kind: 'bundle',
                    writes: [
                        { table: 'items', action: 'insert', rowId: 'ROW1' },
                        { table: 'notes', action: 'insert', rowId: 'ROW1' },
                        { table: 'notes', action: 'update', rowId: 'ROW2' },
                    ],
                };
                const filled = withLocalIds(flip, localIdOf);
                assertEquals(JSON.stringify(filled.writes?.map((w) => w.localId ?? null)), '[7,null,3]',
                    'each write gets the id of its row in its own table; a row with no mapping gets none');
                assertTrue(flip.writes?.every((w) => w.localId === undefined) === true, 'the event passed in is left as it was');

                const row = withLocalIds({ ...flip, kind: 'update', writes: undefined, table: 'notes', rowId: 'ROW2' }, localIdOf);
                assertEquals(row.localId, 3, "a row op's own row gets its id");
                const failure: OpEvent = { origin: 'ingestion', direction: 'failure', groupId: GROUP, opHash: 'F1', kind: 'insert', table: 'notes', rowId: 'ROW2', localId: 9 };
                assertEquals(withLocalIds(failure, localIdOf).localId, 9, 'a local id the event already has is kept');
            },
        },
    ],
};
