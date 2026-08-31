// Unit tests for opVerdictEvents (project.ts): the mapping of a group delta's
// op-verdict flips into durable concurrency op-events. A flip to voided is a
// 'void'; a flip back to live is a 'reinstate'; both are surfaced. The op hash
// is the REAL entry hash (fetchable via loadEntry); the structured reason and
// the (rename-mapped) table travel through. A non-flip is never emitted.

import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import type { OpVerdictChange } from "@hyper-hyper-space/hhs3_rdb";

import { opVerdictEvents } from "../src/project.js";

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
    ],
};
