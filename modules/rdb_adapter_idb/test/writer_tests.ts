// HIP-20's writer on IndexedDB: a second target over the same database, as a
// tab opens one, follows the host's schema changes through its queries and its
// writer, with a commit listener and without one.

import "fake-indexeddb/auto";

import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { projectGroup, syncGroup, WriteRefusedError } from "@hyper-hyper-space/hhs3_rdb_adapter";
import { createTypedGroup, rdbRows, recordWriter } from "@hyper-hyper-space/hhs3_rdb_adapter_test";

import { IdbTarget } from "../src/idb_target.js";

let counter = 0;
const uniqueDbName = (label: string): string => `writer-${label}-${Date.now()}-${counter++}`;

async function refusal(write: Promise<unknown>): Promise<string> {
    try {
        await write;
        return 'no refusal';
    } catch (e) {
        return e instanceof WriteRefusedError ? e.reason : `another error: ${String(e)}`;
    }
}

const settle = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 50));

function observerCase(listening: boolean): { name: string; invoke: () => Promise<void> } {
    const label = listening ? '[ADPTI-12]' : '[ADPTI-13]';
    return {
        name: `${label} a second target over the database, ${listening ? 'with a commit listener' : 'with no commit listener'}, sees through its queries and its writer a column the host adds, one it drops, and a table it creates after the target opened`,
        invoke: async () => {
            const name = uniqueDbName(listening ? 'listening' : 'quiet');
            const host = await IdbTarget.open(name, { captureChanges: true });
            const { schema, group, admin } = await createTypedGroup();
            const deploy = async (rules: Parameters<typeof schema.updateSchema>[0], what: string): Promise<void> => {
                await schema.updateSchema(rules, admin, what);
                await group.deploy(await (await schema.getScopedDag()).getFrontier());
                await syncGroup(group, host, { writer: admin });
                await settle();
            };
            try {
                await projectGroup(group, host);
                await recordWriter(host, 'auto', admin);
                const tab = await IdbTarget.open(name);
                const commits: string[] = [];
                if (listening) tab.addCommitListener((e) => { commits.push(e.source); });
                try {
                    const writer = tab.openWriter();
                    await writer.insert('things', { label: 'before' });

                    await deploy([{ rule: 'add-column', table: 'things', column: 'extra', def: { type: 'string', nullable: true } }], 'add extra');
                    const id = await writer.insert('things', { label: 'added', extra: 'x' });
                    assertEquals((await tab.executeQuery({ table: 'things' })).find((r) => r.id === id)?.extra, 'x',
                        'the second target writes and reads a column the host added');

                    await deploy([{ rule: 'drop-column', table: 'things', column: 'extra' }], 'drop extra');
                    assertEquals(await refusal(writer.insert('things', { label: 'dropped', extra: 'y' })), 'unknown-column',
                        'and refuses one the host dropped');

                    await deploy([{ rule: 'add-table', def: {
                        name: 'later', columns: { note: { type: 'string', nullable: true } }, restrictions: [{ on: 'all', rule: { p: 'true' } }],
                    } }], 'add later');
                    const later = await writer.insert('later', { note: 'z' });
                    assertEquals((await tab.executeQuery({ table: 'later' })).find((r) => r.id === later)?.note, 'z',
                        'and writes and reads a table the host created after it opened');

                    await syncGroup(group, host, { writer: admin });
                    assertEquals((await rdbRows(group, 'things')).map((r) => r.values.label).sort().join(','), 'added,before',
                        "the host ingested the second target's inserts");
                    assertEquals((await rdbRows(group, 'later'))[0]?.values.note, 'z', 'into the new table too');
                    if (listening) assertTrue(commits.includes('apply'), `the listener heard the host's applies: ${commits.join(',')}`);
                } finally {
                    tab.close();
                }
            } finally {
                host.close();
            }
        },
    };
}

export const idbWriterTests = {
    title: '[ADPTI] IndexedDB: HIP-20 writer on a second target',
    tests: [observerCase(true), observerCase(false)],
};
