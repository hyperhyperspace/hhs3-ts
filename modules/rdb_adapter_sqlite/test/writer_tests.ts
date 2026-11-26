// HIP-20's writer on SQLite files: a second handle that follows the host's
// schema changes, and writer calls while another process commits to the file.

import * as os from "node:os";
import * as path from "node:path";
import * as fs from "node:fs";
import { spawn } from "node:child_process";
import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import Database from "better-sqlite3";
import { projectGroup, syncGroup, WriteRefusedError } from "@hyper-hyper-space/hhs3_rdb_adapter";
import { createTypedGroup, rdbRows, recordWriter } from "@hyper-hyper-space/hhs3_rdb_adapter_test";

import { SqliteTarget } from "../src/sqlite_target.js";

async function refusal(write: Promise<unknown>): Promise<string> {
    try {
        await write;
        return 'no refusal';
    } catch (e) {
        return e instanceof WriteRefusedError ? e.reason : `another error: ${String(e)}`;
    }
}

function tempFile(): { file: string; remove: () => void } {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'hip20-writer-'));
    return { file: path.join(dir, 'projection.sqlite'), remove: () => fs.rmSync(dir, { recursive: true, force: true }) };
}

export const sqliteWriterTests = {
    title: '[ADPTS] SQLite: HIP-20 writer on a projection file',
    tests: [
        {
            name: '[ADPTS-SQL29] a second handle, opened as a client opens it, sees through its queries and its writer a column the host adds, one it drops, and a table it creates after the handle opened',
            invoke: async () => {
                const { file, remove } = tempFile();
                const hostDb = new Database(file);
                const host = new SqliteTarget(hostDb, { captureChanges: true, dbPath: file });
                const { schema, group, admin } = await createTypedGroup();
                // The host's cycle: it ingests what the client wrote, then projects.
                const deploy = async (rules: Parameters<typeof schema.updateSchema>[0], what: string): Promise<void> => {
                    await schema.updateSchema(rules, admin, what);
                    await group.deploy(await (await schema.getScopedDag()).getFrontier());
                    await syncGroup(group, host, { writer: admin });
                };
                try {
                    await projectGroup(group, host);
                    await recordWriter(host, 'auto', admin);
                    const clientDb = new Database(file);
                    const client = new SqliteTarget(clientDb, { dbPath: file });
                    try {
                        const writer = client.openWriter();
                        await writer.insert('things', { label: 'before' });

                        await deploy([{ rule: 'add-column', table: 'things', column: 'extra', def: { type: 'string', nullable: true } }], 'add extra');
                        const id = await writer.insert('things', { label: 'added', extra: 'x' });
                        assertEquals((await client.executeQuery({ table: 'things' })).find((r) => r.id === id)?.extra, 'x',
                            'the client writes and reads a column the host added');

                        await deploy([{ rule: 'drop-column', table: 'things', column: 'extra' }], 'drop extra');
                        assertEquals(await refusal(writer.insert('things', { label: 'dropped', extra: 'y' })), 'unknown-column',
                            'and refuses one the host dropped');
                        assertTrue((await client.executeQuery({ table: 'things' })).every((r) => !('extra' in r)), 'which its queries no longer show');

                        await deploy([{ rule: 'add-table', def: {
                            name: 'later', columns: { note: { type: 'string', nullable: true } }, restrictions: [{ on: 'all', rule: { p: 'true' } }],
                        } }], 'add later');
                        const later = await writer.insert('later', { note: 'z' });
                        assertEquals((await client.executeQuery({ table: 'later' })).find((r) => r.id === later)?.note, 'z',
                            'and writes and reads a table the host created after it opened');
                        await syncGroup(group, host, { writer: admin });
                        assertEquals((await rdbRows(group, 'things')).map((r) => r.values.label).sort().join(','), 'added,before',
                            "the host ingested the client's inserts");
                        assertEquals((await rdbRows(group, 'later'))[0]?.values.note, 'z', 'into the new table too');
                    } finally {
                        client.close();
                        clientDb.close();
                    }
                } finally {
                    host.close();
                    hostDb.close();
                    remove();
                }
            },
        },
        {
            name: '[ADPTS-SQL30] writer calls succeed while another process commits to the same file in a loop',
            invoke: async () => {
                const { file, remove } = tempFile();
                const hostDb = new Database(file);
                const host = new SqliteTarget(hostDb, { captureChanges: true, dbPath: file });
                const { group, admin } = await createTypedGroup();
                try {
                    await projectGroup(group, host);
                    await recordWriter(host, 'auto', admin);
                    hostDb.exec('CREATE TABLE race (n INTEGER)');
                    // The other process: an immediate transaction that reads, then
                    // writes, then commits, over and over, until told to stop.
                    const script = `
                        const Database = require('better-sqlite3');
                        const db = new Database(${JSON.stringify(file)});
                        let n = 0;
                        const step = db.transaction(() => {
                            db.prepare('SELECT COUNT(*) FROM race').get();
                            db.prepare('INSERT INTO race (n) VALUES (?)').run(n++);
                        });
                        process.stdout.write('ready\\n');
                        const until = Date.now() + 20000;
                        const pause = new Int32Array(new SharedArrayBuffer(4));
                        while (Date.now() < until && !require('fs').existsSync(${JSON.stringify(file + '.stop')})) { step.immediate(); Atomics.wait(pause, 0, 0, 1); }
                        process.stdout.write('commits ' + n + '\\n');
                    `;
                    const child = spawn(process.execPath, ['-e', script], { cwd: process.cwd(), stdio: ['ignore', 'pipe', 'inherit'] });
                    let out = '';
                    const ready = new Promise<void>((resolve) => child.stdout.on('data', (d: Buffer) => {
                        out += d.toString();
                        if (out.includes('ready')) resolve();
                    }));
                    const done = new Promise<void>((resolve) => child.on('exit', () => resolve()));
                    await ready;
                    const clientDb = new Database(file);
                    const client = new SqliteTarget(clientDb, { dbPath: file });
                    let written = 0;
                    try {
                        const writer = client.openWriter();
                        for (let i = 0; i < 40; i++) {
                            const id = await writer.insert('things', { label: `race ${i}` });
                            assertTrue(await writer.update('things', id, { note: `n ${i}` }), `the update of insert ${i} lands`);
                            written++;
                        }
                    } finally {
                        fs.writeFileSync(file + '.stop', '');
                        await done;
                        client.close();
                        clientDb.close();
                    }
                    const commits = Number(/commits (\d+)/.exec(out)?.[1] ?? '0');
                    assertEquals(written, 40, 'every writer call succeeds');
                    assertTrue(commits > 0, `while the other process committed (${commits} commits)`);
                    assertEquals((await host.drainChanges()).changes.length, 80, 'and capture recorded each of them');
                } finally {
                    host.close();
                    hostDb.close();
                    remove();
                }
            },
        },
    ],
};
