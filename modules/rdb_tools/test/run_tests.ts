import { mkdtemp, rm } from "node:fs/promises";
import { stdin as input } from "node:process";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { version } from "@hyper-hyper-space/hhs3_mvt";
import type { ResolvedTableRef } from "@hyper-hyper-space/hhs3_rdb_lang";
import { testing } from "@hyper-hyper-space/hhs3_util";
import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import { KeyStore } from "../src/keys/keystore.js";
import { runMetaCommand } from "../src/repl/meta.js";
import { promptForSession } from "../src/repl/prompt.js";
import { runCommand } from "../src/script/run_command.js";
import { runScript } from "../src/script/run_script.js";
import { resolveRowIdPrefix } from "../src/session/adapter.js";
import { WorkspaceSession } from "../src/session/session.js";
import { Workspace } from "../src/workspace/workspace.js";
import { formatRows, formatRowsVertical } from "../src/format/rows.js";

const tests = [
    {
        name: '[RDB_TOOLS01] script creates roots, writes rows, selects and logs',
        invoke: async () => {
            await withSession(async (session) => {
                const result = await runScript(session, setupScript());
                assertEquals(result.exitCode, 0, result.output);
                assertTrue(result.output.includes('Widget'), 'SELECT output should include inserted row');
                assertTrue(result.output.includes('create'), 'LOG output should include create entries');
                assertEquals(session.workspace.roots.list('schema').length, 1, 'schema indexed');
                assertEquals(session.workspace.roots.list('group').length, 1, 'group indexed');
            });
        },
    },
    {
        name: '[RDB_TOOLS02] workspace reopens and rehydrates roots from self-describing payloads',
        invoke: async () => {
            const dir = await mkdtemp(join(tmpdir(), 'rdb-tools-'));
            const dbPath = join(dir, 'dev.db');
            try {
                const workspace1 = await Workspace.open({ path: dbPath });
                const keystore1 = await KeyStore.open(`${dbPath}.keys.json`, workspace1.replica.getHashSuite());
                const session1 = new WorkspaceSession({ workspace: workspace1, keystore: keystore1 });
                const created = await runScript(session1, setupScript());
                assertEquals(created.exitCode, 0, created.output);
                await workspace1.close();

                const workspace2 = await Workspace.open({ path: dbPath });
                const session2 = new WorkspaceSession({ workspace: workspace2 });
                try {
                    assertEquals(workspace2.roots.list('schema').length, 1, 'schema rehydrated');
                    assertEquals(workspace2.roots.list('group').length, 1, 'group rehydrated');
                    const selected = await runCommand(session2, "SELECT sku, name FROM shop_prod.products;");
                    assertEquals(selected.exitCode, 0, selected.output);
                    assertTrue(selected.output.includes('Widget'), 'reopened SELECT should see row');
                } finally {
                    await workspace2.close();
                }
            } finally {
                await rm(dir, { recursive: true, force: true });
            }
        },
    },
    {
        name: '[RDB_TOOLS03] keystore encrypts, unlocks and rejects wrong passphrases',
        invoke: async () => {
            await withSession(async (session, dbPath) => {
                if (session.keystore === undefined) throw new Error('missing keystore');
                const identity = await session.createKey('alice', 'correct');
                assertEquals(await session.currentAuthor(), undefined, 'create unlocks but does not select a default author');
                session.selectAuthor('alice');
                assertEquals((await session.currentAuthor())?.keyId, identity.keyId, 'select sets the default author');

                const reopened = await KeyStore.open(`${dbPath}.keys.json`, session.workspace.replica.getHashSuite());
                let failed = false;
                try {
                    await reopened.unlock('alice', 'wrong');
                } catch (_e) {
                    failed = true;
                }
                assertTrue(failed, 'wrong passphrase should fail');
                const unlocked = await reopened.unlock('alice', 'correct');
                assertEquals(unlocked.keyId, identity.keyId, 'unlocked same key');
            });
        },
    },
    {
        name: '[RDB_TOOLS04] meta commands manage output, aliases and current group',
        invoke: async () => {
            await withSession(async (session) => {
                const result = await runScript(session, setupScript());
                assertEquals(result.exitCode, 0, result.output);
                const group = session.workspace.roots.list('group')[0];
                const alias = await runMetaCommand(session, `\\alias prod #${group.id.slice(0, 10)}`);
                assertTrue(alias.output?.includes('prod') === true, 'alias output');
                const use = await runMetaCommand(session, '\\use group prod');
                assertTrue(use.output?.includes('using group') === true, 'use output');
                const useWithSemicolon = await runMetaCommand(session, '\\use group prod;');
                assertTrue(useWithSemicolon.output?.includes('using group') === true, 'semicolon use output');
                const output = await runMetaCommand(session, '\\output json;');
                assertEquals(output.output, 'output json', 'output mode set');
                assertEquals(session.outputMode, 'json', 'session output mode');
                const keys = await runMetaCommand(session, '\\keys;');
                assertTrue(keys.output?.includes('alice') === true, 'keys listed');
                const dump = await runMetaCommand(session, '\\dump schema shop;');
                assertTrue(dump.output?.includes('CREATE SCHEMA shop') === true, 'semicolon dump output');
            });
        },
    },
    {
        name: '[RDB_TOOLS05] vertical output formats SELECT rows as stacked fields',
        invoke: async () => {
            await withSession(async (session) => {
                const setup = await runScript(session, setupScript());
                assertEquals(setup.exitCode, 0, setup.output);
                await runMetaCommand(session, '\\output vertical');
                const selected = await runCommand(session, "SELECT sku, name FROM shop_prod.products;");
                assertEquals(selected.exitCode, 0, selected.output);
                assertTrue(selected.output.includes('*** row 1 ***'), 'vertical row header');
                assertTrue(selected.output.includes('sku: A'), 'vertical sku field');
                assertTrue(selected.output.includes('name: Widget'), 'vertical name field');
                assertTrue(selected.output.includes('rowId:'), 'vertical rowId field');
                assertTrue(!selected.output.includes('uuid'), 'vertical output should omit uuid');
                assertTrue(!selected.output.includes(' | '), 'vertical output should not use table separators');
            });
        },
    },
    {
        name: '[RDB_TOOLS05b] formatRows shows fixed columns with blank absent cells',
        invoke: async () => {
            const table = formatRows([{ rowId: 'abc', name: 'Admin' }], ['rowId', 'nick', 'name']);
            assertTrue(table.includes('nick'), 'header includes absent column');
            const lines = table.split('\n');
            assertEquals(lines.length, 3, 'header separator and body');
            const cells = lines[2].split(' | ');
            assertEquals(cells.length, 3, 'three columns in body');
            assertEquals(cells[1].trim(), '', 'absent nick cell is blank');

            const vertical = formatRowsVertical([{ rowId: 'abc' }], ['rowId', 'nick']);
            assertTrue(vertical.includes('nick: '), 'vertical lists absent column with blank value');
        },
    },
    {
        name: '[RDB_TOOLS05c] SELECT * displays schema columns absent on every row',
        invoke: async () => {
            await withSession(async (session) => {
                const setup = await runScript(session, setupScript());
                assertEquals(setup.exitCode, 0, setup.output);

                const alter = await runCommand(session, 'ALTER SCHEMA shop AS (ADD COLUMN products.tag string NULL);');
                assertEquals(alter.exitCode, 0, alter.output);
                const deploy = await runCommand(session, 'UPDATE SCHEMA shop TO LATEST ON shop_prod;');
                assertEquals(deploy.exitCode, 0, deploy.output);

                const selected = await runCommand(session, 'SELECT * FROM shop_prod.products;');
                assertEquals(selected.exitCode, 0, selected.output);
                const lines = selected.output.split('\n');
                assertEquals(lines.length, 3, 'header separator and body');
                assertTrue(lines[0].includes('tag'), 'SELECT * header includes absent nullable column');
                const headerCols = lines[0].split(' | ');
                const dataCols = lines[2].split(' | ');
                assertEquals(dataCols.length, headerCols.length, 'data row has a cell per header column');
                const tagIndex = headerCols.indexOf('tag');
                assertTrue(tagIndex >= 0, 'tag column found in header');
                assertEquals(dataCols[tagIndex]?.trim(), '', 'tag cell is blank');

                const explicit = await runCommand(session, 'SELECT sku, name FROM shop_prod.products;');
                assertEquals(explicit.exitCode, 0, explicit.output);
                assertTrue(!explicit.output.includes('tag |'), 'explicit select does not add extra columns');
            });
        },
    },
    {
        name: '[RDB_TOOLS06] rowId prefixes resolve against the target table view',
        invoke: async () => {
            await withSession(async (session) => {
                const result = await runScript(session, setupScript());
                assertEquals(result.exitCode, 0, result.output);

                const group = session.workspace.roots.list('group')[0];
                const table = await (group.object as any).getTable('products');
                const rowIds = await (await table.getView()).liveRowIds();
                assertEquals(rowIds.length, 1, 'one live row before delete');
                const rowId = rowIds[0];

                const update = await runCommand(session, `UPDATE shop_prod.products SET name = 'Pineapple' WHERE rowId = '${rowId}';`);
                assertEquals(update.exitCode, 0, update.output);
                const updated = await runCommand(session, "SELECT sku, name FROM shop_prod.products;");
                assertTrue(updated.output.includes('Pineapple'), 'quoted full rowId update works');

                const del = await runCommand(session, `DELETE FROM shop_prod.products WHERE rowId = #${rowId.slice(0, 8)};`);
                assertEquals(del.exitCode, 0, del.output);
                const selected = await runCommand(session, "SELECT sku, name FROM shop_prod.products;");
                assertTrue(!selected.output.includes('Pineapple'), 'rowId prefix delete removes row');

                const unknown = await runCommand(session, "DELETE FROM shop_prod.products WHERE rowId = #zzzzzzzz;");
                assertEquals(unknown.exitCode, 2, 'unknown rowId prefix is a bind error');
                assertTrue(unknown.output.includes('Unknown rowId prefix'), 'unknown prefix diagnostic');

                const fakeTable = {
                    groupId: 'group',
                    group: {},
                    tableName: 'products',
                    table: {
                        getView: async () => ({
                            liveRowIds: async () => ['abc123=', 'abc456='],
                        }),
                    },
                } as unknown as ResolvedTableRef;
                let ambiguous = false;
                try {
                    await resolveRowIdPrefix('abc', fakeTable, version());
                } catch (e) {
                    ambiguous = true;
                    assertTrue(e instanceof Error && e.message.includes('Ambiguous rowId prefix'), 'ambiguous prefix diagnostic');
                }
                assertTrue(ambiguous, 'ambiguous rowId prefix should fail');
            });
        },
    },
    {
        name: '[RDB_TOOLS07] rejected writes surface validation diagnostics',
        invoke: async () => {
            await withSession(async (session) => {
                const result = await runScript(session, setupScript());
                assertEquals(result.exitCode, 0, result.output);

                const group = session.workspace.roots.list('group')[0];
                const table = await (group.object as any).getTable('products');
                const rowIds = await (await table.getView()).liveRowIds();
                assertEquals(rowIds.length, 1, 'one live row before rejected delete');
                const rowId = rowIds[0];

                const anonymousSession = new WorkspaceSession({ workspace: session.workspace });
                const del = await runCommand(anonymousSession, `DELETE FROM shop_prod.products WHERE rowId = '${rowId}';`);
                assertEquals(del.exitCode, 2, 'invalid delete should be a language validation error');
                assertTrue(del.output.includes('VALIDATION_REJECTED'), 'validation diagnostic code');
                assertTrue(del.output.includes('does not satisfy ALLOW delete IF products.rowAuthor = $author'), 'validation reason');

                const selected = await runCommand(session, "SELECT sku, name FROM shop_prod.products;");
                assertTrue(selected.output.includes('Widget'), 'rejected delete should not remove the row');
            });
        },
    },
    {
        name: '[RDB_TOOLS08] current group resolves unqualified table statements',
        invoke: async () => {
            await withSession(async (session) => {
                const result = await runScript(session, setupScript());
                assertEquals(result.exitCode, 0, result.output);

                const selected = await runCommand(session, "SELECT sku, name FROM products;");
                assertEquals(selected.exitCode, 0, selected.output);
                assertTrue(selected.output.includes('Widget'), 'script-created current group selects bare table');

                const newSession = new WorkspaceSession({ workspace: session.workspace, keystore: session.keystore });
                const missingGroup = await runCommand(newSession, "SELECT sku, name FROM products;");
                assertEquals(missingGroup.exitCode, 2, 'bare table requires current group');
                assertTrue(missingGroup.output.includes('requires a group qualifier'), 'missing current group diagnostic');

                const use = await runMetaCommand(newSession, '\\use group shop_prod');
                assertTrue(use.output?.includes('using group') === true, 'use group succeeds');
                const inserted = await runCommand(newSession, "INSERT INTO products (sku, name) VALUES ('B', 'Gadget');");
                assertEquals(inserted.exitCode, 0, inserted.output);
                const afterInsert = await runCommand(newSession, "SELECT sku, name FROM products;");
                assertTrue(afterInsert.output.includes('Gadget'), 'explicit use group inserts and selects bare table');
            });
        },
    },
    {
        name: '[RDB_TOOLS09] prompt shows canonical group and key names',
        invoke: async () => {
            await withSession(async (session) => {
                const result = await runScript(session, setupScript());
                assertEquals(result.exitCode, 0, result.output);
                assertEquals(promptForSession(session), 'rdb:shop_prod:alice> ', 'prompt uses friendly names');

                const group = session.workspace.roots.list('group')[0];
                await runMetaCommand(session, `\\alias prod #${group.id.slice(0, 10)}`);
                await runMetaCommand(session, '\\use group prod');
                assertEquals(promptForSession(session), 'rdb:shop_prod:alice> ', 'prompt uses canonical group name via alias');
            });
        },
    },
    {
        name: '[RDB_TOOLS10] log output uses table and vertical row formatting',
        invoke: async () => {
            await withSession(async (session) => {
                const result = await runScript(session, setupScript());
                assertEquals(result.exitCode, 0, result.output);

                const log = await runCommand(session, "LOG shop_prod LIMIT 2;");
                assertEquals(log.exitCode, 0, log.output);
                assertTrue(log.output.includes('hash'), 'log table includes hash column');
                assertTrue(log.output.includes('prev'), 'log table includes prev column');
                assertTrue(log.output.includes('action'), 'log table includes action column');
                assertTrue(log.output.includes('summary'), 'log table includes summary column');
                assertTrue(log.output.includes(' | '), 'log table uses row formatter separators');
                assertTrue(!log.output.includes('prev='), 'log table should not use legacy inline format');

                await runMetaCommand(session, '\\output vertical');
                const vertical = await runCommand(session, "LOG shop_prod LIMIT 1;");
                assertEquals(vertical.exitCode, 0, vertical.output);
                assertTrue(vertical.output.includes('*** row 1 ***'), 'vertical log row header');
                assertTrue(vertical.output.includes('hash: #'), 'vertical log hash field');
                assertTrue(vertical.output.includes('prev:'), 'vertical log prev field');
                assertTrue(vertical.output.includes('summary:'), 'vertical log summary field');
                assertTrue(!vertical.output.includes(' | '), 'vertical log should not use table separators');
            });
        },
    },
    {
        name: '[RDB_TOOLS11] key create and unlock prompt in REPL and accept inline passphrases for scripts',
        invoke: async () => {
            await withSession(async (session, dbPath) => {
                if (session.keystore === undefined) throw new Error('missing keystore');
                const identity = await session.createKey('alice', 'correct');

                const createPending = await runMetaCommand(session, '\\key create bob');
                assertEquals(createPending.needsPassphrase?.kind, 'create', 'create without passphrase requests prompt');
                assertEquals(createPending.needsPassphrase?.label, 'bob', 'create prompt keeps label');
                assertEquals(createPending.output, undefined, 'create without passphrase has no immediate output');

                const createMissing = await runCommandNonInteractive(session, '\\key create bob');
                assertEquals(createMissing.exitCode, 1, 'non-interactive create requires passphrase');
                assertTrue(createMissing.output.includes('passphrase required'), 'non-interactive create error');

                const created = await runCommand(session, '\\key create bob secret');
                assertEquals(created.exitCode, 0, created.output);
                assertTrue(created.output.includes('created bob'), 'inline passphrase create works');

                const pending = await runMetaCommand(session, '\\key unlock alice');
                assertEquals(pending.needsPassphrase?.kind, 'unlock', 'unlock without passphrase requests unlock prompt');
                assertEquals(pending.needsPassphrase?.label, 'alice', 'unlock without passphrase requests prompt');
                assertEquals(pending.output, undefined, 'unlock without passphrase has no immediate output');

                const missing = await runCommandNonInteractive(session, '\\key unlock alice');
                assertEquals(missing.exitCode, 1, 'non-interactive unlock requires passphrase');
                assertTrue(missing.output.includes('passphrase required'), 'non-interactive unlock error');

                const unlocked = await runCommand(session, '\\key unlock alice correct');
                assertEquals(unlocked.exitCode, 0, unlocked.output);
                assertTrue(unlocked.output.includes('unlocked'), 'inline passphrase unlock works');
                assertEquals(await session.currentAuthor(), undefined, 'unlock does not select a default author');

                const author = await runCommand(session, '\\author alice');
                assertEquals(author.exitCode, 0, author.output);
                assertTrue(author.output.includes('author alice'), '\\author selects the default author');
                assertEquals((await session.currentAuthor())?.keyId, identity.keyId, '\\author sets default to alice');

                const cleared = await runCommand(session, '\\author nobody');
                assertEquals(cleared.exitCode, 0, cleared.output);
                assertTrue(cleared.output.includes('author nobody'), '\\author nobody clears the default');
                assertEquals(await session.currentAuthor(), undefined, '\\author nobody makes writes anonymous');
            });
        },
    },
    {
        name: '[RDB_TOOLS12] \\author unlocks a locked key and \\keys reports unlocked state',
        invoke: async () => {
            await withSession(async (session, dbPath) => {
                if (session.keystore === undefined) throw new Error('missing keystore');
                const alice = await session.createKey('alice', 'correct');
                await session.createKey('bob', 'secret');

                // Reopen the keystore in a fresh session so both keys are locked again.
                const keystore = await KeyStore.open(`${dbPath}.keys.json`, session.workspace.replica.getHashSuite());
                const reopened = new WorkspaceSession({ workspace: session.workspace, keystore });

                const lockedKeys = await runMetaCommand(reopened, '\\keys');
                assertTrue(lockedKeys.output?.includes('unlocked') === true, '\\keys has an unlocked column');
                assertTrue(lockedKeys.output?.includes('false') === true, 'locked keys report unlocked false');

                const missing = await runCommandNonInteractive(reopened, '\\author alice');
                assertEquals(missing.exitCode, 1, 'selecting a locked key without a passphrase fails non-interactively');
                assertTrue(missing.output.includes('passphrase required'), 'locked \\author requests a passphrase');
                assertEquals(await reopened.currentAuthor(), undefined, 'failed \\author leaves the default unset');

                const selected = await runCommand(reopened, '\\author alice correct');
                assertEquals(selected.exitCode, 0, selected.output);
                assertTrue(selected.output.includes('author alice'), 'inline passphrase \\author unlocks and selects');
                assertEquals((await reopened.currentAuthor())?.keyId, alice.keyId, '\\author unlocked and selected alice');
                assertTrue(reopened.isUnlocked(alice.keyId), 'alice is now in the unlocked set');

                const unlockedKeys = await runMetaCommand(reopened, '\\keys');
                assertTrue(unlockedKeys.output?.includes('true') === true, '\\keys reports the unlocked key as true');
            });
        },
    },
];

async function runCommandNonInteractive(session: WorkspaceSession, command: string) {
    const wasTty = input.isTTY;
    try {
        (input as NodeJS.ReadStream & { isTTY?: boolean }).isTTY = false;
        return await runCommand(session, command);
    } finally {
        (input as NodeJS.ReadStream & { isTTY?: boolean }).isTTY = wasTty;
    }
}

async function withSession(fn: (session: WorkspaceSession, dbPath: string) => Promise<void>): Promise<void> {
    const dir = await mkdtemp(join(tmpdir(), 'rdb-tools-'));
    const dbPath = join(dir, 'dev.db');
    const workspace = await Workspace.open({ path: dbPath });
    const keystore = await KeyStore.open(`${dbPath}.keys.json`, workspace.replica.getHashSuite());
    const session = new WorkspaceSession({ workspace, keystore });
    try {
        await fn(session, dbPath);
    } finally {
        await workspace.close();
        await rm(dir, { recursive: true, force: true });
    }
}

function setupScript(): string {
    return `
\\key create alice correct
\\author alice
CREATE SCHEMA shop CREATORS ($me) AS (
  TABLE products (
    sku string PUB READONLY,
    name string
  )
);
CREATE TABLEGROUP shop_prod USING SCHEMA shop;
INSERT INTO shop_prod.products (sku, name) VALUES ('A', 'Widget');
SELECT sku, name FROM shop_prod.products;
LOG shop_prod LIMIT 5;
`;
}

async function main() {
    console.log('Running tests for Hyper Hyper Space v3 rdb_tools module\n');
    for (const test of tests) {
        testing.exitIfFailed(await testing.run(test.name, test.invoke));
    }
}

main();
