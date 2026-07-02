import { mkdtemp, rm } from "node:fs/promises";
import { stdin as input } from "node:process";
import type { Interface } from "node:readline/promises";
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
import { evaluateObserveGateKey, scanKeystore } from "../src/session/authz_suggest.js";
import { resolveRowIdPrefix } from "../src/session/adapter.js";
import { WorkspaceSession } from "../src/session/session.js";
import { Workspace } from "../src/workspace/workspace.js";
import { RTableGroupImpl } from "@hyper-hyper-space/hhs3_rdb";
import { formatRows, formatRowsVertical } from "../src/format/rows.js";
import {
    isTruncatable,
    looksLikeSpeculativeHash,
    uniquePrefixes,
} from "../src/format/display.js";

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
                const alias = await runMetaCommand(session, `\\alias group prod #${group.id.slice(0, 10)}`);
                assertTrue(alias.output?.includes('group prod =>') === true, 'alias output format');
                assertTrue(alias.output?.includes(group.id.slice(0, 8)) === true, 'alias output includes hash prefix');
                assertTrue(alias.output?.includes('(shop_prod)') === true, 'alias output includes payload label');
                assertEquals(session.aliases.get('group', 'prod'), group.id, 'alias stores full group id');
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
        name: '[RDB_TOOLS04c] \\help commands shows language reference',
        invoke: async () => {
            await withSession(async (session) => {
                const create = await runMetaCommand(session, '\\help commands CREATE');
                assertTrue(create.output?.includes('CREATE DATABASE') === true, 'CREATE DATABASE in filtered help');
                assertTrue(create.output?.includes('CREATE SCHEMA') === true, 'CREATE SCHEMA in filtered help');
                assertTrue(create.output?.includes('CREATE TABLEGROUP') === true, 'CREATE TABLEGROUP in filtered help');
                assertTrue(create.output?.startsWith('COMMON') !== true, 'filtered help omits COMMON block');
                assertTrue(create.output?.includes('--- common ---') !== true, 'filtered help omits common section');

                const all = await runMetaCommand(session, '\\help commands');
                assertTrue(all.output?.startsWith('--- common ---\nCOMMON') === true, 'full help starts with common section');
                assertTrue(all.output?.includes('--- creation ---') === true, 'full help includes creation section');

                const common = await runMetaCommand(session, '\\help commands common');
                const commonAlias = await runMetaCommand(session, '\\help command common');
                assertEquals(commonAlias.output, common.output, '\\help command common matches \\help commands common');
                assertTrue(common.output?.startsWith('COMMON') === true, 'common-only help starts with COMMON');
                assertTrue(common.output?.includes('CREATE SCHEMA name') !== true, 'common-only help omits command entries');
                assertTrue(common.output?.includes('--- common ---') !== true, 'common-only help omits section header');

                const noMatch = await runMetaCommand(session, '\\help commands NOPE');
                assertEquals(noMatch.output, "No C-SQL commands match 'NOPE'", 'no-match message');

                const createAlias = await runMetaCommand(session, '\\help command CREATE');
                assertEquals(createAlias.output, create.output, '\\help command matches \\help commands');

                const meta = await runMetaCommand(session, '\\help');
                assertTrue(meta.output?.includes('\\quit') === true, 'meta help includes quit');
                assertTrue(meta.output?.includes('\\help commands [filter]') === true, 'meta help includes C-SQL commands hint');
            });
        },
    },
    {
        name: '[RDB_TOOLS04b] \\dump database full and schema modes',
        invoke: async () => {
            await withSession(async (session) => {
                const setup = await runScript(session, databaseSetupScript());
                assertEquals(setup.exitCode, 0, setup.output);
                const full = await runMetaCommand(session, '\\dump database app;');
                assertTrue(full.output?.includes('CREATE DATABASE app') === true, 'full dump header');
                assertTrue(full.output?.includes('ADD SCHEMA') === true, 'full dump membership');
                assertTrue(full.output?.includes('INSERT INTO products') === true, 'full dump row ops');

                const schema = await runMetaCommand(session, '\\dump database app schema;');
                assertTrue(schema.output?.includes('CREATE DATABASE app') === true, 'schema dump header');
                assertTrue(schema.output?.includes('ADD SCHEMA shop TO app') === true, 'schema dump named ADD SCHEMA');
                assertTrue(schema.output?.includes('ADD TABLEGROUP shop_prod TO app') === true, 'schema dump named ADD TABLEGROUP');
                assertTrue(schema.output?.includes('INSERT INTO products') !== true, 'schema dump omits row ops');
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
                await runMetaCommand(session, `\\alias group prod #${group.id.slice(0, 10)}`);
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

                let unknownFailed = false;
                try {
                    await runMetaCommand(session, '\\key unlock amdin');
                } catch (e) {
                    unknownFailed = true;
                    assertTrue(e instanceof Error && e.message.includes("Unknown key 'amdin'"), 'unknown unlock fails before prompt');
                }
                assertTrue(unknownFailed, 'unknown unlock throws');

                const unknownNonInteractive = await runCommandNonInteractive(session, '\\key unlock amdin');
                assertEquals(unknownNonInteractive.exitCode, 1, 'non-interactive unknown unlock fails');
                assertTrue(unknownNonInteractive.output.includes('Unknown key'), 'unknown unlock reports unknown key');
                assertTrue(!unknownNonInteractive.output.includes('passphrase required'), 'unknown unlock does not request passphrase');

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
    {
        name: '[RDB_TOOLS13] locked key on signed statement fails non-interactively',
        invoke: async () => {
            await withSession(async (session, dbPath) => {
                if (session.keystore === undefined) throw new Error('missing keystore');
                const setup = await runScript(session, setupScript());
                assertEquals(setup.exitCode, 0, setup.output);
                const alice = session.keystore.list().find((key) => key.label === 'alice');
                if (alice === undefined) throw new Error('missing alice key');

                const keystore = await KeyStore.open(`${dbPath}.keys.json`, session.workspace.replica.getHashSuite());
                const reopened = new WorkspaceSession({ workspace: session.workspace, keystore });
                assertTrue(!reopened.isUnlocked(alice.keyId), 'alice is locked in fresh session');

                const missing = await runCommandNonInteractive(
                    reopened,
                    "INSERT INTO shop_prod.products (sku, name) VALUES ('B', 'Gadget') BY $alice;",
                );
                assertEquals(missing.exitCode, 1, 'locked BY author fails non-interactively');
                assertTrue(missing.output.includes('passphrase required'), 'non-interactive signed insert requests passphrase');
                assertTrue(!reopened.isUnlocked(alice.keyId), 'failed unlock leaves key locked');
            });
        },
    },
    {
        name: '[RDB_TOOLS14] scoped aliases resolve keys and versions',
        invoke: async () => {
            await withSession(async (session, dbPath) => {
                const setup = await runScript(session, setupScript());
                assertEquals(setup.exitCode, 0, setup.output);

                const alice = session.keystore!.list().find((k) => k.label === 'alice')!;
                await runMetaCommand(session, `\\alias key signer #${alice.keyId.slice(0, 10)}`);
                assertTrue(session.aliases.get('key', 'signer') === alice.keyId, 'key alias stored');

                const author = await runMetaCommand(session, '\\author signer');
                assertTrue(author.output?.includes('author alice') === true, 'author via key alias');

                const group = session.workspace.roots.list('group')[0];
                const dag = await group.object!.getScopedDag();
                const opHashes: string[] = [];
                for await (const entry of dag.loadAllEntries()) opHashes.push(entry.hash);
                assertTrue(opHashes.length > 0, 'group has op hashes');
                const cutHash = opHashes[opHashes.length - 1];
                await runMetaCommand(session, `\\alias version cut #${cutHash.slice(0, 10)}`);

                const view = await runCommand(session, 'SET VIEW AT {cut};');
                assertEquals(view.exitCode, 0, view.output);

                const overwrite = await runMetaCommand(session, `\\alias group prod #${group.id.slice(0, 10)}`);
                assertTrue(overwrite.output?.includes('group prod =>') === true, 're-alias overwrites');
                await runMetaCommand(session, '\\unalias group prod');

                const freshSession = new WorkspaceSession({ workspace: session.workspace, keystore: session.keystore });
                assertEquals(freshSession.aliases.get('group', 'prod'), undefined, 'aliases session-only');
                const keys = await runMetaCommand(freshSession, '\\keys');
                assertTrue(keys.output?.includes('alice') === true, 'keystore label persists');
            });
        },
    },
    {
        name: '[RDB_TOOLS15] same-hash alias fan-out across scopes',
        invoke: async () => {
            await withSession(async (session) => {
                const setup = await runScript(session, setupScript());
                assertEquals(setup.exitCode, 0, setup.output);

                const group = session.workspace.roots.list('group')[0];
                const dag = await group.object!.getScopedDag();
                const opHashes: string[] = [];
                for await (const entry of dag.loadAllEntries()) opHashes.push(entry.hash);
                assertEquals(opHashes[0], group.id, 'group id equals genesis op hash');

                const prefix = group.id.slice(0, 12);
                const fanOut = await runMetaCommand(session, `\\alias genesis #${prefix}`);
                assertTrue(fanOut.output?.includes('group genesis =>') === true, 'fan-out includes group line');
                assertTrue(fanOut.output?.includes('version genesis =>') === true, 'fan-out includes version line');
                assertTrue(fanOut.output?.includes('(shop_prod)') === true, 'version line includes root name');
                assertEquals(session.aliases.get('group', 'genesis'), group.id, 'group alias stored');
                assertEquals(session.aliases.get('version', 'genesis'), group.id, 'version alias stored');

                const view = await runCommand(session, 'SET VIEW AT {genesis};');
                assertEquals(view.exitCode, 0, view.output);

                const explicit = await runMetaCommand(session, `\\alias group only #${prefix}`);
                assertTrue(explicit.output?.includes('group only =>') === true, 'explicit scope output');
                assertEquals(session.aliases.get('group', 'only'), group.id, 'explicit group alias stored');
                assertEquals(session.aliases.get('version', 'only'), undefined, 'explicit scope does not set version');

                let ambiguous = false;
                try {
                    await runMetaCommand(session, '\\alias bad #');
                } catch (e) {
                    ambiguous = true;
                    assertTrue(
                        e instanceof Error && e.message.includes('Ambiguous alias prefix'),
                        'short prefix still ambiguous across different hashes',
                    );
                }
                assertTrue(ambiguous, 'ambiguous prefix should fail');
            });
        },
    },
    {
        name: '[RDB_TOOLS16] \\dump database emits alias definitions and replays',
        invoke: async () => {
            const dir = await mkdtemp(join(tmpdir(), 'rdb-tools-dump-alias-'));
            const dbPath = join(dir, 'dev.db');
            const keysPath = `${dbPath}.keys.json`;
            try {
                const workspace1 = await Workspace.open({ path: dbPath });
                const keystore1 = await KeyStore.open(keysPath, workspace1.replica.getHashSuite());
                const session1 = new WorkspaceSession({ workspace: workspace1, keystore: keystore1 });
                const setup = await runScript(session1, databaseSetupScript());
                assertEquals(setup.exitCode, 0, setup.output);

                const full = await runMetaCommand(session1, '\\dump database app;');
                const dumpText = full.output ?? '';
                assertTrue(dumpText.includes('\\alias key alice #'), 'dump defines key alias with full hash');
                assertTrue(dumpText.includes('\\alias version '), 'dump defines version aliases');
                assertTrue(dumpText.includes('BY $alice'), 'dump uses aliased BY author');
                assertTrue(dumpText.includes('_ver'), 'dump uses version alias names');
                assertTrue(dumpText.includes('INSERT INTO products'), 'dump includes row ops');
                await workspace1.close();

                const workspace2 = await Workspace.open({ path: join(dir, 'replay.db') });
                const { copyFile } = await import('node:fs/promises');
                await copyFile(keysPath, `${join(dir, 'replay.db')}.keys.json`);
                const keystoreReplay = await KeyStore.open(`${join(dir, 'replay.db')}.keys.json`, workspace2.replica.getHashSuite());
                const session2 = new WorkspaceSession({ workspace: workspace2, keystore: keystoreReplay });
                const replay = await runScript(session2, `\\key unlock alice correct\n${dumpText}`);
                assertEquals(replay.exitCode, 0, replay.output);
                await workspace2.close();
            } finally {
                await rm(dir, { recursive: true, force: true });
            }
        },
    },
    {
        name: '[RDB_TOOLS17] \\delta schema reports schema slot changes',
        invoke: async () => {
            await withSession(async (session) => {
                const setup = await runScript(session, setupScript());
                assertEquals(setup.exitCode, 0, setup.output);
                const altered = await runCommand(session, "ALTER SCHEMA shop AS ( ADD COLUMN products.price integer DEFAULT 0 );");
                assertEquals(altered.exitCode, 0, altered.output);

                const schema = session.workspace.roots.list('schema')[0];
                const delta = await runMetaCommand(session, `\\delta schema shop #${schema.id.slice(0, 10)} LATEST`);
                assertTrue(delta.output?.includes('add-column') === true, 'schema delta includes add-column');
                assertTrue(delta.output?.includes('price') === true, 'schema delta includes new column name');
            });
        },
    },
    {
        name: '[RDB_TOOLS18] \\delta group reports row inserts',
        invoke: async () => {
            await withSession(async (session) => {
                const setup = await runScript(session, setupScript());
                assertEquals(setup.exitCode, 0, setup.output);

                const group = session.workspace.roots.list('group')[0];
                const delta = await runMetaCommand(session, `\\delta group shop_prod #${group.id.slice(0, 10)} LATEST`);
                assertTrue(delta.output?.includes('products') === true, 'group delta names products table');
                assertTrue(delta.output?.includes('false -> true') === true, 'group delta shows row insert liveness');
                assertTrue(delta.output?.includes('Widget') === true, 'group delta shows inserted value');
            });
        },
    },
    {
        name: '[RDB_TOOLS19] \\delta group with identical bounds is empty',
        invoke: async () => {
            await withSession(async (session) => {
                const setup = await runScript(session, setupScript());
                assertEquals(setup.exitCode, 0, setup.output);

                const delta = await runMetaCommand(session, '\\delta group shop_prod LATEST LATEST');
                assertTrue(delta.output?.includes('(no changes)') === true, 'identical bounds produce no changes');
            });
        },
    },
    {
        name: '[RDB_TOOLS20] \\delta group respects json output mode',
        invoke: async () => {
            await withSession(async (session) => {
                const setup = await runScript(session, setupScript());
                assertEquals(setup.exitCode, 0, setup.output);

                const group = session.workspace.roots.list('group')[0];
                await runMetaCommand(session, '\\output json');
                const delta = await runMetaCommand(session, `\\delta group shop_prod #${group.id.slice(0, 10)} LATEST`);
                const parsed = JSON.parse(delta.output ?? '{}') as {
                    kind?: string;
                    schemaChanges?: { tableChanges: unknown[] };
                    tables?: unknown[];
                    opVerdictChanges?: unknown[];
                };
                assertEquals(parsed.kind, 'group', 'json kind');
                assertTrue(Array.isArray(parsed.schemaChanges?.tableChanges), 'json schemaChanges.tableChanges');
                assertTrue(Array.isArray(parsed.tables), 'json tables');
                assertTrue(Array.isArray(parsed.opVerdictChanges), 'json opVerdictChanges');
                assertTrue((parsed.tables?.length ?? 0) > 0, 'json tables non-empty after insert');
            });
        },
    },
    {
        name: '[RDB_TOOLS30] \\delta group op changes include void reason',
        invoke: async () => {
            await withSession(async (session) => {
                const setup = await runScript(session, voidDeltaSetupScript());
                assertEquals(setup.exitCode, 0, setup.output);

                const resolved = await session.workspace.roots.resolveGroup(
                    { kind: 'name', text: 'void_g', parts: ['void_g'], span: { start: 0, end: 6, line: 1, column: 1 } },
                    { aliases: session.aliases },
                );
                if (resolved.group === undefined) throw new Error('void_g not loaded');
                const base = await (await resolved.group.getScopedDag()).getFrontier();
                const baseHash = [...base][0];
                const capsView = await (await resolved.group.getView(base, base)).getTableView('caps');
                const capRows = await capsView.findRowIds({ label: 'grant' });
                assertEquals(capRows.length, 1, 'one cap row at base');
                const capRowId = capRows[0];

                const revoke = await runCommand(
                    session,
                    `DELETE FROM void_g.caps WHERE rowId = '${capRowId}' AT #${baseHash.slice(0, 10)};`,
                );
                assertEquals(revoke.exitCode, 0, revoke.output);
                const insert = await runCommand(
                    session,
                    `INSERT INTO void_g.items (name) VALUES ('thing') AT #${baseHash.slice(0, 10)};`,
                );
                assertEquals(insert.exitCode, 0, insert.output);

                await runMetaCommand(session, '\\output json');
                const delta = await runMetaCommand(session, `\\delta group void_g #${baseHash.slice(0, 10)} LATEST`);
                const parsed = JSON.parse(delta.output ?? '{}') as {
                    opVerdictChanges?: Array<{ reason?: { kind?: string }; voidHorizon?: string }>;
                };
                assertTrue((parsed.opVerdictChanges?.length ?? 0) > 0, 'op flips present');
                const flip = parsed.opVerdictChanges![0];
                assertEquals(flip.voidHorizon, 'end', 'reason at end horizon');
                assertEquals(flip.reason?.kind, 'restriction', 'restriction void reason');
            });
        },
    },
    {
        name: '[RDB_TOOLS21] \\delta rejects database kind',
        invoke: async () => {
            await withSession(async (session) => {
                const setup = await runScript(session, databaseSetupScript());
                assertEquals(setup.exitCode, 0, setup.output);
                const db = session.workspace.roots.list('database')[0];
                let failed = false;
                try {
                    await runMetaCommand(session, `\\delta database app #${db.id.slice(0, 10)} LATEST`);
                } catch (e) {
                    failed = true;
                    assertTrue(
                        e instanceof Error && e.message.includes('schema|group'),
                        'database kind rejected with usage error',
                    );
                }
                assertTrue(failed, 'database delta should fail');
            });
        },
    },
    {
        name: '[RDB_TOOLS22] hash display labels and speculative truncation',
        invoke: async () => {
            await withSession(async (session) => {
                const setup = await runScript(session, capsSetupScript());
                assertEquals(setup.exitCode, 0, setup.output);

                await runMetaCommand(session, '\\hash-labels on');
                const labeled = await runCommand(session, "SELECT label, grantee FROM user.caps;");
                assertEquals(labeled.exitCode, 0, labeled.output);
                assertTrue(labeled.output.includes('$alice'), 'grantee shows keystore label');
                assertTrue(labeled.output.includes('manager'), 'label column unchanged');

                await runMetaCommand(session, '\\hash-labels off');
                const unlabeledFull = await runCommand(session, "SELECT label, grantee FROM user.caps;");
                assertEquals(unlabeledFull.exitCode, 0, unlabeledFull.output);
                assertTrue(!unlabeledFull.output.includes('$alice'), 'labels off hides $name');
                const alice = session.keystore!.list().find((key) => key.label === 'alice');
                assertTrue(alice !== undefined && unlabeledFull.output.includes(alice.keyId), 'script default shows full grantee hash');

                await runMetaCommand(session, '\\hash-width auto');
                const unlabeled = await runCommand(session, "SELECT label, grantee FROM user.caps;");
                assertEquals(unlabeled.exitCode, 0, unlabeled.output);
                assertTrue(alice !== undefined && unlabeled.output.includes(alice.keyId.slice(0, 8)), 'grantee shows truncated hash prefix with auto width');
            });
        },
    },
    {
        name: '[RDB_TOOLS23] hash-width modes affect structural hashes',
        invoke: async () => {
            await withSession(async (session) => {
                const setup = await runScript(session, setupScript());
                assertEquals(setup.exitCode, 0, setup.output);

                const rowIdFromInsert = (output: string) => output.match(/inserted (\S+)/)?.[1] ?? '';

                await runMetaCommand(session, '\\hash-width 8');
                const shortInsert = await runCommand(session, "INSERT INTO shop_prod.products (sku, name) VALUES ('B', 'Other');");
                assertEquals(shortInsert.exitCode, 0, shortInsert.output);
                assertEquals(rowIdFromInsert(shortInsert.output).length, 8, 'hash-width 8 truncates insert rowId');

                await runMetaCommand(session, '\\hash-width full');
                const fullInsert = await runCommand(session, "INSERT INTO shop_prod.products (sku, name) VALUES ('C', 'Other2');");
                assertEquals(fullInsert.exitCode, 0, fullInsert.output);
                assertTrue(rowIdFromInsert(fullInsert.output).length >= 40, 'hash-width full shows full rowId');

                await runMetaCommand(session, '\\hash-width 12');
                const fixedInsert = await runCommand(session, "INSERT INTO shop_prod.products (sku, name) VALUES ('D', 'Other3');");
                assertEquals(fixedInsert.exitCode, 0, fixedInsert.output);
                assertEquals(rowIdFromInsert(fixedInsert.output).length, 12, 'hash-width 12 truncates rowId');
            });
        },
    },
    {
        name: '[RDB_TOOLS24] hash-width auto disambiguates multiple hashes in LOG',
        invoke: async () => {
            await withSession(async (session) => {
                const setup = await runScript(session, setupScript());
                assertEquals(setup.exitCode, 0, setup.output);
                await runMetaCommand(session, '\\hash-width auto');
                const log = await runCommand(session, "LOG shop_prod LIMIT 5;");
                assertEquals(log.exitCode, 0, log.output);
                const hashLines = log.output.split('\n').slice(2).filter((line) => line.includes('|'));
                assertTrue(hashLines.length >= 2, 'log has multiple rows');
                const hashes = hashLines.map((line) => line.split('|')[0]?.trim().replace(/^#/, '') ?? '');
                assertTrue(hashes.every((h) => h.length >= 8), 'auto prefixes are at least 8 chars');
                assertTrue(new Set(hashes).size === hashes.length, 'auto prefixes are unique in batch');
            });
        },
    },
    {
        name: '[RDB_TOOLS25] json output bypasses hash display formatting',
        invoke: async () => {
            await withSession(async (session) => {
                const setup = await runScript(session, setupScript());
                assertEquals(setup.exitCode, 0, setup.output);
                await runMetaCommand(session, '\\hash-width 8');
                await runMetaCommand(session, '\\hash-labels on');
                await runMetaCommand(session, '\\output json');
                const selected = await runCommand(session, "SELECT sku, name FROM shop_prod.products LIMIT 1;");
                assertEquals(selected.exitCode, 0, selected.output);
                const parsed = JSON.parse(selected.output) as {
                    kind: string;
                    rows: Array<{ rowId: string; values: { sku: string } }>;
                };
                assertEquals(parsed.kind, 'select', 'json kind');
                assertTrue(parsed.rows[0].rowId.length >= 40, 'json rowId stays full length');
                assertEquals(parsed.rows[0].values.sku, 'A', 'json keeps raw cell values');
            });
        },
    },
    {
        name: '[RDB_TOOLS26] REPL and script mode defaults',
        invoke: async () => {
            await withSession(async (session) => {
                assertEquals(session.hashLabels, false, 'base session defaults labels off');
                assertEquals(session.hashWidth, 'auto', 'base session defaults hash-width auto');
                assertEquals(session.refAutoUpdate, false, 'base session defaults ref-auto-update off');

                session.enableScriptDefaults();
                assertEquals(session.hashWidth, 'full', 'script defaults hash-width full');
                assertEquals(session.hashLabels, false, 'script defaults labels still off');
                assertEquals(session.refAutoUpdate, false, 'script defaults ref-auto-update off');

                session.enableReplDefaults();
                assertEquals(session.hashLabels, true, 'repl defaults labels on');
                assertEquals(session.refAutoUpdate, true, 'repl defaults ref-auto-update on');
            });
        },
    },
    {
        name: '[RDB_TOOLS27] display helpers classify strings for truncation',
        invoke: async () => {
            assertTrue(!looksLikeSpeculativeHash('manager'), 'short label is not speculative hash');
            assertTrue(!looksLikeSpeculativeHash('something'), 'plain word is not speculative hash');
            assertTrue(
                looksLikeSpeculativeHash('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789=='),
                'long base64 ending in = is speculative hash',
            );
            assertTrue(isTruncatable('manager', 'cell') === false, 'cell role skips normal text');
            assertTrue(isTruncatable('abc', 'hash') === true, 'structural role always truncatable');

            const hashes = ['aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa=', 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb='];
            const prefixes = uniquePrefixes(hashes);
            assertEquals(prefixes.get(hashes[0])?.length, 8, 'uniquePrefixes starts at 8');
            assertTrue(prefixes.get(hashes[0]) !== prefixes.get(hashes[1]), 'uniquePrefixes disambiguates');
        },
    },
    {
        name: '[RDB_TOOLS28] hash-width and hash-labels meta commands',
        invoke: async () => {
            await withSession(async (session) => {
                const width = await runMetaCommand(session, '\\hash-width 16');
                assertEquals(width.output, 'hash-width 16', 'hash-width set');
                assertEquals(session.hashWidth, 16, 'session hashWidth updated');

                const labels = await runMetaCommand(session, '\\hash-labels on');
                assertEquals(labels.output, 'hash-labels on', 'hash-labels set');
                assertEquals(session.hashLabels, true, 'session hashLabels updated');

                const help = await runMetaCommand(session, '\\help');
                assertTrue(help.output?.includes('\\hash-width') === true, 'help lists hash-width');
                assertTrue(help.output?.includes('\\hash-labels') === true, 'help lists hash-labels');
            });
        },
    },
    {
        name: '[RDB_TOOLS29] \\delta group column values use hash display formatting',
        invoke: async () => {
            await withSession(async (session) => {
                const setup = await runScript(session, capsSetupScript());
                assertEquals(setup.exitCode, 0, setup.output);

                const alice = session.keystore!.list().find((key) => key.label === 'alice');
                if (alice === undefined) throw new Error('missing alice key');

                await runMetaCommand(session, '\\hash-labels on');
                const group = session.workspace.roots.list('group')[0];
                const delta = await runMetaCommand(session, `\\delta group user #${group.id.slice(0, 10)} LATEST`);
                assertTrue(delta.output?.includes('grantee:') === true, 'delta shows grantee column change');
                assertTrue(delta.output?.includes('$alice') === true, 'delta columns show keystore label');
                assertTrue(delta.output?.includes(alice.keyId) !== true, 'delta columns hide raw grantee keyId');

                await runMetaCommand(session, '\\hash-labels off');
                const unlabeled = await runMetaCommand(session, `\\delta group user #${group.id.slice(0, 10)} LATEST`);
                assertTrue(unlabeled.output?.includes(alice.keyId) === true, 'delta columns show full grantee hash when labels off');
            });
        },
    },
    {
        name: '[RDB_TOOLS31] ref-auto-update meta command',
        invoke: async () => {
            await withSession(async (session) => {
                const off = await runMetaCommand(session, '\\ref-auto-update off');
                assertEquals(off.output, 'ref-auto-update off', 'meta off output');
                assertEquals(session.refAutoUpdate, false, 'session ref-auto-update off');

                const on = await runMetaCommand(session, '\\ref-auto-update on');
                assertEquals(on.output, 'ref-auto-update on', 'meta on output');
                assertEquals(session.refAutoUpdate, true, 'session ref-auto-update on');
            });
        },
    },
    {
        name: '[RDB_TOOLS32] ref-auto-update propagates to bound observers',
        invoke: async () => {
            await withSession(async (session) => {
                const setup = await runScript(session, crossGroupSetupScript());
                assertEquals(setup.exitCode, 0, setup.output);

                session.setRefAutoUpdate(true);
                const inserted = await runCommand(session, "INSERT INTO users.identities (name) VALUES ('ada');");
                assertEquals(inserted.exitCode, 0, inserted.output);
                assertTrue(inserted.output.includes('updated ref on shop_prod to #'), inserted.output);

                session.setOutputMode('json');
                const selected = await runCommand(session, 'SELECT * FROM users.identities;');
                assertEquals(selected.exitCode, 0, selected.output);
                const parsed = JSON.parse(selected.output) as { rows: { rowId: string }[] };
                const prefix = parsed.rows[0].rowId.slice(0, 8);
                session.setOutputMode('table');
                const order = await runCommand(session, `INSERT INTO shop_prod.orders (customer, label) VALUES (#${prefix}, 'o1');`);
                assertEquals(order.exitCode, 0, order.output);
            });
        },
    },
    {
        name: '[RDB_TOOLS33] ref-auto-update notices suppressed in json output mode',
        invoke: async () => {
            await withSession(async (session) => {
                await runScript(session, crossGroupSetupScript());
                session.setRefAutoUpdate(true);
                session.setOutputMode('json');
                const inserted = await runCommand(session, "INSERT INTO users.identities (name) VALUES ('bob');");
                assertEquals(inserted.exitCode, 0, inserted.output);
                assertTrue(!inserted.output.includes('updated ref on'), inserted.output);
            });
        },
    },
    {
        name: '[RDB_TOOLS34] ref-auto-update off by default in scripts',
        invoke: async () => {
            await withSession(async (session) => {
                await runScript(session, crossGroupSetupScript());
                assertEquals(session.refAutoUpdate, false, 'script mode leaves ref-auto-update off');
                const inserted = await runCommand(session, "INSERT INTO users.identities (name) VALUES ('carl');");
                assertEquals(inserted.exitCode, 0, inserted.output);
                assertTrue(!inserted.output.includes('updated ref on'), inserted.output);
            });
        },
    },
    {
        name: '[RDB_TOOLS35] ref-auto-update resolves gated observe author from keystore',
        invoke: async () => {
            await withSession(async (session) => {
                const setup = await runScript(session, gatedCrossGroupSetupScript());
                assertEquals(setup.exitCode, 0, setup.output);

                session.setRefAutoUpdate(true);
                await runMetaCommand(session, '\\author nobody');
                const inserted = await runCommand(session, "INSERT INTO users.identities (name) VALUES ('ada');");
                assertEquals(inserted.exitCode, 0, inserted.output);
                assertTrue(inserted.output.includes('updated ref on doc to #'), inserted.output);
            });
        },
    },
    {
        name: '[RDB_TOOLS36] ref-auto-update reports locked keystore author for gated observe',
        invoke: async () => {
            const dir = await mkdtemp(join(tmpdir(), 'rdb-tools-'));
            const dbPath = join(dir, 'dev.db');
            try {
                const workspace1 = await Workspace.open({ path: dbPath });
                const keystore1 = await KeyStore.open(`${dbPath}.keys.json`, workspace1.replica.getHashSuite());
                const session1 = new WorkspaceSession({ workspace: workspace1, keystore: keystore1 });
                const setup = await runScript(session1, gatedBobManagerSetupScript());
                assertEquals(setup.exitCode, 0, setup.output);
                await workspace1.close();

                const workspace2 = await Workspace.open({ path: dbPath });
                const keystore2 = await KeyStore.open(`${dbPath}.keys.json`, workspace2.replica.getHashSuite());
                const session2 = new WorkspaceSession({ workspace: workspace2, keystore: keystore2 });
                try {
                    await session2.unlockKey('alice', 'correct');
                    session2.setRefAutoUpdate(true);
                    await runMetaCommand(session2, '\\author nobody');
                    const inserted = await runCommandNonInteractive(
                        session2,
                        "INSERT INTO users.identities (name) VALUES ('mallory');",
                    );
                    assertEquals(inserted.exitCode, 0, inserted.output);
                    assertTrue(inserted.output.includes('needs $bob (locked)'), inserted.output);
                } finally {
                    await workspace2.close();
                }
            } finally {
                await rm(dir, { recursive: true, force: true });
            }
        },
    },
    {
        name: '[RDB_TOOLS37] validation failure suggests keystore author',
        invoke: async () => {
            await withSession(async (session) => {
                const setup = await runScript(session, setupScript());
                assertEquals(setup.exitCode, 0, setup.output);

                const group = session.workspace.roots.list('group')[0];
                const table = await (group.object as RTableGroupImpl).getTable('products');
                const rowId = (await (await table.getView()).liveRowIds())[0];

                const anonymous = new WorkspaceSession({ workspace: session.workspace, keystore: session.keystore });
                const del = await runCommand(
                    anonymous,
                    `DELETE FROM shop_prod.products WHERE rowId = '${rowId}';`,
                );
                assertEquals(del.exitCode, 2, del.output);
                assertTrue(del.output.includes('hint: BY $alice'), del.output);
            });
        },
    },
    {
        name: '[RDB_TOOLS38] scanKeystore finds observe-gate satisfying keys',
        invoke: async () => {
            await withSession(async (session) => {
                const setup = await runScript(session, gatedCrossGroupSetupScript());
                assertEquals(setup.exitCode, 0, setup.output);

                const docRoot = session.workspace.roots.list('group').find((root) => root.name === 'doc');
                const usersRoot = session.workspace.roots.list('group').find((root) => root.name === 'users');
                if (docRoot?.object === undefined || usersRoot === undefined) throw new Error('missing groups');

                const doc = docRoot.object as RTableGroupImpl;
                const users = usersRoot.object as RTableGroupImpl;
                const at = await (await users.getScopedDag()).getFrontier();
                const candidates = await scanKeystore(
                    session,
                    (keyId) => evaluateObserveGateKey(doc, usersRoot.id, at, at, keyId),
                );
                assertEquals(candidates.length, 1, 'one manager-cap key');
                assertEquals(candidates[0].label, 'alice', 'alice holds manager cap');
            });
        },
    },
    {
        name: '[RDB_TOOLS39] REPL sign-and-retry succeeds without validation error',
        invoke: async () => {
            await withSession(async (session) => {
                const setup = await runScript(session, gatedCapsSetupScript());
                assertEquals(setup.exitCode, 0, setup.output);

                await runMetaCommand(session, '\\author nobody');
                const inserted = await runCommandInteractive(
                    session,
                    "INSERT INTO user.caps (label, grantee) VALUES ('writer', 'carl');",
                    ['Y'],
                );
                assertEquals(inserted.exitCode, 0, inserted.output);
                assertTrue(inserted.output.includes('inserted '), inserted.output);
                assertTrue(!inserted.output.includes('VALIDATION_REJECTED'), inserted.output);
            });
        },
    },
    {
        name: '[RDB_TOOLS40] explicit BY NOBODY skips sign-and-retry',
        invoke: async () => {
            await withSession(async (session) => {
                const setup = await runScript(session, gatedCapsSetupScript());
                assertEquals(setup.exitCode, 0, setup.output);

                await runMetaCommand(session, '\\author nobody');
                let prompts = 0;
                const inserted = await runCommandInteractive(
                    session,
                    "INSERT INTO user.caps (label, grantee) VALUES ('writer', 'carl') BY NOBODY;",
                    [],
                    () => { prompts++; return 'Y'; },
                );
                assertEquals(inserted.exitCode, 2, inserted.output);
                assertTrue(inserted.output.includes('VALIDATION_REJECTED'), inserted.output);
                assertTrue(inserted.output.includes('hint: BY $admin'), inserted.output);
                assertEquals(prompts, 0, 'no sign-and-retry prompt');
            });
        },
    },
    {
        name: '[RDB_TOOLS41] explicit BY failing key skips sign-and-retry',
        invoke: async () => {
            await withSession(async (session) => {
                const setup = await runScript(session, gatedCapsSetupScript());
                assertEquals(setup.exitCode, 0, setup.output);

                await runMetaCommand(session, '\\author nobody');
                let prompts = 0;
                const inserted = await runCommandInteractive(
                    session,
                    "INSERT INTO user.caps (label, grantee) VALUES ('writer', 'carl') BY $bob;",
                    [],
                    () => { prompts++; return 'Y'; },
                );
                assertEquals(inserted.exitCode, 2, inserted.output);
                assertTrue(inserted.output.includes('VALIDATION_REJECTED'), inserted.output);
                assertTrue(inserted.output.includes('hint: BY $admin'), inserted.output);
                assertEquals(prompts, 0, 'no sign-and-retry prompt');
            });
        },
    },
    {
        name: '[RDB_TOOLS42] declined sign-and-retry shows validation error and hint',
        invoke: async () => {
            await withSession(async (session) => {
                const setup = await runScript(session, gatedCapsSetupScript());
                assertEquals(setup.exitCode, 0, setup.output);

                await runMetaCommand(session, '\\author nobody');
                const inserted = await runCommandInteractive(
                    session,
                    "INSERT INTO user.caps (label, grantee) VALUES ('writer', 'carl');",
                    ['n'],
                );
                assertEquals(inserted.exitCode, 2, inserted.output);
                assertTrue(inserted.output.includes('VALIDATION_REJECTED'), inserted.output);
                assertTrue(inserted.output.includes('hint: BY $admin'), inserted.output);
            });
        },
    },
    {
        name: '[RDB_TOOLS43] bind-author retry succeeds for ALTER SCHEMA',
        invoke: async () => {
            await withSession(async (session) => {
                const setup = await runScript(session, setupScript());
                assertEquals(setup.exitCode, 0, setup.output);

                await runMetaCommand(session, '\\author nobody');
                const altered = await runCommandInteractive(
                    session,
                    'ALTER SCHEMA shop AS (ADD COLUMN products.tag string NULL);',
                    ['Y'],
                );
                assertEquals(altered.exitCode, 0, altered.output);
                assertTrue(!altered.output.includes('BIND_UNKNOWN_NAME'), altered.output);
            });
        },
    },
    {
        name: '[RDB_TOOLS44] explicit BY NOBODY skips bind-author retry',
        invoke: async () => {
            await withSession(async (session) => {
                const setup = await runScript(session, setupScript());
                assertEquals(setup.exitCode, 0, setup.output);

                await runMetaCommand(session, '\\author nobody');
                let prompts = 0;
                const altered = await runCommandInteractive(
                    session,
                    'ALTER SCHEMA shop AS (ADD COLUMN products.tag string NULL) BY NOBODY;',
                    [],
                    () => { prompts++; return 'Y'; },
                );
                assertEquals(altered.exitCode, 2, altered.output);
                assertTrue(altered.output.includes('BIND_UNKNOWN_NAME'), altered.output);
                assertTrue(altered.output.includes('hint: BY $alice'), altered.output);
                assertEquals(prompts, 0, 'no bind-author retry prompt');
            });
        },
    },
    {
        name: '[RDB_TOOLS45] declined bind-author retry shows error and hint',
        invoke: async () => {
            await withSession(async (session) => {
                const setup = await runScript(session, setupScript());
                assertEquals(setup.exitCode, 0, setup.output);

                await runMetaCommand(session, '\\author nobody');
                const altered = await runCommandInteractive(
                    session,
                    'ALTER SCHEMA shop AS (ADD COLUMN products.tag string NULL);',
                    ['n'],
                );
                assertEquals(altered.exitCode, 2, altered.output);
                assertTrue(altered.output.includes('BIND_UNKNOWN_NAME'), altered.output);
                assertTrue(altered.output.includes('hint: BY $alice'), altered.output);
            });
        },
    },
    {
        name: '[RDB_TOOLS46] bind-author retry succeeds for ADD TABLEGROUP',
        invoke: async () => {
            await withSession(async (session) => {
                const setup = await runScript(session, addMemberBindRetrySetupScript());
                assertEquals(setup.exitCode, 0, setup.output);

                await runMetaCommand(session, '\\author nobody');
                const added = await runCommandInteractive(
                    session,
                    'ADD TABLEGROUP shop_prod TO app;',
                    ['Y'],
                );
                assertEquals(added.exitCode, 0, added.output);
                assertTrue(!added.output.includes('BIND_UNKNOWN_NAME'), added.output);
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

async function runCommandInteractive(
    session: WorkspaceSession,
    command: string,
    answers: string[],
    onQuestion?: () => string,
): Promise<Awaited<ReturnType<typeof runCommand>>> {
    const wasTty = input.isTTY;
    let i = 0;
    const rl = {
        question: async () => {
            if (onQuestion !== undefined) return onQuestion();
            return answers[i++] ?? '';
        },
    } as unknown as Interface;
    try {
        (input as NodeJS.ReadStream & { isTTY?: boolean }).isTTY = true;
        return await runCommand(session, command, undefined, { rl });
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

function voidDeltaSetupScript(): string {
    return `
\\key create alice correct
\\author alice
CREATE SCHEMA voidtest CREATORS ($me) AS (
  TABLE caps (
    label string PUB
  ) CONCURRENT DELETES,
  TABLE items (
    name string
  ) ALLOW insert IF EXISTS caps WHERE label = 'grant'
);
CREATE TABLEGROUP void_g USING SCHEMA voidtest;
INSERT INTO void_g.caps (uuid, label) VALUES ('c-1', 'grant');
`;
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

function crossGroupSetupScript(): string {
    return `
\\key create alice correct
\\author alice
CREATE SCHEMA users_schema CREATORS ($me) AS (
  TABLE identities (name string) ALLOW all IF true
);
CREATE TABLEGROUP users USING SCHEMA users_schema;
CREATE SCHEMA shop CREATORS ($me) AS (
  TABLE orders (
    customer string REFERENCES users.identities,
    label string
  ) ALLOW all IF true
);
CREATE TABLEGROUP shop_prod USING SCHEMA shop BIND users => users;
`;
}

function gatedCapsSetupScript(): string {
    return `
\\key create admin correct
\\author admin
CREATE SCHEMA users_schema CREATORS ($me) AS (
  TABLE identities (name string) ALLOW all IF true,
  TABLE caps (
    label string PUB,
    grantee string PUB
  ) ALLOW insert IF EXISTS caps AS c WHERE c.label = 'manager' AND c.grantee = $author
);
CREATE TABLEGROUP user USING SCHEMA users_schema
  WITH ROWS (
    caps (uuid='61169c8a-4106-43a1-8d37-39373c07da7a', label='manager', grantee=$me)
  );
\\key create bob correct
`;
}

function gatedCrossGroupSetupScript(): string {
    return `
\\key create alice correct
\\author alice
CREATE SCHEMA users_schema CREATORS ($me) AS (
  TABLE identities (name string) ALLOW all IF true,
  TABLE caps (label string PUB, grantee string PUB) ALLOW all IF true
);
CREATE TABLEGROUP users USING SCHEMA users_schema;
INSERT INTO users.caps (label, grantee) VALUES ('manager', $me);
CREATE SCHEMA doc_schema CREATORS ($me) AS (
  TABLE notes (
    body string NULL REFERENCES users.identities,
    label string
  ) ALLOW all IF true
);
CREATE TABLEGROUP doc USING SCHEMA doc_schema
  BIND users => users
  ALLOW UPDATE REF users IF EXISTS caps WHERE label = 'manager' AND grantee = $author;
`;
}

function gatedBobManagerSetupScript(): string {
    return `
\\key create alice correct
\\author alice
CREATE SCHEMA users_schema CREATORS ($me) AS (
  TABLE identities (name string) ALLOW all IF true,
  TABLE caps (label string PUB, grantee string PUB) ALLOW all IF true
);
CREATE TABLEGROUP users USING SCHEMA users_schema;
\\key create bob correct
INSERT INTO users.caps (label, grantee) VALUES ('manager', $bob);
CREATE SCHEMA doc_schema CREATORS ($me) AS (
  TABLE notes (
    body string NULL REFERENCES users.identities,
    label string
  ) ALLOW all IF true
);
CREATE TABLEGROUP doc USING SCHEMA doc_schema
  BIND users => users
  ALLOW UPDATE REF users IF EXISTS caps WHERE label = 'manager' AND grantee = $author;
`;
}

function capsSetupScript(): string {
    return `
\\key create alice correct
\\author alice
CREATE SCHEMA user CREATORS ($me) AS (
  TABLE caps (
    label string PUB,
    grantee string PUB
  )
);
CREATE TABLEGROUP user USING SCHEMA user;
INSERT INTO user.caps (label, grantee) VALUES ('manager', $me);
`;
}

function addMemberBindRetrySetupScript(): string {
    return `
\\key create alice correct
\\author alice
CREATE DATABASE app CREATORS ($me);
CREATE SCHEMA shop CREATORS ($me) AS (
  TABLE products (
    sku string PUB READONLY,
    name string
  )
);
CREATE TABLEGROUP shop_prod USING SCHEMA shop;
ADD SCHEMA shop TO app BY $alice;
`;
}

function databaseSetupScript(): string {
    return `
\\key create alice correct
\\author alice
CREATE DATABASE app;
CREATE SCHEMA shop CREATORS ($me) AS (
  TABLE products (
    sku string PUB READONLY,
    name string
  )
);
CREATE TABLEGROUP shop_prod USING SCHEMA shop;
ADD SCHEMA shop TO app;
ADD TABLEGROUP shop_prod TO app;
INSERT INTO shop_prod.products (sku, name) VALUES ('A', 'Widget');
`;
}

async function main() {
    console.log('Running tests for Hyper Hyper Space v3 rdb_tools module\n');
    for (const test of tests) {
        testing.exitIfFailed(await testing.run(test.name, test.invoke));
    }
}

main();
