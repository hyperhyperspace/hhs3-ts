import {
    MemoryKeyVault,
    openMemWorkspace,
} from "@hyper-hyper-space/hhs3_rdb_runtime";
import {
    ReplSession,
    formatRows,
    promptForSession,
    renderStatementOutput,
    runCommand,
    runLanguageText,
} from "../src/index.js";

function assert(condition: unknown, message: string): asserts condition {
    if (!condition) throw new Error(message);
}

function assertEqual(actual: unknown, expected: unknown, message: string): void {
    assert(actual === expected, `${message}: expected ${String(expected)}, got ${String(actual)}`);
}

async function main(): Promise<void> {
    const workspace = await openMemWorkspace();
    const keyVault = new MemoryKeyVault(workspace.replica.getHashSuite());
    const session = new ReplSession({ workspace, keyVault });

    try {
        assertEqual(session.hashWidth, 'auto', 'portable default hash width');
        assertEqual(session.hashLabels, false, 'portable default hash labels');
        assertEqual(session.refAutoUpdate, 'off', 'portable default ref auto update');
        assertEqual(promptForSession(session), 'rdb:-:-> ', 'portable prompt');
        assert(formatRows([{ a: 1 }]).includes('a'), 'portable row formatter');

        session.enableReplDefaults();
        assertEqual(session.hashLabels, true, 'portable REPL labels default');
        assertEqual(session.refAutoUpdate, 'auto', 'portable REPL ref update default');
        session.enableScriptDefaults();
        assertEqual(session.hashWidth, 'full', 'portable script hash width default');
        assertEqual(session.refAutoUpdate, 'off', 'portable script ref update default');

        const pending = await runCommand(session, '\\key create alice');
        assertEqual(pending.exitCode, 1, 'missing passphrase should be structured failure');
        assertEqual(pending.output, 'passphrase required', 'missing passphrase message excludes secret');
        assertEqual(pending.needsPassphrase?.kind, 'create', 'create passphrase need kind');
        assertEqual(pending.needsPassphrase?.label, 'alice', 'create passphrase need label');

        const created = await runCommand(session, '\\key create alice', undefined, {
            requestPassphrase: async () => 'correct horse',
        });
        assert(created.exitCode === 0 && created.output.includes('created alice'), 'interactive key creation');
        assert(!created.output.includes('correct horse'), 'passphrase excluded from output');

        const author = await runCommand(session, '\\author alice');
        assertEqual(author.output, 'author alice', 'author selection');

        const setup = await runCommand(session, `
CREATE SCHEMA shop CREATORS ($me) AS (
  TABLE products (
    sku string PUB READONLY,
    name string
  )
);
CREATE TABLEGROUP shop_prod USING SCHEMA shop;
INSERT INTO shop_prod.products (sku, name) VALUES ('A', 'Widget');
SELECT sku, name FROM shop_prod.products;
`);
        assert(setup.exitCode === 0 && setup.output.includes('Widget'), 'portable table rendering');
        assertEqual(promptForSession(session), 'rdb:shop_prod:alice> ', 'canonical prompt labels');

        const progress: string[] = [];
        const streamed = await runCommand(session, 'SELECT sku, name FROM shop_prod.products;', undefined, {
            auth: { onProgress: (line) => progress.push(line) },
        });
        assert(progress.some((line) => line.includes('Widget')), 'streamed main output');
        assertEqual(streamed.output, '', 'streamed main output is not rendered twice');

        const streamedRun = await runLanguageText(session, 'SELECT sku, name FROM shop_prod.products;', {
            onProgress: () => undefined,
        });
        const streamedItem = streamedRun.results[0]!;
        streamedItem.mainStreamed = false;
        streamedItem.notices = ['already streamed notice'];
        streamedItem.noticesStreamed = true;
        const unstreamedOnly = renderStatementOutput(session, streamedItem);
        assert(unstreamedOnly.includes('Widget'), 'unstreamed main output remains visible');
        assert(!unstreamedOnly.includes('already streamed notice'), 'streamed notices are not rendered twice');

        const observerSetup = await runCommand(session, `
CREATE SCHEMA users_schema CREATORS ($me) AS (
  TABLE identities (name string) ALLOW all IF true
);
CREATE TABLEGROUP users USING SCHEMA users_schema;
CREATE SCHEMA observer_schema CREATORS ($me) AS (
  TABLE orders (
    customer string REFERENCES users.identities,
    label string
  ) ALLOW all IF true
);
CREATE TABLEGROUP observer USING SCHEMA observer_schema BIND users => users;
`);
        assertEqual(observerSetup.exitCode, 0, 'observer setup');
        session.setRefAutoUpdate('auto');
        const orderedProgress: string[] = [];
        const observedInsert = await runCommand(
            session,
            "INSERT INTO users.identities (name) VALUES ('Ada');",
            undefined,
            { auth: { onProgress: (line) => orderedProgress.push(line) } },
        );
        assertEqual(observedInsert.exitCode, 0, 'observed insert');
        const insertIndex = orderedProgress.findIndex((line) => line.startsWith('inserted '));
        const refIndex = orderedProgress.findIndex((line) => line.startsWith('updated ref on observer'));
        assert(insertIndex >= 0, 'insert result was streamed');
        assert(refIndex > insertIndex, 'ref update was streamed after its triggering insert');

        await runCommand(session, '\\output json');
        const selected = await runCommand(session, 'SELECT sku, name FROM shop_prod.products;');
        assert(selected.exitCode === 0 && selected.output.includes('"Widget"'), 'portable JSON rendering');

        await runCommand(session, '\\hash-width 12');
        assertEqual(session.hashWidth, 12, 'hash-width meta command');
        const invalid = await runCommand(session, 'SELECT FROM;');
        assert(invalid.exitCode === 2 && invalid.output.length > 0, 'portable diagnostics');

        const quit = await runCommand(session, '\\quit');
        assert(quit.quit === true, 'portable quit outcome');
    } finally {
        await workspace.close();
    }
}

void main();
