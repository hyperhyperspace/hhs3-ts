import "./styles.css";

import editorSchemaSql from "../../rdb/examples/editor_web.sql?raw";
import { mountRepl } from "./app.js";
import { DirectReplClient } from "./direct_client.js";
import type { ReplInteractions } from "./protocol.js";

const client = new DirectReplClient();

if (new URLSearchParams(location.search).has('smoke')) {
    void runSmoke(client);
} else {
    void mountRepl(client, [{ id: 'editor', sql: editorSchemaSql }]);
}

async function runSmoke(activeClient: DirectReplClient): Promise<void> {
    const output: string[] = [];
    const interactions: ReplInteractions = {
        requestPassphrase: async () => 'browser-smoke',
        requestConfirmation: async () => true,
        onProgress: (line) => output.push(line),
    };

    try {
        await activeClient.start();
        assertSuccess(await activeClient.execute('\\key create alice', interactions), 'create key');
        assertSuccess(await activeClient.execute('\\author alice', interactions), 'select author');
        const setup = await activeClient.execute(`
CREATE SCHEMA shop CREATORS ($me) AS (
  TABLE products (
    sku string PUB READONLY,
    name string
  )
);
CREATE TABLEGROUP shop_prod USING SCHEMA shop;
INSERT INTO shop_prod.products (sku, name) VALUES ('A', 'Widget');
SELECT sku, name FROM shop_prod.products;
`, interactions);
        assertSuccess(setup, 'setup and select');
        if (!`${output.join('\n')}\n${setup.output}`.includes('Widget')) {
            throw new Error('select output did not contain Widget');
        }

        assertSuccess(await activeClient.execute('\\output json', interactions), 'JSON mode');
        const json = await activeClient.execute('SELECT sku, name FROM shop_prod.products;', interactions);
        assertSuccess(json, 'JSON select');
        if (!json.output.includes('Widget')) throw new Error('JSON output did not contain Widget');

        const invalid = await activeClient.execute('SELECT FROM;', interactions);
        if (invalid.exitCode !== 2) throw new Error(`expected diagnostic exit code 2, received ${invalid.exitCode}`);

        await activeClient.reset();
        const keysAfterReset = await activeClient.execute('\\keys', interactions);
        assertSuccess(keysAfterReset, 'keys after reset');
        if (keysAfterReset.output.includes('alice')) throw new Error('ephemeral key survived reset');

        if (await activeClient.hasKey('admin')) throw new Error('admin key existed before schema load');
        assertSuccess(await activeClient.execute('\\key create admin', interactions), 'create admin key');
        if (!await activeClient.hasKey('admin')) throw new Error('admin key was not created');
        assertSuccess(await activeClient.execute('\\author admin', interactions), 'select admin author');
        assertSuccess(await activeClient.execute(editorSchemaSql, interactions), 'load editor schema');

        document.body.dataset.smoke = 'passed';
        document.body.replaceChildren(resultNode('Browser smoke passed'));
    } catch (error) {
        document.body.dataset.smoke = 'failed';
        document.body.replaceChildren(resultNode(`Browser smoke failed: ${errorMessage(error)}`));
        console.error(error);
    } finally {
        await activeClient.close();
    }
}

function assertSuccess(result: { exitCode: number; output: string }, operation: string): void {
    if (result.exitCode !== 0) {
        throw new Error(`${operation} failed (${result.exitCode}): ${result.output}`);
    }
}

function resultNode(text: string): HTMLPreElement {
    const pre = document.createElement('pre');
    pre.id = 'smoke-result';
    pre.textContent = text;
    return pre;
}

function errorMessage(error: unknown): string {
    return error instanceof Error ? error.message : String(error);
}
