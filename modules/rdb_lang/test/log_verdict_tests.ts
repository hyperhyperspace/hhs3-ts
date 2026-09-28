import { assertEquals, assertFalse, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { deriveRowId } from "@hyper-hyper-space/hhs3_rdb";

import { bind } from "../src/bind/bind.js";
import { deployLabelsFor } from "../src/exec/history.js";
import { renderLogOpLine } from "../src/reverse/log_line.js";
import { parseStatement } from "../src/syntax/parser.js";
import type { LogLangResult } from "../src/exec/result.js";
import { createLangEnv, LangEnv, newIdentity } from "./lang_env.js";

function isObject(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}

// A shop database whose `items` inserts are gated on a `grant` cap; a
// concurrent revoke voids an insert made at the same base.
async function voidedInsertEnv(): Promise<LangEnv> {
    const admin = await newIdentity();
    const env = await createLangEnv({ vars: { admin, me: admin } });
    await env.run(`
        CREATE SCHEMA shop CREATORS ($admin) AS (
          TABLE caps (
            label string PUB
          ) ALLOW all IF true,
          TABLE items (
            name string
          ) ALLOW insert IF EXISTS caps WHERE label = 'grant'
        );
        CREATE CATALOG shop_catalog VERSION '1.0.0' AS (TABLEGROUP shop_prod USING SCHEMA shop);
        CREATE DATABASE shop_db USING CATALOG shop_catalog;
    `);
    const group = await env.group('shop_prod');
    const caps = await group.getTable('caps');
    const items = await group.getTable('items');
    await caps.insert('c-1', { label: 'grant' });
    const base = await (await group.getScopedDag()).getFrontier();
    await caps.delete(deriveRowId('c-1'), undefined, base);
    await items.insert('i-1', { name: 'thing' }, undefined, base);
    return env;
}

async function runLog(env: LangEnv, sql: string): Promise<LogLangResult> {
    const [result] = await env.run(sql);
    if (result.kind !== 'log') throw new Error(`expected a log result, got ${result.kind}`);
    return result;
}

function itemsInsert(log: LogLangResult) {
    return log.rows.find((r) => {
        if (!isObject(r.payload) || r.payload['action'] !== 'row' || r.payload['table'] !== 'items') return false;
        const op = r.payload['op'];
        return isObject(op) && op['action'] === 'insert';
    });
}

function opLines(log: LogLangResult): string[] {
    return log.rows.map((row) => renderLogOpLine(row.payload, row.prev, log.renderContext));
}

export const logVerdictTests = {
    title: '[RDB_LANG:LOG] Op verdict',
    tests: [
        {
            name: '[LOG01] parses LOG AT and FROM',
            invoke: async () => {
                const result = parseStatement('LOG shop_prod AT {#abc} FROM #def LIMIT 5;');
                assertTrue(result.ok, 'parse should succeed');
                if (!result.ok || result.value.kind !== 'log') return;
                assertEquals(result.value.at?.kind, 'set', 'AT version');
                assertEquals(result.value.from?.kind, 'hash', 'FROM version');
            },
        },
        {
            name: '[LOG02] LOG FROM without AT fails bind',
            invoke: async () => {
                const env = await createLangEnv();
                const parsed = parseStatement('LOG shop_prod FROM LATEST;');
                assertTrue(parsed.ok, 'parse should succeed');
                if (!parsed.ok) return;
                const bound = await bind(parsed.value, env.lang);
                assertFalse(bound.ok, 'bind should fail');
                if (bound.ok) return;
                assertTrue(
                    bound.diagnostics.some((d) => d.message.includes('LOG FROM version requires AT')),
                    'bind error mentions FROM requires AT',
                );
            },
        },
        {
            name: '[LOG03] group log annotates void verdict on row ops',
            invoke: async () => {
                const env = await voidedInsertEnv();
                const log = await runLog(env, 'LOG shop_prod LIMIT 20;');

                const createRow = log.rows.find((r) => isObject(r.payload) && r.payload['action'] === 'create');
                assertTrue(createRow !== undefined, 'log includes create entry');
                assertEquals(createRow?.void, undefined, 'create entry has no verdict');

                const insertRow = itemsInsert(log);
                assertTrue(insertRow !== undefined, 'log includes insert entry');
                assertEquals(insertRow?.void, true, 'concurrent revoke voids insert');
            },
        },
        {
            name: '[LOG03b] table log annotates void verdict on row ops',
            invoke: async () => {
                const env = await voidedInsertEnv();
                const log = await runLog(env, 'LOG shop_prod.items LIMIT 20;');

                const insertRow = log.rows.find((r) => {
                    if (!isObject(r.payload) || r.payload['action'] !== 'insert') return false;
                    return isObject(r.payload['values']) && r.payload['values']['name'] === 'thing';
                });
                assertTrue(insertRow !== undefined, 'table log includes insert entry');
                assertEquals(insertRow?.void, true, 'concurrent revoke voids insert in table log');
            },
        },
        {
            name: '[LOG04] parses EXPLAIN LOG',
            invoke: async () => {
                const result = parseStatement('EXPLAIN LOG shop_prod LIMIT 5;');
                assertTrue(result.ok, 'parse should succeed');
                if (!result.ok || result.value.kind !== 'log') return;
                assertEquals(result.value.explain, true, 'explain flag');
            },
        },
        {
            name: '[LOG05] EXPLAIN LOG annotates void reason on cancelled ops',
            invoke: async () => {
                const env = await voidedInsertEnv();
                const log = await runLog(env, 'EXPLAIN LOG shop_prod LIMIT 20;');
                assertTrue(log.explain, 'explain flag on result');

                const insertRow = itemsInsert(log);
                assertTrue(insertRow !== undefined, 'log includes insert entry');
                assertEquals(insertRow?.void, true, 'concurrent revoke voids insert');
                assertTrue(
                    insertRow?.reason !== undefined && insertRow.reason.includes('items'),
                    'cancelled insert has restriction reason',
                );
            },
        },
        {
            name: '[LOG06] EXPLAIN LOG void verdicts stay consistent across concurrent-looking row ops',
            invoke: async () => {
                const admin = await newIdentity();
                const env = await createLangEnv({ vars: { admin, me: admin } });
                await env.run(`
                    CREATE SCHEMA users_schema CREATORS ($admin) AS (
                      TABLE identities (
                        keyId string PUB READONLY,
                        publicKey string PUB READONLY,
                        name string NULL PUB
                      ) IDENTITY PROVIDER
                        ALLOW insert IF true,
                      TABLE caps (
                        label string PUB READONLY,
                        grantee string PUB READONLY
                      ) CONCURRENT DELETES
                        ALLOW insert IF EXISTS caps AS c WHERE c.label = 'manager' AND c.grantee = $author
                        ALLOW delete IF grantee = $author OR EXISTS caps AS c WHERE c.label = 'manager' AND c.grantee = $author
                    );
                    CREATE CATALOG users_catalog VERSION '1.0.0' AS (
                      TABLEGROUP users USING SCHEMA users_schema
                        USING IDENTITIES identities
                        WITH ROWS (
                          identities (keyId = $admin, publicKey = publicKey($admin), name = 'Admin'),
                          caps (label = 'manager', grantee = $admin)
                        )
                    );
                    CREATE DATABASE users_db USING CATALOG users_catalog CREATORS ($admin);
                `);

                const [pickaxerInsert] = await env.run("INSERT INTO users.caps (label, grantee) VALUES ('pickaxer', $admin) BY $admin;");
                if (pickaxerInsert.kind !== 'insert') throw new Error('pickaxer cap insert');

                await env.run(`
                    ALTER SCHEMA users_schema AS (ADD COLUMN caps.reason string NULL);
                    ALTER CATALOG users_catalog VERSION '1.1.0' AS (UPDATE SCHEMA users_schema TO LATEST ON users);
                `);
                const [deploy] = await env.run("UPDATE CATALOG users_catalog TO '1.1.0' ON users_db BY $admin;");
                assertTrue(deploy.kind === 'update-catalog' && deploy.update.deployed.length === 1, 'the reason column is deployed');

                const [reasonUpdate] = await env.run(
                    `UPDATE users.caps SET reason = 'assigned' WHERE rowId = #${pickaxerInsert.rowId.slice(0, 8)} BY $admin;`);
                if (reasonUpdate.kind !== 'update') throw new Error('reason update');
                const updateEntryHash = reasonUpdate.entryHash;

                const horizon = updateEntryHash.slice(0, 8);
                const log = await runLog(env, `EXPLAIN LOG users AT {#${horizon}} FROM {#${horizon}} LIMIT 50;`);
                assertTrue(log.explain, 'explain flag on result');

                const pickaxerRow = log.rows.find((r) => {
                    if (!isObject(r.payload) || r.payload['action'] !== 'row' || r.payload['table'] !== 'caps') return false;
                    const op = r.payload['op'];
                    return isObject(op) && op['action'] === 'insert'
                        && isObject(op['values']) && op['values']['label'] === 'pickaxer';
                });
                assertTrue(pickaxerRow !== undefined, 'log includes pickaxer insert');
                assertEquals(pickaxerRow?.void, false, 'pickaxer insert is not void at update horizon');

                const updateRow = log.rows.find((r) => r.hash === updateEntryHash);
                assertTrue(updateRow !== undefined, 'log includes reason update');
                assertEquals(updateRow?.void, false, 'reason update is not void at update horizon');

                for (const row of log.rows) {
                    if (row.void === true) {
                        assertTrue(row.reason !== undefined && row.reason.length > 0, 'voided row has non-empty reason');
                    }
                }
            },
        },
        {
            name: '[LOG07] deploys, declares and group genesis render as comments; catalogs and databases as statements',
            invoke: async () => {
                const dev = await newIdentity();
                const env = await createLangEnv({ vars: { dev, me: dev } });
                await env.run(`
                    CREATE SCHEMA shop CREATORS ($dev) AS (TABLE products (sku string) ALLOW all IF true);
                    CREATE SCHEMA notes CREATORS ($dev) AS (TABLE notes (text string) ALLOW all IF true);
                    CREATE CATALOG shop_catalog VERSION '1.0.0' AS (TABLEGROUP shop_prod USING SCHEMA shop);
                    CREATE DATABASE shop_db USING CATALOG shop_catalog;
                    ALTER SCHEMA shop AS (ADD COLUMN products.price integer DEFAULT 0);
                    ALTER CATALOG shop_catalog VERSION '1.1.0' AS (
                      UPDATE SCHEMA shop TO LATEST ON shop_prod,
                      ADD TABLEGROUP notes USING SCHEMA notes
                    );
                    UPDATE CATALOG shop_catalog TO LATEST ON shop_db;
                `);
                const db = await env.database('shop_db');
                env.lang.resolveDeployLabels = (groupId) => deployLabelsFor(db, groupId);

                const group = opLines(await runLog(env, 'LOG shop_prod;'));
                assertTrue(group[0]!.startsWith('-- TABLEGROUP shop_prod USING SCHEMA'), `group genesis: ${group[0]}`);
                assertTrue(group.some((l) => l.startsWith('-- deploy shop TO {') && l.endsWith('(shop_catalog 1.1.0)')),
                    `the deploy names its release: ${group.join(' | ')}`);

                const catalog = opLines(await runLog(env, 'LOG shop_catalog;'));
                assertTrue(catalog[0]!.startsWith('CREATE CATALOG shop_catalog'), catalog[0]!);
                assertTrue(catalog.some((l) => l.startsWith('-- declare schemas {')), 'the implied declare is a comment');
                assertTrue(catalog.some((l) => l.startsWith("ALTER CATALOG #") && l.includes("VERSION '1.1.0'")), 'the release is an ALTER CATALOG');

                const database = opLines(await runLog(env, 'LOG shop_db;'));
                assertTrue(database[0]!.startsWith('CREATE DATABASE shop_db USING CATALOG'), database[0]!);
                assertTrue(database[1]!.startsWith('UPDATE CATALOG') && database[1]!.includes('ON shop_db'), database[1]!);
            },
        },
    ],
};
