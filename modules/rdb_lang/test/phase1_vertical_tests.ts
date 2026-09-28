import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import { createLangEnv, newIdentity } from "./lang_env.js";

function isObject(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export const phase1VerticalTests = {
    title: '[RDB_LANG:PHASE1] Vertical execution',
    tests: [
        {
            name: '[PHASE101] CREATE payloads, INSERT, SELECT and LOG compose end to end',
            invoke: async () => {
                const admin = await newIdentity();
                const env = await createLangEnv({ vars: { admin, me: admin } });

                await env.run(`
                    CREATE SCHEMA shop CREATORS ($admin) AS (
                      TABLE products (
                        sku string PUB READONLY,
                        name string,
                        price integer DEFAULT 0
                      ) ALLOW all IF true
                    );
                    CREATE CATALOG shop_catalog VERSION '1.0.0' AS (
                      TABLEGROUP shop_prod USING SCHEMA shop
                    );
                    CREATE DATABASE shop_db USING CATALOG shop_catalog;
                `);

                const [insert] = await env.run("INSERT INTO shop_prod.products (sku, name, price) VALUES ('A', 'Widget', 12);");
                assertTrue(insert.kind === 'insert', 'insert executes');

                const [select] = await env.run("SELECT name, price FROM shop_prod.products WHERE sku = 'A' ORDER BY price DESC LIMIT 1;");
                assertTrue(select.kind === 'select', 'select executes');
                if (select.kind !== 'select') return;
                assertEquals(select.rows.length, 1, 'one selected row');
                assertEquals(select.rows[0].values['name'], 'Widget', 'selected row value');
                assertEquals(select.rows[0].values['price'], 12, 'selected row price');

                const [log] = await env.run("LOG shop_prod LIMIT 10;");
                assertTrue(log.kind === 'log', 'log executes');
                if (log.kind !== 'log') return;
                assertTrue(log.rows.length >= 2, 'group log includes create and row entries');
                assertTrue(log.rows.some((r) => isObject(r.payload) && r.payload['action'] === 'row'), 'group log includes row op');
                const rowEntry = log.rows.find((r) => isObject(r.payload) && r.payload['action'] === 'row');
                assertEquals(rowEntry?.void, false, 'live row op is OK');
                const createEntry = log.rows.find((r) => isObject(r.payload) && r.payload['action'] === 'create');
                assertEquals(createEntry?.void, undefined, 'create entry has no verdict');
            },
        },
    ],
};
