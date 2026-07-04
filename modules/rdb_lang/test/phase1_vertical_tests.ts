import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, createIdentity, HASH_SHA256, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";

import { createMockRContext } from "../../rdb/test/mock_rcontext.js";
import { RSchemaImpl, rSchemaFactory, RTableGroupImpl, rTableGroupFactory } from "@hyper-hyper-space/hhs3_rdb";

import { bind, BoundStatement } from "../src/bind/bind.js";
import { execute } from "../src/exec/execute.js";
import { parseStatement } from "../src/syntax/parser.js";
import { createTestBindContext } from "./mock_bind_context.js";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

async function parseBind(sql: string, context: ReturnType<typeof createTestBindContext>): Promise<BoundStatement> {
    const parsed = parseStatement(sql);
    assertTrue(parsed.ok, `parse should succeed: ${sql}`);
    if (!parsed.ok) throw new Error(parsed.diagnostics[0].message);
    const bound = await bind(parsed.value, context);
    assertTrue(bound.ok, `bind should succeed: ${sql}`);
    if (!bound.ok) throw new Error(bound.diagnostics[0].message);
    return bound.value;
}

function isObject(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export const phase1VerticalTests = {
    title: '[RDB_LANG:PHASE1] Vertical execution',
    tests: [
        {
            name: '[PHASE101] CREATE payloads, INSERT, SELECT and LOG compose end to end',
            invoke: async () => {
                const ctx = createMockRContext({ selfValidate: true });
                ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
                ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);

                const admin = await createIdentity(SIGNING_ED25519, hashSuite);
                const lang = createTestBindContext(ctx, { admin, me: admin });

                const schemaBound = await parseBind(`
                    CREATE SCHEMA shop CREATORS ($admin) AS (
                      TABLE products (
                        sku string PUB READONLY,
                        name string,
                        price integer DEFAULT 0
                      ) ALLOW all IF true
                    );
                `, lang);
                const schemaPlan = await execute(schemaBound);
                assertTrue(schemaPlan.ok && schemaPlan.value.kind === 'create-plan', 'schema create returns a plan');
                if (!schemaPlan.ok || schemaPlan.value.kind !== 'create-plan') return;
                const schema = await ctx.createObject(schemaPlan.value.plan.payload) as RSchemaImpl;
                lang.registerSchema('shop', schema);

                const groupBound = await parseBind("CREATE TABLEGROUP shop_prod USING SCHEMA shop;", lang);
                const groupPlan = await execute(groupBound);
                assertTrue(groupPlan.ok && groupPlan.value.kind === 'create-plan', 'group create returns a plan');
                if (!groupPlan.ok || groupPlan.value.kind !== 'create-plan') return;
                const group = await ctx.createObject(groupPlan.value.plan.payload) as RTableGroupImpl;
                lang.registerGroup('shop_prod', group);

                const insertBound = await parseBind("INSERT INTO shop_prod.products (sku, name, price) VALUES ('A', 'Widget', 12);", lang);
                const insert = await execute(insertBound);
                assertTrue(insert.ok && insert.value.kind === 'insert', 'insert executes');
                if (!insert.ok || insert.value.kind !== 'insert') return;

                const selectBound = await parseBind("SELECT name, price FROM shop_prod.products WHERE sku = 'A' ORDER BY price DESC LIMIT 1;", lang);
                const select = await execute(selectBound);
                assertTrue(select.ok && select.value.kind === 'select', 'select executes');
                if (!select.ok || select.value.kind !== 'select') return;
                assertEquals(select.value.rows.length, 1, 'one selected row');
                assertEquals(select.value.rows[0].values['name'], 'Widget', 'selected row value');
                assertEquals(select.value.rows[0].values['price'], 12, 'selected row price');

                const logBound = await parseBind("LOG shop_prod LIMIT 10;", lang);
                const log = await execute(logBound);
                assertTrue(log.ok && log.value.kind === 'log', 'log executes');
                if (!log.ok || log.value.kind !== 'log') return;
                assertTrue(log.value.rows.length >= 2, 'group log includes create and row entries');
                assertTrue(log.value.rows.some((r) => isObject(r.payload) && r.payload['action'] === 'row'), 'group log includes row op');
                const rowEntry = log.value.rows.find((r) => isObject(r.payload) && r.payload['action'] === 'row');
                assertEquals(rowEntry?.void, false, 'live row op is OK');
                const createEntry = log.value.rows.find((r) => isObject(r.payload) && r.payload['action'] === 'create');
                assertEquals(createEntry?.void, undefined, 'create entry has no verdict');
            },
        },
    ],
};
