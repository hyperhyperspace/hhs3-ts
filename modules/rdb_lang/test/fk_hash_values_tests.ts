import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, createIdentity, HASH_SHA256, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";

import { bind } from "../src/bind/bind.js";
import { execute } from "../src/exec/execute.js";
import { parseStatement } from "../src/syntax/parser.js";
import type { TestBindContext } from "./mock_bind_context.js";
import { createLangEnv } from "./lang_env.js";

const crypto = createBasicCrypto();

async function parseBind(sql: string, context: TestBindContext) {
    const parsed = parseStatement(sql);
    assertTrue(parsed.ok, `parse should succeed: ${sql}`);
    if (!parsed.ok) throw new Error(parsed.diagnostics[0].message);
    const bound = await bind(parsed.value, context);
    assertTrue(bound.ok, `bind should succeed: ${sql}`);
    if (!bound.ok) throw new Error(bound.diagnostics[0].message);
    return bound.value;
}

async function expectBindFailure(sql: string, context: TestBindContext, messageIncludes: string) {
    const parsed = parseStatement(sql);
    assertTrue(parsed.ok, `parse should succeed: ${sql}`);
    if (!parsed.ok) throw new Error(parsed.diagnostics[0].message);
    const bound = await bind(parsed.value, context);
    assertTrue(!bound.ok, `bind should fail: ${sql}`);
    if (bound.ok) throw new Error('expected bind failure');
    assertTrue(bound.diagnostics.some((d) => d.message.includes(messageIncludes)),
        `expected diagnostic containing '${messageIncludes}', got: ${bound.diagnostics.map((d) => d.message).join('; ')}`);
}

async function createLocalFkEnv() {
    const admin = await createIdentity(SIGNING_ED25519, crypto.hash(HASH_SHA256));
    const env = await createLangEnv({ vars: { admin, me: admin } });
    await env.run(`
        CREATE SCHEMA local_fk CREATORS ($admin) AS (
          TABLE orders (id string PUB) ALLOW all IF true,
          TABLE lines (orderRef string REFERENCES orders, qty integer) ALLOW all IF true
        );
        CREATE CATALOG local_fk_catalog VERSION '1.0.0' AS (TABLEGROUP local_fk_group USING SCHEMA local_fk);
        CREATE DATABASE local_fk_db USING CATALOG local_fk_catalog;
    `);
    return { ctx: env.ctx, lang: env.lang, group: await env.group('local_fk_group') };
}

async function createCrossGroupFkEnv() {
    const admin = await createIdentity(SIGNING_ED25519, crypto.hash(HASH_SHA256));
    const env = await createLangEnv({ vars: { admin, me: admin } });
    await env.run(`
        CREATE SCHEMA users_schema CREATORS ($admin) AS (
          TABLE identities (name string) ALLOW all IF true
        );
        CREATE SCHEMA app_schema CREATORS ($admin) AS (
          TABLE profiles (
            ownerId string REFERENCES users.identities,
            label string
          ) ALLOW all IF true
        );
        CREATE CATALOG app_catalog VERSION '1.0.0' AS (
          TABLEGROUP users USING SCHEMA users_schema,
          TABLEGROUP app USING SCHEMA app_schema BIND users => users
        );
        CREATE DATABASE app_db USING CATALOG app_catalog;
    `);
    return { ctx: env.ctx, lang: env.lang, usersGroup: await env.group('users'), appGroup: await env.group('app') };
}

export const fkHashValuesTests = {
    title: '[RDB_LANG:FK_HASH] #prefix in INSERT VALUES for REFERENCES columns',
    tests: [
        {
            name: '[FKHASH01] parser accepts #prefix in VALUES',
            invoke: async () => {
                const result = parseStatement("INSERT INTO profiles (ownerId, label) VALUES (#abc, 'Admin');");
                assertTrue(result.ok, 'parse should succeed');
                if (!result.ok || result.value.kind !== 'insert') return;
                assertEquals(result.value.values[0].kind, 'hash', 'first value is a hash ref');
                if (result.value.values[0].kind === 'hash') assertEquals(result.value.values[0].prefix, 'abc', 'hash prefix');
            },
        },
        {
            name: '[FKHASH02] bind resolves #prefix for a local REFERENCES column',
            invoke: async () => {
                const { lang } = await createLocalFkEnv();

                const orderInsert = await execute(await parseBind(
                    "INSERT INTO local_fk_group.orders (id) VALUES ('order-1');",
                    lang,
                ));
                assertTrue(orderInsert.ok && orderInsert.value.kind === 'insert', 'order insert succeeds');
                if (!orderInsert.ok || orderInsert.value.kind !== 'insert') return;

                const prefix = orderInsert.value.rowId.slice(0, 8);
                const lineBound = await parseBind(
                    `INSERT INTO local_fk_group.lines (orderRef, qty) VALUES (#${prefix}, 1);`,
                    lang,
                );
                assertEquals(lineBound.kind, 'insert', 'bound insert');
                if (lineBound.kind !== 'insert') return;
                assertEquals(lineBound.values['orderRef'], orderInsert.value.rowId, 'FK column resolved to full rowId');

                const lineInsert = await execute(lineBound);
                assertTrue(lineInsert.ok && lineInsert.value.kind === 'insert', 'line insert succeeds');
            },
        },
        {
            name: '[FKHASH03] bind resolves #prefix for a cross-group REFERENCES column',
            invoke: async () => {
                const { lang, usersGroup, appGroup } = await createCrossGroupFkEnv();

                const identityInsert = await execute(await parseBind(
                    "INSERT INTO users.identities (name) VALUES ('Ada');",
                    lang,
                ));
                assertTrue(identityInsert.ok && identityInsert.value.kind === 'insert', 'identity insert succeeds');
                if (!identityInsert.ok || identityInsert.value.kind !== 'insert') return;

                const usersFrontier = await (await usersGroup.getScopedDag()).getFrontier();
                await appGroup.observe('users', usersFrontier);

                const prefix = identityInsert.value.rowId.slice(0, 8);
                const profileBound = await parseBind(
                    `INSERT INTO app.profiles (ownerId, label) VALUES (#${prefix}, 'Admin');`,
                    lang,
                );
                assertEquals(profileBound.kind, 'insert', 'bound insert');
                if (profileBound.kind !== 'insert') return;
                assertEquals(profileBound.values['ownerId'], identityInsert.value.rowId, 'cross-group FK resolved');

                const profileInsert = await execute(profileBound);
                assertTrue(profileInsert.ok && profileInsert.value.kind === 'insert', 'profile insert succeeds');
            },
        },
        {
            name: '[FKHASH04] rejects #prefix on a non-FK column',
            invoke: async () => {
                const { lang } = await createLocalFkEnv();
                await expectBindFailure(
                    "INSERT INTO local_fk_group.orders (id) VALUES (#abc);",
                    lang,
                    'not a REFERENCES column',
                );
            },
        },
        {
            name: '[FKHASH05] rejects unknown #prefix in FK column',
            invoke: async () => {
                const { lang } = await createLocalFkEnv();
                await expectBindFailure(
                    "INSERT INTO local_fk_group.lines (orderRef, qty) VALUES (#deadbeef, 1);",
                    lang,
                    "Unknown rowId prefix '#deadbeef'",
                );
            },
        },
    ],
};
