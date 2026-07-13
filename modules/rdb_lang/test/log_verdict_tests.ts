import { assertEquals, assertFalse, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, createIdentity, HASH_SHA256, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { version } from "@hyper-hyper-space/hhs3_mvt";
import type { VersionScope } from "../src/bind/context.js";
import type { VersionExpr } from "../src/syntax/ast.js";

import { createMockRContext } from "../../rdb/test/mock_rcontext.js";
import { deriveRowId, RSchemaImpl, rSchemaFactory, RTableGroupImpl, rTableGroupFactory } from "@hyper-hyper-space/hhs3_rdb";

import { bind, BoundStatement } from "../src/bind/bind.js";
import { execute } from "../src/exec/execute.js";
import { parseStatement } from "../src/syntax/parser.js";
import { createTestBindContext } from "./mock_bind_context.js";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

async function collectEntryHashes(group: RTableGroupImpl): Promise<B64Hash[]> {
    const hashes: B64Hash[] = [];
    for await (const entry of (await group.getScopedDag()).loadAllEntries()) hashes.push(entry.hash);
    return hashes;
}

function resolveHashPrefix(prefix: string, hashes: B64Hash[]): B64Hash {
    const matches = hashes.filter((h) => h.startsWith(prefix));
    if (matches.length === 1) return matches[0];
    if (matches.length === 0) throw new Error(`Unknown hash prefix '#${prefix}'`);
    throw new Error(`Ambiguous hash prefix '#${prefix}'`);
}

function installEntryHashVersionResolver(
    lang: ReturnType<typeof createTestBindContext>,
    entryHashes: B64Hash[],
): void {
    const base = lang.resolveVersion.bind(lang);
    lang.resolveVersion = async (expr: VersionExpr | undefined, scope: VersionScope) => {
        if (expr?.kind === 'set') {
            return version(...expr.members.map((m) => m.kind === 'hash'
                ? resolveHashPrefix(m.prefix, entryHashes)
                : (() => { throw new Error(`Unknown version alias '${m.text}'`); })()));
        }
        if (expr?.kind === 'hash') return version(resolveHashPrefix(expr.hash.prefix, entryHashes));
        return base(expr, scope);
    };
}

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
                const ctx = createMockRContext({ selfValidate: true });
                ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
                ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);
                const lang = createTestBindContext(ctx);

                const parsed = parseStatement('LOG shop_prod FROM LATEST;');
                assertTrue(parsed.ok, 'parse should succeed');
                if (!parsed.ok) return;
                const bound = await bind(parsed.value, lang);
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
                const ctx = createMockRContext({ selfValidate: true });
                ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
                ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);

                const admin = await createIdentity(SIGNING_ED25519, hashSuite);
                const lang = createTestBindContext(ctx, { admin, me: admin });

                const schemaBound = await parseBind(`
                    CREATE SCHEMA shop CREATORS ($admin) AS (
                      TABLE caps (
                        label string PUB
                      ) ALLOW all IF true,
                      TABLE items (
                        name string
                      ) ALLOW insert IF EXISTS caps WHERE label = 'grant'
                    );
                `, lang);
                const schemaPlan = await execute(schemaBound);
                assertTrue(schemaPlan.ok && schemaPlan.value.kind === 'create-plan', 'schema create');
                if (!schemaPlan.ok || schemaPlan.value.kind !== 'create-plan') return;
                const schema = await ctx.createObject(schemaPlan.value.plan.payload) as RSchemaImpl;
                lang.registerSchema('shop', schema);

                const groupBound = await parseBind('CREATE TABLEGROUP shop_prod USING SCHEMA shop;', lang);
                const groupPlan = await execute(groupBound);
                assertTrue(groupPlan.ok && groupPlan.value.kind === 'create-plan', 'group create');
                if (!groupPlan.ok || groupPlan.value.kind !== 'create-plan') return;
                const group = await ctx.createObject(groupPlan.value.plan.payload) as RTableGroupImpl;
                lang.registerGroup('shop_prod', group);

                const caps = await group.getTable('caps');
                const items = await group.getTable('items');
                const capId = deriveRowId('c-1');
                await caps.insert('c-1', { label: 'grant' });
                const base = await (await group.getScopedDag()).getFrontier();
                await caps.delete(capId, undefined, base);
                await items.insert('i-1', { name: 'thing' }, undefined, base);

                const logBound = await parseBind('LOG shop_prod LIMIT 20;', lang);
                const log = await execute(logBound);
                assertTrue(log.ok && log.value.kind === 'log', 'log executes');
                if (!log.ok || log.value.kind !== 'log') return;

                const createRow = log.value.rows.find((r) => isObject(r.payload) && r.payload['action'] === 'create');
                assertTrue(createRow !== undefined, 'log includes create entry');
                assertEquals(createRow?.void, undefined, 'create entry has no verdict');

                const insertRow = log.value.rows.find((r) => {
                    if (!isObject(r.payload) || r.payload['action'] !== 'row' || r.payload['table'] !== 'items') return false;
                    const op = r.payload['op'];
                    return isObject(op) && op['action'] === 'insert';
                });
                assertTrue(insertRow !== undefined, 'log includes insert entry');
                assertEquals(insertRow?.void, true, 'concurrent revoke voids insert');
            },
        },
        {
            name: '[LOG03b] table log annotates void verdict on row ops',
            invoke: async () => {
                const ctx = createMockRContext({ selfValidate: true });
                ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
                ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);

                const admin = await createIdentity(SIGNING_ED25519, hashSuite);
                const lang = createTestBindContext(ctx, { admin, me: admin });

                const schemaBound = await parseBind(`
                    CREATE SCHEMA shop CREATORS ($admin) AS (
                      TABLE caps (
                        label string PUB
                      ) ALLOW all IF true,
                      TABLE items (
                        name string
                      ) ALLOW insert IF EXISTS caps WHERE label = 'grant'
                    );
                `, lang);
                const schemaPlan = await execute(schemaBound);
                assertTrue(schemaPlan.ok && schemaPlan.value.kind === 'create-plan', 'schema create');
                if (!schemaPlan.ok || schemaPlan.value.kind !== 'create-plan') return;
                const schema = await ctx.createObject(schemaPlan.value.plan.payload) as RSchemaImpl;
                lang.registerSchema('shop', schema);

                const groupBound = await parseBind('CREATE TABLEGROUP shop_prod USING SCHEMA shop;', lang);
                const groupPlan = await execute(groupBound);
                assertTrue(groupPlan.ok && groupPlan.value.kind === 'create-plan', 'group create');
                if (!groupPlan.ok || groupPlan.value.kind !== 'create-plan') return;
                const group = await ctx.createObject(groupPlan.value.plan.payload) as RTableGroupImpl;
                lang.registerGroup('shop_prod', group);

                const caps = await group.getTable('caps');
                const items = await group.getTable('items');
                const capId = deriveRowId('c-1');
                await caps.insert('c-1', { label: 'grant' });
                const base = await (await group.getScopedDag()).getFrontier();
                await caps.delete(capId, undefined, base);
                await items.insert('i-1', { name: 'thing' }, undefined, base);

                const logBound = await parseBind('LOG shop_prod.items LIMIT 20;', lang);
                const log = await execute(logBound);
                assertTrue(log.ok && log.value.kind === 'log', 'table log executes');
                if (!log.ok || log.value.kind !== 'log') return;

                const insertRow = log.value.rows.find((r) => {
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
                const ctx = createMockRContext({ selfValidate: true });
                ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
                ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);

                const admin = await createIdentity(SIGNING_ED25519, hashSuite);
                const lang = createTestBindContext(ctx, { admin, me: admin });

                const schemaBound = await parseBind(`
                    CREATE SCHEMA shop CREATORS ($admin) AS (
                      TABLE caps (
                        label string PUB
                      ) ALLOW all IF true,
                      TABLE items (
                        name string
                      ) ALLOW insert IF EXISTS caps WHERE label = 'grant'
                    );
                `, lang);
                const schemaPlan = await execute(schemaBound);
                assertTrue(schemaPlan.ok && schemaPlan.value.kind === 'create-plan', 'schema create');
                if (!schemaPlan.ok || schemaPlan.value.kind !== 'create-plan') return;
                const schema = await ctx.createObject(schemaPlan.value.plan.payload) as RSchemaImpl;
                lang.registerSchema('shop', schema);

                const groupBound = await parseBind('CREATE TABLEGROUP shop_prod USING SCHEMA shop;', lang);
                const groupPlan = await execute(groupBound);
                assertTrue(groupPlan.ok && groupPlan.value.kind === 'create-plan', 'group create');
                if (!groupPlan.ok || groupPlan.value.kind !== 'create-plan') return;
                const group = await ctx.createObject(groupPlan.value.plan.payload) as RTableGroupImpl;
                lang.registerGroup('shop_prod', group);

                const caps = await group.getTable('caps');
                const items = await group.getTable('items');
                const capId = deriveRowId('c-1');
                await caps.insert('c-1', { label: 'grant' });
                const base = await (await group.getScopedDag()).getFrontier();
                await caps.delete(capId, undefined, base);
                await items.insert('i-1', { name: 'thing' }, undefined, base);

                const logBound = await parseBind('EXPLAIN LOG shop_prod LIMIT 20;', lang);
                const log = await execute(logBound);
                assertTrue(log.ok && log.value.kind === 'log', 'explain log executes');
                if (!log.ok || log.value.kind !== 'log') return;
                assertTrue(log.value.explain, 'explain flag on result');

                const insertRow = log.value.rows.find((r) => {
                    if (!isObject(r.payload) || r.payload['action'] !== 'row' || r.payload['table'] !== 'items') return false;
                    const op = r.payload['op'];
                    return isObject(op) && op['action'] === 'insert';
                });
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
                const ctx = createMockRContext({ selfValidate: true });
                ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
                ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);

                const admin = await createIdentity(SIGNING_ED25519, hashSuite);
                const lang = createTestBindContext(ctx, { admin, me: admin });

                const schemaBound = await parseBind(`
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
                `, lang);
                const schemaPlan = await execute(schemaBound);
                assertTrue(schemaPlan.ok && schemaPlan.value.kind === 'create-plan', 'schema create');
                if (!schemaPlan.ok || schemaPlan.value.kind !== 'create-plan') return;
                const schema = await ctx.createObject(schemaPlan.value.plan.payload) as RSchemaImpl;
                lang.registerSchema('users_schema', schema);

                const groupBound = await parseBind(`
                    CREATE TABLEGROUP users
                      USING SCHEMA users_schema
                      USING IDENTITIES identities
                      WITH ROWS (
                        identities (keyId = $admin, publicKey = publicKey($admin), name = 'Admin'),
                        caps (label = 'manager', grantee = $admin)
                      );
                `, lang);
                const groupPlan = await execute(groupBound);
                assertTrue(groupPlan.ok && groupPlan.value.kind === 'create-plan', 'group create');
                if (!groupPlan.ok || groupPlan.value.kind !== 'create-plan') return;
                const group = await ctx.createObject(groupPlan.value.plan.payload) as RTableGroupImpl;
                lang.registerGroup('users', group);

                const pickaxerInsert = await execute(await parseBind(
                    "INSERT INTO users.caps (label, grantee) VALUES ('pickaxer', $admin) BY $admin;",
                    lang,
                ));
                assertTrue(pickaxerInsert.ok && pickaxerInsert.value.kind === 'insert', 'pickaxer cap insert');
                if (!pickaxerInsert.ok || pickaxerInsert.value.kind !== 'insert') return;

                const alter = await execute(await parseBind(
                    'ALTER SCHEMA users_schema AS (ADD COLUMN caps.reason string NULL);',
                    lang,
                ));
                assertTrue(alter.ok && alter.value.kind === 'alter-schema', 'alter add reason');

                const deploy = await execute(await parseBind(
                    'UPDATE SCHEMA users_schema TO LATEST ON users;',
                    lang,
                ));
                assertTrue(deploy.ok && deploy.value.kind === 'update-schema', 'deploy reason column');

                const reasonUpdate = await execute(await parseBind(
                    `UPDATE users.caps SET reason = 'assigned' WHERE rowId = #${pickaxerInsert.value.rowId.slice(0, 8)} BY $admin;`,
                    lang,
                ));
                assertTrue(reasonUpdate.ok && reasonUpdate.value.kind === 'update', 'reason update');
                if (!reasonUpdate.ok || reasonUpdate.value.kind !== 'update') return;
                const updateEntryHash = reasonUpdate.value.entryHash;

                installEntryHashVersionResolver(lang, await collectEntryHashes(group));

                const horizon = updateEntryHash.slice(0, 8);
                const log = await execute(await parseBind(
                    `EXPLAIN LOG users AT {#${horizon}} FROM {#${horizon}} LIMIT 50;`,
                    lang,
                ));
                assertTrue(log.ok && log.value.kind === 'log', 'explain log executes');
                if (!log.ok || log.value.kind !== 'log') return;
                assertTrue(log.value.explain, 'explain flag on result');

                const pickaxerRow = log.value.rows.find((r) => {
                    if (!isObject(r.payload) || r.payload['action'] !== 'row' || r.payload['table'] !== 'caps') return false;
                    const op = r.payload['op'];
                    return isObject(op) && op['action'] === 'insert'
                        && isObject(op['values']) && op['values']['label'] === 'pickaxer';
                });
                assertTrue(pickaxerRow !== undefined, 'log includes pickaxer insert');
                assertEquals(pickaxerRow?.void, false, 'pickaxer insert is not void at update horizon');

                const updateRow = log.value.rows.find((r) => r.hash === updateEntryHash);
                assertTrue(updateRow !== undefined, 'log includes reason update');
                assertEquals(updateRow?.void, false, 'reason update is not void at update horizon');

                for (const row of log.value.rows) {
                    if (row.void === true) {
                        assertTrue(
                            row.reason !== undefined && row.reason.length > 0,
                            'voided row has non-empty reason',
                        );
                    }
                }
            },
        },
    ],
};
