import { json } from "@hyper-hyper-space/hhs3_json";
import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { B64Hash, createBasicCrypto, createIdentity, HASH_SHA256, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import { serializePublicKeyToBase64 } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "../../rdb/test/mock_rcontext.js";
import {
    CreateTableGroupPayload, RDbImpl, rDbFactory, RSchemaImpl, rSchemaFactory, RTableGroupImpl, rTableGroupFactory,
    SchemaUpdatePayload,
} from "@hyper-hyper-space/hhs3_rdb";

import { bind, BoundStatement } from "../src/bind/bind.js";
import { execute } from "../src/exec/execute.js";
import { parseStatement } from "../src/syntax/parser.js";
import { renderCreateSchema, renderCreateTableGroup, renderRowOp, renderSchemaUpdate } from "../src/reverse/render.js";
import { dumpGroup } from "../src/reverse/dump.js";
import { createTestBindContext, TestBindContext } from "./mock_bind_context.js";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

async function parseBind(sql: string, context: TestBindContext): Promise<BoundStatement> {
    const parsed = parseStatement(sql);
    assertTrue(parsed.ok, `parse should succeed: ${sql}`);
    if (!parsed.ok) throw new Error(parsed.diagnostics[0].message);
    const bound = await bind(parsed.value, context);
    assertTrue(bound.ok, `bind should succeed: ${sql}`);
    if (!bound.ok) throw new Error(bound.diagnostics[0].message);
    return bound.value;
}

async function createEnv() {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RDbImpl.typeId, rDbFactory);
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);

    const admin = await createIdentity(SIGNING_ED25519, hashSuite);
    const lang = createTestBindContext(ctx, { admin, me: admin });

    const usersSchemaInit = await RSchemaImpl.create({
        name: 'test:users_schema',
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        tables: [{
            name: 'caps',
            columns: { label: { type: 'string', pub: true } },
            restrictions: [{ on: 'all', rule: { p: 'true' } }],
        }],
    });
    const usersSchema = await ctx.createObject(usersSchemaInit) as RSchemaImpl;
    const usersGroupInit = await RTableGroupImpl.create({
        seed: 'users-group',
        schemaRef: usersSchema.getId(),
        schemaVersion: await (await usersSchema.getScopedDag()).getFrontier(),
    });
    const usersGroup = await ctx.createObject(usersGroupInit) as RTableGroupImpl;
    lang.registerSchema('users_schema', usersSchema);
    lang.registerGroup('users', usersGroup);

    const schemaBound = await parseBind(`
        CREATE SCHEMA shop CREATORS ($admin) AS (
          TABLE products (
            sku string PUB READONLY,
            name string
          ) ALLOW all IF true
        );
    `, lang);
    const schemaPlan = await execute(schemaBound);
    if (!schemaPlan.ok || schemaPlan.value.kind !== 'create-plan') throw new Error('schema create failed');
    const schema = await ctx.createObject(schemaPlan.value.plan.payload) as RSchemaImpl;
    lang.registerSchema('shop', schema);

    const groupBound = await parseBind(`CREATE TABLEGROUP shop_prod USING SCHEMA shop BIND users => #${usersGroup.getId()};`, lang);
    const groupPlan = await execute(groupBound);
    if (!groupPlan.ok || groupPlan.value.kind !== 'create-plan') throw new Error('group create failed');
    const group = await ctx.createObject(groupPlan.value.plan.payload) as RTableGroupImpl;
    lang.registerGroup('shop_prod', group);

    return { ctx, lang, schema, group };
}

export const restPhaseTests = {
    title: '[RDB_LANG:REST] Remaining plan phases',
    tests: [
        {
            name: '[REST01] CREATE DATABASE returns a valid create plan',
            invoke: async () => {
                const { ctx, lang } = await createEnv();
                const bound = await parseBind('CREATE DATABASE app;', lang);
                const result = await execute(bound);
                assertTrue(result.ok && result.value.kind === 'create-plan', 'database create returns a plan');
                if (!result.ok || result.value.kind !== 'create-plan') return;
                const db = await ctx.createObject(result.value.plan.payload) as RDbImpl;
                lang.registerDatabase('app', db);
                assertTrue(db.getId().length > 0, 'created db has id');
            },
        },
        {
            name: '[REST02] ALTER, DEPLOY, UPDATE REF, UPDATE, DELETE and BUNDLE execute',
            invoke: async () => {
                const { lang, group } = await createEnv();

                const insert = await execute(await parseBind("INSERT INTO shop_prod.products (sku, name) VALUES ('A', 'Widget');", lang));
                assertTrue(insert.ok && insert.value.kind === 'insert', 'insert succeeds');
                if (!insert.ok || insert.value.kind !== 'insert') return;

                lang.resolveRowId = async (ref, table, at, from) => {
                    const ids = await (await table.table.getView(at, from ?? at)).liveRowIds();
                    const matches = ids.filter((id: B64Hash) => id.startsWith(ref.prefix));
                    if (matches.length !== 1) throw new Error(`rowId prefix did not resolve uniquely: ${ref.prefix}`);
                    return matches[0];
                };

                const update = await execute(await parseBind(`UPDATE shop_prod.products SET name = 'Widget 2' WHERE rowId = #${insert.value.rowId.slice(0, 8)};`, lang));
                assertTrue(update.ok && update.value.kind === 'update', 'update succeeds');

                const bundle = await execute(await parseBind(`BUNDLE ON shop_prod (
                    UPDATE products SET name = 'Widget 3' WHERE rowId = #${insert.value.rowId.slice(0, 10)};
                    INSERT INTO products (sku, name) VALUES ('B', 'Gadget');
                );`, lang));
                assertTrue(bundle.ok && bundle.value.kind === 'bundle', 'bundle succeeds');

                const alter = await execute(await parseBind("ALTER SCHEMA shop AS (ADD COLUMN products.price integer DEFAULT 0);", lang));
                assertTrue(alter.ok && alter.value.kind === 'alter-schema', 'alter succeeds');

                const allowAlter = await execute(await parseBind("ALTER SCHEMA shop AS (SET ALLOW RULES products (ALLOW all IF true));", lang));
                assertTrue(allowAlter.ok && allowAlter.value.kind === 'alter-schema', 'allow rules alter succeeds');

                const deploy = await execute(await parseBind('DEPLOY SCHEMA shop AT LATEST ON shop_prod;', lang));
                assertTrue(deploy.ok && deploy.value.kind === 'deploy-schema', 'deploy succeeds');

                const select = await execute(await parseBind("SELECT sku, price FROM shop_prod.products WHERE sku = 'A';", lang));
                assertTrue(select.ok && select.value.kind === 'select', 'select after deploy succeeds');
                if (select.ok && select.value.kind === 'select') assertEquals(select.value.rows[0].values['price'], 0, 'deployed default visible');

                const updateRef = await execute(await parseBind('UPDATE REF users TO LATEST ON shop_prod;', lang));
                assertTrue(updateRef.ok && updateRef.value.kind === 'update-ref', 'update ref succeeds');

                const del = await execute(await parseBind(`DELETE FROM shop_prod.products WHERE rowId = '${insert.value.rowId}';`, lang));
                assertTrue(del.ok && del.value.kind === 'delete', 'delete succeeds');

                const products = await group.getTable('products');
                assertTrue(!await (await products.getView()).hasRow(insert.value.rowId), 'deleted row is gone');
            },
        },
        {
            name: '[REST03] default group resolves unqualified table writes and reads',
            invoke: async () => {
                const { lang } = await createEnv();
                lang.resolveDefaultGroup = async () => ({
                    kind: 'name',
                    text: 'shop_prod',
                    parts: ['shop_prod'],
                    span: { start: 0, end: 'shop_prod'.length, line: 1, column: 1 },
                });

                const insert = await execute(await parseBind("INSERT INTO products (sku, name) VALUES ('A', 'Widget');", lang));
                assertTrue(insert.ok && insert.value.kind === 'insert', 'unqualified insert succeeds');
                if (!insert.ok || insert.value.kind !== 'insert') return;

                const select = await execute(await parseBind("SELECT sku, name FROM products WHERE sku = 'A';", lang));
                assertTrue(select.ok && select.value.kind === 'select', 'unqualified select succeeds');
                if (select.ok && select.value.kind === 'select') assertEquals(select.value.rows[0].values['name'], 'Widget', 'select sees inserted row');

                const update = await execute(await parseBind(`UPDATE products SET name = 'Widget 2' WHERE rowId = '${insert.value.rowId}';`, lang));
                assertTrue(update.ok && update.value.kind === 'update', 'unqualified update succeeds');

                const del = await execute(await parseBind(`DELETE FROM products WHERE rowId = '${insert.value.rowId}';`, lang));
                assertTrue(del.ok && del.value.kind === 'delete', 'unqualified delete succeeds');
            },
        },
        {
            name: '[REST04] table binding default group diagnostics and explicit precedence',
            invoke: async () => {
                const { lang, group } = await createEnv();

                const missingDefault = parseStatement('SELECT * FROM products;');
                assertTrue(missingDefault.ok, 'unqualified SELECT parses before binding');
                if (missingDefault.ok) {
                    const bound = await bind(missingDefault.value, lang);
                    assertTrue(!bound.ok, 'unqualified SELECT without default group fails binding');
                    if (!bound.ok) assertTrue(bound.diagnostics[0].message.includes('requires a group qualifier'), 'bind diagnostic mentions group qualifier');
                }

                lang.resolveDefaultGroup = async () => {
                    throw new Error('default group should not be used for qualified table refs');
                };
                const explicit = await parseBind("SELECT sku FROM shop_prod.products;", lang);
                assertEquals(explicit.kind, 'select', 'explicit qualified SELECT binds');
                if (explicit.kind === 'select') assertEquals(explicit.table.groupId, group.getId(), 'explicit group wins over default group');
            },
        },
        {
            name: '[REST05] reverse rendering and dump produce SQL-like output',
            invoke: async () => {
                const { schema, group, lang } = await createEnv();
                const renderedSchema = renderCreateSchema(schema.createOp);
                assertTrue(parseStatement(renderedSchema).ok, 'rendered schema parses');
                assertTrue(renderedSchema.includes('ALLOW all IF true'), 'rendered schema uses ALLOW IF syntax');
                assertTrue(renderedSchema.includes('TABLE products (\n    sku string PUB READONLY,\n    name string\n  ) ALLOW all IF true'),
                    'rendered schema uses multiline column layout');
                const renderedMigration = renderSchemaUpdate({
                    action: 'schema-update',
                    migration: [{
                        rule: 'set-restrictions',
                        table: 'products',
                        restrictions: [{ on: 'insert', rule: { p: 'true' } }],
                    }],
                } as SchemaUpdatePayload);
                assertTrue(renderedMigration.includes('SET ALLOW RULES products'), 'rendered migration uses SET ALLOW RULES syntax');
                assertTrue(parseStatement(renderedMigration).ok, 'rendered migration parses');
                const renderedGroup = renderCreateTableGroup({
                    action: 'create',
                    type: RTableGroupImpl.typeId,
                    seed: 'shop_prod',
                    schemaRef: 'schema',
                    schemaVersion: json.toSet(['schemaVersion']),
                    idProvider: 'users.identities',
                    canDeploy: { p: 'true' },
                } as CreateTableGroupPayload);
                assertTrue(renderedGroup.includes('USING IDENTITIES users.identities'), 'rendered tablegroup uses USING IDENTITIES syntax');
                assertTrue(renderedGroup.includes('CAN DEPLOY IF true'), 'rendered tablegroup uses CAN DEPLOY IF syntax');
                assertTrue(parseStatement(renderedGroup).ok, 'rendered tablegroup parses');
                const renderedCorrelated = renderCreateTableGroup({
                    action: 'create',
                    type: RTableGroupImpl.typeId,
                    seed: 'shop_prod',
                    schemaRef: 'schema',
                    schemaVersion: json.toSet(['schemaVersion']),
                    canDeploy: { p: 'exists', table: 'grants', where: { resource: '$row.resource', grantee: '$author' } },
                } as CreateTableGroupPayload);
                assertTrue(renderedCorrelated.includes('resource = $row.resource AND grantee = $author'),
                    'rendered correlated predicate uses unquoted $-terms');
                assertTrue(renderRowOp({ action: 'update', rowId: 'row', values: { name: 'x' } }, 'shop_prod.products').startsWith('UPDATE'), 'row op renders');

                const insert = await execute(await parseBind("INSERT INTO shop_prod.products (sku, name) VALUES ('A', 'Widget');", lang));
                assertTrue(insert.ok && insert.value.kind === 'insert', 'insert for dump succeeds');
                if (!insert.ok || insert.value.kind !== 'insert') return;

                const update = await execute(await parseBind(`UPDATE shop_prod.products SET name = 'Widget 2' WHERE rowId = '${insert.value.rowId}';`, lang));
                assertTrue(update.ok && update.value.kind === 'update', 'update for dump succeeds');

                const del = await execute(await parseBind(`DELETE FROM shop_prod.products WHERE rowId = '${insert.value.rowId}';`, lang));
                assertTrue(del.ok && del.value.kind === 'delete', 'delete for dump succeeds');

                const dump = await dumpGroup(group);
                assertTrue(dump.indexOf('CREATE TABLEGROUP') >= 0, 'group dump includes create statement');
                assertTrue(dump.includes("INSERT INTO products (sku, name) VALUES ('A', 'Widget') AT {#"), 'dumped insert includes causal AT');
                assertTrue(dump.includes(`UPDATE products SET name = 'Widget 2' WHERE rowId = #${insert.value.rowId} AT {#`), 'dumped update includes causal AT');
                assertTrue(dump.includes(`DELETE FROM products WHERE rowId = #${insert.value.rowId} AT {#`), 'dumped delete includes causal AT');
            },
        },
        {
            name: '[REST06] scripted Users-compatible group uses publicKey()',
            invoke: async () => {
                const ctx = createMockRContext({ selfValidate: true });
                ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
                ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);

                const admin = await createIdentity(SIGNING_ED25519, hashSuite);
                const lang = createTestBindContext(ctx, { admin });

                const schemaPlan = await execute(await parseBind(`
                    CREATE SCHEMA users_schema CREATORS ($admin) AS (
                      TABLE identities (
                        keyId string PUB READONLY,
                        publicKey string PUB READONLY,
                        name string NULL PUB
                      ) IDENTITY PROVIDER ALLOW insert IF true,
                      TABLE caps (
                        label string PUB READONLY,
                        grantee string PUB READONLY
                      ) CONCURRENT DELETES
                        ALLOW insert IF EXISTS caps WHERE label = 'manager' AND grantee = $author
                        ALLOW delete IF grantee = $author OR EXISTS caps WHERE label = 'manager' AND grantee = $author
                    );
                `, lang));
                assertTrue(schemaPlan.ok && schemaPlan.value.kind === 'create-plan', 'users schema create plan succeeds');
                if (!schemaPlan.ok || schemaPlan.value.kind !== 'create-plan') return;
                const schema = await ctx.createObject(schemaPlan.value.plan.payload) as RSchemaImpl;
                lang.registerSchema('users_schema', schema);

                const renderedSchema = renderCreateSchema(schema.createOp);
                assertTrue(parseStatement(renderedSchema).ok, 'rendered users schema parses');
                assertTrue(renderedSchema.includes(`TABLE identities (\n    keyId string PUB READONLY,\n    publicKey string PUB READONLY,\n    name string NULL PUB\n  ) IDENTITY PROVIDER ALLOW insert IF true`),
                    'rendered identities table uses multiline columns');
                assertTrue(renderedSchema.includes(`TABLE caps (\n    label string PUB READONLY,\n    grantee string PUB READONLY\n  ) CONCURRENT DELETES\n    ALLOW insert IF`),
                    'rendered caps table indents ALLOW rules after CONCURRENT DELETES');
                assertTrue(renderedSchema.includes('grantee = $author'), 'rendered schema uses unquoted $author');
                assertTrue(!renderedSchema.includes("grantee = '$author'"), 'rendered schema does not quote $author');

                const groupPlan = await execute(await parseBind(`
                    CREATE TABLEGROUP users
                      USING SCHEMA users_schema
                      USING IDENTITIES identities
                      WITH ROWS (
                        identities (keyId = $admin, publicKey = publicKey($admin), name = 'Admin'),
                        caps (label = 'manager', grantee = $admin)
                      );
                `, lang));
                assertTrue(groupPlan.ok && groupPlan.value.kind === 'create-plan', 'users group create plan succeeds');
                if (!groupPlan.ok || groupPlan.value.kind !== 'create-plan') return;

                const payload = groupPlan.value.plan.payload as CreateTableGroupPayload;
                assertEquals(payload.idProvider, 'identities', 'group selects local identity provider');
                const identities = payload.initialRows?.['identities'] as Array<{ values: { [key: string]: unknown } }> | undefined;
                assertEquals(identities?.[0].values['keyId'], admin.keyId, 'plain identity value resolves to keyId');
                assertEquals(identities?.[0].values['publicKey'], serializePublicKeyToBase64(admin.publicKey), 'publicKey() serializes public key');

                const group = await ctx.createObject(payload) as RTableGroupImpl;
                lang.registerGroup('users', group);
                assertTrue(group.getId().length > 0, 'scripted users group creates');
            },
        },
        {
            name: '[REST07] publicKey() rejects bare key ids',
            invoke: async () => {
                const ctx = createMockRContext({ selfValidate: true });
                ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
                const admin = await createIdentity(SIGNING_ED25519, hashSuite);
                const lang = createTestBindContext(ctx, { bare: { kind: 'key-id', keyId: admin.keyId } });

                const schemaInit = await RSchemaImpl.create({
                    name: 'test:bare_schema',
                    creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
                    tables: [{
                        name: 'identities',
                        columns: {
                            keyId: { type: 'string', pub: true, readonly: true },
                            publicKey: { type: 'string', pub: true, readonly: true },
                        },
                        idProvider: { keyIdColumn: 'keyId', publicKeyColumn: 'publicKey' },
                    }],
                });
                const schema = await ctx.createObject(schemaInit) as RSchemaImpl;
                lang.registerSchema('bare_schema', schema);

                const parsed = parseStatement(`
                    CREATE TABLEGROUP bad_users
                      USING SCHEMA bare_schema
                      WITH ROWS (
                        identities (keyId = $bare, publicKey = publicKey($bare))
                      );
                `);
                assertTrue(parsed.ok, 'parse should succeed');
                if (!parsed.ok) return;
                const bound = await bind(parsed.value, lang);
                assertTrue(!bound.ok, 'publicKey() on bare key id should fail binding');
                if (!bound.ok) assertTrue(bound.diagnostics[0].message.includes('publicKey() requires'), 'diagnostic mentions publicKey requirement');
            },
        },
    ],
};
