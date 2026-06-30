import { json } from "@hyper-hyper-space/hhs3_json";
import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { B64Hash, createBasicCrypto, createIdentity, HASH_SHA256, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import { serializePublicKeyToBase64 } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "../../rdb/test/mock_rcontext.js";
import {
    CreateTableGroupPayload, RDbImpl, rDbFactory, RSchemaImpl, rSchemaFactory, RTableGroupImpl, rTableGroupFactory,
    SchemaUpdatePayload, deriveRowId,
} from "@hyper-hyper-space/hhs3_rdb";

import { bind, BoundStatement, PSEUDO_COLUMN_UUID } from "../src/bind/bind.js";
import { execute } from "../src/exec/execute.js";
import { parseStatement } from "../src/syntax/parser.js";
import { renderCreateSchema, renderCreateTableGroup, renderRowOp, renderSchemaUpdate } from "../src/reverse/render.js";
import { dumpDatabase, dumpGroup, dumpSchema } from "../src/reverse/dump.js";
import type { RenderAliasContext, RenderVersionScope } from "../src/reverse/aliases.js";
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
        name: 'users',
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

    return { ctx, lang, schema, group, admin, usersGroup };
}

type MockCtx = ReturnType<typeof createMockRContext>;

function dumpLoaders(ctx: MockCtx) {
    return {
        loadSchema: async (id: B64Hash) => {
            const object = await ctx.getObject(id);
            if (object === undefined) throw new Error(`Schema '${id}' not found`);
            return object as RSchemaImpl;
        },
        loadGroup: async (id: B64Hash) => {
            const object = await ctx.getObject(id);
            if (object === undefined) throw new Error(`Group '${id}' not found`);
            return object as RTableGroupImpl;
        },
    };
}

class TestAliasContext implements RenderAliasContext {
    private readonly hashToName = new Map<string, string>();
    private readonly usedNames = new Map<string, Set<string>>();
    private readonly pending: string[] = [];
    private readonly versionCounters = new Map<B64Hash, number>();
    private keyCounter = 0;

    constructor(private readonly keyLabels = new Map<B64Hash, string>()) {}

    key(keyId: B64Hash, hint?: string): string {
        return this.ensure('key', keyId, hint ?? this.keyLabels.get(keyId) ?? `keyId${++this.keyCounter}`);
    }

    schema(id: B64Hash, hint?: string): string {
        return this.ensure('schema', id, hint ?? 'schema');
    }

    group(id: B64Hash, hint?: string): string {
        return this.ensure('group', id, hint ?? 'group');
    }

    db(_id: B64Hash, hint?: string): string {
        return hint ?? 'db';
    }

    version(hash: B64Hash, scope: RenderVersionScope): string {
        const existing = this.hashToName.get(`version:${hash}`);
        if (existing !== undefined) return existing;
        const n = (this.versionCounters.get(scope.objectId) ?? 0) + 1;
        this.versionCounters.set(scope.objectId, n);
        const name = `${scope.objectName}_ver${n}`;
        this.register('version', hash, name);
        return name;
    }

    drainDefinitions(): string[] {
        const out = [...this.pending];
        this.pending.length = 0;
        return out;
    }

    private ensure(scope: string, hash: B64Hash, preferred: string): string {
        const existing = this.hashToName.get(`${scope}:${hash}`);
        if (existing !== undefined) return existing;
        const name = this.uniqueName(scope, preferred);
        this.register(scope, hash, name);
        return name;
    }

    private register(scope: string, hash: B64Hash, name: string): void {
        this.hashToName.set(`${scope}:${hash}`, name);
        this.pending.push(`\\alias ${scope} ${name} #${hash}`);
    }

    private uniqueName(scope: string, preferred: string): string {
        let names = this.usedNames.get(scope);
        if (names === undefined) {
            names = new Set();
            this.usedNames.set(scope, names);
        }
        if (!names.has(preferred)) {
            names.add(preferred);
            return preferred;
        }
        let i = 2;
        while (names.has(`${preferred}${i}`)) i++;
        const name = `${preferred}${i}`;
        names.add(name);
        return name;
    }
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
            name: '[REST01b] ADD SCHEMA / ADD TABLEGROUP register members and dump round-trips',
            invoke: async () => {
                const { ctx, lang, schema, group } = await createEnv();
                const dbPlan = await execute(await parseBind('CREATE DATABASE app;', lang));
                if (!dbPlan.ok || dbPlan.value.kind !== 'create-plan') throw new Error('database create failed');
                const db = await ctx.createObject(dbPlan.value.plan.payload) as RDbImpl;
                lang.registerDatabase('app', db);

                const addSchema = await execute(await parseBind('ADD SCHEMA shop TO app NOTE \'shop schema\';', lang));
                assertTrue(addSchema.ok && addSchema.value.kind === 'add-member', 'ADD SCHEMA returns add-member');
                const addGroup = await execute(await parseBind('ADD TABLEGROUP shop_prod TO app;', lang));
                assertTrue(addGroup.ok && addGroup.value.kind === 'add-member', 'ADD TABLEGROUP returns add-member');

                const memberSchemas = await db.getMemberSchemas();
                const memberGroups = await db.getMemberGroups();
                assertTrue(memberSchemas.includes(schema.getId()), 'schema is a member');
                assertTrue(memberGroups.includes(group.getId()), 'group is a member');

                const dump = await dumpDatabase(db, dumpLoaders(ctx));
                assertTrue(dump.includes('CREATE DATABASE app'), 'dump includes CREATE DATABASE');
                assertTrue(dump.includes('CREATE SCHEMA shop'), 'dump includes CREATE SCHEMA');
                assertTrue(dump.indexOf('ADD SCHEMA') < dump.indexOf('CREATE TABLEGROUP'), 'ADD SCHEMA before CREATE TABLEGROUP');
                assertTrue(dump.includes(`ADD SCHEMA #${schema.getId()} TO app`) && dump.includes(`NOTE 'shop schema'`), 'ADD SCHEMA round-trips');
                assertTrue(dump.includes(`ADD TABLEGROUP #${group.getId()} TO app`), 'ADD TABLEGROUP round-trips');
                assertTrue(!dump.includes('TO <database>'), 'no database placeholder');
            },
        },
        {
            name: '[REST01c] gated database requires BY on ADD and dump round-trips CREATORS/BY',
            invoke: async () => {
                const { ctx, lang, schema, group, admin } = await createEnv();
                const dbPlan = await execute(await parseBind('CREATE DATABASE gated CREATORS ($admin);', lang));
                if (!dbPlan.ok || dbPlan.value.kind !== 'create-plan') throw new Error('database create failed');
                const db = await ctx.createObject(dbPlan.value.plan.payload) as RDbImpl;
                lang.registerDatabase('gated', db);

                const unsignedParsed = parseStatement('ADD SCHEMA shop TO gated;');
                assertTrue(unsignedParsed.ok, 'parse unsigned ADD');
                if (!unsignedParsed.ok) return;
                const unsigned = await bind(unsignedParsed.value, lang);
                assertTrue(!unsigned.ok, 'unsigned ADD must fail bind when database declares creators');

                const addSchema = await execute(await parseBind("ADD SCHEMA shop TO gated BY $admin NOTE 'shop schema';", lang));
                assertTrue(addSchema.ok && addSchema.value.kind === 'add-member', 'signed ADD SCHEMA succeeds');
                const addGroup = await execute(await parseBind('ADD TABLEGROUP shop_prod TO gated BY $admin;', lang));
                assertTrue(addGroup.ok && addGroup.value.kind === 'add-member', 'signed ADD TABLEGROUP succeeds');

                assertTrue((await db.getMemberSchemas()).includes(schema.getId()), 'schema is a member');
                assertTrue((await db.getMemberGroups()).includes(group.getId()), 'group is a member');

                const dump = await dumpDatabase(db, dumpLoaders(ctx));
                assertTrue(dump.includes('CREATORS ('), 'dump includes CREATORS');
                assertTrue(dump.includes(admin.keyId), 'dump includes creator keyId');
                assertTrue(dump.includes(`ADD SCHEMA #${schema.getId()} TO gated`) && dump.includes(`NOTE 'shop schema'`) && dump.includes(`BY #${admin.keyId}`), 'ADD SCHEMA BY round-trips');
                assertTrue(dump.includes(`ADD TABLEGROUP #${group.getId()} TO gated`) && dump.includes(`BY #${admin.keyId}`), 'ADD TABLEGROUP BY round-trips');
            },
        },
        {
            name: '[REST02] ALTER, UPDATE SCHEMA, UPDATE REF, UPDATE, DELETE and BUNDLE execute',
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

                const deploy = await execute(await parseBind('UPDATE SCHEMA shop TO LATEST ON shop_prod;', lang));
                assertTrue(deploy.ok && deploy.value.kind === 'update-schema', 'update schema succeeds');

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
            name: '[REST02b] SELECT * returns schema columns without materializing absent nullable values',
            invoke: async () => {
                const { lang } = await createEnv();

                const insert = await execute(await parseBind("INSERT INTO shop_prod.products (sku, name) VALUES ('A', 'Widget');", lang));
                assertTrue(insert.ok && insert.value.kind === 'insert', 'insert succeeds');
                if (!insert.ok || insert.value.kind !== 'insert') return;

                const alter = await execute(await parseBind('ALTER SCHEMA shop AS (ADD COLUMN products.tag string NULL);', lang));
                assertTrue(alter.ok && alter.value.kind === 'alter-schema', 'alter succeeds');

                const deploy = await execute(await parseBind('UPDATE SCHEMA shop TO LATEST ON shop_prod;', lang));
                assertTrue(deploy.ok && deploy.value.kind === 'update-schema', 'deploy succeeds');

                const select = await execute(await parseBind("SELECT * FROM shop_prod.products WHERE sku = 'A';", lang));
                assertTrue(select.ok && select.value.kind === 'select', 'select succeeds');
                if (!select.ok || select.value.kind !== 'select') return;
                assertTrue(select.value.columns !== undefined && select.value.columns.includes('tag'), 'columns includes absent nullable column');
                assertTrue(select.value.columns !== undefined && select.value.columns.includes('sku'), 'columns includes sku');
                assertEquals(select.value.rows[0].values['tag'], undefined, 'absent nullable not in row values');

                const explicit = await execute(await parseBind("SELECT sku, name FROM shop_prod.products WHERE sku = 'A';", lang));
                assertTrue(explicit.ok && explicit.value.kind === 'select', 'explicit select succeeds');
                if (explicit.ok && explicit.value.kind === 'select') {
                    assertEquals(explicit.value.columns, undefined, 'explicit projection omits columns metadata');
                }
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
            name: '[REST05] reverse rendering and dump produce C-SQL output',
            invoke: async () => {
                const { schema, group, lang } = await createEnv();
                const renderedSchema = renderCreateSchema(schema.createOp);
                const renderedParsed = parseStatement(renderedSchema);
                assertTrue(renderedParsed.ok, 'rendered schema parses');
                if (renderedParsed.ok) {
                    const renderedBound = await bind(renderedParsed.value, lang);
                    assertTrue(renderedBound.ok, 'rendered schema binds with keystore creator lookup');
                }
                assertTrue(renderedSchema.includes('ALLOW all IF true'), 'rendered schema uses ALLOW IF syntax');
                assertTrue(renderedSchema.includes('TABLE products (\n    sku string PUB READONLY,\n    name string\n  )\n    ALLOW all IF true'),
                    'rendered schema uses multiline column layout');
                const authorKeyId = schema.createOp.creators[0].keyId;
                const renderedMigration = renderSchemaUpdate({
                    action: 'schema-update',
                    migration: [{
                        rule: 'set-restrictions',
                        table: 'products',
                        restrictions: [{ on: 'insert', rule: { p: 'true' } }],
                    }],
                    author: authorKeyId,
                    signature: 'sig',
                } as SchemaUpdatePayload, {
                    schemaRef: schema.getId(),
                    schemaName: schema.getName(),
                });
                assertTrue(renderedMigration.startsWith('-- shop\n'), 'rendered migration includes schema name comment');
                assertTrue(renderedMigration.includes(`ALTER SCHEMA #${schema.getId()} AS (`), 'rendered migration uses schema hash ref');
                assertTrue(renderedMigration.includes(` BY #${authorKeyId}`), 'rendered migration includes BY author');
                assertTrue(renderedMigration.includes('SET ALLOW RULES products'), 'rendered migration uses SET ALLOW RULES syntax');
                assertTrue(parseStatement(renderedMigration).ok, 'rendered migration parses');

                const alter = await execute(await parseBind('ALTER SCHEMA shop AS (ADD COLUMN products.note string NULL);', lang));
                assertTrue(alter.ok && alter.value.kind === 'alter-schema', 'alter for schema dump succeeds');
                const schemaDump = await dumpSchema(schema);
                assertTrue(!schemaDump.includes('#unknown'), 'schema dump does not use unknown schema ref');
                assertTrue(schemaDump.includes('-- shop\n'), 'schema dump includes schema name comment');
                assertTrue(schemaDump.includes(`ALTER SCHEMA #${schema.getId()} AS (`), 'schema dump uses schema hash ref');
                assertTrue(schemaDump.includes('ADD COLUMN products.note string NULL'), 'schema dump includes alter migration');
                const schemaDumpLines = schemaDump.split('\n');
                assertTrue(schemaDumpLines.some((line) =>
                    line.includes(` BY #${authorKeyId}`) && line.includes(' AT {#')),
                    'dumped alter includes BY author and causal AT');
                const renderedGroup = renderCreateTableGroup({
                    action: 'create',
                    type: RTableGroupImpl.typeId,
                    name: 'shop_prod',
                    seed: 'shop_prod',
                    schemaRef: 'schema',
                    schemaVersion: json.toSet(['schemaVersion']),
                    idProvider: 'users.identities',
                    canDeploy: { p: 'true' },
                } as CreateTableGroupPayload);
                assertTrue(renderedGroup.includes('USING IDENTITIES users.identities'), 'rendered tablegroup uses USING IDENTITIES syntax');
                assertTrue(renderedGroup.includes('ALLOW UPDATE SCHEMA IF true'), 'rendered tablegroup uses ALLOW UPDATE SCHEMA IF syntax');
                assertTrue(parseStatement(renderedGroup).ok, 'rendered tablegroup parses');
                const renderedCorrelated = renderCreateTableGroup({
                    action: 'create',
                    type: RTableGroupImpl.typeId,
                    name: 'shop_prod',
                    seed: 'shop_prod',
                    schemaRef: 'schema',
                    schemaVersion: json.toSet(['schemaVersion']),
                    canDeploy: { p: 'exists', table: 'grants', where: { resource: '$row.resource', grantee: '$author' } },
                } as CreateTableGroupPayload);
                assertTrue(renderedCorrelated.includes('EXISTS grants WHERE grants.resource = resource AND grants.grantee = $author'),
                    'rendered correlated predicate uses qualified exists columns');
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
                const dumpLines = dump.split('\n');
                const hasLine = (needle: string) => dumpLines.some((line) =>
                    line.includes(needle) && line.includes(' BY #') && line.includes(' AT {#'));
                assertTrue(dumpLines.some((line) =>
                    line.includes('INSERT INTO products') && line.includes('(uuid,') && line.includes("'A'")
                    && line.includes(' BY #') && line.includes(' AT {#')),
                    'dumped insert includes uuid, BY author and causal AT');
                assertTrue(hasLine(`UPDATE products SET name = 'Widget 2' WHERE rowId = #${insert.value.rowId}`), 'dumped update includes BY author and causal AT');
                assertTrue(hasLine(`DELETE FROM products WHERE rowId = #${insert.value.rowId}`), 'dumped delete includes BY author and causal AT');
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
                        ALLOW insert IF EXISTS caps AS c WHERE c.label = 'manager' AND c.grantee = $author
                        ALLOW delete IF grantee = $author OR EXISTS caps AS c WHERE c.label = 'manager' AND c.grantee = $author
                    );
                `, lang));
                assertTrue(schemaPlan.ok && schemaPlan.value.kind === 'create-plan', 'users schema create plan succeeds');
                if (!schemaPlan.ok || schemaPlan.value.kind !== 'create-plan') return;
                const schema = await ctx.createObject(schemaPlan.value.plan.payload) as RSchemaImpl;
                lang.registerSchema('users_schema', schema);

                const renderedSchema = renderCreateSchema(schema.createOp);
                assertTrue(parseStatement(renderedSchema).ok, 'rendered users schema parses');
                const renderedParsed = parseStatement(renderedSchema);
                if (renderedParsed.ok) {
                    const renderedBound = await bind(renderedParsed.value, lang);
                    assertTrue(renderedBound.ok, 'rendered users schema binds with keystore creator lookup');
                }
                assertTrue(renderedSchema.includes(`TABLE identities (\n    keyId string PUB READONLY,\n    publicKey string PUB READONLY,\n    name string NULL PUB\n  ) IDENTITY PROVIDER\n    ALLOW insert IF true`),
                    'rendered identities table uses multiline columns');
                assertTrue(renderedSchema.includes(`TABLE caps (\n    label string PUB READONLY,\n    grantee string PUB READONLY\n  ) CONCURRENT DELETES\n    ALLOW insert IF`),
                    'rendered caps table indents ALLOW rules after CONCURRENT DELETES');
                assertTrue(renderedSchema.includes('EXISTS caps AS c WHERE c.label'), 'self-referential EXISTS uses first-letter alias');
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
        {
            name: '[REST08] SEED and uuid pseudo-column bind deterministically',
            invoke: async () => {
                const { ctx, lang, admin } = await createEnv();
                const dbPlan = await execute(await parseBind("CREATE DATABASE app SEED 'db-seed-fixed';", lang));
                if (!dbPlan.ok || dbPlan.value.kind !== 'create-plan') throw new Error('database create failed');
                const db1 = await ctx.createObject(dbPlan.value.plan.payload) as RDbImpl;
                const db2 = await ctx.createObject(dbPlan.value.plan.payload) as RDbImpl;
                assertEquals(db1.getId(), db2.getId(), 'same SEED yields same database id');

                const insert = await execute(await parseBind(
                    "INSERT INTO shop_prod.products (uuid, sku, name) VALUES ('row-uuid-1', 'A', 'Widget');",
                    lang,
                ));
                assertTrue(insert.ok && insert.value.kind === 'insert', 'insert with uuid succeeds');
                if (!insert.ok || insert.value.kind !== 'insert') return;
                assertEquals(insert.value.rowId, deriveRowId('row-uuid-1', admin.keyId), 'uuid pseudo-column fixes rowId');
            },
        },
        {
            name: '[REST09] dumpDatabase full and schema profiles',
            invoke: async () => {
                const { ctx, lang, schema, group, usersGroup } = await createEnv();
                const dbPlan = await execute(await parseBind("CREATE DATABASE app SEED 'dump-test-db';", lang));
                if (!dbPlan.ok || dbPlan.value.kind !== 'create-plan') throw new Error('database create failed');
                const db = await ctx.createObject(dbPlan.value.plan.payload) as RDbImpl;
                lang.registerDatabase('app', db);

                await execute(await parseBind('ADD SCHEMA shop TO app;', lang));
                await execute(await parseBind('ADD TABLEGROUP users TO app;', lang));
                await execute(await parseBind('ADD TABLEGROUP shop_prod TO app;', lang));
                await execute(await parseBind("INSERT INTO shop_prod.products (sku, name) VALUES ('A', 'Widget');", lang));
                await execute(await parseBind('ALTER SCHEMA shop AS (ADD COLUMN products.note string NULL);', lang));

                const loaders = dumpLoaders(ctx);
                const fullDump = await dumpDatabase(db, { ...loaders, mode: 'full' });
                assertTrue(fullDump.includes("SEED 'dump-test-db'"), 'full dump includes database SEED');
                assertTrue(fullDump.indexOf('ADD SCHEMA') < fullDump.indexOf('CREATE TABLEGROUP'), 'ADD SCHEMA before groups');
                assertTrue(fullDump.indexOf('CREATE TABLEGROUP users') < fullDump.indexOf('CREATE TABLEGROUP shop_prod'), 'users before shop_prod');
                assertTrue(fullDump.includes(`ADD TABLEGROUP #${usersGroup.getId()} TO app`), 'full ADD TABLEGROUP by hash');
                assertTrue(fullDump.includes(`BIND users => #${usersGroup.getId()}`), 'full BIND by hash');
                assertTrue(fullDump.includes('INSERT INTO products'), 'full dump includes row ops');
                for (const line of fullDump.split('\n')) {
                    if (!line.startsWith('ADD SCHEMA ') && !line.startsWith('ADD TABLEGROUP ')) continue;
                    assertTrue(line.includes(' AT {#'), `full dump membership includes AT: ${line}`);
                }

                const schemaDump = await dumpDatabase(db, { ...loaders, mode: 'schema' });
                assertTrue(!schemaDump.includes("SEED 'dump-test-db'"), 'schema dump omits database SEED');
                assertTrue(schemaDump.includes('ADD SCHEMA shop TO app'), 'schema dump ADD SCHEMA by name');
                assertTrue(schemaDump.includes('ADD TABLEGROUP shop_prod TO app'), 'schema dump ADD TABLEGROUP by name');
                assertTrue(schemaDump.includes('BIND users => users'), 'schema dump BIND by name');
                assertTrue(!schemaDump.includes('INSERT INTO products'), 'schema dump omits row ops');
                assertTrue(schemaDump.includes('CREATE SCHEMA shop'), 'schema dump includes schema DDL');
                for (const line of schemaDump.split('\n')) {
                    if (!line.startsWith('ADD SCHEMA ') && !line.startsWith('ADD TABLEGROUP ')) continue;
                    assertTrue(!line.includes(' AT {#'), `schema dump membership omits AT: ${line}`);
                }
                const alterStmt = schemaDump.split('\n\n').find((s) =>
                    s.includes('ALTER SCHEMA #') && s.includes('ADD COLUMN products.note string NULL'));
                assertTrue(alterStmt !== undefined && alterStmt.includes(' AT {#'), 'schema dump alter keeps causal AT');
            },
        },
        {
            name: '[REST10] full dumpGroup renders group-scoped ops with #groupId',
            invoke: async () => {
                const { lang, group, schema, usersGroup } = await createEnv();

                const insert = await execute(await parseBind("INSERT INTO shop_prod.products (sku, name) VALUES ('A', 'Widget');", lang));
                assertTrue(insert.ok && insert.value.kind === 'insert', 'insert succeeds');
                if (!insert.ok || insert.value.kind !== 'insert') return;

                lang.resolveRowId = async (ref, table, at, from) => {
                    const ids = await (await table.table.getView(at, from ?? at)).liveRowIds();
                    const matches = ids.filter((id: B64Hash) => id.startsWith(ref.prefix));
                    if (matches.length !== 1) throw new Error(`rowId prefix did not resolve uniquely: ${ref.prefix}`);
                    return matches[0];
                };

                const bundle = await execute(await parseBind(`BUNDLE ON shop_prod (
                    UPDATE products SET name = 'Widget 3' WHERE rowId = #${insert.value.rowId.slice(0, 10)};
                );`, lang));
                assertTrue(bundle.ok && bundle.value.kind === 'bundle', 'bundle succeeds');

                const deploy = await execute(await parseBind('UPDATE SCHEMA shop TO LATEST ON shop_prod;', lang));
                assertTrue(deploy.ok && deploy.value.kind === 'update-schema', 'update schema succeeds');

                const updateRef = await execute(await parseBind('UPDATE REF users TO LATEST ON shop_prod;', lang));
                assertTrue(updateRef.ok && updateRef.value.kind === 'update-ref', 'update ref succeeds');

                const dump = await dumpGroup(group, { render: { profile: 'full' } });
                const groupTarget = `#${group.getId()}`;
                assertTrue(!dump.includes('<group>'), 'full dump does not emit <group> placeholder');
                assertTrue(dump.includes(`BUNDLE ON ${groupTarget}`), 'dumped bundle uses group id');
                assertTrue(
                    dump.includes(`UPDATE SCHEMA #${schema.getId()} TO`) && dump.includes(` ON ${groupTarget}`),
                    'dumped UPDATE SCHEMA uses group id',
                );
                assertTrue(dump.includes('UPDATE REF #') && dump.includes(` ON ${groupTarget}`), 'dumped UPDATE REF uses group id');

                for (const statement of dump.split('\n\n')) {
                    const line = statement.split('\n')[0] ?? '';
                    if (!line.startsWith('BUNDLE ON ') && !line.startsWith('UPDATE REF ') && !line.startsWith('UPDATE SCHEMA ')) continue;
                    const parsed = parseStatement(statement);
                    assertTrue(parsed.ok, `dumped group-scoped statement parses: ${line}`);
                }

                const rowPrefix = insert.value.rowId.slice(0, 10);
                await parseBind(`BUNDLE ON #${group.getId()} (
                    UPDATE products SET name = 'Widget 4' WHERE rowId = #${rowPrefix};
                );`, lang);
                await parseBind(`UPDATE SCHEMA #${schema.getId()} TO LATEST ON #${group.getId()};`, lang);
                await parseBind(`UPDATE REF #${usersGroup.getId()} TO LATEST ON #${group.getId()};`, lang);
            },
        },
        {
            name: '[REST11] aliasMode dump uses aliases not raw hashes',
            invoke: async () => {
                const { ctx, lang, schema, group, admin, usersGroup } = await createEnv();
                const dbPlan = await execute(await parseBind("CREATE DATABASE app SEED 'alias-dump-db';", lang));
                if (!dbPlan.ok || dbPlan.value.kind !== 'create-plan') throw new Error('database create failed');
                const db = await ctx.createObject(dbPlan.value.plan.payload) as RDbImpl;
                lang.registerDatabase('app', db);

                await execute(await parseBind('ADD SCHEMA shop TO app BY $admin;', lang));
                await execute(await parseBind('ADD TABLEGROUP shop_prod TO app BY $admin;', lang));
                await execute(await parseBind("INSERT INTO shop_prod.products (sku, name) VALUES ('A', 'Widget') BY $admin;", lang));
                await execute(await parseBind('ALTER SCHEMA shop AS (ADD COLUMN products.note string NULL) BY $admin;', lang));

                const aliases = new TestAliasContext(new Map([[admin.keyId, 'admin']]));
                const loaders = dumpLoaders(ctx);
                const dump = await dumpDatabase(db, {
                    ...loaders,
                    mode: 'full',
                    render: {
                        aliasMode: true,
                        aliases,
                        resolveSchemaName: (id) => (id === schema.getId() ? 'shop' : undefined),
                        resolveGroupName: (id) => {
                            if (id === group.getId()) return 'shop_prod';
                            if (id === usersGroup.getId()) return 'users';
                            return undefined;
                        },
                    },
                });

                assertTrue(dump.includes('\\alias key admin #'), 'dump defines key alias with full hash');
                assertTrue(dump.includes('\\alias version '), 'dump defines version aliases');
                assertTrue(dump.includes('BY $admin'), 'dump uses aliased BY author');
                assertTrue(!dump.includes(`BY #${admin.keyId}`), 'dump omits raw BY key hash');
                assertTrue(dump.includes(' AT {') && dump.includes('_ver'), 'dump uses version alias names in AT');
                assertTrue(!/ AT \{#[A-Za-z0-9+/=]+/.test(dump), 'dump AT clauses omit raw version hashes');
                assertTrue(dump.includes(`BIND users => #${usersGroup.getId()}`), 'BIND RHS still uses hash');
                assertTrue(dump.includes('ADD SCHEMA shop TO app'), 'ADD SCHEMA uses schema alias name');
                assertTrue(dump.includes('ADD TABLEGROUP shop_prod TO app'), 'ADD TABLEGROUP uses group alias name');
            },
        },
    ],
};
