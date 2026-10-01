import { json } from "@hyper-hyper-space/hhs3_json";
import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { serializePublicKeyToBase64 } from "@hyper-hyper-space/hhs3_mvt";
import {
    CatalogReleasePayload, RDbImpl, RSchemaImpl, RTableGroupImpl,
    SchemaUpdatePayload, deriveRowId,
} from "@hyper-hyper-space/hhs3_rdb";

import { bind, BoundStatement } from "../src/bind/bind.js";
import { execute } from "../src/exec/execute.js";
import { parseStatement } from "../src/syntax/parser.js";
import { splitStatements } from "../src/syntax/scanner.js";
import { renderAlterCatalog, renderCreateCatalog, renderCreateSchema, renderLiteral, renderRowOp, renderSchemaUpdate } from "../src/reverse/render.js";
import { dumpDatabase, dumpGroup, dumpSchema } from "../src/reverse/dump.js";
import type { RenderAliasContext, RenderVersionScope } from "../src/reverse/aliases.js";
import type { TestBindContext } from "./mock_bind_context.js";
import { createLangEnv, LangEnv, newIdentity } from "./lang_env.js";

async function parseBind(sql: string, context: TestBindContext): Promise<BoundStatement> {
    const parsed = parseStatement(sql);
    assertTrue(parsed.ok, `parse should succeed: ${sql}`);
    if (!parsed.ok) throw new Error(parsed.diagnostics[0].message);
    const bound = await bind(parsed.value, context);
    assertTrue(bound.ok, `bind should succeed: ${sql}`);
    if (!bound.ok) throw new Error(bound.diagnostics[0].message);
    return bound.value;
}

// A `shop` schema and a `users` schema released as the `users` and
// `shop_prod` groups of `shop_catalog`, deployed as the database `app`.
async function createEnv() {
    const admin = await newIdentity();
    const env = await createLangEnv({ vars: { admin, me: admin } });
    await env.run(`
        CREATE SCHEMA test:users_schema CREATORS ($admin) AS (
          TABLE caps (label string PUB) ALLOW all IF true
        );
        CREATE SCHEMA shop CREATORS ($admin) AS (
          TABLE products (
            sku string PUB READONLY,
            name string
          ) ALLOW all IF true
        );
        CREATE CATALOG shop_catalog VERSION '1.0.0' AS (
          TABLEGROUP users USING SCHEMA test:users_schema,
          TABLEGROUP shop_prod USING SCHEMA shop BIND users => users
        );
        CREATE DATABASE app SEED 'app-seed' USING CATALOG shop_catalog;
    `);
    return {
        env,
        ctx: env.ctx,
        lang: env.lang,
        admin,
        schema: await env.schema('shop'),
        catalog: await env.catalog('shop_catalog'),
        db: await env.database('app'),
        group: await env.group('shop_prod'),
        usersGroup: await env.group('users'),
    };
}

// Releases `body` as `version` and deploys it into `db`.
async function releaseAndDeploy(env: LangEnv, version: string, body: string, db = 'app', by = ''): Promise<void> {
    await env.run(`ALTER CATALOG shop_catalog VERSION '${version}' AS (${body}) ${by};`);
    await env.run(`UPDATE CATALOG shop_catalog TO '${version}' ON ${db} ${by};`);
}

function dumpLoaders(env: LangEnv) {
    return {
        loadSchema: async (id: B64Hash) => {
            const object = await env.ctx.getObject(id);
            if (object === undefined) throw new Error(`Schema '${id}' not found`);
            return object as RSchemaImpl;
        },
        loadGroup: async (id: B64Hash) => {
            const object = await env.ctx.getObject(id);
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

    constructor(
        private readonly keyLabels = new Map<B64Hash, string>(),
        private readonly keyPublicKeys = new Map<B64Hash, string>(),
    ) {}

    key(keyId: B64Hash, hint?: string): string {
        return this.ensure('key', keyId, hint ?? this.keyLabels.get(keyId) ?? `keyId${++this.keyCounter}`);
    }

    schema(id: B64Hash, hint?: string): string {
        return this.ensure('schema', id, hint ?? 'schema');
    }

    group(id: B64Hash, hint?: string): string {
        return this.ensure('group', id, hint ?? 'group');
    }

    db(id: B64Hash, hint?: string): string {
        return this.ensure('db', id, hint ?? 'db');
    }

    catalog(id: B64Hash, hint?: string): string {
        return this.ensure('catalog', id, hint ?? 'catalog');
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

    lookupKeyAlias(keyId: B64Hash): string | undefined {
        return this.hashToName.get(`key:${keyId}`);
    }

    lookupPublicKeyAlias(serialized: string): string | undefined {
        for (const [keyId, pubkey] of this.keyPublicKeys) {
            if (pubkey !== serialized) continue;
            const alias = this.lookupKeyAlias(keyId);
            if (alias !== undefined) return alias;
        }
        return undefined;
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
            name: '[REST01] CREATE DATABASE returns a create plan whose afterCreate creates the member groups',
            invoke: async () => {
                const { ctx, lang, catalog } = await createEnv();
                const result = await execute(await parseBind('CREATE DATABASE app2 USING CATALOG shop_catalog;', lang));
                assertTrue(result.ok && result.value.kind === 'create-plan', 'database create returns a plan');
                if (!result.ok || result.value.kind !== 'create-plan' || result.value.plan.kind !== 'create-database') return;
                const plan = result.value.plan;
                assertEquals(plan.payload.catalog, catalog.getId(), 'the payload names the catalog');
                assertEquals(plan.payload.release, catalog.getId(), 'LATEST selects the only release, the genesis');
                const db = await ctx.createObject(plan.payload) as RDbImpl;
                assertEquals((await db.getMemberGroups()).length, 2, 'the membership is computed before any group exists');
                const update = await plan.afterCreate(db);
                assertEquals(update.created.length, 2, 'afterCreate creates both groups');
                assertEquals(update.commit, undefined, 'the create release is already recorded by the create');
                for (const id of await db.getMemberGroups()) assertTrue(await ctx.getObject(id) !== undefined, `member ${id} exists`);
            },
        },
        {
            name: '[REST01b] a release adding a group deploys a new member, and the database dump replays',
            invoke: async () => {
                const { env, db, admin } = await createEnv();
                await env.run(`CREATE SCHEMA notes CREATORS ($admin) AS (TABLE notes (text string) ALLOW all IF true);`);
                await releaseAndDeploy(env, '1.1.0', 'ADD TABLEGROUP notes USING SCHEMA notes BIND shop => shop_prod');
                const names = await db.getMemberGroupNames();
                assertEquals([...names.keys()].sort().join(','), 'notes,shop_prod,users', 'the new group is a member');
                await env.run("INSERT INTO notes.notes (text) VALUES ('hi');");

                const dump = await dumpDatabase(db, dumpLoaders(env));
                assertTrue(dump.includes('CREATE CATALOG shop_catalog'), 'dump includes CREATE CATALOG');
                assertTrue(dump.includes("ALTER CATALOG #") && dump.includes('ADD TABLEGROUP notes'), 'dump includes the release');
                assertTrue(dump.indexOf('CREATE CATALOG') < dump.indexOf('CREATE DATABASE app'), 'the catalog comes first');
                assertTrue(dump.includes('UPDATE CATALOG #') && dump.includes(`ON #${db.getId()}`), 'dump includes the deploy');
                assertTrue(dump.indexOf('UPDATE CATALOG') < dump.indexOf('INSERT INTO notes.notes'),
                    'ops on the new group follow the deploy that creates it');

                const replay = await createLangEnv({ vars: { admin, me: admin } });
                await replay.run(dump);
                const replayed = await replay.database('app');
                assertEquals(replayed.getId(), db.getId(), 'the replayed database has the same id');
                assertEquals(JSON.stringify([...(await replayed.getMemberGroupNames()).entries()].sort()),
                    JSON.stringify([...names.entries()].sort()), 'the replayed members have the same ids');
                const frontier = async (e: LangEnv) => [...await (await (await e.group('app.notes')).getScopedDag()).getFrontier()].sort().join();
                assertEquals(await frontier(replay), await frontier(env), 'the new group replays to the same DAG');
            },
        },
        {
            name: '[REST01c] a database with creators requires BY on UPDATE CATALOG; the dump carries CREATORS and BY',
            invoke: async () => {
                const { env, admin } = await createEnv();
                await env.run('CREATE DATABASE gated USING CATALOG shop_catalog CREATORS ($admin) BY $admin;');
                await env.run("ALTER SCHEMA shop AS (ADD COLUMN products.price integer DEFAULT 0);");
                await env.run("ALTER CATALOG shop_catalog VERSION '1.1.0' AS (UPDATE SCHEMA shop TO LATEST ON shop_prod);");

                const unsigned = await env.fail("UPDATE CATALOG shop_catalog TO '1.1.0' ON gated BY NOBODY;");
                assertTrue(unsigned.includes('requires an author'), unsigned);
                const [deployed] = await env.run("UPDATE CATALOG shop_catalog TO '1.1.0' ON gated BY $admin;");
                assertTrue(deployed.kind === 'update-catalog' && deployed.update.deployed.length === 1, 'the signed deploy succeeds');

                const gated = await env.database('gated');
                const dump = await dumpDatabase(gated, dumpLoaders(env));
                assertTrue(dump.includes(`CREATORS (publicKey('${serializePublicKeyToBase64(admin.publicKey)}'))`), 'dump includes CREATORS');
                const updateLine = dump.split('\n').find((l) => l.startsWith('UPDATE CATALOG'));
                assertTrue(updateLine !== undefined && updateLine.includes(`BY #${admin.keyId}`), `the deploy keeps its author: ${updateLine}`);
            },
        },
        {
            name: '[REST02] ALTER SCHEMA, a released deploy, UPDATE REF, UPDATE, DELETE and BUNDLE execute',
            invoke: async () => {
                const { env, lang, group } = await createEnv();

                const insert = await execute(await parseBind("INSERT INTO shop_prod.products (sku, name) VALUES ('A', 'Widget');", lang));
                assertTrue(insert.ok && insert.value.kind === 'insert', 'insert succeeds');
                if (!insert.ok || insert.value.kind !== 'insert') return;

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

                await releaseAndDeploy(env, '1.1.0', 'UPDATE SCHEMA shop TO LATEST ON shop_prod');

                const select = await execute(await parseBind("SELECT sku, price FROM shop_prod.products WHERE sku = 'A';", lang));
                assertTrue(select.ok && select.value.kind === 'select', 'select after deploy succeeds');
                if (select.ok && select.value.kind === 'select') assertEquals(select.value.rows[0].values['price'], 0, 'deployed default visible');

                await env.run("INSERT INTO users.caps (label) VALUES ('x');");
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
                const { env, lang } = await createEnv();

                const insert = await execute(await parseBind("INSERT INTO shop_prod.products (sku, name) VALUES ('A', 'Widget');", lang));
                assertTrue(insert.ok && insert.value.kind === 'insert', 'insert succeeds');
                if (!insert.ok || insert.value.kind !== 'insert') return;

                await env.run('ALTER SCHEMA shop AS (ADD COLUMN products.tag string NULL);');
                await releaseAndDeploy(env, '1.1.0', 'UPDATE SCHEMA shop TO LATEST ON shop_prod');

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
                const qualified = await parseBind("SELECT sku FROM app.shop_prod.products;", lang);
                if (qualified.kind === 'select') assertEquals(qualified.table.groupId, group.getId(), 'db.group.table resolves the same group');
            },
        },
        {
            name: '[REST05] reverse rendering and dump produce C-SQL output',
            invoke: async () => {
                const { env, admin, schema, group, lang, catalog, usersGroup } = await createEnv();
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
                    version: '0.0.2',
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
                assertTrue(renderedMigration.includes(`ALTER SCHEMA #${schema.getId()} VERSION '0.0.2' AS (`), 'rendered migration uses schema hash ref and carries the version');
                assertTrue(renderedSchema.includes(`VERSION '${schema.createOp.version}'`), 'rendered schema carries its version');
                assertTrue(renderedMigration.includes(` BY #${authorKeyId}`), 'rendered migration includes BY author');
                assertTrue(renderedMigration.includes('SET ALLOW RULES products'), 'rendered migration uses SET ALLOW RULES syntax');
                assertTrue(parseStatement(renderedMigration).ok, 'rendered migration parses');

                const alter = await execute(await parseBind('ALTER SCHEMA shop AS (ADD COLUMN products.note string NULL);', lang));
                assertTrue(alter.ok && alter.value.kind === 'alter-schema', 'alter for schema dump succeeds');
                const schemaDump = await dumpSchema(schema);
                assertTrue(!schemaDump.includes('#unknown'), 'schema dump does not use unknown schema ref');
                assertTrue(schemaDump.includes('-- shop\n'), 'schema dump includes schema name comment');
                assertTrue(schemaDump.includes(`ALTER SCHEMA #${schema.getId()} VERSION '0.0.2' AS (`), 'schema dump uses schema hash ref and the default next-patch version');
                assertTrue(schemaDump.includes('ADD COLUMN products."note" string NULL'), 'schema dump includes alter migration');
                const noted = await execute(await parseBind("ALTER SCHEMA shop AS (ADD COLUMN products.memo string NULL) NOTE 'v3: it''s a memo';", lang));
                assertTrue(noted.ok && noted.value.kind === 'alter-schema', 'alter with NOTE succeeds');
                assertTrue((await dumpSchema(schema)).includes(") NOTE 'v3: it''s a memo'"),
                    'the executed NOTE is stored on the update and dumped');
                const schemaDumpLines = schemaDump.split('\n');
                assertTrue(schemaDumpLines.some((line) =>
                    line.includes(` BY #${authorKeyId}`) && line.includes(' AT {#')),
                    'dumped alter includes BY author and causal AT');

                const renderedCatalog = renderCreateCatalog(catalog.createOp);
                assertTrue(renderedCatalog.includes(`TABLEGROUP shop_prod USING SCHEMA #${schema.getId()} AT {#`), 'catalog groups pin their schema by hash');
                assertTrue(renderedCatalog.includes('BIND users => users'), 'bindings render by definition name');
                assertTrue(parseStatement(renderedCatalog).ok, 'rendered catalog parses');

                const release = {
                    action: 'release',
                    version: '1.1.0',
                    add: [{
                        name: 'extra', seedSource: 'rdb', schemaRef: schema.getId(), schemaVersion: json.toSet([schema.getId()]),
                        idProvider: 'users.identities',
                        canDeploy: { p: 'exists', table: 'grants', where: { resource: '$row.resource', grantee: '$author' } },
                    }],
                    author: authorKeyId,
                    signature: 'sig',
                } as unknown as CatalogReleasePayload;
                const renderedRelease = renderAlterCatalog(release, { catalogRef: catalog.getId(), catalogName: catalog.getName(), at: json.toSet([catalog.getId()]) });
                assertTrue(renderedRelease.includes('USING IDENTITIES users.identities'), 'rendered release uses USING IDENTITIES syntax');
                assertTrue(renderedRelease.includes('ALLOW DEPLOY IF EXISTS grants WHERE grants.resource = resource AND grants.grantee = $author'),
                    'rendered correlated deploy predicate uses qualified exists columns');
                assertTrue(renderedRelease.endsWith(`AT {#${catalog.getId()}};`), 'a release renders its insertion point by hash');
                assertTrue(parseStatement(renderedRelease).ok, 'rendered release parses');
                assertTrue(renderRowOp({ action: 'update', rowId: 'row', values: { name: 'x' } }, 'shop_prod.products').startsWith('UPDATE'), 'row op renders');

                const insert = await execute(await parseBind("INSERT INTO shop_prod.products (sku, name) VALUES ('A', 'Widget');", lang));
                assertTrue(insert.ok && insert.value.kind === 'insert', 'insert for dump succeeds');
                if (!insert.ok || insert.value.kind !== 'insert') return;

                const update = await execute(await parseBind(`UPDATE shop_prod.products SET name = 'Widget 2' WHERE rowId = '${insert.value.rowId}';`, lang));
                assertTrue(update.ok && update.value.kind === 'update', 'update for dump succeeds');

                const del = await execute(await parseBind(`DELETE FROM shop_prod.products WHERE rowId = '${insert.value.rowId}';`, lang));
                assertTrue(del.ok && del.value.kind === 'delete', 'delete for dump succeeds');

                // shop_prod has no USING IDENTITIES, so its writes are anonymous
                const dump = await dumpGroup(group);
                assertTrue(dump.startsWith('-- TABLEGROUP shop_prod USING SCHEMA'), 'a group genesis renders as a comment');
                assertTrue(!(await dumpGroup(usersGroup)).includes('CREATE TABLEGROUP'), 'no CREATE TABLEGROUP statement is rendered');
                const dumpLines = dump.split('\n');
                const hasLine = (needle: string) => dumpLines.some((line) =>
                    line.includes(needle) && !line.includes(' BY ') && line.includes(' AT {#'));
                assertTrue(dumpLines.some((line) =>
                    line.includes('INSERT INTO products') && line.includes('(uuid,') && line.includes("'A'")
                    && !line.includes(' BY ') && line.includes(' AT {#')),
                    'dumped anonymous insert includes uuid and causal AT, and no BY');
                assertTrue(hasLine(`UPDATE products SET name = 'Widget 2' WHERE rowId = #${insert.value.rowId}`), 'dumped anonymous update includes causal AT');
                assertTrue(hasLine(`DELETE FROM products WHERE rowId = #${insert.value.rowId}`), 'dumped anonymous delete includes causal AT');

                await env.run(`
                    CREATE SCHEMA test:members CREATORS ($admin) AS (
                      TABLE identities (keyId string PUB READONLY, publicKey string PUB READONLY) IDENTITY PROVIDER ALLOW insert IF true,
                      TABLE notes (body string)
                    );
                    CREATE CATALOG members_catalog VERSION '1.0.0' AS (
                      TABLEGROUP members USING SCHEMA test:members USING IDENTITIES identities
                        WITH ROWS (identities (keyId = $admin, publicKey = publicKey($admin)))
                    );
                    CREATE DATABASE members_db USING CATALOG members_catalog;
                    INSERT INTO members.notes (body) VALUES ('hi');
                `);
                const signedLines = (await dumpGroup(await env.group('members'))).split('\n');
                assertTrue(signedLines.some((line) =>
                    line.includes('INSERT INTO notes') && line.includes(` BY #${admin.keyId}`) && line.includes(' AT {#')),
                    'dumped insert of a group with USING IDENTITIES includes BY author and causal AT');
            },
        },
        {
            name: '[REST06] catalog rows take identity literals and params; publicKey() serializes; aliasMode renders $names',
            invoke: async () => {
                // The rows name $admin, who is not a catalog creator (the
                // CREATORS clause would register the key alias first).
                const admin = await newIdentity();
                const dev = await newIdentity();
                const env = await createLangEnv({ vars: { admin, dev, me: dev } });
                await env.run(`
                    CREATE SCHEMA users_schema CREATORS ($dev) AS (
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
                `);
                const schema = await env.schema('users_schema');
                const renderedSchema = renderCreateSchema(schema.createOp);
                const renderedParsed = parseStatement(renderedSchema);
                assertTrue(renderedParsed.ok, 'rendered users schema parses');
                if (renderedParsed.ok) {
                    const renderedBound = await bind(renderedParsed.value, env.lang);
                    assertTrue(renderedBound.ok, 'rendered users schema binds with keystore creator lookup');
                }
                assertTrue(renderedSchema.includes(`TABLE identities (\n    keyId string PUB READONLY,\n    publicKey string PUB READONLY,\n    name string NULL PUB\n  ) IDENTITY PROVIDER\n    ALLOW insert IF true`),
                    'rendered identities table uses multiline columns');
                assertTrue(renderedSchema.includes(`TABLE caps (\n    label string PUB READONLY,\n    grantee string PUB READONLY\n  ) CONCURRENT DELETES\n    ALLOW insert IF`),
                    'rendered caps table indents ALLOW rules after CONCURRENT DELETES');
                assertTrue(renderedSchema.includes('EXISTS caps AS c WHERE c.label'), 'self-referential EXISTS uses first-letter alias');
                assertTrue(renderedSchema.includes('grantee = $author'), 'rendered schema uses unquoted $author');
                assertTrue(!renderedSchema.includes("grantee = '$author'"), 'rendered schema does not quote $author');

                await env.run(`
                    CREATE CATALOG users_catalog VERSION '1.0.0' PARAMS (:owner identity) AS (
                      TABLEGROUP users USING SCHEMA users_schema
                        USING IDENTITIES identities
                        WITH ROWS (
                          identities (keyId = $admin, publicKey = publicKey($admin), name = 'Admin'),
                          caps (label = 'manager', grantee = $admin),
                          caps (label = 'owner', grantee = :owner)
                        )
                    );
                    CREATE DATABASE users_db USING CATALOG users_catalog WITH PARAMS (:owner = $admin);
                `);
                const catalog = await env.catalog('users_catalog');
                const def = catalog.createOp.add![0];
                assertEquals(def.initialRows?.['identities']?.[0].values['keyId'], admin.keyId, 'an identity literal resolves to its key id');
                assertEquals(def.initialRows?.['identities']?.[0].values['publicKey'], serializePublicKeyToBase64(admin.publicKey),
                    'publicKey() serializes the public key');
                assertEquals(JSON.stringify(def.initialRows?.['caps']?.[1].params), JSON.stringify({ grantee: { param: 'owner' } }),
                    'a :param stays a template slot in the catalog');

                const group = await env.group('users');
                assertEquals(group.getIdProvider(), 'identities', 'the group selects the local identity provider');
                const caps = await (await group.getTable('caps')).getView();
                assertEquals((await caps.findRowIds({ label: 'owner', grantee: admin.keyId })).length, 1,
                    'the param is filled from WITH PARAMS at instantiation');

                const aliases = new TestAliasContext(
                    new Map([[admin.keyId, 'admin']]),
                    new Map([[admin.keyId, serializePublicKeyToBase64(admin.publicKey)]]),
                );
                aliases.key(admin.keyId);
                const aliased = renderCreateCatalog(catalog.createOp, { aliasMode: true, aliases });
                assertTrue(aliased.includes('keyId=$admin'), 'aliased WITH ROWS uses $admin for keyId');
                assertTrue(aliased.includes('publicKey=publicKey($admin)'), 'aliased WITH ROWS uses publicKey($admin)');
                assertTrue(aliased.includes('grantee=$admin'), 'aliased WITH ROWS uses $admin for grantee');
                assertTrue(aliased.includes('grantee=:owner'), 'a param renders as :owner');
                const withRows = aliased.slice(aliased.indexOf('WITH ROWS'));
                assertTrue(!withRows.includes(admin.keyId), 'aliased WITH ROWS omits raw keyId literals');

                const unregistered = new TestAliasContext(
                    new Map([[admin.keyId, 'admin']]),
                    new Map([[admin.keyId, serializePublicKeyToBase64(admin.publicKey)]]),
                );
                const literal = renderCreateCatalog(catalog.createOp, { aliasMode: true, aliases: unregistered });
                assertTrue(literal.includes(`keyId='${admin.keyId}'`), 'unregistered alias keeps keyId literal');
                assertTrue(literal.includes(`publicKey='${serializePublicKeyToBase64(admin.publicKey)}'`),
                    'unregistered alias keeps publicKey literal');
            },
        },
        {
            name: '[REST07] publicKey() rejects bare key ids; params are only allowed in catalog rows',
            invoke: async () => {
                const admin = await newIdentity();
                const env = await createLangEnv({ vars: { admin, me: admin, bare: { kind: 'key-id', keyId: admin.keyId } } });

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
                env.lang.registerSchema('bare_schema', await env.ctx.createObject(schemaInit) as RSchemaImpl);

                const bare = await env.fail(`CREATE CATALOG c VERSION '1.0.0' AS (
                    TABLEGROUP bad_users USING SCHEMA bare_schema
                      WITH ROWS (identities (keyId = $bare, publicKey = publicKey($bare)))
                );`);
                assertTrue(bare.includes('publicKey() requires'), bare);

                const undeclared = await env.fail(`CREATE CATALOG c VERSION '1.0.0' AS (
                    TABLEGROUP users USING SCHEMA bare_schema WITH ROWS (identities (keyId = :who))
                );`);
                assertTrue(undeclared.includes("param ':who' is not declared"), undeclared);

                const outside = await env.fail("INSERT INTO nowhere.identities (keyId) VALUES (:who);");
                assertTrue(outside.includes('only allowed in WITH ROWS') || outside.includes('Unknown group'), outside);
            },
        },
        {
            name: '[REST08] SEED and uuid pseudo-column bind deterministically',
            invoke: async () => {
                const { lang } = await createEnv();
                const plan = async () => {
                    const result = await execute(await parseBind("CREATE DATABASE app2 SEED 'db-seed-fixed' USING CATALOG shop_catalog;", lang));
                    if (!result.ok || result.value.kind !== 'create-plan') throw new Error('database create failed');
                    return json.toStringNormalized(result.value.plan.payload as unknown as json.Literal);
                };
                assertEquals(await plan(), await plan(), 'same SEED yields the same database payload');

                const insert = await execute(await parseBind(
                    "INSERT INTO shop_prod.products (uuid, sku, name) VALUES ('row-uuid-1', 'A', 'Widget');",
                    lang,
                ));
                assertTrue(insert.ok && insert.value.kind === 'insert', 'insert with uuid succeeds');
                if (!insert.ok || insert.value.kind !== 'insert') return;
                assertEquals(insert.value.rowId, deriveRowId('row-uuid-1'), 'uuid pseudo-column fixes rowId (anonymous: shop_prod has no USING IDENTITIES)');
            },
        },
        {
            name: '[REST09] dumpDatabase full and schema profiles',
            invoke: async () => {
                const { env, db, lang } = await createEnv();
                await execute(await parseBind("INSERT INTO shop_prod.products (sku, name) VALUES ('A', 'Widget');", lang));
                await execute(await parseBind('ALTER SCHEMA shop AS (ADD COLUMN products.note string NULL);', lang));

                const loaders = dumpLoaders(env);
                const fullDump = await dumpDatabase(db, { ...loaders, mode: 'full' });
                assertTrue(fullDump.includes("SEED 'app-seed'"), 'full dump includes database SEED');
                assertTrue(fullDump.indexOf('CREATE SCHEMA shop') < fullDump.indexOf('CREATE CATALOG'), 'schemas before the catalog');
                assertTrue(fullDump.indexOf('TABLEGROUP users') < fullDump.indexOf('TABLEGROUP shop_prod'), 'users before shop_prod');
                assertTrue(fullDump.includes('BIND users => users'), 'full BIND by definition name');
                assertTrue(fullDump.indexOf('CREATE DATABASE app') < fullDump.indexOf('USE DATABASE'), 'USE DATABASE follows CREATE DATABASE');
                assertTrue(fullDump.includes('INSERT INTO shop_prod.products'), 'full dump includes row ops qualified by member name');

                const schemaDump = await dumpDatabase(db, { ...loaders, mode: 'schema' });
                assertTrue(!schemaDump.includes("SEED 'app-seed'"), 'schema dump omits database SEED');
                assertTrue(schemaDump.includes('CREATE DATABASE app USING CATALOG shop_catalog'), 'schema dump names the catalog');
                assertTrue(schemaDump.includes('TABLEGROUP shop_prod USING SCHEMA shop'), 'schema dump names the schema');
                assertTrue(!schemaDump.includes('INSERT INTO'), 'schema dump omits row ops');
                assertTrue(schemaDump.includes('CREATE SCHEMA shop'), 'schema dump includes schema DDL');
                const alterStmt = schemaDump.split('\n\n').find((s) =>
                    s.includes('ALTER SCHEMA #') && s.includes('ADD COLUMN products."note" string NULL'));
                assertTrue(alterStmt !== undefined && alterStmt.includes(' AT {#'), 'schema dump alter keeps causal AT');
            },
        },
        {
            name: '[REST10] full dumpGroup renders group-scoped ops with #groupId; deploys are comments',
            invoke: async () => {
                const { env, lang, group, usersGroup } = await createEnv();

                const insert = await execute(await parseBind("INSERT INTO shop_prod.products (sku, name) VALUES ('A', 'Widget');", lang));
                assertTrue(insert.ok && insert.value.kind === 'insert', 'insert succeeds');
                if (!insert.ok || insert.value.kind !== 'insert') return;

                const bundle = await execute(await parseBind(`BUNDLE ON shop_prod (
                    UPDATE products SET name = 'Widget 3' WHERE rowId = #${insert.value.rowId.slice(0, 10)};
                );`, lang));
                assertTrue(bundle.ok && bundle.value.kind === 'bundle', 'bundle succeeds');

                await env.run('ALTER SCHEMA shop AS (ADD COLUMN products.note string NULL);');
                await releaseAndDeploy(env, '1.1.0', 'UPDATE SCHEMA shop TO LATEST ON shop_prod');

                await env.run("INSERT INTO users.caps (label) VALUES ('x');");
                const updateRef = await execute(await parseBind('UPDATE REF users TO LATEST ON shop_prod;', lang));
                assertTrue(updateRef.ok && updateRef.value.kind === 'update-ref', 'update ref succeeds');

                const dump = await dumpGroup(group, { render: { profile: 'full' } });
                const groupTarget = `#${group.getId()}`;
                assertTrue(!dump.includes('<group>'), 'full dump does not emit <group> placeholder');
                assertTrue(dump.includes(`BUNDLE ON ${groupTarget}`), 'dumped bundle uses group id');
                assertTrue(dump.includes('-- deploy shop TO {#'), 'the deploy renders as a comment');
                assertTrue(!dump.includes('UPDATE SCHEMA'), 'no group-level UPDATE SCHEMA statement');
                assertTrue(dump.includes('UPDATE REF #') && dump.includes(` ON ${groupTarget}`), 'dumped UPDATE REF uses group id');

                for (const statement of dump.split('\n\n')) {
                    const line = statement.split('\n')[0] ?? '';
                    if (!line.startsWith('BUNDLE ON ') && !line.startsWith('UPDATE REF ')) continue;
                    const parsed = parseStatement(statement);
                    assertTrue(parsed.ok, `dumped group-scoped statement parses: ${line}`);
                }

                const rowPrefix = insert.value.rowId.slice(0, 10);
                await parseBind(`BUNDLE ON #${group.getId()} (
                    UPDATE products SET name = 'Widget 4' WHERE rowId = #${rowPrefix};
                );`, lang);
                await parseBind(`UPDATE REF #${usersGroup.getId()} TO LATEST ON #${group.getId()};`, lang);
            },
        },
        {
            name: '[REST11] aliasMode dump uses aliases not raw hashes, and replays',
            invoke: async () => {
                const { env, db, admin, schema, group, usersGroup, catalog } = await createEnv();
                await env.run("INSERT INTO shop_prod.products (sku, name) VALUES ('A', 'Widget');");
                await env.run('ALTER SCHEMA shop AS (ADD COLUMN products.note string NULL) BY $admin;');
                await releaseAndDeploy(env, '1.1.0', 'UPDATE SCHEMA shop TO LATEST ON shop_prod', 'app', 'BY $admin');

                const aliases = new TestAliasContext(new Map([[admin.keyId, 'admin']]));
                const dump = await dumpDatabase(db, {
                    ...dumpLoaders(env),
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
                assertTrue(dump.includes('BIND users => users'), 'BIND names the catalog definition');
                assertTrue(dump.includes(`\\alias catalog shop_catalog #${catalog.getId()}`), 'the catalog is aliased');
                assertTrue(dump.includes('USING CATALOG shop_catalog AT {shop_catalog_ver'), 'the release selection uses a version alias');
                assertTrue(dump.includes('USING SCHEMA shop AT {shop_ver'), 'catalog pins use the schema alias');
            },
        },
        {
            name: '[REST12] precise column types: render round-trip, canonical value fidelity, bind-time reject',
            invoke: async () => {
                const { env, lang } = await createEnv();

                await env.run(`
                    CREATE SCHEMA finance CREATORS ($admin) AS (
                      TABLE ledger (
                        seq bigint PUB READONLY,
                        memo string(8),
                        amount decimal(18, 2),
                        qty integer MIN 0 MAX 100
                      ) ALLOW all IF true
                    );
                `);
                const schema = await env.schema('finance');

                // reverse render preserves type parameters and MIN/MAX modifiers, and re-parses
                const rendered = renderCreateSchema(schema.createOp);
                assertTrue(rendered.includes('seq bigint'), 'rendered keeps bigint type');
                assertTrue(rendered.includes('memo string(8)'), 'rendered keeps string(8) length param');
                assertTrue(rendered.includes('amount decimal(18, 2)'), 'rendered keeps decimal(precision, scale)');
                assertTrue(rendered.includes('qty integer') && rendered.includes('MIN ') && rendered.includes('MAX '),
                    'rendered keeps MIN/MAX modifiers');
                assertTrue(parseStatement(rendered).ok, 'rendered precise schema re-parses');

                await env.run(`
                    CREATE CATALOG fin VERSION '1.0.0' AS (TABLEGROUP fin_prod USING SCHEMA finance);
                    CREATE DATABASE fin_db USING CATALOG fin;
                `);

                // canonical string carriers survive a full insert -> select round-trip exactly
                const insert = await execute(await parseBind(
                    "INSERT INTO fin_prod.ledger (seq, memo, amount, qty) VALUES ('9', 'hi', '9.99', 5);", lang));
                assertTrue(insert.ok && insert.value.kind === 'insert', 'canonical insert succeeds');

                const select = await execute(await parseBind(
                    "SELECT seq, amount, qty FROM fin_prod.ledger WHERE seq = '9';", lang));
                assertTrue(select.ok && select.value.kind === 'select', 'bigint pub-eq select succeeds');
                if (select.ok && select.value.kind === 'select') {
                    assertEquals(select.value.rows.length, 1, 'pub-eq lookup finds the row');
                    assertEquals(select.value.rows[0].values['seq'], '9', 'bigint carrier round-trips exactly');
                    assertEquals(select.value.rows[0].values['amount'], '9.99', 'decimal carrier round-trips exactly');
                    assertEquals(select.value.rows[0].values['qty'], 5, 'integer round-trips');
                }

                // bind-time canonical encoding rejects non-canonical numeric literals (reject, never round)
                const overScale = parseStatement("INSERT INTO fin_prod.ledger (seq, amount) VALUES ('1', '9.999');");
                assertTrue(overScale.ok, 'over-scale insert parses');
                if (overScale.ok) assertTrue(!(await bind(overScale.value, lang)).ok, 'over-scale decimal rejected at bind');

                const badBigint = parseStatement("INSERT INTO fin_prod.ledger (seq, amount) VALUES ('1.5', '1.00');");
                assertTrue(badBigint.ok, 'non-integer bigint insert parses');
                if (badBigint.ok) assertTrue(!(await bind(badBigint.value, lang)).ok, 'non-integer bigint rejected at bind');

                // Tier-1 constraint pre-check: maxLength / range now fail at BIND with the
                // engine's message, instead of only surfacing later at execute.
                const expectBindReject = async (sql: string, why: string, msgIncludes: string) => {
                    const parsed = parseStatement(sql);
                    assertTrue(parsed.ok, `${why}: parses`);
                    if (!parsed.ok) return;
                    const bound = await bind(parsed.value, lang);
                    assertTrue(!bound.ok, `${why}: rejected at bind`);
                    if (!bound.ok) {
                        assertTrue(bound.diagnostics.some((d) => d.message.includes(msgIncludes)),
                            `${why}: diagnostic should include '${msgIncludes}', got: ${bound.diagnostics.map((d) => d.message).join(' | ')}`);
                    }
                };
                await expectBindReject(
                    "INSERT INTO fin_prod.ledger (seq, memo, amount) VALUES ('1', 'toolongmemo', '1.00');",
                    'over-length string', "column 'memo' (string): string length 11 exceeds maxLength 8");
                await expectBindReject(
                    "INSERT INTO fin_prod.ledger (seq, amount, qty) VALUES ('1', '1.00', 200);",
                    'out-of-range integer', "column 'qty' (integer): integer 200 is out of range [0, 100]");

                // an UPDATE path is covered by the same shared encoder too
                await expectBindReject(
                    "UPDATE fin_prod.ledger SET memo = 'toolongmemo' WHERE rowId = '#deadbeef';",
                    'over-length string on update', "column 'memo' (string): string length 11 exceeds maxLength 8");
            },
        },
        {
            name: '[REST13] JSON literals: insert/update json objects, dumped row ops re-parse and bind to the same values',
            invoke: async () => {
                const { env, lang } = await createEnv();

                await env.run(`
                    CREATE SCHEMA docs CREATORS ($admin) AS (
                      TABLE notes (
                        slug string PUB READONLY,
                        body json DEFAULT JSON '{"tags":[],"v":0}'
                      ) ALLOW all IF true
                    );
                    CREATE CATALOG docs_catalog VERSION '1.0.0' AS (TABLEGROUP docs_prod USING SCHEMA docs);
                    CREATE DATABASE docs_db USING CATALOG docs_catalog;
                `);
                const group = await env.group('docs_prod');

                const inserted: json.Literal = { tags: ["it's", 'a"b', 'back\\slash', 'line\nbreak\ttab'], seq: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11] };
                const updated: json.Literal = [{ k: 'v' }, 'x', -2.5, true];

                const insert = await execute(await parseBind(
                    `INSERT INTO docs_prod.notes (slug, body) VALUES ('n1', ${renderLiteral(inserted)});`, lang));
                assertTrue(insert.ok && insert.value.kind === 'insert', 'json object insert succeeds');
                if (!insert.ok || insert.value.kind !== 'insert') return;

                const readBody = async () => {
                    const select = await execute(await parseBind("SELECT body FROM docs_prod.notes WHERE slug = 'n1';", lang));
                    if (!select.ok || select.value.kind !== 'select' || select.value.rows.length !== 1) return undefined;
                    return select.value.rows[0].values['body'];
                };
                const canon = (value: json.Literal | undefined) => value === undefined ? '<missing>' : json.toStringCanonical(value);
                assertEquals(canon(await readBody()), canon(inserted), 'inserted json object reads back exactly');

                const update = await execute(await parseBind(
                    `UPDATE docs_prod.notes SET body = JSON '${json.toStringCanonical(updated)}' WHERE rowId = '${insert.value.rowId}';`, lang));
                assertTrue(update.ok && update.value.kind === 'update', 'json array update succeeds');
                assertEquals(canon(await readBody()), canon(updated), 'updated json array reads back exactly');

                const dump = await dumpGroup(group, { render: { profile: 'full' } });
                // The group genesis is a comment line, which the splitter keeps
                // at the head of the statement that follows it.
                const statements = splitStatements(dump).map((s) => s.replace(/^(\s*--[^\n]*\n)+/, ''));
                const rowOps = statements.filter((s) => /^\s*(INSERT|UPDATE) /.test(s) && s.includes('body'));
                assertEquals(rowOps.length, 2, 'dump holds the insert and update row ops');
                assertTrue(rowOps.every((s) => s.includes("body") && s.includes("JSON '")), 'dumped json values use JSON literals');

                const expected = [inserted, updated];
                for (let i = 0; i < rowOps.length; i++) {
                    const replay = rowOps[i]
                        .replace(/^(\s*(?:INSERT INTO|UPDATE)) notes /, '$1 docs_prod.notes ')
                        .replace(/ BY #\S+/, '')
                        .replace(/ AT \{[^}]*\}/, '');
                    const bound = await parseBind(replay, lang);
                    assertTrue(bound.kind === 'insert' || bound.kind === 'update', `dumped row op ${i} binds as a row op`);
                    if (bound.kind === 'insert' || bound.kind === 'update') {
                        assertEquals(canon(bound.values['body']), canon(expected[i]), `dumped row op ${i} carries the original json value`);
                    }
                }
            },
        },
        {
            name: '[REST14] a TABLEGROUP without USING IDENTITIES takes no BY, and drops the session author',
            invoke: async () => {
                const { lang, admin } = await createEnv();
                const bindFailure = async (sql: string): Promise<string> => {
                    const parsed = parseStatement(sql);
                    if (!parsed.ok) throw new Error(parsed.diagnostics[0].message);
                    const bound = await bind(parsed.value, lang);
                    assertTrue(!bound.ok, `bind should fail: ${sql}`);
                    return bound.ok ? '' : bound.diagnostics[0].message;
                };

                const noBy = 'TABLEGROUP shop_prod has no USING IDENTITIES; its writes are anonymous, so it takes no BY';
                assertEquals(await bindFailure("INSERT INTO shop_prod.products (sku, name) VALUES ('A', 'x') BY $admin;"), noBy,
                    'BY on an insert is an error');
                assertEquals(await bindFailure(`UPDATE shop_prod.products SET name = 'y' WHERE rowId = '${deriveRowId('A')}' BY $admin;`), noBy,
                    'BY on an update is an error');
                assertEquals(await bindFailure("BUNDLE ON shop_prod (INSERT INTO products (sku, name) VALUES ('B', 'y');) BY $admin;"), noBy,
                    'BY on a bundle is an error');
                assertEquals(await bindFailure('UPDATE REF users TO LATEST ON shop_prod BY $admin;'),
                    'TABLEGROUP shop_prod has no USING IDENTITIES; its ref updates are anonymous, so it takes no BY',
                    'BY on an UPDATE REF is an error');

                const insert = await parseBind("INSERT INTO shop_prod.products (sku, name) VALUES ('A', 'x');", lang);
                assertTrue(insert.kind === 'insert' && insert.author === undefined, 'without BY, the session author is dropped');
                const nobody = await parseBind("INSERT INTO shop_prod.products (sku, name) VALUES ('B', 'x') BY NOBODY;", lang);
                assertTrue(nobody.kind === 'insert' && nobody.author === undefined, 'BY NOBODY stays anonymous');
                const updateRef = await parseBind('UPDATE REF users TO LATEST ON shop_prod;', lang);
                assertTrue(updateRef.kind === 'update-ref' && updateRef.author === undefined, 'an UPDATE REF of an ungated binding is anonymous');

                const authorValue = await bindFailure("INSERT INTO shop_prod.products (sku, name) VALUES ('C', $author);");
                assertTrue(authorValue.includes('$author has no value: the statement is anonymous'), authorValue);
                const me = await parseBind("INSERT INTO shop_prod.products (sku, name) VALUES ('D', $me);", lang);
                assertTrue(me.kind === 'insert' && me.values['name'] === admin.keyId, '$me still names the session identity');
            },
        },
    ],
};
