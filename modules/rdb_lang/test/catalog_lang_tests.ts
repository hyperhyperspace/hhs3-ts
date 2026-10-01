import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import {
    catalogFilesHash, catalogGroupHash,
    type RBlobStoreImpl, type RFileMapImpl, type RSchemaImpl, type RTableGroupImpl,
} from "@hyper-hyper-space/hhs3_rdb";

import { bind } from "../src/bind/bind.js";
import { execute } from "../src/exec/execute.js";
import { parseScript, parseStatement } from "../src/syntax/parser.js";
import { renderSourceCatalog, renderSourceFiles, renderSourceSchema, type SourceGroup } from "../src/reverse/source.js";
import { dumpCatalog, dumpDatabase } from "../src/reverse/dump.js";
import { createLangEnv, LangEnv, newIdentity } from "./lang_env.js";

const USERS_SCHEMA = `
CREATE SCHEMA users_schema CREATORS ($dev) AS (
  TABLE identities (
    keyId string PUB READONLY,
    publicKey string PUB READONLY,
    name string NULL PUB
  ) IDENTITY PROVIDER ALLOW insert IF true,
  TABLE caps (
    label string PUB READONLY,
    grantee string PUB READONLY,
    memo string NULL
  ) CONCURRENT DELETES
    ALLOW insert IF EXISTS caps AS c WHERE c.label = 'manager' AND c.grantee = $author
);
CREATE SCHEMA docs_schema CREATORS ($dev) AS (
  TABLE docs (
    body string
  ) ALLOW insert IF EXISTS users.caps WHERE label = 'writer' AND grantee = $author
);`;

const APP_CATALOG = `
CREATE CATALOG app CREATORS ($dev) VERSION '1.0.0' PARAMS (:admin identity) AS (
  TABLEGROUP users USING SCHEMA users_schema AT LATEST
    USING IDENTITIES identities
    WITH ROWS (
      identities (keyId = :admin, publicKey = publicKey(:admin), name = 'Admin'),
      caps (label = 'manager', grantee = :admin)
    ),
  TABLEGROUP docs USING SCHEMA docs_schema
    BIND users => users USING IDENTITIES users.identities
) NOTE 'initial' BY $dev;`;

const FILES_CATALOG = `
CREATE CATALOG app CREATORS ($dev) VERSION '1.0.0' PARAMS (:admin identity) AS (
  TABLEGROUP users USING SCHEMA users_schema AT LATEST
    USING IDENTITIES identities
    WITH ROWS (
      identities (keyId = :admin, publicKey = publicKey(:admin), name = 'Admin'),
      caps (label = 'manager', grantee = :admin)
    ),
  TABLEGROUP docs USING SCHEMA docs_schema
    BIND users => users USING IDENTITIES users.identities,
  FILES media
    USING IDENTITIES users.identities
    ALLOW WRITE IF EXISTS users.caps WHERE users.caps.label = 'writer' AND users.caps.grantee = $author
) NOTE 'initial' BY $dev;`;

type Actors = { dev: OwnIdentity; admin: OwnIdentity; alice: OwnIdentity };

async function filesEnv(a: Actors): Promise<LangEnv> {
    const env = await createLangEnv({ vars: { ...a, me: a.dev } });
    await env.run(USERS_SCHEMA + FILES_CATALOG);
    return env;
}

async function actors(): Promise<Actors> {
    return { dev: await newIdentity(), admin: await newIdentity(), alice: await newIdentity() };
}

async function appEnv(a: Actors): Promise<LangEnv> {
    const env = await createLangEnv({ vars: { ...a, me: a.dev } });
    await env.run(USERS_SCHEMA + APP_CATALOG);
    return env;
}

async function frontierKey(object: { getScopedDag(): Promise<{ getFrontier(): Promise<Set<B64Hash>> }> }): Promise<string> {
    return [...await (await object.getScopedDag()).getFrontier()].sort().join(',');
}

function loaders(env: LangEnv) {
    return {
        loadSchema: async (id: B64Hash) => (await env.ctx.getObject(id)) as RSchemaImpl,
        loadGroup: async (id: B64Hash) => (await env.ctx.getObject(id)) as RTableGroupImpl,
    };
}

export const catalogLangTests = {
    title: '[RDB_LANG:CATALOG] Catalogs, releases and deploys',
    tests: [
        {
            name: '[CLANG01] release, deploy and adopt a schema change end to end; the dump replays to identical DAGs',
            invoke: async () => {
                const a = await actors();
                const env = await appEnv(a);
                env.vars['me'] = a.admin;
                await env.run(`
                    CREATE DATABASE prod USING CATALOG app AT '1.0.0' CREATORS ($admin) WITH PARAMS (:admin = $admin) BY $admin;
                    INSERT INTO users.identities (keyId, publicKey, name) VALUES ($alice, publicKey($alice), 'Alice');
                    INSERT INTO users.caps (label, grantee) VALUES ('writer', $alice);
                    UPDATE REF users TO LATEST ON docs;
                `);
                env.vars['me'] = a.alice;
                await env.run("INSERT INTO docs.docs (body) VALUES ('hello');");

                env.vars['me'] = a.dev;
                const [, released] = await env.run(`
                    ALTER SCHEMA docs_schema AS (ADD COLUMN docs.status string DEFAULT 'draft');
                    ALTER CATALOG app VERSION '1.1.0' AS (UPDATE SCHEMA docs_schema TO LATEST ON docs) NOTE 'status' BY $dev;
                `);
                assertTrue(released.kind === 'alter-catalog' && released.declare === undefined, 'no new schema: no declare');

                env.vars['me'] = a.admin;
                const [deployed] = await env.run("UPDATE CATALOG app TO '1.1.0' ON prod BY $admin;");
                assertTrue(deployed.kind === 'update-catalog' && deployed.update.deployed.map((d) => d.name).join() === 'docs',
                    'the planner deploys the docs group');
                env.vars['me'] = a.alice;
                await env.run("INSERT INTO docs.docs (body, status) VALUES ('second', 'final');");

                const db = await env.database('prod');
                const dump = await dumpDatabase(db, loaders(env));
                const replay = await createLangEnv({ vars: { ...a, me: a.dev } });
                await replay.run(dump);
                assertEquals((await replay.database('prod')).getId(), db.getId(), 'same database id');
                assertEquals(await frontierKey(await replay.group('prod.docs')), await frontierKey(await env.group('prod.docs')),
                    'the docs group replays to the same DAG, deploys included');
                assertEquals(await frontierKey(await replay.catalog('app')), await frontierKey(await env.catalog('app')),
                    'the catalog replays to the same DAG');
            },
        },
        {
            name: '[CLANG02] a trailing AT naming two maximal releases makes a merge; conflicting groups must be resolved',
            invoke: async () => {
                const a = await actors();
                const env = await appEnv(a);
                const catalog = await env.catalog('app');
                const genesis = catalog.getId();
                await env.run(`
                    ALTER SCHEMA docs_schema AS (ADD COLUMN docs.a string NULL);
                    ALTER CATALOG app VERSION '1.1.0' AS (UPDATE SCHEMA docs_schema TO LATEST ON docs);
                    ALTER SCHEMA docs_schema AS (ADD COLUMN docs.b string NULL);
                `);
                const [second] = await env.run(`ALTER CATALOG app VERSION '1.1.1' AS (UPDATE SCHEMA docs_schema TO LATEST ON docs) AT {#${genesis}};`);
                if (second.kind !== 'alter-catalog') throw new Error('expected a release');
                const index = await catalog.getIndex();
                const frontier = await (await catalog.getScopedDag()).getFrontier();
                assertEquals(index.maximalReleasesAt(frontier).length, 2, 'two concurrent releases');

                const unresolved = await env.fail("ALTER CATALOG app VERSION '1.2.0';");
                assertTrue(unresolved.includes("disagree on the version of 'docs'"), unresolved);

                const [merged] = await env.run("ALTER CATALOG app VERSION '1.2.0' AS (UPDATE SCHEMA docs_schema TO LATEST ON docs) AT LATEST;");
                if (merged.kind !== 'alter-catalog') throw new Error('expected a release');
                const parents = (await catalog.getIndex()).releaseState(merged.release).parents;
                assertEquals(parents.length, 2, 'the merge has both releases as parents');

                const tooLow = await env.fail("ALTER CATALOG app VERSION '1.1.5' AT LATEST;");
                assertTrue(tooLow.includes('must be greater than the parent release'), tooLow);
            },
        },
        {
            name: '[CLANG03] a semver or LATEST selection matching two releases is ambiguous; a hash selects one',
            invoke: async () => {
                const a = await actors();
                const env = await appEnv(a);
                const catalog = await env.catalog('app');
                const genesis = catalog.getId();
                await env.run(`
                    CREATE SCHEMA notes_schema CREATORS ($dev) AS (TABLE notes (text string) ALLOW all IF true);
                    ALTER CATALOG app VERSION '1.1.0' AS (ADD TABLEGROUP notes USING SCHEMA notes_schema);
                `);
                const [other] = await env.run(`ALTER CATALOG app VERSION '1.1.0' AS (ADD TABLEGROUP more USING SCHEMA notes_schema) AT {#${genesis}};`);
                if (other.kind !== 'alter-catalog') throw new Error('expected a release');

                env.vars['me'] = a.admin;
                const semver = await env.fail("CREATE DATABASE d USING CATALOG app AT '1.1.0' WITH PARAMS (:admin = $admin);");
                assertTrue(semver.includes("release '1.1.0' is ambiguous"), semver);
                const latest = await env.fail('CREATE DATABASE d USING CATALOG app WITH PARAMS (:admin = $admin);');
                assertTrue(latest.includes('2 latest releases'), latest);

                await env.run(`CREATE DATABASE d USING CATALOG app AT {#${other.release}} WITH PARAMS (:admin = $admin);`);
                const names = [...(await (await env.database('d')).getMemberGroupNames()).keys()].sort();
                assertEquals(names.join(','), 'docs,more,users', 'the hash selects the release that adds `more`');
                const notARelease = await env.fail(`UPDATE CATALOG app TO {#${other.declare ?? genesis}} ON d;`);
                assertTrue(notARelease.includes('is not a release') || notARelease.includes('already set') || notARelease.includes('move forward'),
                    notARelease);
            },
        },
        {
            name: '[CLANG04] a catalog dump with two concurrent releases sharing a version replays to the same release DAG',
            invoke: async () => {
                const a = await actors();
                const env = await appEnv(a);
                const catalog = await env.catalog('app');
                await env.run(`
                    CREATE SCHEMA notes_schema CREATORS ($dev) AS (TABLE notes (text string) ALLOW all IF true);
                    ALTER CATALOG app VERSION '1.1.0' AS (ADD TABLEGROUP notes USING SCHEMA notes_schema);
                    ALTER CATALOG app VERSION '1.1.0' AS (ADD TABLEGROUP more USING SCHEMA notes_schema) AT {#${catalog.getId()}};
                    ALTER CATALOG app VERSION '1.2.0' AT LATEST;
                `);
                const dump = await dumpCatalog(catalog, { loadSchema: loaders(env).loadSchema });
                assertEquals(dump.split('\n').filter((l) => l.startsWith('ALTER CATALOG')).length, 3, 'three releases, no declare statements');
                assertTrue(!dump.includes('-- declare'), 'declares are implied, not rendered');

                const replay = await createLangEnv({ vars: { ...a, me: a.dev } });
                await replay.run(dump);
                const replayed = await replay.catalog('app');
                const entries = async (c: typeof catalog) => {
                    const hashes: B64Hash[] = [];
                    for await (const entry of (await c.getScopedDag()).loadAllEntries()) hashes.push(entry.hash);
                    return hashes.sort().join(',');
                };
                assertEquals(await entries(replayed), await entries(catalog), 'every entry, declares included, replays to the same hash');
            },
        },
        {
            name: '[CLANG05] params: declared, typed, complete, and not re-set',
            invoke: async () => {
                const a = await actors();
                const env = await appEnv(a);
                env.vars['me'] = a.admin;
                const missing = await env.fail('CREATE DATABASE d USING CATALOG app;');
                assertTrue(missing.includes('the release needs :admin'), missing);
                const undeclared = await env.fail('CREATE DATABASE d USING CATALOG app WITH PARAMS (:admin = $admin, :other = 1);');
                assertTrue(undeclared.includes("param ':other' is not declared"), undeclared);
                const wrongType = await env.fail("CREATE DATABASE d USING CATALOG app WITH PARAMS (:admin = 'text');");
                assertTrue(wrongType.includes('CREATORS') || wrongType.includes('keystore') || wrongType.includes('identity'), wrongType);

                await env.run('CREATE DATABASE d USING CATALOG app WITH PARAMS (:admin = $admin);');
                env.vars['me'] = a.dev;
                await env.run("ALTER CATALOG app VERSION '1.1.0' PARAMS (:limit integer);");
                const reset = await env.fail("UPDATE CATALOG app TO '1.1.0' ON d WITH PARAMS (:admin = $alice, :limit = 3);");
                assertTrue(reset.includes("param ':admin' is already set"), reset);
                const newMissing = await env.fail("UPDATE CATALOG app TO '1.1.0' ON d;");
                assertTrue(newMissing.includes('the release needs :limit'), newMissing);
                const [ok] = await env.run("UPDATE CATALOG app TO '1.1.0' ON d WITH PARAMS (:limit = 3);");
                assertTrue(ok.kind === 'update-catalog' && ok.update.commit !== undefined, 'the new param is supplied by the deploy');
                const redeclared = await env.fail("ALTER CATALOG app VERSION '1.2.0' PARAMS (:limit integer);");
                assertTrue(redeclared.includes('already declared'), redeclared);
            },
        },
        {
            name: '[CLANG06] deploy authority: the default is the database creators; ALLOW DEPLOY IF $author needs USING IDENTITIES',
            invoke: async () => {
                const a = await actors();
                const env = await appEnv(a);
                env.vars['me'] = a.admin;
                await env.run('CREATE DATABASE d USING CATALOG app CREATORS ($admin) WITH PARAMS (:admin = $admin);');
                env.vars['me'] = a.dev;
                await env.run(`
                    ALTER SCHEMA docs_schema AS (ADD COLUMN docs.tag string NULL);
                    ALTER CATALOG app VERSION '1.1.0' AS (UPDATE SCHEMA docs_schema TO LATEST ON docs);
                `);
                const stranger = await env.fail("UPDATE CATALOG app TO '1.1.0' ON d BY $alice;");
                assertTrue(stranger.length > 0, 'a non-creator cannot deploy into the database');
                const [ok] = await env.run("UPDATE CATALOG app TO '1.1.0' ON d BY $admin;");
                assertTrue(ok.kind === 'update-catalog' && ok.update.deployed.length === 1, 'a creator deploys');

                const noProvider = await env.fail(`ALTER CATALOG app VERSION '1.2.0' AS (
                    ADD TABLEGROUP gated USING SCHEMA users_schema ALLOW DEPLOY IF EXISTS caps WHERE grantee = $author
                );`);
                assertTrue(noProvider.includes('needs USING IDENTITIES'), noProvider);
                await env.run(`ALTER CATALOG app VERSION '1.2.0' AS (
                    ADD TABLEGROUP gated USING SCHEMA users_schema USING IDENTITIES identities
                      ALLOW DEPLOY IF EXISTS caps WHERE grantee = $author
                );`);
            },
        },
        {
            name: '[CLANG07] UPDATE CATALOG checks the database catalog; USE DATABASE scopes bare group names',
            invoke: async () => {
                const a = await actors();
                const env = await appEnv(a);
                env.vars['me'] = a.dev;
                await env.run(`
                    CREATE SCHEMA misc CREATORS ($dev) AS (TABLE notes (text string) ALLOW all IF true);
                    CREATE CATALOG other VERSION '1.0.0' AS (TABLEGROUP users USING SCHEMA misc);
                `);
                env.vars['me'] = a.admin;
                await env.run(`
                    CREATE DATABASE east USING CATALOG app WITH PARAMS (:admin = $admin);
                    CREATE DATABASE west USING CATALOG other;
                `);
                const mismatch = await env.fail("UPDATE CATALOG other TO LATEST ON east;");
                assertTrue(mismatch.includes('uses catalog'), mismatch);

                const [west] = await env.run('SELECT text FROM users.notes;');
                assertTrue(west.kind === 'select', 'the current database (west) resolves users');
                await env.run('USE DATABASE east;');
                const [east] = await env.run('SELECT name FROM users.identities;');
                assertTrue(east.kind === 'select' && east.rows.length === 1, 'after USE DATABASE east, users is the east group');
                const [qualified] = await env.run('SELECT text FROM west.users.notes;');
                assertTrue(qualified.kind === 'select', 'db.group.table resolves regardless of the current database');
            },
        },
        {
            name: '[CLANG08] schema versions: defaults, explicit VERSION, ordering, and the higher version wins a concurrent slot',
            invoke: async () => {
                const a = await actors();
                const env = await createLangEnv({ vars: { ...a, me: a.dev } });
                await env.run(`
                    CREATE SCHEMA plain CREATORS ($dev) AS (TABLE t (x string) ALLOW all IF true);
                    CREATE SCHEMA pinned CREATORS ($dev) VERSION '1.0.0' AS (TABLE t (x string) ALLOW all IF true);
                `);
                const plain = await env.schema('plain');
                const pinned = await env.schema('pinned');
                assertEquals(plain.createOp.version, '0.0.1', 'CREATE SCHEMA defaults to 0.0.1');
                assertEquals(pinned.createOp.version, '1.0.0', 'an explicit VERSION is kept');

                await env.run("ALTER SCHEMA pinned AS (ADD COLUMN t.y string NULL);");
                assertEquals((await pinned.getView()).getVersions().join(','), '1.0.1', 'ALTER SCHEMA defaults to the next patch');
                await env.run("ALTER SCHEMA pinned VERSION '1.2.0' AS (ADD COLUMN t.z string NULL);");
                assertEquals((await pinned.getView()).getVersions().join(','), '1.2.0', 'an explicit VERSION is kept on updates');

                const tooLow = await env.fail("ALTER SCHEMA pinned VERSION '1.1.0' AS (ADD COLUMN t.w string NULL);");
                assertTrue(tooLow.includes('must be greater'), tooLow);
                const notSemver = await env.fail("ALTER SCHEMA pinned VERSION 'v2' AS (ADD COLUMN t.w string NULL);");
                assertTrue(notSemver.includes('not a semver'), notSemver);

                // two concurrent updates to the same slot: the higher version decides
                const base = [...await (await pinned.getScopedDag()).getFrontier()];
                const at = `{${base.map((h) => `#${h}`).join(', ')}}`;
                await env.run(`ALTER SCHEMA pinned VERSION '1.3.0' AS (SET CONCURRENT DELETES t true) AT ${at};`);
                await env.run(`ALTER SCHEMA pinned VERSION '2.0.0' AS (SET CONCURRENT DELETES t false) AT ${at};`);
                const merged = await pinned.getView();
                assertEquals(merged.getVersions().join(','), '2.0.0,1.3.0', 'a two-headed version lists both, highest first');
                assertEquals(merged.getConcurrentDeletes('t'), false, 'the write from the higher version wins the slot');
            },
        },
        {
            name: '[CLANG09] source mode: CREATE CATALOG without VERSION binds from defaultCatalogVersion',
            invoke: async () => {
                const a = await actors();
                const env = await createLangEnv({ vars: { ...a, me: a.dev } });
                await env.run(USERS_SCHEMA);
                const sql = `
                    CREATE CATALOG app CREATORS ($dev) PARAMS (:admin identity) AS (
                      TABLEGROUP users USING SCHEMA users_schema USING IDENTITIES identities
                    );`;
                const parsed = parseStatement(sql, { catalogVersionOptional: true });
                assertTrue(parsed.ok, 'source mode parses the catalog without VERSION');
                if (!parsed.ok) return;

                const unsupplied = await bind(parsed.value, env.lang);
                assertTrue(!unsupplied.ok && unsupplied.diagnostics[0]!.message.includes('requires VERSION'),
                    'without a default the binder asks for a version');

                const sourceContext = { ...env.lang, defaultCatalogVersion: async () => '1.4.0' };
                const bound = await bind(parsed.value, sourceContext);
                assertTrue(bound.ok && bound.value.kind === 'create-catalog' && bound.value.version === '1.4.0',
                    'the bind context supplies the version');
                if (!bound.ok) return;
                const executed = await execute(bound.value);
                assertTrue(executed.ok && executed.value.kind === 'create-plan' && executed.value.plan.kind === 'create-catalog'
                    && (executed.value.plan.payload as { version: string }).version === '1.4.0',
                    'the compiled genesis carries the supplied version');
            },
        },
        {
            name: '[CLANG10] source form reads back to the same schemas and group definitions; body spans',
            invoke: async () => {
                const a = await actors();
                const env = await appEnv(a);
                const labels = { keyId: (k: string) => (k === a.dev.keyId ? 'dev' : undefined) };

                const schemaSource = async (name: string) => {
                    const view = await (await env.schema(name)).getView();
                    return renderSourceSchema({
                        name, creators: view.getCreators(), tables: view.getTableNames().map((t) => view.getTable(t)!),
                    }, labels);
                };
                const catalogView = await (await env.catalog('app')).getView();
                const genesis = catalogView.getRelease(catalogView.getGenesisHash())!;
                const groups: SourceGroup[] = [];
                for (const def of genesis.defs.values()) {
                    const bindings = Object.fromEntries(Object.entries(def.bindings ?? {}).map(([alias, hash]) => [alias, genesis.defs.get(hash)!.name]));
                    groups.push({
                        name: def.name,
                        schema: ((await env.ctx.getObject(def.schemaRef)) as RSchemaImpl).getName(),
                        bindings,
                        ...(def.idProvider !== undefined ? { idProvider: def.idProvider } : {}),
                        ...(def.initialRows !== undefined ? { initialRows: def.initialRows } : {}),
                    });
                }
                const catalogSource = renderSourceCatalog({
                    name: 'app', creators: catalogView.getCreators(), params: [...genesis.params.values()], groups,
                }, labels);
                const source = `${await schemaSource('users_schema')}\n\n${await schemaSource('docs_schema')}\n\n${catalogSource}\n`;
                assertTrue(!source.includes('VERSION') && !source.includes(' AT ') && !source.includes(' BY '), 'no VERSION, AT or BY');
                assertTrue(source.includes('CREATORS ($dev)'), 'creators render as labels');

                const parsed = parseScript(source, { catalogVersionOptional: true });
                assertTrue(parsed.ok, 'the source parses in source mode');
                if (!parsed.ok) return;
                const [users, docs, catalog] = parsed.value.statements;
                assertTrue(users!.kind === 'create-schema' && docs!.kind === 'create-schema' && catalog!.kind === 'create-catalog', 'three statements');
                if (users!.kind !== 'create-schema' || catalog!.kind !== 'create-catalog') return;
                const slice = (span: { start: number; end: number }) => source.slice(span.start, span.end);
                assertTrue(slice(users.body).startsWith('AS (') && slice(users.body).endsWith(')'), 'the schema body spans AS ( ... )');
                assertTrue(slice(users.tables[0]!.body).startsWith('(') && slice(users.tables[0]!.body).endsWith(')'), 'a table body spans its column list');
                assertTrue(slice(catalog.body).startsWith('AS (') && slice(catalog.body).endsWith(')'), 'the catalog body spans AS ( ... )');

                const again = await createLangEnv({ vars: { ...a, me: a.dev } });
                await again.run(`${slice(users.span)};\n${slice(docs!.span)};`);
                for (const name of ['users_schema', 'docs_schema']) {
                    assertEquals((await again.schema(name)).getId(), (await env.schema(name)).getId(), `${name} reads back to the same create`);
                }
                const bound = await bind(catalog, { ...again.lang, defaultCatalogVersion: async () => '1.0.0' });
                assertTrue(bound.ok && bound.value.kind === 'create-catalog', 'the catalog binds');
                if (!bound.ok || bound.value.kind !== 'create-catalog') return;
                assertEquals(bound.value.add.map(catalogGroupHash).join(','), [...genesis.defs.keys()].join(','), 'the groups read back to the same definitions');
            },
        },
        {
            name: '[CLANG11] a group or WITH ROWS the catalog would reject fails at bind, located at the row or the group',
            invoke: async () => {
                const a = await actors();
                const env = await createLangEnv({ vars: { ...a, me: a.dev } });
                await env.run(USERS_SCHEMA);
                const rejection = async (lines: string[]) => {
                    const parsed = parseStatement(lines.join('\n'));
                    if (!parsed.ok) throw new Error(`expected the statement to parse: ${parsed.diagnostics[0]?.message}`);
                    const bound = await bind(parsed.value, env.lang);
                    if (bound.ok) throw new Error('expected a bind failure');
                    return bound.diagnostics[0]!;
                };
                const catalog = (...groups: string[][]) => [
                    "CREATE CATALOG app CREATORS ($dev) VERSION '1.0.0' PARAMS (:admin identity) AS (",
                    groups.map((group) => group.join('\n')).join(',\n'),
                    ');',
                ];
                const users = (...rows: string[]) => [
                    '  TABLEGROUP users USING SCHEMA users_schema',
                    '    USING IDENTITIES identities',
                    '    WITH ROWS (',
                    ["      identities (keyId = :admin, publicKey = publicKey(:admin), name = 'Admin')", ...rows].join(',\n'),
                    '    )',
                ];

                const missing = await rejection(catalog(users("      caps (label = 'manager', grantee = :admin)", '      caps (grantee = :admin)')));
                assertEquals(missing.code, 'VALIDATION_REJECTED', 'a missing NOT NULL column is a validation rejection');
                assertEquals(missing.message,
                    "TABLEGROUP users: caps row 2 in WITH ROWS doesn't set label, which is NOT NULL with no DEFAULT: "
                    + 'set it in the row, make label NULL, or give it a DEFAULT');
                assertEquals(missing.span?.line, 7, 'reported on the row line');
                assertEquals(missing.span?.column, 7, 'reported at the row table name');

                const unknownTable = await rejection(catalog(users("      pages (title = 'x')")));
                assertEquals(unknownTable.message, "TABLEGROUP users: WITH ROWS fills pages, which schema users_schema doesn't have");
                assertEquals(unknownTable.span?.line, 6, 'an unknown table is reported on its row');

                const unbound = await rejection(catalog(users(), ['  TABLEGROUP docs USING SCHEMA docs_schema']));
                assertEquals(unbound.code, 'VALIDATION_REJECTED', 'an unbound alias is a validation rejection');
                assertEquals(unbound.message,
                    "TABLEGROUP docs: schema docs_schema references users.caps (from docs), and the TABLEGROUP doesn't BIND users");
                assertEquals(unbound.span?.line, 7, 'a group problem is reported at the TABLEGROUP');
            },
        },
        {
            name: '[CLANG12] $author gates, restrictions and schema changes need USING IDENTITIES, checked at compile',
            invoke: async () => {
                const a = await actors();
                const env = await appEnv(a);
                await env.run('CREATE SCHEMA notes_schema CREATORS ($dev) AS (TABLE notes (body string));');

                const restriction = await env.fail(`ALTER CATALOG app VERSION '1.1.0' AS (ADD TABLEGROUP loose USING SCHEMA users_schema);`);
                assertTrue(restriction.includes('ALLOW insert IF on caps reads $author, which needs USING IDENTITIES'), restriction);

                const gate = await env.fail(`ALTER CATALOG app VERSION '1.1.0' AS (
                    ADD TABLEGROUP notes USING SCHEMA notes_schema BIND users => users
                      ALLOW UPDATE REF users IF EXISTS caps WHERE grantee = $author
                );`);
                assertTrue(gate.includes('ALLOW UPDATE REF users IF reads $author, which needs USING IDENTITIES'), gate);

                await env.run(`ALTER CATALOG app VERSION '1.1.0' AS (ADD TABLEGROUP notes USING SCHEMA notes_schema);`);
                await env.run('ALTER SCHEMA notes_schema AS (SET ALLOW RULES notes (ALLOW update IF rowAuthor = $author));');
                const change = await env.fail(`ALTER CATALOG app VERSION '1.2.0' AS (UPDATE SCHEMA notes_schema TO LATEST ON notes);`);
                assertTrue(change.includes('UPDATE SCHEMA ... ON notes:')
                    && change.includes('ALLOW update IF on notes reads $author, which needs USING IDENTITIES'), change);
            },
        },
        {
            name: '[CLANG13] FILES compiles to a definition, deploys as a blob store and a file map, and ADD FILES publishes another',
            invoke: async () => {
                const a = await actors();
                const env = await filesEnv(a);
                const catalog = await env.catalog('app');
                const genesis = (await catalog.getView()).getRelease(catalog.getId())!;
                const [media] = [...genesis.files.values()];
                const usersHash = [...genesis.defs.entries()].find(([, d]) => d.name === 'users')![0];
                assertEquals(media.name, 'media', 'the FILES definition is in the genesis');
                assertEquals(media.bindings['users'], usersHash, 'without BIND the alias is the group name');
                assertEquals(json.toStringNormalized(media.canWrite),
                    json.toStringNormalized({ p: 'exists', table: 'users.caps', where: { label: 'writer', grantee: '$author' } }),
                    'ALLOW WRITE IF lowers to a qualified EXISTS');

                env.vars['me'] = a.admin;
                await env.run('CREATE DATABASE prod USING CATALOG app WITH PARAMS (:admin = $admin) BY $admin;');
                const db = await env.database('prod');
                const [member] = await db.getMemberFiles();
                assertEquals(member.name, 'media', 'the FILES is a member of the database');
                const store = (await env.ctx.getObject(member.storeId)) as RBlobStoreImpl;
                const map = (await env.ctx.getObject(member.mapId)) as RFileMapImpl;
                assertTrue(store !== undefined && map !== undefined, 'CREATE DATABASE creates both objects');

                await env.run(`
                    INSERT INTO users.identities (keyId, publicKey, name) VALUES ($alice, publicKey($alice), 'Alice');
                    INSERT INTO users.caps (label, grantee) VALUES ('writer', $alice);
                `);
                const bytes = new TextEncoder().encode('hello');
                const source = { size: bytes.length, read: async function* () { yield bytes; } };
                const stored = await store.putFile(source, a.alice, { lane: 0 });
                await map.add({ section: 'common', path: 'hello.txt', fileHash: stored.fileHash }, a.alice);
                assertEquals((await map.list()).map((f) => f.path).join(','), 'hello.txt', 'a writer adds a file');
                let refused = false;
                try { await map.add({ section: 'common', path: 'nope.txt', fileHash: stored.fileHash }, a.admin); } catch { refused = true; }
                assertTrue(refused, 'the manager is not a writer');

                env.vars['me'] = a.dev;
                await env.run(`ALTER CATALOG app VERSION '1.1.0' AS (
                    ADD FILES attachments USING IDENTITIES users.identities ALLOW WRITE IF true
                ) NOTE 'shared attachments' BY $dev;`);
                env.vars['me'] = a.admin;
                const [deployed] = await env.run("UPDATE CATALOG app TO '1.1.0' ON prod BY $admin;");
                assertTrue(deployed.kind === 'update-catalog' && deployed.update.created.length === 2, 'the deploy creates the new FILES objects');
                assertEquals((await db.getMemberFiles()).map((m) => m.name).join(','), 'attachments,media', 'both FILES are members');
            },
        },
        {
            name: '[CLANG14] FILES compile errors: unknown group, provider and predicate tables, qualification, missing or non-PUB columns and names',
            invoke: async () => {
                const a = await actors();
                const env = await filesEnv(a);
                const add = (item: string) => env.fail(`ALTER CATALOG app VERSION '1.1.0' AS (ADD FILES ${item});`);
                const expect = async (item: string, part: string, why: string) => {
                    const message = await add(item);
                    assertTrue(message.includes(part), `${why}: expected '${part}', got '${message}'`);
                };

                await expect('x USING IDENTITIES ghost.identities ALLOW WRITE IF true',
                    "FILES x: no catalog group named 'ghost' is defined before this point", 'an unknown group');
                await expect('x BIND u => ghost USING IDENTITIES u.identities ALLOW WRITE IF true',
                    "FILES x BIND u: no catalog group named 'ghost'", 'an unknown BIND target');
                await expect('x USING IDENTITIES users.caps ALLOW WRITE IF true',
                    "FILES x: USING IDENTITIES users.caps: caps isn't an IDENTITY PROVIDER table", 'a non-provider table');
                await expect('x USING IDENTITIES users.people ALLOW WRITE IF true',
                    'FILES x: USING IDENTITIES users.people: schema users_schema has no table people', 'a missing provider table');
                await expect('x USING IDENTITIES identities ALLOW WRITE IF true',
                    'FILES x: USING IDENTITIES identities must name <group>.<table>', 'an unqualified identity table');
                await expect('x USING IDENTITIES users.identities ALLOW WRITE IF EXISTS caps WHERE grantee = $author',
                    'FILES x: ALLOW WRITE IF reads caps: qualify it as users.caps', 'an unqualified predicate table');
                await expect('x USING IDENTITIES users.identities ALLOW WRITE IF EXISTS docs.docs WHERE body = $author',
                    'FILES x: ALLOW WRITE IF reads docs.docs: tables must be qualified with users', 'a second group in the predicate');
                await expect('x BIND u => users USING IDENTITIES users.identities ALLOW WRITE IF true',
                    'FILES x: USING IDENTITIES users.identities must be qualified with u', 'the identity table of another alias');
                await expect('x USING IDENTITIES users.identities ALLOW WRITE IF EXISTS users.pages WHERE title = $author',
                    'FILES x: ALLOW WRITE IF reads users.pages: schema users_schema has no table pages', 'a missing predicate table');
                await expect("x USING IDENTITIES users.identities ALLOW WRITE IF EXISTS users.caps WHERE users.caps.colour = 'red'",
                    "FILES x: ALLOW WRITE IF reads users.caps.colour, which caps doesn't have", 'an unknown column');
                await expect("x USING IDENTITIES users.identities ALLOW WRITE IF EXISTS users.caps WHERE users.caps.memo = 'red'",
                    "FILES x: ALLOW WRITE IF reads users.caps.memo, which isn't PUB in caps", 'a non-PUB column');
                await expect('media USING IDENTITIES users.identities ALLOW WRITE IF true',
                    "FILES media: name 'media' is already used by a TABLEGROUP or FILES", 'a FILES name taken by a FILES');
                await expect('docs USING IDENTITIES users.identities ALLOW WRITE IF true',
                    "FILES docs: name 'docs' is already used by a TABLEGROUP or FILES", 'a FILES name taken by a group');
                const twice = await env.fail(`ALTER CATALOG app VERSION '1.1.0' AS (
                    ADD FILES x USING IDENTITIES users.identities ALLOW WRITE IF true,
                    ADD FILES x USING IDENTITIES users.identities ALLOW WRITE IF false
                );`);
                assertTrue(twice.includes("FILES x: name 'x' is already used"), twice);
                const group = await env.fail(`ALTER CATALOG app VERSION '1.1.0' AS (
                    ADD TABLEGROUP media USING SCHEMA docs_schema BIND users => users USING IDENTITIES users.identities
                );`);
                assertTrue(group.includes("group name 'media' is already used"), `a group cannot take a FILES name: ${group}`);
            },
        },
        {
            name: '[CLANG15] an ambiguous group needs BIND alias => #hash; the dump renders BIND only then and replays to the same DAG',
            invoke: async () => {
                const a = await actors();
                const env = await filesEnv(a);
                const catalog = await env.catalog('app');
                const [first] = await env.run(`ALTER CATALOG app VERSION '1.1.0' AS (
                    ADD TABLEGROUP staff USING SCHEMA users_schema USING IDENTITIES identities
                );`);
                const [second] = await env.run(`ALTER CATALOG app VERSION '1.1.1' AS (
                    ADD TABLEGROUP staff USING SCHEMA users_schema USING IDENTITIES identities ALLOW DEPLOY IF true
                ) AT {#${catalog.getId()}};`);
                if (first.kind !== 'alter-catalog' || second.kind !== 'alter-catalog') throw new Error('expected releases');
                const index = await catalog.getIndex();
                const staffHash = index.releaseState(second.release).added[0];

                const ambiguous = await env.fail(`ALTER CATALOG app VERSION '1.2.0' AS (
                    ADD FILES desk USING IDENTITIES staff.identities ALLOW WRITE IF true
                ) AT LATEST;`);
                assertTrue(ambiguous.includes("FILES desk: 'staff' names several catalog groups; use #hash"), ambiguous);
                await env.run(`ALTER CATALOG app VERSION '1.2.0' AS (
                    ADD FILES desk BIND s => #${staffHash} USING IDENTITIES s.identities ALLOW WRITE IF true
                ) AT LATEST;`);
                const merged = (await catalog.getView()).getMaximalReleases();
                const desk = [...(await catalog.getIndex()).releaseState(merged[0]).files.values()].find((f) => f.name === 'desk')!;
                assertEquals(desk.bindings['s'], staffHash, 'the hash picks one definition');

                const dump = await dumpCatalog(catalog, { loadSchema: loaders(env).loadSchema });
                assertTrue(dump.includes(`ADD FILES desk\n    BIND s => #${staffHash}\n    USING IDENTITIES s.identities`), 'the ambiguous FILES binds by hash');
                assertTrue(dump.includes('FILES media\n    USING IDENTITIES users.identities'), 'the unambiguous FILES has no BIND');

                const replay = await createLangEnv({ vars: { ...a, me: a.dev } });
                await replay.run(dump);
                const entries = async (c: typeof catalog) => {
                    const hashes: B64Hash[] = [];
                    for await (const entry of (await c.getScopedDag()).loadAllEntries()) hashes.push(entry.hash);
                    return hashes.sort().join(',');
                };
                assertEquals(await entries(await replay.catalog('app')), await entries(catalog), 'the dump replays to the same catalog DAG');
            },
        },
        {
            name: '[CLANG16] a FILES in source form reads back to the same definition',
            invoke: async () => {
                const a = await actors();
                const env = await filesEnv(a);
                const view = await (await env.catalog('app')).getView();
                const genesis = view.getRelease(view.getGenesisHash())!;
                const groups: SourceGroup[] = [];
                for (const def of genesis.defs.values()) {
                    const bindings = Object.fromEntries(Object.entries(def.bindings ?? {}).map(([alias, hash]) => [alias, genesis.defs.get(hash)!.name]));
                    groups.push({
                        name: def.name,
                        schema: ((await env.ctx.getObject(def.schemaRef)) as RSchemaImpl).getName(),
                        bindings,
                        ...(def.idProvider !== undefined ? { idProvider: def.idProvider } : {}),
                        ...(def.initialRows !== undefined ? { initialRows: def.initialRows } : {}),
                    });
                }
                const files = [...genesis.files.values()].map((def) => {
                    const [alias, hash] = Object.entries(def.bindings)[0];
                    return { name: def.name, alias, group: genesis.defs.get(hash)!.name, idProvider: def.idProvider, canWrite: def.canWrite };
                });
                const source = renderSourceCatalog({
                    name: 'app', creators: view.getCreators(), params: [...genesis.params.values()], groups, files,
                }, { keyId: (k: string) => (k === a.dev.keyId ? 'dev' : undefined) });
                assertTrue(source.includes('FILES media\n    USING IDENTITIES users.identities\n    ALLOW WRITE IF'), 'the FILES renders without BIND');
                assertTrue(renderSourceFiles({ ...files[0], alias: 'u', idProvider: 'u.identities' }).includes('BIND u => users'),
                    'another alias renders BIND');

                const parsed = parseStatement(source, { catalogVersionOptional: true });
                if (!parsed.ok) throw new Error(`the source parses: ${parsed.diagnostics.map((d) => d.message).join('; ')}`);
                const bound = await bind(parsed.value, { ...env.lang, defaultCatalogVersion: async () => '1.0.0' });
                if (!bound.ok || bound.value.kind !== 'create-catalog') throw new Error('the source binds');
                assertEquals(bound.value.files.map(catalogFilesHash).join(','), [...genesis.files.keys()].join(','),
                    'the FILES reads back to the same definition');
            },
        },
    ],
};
