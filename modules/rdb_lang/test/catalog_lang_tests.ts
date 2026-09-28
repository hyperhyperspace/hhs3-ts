import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { RSchemaImpl, RTableGroupImpl } from "@hyper-hyper-space/hhs3_rdb";

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
    grantee string PUB READONLY
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

type Actors = { dev: OwnIdentity; admin: OwnIdentity; alice: OwnIdentity };

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
    ],
};
