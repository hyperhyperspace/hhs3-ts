import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { RObject, Version } from "@hyper-hyper-space/hhs3_mvt";
import type { RTableGroupImpl } from "@hyper-hyper-space/hhs3_rdb";

import type { LangExecutionResult } from "../src/index.js";
import { createLangEnv, LangEnv, newIdentity } from "./lang_env.js";

const SCRIPT_DIR = resolve("test/scripts/users_permissions");

type ScriptEnv = LangEnv & {
    admin: OwnIdentity;
    alice: OwnIdentity;
};

async function createEnv(): Promise<ScriptEnv> {
    const admin = await newIdentity();
    const alice = await newIdentity();
    const env = await createLangEnv({
        vars: {
            admin,
            alice,
            aliceKey: { kind: 'key-id', keyId: alice.keyId },
            me: admin,
        },
    });
    return { ...env, admin, alice };
}

// The schemas, then the app catalog and the database deployed from it.
async function setupUsersAndDocs(env: ScriptEnv): Promise<void> {
    env.vars['me'] = env.admin;
    await runScriptFile('create_users.sql', env);
    await runScriptFile('create_docs.sql', env);
    await runScriptFile('create_app.sql', env);
}

async function setupAliceWriter(env: ScriptEnv): Promise<void> {
    env.vars['me'] = env.admin;
    await runScriptFile('register_alice.sql', env);
    await runScriptFile('grant_alice_writer.sql', env);
}

async function runScriptFile(name: string, env: ScriptEnv): Promise<LangExecutionResult[]> {
    return env.run(await readFile(resolve(SCRIPT_DIR, name), 'utf8'));
}

async function runScriptText(sql: string, env: ScriptEnv, _label: string): Promise<LangExecutionResult[]> {
    return env.run(sql);
}

async function expectScriptFailure(nameOrSql: string, env: ScriptEnv, isFile: boolean): Promise<void> {
    await env.fail(isFile ? await readFile(resolve(SCRIPT_DIR, nameOrSql), 'utf8') : nameOrSql);
}

async function group(env: ScriptEnv, name: string): Promise<RTableGroupImpl> {
    return env.group(name);
}

async function frontier(object: RObject & { getScopedDag(): Promise<{ getFrontier(): Promise<Version> }> }): Promise<Version> {
    return (await object.getScopedDag()).getFrontier();
}

function versionExpr(v: Version): string {
    return `{${[...v].map((hash) => `#${hash}`).join(', ')}}`;
}

export const usersPermissionScriptTests = {
    title: '[RDB_LANG:USERS_SCRIPT] Scripted Users permissions',
    tests: [
        {
            name: '[USERS_SCRIPT01] scripts create Users and docs groups',
            invoke: async () => {
                const env = await createEnv();
                await setupUsersAndDocs(env);
                const users = await group(env, 'users');
                const docs = await group(env, 'docs_group');
                assertEquals(users.getIdProvider(), 'identities', 'users group uses local identities');
                assertEquals(docs.getIdProvider(), 'users.identities', 'docs group uses bound identities');
                assertEquals(docs.getBindings()['users'], users.getId(), 'the docs binding is the same database\'s users group');
            },
        },
        {
            name: '[USERS_SCRIPT02] ref advance gates granted permission visibility',
            invoke: async () => {
                const env = await createEnv();
                await setupUsersAndDocs(env);
                await setupAliceWriter(env);

                env.vars['me'] = env.alice;
                await expectScriptFailure('insert_alice_doc.sql', env, true);

                env.vars['me'] = env.admin;
                await runScriptFile('observe_users.sql', env);

                env.vars['me'] = env.alice;
                await runScriptFile('insert_alice_doc.sql', env);
                const docs = await group(env, 'docs_group');
                const rows = await (await (await docs.getTable('docs')).getView()).query({ where: { p: 'cmp', cmp: 'eq', left: { col: 'body' }, right: { lit: 'hello from alice' } } });
                assertEquals(rows.length, 1, 'observed writer cap permits Alice doc insert');
            },
        },
        {
            name: '[USERS_SCRIPT03] a released schema change deploys to the scripted app group',
            invoke: async () => {
                const env = await createEnv();
                await setupUsersAndDocs(env);
                await setupAliceWriter(env);
                await runScriptFile('observe_users.sql', env);

                env.vars['me'] = env.alice;
                await runScriptFile('insert_alice_doc.sql', env);

                env.vars['me'] = env.admin;
                await runScriptFile('alter_docs_add_status.sql', env);
                await runScriptFile('deploy_docs_latest.sql', env);

                const selected = await runScriptText("SELECT body, status FROM docs_group.docs WHERE body = 'hello from alice';", env, 'select deployed default');
                const result = selected[selected.length - 1];
                assertTrue(result.kind === 'select', 'select result expected');
                if (result.kind !== 'select') return;
                assertEquals(result.rows.length, 1, 'one doc row');
                assertEquals(result.rows[0].values['status'], 'draft', 'deployed default is visible');
            },
        },
        {
            name: '[USERS_SCRIPT04] concurrent cap revoke voids permitted use at merge',
            invoke: async () => {
                const env = await createEnv();
                await setupUsersAndDocs(env);
                await setupAliceWriter(env);
                await runScriptFile('observe_users.sql', env);

                const users = await group(env, 'users');
                const docs = await group(env, 'docs_group');
                const docsBase = await frontier(docs);
                const usersBase = await frontier(users);

                env.vars['me'] = env.alice;
                const insertResults = await runScriptText(
                    `INSERT INTO docs_group.docs (body) VALUES ('concurrent use') AT ${versionExpr(docsBase)};`,
                    env,
                    'concurrent doc insert',
                );
                const insert = insertResults[0];
                assertTrue(insert.kind === 'insert', 'insert result expected');
                if (insert.kind !== 'insert') return;

                const caps = await users.getTable('caps');
                const capRows = await (await caps.getView(usersBase, usersBase)).findRowIds({ label: 'writer', grantee: env.alice.keyId });
                assertEquals(capRows.length, 1, 'one writer cap row');

                env.vars['me'] = env.admin;
                await runScriptText(
                    `DELETE FROM users.caps WHERE rowId = '${capRows[0]}' AT ${versionExpr(usersBase)};`,
                    env,
                    'concurrent cap revoke',
                );
                await runScriptText(
                    `UPDATE REF users TO LATEST ON docs_group AT ${versionExpr(docsBase)};`,
                    env,
                    'concurrent users observation',
                );

                const docsView = await (await docs.getTable('docs')).getView();
                assertTrue(!await docsView.hasRow(insert.rowId), 'concurrent revoke voids the permitted doc insert');
            },
        },
        {
            name: '[USERS_SCRIPT05] BY clause overrides the default author',
            invoke: async () => {
                const env = await createEnv();
                await setupUsersAndDocs(env);
                await setupAliceWriter(env);
                await runScriptFile('observe_users.sql', env);

                // The default author is admin (who is not a docs writer), but
                // `BY $alice` signs as alice, who holds the writer cap that gates
                // docs inserts.
                env.vars['me'] = env.admin;
                await runScriptText(
                    "INSERT INTO docs_group.docs (body) VALUES ('signed by alice') BY $alice;",
                    env,
                    'insert by alice',
                );

                const docs = await group(env, 'docs_group');
                const rows = await (await (await docs.getTable('docs')).getView()).query({
                    where: { p: 'cmp', cmp: 'eq', left: { col: 'body' }, right: { lit: 'signed by alice' } },
                });
                assertEquals(rows.length, 1, 'BY $alice insert is permitted by the alice writer cap');
                assertEquals(rows[0].author, env.alice.keyId, 'row author is alice, not the default admin');

                // BY NOBODY forces an unauthored op even though a default author
                // is set; the writer gate rejects it.
                await expectScriptFailure(
                    "INSERT INTO docs_group.docs (body) VALUES ('anon') BY NOBODY;",
                    env,
                    false,
                );
            },
        },
        {
            name: '[USERS_SCRIPT06] ALLOW UPDATE REF gate + BY on UPDATE REF: a manager observes, unauthored is rejected',
            invoke: async () => {
                const env = await createEnv();
                env.vars['me'] = env.admin;
                await runScriptFile('create_users.sql', env);
                await runScriptFile('create_docs.sql', env);

                // a docs group whose `users` observation is gated on a manager
                // cap (evaluated in the users frame: `caps` is local there),
                // next to an ungated one
                await runScriptText(`
                    CREATE SCHEMA docs_gated_schema CREATORS ($admin) AS (
                      TABLE docs ( body string )
                    );
                    CREATE CATALOG gated VERSION '1.0.0' AS (
                      TABLEGROUP users USING SCHEMA users_schema
                        USING IDENTITIES identities
                        WITH ROWS (
                          identities (keyId = $admin, publicKey = publicKey($admin), name = 'Admin'),
                          caps (label = 'manager', grantee = $admin)
                        ),
                      TABLEGROUP docs_gated USING SCHEMA docs_gated_schema
                        BIND users => users
                        USING IDENTITIES users.identities
                        ALLOW UPDATE REF users IF EXISTS caps WHERE label = 'manager' AND grantee = $author,
                      TABLEGROUP docs_group USING SCHEMA docs_schema
                        BIND users => users
                        USING IDENTITIES users.identities
                    );
                    CREATE DATABASE gated_db USING CATALOG gated;
                `, env, 'create gated docs group');

                const gated = await group(env, 'docs_gated');
                const canObserve = gated.getCanObserve();
                assertTrue(canObserve !== undefined && canObserve['users'] !== undefined,
                    'ALLOW UPDATE REF clause compiles into the instantiated create payload');

                // an explicitly unauthored observation of a gated binding is rejected
                await expectScriptFailure('UPDATE REF users TO LATEST ON docs_gated BY NOBODY;', env, false);

                // the admin (a genesis manager) is admitted via `BY $admin`
                const results = await runScriptText(
                    'UPDATE REF users TO LATEST ON docs_gated BY $admin;', env, 'authored observe by manager');
                assertEquals(results[0].kind, 'update-ref', 'a manager-authored observe of a gated binding executes');

                // BY NOBODY on an UNGATED binding stays allowed (explicit unauthored)
                const ungated = await runScriptText(
                    'UPDATE REF users TO LATEST ON docs_group BY NOBODY;', env, 'unauthored observe of ungated binding');
                assertEquals(ungated[0].kind, 'update-ref', 'an ungated binding still observes unauthored');
            },
        },
    ],
};
