import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, createIdentity, HASH_SHA256, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { version, Version } from "@hyper-hyper-space/hhs3_mvt";
import type { RContext, RObject } from "@hyper-hyper-space/hhs3_mvt";
import {
    RDbImpl, rDbFactory, RSchemaImpl, rSchemaFactory, RTableGroupImpl, rTableGroupFactory,
} from "@hyper-hyper-space/hhs3_rdb";

import { createMockRContext } from "../../rdb/test/mock_rcontext.js";
import { bind } from "../src/bind/bind.js";
import type { LangExecutionResult, LangValue, VersionScope } from "../src/index.js";
import { execute } from "../src/exec/execute.js";
import { parseScript } from "../src/syntax/parser.js";
import type { VersionExpr } from "../src/syntax/ast.js";
import { createTestBindContext, TestBindContext } from "./mock_bind_context.js";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);
const SCRIPT_DIR = resolve("test/scripts/users_permissions");

type Vars = { [name: string]: LangValue };

type ScriptEnv = {
    ctx: RContext;
    lang: TestBindContext;
    vars: Vars;
    admin: OwnIdentity;
    alice: OwnIdentity;
};

async function createEnv(): Promise<ScriptEnv> {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RDbImpl.typeId, rDbFactory);
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);

    const admin = await createIdentity(SIGNING_ED25519, hashSuite);
    const alice = await createIdentity(SIGNING_ED25519, hashSuite);
    const vars: Vars = {
        admin,
        alice,
        aliceKey: { kind: 'key-id', keyId: alice.keyId },
        me: admin,
    };
    const lang = createTestBindContext(ctx, vars);
    lang.resolveVersion = async (expr, scope) => resolveVersionExpr(expr, scope);
    return { ctx, lang, vars, admin, alice };
}

async function setupUsersAndDocs(env: ScriptEnv): Promise<void> {
    env.vars['me'] = env.admin;
    await runScriptFile('create_users.sql', env);
    await runScriptFile('create_docs.sql', env);
}

async function setupAliceWriter(env: ScriptEnv): Promise<void> {
    env.vars['me'] = env.admin;
    await runScriptFile('register_alice.sql', env);
    await runScriptFile('grant_alice_writer.sql', env);
}

async function runScriptFile(name: string, env: ScriptEnv): Promise<LangExecutionResult[]> {
    const sql = await readFile(resolve(SCRIPT_DIR, name), 'utf8');
    return runScriptText(sql, env, name);
}

async function runScriptText(sql: string, env: ScriptEnv, label: string): Promise<LangExecutionResult[]> {
    const parsed = parseScript(sql);
    assertTrue(parsed.ok, `parse should succeed for ${label}`);
    if (!parsed.ok) throw new Error(parsed.diagnostics.map((d) => d.message).join('\n'));

    const results: LangExecutionResult[] = [];
    for (const statement of parsed.value.statements) {
        const bound = await bind(statement, env.lang);
        assertTrue(bound.ok, `bind should succeed for ${label}`);
        if (!bound.ok) throw new Error(bound.diagnostics.map((d) => d.message).join('\n'));

        const executed = await execute(bound.value);
        assertTrue(executed.ok, `execute should succeed for ${label}`);
        if (!executed.ok) throw new Error(executed.diagnostics.map((d) => d.message).join('\n'));

        const result = executed.value;
        if (result.kind === 'create-plan') {
            const object = await env.ctx.createObject(result.plan.payload);
            if (result.plan.kind === 'create-database') env.lang.registerDatabase(result.plan.name, object as RDbImpl);
            if (result.plan.kind === 'create-schema') env.lang.registerSchema(result.plan.name, object as RSchemaImpl);
            if (result.plan.kind === 'create-tablegroup') env.lang.registerGroup(result.plan.name, object as RTableGroupImpl);
        }
        results.push(result);
    }
    return results;
}

async function expectScriptFailure(nameOrSql: string, env: ScriptEnv, isFile: boolean): Promise<void> {
    let failed = false;
    try {
        if (isFile) await runScriptFile(nameOrSql, env);
        else await runScriptText(nameOrSql, env, 'expected failure');
    } catch (_e) {
        failed = true;
    }
    assertTrue(failed, `${nameOrSql} should fail`);
}

async function resolveVersionExpr(expr: VersionExpr | undefined, scope: VersionScope): Promise<Version> {
    if (expr?.kind === 'set') return version(...expr.hashes.map((h) => h.prefix));
    if (expr?.kind === 'hash') return version(expr.hash.prefix);
    return frontierForScope(scope);
}

async function frontierForScope(scope: VersionScope): Promise<Version> {
    const object = scope.kind === 'schema'
        ? scope.schema
        : scope.kind === 'group'
            ? scope.group
            : scope.kind === 'table'
                ? scope.table
                : scope.object;
    if (object === undefined) return version();
    return (await object.getScopedDag()).getFrontier();
}

async function group(env: ScriptEnv, name: string): Promise<RTableGroupImpl> {
    const resolved = await env.lang.resolveGroup({ kind: 'name', text: name, parts: [name], span: zeroSpan() });
    return resolved.group as RTableGroupImpl;
}

function zeroSpan() {
    return { start: 0, end: 0, line: 1, column: 1 };
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
            name: '[USERS_SCRIPT03] schema deploy works on scripted app group',
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
    ],
};
