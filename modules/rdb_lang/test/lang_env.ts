// A C-SQL test environment over a mock RContext: runs statements through
// parse, bind and execute, and applies create plans the way a host does
// (create the root, run afterCreate, register it by name).

import { createBasicCrypto, createIdentity, HASH_SHA256, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { RContext } from "@hyper-hyper-space/hhs3_mvt";
import {
    RCatalogImpl, rCatalogFactory, RDbImpl, rDbFactory, RDeployGateImpl, rDeployGateFactory,
    RSchemaImpl, rSchemaFactory, RTableGroupImpl, rTableGroupFactory,
    RBLOB_STORE_TYPE_ID, rBlobStoreFactory, RFILE_MAP_TYPE_ID, rFileMapFactory,
} from "@hyper-hyper-space/hhs3_rdb";

import { createMockRContext } from "../../rdb/test/mock_rcontext.js";
import { bind } from "../src/bind/bind.js";
import type { LangDiagnostic, LangExecutionResult, LangValue } from "../src/index.js";
import { execute } from "../src/exec/execute.js";
import { parseScript } from "../src/syntax/parser.js";
import { createTestBindContext, TestBindContext } from "./mock_bind_context.js";

const hashSuite = createBasicCrypto().hash(HASH_SHA256);

export type LangEnv = {
    ctx: RContext;
    lang: TestBindContext;
    vars: { [name: string]: LangValue };
    run(sql: string): Promise<LangExecutionResult[]>;
    // Runs `sql` expecting a failure; returns the first diagnostic message.
    fail(sql: string): Promise<string>;
    database(name: string): Promise<RDbImpl>;
    catalog(name: string): Promise<RCatalogImpl>;
    schema(name: string): Promise<RSchemaImpl>;
    group(ref: string): Promise<RTableGroupImpl>;
};

export function registerRdbTypes(ctx: RContext): void {
    ctx.getRegistry().register(RDbImpl.typeId, rDbFactory);
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);
    ctx.getRegistry().register(RCatalogImpl.typeId, rCatalogFactory);
    ctx.getRegistry().register(RDeployGateImpl.typeId, rDeployGateFactory);
    ctx.getRegistry().register(RBLOB_STORE_TYPE_ID, rBlobStoreFactory);
    ctx.getRegistry().register(RFILE_MAP_TYPE_ID, rFileMapFactory);
}

export async function newIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

class StatementError extends Error {
    constructor(readonly stage: 'parse' | 'bind' | 'execute', readonly diagnostics: LangDiagnostic[]) {
        super(`${stage}: ${diagnostics.map((d) => d.message).join('\n')}`);
    }
}

export async function createLangEnv(opts: {
    vars?: { [name: string]: LangValue };
    ctx?: RContext;
} = {}): Promise<LangEnv> {
    const ctx = opts.ctx ?? createMockRContext({ selfValidate: true });
    if (opts.ctx === undefined) registerRdbTypes(ctx);
    const vars: { [name: string]: LangValue } = { ...opts.vars };
    const lang = createTestBindContext(ctx, vars);
    const span = { start: 0, end: 0, line: 1, column: 1 };
    const nameRef = (text: string) => ({ kind: 'name' as const, text, parts: text.split('.'), span });

    const run = async (sql: string): Promise<LangExecutionResult[]> => {
        const parsed = parseScript(sql);
        if (!parsed.ok) throw new StatementError('parse', parsed.diagnostics);
        const results: LangExecutionResult[] = [];
        for (const statement of parsed.value.statements) {
            const bound = await bind(statement, lang);
            if (!bound.ok) throw new StatementError('bind', bound.diagnostics);
            const executed = await execute(bound.value);
            if (!executed.ok) throw new StatementError('execute', executed.diagnostics);
            const result = executed.value;
            if (result.kind === 'create-plan') {
                const plan = result.plan;
                const object = await ctx.createObject(plan.payload);
                if (plan.kind === 'create-schema') lang.registerSchema(plan.name, object as RSchemaImpl);
                if (plan.kind === 'create-catalog') lang.registerCatalog(plan.name, object as RCatalogImpl);
                if (plan.kind === 'create-database') {
                    lang.registerDatabase(plan.name, object as RDbImpl);
                    await plan.afterCreate(object);
                    lang.setCurrentDatabase(object.getId());
                }
            } else if (result.kind === 'use-database') {
                lang.setCurrentDatabase(result.database);
            }
            results.push(result);
        }
        return results;
    };

    const fail = async (sql: string): Promise<string> => {
        try {
            await run(sql);
        } catch (e) {
            if (e instanceof StatementError) return e.diagnostics[0]?.message ?? e.message;
            return e instanceof Error ? e.message : String(e);
        }
        throw new Error(`expected a failure: ${sql}`);
    };

    return {
        ctx, lang, vars, run, fail,
        database: async (name) => (await lang.resolveDatabase(nameRef(name))).db!,
        catalog: async (name) => (await lang.resolveCatalog(nameRef(name))).catalog!,
        schema: async (name) => (await lang.resolveSchema(nameRef(name))).schema as RSchemaImpl,
        group: async (ref) => (await lang.resolveGroup(nameRef(ref))).group as RTableGroupImpl,
    };
}

export type ScriptIds = { [name: string]: B64Hash };
