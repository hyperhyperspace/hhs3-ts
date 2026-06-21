import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { Version, version } from "@hyper-hyper-space/hhs3_mvt";
import {
    bind,
    execute,
    HashScope,
    LangBindContext,
    LangDiagnostic,
    LangExecutionResult,
    parseScript,
    ResolvedTableRef,
    VersionExpr,
    VersionScope,
} from "@hyper-hyper-space/hhs3_rdb_lang";

import { WorkspaceSession } from "./session.js";

export type StatementRunResult = {
    result: LangExecutionResult;
};

export type ScriptRunResult = {
    results: StatementRunResult[];
};

export function createBindContext(session: WorkspaceSession): LangBindContext {
    return {
        resolveSchema: (ref) => session.workspace.roots.resolveSchema(ref),
        resolveGroup: (ref) => session.workspace.roots.resolveGroup(ref),
        resolveTable: (ref) => session.workspace.roots.resolveTable(ref),
        resolveDefaultGroup: async () => session.currentGroup === undefined
            ? undefined
            : {
                kind: 'name',
                text: session.currentGroup,
                parts: [session.currentGroup],
                span: { start: 0, end: session.currentGroup.length, line: 1, column: 1 },
            },
        resolveHash: (ref, scope) => session.workspace.roots.resolveHash(ref, scope),
        resolveRowId: (ref, table, at, from) => resolveRowIdPrefix(ref.prefix, table, at, from),
        resolveVersion: (expr, scope) => resolveVersionExpr(session, expr, scope),
        resolveDefaultView: async () => session.defaultView,
        resolveVariable: (name) => session.resolveVariable(name),
        resolveLogTarget: (ref) => session.workspace.roots.resolveLogTarget(ref),
        currentAuthor: () => session.currentAuthor(),
        createUuid: () => session.createUuid(),
        createSeed: (kind, name) => session.createSeed(kind, name),
    };
}

export async function runLanguageText(session: WorkspaceSession, text: string): Promise<ScriptRunResult> {
    const parsed = parseScript(text);
    if (!parsed.ok) throw new LanguageError(parsed.diagnostics);

    const results: StatementRunResult[] = [];
    const context = createBindContext(session);
    for (const statement of parsed.value.statements) {
        const bound = await bind(statement, context);
        if (!bound.ok) throw new LanguageError(bound.diagnostics);

        const executed = await execute(bound.value);
        if (!executed.ok) throw new LanguageError(executed.diagnostics);

        const result = executed.value;
        if (result.kind === 'create-plan') {
            const object = await session.workspace.createRoot(result.plan);
            if (result.plan.kind === 'create-database') session.setCurrentDatabase(object.getId());
            if (result.plan.kind === 'create-tablegroup') session.setCurrentGroup(object.getId());
        } else if (result.kind === 'set-view') {
            session.setDefaultView({
                at: await resolveVersionExpr(session, result.at, { kind: 'group', id: session.currentGroup ?? '', group: undefined }),
                from: result.from === undefined
                    ? undefined
                    : await resolveVersionExpr(session, result.from, { kind: 'group', id: session.currentGroup ?? '', group: undefined }),
            });
        }

        results.push({ result });
    }

    return { results };
}

export class LanguageError extends Error {
    constructor(readonly diagnostics: LangDiagnostic[]) {
        super(diagnostics.map((d) => `${d.code}: ${d.message}`).join('\n'));
    }
}

async function resolveVersionExpr(session: WorkspaceSession, expr: VersionExpr | undefined, scope: VersionScope): Promise<Version> {
    if (expr === undefined) {
        if (session.defaultView !== undefined) return session.defaultView.at;
        return frontierForScope(scope);
    }

    if (expr.kind === 'latest') return frontierForScope(scope);

    const hashScope = hashScopeForVersionScope(scope);
    if (expr.kind === 'hash') {
        const hash = await session.workspace.roots.resolveHash(expr.hash, hashScope);
        return version(hash);
    }

    const hashes: B64Hash[] = [];
    for (const hash of expr.hashes) hashes.push(await session.workspace.roots.resolveHash(hash, hashScope));
    return version(...hashes);
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

function hashScopeForVersionScope(scope: VersionScope): HashScope {
    if (scope.kind === 'schema') return { kind: 'object', objectId: scope.id };
    if (scope.kind === 'group') return { kind: 'object', objectId: scope.id };
    if (scope.kind === 'table') return { kind: 'object', objectId: scope.groupId };
    return { kind: 'object', objectId: scope.id };
}

export async function resolveRowIdPrefix(prefix: string, table: ResolvedTableRef, at: Version, from?: Version): Promise<B64Hash> {
    const view = await table.table.getView(at, from ?? at);
    const rowIds = await view.liveRowIds();
    const matches = rowIds.filter((rowId) => rowId.startsWith(prefix));
    if (matches.length === 1) return matches[0];

    const tableName = `${table.groupId}.${table.tableName}`;
    if (matches.length === 0) throw new Error(`Unknown rowId prefix '#${prefix}' in ${tableName}`);
    const examples = matches.slice(0, 5).map((rowId) => `#${rowId}`).join(', ');
    throw new Error(`Ambiguous rowId prefix '#${prefix}' in ${tableName}: ${examples}`);
}
