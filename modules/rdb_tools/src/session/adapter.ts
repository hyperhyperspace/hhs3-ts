import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { Version, version } from "@hyper-hyper-space/hhs3_mvt";
import type { RTableGroup, RTableView } from "@hyper-hyper-space/hhs3_rdb";
import { splitTableRef } from "@hyper-hyper-space/hhs3_rdb";
import {
    bind,
    execute,
    LangBindContext,
    LangDiagnostic,
    LangExecutionResult,
    parseScript,
    ResolvedTableRef,
    VersionExpr,
    VersionScope,
} from "@hyper-hyper-space/hhs3_rdb_lang";

import { KeyPassphraseRequiredError } from "./session.js";
import { WorkspaceSession } from "./session.js";
import {
    frontierForScope,
    hashScopeForVersionScope,
    resolveVersionMember,
} from "./version.js";
import type { RootResolveContext } from "../workspace/root_index.js";

export type StatementRunResult = {
    result: LangExecutionResult;
};

export type ScriptRunResult = {
    results: StatementRunResult[];
};

function rootCtx(session: WorkspaceSession): RootResolveContext {
    return { aliases: session.aliases };
}

export function createBindContext(session: WorkspaceSession): LangBindContext {
    const ctx = rootCtx(session);
    return {
        resolveSchema: (ref) => session.workspace.roots.resolveSchema(ref, ctx),
        resolveGroup: (ref) => session.workspace.roots.resolveGroup(ref, ctx),
        resolveDatabase: (ref) => session.workspace.roots.resolveDatabase(ref, ctx),
        resolveTable: (ref) => session.workspace.roots.resolveTable(ref, ctx),
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
        resolveFkRowId: (prefix, sourceTable, column, at, from) => resolveFkRowId(prefix, sourceTable, column, at, from),
        resolveVersion: (expr, scope) => resolveVersionExpr(session, expr, scope),
        resolveDefaultView: async () => session.defaultView,
        resolveVariable: (name) => session.resolveVariable(name),
        resolvePublicKey: (labelOrPrefix) => session.resolvePublicKey(labelOrPrefix),
        resolveLogTarget: (ref) => session.workspace.roots.resolveLogTarget(ref, ctx),
        currentAuthor: () => session.currentAuthor(),
        resolveAuthor: (ref) => session.resolveAuthor(ref),
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

const KEY_PASSPHRASE_REQUIRED = /^Key '([^']+)' is not unlocked$/;

/** Recover a locked-key deferral swallowed by rdb_lang bind error handling. */
export function keyPassphraseRequiredFromError(e: unknown): KeyPassphraseRequiredError | undefined {
    if (e instanceof KeyPassphraseRequiredError) return e;
    if (e instanceof LanguageError) {
        for (const diagnostic of e.diagnostics) {
            const match = KEY_PASSPHRASE_REQUIRED.exec(diagnostic.message);
            if (match !== null) return new KeyPassphraseRequiredError(match[1]);
        }
    }
    return undefined;
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
    for (const member of expr.members) {
        hashes.push(await resolveVersionMember(session, member, hashScope));
    }
    return version(...hashes);
}

export async function resolveRowIdPrefix(prefix: string, table: ResolvedTableRef, at: Version, from?: Version): Promise<B64Hash> {
    const view = await table.table.getView(at, from ?? at);
    const tableName = `${table.groupId}.${table.tableName}`;
    return matchRowIdPrefix(prefix, await view.liveRowIds(), tableName);
}

export async function resolveFkRowId(
    prefix: string,
    sourceTable: ResolvedTableRef,
    column: string,
    at: Version,
    from?: Version,
): Promise<B64Hash> {
    const fromVersion = from ?? at;
    const groupView = await sourceTable.group.getView(at, fromVersion);
    const schemaView = groupView.getSchemaView();
    const targetRef = schemaView.getFKs(sourceTable.tableName)[column];
    if (targetRef === undefined) {
        throw new Error(`Column '${column}' is not a REFERENCES column`);
    }

    const [groupName, targetTable] = splitTableRef(targetRef);

    if (groupName === undefined) {
        const view = await groupView.getTableView(targetTable);
        const tableName = `${sourceTable.groupId}.${targetTable}`;
        return matchRowIdPrefix(prefix, await view.liveRowIds(), tableName);
    }

    const fkGroup = sourceTable.group as CrossGroupFkResolvable;
    const view = await fkGroup.resolveForeignTableView(groupName, targetTable, at, fromVersion);
    if (view === undefined) {
        throw new Error(`Unknown foreign table '${groupName}.${targetTable}' for FK column '${column}'`);
    }
    const tableName = `${sourceTable.groupId}.${groupName}.${targetTable}`;
    return matchRowIdPrefix(prefix, await view.liveRowIds(), tableName);
}

type CrossGroupFkResolvable = RTableGroup & {
    resolveForeignTableView(
        groupName: string,
        table: string,
        at: Version,
        from: Version,
        filterVoided?: boolean,
    ): Promise<RTableView | undefined>;
};

function matchRowIdPrefix(prefix: string, rowIds: B64Hash[], tableName: string): B64Hash {
    const matches = rowIds.filter((rowId) => rowId.startsWith(prefix));
    if (matches.length === 1) return matches[0];

    if (matches.length === 0) throw new Error(`Unknown rowId prefix '#${prefix}' in ${tableName}`);
    const examples = matches.slice(0, 5).map((rowId) => `#${rowId}`).join(', ');
    throw new Error(`Ambiguous rowId prefix '#${prefix}' in ${tableName}: ${examples}`);
}
