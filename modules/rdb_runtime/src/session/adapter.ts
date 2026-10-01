import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { formatValidationFailure, ValidationRejectedError, Version, version } from "@hyper-hyper-space/hhs3_mvt";
import type { CatalogUpdateResult, RDbImpl, RTableGroup, RTableView } from "@hyper-hyper-space/hhs3_rdb";
import { splitTableRef } from "@hyper-hyper-space/hhs3_rdb";
import {
    bind,
    deployLabelsFor,
    execute,
    LangBindContext,
    LangDiagnostic,
    LangExecutionResult,
    parseScript,
    ResolvedTableRef,
    TextSpan,
    VersionExpr,
    VersionScope,
} from "@hyper-hyper-space/hhs3_rdb_lang";

import { KeyPassphraseRequiredError } from "./session.js";
import { RdbSession } from "./session.js";
import { suggestAuthorsForBindFailure, suggestAuthorsForFailure } from "./authz_suggest.js";
import { tryAuthSignRetry, tryBindAuthorRetry } from "./sign_retry.js";
import { extractRefUpdateTrigger, propagateRefUpdates, type RefUpdateOutcome } from "./ref_auto_update.js";
import {
    frontierForScope,
    hashScopeForVersionScope,
    resolveVersionMember,
} from "./version.js";
import { rootCtx } from "./root_context.js";
import type { AuthInteractionContext } from "./prompts.js";

export type StatementRunResult = {
    result: LangExecutionResult;
    refUpdates?: RefUpdateOutcome[];
    // CREATE DATABASE: the groups created (and deployed) for its release.
    deploy?: CatalogUpdateResult;
};

export type ScriptRunResult = {
    results: StatementRunResult[];
};

export type ExecuteTextOptions = AuthInteractionContext & {
    // Source mode (a catalog repository's target-catalog.sql): CREATE CATALOG may omit
    // VERSION, and takes catalogVersion. schemaVersion, when set, is the version
    // a CREATE SCHEMA with no VERSION is created at (the catalog's version).
    source?: { catalogVersion: string; schemaVersion?: string };
};

export function createBindContext(session: RdbSession): LangBindContext {
    const roots = session.workspace.roots;
    return {
        resolveSchema: (ref) => roots.resolveSchema(ref, rootCtx(session)),
        resolveGroup: (ref) => roots.resolveGroup(ref, rootCtx(session)),
        resolveDatabase: (ref) => roots.resolveDatabase(ref, rootCtx(session)),
        resolveCatalog: (ref) => roots.resolveCatalog(ref, rootCtx(session)),
        resolveTable: (ref) => roots.resolveTable(ref, rootCtx(session)),
        resolveFiles: (ref) => roots.resolveFiles(ref, rootCtx(session)),
        get localFiles() { return session.localFiles; },
        resolveDefaultGroup: async () => session.currentGroup === undefined
            ? undefined
            : {
                kind: 'name',
                text: session.currentGroup,
                parts: [session.currentGroup],
                span: { start: 0, end: session.currentGroup.length, line: 1, column: 1 },
            },
        resolveDefaultDatabase: async () => {
            if (session.currentDatabase === undefined) return undefined;
            const root = roots.get(session.currentDatabase);
            return { id: session.currentDatabase, db: root?.object as RDbImpl | undefined };
        },
        resolveDeployLabels: async (groupId) => {
            const member = await roots.memberName(groupId);
            if (member?.database.object === undefined) return undefined;
            return deployLabelsFor(member.database.object as RDbImpl, groupId);
        },
        resolveHash: (ref, scope) => session.workspace.roots.resolveHash(ref, scope),
        resolveRowId: (ref, table, at, from) => resolveRowIdPrefix(ref.prefix, table, at, from),
        resolveFkRowId: (prefix, sourceTable, column, at, from) => resolveFkRowId(prefix, sourceTable, column, at, from),
        resolveVersion: (expr, scope) => resolveVersionExpr(session, expr, scope),
        resolveDefaultView: async () => session.defaultView,
        resolveVariable: (name) => session.resolveVariable(name),
        resolvePublicKey: (labelOrPrefix) => session.resolvePublicKey(labelOrPrefix),
        hashSuite: () => session.workspace.replica.getHashSuite(),
        resolveLogTarget: (ref) => roots.resolveLogTarget(ref, rootCtx(session)),
        currentAuthor: () => session.currentAuthor(),
        resolveAuthor: (ref) => session.resolveAuthor(ref),
        createUuid: () => session.createUuid(),
        createSeed: (kind, name) => session.createSeed(kind, name),
    };
}

export async function executeText(
    session: RdbSession,
    text: string,
    options?: ExecuteTextOptions,
): Promise<ScriptRunResult> {
    const source = options?.source;
    const parsed = parseScript(text, source !== undefined ? { catalogVersionOptional: true } : undefined);
    if (!parsed.ok) throw new LanguageError(parsed.diagnostics);

    const results: StatementRunResult[] = [];
    const context: LangBindContext = source !== undefined
        ? {
            ...createBindContext(session),
            defaultCatalogVersion: async () => source.catalogVersion,
            ...(source.schemaVersion !== undefined ? { defaultSchemaVersion: async () => source.schemaVersion } : {}),
        }
        : createBindContext(session);
    for (const statement of parsed.value.statements) {
        let bound = await bind(statement, context);
        if (!bound.ok) {
            const retried = await tryBindAuthorRetry(session, statement, bound.diagnostics, context, options);
            if (retried !== undefined) {
                bound = { ok: true, value: retried };
            } else {
                const hint = await suggestAuthorsForBindFailure(session, statement, bound.diagnostics, context);
                throw new LanguageError(bound.diagnostics, hint === undefined ? [] : [hint]);
            }
        }

        const executed = await execute(bound.value);
        let effectiveBound = bound.value;
        let result: LangExecutionResult;
        if (!executed.ok) {
            const retried = await tryAuthSignRetry(session, bound.value, executed.diagnostics, options);
            if (retried !== undefined) {
                effectiveBound = retried.bound;
                result = retried.result;
            } else {
                const hint = await suggestAuthorsForFailure(session, bound.value, executed.diagnostics);
                throw new LanguageError(executed.diagnostics, hint === undefined ? [] : [hint]);
            }
        } else {
            result = executed.value;
        }

        const item: StatementRunResult = { result };
        if (result.kind === 'create-plan') {
            const plan = result.plan;
            const object = await rejectedAt(statement.span, () => session.workspace.createRoot(plan));
            if (plan.kind === 'create-database') {
                item.deploy = await rejectedAt(statement.span, () => plan.afterCreate(object));
                await useDatabase(session, object.getId());
            }
        } else if (result.kind === 'use-database') {
            await useDatabase(session, result.database);
        } else if (result.kind === 'set-view') {
            session.setDefaultView({
                at: await resolveVersionExpr(session, result.at, { kind: 'group', id: session.currentGroup ?? '', group: undefined }),
                from: result.from === undefined
                    ? undefined
                    : await resolveVersionExpr(session, result.from, { kind: 'group', id: session.currentGroup ?? '', group: undefined }),
            });
        }

        const trigger = extractRefUpdateTrigger(effectiveBound);
        if (session.refAutoUpdate !== 'off' && trigger !== undefined) {
            item.refUpdates = await propagateRefUpdates(session, trigger.sourceGroupId, trigger.author, options);
        }

        results.push(item);
    }

    return { results };
}

// Creating the root object validates it again; the binder catches what it
// can, and anything else is reported at the statement.
async function rejectedAt<T>(span: TextSpan, run: () => Promise<T>): Promise<T> {
    try {
        return await run();
    } catch (e) {
        if (!(e instanceof ValidationRejectedError)) throw e;
        throw new LanguageError([{ code: 'VALIDATION_REJECTED', message: formatValidationFailure(e.why), span, severity: 'error' }]);
    }
}

// Makes `id` the current database; a current group of another database is
// cleared.
export async function useDatabase(session: RdbSession, id: B64Hash): Promise<void> {
    session.setCurrentDatabase(id);
    if (session.currentGroup === undefined) return;
    const db = session.workspace.roots.get(id)?.object as RDbImpl | undefined;
    if (db !== undefined && !(await db.getMemberGroups()).includes(session.currentGroup)) session.clearCurrentGroup();
}

export class LanguageError extends Error {
    constructor(readonly diagnostics: LangDiagnostic[], readonly hints: string[] = []) {
        super(diagnostics.map((d) => `${d.code}: ${d.message}`).join('\n'));
    }
}

const KEY_PASSPHRASE_REQUIRED = /^Key '([^']+)' is not unlocked$/;

export function keyPassphraseRequiredFromError(e: unknown): KeyPassphraseRequiredError | undefined {
    if (e instanceof KeyPassphraseRequiredError) return e;
    if (e instanceof LanguageError) {
        for (const diagnostic of e.diagnostics) {
            const match = KEY_PASSPHRASE_REQUIRED.exec(diagnostic.message);
            if (match !== null) return new KeyPassphraseRequiredError(match[1]!);
        }
    }
    return undefined;
}

async function resolveVersionExpr(session: RdbSession, expr: VersionExpr | undefined, scope: VersionScope): Promise<Version> {
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
    if (matches.length === 1) return matches[0]!;

    if (matches.length === 0) throw new Error(`Unknown rowId prefix '#${prefix}' in ${tableName}`);
    const examples = matches.slice(0, 5).map((rowId) => `#${rowId}`).join(', ');
    throw new Error(`Ambiguous rowId prefix '#${prefix}' in ${tableName}: ${examples}`);
}
