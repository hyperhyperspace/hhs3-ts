import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type {
    AddMemberStatement,
    AlterSchemaStatement,
    AstStatement,
    BoundStatement,
    LangBindContext,
    LangDiagnostic,
    LangExecutionResult,
} from "@hyper-hyper-space/hhs3_rdb_lang";
import { bind, execute } from "@hyper-hyper-space/hhs3_rdb_lang";

import type { RdbSession } from "./session.js";
import {
    boundWithAuthor,
    hasExplicitBy,
    hasExplicitByAst,
    isAuthRetryBound,
    isBindAuthorRequiredFailure,
    isBindAuthorRetryStatement,
    labelForKeyId,
    resolveAuthorForBoundFailure,
    resolveAuthorsForAddMember,
    resolveAuthorsForAlterSchema,
    type AuthRetryBound,
    type AuthorResolution,
} from "./authz_suggest.js";
import type { AuthInteractionContext } from "./prompts.js";

export type AuthSignRetryResult = {
    result: LangExecutionResult;
    bound: AuthRetryBound;
};

const AUTH_FAILURE_PATTERNS = [
    'does not satisfy ALLOW',
    'canObserve predicate rejected',
    'canDeploy predicate rejected',
    'must be authored',
    'rowAuthor = $author',
];

function isAuthRelatedFailure(diagnostics: LangDiagnostic[]): boolean {
    return diagnostics.some((d) =>
        d.code === 'VALIDATION_REJECTED'
        && AUTH_FAILURE_PATTERNS.some((pattern) => d.message.includes(pattern)),
    );
}

function formatOpKind(kind: AuthRetryBound['kind']): string {
    switch (kind) {
        case 'insert': return 'Insert';
        case 'update': return 'Update';
        case 'delete': return 'Delete';
        case 'update-ref': return 'Update ref';
        case 'update-schema': return 'Update schema';
    }
}

function formatBindOpKind(stmt: AlterSchemaStatement | AddMemberStatement): string {
    switch (stmt.kind) {
        case 'alter-schema': return 'Alter schema';
        case 'add-member': return stmt.member === 'schema' ? 'Add schema' : 'Add tablegroup';
    }
}

export function bindContextWithAuthor(context: LangBindContext, author: OwnIdentity): LangBindContext {
    return {
        ...context,
        currentAuthor: () => Promise.resolve(author),
        resolveVariable: (name) =>
            name === 'me' || name === 'author'
                ? Promise.resolve(author)
                : context.resolveVariable(name),
    };
}

async function identityForSignRetry(
    session: RdbSession,
    resolution: AuthorResolution,
    auth?: AuthInteractionContext,
): Promise<OwnIdentity | undefined> {
    if (resolution.identity !== undefined) return resolution.identity;

    const locked = resolution.locked;
    if (locked === undefined) return undefined;
    if (auth?.unlockIdentity === undefined) return undefined;

    return auth.unlockIdentity(locked.label);
}

function signRetryAuthorLabel(session: RdbSession, resolution: AuthorResolution): string | undefined {
    if (resolution.identity !== undefined) return labelForKeyId(session, resolution.identity.keyId);
    if (resolution.locked !== undefined) return `$${resolution.locked.label}`;
    return undefined;
}

export async function tryAuthSignRetry(
    session: RdbSession,
    bound: BoundStatement,
    diagnostics: LangDiagnostic[],
    auth?: AuthInteractionContext,
): Promise<AuthSignRetryResult | undefined> {
    if (auth?.canPrompt?.() !== true) return undefined;
    if (session.keyVault === undefined) return undefined;
    if (!isAuthRelatedFailure(diagnostics)) return undefined;
    if (!isAuthRetryBound(bound)) return undefined;
    if (hasExplicitBy(bound)) return undefined;

    const resolution = await resolveAuthorForBoundFailure(session, bound);
    const authorLabel = signRetryAuthorLabel(session, resolution);
    if (authorLabel === undefined) return undefined;

    if (auth.confirmSignRetry === undefined) return undefined;
    if (!await auth.confirmSignRetry(authorLabel, formatOpKind(bound.kind))) return undefined;

    const identity = await identityForSignRetry(session, resolution, auth);
    if (identity === undefined) return undefined;

    const signed = boundWithAuthor(bound, identity);
    const retried = await execute(signed);
    if (!retried.ok) return undefined;

    return { result: retried.value, bound: signed };
}

export async function tryBindAuthorRetry(
    session: RdbSession,
    statement: AstStatement,
    diagnostics: LangDiagnostic[],
    context: LangBindContext,
    auth?: AuthInteractionContext,
): Promise<BoundStatement | undefined> {
    if (auth?.canPrompt?.() !== true) return undefined;
    if (session.keyVault === undefined) return undefined;
    if (!isBindAuthorRequiredFailure(diagnostics)) return undefined;
    if (!isBindAuthorRetryStatement(statement)) return undefined;
    if (hasExplicitByAst(statement)) return undefined;

    let resolution: AuthorResolution;
    try {
        resolution = statement.kind === 'alter-schema'
            ? await resolveAuthorsForAlterSchema(session, statement, context)
            : await resolveAuthorsForAddMember(session, statement, context);
    } catch {
        return undefined;
    }

    const authorLabel = signRetryAuthorLabel(session, resolution);
    if (authorLabel === undefined) return undefined;

    if (auth.confirmSignRetry === undefined) return undefined;
    if (!await auth.confirmSignRetry(authorLabel, formatBindOpKind(statement))) return undefined;

    const identity = await identityForSignRetry(session, resolution, auth);
    if (identity === undefined) return undefined;

    const bindStatement = statementForBindRetry(statement, resolution, session);
    const rebound = await bind(bindStatement, bindContextWithAuthor(context, identity));
    if (!rebound.ok) return undefined;
    return rebound.value;
}

function statementForBindRetry(
    statement: AlterSchemaStatement | AddMemberStatement,
    resolution: AuthorResolution,
    session: RdbSession,
): AstStatement {
    if (statement.kind !== 'add-member') return statement;

    const label = resolution.identity !== undefined
        ? session.keyVault?.list().find((key) => key.keyId === resolution.identity!.keyId)?.label
        : resolution.locked?.label;
    if (label === undefined) return statement;

    return {
        ...statement,
        author: { kind: 'variable', name: label, span: statement.span },
    };
}
