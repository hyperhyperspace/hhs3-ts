import type { Interface } from "node:readline/promises";

import type { B64Hash, KeyId, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import {
    deriveRowId,
    evaluatePredicate,
    evaluateRowOpRestriction,
    RTableGroupImpl,
} from "@hyper-hyper-space/hhs3_rdb";
import type { RowOpPayload } from "@hyper-hyper-space/hhs3_rdb";
import type {
    AddMemberStatement,
    AlterSchemaStatement,
    AstStatement,
    AuthorExpr,
    BoundDelete,
    BoundInsert,
    BoundStatement,
    BoundUpdate,
    BoundUpdateRef,
    BoundUpdateSchema,
    LangBindContext,
    LangDiagnostic,
} from "@hyper-hyper-space/hhs3_rdb_lang";

import { formatDisplayString } from "../format/display.js";
import type { StoredKeyRecord } from "../keys/keystore.js";
import { WorkspaceSession } from "./session.js";

export type AuthRetryBound =
    | BoundInsert
    | BoundUpdate
    | BoundDelete
    | BoundUpdateRef
    | BoundUpdateSchema;

export type AuthorCandidate = {
    keyId: KeyId;
    label: string;
    unlocked: boolean;
};

export type AuthorResolution = {
    identity?: OwnIdentity;
    locked?: AuthorCandidate;
    candidates: AuthorCandidate[];
    rejected?: AuthorCandidate[];
};

export type ResolveAuthorOptions = {
    scanKeystore?: boolean;
};

export type ReplAuthContext = {
    rl?: Interface;
    onProgress?: (line: string) => void;
};

const AUTH_FAILURE_PATTERNS = [
    'does not satisfy ALLOW',
    'canObserve predicate rejected',
    'canDeploy predicate rejected',
    'must be authored',
    'rowAuthor = $author',
];

const BIND_AUTHOR_REQUIRED_MESSAGES = new Set([
    'ALTER SCHEMA requires an author identity',
    'ADD SCHEMA requires BY when the database declares creators',
    'ADD TABLEGROUP requires BY when the database declares creators',
    'ADD SCHEMA requires an author when the database declares creators',
    'ADD TABLEGROUP requires an author when the database declares creators',
]);

export function isBindAuthorRequiredFailure(diagnostics: LangDiagnostic[]): boolean {
    return diagnostics.some((d) =>
        d.code === 'BIND_UNKNOWN_NAME' && BIND_AUTHOR_REQUIRED_MESSAGES.has(d.message),
    );
}

export function hasExplicitByAst(stmt: { author?: AuthorExpr }): boolean {
    return stmt.author !== undefined;
}

export function isBindAuthorRetryStatement(stmt: AstStatement): stmt is AlterSchemaStatement | AddMemberStatement {
    return stmt.kind === 'alter-schema' || stmt.kind === 'add-member';
}

export function labelForKeyId(session: WorkspaceSession, keyId: KeyId): string {
    const record = session.keystore?.list().find((key) => key.keyId === keyId);
    if (record !== undefined) return `$${record.label}`;
    return formatDisplayString(session, keyId, { role: 'hash', hashPrefix: true });
}

export function formatAuthorHint(candidates: AuthorCandidate[]): string | undefined {
    if (candidates.length === 0) return undefined;
    const labels = candidates.map((c) => `$${c.label}`);
    if (labels.length === 1) return `hint: BY ${labels[0]}`;
    return `hint: BY ${labels.slice(0, -1).join(', ')} or ${labels[labels.length - 1]}`;
}

function toCandidate(session: WorkspaceSession, record: StoredKeyRecord): AuthorCandidate {
    return {
        keyId: record.keyId,
        label: record.label,
        unlocked: session.isUnlocked(record.keyId),
    };
}

function candidateFromIdentity(session: WorkspaceSession, identity: OwnIdentity): AuthorCandidate {
    const record = session.keystore?.list().find((key) => key.keyId === identity.keyId);
    return {
        keyId: identity.keyId,
        label: record?.label ?? identity.keyId,
        unlocked: session.isUnlocked(identity.keyId),
    };
}

export async function scanKeystore(
    session: WorkspaceSession,
    test: (keyId: KeyId) => Promise<boolean>,
): Promise<AuthorCandidate[]> {
    if (session.keystore === undefined) return [];
    const out: AuthorCandidate[] = [];
    for (const record of session.keystore.list()) {
        if (await test(record.keyId)) out.push(toCandidate(session, record));
    }
    return out;
}

export async function resolveAuthorForGate(
    session: WorkspaceSession,
    test: (keyId: KeyId) => Promise<boolean>,
    preferred: (OwnIdentity | undefined)[],
    options?: ResolveAuthorOptions,
): Promise<AuthorResolution> {
    const doScanKeystore = options?.scanKeystore !== false;
    const rejected: AuthorCandidate[] = [];
    const seen = new Set<KeyId>();

    for (const identity of preferred) {
        if (identity === undefined || seen.has(identity.keyId)) continue;
        seen.add(identity.keyId);
        const candidate = candidateFromIdentity(session, identity);

        if (!await test(identity.keyId)) {
            rejected.push(candidate);
            continue;
        }

        if (session.isUnlocked(identity.keyId)) {
            return {
                identity,
                candidates: doScanKeystore ? await scanKeystore(session, test) : [],
                rejected: rejected.length > 0 ? rejected : undefined,
            };
        }

        return {
            locked: candidate,
            candidates: [],
            rejected: rejected.length > 0 ? rejected : undefined,
        };
    }

    if (!doScanKeystore) {
        return { candidates: [], rejected: rejected.length > 0 ? rejected : undefined };
    }

    const candidates = await scanKeystore(session, test);
    const unlocked = candidates.find((c) => c.unlocked);
    if (unlocked !== undefined) {
        const identity = session.resolveIdentity(unlocked.label);
        if (identity !== undefined) return { identity, candidates };
    }

    const locked = candidates.find((c) => !c.unlocked);
    return { locked, candidates };
}

export async function evaluateObserveGateKey(
    observer: RTableGroupImpl,
    foreignGroupId: B64Hash,
    refAt: Version,
    refFrom: Version,
    keyId: KeyId,
): Promise<boolean> {
    return observer.evaluateObserveGate(foreignGroupId, keyId, refAt, refFrom);
}

export async function evaluateCanDeployKey(
    group: RTableGroupImpl,
    at: Version,
    keyId: KeyId,
): Promise<boolean> {
    const canDeploy = group.getCanDeploy();
    if (canDeploy === undefined) return true;
    return evaluatePredicate(canDeploy, {
        getTableView: async (table) => group.makeTable(table).getView(at, at),
        getForeignTableView: (groupName, table) => group.resolveForeignTableView(groupName, table, at, at),
        author: keyId,
        context: 'object',
    });
}

function rowOpFromBound(bound: BoundInsert | BoundUpdate | BoundDelete, keyId: KeyId): RowOpPayload {
    switch (bound.kind) {
        case 'insert':
            return {
                action: 'insert',
                rowId: deriveRowId(bound.uuid, keyId),
                uuid: bound.uuid,
                values: bound.values,
                author: keyId,
            };
        case 'update':
            return { action: 'update', rowId: bound.rowId, values: bound.values, author: keyId };
        case 'delete':
            return { action: 'delete', rowId: bound.rowId, author: keyId };
    }
}

export async function evaluateRowRestrictionKey(
    bound: BoundInsert | BoundUpdate | BoundDelete,
    keyId: KeyId,
): Promise<boolean> {
    const group = bound.table.group as RTableGroupImpl;
    const at = bound.at;
    const groupView = await group.getView(at, at);
    const schemaView = groupView.getSchemaView();
    const table = bound.table.tableName;
    const op = rowOpFromBound(bound, keyId);

    return evaluateRowOpRestriction(
        op,
        table,
        schemaView,
        (name) => groupView.getTableView(name),
        (groupName, name) => group.resolveForeignTableView(groupName, name, at, at),
    );
}

export async function resolveObserveAuthor(
    session: WorkspaceSession,
    observer: RTableGroupImpl,
    foreignGroupId: B64Hash,
    refAt: Version,
    refFrom: Version,
    preferred: (OwnIdentity | undefined)[],
    options?: ResolveAuthorOptions,
): Promise<AuthorResolution> {
    return resolveAuthorForGate(
        session,
        (keyId) => evaluateObserveGateKey(observer, foreignGroupId, refAt, refFrom, keyId),
        preferred,
        options,
    );
}

function isAuthRelatedFailure(diagnostics: LangDiagnostic[]): boolean {
    return diagnostics.some((d) =>
        d.code === 'VALIDATION_REJECTED'
        && AUTH_FAILURE_PATTERNS.some((pattern) => d.message.includes(pattern)),
    );
}

export function isAuthRetryBound(bound: BoundStatement): bound is AuthRetryBound {
    switch (bound.kind) {
        case 'insert':
        case 'update':
        case 'delete':
        case 'update-ref':
        case 'update-schema':
            return true;
        default:
            return false;
    }
}

export function hasExplicitBy(bound: AuthRetryBound): boolean {
    return bound.ast.author !== undefined;
}

export function boundWithAuthor(bound: AuthRetryBound, author: OwnIdentity): AuthRetryBound {
    return { ...bound, author };
}

async function gateTestForBound(bound: AuthRetryBound): Promise<(keyId: KeyId) => Promise<boolean>> {
    switch (bound.kind) {
        case 'insert':
        case 'update':
        case 'delete':
            return (keyId) => evaluateRowRestrictionKey(bound, keyId);
        case 'update-ref': {
            const group = bound.group.group as RTableGroupImpl | undefined;
            if (group === undefined) throw new Error('UPDATE REF target group is not loaded');
            const foreignId = resolveForeignGroupId(group, bound.ref);
            if (foreignId === undefined) throw new Error(`Unknown bound group '${bound.ref}'`);
            const at = bound.at;
            return (keyId) => evaluateObserveGateKey(group, foreignId, at, at, keyId);
        }
        case 'update-schema': {
            const group = bound.group.group as RTableGroupImpl | undefined;
            if (group === undefined) throw new Error('UPDATE SCHEMA target group is not loaded');
            return (keyId) => evaluateCanDeployKey(group, bound.at, keyId);
        }
    }
}

export async function resolveAuthorForBoundFailure(
    session: WorkspaceSession,
    bound: AuthRetryBound,
): Promise<AuthorResolution> {
    const test = await gateTestForBound(bound);
    return resolveAuthorForGate(
        session,
        test,
        [bound.author, await session.currentAuthor()],
    );
}

function resolveForeignGroupId(group: RTableGroupImpl, ref: string): B64Hash | undefined {
    const bindings = group.getBindings();
    if (Object.prototype.hasOwnProperty.call(bindings, ref)) return bindings[ref];
    if (Object.values(bindings).includes(ref as B64Hash)) return ref as B64Hash;
    return undefined;
}

export async function suggestAuthorsForFailure(
    session: WorkspaceSession,
    bound: BoundStatement,
    diagnostics: LangDiagnostic[],
): Promise<string | undefined> {
    if (!isAuthRelatedFailure(diagnostics)) return undefined;

    let candidates: AuthorCandidate[] = [];

    switch (bound.kind) {
        case 'insert':
        case 'update':
        case 'delete':
            candidates = await scanKeystore(session, (keyId) => evaluateRowRestrictionKey(bound, keyId));
            break;
        case 'update-ref': {
            const group = bound.group.group as RTableGroupImpl | undefined;
            if (group === undefined) break;
            const foreignId = resolveForeignGroupId(group, bound.ref);
            if (foreignId === undefined) break;
            const at = bound.at;
            candidates = await scanKeystore(session, (keyId) =>
                evaluateObserveGateKey(group, foreignId, at, at, keyId),
            );
            break;
        }
        case 'update-schema': {
            const group = bound.group.group as RTableGroupImpl | undefined;
            if (group === undefined) break;
            candidates = await scanKeystore(session, (keyId) => evaluateCanDeployKey(group, bound.at, keyId));
            break;
        }
        default:
            break;
    }

    return formatAuthorHint(candidates);
}

export async function resolveAuthorsForAlterSchema(
    session: WorkspaceSession,
    stmt: AlterSchemaStatement,
    context: LangBindContext,
): Promise<AuthorResolution> {
    const schema = await context.resolveSchema(stmt.schema);
    if (schema.schema === undefined) return { candidates: [] };
    const at = await context.resolveVersion(stmt.at, { kind: 'schema', id: schema.id, schema: schema.schema });
    const view = await schema.schema.getView(at, at);
    return resolveAuthorForGate(
        session,
        (keyId) => Promise.resolve(view.isCreator(keyId)),
        [await session.currentAuthor()],
    );
}

export async function resolveAuthorsForAddMember(
    session: WorkspaceSession,
    stmt: AddMemberStatement,
    context: LangBindContext,
): Promise<AuthorResolution> {
    const database = await context.resolveDatabase(stmt.database);
    if (database.db === undefined || database.db.getCreators().length === 0) return { candidates: [] };
    return resolveAuthorForGate(
        session,
        (keyId) => Promise.resolve(database.db!.isCreator(keyId)),
        [await session.currentAuthor()],
    );
}

async function resolveAuthorsForBindStatement(
    session: WorkspaceSession,
    stmt: AstStatement,
    context: LangBindContext,
): Promise<AuthorResolution | undefined> {
    switch (stmt.kind) {
        case 'alter-schema':
            return resolveAuthorsForAlterSchema(session, stmt, context);
        case 'add-member':
            return resolveAuthorsForAddMember(session, stmt, context);
        default:
            return undefined;
    }
}

export async function suggestAuthorsForBindFailure(
    session: WorkspaceSession,
    stmt: AstStatement,
    diagnostics: LangDiagnostic[],
    context: LangBindContext,
): Promise<string | undefined> {
    if (!isBindAuthorRequiredFailure(diagnostics)) return undefined;
    try {
        const resolution = await resolveAuthorsForBindStatement(session, stmt, context);
        if (resolution === undefined) return undefined;
        return formatAuthorHint(resolution.candidates);
    } catch {
        return undefined;
    }
}
