import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { Version, version } from "@hyper-hyper-space/hhs3_mvt";
import type { HashScope, VersionMember, VersionScope } from "@hyper-hyper-space/hhs3_rdb_lang";

import type { WorkspaceSession } from "./session.js";

export async function resolveVersionRef(
    session: WorkspaceSession,
    text: string,
    objectId: B64Hash,
): Promise<Version> {
    const trimmed = text.trim();
    if (trimmed.toLowerCase() === 'latest') {
        return frontierForObjectId(session, objectId);
    }
    if (trimmed.startsWith('#')) {
        const hash = await session.workspace.roots.resolveHash(
            { kind: 'hash', prefix: trimmed.slice(1), span: { start: 0, end: trimmed.length, line: 1, column: 1 } },
            { kind: 'object', objectId },
        );
        return version(hash);
    }
    const hash = session.aliases.get('version', trimmed);
    if (hash === undefined) throw new Error(`Unknown version ref '${trimmed}'`);
    await assertHashInScopedDag(session, hash, { kind: 'object', objectId });
    return version(hash);
}

export async function resolveVersionMember(
    session: WorkspaceSession,
    member: VersionMember,
    hashScope: HashScope,
): Promise<B64Hash> {
    if (member.kind === 'hash') {
        return session.workspace.roots.resolveHash(member, hashScope);
    }
    const hash = session.aliases.get('version', member.text);
    if (hash === undefined) throw new Error(`Unknown version alias '${member.text}'`);
    await assertHashInScopedDag(session, hash, hashScope);
    return hash;
}

export async function assertHashInScopedDag(
    session: WorkspaceSession,
    hash: B64Hash,
    hashScope: HashScope,
): Promise<void> {
    const candidates = await session.workspace.roots.hashCandidates(hashScope);
    if (!candidates.includes(hash)) {
        throw new Error(`Version alias hash '${hash}' is not in this object's history`);
    }
}

export async function frontierForScope(scope: VersionScope): Promise<Version> {
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

export function hashScopeForVersionScope(scope: VersionScope): HashScope {
    if (scope.kind === 'schema') return { kind: 'object', objectId: scope.id };
    if (scope.kind === 'group') return { kind: 'object', objectId: scope.id };
    if (scope.kind === 'table') return { kind: 'object', objectId: scope.groupId };
    return { kind: 'object', objectId: scope.id };
}

async function frontierForObjectId(session: WorkspaceSession, objectId: B64Hash): Promise<Version> {
    const root = session.workspace.roots.get(objectId);
    const object = root?.object;
    if (object === undefined) throw new Error(`Object '${objectId}' is not loaded`);
    return (await object.getScopedDag()).getFrontier();
}
