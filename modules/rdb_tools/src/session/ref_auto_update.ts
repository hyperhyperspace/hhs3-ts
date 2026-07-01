import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import { refVersionAtOrAbove } from "@hyper-hyper-space/hhs3_mvt";
import type { RTableGroup } from "@hyper-hyper-space/hhs3_rdb";
import { RTableGroupImpl } from "@hyper-hyper-space/hhs3_rdb";
import type { BoundStatement } from "@hyper-hyper-space/hhs3_rdb_lang";

import { formatDisplayString } from "../format/display.js";
import { WorkspaceSession } from "./session.js";

export type RefUpdateTrigger = {
    sourceGroupId: B64Hash;
    author: OwnIdentity | undefined;
};

export type ObserverRef = {
    observerId: B64Hash;
    observer: RTableGroup;
    bindingName: string;
};

export function extractRefUpdateTrigger(bound: BoundStatement): RefUpdateTrigger | undefined {
    switch (bound.kind) {
        case 'insert':
        case 'update':
        case 'delete':
            return { sourceGroupId: bound.table.groupId, author: bound.author };
        case 'bundle':
        case 'update-schema':
            return { sourceGroupId: bound.group.id, author: bound.author };
        default:
            return undefined;
    }
}

export function findObservers(session: WorkspaceSession, foreignGroupId: B64Hash): ObserverRef[] {
    const observers: ObserverRef[] = [];
    for (const root of session.workspace.roots.list('group')) {
        if (root.object === undefined) continue;
        const observer = root.object as RTableGroup;
        for (const [bindingName, boundId] of Object.entries(observer.getBindings())) {
            if (boundId === foreignGroupId) {
                observers.push({ observerId: root.id, observer, bindingName });
            }
        }
    }
    return observers;
}

function observerDisplayName(session: WorkspaceSession, observerGroupId: B64Hash): string {
    const name = session.workspace.roots.get(observerGroupId)?.name;
    if (name !== undefined && name.length > 0) return name;
    return formatDisplayString(session, observerGroupId, { role: 'hash', hashPrefix: true });
}

export function formatRefAutoUpdateNotice(
    session: WorkspaceSession,
    observerGroupId: B64Hash,
    entryHash: B64Hash,
): string {
    const group = observerDisplayName(session, observerGroupId);
    const hash = formatDisplayString(session, entryHash, { role: 'hash', hashPrefix: true });
    return `updated ref on ${group} to ${hash}`;
}

export function formatRefAutoUpdateFailure(
    session: WorkspaceSession,
    observerGroupId: B64Hash,
    message: string,
): string {
    const group = observerDisplayName(session, observerGroupId);
    return `ref update on ${group} failed: ${message}`;
}

async function loadForeignGroup(session: WorkspaceSession, foreignGroupId: B64Hash): Promise<RTableGroupImpl> {
    const root = session.workspace.roots.get(foreignGroupId);
    if (root?.object !== undefined) return root.object as RTableGroupImpl;
    const object = await session.workspace.replica.getObject(foreignGroupId);
    if (object === undefined) throw new Error(`Group '${foreignGroupId}' is not loaded`);
    return object as RTableGroupImpl;
}

async function isAlreadyObserved(
    observer: RTableGroup,
    foreignGroupId: B64Hash,
    targetVersion: Version,
    foreignGroup: RTableGroupImpl,
): Promise<boolean> {
    const view = await observer.getView();
    const current = await view.resolveRefVersion(foreignGroupId);
    const foreignDag = await foreignGroup.getCausalDag();
    return refVersionAtOrAbove(foreignDag, current, targetVersion);
}

function pairKey(observerId: B64Hash, foreignId: B64Hash): string {
    return `${observerId}:${foreignId}`;
}

export async function propagateRefUpdates(
    session: WorkspaceSession,
    sourceGroupId: B64Hash,
    author: OwnIdentity | undefined,
): Promise<string[]> {
    const notices: string[] = [];
    const visitedPairs = new Set<string>();
    const queue: B64Hash[] = [sourceGroupId];

    while (queue.length > 0) {
        const foreignGroupId = queue.shift()!;
        const foreignGroup = await loadForeignGroup(session, foreignGroupId);
        const targetVersion = await (await foreignGroup.getScopedDag()).getFrontier();

        for (const { observerId, observer, bindingName } of findObservers(session, foreignGroupId)) {
            const key = pairKey(observerId, foreignGroupId);
            if (visitedPairs.has(key)) continue;

            try {
                if (await isAlreadyObserved(observer, foreignGroupId, targetVersion, foreignGroup)) {
                    visitedPairs.add(key);
                    continue;
                }

                const entryHash = await observer.observe(bindingName, targetVersion, author);
                visitedPairs.add(key);
                notices.push(formatRefAutoUpdateNotice(session, observerId, entryHash));
                queue.push(observerId);
            } catch (e) {
                visitedPairs.add(key);
                const message = e instanceof Error ? e.message : String(e);
                notices.push(formatRefAutoUpdateFailure(session, observerId, message));
            }
        }
    }

    return notices;
}
