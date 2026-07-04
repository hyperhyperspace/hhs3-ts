import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { refVersionAtOrAbove } from "@hyper-hyper-space/hhs3_mvt";
import type { RTableGroup } from "@hyper-hyper-space/hhs3_rdb";
import { RTableGroupImpl } from "@hyper-hyper-space/hhs3_rdb";
import type { BoundStatement } from "@hyper-hyper-space/hhs3_rdb_lang";

import { formatDisplayString } from "../format/display.js";
import { confirmRefUpdateUnlock, fulfillKeyPassphrase } from "../repl/passphrase.js";
import { canPromptForKeys } from "../repl/prompt_tty.js";
import {
    labelForKeyId,
    ReplAuthContext,
    resolveObserveAuthor,
} from "./authz_suggest.js";
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
    targetVersion: import("@hyper-hyper-space/hhs3_mvt").Version,
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

async function observeWithResolvedAuthor(
    session: WorkspaceSession,
    observer: RTableGroup,
    observerId: B64Hash,
    bindingName: string,
    foreignGroupId: B64Hash,
    targetVersion: import("@hyper-hyper-space/hhs3_mvt").Version,
    triggerAuthor: OwnIdentity | undefined,
    auth?: ReplAuthContext,
): Promise<B64Hash> {
    const impl = observer as RTableGroupImpl;
    const refAt = targetVersion;
    const refFrom = targetVersion;

    if (impl.observeGateFor(foreignGroupId) === undefined) {
        return observer.observe(bindingName, targetVersion, triggerAuthor);
    }

    const preferred = [triggerAuthor, await session.currentAuthor()];
    const resolution = await resolveObserveAuthor(session, impl, foreignGroupId, refAt, refFrom, preferred);

    if (resolution.identity !== undefined) {
        return observer.observe(bindingName, targetVersion, resolution.identity);
    }

    if (resolution.locked !== undefined && canPromptForKeys(session) && auth?.rl !== undefined) {
        const label = resolution.locked.label;
        const authorLabel = labelForKeyId(session, resolution.locked.keyId);
        await confirmRefUpdateUnlock(auth.rl, observerDisplayName(session, observerId), authorLabel);
        await fulfillKeyPassphrase(session, { kind: 'unlock', label }, auth.rl);
        const identity = session.resolveIdentity(label);
        if (identity === undefined) throw new Error(`Key '${label}' is not unlocked`);
        return observer.observe(bindingName, targetVersion, identity);
    }

    if (resolution.locked !== undefined) {
        throw new Error(`needs ${labelForKeyId(session, resolution.locked.keyId)} (locked)`);
    }

    throw new Error('no keystore identity satisfies gate');
}

export async function propagateRefUpdates(
    session: WorkspaceSession,
    sourceGroupId: B64Hash,
    author: OwnIdentity | undefined,
    auth?: ReplAuthContext,
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

                const entryHash = await observeWithResolvedAuthor(
                    session,
                    observer,
                    observerId,
                    bindingName,
                    foreignGroupId,
                    targetVersion,
                    author,
                    auth,
                );
                visitedPairs.add(key);
                const notice = formatRefAutoUpdateNotice(session, observerId, entryHash);
                notices.push(notice);
                auth?.onProgress?.(notice);
                queue.push(observerId);
            } catch (e) {
                visitedPairs.add(key);
                const message = e instanceof Error ? e.message : String(e);
                const notice = formatRefAutoUpdateFailure(session, observerId, message);
                notices.push(notice);
                auth?.onProgress?.(notice);
            }
        }
    }

    return notices;
}
