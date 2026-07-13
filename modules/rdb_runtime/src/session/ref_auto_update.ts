import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { refVersionAtOrAbove } from "@hyper-hyper-space/hhs3_mvt";
import type { RTableGroup } from "@hyper-hyper-space/hhs3_rdb";
import { RTableGroupImpl } from "@hyper-hyper-space/hhs3_rdb";
import type { BoundStatement } from "@hyper-hyper-space/hhs3_rdb_lang";

import {
    AuthorCandidate,
    labelForKeyId,
    resolveObserveAuthor,
} from "./authz_suggest.js";
import type { AuthInteractionContext } from "./prompts.js";
import { KeyUnlockDeclinedError } from "./prompts.js";
import type { RefAutoUpdateMode, RdbSession } from "./session.js";

export type RefUpdateTrigger = {
    sourceGroupId: B64Hash;
    author: OwnIdentity | undefined;
};

export type ObserverRef = {
    observerId: B64Hash;
    observer: RTableGroup;
    bindingName: string;
};

export type RefUpdateEvent = {
    kind: 'updated' | 'skipped' | 'failed';
    observerGroupId: B64Hash;
    message: string;
    entryHash?: B64Hash;
};

export class RefAutoUpdateSkippedError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'RefAutoUpdateSkippedError';
    }
}

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

export function findObservers(session: RdbSession, foreignGroupId: B64Hash): ObserverRef[] {
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

function observerDisplayName(session: RdbSession, observerGroupId: B64Hash): string {
    const name = session.workspace.roots.get(observerGroupId)?.name;
    if (name !== undefined && name.length > 0) return name;
    return observerGroupId.slice(0, 8);
}

export function formatRefAutoUpdateNotice(
    session: RdbSession,
    observerGroupId: B64Hash,
    entryHash: B64Hash,
): string {
    const group = observerDisplayName(session, observerGroupId);
    const hash = `#${entryHash.slice(0, 8)}`;
    return `updated ref on ${group} to ${hash}`;
}

export function formatRefAutoUpdateFailure(
    session: RdbSession,
    observerGroupId: B64Hash,
    message: string,
): string {
    const group = observerDisplayName(session, observerGroupId);
    return `ref update on ${group} failed: ${message}`;
}

export function formatRefAutoUpdateSkipped(
    session: RdbSession,
    observerGroupId: B64Hash,
    rejected: AuthorCandidate[],
): string {
    const group = observerDisplayName(session, observerGroupId);
    if (rejected.length === 0) return `ref update on ${group} skipped: no author configured`;
    const labels = rejected.map((c) => `$${c.label}`).join(', ');
    return `ref update on ${group} skipped: ${labels} not authorized`;
}

async function loadForeignGroup(session: RdbSession, foreignGroupId: B64Hash): Promise<RTableGroupImpl> {
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
    session: RdbSession,
    observer: RTableGroup,
    observerId: B64Hash,
    bindingName: string,
    foreignGroupId: B64Hash,
    targetVersion: import("@hyper-hyper-space/hhs3_mvt").Version,
    triggerAuthor: OwnIdentity | undefined,
    mode: RefAutoUpdateMode,
    auth?: AuthInteractionContext,
): Promise<B64Hash> {
    const impl = observer as RTableGroupImpl;
    const refAt = targetVersion;
    const refFrom = targetVersion;

    if (impl.observeGateFor(foreignGroupId) === undefined) {
        return observer.observe(bindingName, targetVersion, triggerAuthor);
    }

    const preferred = [triggerAuthor, await session.currentAuthor()];
    const resolution = await resolveObserveAuthor(
        session,
        impl,
        foreignGroupId,
        refAt,
        refFrom,
        preferred,
        { scanKeystore: mode === 'auto' },
    );

    if (resolution.identity !== undefined) {
        return observer.observe(bindingName, targetVersion, resolution.identity);
    }

    if (resolution.locked !== undefined && auth?.canPrompt?.() === true && auth.unlockIdentity !== undefined) {
        const label = resolution.locked.label;
        const authorLabel = labelForKeyId(session, resolution.locked.keyId);
        if (auth.confirmRefUpdateUnlock !== undefined) {
            await auth.confirmRefUpdateUnlock(observerDisplayName(session, observerId), authorLabel);
        }
        const identity = await auth.unlockIdentity(label);
        if (identity === undefined) throw new KeyUnlockDeclinedError(authorLabel);
        return observer.observe(bindingName, targetVersion, identity);
    }

    if (resolution.locked !== undefined) {
        throw new Error(`needs ${labelForKeyId(session, resolution.locked.keyId)} (locked)`);
    }

    if (mode === 'self') {
        throw new RefAutoUpdateSkippedError(
            formatRefAutoUpdateSkipped(session, observerId, resolution.rejected ?? []),
        );
    }

    throw new Error('no keystore identity satisfies gate');
}

export async function propagateRefUpdates(
    session: RdbSession,
    sourceGroupId: B64Hash,
    author: OwnIdentity | undefined,
    auth?: AuthInteractionContext,
): Promise<RefUpdateEvent[]> {
    const mode = session.refAutoUpdate;
    if (mode === 'off') return [];

    const events: RefUpdateEvent[] = [];
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
                    mode,
                    auth,
                );
                visitedPairs.add(key);
                const message = formatRefAutoUpdateNotice(session, observerId, entryHash);
                const event: RefUpdateEvent = { kind: 'updated', observerGroupId: observerId, message, entryHash };
                events.push(event);
                auth?.onProgress?.(message);
                queue.push(observerId);
            } catch (e) {
                visitedPairs.add(key);
                if (e instanceof RefAutoUpdateSkippedError) {
                    const event: RefUpdateEvent = { kind: 'skipped', observerGroupId: observerId, message: e.message };
                    events.push(event);
                    auth?.onProgress?.(e.message);
                    continue;
                }
                const errMessage = e instanceof Error ? e.message : String(e);
                const message = formatRefAutoUpdateFailure(session, observerId, errMessage);
                const event: RefUpdateEvent = { kind: 'failed', observerGroupId: observerId, message };
                events.push(event);
                auth?.onProgress?.(message);
            }
        }
    }

    return events;
}
