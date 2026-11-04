import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { refVersionAtOrAbove } from "@hyper-hyper-space/hhs3_mvt";
import type { RTableGroup } from "@hyper-hyper-space/hhs3_rdb";
import { RTableGroupImpl, gateGraph, gateGraphOrder } from "@hyper-hyper-space/hhs3_rdb";
import type { BoundStatement } from "@hyper-hyper-space/hhs3_rdb_lang";

import {
    AuthorCandidate,
    labelForKeyId,
    resolveObserveAuthor,
} from "./authz_suggest.js";
import type { AuthInteractionContext } from "./prompts.js";
import { KeyUnlockDeclinedError } from "./prompts.js";
import type { RefAutoUpdateMode, RdbSession } from "./session.js";

// What starts a check: a local statement that writes to a group (a row
// write, UPDATE REF, UPDATE CATALOG). `written` names the groups it wrote, or
// 'all' for a catalog update; `author` is the statement's author.
export type RefUpdateTrigger = {
    written: Set<B64Hash> | 'all';
    author: OwnIdentity | undefined;
};

export type ObserverRef = {
    observerId: B64Hash;
    observer: RTableGroup;
    bindingName: string;
};

export type RefUpdateOutcome = {
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

// Statement inspection at execution: the statements that write to a group
// start a check. ALTER SCHEMA does not: it changes no group until a release is
// deployed.
export function extractRefUpdateTrigger(bound: BoundStatement): RefUpdateTrigger | undefined {
    switch (bound.kind) {
        case 'insert':
        case 'update':
        case 'delete':
            return { written: new Set([bound.table.groupId]), author: bound.author };
        case 'bundle':
            return { written: new Set([bound.group.id]), author: bound.author };
        case 'update-ref':
            return { written: new Set([bound.group.id]), author: bound.author };
        case 'update-catalog':
            return { written: 'all', author: bound.author };
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
                break;
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
    mayPrompt = true,
): Promise<B64Hash> {
    const impl = observer as RTableGroupImpl;

    if (!observer.observeNeedsAuthor(foreignGroupId)) {
        return observer.observe(bindingName, targetVersion);
    }

    // the gate is checked as validation will check the observe, written at
    // the observer's current frontier
    const opPos = await (await impl.getScopedDag()).getFrontier();
    const preferred = [triggerAuthor, await session.currentAuthor()];
    const resolution = await resolveObserveAuthor(
        session,
        impl,
        foreignGroupId,
        { opPos, imported: targetVersion, fold: 'admission' },
        preferred,
        { scanKeystore: mode === 'auto' },
    );

    if (resolution.identity !== undefined) {
        return observer.observe(bindingName, targetVersion, resolution.identity, opPos);
    }

    if (resolution.locked !== undefined && mayPrompt && auth?.canPrompt?.() === true && auth.unlockIdentity !== undefined) {
        const label = resolution.locked.label;
        const authorLabel = labelForKeyId(session, resolution.locked.keyId);
        if (auth.confirmRefUpdateUnlock !== undefined) {
            await auth.confirmRefUpdateUnlock(observerDisplayName(session, observerId), authorLabel);
        }
        const identity = await auth.unlockIdentity(label);
        if (identity === undefined) throw new KeyUnlockDeclinedError(authorLabel);
        return observer.observe(bindingName, targetVersion, identity, opPos);
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

// The groups of the workspace that bind another, each after the groups it
// binds: an observe written for a bound group advances that group's frontier,
// and the groups that bind it then see the new frontier in the same check.
function groupsInBindingOrder(session: RdbSession): { id: B64Hash; group: RTableGroup }[] {
    const groups = new Map<B64Hash, RTableGroup>();
    for (const root of session.workspace.roots.list('group')) {
        if (root.object !== undefined) groups.set(root.id, root.object as RTableGroup);
    }
    const order: { id: B64Hash; group: RTableGroup }[] = [];
    const placed = new Set<B64Hash>();
    const place = (id: B64Hash): void => {
        if (placed.has(id)) return;
        placed.add(id);
        const group = groups.get(id);
        if (group === undefined) return;
        for (const bound of [...new Set(Object.values(group.getBindings()))].sort()) place(bound);
        if (Object.keys(group.getBindings()).length > 0) order.push({ id, group });
    };
    for (const id of [...groups.keys()].sort()) place(id);
    return order;
}

// Keeps every group's view of its bindings at the bound groups' frontiers,
// after a local statement that wrote to a group. Every group of the workspace
// that binds another is checked, bound groups first, and each group's
// bindings in gate-graph order, so a group a gate reads is observed before
// the binding the gate guards. A binding is behind when the observer's
// (geometric) fold at its frontier is below the bound group's frontier; it is
// then observed with an author who passes the gate. What sync brought is thus
// caught up at the next local statement; the check never runs on sync.
//
// Every observe written is reported. A refusal or failure is reported, and a
// locked author is offered for unlocking, only for a binding of a group the
// statement wrote, as before this check covered the whole workspace: the
// other bindings are caught up quietly, with unlocked authors only, so a
// binding that stays behind does not prompt after every statement.
export async function propagateRefUpdates(
    session: RdbSession,
    trigger: RefUpdateTrigger,
    auth?: AuthInteractionContext,
): Promise<RefUpdateOutcome[]> {
    const mode = session.refAutoUpdate;
    if (mode === 'off') return [];

    const reported = (boundId: B64Hash) => trigger.written === 'all' || trigger.written.has(boundId);
    const events: RefUpdateOutcome[] = [];

    for (const { id: observerId, group: observer } of groupsInBindingOrder(session)) {
        const bindings = observer.getBindings();
        const firstAlias = new Map<B64Hash, string>();
        for (const [alias, boundId] of Object.entries(bindings)) {
            if (!firstAlias.has(boundId)) firstAlias.set(boundId, alias);
        }

        for (const foreignGroupId of gateGraphOrder(gateGraph(bindings, observer.getCanObserve() ?? {}))) {
            const bindingName = firstAlias.get(foreignGroupId)!;
            let foreignGroup: RTableGroupImpl;
            try {
                foreignGroup = await loadForeignGroup(session, foreignGroupId);
            } catch {
                continue;   // the bound group is not present here: nothing to observe
            }

            try {
                const targetVersion = await (await foreignGroup.getScopedDag()).getFrontier();
                if (await isAlreadyObserved(observer, foreignGroupId, targetVersion, foreignGroup)) continue;

                const entryHash = await observeWithResolvedAuthor(
                    session,
                    observer,
                    observerId,
                    bindingName,
                    foreignGroupId,
                    targetVersion,
                    trigger.author,
                    mode,
                    auth,
                    reported(foreignGroupId),
                );
                const message = formatRefAutoUpdateNotice(session, observerId, entryHash);
                events.push({ kind: 'updated', observerGroupId: observerId, message, entryHash });
                auth?.onProgress?.(message);
            } catch (e) {
                if (!reported(foreignGroupId)) continue;
                if (e instanceof RefAutoUpdateSkippedError) {
                    events.push({ kind: 'skipped', observerGroupId: observerId, message: e.message });
                    auth?.onProgress?.(e.message);
                    continue;
                }
                const errMessage = e instanceof Error ? e.message : String(e);
                const message = formatRefAutoUpdateFailure(session, observerId, errMessage);
                events.push({ kind: 'failed', observerGroupId: observerId, message });
                auth?.onProgress?.(message);
            }
        }
    }

    return events;
}
