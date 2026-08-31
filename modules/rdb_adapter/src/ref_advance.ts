// Cross-group ref-advance for ingestion. When several groups of one RDb are
// co-projected into a shared target, a group A that BINDS (observes) group B
// resolves B's rows - both cross-group FK targets AND `exists` / restriction
// reads - "through the bound foreign group at the foreign version observed at
// `at`". So for A's ingested writes to validate against B's freshly-ingested
// rows, A must first OBSERVE B up to the version B reached.
//
// This module is the mechanism (pure index + a thin observe wrapper); the
// orchestrator drives WHEN to advance (a dirty map walked in commit order). We
// only ever advance bindings to CO-PROJECTED member groups: a binding to a
// non-member group projects as an opaque row_hash and is never made observable
// here.

import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import type { RTableGroup } from "@hyper-hyper-space/hhs3_rdb";

// One observer that binds a given foreign group, and the binding name it uses
// (observe() needs the binding name to advance the right ref).
export type ObserverBinding = {
    observer: RTableGroup;
    observerId: B64Hash;
    bindingName: string;
};

// A ref-advance that observe() rejected (e.g. an unauthorized writer against a
// gated binding). `foreignGroupId` is the group we failed to advance TO.
export type RefAdvanceFailure = { foreignGroupId: B64Hash; reason: string };

// Invert the members' bindings into observed -> observers, restricted to member
// (co-projected) groups. Members whose binding points outside the co-projected
// set contribute nothing (that ref stays a row_hash passthrough).
export function buildObservedBy(members: { group: RTableGroup }[]): Map<B64Hash, ObserverBinding[]> {
    const memberIds = new Set(members.map((m) => m.group.getId()));
    const index = new Map<B64Hash, ObserverBinding[]>();
    for (const m of members) {
        const observer = m.group;
        const observerId = observer.getId();
        for (const [bindingName, foreignId] of Object.entries(observer.getBindings())) {
            if (!memberIds.has(foreignId)) continue;
            const list = index.get(foreignId) ?? [];
            list.push({ observer, observerId, bindingName });
            index.set(foreignId, list);
        }
    }
    return index;
}

// The group's current DAG frontier (the version an observer must advance to).
export async function frontierOf(group: RTableGroup): Promise<Version> {
    return (await group.getScopedDag()).getFrontier();
}

// Advance one observer -> foreign binding to `version`. observe() is a
// monotonic, canObserve-gated barrier, so re-observing an already-covered
// version is a cheap no-op; a gate rejection (unauthorized author) surfaces as
// a captured failure rather than throwing through the ingest walk.
export async function observeToVersion(
    observer: RTableGroup,
    bindingName: string,
    foreignGroupId: B64Hash,
    version: Version,
    author: OwnIdentity,
): Promise<RefAdvanceFailure | undefined> {
    try {
        await observer.observe(bindingName, version, author);
        return undefined;
    } catch (e) {
        return { foreignGroupId, reason: e instanceof Error ? e.message : String(e) };
    }
}
