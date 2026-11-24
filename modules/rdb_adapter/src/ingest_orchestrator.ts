// Inbound orchestrator: the side-effecting counterpart of projectGroup. It
// ingests captured changes into rdb, one entry per change at each group's
// checkpoint, recording in each outbox row what became of the change before
// anything reaches rdb, and settles the pass in one target transaction that
// moves the checkpoints (HIP-29; see ingest_chain.ts).
//
//   await ingestChanges(group, backend, { writer });   // local edits -> rdb
//   await syncGroup(group, backend, { writer });        // ingest THEN project
//
// A refused change is recorded in the durable op-event log, the same channel
// p2p concurrency void/reinstate flips flow through (projectGroup), and its
// row is reverted to rdb's, with the changes still pending on top.

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { RTableGroup } from "@hyper-hyper-space/hhs3_rdb";

import {
    AdapterConfig, BidirectionalTarget, IngestResult, MaterializationTarget, PendingChangesError,
} from "./types.js";
import { ingestOnce, mergeResults } from "./ingest_chain.js";
import { projectGroup } from "./project.js";

export type { IngestPassOutcome } from "./ingest_chain.js";

function defaultNewUuid(): string {
    const c = (globalThis as { crypto?: { randomUUID?: () => string } }).crypto;
    if (c?.randomUUID === undefined) {
        throw new Error("no crypto.randomUUID() available; provide one via config or a newer runtime");
    }
    return c.randomUUID();
}

// One lock per target object (HIP-29): a sync cycle, a single-group cycle and
// an index reconcile on one target never overlap in this process, whatever the
// members are. What a cycle reads and then writes belongs to the target: the
// outbox, the checkpoints and the index spec. Calls are chained per target.
const locks = new WeakMap<object, Promise<unknown>>();

export async function withTargetLock<T>(target: object, fn: () => Promise<T>): Promise<T> {
    const prev = locks.get(target) ?? Promise.resolve();
    const run = prev.then(fn, fn);
    const guard = run.catch(() => undefined);
    locks.set(target, guard);
    try {
        return await run;
    } finally {
        if (locks.get(target) === guard) locks.delete(target);
    }
}

// Ingest local changes as rdb ops, and settle them. Single-group convenience
// wrapper over the replica-wide path (one member). The target is one that can
// be projected to: ingestion reads its checkpoints.
export async function ingestChanges(
    group: RTableGroup,
    source: BidirectionalTarget,
    config: AdapterConfig = {},
    newUuid: () => string = defaultNewUuid,
): Promise<IngestResult> {
    if (config.writer === undefined && group.getIdProvider() !== undefined) {
        throw new Error("ingestChanges requires config.writer for a group with an identity provider");
    }
    const results = await ingestDatabaseChanges([{ group, config }], source, newUuid);
    return results.get(group.getId()) ?? { accepted: 0, rejected: [] };
}

// The coordinated sync cycle for one group: ingest local edits into rdb, THEN
// project rdb back into the target, under the target's lock. A projection
// refused by a change captured meanwhile (PendingChangesError) ends the cycle
// without projecting: the next cycle ingests the change first.
export async function syncGroup(
    group: RTableGroup,
    backend: BidirectionalTarget,
    config: AdapterConfig = {},
    newUuid: () => string = defaultNewUuid,
): Promise<IngestResult> {
    const outcome = await syncDatabase([{ group, config }], backend, newUuid);
    return outcome.results.get(group.getId()) ?? { accepted: 0, rejected: [] };
}

// ---------------------------------------------------------------------------
// Replica-wide (multi-group) orchestration
// ---------------------------------------------------------------------------

// One group of a replica-wide projection: the group plus its resolved config.
export type GroupProjection = {
    group: RTableGroup;
    config: AdapterConfig;
};

// Topologically order members so a group is projected AFTER every co-projected
// group it references (via bindings).
function orderByBindings(members: GroupProjection[]): GroupProjection[] {
    const byId = new Map<B64Hash, GroupProjection>();
    for (const m of members) byId.set(m.group.getId(), m);

    const ordered: GroupProjection[] = [];
    const done = new Set<B64Hash>();
    const active = new Set<B64Hash>();

    const visit = (m: GroupProjection): void => {
        const id = m.group.getId();
        if (done.has(id) || active.has(id)) return;
        active.add(id);
        for (const foreignId of Object.values(m.group.getBindings())) {
            const dep = byId.get(foreignId);
            if (dep !== undefined) visit(dep);
        }
        active.delete(id);
        done.add(id);
        ordered.push(m);
    };

    for (const m of members) visit(m);
    return ordered;
}

// Project every member group into the shared target, referenced groups first.
export async function projectDatabase(
    members: GroupProjection[],
    target: MaterializationTarget,
): Promise<void> {
    for (const m of orderByBindings(members)) {
        await projectGroup(m.group, target, m.config);
    }
}

// One ingestion over the shared outbox, in commit order, across every member
// (ingest_chain.ts): the end of a crashed pass first, then a pass.
export async function ingestDatabaseChanges(
    members: GroupProjection[],
    source: BidirectionalTarget,
    newUuid: () => string = defaultNewUuid,
): Promise<Map<B64Hash, IngestResult>> {
    return (await ingestOnce(members, source, newUuid)).results;
}

// What a sync cycle did. `results`: each member's ingestion. `refused`: the
// projection pass didn't apply, because a change was captured after ingestion
// (PendingChangesError); the next cycle ingests it first. `stoppedAt`: the
// groups with an identity provider and no writer key whose pending change
// stopped ingestion; no projection applies while it is pending, and no cycle
// is due because of it (HIP-29).
export type SyncOutcome = { results: Map<B64Hash, IngestResult>; refused: boolean; stoppedAt: B64Hash[] };

// The replica-wide coordinated cycle, under the target's lock: ingest all
// members' local edits, once more if the outbox filled up meanwhile, THEN
// project every member back into the shared target. The projection never
// applies over a pending change: a change captured after the second ingestion
// makes it refuse, and the cycle ends.
export async function syncDatabase(
    members: GroupProjection[],
    backend: BidirectionalTarget,
    newUuid: () => string = defaultNewUuid,
): Promise<SyncOutcome> {
    return withTargetLock(backend, async () => {
        const first = await ingestOnce(members, backend, newUuid);
        const results = first.results;
        let stoppedAt = first.stoppedAt;
        if (stoppedAt === undefined && await backend.hasPendingChanges()) {
            const second = await ingestOnce(members, backend, newUuid);
            mergeResults(results, second.results);
            stoppedAt = second.stoppedAt;
        }
        const stopped = stoppedAt === undefined ? [] : [stoppedAt];

        try {
            await projectDatabase(members, backend);
        } catch (e) {
            if (e instanceof PendingChangesError) return { results, refused: true, stoppedAt: stopped };
            throw e;
        }
        return { results, refused: false, stoppedAt: stopped };
    });
}
