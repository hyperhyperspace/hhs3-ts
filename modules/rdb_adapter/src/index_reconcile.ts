// Install a projection index spec. Never triggered by sync - the app calls it
// when it ships a spec. The same fingerprint as the installed spec is a no-op.
// Anything else is diffed (resolved at each member's current checkpoint)
// against the indexes the target actually materialized, applied, and stored,
// atomically. From then on every apply() maintains that spec across schema
// deltas. An identical resolved index is left in place, so re-applying a spec
// does not rebuild indexes that already match.

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";

import {
    CheckpointMovedError, IndexSpec, IndexTarget, isIndexTarget, MaterializationTarget, SchemaAction,
} from "./types.js";
import {
    groupIndexDecls, indexSpecFingerprint, PendingIndex, planIndexActions, resolveIndexes, validateIndexSpec,
} from "./index_actions.js";
import { GroupProjection, withDatabaseLock } from "./ingest_orchestrator.js";

export type IndexReconcileStatus = 'installed' | 'unchanged' | 'dry-run';

export type IndexReconcileReport = {
    status: IndexReconcileStatus;
    // The index actions applied (or, for a dry run, that would be applied).
    actions: SchemaAction[];
    // Declarations that cannot be built yet: a missing group, table, or column.
    // Legitimate while waiting for a schema change; otherwise usually a typo.
    pending: PendingIndex[];
};

export type ReconcileIndexesOptions = { dryRun?: boolean };

const MAX_ATTEMPTS = 3;

export async function reconcileIndexes(
    members: GroupProjection[], target: MaterializationTarget, spec: IndexSpec,
    opts: ReconcileIndexesOptions = {},
): Promise<IndexReconcileReport> {
    if (!isIndexTarget(target)) throw new Error('projection target does not support indexes');
    const invalid = validateIndexSpec(spec);
    if (invalid !== undefined) throw new Error(invalid);
    for (const decl of spec.indexes) {
        const reason = target.validateIndexOptions(decl);
        if (reason !== undefined) throw new Error(`index '${decl.group}.${decl.name}': ${reason}`);
    }
    const fingerprint = indexSpecFingerprint(spec);

    return withDatabaseLock(members, async () => {
        for (let attempt = 1; ; attempt++) {
            try {
                return await reconcileOnce(members, target, spec, fingerprint, opts.dryRun === true);
            } catch (e) {
                if (e instanceof CheckpointMovedError && attempt < MAX_ATTEMPTS) continue;
                throw e;
            }
        }
    });
}

async function reconcileOnce(
    members: GroupProjection[], target: MaterializationTarget & IndexTarget,
    spec: IndexSpec, fingerprint: string, dryRun: boolean,
): Promise<IndexReconcileReport> {
    const state = await target.getIndexState();
    if (state.spec !== undefined && fingerprint === state.specFingerprint) {
        return { status: 'unchanged', actions: [], pending: [] };
    }

    const drops: SchemaAction[] = [];
    const ensures: SchemaAction[] = [];
    const pending: PendingIndex[] = [];
    const checkpoints = new Map<B64Hash, Version>();
    const projected = new Set<B64Hash>();
    const memberNames = new Set<string>();

    for (const m of members) {
        const groupId = m.group.getId();
        memberNames.add(m.group.getName());
        const cp = await target.getCheckpoint(groupId);
        // Not materialized yet: its initial projection builds the installed spec.
        if (cp === undefined) continue;
        checkpoints.set(groupId, cp);
        projected.add(groupId);

        const view = (await m.group.getView(cp, cp)).getSchemaView();
        const decls = groupIndexDecls(spec, m.group.getName(), view);
        const resolution = resolveIndexes(decls, groupId, view, m.config);
        pending.push(...resolution.pending);
        const plan = planIndexActions(
            resolution.resolved, state.materialized.filter((r) => r.groupId === groupId), []);
        drops.push(...plan.drops);
        ensures.push(...plan.ensures);
    }

    // Records left behind by a group that is no longer projected here.
    for (const r of state.materialized) {
        if (!projected.has(r.groupId)) drops.push({ kind: 'drop-index', table: r.table, name: r.name });
    }

    // A declared group may not be a member yet, or may not have arrived.
    for (const decl of spec.indexes) {
        if (!memberNames.has(decl.group)) {
            pending.push({ name: decl.name, group: decl.group, table: decl.table, missing: [`group '${decl.group}'`] });
        }
    }

    const actions = [...drops, ...ensures];
    if (dryRun) return { status: 'dry-run', actions, pending };

    await target.installIndexSpec(spec, fingerprint, actions, {
        specFingerprint: state.specFingerprint, checkpoints,
    });
    return { status: 'installed', actions, pending };
}
