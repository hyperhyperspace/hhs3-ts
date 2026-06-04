// Building-block helpers for types that implement inter-object references.
//
// An observer RObject can hold versioned references to other RObjects and
// advance those references through ref-advance operations in its own DAG.
// This module provides the canonical payload shape, metadata tagging, DAG query
// utilities, and monotonicity validation for that mechanism. These are thin,
// generic helpers; types still own authorization checks, barrier semantics, and
// view-time reference resolution.

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { EntryMetaFilter, MetaProps, Position } from "@hyper-hyper-space/hhs3_dag";
import { position } from "@hyper-hyper-space/hhs3_dag";
import { json } from "@hyper-hyper-space/hhs3_json";

import type { ScopedDag, CausalDag } from "./dag/dag_nesting.js";
import { version } from "./mvt.js";
import type { Version } from "./mvt.js";

export const MAX_REF_ID_LENGTH = 256;

// Canonical payload shape for a ref-advance operation. Types may embed
// additional type-specific fields alongside these; use refAdvanceFormat
// with strict=false to allow extensible payloads.
export type RefAdvancePayload = {
    action: 'ref-advance';
    refId: B64Hash;
    refVersion: json.Set;
};

// json.Format for validating the ref-advance portion of a payload.
// Designed for non-strict checking so types can add extra fields.
export const refAdvanceFormat: json.Format = {
    action: [json.Type.Constant, 'ref-advance'],
    refId: [json.Type.BoundedString, MAX_REF_ID_LENGTH],
    refVersion: json.Type.Something,
};

// Type guard: returns true if the payload is a ref-advance operation.
export function isRefAdvancePayload(payload: json.Literal): payload is RefAdvancePayload {
    return typeof payload === 'object'
        && !Array.isArray(payload)
        && payload['action'] === 'ref-advance';
}

// Construct a ref-advance payload for the given reference and target version.
export function createRefAdvancePayload(refId: B64Hash, refVersion: Version): RefAdvancePayload {
    return {
        action: 'ref-advance',
        refId,
        refVersion: json.toSet(refVersion),
    };
}

// Extract the target version from a ref-advance payload.
export function extractRefVersion(payload: RefAdvancePayload): Version {
    return new Set(json.fromSet(payload.refVersion)) as Version;
}

export type RefAdvanceMetaOptions = {
    barrier?: boolean;
};

// Metadata for appending a ref-advance entry. Tags `ref` for indexed queries and,
// by default, `barrier` for BFT revision. Pass `{ barrier: false }` for a non-barrier ref-advance.
export function createRefAdvanceMeta(refId: B64Hash, opts?: RefAdvanceMetaOptions): MetaProps {
    const meta: MetaProps = { ref: json.toSet([refId]) };
    if (opts?.barrier ?? true) {
        meta.barrier = json.toSet(['t']);
    }
    return meta;
}

export type RefAdvanceAppend = { payload: RefAdvancePayload; meta: MetaProps };

// Payload and append meta for a barrier ref-advance.
export function prepareRefAdvance(refId: B64Hash, refVersion: Version): RefAdvanceAppend {
    return {
        payload: createRefAdvancePayload(refId, refVersion),
        meta: createRefAdvanceMeta(refId),
    };
}

// Find all ref-advance entries for a given refId up to a DAG position.
export async function findRefAdvances(
    dag: ScopedDag,
    refId: B64Hash,
    at: Version,
): Promise<Position> {
    return dag.findCoverWithFilter(at, { containsValues: { ref: [refId] } });
}

// Find ref-advance barrier entries for a given refId that are concurrent
// to `at` when observed from `from`. Used for (at, from) revision semantics:
// barriers in this set may retroactively affect how concurrent operations
// are interpreted.
export async function findConcurrentRefAdvanceBarriers(
    dag: ScopedDag,
    refId: B64Hash,
    at: Version,
    from: Version,
): Promise<Position> {
    return dag.findConcurrentCoverWithFilter(
        from,
        at,
        { containsValues: { ref: [refId], barrier: ['t'] } },
    );
}

// Resolve referenced version(s) at `at` in the observer DAG, optionally widened
// by ref-advance barriers concurrent to `at` when observed from `from`.
// Observers pass the result as the referenced object's `at`; the referenced
// object's `from` is typically a separate resolution at the view frontier
// (call with from === at to get ref-advances in the frontier's history only,
// without concurrent barrier widening).
//
// When from === at, only ref-advances in the history of `at` contribute.
// When from !== at, concurrent ref-advance barriers widen the result,
// implementing the BFT revision mechanism in the observer DAG.
export async function resolveRefVersionAtPosition(
    dag: ScopedDag,
    refId: B64Hash,
    at: Version,
    from: Version,
): Promise<Version> {
    const causal = await findRefAdvances(dag, refId, at);
    const concurrent = await findConcurrentRefAdvanceBarriers(dag, refId, at, from);

    const result = version();

    for (const hash of causal) {
        const entry = await dag.loadEntry(hash);
        if (entry === undefined) continue;
        if (isRefAdvancePayload(entry.payload)) {
            for (const h of extractRefVersion(entry.payload as RefAdvancePayload)) result.add(h);
        }
    }

    for (const hash of concurrent) {
        if (causal.has(hash)) continue;
        const entry = await dag.loadEntry(hash);
        if (entry === undefined) continue;
        if (isRefAdvancePayload(entry.payload)) {
            for (const h of extractRefVersion(entry.payload as RefAdvancePayload)) result.add(h);
        }
    }

    return result.size > 0 ? result : version(refId);
}

// Resolve referenced-object versions for checking an observer entry against a foreign
// object. `entryHash` and `observerFrom` are positions in the observer DAG; the returned
// `refAt` / `refFrom` are versions in the referenced object's DAG (suitable as args to
// `referenced.getView(refAt, refFrom)`).
export async function resolveRefVersions(
    observerDag: ScopedDag,
    refId: B64Hash,
    entryHash: B64Hash,
    observerFrom: Version,
): Promise<{ refAt: Version; refFrom: Version }> {
    const refAt = await resolveRefVersionAtPosition(observerDag, refId, version(entryHash), observerFrom);
    const refFrom = await resolveRefVersionAtPosition(observerDag, refId, observerFrom, observerFrom);
    return { refAt, refFrom };
}

// Returns true iff `newer` is at or above `older` in the referenced object's DAG
// (older is in newer's causal past, or they denote the same position). Closed on both
// ends: equal positions pass. This is not negated by refVersionAtOrBelow; concurrent
// positions fail both comparisons.
export async function refVersionAtOrAbove(
    referencedDag: CausalDag,
    newer: Version,
    older: Version,
): Promise<boolean> {
    const fork = await referencedDag.findForkPosition(older, newer);
    return fork.forkA.size === 0;
}

// Converse of refVersionAtOrAbove: true iff `v` is at or below `ceiling` in the
// referenced object's DAG. Use for ref-advance stability (e.g. projectForeignBound);
// do not implement as !refVersionAtOrAbove(...).
export async function refVersionAtOrBelow(
    referencedDag: CausalDag,
    v: Version,
    ceiling: Version,
): Promise<boolean> {
    return refVersionAtOrAbove(referencedDag, ceiling, v);
}

// Project a foreign revision bound into the observer DAG: find the earliest unstable
// ref-advance(s) at or below `localAt`. A ref-advance is stable iff its referenced
// version is at or below `foreignRevisionBound`; unstable otherwise.
//
// Starting from the ref-advance cover at `localAt`, descend through unstable ref-advances
// via their preds; a branch settles when no unstable ref-advance sits below it. The
// referenced object's create op is an implicit stable ref-advance to version(refId)
// (always at or below `foreignRevisionBound`), so an empty below-cover settles the
// branch. Below the returned floor the referenced version is bounded by
// `foreignRevisionBound`. If no ref-advance is unstable, return `localAt`.
//
// Assumes monotonic ref-advances: a stable ref-advance has only stable ref-advances
// below it, so the descent can stop at the first stable ref-advance on each branch.
export async function projectForeignBound(
    observerDag: ScopedDag,
    refId: B64Hash,
    referencedDag: CausalDag,
    localAt: Version,
    foreignRevisionBound: Version,
): Promise<Version> {
    const refFilter: EntryMetaFilter = { containsValues: { ref: [refId] } };

    const floor = version();
    const visited = new Set<B64Hash>();
    const stabilityCache = new Map<B64Hash, boolean>();

    const isStable = async (hash: B64Hash): Promise<boolean> => {
        const cached = stabilityCache.get(hash);
        if (cached !== undefined) return cached;
        const entry = await observerDag.loadEntry(hash);
        const refVersion = extractRefVersion(entry!.payload as RefAdvancePayload);
        const result = await refVersionAtOrBelow(referencedDag, refVersion, foreignRevisionBound);
        stabilityCache.set(hash, result);
        return result;
    };

    const queue: B64Hash[] = [...(await observerDag.findCoverWithFilter(localAt, refFilter))];
    while (queue.length > 0) {
        const r = queue.shift()!;
        if (visited.has(r)) continue;
        visited.add(r);
        if (await isStable(r)) continue;

        const entry = await observerDag.loadEntry(r);
        const preds = position(...json.fromSet(entry!.header.prevEntryHashes));
        const below = await observerDag.findCoverWithFilter(preds, refFilter);

        const unstableBelow: B64Hash[] = [];
        for (const b of below) {
            if (!(await isStable(b))) unstableBelow.push(b);
        }

        if (unstableBelow.length === 0) {
            floor.add(r);
        } else {
            queue.push(...unstableBelow);
        }
    }

    return floor.size > 0 ? floor : localAt;
}

// Validates that a proposed ref-advance is monotonic at insertion time. For each
// predecessor in `at`, resolves the current reference in the observer DAG (causal
// only, from === at) and requires `newRefVersion` to be at or above that version
// in the referenced object's DAG. On merge, every branch predecessor must pass.
export async function validateRefAdvanceMonotonicity(
    observerDag: ScopedDag,
    referencedDag: CausalDag,
    refId: B64Hash,
    newRefVersion: Version,
    at: Version,
): Promise<boolean> {
    if (at.size === 0) {
        const current = await resolveRefVersionAtPosition(observerDag, refId, at, at);
        return refVersionAtOrAbove(referencedDag, newRefVersion, current);
    }

    for (const pred of at) {
        const current = await resolveRefVersionAtPosition(
            observerDag, refId, version(pred), version(pred),
        );
        if (!await refVersionAtOrAbove(referencedDag, newRefVersion, current)) {
            return false;
        }
    }

    return true;
}
