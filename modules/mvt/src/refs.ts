// Building-block helpers for types that implement inter-object references.
//
// An observer RObject can hold versioned references to other RObjects and
// advance those references through ref-advance operations in its own DAG.
// This module provides the canonical payload shape, metadata tagging, DAG query
// utilities, and monotonicity validation for that mechanism. These are thin,
// generic helpers; types still own authorization checks, barrier semantics, and
// view-time reference resolution.

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { EntryPredicate, MetaProps, Position } from "@hyper-hyper-space/hhs3_dag";
import { position } from "@hyper-hyper-space/hhs3_dag";
import { json } from "@hyper-hyper-space/hhs3_json";

import type { ScopedDag, CausalDag } from "./dag/dag_nesting.js";
import { version } from "./mvt.js";
import type { Version } from "./mvt.js";
import { validationFailure, validationOk } from "./validation.js";
import type { ValidationResult } from "./validation.js";

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

// Find the latest ref-advance entries for a given refId up to a DAG position.
// `predicate`, when supplied, makes the cover see-through: an entry it rejects
// is skipped and the walk continues below it.
export async function findRefAdvances(
    dag: ScopedDag,
    refId: B64Hash,
    at: Version,
    predicate?: EntryPredicate,
): Promise<Position> {
    return dag.findCoverWithFilter(at, { containsValues: { ref: [refId] } }, predicate);
}

// Find ref-advance barrier entries for a given refId that are concurrent
// to `at` when observed from `from`. Used for (at, from) revision semantics:
// barriers in this set may retroactively affect how concurrent operations
// are interpreted. `predicate` is see-through, as in findRefAdvances.
export async function findConcurrentRefAdvanceBarriers(
    dag: ScopedDag,
    refId: B64Hash,
    at: Version,
    from: Version,
    predicate?: EntryPredicate,
): Promise<Position> {
    return dag.findConcurrentCoverWithFilter(
        from,
        at,
        { containsValues: { ref: [refId], barrier: ['t'] } },
        predicate,
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
//
// `isLive`, when supplied, is the see-through predicate of BOTH folds: an entry
// for which it returns false contributes no version and does not hide the
// ref-advances below it, so a voided ref-advance (e.g. an observation rejected
// at-use by its authorization gate) falls through to the latest live one, like
// a voided write in a column read. Omitting `isLive` is the geometric
// resolution (every ref-advance counts) and is the default everywhere a gate
// does not apply.
export async function resolveRefVersionAtPosition(
    dag: ScopedDag,
    refId: B64Hash,
    at: Version,
    from: Version,
    isLive?: (entryHash: B64Hash) => Promise<boolean>,
): Promise<Version> {
    const predicate: EntryPredicate | undefined = isLive === undefined ? undefined : (hash) => isLive(hash);
    const causal = await findRefAdvances(dag, refId, at, predicate);
    // Nothing in from's past is concurrent to at when from == at, and the
    // concurrent search would walk (and load) the whole causal past to say so.
    const concurrent = sameSet(at, from)
        ? position()
        : await findConcurrentRefAdvanceBarriers(dag, refId, at, from, predicate);

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

function sameSet(a: Version, b: Version): boolean {
    if (a === b) return true;
    if (a.size !== b.size) return false;
    for (const h of a) if (!b.has(h)) return false;
    return true;
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
// referenced object's DAG. Use for ref-advance stability (e.g. walkRevisionBound);
// do not implement as !refVersionAtOrAbove(...).
export async function refVersionAtOrBelow(
    referencedDag: CausalDag,
    v: Version,
    ceiling: Version,
): Promise<boolean> {
    return refVersionAtOrAbove(referencedDag, ceiling, v);
}

// The minimal cover of `v` in `dag`. A single head is already canonical.
export async function canonicalVersion(dag: CausalDag, v: Version): Promise<Version> {
    if (v.size <= 1) return v;
    return dag.findMinimalCover(v);
}

// The cut a ref-advance's own gate reads at, observed from `from` (Layer 1),
// canonical in the referenced DAG. The base is the version published by the
// ref-advance at `opPos`, folded with the ref-advances in its causal past.
// A concurrent ref-advance barrier widens it only when its published version
// strictly dominates the base in the referenced DAG and `isLive` says it is
// live. Strict domination makes a recursive `isLive` ascend the referenced
// version, so it terminates, and ref-advances that publish incomparable
// versions never consult each other. A revoke of the author rides a strictly
// dominating version, so restricting to those loses no widening that matters
// for security.
//
// The search sees through a void candidate, so it does not hide a live one
// below it. A candidate that is not strictly above stops the search there and
// is dropped: write-time monotonicity puts every ref-advance below it at or
// below its version, so none of them is strictly above the base either.
// `isLive` is asked only of candidates strictly above the base. It is not a
// reader's see-through predicate (resolveRefVersionAtPosition's `isLive`),
// which is asked of every ref-advance in a fold: passing that one to the
// concurrent search would ask liveness of incomparable ref-advances.
export async function widenRefAtUpward(
    observerDag: ScopedDag,
    refId: B64Hash,
    opPos: Version,
    from: Version,
    referencedDag: CausalDag,
    isLive: (entryHash: B64Hash) => Promise<boolean>,
): Promise<Version> {
    // Uncanonicalized: the dominance tests read the causal past, which is the same.
    const base = await resolveRefVersionAtPosition(observerDag, refId, opPos, opPos);
    const refAt = version(...base);

    // A candidate's published version, when it is strictly above the base.
    const strictlyAbove = new Map<B64Hash, Version | undefined>();
    const widening = async (hash: B64Hash, payload: json.Literal): Promise<Version | undefined> => {
        if (!strictlyAbove.has(hash)) {
            let widened: Version | undefined;
            if (isRefAdvancePayload(payload)) {
                const candidate = extractRefVersion(payload as RefAdvancePayload);
                if (await refVersionAtOrAbove(referencedDag, candidate, base)
                    && !await refVersionAtOrAbove(referencedDag, base, candidate)) {
                    widened = candidate;
                }
            }
            strictlyAbove.set(hash, widened);
        }
        return strictlyAbove.get(hash);
    };

    const concurrent = await findConcurrentRefAdvanceBarriers(observerDag, refId, opPos, from,
        async (hash, entry) => (await widening(hash, entry.payload)) === undefined || await isLive(hash));
    for (const hash of concurrent) {
        const entry = await observerDag.loadEntry(hash);
        if (entry === undefined) continue;
        const widened = await widening(hash, entry.payload);
        if (widened === undefined) continue;
        for (const h of widened) refAt.add(h);
    }
    return canonicalVersion(referencedDag, refAt);
}

// A ref-advance version must be a non-empty antichain. Missing entries are an
// infrastructure error (the caller throws), not a validation failure.
export async function validateRefVersionShape(
    referencedDag: CausalDag,
    refVersion: Version,
): Promise<ValidationResult> {
    if (refVersion.size === 0) return validationFailure("ref-advance version is empty");
    const cover = await referencedDag.findMinimalCover(refVersion);
    if (cover.size !== refVersion.size) return validationFailure("ref-advance version is not an antichain");
    return validationOk();
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
