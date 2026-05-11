// Building-block helpers for types that implement inter-object references.
//
// An observer RObject can hold versioned references to other RObjects and
// advance those references through ref-advance operations in its own DAG.
// This module provides the canonical payload shape, metadata tagging, and
// DAG query utilities for that mechanism. These are thin, generic helpers;
// full reference resolution -- including authorization checks and barrier
// semantics -- is the responsibility of each type's View implementation.

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { MetaProps, Position } from "@hyper-hyper-space/hhs3_dag";
import { json } from "@hyper-hyper-space/hhs3_json";

import type { ScopedDag } from "./dag/dag_nesting.js";
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

// Metadata props to attach when appending a ref-advance entry to the DAG.
// Tags the entry with the referenced object's ID so it can be found by
// indexed queries (findRefAdvances, findConcurrentRefAdvanceBarriers).
export function refAdvanceMeta(refId: B64Hash): MetaProps {
    return { ref: json.toSet([refId]) };
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
