import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { MetaProps, Position } from "@hyper-hyper-space/hhs3_dag";
import { json } from "@hyper-hyper-space/hhs3_json";

import type { ScopedDag } from "./dag/dag_nesting.js";
import type { Version } from "./mvt.js";

export const MAX_REF_ID_LENGTH = 256;

export type RefAdvancePayload = {
    action: 'ref-advance';
    refId: B64Hash;
    refVersion: json.Set;
};

export const refAdvanceFormat: json.Format = {
    action: [json.Type.Constant, 'ref-advance'],
    refId: [json.Type.BoundedString, MAX_REF_ID_LENGTH],
    refVersion: json.Type.Something,
};

export function isRefAdvancePayload(payload: json.Literal): payload is RefAdvancePayload {
    return typeof payload === 'object'
        && !Array.isArray(payload)
        && payload['action'] === 'ref-advance';
}

export function createRefAdvancePayload(refId: B64Hash, refVersion: Version): RefAdvancePayload {
    return {
        action: 'ref-advance',
        refId,
        refVersion: json.toSet(refVersion),
    };
}

export function extractRefVersion(payload: RefAdvancePayload): Version {
    return new Set(json.fromSet(payload.refVersion)) as Version;
}

export function refAdvanceMeta(refId: B64Hash): MetaProps {
    return { ref: json.toSet([refId]) };
}

export async function findRefAdvances(
    dag: ScopedDag,
    refId: B64Hash,
    at: Version,
): Promise<Position> {
    return dag.findCoverWithFilter(at, { containsValues: { ref: [refId] } });
}

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
