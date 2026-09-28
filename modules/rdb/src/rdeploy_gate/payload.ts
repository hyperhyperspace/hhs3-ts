// Payloads for RDeployGate operations, and their format validators.
//
// An RDeployGate is a replica-local, never-synced RObject, one per table
// group. It records which schema entries this replica has adopted for the
// group: its DAG mirrors the adopted part of the group's schema DAG, one
// `admit` entry per schema entry, with the mirrors of the schema entry's
// predecessors as its own predecessors. A synced schema deploy names the
// mirrors of its target version, so it waits (through the synchronizer's
// dependency check) until this replica admits that version.
//
// The genesis names the group and its schema, so the gate id derives from
// them alone (see mirror.ts) and every replica computes the same one.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { createPayloadTypeFormat } from "@hyper-hyper-space/hhs3_mvt";

import { MAX_HASH_LENGTH } from "../rschema/payload.js";

export const RDEPLOY_GATE_TYPE_ID = 'hhs/rdeploy_gate_v1';

// The metadata key tagging each admit entry with the schema entry it mirrors.
export const MIRROR_SOURCE_META = 'src';

export type CreateDeployGatePayload = {
    action: 'create';
    type: string;
    group: B64Hash;
    schema: B64Hash;
};

export const createDeployGateFormat: json.Format = {
    action: [json.Type.Constant, 'create'],
    type: createPayloadTypeFormat(RDEPLOY_GATE_TYPE_ID),
    group: [json.Type.BoundedString, MAX_HASH_LENGTH],
    schema: [json.Type.BoundedString, MAX_HASH_LENGTH],
};

export type AdmitPayload = {
    action: 'admit';
    entry: B64Hash;
};

export const admitFormat: json.Format = {
    action: [json.Type.Constant, 'admit'],
    entry: [json.Type.BoundedString, MAX_HASH_LENGTH],
};
