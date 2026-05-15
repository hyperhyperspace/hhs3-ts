// Actual payload for RSet operations, and their format validators.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";

import { MAX_TYPE_LENGTH } from "@hyper-hyper-space/hhs3_mvt";
import { MAX_KEY_ID_LENGTH, MAX_SIGNATURE_LENGTH } from "../../authorship.js";

export const MAX_SEED_LENGTH = 1024;
export const MAX_HASH_LENGTH = 128;
export const MAX_ELEMENTS_TYPE_ID_LENGTH = 256;
export const MAX_INITIAL_ELEMENTS = 1024;
export const MAX_HASH_ALGORITHM_LENGTH = 256;
export const MAX_CAP_NAME = 256;
export const MAX_REF_ADVANCE_CAPS = 64;

export type SetPayload = CreateSetPayload | AddElmtPayload | DeleteElmtPayload | UpdateElmtPayload;

// Create a set:

// Note: Sets of RObjects (when contentType !== undefined) cannot have initial elements.
//       In that case, initialElements MUST be an empty array.

export type CapRequirements = {
    add?: string;
    delete?: string;
    refAdvance?: string[];
    refAdvanceCreators?: boolean;
};

export const capRequirementsFormat: json.Format = {
    add:                [json.Type.Option, [json.Type.BoundedString, MAX_CAP_NAME]],
    delete:             [json.Type.Option, [json.Type.BoundedString, MAX_CAP_NAME]],
    refAdvance:         [json.Type.Option, [json.Type.BoundedArray, [json.Type.BoundedString, MAX_CAP_NAME], MAX_REF_ADVANCE_CAPS]],
    refAdvanceCreators: [json.Type.Option, json.Type.Boolean],
};

export type CreateSetPayload = {
    action: 'create';
    seed: string;
    contentType?: string;
    initialElements: Array<json.Literal>;
    acceptRedundantAdd?: boolean;
    acceptRedundantDelete: boolean;
    acceptUpdateForDeleted?: boolean;
    supportBarrierAdd?: boolean;
    supportBarrierDelete?: boolean;
    hashAlgorithm?: string;
    parent?: B64Hash;
    capabilityRef?: B64Hash;
    capRequirements?: CapRequirements;
}
 
export const createSetFormat: json.Format = {
    action: [json.Type.Constant, 'create'],
    seed: [json.Type.BoundedString, MAX_SEED_LENGTH],
    contentType: [json.Type.Option, [json.Type.BoundedString, MAX_TYPE_LENGTH]],
    initialElements: [json.Type.BoundedArray, json.Type.String, MAX_INITIAL_ELEMENTS],
    acceptRedundantAdd: [json.Type.Option, json.Type.Boolean],
    acceptRedundantDelete: json.Type.Boolean,
    acceptUpdateForDeleted: [json.Type.Option, json.Type.Boolean],
    supportBarrierAdd: [json.Type.Option, json.Type.Boolean],
    supportBarrierDelete: [json.Type.Option, json.Type.Boolean],
    hashAlgorithm: [json.Type.Option, [json.Type.BoundedString, MAX_HASH_ALGORITHM_LENGTH]],
    parent: [json.Type.Option, [json.Type.BoundedString, MAX_HASH_LENGTH]],
    capabilityRef: [json.Type.Option, [json.Type.BoundedString, MAX_HASH_LENGTH]],
    capRequirements: [json.Type.Option, capRequirementsFormat],
};

// Add an element:

export type AddElmtPayload = {
    action: 'add';
    element: json.Literal;
    barrier?: boolean;
    type?: string;
};

export const addElmtFormat: json.Format = {
    action: [json.Type.Constant, 'add'],
    element: json.Type.Something,
    barrier: [json.Type.Option, json.Type.Boolean],
};

// Delete an element:

export type DeleteElmtPayload = {
    action: 'delete';
    elementHash: B64Hash;
    barrier?: boolean;
};

export const deleteElmtFormat: json.Format = {
    action: [json.Type.Constant, 'delete'],
    elementHash: [json.Type.BoundedString, MAX_HASH_LENGTH],
    barrier: [json.Type.Option, json.Type.Boolean],
};

// Update an element (*):

// (*) Used by the DAG wrapper automatically when the contained element is updated

export type UpdateElmtPayload = {
    action: 'update';
    elementHash: B64Hash;
    updatePayload: json.Literal;
}

export const updateElmtFormat: json.Format = {
    action: [json.Type.Constant, 'update'],
    elementHash: [json.Type.BoundedString, MAX_HASH_LENGTH],
    updatePayload: json.Type.Something,
};

// Authored variants for permissioned sets:

export const addElmtAuthoredFormat: json.Format = {
    action: [json.Type.Constant, 'add'],
    element: json.Type.Something,
    barrier: [json.Type.Option, json.Type.Boolean],
    author: [json.Type.BoundedString, MAX_KEY_ID_LENGTH],
    signature: [json.Type.BoundedString, MAX_SIGNATURE_LENGTH],
};

export const deleteElmtAuthoredFormat: json.Format = {
    action: [json.Type.Constant, 'delete'],
    elementHash: [json.Type.BoundedString, MAX_HASH_LENGTH],
    barrier: [json.Type.Option, json.Type.Boolean],
    author: [json.Type.BoundedString, MAX_KEY_ID_LENGTH],
    signature: [json.Type.BoundedString, MAX_SIGNATURE_LENGTH],
};