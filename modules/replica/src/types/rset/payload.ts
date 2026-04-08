// Actual payload for RSet operations, and their format validators.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";

import { MAX_TYPE_LENGTH } from "../../replica.js";

export const MAX_SEED_LENGTH = 1024;
export const MAX_HASH_LENGTH = 128;
export const MAX_ELEMENTS_TYPE_ID_LENGTH = 256;
export const MAX_INITIAL_ELEMENTS = 1024;
export const MAX_HASH_ALGORITHM_LENGTH = 256;

export type SetPayload = CreateSetPayload | AddElmtPayload | DeleteElmtPayload | UpdateElmtPayload;

// Create a set:

// Note: Sets of RObjects (when contentType !== undefined) cannot have initial elements.
//       In that case, initialElements MUST be an empty array.

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