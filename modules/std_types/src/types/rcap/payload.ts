import { json } from "@hyper-hyper-space/hhs3_json";

import { createPayloadTypeFormat } from "@hyper-hyper-space/hhs3_mvt";

import { AuthoredFields, authoredFormat, MAX_KEY_ID_LENGTH, MAX_SIGNATURE_LENGTH } from "../../authorship.js";

export const CAP_MAX_SEED_LENGTH = 1024;
export const CAP_MAX_HASH_LENGTH = 128;
export const CAP_MAX_CAP_NAME_LENGTH = 256;
export const CAP_MAX_HASH_ALGORITHM_LENGTH = 256;
export const MAX_PUBLIC_KEY_LENGTH = 8192;
export const MAX_CREATORS = 64;
export const MAX_INITIAL_CAPS = 256;
export const MAX_MANAGED_BY = 64;
export const MAX_CAP_ORIGINS = 16;

export type CapPayload = CreateRCapPayload | AddIdentityPayload | CreateCapabilityPayload
                       | DeleteCapabilityPayload | GrantPayload | RevokePayload;

export type CapDefinition = {
    managedBy: string[];
};

export const RCAP_TYPE_ID = 'hhs/cap_v1';

export type CreateRCapPayload = {
    action: 'create';
    type: string;
    seed: string;
    creators: string[];
    creatorKeys: string[];
    initialCaps: { [capName: string]: CapDefinition };
    enrollCapability?: string;
    hashAlgorithm?: string;
};

export const createRCapFormat: json.Format = {
    action: [json.Type.Constant, 'create'],
    type: createPayloadTypeFormat(RCAP_TYPE_ID),
    seed: [json.Type.BoundedString, CAP_MAX_SEED_LENGTH],
    creators: [json.Type.BoundedArray, [json.Type.BoundedString, MAX_KEY_ID_LENGTH], MAX_CREATORS],
    creatorKeys: [json.Type.BoundedArray, [json.Type.BoundedString, MAX_PUBLIC_KEY_LENGTH], MAX_CREATORS],
    initialCaps: [json.Type.BoundedMap,
        [json.Type.BoundedString, CAP_MAX_CAP_NAME_LENGTH],
        { managedBy: [json.Type.BoundedArray, [json.Type.BoundedString, CAP_MAX_CAP_NAME_LENGTH], MAX_MANAGED_BY] },
        MAX_INITIAL_CAPS,
    ],
    enrollCapability: [json.Type.Option, [json.Type.BoundedString, CAP_MAX_CAP_NAME_LENGTH]],
    hashAlgorithm: [json.Type.Option, [json.Type.BoundedString, CAP_MAX_HASH_ALGORITHM_LENGTH]],
};

export type AddIdentityPayload = AuthoredFields & {
    action: 'add-identity';
    keyId: string;
    publicKey: string;
};

export const addIdentityFormat: json.Format = {
    action: [json.Type.Constant, 'add-identity'],
    keyId: [json.Type.BoundedString, MAX_KEY_ID_LENGTH],
    publicKey: [json.Type.BoundedString, MAX_PUBLIC_KEY_LENGTH],
    author: [json.Type.BoundedString, MAX_KEY_ID_LENGTH],
    signature: [json.Type.BoundedString, MAX_SIGNATURE_LENGTH],
};

export type CreateCapabilityPayload = AuthoredFields & {
    action: 'create-cap';
    capName: string;
    managedBy: string[];
};

export const createCapabilityFormat: json.Format = {
    action: [json.Type.Constant, 'create-cap'],
    capName: [json.Type.BoundedString, CAP_MAX_CAP_NAME_LENGTH],
    managedBy: [json.Type.BoundedArray, [json.Type.BoundedString, CAP_MAX_CAP_NAME_LENGTH], MAX_MANAGED_BY],
    author: [json.Type.BoundedString, MAX_KEY_ID_LENGTH],
    signature: [json.Type.BoundedString, MAX_SIGNATURE_LENGTH],
};

export type DeleteCapabilityPayload = AuthoredFields & {
    action: 'delete-cap';
    capName: string;
};

export const deleteCapabilityFormat: json.Format = {
    action: [json.Type.Constant, 'delete-cap'],
    capName: [json.Type.BoundedString, CAP_MAX_CAP_NAME_LENGTH],
    author: [json.Type.BoundedString, MAX_KEY_ID_LENGTH],
    signature: [json.Type.BoundedString, MAX_SIGNATURE_LENGTH],
};

export type GrantPayload = AuthoredFields & {
    action: 'grant';
    grantee: string;
    capName: string;
    capOrigins: string[];
};

export const grantFormat: json.Format = {
    action: [json.Type.Constant, 'grant'],
    grantee: [json.Type.BoundedString, MAX_KEY_ID_LENGTH],
    capName: [json.Type.BoundedString, CAP_MAX_CAP_NAME_LENGTH],
    capOrigins: [json.Type.BoundedArray, [json.Type.BoundedString, CAP_MAX_HASH_LENGTH], MAX_CAP_ORIGINS],
    author: [json.Type.BoundedString, MAX_KEY_ID_LENGTH],
    signature: [json.Type.BoundedString, MAX_SIGNATURE_LENGTH],
};

export type RevokePayload = AuthoredFields & {
    action: 'revoke';
    grantee: string;
    capName: string;
};

export const revokeFormat: json.Format = {
    action: [json.Type.Constant, 'revoke'],
    grantee: [json.Type.BoundedString, MAX_KEY_ID_LENGTH],
    capName: [json.Type.BoundedString, CAP_MAX_CAP_NAME_LENGTH],
    author: [json.Type.BoundedString, MAX_KEY_ID_LENGTH],
    signature: [json.Type.BoundedString, MAX_SIGNATURE_LENGTH],
};
