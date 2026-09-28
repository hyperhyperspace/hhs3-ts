// Payloads for RDb operations, and their format validators.
//
// An RDb is the sync root and orchestrator for a database deployed from a
// catalog: its DAG records which catalog releases the admin deployed and with
// which params, and its member groups are computed from those releases by the
// normative instantiation in instantiate.ts. Its runtime role is to keep the
// catalog, the member groups, their schemas and bound groups present and
// syncing, and to adopt deployed releases into the members' local gates.
//
// RDb state never decides any group's validity: groups are fully valid and
// verifiable without their RDb, and never depend on it.
//
//   create          seed, optional name and creators, the catalog, the
//                   deployed genesis release and its params. Validated
//                   without the catalog (a joining peer materializes the RDb
//                   before it has the catalog); a create that does not match
//                   its catalog resolves to an "unresolved" state.
//   update-catalog  deploys a later release of the same catalog, forward
//                   only, with the params that release first needs. When the
//                   RDb declares creators it must be signed by one of them.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, KeyId } from "@hyper-hyper-space/hhs3_crypto";
import { createPayloadTypeFormat } from "@hyper-hyper-space/hhs3_mvt";

import {
    MAX_NAME_LENGTH, MAX_NOTE_LENGTH,
    MAX_SEED_LENGTH, MAX_HASH_ALGORITHM_LENGTH,
    MAX_HASH_LENGTH, MAX_KEY_ID_LENGTH, MAX_SIGNATURE_LENGTH, MAX_PUBLIC_KEY_LENGTH,
    MAX_CREATORS, SchemaCreator, schemaCreatorFormat,
} from "../rschema/payload.js";
import { MAX_CATALOG_PARAMS } from "../rcatalog/payload.js";

export type { SchemaCreator } from "../rschema/payload.js";

// A deploy-time catalog param value: an identity (key id plus public key) for
// an 'identity' param, or a literal of the declared column type.
export type ParamValue =
    | { identity: { keyId: KeyId; publicKey: string } }
    | { value: json.Literal };

export const paramValueFormat: json.Format = [json.Type.Union, [
    { identity: { keyId: [json.Type.BoundedString, MAX_KEY_ID_LENGTH], publicKey: [json.Type.BoundedString, MAX_PUBLIC_KEY_LENGTH] } },
    { value: json.Type.Something },
]];

export const paramsFormat: json.Format =
    [json.Type.BoundedMap, [json.Type.BoundedString, MAX_NAME_LENGTH], paramValueFormat, MAX_CATALOG_PARAMS];

export type RDbPayload = CreateRDbPayload | UpdateCatalogPayload;

export const RDB_TYPE_ID = 'hhs/rdb_v1';

export type CreateRDbPayload = {
    action: 'create';
    type: string;
    seed: string;
    name?: string;
    creators?: SchemaCreator[];
    catalog: B64Hash;
    release: B64Hash;
    params?: { [name: string]: ParamValue };
    hashAlgorithm?: string;
};

export const createRDbFormat: json.Format = {
    action: [json.Type.Constant, 'create'],
    type: createPayloadTypeFormat(RDB_TYPE_ID),
    seed: [json.Type.BoundedString, MAX_SEED_LENGTH],
    name: [json.Type.Option, [json.Type.BoundedString, MAX_NAME_LENGTH]],
    creators: [json.Type.Option, [json.Type.BoundedArray, schemaCreatorFormat, MAX_CREATORS]],
    catalog: [json.Type.BoundedString, MAX_HASH_LENGTH],
    release: [json.Type.BoundedString, MAX_HASH_LENGTH],
    params: [json.Type.Option, paramsFormat],
    hashAlgorithm: [json.Type.Option, [json.Type.BoundedString, MAX_HASH_ALGORITHM_LENGTH]],
};

// Deploy a later release of the catalog. `catalog` must equal the create's in
// v1 (future: forks). When the RDb declares creators, author + signature are
// required (enforced in validate_ops.ts).

export type UpdateCatalogPayload = {
    action: 'update-catalog';
    catalog: B64Hash;
    release: B64Hash;
    params?: { [name: string]: ParamValue };
    note?: string;
    author?: KeyId;
    signature?: string;
};

export const updateCatalogFormat: json.Format = {
    action: [json.Type.Constant, 'update-catalog'],
    catalog: [json.Type.BoundedString, MAX_HASH_LENGTH],
    release: [json.Type.BoundedString, MAX_HASH_LENGTH],
    params: [json.Type.Option, paramsFormat],
    note: [json.Type.Option, [json.Type.BoundedString, MAX_NOTE_LENGTH]],
    author: [json.Type.Option, [json.Type.BoundedString, MAX_KEY_ID_LENGTH]],
    signature: [json.Type.Option, [json.Type.BoundedString, MAX_SIGNATURE_LENGTH]],
};
