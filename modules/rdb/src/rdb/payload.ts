// Payloads for RDb operations, and their format validators.
//
// An RDb is the sync root and orchestrator for a deployed database: its DAG
// records which RSchemas and RTableGroups belong to the deployment, and its
// runtime role is to ensure those objects (and their transitive references:
// schemas, bound foreign groups) are present and syncing in the replica.
//
// RDb state is ADVISORY: nothing's validity ever depends on it. Groups remain
// fully valid and verifiable without their RDb. Membership is monotonic
// (add-only, no removal in v1), which keeps even advisory reads confluent.
// A member or referenced object missing from the replica is an infrastructure
// error (throw), never an MVT data condition.
//
// Deployment authority: the create payload may name creators (keyIds plus
// public keys); when the list is non-empty, every add-schema / add-group must
// be signed by one of them. When omitted or empty, membership ops are
// unsigned (legacy / open mode). This is distinct from group row-level
// idProvider authentication.
//
// Name resolution for qualified FK targets does NOT go through the RDb: each
// group fixes its own bindings (name -> group id) at creation time. The
// membership ops carry an optional free-form `note` for human bookkeeping
// only; it is never resolved and is never a key (RDb membership is keyed by
// schema / group id). Qualified FK / exists / idProvider names resolve through
// each RTableGroup's immutable `bindings`, so RDb carries no resolvable labels.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, KeyId } from "@hyper-hyper-space/hhs3_crypto";
import { createPayloadTypeFormat } from "@hyper-hyper-space/hhs3_mvt";

import {
    MAX_NAME_LENGTH, MAX_NOTE_LENGTH,
    MAX_SEED_LENGTH, MAX_HASH_ALGORITHM_LENGTH,
    MAX_HASH_LENGTH, MAX_KEY_ID_LENGTH, MAX_SIGNATURE_LENGTH,
    MAX_CREATORS, SchemaCreator, schemaCreatorFormat,
} from "../rschema/payload.js";

export type { SchemaCreator } from "../rschema/payload.js";

export type RDbPayload = CreateRDbPayload | AddSchemaPayload | AddGroupPayload;

// Create a database (sync root):

export const RDB_TYPE_ID = 'hhs/rdb_v1';

export type CreateRDbPayload = {
    action: 'create';
    type: string;
    seed: string;
    name?: string;
    creators?: SchemaCreator[];
    hashAlgorithm?: string;
};

export const createRDbFormat: json.Format = {
    action: [json.Type.Constant, 'create'],
    type: createPayloadTypeFormat(RDB_TYPE_ID),
    seed: [json.Type.BoundedString, MAX_SEED_LENGTH],
    name: [json.Type.Option, [json.Type.BoundedString, MAX_NAME_LENGTH]],
    creators: [json.Type.Option, [json.Type.BoundedArray, schemaCreatorFormat, MAX_CREATORS]],
    hashAlgorithm: [json.Type.Option, [json.Type.BoundedString, MAX_HASH_ALGORITHM_LENGTH]],
};

// Add a schema to the deployment (monotonic; `note` is a free-form comment,
// never resolved, never a key). When the RDb declares creators, author +
// signature are required (enforced in validate_ops.ts).

export type AddSchemaPayload = {
    action: 'add-schema';
    schemaId: B64Hash;
    note?: string;
    author?: KeyId;
    signature?: string;
};

export const addSchemaFormat: json.Format = {
    action: [json.Type.Constant, 'add-schema'],
    schemaId: [json.Type.BoundedString, MAX_HASH_LENGTH],
    note: [json.Type.Option, [json.Type.BoundedString, MAX_NOTE_LENGTH]],
    author: [json.Type.Option, [json.Type.BoundedString, MAX_KEY_ID_LENGTH]],
    signature: [json.Type.Option, [json.Type.BoundedString, MAX_SIGNATURE_LENGTH]],
};

// Add a deployed table group to the deployment (monotonic; `note` free-form).
// When the RDb declares creators, author + signature are required.

export type AddGroupPayload = {
    action: 'add-group';
    groupId: B64Hash;
    note?: string;
    author?: KeyId;
    signature?: string;
};

export const addGroupFormat: json.Format = {
    action: [json.Type.Constant, 'add-group'],
    groupId: [json.Type.BoundedString, MAX_HASH_LENGTH],
    note: [json.Type.Option, [json.Type.BoundedString, MAX_NOTE_LENGTH]],
    author: [json.Type.Option, [json.Type.BoundedString, MAX_KEY_ID_LENGTH]],
    signature: [json.Type.Option, [json.Type.BoundedString, MAX_SIGNATURE_LENGTH]],
};
