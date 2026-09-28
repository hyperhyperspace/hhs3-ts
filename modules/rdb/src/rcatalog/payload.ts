// Payloads for RCatalog operations, and their format validators.
//
// An RCatalog is a developer-signed, standalone RObject: a DAG of semver
// releases describing the table groups of a database. Admins deploy a release
// into an RDb, whose member groups are then computed from the release by the
// normative instantiation in ../rdb/instantiate.ts.
//
// Every entry is signed by one of the creators the genesis declares:
//
//   create   - the genesis, which is also the first release. Signed at the
//              empty position (the genesis signature binds the payload to its
//              own object id).
//   release  - a diff against its parents (the maximal releases in its causal
//              past): the group definitions it adds (`add`, at their pins),
//              the version changes of existing groups (`changes`), and the
//              params it declares. Its state is folded from its parents.
//   declare  - dependency-free: names schemas that later releases will
//              reference, so the RDb can discover and fetch them before the
//              release that needs them validates.
//
// Group definitions are immutable and content-addressed: a definition's
// identity is the hash of its normalized payload (catalogGroupHash), and
// bindings point at other definitions by that hash.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, KeyId, sha256, stringToUint8Array } from "@hyper-hyper-space/hhs3_crypto";
import { createPayloadTypeFormat } from "@hyper-hyper-space/hhs3_mvt";

import {
    ColumnType, COLUMN_TYPES, Predicate, SchemaCreator, schemaCreatorFormat,
    MAX_NAME_LENGTH, MAX_QUALIFIED_NAME_LENGTH, MAX_COLUMNS, MAX_TABLES,
    MAX_SEED_LENGTH, MAX_HASH_ALGORITHM_LENGTH, MAX_HASH_LENGTH,
    MAX_KEY_ID_LENGTH, MAX_SIGNATURE_LENGTH, MAX_NOTE_LENGTH, MAX_CREATORS,
} from "../rschema/payload.js";
import { MAX_BINDINGS, MAX_INITIAL_ROWS_PER_TABLE } from "../rtable_group/payload.js";

export type { SchemaCreator } from "../rschema/payload.js";

export const RCATALOG_TYPE_ID = 'hhs/rcatalog_v1';

export const MAX_CATALOG_GROUPS = 256;
export const MAX_CATALOG_PARAMS = 64;
export const MAX_DECLARED_SCHEMAS = 256;
export const MAX_VERSION_WIDTH = 64;
export const MAX_SEMVER_LENGTH = 64;

export const versionSetFormat: json.Format =
    [json.Type.BoundedMap, [json.Type.BoundedString, MAX_HASH_LENGTH], [json.Type.Constant, ''], MAX_VERSION_WIDTH];

// A genesis row of a catalog group. Literal columns go in `values`; columns
// filled from a deploy-time param go in `params` (kept out of band, so a
// template never mixes a placeholder into a literal value). `publicKey`
// substitutes an identity param's public key instead of its key id.

export type CatalogRowParam = { param: string; fn?: 'publicKey' };

export type CatalogRowTemplate = {
    values: json.LiteralMap;
    params?: { [column: string]: CatalogRowParam };
};

export const catalogRowParamFormat: json.Format = {
    param: [json.Type.BoundedString, MAX_NAME_LENGTH],
    fn: [json.Type.Option, [json.Type.Constant, 'publicKey']],
};

export const catalogRowTemplateFormat: json.Format = {
    values: [json.Type.BoundedMap, [json.Type.BoundedString, MAX_NAME_LENGTH], json.Type.Something, MAX_COLUMNS],
    params: [json.Type.Option, [json.Type.BoundedMap, [json.Type.BoundedString, MAX_NAME_LENGTH], catalogRowParamFormat, MAX_COLUMNS]],
};

// A deploy-time parameter. An 'identity' param is supplied as a key id plus
// public key; any other type is supplied as a literal of that column type.

export type CatalogParamDecl = { name: string; type: ColumnType };

export const catalogParamDeclFormat: json.Format = {
    name: [json.Type.BoundedString, MAX_NAME_LENGTH],
    type: [json.Type.Union, COLUMN_TYPES.map((t) => [json.Type.Constant, t] as json.Format)],
};

// A catalog group definition (immutable; identity = catalogGroupHash(def)).
// `schemaRef` + `schemaVersion` pin the schema the group is created at.
// `bindings` map an alias to another definition's hash; instantiation turns
// them into the concrete group ids of the same RDb. `seedSource: 'rdb'` means
// the group's seed derives from the RDb id (v1 only; future: shared groups).

export type CatalogGroupDef = {
    name: string;
    seedSource: 'rdb';
    schemaRef: B64Hash;
    schemaVersion: json.Set;
    bindings?: { [alias: string]: B64Hash };
    initialRows?: { [table: string]: CatalogRowTemplate[] };
    canDeploy?: Predicate;
    canObserve?: { [alias: string]: Predicate };
    idProvider?: string;
};

export const catalogGroupDefFormat: json.Format = {
    name: [json.Type.BoundedString, MAX_NAME_LENGTH],
    seedSource: [json.Type.Constant, 'rdb'],
    schemaRef: [json.Type.BoundedString, MAX_HASH_LENGTH],
    schemaVersion: versionSetFormat,
    bindings: [json.Type.Option, [json.Type.BoundedMap,
        [json.Type.BoundedString, MAX_NAME_LENGTH],
        [json.Type.BoundedString, MAX_HASH_LENGTH],
        MAX_BINDINGS]],
    initialRows: [json.Type.Option, [json.Type.BoundedMap,
        [json.Type.BoundedString, MAX_NAME_LENGTH],
        [json.Type.BoundedArray, catalogRowTemplateFormat, MAX_INITIAL_ROWS_PER_TABLE],
        MAX_TABLES]],
    canDeploy: [json.Type.Option, json.Type.Something],
    canObserve: [json.Type.Option, [json.Type.BoundedMap,
        [json.Type.BoundedString, MAX_NAME_LENGTH],
        json.Type.Something,
        MAX_BINDINGS]],
    idProvider: [json.Type.Option, [json.Type.BoundedString, MAX_QUALIFIED_NAME_LENGTH]],
};

export function catalogGroupHash(def: CatalogGroupDef): B64Hash {
    return sha256.hashToB64(stringToUint8Array(json.toStringNormalized(def as unknown as json.Literal)));
}

// A version change of an existing group. `schema` repeats the definition's
// schemaRef so that foreign deps can be extracted from the payload alone.

export type CatalogGroupChange = { schema: B64Hash; version: json.Set };

export const catalogGroupChangeFormat: json.Format = {
    schema: [json.Type.BoundedString, MAX_HASH_LENGTH],
    version: versionSetFormat,
};

export type CatalogReleaseBody = {
    version: string;
    changes?: { [catalogGroupHash: string]: CatalogGroupChange };
    add?: CatalogGroupDef[];
    params?: CatalogParamDecl[];
    note?: string;
    author: KeyId;
    signature: string;
};

const releaseBodyFormat: { [key: string]: json.OptionFormat } = {
    version: [json.Type.BoundedString, MAX_SEMVER_LENGTH],
    changes: [json.Type.Option, [json.Type.BoundedMap,
        [json.Type.BoundedString, MAX_HASH_LENGTH],
        catalogGroupChangeFormat,
        MAX_CATALOG_GROUPS]],
    add: [json.Type.Option, [json.Type.BoundedArray, catalogGroupDefFormat, MAX_CATALOG_GROUPS]],
    params: [json.Type.Option, [json.Type.BoundedArray, catalogParamDeclFormat, MAX_CATALOG_PARAMS]],
    note: [json.Type.Option, [json.Type.BoundedString, MAX_NOTE_LENGTH]],
    author: [json.Type.BoundedString, MAX_KEY_ID_LENGTH],
    signature: [json.Type.BoundedString, MAX_SIGNATURE_LENGTH],
};

export type CreateRCatalogPayload = CatalogReleaseBody & {
    action: 'create';
    type: string;
    name: string;
    creators: SchemaCreator[];
    seed?: string;
    hashAlgorithm?: string;
};

export const createRCatalogFormat: json.Format = {
    ...releaseBodyFormat,
    action: [json.Type.Constant, 'create'],
    type: createPayloadTypeFormat(RCATALOG_TYPE_ID),
    name: [json.Type.BoundedString, MAX_NAME_LENGTH],
    creators: [json.Type.BoundedArray, schemaCreatorFormat, MAX_CREATORS],
    seed: [json.Type.Option, [json.Type.BoundedString, MAX_SEED_LENGTH]],
    hashAlgorithm: [json.Type.Option, [json.Type.BoundedString, MAX_HASH_ALGORITHM_LENGTH]],
};

export type CatalogReleasePayload = CatalogReleaseBody & { action: 'release' };

export const catalogReleaseFormat: json.Format = {
    ...releaseBodyFormat,
    action: [json.Type.Constant, 'release'],
};

export type CatalogDeclarePayload = {
    action: 'declare';
    schemas: json.Set;
    author: KeyId;
    signature: string;
};

export const catalogDeclareFormat: json.Format = {
    action: [json.Type.Constant, 'declare'],
    schemas: [json.Type.BoundedMap, [json.Type.BoundedString, MAX_HASH_LENGTH], [json.Type.Constant, ''], MAX_DECLARED_SCHEMAS],
    author: [json.Type.BoundedString, MAX_KEY_ID_LENGTH],
    signature: [json.Type.BoundedString, MAX_SIGNATURE_LENGTH],
};

export type RCatalogPayload = CreateRCatalogPayload | CatalogReleasePayload | CatalogDeclarePayload;
