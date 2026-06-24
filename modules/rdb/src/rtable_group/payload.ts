// Payloads for RTableGroup operations, and their format validators.
//
// An RTableGroup owns one physical DAG: it is the unit of atomicity, snapshot,
// observation and composition. Member RTables live on scoped projections of
// this DAG; a group position is a consistent snapshot of all member tables.
//
// The group observes its RSchema (and any foreign groups referenced by
// cross-group FKs) via ref-advance ops; the canonical ref-advance payload from
// the mvt module is reused as-is. The group's barrier ref-advance to a new
// RSchema version is the schema deploy moment. Deploy authority is the
// group's own policy: the create payload may carry a `canDeploy` predicate
// ('object' context: $author available, no subject row), evaluated against
// the ref-advance author at the op's position; the ref-advance op carries
// author/signature as extra fields (the canonical format is checked
// non-strictly to allow this). Like bindings, canDeploy is fixed at creation
// in v1; rotating deploy authority = granting/revoking cap rows it points at.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { createPayloadTypeFormat } from "@hyper-hyper-space/hhs3_mvt";

import {
    Predicate,
    MAX_NAME_LENGTH, MAX_QUALIFIED_NAME_LENGTH, MAX_TABLES,
    MAX_SEED_LENGTH, MAX_HASH_ALGORITHM_LENGTH,
    MAX_HASH_LENGTH,
} from "../rschema/payload.js";
export const MAX_INITIAL_ROWS_PER_TABLE = 1024;
export const MAX_BINDINGS = 256;
export const MAX_BUNDLE_OPS = 1024;

// Create a table group:

// `schemaRef` + `schemaVersion` pin the RSchema (id and initial version) this
// group instance deploys. Validating a group create therefore requires
// resolving the RSchema at the pinned version (a cross-DAG dependency at
// genesis).
//
// `initialRows` are genesis fiat rows, keyed by table: they are given, not
// validated as ops (no preconditions, no signatures), like RCap's irrevocable
// creators. They solve the permissions bootstrap: a pre-filled admin or
// capability row roots every delegation chain, and identity rows carry public
// keys as column values (solving keyId -> publicKey lookup). Each entry uses
// the insert row op shape.
//
// `bindings` fix the group-name resolution for qualified FK targets
// ('group.table') and gate targets, by value, at creation time: name ->
// concrete group object id. v1 bindings are immutable; mutation via barrier
// ops may come later. A binding whose target object is not present in the
// replica is an infrastructure error (throw), never an MVT data condition.
//
// `idProvider` selects this group's identity provider for signature
// verification: a LOCAL table name (must exist in the pinned schema and be
// flagged idProvider) or a qualified 'group.table' (name-resolvability only at
// create — its group-name must be in `bindings`; the foreign table being a
// provider is a runtime concern). Fixed v1 like bindings / canDeploy. A group
// with no idProvider performs no authentication (claimed authors are trusted).

export const RTABLE_GROUP_TYPE_ID = 'hhs/rtable_group_v1';

export type CreateTableGroupPayload = {
    action: 'create';
    type: string;
    name: string;                                  // descriptive label (like RSchema's name); not used in validation
    seed: string;
    schemaRef: B64Hash;                            // RSchema object id
    schemaVersion: json.Set;                       // pinned initial RSchema version (set of entry hashes)
    initialRows?: { [table: string]: json.Literal[] };   // genesis fiat rows (insert op shape)
    bindings?: { [name: string]: B64Hash };        // group name -> group object id (fixed v1)
    canDeploy?: Predicate;                         // gates schema ref-advances ('object' context; fixed v1)
    idProvider?: string;                           // local table name or 'group.table' (fixed v1)
    hashAlgorithm?: string;
};

export const createTableGroupFormat: json.Format = {
    action: [json.Type.Constant, 'create'],
    type: createPayloadTypeFormat(RTABLE_GROUP_TYPE_ID),
    name: [json.Type.BoundedString, MAX_NAME_LENGTH],
    seed: [json.Type.BoundedString, MAX_SEED_LENGTH],
    schemaRef: [json.Type.BoundedString, MAX_HASH_LENGTH],
    schemaVersion: json.Type.Something,
    initialRows: [json.Type.Option, [json.Type.BoundedMap,
        [json.Type.BoundedString, MAX_NAME_LENGTH],
        [json.Type.BoundedArray, json.Type.Something, MAX_INITIAL_ROWS_PER_TABLE],
        MAX_TABLES]],
    bindings: [json.Type.Option, [json.Type.BoundedMap,
        [json.Type.BoundedString, MAX_NAME_LENGTH],
        [json.Type.BoundedString, MAX_HASH_LENGTH],
        MAX_BINDINGS]],
    canDeploy: [json.Type.Option, json.Type.Something],   // checked with validatePredicate('object')
    idProvider: [json.Type.Option, [json.Type.BoundedString, MAX_QUALIFIED_NAME_LENGTH]],
    hashAlgorithm: [json.Type.Option, [json.Type.BoundedString, MAX_HASH_ALGORITHM_LENGTH]],
};

// Row envelope (*):

// (*) Used by the table DagScope automatically when a row op is appended
// through a member RTable: one inner row op wrapped into one group entry,
// tagged with its table so the table's scope filter picks it back out.

export type RowEnvelopePayload = {
    action: 'row';
    table: string;
    op: json.Literal;                      // an RTable row op (insert / update / delete)
};

export const rowEnvelopeFormat: json.Format = {
    action: [json.Type.Constant, 'row'],
    table: [json.Type.BoundedString, MAX_NAME_LENGTH],
    op: json.Type.Something,
};

// Bundle: a single-entry atomic write across several member tables.

// The parts hash, validate and apply together and can never exist apart, even
// mid-sync. Entry meta is tagged with every table the bundle touches, so each
// table's scope filter picks up its slice.
//
// `writes` is an ORDERED ARRAY (not a table-keyed map): the bundle's ops are
// totally ordered, and op i's FK conditions are checked at the sequential cut
// `at` ∪ the ops before i (see validate_ops.ts). The order must be carried
// explicitly because entry hashing normalizes payloads with sorted map keys
// (json.toStringNormalized) — array order survives normalization and is
// hash-stable across replicas.

export type BundlePayload = {
    action: 'bundle';
    // ordered (the bundle order); op is an RTable row op (insert/update/delete)
    writes: Array<{ table: string; op: json.Literal }>;
};

export const bundleWriteFormat: json.Format = {
    table: [json.Type.BoundedString, MAX_NAME_LENGTH],
    op: json.Type.Something,
};

export const bundleFormat: json.Format = {
    action: [json.Type.Constant, 'bundle'],
    writes: [json.Type.BoundedArray, bundleWriteFormat, MAX_BUNDLE_OPS],
};
