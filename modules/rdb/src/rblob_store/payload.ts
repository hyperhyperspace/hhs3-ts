// Payloads for RBlobStore operations, and their format validators.
//
// An RBlobStore holds file contents as chains of signed chunk ops. A file is
// uploaded as a header (`file`) followed by its chunks, each chunk the only
// successor of the one before it, so an upload chain is a path in the DAG:
//
//   ref-advance -> file -> chunk 0 -> chunk 1 -> ... -> chunk count-1
//
// Each chunk carries `next`, the link of the chunk after it (END for the last),
// and must hash to the link its predecessor promised (the header's `first` for
// chunk 0). The header's fileHash commits to the size and the first link, so a
// chunk is verified on arrival and a chain whose last chunk is present is a
// complete, verified file.
//
// Writes go through the bound group's identities (see ../rfiles/access.ts).
// Lanes spread concurrent uploads over parallel chains: a writer appends an
// upload on the frontier entries of one lane. Lane discipline is a writer
// convention and is not validated; breaking it only costs parallelism.

import { json } from "@hyper-hyper-space/hhs3_json";
import type { B64Hash, KeyId } from "@hyper-hyper-space/hhs3_crypto";
import { createPayloadTypeFormat } from "@hyper-hyper-space/hhs3_mvt";

import type { Predicate } from "../rschema/payload.js";
import { MAX_NAME_LENGTH, MAX_SEED_LENGTH, MAX_HASH_LENGTH } from "../rschema/payload.js";
import { filesAccessFormat, authorFormat, signedRefAdvanceFormat } from "../rfiles/access.js";
import { base64Length } from "../rfiles/hashes.js";

export const RBLOB_STORE_TYPE_ID = 'hhs/rblob_store_v1';

export const LANES = 6;
export const CHUNK_BYTES = 131072;
export const MAX_FILE_BYTES = 2 ** 37;
export const MAX_CHUNKS = MAX_FILE_BYTES / CHUNK_BYTES;
export const CHUNK_B64_CHARS = base64Length(CHUNK_BYTES);

export function chunkCount(size: number): number {
    return Math.ceil(size / CHUNK_BYTES);
}

// The byte length of chunk `index` of a file of `size` bytes.
export function chunkLength(size: number, index: number): number {
    const count = chunkCount(size);
    return index < count - 1 ? CHUNK_BYTES : size - (count - 1) * CHUNK_BYTES;
}

export type CreateBlobStorePayload = {
    action: 'create';
    type: string;
    name: string;
    seed: string;
    bindings: { [alias: string]: B64Hash };
    idProvider: string;
    canWrite: Predicate;
    lanes: number;
    chunkBytes: number;
};

export const createBlobStoreFormat: json.Format = {
    action: [json.Type.Constant, 'create'],
    type: createPayloadTypeFormat(RBLOB_STORE_TYPE_ID),
    name: [json.Type.BoundedString, MAX_NAME_LENGTH],
    seed: [json.Type.BoundedString, MAX_SEED_LENGTH],
    ...filesAccessFormat,
    lanes: [json.Type.Constant, LANES],
    chunkBytes: [json.Type.Constant, CHUNK_BYTES],
};

export type BlobRefAdvancePayload = {
    action: 'ref-advance';
    refId: B64Hash;
    refVersion: json.Set;
    lane: number;
    author: KeyId;
    signature: string;
};

export const blobRefAdvanceFormat: json.Format = {
    ...signedRefAdvanceFormat,
    lane: [json.Type.BoundedInt, 0, LANES - 1],
};

// The header of one upload.
export type FileHeaderPayload = {
    action: 'file';
    lane: number;
    fileHash: B64Hash;
    size: number;
    first: B64Hash;
    author: KeyId;
    signature: string;
};

export const fileHeaderFormat: json.Format = {
    action: [json.Type.Constant, 'file'],
    lane: [json.Type.BoundedInt, 0, LANES - 1],
    fileHash: [json.Type.BoundedString, MAX_HASH_LENGTH],
    size: [json.Type.BoundedInt, 0, MAX_FILE_BYTES],
    first: [json.Type.BoundedString, MAX_HASH_LENGTH],
    ...authorFormat,
};

export type ChunkPayload = {
    action: 'chunk';
    header: B64Hash;       // entry hash of the upload's file op
    index: number;
    bytes: string;         // canonical base64
    next: B64Hash;         // link of chunk index+1, END for the last chunk
    author: KeyId;
    signature: string;
};

export const chunkOpFormat: json.Format = {
    action: [json.Type.Constant, 'chunk'],
    header: [json.Type.BoundedString, MAX_HASH_LENGTH],
    index: [json.Type.BoundedInt, 0, MAX_CHUNKS - 1],
    bytes: [json.Type.BoundedString, CHUNK_B64_CHARS],
    next: [json.Type.BoundedString, MAX_HASH_LENGTH],
    ...authorFormat,
};

export type BlobStoreOpPayload = BlobRefAdvancePayload | FileHeaderPayload | ChunkPayload;

// Entry meta: every op is tagged with its lane; headers and chunks with their
// file hash (findChains looks up chain progress by it); the op that completes
// a file (the last chunk, or the header of an empty file) with `done`.
export const META_LANE = 'lane';
export const META_FILE = 'file';
export const META_DONE = 'done';
