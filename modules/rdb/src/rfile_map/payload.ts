// Payloads for RFileMap operations, and their format validators.
//
// An RFileMap is a set of elements (section, owner, path, fileHash): which file
// contents sit at which path. It names contents by fileHash only and never
// observes or waits on its blob store.
//
// Sections:
//   common   any admitted writer may add or remove
//   key      one folder per key: `owner` is required and must be the author,
//            so only the owner writes there
//
// `remove` is a barrier: it also removes concurrent adds of the same element.
// Several hashes at one path coexist; replacing a file is a remove of the old
// element plus an add of the new one. Writes go through the bound group's
// identities (see ../rfiles/access.ts).

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, KeyId, sha256, stringToUint8Array } from "@hyper-hyper-space/hhs3_crypto";
import { createPayloadTypeFormat } from "@hyper-hyper-space/hhs3_mvt";

import type { Predicate } from "../rschema/payload.js";
import { MAX_NAME_LENGTH, MAX_SEED_LENGTH, MAX_HASH_LENGTH, MAX_KEY_ID_LENGTH } from "../rschema/payload.js";
import { filesAccessFormat, authorFormat, signedRefAdvanceFormat } from "../rfiles/access.js";
import { MAX_PATH_BYTES } from "../rfiles/path.js";

export const RFILE_MAP_TYPE_ID = 'hhs/rfile_map_v1';

export type CreateFileMapPayload = {
    action: 'create';
    type: string;
    name: string;
    seed: string;
    bindings: { [alias: string]: B64Hash };
    idProvider: string;
    canWrite: Predicate;
    blobStore: B64Hash;
};

export const createFileMapFormat: json.Format = {
    action: [json.Type.Constant, 'create'],
    type: createPayloadTypeFormat(RFILE_MAP_TYPE_ID),
    name: [json.Type.BoundedString, MAX_NAME_LENGTH],
    seed: [json.Type.BoundedString, MAX_SEED_LENGTH],
    ...filesAccessFormat,
    blobStore: [json.Type.BoundedString, MAX_HASH_LENGTH],
};

export type FileSection = 'common' | 'key';

export type FileElement = {
    section: FileSection;
    owner?: KeyId;
    path: string;
    fileHash: B64Hash;
};

export type FileMapElementPayload = FileElement & {
    action: 'add' | 'remove';
    author: KeyId;
    signature: string;
};

function elementOpFormat(action: 'add' | 'remove'): json.Format {
    return {
        action: [json.Type.Constant, action],
        section: [json.Type.Union, [[json.Type.Constant, 'common'], [json.Type.Constant, 'key']]],
        owner: [json.Type.Option, [json.Type.BoundedString, MAX_KEY_ID_LENGTH]],
        path: [json.Type.BoundedString, MAX_PATH_BYTES],
        fileHash: [json.Type.BoundedString, MAX_HASH_LENGTH],
        ...authorFormat,
    };
}

export const addFileFormat = elementOpFormat('add');
export const removeFileFormat = elementOpFormat('remove');
export const fileMapRefAdvanceFormat: json.Format = signedRefAdvanceFormat;

export function elementOf(p: FileElement): FileElement {
    const element: FileElement = { section: p.section, path: p.path, fileHash: p.fileHash };
    if (p.owner !== undefined) element.owner = p.owner;
    return element;
}

// The element id tags add and remove entries, so the DAG meta index finds
// every op on one element.
export function elementIdOf(element: FileElement): B64Hash {
    const literal: json.LiteralMap = { domain: 'hhs3-file-elmt-v1', ...elementOf(element) };
    return sha256.hashToB64(stringToUint8Array(json.toStringNormalized(literal)));
}

export const META_ELEMENT = 'elmt';
