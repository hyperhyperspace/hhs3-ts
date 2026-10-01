// Format-level and position-independent validation for RBlobStore payloads:
// object and action, strict json format, then semantics (canonical base64 of
// exact lengths, hash recomputation). Positional checks are in validate_ops.ts.
//
//   validateBlobStoreCreate   create payloads
//   checkBlobStoreOp          ops; returns the decoded chunk bytes, so the
//                             positional checks never decode twice

import { json } from "@hyper-hyper-space/hhs3_json";
import { validationFailure, validationOk, ValidationResult, ValidationFailure } from "@hyper-hyper-space/hhs3_mvt";

import { isValidName } from "../rschema/validate.js";
import { filesAccessReason, authorReason, refVersionReason } from "../rfiles/access.js";
import { decodeCanonicalBase64, isContentHash, fileHashOf, END_LINK } from "../rfiles/hashes.js";
import {
    createBlobStoreFormat, blobRefAdvanceFormat, fileHeaderFormat, chunkOpFormat,
    CreateBlobStorePayload, BlobRefAdvancePayload, FileHeaderPayload, ChunkPayload, CHUNK_BYTES,
} from "./payload.js";

function isObject(payload: json.Literal): payload is json.LiteralMap {
    return typeof payload === 'object' && payload !== null && !Array.isArray(payload);
}

export function validateBlobStoreCreate(payload: json.Literal): ValidationResult {
    if (!isObject(payload) || payload['action'] !== 'create') return validationFailure("RBlobStore create payload must be a create object");
    if (!json.checkFormat(createBlobStoreFormat, payload)) return validationFailure("RBlobStore create payload format is invalid");
    const create = payload as unknown as CreateBlobStorePayload;
    if (!isValidName(create.name)) return validationFailure(`invalid RBlobStore name '${create.name}'`);
    const reason = filesAccessReason(create);
    return reason === undefined ? validationOk() : validationFailure(`RBlobStore access is invalid: ${reason}`);
}

export type CheckedBlobOp =
    | { action: 'ref-advance'; payload: BlobRefAdvancePayload }
    | { action: 'file'; payload: FileHeaderPayload }
    | { action: 'chunk'; payload: ChunkPayload; bytes: Uint8Array };

export type CheckedBlobOpResult = { valid: true; op: CheckedBlobOp } | { valid: false; why: ValidationFailure };

function fail(reason: string): CheckedBlobOpResult {
    return { valid: false, why: { reason } };
}

export function checkBlobStoreOp(payload: json.Literal, groupId: string): CheckedBlobOpResult {
    if (!isObject(payload)) return fail("RBlobStore payload must be an object");
    const action = payload['action'];

    if (action === 'ref-advance') {
        if (!json.checkFormat(blobRefAdvanceFormat, payload)) return fail("RBlobStore ref-advance format is invalid");
        const p = payload as unknown as BlobRefAdvancePayload;
        if (p.refId !== groupId) return fail(`ref '${p.refId}' is not the bound group`);
        const reason = refVersionReason(p.refVersion as unknown as json.LiteralMap) ?? authorReason(payload);
        return reason === undefined ? { valid: true, op: { action, payload: p } } : fail(`RBlobStore ref-advance is invalid: ${reason}`);
    }

    if (action === 'file') {
        if (!json.checkFormat(fileHeaderFormat, payload)) return fail("RBlobStore file header format is invalid");
        const p = payload as unknown as FileHeaderPayload;
        const reason = authorReason(payload);
        if (reason !== undefined) return fail(`RBlobStore file header is invalid: ${reason}`);
        if (!isContentHash(p.fileHash) || !isContentHash(p.first)) return fail("RBlobStore file header hashes are malformed");
        if (p.size === 0 && p.first !== END_LINK) return fail("an empty file's first link must be END");
        if (fileHashOf(p.size, p.first) !== p.fileHash) return fail("RBlobStore file header fileHash does not match its size and first link");
        return { valid: true, op: { action, payload: p } };
    }

    if (action === 'chunk') {
        if (!json.checkFormat(chunkOpFormat, payload)) return fail("RBlobStore chunk format is invalid");
        const p = payload as unknown as ChunkPayload;
        const reason = authorReason(payload);
        if (reason !== undefined) return fail(`RBlobStore chunk is invalid: ${reason}`);
        if (!isContentHash(p.header) || !isContentHash(p.next)) return fail("RBlobStore chunk hashes are malformed");
        const bytes = decodeCanonicalBase64(p.bytes);
        if (bytes === undefined) return fail("RBlobStore chunk bytes are not canonical base64");
        if (bytes.length === 0 || bytes.length > CHUNK_BYTES) return fail("RBlobStore chunk length is out of range");
        return { valid: true, op: { action, payload: p, bytes } };
    }

    return fail(`unknown RBlobStore action '${String(action)}'`);
}
