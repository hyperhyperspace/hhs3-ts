// Format-level and position-independent validation for RFileMap payloads:
// object and action, strict json format, then semantics (section and owner
// rule, portable path, canonical hashes). Positional checks are in
// validate_ops.ts.

import { json } from "@hyper-hyper-space/hhs3_json";
import { validationFailure, validationOk, ValidationResult, ValidationFailure } from "@hyper-hyper-space/hhs3_mvt";
import type { RefAdvancePayload } from "@hyper-hyper-space/hhs3_mvt";

import { isValidName } from "../rschema/validate.js";
import { filesAccessReason, authorReason, refVersionReason } from "../rfiles/access.js";
import { isContentHash } from "../rfiles/hashes.js";
import { filePathReason } from "../rfiles/path.js";
import {
    createFileMapFormat, addFileFormat, removeFileFormat, fileMapRefAdvanceFormat,
    CreateFileMapPayload, FileMapElementPayload,
} from "./payload.js";

function isObject(payload: json.Literal): payload is json.LiteralMap {
    return typeof payload === 'object' && payload !== null && !Array.isArray(payload);
}

export function validateFileMapCreate(payload: json.Literal): ValidationResult {
    if (!isObject(payload) || payload['action'] !== 'create') return validationFailure("RFileMap create payload must be a create object");
    if (!json.checkFormat(createFileMapFormat, payload)) return validationFailure("RFileMap create payload format is invalid");
    const create = payload as unknown as CreateFileMapPayload;
    if (!isValidName(create.name)) return validationFailure(`invalid RFileMap name '${create.name}'`);
    if (!isContentHash(create.blobStore)) return validationFailure("RFileMap blobStore is not an object id");
    const reason = filesAccessReason(create);
    return reason === undefined ? validationOk() : validationFailure(`RFileMap access is invalid: ${reason}`);
}

export type CheckedMapOp =
    | { action: 'ref-advance'; payload: RefAdvancePayload & json.LiteralMap }
    | { action: 'add' | 'remove'; payload: FileMapElementPayload };

export type CheckedMapOpResult = { valid: true; op: CheckedMapOp } | { valid: false; why: ValidationFailure };

function fail(reason: string): CheckedMapOpResult {
    return { valid: false, why: { reason } };
}

export function checkFileMapOp(payload: json.Literal, groupId: string): CheckedMapOpResult {
    if (!isObject(payload)) return fail("RFileMap payload must be an object");
    const action = payload['action'];

    if (action === 'ref-advance') {
        if (!json.checkFormat(fileMapRefAdvanceFormat, payload)) return fail("RFileMap ref-advance format is invalid");
        const p = payload as unknown as RefAdvancePayload & json.LiteralMap;
        if (p.refId !== groupId) return fail(`ref '${p.refId}' is not the bound group`);
        const reason = refVersionReason(p.refVersion as unknown as json.LiteralMap) ?? authorReason(payload);
        return reason === undefined ? { valid: true, op: { action, payload: p } } : fail(`RFileMap ref-advance is invalid: ${reason}`);
    }

    if (action === 'add' || action === 'remove') {
        if (!json.checkFormat(action === 'add' ? addFileFormat : removeFileFormat, payload)) return fail(`RFileMap ${action} format is invalid`);
        const p = payload as unknown as FileMapElementPayload;
        const reason = authorReason(payload);
        if (reason !== undefined) return fail(`RFileMap ${action} is invalid: ${reason}`);
        if (p.section === 'key') {
            if (p.owner === undefined) return fail(`RFileMap ${action} in the key section needs an owner`);
            if (p.owner !== p.author) return fail(`RFileMap ${action} in the key section must be by its owner`);
        } else if (p.owner !== undefined) {
            return fail(`RFileMap ${action} in the common section has no owner`);
        }
        const pathReason = filePathReason(p.path);
        if (pathReason !== undefined) return fail(`RFileMap ${action} path ${pathReason}`);
        if (!isContentHash(p.fileHash)) return fail(`RFileMap ${action} fileHash is malformed`);
        return { valid: true, op: { action, payload: p } };
    }

    return fail(`unknown RFileMap action '${String(action)}'`);
}
