// Semantic validation for RDb payloads, layered on format checks in validate.ts.
//
//   create       - every creator's keyId matches its public key (when declared)
//   add-schema   - when creators non-empty: signature by a creator; when empty:
//                  reject unexpected author/signature fields
//   add-group    - same as add-schema

import { json } from "@hyper-hyper-space/hhs3_json";
import { KeyId, PublicKey } from "@hyper-hyper-space/hhs3_crypto";
import {
    RContext,
    validationFailure, validationOk, ValidationResult,
} from "@hyper-hyper-space/hhs3_mvt";
import { verifyPayloadSignature, deserializePublicKeyFromBase64, computeKeyId } from "@hyper-hyper-space/hhs3_mvt";

import {
    CreateRDbPayload, AddSchemaPayload, AddGroupPayload, SchemaCreator,
} from "./payload.js";
import { validateRDbPayloadFormat } from "./validate.js";
import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";

export type RDbOpHost = {
    getId(): B64Hash;
    getCreators(): SchemaCreator[];
    isCreator(keyId: KeyId): boolean;
};

export type RDbValidationContext =
    | { mode: 'create'; ctx: RContext }
    | { mode: 'op'; rdb: RDbOpHost };

export async function validateRDbPayload(payload: json.Literal, context: RDbValidationContext): Promise<ValidationResult> {
    const formatResult = validateRDbPayloadFormat(payload);
    if (!formatResult.valid) return formatResult;

    if (context.mode === 'create') {
        return validateCreate(payload as CreateRDbPayload, context.ctx);
    }

    const action = (payload as json.LiteralMap)['action'];
    if (action === 'add-schema') {
        return validateMembership(payload as AddSchemaPayload, context.rdb, 'add-schema');
    }
    if (action === 'add-group') {
        return validateMembership(payload as AddGroupPayload, context.rdb, 'add-group');
    }

    return validationFailure(`unknown RDb action '${String(action)}'`, { objectHash: context.rdb.getId() });
}

function validateCreate(create: CreateRDbPayload, ctx: RContext): ValidationResult {
    const creators = create.creators ?? [];
    const hashSuite = ctx.getHashSuite();
    const seen = new Set<KeyId>();

    for (const creator of creators) {
        if (seen.has(creator.keyId)) return validationFailure(`duplicate RDb creator '${creator.keyId}'`);
        seen.add(creator.keyId);
        try {
            const pk = deserializePublicKeyFromBase64(creator.publicKey);
            if (computeKeyId(pk, hashSuite) !== creator.keyId) {
                return validationFailure(`RDb creator keyId '${creator.keyId}' does not match public key`);
            }
        } catch {
            return validationFailure(`RDb creator '${creator.keyId}' public key is invalid`);
        }
    }

    return validationOk();
}

function creatorKeyLookup(creators: SchemaCreator[]): (keyId: KeyId) => Promise<PublicKey | undefined> {
    return async (keyId: KeyId) => {
        const creator = creators.find((c) => c.keyId === keyId);
        if (creator === undefined) return undefined;
        try {
            return deserializePublicKeyFromBase64(creator.publicKey);
        } catch {
            return undefined;
        }
    };
}

function hasAuthFields(payload: AddSchemaPayload | AddGroupPayload): boolean {
    return payload.author !== undefined || payload.signature !== undefined;
}

async function validateMembership(
    payload: AddSchemaPayload | AddGroupPayload,
    rdb: RDbOpHost,
    actionLabel: string,
): Promise<ValidationResult> {
    const creators = rdb.getCreators();
    const objectHash = rdb.getId();

    if (creators.length === 0) {
        if (hasAuthFields(payload)) {
            return validationFailure(`${actionLabel} must not carry author or signature when the RDb declares no creators`, { objectHash });
        }
        return validationOk();
    }

    if (payload.author === undefined || payload.signature === undefined) {
        return validationFailure(`${actionLabel} requires author and signature when the RDb declares creators`, { objectHash });
    }

    if (!rdb.isCreator(payload.author)) {
        return validationFailure(`${actionLabel} author '${payload.author}' is not an RDb creator`, { objectHash });
    }

    if (!await verifyPayloadSignature(payload as unknown as json.LiteralMap, creatorKeyLookup(creators))) {
        return validationFailure(`${actionLabel} signature from '${payload.author}' could not be verified`, { objectHash });
    }

    return validationOk();
}
