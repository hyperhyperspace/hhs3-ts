// Semantic validation for RDb payloads, layered on format checks in validate.ts.
//
//   create          - creators are self-certifying; identity params are
//                     self-certifying. Validated without the catalog.
//   update-catalog  - when creators are declared: signed by a creator (when
//                     none are: no author/signature fields); names the
//                     create's catalog; its foreign dep {catalog, [release]}
//                     makes the release present before validation; the target
//                     is a release and every release deployed at `at` is
//                     strictly in its causal past (forward only); each
//                     supplied param is declared by the target, not already
//                     set at `at`, and of the declared type; afterwards every
//                     param the target declares is set.

import { json } from "@hyper-hyper-space/hhs3_json";
import { KeyId, PublicKey, HashSuite } from "@hyper-hyper-space/hhs3_crypto";
import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import {
    RContext, Version,
    validationFailure, validationOk, ValidationResult,
} from "@hyper-hyper-space/hhs3_mvt";
import { verifyPayloadSignature, deserializePublicKeyFromBase64, computeKeyId } from "@hyper-hyper-space/hhs3_mvt";

import type { CatalogIndex } from "../rcatalog/resolve.js";
import { CreateRDbPayload, UpdateCatalogPayload, SchemaCreator, ParamValue } from "./payload.js";
import { validateRDbPayloadFormat } from "./validate.js";
import { paramTypeFits } from "./instantiate.js";
import { RDbOps, deployedHistory, resolveParams } from "./resolve.js";

export type RDbOpHost = {
    getId(): B64Hash;
    getCatalogRef(): B64Hash;
    getCreators(): SchemaCreator[];
    isCreator(keyId: KeyId): boolean;
    getContext(): RContext;
    // the create and update-catalog ops at or below `at`
    opsAt(at: Version): Promise<RDbOps>;
    // the catalog's index, or undefined when the catalog is not present
    getCatalogIndex(): Promise<CatalogIndex | undefined>;
};

export type RDbValidationContext =
    | { mode: 'create'; ctx: RContext }
    | { mode: 'op'; rdb: RDbOpHost; at: Version };

export async function validateRDbPayload(payload: json.Literal, context: RDbValidationContext): Promise<ValidationResult> {
    const formatResult = validateRDbPayloadFormat(payload);
    if (!formatResult.valid) return formatResult;

    if (context.mode === 'create') {
        return validateCreate(payload as CreateRDbPayload, context.ctx);
    }

    const action = (payload as json.LiteralMap)['action'];
    if (action === 'update-catalog') {
        return validateUpdateCatalog(payload as UpdateCatalogPayload, context.rdb, context.at);
    }

    return validationFailure(`action '${String(action)}' is not an RDb op`, { objectHash: context.rdb.getId() });
}

function selfCertifying(keyId: KeyId, publicKey: string, hashSuite: HashSuite): boolean {
    try {
        return computeKeyId(deserializePublicKeyFromBase64(publicKey), hashSuite) === keyId;
    } catch {
        return false;
    }
}

function validateIdentityParams(params: { [name: string]: ParamValue } | undefined, hashSuite: HashSuite): ValidationResult {
    for (const [name, value] of Object.entries(params ?? {})) {
        if ('identity' in value && !selfCertifying(value.identity.keyId, value.identity.publicKey, hashSuite)) {
            return validationFailure(`identity param ':${name}' key id does not match its public key`);
        }
    }
    return validationOk();
}

function validateCreate(create: CreateRDbPayload, ctx: RContext): ValidationResult {
    const hashSuite = ctx.getHashSuite();
    const seen = new Set<KeyId>();

    for (const creator of create.creators ?? []) {
        if (seen.has(creator.keyId)) return validationFailure(`duplicate RDb creator '${creator.keyId}'`);
        seen.add(creator.keyId);
        if (!selfCertifying(creator.keyId, creator.publicKey, hashSuite)) {
            return validationFailure(`RDb creator keyId '${creator.keyId}' does not match its public key`);
        }
    }

    return validateIdentityParams(create.params, hashSuite);
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

async function validateUpdateCatalog(update: UpdateCatalogPayload, rdb: RDbOpHost, at: Version): Promise<ValidationResult> {
    const objectHash = rdb.getId();
    const creators = rdb.getCreators();

    if (creators.length === 0) {
        if (update.author !== undefined || update.signature !== undefined) {
            return validationFailure("update-catalog must not carry author or signature when the RDb declares no creators", { objectHash });
        }
    } else {
        if (update.author === undefined || update.signature === undefined) {
            return validationFailure("update-catalog requires author and signature when the RDb declares creators", { objectHash });
        }
        if (!rdb.isCreator(update.author)) {
            return validationFailure(`update-catalog author '${update.author}' is not an RDb creator`, { objectHash });
        }
        if (!await verifyPayloadSignature(update as unknown as json.LiteralMap, at, creatorKeyLookup(creators))) {
            return validationFailure(`update-catalog signature from '${update.author}' could not be verified`, { objectHash });
        }
    }

    if (update.catalog !== rdb.getCatalogRef()) {
        return validationFailure(`update-catalog names catalog '${update.catalog}', not the database's catalog`, { objectHash });
    }

    // the foreign dep guarantees the release is present before validation; a
    // missing catalog here is an infrastructure error (throw -> defer)
    const index = await rdb.getCatalogIndex();
    if (index === undefined) throw new Error(`RCatalog '${update.catalog}' is not present in the replica`);
    if (!index.hasEntry(update.release) || !index.isRelease(update.release)) {
        return validationFailure(`'${update.release}' is not a release of the catalog`, { objectHash });
    }

    const ops = await rdb.opsAt(at);
    for (const deployed of deployedHistory(ops)) {
        if (!index.hasEntry(deployed)) throw new Error(`catalog release '${deployed}' is not present in the replica`);
        if (!index.isReleaseBelow(deployed, update.release)) {
            return validationFailure(`update-catalog must move forward: release '${deployed}' is not below the target`, { objectHash });
        }
    }

    const target = index.releaseState(update.release);
    const already = resolveParams(ops);
    for (const [name, value] of Object.entries(update.params ?? {})) {
        const decl = target.params.get(name);
        if (decl === undefined) return validationFailure(`param ':${name}' is not declared by the target release`, { objectHash });
        if (Object.prototype.hasOwnProperty.call(already, name)) return validationFailure(`param ':${name}' is already set`, { objectHash });
        if (!paramTypeFits(decl, value)) return validationFailure(`param ':${name}' is not of type '${decl.type}'`, { objectHash });
    }

    const identityResult = validateIdentityParams(update.params, rdb.getContext().getHashSuite());
    if (!identityResult.valid) return identityResult;

    const after = { ...already, ...(update.params ?? {}) };
    for (const name of target.params.keys()) {
        if (!Object.prototype.hasOwnProperty.call(after, name)) {
            return validationFailure(`param ':${name}' declared by the target release has no value`, { objectHash });
        }
    }

    return validationOk();
}
