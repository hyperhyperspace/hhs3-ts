import { json } from "@hyper-hyper-space/hhs3_json";
import { KeyId } from "@hyper-hyper-space/hhs3_crypto";

import {
    RContext, Version,
    validationFailure, validationOk, ValidationResult,
} from "@hyper-hyper-space/hhs3_mvt";

import { verifyPayloadSignature, deserializePublicKeyFromBase64, computeKeyId } from "../../authorship.js";
import type { KeyLookup } from "../../authorship.js";

import {
    createRCapFormat, CreateRCapPayload,
    addIdentityFormat, AddIdentityPayload,
    createCapabilityFormat, CreateCapabilityPayload,
    deleteCapabilityFormat, DeleteCapabilityPayload,
    grantFormat, GrantPayload, MAX_CAP_ORIGINS,
    revokeFormat, RevokePayload,
    CapPayload,
} from "./payload.js";

import type { RCap } from "./interfaces.js";

type CreateValidationContext = {
    mode: "create";
    ctx: RContext;
};

type OpValidationContext = {
    mode: "op";
    cap: RCap;
    at: Version;
};

export type RCapValidationContext = CreateValidationContext | OpValidationContext;

export async function validateRCapPayload(payload: json.Literal, context: RCapValidationContext): Promise<ValidationResult> {
    if (typeof payload !== "object" || payload === null || Array.isArray(payload)) return validationFailure("RCap payload must be an object");
    if (typeof payload["action"] !== "string") return validationFailure("RCap payload action must be a string");

    const action = payload["action"];

    if (context.mode === "create") {
        if (action !== "create") return validationFailure("RCap creation action must be 'create'");
        return validateCreate(payload, context.ctx);
    }

    if (action === "create") return validationFailure("create payload cannot be applied to an existing RCap");

    switch (action) {
        case "add-identity": return validateAddIdentity(payload, context.cap, context.at);
        case "create-cap":   return validateCreateCap(payload, context.cap, context.at);
        case "delete-cap":   return validateDeleteCap(payload, context.cap, context.at);
        case "grant":        return validateGrant(payload, context.cap, context.at);
        case "revoke":       return validateRevoke(payload, context.cap, context.at);
        default: return validationFailure(`unknown RCap action '${action}'`);
    }
}

async function validateCreate(payload: json.Literal, ctx: RContext): Promise<ValidationResult> {
    if (!json.checkFormat(createRCapFormat, payload)) return validationFailure("RCap create payload format is invalid");

    const cp = payload as CreateRCapPayload;

    if (cp.creators.length === 0) return validationFailure("RCap create must have at least one creator");
    if (cp.creators.length !== cp.creatorKeys.length) return validationFailure("RCap creators and creatorKeys lengths differ");

    const hashSuite = ctx.getHashSuite();
    for (let i = 0; i < cp.creators.length; i++) {
        try {
            const pk = deserializePublicKeyFromBase64(cp.creatorKeys[i]);
            if (computeKeyId(pk, hashSuite) !== cp.creators[i]) {
                return validationFailure(`creator keyId '${cp.creators[i]}' does not match creator public key`);
            }
        } catch {
            return validationFailure(`creator key '${cp.creators[i]}' is invalid`);
        }
    }

    const capNames = new Set(Object.keys(cp.initialCaps));
    for (const def of Object.values(cp.initialCaps)) {
        for (const mgr of def.managedBy) {
            if (mgr !== "creator" && !capNames.has(mgr)) return validationFailure(`initial capability manager '${mgr}' does not exist`);
        }
    }

    if (cp.enrollCapability !== undefined) {
        if (!capNames.has(cp.enrollCapability)) return validationFailure(`enroll capability '${cp.enrollCapability}' does not exist`);
    }

    return validationOk();
}

async function verifySignedPayload(payload: json.Literal, cap: RCap): Promise<boolean> {
    const keyLookup: KeyLookup = (keyId) => cap.lookupKey(keyId);
    return verifyPayloadSignature(payload as json.LiteralMap, keyLookup);
}

async function validateAddIdentity(payload: json.Literal, cap: RCap, at: Version): Promise<ValidationResult> {
    if (!json.checkFormat(addIdentityFormat, payload)) return validationFailure("add-identity payload format is invalid");
    const p = payload as AddIdentityPayload;

    try {
        const pk = deserializePublicKeyFromBase64(p.publicKey);
        if (computeKeyId(pk, cap.getHashSuite()) !== p.keyId) return validationFailure(`identity keyId '${p.keyId}' does not match public key`);
    } catch {
        return validationFailure(`identity public key for '${p.keyId}' is invalid`);
    }

    if (!await verifySignedPayload(payload, cap)) return validationFailure("add-identity signature could not be verified");

    const authorId = p.author as KeyId;
    if (!cap.isCreator(authorId)) {
        const view = await cap.getView(at, at);
        if (!await view.hasCapability(authorId, cap.getEnrollCapabilityName())) {
            return validationFailure(`author '${authorId}' lacks enroll capability`);
        }
    }

    return validationOk();
}

async function validateCreateCap(payload: json.Literal, cap: RCap, at: Version): Promise<ValidationResult> {
    if (!json.checkFormat(createCapabilityFormat, payload)) return validationFailure("create-cap payload format is invalid");
    const p = payload as CreateCapabilityPayload;

    if (!await verifySignedPayload(payload, cap)) return validationFailure("create-cap signature could not be verified");
    if (!cap.isCreator(p.author as KeyId)) return validationFailure(`author '${p.author}' is not a creator`);

    const view = await cap.getView(at, at);
    if (await view.capabilityExists(p.capName)) return validationFailure(`capability '${p.capName}' already exists`);

    for (const mgr of p.managedBy) {
        if (mgr !== "creator" && !await view.capabilityExists(mgr)) return validationFailure(`manager capability '${mgr}' does not exist`);
    }

    return validationOk();
}

async function validateDeleteCap(payload: json.Literal, cap: RCap, at: Version): Promise<ValidationResult> {
    if (!json.checkFormat(deleteCapabilityFormat, payload)) return validationFailure("delete-cap payload format is invalid");
    const p = payload as DeleteCapabilityPayload;

    if (!await verifySignedPayload(payload, cap)) return validationFailure("delete-cap signature could not be verified");
    if (!cap.isCreator(p.author as KeyId)) return validationFailure(`author '${p.author}' is not a creator`);

    const view = await cap.getView(at, at);
    if (!await view.capabilityExists(p.capName)) return validationFailure(`capability '${p.capName}' does not exist`);

    return validationOk();
}

async function validateGrant(payload: json.Literal, cap: RCap, at: Version): Promise<ValidationResult> {
    if (!json.checkFormat(grantFormat, payload)) return validationFailure("grant payload format is invalid");
    const p = payload as GrantPayload;

    if (!await verifySignedPayload(payload, cap)) return validationFailure("grant signature could not be verified");

    const authorId = p.author as KeyId;
    const view = await cap.getView(at, at);

    if (!await view.capabilityExists(p.capName)) return validationFailure(`capability '${p.capName}' does not exist`);
    if (!await view.isIdentity(p.grantee)) return validationFailure(`grantee '${p.grantee}' is not an identity`);

    if (!cap.isCreator(authorId)) {
        const managedBy = await view.getManagedBy(p.capName);
        let authorized = false;
        for (const mgr of managedBy) {
            if (mgr === "creator") continue;
            if (await view.hasCapability(authorId, mgr)) {
                authorized = true;
                break;
            }
        }
        if (!authorized) return validationFailure(`author '${authorId}' is not authorized to manage '${p.capName}'`);
    }

    const expected = Array.from(await view.currentCapCreationVersion(p.capName))
        .sort()
        .slice(0, MAX_CAP_ORIGINS);
    if (expected.length !== p.capOrigins.length) return validationFailure(`grant origins for '${p.capName}' do not match current capability version`);
    for (let i = 0; i < expected.length; i++) {
        if (expected[i] !== p.capOrigins[i]) return validationFailure(`grant origins for '${p.capName}' do not match current capability version`);
    }

    return validationOk();
}

async function validateRevoke(payload: json.Literal, cap: RCap, at: Version): Promise<ValidationResult> {
    if (!json.checkFormat(revokeFormat, payload)) return validationFailure("revoke payload format is invalid");
    const p = payload as RevokePayload;

    if (!await verifySignedPayload(payload, cap)) return validationFailure("revoke signature could not be verified");

    const authorId = p.author as KeyId;
    const view = await cap.getView(at, at);

    if (!await view.capabilityExists(p.capName)) return validationFailure(`capability '${p.capName}' does not exist`);

    if (!cap.isCreator(authorId)) {
        const managedBy = await view.getManagedBy(p.capName);
        let authorized = false;
        for (const mgr of managedBy) {
            if (mgr === "creator") continue;
            if (await view.hasCapability(authorId, mgr)) {
                authorized = true;
                break;
            }
        }
        if (!authorized) return validationFailure(`author '${authorId}' is not authorized to manage '${p.capName}'`);
    }

    return validationOk();
}
