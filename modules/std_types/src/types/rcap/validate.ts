import { json } from "@hyper-hyper-space/hhs3_json";
import { KeyId } from "@hyper-hyper-space/hhs3_crypto";

import { RContext, Version } from "@hyper-hyper-space/hhs3_mvt";

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

export async function validateRCapPayload(payload: json.Literal, context: RCapValidationContext): Promise<boolean> {
    if (typeof payload !== "object" || Array.isArray(payload)) return false;
    if (typeof payload["action"] !== "string") return false;

    const action = payload["action"];

    if (context.mode === "create") {
        if (action !== "create") return false;
        return validateCreate(payload, context.ctx);
    }

    if (action === "create") return false;

    switch (action) {
        case "add-identity": return validateAddIdentity(payload, context.cap, context.at);
        case "create-cap":   return validateCreateCap(payload, context.cap, context.at);
        case "delete-cap":   return validateDeleteCap(payload, context.cap, context.at);
        case "grant":        return validateGrant(payload, context.cap, context.at);
        case "revoke":       return validateRevoke(payload, context.cap, context.at);
        default: return false;
    }
}

async function validateCreate(payload: json.Literal, ctx: RContext): Promise<boolean> {
    if (!json.checkFormat(createRCapFormat, payload)) return false;

    const cp = payload as CreateRCapPayload;

    if (cp.creators.length === 0) return false;
    if (cp.creators.length !== cp.creatorKeys.length) return false;

    const hashSuite = ctx.getHashSuite();
    for (let i = 0; i < cp.creators.length; i++) {
        try {
            const pk = deserializePublicKeyFromBase64(cp.creatorKeys[i]);
            if (computeKeyId(pk, hashSuite) !== cp.creators[i]) return false;
        } catch {
            return false;
        }
    }

    const capNames = new Set(Object.keys(cp.initialCaps));
    for (const def of Object.values(cp.initialCaps)) {
        for (const mgr of def.managedBy) {
            if (mgr !== "creator" && !capNames.has(mgr)) return false;
        }
    }

    if (cp.enrollCapability !== undefined) {
        if (!capNames.has(cp.enrollCapability)) return false;
    }

    return true;
}

async function verifySignedPayload(payload: json.Literal, cap: RCap): Promise<boolean> {
    const keyLookup: KeyLookup = (keyId) => cap.lookupKey(keyId);
    return verifyPayloadSignature(payload as json.LiteralMap, keyLookup);
}

async function validateAddIdentity(payload: json.Literal, cap: RCap, at: Version): Promise<boolean> {
    if (!json.checkFormat(addIdentityFormat, payload)) return false;
    const p = payload as AddIdentityPayload;

    try {
        const pk = deserializePublicKeyFromBase64(p.publicKey);
        if (computeKeyId(pk, cap.getHashSuite()) !== p.keyId) return false;
    } catch {
        return false;
    }

    if (!await verifySignedPayload(payload, cap)) return false;

    const authorId = p.author as KeyId;
    if (!cap.isCreator(authorId)) {
        const view = await cap.getView(at, at);
        if (!await view.hasCapability(authorId, cap.getEnrollCapabilityName())) return false;
    }

    return true;
}

async function validateCreateCap(payload: json.Literal, cap: RCap, at: Version): Promise<boolean> {
    if (!json.checkFormat(createCapabilityFormat, payload)) return false;
    const p = payload as CreateCapabilityPayload;

    if (!await verifySignedPayload(payload, cap)) return false;
    if (!cap.isCreator(p.author as KeyId)) return false;

    const view = await cap.getView(at, at);
    if (await view.capabilityExists(p.capName)) return false;

    for (const mgr of p.managedBy) {
        if (mgr !== "creator" && !await view.capabilityExists(mgr)) return false;
    }

    return true;
}

async function validateDeleteCap(payload: json.Literal, cap: RCap, at: Version): Promise<boolean> {
    if (!json.checkFormat(deleteCapabilityFormat, payload)) return false;
    const p = payload as DeleteCapabilityPayload;

    if (!await verifySignedPayload(payload, cap)) return false;
    if (!cap.isCreator(p.author as KeyId)) return false;

    const view = await cap.getView(at, at);
    if (!await view.capabilityExists(p.capName)) return false;

    return true;
}

async function validateGrant(payload: json.Literal, cap: RCap, at: Version): Promise<boolean> {
    if (!json.checkFormat(grantFormat, payload)) return false;
    const p = payload as GrantPayload;

    if (!await verifySignedPayload(payload, cap)) return false;

    const authorId = p.author as KeyId;
    const view = await cap.getView(at, at);

    if (!await view.capabilityExists(p.capName)) return false;
    if (!await view.isIdentity(p.grantee)) return false;

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
        if (!authorized) return false;
    }

    const expected = Array.from(await view.currentCapCreationVersion(p.capName))
        .sort()
        .slice(0, MAX_CAP_ORIGINS);
    if (expected.length !== p.capOrigins.length) return false;
    for (let i = 0; i < expected.length; i++) {
        if (expected[i] !== p.capOrigins[i]) return false;
    }

    return true;
}

async function validateRevoke(payload: json.Literal, cap: RCap, at: Version): Promise<boolean> {
    if (!json.checkFormat(revokeFormat, payload)) return false;
    const p = payload as RevokePayload;

    if (!await verifySignedPayload(payload, cap)) return false;

    const authorId = p.author as KeyId;
    const view = await cap.getView(at, at);

    if (!await view.capabilityExists(p.capName)) return false;

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
        if (!authorized) return false;
    }

    return true;
}
