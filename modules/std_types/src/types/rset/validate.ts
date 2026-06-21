import { B64Hash, KeyId } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import type { NestingParent, Payload, Version } from "@hyper-hyper-space/hhs3_mvt";
import {
    isRefAdvancePayload, refAdvanceFormat, extractRefVersion, validateRefAdvanceMonotonicity,
    validationFailure, validationOk, ValidationResult, wrapValidationFailure,
} from "@hyper-hyper-space/hhs3_mvt";
import type { RefAdvancePayload } from "@hyper-hyper-space/hhs3_mvt";

import { verifyPayloadSignature, isAuthoredPayload, extractAuthor } from "../../authorship.js";

import {
    createSetFormat, CreateSetPayload,
    addElmtFormat, addElmtAuthoredFormat, AddElmtPayload,
    deleteElmtFormat, deleteElmtAuthoredFormat, DeleteElmtPayload,
    updateElmtFormat, UpdateElmtPayload,
} from "./payload.js";
import type { RSet } from "./interfaces.js";
import { hashElement } from "./hash.js";

type CreateValidationContext = {
    mode: "create";
    parent?: NestingParent;
};

type OpValidationContext = {
    mode: "op";
    set: RSet;
    at: Version;
};

export type RSetValidationContext = CreateValidationContext | OpValidationContext;

export async function validateRSetPayload(payload: json.Literal, context: RSetValidationContext): Promise<ValidationResult> {
    if (context.mode === "create") {
        return validateCreatePayload(payload, context.parent);
    }

    return validateOpPayload(payload, context.set, context.at);
}

async function validateCreatePayload(payload: json.Literal, parent?: NestingParent): Promise<ValidationResult> {
    if (!json.checkFormat(createSetFormat, payload)) {
        return validationFailure("RSet create payload format is invalid");
    }

    const createPayload = payload as CreateSetPayload;

    if (createPayload.parent !== undefined && parent !== undefined) {
        if (createPayload.parent !== parent.getId()) {
            return validationFailure("RSet create parent does not match nesting parent");
        }
    }

    if (createPayload.contentType === undefined && createPayload.acceptUpdateForDeleted !== undefined) {
        return validationFailure("acceptUpdateForDeleted only makes sense if contentType is present");
    }

    if (createPayload.contentType !== undefined && createPayload.acceptRedundantAdd !== undefined) {
        return validationFailure("acceptRedundantAdd only makes sense if contentType is not present");
    }

    if (createPayload.contentType !== undefined) {
        if (createPayload.initialElements.length > 0) {
            return validationFailure("initialElements must be empty if contentType is present");
        }
    }

    if (createPayload.supportBarrierAdd && createPayload.supportBarrierDelete) {
        return validationFailure("supportBarrierAdd and supportBarrierDelete are mutually exclusive");
    }

    if (createPayload.capabilityRef !== undefined) {
        const reqs = createPayload.capRequirements;
        if (reqs === undefined) return validationFailure("permissioned RSet must declare capability requirements");
        if (reqs.add === undefined && reqs.delete === undefined) return validationFailure("permissioned RSet must require add or delete capability");
    } else if (createPayload.capRequirements !== undefined) {
        return validationFailure("capRequirements only make sense when capabilityRef is present");
    }

    return validationOk();
}

async function validateOpPayload(payload: json.Literal, rset: RSet, at: Version): Promise<ValidationResult> {
    if (typeof payload !== "object" || payload === null || Array.isArray(payload)) return validationFailure("RSet payload must be an object");
    if (typeof payload["action"] !== "string") return validationFailure("RSet payload action must be a string");

    const action = payload["action"];
    switch (action) {
        case "add":
            return validateAddPayload(payload, rset, at);
        case "delete":
            return validateDeletePayload(payload, rset, at);
        case "update":
            return validateUpdatePayload(payload, rset, at);
        case "ref-advance":
            return validateRefAdvancePayload(payload, rset, at);
        default:
            return validationFailure(`unknown RSet action '${action}'`);
    }
}

async function validateAddPayload(payload: Payload, rset: RSet, at: Version): Promise<ValidationResult> {
    const format = rset.isPermissioned() ? addElmtAuthoredFormat : addElmtFormat;
    if (!json.checkFormat(format, payload)) return validationFailure("RSet add payload format is invalid");

    const addPayload = payload as AddElmtPayload;
    if (!rset.supportBarrierAdd() && json.hasKey(addPayload, "barrier")) return validationFailure("barrier add is not supported");
    if (!rset.acceptRedundantAdd()) {
        const view = await rset.getView(at, at);
        if (await view.hasByHash(hashElement(addPayload.element))) return validationFailure("element already exists in set");
    }

    if (rset.isPermissioned()) {
        return checkPayloadAuth(payload, rset, at, rset.capRequirementForAdd());
    }

    return validationOk();
}

async function validateDeletePayload(payload: Payload, rset: RSet, at: Version): Promise<ValidationResult> {
    const format = rset.isPermissioned() ? deleteElmtAuthoredFormat : deleteElmtFormat;
    if (!json.checkFormat(format, payload)) return validationFailure("RSet delete payload format is invalid");

    const deletePayload = payload as DeleteElmtPayload;
    if (!rset.supportBarrierDelete() && json.hasKey(deletePayload, "barrier")) return validationFailure("barrier delete is not supported");
    if (!rset.acceptRedundantDelete()) {
        const view = await rset.getView(at, at);
        if (!await view.hasByHash(deletePayload.elementHash)) return validationFailure(`element '${deletePayload.elementHash}' does not exist in set`);
    }

    if (rset.isPermissioned()) {
        return checkPayloadAuth(payload, rset, at, rset.capRequirementForDelete());
    }

    return validationOk();
}

async function validateRefAdvancePayload(payload: Payload, rset: RSet, at: Version): Promise<ValidationResult> {
    if (!rset.isPermissioned()) return validationFailure("ref-advance is only valid for permissioned sets");
    if (!json.checkFormat(refAdvanceFormat, payload, { strict: false })) return validationFailure("RSet ref-advance payload format is invalid");
    if (!isAuthoredPayload(payload)) return validationFailure("RSet ref-advance must be authored");

    const refPayload = payload as unknown as RefAdvancePayload;
    if (refPayload.refId !== rset.capabilityRef()) return validationFailure(`ref-advance ref '${refPayload.refId}' is not the set capability ref`);

    const rcap = await rset.loadRCap();
    if (rcap === undefined) return validationFailure("permissioned set capability object is not available");

    const newRefVersion = extractRefVersion(refPayload);
    const observerDag = await rset.getScopedDag();
    const referencedDag = await rcap.getCausalDag();
    if (!await validateRefAdvanceMonotonicity(observerDag, referencedDag, refPayload.refId, newRefVersion, at)) {
        return validationFailure("RSet ref-advance is not monotonic");
    }

    if (!await verifyPayloadSignature(payload as json.LiteralMap, (keyId) => rcap.lookupKey(keyId))) {
        return validationFailure("RSet ref-advance signature could not be verified");
    }

    const authorId = extractAuthor(payload) as KeyId;

    if (rset.refAdvanceCreators() && rcap.isCreator(authorId)) return validationOk();

    const rsetView = await rset.getView(at, at);
    const rcapVersion = await rsetView.resolveRefVersion(rset.capabilityRef()!);
    const rcapView = await rcap.getView(rcapVersion, rcapVersion);
    for (const cap of rset.refAdvanceCaps()) {
        if (await rcapView.hasCapability(authorId, cap)) return validationOk();
    }

    return validationFailure(`author '${authorId}' is not authorized to advance the capability ref`);
}

async function checkPayloadAuth(payload: Payload, rset: RSet, at: Version, capName?: string): Promise<ValidationResult> {
    const rcap = await rset.loadRCap();
    if (rcap === undefined) return validationFailure("permissioned set capability object is not available");

    if (!await verifyPayloadSignature(payload as json.LiteralMap, (keyId) => rcap.lookupKey(keyId))) {
        return validationFailure("RSet payload signature could not be verified");
    }

    if (capName !== undefined) {
        const authorId = extractAuthor(payload) as KeyId;
        const rsetView = await rset.getView(at, at);
        const rcapVersion = await rsetView.resolveRefVersion(rset.capabilityRef()!);
        const rcapView = await rcap.getView(rcapVersion, rcapVersion);
        if (!await rcapView.hasCapability(authorId, capName)) return validationFailure(`author '${authorId}' does not have capability '${capName}'`);
    }

    return validationOk();
}

async function validateUpdatePayload(payload: Payload, rset: RSet, at: Version): Promise<ValidationResult> {
    if (!json.checkFormat(updateElmtFormat, payload)) {
        return validationFailure("RSet update payload format is invalid");
    }

    if (rset.contentType() === undefined) {
        return validationFailure("RSet update requires contentType");
    }

    const updatePayload = payload as UpdateElmtPayload;

    if (!rset.acceptUpdateForDeleted()) {
        const view = await rset.getView(at, at);
        const hasElmt = await view.hasByHash(updatePayload.elementHash);
        if (!hasElmt) {
            return validationFailure(`element '${updatePayload.elementHash}' does not exist in set`);
        }
    }

    const contentType = rset.contentType();

    if (contentType !== undefined) {
        const innerFactory = await rset.getContext().getRegistry().lookup(contentType);
        const innerRObject = await rset.loadChildObject(innerFactory, updatePayload.elementHash);
        const innerResult = await innerRObject.validatePayload(updatePayload.updatePayload, at);
        if (!innerResult.valid) return wrapValidationFailure("nested element update rejected", innerResult, updatePayload.elementHash);
    }

    return validationOk();
}
