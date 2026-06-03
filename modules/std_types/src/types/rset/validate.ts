import { B64Hash, KeyId } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import type { NestingParent, Payload, Version } from "@hyper-hyper-space/hhs3_mvt";
import { isRefAdvancePayload, refAdvanceFormat, extractRefVersion, validateRefAdvanceMonotonicity } from "@hyper-hyper-space/hhs3_mvt";
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

export async function validateRSetPayload(payload: json.Literal, context: RSetValidationContext): Promise<boolean> {
    if (context.mode === "create") {
        return validateCreatePayload(payload, context.parent);
    }

    return validateOpPayload(payload, context.set, context.at);
}

async function validateCreatePayload(payload: json.Literal, parent?: NestingParent): Promise<boolean> {
    if (!json.checkFormat(createSetFormat, payload)) {
        console.log("fmt");
        console.log(payload);
        return false;
    }

    const createPayload = payload as CreateSetPayload;

    if (createPayload.parent !== undefined && parent !== undefined) {
        if (createPayload.parent !== parent.getId()) {
            return false;
        }
    }

    if (createPayload.contentType === undefined && createPayload.acceptUpdateForDeleted !== undefined) {
        console.log("acceptUpdateForDeleted only makes sense if contentType is present");
        return false;
    }

    if (createPayload.contentType !== undefined && createPayload.acceptRedundantAdd !== undefined) {
        console.log("acceptRedundantAdd only makes sense if contentType is not present");
        return false;
    }

    if (createPayload.contentType !== undefined) {
        if (createPayload.initialElements.length > 0) {
            console.log("initialElements must be empty if contentType is present");
            return false;
        }
    }

    if (createPayload.supportBarrierAdd && createPayload.supportBarrierDelete) {
        return false;
    }

    if (createPayload.capabilityRef !== undefined) {
        const reqs = createPayload.capRequirements;
        if (reqs === undefined) return false;
        if (reqs.add === undefined && reqs.delete === undefined) return false;
    } else if (createPayload.capRequirements !== undefined) {
        return false;
    }

    return true;
}

async function validateOpPayload(payload: json.Literal, rset: RSet, at: Version): Promise<boolean> {
    if (typeof payload !== "object" || Array.isArray(payload)) return false;
    if (typeof payload["action"] !== "string") return false;

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
            return false;
    }
}

async function validateAddPayload(payload: Payload, rset: RSet, at: Version): Promise<boolean> {
    const format = rset.isPermissioned() ? addElmtAuthoredFormat : addElmtFormat;
    if (!json.checkFormat(format, payload)) return false;

    const addPayload = payload as AddElmtPayload;
    if (!rset.supportBarrierAdd() && json.hasKey(addPayload, "barrier")) return false;
    if (!rset.acceptRedundantAdd()) {
        const view = await rset.getView(at, at);
        if (await view.hasByHash(hashElement(addPayload.element))) return false;
    }

    if (rset.isPermissioned()) {
        return checkPayloadAuth(payload, rset, at, rset.capRequirementForAdd());
    }

    return true;
}

async function validateDeletePayload(payload: Payload, rset: RSet, at: Version): Promise<boolean> {
    const format = rset.isPermissioned() ? deleteElmtAuthoredFormat : deleteElmtFormat;
    if (!json.checkFormat(format, payload)) return false;

    const deletePayload = payload as DeleteElmtPayload;
    if (!rset.supportBarrierDelete() && json.hasKey(deletePayload, "barrier")) return false;
    if (!rset.acceptRedundantDelete()) {
        const view = await rset.getView(at, at);
        if (!await view.hasByHash(deletePayload.elementHash)) return false;
    }

    if (rset.isPermissioned()) {
        return checkPayloadAuth(payload, rset, at, rset.capRequirementForDelete());
    }

    return true;
}

async function validateRefAdvancePayload(payload: Payload, rset: RSet, at: Version): Promise<boolean> {
    if (!rset.isPermissioned()) return false;
    if (!json.checkFormat(refAdvanceFormat, payload, { strict: false })) return false;
    if (!isAuthoredPayload(payload)) return false;

    const refPayload = payload as unknown as RefAdvancePayload;
    if (refPayload.refId !== rset.capabilityRef()) return false;

    const newRefVersion = extractRefVersion(refPayload);
    const observerDag = await rset.getScopedDag();
    const referencedDag = await rset.getContext().getDag(refPayload.refId);
    if (referencedDag === undefined) return false;
    if (!await validateRefAdvanceMonotonicity(observerDag, referencedDag, refPayload.refId, newRefVersion, at)) return false;

    const rcap = await rset.loadRCap();
    if (rcap === undefined) return false;

    if (!await verifyPayloadSignature(payload as json.LiteralMap, (keyId) => rcap.lookupKey(keyId))) return false;

    const authorId = extractAuthor(payload) as KeyId;

    if (rset.refAdvanceCreators() && rcap.isCreator(authorId)) return true;

    const rsetView = await rset.getView(at, at);
    const rcapVersion = await rsetView.resolveRefVersion(rset.capabilityRef()!);
    const rcapView = await rcap.getView(rcapVersion, rcapVersion);
    for (const cap of rset.refAdvanceCaps()) {
        if (await rcapView.hasCapability(authorId, cap)) return true;
    }

    return false;
}

async function checkPayloadAuth(payload: Payload, rset: RSet, at: Version, capName?: string): Promise<boolean> {
    const rcap = await rset.loadRCap();
    if (rcap === undefined) return false;

    if (!await verifyPayloadSignature(payload as json.LiteralMap, (keyId) => rcap.lookupKey(keyId))) return false;

    if (capName !== undefined) {
        const authorId = extractAuthor(payload) as KeyId;
        const rsetView = await rset.getView(at, at);
        const rcapVersion = await rsetView.resolveRefVersion(rset.capabilityRef()!);
        const rcapView = await rcap.getView(rcapVersion, rcapVersion);
        if (!await rcapView.hasCapability(authorId, capName)) return false;
    }

    return true;
}

async function validateUpdatePayload(payload: Payload, rset: RSet, at: Version): Promise<boolean> {
    if (!json.checkFormat(updateElmtFormat, payload)) {
        return false;
    }

    if (rset.contentType() === undefined) {
        return false;
    }

    const updatePayload = payload as UpdateElmtPayload;

    if (!rset.acceptUpdateForDeleted()) {
        const view = await rset.getView(at, at);
        const hasElmt = await view.hasByHash(updatePayload.elementHash);
        if (!hasElmt) {
            return false;
        }
    }

    const contentType = rset.contentType();

    if (contentType !== undefined) {
        const innerFactory = await rset.getContext().getRegistry().lookup(contentType);
        const innerRObject = await rset.loadChildObject(innerFactory, updatePayload.elementHash);
        if (!await innerRObject.validatePayload(updatePayload.updatePayload, at)) {
            return false;
        }
    }

    return true;
}
