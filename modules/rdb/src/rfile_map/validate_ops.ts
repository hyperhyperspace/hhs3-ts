// Positional validation for RFileMap ops.
//
//   ref-advance   monotonic, author admitted at the new version
//   add, remove   author admitted at the op's observed group version
//
// A remove needs no matching add: removing an absent element changes nothing.

import { json } from "@hyper-hyper-space/hhs3_json";
import {
    ScopedDag, Version, ValidationResult, validationFailure, validationOk, wrapValidationFailure,
} from "@hyper-hyper-space/hhs3_mvt";

import type { FilesAccessControl } from "../rfiles/access.js";
import { checkFileMapOp } from "./validate.js";

export async function validateFileMapOp(
    payload: json.Literal, at: Version, dag: ScopedDag, access: FilesAccessControl,
): Promise<ValidationResult> {
    const checked = checkFileMapOp(payload, access.groupId);
    if (!checked.valid) return { valid: false, why: checked.why };
    if (at.size === 0) return validationFailure("RFileMap ops must follow the create entry");

    const op = checked.op;
    if (op.action === 'ref-advance') {
        return wrapValidationFailure('RFileMap ref-advance rejected', await access.validateRefAdvance(dag, op.payload, at, []));
    }

    const observed = await access.observedVersion(dag, at);
    const admission = await access.admit(op.payload as unknown as json.LiteralMap, at, observed, []);
    return admission.valid ? validationOk() : wrapValidationFailure(`RFileMap ${op.action} rejected`, admission);
}
