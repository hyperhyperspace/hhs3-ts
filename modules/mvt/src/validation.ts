import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";

export type ValidationFailure = {
    reason: string;
    objectHash?: B64Hash;
    parent?: ValidationFailure;
};

export type ValidationResult =
    | { valid: true }
    | { valid: false; why: ValidationFailure };

export function validationOk(): ValidationResult {
    return { valid: true };
}

export function validationFailure(
    reason: string,
    opts?: { objectHash?: B64Hash; parent?: ValidationFailure },
): ValidationResult {
    const why: ValidationFailure = { reason };
    if (opts?.objectHash !== undefined) why.objectHash = opts.objectHash;
    if (opts?.parent !== undefined) why.parent = opts.parent;
    return { valid: false, why };
}

export function wrapValidationFailure(
    reason: string,
    result: ValidationResult,
    objectHash?: B64Hash,
): ValidationResult {
    if (result.valid) return result;
    return validationFailure(reason, { objectHash, parent: result.why });
}

export function formatValidationFailure(why: ValidationFailure): string {
    const parts: string[] = [];
    let current: ValidationFailure | undefined = why;
    while (current !== undefined) {
        parts.push(current.objectHash === undefined
            ? current.reason
            : `${current.reason} (object ${current.objectHash})`);
        current = current.parent;
    }
    return parts.join(": ");
}

export class ValidationRejectedError extends Error {
    readonly why: ValidationFailure;

    constructor(message: string, why: ValidationFailure) {
        super(message);
        this.name = "ValidationRejectedError";
        this.why = why;
    }
}
