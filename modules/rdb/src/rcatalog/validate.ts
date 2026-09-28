// Format-level and position-independent validation for RCatalog payloads.
//
//   validateRCatalogPayloadFormat - json.Format + checks that need nothing but
//                                   the payload
//   validate_ops.ts               - signatures, parents, schemas at the pins

import { json } from "@hyper-hyper-space/hhs3_json";
import { validationFailure, validationOk, ValidationResult } from "@hyper-hyper-space/hhs3_mvt";

import { isValidName, isValidSchemaName, isValidTableRef, validatePredicate } from "../rschema/validate.js";
import {
    createRCatalogFormat, catalogReleaseFormat, catalogDeclareFormat,
    CreateRCatalogPayload, CatalogReleasePayload, CatalogDeclarePayload,
    CatalogGroupDef, CatalogReleaseBody, catalogGroupHash,
} from "./payload.js";
import { isValidSemver } from "./semver.js";

export function isValidCatalogName(name: string): boolean {
    return isValidSchemaName(name);
}

function isNonEmptySet(set: json.Set): boolean {
    return json.setSize(set) > 0;
}

// Undefined when valid, else a reason.
export function checkGroupDefFormat(def: CatalogGroupDef): string | undefined {
    if (!isValidName(def.name)) return `invalid group name '${def.name}'`;
    if (!isNonEmptySet(def.schemaVersion)) return `group '${def.name}' pins an empty schema version`;

    const bindings = def.bindings ?? {};
    const seenTargets = new Map<string, string>();
    for (const [alias, target] of Object.entries(bindings)) {
        if (!isValidName(alias)) return `group '${def.name}' has an invalid binding alias '${alias}'`;
        const prior = seenTargets.get(target);
        if (prior !== undefined) {
            return `group '${def.name}' bindings must be injective: aliases '${prior}' and '${alias}' both bind '${target}'`;
        }
        seenTargets.set(target, alias);
    }

    if (def.canDeploy !== undefined && !validatePredicate(def.canDeploy, 'object')) {
        return `group '${def.name}' has an invalid ALLOW DEPLOY IF predicate`;
    }
    for (const [alias, pred] of Object.entries(def.canObserve ?? {})) {
        if (!Object.prototype.hasOwnProperty.call(bindings, alias)) {
            return `group '${def.name}' gates observation of '${alias}', which is not a binding alias`;
        }
        if (!validatePredicate(pred, 'object')) {
            return `group '${def.name}' has an invalid ALLOW UPDATE REF predicate for '${alias}'`;
        }
    }

    if (def.idProvider !== undefined && !isValidTableRef(def.idProvider)) {
        return `group '${def.name}' idProvider '${def.idProvider}' is not a valid table reference`;
    }

    for (const [table, rows] of Object.entries(def.initialRows ?? {})) {
        if (!isValidName(table)) return `group '${def.name}' has an invalid initial row table '${table}'`;
        for (const [index, row] of rows.entries()) {
            for (const column of Object.keys(row.values)) {
                if (!isValidName(column)) return `group '${def.name}' row ${index} of '${table}' has an invalid column '${column}'`;
            }
            for (const [column, ref] of Object.entries(row.params ?? {})) {
                if (!isValidName(column)) return `group '${def.name}' row ${index} of '${table}' has an invalid column '${column}'`;
                if (Object.prototype.hasOwnProperty.call(row.values, column)) {
                    return `group '${def.name}' row ${index} of '${table}' sets column '${column}' both literally and from a param`;
                }
                if (!isValidName(ref.param)) return `group '${def.name}' row ${index} of '${table}' names an invalid param '${ref.param}'`;
            }
        }
    }

    return undefined;
}

function checkReleaseBodyFormat(body: CatalogReleaseBody): string | undefined {
    if (!isValidSemver(body.version)) return `invalid release version '${body.version}' (expected MAJOR.MINOR.PATCH)`;

    const addHashes = new Set<string>();
    for (const def of body.add ?? []) {
        const reason = checkGroupDefFormat(def);
        if (reason !== undefined) return reason;
        const hash = catalogGroupHash(def);
        if (addHashes.has(hash)) return `group '${def.name}' is added twice`;
        addHashes.add(hash);
    }

    for (const [hash, change] of Object.entries(body.changes ?? {})) {
        if (addHashes.has(hash)) return `group '${hash}' is added and changed in the same release`;
        if (!isNonEmptySet(change.version)) return `change of group '${hash}' names an empty version`;
    }

    const paramNames = new Set<string>();
    for (const decl of body.params ?? []) {
        if (!isValidName(decl.name)) return `invalid param name '${decl.name}'`;
        if (paramNames.has(decl.name)) return `param '${decl.name}' is declared twice`;
        paramNames.add(decl.name);
    }

    return undefined;
}

export function validateRCatalogPayloadFormat(payload: json.Literal): ValidationResult {
    if (typeof payload !== 'object' || payload === null || Array.isArray(payload)) {
        return validationFailure("RCatalog payload must be an object");
    }

    const action = (payload as json.LiteralMap)['action'];

    if (action === 'create') {
        if (!json.checkFormat(createRCatalogFormat, payload)) return validationFailure("RCatalog create payload format is invalid");
        const create = payload as CreateRCatalogPayload;
        if (!isValidCatalogName(create.name)) return validationFailure(`invalid catalog name '${create.name}'`);
        if (create.creators.length === 0) return validationFailure("RCatalog create payload must have at least one creator");
        if (create.changes !== undefined) return validationFailure("the catalog genesis cannot carry changes");
        const reason = checkReleaseBodyFormat(create);
        return reason === undefined ? validationOk() : validationFailure(`RCatalog genesis is invalid: ${reason}`);
    }

    if (action === 'release') {
        if (!json.checkFormat(catalogReleaseFormat, payload)) return validationFailure("RCatalog release payload format is invalid");
        const reason = checkReleaseBodyFormat(payload as CatalogReleasePayload);
        return reason === undefined ? validationOk() : validationFailure(`RCatalog release is invalid: ${reason}`);
    }

    if (action === 'declare') {
        if (!json.checkFormat(catalogDeclareFormat, payload)) return validationFailure("RCatalog declare payload format is invalid");
        if (!isNonEmptySet((payload as CatalogDeclarePayload).schemas)) return validationFailure("RCatalog declare names no schemas");
        return validationOk();
    }

    return validationFailure(`unknown RCatalog action '${String(action)}'`);
}
