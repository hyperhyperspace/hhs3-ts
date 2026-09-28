// Semantic (position-dependent) validation for RCatalog payloads, layered on
// top of the format checks in validate.ts.
//
//   create   - creators are self-certifying; the genesis is signed at the
//              empty position by one of them; its body follows the release
//              rules with no parents.
//   release  - signed by a creator at `at`; its version is greater than every
//              parent's (the maximal releases below `at`); every schema it
//              references is pinned by the genesis or declared below `at`;
//              added definitions and changes are checked against the schemas
//              at their versions; a merge sets every group its parents
//              disagree on.
//   declare  - signed by a creator at `at`.
//
// Schema-dependent checks read the schemas at the pinned versions. Those are
// the release's foreign deps, so the synchronizer only validates once they are
// present; a missing schema object here is an infrastructure error (throw).

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, KeyId, PublicKey, HashSuite } from "@hyper-hyper-space/hhs3_crypto";
import {
    RContext, Version, version,
    validationFailure, validationOk, ValidationResult, wrapValidationFailure,
} from "@hyper-hyper-space/hhs3_mvt";
import {
    verifyPayloadSignature, deserializePublicKeyFromBase64, computeKeyId, refVersionAtOrAbove,
} from "@hyper-hyper-space/hhs3_mvt";

import type { RSchema, RSchemaView } from "../rschema/interfaces.js";
import { RSCHEMA_TYPE_ID, splitTableRef, ColumnType } from "../rschema/payload.js";
import { columnValueValidReason, predicateReferencesAuthor } from "../rschema/validate.js";
import { validateProviderInsertIntegrity } from "../rtable/validate_ops.js";
import type { InsertRowPayload } from "../rtable/payload.js";
import { qualifiedTargetGroups } from "../rtable_group/validate_ops.js";

import {
    CreateRCatalogPayload, CatalogReleasePayload, CatalogDeclarePayload, CatalogReleaseBody,
    CatalogGroupDef, CatalogParamDecl, CatalogRowParam, SchemaCreator, catalogGroupHash,
} from "./payload.js";
import { validateRCatalogPayloadFormat } from "./validate.js";
import { CatalogIndex, ParentFold, ReleaseState, versionKey } from "./resolve.js";
import { compareSemver } from "./semver.js";

export type RCatalogOpHost = {
    getId(): B64Hash;
    getCreators(): SchemaCreator[];
    isCreator(keyId: KeyId): boolean;
    getContext(): RContext;
    getIndex(): Promise<CatalogIndex>;
};

export type RCatalogValidationContext =
    | { mode: 'create'; ctx: RContext }
    | { mode: 'op'; catalog: RCatalogOpHost; at: Version };

export async function validateRCatalogPayload(payload: json.Literal, context: RCatalogValidationContext): Promise<ValidationResult> {
    const formatResult = validateRCatalogPayloadFormat(payload);
    if (!formatResult.valid) return formatResult;

    const action = (payload as json.LiteralMap)['action'];

    if (context.mode === 'create') {
        if (action !== 'create') return validationFailure("RCatalog creation action must be 'create'");
        return validateCreate(payload as CreateRCatalogPayload, context.ctx);
    }

    const { catalog, at } = context;
    const objectHash = catalog.getId();

    if (action === 'release') {
        return wrapValidationFailure(
            'catalog release rejected',
            await validateRelease(payload as CatalogReleasePayload, catalog, at),
            objectHash,
        );
    }
    if (action === 'declare') {
        return validateDeclare(payload as CatalogDeclarePayload, catalog, at);
    }

    return validationFailure("create payload cannot be applied to an existing catalog", { objectHash });
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

function validateCreators(creators: SchemaCreator[], hashSuite: HashSuite): ValidationResult {
    const seen = new Set<KeyId>();
    for (const creator of creators) {
        if (seen.has(creator.keyId)) return validationFailure(`duplicate catalog creator '${creator.keyId}'`);
        seen.add(creator.keyId);
        try {
            const pk = deserializePublicKeyFromBase64(creator.publicKey);
            if (computeKeyId(pk, hashSuite) !== creator.keyId) {
                return validationFailure(`catalog creator keyId '${creator.keyId}' does not match public key`);
            }
        } catch {
            return validationFailure(`catalog creator '${creator.keyId}' public key is invalid`);
        }
    }
    return validationOk();
}

async function validateCreate(create: CreateRCatalogPayload, ctx: RContext): Promise<ValidationResult> {
    const creatorsResult = validateCreators(create.creators, ctx.getHashSuite());
    if (!creatorsResult.valid) return creatorsResult;

    if (!create.creators.some((c) => c.keyId === create.author)) {
        return validationFailure(`catalog genesis author '${create.author}' is not one of its creators`);
    }
    if (!await verifyPayloadSignature(create as unknown as json.LiteralMap, version(), creatorKeyLookup(create.creators))) {
        return validationFailure(`catalog genesis signature from '${create.author}' could not be verified`);
    }

    const referenced = new Set((create.add ?? []).map((def) => def.schemaRef));
    const empty: ParentFold = {
        groups: new Map(), conflicts: new Map(), defs: new Map(), addedIn: new Map(),
        params: new Map(), ancestors: new Set(),
    };
    return wrapValidationFailure('catalog genesis rejected', await validateReleaseBody(create, [], empty, referenced, ctx));
}

async function verifyOpSignature(
    payload: CatalogReleasePayload | CatalogDeclarePayload, catalog: RCatalogOpHost, at: Version, label: string,
): Promise<ValidationResult> {
    if (at.size === 0) return validationFailure(`a catalog ${label} must follow the genesis`);
    if (!catalog.isCreator(payload.author)) return validationFailure(`catalog ${label} author '${payload.author}' is not a creator`);
    if (!await verifyPayloadSignature(payload as unknown as json.LiteralMap, at, creatorKeyLookup(catalog.getCreators()))) {
        return validationFailure(`catalog ${label} signature from '${payload.author}' could not be verified`);
    }
    return validationOk();
}

async function validateDeclare(declare: CatalogDeclarePayload, catalog: RCatalogOpHost, at: Version): Promise<ValidationResult> {
    return verifyOpSignature(declare, catalog, at, 'declare');
}

async function validateRelease(release: CatalogReleasePayload, catalog: RCatalogOpHost, at: Version): Promise<ValidationResult> {
    const signatureResult = await verifyOpSignature(release, catalog, at, 'release');
    if (!signatureResult.valid) return signatureResult;

    const index = await catalog.getIndex();
    const parents = index.parentsOf(at);
    if (parents.length === 0) return validationFailure("a catalog release must have a parent release");

    const parentStates = parents.map((p) => index.releaseState(p));
    for (const parent of parentStates) {
        if (compareSemver(release.version, parent.version) <= 0) {
            return validationFailure(`release version '${release.version}' is not greater than parent '${parent.version}'`);
        }
    }

    const fold = index.foldParents(parents);
    const referenced = index.referencedSchemasAt(at);
    return validateReleaseBody(release, parentStates, fold, referenced, catalog.getContext());
}

async function loadSchema(ctx: RContext, schemaRef: B64Hash): Promise<RSchema | undefined> {
    const obj = await ctx.getObject(schemaRef);
    if (obj === undefined) {
        throw new Error(`RSchema '${schemaRef}' is not present in the replica`);
    }
    return obj.getType() === RSCHEMA_TYPE_ID ? obj as RSchema : undefined;
}

async function isMinimalCover(schema: RSchema, v: Version): Promise<boolean> {
    const scoped = await schema.getScopedDag();
    const cover = await scoped.findMinimalCover(v);
    return versionKey(cover) === versionKey(v);
}

// The shared release rules: the genesis runs them with no parents.
async function validateReleaseBody(
    body: CatalogReleaseBody,
    parents: ReleaseState[],
    fold: ParentFold,
    referenced: Set<B64Hash>,
    ctx: RContext,
): Promise<ValidationResult> {
    for (const decl of body.params ?? []) {
        if (fold.params.has(decl.name)) return validationFailure(`param '${decl.name}' is already declared`);
    }
    const params = new Map(fold.params);
    for (const decl of body.params ?? []) params.set(decl.name, decl);

    for (const def of body.add ?? []) {
        if (!referenced.has(def.schemaRef)) {
            return validationFailure(`group '${def.name}' references schema '${def.schemaRef}', which is not declared`);
        }
    }
    for (const [hash, change] of Object.entries(body.changes ?? {})) {
        if (!referenced.has(change.schema)) {
            return validationFailure(`change of group '${hash}' references schema '${change.schema}', which is not declared`);
        }
    }

    const names = new Set([...fold.defs.values()].map((def) => def.name));
    const addedSoFar = new Set<B64Hash>();
    for (const def of body.add ?? []) {
        const hash = catalogGroupHash(def);
        if (fold.defs.has(hash)) return validationFailure(`group '${def.name}' is already defined`);
        if (names.has(def.name)) return validationFailure(`group name '${def.name}' is already used`);
        names.add(def.name);

        const result = await validateGroupDef(def, fold, addedSoFar, params, ctx);
        if (!result.valid) return wrapValidationFailure(`group '${def.name}' is invalid`, result);
        addedSoFar.add(hash);
    }

    for (const [hash, change] of Object.entries(body.changes ?? {})) {
        const def = fold.defs.get(hash);
        if (def === undefined) return validationFailure(`change names unknown group '${hash}'`);
        const result = await validateChange(hash, def, change.schema, json.fromSet(change.version), parents, ctx);
        if (!result.valid) return result;
    }

    const changes = body.changes ?? {};
    const unresolved = [...fold.conflicts.keys()].filter((hash) => !Object.prototype.hasOwnProperty.call(changes, hash));
    if (unresolved.length > 0) {
        const labels = unresolved.map((hash) => fold.defs.get(hash)?.name ?? hash).sort();
        return validationFailure(`the parents disagree on the version of ${labels.map((l) => `'${l}'`).join(', ')}; the merge must set it`);
    }

    return validationOk();
}

async function validateGroupDef(
    def: CatalogGroupDef,
    fold: ParentFold,
    addedSoFar: Set<B64Hash>,
    params: Map<string, CatalogParamDecl>,
    ctx: RContext,
): Promise<ValidationResult> {
    const bindings = def.bindings ?? {};
    for (const [alias, target] of Object.entries(bindings)) {
        if (!fold.defs.has(target) && !addedSoFar.has(target)) {
            return validationFailure(`binding '${alias}' points at '${target}', which is not defined earlier`);
        }
    }

    if (def.canDeploy !== undefined && predicateReferencesAuthor(def.canDeploy) && def.idProvider === undefined) {
        return validationFailure("an ALLOW DEPLOY IF predicate over $author requires an identity provider");
    }

    const schema = await loadSchema(ctx, def.schemaRef);
    if (schema === undefined) return validationFailure(`'${def.schemaRef}' is not a schema`);

    const pin = version(...json.fromSet(def.schemaVersion));
    if (!await isMinimalCover(schema, pin)) return validationFailure("the pinned schema version is not a minimal cover");
    const view = await schema.getView(pin, pin);

    for (const alias of qualifiedTargetGroups(view)) {
        if (!Object.prototype.hasOwnProperty.call(bindings, alias)) {
            return validationFailure(`schema target group '${alias}' is not bound`);
        }
    }

    if (def.idProvider !== undefined) {
        const [alias, table] = splitTableRef(def.idProvider);
        if (alias === undefined) {
            if (!view.hasTable(table)) return validationFailure(`idProvider table '${table}' does not exist`);
            if (view.getIdProvider(table) === undefined) return validationFailure(`table '${table}' is not an identity provider`);
        } else if (!Object.prototype.hasOwnProperty.call(bindings, alias)) {
            return validationFailure(`idProvider group '${alias}' is not bound`);
        }
    }

    return validateRowTemplates(def, view, params, ctx.getHashSuite());
}

function paramFitsColumn(paramType: ColumnType, fn: CatalogRowParam['fn'], columnType: ColumnType): boolean {
    if (fn === 'publicKey') return paramType === 'identity' && columnType === 'string';
    if (paramType === 'identity') return columnType === 'identity' || columnType === 'string';
    return paramType === columnType;
}

function validateRowTemplates(
    def: CatalogGroupDef, view: RSchemaView, params: Map<string, CatalogParamDecl>, hashSuite: HashSuite,
): ValidationResult {
    for (const [table, rows] of Object.entries(def.initialRows ?? {})) {
        const tableDef = view.getTable(table);
        if (tableDef === undefined) return validationFailure(`initial row table '${table}' does not exist in the pinned schema`);
        const provider = view.getIdProvider(table);

        for (const [index, row] of rows.entries()) {
            const where = `row ${index} of '${table}'`;
            for (const [column, value] of Object.entries(row.values)) {
                const columnDef = tableDef.columns[column];
                if (columnDef === undefined) return validationFailure(`${where}: column '${column}' does not exist`);
                const reason = columnValueValidReason(value, columnDef);
                if (reason !== undefined) return validationFailure(`${where}: column '${column}' (${columnDef.type}): ${reason}`);
            }
            const rowParams = row.params ?? {};
            for (const [column, ref] of Object.entries(rowParams)) {
                const columnDef = tableDef.columns[column];
                if (columnDef === undefined) return validationFailure(`${where}: column '${column}' does not exist`);
                const decl = params.get(ref.param);
                if (decl === undefined) return validationFailure(`${where}: param ':${ref.param}' is not declared`);
                if (!paramFitsColumn(decl.type, ref.fn, columnDef.type)) {
                    return validationFailure(`${where}: param ':${ref.param}' (${decl.type}) does not fit column '${column}' (${columnDef.type})`);
                }
            }
            for (const [column, columnDef] of Object.entries(tableDef.columns)) {
                if (columnDef.nullable ?? false) continue;
                if (columnDef.default !== undefined) continue;
                if (row.values[column] === undefined && rowParams[column] === undefined) {
                    return validationFailure(`${where}: required column '${column}' is missing`);
                }
            }

            if (provider !== undefined) {
                const keyRef = rowParams[provider.keyIdColumn];
                const pkRef = rowParams[provider.publicKeyColumn];
                if (keyRef !== undefined || pkRef !== undefined) {
                    if (keyRef === undefined || pkRef === undefined
                        || keyRef.fn !== undefined || pkRef.fn !== 'publicKey' || keyRef.param !== pkRef.param
                        || params.get(keyRef.param)?.type !== 'identity') {
                        return validationFailure(`${where}: '${provider.keyIdColumn}' and '${provider.publicKeyColumn}' must come from the same identity param`);
                    }
                } else {
                    const insert: InsertRowPayload = { action: 'insert', rowId: '', uuid: '', values: row.values };
                    const integrity = validateProviderInsertIntegrity(insert, view, table, hashSuite);
                    if (!integrity.valid) return wrapValidationFailure(`${where} is not self-certifying`, integrity);
                }
            }
        }
    }
    return validationOk();
}

async function validateChange(
    hash: B64Hash,
    def: CatalogGroupDef,
    schemaRef: B64Hash,
    versionHashes: Iterable<B64Hash>,
    parents: ReleaseState[],
    ctx: RContext,
): Promise<ValidationResult> {
    const label = `'${def.name}'`;
    if (schemaRef !== def.schemaRef) return validationFailure(`change of group ${label} names schema '${schemaRef}', not its own`);

    const schema = await loadSchema(ctx, schemaRef);
    if (schema === undefined) return validationFailure(`'${schemaRef}' is not a schema`);

    const target = version(...versionHashes);
    if (!await isMinimalCover(schema, target)) return validationFailure(`the version of group ${label} is not a minimal cover`);

    const causal = await schema.getCausalDag();
    let differs = false;
    let present = false;
    for (const parent of parents) {
        const current = parent.groups.get(hash);
        if (current === undefined) continue;
        present = true;
        if (!await refVersionAtOrAbove(causal, target, new Set(current.version))) {
            return validationFailure(`the version of group ${label} is below its version in parent '${parent.version}'`);
        }
        if (versionKey(current.version) !== versionKey(target)) differs = true;
    }
    if (!present) return validationFailure(`group ${label} is not in any parent release`);
    if (!differs) return validationFailure(`the change of group ${label} does not change its version`);
    return validationOk();
}
