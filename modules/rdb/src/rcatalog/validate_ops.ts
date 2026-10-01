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
//              disagree on. An added FILES binds a known group, has a name no
//              group or FILES uses, and fits the group's schema at its version
//              in this release. Existing FILES are not re-checked: a later
//              change (or concurrent releases deployed together) may leave
//              one that no longer fits, and it becomes read-only.
//   declare  - signed by a creator at `at`.
//
// Schema-dependent checks read the schemas at the pinned versions. Those are
// the release's foreign deps, so the synchronizer only validates once they are
// present; a missing schema object here is an infrastructure error (throw).

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, KeyId, PublicKey, HashSuite } from "@hyper-hyper-space/hhs3_crypto";
import {
    RContext, Version, version,
    formatValidationFailure, validationFailure, validationOk, ValidationResult, wrapValidationFailure,
} from "@hyper-hyper-space/hhs3_mvt";
import {
    verifyPayloadSignature, deserializePublicKeyFromBase64, computeKeyId, refVersionAtOrAbove,
} from "@hyper-hyper-space/hhs3_mvt";

import type { RSchema, RSchemaView } from "../rschema/interfaces.js";
import { RSCHEMA_TYPE_ID, splitTableRef, ColumnType, type IdProvider, type TableDef } from "../rschema/payload.js";
import { columnValueValidReason, findAuthorRestriction, predicateReferencesAuthor } from "../rschema/validate.js";
import { filesSchemaReason } from "../rfiles/access.js";
import { validateProviderValues } from "../rtable/validate_ops.js";
import { qualifiedTargetReferences, unboundTargetReason } from "../rtable_group/validate_ops.js";

import {
    CreateRCatalogPayload, CatalogReleasePayload, CatalogDeclarePayload, CatalogReleaseBody,
    CatalogGroupDef, CatalogFilesDef, CatalogParamDecl, CatalogRowParam, CatalogRowTemplate, SchemaCreator,
    catalogGroupHash, catalogFilesHash,
} from "./payload.js";
import { validateRCatalogPayloadFormat } from "./validate.js";
import { CatalogIndex, ParentFold, ReleaseState, emptyParentFold, sortedVersion, versionKey } from "./resolve.js";
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
    return wrapValidationFailure('catalog genesis rejected', await validateReleaseBody(create, [], emptyParentFold(), referenced, ctx));
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

    const names = new Set([...fold.defs.values(), ...fold.files.values()].map((def) => def.name));
    const addedSoFar = new Set<B64Hash>();
    for (const def of body.add ?? []) {
        const hash = catalogGroupHash(def);
        if (fold.defs.has(hash)) return validationFailure(`group '${def.name}' is already defined`);
        if (names.has(def.name)) return validationFailure(`group name '${def.name}' is already used`);
        names.add(def.name);

        const result = await validateGroupDef(def, fold, addedSoFar, params, ctx);
        if (!result.valid) return wrapValidationFailure(`TABLEGROUP ${def.name}`, result);
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

    return validateFilesBody(body, fold, names, ctx);
}

// The FILES rules of a release, after its groups are checked. `names` holds
// every group name in the release's state.
async function validateFilesBody(
    body: CatalogReleaseBody, fold: ParentFold, names: Set<string>, ctx: RContext,
): Promise<ValidationResult> {
    const defs = new Map(fold.defs);
    const versions = new Map<B64Hash, B64Hash[]>();
    for (const [hash, group] of fold.groups) versions.set(hash, group.version);
    for (const def of body.add ?? []) {
        const hash = catalogGroupHash(def);
        defs.set(hash, def);
        versions.set(hash, sortedVersion(json.fromSet(def.schemaVersion)));
    }
    for (const [hash, change] of Object.entries(body.changes ?? {})) {
        versions.set(hash, sortedVersion(json.fromSet(change.version)));
    }

    const taken = new Set(names);
    for (const def of fold.files.values()) taken.add(def.name);

    for (const def of body.files ?? []) {
        if (fold.files.has(catalogFilesHash(def))) return validationFailure(`FILES '${def.name}' is already defined`);
        if (taken.has(def.name)) return validationFailure(`name '${def.name}' is already used by a TABLEGROUP or FILES`);
        taken.add(def.name);
        const result = await validateFilesDef(def, defs, versions, ctx);
        if (!result.valid) return wrapValidationFailure(`FILES ${def.name}`, result);
    }

    return validationOk();
}

async function validateFilesDef(
    def: CatalogFilesDef, defs: Map<B64Hash, CatalogGroupDef>, versions: Map<B64Hash, B64Hash[]>, ctx: RContext,
): Promise<ValidationResult> {
    const [alias, target] = Object.entries(def.bindings)[0];
    const group = defs.get(target);
    const groupVersion = versions.get(target);
    if (group === undefined || groupVersion === undefined) {
        return validationFailure(`BIND ${alias} points at '${target}', which is not a TABLEGROUP defined earlier`);
    }
    const schema = await loadSchema(ctx, group.schemaRef);
    if (schema === undefined) return validationFailure(`'${group.schemaRef}' is not a schema`);
    const at = version(...groupVersion);
    const reason = filesViewReason(def, await schema.getView(at, at));
    return reason === undefined ? validationOk() : validationFailure(reason);
}

// Why a FILES definition doesn't fit its bound group's schema at `view` (see
// filesSchemaReason).
export function filesViewReason(def: CatalogFilesDef, view: RSchemaView): string | undefined {
    return filesSchemaReason(def, view.getName(), (t) => view.getTable(t));
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

    const schema = await loadSchema(ctx, def.schemaRef);
    if (schema === undefined) return validationFailure(`'${def.schemaRef}' is not a schema`);

    const pin = version(...json.fromSet(def.schemaVersion));
    if (!await isMinimalCover(schema, pin)) return validationFailure("the pinned schema version is not a minimal cover");
    const view = await schema.getView(pin, pin);

    const viewReason = groupViewReason(def, schema.getName(), view);
    if (viewReason !== undefined) return validationFailure(viewReason);

    return validateRowTemplates(def, schema.getName(), view, params, ctx.getHashSuite());
}

// Why a group definition doesn't fit the schema at its pin: the schema names
// a group it doesn't bind, or its identity setup doesn't fit (see
// groupIdentityReason). Messages use C-SQL terms and name no file, so any
// front end can show them.
export function groupViewReason(def: CatalogGroupDef, schemaName: string, view: RSchemaView): string | undefined {
    const bindings = def.bindings ?? {};
    for (const [alias, reference] of qualifiedTargetReferences(view)) {
        if (!Object.prototype.hasOwnProperty.call(bindings, alias)) return unboundTargetReason(schemaName, alias, reference);
    }
    return groupIdentityReason(def, view);
}

// Why a group definition's identity setup doesn't fit the schema at `view`.
// With USING IDENTITIES, the table must be a provider in the schema, or a
// bound group's table. Without it, every write and UPDATE REF in the group is
// anonymous, so no gate or table rule may read $author: it could never pass. A
// declared ALLOW DEPLOY IF counts too, since the group then embeds no creator
// keys to verify deploys with. Checked on the pinned version when a group is
// added, and on the target version whenever a release changes it.
export function groupIdentityReason(def: CatalogGroupDef, view: RSchemaView): string | undefined {
    if (def.idProvider !== undefined) {
        const [alias, table] = splitTableRef(def.idProvider);
        if (alias === undefined) {
            if (!view.hasTable(table)) return `USING IDENTITIES ${table}: schema ${view.getName()} has no table ${table}`;
            if (view.getIdProvider(table) === undefined) return `USING IDENTITIES ${table}: ${table} isn't an IDENTITY PROVIDER table`;
        } else if (!Object.prototype.hasOwnProperty.call(def.bindings ?? {}, alias)) {
            return `USING IDENTITIES ${def.idProvider}: the TABLEGROUP doesn't BIND ${alias}`;
        }
        return undefined;
    }

    if (def.canDeploy !== undefined && predicateReferencesAuthor(def.canDeploy)) {
        return 'ALLOW DEPLOY IF reads $author, which needs USING IDENTITIES';
    }
    for (const [alias, gate] of Object.entries(def.canObserve ?? {})) {
        if (predicateReferencesAuthor(gate)) return `ALLOW UPDATE REF ${alias} IF reads $author, which needs USING IDENTITIES`;
    }
    const found = findAuthorRestriction(view);
    if (found !== undefined) {
        return `schema ${view.getName()}: ALLOW ${found.restriction.on} IF on ${found.table} reads $author, which needs USING IDENTITIES`;
    }
    return undefined;
}

export function rowsTableReason(table: string, schemaName: string): string {
    return `WITH ROWS fills ${table}, which schema ${schemaName} doesn't have`;
}

function paramFitsColumn(paramType: ColumnType, fn: CatalogRowParam['fn'], columnType: ColumnType): boolean {
    if (fn === 'publicKey') return paramType === 'identity' && columnType === 'string';
    if (paramType === 'identity') return columnType === 'identity' || columnType === 'string';
    return paramType === columnType;
}

function withArticle(word: string): string {
    return `${/^[aeiou]/i.test(word) ? 'an' : 'a'} ${word}`;
}

function paramMisfitReason(column: string, columnType: ColumnType, ref: CatalogRowParam, paramType: ColumnType): string {
    if (ref.fn === 'publicKey') {
        return paramType === 'identity'
            ? `${column} is ${columnType}, and publicKey(:${ref.param}) is a string`
            : `publicKey(:${ref.param}) needs an identity param, and :${ref.param} is ${withArticle(paramType)} param`;
    }
    return `${column} is ${columnType}, and :${ref.param} is ${withArticle(paramType)} param`;
}

// Why a WITH ROWS row doesn't fit its table. `index` is the row's position
// among its table's rows; messages count rows from 1.
export function rowTemplateReason(
    table: string,
    index: number,
    row: CatalogRowTemplate,
    tableDef: TableDef,
    provider: IdProvider | undefined,
    params: Map<string, CatalogParamDecl>,
    hashSuite: HashSuite,
): string | undefined {
    const where = `${table} row ${index + 1} in WITH ROWS`;
    for (const [column, value] of Object.entries(row.values)) {
        const columnDef = tableDef.columns[column];
        if (columnDef === undefined) return `${where} sets ${column}, which ${table} doesn't have`;
        const reason = columnValueValidReason(value, columnDef);
        if (reason !== undefined) return `${where}: ${column} (${columnDef.type}): ${reason}`;
    }
    const rowParams = row.params ?? {};
    for (const [column, ref] of Object.entries(rowParams)) {
        const columnDef = tableDef.columns[column];
        if (columnDef === undefined) return `${where} sets ${column}, which ${table} doesn't have`;
        const decl = params.get(ref.param);
        if (decl === undefined) return `${where}: param ':${ref.param}' is not declared`;
        if (!paramFitsColumn(decl.type, ref.fn, columnDef.type)) {
            return `${where}: ${paramMisfitReason(column, columnDef.type, ref, decl.type)}`;
        }
    }
    for (const [column, columnDef] of Object.entries(tableDef.columns)) {
        if (columnDef.nullable ?? false) continue;
        if (columnDef.default !== undefined) continue;
        if (row.values[column] === undefined && rowParams[column] === undefined) {
            return `${where} doesn't set ${column}, which is NOT NULL with no DEFAULT: set it in the row, make ${column} NULL, or give it a DEFAULT`;
        }
    }

    if (provider !== undefined) {
        const keyRef = rowParams[provider.keyIdColumn];
        const pkRef = rowParams[provider.publicKeyColumn];
        if (keyRef !== undefined || pkRef !== undefined) {
            if (keyRef === undefined || pkRef === undefined
                || keyRef.fn !== undefined || pkRef.fn !== 'publicKey' || keyRef.param !== pkRef.param
                || params.get(keyRef.param)?.type !== 'identity') {
                return `${where}: ${provider.keyIdColumn} and ${provider.publicKeyColumn} must be :p and publicKey(:p) of one identity param`;
            }
        } else {
            const integrity = validateProviderValues(row.values, provider, hashSuite);
            if (!integrity.valid) return `${where} is not self-certifying: ${formatValidationFailure(integrity.why)}`;
        }
    }
    return undefined;
}

function validateRowTemplates(
    def: CatalogGroupDef, schemaName: string, view: RSchemaView, params: Map<string, CatalogParamDecl>, hashSuite: HashSuite,
): ValidationResult {
    for (const [table, rows] of Object.entries(def.initialRows ?? {})) {
        const tableDef = view.getTable(table);
        if (tableDef === undefined) return validationFailure(rowsTableReason(table, schemaName));
        const provider = view.getIdProvider(table);

        for (const [index, row] of rows.entries()) {
            const reason = rowTemplateReason(table, index, row, tableDef, provider, params, hashSuite);
            if (reason !== undefined) return validationFailure(reason);
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

    const identityReason = groupIdentityReason(def, await schema.getView(target, target));
    if (identityReason !== undefined) return validationFailure(`TABLEGROUP ${def.name}: ${identityReason}`);
    return validationOk();
}
