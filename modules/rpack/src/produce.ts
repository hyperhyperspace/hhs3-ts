// Producing a release: signing a draft in the released replica, then checking
// the result before anything is written.
//
//   1. create the new schemas and append each update, at the parents' pins
//   2. create the catalog (first release) or publish the release on the
//      parents, which declares the new schemas
//   3. export the release file, and verify its bytes in a fresh replica
//   4. fixpoint: the model read back from the new release must equal
//      target-catalog.sql's, and no stand-in key may appear in the file

import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { deserializePublicKeyFromBase64, version as versionOf } from "@hyper-hyper-space/hhs3_mvt";
import {
    catalogGroupHash, RCatalogImpl, RSchemaImpl,
    type CatalogFilesDef, type CatalogGroupChange, type CatalogGroupDef, type CatalogReleaseSpec, type SchemaCreator,
} from "@hyper-hyper-space/hhs3_rdb";

import { exportRelease } from "./export.js";
import { releaseFileName, serializeReleaseFile, type ReleaseFile } from "./format.js";
import { modelDifferences, type GroupModel } from "./model.js";
import type { ReleaseDraft } from "./draft.js";
import { SOURCE_FILE } from "./project.js";
import type { Released } from "./released.js";
import { verifyRelease } from "./verify.js";

export class ProduceError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'ProduceError';
    }
}

export type ProducedRelease = {
    hash: B64Hash;
    catalog: B64Hash;
    version: string;
    // editor-2.0.0-8b21d0aa
    name: string;
    file: ReleaseFile;
    text: string;
};

function publicCreators(creators: SchemaCreator[]) {
    return creators.map((c) => ({ keyId: c.keyId, publicKey: deserializePublicKeyFromBase64(c.publicKey) }));
}

export async function produceRelease(
    draft: ReleaseDraft,
    released: Released,
    key: OwnIdentity,
    options: { note?: string } = {},
): Promise<ProducedRelease> {
    if (draft.refusals.length > 0) throw new ProduceError(`the draft has refusals:\n${draft.refusals.map((r) => r.message).join('\n')}`);
    if (key.keyId !== draft.signer.keyId) throw new ProduceError(`the signing key is not rpack.json's key '${draft.signer.label}'`);
    const ctx = released.ctx;
    const base = draft.base;

    const schemaIds = new Map<string, B64Hash>();
    const pins = new Map<string, B64Hash[]>();
    for (const [name, schema] of base?.schemas ?? []) schemaIds.set(name, schema.id);
    for (const [name, pin] of draft.pins) if (pin.kind === 'entries') pins.set(name, pin.entries);

    for (const step of draft.schemas) {
        if (step.kind === 'create') {
            const payload = await RSchemaImpl.create({
                name: step.schema.name,
                version: step.version,
                creators: publicCreators(step.schema.creators),
                tables: [...step.schema.tables.values()],
                ...(step.schema.hashAlgorithm !== undefined ? { hashAlgorithm: step.schema.hashAlgorithm } : {}),
            });
            const object = await ctx.createObject(payload as unknown as json.LiteralMap);
            schemaIds.set(step.schema.name, object.getId());
            pins.set(step.schema.name, [object.getId()]);
        } else {
            const schema = base!.schemas.get(step.schema)!;
            const hash = await schema.object.updateSchema(step.rules.map((r) => r.rule), key, {
                version: step.version,
                at: versionOf(...schema.at),
            });
            pins.set(step.schema, [hash]);
        }
    }

    const defHashes = new Map<string, B64Hash>();
    for (const [name, group] of base?.groups ?? []) defHashes.set(name, group.hash);
    const defOf = (group: GroupModel): CatalogGroupDef => {
        const def: CatalogGroupDef = {
            name: group.name,
            seedSource: 'rdb',
            schemaRef: schemaIds.get(group.schema)!,
            schemaVersion: json.toSet(pins.get(group.schema)!),
        };
        const bindings = Object.entries(group.bindings ?? {});
        if (bindings.length > 0) def.bindings = Object.fromEntries(bindings.map(([alias, name]) => [alias, defHashes.get(name)!]));
        if (group.idProvider !== undefined) def.idProvider = group.idProvider;
        if (group.canDeploy !== undefined) def.canDeploy = group.canDeploy;
        if (group.canObserve !== undefined) def.canObserve = group.canObserve;
        if (group.initialRows !== undefined) def.initialRows = group.initialRows;
        return def;
    };
    const add: CatalogGroupDef[] = [];
    for (const group of draft.add) {
        const def = defOf(group);
        defHashes.set(group.name, catalogGroupHash(def));
        add.push(def);
    }
    const files: CatalogFilesDef[] = draft.addFiles.map((f) => ({
        name: f.name,
        bindings: { [f.alias]: defHashes.get(f.group)! },
        idProvider: f.idProvider,
        canWrite: f.canWrite,
    }));

    let catalogId: B64Hash;
    let release: B64Hash;
    if (draft.first) {
        const desired = draft.desired;
        const payload = await RCatalogImpl.create({
            name: desired.name,
            creators: publicCreators(desired.creators),
            author: key,
            version: draft.version,
            ...(add.length > 0 ? { add } : {}),
            ...(files.length > 0 ? { files } : {}),
            ...(draft.params.length > 0 ? { params: draft.params } : {}),
            ...(options.note !== undefined ? { note: options.note } : {}),
            ...(desired.seed !== undefined ? { seed: desired.seed } : {}),
            ...(desired.hashAlgorithm !== undefined ? { hashAlgorithm: desired.hashAlgorithm } : {}),
        });
        const object = await ctx.createObject(payload as unknown as json.LiteralMap);
        catalogId = object.getId();
        release = catalogId;
    } else {
        catalogId = base!.catalogId;
        const changes: { [defHash: string]: CatalogGroupChange } = {};
        for (const change of draft.changes) {
            changes[defHashes.get(change.group)!] = { schema: schemaIds.get(change.schema)!, version: json.toSet(pins.get(change.schema)!) };
        }
        const spec: CatalogReleaseSpec = { version: draft.version };
        if (Object.keys(changes).length > 0) spec.changes = changes;
        if (add.length > 0) spec.add = add;
        if (files.length > 0) spec.files = files;
        if (draft.params.length > 0) spec.params = draft.params;
        if (options.note !== undefined) spec.note = options.note;
        const catalog = (await ctx.getObject(catalogId)) as unknown as RCatalogImpl;
        release = (await catalog.publishRelease(spec, key, versionOf(...draft.parents.map((p) => p.hash)))).release;
    }
    await released.refresh(catalogId);

    const file = await exportRelease(ctx, catalogId, release);
    const text = serializeReleaseFile(file);
    const verified = await verifyRelease(text);
    if (!verified.ok) throw new ProduceError(`the release file doesn't verify: ${verified.problems.join('; ')}`);

    const after = await released.base([release]);
    const left = modelDifferences(after.model, draft.desired);
    if (left.length > 0) throw new ProduceError(`the release doesn't describe ${SOURCE_FILE} (a bug in rpack): ${left.join('; ')}`);
    for (const step of draft.schemas) {
        const name = step.kind === 'create' ? step.schema.name : step.schema;
        const versions = after.schemas.get(name)?.view.getVersions() ?? [];
        if (versions.length !== 1 || versions[0] !== step.version) {
            throw new ProduceError(`schema ${name} is at ${versions.join(', ') || 'no version'} in the release, not ${step.version} (a bug in rpack)`);
        }
    }
    const leaked = draft.standIns.find((s) => text.includes(s));
    if (leaked !== undefined) throw new ProduceError(`a stand-in key (${leaked.slice(0, 12)}...) is in the release (a bug in rpack)`);

    return { hash: release, catalog: catalogId, version: draft.version, name: releaseFileName(draft.catalog, draft.version, release).replace(/\.rpack$/, ''), file, text };
}
