// Install: apply a release file to a replica.
//
// The synchronizer's apply sequence without the network. For each object, in
// file order (schemas first, so the catalog's pins resolve): createObject for a
// missing root, after checking that its create hashes to the recorded id. Then
// for each entry not already present: recompute its hash from payload and
// prevs and reject a mismatch before touching the DAG, validatePayload at its
// prevs, applyPayload. Entries already present are skipped, so installing the
// same file twice, or an older release's file, changes nothing.
//
// A release file holds only schemas and one catalog, so any other object type
// is refused before anything is written.

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { dag, position } from "@hyper-hyper-space/hhs3_dag";
import type { json } from "@hyper-hyper-space/hhs3_json";
import {
    extractCreatePayloadType, formatValidationFailure, ValidationRejectedError,
    type RContext, type RObject, type ValidationResult,
} from "@hyper-hyper-space/hhs3_mvt";
import { RCATALOG_TYPE_ID, RSCHEMA_TYPE_ID, type RCatalogImpl } from "@hyper-hyper-space/hhs3_rdb";

import type { ReleaseFile, ReleaseObject } from "./format.js";

export class InstallError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'InstallError';
    }
}

export type InstallReport = {
    catalog: B64Hash;
    release: B64Hash;
    createdRoots: B64Hash[];
    appliedEntries: number;
    skippedEntries: number;
};

export type InstallOptions = {
    backendLabel?: string;
};

function describe(object: ReleaseObject): string {
    const type = extractCreatePayloadType(object.create);
    const name = object.create['name'];
    const kind = type === RCATALOG_TYPE_ID ? 'catalog' : type === RSCHEMA_TYPE_ID ? 'schema' : 'object';
    return typeof name === 'string' ? `${kind} '${name}' (#${object.id})` : `${kind} #${object.id}`;
}

function messageOf(err: unknown): string {
    if (err instanceof ValidationRejectedError) return formatValidationFailure(err.why);
    return err instanceof Error ? err.message : String(err);
}

// Refuses a file whose objects aren't schemas followed by the manifest's catalog.
function checkShape(file: ReleaseFile): void {
    const { objects, manifest } = file;
    if (objects.length === 0) throw new InstallError('the release file has no objects');
    const seen = new Set<B64Hash>();
    objects.forEach((object, i) => {
        if (seen.has(object.id)) throw new InstallError(`${describe(object)} appears twice`);
        seen.add(object.id);
        const type = extractCreatePayloadType(object.create);
        const last = i === objects.length - 1;
        if (last) {
            if (type !== RCATALOG_TYPE_ID) throw new InstallError(`the last object must be the catalog, found ${describe(object)}`);
            if (object.id !== manifest.catalog) {
                throw new InstallError(`the catalog object #${object.id} is not the manifest's catalog #${manifest.catalog}`);
            }
        } else if (type !== RSCHEMA_TYPE_ID) {
            throw new InstallError(`${describe(object)} has type '${String(type)}': a release file holds only schemas and one catalog`);
        }
    });
}

async function installObject(
    ctx: RContext, object: ReleaseObject, backendLabel: string | undefined, report: InstallReport,
): Promise<void> {
    const what = describe(object);
    const type = extractCreatePayloadType(object.create)!;
    const factory = await ctx.getRegistry().lookup(type);
    const id = await factory.computeRootObjectId(object.create, ctx);
    if (id !== object.id) throw new InstallError(`${what}: its create hashes to #${id}, not the recorded id`);

    let target: RObject | undefined = await ctx.getObject(id);
    if (target === undefined) {
        try {
            target = await ctx.createObject(object.create, backendLabel);
        } catch (err) {
            throw new InstallError(`${what}: its create was rejected: ${messageOf(err)}`);
        }
        report.createdRoots.push(id);
    }

    const scoped = await target.getScopedDag();
    for (const [j, entry] of object.entries.entries()) {
        const where = `${what} entry ${j} (#${entry.hash})`;
        if (await scoped.loadEntry(entry.hash) !== undefined) {
            report.skippedEntries += 1;
            continue;
        }
        for (const prev of entry.prevs) {
            if (await scoped.loadEntry(prev) === undefined) throw new InstallError(`${where}: its predecessor #${prev} is missing`);
        }

        const at = position(...entry.prevs);
        const computed = dag.createEntry(entry.payload, {}, at, ctx.getHashSuite()).hash;
        if (computed !== entry.hash) throw new InstallError(`${where}: hash mismatch (the payload and prevs hash to #${computed})`);

        let result: ValidationResult;
        try {
            result = await target.validatePayload(entry.payload as json.Literal, at);
        } catch (err) {
            throw new InstallError(`${where}: could not be validated: ${messageOf(err)}`);
        }
        if (!result.valid) throw new InstallError(`${where}: rejected: ${formatValidationFailure(result.why)}`);

        const applied = await target.applyPayload(entry.payload as json.Literal, at);
        if (applied !== entry.hash) throw new InstallError(`${where}: applied as #${applied}, not the recorded hash`);
        report.appliedEntries += 1;
    }
}

// The manifest's claims, checked against the installed catalog.
async function checkManifest(ctx: RContext, file: ReleaseFile): Promise<void> {
    const { manifest } = file;
    const catalogObject = await ctx.getObject(manifest.catalog);
    if (catalogObject === undefined || catalogObject.getType() !== RCATALOG_TYPE_ID) {
        throw new InstallError(`manifest: catalog #${manifest.catalog} is not in the replica`);
    }
    const index = await (catalogObject as unknown as RCatalogImpl).getIndex();
    if (index.getGenesis().name !== manifest.name) {
        throw new InstallError(`manifest: name '${manifest.name}' does not match the catalog name '${index.getGenesis().name}'`);
    }
    if (!index.hasEntry(manifest.release) || !index.isRelease(manifest.release)) {
        throw new InstallError(`manifest: release #${manifest.release} is not a release of the catalog`);
    }
    const state = index.releaseState(manifest.release);
    if (state.version !== manifest.version) {
        throw new InstallError(`manifest: version '${manifest.version}' does not match the release version '${state.version}'`);
    }
    if ([...state.parents].sort().join(',') !== [...manifest.parents].sort().join(',')) {
        throw new InstallError('manifest: the parents do not match the release parents');
    }
    if (state.note !== manifest.note) {
        throw new InstallError('manifest: the note does not match the release note');
    }
}

export async function installRelease(ctx: RContext, file: ReleaseFile, options: InstallOptions = {}): Promise<InstallReport> {
    checkShape(file);
    const report: InstallReport = {
        catalog: file.manifest.catalog,
        release: file.manifest.release,
        createdRoots: [],
        appliedEntries: 0,
        skippedEntries: 0,
    };
    for (const object of file.objects) await installObject(ctx, object, options.backendLabel, report);
    await checkManifest(ctx, file);
    return report;
}
