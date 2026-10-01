// Export: build the release file of one catalog release from a replica.
//
// The file carries the release's whole causal past:
//   - the catalog: every entry at or below the release (declares included);
//   - each schema that past references (genesis pins, pins of any release in
//     the past, and declares): its entries at or below the union of every
//     version a release in the past pins for it. Schema work above those
//     versions was never released, so it stays out. A declared schema no
//     release pins yet ships as its create alone.

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { Entry } from "@hyper-hyper-space/hhs3_dag";
import { json } from "@hyper-hyper-space/hhs3_json";
import type { RContext, RObject } from "@hyper-hyper-space/hhs3_mvt";
import { RCATALOG_TYPE_ID, RSCHEMA_TYPE_ID, type RCatalogImpl } from "@hyper-hyper-space/hhs3_rdb";

import {
    RELEASE_FILE_FORMAT, canonicalEntryOrder,
    type ReleaseEntry, type ReleaseFile, type ReleaseManifest, type ReleaseObject,
} from "./format.js";

export class ExportError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'ExportError';
    }
}

async function loadEntries(object: RObject): Promise<Map<B64Hash, Entry>> {
    const scoped = await object.getScopedDag();
    const entries = new Map<B64Hash, Entry>();
    for await (const entry of scoped.loadAllEntries()) entries.set(entry.hash, entry);
    return entries;
}

function prevsOf(entry: Entry): B64Hash[] {
    return [...json.fromSet(entry.header.prevEntryHashes)].sort();
}

// Every entry at or below `heads`.
function pastOf(entries: Map<B64Hash, Entry>, heads: Iterable<B64Hash>, what: string): Set<B64Hash> {
    const past = new Set<B64Hash>();
    const pending = [...heads];
    while (pending.length > 0) {
        const hash = pending.pop()!;
        if (past.has(hash)) continue;
        const entry = entries.get(hash);
        if (entry === undefined) throw new ExportError(`${what}: entry '${hash}' is not in the replica`);
        past.add(hash);
        for (const prev of prevsOf(entry)) pending.push(prev);
    }
    return past;
}

function objectOf(id: B64Hash, entries: Map<B64Hash, Entry>, past: Set<B64Hash>, what: string): ReleaseObject {
    const create = entries.get(id);
    if (create === undefined) throw new ExportError(`${what}: the create entry is not in the replica`);
    const released: ReleaseEntry[] = [];
    for (const [hash, entry] of entries) {
        if (hash === id || !past.has(hash)) continue;
        released.push({ hash, prevs: prevsOf(entry), payload: entry.payload });
    }
    return { id, create: create.payload as json.LiteralMap, entries: canonicalEntryOrder(released) };
}

export async function exportRelease(ctx: RContext, catalogId: B64Hash, release: B64Hash): Promise<ReleaseFile> {
    const catalogObject = await ctx.getObject(catalogId);
    if (catalogObject === undefined) throw new ExportError(`catalog '${catalogId}' is not in the replica`);
    if (catalogObject.getType() !== RCATALOG_TYPE_ID) {
        throw new ExportError(`object '${catalogId}' is a '${catalogObject.getType()}', not a catalog`);
    }
    const catalog = catalogObject as unknown as RCatalogImpl;
    const index = await catalog.getIndex();
    if (!index.hasEntry(release) || !index.isRelease(release)) {
        throw new ExportError(`'${release}' is not a release of catalog '${catalog.getName()}'`);
    }

    // Schemas and the versions the release's past pins for each.
    const pins = new Map<B64Hash, Set<B64Hash>>();
    for (const r of index.releasesAt([release])) {
        for (const group of index.releaseState(r).groups.values()) {
            const set = pins.get(group.schema) ?? new Set<B64Hash>();
            for (const h of group.version) set.add(h);
            pins.set(group.schema, set);
        }
    }
    for (const schemaId of index.referencedSchemasAt([release])) {
        if (!pins.has(schemaId)) pins.set(schemaId, new Set());
    }

    const objects: ReleaseObject[] = [];
    for (const schemaId of [...pins.keys()].sort()) {
        const schema = await ctx.getObject(schemaId);
        if (schema === undefined) throw new ExportError(`schema '${schemaId}' is not in the replica`);
        if (schema.getType() !== RSCHEMA_TYPE_ID) {
            throw new ExportError(`object '${schemaId}' is a '${schema.getType()}', not a schema`);
        }
        const what = `schema '${schemaId}'`;
        const entries = await loadEntries(schema);
        objects.push(objectOf(schemaId, entries, pastOf(entries, pins.get(schemaId)!, what), what));
    }

    const catalogWhat = `catalog '${catalog.getName()}'`;
    const catalogEntries = await loadEntries(catalogObject);
    objects.push(objectOf(catalogId, catalogEntries, pastOf(catalogEntries, [release], catalogWhat), catalogWhat));

    const state = index.releaseState(release);
    const manifest: ReleaseManifest = {
        name: index.getGenesis().name,
        catalog: catalogId,
        version: state.version,
        release,
        parents: [...state.parents].sort(),
    };
    if (state.note !== undefined) manifest.note = state.note;

    return { format: RELEASE_FILE_FORMAT, manifest, objects };
}
