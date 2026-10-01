// Verify: check a release file offline, with no keys.
//
// Checks the format, then installs the file into a fresh in-memory replica,
// which recomputes every hash, validates every entry (signatures included)
// and checks the manifest's claims against the installed catalog. The replica
// hashes with SHA-256; a catalog declaring another hash algorithm isn't
// supported yet.

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { createBasicCrypto, HASH_SHA256 } from "@hyper-hyper-space/hhs3_crypto";
import { version, type RContext } from "@hyper-hyper-space/hhs3_mvt";
import {
    RBLOB_STORE_TYPE_ID, RCATALOG_TYPE_ID, RDB_TYPE_ID, RDEPLOY_GATE_TYPE_ID, RFILE_MAP_TYPE_ID, RSCHEMA_TYPE_ID, RTABLE_GROUP_TYPE_ID,
    rBlobStoreFactory, rCatalogFactory, rDbFactory, rDeployGateFactory, rFileMapFactory, rSchemaFactory, rTableGroupFactory,
    type RSchema,
} from "@hyper-hyper-space/hhs3_rdb";
import { MemDagBackend, Replica } from "@hyper-hyper-space/hhs3_replica";

import { checkReleaseFile, parseReleaseFile, releaseTag, type ReleaseFile, type ReleaseObject } from "./format.js";
import { installRelease } from "./install.js";

export type VerifiedSchema = {
    id: B64Hash;
    name: string;
    versions: string[];     // the schema's versions at the heads the file ships
    entries: number;        // entries shipped beyond the create
};

export type VerifySummary = {
    name: string;
    version: string;
    release: B64Hash;
    parents: B64Hash[];
    note?: string;
    schemas: VerifiedSchema[];
    catalog: { id: B64Hash; releases: number; declares: number };
};

export type VerifyReport = {
    ok: boolean;
    problems: string[];
    summary?: VerifySummary;
};

// A fresh in-memory replica with the Rdb types registered, hashing with
// SHA-256.
export function memoryReplica(): Replica {
    const crypto = createBasicCrypto();
    const hashSuite = crypto.hash(HASH_SHA256);
    const replica = new Replica({ crypto, hashSuite });
    replica.attachBackend('default', new MemDagBackend(hashSuite));
    replica.registerType(RDB_TYPE_ID, rDbFactory);
    replica.registerType(RSCHEMA_TYPE_ID, rSchemaFactory);
    replica.registerType(RCATALOG_TYPE_ID, rCatalogFactory);
    replica.registerType(RTABLE_GROUP_TYPE_ID, rTableGroupFactory);
    replica.registerType(RDEPLOY_GATE_TYPE_ID, rDeployGateFactory);
    replica.registerType(RBLOB_STORE_TYPE_ID, rBlobStoreFactory);
    replica.registerType(RFILE_MAP_TYPE_ID, rFileMapFactory);
    return replica;
}

// The entries of an object that no other entry of it builds on; the create
// when it ships no entries.
function headsOf(object: ReleaseObject): B64Hash[] {
    if (object.entries.length === 0) return [object.id];
    const below = new Set<B64Hash>();
    for (const entry of object.entries) for (const prev of entry.prevs) below.add(prev);
    return object.entries.map((e) => e.hash).filter((h) => !below.has(h)).sort();
}

async function summarize(ctx: RContext, file: ReleaseFile): Promise<VerifySummary> {
    const schemas: VerifiedSchema[] = [];
    for (const object of file.objects.slice(0, -1)) {
        const schema = (await ctx.getObject(object.id)) as unknown as RSchema;
        const view = await schema.getView(version(...headsOf(object)));
        schemas.push({ id: object.id, name: schema.getName(), versions: view.getVersions(), entries: object.entries.length });
    }
    const catalog = file.objects[file.objects.length - 1];
    const actions = catalog.entries.map((e) => (e.payload as { action?: unknown }).action);
    const { manifest } = file;
    const summary: VerifySummary = {
        name: manifest.name,
        version: manifest.version,
        release: manifest.release,
        parents: [...manifest.parents],
        schemas,
        catalog: {
            id: catalog.id,
            releases: 1 + actions.filter((a) => a === 'release').length,
            declares: actions.filter((a) => a === 'declare').length,
        },
    };
    if (manifest.note !== undefined) summary.note = manifest.note;
    return summary;
}

export async function verifyRelease(input: string | ReleaseFile): Promise<VerifyReport> {
    let file: ReleaseFile;
    try {
        file = typeof input === 'string' ? parseReleaseFile(input) : checkReleaseFile(input);
    } catch (err) {
        return { ok: false, problems: [err instanceof Error ? err.message : String(err)] };
    }

    const ctx = memoryReplica();
    try {
        await installRelease(ctx, file);
        return { ok: true, problems: [], summary: await summarize(ctx, file) };
    } catch (err) {
        return { ok: false, problems: [err instanceof Error ? err.message : String(err)] };
    } finally {
        await ctx.destroy();
    }
}

// A verify report as text lines, for command-line tools.
export function formatVerifyReport(report: VerifyReport): string[] {
    if (!report.ok || report.summary === undefined) return report.problems.map((p) => `problem: ${p}`);
    const s = report.summary;
    const lines = [
        `release  ${s.name} ${s.version} #${s.release} (tag ${releaseTag(s.release)})`,
        `parents  ${s.parents.length === 0 ? '(genesis)' : s.parents.map((p) => `#${p}`).join(', ')}`,
    ];
    if (s.note !== undefined) lines.push(`note     ${s.note}`);
    const count = (n: number, one: string, many: string) => `${n} ${n === 1 ? one : many}`;
    lines.push(`catalog  #${s.catalog.id}: ${count(s.catalog.releases, 'release', 'releases')}, ${count(s.catalog.declares, 'declare', 'declares')}`);
    for (const schema of s.schemas) {
        lines.push(`schema   ${schema.name} ${schema.versions.join(' + ')} #${schema.id}: ${count(schema.entries, 'entry', 'entries')} beyond the create`);
    }
    lines.push('ok');
    return lines;
}
