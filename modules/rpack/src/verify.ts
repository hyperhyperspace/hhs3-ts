// Verify: check a release file offline, with no keys.
//
// Checks the format, then installs the file into a fresh in-memory replica,
// which recomputes every hash, validates every entry (signatures included)
// and checks the manifest's claims against the installed catalog. The replica
// hashes with SHA-256; a catalog declaring another hash algorithm isn't
// supported yet.

import type { B64Hash, KeyId, OwnIdentity, PublicKey } from "@hyper-hyper-space/hhs3_crypto";
import { createBasicCrypto, createIdentity, HASH_SHA256, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { version, type RContext, type Version } from "@hyper-hyper-space/hhs3_mvt";
import {
    compareSemver, isValidSemver,
    RBLOB_STORE_TYPE_ID, RCATALOG_TYPE_ID, RDB_TYPE_ID, RDEPLOY_GATE_TYPE_ID, RFILE_MAP_TYPE_ID, RSCHEMA_TYPE_ID, RTABLE_GROUP_TYPE_ID,
    rBlobStoreFactory, rCatalogFactory, rDbFactory, rDeployGateFactory, rFileMapFactory, rSchemaFactory, rTableGroupFactory,
    RSchemaImpl, type RSchema,
} from "@hyper-hyper-space/hhs3_rdb";
import { MemDagBackend, Replica } from "@hyper-hyper-space/hhs3_replica";

import {
    checkAnyReleaseFile, isSchemaReleaseFile, parseAnyReleaseFile, releaseTag, schemaStepHash,
    type AnyReleaseFile, type ReleaseFile, type ReleaseObject, type SchemaReleaseFile, type SchemaReleaseStep,
} from "./format.js";
import { installRelease } from "./install.js";

export type VerifiedSchema = {
    id: B64Hash;
    name: string;
    versions: string[];     // the schema's versions at the heads the file ships
    entries: number;        // entries shipped beyond the create
};

export type VerifySummary = {
    kind?: 'catalog' | 'schema';
    name: string;
    version: string;
    release: B64Hash;
    parents: B64Hash[];
    note?: string;
    schemas: VerifiedSchema[];
    catalog: { id: B64Hash; releases: number; declares: number };
};

export type ReplayedSchemaPack = {
    schema: RSchemaImpl;
    // Pack step hash -> catalog/scratch RSchema entry hash.
    stepToEntry: Map<B64Hash, B64Hash>;
    // Pack step version -> step.
    versionToStep: Map<string, SchemaReleaseStep>;
    // Pack step version -> catalog/scratch RSchema entry hash.
    versionToEntry: Map<string, B64Hash>;
};

// Replays a schema pack's `steps` DAG from its genesis in `ctx`, preserving
// the pack's parent links. Validates step hashes, unique versions, topological
// order, non-empty changes, and semver monotonicity along `parents`. When
// `upToVersion` is given, only steps in the causal past of `upToVersion` (and
// `upToVersion` itself) are replayed.
export async function replaySchemaSteps(
    ctx: RContext,
    name: string,
    steps: SchemaReleaseStep[],
    signer: OwnIdentity,
    creators: { keyId: KeyId; publicKey: PublicKey }[] = [{ keyId: signer.keyId, publicKey: signer.publicKey }],
    upToVersion?: string,
): Promise<ReplayedSchemaPack> {
    if (steps.length === 0) throw new Error(`schema pack '${name}' has no steps`);

    const byHash = new Map<B64Hash, SchemaReleaseStep>();
    const byVersion = new Map<string, SchemaReleaseStep>();
    const seenHashes = new Set<B64Hash>();
    let genesis: SchemaReleaseStep | undefined;

    for (const [i, step] of steps.entries()) {
        if (!isValidSemver(step.version)) {
            throw new Error(`step ${i} ('${step.version}') does not have a valid semver`);
        }
        const expectedHash = schemaStepHash(name, step);
        if (step.hash !== expectedHash) {
            throw new Error(`step ${i} (${step.version}): hash '${step.hash}' does not match content hash '${expectedHash}'`);
        }
        if (byHash.has(step.hash)) {
            throw new Error(`duplicate step hash '${step.hash}'`);
        }
        if (byVersion.has(step.version)) {
            throw new Error(`duplicate step version '${step.version}' in schema pack '${name}'`);
        }
        if (step.parents.length === 0) {
            if (genesis !== undefined) throw new Error(`schema pack '${name}' has multiple genesis steps`);
            if (step.tables === undefined) throw new Error(`genesis step '${step.version}' is missing tables`);
            if (step.migration !== undefined && step.migration.length > 0) {
                throw new Error(`genesis step '${step.version}' cannot carry migration rules`);
            }
            genesis = step;
        } else {
            if (step.tables !== undefined) {
                throw new Error(`update step '${step.version}' cannot carry tables`);
            }
            const hasRules = step.migration !== undefined && step.migration.length > 0;
            const hasParams = step.params !== undefined && step.params.length > 0;
            if (!hasRules && !hasParams) {
                throw new Error(`update step '${step.version}' carries neither migration rules nor new parameters`);
            }
            for (const p of step.parents) {
                if (!seenHashes.has(p)) {
                    throw new Error(`step '${step.version}' references parent '${p}' that does not precede it in topological order`);
                }
                const parentStep = byHash.get(p)!;
                if (compareSemver(step.version, parentStep.version) <= 0) {
                    throw new Error(`step version '${step.version}' is not greater than parent '${parentStep.version}'`);
                }
            }
        }
        seenHashes.add(step.hash);
        byHash.set(step.hash, step);
        byVersion.set(step.version, step);
    }

    if (genesis === undefined) throw new Error(`schema pack '${name}' has no genesis step`);

    // Determine which steps to replay (all, or causal past of upToVersion).
    let included: Set<B64Hash>;
    if (upToVersion === undefined) {
        included = new Set(byHash.keys());
    } else {
        const target = byVersion.get(upToVersion);
        if (target === undefined) {
            throw new Error(`schema pack '${name}' has no version '${upToVersion}'`);
        }
        included = new Set<B64Hash>();
        const queue: B64Hash[] = [target.hash];
        while (queue.length > 0) {
            const h = queue.pop()!;
            if (included.has(h)) continue;
            included.add(h);
            const s = byHash.get(h)!;
            for (const p of s.parents) queue.push(p);
        }
    }

    const createPayload = await RSchemaImpl.create({
        name,
        version: genesis.version,
        creators,
        ...(genesis.params !== undefined && genesis.params.length > 0 ? { params: genesis.params } : {}),
        tables: genesis.tables!,
    });
    const schema = (await ctx.createObject(createPayload as unknown as json.LiteralMap)) as unknown as RSchemaImpl;

    const stepToEntry = new Map<B64Hash, B64Hash>([[genesis.hash, schema.getId()]]);
    const versionToEntry = new Map<string, B64Hash>([[genesis.version, schema.getId()]]);

    for (const step of steps) {
        if (step.hash === genesis.hash || !included.has(step.hash)) continue;
        const atHashes = step.parents.map((p) => stepToEntry.get(p)!);
        const entryHash = await schema.updateSchema(step.migration ?? [], signer, {
            version: step.version,
            ...(step.params !== undefined && step.params.length > 0 ? { params: step.params } : {}),
            ...(step.note !== undefined ? { note: step.note } : {}),
            at: version(...atHashes),
        });
        stepToEntry.set(step.hash, entryHash);
        versionToEntry.set(step.version, entryHash);
    }

    return { schema, stepToEntry, versionToStep: byVersion, versionToEntry };
}

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

export async function verifyRelease(input: string | AnyReleaseFile): Promise<VerifyReport> {
    let file: AnyReleaseFile;
    try {
        file = typeof input === 'string' ? parseAnyReleaseFile(input) : checkAnyReleaseFile(input);
    } catch (err) {
        return { ok: false, problems: [err instanceof Error ? err.message : String(err)] };
    }

    const ctx = memoryReplica();
    try {
        if (isSchemaReleaseFile(file)) {
            const summary = await verifySchemaFile(ctx, file);
            return { ok: true, problems: [], summary };
        }
        await installRelease(ctx, file);
        return { ok: true, problems: [], summary: await summarize(ctx, file) };
    } catch (err) {
        return { ok: false, problems: [err instanceof Error ? err.message : String(err)] };
    } finally {
        await ctx.destroy();
    }
}

async function verifySchemaFile(ctx: RContext, file: SchemaReleaseFile): Promise<VerifySummary> {
    const { manifest, steps } = file;
    const signer = await createIdentity(SIGNING_ED25519, ctx.getHashSuite());
    const replayed = await replaySchemaSteps(ctx, manifest.name, steps, signer);

    const releaseStep = steps.find((s) => s.hash === manifest.release);
    if (releaseStep === undefined) {
        throw new Error(`manifest release '${manifest.release}' is not in steps`);
    }
    if (releaseStep.version !== manifest.version) {
        throw new Error(`manifest version '${manifest.version}' does not match release step version '${releaseStep.version}'`);
    }
    if ([...releaseStep.parents].sort().join(',') !== [...manifest.parents].sort().join(',')) {
        throw new Error('manifest parents do not match release step parents');
    }
    if (releaseStep.note !== manifest.note) {
        throw new Error('manifest note does not match release step note');
    }

    const headEntry = replayed.stepToEntry.get(manifest.release)!;
    const view = await replayed.schema.getView(version(headEntry));
    const summary: VerifySummary = {
        kind: 'schema',
        name: manifest.name,
        version: manifest.version,
        release: manifest.release,
        parents: [...manifest.parents],
        schemas: [{
            id: replayed.schema.getId(),
            name: manifest.name,
            versions: view.getVersions(),
            entries: steps.length - 1,
        }],
        catalog: { id: '' as B64Hash, releases: 0, declares: 0 },
    };
    if (manifest.note !== undefined) summary.note = manifest.note;
    return summary;
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
    if (s.kind !== 'schema') {
        lines.push(`catalog  #${s.catalog.id}: ${count(s.catalog.releases, 'release', 'releases')}, ${count(s.catalog.declares, 'declare', 'declares')}`);
    }
    for (const schema of s.schemas) {
        lines.push(`schema   ${schema.name} ${schema.versions.join(' + ')} #${schema.id}: ${count(schema.entries, 'entry', 'entries')} beyond the create`);
    }
    lines.push('ok');
    return lines;
}
