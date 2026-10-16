// Drafting a release: from the parents' merged state, through
// upgrade-manual.sql, to target-catalog.sql. Pure and keyless: the draft is
// what `rpack build` writes and what `rpack release` signs.

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { createBasicCrypto, createIdentity, HASH_SHA256, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { version as versionOf } from "@hyper-hyper-space/hhs3_mvt";
import {
    applyMigrationRules, compareSemver, isValidSemver, parseSemver,
    type CatalogParamDecl, type ColumnType, type MigrationRule, type SchemaParamDecl, type TableDef,
} from "@hyper-hyper-space/hhs3_rdb";

import { diffCatalog, type CatalogChange, type Pin } from "./catalog_diff.js";
import type { SchemaReleaseStep } from "./format.js";
import type { DevKey } from "./keys.js";
import { creatorsKey, paramDifferences, tableDifferences, type CatalogModel, type FilesModel, type GroupModel, type SchemaModel } from "./model.js";
import { describeRelease, type BaseSchema, type BaseState, type Released, type ReleaseInfo } from "./released.js";
import { SOURCE_FILE, UPGRADE_MANUAL_FILE } from "./project.js";
import { diffSchemaParams, diffTables, isBreaking, type Refusal } from "./schema_diff.js";
import { isBlankSql, readNext, SourceError, unitKey, type ReadSource, type Where } from "./source.js";
import { memoryReplica, replaySchemaSteps } from "./verify.js";

export type DraftRule = { rule: MigrationRule; where?: Where };

// `version` is the VERSION target-catalog.sql states for the schema, or the
// release's own.
export type SchemaStep =
    | { kind: 'create'; schema: SchemaModel; version: string; stepHash?: B64Hash }
    | {
        kind: 'update';
        schema: string;
        rules: DraftRule[];
        params?: SchemaParamDecl[];
        version: string;
        note?: string;
        stepHash?: B64Hash;
        parentStepHashes?: B64Hash[];
        at?: B64Hash[];
    };

export type ReleaseDraft = {
    catalog: string;
    version: string;
    parents: ReleaseInfo[];
    first: boolean;
    signer: DevKey;
    schemas: SchemaStep[];
    pins: Map<string, Pin>;
    // New groups, each after the groups it binds; every group for the first
    // release, in target-catalog.sql's order.
    add: GroupModel[];
    // New FILES, by name; every FILES for the first release, in source order.
    addFiles: FilesModel[];
    changes: CatalogChange[];
    params: CatalogParamDecl[];
    warnings: string[];
    refusals: Refusal[];
    desired: CatalogModel;
    base?: BaseState;
    standIns: string[];
};

export type DraftInputs = {
    catalog: string;
    version: string;
    parents: ReleaseInfo[];
    released: Released;
    source: ReadSource;
    nextText: string | undefined;
    signer: DevKey;
};

export class DraftError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'DraftError';
    }
}

function copyTables(tables: Map<string, TableDef>): Map<string, TableDef> {
    return new Map(tables);
}

function appendNewSchemaSteps(plan: ReleaseDraft, schema: SchemaModel, version: string): void {
    if (schema.origin?.kind === 'imported') {
        const steps = schema.origin.steps;
        const genesis = steps.find((s) => s.parents.length === 0)!;
        const genesisModel: SchemaModel = {
            name: schema.name,
            creators: schema.creators,
            ...(schema.hashAlgorithm !== undefined ? { hashAlgorithm: schema.hashAlgorithm } : {}),
            params: new Map((genesis.params ?? []).map((p) => [p.name, p])),
            tables: new Map((genesis.tables ?? []).map((t) => [t.name, t])),
        };
        plan.schemas.push({ kind: 'create', schema: genesisModel, version: genesis.version, stepHash: genesis.hash });
        for (const step of steps) {
            if (step.hash === genesis.hash) continue;
            plan.schemas.push({
                kind: 'update',
                schema: schema.name,
                rules: (step.migration ?? []).map((rule) => ({ rule })),
                ...(step.params !== undefined && step.params.length > 0 ? { params: step.params } : {}),
                version: step.version,
                ...(step.note !== undefined ? { note: step.note } : {}),
                stepHash: step.hash,
                parentStepHashes: [...step.parents],
            });
        }
        plan.pins.set(schema.name, steps.length === 1 ? { kind: 'create' } : { kind: 'update' });
        return;
    }
    plan.schemas.push({ kind: 'create', schema, version });
    plan.pins.set(schema.name, { kind: 'create' });
}

// Finds which pack steps are mirrored in `baseSchema.object` by matching the
// step's unique `version` string and confirming by resolved content (`tables`
// and `params`).
async function findMirroredPackSteps(
    baseSchema: BaseSchema,
    steps: SchemaReleaseStep[],
): Promise<Map<B64Hash, B64Hash[]>> {
    const replica = memoryReplica();
    try {
        const standIn = await createIdentity(SIGNING_ED25519, createBasicCrypto().hash(HASH_SHA256));
        const replayed = await replaySchemaSteps(replica, baseSchema.name, steps, standIn);

        const entriesByVersion = new Map<string, B64Hash[]>();
        const dag = await baseSchema.object.getScopedDag();
        for await (const entry of dag.loadAllEntries()) {
            const p = entry.payload as json.LiteralMap;
            const v = p['version'];
            if (typeof v === 'string') {
                const list = entriesByVersion.get(v) ?? [];
                list.push(entry.hash);
                entriesByVersion.set(v, list);
            }
        }

        const mirrored = new Map<B64Hash, B64Hash[]>();
        for (const step of steps) {
            const candidates = entriesByVersion.get(step.version);
            if (candidates === undefined || candidates.length === 0) continue;
            const catView = await baseSchema.object.getView(versionOf(...candidates));
            const packEntry = replayed.stepToEntry.get(step.hash)!;
            const packView = await replayed.schema.getView(versionOf(packEntry));

            const catTables = new Map<string, TableDef>(catView.getTableNames().map((t) => [t, catView.getTable(t)!]));
            const packTables = new Map<string, TableDef>(packView.getTableNames().map((t) => [t, packView.getTable(t)!]));
            const catParams = new Map<string, SchemaParamDecl>(catView.getParams().map((p) => [p.name, p]));
            const packParams = new Map<string, SchemaParamDecl>(packView.getParams().map((p) => [p.name, p]));

            if (tableDifferences(baseSchema.name, catTables, packTables).length === 0
                && paramDifferences(baseSchema.name, catParams, packParams).length === 0) {
                mirrored.set(step.hash, [...candidates].sort());
            }
        }
        return mirrored;
    } finally {
        await replica.destroy();
    }
}

function stepAncestorsOf(steps: SchemaReleaseStep[], targetHash: B64Hash): Set<B64Hash> {
    const byHash = new Map(steps.map((s) => [s.hash, s]));
    const ancestors = new Set<B64Hash>();
    const queue = [...(byHash.get(targetHash)?.parents ?? [])];
    while (queue.length > 0) {
        const h = queue.pop()!;
        if (ancestors.has(h)) continue;
        ancestors.add(h);
        for (const p of byHash.get(h)?.parents ?? []) queue.push(p);
    }
    return ancestors;
}

export async function draftRelease(inputs: DraftInputs): Promise<ReleaseDraft> {
    const { version, parents, released, source, signer } = inputs;
    const desired = source.model;
    const plan: ReleaseDraft = {
        catalog: inputs.catalog,
        version,
        parents,
        first: parents.length === 0,
        signer,
        schemas: [],
        pins: new Map(),
        add: [],
        addFiles: [],
        changes: [],
        params: [],
        warnings: [],
        refusals: [],
        desired,
        standIns: source.standIns,
    };
    const refuse = (message: string, extra: { hint?: string; where?: Where } = {}) => plan.refusals.push({ message, ...extra });
    const schemaVersion = (name: string) => source.schemaVersions.get(name) ?? version;

    if (!isValidSemver(version)) refuse(`'${version}' is not a version (major.minor.patch)`);
    for (const parent of parents) {
        if (isValidSemver(version) && compareSemver(version, parent.version) <= 0) {
            refuse(`${version} is not above its parent ${describeRelease(parent)}`);
        }
    }
    const same = released.releases().filter((r) => r.version === version);
    if (same.length > 0) refuse(`release ${version} already exists (${same.map((r) => r.tag).join(', ')})`);
    if (desired.name !== inputs.catalog) {
        refuse(`${SOURCE_FILE} creates catalog '${desired.name}', but rpack.json names '${inputs.catalog}'`, { ...at(source.locate(unitKey.catalog())) });
    }
    if (plan.first && released.releases().length > 0) {
        refuse(`every release after the first has parents, and no release is below ${version}`);
    }
    if (plan.refusals.length > 0) return plan;

    if (plan.first) {
        if (!isBlankSql(inputs.nextText)) refuse(`${UPGRADE_MANUAL_FILE} alters the parents, and the first release has none: leave it empty`);
        const used = new Set([...desired.groups.values()].map((g) => g.schema));
        for (const schema of desired.schemas.values()) {
            if (!used.has(schema.name)) {
                refuse(`schema ${schema.name} is used by no group, so no release can ship it`, { ...at(source.locate(unitKey.schema(schema.name))) });
                continue;
            }
            appendNewSchemaSteps(plan, schema, schemaVersion(schema.name));
        }
        plan.add = [...desired.groups.values()];
        plan.addFiles = [...desired.files.values()];
        plan.params = [...desired.params.values()];
        return plan;
    }

    const base = await released.base(parents.map((p) => p.hash));
    plan.base = base;

    let hand;
    try {
        hand = readNext(
            inputs.nextText ?? '',
            (name) => base.schemas.get(name)?.view,
            UPGRADE_MANUAL_FILE,
            (name) => {
                const p = desired.schemas.get(name)?.params;
                return p !== undefined ? new Set(p.keys()) : undefined;
            },
        );
    } catch (err) {
        if (!(err instanceof SourceError)) throw err;
        for (const issue of err.issues) refuse(issue.message, { ...at(issue.where) });
        return plan;
    }

    const breaking = new Map<string, string[]>();
    for (const [name, baseSchema] of [...base.schemas].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))) {
        if (!desired.schemas.has(name)) {
            const users = [...base.groups.values()].filter((g) => g.model.schema === name).map((g) => g.model.name);
            refuse(`schema ${name} is missing from ${SOURCE_FILE}${users.length > 0 ? ` (group ${users.join(', ')} uses it)` : ''}; schemas can't be removed`);
            continue;
        }
        const wanted = desired.schemas.get(name)!;
        const where = at(source.locate(unitKey.schema(name)));
        if (creatorsKey(wanted.creators) !== creatorsKey(baseSchema.creators)) {
            refuse(`schema ${name}: its CREATORS differ from its parents', and a schema's creators can't change`, where);
            continue;
        }
        if (wanted.hashAlgorithm !== baseSchema.hashAlgorithm) {
            refuse(`schema ${name}: its HASH ALGORITHM differs from its parents'`, where);
            continue;
        }

        const handRules = hand.rules.get(name) ?? [];

        if (wanted.origin?.kind === 'imported') {
            const packSteps = wanted.origin.steps;
            const targetVersion = wanted.origin.version;
            const targetStep = packSteps.find((s) => s.version === targetVersion)!;
            const mirrored = await findMirroredPackSteps(baseSchema, packSteps);

            // Find which pack step(s) mirror baseSchema.at
            const baseStepHashes: B64Hash[] = [];
            for (const [stepHash, entryHashes] of mirrored) {
                if (entryHashes.every((e) => baseSchema.at.includes(e))) baseStepHashes.push(stepHash);
            }
            const coveredEntries = new Set(baseStepHashes.flatMap((s) => mirrored.get(s) ?? []));
            const isBaseMirrored = baseSchema.at.every((e) => coveredEntries.has(e));

            if (isBaseMirrored) {
                if (handRules.length > 0) {
                    refuse(`schema ${name} is imported; its migration rules come from its package, not ${UPGRADE_MANUAL_FILE}`,
                        { ...at(handRules[0]?.where) });
                    continue;
                }
                if (baseStepHashes.length === 1 && baseStepHashes[0] === targetStep.hash
                    && tableDifferences(name, baseSchema.tables, wanted.tables).length === 0
                    && paramDifferences(name, baseSchema.params, wanted.params).length === 0) {
                    plan.pins.set(name, { kind: 'entries', entries: baseSchema.at });
                    continue;
                }
                if (!baseSchema.view.isCreator(signer.keyId)) {
                    refuse(`schema ${name}: the release key '${signer.label}' isn't one of its creators, so it can't update it`, where);
                    continue;
                }
                const targetAncestors = stepAncestorsOf(packSteps, targetStep.hash);
                const unbuilt = baseStepHashes.filter((h) => h !== targetStep.hash && !targetAncestors.has(h));
                if (unbuilt.length > 0) {
                    refuse(`schema ${name}: imported version '${targetStep.version}' does not build on ${baseSchema.view.getVersions().join(', ')}`, where);
                    continue;
                }
                const neededSteps = packSteps.filter((s) => (s.hash === targetStep.hash || targetAncestors.has(s.hash)) && !mirrored.has(s.hash));
                if (neededSteps.length === 0) {
                    plan.pins.set(name, { kind: 'entries', entries: mirrored.get(targetStep.hash) ?? baseSchema.at });
                    continue;
                }
                const allNewRules: MigrationRule[] = [];
                for (const s of neededSteps) {
                    const existingParentEntries = s.parents.flatMap((p) => mirrored.get(p) ?? []);
                    const newParentStepHashes = s.parents.filter((p) => !mirrored.has(p));
                    plan.schemas.push({
                        kind: 'update',
                        schema: name,
                        rules: (s.migration ?? []).map((rule) => ({ rule })),
                        ...(s.params !== undefined && s.params.length > 0 ? { params: s.params } : {}),
                        version: s.version,
                        ...(s.note !== undefined ? { note: s.note } : {}),
                        stepHash: s.hash,
                        ...(newParentStepHashes.length > 0 ? { parentStepHashes: newParentStepHashes } : {}),
                        ...(existingParentEntries.length > 0 ? { at: existingParentEntries } : {}),
                    });
                    allNewRules.push(...(s.migration ?? []));
                }
                plan.pins.set(name, { kind: 'update' });
                const lost = allNewRules.filter((r) => isBreaking(r) && r.rule !== 'add-table' && baseSchema.tables.has(r.table));
                if (lost.length > 0) breaking.set(name, lost.map((r) => describeBreaking(r)));
                continue;
            } else {
                refuse(`schema ${name}: cannot switch to IMPORT SCHEMA '${targetVersion}' because the schema's history (${baseSchema.view.getVersions().join(', ')}) does not match the imported package's step history`, where);
                continue;
            }
        }

        const paramDiff = diffSchemaParams(name, baseSchema.params, wanted.params, (p) => source.locate(unitKey.schemaParam(name, p)) ?? where.where);
        if (paramDiff.refusals.length > 0) { plan.refusals.push(...paramDiff.refusals); continue; }

        const paramTypes = new Map<string, ColumnType>();
        for (const [pName, decl] of baseSchema.params) paramTypes.set(pName, decl.type);
        for (const decl of paramDiff.added) paramTypes.set(decl.name, decl.type);
        const resolveParam = (pName: string) => paramTypes.get(pName);

        const working = copyTables(baseSchema.tables);
        const applied = applyMigrationRules(working, handRules.map((h) => h.rule), resolveParam);
        if (!applied.ok) {
            refuse(`ALTER SCHEMA ${name}: ${applied.reason}`, { ...at(handRules[applied.index]?.where) });
            continue;
        }

        const diff = diffTables(name, working, wanted.tables, {
            table: (t) => source.locate(unitKey.table(name, t)),
            column: (t, c) => source.locate(unitKey.column(name, t, c)),
        });
        if (diff.refusals.length > 0) { plan.refusals.push(...diff.refusals); continue; }

        const rules: DraftRule[] = [...handRules.map((h) => ({ rule: h.rule, where: h.where })), ...diff.rules.map((rule) => ({ rule }))];
        const stated = source.schemaVersions.get(name);
        const current = baseSchema.view.getVersions();
        if (rules.length === 0 && paramDiff.added.length === 0) {
            if (stated !== undefined && !current.includes(stated)) {
                refuse(`schema ${name}: VERSION '${stated}' but nothing in it changes (it is at ${current.join(', ')}); a version needs a change`, where);
                continue;
            }
            plan.pins.set(name, { kind: 'entries', entries: baseSchema.at });
            continue;
        }

        const simulated = copyTables(baseSchema.tables);
        const check = applyMigrationRules(simulated, rules.map((r) => r.rule), resolveParam);
        const left = check.ok ? tableDifferences(name, simulated, wanted.tables) : [];
        if (!check.ok || left.length > 0) {
            throw new DraftError(`the generated rules for ${name} don't reach ${SOURCE_FILE} (a bug in rpack): `
                + (check.ok ? left.join('; ') : `rule ${check.index + 1} is refused: ${check.reason}`));
        }
        if (!baseSchema.view.isCreator(signer.keyId)) {
            refuse(`schema ${name}: the release key '${signer.label}' isn't one of its creators, so it can't update it`, where);
            continue;
        }
        const stepVersion = stated ?? version;
        const below = current.filter((v) => compareSemver(stepVersion, v) <= 0);
        if (below.length > 0) {
            if (stated !== undefined) {
                refuse(`schema ${name}: VERSION '${stated}' is not above ${below.join(', ')}; raise it`, where);
            } else {
                for (const v of below) refuse(`schema ${name} is already at version ${v}, which ${version} isn't above`, where);
            }
            continue;
        }
        plan.schemas.push({
            kind: 'update',
            schema: name,
            rules,
            ...(paramDiff.added.length > 0 ? { params: paramDiff.added } : {}),
            version: stepVersion,
        });
        plan.pins.set(name, { kind: 'update' });
        const lost = rules.filter((r) => isBreaking(r.rule) && r.rule.rule !== 'add-table' && baseSchema.tables.has(r.rule.table));
        if (lost.length > 0) breaking.set(name, lost.map((r) => describeBreaking(r.rule)));
    }

    for (const schema of desired.schemas.values()) {
        if (base.schemas.has(schema.name)) continue;
        appendNewSchemaSteps(plan, schema, schemaVersion(schema.name));
    }

    const catalogDiff = diffCatalog(base, desired, plan.pins, source.locate, version);
    plan.refusals.push(...catalogDiff.refusals);
    plan.add = catalogDiff.add;
    plan.addFiles = catalogDiff.addFiles;
    plan.changes = catalogDiff.changes;
    plan.params = catalogDiff.params;

    const highest = parents.reduce((a, b) => (compareSemver(a.version, b.version) >= 0 ? a : b));
    const major = (v: string) => parseSemver(v)?.major ?? 0;
    if (major(version) <= major(highest.version)) {
        for (const [schema, lost] of breaking) {
            plan.warnings.push(`${schema} ${lost.join(', ')}, and ${version} is not a major bump from ${highest.version}`);
        }
    }
    plan.warnings.push(...catalogDiff.warnings);
    for (const param of plan.params) plan.warnings.push(`:${param.name} is new: deploying ${version} needs a value for it`);
    for (const lower of await released.outside(version, parents.map((p) => p.hash))) {
        const including = await released.maximal([...parents.map((p) => p.hash), lower.hash]);
        const selectors = including.map((h) => released.get(h)!.version).join(' + ');
        plan.warnings.push(`${describeRelease(lower)} is below ${version} and not in its past: an instance running it would run it alongside ${version}`
            + ` (\`rpack set base ${selectors}\` includes it)`);
    }

    const nothing = plan.schemas.length === 0 && plan.add.length === 0 && plan.addFiles.length === 0
        && plan.changes.length === 0 && plan.params.length === 0;
    if (plan.refusals.length === 0 && parents.length === 1 && nothing) {
        refuse(`nothing to release: ${SOURCE_FILE} and ${UPGRADE_MANUAL_FILE} describe ${describeRelease(parents[0])} as it is`);
    }
    return plan;
}

function at(where: Where | undefined): { where?: Where } {
    return where === undefined ? {} : { where };
}

function describeBreaking(rule: MigrationRule): string {
    switch (rule.rule) {
        case 'drop-table': return `drops table ${rule.table}`;
        case 'drop-column': return `drops column ${rule.table}.${rule.column}`;
        case 'set-fks': return `changes the foreign keys of ${rule.table}`;
        case 'set-restrictions': return `changes the restrictions of ${rule.table}`;
        case 'set-concurrent-deletes': return `changes the concurrent deletes of ${rule.table}`;
        default: return rule.rule;
    }
}
