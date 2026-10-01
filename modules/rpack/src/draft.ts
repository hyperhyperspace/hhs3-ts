// Drafting a release: from the parents' merged state, through
// upgrade-manual.sql, to target-catalog.sql. Pure and keyless: the draft is
// what `rpack build` writes and what `rpack release` signs.

import {
    applyMigrationRules, compareSemver, isValidSemver, parseSemver,
    type CatalogParamDecl, type MigrationRule, type TableDef,
} from "@hyper-hyper-space/hhs3_rdb";

import { diffCatalog, type CatalogChange, type Pin } from "./catalog_diff.js";
import type { DevKey } from "./keys.js";
import { creatorsKey, tableDifferences, type CatalogModel, type FilesModel, type GroupModel, type SchemaModel } from "./model.js";
import { describeRelease, type BaseState, type Released, type ReleaseInfo } from "./released.js";
import { SOURCE_FILE, UPGRADE_MANUAL_FILE } from "./project.js";
import { diffTables, isBreaking, type Refusal } from "./schema_diff.js";
import { isBlankSql, readNext, SourceError, unitKey, type ReadSource, type Where } from "./source.js";

export type DraftRule = { rule: MigrationRule; where?: Where };

// `version` is the VERSION target-catalog.sql states for the schema, or the
// release's own.
export type SchemaStep =
    | { kind: 'create'; schema: SchemaModel; version: string }
    | { kind: 'update'; schema: string; rules: DraftRule[]; version: string };

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
            plan.schemas.push({ kind: 'create', schema, version: schemaVersion(schema.name) });
            plan.pins.set(schema.name, { kind: 'create' });
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
        hand = readNext(inputs.nextText ?? '', (name) => base.schemas.get(name)?.view);
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
        const working = copyTables(baseSchema.tables);
        const applied = applyMigrationRules(working, handRules.map((h) => h.rule));
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
        if (rules.length === 0) {
            if (stated !== undefined && !current.includes(stated)) {
                refuse(`schema ${name}: VERSION '${stated}' but nothing in it changes (it is at ${current.join(', ')}); a version needs a change`, where);
                continue;
            }
            plan.pins.set(name, { kind: 'entries', entries: baseSchema.at });
            continue;
        }

        const simulated = copyTables(baseSchema.tables);
        const check = applyMigrationRules(simulated, rules.map((r) => r.rule));
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
        plan.schemas.push({ kind: 'update', schema: name, rules, version: stepVersion });
        plan.pins.set(name, { kind: 'update' });
        const lost = rules.filter((r) => isBreaking(r.rule) && r.rule.rule !== 'add-table' && baseSchema.tables.has(r.rule.table));
        if (lost.length > 0) breaking.set(name, lost.map((r) => describeBreaking(r.rule)));
    }

    for (const schema of desired.schemas.values()) {
        if (base.schemas.has(schema.name)) continue;
        plan.schemas.push({ kind: 'create', schema, version: schemaVersion(schema.name) });
        plan.pins.set(schema.name, { kind: 'create' });
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
