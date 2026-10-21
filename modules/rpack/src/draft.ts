// Drafting a release: from the parents' merged state, through
// upgrade-manual.sql, to target-catalog.sql. Pure and keyless: the draft is
// what `rpack build` writes and what `rpack release` signs.

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { Entry } from "@hyper-hyper-space/hhs3_dag";
import { json } from "@hyper-hyper-space/hhs3_json";
import { version as versionOf } from "@hyper-hyper-space/hhs3_mvt";
import {
    applyMigrationRules, compareSemver, isValidSemver, parseSemver, resolveSchemaState, slotsTouchedByRule,
    type CatalogParamDecl, type ColumnType, type MigrationRule, type SchemaParamDecl, type TableDef,
} from "@hyper-hyper-space/hhs3_rdb";

import { diffCatalog, type CatalogChange, type Pin } from "./catalog_diff.js";
import { canonicalEntryOrder, type SchemaReleaseStep } from "./format.js";
import type { DevKey } from "./keys.js";
import {
    creatorsKey, effectiveConcurrentDeletes, effectiveFks, isPatchVersion, paramDifferences, PATCH_VERSION_RESERVED, patchVersion,
    tableDifferences,
    type CatalogModel, type FilesModel, type GroupModel, type SchemaModel,
} from "./model.js";
import { describeRelease, type BaseSchema, type BaseState, type Released, type ReleaseInfo } from "./released.js";
import { SOURCE_FILE, UPGRADE_MANUAL_FILE, type RpackImportsLock } from "./project.js";
import { diffSchemaParams, diffTables, isBreaking, type Refusal } from "./schema_diff.js";
import { isBlankSql, readNext, SourceError, unitKey, type ReadSource, type Where } from "./source.js";

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
    imports?: RpackImportsLock;
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
        const heads = headStepHashes(schema);
        const diff = diffTables(schema.name, schema.origin.upstream.tables, schema.tables);
        if (diff.refusals.length > 0) { plan.refusals.push(...diff.refusals); return; }
        if (paramDifferences(schema.name, schema.origin.upstream.params, schema.params).length > 0) {
            plan.refusals.push({ message: `schema ${schema.name}: its parameters differ from the imported schema, and a local patch cannot change parameters` });
            return;
        }
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
            pushUpstreamStep(plan, schema.name, step);
        }
        if (diff.rules.length > 0) {
            plan.schemas.push({
                kind: 'update',
                schema: schema.name,
                rules: diff.rules.map((rule) => ({ rule })),
                version: patchVersion(version),
                parentStepHashes: heads,
            });
        }
        plan.pins.set(schema.name, steps.length === 1 && diff.rules.length === 0 ? { kind: 'create' } : { kind: 'update' });
        return;
    }
    plan.schemas.push({ kind: 'create', schema, version });
    plan.pins.set(schema.name, { kind: 'create' });
}

function pushUpstreamStep(plan: ReleaseDraft, name: string, step: SchemaReleaseStep, at?: B64Hash[]): void {
    plan.schemas.push({
        kind: 'update',
        schema: name,
        rules: (step.migration ?? []).map((rule) => ({ rule })),
        ...(step.params !== undefined && step.params.length > 0 ? { params: step.params } : {}),
        version: step.version,
        ...(step.note !== undefined ? { note: step.note } : {}),
        stepHash: step.hash,
        parentStepHashes: [...step.parents],
        ...(at !== undefined && at.length > 0 ? { at } : {}),
    });
}

function headStepHashes(schema: SchemaModel): B64Hash[] {
    if (schema.origin?.kind !== 'imported') return [];
    const byVersion = new Map(schema.origin.steps.map((s) => [s.version, s.hash]));
    return schema.origin.versions.map((v) => byVersion.get(v)!);
}

async function loadSchemaEntries(schema: BaseSchema): Promise<Entry[]> {
    const entries: Entry[] = [];
    for await (const entry of (await schema.object.getScopedDag()).loadAllEntries()) entries.push(entry);
    return entries;
}

function isPatchEntry(entry: Entry): boolean {
    const version = (entry.payload as json.LiteralMap)['version'];
    return typeof version === 'string' && isPatchVersion(version);
}

function entryPrevs(entry: Entry): B64Hash[] {
    return [...json.fromSet(entry.header.prevEntryHashes)];
}

// Matches each non-patch catalog entry to a pack step by version, parent map
// and payload. A normal entry's parents are catalog hashes; the step's parents
// are pack step hashes.
function causalPast(entries: Entry[], roots: B64Hash[]): Set<B64Hash> {
    const byHash = new Map(entries.map((e) => [e.hash, e]));
    const out = new Set<B64Hash>();
    const queue = [...roots];
    while (queue.length > 0) {
        const hash = queue.pop()!;
        if (out.has(hash) || !byHash.has(hash)) continue;
        out.add(hash);
        for (const parent of entryPrevs(byHash.get(hash)!)) queue.push(parent);
    }
    return out;
}

function entryMatchesStep(entry: Entry, step: SchemaReleaseStep, entryToStep: Map<B64Hash, B64Hash>): boolean {
    const parents = entryPrevs(entry);
    if (!parents.every((p) => entryToStep.has(p))) return false;
    const mapped = parents.map((p) => entryToStep.get(p)!).sort();
    if (mapped.join(',') !== [...step.parents].sort().join(',')) return false;
    return sameStepPayload(entry.payload as json.LiteralMap, step);
}

// Matches normal entries to pack steps. Entries at or below `at` (the parents'
// pin) must match. Entries from other installed releases are mapped when they
// match and skipped otherwise, and so is anything whose parent was skipped.
function mapUpstreamEntries(
    name: string,
    entries: Entry[],
    steps: SchemaReleaseStep[],
    at: B64Hash[],
    versions: string[],
): { stepToEntry: Map<B64Hash, B64Hash> } | { error: string } {
    const byVersion = new Map(steps.map((s) => [s.version, s]));
    const ordered = canonicalEntryOrder(entries.map((e) => ({ hash: e.hash, prevs: entryPrevs(e), payload: e.payload })));
    const byHash = new Map(entries.map((e) => [e.hash, e]));
    const past = causalPast(entries, at);
    const entryToStep = new Map<B64Hash, B64Hash>();
    const stepToEntry = new Map<B64Hash, B64Hash>();
    const skipped = new Set<B64Hash>();
    for (const item of ordered) {
        const entry = byHash.get(item.hash)!;
        if (isPatchEntry(entry)) continue;
        const parents = entryPrevs(entry);
        const inPast = past.has(entry.hash);
        if (!inPast && parents.some((p) => skipped.has(p))) {
            skipped.add(entry.hash);
            continue;
        }
        const payload = entry.payload as json.LiteralMap;
        const version = payload['version'];
        if (typeof version !== 'string') {
            if (!inPast) { skipped.add(entry.hash); continue; }
            return { error: `schema ${name} has an entry without a version` };
        }
        const step = byVersion.get(version);
        if (!inPast) {
            if (step !== undefined && entryMatchesStep(entry, step, entryToStep)) {
                entryToStep.set(entry.hash, step.hash);
                stepToEntry.set(step.hash, entry.hash);
            } else {
                skipped.add(entry.hash);
            }
            continue;
        }
        for (const parent of parents) {
            if (!entryToStep.has(parent)) {
                return { error: `schema ${name} has an upstream entry whose parent is not an upstream entry` };
            }
        }
        if (step === undefined) {
            if (parents.length === 0) {
                return { error: `schema ${name} was defined inline or has a version '${version}' not in the imported package history` };
            }
            const listed = versions.map((v) => `'${v}'`).join(', ');
            return { error: `schema ${name}: imported version(s) ${listed} do not have version '${version}' in their history, and a parent release imported it` };
        }
        const mappedParents = parents.map((p) => entryToStep.get(p)!).sort();
        if (mappedParents.join(',') !== [...step.parents].sort().join(',')) {
            return { error: `schema ${name} version '${version}' does not have the imported step's parents` };
        }
        if (!sameStepPayload(payload, step)) {
            return { error: `schema ${name} version '${version}' does not match the imported step` };
        }
        entryToStep.set(entry.hash, step.hash);
        stepToEntry.set(step.hash, entry.hash);
    }
    return { stepToEntry };
}

function sameStepPayload(payload: json.LiteralMap, step: SchemaReleaseStep): boolean {
    const params = json.toStringNormalized((payload['params'] ?? []) as json.Literal);
    const stepParams = json.toStringNormalized((step.params ?? []) as unknown as json.Literal);
    if (params !== stepParams) return false;
    if (payload['action'] === 'create') {
        return json.toStringNormalized(payload['tables'] as json.Literal) === json.toStringNormalized((step.tables ?? []) as unknown as json.Literal);
    }
    const migration = json.toStringNormalized((payload['migration'] ?? []) as json.Literal);
    const stepMigration = json.toStringNormalized((step.migration ?? []) as unknown as json.Literal);
    return migration === stepMigration && (payload['note'] ?? undefined) === (step.note ?? undefined);
}

async function planImportedUpdate(
    plan: ReleaseDraft,
    name: string,
    wanted: SchemaModel,
    baseSchema: BaseSchema,
    handRules: { rule: MigrationRule; where?: Where }[],
    catalogVersion: string,
    signerKeyId: string,
    signerLabel: string,
    imports: RpackImportsLock | undefined,
    where: { where?: Where },
    refuse: (message: string, extra?: { where?: Where }) => void,
    breaking: Map<string, string[]>,
    source: ReadSource,
): Promise<void> {
    const origin = wanted.origin;
    if (origin?.kind !== 'imported') return;
    if (imports?.[name] === undefined) {
        refuse(`schema ${name} is imported in ${SOURCE_FILE} but is not locked in rpack.json`, where);
        return;
    }
    const entries = await loadSchemaEntries(baseSchema);
    const mapped = mapUpstreamEntries(name, entries, origin.steps, baseSchema.at, origin.versions);
    if ('error' in mapped) { refuse(mapped.error, where); return; }
    const { stepToEntry } = mapped;

    const queued = origin.steps.filter((step) => step.parents.length > 0 && !stepToEntry.has(step.hash));

    const heads = origin.versions.map((v) => {
        const step = origin.steps.find((s) => s.version === v)!;
        return stepToEntry.get(step.hash) ?? step.hash;
    });
    const synthetic = queued.map((step) => syntheticUpdate(step, step.parents.map((p) => stepToEntry.get(p) ?? p)));
    const all = [...entries, ...synthetic];
    const frontier = minimalCover(all, [...baseSchema.at, ...heads]);
    const state = resolveSchemaState(all, versionOf(...frontier));
    const candidateTables = state.tables;
    const candidateParams = state.params;

    if (paramDifferences(name, candidateParams, wanted.params).length > 0) {
        refuse(`schema ${name}: its parameters differ from the imported schema, and a local patch cannot change parameters`, where);
        return;
    }
    const paramTypes = new Map<string, ColumnType>();
    for (const [pName, decl] of candidateParams) paramTypes.set(pName, decl.type);
    const resolveParam = (pName: string) => paramTypes.get(pName);
    const working = copyTables(candidateTables);
    const applied = applyMigrationRules(working, handRules.map((h) => h.rule), resolveParam);
    if (!applied.ok) {
        refuse(`ALTER SCHEMA ${name}: ${applied.reason}`, { where: handRules[applied.index]?.where });
        return;
    }
    const diff = diffTables(name, working, wanted.tables, {
        table: (t) => source.locate(unitKey.table(name, t)),
        column: (t, c) => source.locate(unitKey.column(name, t, c)),
    });
    if (diff.refusals.length > 0) { plan.refusals.push(...diff.refusals); return; }
    const patchRules = [...handRules.map((h) => h.rule), ...diff.rules];
    warnOverriddenSlots(plan, name, queued, candidateTables, origin.upstream.tables, wanted.tables);

    const queuedSomething = queued.length > 0 || patchRules.length > 0;
    if (queuedSomething && !baseSchema.view.isCreator(signerKeyId)) {
        refuse(`schema ${name}: the release key '${signerLabel}' isn't one of its creators, so it can't update it`, where);
        return;
    }
    for (const step of queued) {
        const existing = step.parents.filter((p) => stepToEntry.has(p)).map((p) => stepToEntry.get(p)!);
        const fresh = step.parents.filter((p) => !stepToEntry.has(p));
        plan.schemas.push({
            kind: 'update',
            schema: name,
            rules: (step.migration ?? []).map((rule) => ({ rule })),
            ...(step.params !== undefined && step.params.length > 0 ? { params: step.params } : {}),
            version: step.version,
            ...(step.note !== undefined ? { note: step.note } : {}),
            stepHash: step.hash,
            ...(fresh.length > 0 ? { parentStepHashes: fresh } : {}),
            ...(existing.length > 0 ? { at: existing } : {}),
        });
    }
    const mappedHeads = heads.filter((h) => entries.some((e) => e.hash === h));
    const freshHeads = origin.versions.map((v) => origin.steps.find((s) => s.version === v)!.hash).filter((h) => !stepToEntry.has(h));
    if (patchRules.length > 0) {
        const simulated = copyTables(candidateTables);
        const check = applyMigrationRules(simulated, patchRules, resolveParam);
        const left = check.ok ? tableDifferences(name, simulated, wanted.tables) : [];
        if (!check.ok || left.length > 0) {
            throw new DraftError(`the generated rules for ${name} don't reach ${SOURCE_FILE} (a bug in rpack): `
                + (check.ok ? left.join('; ') : `rule ${check.index + 1} is refused: ${check.reason}`));
        }
        plan.schemas.push({
            kind: 'update',
            schema: name,
            rules: patchRules.map((rule) => ({ rule })),
            version: patchVersion(catalogVersion),
            at: [...baseSchema.at, ...mappedHeads],
            ...(freshHeads.length > 0 ? { parentStepHashes: freshHeads } : {}),
        });
    }
    if (!queuedSomething) {
        plan.pins.set(name, { kind: 'entries', entries: frontier });
    } else if (patchRules.length === 0) {
        plan.pins.set(name, { kind: 'update', from: minimalCover(entries, [...baseSchema.at, ...mappedHeads]) });
    } else {
        plan.pins.set(name, { kind: 'update' });
    }
    const lost = [...queued.flatMap((s) => s.migration ?? []), ...patchRules]
        .filter((r) => isBreaking(r) && r.rule !== 'add-table' && baseSchema.tables.has(r.table));
    if (lost.length > 0) breaking.set(name, lost.map((r) => describeBreaking(r)));
}

function syntheticUpdate(step: SchemaReleaseStep, prevs: B64Hash[]): Entry {
    const payload: json.LiteralMap = {
        action: 'schema-update',
        version: step.version,
        migration: (step.migration ?? []) as unknown as json.Literal,
        author: 'planner',
        signature: 'planner',
    };
    if (step.params !== undefined && step.params.length > 0) payload['params'] = step.params as unknown as json.Literal;
    if (step.note !== undefined) payload['note'] = step.note;
    return {
        hash: step.hash,
        header: { payloadHash: step.hash, prevEntryHashes: json.toSet(prevs) },
        payload,
        meta: {},
    };
}

function minimalCover(entries: Entry[], hashes: B64Hash[]): B64Hash[] {
    const byHash = new Map(entries.map((e) => [e.hash, e]));
    const ancestors = new Map<B64Hash, Set<B64Hash>>();
    const of = (hash: B64Hash): Set<B64Hash> => {
        const cached = ancestors.get(hash);
        if (cached !== undefined) return cached;
        const out = new Set<B64Hash>();
        ancestors.set(hash, out);
        const entry = byHash.get(hash);
        if (entry === undefined) return out;
        for (const parent of entryPrevs(entry)) {
            out.add(parent);
            for (const older of of(parent)) out.add(older);
        }
        return out;
    };
    const unique = [...new Set(hashes)];
    return unique.filter((hash) => !unique.some((other) => other !== hash && of(other).has(hash))).sort();
}

function warnOverriddenSlots(
    plan: ReleaseDraft,
    name: string,
    queued: SchemaReleaseStep[],
    candidate: Map<string, TableDef>,
    upstream: Map<string, TableDef>,
    wanted: Map<string, TableDef>,
): void {
    const touched = new Map<string, string[]>();
    const note = (slot: string, version: string) => {
        const versions = touched.get(slot) ?? [];
        if (!versions.includes(version)) versions.push(version);
        touched.set(slot, versions);
    };
    for (const step of queued) {
        for (const rule of step.migration ?? []) {
            for (const slot of slotsOfQueuedRule(rule, candidate, upstream, wanted)) note(slot, step.version);
        }
    }
    for (const [slot, versions] of touched) {
        if (slotSnapshot(wanted, slot) === slotSnapshot(upstream, slot)) continue;
        plan.warnings.push(`schema ${name}: imported ${versions.join(', ')} changes ${describeSlot(slot)}, which is overridden by the local ALTER SCHEMA patch`);
    }
}

function slotsOfQueuedRule(
    rule: MigrationRule,
    candidate: Map<string, TableDef>,
    upstream: Map<string, TableDef>,
    wanted: Map<string, TableDef>,
): string[] {
    if (rule.rule !== 'drop-table' && rule.rule !== 'add-table') return slotsTouchedByRule(rule);
    const table = rule.rule === 'drop-table' ? rule.table : rule.def.name;
    const columns = new Set<string>([
        ...Object.keys(candidate.get(table)?.columns ?? {}),
        ...Object.keys(upstream.get(table)?.columns ?? {}),
        ...Object.keys(wanted.get(table)?.columns ?? {}),
    ]);
    return [`t:${table}`, `m:${table}`, `f:${table}`, `r:${table}`, ...[...columns].map((column) => `c:${table}:${column}`)];
}

function slotSnapshot(tables: Map<string, TableDef>, slot: string): string {
    const [kind, table, column] = slot.split(':');
    const def = table === undefined ? undefined : tables.get(table);
    if (kind === 't') return def === undefined ? 'absent' : 'present';
    if (def === undefined) return 'absent';
    if (kind === 'm') return String(effectiveConcurrentDeletes(def));
    if (kind === 'f') return json.toStringNormalized(effectiveFks(def) as unknown as json.Literal);
    if (kind === 'r') return json.toStringNormalized((def.restrictions ?? []) as unknown as json.Literal);
        if (kind === 'c' && column !== undefined) {
        const col = def.columns[column];
        return col === undefined ? 'absent' : json.toStringNormalized(col as unknown as json.Literal);
    }
    return '';
}

function describeSlot(slot: string): string {
    const [kind, table, column] = slot.split(':');
    if (kind === 'r') return `restrictions of table ${table}`;
    if (kind === 'f') return `foreign keys of table ${table}`;
    if (kind === 'm') return `concurrent deletes of table ${table}`;
    if (kind === 't') return `table ${table}`;
    return `column ${table}.${column}`;
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
    else if (isPatchVersion(version)) refuse(`'${version}': ${PATCH_VERSION_RESERVED}`);
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
            await planImportedUpdate(plan, name, wanted, baseSchema, handRules, version, signer.keyId, signer.label, inputs.imports, where, refuse, breaking, source);
            continue;
        }
        const entries = await loadSchemaEntries(baseSchema);
        if (inputs.imports?.[name] !== undefined || entries.some((e) => isPatchEntry(e))) {
            refuse(`schema ${name} was imported; keep IMPORT SCHEMA and use ALTER SCHEMA ${name} in ${SOURCE_FILE} for local modifications`, where);
            continue;
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
