// The catalog diff: what a release adds to the catalog on top of its parents'
// merged state, given the pin each schema gets.
//
//   - groups match by name: a new group is added after the groups it binds; a
//     missing or changed group is refused (definitions are immutable);
//   - FILES match by name the same way: a new one is added, a changed one is
//     refused. target-catalog.sql lists only writable FILES: a missing one is
//     refused while it still fits its group's schema in the release, and
//     otherwise becomes read-only (a warning) and stays in the catalog. A
//     read-only FILES listed again, unchanged, is writable again;
//   - a group whose version in any parent differs from its schema's pin gets
//     a change, which covers every group the parents disagree on;
//   - new params are declared; a removed or retyped param is refused.

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { filesSchemaReason, versionKey, type CatalogParamDecl } from "@hyper-hyper-space/hhs3_rdb";

import {
    creatorsKey, filesDifferences, filesKey, groupDifferences, orderByBindings, type CatalogModel, type FilesModel, type GroupModel,
} from "./model.js";
import { SOURCE_FILE } from "./project.js";
import type { BaseState } from "./released.js";
import type { Refusal } from "./schema_diff.js";
import { unitKey, type Locator, type SourceIssue, type Where } from "./source.js";

// The version a release gives a schema: its own update, the create of a new
// schema, or existing entries (the minimal cover of the parents' pins).
export type Pin =
    | { kind: 'update' }
    | { kind: 'create' }
    | { kind: 'entries'; entries: B64Hash[] };

export type CatalogChange = { group: string; schema: string };

export type CatalogDiff = {
    add: GroupModel[];
    addFiles: FilesModel[];
    changes: CatalogChange[];
    params: CatalogParamDecl[];
    warnings: string[];
    refusals: Refusal[];
};

function at(where: Where | undefined): { where?: Where } {
    return where === undefined ? {} : { where };
}

export function diffCatalog(base: BaseState, desired: CatalogModel, pins: Map<string, Pin>, locate: Locator, version: string): CatalogDiff {
    const refusals: Refusal[] = [];
    const catalogAt = at(locate(unitKey.catalog()));
    const current = base.model;

    if (desired.name !== current.name) refusals.push({ message: `${SOURCE_FILE} creates catalog '${desired.name}', but the releases are of '${current.name}'`, ...catalogAt });
    if (creatorsKey(desired.creators) !== creatorsKey(current.creators)) {
        refusals.push({ message: "the catalog's CREATORS differ from its first release's, and a catalog's creators can't change", ...catalogAt });
    }
    if (desired.seed !== current.seed) refusals.push({ message: "the catalog's SEED differs from its first release's", ...catalogAt });
    if (desired.hashAlgorithm !== current.hashAlgorithm) refusals.push({ message: "the catalog's HASH ALGORITHM differs from its first release's", ...catalogAt });
    for (const clash of base.clashes) refusals.push({ message: `the parents can't be written as one ${SOURCE_FILE}: ${clash}` });

    const params: CatalogParamDecl[] = [];
    for (const [name, decl] of current.params) {
        const wanted = desired.params.get(name);
        if (wanted === undefined) refusals.push({ message: `param :${name} is missing from ${SOURCE_FILE}; params can't be removed`, ...catalogAt });
        else if (wanted.type !== decl.type) {
            refusals.push({ message: `param :${name} changes type from ${decl.type} to ${wanted.type}, which a param can't do`, ...at(locate(unitKey.param(name))) });
        }
    }
    for (const [name, decl] of desired.params) if (!current.params.has(name)) params.push(decl);

    for (const [name, group] of current.groups) {
        const wanted = desired.groups.get(name);
        if (wanted === undefined) {
            refusals.push({
                message: `group ${name} is missing from ${SOURCE_FILE}; groups can't be removed`,
                hint: 'put its TABLEGROUP back',
                ...catalogAt,
            });
            continue;
        }
        const fields = groupDifferences(group, wanted);
        if (fields.length > 0) {
            refusals.push({
                message: `group ${name}: its ${fields.join(', ')} change, and a group's definition can't`,
                hint: 'a different definition needs a new group, with a new name',
                ...at(locate(unitKey.group(name))),
            });
        }
    }

    const warnings: string[] = [];
    for (const [name, files] of current.files) {
        const wanted = desired.files.get(name);
        if (wanted === undefined) {
            const misfit = filesMisfit(files, desired);
            if (misfit !== undefined) {
                warnings.push(`FILES ${name} becomes read-only in ${version}: ${misfit}; it stays in the catalog, and a new FILES can take its place`);
                continue;
            }
            refusals.push({
                message: `FILES ${name} is missing from ${SOURCE_FILE}; FILES can't be removed`,
                hint: 'put its FILES back',
                ...catalogAt,
            });
            continue;
        }
        const fields = filesDifferences(files, wanted);
        if (fields.length > 0) {
            refusals.push({
                message: `FILES ${name}: its ${fields.join(', ')} change, and FILES definitions are immutable, use a new name`,
                hint: 'a different definition needs a new FILES, with a new name',
                ...at(locate(unitKey.files(name))),
            });
        }
    }

    const used = new Set([...desired.groups.values()].map((g) => g.schema));
    for (const name of desired.schemas.keys()) {
        if (!used.has(name)) refusals.push({ message: `schema ${name} is used by no group, so no release can ship it`, ...at(locate(unitKey.schema(name))) });
    }

    const fresh = [...desired.groups.values()].filter((g) => !current.groups.has(g.name));
    const add = orderByBindings(fresh, new Set(current.groups.keys()));
    const addFiles: FilesModel[] = [];
    for (const files of [...desired.files.values()].sort((x, y) => (x.name < y.name ? -1 : x.name > y.name ? 1 : 0))) {
        if (current.files.has(files.name)) continue;
        const readOnly = base.files.get(files.name);
        if (readOnly === undefined) { addFiles.push(files); continue; }
        if (filesKey(readOnly.model) !== filesKey(files)) {
            refusals.push({
                message: `FILES ${files.name} is read-only in the parents, and its definition can't change`,
                hint: 'a different definition needs a new FILES, with a new name',
                ...at(locate(unitKey.files(files.name))),
            });
        }
    }

    const changes: CatalogChange[] = [];
    for (const [name, group] of [...base.groups].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))) {
        const pin = pins.get(group.model.schema);
        if (pin === undefined) continue;
        const moves = pin.kind !== 'entries' || group.pins.some((p) => versionKey(p.version) !== versionKey(pin.entries));
        if (moves) changes.push({ group: name, schema: group.model.schema });
    }

    return { add, addFiles, changes, params, warnings, refusals };
}

// Why a FILES doesn't fit its group's schema in `model`, when it doesn't.
function filesMisfit(files: FilesModel, model: CatalogModel): string | undefined {
    const schema = model.groups.get(files.group)?.schema;
    const tables = schema === undefined ? undefined : model.schemas.get(schema)?.tables;
    if (tables === undefined) return undefined;
    return filesSchemaReason(files, schema!, (t) => tables.get(t));
}

const FILES_MISFIT = /^FILES (\S+): (?:USING IDENTITIES \S+: (?:schema \S+ has no table|\S+ isn't an IDENTITY PROVIDER table)|ALLOW WRITE IF reads \S+(?:: schema \S+ has no table|, which ))/;

// A released FILES that target-catalog.sql still lists, though its group's
// schema there no longer has what it reads: the source can't be read, and
// the way on is to leave the FILES out. Undefined when no issue is one.
export function hintReadOnlyFiles(issues: SourceIssue[], base: BaseState): SourceIssue[] | undefined {
    let hinted = false;
    const out = issues.map((issue) => {
        const name = FILES_MISFIT.exec(issue.message)?.[1];
        if (name === undefined || !base.files.has(name)) return issue;
        hinted = true;
        return { ...issue, message: `${issue.message}; ${name} is already released: remove it from ${SOURCE_FILE} to leave it read-only, and add a new FILES in its place` };
    });
    return hinted ? out : undefined;
}
