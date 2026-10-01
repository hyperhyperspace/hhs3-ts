// The catalog as rpack diffs it: the same shape for what target-catalog.sql describes
// and for the merged state of a set of releases.
//
// Everything is by name: schemas, their tables, groups, and each group's
// schema and bindings. The model holds no object ids, and its keys are real
// (source keys are stand-ins until rpack replaces them). Equality compares
// effective values, with the defaults RSchemaView applies, so two spellings of
// the same schema are equal.

import { json } from "@hyper-hyper-space/hhs3_json";
import {
    DEFAULT_CONCURRENT_DELETES, defaultRestrictionRule,
    type CatalogFilesDef, type CatalogGroupDef, type CatalogParamDecl, type ColumnDef, type FKs, type Predicate, type SchemaCreator, type TableDef,
} from "@hyper-hyper-space/hhs3_rdb";
import type { SourceFiles, SourceGroup } from "@hyper-hyper-space/hhs3_rdb_lang";

export type SchemaModel = {
    name: string;
    creators: SchemaCreator[];
    hashAlgorithm?: string;
    tables: Map<string, TableDef>;
};

export type GroupModel = SourceGroup;

export type FilesModel = SourceFiles;

export type CatalogModel = {
    name: string;
    creators: SchemaCreator[];
    seed?: string;
    hashAlgorithm?: string;
    params: Map<string, CatalogParamDecl>;
    schemas: Map<string, SchemaModel>;
    groups: Map<string, GroupModel>;
    files: Map<string, FilesModel>;
};

export const RESTRICTION_OPS = ['insert', 'update', 'delete'] as const;

export function literalKey(value: json.Literal): string {
    return json.toStringCanonical(value);
}

export function effectiveFks(def: TableDef): FKs {
    return def.fks ?? {};
}

export function effectiveConcurrentDeletes(def: TableDef): boolean {
    return def.concurrentDeletes ?? DEFAULT_CONCURRENT_DELETES;
}

export function effectiveRestriction(def: TableDef, op: typeof RESTRICTION_OPS[number], authenticated: boolean): Predicate {
    const matching = (def.restrictions ?? []).find((r) => r.on === op || r.on === 'all');
    return matching?.rule ?? defaultRestrictionRule(op, authenticated);
}

export function sameColumn(a: ColumnDef, b: ColumnDef): boolean {
    return literalKey(a as json.Literal) === literalKey(b as json.Literal);
}

export function sameFks(a: TableDef, b: TableDef): boolean {
    return literalKey(effectiveFks(a)) === literalKey(effectiveFks(b));
}

// A schema can be deployed in groups with and without an identity provider,
// and the default rule differs between them, so two tables have the same
// restrictions only when they agree under both defaults.
export function sameRestrictions(a: TableDef, b: TableDef): boolean {
    return [true, false].every((authenticated) => RESTRICTION_OPS.every((op) =>
        literalKey(effectiveRestriction(a, op, authenticated) as json.Literal) === literalKey(effectiveRestriction(b, op, authenticated) as json.Literal)));
}

export function sameConcurrentDeletes(a: TableDef, b: TableDef): boolean {
    return effectiveConcurrentDeletes(a) === effectiveConcurrentDeletes(b);
}

export function sameIdProvider(a: TableDef, b: TableDef): boolean {
    const key = (def: TableDef) => (def.idProvider === undefined ? '' : literalKey(def.idProvider as json.Literal));
    return key(a) === key(b);
}

export function sameSettings(a: TableDef, b: TableDef): boolean {
    return sameFks(a, b) && sameRestrictions(a, b) && sameConcurrentDeletes(a, b) && sameIdProvider(a, b);
}

export function sameTable(a: TableDef, b: TableDef): boolean {
    const columns = Object.keys(a.columns);
    if (columns.length !== Object.keys(b.columns).length) return false;
    for (const column of columns) {
        const other = b.columns[column];
        if (other === undefined || !sameColumn(a.columns[column], other)) return false;
    }
    return sameSettings(a, b);
}

// The differences between two table sets, one line each (empty when equal).
export function tableDifferences(schema: string, a: Map<string, TableDef>, b: Map<string, TableDef>): string[] {
    const out: string[] = [];
    for (const [name, def] of a) {
        const other = b.get(name);
        if (other === undefined) { out.push(`${schema}: table ${name} is only on the first side`); continue; }
        for (const column of Object.keys(def.columns)) {
            if (other.columns[column] === undefined) out.push(`${schema}: column ${name}.${column} is only on the first side`);
            else if (!sameColumn(def.columns[column], other.columns[column])) out.push(`${schema}: column ${name}.${column} differs`);
        }
        for (const column of Object.keys(other.columns)) {
            if (def.columns[column] === undefined) out.push(`${schema}: column ${name}.${column} is only on the second side`);
        }
        if (!sameFks(def, other)) out.push(`${schema}: the foreign keys of ${name} differ`);
        if (!sameRestrictions(def, other)) out.push(`${schema}: the restrictions of ${name} differ`);
        if (!sameConcurrentDeletes(def, other)) out.push(`${schema}: the concurrent deletes of ${name} differ`);
        if (!sameIdProvider(def, other)) out.push(`${schema}: the identity provider of ${name} differs`);
    }
    for (const name of b.keys()) {
        if (!a.has(name)) out.push(`${schema}: table ${name} is only on the second side`);
    }
    return out;
}

export function creatorsKey(creators: SchemaCreator[]): string {
    return creators.map((c) => c.keyId).sort().join(',');
}

// A group's definition without its name, for comparing two groups of one name.
export function groupKey(group: GroupModel): string {
    const { name: _name, ...rest } = group;
    return literalKey(rest as unknown as json.Literal);
}

// The fields in which two groups of one name differ.
export function groupDifferences(a: GroupModel, b: GroupModel): string[] {
    const fields: string[] = [];
    const field = (label: string, x: unknown, y: unknown) => {
        const key = (v: unknown) => (v === undefined ? '' : literalKey(v as json.Literal));
        if (key(x) !== key(y)) fields.push(label);
    };
    field('schema', a.schema, b.schema);
    field('bindings', a.bindings ?? {}, b.bindings ?? {});
    field('identity provider', a.idProvider, b.idProvider);
    field('ALLOW DEPLOY', a.canDeploy, b.canDeploy);
    field('ALLOW UPDATE REF', a.canObserve ?? {}, b.canObserve ?? {});
    field('rows', a.initialRows ?? {}, b.initialRows ?? {});
    return fields;
}

// A group model from a definition: its schema and bindings by name.
export function groupModelOf(
    def: CatalogGroupDef,
    schemaName: string,
    bindingName: (hash: string) => string,
): GroupModel {
    const group: GroupModel = { name: def.name, schema: schemaName };
    const bindings = Object.entries(def.bindings ?? {});
    if (bindings.length > 0) group.bindings = Object.fromEntries(bindings.map(([alias, hash]) => [alias, bindingName(hash)]));
    if (def.idProvider !== undefined) group.idProvider = def.idProvider;
    if (def.canDeploy !== undefined) group.canDeploy = def.canDeploy;
    if (def.canObserve !== undefined && Object.keys(def.canObserve).length > 0) group.canObserve = def.canObserve;
    if (def.initialRows !== undefined && Object.keys(def.initialRows).length > 0) group.initialRows = def.initialRows;
    return group;
}

// A FILES model from a definition: its bound group by name.
export function filesModelOf(def: CatalogFilesDef, groupName: (hash: string) => string): FilesModel {
    const [alias, hash] = Object.entries(def.bindings)[0];
    return { name: def.name, alias, group: groupName(hash), idProvider: def.idProvider, canWrite: def.canWrite };
}

// A FILES definition without its name, for comparing two FILES of one name.
export function filesKey(files: FilesModel): string {
    const { name: _name, ...rest } = files;
    return literalKey(rest as unknown as json.Literal);
}

// The fields in which two FILES of one name differ.
export function filesDifferences(a: FilesModel, b: FilesModel): string[] {
    const fields: string[] = [];
    if (a.group !== b.group) fields.push('group');
    if (a.alias !== b.alias) fields.push('BIND alias');
    if (a.idProvider !== b.idProvider) fields.push('identity table');
    if (literalKey(a.canWrite as json.Literal) !== literalKey(b.canWrite as json.Literal)) fields.push('ALLOW WRITE');
    return fields;
}

// Every difference between two models, one line each; empty when they are
// equal.
export function modelDifferences(a: CatalogModel, b: CatalogModel): string[] {
    const out: string[] = [];
    if (a.name !== b.name) out.push(`the catalog is named '${a.name}' on one side and '${b.name}' on the other`);
    if (creatorsKey(a.creators) !== creatorsKey(b.creators)) out.push('the catalog creators differ');
    if (a.seed !== b.seed) out.push('the catalog seed differs');
    if (a.hashAlgorithm !== b.hashAlgorithm) out.push('the catalog hash algorithm differs');

    for (const [name, decl] of a.params) {
        const other = b.params.get(name);
        if (other === undefined) out.push(`param :${name} is only on the first side`);
        else if (other.type !== decl.type) out.push(`param :${name} has two types`);
    }
    for (const name of b.params.keys()) if (!a.params.has(name)) out.push(`param :${name} is only on the second side`);

    for (const [name, schema] of a.schemas) {
        const other = b.schemas.get(name);
        if (other === undefined) { out.push(`schema ${name} is only on the first side`); continue; }
        if (creatorsKey(schema.creators) !== creatorsKey(other.creators)) out.push(`the creators of schema ${name} differ`);
        if (schema.hashAlgorithm !== other.hashAlgorithm) out.push(`the hash algorithm of schema ${name} differs`);
        out.push(...tableDifferences(name, schema.tables, other.tables));
    }
    for (const name of b.schemas.keys()) if (!a.schemas.has(name)) out.push(`schema ${name} is only on the second side`);

    for (const [name, group] of a.groups) {
        const other = b.groups.get(name);
        if (other === undefined) { out.push(`group ${name} is only on the first side`); continue; }
        for (const field of groupDifferences(group, other)) out.push(`group ${name}: its ${field} differ`);
    }
    for (const name of b.groups.keys()) if (!a.groups.has(name)) out.push(`group ${name} is only on the second side`);

    for (const [name, files] of a.files) {
        const other = b.files.get(name);
        if (other === undefined) { out.push(`FILES ${name} is only on the first side`); continue; }
        for (const field of filesDifferences(files, other)) out.push(`FILES ${name}: its ${field} differ`);
    }
    for (const name of b.files.keys()) if (!a.files.has(name)) out.push(`FILES ${name} is only on the second side`);
    return out;
}

// Groups ordered so each comes after the groups it binds, ties by name.
export function orderByBindings(groups: GroupModel[], present: Set<string> = new Set()): GroupModel[] {
    const pending = [...groups].sort((x, y) => (x.name < y.name ? -1 : x.name > y.name ? 1 : 0));
    const placed = new Set(present);
    const out: GroupModel[] = [];
    while (pending.length > 0) {
        const index = pending.findIndex((g) => Object.values(g.bindings ?? {}).every((bound) => placed.has(bound) || !pending.some((p) => p.name === bound)));
        const next = pending.splice(index === -1 ? 0 : index, 1)[0];
        placed.add(next.name);
        out.push(next);
    }
    return out;
}
