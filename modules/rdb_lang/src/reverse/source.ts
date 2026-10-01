// Source form: the C-SQL a catalog repository's target-catalog.sql is written in, the
// inverse of source mode (ParseOptions.catalogVersionOptional). No VERSION, AT
// or BY: schemas and groups are named, and a release supplies the versions,
// the pins and the signatures.

import type { json } from "@hyper-hyper-space/hhs3_json";
import type { CatalogParamDecl, CatalogRowTemplate, Predicate, TableDef } from "@hyper-hyper-space/hhs3_rdb";

import { renderIdent, renderLiteral, renderPredicate, renderTableDef } from "./render.js";

// `$label`s for keys: by key id, and by public key (base64) for
// `publicKey($label)`. A key without a label renders as a literal.
export type SourceKeyLabels = {
    keyId?(keyId: string): string | undefined;
    publicKey?(publicKey: string): string | undefined;
};

export type SourceCreator = { keyId: string; publicKey: string };

export type SourceSchema = {
    name: string;
    creators: SourceCreator[];
    hashAlgorithm?: string;
    tables: TableDef[];
};

// A catalog group by name: its schema by name, and each binding's alias to the
// name of the group it binds.
export type SourceGroup = {
    name: string;
    schema: string;
    bindings?: { [alias: string]: string };
    idProvider?: string;
    canDeploy?: Predicate;
    canObserve?: { [binding: string]: Predicate };
    initialRows?: { [table: string]: CatalogRowTemplate[] };
};

// A catalog FILES by name: its one binding's alias and the name of the group
// it binds.
export type SourceFiles = {
    name: string;
    alias: string;
    group: string;
    idProvider: string;
    canWrite: Predicate;
};

export type SourceCatalog = {
    name: string;
    creators: SourceCreator[];
    seed?: string;
    hashAlgorithm?: string;
    params: CatalogParamDecl[];
    groups: SourceGroup[];
    files?: SourceFiles[];
};

export type SourceGroupOptions = {
    labels?: SourceKeyLabels;
    // 'ADD TABLEGROUP' and a rendered pin (`AT <pin>`) serve release plans.
    keyword?: 'TABLEGROUP' | 'ADD TABLEGROUP';
    pin?: string;
};

export function renderSourceCreators(creators: SourceCreator[], labels?: SourceKeyLabels): string {
    if (creators.length === 0) return '';
    const names = creators.map((c) => {
        const label = labels?.keyId?.(c.keyId) ?? labels?.publicKey?.(c.publicKey);
        return label !== undefined ? `$${label}` : `publicKey(${renderLiteral(c.publicKey)})`;
    });
    return `CREATORS (${names.join(', ')})`;
}

export function renderSourceSchema(schema: SourceSchema, labels?: SourceKeyLabels): string {
    const head = [`CREATE SCHEMA ${renderIdent(schema.name)}`];
    const creators = renderSourceCreators(schema.creators, labels);
    if (creators.length > 0) head.push(creators);
    if (schema.hashAlgorithm !== undefined) head.push(`HASH ALGORITHM ${renderLiteral(schema.hashAlgorithm)}`);
    return `${head.join(' ')} AS (\n  ${schema.tables.map(renderSourceTable).join(',\n\n  ')}\n);`;
}

export function renderSourceTable(table: TableDef): string {
    return renderTableDef(table);
}

export function renderSourceParam(decl: CatalogParamDecl): string {
    return `:${renderIdent(decl.name)} ${decl.type}`;
}

export function renderSourceParams(params: CatalogParamDecl[]): string {
    return params.length === 0 ? '' : `PARAMS (${params.map(renderSourceParam).join(', ')})`;
}

export function renderSourceGroup(group: SourceGroup, options?: SourceGroupOptions): string {
    const at = options?.pin !== undefined ? ` AT ${options.pin}` : '';
    const parts = [`${options?.keyword ?? 'TABLEGROUP'} ${renderIdent(group.name)} USING SCHEMA ${renderIdent(group.schema)}${at}`];
    const bindings = Object.entries(group.bindings ?? {});
    if (bindings.length > 0) {
        parts.push(`BIND ${bindings.map(([alias, name]) => `${renderIdent(alias)} => ${renderIdent(name)}`).join(', ')}`);
    }
    if (group.idProvider !== undefined) parts.push(`USING IDENTITIES ${renderIdent(group.idProvider)}`);
    if (group.canDeploy !== undefined) parts.push(`ALLOW DEPLOY IF ${renderPredicate(group.canDeploy)}`);
    for (const [binding, pred] of Object.entries(group.canObserve ?? {})) {
        parts.push(`ALLOW UPDATE REF ${renderIdent(binding)} IF ${renderPredicate(pred)}`);
    }
    const rows: string[] = [];
    for (const [table, templates] of Object.entries(group.initialRows ?? {})) {
        for (const template of templates) rows.push(renderSourceRow(table, template, options?.labels));
    }
    if (rows.length > 0) parts.push(`WITH ROWS (\n      ${rows.join(',\n      ')}\n    )`);
    return parts.join('\n    ');
}

export function renderSourceCatalog(catalog: SourceCatalog, labels?: SourceKeyLabels): string {
    const head = [`CREATE CATALOG ${renderIdent(catalog.name)}`];
    const creators = renderSourceCreators(catalog.creators, labels);
    if (creators.length > 0) head.push(creators);
    const params = renderSourceParams(catalog.params);
    if (params.length > 0) head.push(params);
    if (catalog.seed !== undefined) head.push(`SEED ${renderLiteral(catalog.seed)}`);
    if (catalog.hashAlgorithm !== undefined) head.push(`HASH ALGORITHM ${renderLiteral(catalog.hashAlgorithm)}`);
    const groups = catalog.groups.map((g) => renderSourceGroup(g, { labels }));
    const files = (catalog.files ?? []).map((f) => renderSourceFiles(f));
    return `${head.join(' ')} AS (\n  ${[...groups, ...files].join(',\n  ')}\n);`;
}

// BIND only when the alias isn't the group's name.
export function renderSourceFiles(files: SourceFiles, options?: { keyword?: 'FILES' | 'ADD FILES' }): string {
    const parts = [`${options?.keyword ?? 'FILES'} ${renderIdent(files.name)}`];
    if (files.alias !== files.group) parts.push(`BIND ${renderIdent(files.alias)} => ${renderIdent(files.group)}`);
    parts.push(`USING IDENTITIES ${renderIdent(files.idProvider)}`);
    parts.push(`ALLOW WRITE IF ${renderPredicate(files.canWrite)}`);
    return parts.join('\n    ');
}

function renderSourceRow(table: string, template: CatalogRowTemplate, labels?: SourceKeyLabels): string {
    const parts = Object.entries(template.values).map(([column, value]) => `${renderIdent(column)} = ${renderSourceValue(value, labels)}`);
    for (const [column, param] of Object.entries(template.params ?? {})) {
        parts.push(`${renderIdent(column)} = ${param.fn === 'publicKey' ? `publicKey(:${param.param})` : `:${param.param}`}`);
    }
    return `${renderIdent(table)} (${parts.join(', ')})`;
}

function renderSourceValue(value: json.Literal, labels?: SourceKeyLabels): string {
    if (typeof value === 'string') {
        const publicKey = labels?.publicKey?.(value);
        if (publicKey !== undefined) return `publicKey($${publicKey})`;
        const keyId = labels?.keyId?.(value);
        if (keyId !== undefined) return `$${keyId}`;
    }
    return renderLiteral(value);
}
