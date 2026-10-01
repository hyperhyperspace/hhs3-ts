import { json } from "@hyper-hyper-space/hhs3_json";
import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import {
    BundlePayload, CatalogDeclarePayload, CatalogFilesDef, CatalogGroupDef, CatalogParamDecl, CatalogReleasePayload, CatalogRowTemplate,
    ColumnDef, CreateRCatalogPayload, CreateRDbPayload, CreateRSchemaPayload,
    CreateTableGroupPayload, InsertRowPayload, MigrationRule, ParamValue, Predicate,
    RowEnvelopePayload, RowOpPayload, SchemaUpdatePayload, TableDef, UpdateCatalogPayload, UpdateRowPayload,
    RCATALOG_TYPE_ID, RDB_TYPE_ID, RSCHEMA_TYPE_ID, RTABLE_GROUP_TYPE_ID,
    catalogGroupHash, formatPredicate, renderStringLiteral, versionKey,
} from "@hyper-hyper-space/hhs3_rdb";
import type { RefAdvancePayload } from "@hyper-hyper-space/hhs3_mvt";

import { KEYWORDS } from "../syntax/tokens.js";
import type { RenderAliasContext, RenderVersionScope } from "./aliases.js";

export type DumpRenderProfile = 'full' | 'schema';

// Renders a name, possibly '.' / ':' qualified, double-quoting each part that
// spells a keyword so it lexes back as an identifier.
export function renderIdent(name: string): string {
    return name.split(/([.:])/)
        .map((part) => (part !== '.' && part !== ':' && KEYWORDS.has(part.toUpperCase()) ? `"${part}"` : part))
        .join('');
}

export function renderPredicate(pred: Predicate, gatedTable?: string): string {
    return formatPredicate(pred, { gatedTable, quoteIdent: renderIdent });
}

export type RenderOptions = {
    profile?: DumpRenderProfile;
    aliasMode?: boolean;
    aliases?: RenderAliasContext;
    versionScope?: RenderVersionScope;
    refVersionScope?: RenderVersionScope;
    at?: json.Set;
    schemaRef?: B64Hash;
    schemaName?: string;
    databaseRef?: B64Hash;
    databaseName?: string;
    catalogRef?: B64Hash;
    catalogName?: string;
    groupRef?: B64Hash;
    groupName?: string;
    // The rendered group reference for row writes and ON clauses (a database
    // dump uses member names, resolved within its USE DATABASE); when set,
    // table targets are qualified with it.
    groupTarget?: string;
    tableName?: string;
    resolveSchemaName?: (id: B64Hash) => string | undefined;
    resolveGroupName?: (id: B64Hash) => string | undefined;
    // The catalog group definitions a release's body can refer to (those of
    // its parents); bindings and change targets render by name when unique.
    catalogDefs?: Map<B64Hash, CatalogGroupDef>;
    // Group deploys: version key (sorted hashes, comma-joined) -> the deployed
    // release that pins it, e.g. "editor 1.1.0". Host-provided.
    deployLabels?: { [versionKey: string]: string };
    comments?: boolean;
};

function isFullProfile(options?: RenderOptions): boolean {
    return options?.profile !== 'schema';
}

function useAliases(options?: RenderOptions): boolean {
    return options?.aliasMode === true && options.aliases !== undefined;
}

function useComments(options?: RenderOptions): boolean {
    return options?.comments !== false;
}

function schemaVersionScope(options?: RenderOptions): RenderVersionScope | undefined {
    if (options?.schemaRef === undefined) return undefined;
    return { objectId: options.schemaRef, objectName: options.schemaName ?? 'schema' };
}

function renderObjectRef(
    scope: 'schema' | 'group' | 'catalog' | 'db',
    id: B64Hash,
    hint: string | undefined,
    options?: RenderOptions,
): string {
    if (useAliases(options) && isFullProfile(options)) {
        const aliases = options!.aliases!;
        switch (scope) {
            case 'schema': return aliases.schema(id, hint);
            case 'group': return aliases.group(id, hint);
            case 'db': return aliases.db(id, hint);
            case 'catalog': return aliases.catalog !== undefined ? aliases.catalog(id, hint) : `#${id}`;
        }
    }
    if (isFullProfile(options)) return `#${id}`;
    const name = scope === 'schema'
        ? options?.resolveSchemaName?.(id) ?? hint
        : scope === 'group' ? options?.resolveGroupName?.(id) ?? hint : hint;
    return name !== undefined ? renderIdent(name) : `#${id}`;
}

function renderGroupTarget(options?: RenderOptions): string {
    if (options?.groupTarget !== undefined) return options.groupTarget;
    if (options?.groupRef === undefined) return '<group>';
    return renderObjectRef('group', options.groupRef, options.groupName, options);
}

function catalogVersionScope(options?: RenderOptions): RenderVersionScope | undefined {
    if (options?.catalogRef === undefined) return undefined;
    return { objectId: options.catalogRef, objectName: options.catalogName ?? 'catalog' };
}

function renderCatalogTarget(catalogRef: B64Hash, options?: RenderOptions): string {
    return renderObjectRef('catalog', catalogRef, options?.catalogName, options);
}

// A release selection: always a hash (an alias in aliasMode), never a semver,
// since concurrent releases can share a version.
function renderRelease(release: B64Hash, options?: RenderOptions): string {
    return renderVersionSet(json.toSet([release]), catalogVersionScope(options), options);
}

export function renderCreateDatabase(payload: CreateRDbPayload, options?: RenderOptions): string {
    const name = payload.name !== undefined ? renderIdent(payload.name) : payload.seed;
    const catalogOptions: RenderOptions = { ...options, catalogRef: payload.catalog };
    const parts = [
        `CREATE DATABASE ${name} USING CATALOG ${renderCatalogTarget(payload.catalog, catalogOptions)} AT ${renderRelease(payload.release, catalogOptions)}`,
    ];
    if (isFullProfile(options) && payload.seed !== undefined && payload.seed.length > 0) parts.push(`SEED ${renderStringLiteral(payload.seed)}`);
    const creators = renderCreators(payload.creators ?? [], options).trimStart();
    if (creators.length > 0) parts.push(creators);
    const params = renderParamAssignments(payload.params, options);
    if (params.length > 0) parts.push(params);
    if (payload.hashAlgorithm !== undefined) parts.push(renderHashAlgorithm(payload.hashAlgorithm).trimStart());
    return `${parts.join('\n  ')};`;
}

export function renderUseDatabase(id: B64Hash, name: string | undefined, options?: RenderOptions): string {
    return `USE DATABASE ${renderObjectRef('db', id, name, options)};`;
}

export function renderUpdateCatalog(payload: UpdateCatalogPayload, options?: RenderOptions): string {
    const catalogOptions: RenderOptions = { ...options, catalogRef: payload.catalog };
    const db = options?.databaseRef !== undefined
        ? renderObjectRef('db', options.databaseRef, options.databaseName, options)
        : options?.databaseName !== undefined ? renderIdent(options.databaseName) : '<database>';
    const params = renderParamAssignments(payload.params, options);
    return `UPDATE CATALOG ${renderCatalogTarget(payload.catalog, catalogOptions)} TO ${renderRelease(payload.release, catalogOptions)} ON ${db}`
        + `${params.length > 0 ? ` ${params}` : ''}${renderNote(payload.note)}${renderBy(payload.author, options)};`;
}

function renderParamAssignments(params: { [name: string]: ParamValue } | undefined, options?: RenderOptions): string {
    const entries = Object.entries(params ?? {}).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
    if (entries.length === 0) return '';
    const rendered = entries.map(([name, value]) => `:${name} = ${renderParamValue(value, options)}`);
    return `WITH PARAMS (${rendered.join(', ')})`;
}

// An identity param spells out the public key (self-certifying), or the key
// alias in aliasMode.
function renderParamValue(value: ParamValue, options?: RenderOptions): string {
    if ('identity' in value) {
        if (useAliases(options)) return `$${options!.aliases!.key(value.identity.keyId as B64Hash)}`;
        return `publicKey(${renderStringLiteral(value.identity.publicKey)})`;
    }
    return renderLiteral(value.value);
}

export function renderCreateCatalog(payload: CreateRCatalogPayload, options?: RenderOptions): string {
    const head = [`CREATE CATALOG ${renderIdent(payload.name)}`];
    const creators = renderCreators(payload.creators, options).trimStart();
    if (creators.length > 0) head.push(creators);
    head.push(`VERSION ${renderStringLiteral(payload.version)}`);
    const params = renderParamDecls(payload.params);
    if (params.length > 0) head.push(params);
    if (payload.seed !== undefined) head.push(`SEED ${renderStringLiteral(payload.seed)}`);
    if (payload.hashAlgorithm !== undefined) head.push(renderHashAlgorithm(payload.hashAlgorithm).trimStart());

    const defs = new Map<B64Hash, CatalogGroupDef>();
    const groups = (payload.add ?? []).map((def) => {
        const rendered = renderCatalogGroupDef('TABLEGROUP', def, defs, options);
        defs.set(catalogGroupHash(def), def);
        return rendered;
    });
    const files = (payload.files ?? []).map((def) => renderCatalogFilesDef('FILES', def, defs));
    const items = [...groups, ...files];
    return `${head.join(' ')} AS (\n  ${items.join(',\n  ')}\n)${renderNote(payload.note)}${renderBy(payload.author, options)};`;
}

// The release payload rendered literally: its `changes` and `add` become the
// body, and the trailing AT is its insertion point (explicit hashes, never
// LATEST; for a release on a declare, the declare's position, since the
// declare is regenerated on replay).
export function renderAlterCatalog(payload: CatalogReleasePayload, options?: RenderOptions): string {
    const target = options?.catalogRef !== undefined ? renderCatalogTarget(options.catalogRef, options) : '<catalog>';
    const head = [`ALTER CATALOG ${target}`, `VERSION ${renderStringLiteral(payload.version)}`];
    const params = renderParamDecls(payload.params);
    if (params.length > 0) head.push(params);

    const defs = new Map<B64Hash, CatalogGroupDef>(options?.catalogDefs ?? []);
    const body: string[] = [];
    for (const [hash, change] of Object.entries(payload.changes ?? {}).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))) {
        const schemaName = options?.resolveSchemaName?.(change.schema);
        const schema = renderObjectRef('schema', change.schema, schemaName, options);
        const version = renderVersionSet(change.version, { objectId: change.schema, objectName: schemaName ?? 'schema' }, options);
        body.push(`UPDATE SCHEMA ${schema} TO ${version} ON ${renderDefRef(hash, defs)}`);
    }
    for (const def of payload.add ?? []) {
        body.push(renderCatalogGroupDef('ADD TABLEGROUP', def, defs, options));
        defs.set(catalogGroupHash(def), def);
    }
    for (const def of payload.files ?? []) body.push(renderCatalogFilesDef('ADD FILES', def, defs));
    const asBody = body.length > 0 ? ` AS (\n  ${body.join(',\n  ')}\n)` : '';
    return `${head.join(' ')}${asBody}${renderNote(payload.note)}${renderBy(payload.author, options)}${renderAt({ ...options, versionScope: catalogVersionScope(options) ?? options?.versionScope })};`;
}

export function renderDeclare(payload: CatalogDeclarePayload, options?: RenderOptions): string {
    const schemas = [...json.fromSet(payload.schemas)].map((id) => renderObjectRef('schema', id, options?.resolveSchemaName?.(id), options));
    return `-- declare schemas {${schemas.join(', ')}}`;
}

function renderParamDecls(params: CatalogParamDecl[] | undefined): string {
    if (params === undefined || params.length === 0) return '';
    return `PARAMS (${params.map((p) => `:${p.name} ${p.type}`).join(', ')})`;
}

// A definition by name when no other visible definition shares it; by full
// hash otherwise (a merge can join two definitions with the same name).
function renderDefRef(hash: B64Hash, defs: Map<B64Hash, CatalogGroupDef>): string {
    const def = defs.get(hash);
    if (def === undefined) return `#${hash}`;
    const clashes = [...defs.values()].filter((d) => d.name === def.name).length;
    return clashes === 1 ? renderIdent(def.name) : `#${hash}`;
}

function renderCatalogGroupDef(
    keyword: 'TABLEGROUP' | 'ADD TABLEGROUP',
    def: CatalogGroupDef,
    defs: Map<B64Hash, CatalogGroupDef>,
    options?: RenderOptions,
): string {
    const schemaName = options?.resolveSchemaName?.(def.schemaRef);
    const schema = renderObjectRef('schema', def.schemaRef, schemaName, options);
    const pin = renderVersionSet(def.schemaVersion, { objectId: def.schemaRef, objectName: schemaName ?? 'schema' }, options);
    const parts = [`${keyword} ${renderIdent(def.name)} USING SCHEMA ${schema} AT ${pin}`];
    const bindings = Object.entries(def.bindings ?? {});
    if (bindings.length > 0) {
        parts.push(`BIND ${bindings.map(([alias, hash]) => `${renderIdent(alias)} => ${renderDefRef(hash, defs)}`).join(', ')}`);
    }
    if (def.idProvider !== undefined) parts.push(`USING IDENTITIES ${renderIdent(def.idProvider)}`);
    if (def.canDeploy !== undefined) parts.push(`ALLOW DEPLOY IF ${renderPredicate(def.canDeploy)}`);
    for (const [binding, pred] of Object.entries(def.canObserve ?? {})) {
        parts.push(`ALLOW UPDATE REF ${renderIdent(binding)} IF ${renderPredicate(pred)}`);
    }
    const rows: string[] = [];
    for (const [table, templates] of Object.entries(def.initialRows ?? {})) {
        for (const template of templates) rows.push(renderRowTemplate(table, template, options));
    }
    if (rows.length > 0) parts.push(`WITH ROWS (\n      ${rows.join(',\n      ')}\n    )`);
    return parts.join('\n    ');
}

// BIND only when the alias isn't the group's name, or the name is ambiguous
// among `defs`; otherwise the qualifier of USING IDENTITIES names the group.
export function renderCatalogFilesDef(
    keyword: 'FILES' | 'ADD FILES',
    def: CatalogFilesDef,
    defs: Map<B64Hash, CatalogGroupDef>,
): string {
    const [alias, hash] = Object.entries(def.bindings)[0];
    const ref = renderDefRef(hash, defs);
    const parts = [`${keyword} ${renderIdent(def.name)}`];
    if (defs.get(hash)?.name !== alias || ref.startsWith('#')) parts.push(`BIND ${renderIdent(alias)} => ${ref}`);
    parts.push(`USING IDENTITIES ${renderIdent(def.idProvider)}`);
    parts.push(`ALLOW WRITE IF ${renderPredicate(def.canWrite)}`);
    return parts.join('\n    ');
}

function renderRowTemplate(table: string, template: CatalogRowTemplate, options?: RenderOptions): string {
    const parts = Object.entries(template.values).map(([k, v]) => renderRowValue(k, v, options));
    for (const [column, param] of Object.entries(template.params ?? {})) {
        parts.push(`${renderIdent(column)}=${param.fn === 'publicKey' ? `publicKey(:${param.param})` : `:${param.param}`}`);
    }
    return `${renderIdent(table)} (${parts.join(', ')})`;
}

// A group's genesis is computed by its database from a catalog definition,
// so it has no statement of its own.
export function renderGroupGenesis(payload: CreateTableGroupPayload, options?: RenderOptions): string {
    const schema = renderObjectRef('schema', payload.schemaRef, options?.schemaName ?? options?.resolveSchemaName?.(payload.schemaRef), options);
    return `-- TABLEGROUP ${renderIdent(payload.name)} USING SCHEMA ${schema} AT ${renderVersionSet(payload.schemaVersion, schemaVersionScope(options), options)} (created by its database)`;
}

export function renderCreateSchema(payload: CreateRSchemaPayload, options?: RenderOptions): string {
    const creators = renderCreators(payload.creators, options);
    const tables = payload.tables.map(renderTableDef).join(',\n  ');
    return `CREATE SCHEMA ${renderIdent(payload.name)}${creators} VERSION ${renderStringLiteral(payload.version)}${renderHashAlgorithm(payload.hashAlgorithm)} AS (\n  ${tables}\n);`;
}

export function renderSchemaUpdate(payload: SchemaUpdatePayload, options?: RenderOptions): string {
    const rules = payload.migration.map(renderMigrationRule).join(',\n  ');
    const schemaRef = options?.schemaRef ?? 'unknown';
    const comment = useComments(options) && options?.schemaName !== undefined ? `-- ${options.schemaName}\n` : '';
    const schemaTarget = schemaRef === 'unknown'
        ? schemaRef
        : (useAliases(options) && isFullProfile(options)
            ? renderObjectRef('schema', schemaRef as B64Hash, options?.schemaName, options)
            : `#${schemaRef}`);
    return `${comment}ALTER SCHEMA ${schemaTarget} VERSION ${renderStringLiteral(payload.version)} AS (\n  ${rules}\n)${renderNote(payload.note)}${renderBy(payload.author, options)}${renderAt(options)};`;
}

export function renderRowOp(payload: RowOpPayload, table?: string, options?: RenderOptions): string {
    const bare = table !== undefined ? renderIdent(table) : '<table>';
    const target = options?.groupTarget !== undefined ? `${options.groupTarget}.${bare}` : bare;
    if (payload.action === 'insert') {
        const insert = payload as InsertRowPayload;
        const cols = Object.keys(insert.values).map(renderIdent);
        const vals = Object.values(insert.values).map(renderLiteral);
        if (isFullProfile(options) && insert.uuid !== undefined) {
            return `INSERT INTO ${target} (uuid, ${cols.join(', ')}) VALUES (${renderStringLiteral(insert.uuid)}, ${vals.join(', ')})${renderBy(insert.author, options)}${renderAt(options)};`;
        }
        return `INSERT INTO ${target} (${cols.join(', ')}) VALUES (${vals.join(', ')})${renderBy(insert.author, options)}${renderAt(options)};`;
    }
    if (payload.action === 'update') {
        const update = payload as UpdateRowPayload;
        const values = Object.entries(update.values).map(([k, v]) => `${renderIdent(k)} = ${renderLiteral(v)}`).join(', ');
        return `UPDATE ${target} SET ${values} WHERE rowId = #${update.rowId}${renderBy(update.author, options)}${renderAt(options)};`;
    }
    return `DELETE FROM ${target} WHERE rowId = #${payload.rowId}${renderBy(payload.author, options)}${renderAt(options)};`;
}

export function isSchemaDeployPayload(payload: json.Literal, schemaRef?: B64Hash): boolean {
    if (!isObject(payload) || payload['action'] !== 'ref-advance') return false;
    return payload['gate'] !== undefined || (schemaRef !== undefined && payload['refId'] === schemaRef);
}

export function renderRefOp(payload: RefAdvancePayload, options?: RenderOptions): string {
    if (isSchemaDeployPayload(payload as unknown as json.Literal, options?.schemaRef)) return renderDeployComment(payload, options);
    const author = (payload as { author?: string }).author;
    const trailing = `${renderBy(author, options)}${renderAt(options)}`;
    const refVersionScope = options?.refVersionScope ?? {
        objectId: payload.refId,
        objectName: options?.resolveGroupName?.(payload.refId) ?? 'group',
    };
    const version = renderVersionSet(payload.refVersion, refVersionScope, options);
    const refTarget = renderObjectRef('group', payload.refId, options?.resolveGroupName?.(payload.refId), options);
    return `UPDATE REF ${refTarget} TO ${version} ON ${renderGroupTarget(options)}${trailing};`;
}

// A schema deploy has no statement: the catalog planner makes it (UPDATE
// CATALOG). The label names the deployed release that pins the version, when
// the host knows it.
function renderDeployComment(payload: RefAdvancePayload, options?: RenderOptions): string {
    const schemaName = options?.schemaName ?? options?.resolveSchemaName?.(payload.refId);
    const schema = schemaName !== undefined ? renderIdent(schemaName) : `#${payload.refId}`;
    const version = renderVersionSet(payload.refVersion, schemaVersionScope(options), { ...options, aliasMode: false });
    let label = '';
    if (options?.deployLabels !== undefined) {
        const found = options.deployLabels[versionKey(json.fromSet(payload.refVersion))];
        label = found !== undefined ? ` (${found})` : ' (no deployed release pins this version)';
    }
    return `-- deploy ${schema} TO ${version}${label}`;
}

export function renderBundle(payload: BundlePayload, options?: RenderOptions): string {
    const writes = payload.writes.map((w) => {
        const op = w.op as unknown as RowOpPayload;
        const { author: _author, ...innerOp } = op as RowOpPayload & { author?: string };
        return renderRowOp(innerOp as RowOpPayload, w.table, { ...options, groupTarget: undefined });
    }).join('\n  ');
    const author = (payload as { author?: string }).author;
    return `BUNDLE ON ${renderGroupTarget(options)} (\n  ${writes}\n)${renderBy(author, options)}${renderAt(options)};`;
}

function isRowOpAction(action: unknown): action is 'insert' | 'update' | 'delete' {
    return action === 'insert' || action === 'update' || action === 'delete';
}

function renderTableScopePayload(
    payload: json.LiteralMap,
    tableName: string,
    options?: RenderOptions,
): string | undefined {
    const action = payload['action'];
    if (isRowOpAction(action)) {
        return renderRowOp(payload as unknown as RowOpPayload, tableName, options);
    }
    if (action === 'rows' && Array.isArray(payload['ops'])) {
        return (payload['ops'] as RowOpPayload[])
            .map((op) => renderRowOp(op, tableName, options))
            .join('\n');
    }
    return undefined;
}

export function renderOp(payload: json.Literal, options?: RenderOptions): string {
    if (!isObject(payload)) return `-- unknown payload ${json.toStringNormalized(payload)}`;
    if (payload['action'] === 'create' && payload['type'] === RDB_TYPE_ID) {
        return renderCreateDatabase(payload as CreateRDbPayload, options);
    }
    if (payload['action'] === 'create' && payload['type'] === RSCHEMA_TYPE_ID) {
        return renderCreateSchema(payload as CreateRSchemaPayload, options);
    }
    if (payload['action'] === 'create' && payload['type'] === RCATALOG_TYPE_ID) {
        return renderCreateCatalog(payload as unknown as CreateRCatalogPayload, options);
    }
    if (payload['action'] === 'create' && payload['type'] === RTABLE_GROUP_TYPE_ID) {
        return renderGroupGenesis(payload as CreateTableGroupPayload, options);
    }
    if (payload['action'] === 'release') return renderAlterCatalog(payload as unknown as CatalogReleasePayload, options);
    if (payload['action'] === 'declare') return renderDeclare(payload as unknown as CatalogDeclarePayload, options);
    if (payload['action'] === 'update-catalog') return renderUpdateCatalog(payload as unknown as UpdateCatalogPayload, options);
    if (payload['action'] === 'schema-update') return renderSchemaUpdate(payload as unknown as SchemaUpdatePayload, options);
    if (payload['action'] === 'row') {
        const row = payload as unknown as RowEnvelopePayload;
        return renderRowOp(row.op as unknown as RowOpPayload, row.table, options);
    }
    if (payload['action'] === 'bundle') return renderBundle(payload as unknown as BundlePayload, options);
    if (payload['action'] === 'ref-advance') return renderRefOp(payload as unknown as RefAdvancePayload, options);
    if (options?.tableName !== undefined) {
        const rendered = renderTableScopePayload(payload, options.tableName, options);
        if (rendered !== undefined) return rendered;
    }
    return `-- unknown payload ${json.toStringNormalized(payload)}`;
}

export function renderTableDef(table: TableDef): string {
    const colIndent = '    ';
    const colLines = Object.entries(table.columns)
        .map(([name, def]) => `${colIndent}${renderColumnDef(name, def, table.fks?.[name])}`);
    const cols = colLines.length === 0 ? '' : `\n${colLines.join(',\n')}\n  `;

    return `TABLE ${renderIdent(table.name)} (${cols})${renderTableOptions(table)}`;
}

// What follows a table's column list: its concurrent-deletes and
// identity-provider flags, then one ALLOW line per restriction. Empty when the
// table has none; otherwise it starts with its separator (a space or a line
// break).
export function renderTableOptions(table: TableDef): string {
    const structural: string[] = [];
    if (table.concurrentDeletes !== undefined) {
        structural.push(table.concurrentDeletes ? 'CONCURRENT DELETES' : 'NO CONCURRENT DELETES');
    }
    if (table.idProvider !== undefined) {
        const provider = table.idProvider.keyIdColumn === 'keyId' && table.idProvider.publicKeyColumn === 'publicKey'
            ? 'IDENTITY PROVIDER'
            : `IDENTITY PROVIDER (${renderIdent(table.idProvider.keyIdColumn)}, ${renderIdent(table.idProvider.publicKeyColumn)})`;
        structural.push(provider);
    }

    const allows = (table.restrictions ?? [])
        .map((r) => `ALLOW ${r.on} IF ${renderPredicate(r.rule, table.name)}`);

    let suffix = '';
    if (structural.length > 0) {
        suffix = ` ${structural.join(' ')}`;
    }
    if (allows.length > 0) {
        suffix += allows.map((allow) => `\n    ${allow}`).join('');
    }

    return suffix;
}

export function renderColumnDef(name: string, def: ColumnDef, fk?: string): string {
    const parts = [renderIdent(name), renderColumnType(def)];
    if (def.nullable) parts.push('NULL');
    if (def.default !== undefined) parts.push(`DEFAULT ${renderLiteral(def.default)}`);
    if (def.pub) parts.push('PUB');
    if (def.readonly) parts.push('READONLY');
    if (fk !== undefined) parts.push(`REFERENCES ${renderIdent(fk)}`);
    const c = def.constraints;
    if (c?.min !== undefined) parts.push(`MIN ${renderStringLiteral(c.min)}`);
    if (c?.max !== undefined) parts.push(`MAX ${renderStringLiteral(c.max)}`);
    return parts.join(' ');
}

// Render the column type with its parenthesized params: string(n) / bytes(n)
// carry maxLength; decimal(p, s) carries precision + scale (SQL-standard order),
// with `*` for an absent precision.
function renderColumnType(def: ColumnDef): string {
    const c = def.constraints;
    switch (def.type) {
        case 'string':
            return c?.maxLength !== undefined ? `string(${c.maxLength})` : 'string';
        case 'bytes':
            return c?.maxLength !== undefined ? `bytes(${c.maxLength})` : 'bytes';
        case 'decimal':
            return `decimal(${c?.precision ?? '*'}, ${c?.scale ?? 0})`;
        case 'identity':
            return 'identity';
        default:
            return def.type;
    }
}

export function renderMigrationRule(rule: MigrationRule): string {
    switch (rule.rule) {
        case 'add-table':
            return `ADD TABLE ${renderTableDef(rule.def).replace(/^TABLE /, '')}`;
        case 'drop-table':
            return `DROP TABLE ${renderIdent(rule.table)}`;
        case 'add-column':
            return `ADD COLUMN ${renderIdent(rule.table)}.${renderColumnDef(rule.column, rule.def)}`;
        case 'drop-column':
            return `DROP COLUMN ${renderIdent(rule.table)}.${renderIdent(rule.column)}`;
        case 'set-concurrent-deletes':
            return `SET CONCURRENT DELETES ${renderIdent(rule.table)} ${String(rule.value)}`;
        case 'set-fks':
            return `SET FKS ${renderIdent(rule.table)} (${Object.entries(rule.fks).map(([c, r]) => `${renderIdent(c)} REFERENCES ${renderIdent(r)}`).join(', ')})`;
        case 'set-restrictions':
            return `SET ALLOW RULES ${renderIdent(rule.table)} (\n    ${rule.restrictions.map((r) => `ALLOW ${r.on} IF ${renderPredicate(r.rule, rule.table)}`).join(',\n    ')}\n  )`;
    }
}

function renderRowValue(column: string, value: json.Literal, options?: RenderOptions): string {
    const col = renderIdent(column);
    if (!useAliases(options) || typeof value !== 'string') {
        return `${col}=${renderLiteral(value)}`;
    }
    const aliases = options!.aliases!;
    if (column === 'publicKey' && aliases.lookupPublicKeyAlias !== undefined) {
        const name = aliases.lookupPublicKeyAlias(value);
        if (name !== undefined) return `${col}=publicKey($${name})`;
    }
    if (aliases.lookupKeyAlias !== undefined) {
        const name = aliases.lookupKeyAlias(value as B64Hash);
        if (name !== undefined) return `${col}=$${name}`;
    }
    return `${col}=${renderLiteral(value)}`;
}

export function renderLiteral(value: json.Literal): string {
    if (typeof value === 'string') return renderStringLiteral(value);
    if (typeof value === 'object') return `JSON ${renderStringLiteral(json.toStringCanonical(value))}`;
    return json.toStringCanonical(value);
}

function renderNote(note?: string): string {
    return note === undefined ? '' : ` NOTE ${renderStringLiteral(note)}`;
}

function renderHashAlgorithm(hashAlgorithm?: string): string {
    return hashAlgorithm === undefined ? '' : ` HASH ALGORITHM ${renderStringLiteral(hashAlgorithm)}`;
}

function renderAt(options?: RenderOptions): string {
    return options?.at === undefined ? '' : ` AT ${renderVersionSet(options.at, options.versionScope, options)}`;
}

function renderBy(author?: string, options?: RenderOptions): string {
    if (author === undefined) return '';
    if (useAliases(options)) return ` BY $${options!.aliases!.key(author as B64Hash)}`;
    return ` BY #${author}`;
}

// The full profile spells out each creator's public key, so replay needs no
// keystore; the schema profile references the key id.
function renderCreators(creators: { keyId: string; publicKey: string }[], options?: RenderOptions): string {
    if (creators.length === 0) return '';
    const names = creators.map((c) => {
        if (useAliases(options)) return `$${options!.aliases!.key(c.keyId as B64Hash)}`;
        if (isFullProfile(options)) return `publicKey(${renderStringLiteral(c.publicKey)})`;
        return renderStringLiteral(c.keyId);
    });
    return ` CREATORS (${names.join(', ')})`;
}

function renderVersionSet(set: json.Set, scope: RenderVersionScope | undefined, options?: RenderOptions): string {
    if (useAliases(options) && scope !== undefined) {
        const names = [...json.fromSet(set)].map((h) => options!.aliases!.version(h, scope));
        return `{${names.join(', ')}}`;
    }
    return `{${[...json.fromSet(set)].map((h) => `#${h}`).join(', ')}}`;
}

function isObject(value: unknown): value is { [key: string]: unknown } {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}
