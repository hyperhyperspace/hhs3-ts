import { json } from "@hyper-hyper-space/hhs3_json";
import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import {
    AddGroupPayload, AddSchemaPayload, BundlePayload, ColumnDef, CreateRDbPayload, CreateRSchemaPayload,
    CreateTableGroupPayload, InsertRowPayload, MigrationRule,
    RowEnvelopePayload, RowOpPayload, SchemaUpdatePayload, TableDef, UpdateRowPayload,
    formatPredicate,
} from "@hyper-hyper-space/hhs3_rdb";
import type { RefAdvancePayload } from "@hyper-hyper-space/hhs3_mvt";

import type { RenderAliasContext, RenderVersionScope } from "./aliases.js";

export type DumpRenderProfile = 'full' | 'schema';

export type RenderOptions = {
    profile?: DumpRenderProfile;
    aliasMode?: boolean;
    aliases?: RenderAliasContext;
    versionScope?: RenderVersionScope;
    refVersionScope?: RenderVersionScope;
    at?: json.Set;
    schemaRef?: B64Hash;
    schemaName?: string;
    databaseName?: string;
    groupRef?: B64Hash;
    groupName?: string;
    resolveSchemaName?: (id: B64Hash) => string | undefined;
    resolveGroupName?: (id: B64Hash) => string | undefined;
};

function isFullProfile(options?: RenderOptions): boolean {
    return options?.profile !== 'schema';
}

function useAliases(options?: RenderOptions): boolean {
    return options?.aliasMode === true && options.aliases !== undefined;
}

function schemaVersionScope(options?: RenderOptions): RenderVersionScope | undefined {
    if (options?.schemaRef === undefined) return undefined;
    return { objectId: options.schemaRef, objectName: options.schemaName ?? 'schema' };
}

function renderObjectRef(
    scope: 'schema' | 'group',
    id: B64Hash,
    hint: string | undefined,
    options?: RenderOptions,
): string {
    if (useAliases(options) && isFullProfile(options)) {
        const aliases = options!.aliases!;
        return scope === 'schema' ? aliases.schema(id, hint) : aliases.group(id, hint);
    }
    if (isFullProfile(options)) return `#${id}`;
    if (scope === 'schema') return options?.resolveSchemaName?.(id) ?? `#${id}`;
    return options?.resolveGroupName?.(id) ?? hint ?? `#${id}`;
}

function renderGroupTarget(options?: RenderOptions): string {
    if (options?.groupRef === undefined) return '<group>';
    return renderObjectRef('group', options.groupRef, options.groupName, options);
}

export function renderCreateDatabase(payload: CreateRDbPayload, options?: RenderOptions): string {
    const creators = renderCreators(payload.creators ?? [], options);
    const seed = isFullProfile(options) && payload.seed !== undefined && payload.seed.length > 0
        ? ` SEED ${sqlString(payload.seed)}`
        : '';
    return `CREATE DATABASE ${payload.name ?? payload.seed}${seed}${creators};`;
}

export function renderCreateSchema(payload: CreateRSchemaPayload, options?: RenderOptions): string {
    const creators = renderCreators(payload.creators, options);
    const tables = payload.tables.map(renderTableDef).join(',\n  ');
    return `CREATE SCHEMA ${payload.name}${creators} AS (\n  ${tables}\n);`;
}

export function renderCreateTableGroup(payload: CreateTableGroupPayload, options?: RenderOptions): string {
    const schemaComment = options?.schemaName !== undefined && !isFullProfile(options)
        ? `-- ${options.schemaName}\n`
        : '';
    const seed = isFullProfile(options) && payload.seed !== undefined && payload.seed.length > 0
        ? ` SEED ${sqlString(payload.seed)}`
        : '';
    const schemaRef = renderObjectRef(
        'schema',
        payload.schemaRef,
        options?.schemaName ?? options?.resolveSchemaName?.(payload.schemaRef),
        options,
    );
    const parts = [
        `${schemaComment}CREATE TABLEGROUP ${payload.name}${seed} USING SCHEMA ${schemaRef} AT ${renderVersionSet(payload.schemaVersion, schemaVersionScope(options), options)}`,
    ];
    for (const [name, id] of Object.entries(payload.bindings ?? {})) {
        if (isFullProfile(options)) {
            parts.push(`BIND ${name} => #${id}`);
        } else {
            const groupName = options?.resolveGroupName?.(id) ?? name;
            parts.push(`BIND ${name} => ${groupName}`);
        }
    }
    if (payload.idProvider !== undefined) parts.push(`USING IDENTITIES ${payload.idProvider}`);
    if (payload.canDeploy !== undefined) parts.push(`ALLOW UPDATE SCHEMA IF ${formatPredicate(payload.canDeploy)}`);
    for (const [binding, pred] of Object.entries(payload.canObserve ?? {})) {
        parts.push(`ALLOW UPDATE REF ${binding} IF ${formatPredicate(pred)}`);
    }
    if (payload.initialRows !== undefined) {
        const rows: string[] = [];
        for (const [table, tableRows] of Object.entries(payload.initialRows)) {
            for (const row of tableRows) {
                rows.push(renderInitialRow(table, row as unknown as InsertRowPayload, options));
            }
        }
        if (rows.length > 0) parts.push(`WITH ROWS (\n  ${rows.join(',\n  ')}\n)`);
    }
    return `${parts.join('\n  ')};`;
}

export function renderAddSchema(payload: AddSchemaPayload, options?: RenderOptions): string {
    const target = renderObjectRef(
        'schema',
        payload.schemaId,
        options?.resolveSchemaName?.(payload.schemaId),
        options,
    );
    const db = options?.databaseName ?? '<database>';
    return `ADD SCHEMA ${target} TO ${db}${renderNote(payload.note)}${renderBy(payload.author, options)}${isFullProfile(options) ? renderAt(options) : ''};`;
}

export function renderAddGroup(payload: AddGroupPayload, options?: RenderOptions): string {
    const target = renderObjectRef(
        'group',
        payload.groupId,
        options?.resolveGroupName?.(payload.groupId),
        options,
    );
    const db = options?.databaseName ?? '<database>';
    return `ADD TABLEGROUP ${target} TO ${db}${renderNote(payload.note)}${renderBy(payload.author, options)}${isFullProfile(options) ? renderAt(options) : ''};`;
}

export function renderSchemaUpdate(payload: SchemaUpdatePayload, options?: RenderOptions): string {
    const rules = payload.migration.map(renderMigrationRule).join(',\n  ');
    const schemaRef = options?.schemaRef ?? 'unknown';
    const comment = options?.schemaName === undefined ? '' : `-- ${options.schemaName}\n`;
    const schemaTarget = schemaRef === 'unknown'
        ? schemaRef
        : (useAliases(options) && isFullProfile(options)
            ? renderObjectRef('schema', schemaRef as B64Hash, options?.schemaName, options)
            : `#${schemaRef}`);
    return `${comment}ALTER SCHEMA ${schemaTarget} AS (\n  ${rules}\n)${renderBy(payload.author, options)}${renderAt(options)};`;
}

export function renderRowOp(payload: RowOpPayload, table?: string, options?: RenderOptions): string {
    const target = table ?? '<table>';
    if (payload.action === 'insert') {
        const insert = payload as InsertRowPayload;
        const cols = Object.keys(insert.values);
        const vals = cols.map((c) => renderLiteral(insert.values[c]));
        if (isFullProfile(options) && insert.uuid !== undefined) {
            return `INSERT INTO ${target} (uuid, ${cols.join(', ')}) VALUES (${sqlString(insert.uuid)}, ${vals.join(', ')})${renderBy(insert.author, options)}${renderAt(options)};`;
        }
        return `INSERT INTO ${target} (${cols.join(', ')}) VALUES (${vals.join(', ')})${renderBy(insert.author, options)}${renderAt(options)};`;
    }
    if (payload.action === 'update') {
        const update = payload as UpdateRowPayload;
        const values = Object.entries(update.values).map(([k, v]) => `${k} = ${renderLiteral(v)}`).join(', ');
        return `UPDATE ${target} SET ${values} WHERE rowId = #${update.rowId}${renderBy(update.author, options)}${renderAt(options)};`;
    }
    return `DELETE FROM ${target} WHERE rowId = #${payload.rowId}${renderBy(payload.author, options)}${renderAt(options)};`;
}

export function renderRefOp(payload: RefAdvancePayload, options?: RenderOptions): string {
    const author = (payload as { author?: string }).author;
    const trailing = `${renderBy(author, options)}${renderAt(options)}`;
    const isSchemaDeploy = options?.schemaRef !== undefined && payload.refId === options.schemaRef;
    const refVersionScope = options?.refVersionScope ?? (isSchemaDeploy
        ? schemaVersionScope(options)
        : {
            objectId: payload.refId,
            objectName: options?.resolveGroupName?.(payload.refId) ?? 'group',
        });
    const version = renderVersionSet(payload.refVersion, refVersionScope, options);
    const group = renderGroupTarget(options);
    const refTarget = isSchemaDeploy
        ? renderObjectRef('schema', payload.refId, options?.schemaName, options)
        : renderObjectRef('group', payload.refId, options?.resolveGroupName?.(payload.refId), options);
    if (isSchemaDeploy) {
        return `UPDATE SCHEMA ${refTarget} TO ${version} ON ${group}${trailing};`;
    }
    return `UPDATE REF ${refTarget} TO ${version} ON ${group}${trailing};`;
}

export function renderBundle(payload: BundlePayload, options?: RenderOptions): string {
    const writes = payload.writes.map((w) => {
        const op = w.op as unknown as RowOpPayload;
        const { author: _author, ...innerOp } = op as RowOpPayload & { author?: string };
        return renderRowOp(innerOp as RowOpPayload, w.table, options);
    }).join('\n  ');
    const author = (payload as { author?: string }).author;
    return `BUNDLE ON ${renderGroupTarget(options)} (\n  ${writes}\n)${renderBy(author, options)}${renderAt(options)};`;
}

export function renderOp(payload: json.Literal, options?: RenderOptions): string {
    if (!isObject(payload)) return `-- unknown payload ${json.toStringNormalized(payload)}`;
    if (payload['action'] === 'create' && payload['type'] === 'hhs/rdb_v1') {
        return renderCreateDatabase(payload as CreateRDbPayload, options);
    }
    if (payload['action'] === 'create' && payload['type'] === 'hhs/rschema_v1') {
        return renderCreateSchema(payload as CreateRSchemaPayload, options);
    }
    if (payload['action'] === 'create' && payload['type'] === 'hhs/rtable_group_v1') {
        return renderCreateTableGroup(payload as CreateTableGroupPayload, options);
    }
    if (payload['action'] === 'add-schema') return renderAddSchema(payload as unknown as AddSchemaPayload, options);
    if (payload['action'] === 'add-group') return renderAddGroup(payload as unknown as AddGroupPayload, options);
    if (payload['action'] === 'schema-update') return renderSchemaUpdate(payload as unknown as SchemaUpdatePayload, options);
    if (payload['action'] === 'row') {
        const row = payload as unknown as RowEnvelopePayload;
        return renderRowOp(row.op as unknown as RowOpPayload, row.table, options);
    }
    if (payload['action'] === 'bundle') return renderBundle(payload as unknown as BundlePayload, options);
    if (payload['action'] === 'ref-advance') return renderRefOp(payload as unknown as RefAdvancePayload, options);
    return `-- unknown payload ${json.toStringNormalized(payload)}`;
}

function renderTableDef(table: TableDef): string {
    const colIndent = '    ';
    const colLines = Object.entries(table.columns)
        .map(([name, def]) => `${colIndent}${renderColumnDef(name, def, table.fks?.[name])}`);
    const cols = colLines.length === 0 ? '' : `\n${colLines.join(',\n')}\n  `;

    const structural: string[] = [];
    if (table.concurrentDeletes !== undefined) {
        structural.push(table.concurrentDeletes ? 'CONCURRENT DELETES' : 'NO CONCURRENT DELETES');
    }
    if (table.idProvider !== undefined) {
        const provider = table.idProvider.keyIdColumn === 'keyId' && table.idProvider.publicKeyColumn === 'publicKey'
            ? 'IDENTITY PROVIDER'
            : `IDENTITY PROVIDER (${table.idProvider.keyIdColumn}, ${table.idProvider.publicKeyColumn})`;
        structural.push(provider);
    }

    const allows = (table.restrictions ?? [])
        .map((r) => `ALLOW ${r.on} IF ${formatPredicate(r.rule, { gatedTable: table.name })}`);

    let suffix = '';
    if (structural.length > 0) {
        suffix = ` ${structural.join(' ')}`;
    }
    if (allows.length > 0) {
        suffix += allows.map((allow) => `\n    ${allow}`).join('');
    }

    return `TABLE ${table.name} (${cols})${suffix}`;
}

function renderColumnDef(name: string, def: ColumnDef, fk?: string): string {
    const parts = [name, def.type];
    if (def.nullable) parts.push('NULL');
    if (def.default !== undefined) parts.push(`DEFAULT ${renderLiteral(def.default)}`);
    if (def.pub) parts.push('PUB');
    if (def.readonly) parts.push('READONLY');
    if (fk !== undefined) parts.push(`REFERENCES ${fk}`);
    return parts.join(' ');
}

function renderMigrationRule(rule: MigrationRule): string {
    switch (rule.rule) {
        case 'add-table':
            return `ADD TABLE ${renderTableDef(rule.def).replace(/^TABLE /, '')}`;
        case 'drop-table':
            return `DROP TABLE ${rule.table}`;
        case 'add-column':
            return `ADD COLUMN ${rule.table}.${renderColumnDef(rule.column, rule.def)}`;
        case 'drop-column':
            return `DROP COLUMN ${rule.table}.${rule.column}`;
        case 'set-concurrent-deletes':
            return `SET CONCURRENT DELETES ${rule.table} ${String(rule.value)}`;
        case 'set-fks':
            return `SET FKS ${rule.table} (${Object.entries(rule.fks).map(([c, r]) => `${c} REFERENCES ${r}`).join(', ')})`;
        case 'set-restrictions':
            return `SET ALLOW RULES ${rule.table} (\n    ${rule.restrictions.map((r) => `ALLOW ${r.on} IF ${formatPredicate(r.rule, { gatedTable: rule.table })}`).join(',\n    ')}\n  )`;
    }
}

function renderInitialRow(table: string, row: InsertRowPayload, options?: RenderOptions): string {
    const parts = Object.entries(row.values).map(([k, v]) => renderRowValue(k, v, options));
    if (isFullProfile(options) && row.uuid !== undefined) {
        parts.unshift(`uuid=${sqlString(row.uuid)}`);
    }
    return `${table} (${parts.join(', ')})`;
}

function renderRowValue(column: string, value: json.Literal, options?: RenderOptions): string {
    if (!useAliases(options) || typeof value !== 'string') {
        return `${column}=${renderLiteral(value)}`;
    }
    const aliases = options!.aliases!;
    if (column === 'publicKey' && aliases.lookupPublicKeyAlias !== undefined) {
        const name = aliases.lookupPublicKeyAlias(value);
        if (name !== undefined) return `${column}=publicKey($${name})`;
    }
    if (aliases.lookupKeyAlias !== undefined) {
        const name = aliases.lookupKeyAlias(value as B64Hash);
        if (name !== undefined) return `${column}=$${name}`;
    }
    return `${column}=${renderLiteral(value)}`;
}

function renderLiteral(value: json.Literal): string {
    if (typeof value === 'string') return sqlString(value);
    return json.toStringNormalized(value);
}

function sqlString(value: string): string {
    return `'${value.replace(/'/g, "''")}'`;
}

function renderNote(note?: string): string {
    return note === undefined ? '' : ` NOTE ${sqlString(note)}`;
}

function renderAt(options?: RenderOptions): string {
    return options?.at === undefined ? '' : ` AT ${renderVersionSet(options.at, options.versionScope, options)}`;
}

function renderBy(author?: string, options?: RenderOptions): string {
    if (author === undefined) return '';
    if (useAliases(options)) return ` BY $${options!.aliases!.key(author as B64Hash)}`;
    return ` BY #${author}`;
}

function renderCreators(creators: { keyId: string }[], options?: RenderOptions): string {
    if (creators.length === 0) return '';
    const names = creators.map((c) => {
        if (useAliases(options)) return `$${options!.aliases!.key(c.keyId as B64Hash)}`;
        return sqlString(c.keyId);
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
