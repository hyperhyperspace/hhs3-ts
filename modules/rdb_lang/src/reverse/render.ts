import { json } from "@hyper-hyper-space/hhs3_json";
import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import {
    AddGroupPayload, AddSchemaPayload, BundlePayload, ColumnDef, CreateRDbPayload, CreateRSchemaPayload,
    CreateTableGroupPayload, InsertRowPayload, MigrationRule,
    RowEnvelopePayload, RowOpPayload, SchemaUpdatePayload, TableDef, UpdateRowPayload,
    formatPredicate,
} from "@hyper-hyper-space/hhs3_rdb";
import type { RefAdvancePayload } from "@hyper-hyper-space/hhs3_mvt";

export type RenderOptions = {
    at?: json.Set;
    schemaRef?: B64Hash;
    schemaName?: string;
};

export function renderCreateDatabase(payload: CreateRDbPayload): string {
    const creators = (payload.creators ?? []).length > 0
        ? ` CREATORS (${payload.creators!.map((c) => sqlString(c.keyId)).join(', ')})`
        : '';
    return `CREATE DATABASE ${payload.name ?? payload.seed}${creators};`;
}

export function renderCreateSchema(payload: CreateRSchemaPayload): string {
    const creators = payload.creators.length > 0
        ? ` CREATORS (${payload.creators.map((c) => sqlString(c.keyId)).join(', ')})`
        : '';
    const tables = payload.tables.map(renderTableDef).join(',\n  ');
    return `CREATE SCHEMA ${payload.name}${creators} AS (\n  ${tables}\n);`;
}

export function renderCreateTableGroup(payload: CreateTableGroupPayload): string {
    const parts = [
        `CREATE TABLEGROUP ${payload.name} USING SCHEMA #${payload.schemaRef} AT ${renderVersionSet(payload.schemaVersion)}`,
    ];
    for (const [name, id] of Object.entries(payload.bindings ?? {})) parts.push(`BIND ${name} => #${id}`);
    if (payload.idProvider !== undefined) parts.push(`USING IDENTITIES ${payload.idProvider}`);
    if (payload.canDeploy !== undefined) parts.push(`ALLOW UPDATE SCHEMA IF ${formatPredicate(payload.canDeploy)}`);
    for (const [binding, pred] of Object.entries(payload.canObserve ?? {})) {
        parts.push(`ALLOW UPDATE REF ${binding} IF ${formatPredicate(pred)}`);
    }
    if (payload.initialRows !== undefined) {
        const rows: string[] = [];
        for (const [table, tableRows] of Object.entries(payload.initialRows)) {
            for (const row of tableRows) rows.push(renderInitialRow(table, row as unknown as InsertRowPayload));
        }
        if (rows.length > 0) parts.push(`WITH ROWS (\n  ${rows.join(',\n  ')}\n)`);
    }
    return `${parts.join('\n  ')};`;
}

export function renderAddSchema(payload: AddSchemaPayload): string {
    return `ADD SCHEMA #${payload.schemaId} TO <database>${renderNote(payload.note)}${renderBy(payload.author)};`;
}

export function renderAddGroup(payload: AddGroupPayload): string {
    return `ADD TABLEGROUP #${payload.groupId} TO <database>${renderNote(payload.note)}${renderBy(payload.author)};`;
}

export function renderSchemaUpdate(payload: SchemaUpdatePayload, options?: RenderOptions): string {
    const rules = payload.migration.map(renderMigrationRule).join(',\n  ');
    const schemaRef = options?.schemaRef ?? 'unknown';
    const comment = options?.schemaName === undefined ? '' : `-- ${options.schemaName}\n`;
    return `${comment}ALTER SCHEMA #${schemaRef} AS (\n  ${rules}\n)${renderBy(payload.author)}${renderAt(options)};`;
}

export function renderRowOp(payload: RowOpPayload, table?: string, options?: RenderOptions): string {
    const target = table ?? '<table>';
    if (payload.action === 'insert') {
        const cols = Object.keys(payload.values);
        const vals = cols.map((c) => renderLiteral(payload.values[c]));
        return `INSERT INTO ${target} (${cols.join(', ')}) VALUES (${vals.join(', ')})${renderBy(payload.author)}${renderAt(options)};`;
    }
    if (payload.action === 'update') {
        const values = Object.entries(payload.values).map(([k, v]) => `${k} = ${renderLiteral(v)}`).join(', ');
        return `UPDATE ${target} SET ${values} WHERE rowId = #${payload.rowId}${renderBy(payload.author)}${renderAt(options)};`;
    }
    return `DELETE FROM ${target} WHERE rowId = #${payload.rowId}${renderBy(payload.author)}${renderAt(options)};`;
}

export function renderRefOp(payload: RefAdvancePayload, options?: RenderOptions): string {
    const author = (payload as { author?: string }).author;
    const trailing = `${renderBy(author)}${renderAt(options)}`;
    const version = renderVersionSet(payload.refVersion);
    if (options?.schemaRef !== undefined && payload.refId === options.schemaRef) {
        return `UPDATE SCHEMA #${payload.refId} TO ${version} ON <group>${trailing};`;
    }
    return `UPDATE REF #${payload.refId} TO ${version} ON <group>${trailing};`;
}

export function renderBundle(payload: BundlePayload, options?: RenderOptions): string {
    const writes = payload.writes.map((w) => renderRowOp(w.op as unknown as RowOpPayload, w.table)).join('\n  ');
    const author = (payload as { author?: string }).author;
    return `BUNDLE ON <group> (\n  ${writes}\n)${renderBy(author)}${renderAt(options)};`;
}

export function renderOp(payload: json.Literal, options?: RenderOptions): string {
    if (!isObject(payload)) return `-- unknown payload ${json.toStringNormalized(payload)}`;
    if (payload['action'] === 'create' && payload['type'] === 'hhs/rdb_v1') return renderCreateDatabase(payload as CreateRDbPayload);
    if (payload['action'] === 'create' && payload['type'] === 'hhs/rschema_v1') return renderCreateSchema(payload as CreateRSchemaPayload);
    if (payload['action'] === 'create' && payload['type'] === 'hhs/rtable_group_v1') return renderCreateTableGroup(payload as CreateTableGroupPayload);
    if (payload['action'] === 'add-schema') return renderAddSchema(payload as unknown as AddSchemaPayload);
    if (payload['action'] === 'add-group') return renderAddGroup(payload as unknown as AddGroupPayload);
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

function renderInitialRow(table: string, row: InsertRowPayload): string {
    const values = Object.entries(row.values).map(([k, v]) => `${k}=${renderLiteral(v)}`).join(', ');
    return `${table} (${values})`;
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
    return options?.at === undefined ? '' : ` AT ${renderVersionSet(options.at)}`;
}

function renderBy(author?: string): string {
    return author === undefined ? '' : ` BY #${author}`;
}

function renderVersionSet(set: json.Set): string {
    return `{${[...json.fromSet(set)].map((h) => `#${h}`).join(', ')}}`;
}

function isObject(value: unknown): value is { [key: string]: unknown } {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}
