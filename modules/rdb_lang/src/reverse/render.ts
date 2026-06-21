import { json } from "@hyper-hyper-space/hhs3_json";
import {
    BundlePayload, ColumnDef, CreateRDbPayload, CreateRSchemaPayload,
    CreateTableGroupPayload, DeleteRowPayload, InsertRowPayload, MigrationRule,
    RowEnvelopePayload, RowOpPayload, SchemaUpdatePayload, TableDef, UpdateRowPayload,
} from "@hyper-hyper-space/hhs3_rdb";
import type { RefAdvancePayload } from "@hyper-hyper-space/hhs3_mvt";

export type RenderOptions = {
    at?: json.Set;
};

export function renderCreateDatabase(payload: CreateRDbPayload): string {
    return `CREATE DATABASE ${payload.name ?? payload.seed};`;
}

export function renderCreateSchema(payload: CreateRSchemaPayload): string {
    const creators = payload.creators.length > 0
        ? ` CREATORS (${payload.creators.map((c) => sqlString(c.keyId)).join(', ')})`
        : '';
    const tables = payload.tables.map(renderTableDef).join(',\n  ');
    return `CREATE SCHEMA ${payload.name ?? payload.seed}${creators} AS (\n  ${tables}\n);`;
}

export function renderCreateTableGroup(payload: CreateTableGroupPayload): string {
    const parts = [
        `CREATE TABLEGROUP ${payload.seed} USING SCHEMA #${payload.schemaRef} AT ${renderVersionSet(payload.schemaVersion)}`,
    ];
    for (const [name, id] of Object.entries(payload.bindings ?? {})) parts.push(`BIND ${name} => #${id}`);
    if (payload.idProvider !== undefined) parts.push(`USING IDENTITIES ${payload.idProvider}`);
    if (payload.canDeploy !== undefined) parts.push(`CAN DEPLOY IF ${renderPredicate(payload.canDeploy)}`);
    if (payload.initialRows !== undefined) {
        const rows: string[] = [];
        for (const [table, tableRows] of Object.entries(payload.initialRows)) {
            for (const row of tableRows) rows.push(renderInitialRow(table, row as unknown as InsertRowPayload));
        }
        if (rows.length > 0) parts.push(`WITH ROWS (\n  ${rows.join(',\n  ')}\n)`);
    }
    return `${parts.join('\n  ')};`;
}

export function renderSchemaUpdate(payload: SchemaUpdatePayload, options?: RenderOptions): string {
    const rules = payload.migration.map(renderMigrationRule).join(',\n  ');
    return `ALTER SCHEMA #unknown AS (\n  ${rules}\n)${renderAt(options)};`;
}

export function renderRowOp(payload: RowOpPayload, table?: string, options?: RenderOptions): string {
    const target = table ?? '<table>';
    if (payload.action === 'insert') {
        const cols = Object.keys(payload.values);
        const vals = cols.map((c) => renderLiteral(payload.values[c]));
        return `INSERT INTO ${target} (${cols.join(', ')}) VALUES (${vals.join(', ')})${renderAt(options)};`;
    }
    if (payload.action === 'update') {
        const values = Object.entries(payload.values).map(([k, v]) => `${k} = ${renderLiteral(v)}`).join(', ');
        return `UPDATE ${target} SET ${values} WHERE rowId = #${payload.rowId}${renderAt(options)};`;
    }
    return `DELETE FROM ${target} WHERE rowId = #${payload.rowId}${renderAt(options)};`;
}

export function renderRefOp(payload: RefAdvancePayload, options?: RenderOptions): string {
    return `UPDATE REF #${payload.refId} TO ${renderVersionSet(payload.refVersion)} ON <group>${renderAt(options)};`;
}

export function renderBundle(payload: BundlePayload, options?: RenderOptions): string {
    const writes = payload.writes.map((w) => renderRowOp(w.op as unknown as RowOpPayload, w.table)).join('\n  ');
    return `BUNDLE ON <group> (\n  ${writes}\n)${renderAt(options)};`;
}

export function renderOp(payload: json.Literal, options?: RenderOptions): string {
    if (!isObject(payload)) return `-- unknown payload ${json.toStringNormalized(payload)}`;
    if (payload['action'] === 'create' && payload['type'] === 'hhs/rdb_v1') return renderCreateDatabase(payload as CreateRDbPayload);
    if (payload['action'] === 'create' && payload['type'] === 'hhs/rschema_v1') return renderCreateSchema(payload as CreateRSchemaPayload);
    if (payload['action'] === 'create' && payload['type'] === 'hhs/rtable_group_v1') return renderCreateTableGroup(payload as CreateTableGroupPayload);
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
    const cols = Object.entries(table.columns).map(([name, def]) => renderColumnDef(name, def, table.fks?.[name])).join(', ');
    const options: string[] = [];
    if (table.concurrentDeletes !== undefined) options.push(table.concurrentDeletes ? 'CONCURRENT DELETES' : 'NO CONCURRENT DELETES');
    if (table.idProvider !== undefined) {
        const provider = table.idProvider.keyIdColumn === 'keyId' && table.idProvider.publicKeyColumn === 'publicKey'
            ? 'IDENTITY PROVIDER'
            : `IDENTITY PROVIDER (${table.idProvider.keyIdColumn}, ${table.idProvider.publicKeyColumn})`;
        options.push(provider);
    }
    for (const restriction of table.restrictions ?? []) {
        options.push(`ALLOW ${restriction.on} IF ${renderPredicate(restriction.rule)}`);
    }
    return `TABLE ${table.name} (${cols})${options.length > 0 ? ' ' + options.join(' ') : ''}`;
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
            return `SET ALLOW RULES ${rule.table} (${rule.restrictions.map((r) => `ALLOW ${r.on} IF ${renderPredicate(r.rule)}`).join(', ')})`;
    }
}

function renderInitialRow(table: string, row: InsertRowPayload): string {
    const values = Object.entries(row.values).map(([k, v]) => `${k}=${renderLiteral(v)}`).join(', ');
    return `${table} (${values})`;
}

function renderPredicate(predicate: unknown): string {
    if (!isObject(predicate)) return 'true';
    switch (predicate['p']) {
        case 'true':
            return 'true';
        case 'false':
            return 'false';
        case 'exists': {
            const where = isObject(predicate['where'])
                ? ` WHERE ${Object.entries(predicate['where']).map(([k, v]) => `${k} = ${renderLiteral(v as json.Literal)}`).join(' AND ')}`
                : '';
            return `EXISTS ${String(predicate['table'])}${where}`;
        }
        case 'cmp':
            return `${renderOperand(predicate['left'])} ${renderCmp(String(predicate['cmp']))} ${renderOperand(predicate['right'])}`;
        case 'str':
            return `${renderOperand(predicate['value'])} LIKE ${renderStringPattern(String(predicate['str']), predicate['sub'])}`;
        case 'and':
            return Array.isArray(predicate['args']) ? predicate['args'].map(renderPredicate).join(' AND ') : 'true';
        case 'or':
            return Array.isArray(predicate['args']) ? predicate['args'].map(renderPredicate).join(' OR ') : 'true';
        default:
            return 'true';
    }
}

function renderOperand(operand: unknown): string {
    if (!isObject(operand)) return 'NULL';
    if ('col' in operand) return String(operand['col']);
    if ('lit' in operand) return renderLiteral(operand['lit'] as json.Literal);
    return 'NULL';
}

function renderCmp(cmp: string): string {
    return ({ eq: '=', ne: '!=', lt: '<', le: '<=', gt: '>', ge: '>=' } as Record<string, string>)[cmp] ?? '=';
}

function renderStringPattern(op: string, sub: unknown): string {
    const lit = isObject(sub) && typeof sub['lit'] === 'string' ? sub['lit'] : '';
    if (op === 'prefix') return sqlString(`${lit}%`);
    if (op === 'suffix') return sqlString(`%${lit}`);
    return sqlString(`%${lit}%`);
}

function renderLiteral(value: json.Literal): string {
    if (typeof value === 'string') return sqlString(value);
    return json.toStringNormalized(value);
}

function sqlString(value: string): string {
    return `'${value.replace(/'/g, "''")}'`;
}

function renderAt(options?: RenderOptions): string {
    return options?.at === undefined ? '' : ` AT ${renderVersionSet(options.at)}`;
}

function renderVersionSet(set: json.Set): string {
    return `{${[...json.fromSet(set)].map((h) => `#${h}`).join(', ')}}`;
}

function isObject(value: unknown): value is { [key: string]: unknown } {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}
