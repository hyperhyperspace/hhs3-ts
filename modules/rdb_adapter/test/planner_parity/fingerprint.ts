import { json } from "@hyper-hyper-space/hhs3_json";
import type { RSchemaView, RTableGroupView, Row } from "@hyper-hyper-space/hhs3_rdb";

import type { AdapterConfig } from "../../src/types.js";
import {
    projectedColumnName, projectedIdentityColumnName, providerColumnRole, targetTableName,
} from "../../src/names.js";

export type ColumnFp = { name: string; type: string; fkTarget?: string };
export type RowFp = { rowId: string; author?: string; values: { [column: string]: json.Literal } };
export type TableFp = { name: string; columns: ColumnFp[]; rows: RowFp[] };
export type StoreFingerprint = { tables: TableFp[] };

export function canonicalFingerprint(fp: StoreFingerprint): string {
    return json.toStringNormalized(fp as unknown as json.Literal);
}

function omitEmpty(values: { [column: string]: json.Literal | undefined }): { [column: string]: json.Literal } {
    const out: { [column: string]: json.Literal } = {};
    for (const [k, v] of Object.entries(values)) {
        if (v !== undefined && v !== null) out[k] = v;
    }
    return out;
}

function sortRecord(values: { [column: string]: json.Literal }): { [column: string]: json.Literal } {
    const out: { [column: string]: json.Literal } = {};
    for (const k of Object.keys(values).sort()) out[k] = values[k];
    return out;
}

export function sortFingerprint(fp: StoreFingerprint): StoreFingerprint {
    const tables = fp.tables.map((t) => ({
        name: t.name,
        columns: [...t.columns].sort((a, b) => a.name.localeCompare(b.name)),
        rows: [...t.rows]
            .map((r) => {
                const row: RowFp = { rowId: r.rowId, values: sortRecord(omitEmpty(r.values)) };
                if (r.author !== undefined) row.author = r.author;
                return row;
            })
            .sort((a, b) => a.rowId.localeCompare(b.rowId)),
    })).sort((a, b) => a.name.localeCompare(b.name));
    return { tables };
}

// Projected name for one rdb column, restated from names.ts (not schema_actions
// reshape). Returns undefined when the column is dropped from the projection
// (a provider publicKeyColumn).
export function projectedNameForRdbColumn(
    config: AdapterConfig, rdbTable: string, rdbColumn: string, schemaView: RSchemaView,
): string | undefined {
    const provider = schemaView.getIdProvider?.(rdbTable);
    const role = providerColumnRole(provider, rdbColumn);
    if (role.role === 'publicKey') return undefined;
    const colDef = schemaView.getTable(rdbTable)?.columns?.[rdbColumn];
    if (colDef !== undefined && colDef.type === 'identity') {
        return projectedIdentityColumnName(config, rdbTable, rdbColumn);
    }
    return projectedColumnName(config, rdbTable, rdbColumn, schemaView.getFKs(rdbTable), provider);
}

function projectRow(row: Row, rdbTable: string, schemaView: RSchemaView, config: AdapterConfig): RowFp {
    const values: { [column: string]: json.Literal } = {};
    for (const [column, value] of Object.entries(row.values)) {
        const name = projectedNameForRdbColumn(config, rdbTable, column, schemaView);
        if (name === undefined) continue;
        values[name] = value;
    }
    const fp: RowFp = { rowId: row.rowId, values: sortRecord(omitEmpty(values)) };
    if (row.author !== undefined) fp.author = row.author;
    return fp;
}

// Live rdb rows at `view`, keyed by projected column names via names.ts.
export async function fingerprintRdbProjectedRows(
    view: RTableGroupView, config: AdapterConfig = {},
): Promise<string> {
    const schemaView = view.getSchemaView();
    const tables: TableFp[] = [];
    for (const rdbTable of view.getTableNames()) {
        const tableView = await view.getTableView(rdbTable);
        const rows = (await tableView.query({})).map((row) => projectRow(row, rdbTable, schemaView, config));
        tables.push({ name: targetTableName(config, rdbTable), columns: [], rows });
    }
    return canonicalFingerprint(sortFingerprint({ tables }));
}

// Live rdb rows in rdb vocabulary (ingest A vs B). Columns unused.
export async function fingerprintRdbRows(view: RTableGroupView): Promise<string> {
    const tables: TableFp[] = [];
    for (const rdbTable of view.getTableNames()) {
        const tableView = await view.getTableView(rdbTable);
        const rows: RowFp[] = [];
        for (const row of await tableView.query({})) {
            const fp: RowFp = { rowId: row.rowId, values: sortRecord(omitEmpty(row.values)) };
            if (row.author !== undefined) fp.author = row.author;
            rows.push(fp);
        }
        tables.push({ name: rdbTable, columns: [], rows });
    }
    return canonicalFingerprint(sortFingerprint({ tables }));
}

export function rowsOnlyFingerprint(fp: StoreFingerprint): string {
    return canonicalFingerprint(sortFingerprint({
        tables: fp.tables.map((t) => ({ name: t.name, columns: [], rows: t.rows })),
    }));
}

export function schemaOnlyFingerprint(fp: StoreFingerprint): string {
    return canonicalFingerprint(sortFingerprint({
        tables: fp.tables.map((t) => ({ name: t.name, columns: t.columns, rows: [] })),
    }));
}
