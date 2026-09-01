import { json } from "@hyper-hyper-space/hhs3_json";
import type { RSchemaView, RTableGroupView, Row } from "@hyper-hyper-space/hhs3_rdb";
import {
    AdapterConfig, DEFAULT_KEY_DOMAIN, KeyIndex, RowIdentityIndex,
    projectedColumnName, projectedIdentityColumnName, providerColumnRole,
    rdbFkColumnFromTarget, rdbTableName, targetTableName,
} from "@hyper-hyper-space/hhs3_rdb_adapter";

import type { ProjectionReader } from "./projection_reader.js";

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
// FK values stay rdb rowIds (the target intern to serial ids is reversed on
// the reader side before comparison).
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

export type ReaderOracleOpts = {
    schemaView: RSchemaView;
    keyIndex?: KeyIndex;
    rowIndex?: RowIdentityIndex;
    config?: AdapterConfig;
};

// Read a materialized target back into the same fingerprint shape as
// fingerprintRdbProjectedRows: projected column names, rdb rowId keys, FK
// companions reversed from serial ids to rowIds, author ids reversed to key
// hashes.
export async function readerProjectedRows(
    read: ProjectionReader, opts: ReaderOracleOpts,
): Promise<string> {
    const config = opts.config ?? {};
    const schemaView = opts.schemaView;
    const tables: TableFp[] = [];
    for (const targetTable of await read.listTables()) {
        const rdbTable = rdbTableName(config, targetTable);
        const fks = schemaView.hasTable(rdbTable) ? schemaView.getFKs(rdbTable) : {};
        const rowIds = await read.getRowIds(targetTable);
        const rows: RowFp[] = [];
        for (const rowId of rowIds) {
            const raw = await read.getRow(targetTable, rowId);
            if (raw === undefined) continue;
            const values: { [column: string]: json.Literal | undefined } = {};
            for (const [col, value] of Object.entries(raw.values)) {
                const fk = rdbFkColumnFromTarget(config, rdbTable, fks, col);
                if (fk?.resolvesToId && typeof value === 'number' && opts.rowIndex !== undefined && fk.targetTable !== undefined) {
                    values[col] = await opts.rowIndex.rowHashForLocalId(fk.targetTable, value);
                } else {
                    values[col] = value;
                }
            }
            const fp: RowFp = { rowId, values: sortRecord(omitEmpty(values)) };
            if (raw.author !== undefined && opts.keyIndex !== undefined) {
                const hash = await opts.keyIndex.keyHashForId(DEFAULT_KEY_DOMAIN, raw.author);
                if (hash !== undefined) fp.author = hash;
            } else if (raw.author !== undefined) {
                fp.author = String(raw.author);
            }
            rows.push(fp);
        }
        tables.push({ name: targetTable, columns: [], rows });
    }
    return canonicalFingerprint(sortFingerprint({ tables }));
}
