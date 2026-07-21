// Shared name resolution for the projection. Both the schema mapper
// (schema_actions.ts) and the row planner (row_actions.ts) resolve rdb-side
// names to target names through exactly these helpers, so renames and the
// system-column defaults stay identical across the schema and data channels.
//
// Collision checking lives in schema_actions.ts (it owns the schema shape); the
// row planner trusts the already-validated names and only needs the mapping.

import {
    AdapterConfig, DEFAULT_AUTHOR_COLUMN, DEFAULT_ID_COLUMN, DEFAULT_SYNC_TABLE_SUFFIX,
} from "./types.js";

export function idColumn(config: AdapterConfig): string {
    return config.idColumn ?? DEFAULT_ID_COLUMN;
}

// The in-row author column name, or undefined when authorship is not projected.
export function authorColumn(config: AdapterConfig): string | undefined {
    if (config.authorColumn === false) return undefined;
    return config.authorColumn ?? DEFAULT_AUTHOR_COLUMN;
}

export function syncTableSuffix(config: AdapterConfig): string {
    return config.syncTableSuffix ?? DEFAULT_SYNC_TABLE_SUFFIX;
}

export function targetTableName(config: AdapterConfig, rdbTable: string): string {
    return config.tableNames?.[rdbTable] ?? rdbTable;
}

export function targetColumnName(config: AdapterConfig, rdbTable: string, rdbColumn: string): string {
    return config.columnNames?.[rdbTable]?.[rdbColumn] ?? rdbColumn;
}

// The per-table sync-table name for a target table name (already resolved).
export function syncTableName(config: AdapterConfig, targetTable: string): string {
    return targetTable + syncTableSuffix(config);
}
