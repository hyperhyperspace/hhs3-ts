// Shared name resolution for the projection. Both the schema mapper
// (schema_actions.ts) and the row planner (row_actions.ts) resolve rdb-side
// names to target names through exactly these helpers, so renames and the
// system-column defaults stay identical across the schema and data channels.
//
// Collision checking lives in schema_actions.ts (it owns the schema shape); the
// row planner trusts the already-validated names and only needs the mapping.

import type { FKs, IdProvider } from "@hyper-hyper-space/hhs3_rdb";

import {
    AdapterConfig, DEFAULT_AUTHOR_COLUMN, DEFAULT_CROSS_GROUP_FK_SUFFIX, DEFAULT_FK_ID_SUFFIX,
    DEFAULT_ID_COLUMN, DEFAULT_KEY_ID_COLUMN, DEFAULT_KEY_REF_SUFFIX, DEFAULT_KEY_TABLE,
    DEFAULT_SYNC_TABLE_SUFFIX,
} from "./types.js";

export function idColumn(config: AdapterConfig): string {
    return config.idColumn ?? DEFAULT_ID_COLUMN;
}

// The in-row author column name (an integer key-ref into rdb_keys), or undefined
// when authorship is not projected.
export function authorColumn(config: AdapterConfig): string | undefined {
    if (config.authorColumn === false) return undefined;
    return config.authorColumn ?? DEFAULT_AUTHOR_COLUMN;
}

export function keyTable(config: AdapterConfig): string {
    return config.keyTable ?? DEFAULT_KEY_TABLE;
}

// Fixed projected name for an identity-provider table's keyIdColumn.
export function providerKeyIdColumn(config: AdapterConfig): string {
    return config.keyIdColumn ?? DEFAULT_KEY_ID_COLUMN;
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

export function fkIdSuffix(config: AdapterConfig): string {
    return config.fkIdSuffix ?? DEFAULT_FK_ID_SUFFIX;
}

export function crossGroupFkSuffix(config: AdapterConfig): string {
    return config.crossGroupFkSuffix ?? DEFAULT_CROSS_GROUP_FK_SUFFIX;
}

export function keyRefSuffix(config: AdapterConfig): string {
    return config.keyRefSuffix ?? DEFAULT_KEY_REF_SUFFIX;
}

// Projected name for an identity-typed business column: `<renamedCol>_key_id`
// (a key-ref into rdb_keys). Distinct from FK `<col>_id` (a row serial) and
// from the provider keyIdColumn (the unsuffixed `key_id`).
export function projectedIdentityColumnName(
    config: AdapterConfig, rdbTable: string, rdbColumn: string,
): string {
    return targetColumnName(config, rdbTable, rdbColumn) + keyRefSuffix(config);
}

// Whether an FK target reference is LOCAL (same group) rather than cross-group.
// A cross-group reference is qualified ('group.table').
export function isLocalFkRef(ref: string): boolean {
    return !ref.includes('.');
}

// How an FK ref projects. An `id` resolution gets an integer `<col>_id`
// companion translated to the referenced table's serial id: LOCAL always, and a
// CROSS-GROUP ref whose group is co-projected (via config.crossGroup). A `hash`
// resolution (a cross-group ref to a non-co-projected group) keeps the opaque
// text `<col>_row_hash` passthrough.
export type FkResolution =
    | { kind: 'id'; targetTable: string; crossGroup: boolean }
    | { kind: 'hash' };

export function resolveFk(config: AdapterConfig, ref: string): FkResolution {
    if (isLocalFkRef(ref)) return { kind: 'id', targetTable: targetTableName(config, ref), crossGroup: false };
    const targetTable = config.crossGroup?.(ref);
    if (targetTable !== undefined) return { kind: 'id', targetTable, crossGroup: true };
    return { kind: 'hash' };
}

// Whether `rdbColumn` of an id-provider table is the designated keyIdColumn
// (projects as integer `key_id`) or the designated publicKeyColumn (DROPPED
// from the projection — crypto material lives only in rdb_keys).
export type ProviderColumnRole =
    | { role: 'keyId' }
    | { role: 'publicKey' }
    | { role: 'plain' };

export function providerColumnRole(provider: IdProvider | undefined, rdbColumn: string): ProviderColumnRole {
    if (provider === undefined) return { role: 'plain' };
    if (rdbColumn === provider.keyIdColumn) return { role: 'keyId' };
    if (rdbColumn === provider.publicKeyColumn) return { role: 'publicKey' };
    return { role: 'plain' };
}

// The projected column name for an rdb column, FK- and provider-aware. An
// id-resolving FK column gets the integer-id suffix; a non-co-projected
// cross-group FK gets the row_hash suffix; a provider keyIdColumn projects as
// the fixed `key_id`; a provider publicKeyColumn is never projected (caller
// must skip it); a plain column keeps its (renamed) name — identity-typed
// columns get the `_key_id` suffix via their own keyRef path in the schema mapper.
export function projectedColumnName(
    config: AdapterConfig, rdbTable: string, rdbColumn: string, fks: FKs,
    provider?: IdProvider,
): string {
    const role = providerColumnRole(provider, rdbColumn);
    if (role.role === 'keyId') return providerKeyIdColumn(config);
    if (role.role === 'publicKey') {
        throw new Error(
            `column '${rdbTable}.${rdbColumn}' is an identity-provider publicKeyColumn and is not projected; `
            + `crypto material lives only in '${keyTable(config)}'`);
    }
    const base = targetColumnName(config, rdbTable, rdbColumn);
    const ref = fks[rdbColumn];
    if (ref === undefined) return base;
    return base + (resolveFk(config, ref).kind === 'id' ? fkIdSuffix(config) : crossGroupFkSuffix(config));
}

// ---------------------------------------------------------------------------
// Reverse resolution (inbound / change ingestion). The forward mapping is a
// bijection on the names actually in play (forward collision-checking in
// schema_actions.ts guarantees target names are unique), so inversion is
// well-defined; anything without an explicit rename is identity.
// ---------------------------------------------------------------------------

// The rdb table name for a target table name (inverse of targetTableName).
// Falls back to identity when no rename maps to `targetTable`.
export function rdbTableName(config: AdapterConfig, targetTable: string): string {
    const renames = config.tableNames;
    if (renames !== undefined) {
        for (const rdbTable of Object.keys(renames)) {
            if (renames[rdbTable] === targetTable) return rdbTable;
        }
    }
    return targetTable;
}

// The rdb column name for a target column of a KNOWN rdb table (inverse of
// targetColumnName). Falls back to identity.
export function rdbColumnName(config: AdapterConfig, rdbTable: string, targetColumn: string): string {
    const renames = config.columnNames?.[rdbTable];
    if (renames !== undefined) {
        for (const rdbColumn of Object.keys(renames)) {
            if (renames[rdbColumn] === targetColumn) return rdbColumn;
        }
    }
    return targetColumn;
}

// Reverse of projectedColumnName for FK columns: given the rdb table's FK map,
// find the rdb FK column whose projection is `targetColumn`, how it resolves
// (`resolvesToId`: an integer id companion whose value is the referenced row's
// serial id, for local + co-projected cross-group; else a row_hash passthrough),
// whether it is cross-group, and the referenced TARGET table when id-resolving.
// Returns undefined when `targetColumn` is not a projected FK column. (Iterating
// the FK set - rather than stripping the suffix - is robust to renames and to a
// business column that merely ends in the suffix.)
export function rdbFkColumnFromTarget(
    config: AdapterConfig, rdbTable: string, fks: FKs, targetColumn: string,
): { rdbColumn: string; resolvesToId: boolean; crossGroup: boolean; targetTable?: string } | undefined {
    for (const rdbColumn of Object.keys(fks)) {
        if (projectedColumnName(config, rdbTable, rdbColumn, fks) === targetColumn) {
            const res = resolveFk(config, fks[rdbColumn]);
            if (res.kind === 'id') {
                return { rdbColumn, resolvesToId: true, crossGroup: res.crossGroup, targetTable: res.targetTable };
            }
            return { rdbColumn, resolvesToId: false, crossGroup: true };
        }
    }
    return undefined;
}

// Reverse of a key-ref column: the provider keyIdColumn projects as `key_id`,
// and an identity-typed column projects as `<col>_key_id`. Returns the
// rdb column whose projection is `targetColumn`, or undefined when it is not a
// key-ref. For a provider keyIdColumn, ingest also reconstructs the
// publicKeyColumn from KeyIndex.
export function keyRefColumnFromTarget(
    config: AdapterConfig, rdbTable: string, provider: IdProvider | undefined,
    targetColumn: string, identityColumns?: Set<string>,
): { rdbColumn: string; isProviderKeyId: boolean } | undefined {
    if (provider !== undefined && targetColumn === providerKeyIdColumn(config)) {
        return { rdbColumn: provider.keyIdColumn, isProviderKeyId: true };
    }
    if (identityColumns !== undefined) {
        for (const rdbColumn of identityColumns) {
            const projected = projectedIdentityColumnName(config, rdbTable, rdbColumn);
            if (projected === targetColumn) {
                return { rdbColumn, isProviderKeyId: false };
            }
        }
    }
    return undefined;
}

// Whether a target column name is a system column (the id PK or, when
// projected, the author column) rather than a business column. Inbound values
// touching these are not app data: they are stripped before translation.
export function isSystemColumn(config: AdapterConfig, targetColumn: string): boolean {
    if (targetColumn === idColumn(config)) return true;
    const author = authorColumn(config);
    return author !== undefined && targetColumn === author;
}
