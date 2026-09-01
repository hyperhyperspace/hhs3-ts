// The test-only read side of a projection target. MaterializationTarget itself
// exposes only apply + getCheckpoint (write side); to assert what a projection
// produced, each backend supplies a ProjectionReader that reads its store back
// into LOGICAL values (booleans as booleans, json parsed, etc.). Engine-native
// facts (column affinity, NOT NULL, DEFAULT rendering) are NOT part of this
// interface - those stay in the per-backend test package.

import type { json } from "@hyper-hyper-space/hhs3_json";
import type { ColumnType } from "@hyper-hyper-space/hhs3_rdb";
import { MaterializationTarget, MemoryTarget } from "@hyper-hyper-space/hhs3_rdb_adapter";

export type RowValues = { [column: string]: json.Literal };

// A materialized app row read back as logical values. `values` carries only
// columns that hold an actual (non-null) value, so an omitted nullable column
// is simply absent (uniform across backends).
export type ReadRow = {
    // Numeric id into rdb_keys (recover the key hash via KeyIndex.keyHashForId).
    author?: number;
    values: RowValues;
};

// Backends may be sync (SQLite, MemoryTarget) or async (IndexedDB). Suites
// always `await` reader methods, so a bare value is fine.
export type MaybePromise<T> = T | Promise<T>;

export interface ProjectionReader {
    // Whether the app table exists (materialized), regardless of row count.
    hasTable(table: string): MaybePromise<boolean>;
    // Materialized app table names (not sync tables, not rdb_keys).
    listTables(): MaybePromise<string[]>;
    // The live rowIds of a table (a deleted row is absent even though its sync
    // mapping survives).
    getRowIds(table: string): MaybePromise<string[]>;
    // A live row by its content-addressed rowId, or undefined when not live.
    getRow(table: string, rowId: string): MaybePromise<ReadRow | undefined>;
    // The projection-local serial id allocated for a rowId, or undefined when
    // never seen. Stable across update and SURVIVES delete (id stability).
    syncId(table: string, rowId: string): MaybePromise<number | undefined>;
    // The logical column type the projection gave a target column, or undefined
    // when the column is absent. Used to assert FK companion shape (a local FK
    // projects as an integer `<col>_id`, a cross-group FK as a text
    // `<col>_row_hash`). The system id/author columns are not reported.
    columnType(table: string, column: string): MaybePromise<ColumnType | undefined>;
}

// A target under test paired with a reader over its store. `cleanup` releases
// backend resources (e.g. closes a db handle) after each test.
export type TargetHarness = {
    target: MaterializationTarget;
    read: ProjectionReader;
    cleanup?: () => void | Promise<void>;
};

export type TargetFactory = () => TargetHarness | Promise<TargetHarness>;

// The reference in-memory harness: MemoryTarget wrapped by a trivial reader
// over its public accessors.
export function memoryHarness(): TargetHarness {
    const target = new MemoryTarget();
    const read: ProjectionReader = {
        hasTable: (table) => target.hasTable(table),
        listTables: () => target.listTables(),
        getRowIds: (table) => target.getRowIds(table),
        getRow: (table, rowId) => target.getRowByRowId(table, rowId),
        syncId: (table, rowId) => target.syncId(table, rowId),
        columnType: (table, column) => target.columnTypes(table)?.[column],
    };
    return { target, read };
}
