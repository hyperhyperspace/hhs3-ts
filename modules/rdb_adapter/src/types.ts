// Phase 1 vocabulary for the rdb -> relational projection: adapter config, the
// closed set of schema actions the mapper emits, and the interface concrete
// backends implement to materialize them.
//
// Anchor: ONE RTableGroup per checkpoint/target (per-group materialization).
// Table names are therefore unique within the group and actions carry bare
// (un-qualified) table names. Multi-group (RDb-level) projection is a later
// phase that orchestrates several per-group projections at compatible versions.
//
// Anchor: the projection reads like a normal database. App tables carry no
// hashes and no underscore-prefixed columns — just an integer `id` PK, an
// `author` column (application semantics), and the business columns. The
// content-addressed rowId / uuid live in a per-table sync table (`<table>_sync`,
// a target-side convention) which also allocates `id` stably across void flips.

import type { json } from "@hyper-hyper-space/hhs3_json";
import type { B64Hash, KeyId } from "@hyper-hyper-space/hhs3_crypto";
import type { ColumnDef } from "@hyper-hyper-space/hhs3_rdb";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";

export const DEFAULT_ID_COLUMN = 'id';
export const DEFAULT_AUTHOR_COLUMN = 'author';
export const DEFAULT_SYNC_TABLE_SUFFIX = '_sync';

// Fully-customizable naming. Renames are optional per table and per column;
// anything not listed passes through unchanged (rdb identifiers are already
// SQL-identifier-safe). All keys are rdb-side names.
export type AdapterConfig = {
    tableNames?: { [rdbTable: string]: string };
    columnNames?: { [rdbTable: string]: { [rdbColumn: string]: string } };
    // Projection-local integer primary key column. Allocated once per rowId via
    // the per-table sync table and stable across void flips. Defaults to 'id'.
    idColumn?: string;
    // In-row author KeyId column (authorship carries application semantics).
    // Defaults to 'author'; set to false to omit it entirely.
    authorColumn?: string | false;
    // Per-table sync-table naming convention. The sync tables themselves are a
    // target-side concern; the mapper only reserves these names in its collision
    // check so an rdb table cannot silently collide with the convention.
    syncTableSuffix?: string;
};

// A materialized column: its target name plus the rdb ColumnDef verbatim. The
// target owns the mapping of ColumnDef (type + constraints) to native SQL types.
export type SchemaActionColumn = {
    name: string;
    def: ColumnDef;
};

// The ordered vocabulary the mapper emits. Names are already target names
// (renames applied); tables are bare. A column whose type changes in rdb (a new
// incarnation) is expressed as drop-column + add-column, never an in-place type
// change, matching the rdb schema model. `create-table` names its system columns
// (`primaryKey`, and `authorColumn` when present) so targets need no config.
// `create-table` / `drop-table` also name the per-table sync table (`syncTable`)
// so the target owns no naming config; the mapper resolves it from the rename
// config + sync-table suffix.
export type SchemaAction =
    | { kind: 'create-table'; table: string; syncTable: string; primaryKey: string;
        authorColumn?: string; columns: SchemaActionColumn[] }
    | { kind: 'drop-table'; table: string; syncTable: string }
    | { kind: 'add-column'; table: string; column: string; def: ColumnDef }
    | { kind: 'drop-column'; table: string; column: string };

// The data-side vocabulary, the sibling of SchemaAction. Rows are addressed by
// their content-addressed `rowId`; the target maps that to the projection-local
// integer `id` through the per-table sync table (allocating on first sight,
// reusing across void-flip reinstatement). The planner never sees an `id`.
//
// `liveAfter` from the rdb RowChange drives the kind: a live row upserts, a
// non-live row (deleted OR voided by an at-use verdict flip) deletes. `values`
// keys and `table` are already TARGET names; `author` is carried out-of-band so
// the target routes it to the configured author column (or drops it).
export type RowAction =
    | { kind: 'upsert-row'; table: string; rowId: B64Hash; author?: KeyId;
        values: { [column: string]: json.Literal } }
    | { kind: 'delete-row'; table: string; rowId: B64Hash };

// What concrete backends (SQLite / Postgres / IndexedDB / ...) implement. Each
// engine is a full, self-contained target (no shared dialect layer): there are
// deeper per-engine nuances best kept isolated rather than abstracted.
export interface MaterializationTarget {
    // Apply the ordered schema actions THEN the ordered row actions and persist
    // `checkpoint` as the new materialized version, ALL IN ONE transaction (both
    // channels + the checkpoint commit atomically, or nothing). A crash must
    // never leave the target claiming a checkpoint it does not reflect.
    apply(schemaActions: SchemaAction[], rowActions: RowAction[], checkpoint: Version): Promise<void>;

    // The last materialized group version, or undefined when this target has
    // never been materialized (drives the initial-vs-delta decision).
    getCheckpoint(): Promise<Version | undefined>;
}
