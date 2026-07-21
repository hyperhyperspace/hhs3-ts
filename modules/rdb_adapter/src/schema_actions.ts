// Pure mapper: rdb schema state (initial) or an RSchemaChanges delta -> an
// ordered list of SchemaActions. No IO, no target, no dag_sql. Callers drive it:
//
//   const cp = await target.getCheckpoint();
//   const schemaActions = cp === undefined
//     ? initialSchemaActions(view, config)
//     : schemaDeltaActions(
//         ((await group.computeDelta(cp, to)) as RTableGroupDelta).schemaChanges,
//         (await group.getView(to, to)).getSchemaView(),
//         config);
//   await target.apply(schemaActions, rowActions, to);   // see project.ts

import type { ColumnDef, RSchemaChanges, RSchemaView, TableDef } from "@hyper-hyper-space/hhs3_rdb";

import { AdapterConfig, SchemaAction, SchemaActionColumn } from "./types.js";
import {
    authorColumn, idColumn, syncTableName, syncTableSuffix, targetColumnName, targetTableName,
} from "./names.js";

// ---------------------------------------------------------------------------
// Collision checks (name resolution lives in names.ts)
// ---------------------------------------------------------------------------

// Resolve every column of a table to its target name, rejecting collisions:
// two rdb columns mapping to the same target name, or a column colliding with a
// reserved system column (`id` / `author`). Returns columns in rdb-key order.
function resolveTableColumns(config: AdapterConfig, rdbTable: string, def: TableDef): SchemaActionColumn[] {
    const reserved = new Map<string, string>();   // reserved name -> label
    reserved.set(idColumn(config), 'the id column');
    const author = authorColumn(config);
    if (author !== undefined) reserved.set(author, 'the author column');

    const seen = new Map<string, string>();   // targetName -> rdbColumn
    const columns: SchemaActionColumn[] = [];

    for (const rdbColumn of Object.keys(def.columns)) {
        const name = targetColumnName(config, rdbTable, rdbColumn);
        const reservedLabel = reserved.get(name);
        if (reservedLabel !== undefined) {
            throw new Error(
                `column '${rdbTable}.${rdbColumn}' maps to '${name}', which collides with ${reservedLabel}; `
                + `rename it via columnNames or change idColumn/authorColumn`);
        }
        const prior = seen.get(name);
        if (prior !== undefined) {
            throw new Error(
                `columns '${rdbTable}.${prior}' and '${rdbTable}.${rdbColumn}' both map to '${name}'; `
                + `disambiguate via columnNames`);
        }
        seen.set(name, rdbColumn);
        columns.push({ name, def: def.columns[rdbColumn] });
    }

    return columns;
}

// Reject two rdb tables mapping to the same target table name, and reserve the
// per-table sync-table names (`<target><suffix>`) so a real table cannot
// silently collide with the target-side sync convention.
function checkTableNameCollisions(config: AdapterConfig, rdbTables: string[]): void {
    const suffix = syncTableSuffix(config);
    const byTarget = new Map<string, string>();   // targetName -> rdbTable
    for (const rdbTable of rdbTables) {
        const name = targetTableName(config, rdbTable);
        const prior = byTarget.get(name);
        if (prior !== undefined) {
            throw new Error(
                `tables '${prior}' and '${rdbTable}' both map to '${name}'; disambiguate via tableNames`);
        }
        byTarget.set(name, rdbTable);
    }

    if (suffix.length === 0) return;
    for (const [name, rdbTable] of byTarget) {
        const syncName = name + suffix;
        if (syncName === name) continue;
        const clash = byTarget.get(syncName);
        if (clash !== undefined) {
            throw new Error(
                `table '${clash}' maps to '${syncName}', which collides with the sync table of '${rdbTable}' `
                + `('${name}${suffix}'); rename it via tableNames or change syncTableSuffix`);
        }
    }
}

function createTableAction(config: AdapterConfig, rdbTable: string, def: TableDef): SchemaAction {
    const table = targetTableName(config, rdbTable);
    const action: SchemaAction = {
        kind: 'create-table',
        table,
        syncTable: syncTableName(config, table),
        primaryKey: idColumn(config),
        columns: resolveTableColumns(config, rdbTable, def),
    };
    const author = authorColumn(config);
    if (author !== undefined) action.authorColumn = author;
    return action;
}

// ---------------------------------------------------------------------------
// Mapping entry points
// ---------------------------------------------------------------------------

// Full materialization from a resolved schema view: one create-table per table.
export function initialSchemaActions(view: RSchemaView, config: AdapterConfig = {}): SchemaAction[] {
    const tables = view.getTableNames();
    checkTableNameCollisions(config, tables);

    const actions: SchemaAction[] = [];
    for (const rdbTable of tables) {
        const def = view.getTable(rdbTable);
        if (def === undefined) continue;   // defensive: name came from getTableNames()
        actions.push(createTableAction(config, rdbTable, def));
    }
    return actions;
}

// Incremental actions from a schema delta. `endView` is the resolved schema at
// the delta's end version (used to read full defs for created/altered tables).
//
// Ordering is deterministic and drop-before-add: drop-table, create-table,
// then per altered table drop-column before add-column.
export function schemaDeltaActions(
    changes: RSchemaChanges,
    endView: RSchemaView,
    config: AdapterConfig = {},
): SchemaAction[] {
    checkTableNameCollisions(config, endView.getTableNames());

    const dropTables: SchemaAction[] = [];
    const createTables: SchemaAction[] = [];
    const dropColumns: SchemaAction[] = [];
    const addColumns: SchemaAction[] = [];

    for (const change of changes.tableChanges) {
        const rdbTable = change.table;

        if (change.existedBefore && !change.existsAfter) {
            const table = targetTableName(config, rdbTable);
            dropTables.push({ kind: 'drop-table', table, syncTable: syncTableName(config, table) });
            continue;
        }

        if (!change.existedBefore && change.existsAfter) {
            const def = endView.getTable(rdbTable);
            if (def !== undefined) createTables.push(createTableAction(config, rdbTable, def));
            continue;
        }

        // Table exists on both sides: validate its resulting columns, then fold
        // per-column changes. `before`/`after` both set means a type/def change,
        // expressed as drop + add (no in-place type change in the rdb model).
        const endDef = endView.getTable(rdbTable);
        if (endDef !== undefined) resolveTableColumns(config, rdbTable, endDef);

        const targetTable = targetTableName(config, rdbTable);
        for (const col of change.columnChanges) {
            const dropped = col.before !== undefined;
            const added = col.after !== undefined;
            if (dropped) {
                dropColumns.push({
                    kind: 'drop-column',
                    table: targetTable,
                    column: targetColumnName(config, rdbTable, col.column),
                });
            }
            if (added) {
                addColumns.push({
                    kind: 'add-column',
                    table: targetTable,
                    column: targetColumnName(config, rdbTable, col.column),
                    def: col.after as ColumnDef,
                });
            }
        }

        // FK / restriction / concurrentDeletes flips are rdb-side at-use
        // semantics, not relational DDL; intentionally not projected.
    }

    return [...dropTables, ...createTables, ...dropColumns, ...addColumns];
}
