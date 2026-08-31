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

import { json } from "@hyper-hyper-space/hhs3_json";
import type { ColumnDef, FKs, IdProvider, RSchemaChanges, RSchemaView, TableDef } from "@hyper-hyper-space/hhs3_rdb";

import { AdapterConfig, SchemaAction, SchemaActionColumn } from "./types.js";
import {
    authorColumn, idColumn, keyTable, projectedColumnName, projectedIdentityColumnName,
    providerColumnRole, providerKeyIdColumn, resolveFk, syncTableName, syncTableSuffix,
    targetTableName,
} from "./names.js";

// ---------------------------------------------------------------------------
// Collision checks (name resolution lives in names.ts)
// ---------------------------------------------------------------------------

// Reshape one rdb column into the column the target should realize. Returns
// undefined when the column is DROPPED from the projection (an identity-
// provider publicKeyColumn — crypto material lives only in rdb_keys).
//
// A plain column carries its ColumnDef verbatim; an FK column is RETYPED to its
// companion form; a provider keyIdColumn becomes integer `key_id` (keyRef);
// an identity-typed column becomes integer `<col>_key_id` (keyRef).
function reshapeColumn(
    config: AdapterConfig, rdbTable: string, rdbColumn: string, colDef: ColumnDef, fks: FKs,
    provider?: IdProvider,
): SchemaActionColumn | undefined {
    const role = providerColumnRole(provider, rdbColumn);
    if (role.role === 'publicKey') return undefined;

    if (role.role === 'keyId') {
        const def: ColumnDef = { type: 'integer' };
        if (colDef.nullable === true) def.nullable = true;
        if (colDef.readonly === true) def.readonly = true;
        return { name: providerKeyIdColumn(config), def, keyRef: true };
    }

    // Identity-typed business columns project as integer `<col>_key_id`.
    if (colDef.type === 'identity') {
        const def: ColumnDef = { type: 'integer' };
        if (colDef.nullable === true) def.nullable = true;
        if (colDef.readonly === true) def.readonly = true;
        return { name: projectedIdentityColumnName(config, rdbTable, rdbColumn), def, keyRef: true };
    }

    const name = projectedColumnName(config, rdbTable, rdbColumn, fks, provider);
    const ref = fks[rdbColumn];
    if (ref === undefined) return { name, def: colDef };

    const res = resolveFk(config, ref);
    const def: ColumnDef = { type: res.kind === 'id' ? 'integer' : 'string' };
    if (colDef.nullable === true) def.nullable = true;
    if (colDef.readonly === true) def.readonly = true;
    const out: SchemaActionColumn = { name, def };
    if (res.kind === 'id') {
        out.fk = res.crossGroup ? { targetTable: res.targetTable, crossGroup: true } : { targetTable: res.targetTable };
    }
    return out;
}

// Two projected columns are the same iff their name, def, fk metadata, and
// keyRef flag all match. A defined projection is never equal to an absent one
// (undefined marks a column that is dropped from — or not yet in — the target).
function sameProjection(a: SchemaActionColumn | undefined, b: SchemaActionColumn | undefined): boolean {
    if (a === undefined || b === undefined) return a === b;
    const canon = (c: SchemaActionColumn): json.Literal => {
        const out: json.LiteralMap = { name: c.name, def: c.def as json.Literal, keyRef: c.keyRef ?? false };
        if (c.fk !== undefined) out.fk = c.fk as json.Literal;
        return out;
    };
    return json.toStringNormalized(canon(a)) === json.toStringNormalized(canon(b));
}

// Resolve every column of a table to its projected (FK- / key-aware) target
// column, rejecting collisions: two rdb columns mapping to the same target name,
// or a column colliding with a reserved system column (`id` / `author_key_id`).
// Because the EMITTED name (with any FK / key suffix) is what enters the
// collision set, a business column that clashes with a projected companion is
// caught here too. Returns columns in rdb-key order (skipping dropped ones).
function resolveTableColumns(
    config: AdapterConfig, rdbTable: string, def: TableDef, fks: FKs, provider?: IdProvider,
): SchemaActionColumn[] {
    const reserved = new Map<string, string>();   // reserved name -> label
    reserved.set(idColumn(config), 'the id column');
    const author = authorColumn(config);
    if (author !== undefined) reserved.set(author, 'the author column');

    const seen = new Map<string, string>();   // targetName -> rdbColumn
    const columns: SchemaActionColumn[] = [];

    for (const rdbColumn of Object.keys(def.columns)) {
        const column = reshapeColumn(config, rdbTable, rdbColumn, def.columns[rdbColumn], fks, provider);
        if (column === undefined) continue;   // dropped (publicKeyColumn)
        const name = column.name;
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
        columns.push(column);
    }

    return columns;
}

// Reject two rdb tables mapping to the same target table name, reserve the
// per-table sync-table names (`<target><suffix>`), and reserve the shared
// keys side table (`rdb_keys`) so a real table cannot silently collide with
// the target-side conventions.
function checkTableNameCollisions(config: AdapterConfig, rdbTables: string[]): void {
    const suffix = syncTableSuffix(config);
    const keys = keyTable(config);
    const byTarget = new Map<string, string>();   // targetName -> rdbTable
    for (const rdbTable of rdbTables) {
        const name = targetTableName(config, rdbTable);
        if (name === keys) {
            throw new Error(
                `table '${rdbTable}' maps to '${name}', which collides with the keys side table; `
                + `rename it via tableNames or change keyTable`);
        }
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
        if (syncName === keys) {
            throw new Error(
                `sync table of '${rdbTable}' ('${syncName}') collides with the keys side table; `
                + `change syncTableSuffix or keyTable`);
        }
    }
}

function createTableAction(
    config: AdapterConfig, rdbTable: string, def: TableDef, fks: FKs, provider?: IdProvider,
): SchemaAction {
    const table = targetTableName(config, rdbTable);
    const action: SchemaAction = {
        kind: 'create-table',
        table,
        syncTable: syncTableName(config, table),
        primaryKey: idColumn(config),
        columns: resolveTableColumns(config, rdbTable, def, fks, provider),
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
        actions.push(createTableAction(config, rdbTable, def, view.getFKs(rdbTable), view.getIdProvider?.(rdbTable)));
    }
    return actions;
}

// Incremental actions from a schema delta. `endView` is the resolved schema at
// the delta's end version (used to read full defs for created/altered tables);
// `startView` is the resolved schema at the delta's start version, used to
// reshape a DROPPED column by its BEFORE-state (a dropped FK / identity /
// provider column projects to a companion name — `<col>_id` / `<col>_key_id` /
// `key_id` — that the end view no longer knows). Required, not optional: a
// silent fallback to the plain name would re-emit a drop for a column the
// target never created (diverging SQLite, which throws, from memory, which
// no-ops).
//
// Ordering is deterministic and drop-before-add: drop-table, create-table,
// then per altered table drop-column before add-column.
export function schemaDeltaActions(
    changes: RSchemaChanges,
    endView: RSchemaView,
    startView: RSchemaView,
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
            if (def !== undefined) {
                createTables.push(createTableAction(
                    config, rdbTable, def, endView.getFKs(rdbTable), endView.getIdProvider?.(rdbTable)));
            }
            continue;
        }

        // Table exists on both sides: validate its resulting columns, then diff
        // each column's PROJECTION across the delta. A column's target shape
        // (name / type / fk / keyRef) is fully determined by its def AND its
        // FK-ness, so comparing the START-reshaped and END-reshaped projections
        // subsumes def changes (a type change is drop+add), pure drops/adds, AND
        // set-fks flips or retargets (which carry no columnChanges entry) in one
        // pass. The drop names the companion by the START view (so a dropped /
        // un-FK'd column is named as the target last had it); the add names it
        // by the END view.
        const endDef = endView.getTable(rdbTable);
        const endFks = endView.getFKs(rdbTable);
        const provider = endView.getIdProvider?.(rdbTable);
        if (endDef !== undefined) resolveTableColumns(config, rdbTable, endDef, endFks, provider);

        const startDef = startView.getTable(rdbTable);
        const startFks = startView.getFKs(rdbTable);
        const startProvider = startView.getIdProvider?.(rdbTable);

        const beforeCols = startDef?.columns ?? {};
        const afterCols = endDef?.columns ?? {};
        const names = [
            ...Object.keys(beforeCols),
            ...Object.keys(afterCols).filter((n) => beforeCols[n] === undefined),
        ];

        // Columns the delta reports as REINCARNATED: same resolved def (so the
        // projection is unchanged), but a new live incarnation that masks old
        // rows' written values. The projection diff below would skip them
        // (sameProjection is true), so force a drop+add — the drop clears every
        // row's cell, the add re-materializes it (default-backfilled or empty),
        // converging both walked and unwalked rows to a fresh full projection.
        const reincarnatedCols = new Set(
            change.columnChanges.filter((c) => c.reincarnated).map((c) => c.column));

        const targetTable = targetTableName(config, rdbTable);
        for (const col of names) {
            // reshapeColumn returns undefined for a column dropped from the
            // projection (a provider publicKeyColumn), which sameProjection
            // treats as absent — so such a column yields no drop / add.
            const beforeProj = beforeCols[col] !== undefined
                ? reshapeColumn(config, rdbTable, col, beforeCols[col], startFks, startProvider) : undefined;
            const afterProj = afterCols[col] !== undefined
                ? reshapeColumn(config, rdbTable, col, afterCols[col], endFks, provider) : undefined;
            if (sameProjection(beforeProj, afterProj) && !reincarnatedCols.has(col)) continue;
            if (beforeProj !== undefined) {
                dropColumns.push({ kind: 'drop-column', table: targetTable, column: beforeProj.name });
            }
            if (afterProj !== undefined) {
                const add: Extract<SchemaAction, { kind: 'add-column' }> = {
                    kind: 'add-column',
                    table: targetTable,
                    column: afterProj.name,
                    def: afterProj.def,
                };
                if (afterProj.fk !== undefined) add.fk = afterProj.fk;
                if (afterProj.keyRef === true) add.keyRef = true;
                addColumns.push(add);
            }
        }

        // Restriction / concurrentDeletes flips are rdb-side at-use semantics,
        // not relational DDL; intentionally not projected. FK-ness and def
        // changes ARE projected, via the per-column projection diff above.
    }

    return [...dropTables, ...createTables, ...dropColumns, ...addColumns];
}

// Tables whose EXISTING data was RE-PROJECTED without a def change — i.e. a
// column present on BOTH sides, with an UNCHANGED ColumnDef, whose projection
// (name / type / fk / keyRef) nonetheless differs across the delta. This is a
// pure FK-ness / provider reinterpretation (a `set-fks` flip or retarget)
// carrying no columnChanges entry, so the row-delta channel omits the affected
// rows and their new companion would stay empty. The orchestrator uses this to
// trigger a scoped live-view row backfill so incremental projection converges
// to a fresh full projection.
//
// Deliberately EXCLUDES: pure add / drop (a column not in the start∩end
// intersection — the row channel handles those), TYPE changes (a def change is
// a new column incarnation whose live value is empty for old rows anyway,
// tracked via `columnChanges`), and same-shape REINCARNATIONS (also a
// `columnChanges` entry, `reincarnated: true`): schemaDeltaActions emits a
// drop+add for those, which clears every row's cell without a row backfill.
// Reuses the exact `reshapeColumn` / `sameProjection` the mapper emits DDL
// from, so detection cannot drift from it.
export function reprojectedTables(
    changes: RSchemaChanges, endView: RSchemaView, startView: RSchemaView, config: AdapterConfig = {},
): Set<string> {
    const tables = new Set<string>();
    for (const change of changes.tableChanges) {
        if (!(change.existedBefore && change.existsAfter)) continue;
        const rdbTable = change.table;
        const startDef = startView.getTable(rdbTable);
        const endDef = endView.getTable(rdbTable);
        if (startDef === undefined || endDef === undefined) continue;

        const defChanged = new Set(change.columnChanges
            .filter((c) => c.before !== undefined && c.after !== undefined).map((c) => c.column));
        const startFks = startView.getFKs(rdbTable);
        const startProvider = startView.getIdProvider?.(rdbTable);
        const endFks = endView.getFKs(rdbTable);
        const provider = endView.getIdProvider?.(rdbTable);

        for (const col of Object.keys(startDef.columns)) {
            if (endDef.columns[col] === undefined || defChanged.has(col)) continue;
            const before = reshapeColumn(config, rdbTable, col, startDef.columns[col], startFks, startProvider);
            const after = reshapeColumn(config, rdbTable, col, endDef.columns[col], endFks, provider);
            if (!sameProjection(before, after)) { tables.add(rdbTable); break; }
        }
    }
    return tables;
}
