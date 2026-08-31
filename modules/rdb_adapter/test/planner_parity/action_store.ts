import type { json } from "@hyper-hyper-space/hhs3_json";

import type { ColumnDef } from "@hyper-hyper-space/hhs3_rdb";
import type { RowAction, SchemaAction, SchemaActionColumn } from "../../src/types.js";

import {
    canonicalFingerprint, sortFingerprint, type ColumnFp, type RowFp, type StoreFingerprint, type TableFp,
} from "./fingerprint.js";

type ColState = { def: ColumnDef; fkTarget?: string };

type TableState = {
    columns: Map<string, ColState>;
    rows: Map<string, { author?: string; values: { [column: string]: json.Literal } }>;
};

// Test-only interpreter of SchemaAction / RowAction lists. Rows are keyed by
// rdb rowId; FK values stay rowIds (no serial-id interning). add-column and
// new-row upserts materialize schema defaults so the store matches an rdb live
// view after those actions.
export class ActionStore {
    private tables = new Map<string, TableState>();

    apply(schemaActions: SchemaAction[], rowActions: RowAction[]): void {
        for (const a of schemaActions) this.applySchema(a);
        for (const a of rowActions) this.applyRow(a);
    }

    fingerprint(): StoreFingerprint {
        const tables: TableFp[] = [];
        for (const [name, state] of this.tables) {
            const columns: ColumnFp[] = [];
            for (const [col, meta] of state.columns) {
                const c: ColumnFp = { name: col, type: meta.def.type };
                if (meta.fkTarget !== undefined) c.fkTarget = meta.fkTarget;
                columns.push(c);
            }
            const rows: RowFp[] = [];
            for (const [rowId, row] of state.rows) {
                const r: RowFp = { rowId, values: { ...row.values } };
                if (row.author !== undefined) r.author = row.author;
                rows.push(r);
            }
            tables.push({ name, columns, rows });
        }
        return sortFingerprint({ tables });
    }

    canonical(): string {
        return canonicalFingerprint(this.fingerprint());
    }

    private applySchema(action: SchemaAction): void {
        switch (action.kind) {
            case 'create-table': {
                const columns = new Map<string, ColState>();
                for (const c of action.columns) columns.set(c.name, colState(c));
                this.tables.set(action.table, { columns, rows: new Map() });
                return;
            }
            case 'drop-table': {
                this.tables.delete(action.table);
                return;
            }
            case 'add-column': {
                const table = requireTable(this.tables, action.table);
                table.columns.set(action.column, {
                    def: action.def,
                    fkTarget: action.fk?.targetTable,
                });
                if (action.def.default !== undefined) {
                    for (const row of table.rows.values()) {
                        if (row.values[action.column] === undefined) row.values[action.column] = action.def.default;
                    }
                }
                return;
            }
            case 'drop-column': {
                const table = requireTable(this.tables, action.table);
                table.columns.delete(action.column);
                for (const row of table.rows.values()) delete row.values[action.column];
                return;
            }
        }
    }

    private applyRow(action: RowAction): void {
        if (action.kind === 'delete-row') {
            const table = this.tables.get(action.table);
            table?.rows.delete(action.rowId);
            return;
        }
        const table = requireTable(this.tables, action.table);
        const existing = table.rows.get(action.rowId);
        if (existing !== undefined) {
            for (const [column, value] of Object.entries(action.values)) existing.values[column] = value;
            if (action.author !== undefined) existing.author = action.author;
            return;
        }
        const values: { [column: string]: json.Literal } = { ...action.values };
        for (const [column, meta] of table.columns) {
            if (values[column] === undefined && meta.def.default !== undefined) values[column] = meta.def.default;
        }
        const row: { author?: string; values: { [column: string]: json.Literal } } = { values };
        if (action.author !== undefined) row.author = action.author;
        table.rows.set(action.rowId, row);
    }
}

function colState(c: SchemaActionColumn): ColState {
    const out: ColState = { def: c.def };
    if (c.fk?.targetTable !== undefined) out.fkTarget = c.fk.targetTable;
    return out;
}

function requireTable(tables: Map<string, TableState>, name: string): TableState {
    const table = tables.get(name);
    if (table === undefined) throw new Error(`ActionStore: no table '${name}'`);
    return table;
}
