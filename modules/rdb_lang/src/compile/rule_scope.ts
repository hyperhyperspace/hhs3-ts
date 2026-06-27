import type { RSchemaView } from "@hyper-hyper-space/hhs3_rdb";
import type { TableDef } from "@hyper-hyper-space/hhs3_rdb";

import type { MigrationRuleExpr, TableDecl } from "../syntax/ast.js";

export type ColumnsOf = (tableRef: string) => Set<string> | undefined;

export type GatedTable = {
    name: string;
    columns: Set<string>;
};

export type ExistsScope = {
    table: string;
    alias?: string;
    columns?: Set<string>;
};

export type RuleScope = {
    gated?: GatedTable;
    columnsOf: ColumnsOf;
    exists?: ExistsScope;
};

export type ResolvedColumnRole = 'gated' | 'exists';

export type ResolvedColumn = {
    role: ResolvedColumnRole;
    column: string;
};

const ROW_AUTHOR = 'rowAuthor';

export function splitExistsTableRef(ref: string): { group: string | undefined; table: string; bare: string } {
    const idx = ref.indexOf('.');
    if (idx < 0) return { group: undefined, table: ref, bare: ref };
    return { group: ref.substring(0, idx), table: ref.substring(idx + 1), bare: ref.substring(idx + 1) };
}

export function isLocalTableRef(ref: string): boolean {
    return splitExistsTableRef(ref).group === undefined;
}

export function columnSetFromTableDecl(table: TableDecl): Set<string> {
    const cols = new Set(table.columns.map((c) => c.name));
    cols.add(ROW_AUTHOR);
    return cols;
}

export function columnSetFromTableDef(def: TableDef): Set<string> {
    const cols = new Set(Object.keys(def.columns));
    cols.add(ROW_AUTHOR);
    return cols;
}

export function columnsOfFromTableDecls(tables: TableDecl[]): ColumnsOf {
    const map = new Map<string, Set<string>>();
    for (const table of tables) map.set(table.name, columnSetFromTableDecl(table));
    return (tableRef) => {
        if (tableRef.includes('.')) return undefined;
        return map.get(tableRef);
    };
}

export function columnsOfFromSchemaView(view: RSchemaView): ColumnsOf {
    const map = new Map<string, Set<string>>();
    for (const name of view.getTableNames()) {
        const def = view.getTable(name);
        if (def !== undefined) map.set(name, columnSetFromTableDef(def));
    }
    return (tableRef) => {
        if (tableRef.includes('.')) return undefined;
        return map.get(tableRef);
    };
}

export function buildAlterColumnsOf(view: RSchemaView, rules: MigrationRuleExpr[]): ColumnsOf {
    const map = new Map<string, Set<string>>();
    for (const name of view.getTableNames()) {
        const def = view.getTable(name);
        if (def !== undefined) map.set(name, columnSetFromTableDef(def));
    }

    for (const rule of rules) {
        switch (rule.kind) {
            case 'add-table':
                map.set(rule.table.name, columnSetFromTableDecl(rule.table));
                break;
            case 'drop-table':
                map.delete(rule.table);
                break;
            case 'add-column': {
                const cols = map.get(rule.table);
                if (cols !== undefined) cols.add(rule.column.name);
                break;
            }
            case 'drop-column': {
                const cols = map.get(rule.table);
                if (cols !== undefined) cols.delete(rule.column);
                break;
            }
            default:
                break;
        }
    }

    return (tableRef) => {
        if (tableRef.includes('.')) return undefined;
        return map.get(tableRef);
    };
}

export function existsColumns(scope: RuleScope): Set<string> | undefined {
    if (scope.exists === undefined) return undefined;
    if (scope.exists.columns !== undefined) return scope.exists.columns;
    return scope.columnsOf(scope.exists.table);
}

export function isSelfReferentialExists(scope: RuleScope, existsTable: string): boolean {
    if (scope.gated === undefined) return false;
    if (!isLocalTableRef(existsTable)) return false;
    return splitExistsTableRef(existsTable).bare === scope.gated.name;
}

export function existsTableMatchesQualifier(scope: RuleScope, qualifier: string): boolean {
    const exists = scope.exists;
    if (exists === undefined) return false;
    if (exists.alias !== undefined && qualifier === exists.alias) return true;
    if (exists.alias !== undefined) return false;
    const { group, table, bare } = splitExistsTableRef(exists.table);
    if (qualifier === bare || qualifier === table) return true;
    if (group !== undefined && qualifier === exists.table) return true;
    return false;
}

export function gatedTableMatchesQualifier(scope: RuleScope, qualifier: string): boolean {
    return scope.gated !== undefined && qualifier === scope.gated.name;
}
