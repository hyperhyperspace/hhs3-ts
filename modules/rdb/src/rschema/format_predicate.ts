import { json } from "@hyper-hyper-space/hhs3_json";

import type { Operand, Predicate } from "./payload.js";
import { parseRowFieldTerm } from "./payload.js";
import type { RowOpPayload } from "../rtable/payload.js";

export type FormatPredicateScope = { gatedTable?: string };

export function formatPredicate(pred: Predicate, scope: FormatPredicateScope = {}): string {
    switch (pred.p) {
        case 'true':
            return 'true';
        case 'false':
            return 'false';
        case 'exists': {
            const existsTable = pred.table;
            const isSelfRef = scope.gatedTable !== undefined && existsTable === scope.gatedTable;
            const alias = isSelfRef ? selfReferentialExistsAlias(existsTable) : undefined;
            const existsRef = alias !== undefined ? `${existsTable} AS ${alias}` : existsTable;
            const existsQual = alias ?? existsTable;
            const where = pred.where !== undefined
                ? ` WHERE ${Object.entries(pred.where).map(([k, v]) => `${formatExistsWhereKey(k, existsQual)} = ${formatExistsWhereValue(v, scope.gatedTable)}`).join(' AND ')}`
                : '';
            return `EXISTS ${existsRef}${where}`;
        }
        case 'cmp':
            return `${formatOperand(pred.left, scope.gatedTable, false)} ${formatCmp(pred.cmp)} ${formatOperand(pred.right, scope.gatedTable, false)}`;
        case 'str':
            return `${formatOperand(pred.value, scope.gatedTable, false)} LIKE ${formatStringPattern(pred.str, pred.sub)}`;
        case 'and':
            return pred.args.map((a) => formatPredicate(a, scope)).join(' AND ');
        case 'or':
            return pred.args.map((a) => formatPredicate(a, scope)).join(' OR ');
    }
}

export function formatRestrictionFailureReason(
    table: string,
    op: RowOpPayload,
    rule: Predicate,
): string {
    return `${table} ${op.action} on row '${op.rowId}' does not satisfy ALLOW ${op.action} IF ${formatPredicate(rule, { gatedTable: table })}`;
}

export function formatRowNotLiveFailureReason(
    table: string,
    op: Pick<RowOpPayload, 'action' | 'rowId'>,
): string {
    return `${table} ${op.action} on row '${op.rowId}': rowId is not live in table '${table}'`;
}

function selfReferentialExistsAlias(tableName: string): string {
    const letter = tableName[0];
    return tableName.length === 1 ? `${letter}2` : letter;
}

function formatExistsWhereKey(key: string, existsQual: string): string {
    return `${existsQual}.${key}`;
}

function formatExistsWhereValue(value: json.Literal | string, gatedTable?: string): string {
    if (typeof value === 'string' && value === '$author') return '$author';
    if (typeof value === 'string') {
        const col = parseRowFieldTerm(value);
        if (col !== undefined) {
            return gatedTable !== undefined ? `${gatedTable}.${col}` : col;
        }
    }
    return formatLiteral(value as json.Literal);
}

function formatOperand(operand: Operand, gatedTable?: string, bareTopLevel = true): string {
    if ('col' in operand) {
        const col = operand.col;
        if (gatedTable !== undefined && !bareTopLevel) return `${gatedTable}.${col}`;
        return col;
    }
    if ('lit' in operand) return formatTermOrLiteral(operand.lit);
    if ('fn' in operand) return 'NULL';
    return 'NULL';
}

function formatTermOrLiteral(value: json.Literal | string): string {
    if (typeof value === 'string' && value.startsWith('$')) {
        if (value === '$author') return '$author';
    }
    return formatLiteral(value as json.Literal);
}

function formatCmp(cmp: string): string {
    return ({ eq: '=', ne: '!=', lt: '<', le: '<=', gt: '>', ge: '>=' } as Record<string, string>)[cmp] ?? '=';
}

function formatStringPattern(op: string, sub: Operand): string {
    const lit = 'lit' in sub && typeof sub.lit === 'string' ? sub.lit : '';
    if (op === 'prefix') return sqlString(`${lit}%`);
    if (op === 'suffix') return sqlString(`%${lit}`);
    return sqlString(`%${lit}%`);
}

function formatLiteral(value: json.Literal): string {
    if (typeof value === 'string') return sqlString(value);
    return json.toStringNormalized(value);
}

function sqlString(value: string): string {
    return `'${value.replace(/'/g, "''")}'`;
}
