import type { json } from "@hyper-hyper-space/hhs3_json";
import type { Operand, Predicate } from "@hyper-hyper-space/hhs3_rdb";
import type { RowFilter, RowQuery } from "@hyper-hyper-space/hhs3_rdb";

import type { OperandExpr, PredicateExpr, SelectStatement, ValueExpr } from "../syntax/ast.js";
import type { LangBindContext } from "../bind/context.js";
import { asJsonLiteral, resolveValue } from "../bind/values.js";
import type { ResolvedColumn, ResolvedColumnRole, RuleScope } from "./rule_scope.js";
import {
    columnsOfFromTableDecls,
    existsColumns,
    existsTableMatchesQualifier,
    gatedTableMatchesQualifier,
    isSelfReferentialExists,
    splitExistsTableRef,
} from "./rule_scope.js";

export type { ColumnsOf, GatedTable, RuleScope } from "./rule_scope.js";
export { columnsOfFromTableDecls, columnsOfFromSchemaView, buildAlterColumnsOf } from "./rule_scope.js";

const CMP: Record<string, 'eq' | 'ne' | 'lt' | 'le' | 'gt' | 'ge'> = {
    '=': 'eq',
    '!=': 'ne',
    '<': 'lt',
    '<=': 'le',
    '>': 'gt',
    '>=': 'ge',
};

const EMPTY_SCOPE: RuleScope = {
    columnsOf: () => undefined,
};

export async function lowerSelectQuery(stmt: SelectStatement, context: LangBindContext): Promise<RowQuery> {
    const query: RowQuery = {};
    if (stmt.projection !== '*') query.select = stmt.projection;
    if (stmt.where !== undefined) query.where = await lowerRowFilter(stmt.where, context);
    if (stmt.orderBy.length > 0) query.orderBy = stmt.orderBy.map((o) => ({ column: o.column, ...(o.dir !== undefined ? { dir: o.dir } : {}) }));
    if (stmt.limit !== undefined) query.limit = stmt.limit;
    if (stmt.offset !== undefined) query.offset = stmt.offset;
    return query;
}

export async function lowerRowFilter(expr: PredicateExpr, context: LangBindContext): Promise<RowFilter> {
    switch (expr.kind) {
        case 'true':
            return { p: 'true' };
        case 'false':
            return { p: 'not', arg: { p: 'true' } };
        case 'comparison':
            return {
                p: 'cmp',
                cmp: CMP[expr.op],
                left: await lowerQueryOperand(expr.left, context),
                right: await lowerQueryOperand(expr.right, context),
            };
        case 'like':
            return lowerLike(expr.left, expr.pattern, context);
        case 'not':
            return { p: 'not', arg: await lowerRowFilter(expr.arg, context) };
        case 'and':
            return { p: 'and', args: await Promise.all(expr.args.map((a) => lowerRowFilter(a, context))) };
        case 'or':
            return { p: 'or', args: await Promise.all(expr.args.map((a) => lowerRowFilter(a, context))) };
        case 'exists':
            throw new Error('EXISTS is only supported in schema allow rules');
    }
}

async function lowerLike(left: OperandExpr, pattern: ValueExpr, context: LangBindContext): Promise<RowFilter> {
    const value = await resolveValue(pattern, context);
    if (typeof value !== 'string') throw new Error('LIKE pattern must be a string');
    const operand = await lowerQueryOperand(left, context);
    if (value.startsWith('%') && value.endsWith('%') && value.length >= 2) {
        return { p: 'str', str: 'contains', value: operand, sub: { lit: value.slice(1, -1) } };
    }
    if (value.startsWith('%')) {
        return { p: 'str', str: 'suffix', value: operand, sub: { lit: value.slice(1) } };
    }
    if (value.endsWith('%')) {
        return { p: 'str', str: 'prefix', value: operand, sub: { lit: value.slice(0, -1) } };
    }
    return { p: 'cmp', cmp: 'eq', left: operand, right: { lit: value } };
}

async function lowerQueryOperand(expr: OperandExpr, context: LangBindContext): Promise<Operand> {
    if (expr.kind === 'column') return { col: expr.name };
    const value = asJsonLiteral(await resolveValue(expr, context));
    if (!isScalarQueryLiteral(value)) throw new Error('query literal must be a string, number or boolean');
    return { lit: value };
}

export function lowerRestrictionPredicate(expr: PredicateExpr, scope: RuleScope = EMPTY_SCOPE): Predicate {
    switch (expr.kind) {
        case 'true':
            return { p: 'true' };
        case 'false':
            return { p: 'false' };
        case 'comparison':
            return {
                p: 'cmp',
                cmp: CMP[expr.op],
                left: lowerRestrictionOperand(expr.left, scope),
                right: lowerRestrictionOperand(expr.right, scope),
            };
        case 'like': {
            const left = lowerRestrictionOperand(expr.left, scope);
            if (expr.pattern.kind !== 'literal' || typeof expr.pattern.value !== 'string') {
                throw new Error('allow rule LIKE pattern must be a string literal');
            }
            const pattern = expr.pattern.value;
            if (pattern.startsWith('%') && pattern.endsWith('%') && pattern.length >= 2) {
                return { p: 'str', str: 'contains', value: left, sub: { lit: pattern.slice(1, -1) } };
            }
            if (pattern.startsWith('%')) return { p: 'str', str: 'suffix', value: left, sub: { lit: pattern.slice(1) } };
            if (pattern.endsWith('%')) return { p: 'str', str: 'prefix', value: left, sub: { lit: pattern.slice(0, -1) } };
            return { p: 'cmp', cmp: 'eq', left, right: { lit: pattern } };
        }
        case 'exists': {
            if (isSelfReferentialExists(scope, expr.table) && expr.alias === undefined) {
                throw new Error(`EXISTS ${expr.table} is self-referential; use AS alias`);
            }
            const existsScope: RuleScope = {
                ...scope,
                exists: {
                    table: expr.table,
                    alias: expr.alias,
                    columns: existsColumns({
                        ...scope,
                        exists: { table: expr.table, alias: expr.alias },
                    }),
                },
            };
            return { p: 'exists', table: expr.table, where: lowerExistsWhere(expr.where, existsScope) };
        }
        case 'not':
            throw new Error('NOT is not supported in schema allow rules');
        case 'and':
            return { p: 'and', args: expr.args.map((a) => lowerRestrictionPredicate(a, scope)) };
        case 'or':
            return { p: 'or', args: expr.args.map((a) => lowerRestrictionPredicate(a, scope)) };
    }
}

function lowerExistsWhere(expr: PredicateExpr, scope: RuleScope): { [field: string]: json.Literal | '$author' | string } {
    if (expr.kind === 'comparison' && expr.op === '=') {
        return lowerExistsEquality(expr.left, expr.right, scope);
    }
    if (expr.kind === 'and') {
        const out: { [field: string]: json.Literal | '$author' | string } = {};
        for (const arg of expr.args) Object.assign(out, lowerExistsWhere(arg, scope));
        return out;
    }
    throw new Error('EXISTS WHERE only supports equality predicates');
}

function lowerExistsEquality(
    left: OperandExpr,
    right: OperandExpr,
    scope: RuleScope,
): { [field: string]: json.Literal | '$author' | string } {
    const leftIsColumn = left.kind === 'column';
    const rightIsColumn = right.kind === 'column';

    if (leftIsColumn && !rightIsColumn) {
        const existsCol = resolveExistsFilterColumn(left, scope);
        return { [existsCol.column]: lowerExistsWhereValue(right, scope) };
    }
    if (rightIsColumn && !leftIsColumn) {
        const existsCol = resolveExistsFilterColumn(right, scope);
        return { [existsCol.column]: lowerExistsWhereValue(left, scope) };
    }
    if (leftIsColumn && rightIsColumn) {
        const l = resolveColumnReference(left, scope);
        const r = resolveColumnReference(right, scope);
        if (l.role === 'exists' && r.role === 'gated') {
            return { [l.column]: `$row.${r.column}` };
        }
        if (r.role === 'exists' && l.role === 'gated') {
            return { [r.column]: `$row.${l.column}` };
        }
        if (l.role === 'exists' && r.role === 'exists') {
            throw new Error('EXISTS WHERE cannot compare two EXISTS table columns');
        }
        throw new Error('EXISTS WHERE correlation must reference the gated table');
    }
    throw new Error('EXISTS WHERE only supports equality predicates');
}

function resolveExistsFilterColumn(expr: OperandExpr, scope: RuleScope): ResolvedColumn {
    if (expr.kind !== 'column') throw new Error('EXISTS WHERE filter must be a column');
    if (expr.table !== undefined) {
        const resolved = resolveQualifiedColumn(expr.table, expr.name, scope);
        if (resolved.role !== 'exists') {
            throw new Error(`EXISTS WHERE filter must reference the EXISTS table, not '${expr.table}.${expr.name}'`);
        }
        return resolved;
    }
    return { role: 'exists', column: expr.name };
}

function resolveColumnReference(expr: OperandExpr, scope: RuleScope): ResolvedColumn {
    if (expr.kind !== 'column') throw new Error('Expected column reference');
    if (expr.table !== undefined) return resolveQualifiedColumn(expr.table, expr.name, scope);
    return resolveUnqualifiedColumn(expr.name, scope);
}

function resolveQualifiedColumn(qualifier: string, column: string, scope: RuleScope): ResolvedColumn {
    const matchesGated = gatedTableMatchesQualifier(scope, qualifier);
    const matchesExists = scope.exists !== undefined && existsTableMatchesQualifier(scope, qualifier);
    if (matchesGated && matchesExists) {
        return { role: 'gated', column };
    }
    if (matchesExists) return { role: 'exists', column };
    if (matchesGated) return { role: 'gated', column };
    throw new Error(`unknown table qualifier '${qualifier}'`);
}

function resolveUnqualifiedColumn(column: string, scope: RuleScope): ResolvedColumn {
    if (scope.exists === undefined) {
        if (scope.gated === undefined) {
            throw new Error('column references are not allowed in this predicate context');
        }
        return { role: 'gated', column };
    }

    if (isSelfReferentialExists(scope, scope.exists.table) && scope.exists.alias === undefined) {
        throw new Error(`ambiguous column '${column}'; self-referential EXISTS requires AS alias`);
    }

    const inT = scope.gated !== undefined && scope.gated.columns.has(column);
    const eCols = existsColumns(scope);
    const inE = eCols?.has(column) ?? false;

    if (eCols !== undefined) {
        const candidates: ResolvedColumnRole[] = [];
        if (inT) candidates.push('gated');
        if (inE) candidates.push('exists');
        if (candidates.length === 0) throw new Error(`unknown column '${column}'`);
        if (candidates.length === 1) return { role: candidates[0], column };
        throw new Error(`ambiguous column '${column}'; qualify with table name (exists in both '${scope.gated!.name}' and '${splitExistsTableRef(scope.exists.table).bare}')`);
    }

    if (inT) {
        throw new Error(`ambiguous column '${column}'; qualify with table name`);
    }
    return { role: 'exists', column };
}

function lowerRestrictionOperand(expr: OperandExpr, scope: RuleScope): Operand {
    if (expr.kind === 'column') {
        if (scope.exists !== undefined) {
            throw new Error('column references in EXISTS WHERE must appear inside the EXISTS WHERE clause');
        }
        const resolved = expr.table !== undefined
            ? resolveQualifiedColumn(expr.table, expr.name, scope)
            : resolveUnqualifiedColumn(expr.name, scope);
        if (resolved.role !== 'gated') {
            throw new Error('allow rule operands must reference the gated table');
        }
        return { col: resolved.column };
    }
    if (expr.kind === 'literal') {
        if (expr.value === null) throw new Error('NULL is not supported in allow rule operands');
        return { lit: expr.value };
    }
    if (expr.kind === 'variable') {
        if (expr.field !== undefined) {
            throw new Error('$row.<column> is no longer supported; use table.column instead');
        }
        if (expr.name === 'author') return { lit: '$author' };
        throw new Error('Only $author is supported in allow rule operands');
    }
    throw new Error('Only $author is supported in allow rule operands');
}

function lowerExistsWhereValue(expr: OperandExpr, scope: RuleScope): json.Literal | '$author' | string {
    if (expr.kind === 'literal') {
        if (expr.value === null) throw new Error('NULL is not supported in EXISTS WHERE values');
        return expr.value;
    }
    if (expr.kind === 'variable') {
        if (expr.field !== undefined) {
            throw new Error('$row.<column> is no longer supported; use table.column instead');
        }
        return lowerIdTerm(expr);
    }
    if (expr.kind === 'column') {
        const resolved = expr.table !== undefined
            ? resolveQualifiedColumn(expr.table, expr.name, scope)
            : resolveUnqualifiedColumn(expr.name, scope);
        if (resolved.role !== 'gated') {
            throw new Error('EXISTS WHERE correlation must reference the gated table');
        }
        return `$row.${resolved.column}`;
    }
    throw new Error('EXISTS WHERE value must be a literal, $author, or gated-table column');
}

function lowerIdTerm(expr: ValueExpr): '$author' {
    if (expr.kind === 'variable' && expr.field === undefined && expr.name === 'author') return '$author';
    throw new Error('Expected $author identity term');
}

function isScalarQueryLiteral(value: json.Literal): value is string | number | boolean {
    return typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean';
}
