import type { json } from "@hyper-hyper-space/hhs3_json";
import type { Operand, Predicate } from "@hyper-hyper-space/hhs3_rdb";
import type { RowFilter, RowQuery } from "@hyper-hyper-space/hhs3_rdb";

import type { OperandExpr, PredicateExpr, SelectStatement, ValueExpr } from "../syntax/ast.js";
import type { LangBindContext } from "../bind/context.js";
import { asJsonLiteral, resolveValue } from "../bind/values.js";

const CMP: Record<string, 'eq' | 'ne' | 'lt' | 'le' | 'gt' | 'ge'> = {
    '=': 'eq',
    '!=': 'ne',
    '<': 'lt',
    '<=': 'le',
    '>': 'gt',
    '>=': 'ge',
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

export function lowerRestrictionPredicate(expr: PredicateExpr): Predicate {
    switch (expr.kind) {
        case 'true':
            return { p: 'true' };
        case 'false':
            return { p: 'false' };
        case 'comparison':
            return {
                p: 'cmp',
                cmp: CMP[expr.op],
                left: lowerRestrictionOperand(expr.left),
                right: lowerRestrictionOperand(expr.right),
            };
        case 'like': {
            const left = lowerRestrictionOperand(expr.left);
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
            return { p: 'exists', table: expr.table, where: lowerExistsWhere(expr.where) };
        }
        case 'not':
            throw new Error('NOT is not supported in schema allow rules');
        case 'and':
            return { p: 'and', args: expr.args.map(lowerRestrictionPredicate) };
        case 'or':
            return { p: 'or', args: expr.args.map(lowerRestrictionPredicate) };
    }
}

function lowerExistsWhere(expr: PredicateExpr): { [field: string]: json.Literal | '$author' | string } {
    if (expr.kind === 'comparison' && expr.op === '=' && expr.left.kind === 'column') {
        return { [expr.left.name]: lowerWhereValue(expr.right) };
    }
    if (expr.kind === 'and') {
        const out: { [field: string]: json.Literal | '$author' | string } = {};
        for (const arg of expr.args) Object.assign(out, lowerExistsWhere(arg));
        return out;
    }
    throw new Error('EXISTS WHERE only supports equality predicates');
}

function lowerRestrictionOperand(expr: OperandExpr): Operand {
    if (expr.kind === 'column') return { col: expr.name };
    if (expr.kind === 'literal') {
        if (expr.value === null) throw new Error('NULL is not supported in allow rule operands');
        return { lit: expr.value };
    }
    if (expr.name === 'author') return { lit: '$author' };
    throw new Error('Only $author is supported in allow rule operands');
}

function lowerWhereValue(expr: OperandExpr): json.Literal | '$author' | string {
    if (expr.kind === 'column') throw new Error('EXISTS WHERE value must be a literal or identity term');
    if (expr.kind === 'literal') {
        if (expr.value === null) throw new Error('NULL is not supported in EXISTS WHERE values');
        return expr.value;
    }
    return lowerIdTerm(expr);
}

function lowerIdTerm(expr: ValueExpr): '$author' {
    if (expr.kind === 'variable' && expr.name === 'author') return '$author';
    throw new Error('Expected $author identity term');
}

function isScalarQueryLiteral(value: json.Literal): value is string | number | boolean {
    return typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean';
}
