import type { json } from "@hyper-hyper-space/hhs3_json";
import type { Operand, Predicate } from "@hyper-hyper-space/hhs3_rdb";
import type { RowFilter, RowQuery } from "@hyper-hyper-space/hhs3_rdb";
import { isValidLikePattern } from "@hyper-hyper-space/hhs3_rdb";

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

const ARITH_FN = { '+': 'add', '-': 'sub', '*': 'mul' } as const;

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
            return {
                p: 'like',
                value: await lowerQueryOperand(expr.left, context),
                pattern: lowerLikePattern(expr, await lowerQueryOperand(expr.pattern, context)),
            };
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

type LikeExpr = Extract<PredicateExpr, { kind: 'like' }>;

// The payload pattern for a lowered LIKE: a column pattern passes through; a
// literal pattern must be a string, and an ESCAPE clause (literal patterns
// only) is rewritten into the canonical `\` escape form.
function lowerLikePattern(expr: LikeExpr, pattern: Operand): Operand {
    if (expr.escape !== undefined && expr.pattern.kind !== 'literal') {
        throw new Error('LIKE ... ESCAPE requires a literal pattern');
    }
    if (!('lit' in pattern)) return pattern;
    if (typeof pattern.lit !== 'string') throw new Error('LIKE pattern must be a string');
    const canonical = expr.escape !== undefined ? canonicalLikePattern(pattern.lit, expr.escape) : pattern.lit;
    if (!isValidLikePattern(canonical)) throw new Error("LIKE pattern ends in an unescaped '\\'");
    return { lit: canonical };
}

// Rewrite a pattern written with `ESCAPE '<c>'` into the payload's form, where
// `\` is the escape character. An empty ESCAPE means no escape character.
export function canonicalLikePattern(pattern: string, escape: string): string {
    const esc = Array.from(escape);
    if (esc.length > 1) throw new Error('ESCAPE must be a single character');
    const escapeChar = esc[0];
    const cps = Array.from(pattern);
    let out = '';
    for (let k = 0; k < cps.length; k++) {
        const c = cps[k];
        if (c === escapeChar) {
            if (k + 1 === cps.length) throw new Error('LIKE pattern ends with its ESCAPE character');
            const next = cps[++k];
            out += next === '%' || next === '_' || next === '\\' ? `\\${next}` : next;
        } else if (c === '\\') {
            out += '\\\\';
        } else {
            out += c;
        }
    }
    return out;
}

async function lowerQueryOperand(expr: OperandExpr, context: LangBindContext): Promise<Operand> {
    switch (expr.kind) {
        case 'column':
            return { col: expr.name };
        case 'arith':
            return {
                fn: ARITH_FN[expr.op],
                args: [await lowerQueryOperand(expr.left, context), await lowerQueryOperand(expr.right, context)],
            };
        case 'length':
            return { fn: 'len', args: [await lowerQueryOperand(expr.arg, context)] };
    }
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
        case 'like':
            return {
                p: 'like',
                value: lowerRestrictionOperand(expr.left, scope),
                pattern: lowerLikePattern(expr, lowerRestrictionOperand(expr.pattern, scope)),
            };
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
    if (expr.kind === 'arith') {
        return { fn: ARITH_FN[expr.op], args: [lowerRestrictionOperand(expr.left, scope), lowerRestrictionOperand(expr.right, scope)] };
    }
    if (expr.kind === 'length') {
        return { fn: 'len', args: [lowerRestrictionOperand(expr.arg, scope)] };
    }
    if (expr.kind === 'literal') {
        if (expr.value === null) throw new Error('NULL is not supported in allow rule operands');
        rejectReservedString(expr.value);
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

// In allow rules a '$'-prefixed string payload value is a term ($author,
// $row.<col>), so a quoted '$...' literal would silently change meaning.
function rejectReservedString(value: json.Literal): void {
    if (typeof value === 'string' && value.startsWith('$')) {
        throw new Error(`string literal '${value}' is reserved in allow rules: strings starting with '$' denote terms (write $author, not '$author')`);
    }
}

function lowerExistsWhereValue(expr: OperandExpr, scope: RuleScope): json.Literal | '$author' | string {
    if (expr.kind === 'literal') {
        if (expr.value === null) throw new Error('NULL is not supported in EXISTS WHERE values');
        rejectReservedString(expr.value);
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
