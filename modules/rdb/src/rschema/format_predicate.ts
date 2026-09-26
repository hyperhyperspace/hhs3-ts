import { json } from "@hyper-hyper-space/hhs3_json";

import type { Operand, Predicate, WhereValue } from "./payload.js";
import { parseRowFieldTerm } from "./payload.js";
import type { RowOpPayload } from "../rtable/payload.js";

// quoteIdent renders a table / column name; C-SQL passes one that quotes
// names colliding with its keywords.
export type FormatPredicateScope = { gatedTable?: string; quoteIdent?: (name: string) => string };

type FormatContext = { gatedTable?: string; ident: (name: string) => string };

// Renders C-SQL that parses back to the same predicate: groups get only the
// parentheses the grammar needs (OR inside AND, a group inside a group of
// the same kind, which would otherwise merge into it, and EXISTS, whose
// WHERE takes the rest of the condition).
export function formatPredicate(pred: Predicate, scope: FormatPredicateScope = {}): string {
    return formatPred(pred, { gatedTable: scope.gatedTable, ident: scope.quoteIdent ?? ((name) => name) });
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

function formatPred(pred: Predicate, ctx: FormatContext): string {
    switch (pred.p) {
        case 'true':
            return 'true';
        case 'false':
            return 'false';
        case 'exists':
            return formatExists(pred, ctx);
        case 'cmp':
            return `${formatOperand(pred.left, ctx, PREC_SUM)} ${formatCmp(pred.cmp)} ${formatOperand(pred.right, ctx, PREC_SUM)}`;
        case 'like':
            return `${formatOperand(pred.value, ctx, PREC_SUM)} LIKE ${formatOperand(pred.pattern, ctx, PREC_SUM)}`;
        case 'and':
        case 'or':
            return pred.args.map((a) => formatGroupMember(a, pred.p, ctx)).join(pred.p === 'and' ? ' AND ' : ' OR ');
    }
}

function formatGroupMember(member: Predicate, parent: 'and' | 'or', ctx: FormatContext): string {
    const text = formatPred(member, ctx);
    const needsParens = member.p === 'exists'
        || member.p === parent
        || (member.p === 'or' && parent === 'and');
    return needsParens ? `(${text})` : text;
}

function formatExists(pred: Extract<Predicate, { p: 'exists' }>, ctx: FormatContext): string {
    // C-SQL resolves a qualifier naming the exists table's bare name to the
    // exists row, so when that is also the gated table's name (self-reference,
    // or a same-named table in another group) the exists table gets an alias.
    const bare = pred.table.substring(pred.table.indexOf('.') + 1);
    const alias = ctx.gatedTable !== undefined && bare === ctx.gatedTable ? existsAlias(bare) : undefined;
    const table = pred.table.split('.').map(ctx.ident).join('.');
    const existsRef = alias !== undefined ? `${table} AS ${ctx.ident(alias)}` : table;
    const existsQual = alias !== undefined ? ctx.ident(alias) : table;
    const conds = Object.entries(pred.where)
        .map(([k, v]) => `${existsQual}.${ctx.ident(k)} = ${formatExistsWhereValue(v, ctx)}`);
    return conds.length > 0 ? `EXISTS ${existsRef} WHERE ${conds.join(' AND ')}` : `EXISTS ${existsRef}`;
}

function existsAlias(tableName: string): string {
    const letter = tableName[0];
    return tableName.length === 1 ? `${letter}2` : letter;
}

function formatExistsWhereValue(value: WhereValue, ctx: FormatContext): string {
    if (value === '$author') return '$author';
    if (typeof value === 'string') {
        const col = parseRowFieldTerm(value);
        if (col !== undefined) return formatColumn(col, ctx);
    }
    return formatLiteral(value);
}

// Operand precedence, loosest first; an operand below the minimum its
// position allows is parenthesized.
const PREC_SUM = 1;
const PREC_PRODUCT = 2;
const PREC_UNARY = 3;
const PREC_PRIMARY = 4;

function formatOperand(operand: Operand, ctx: FormatContext, minPrec: number): string {
    const [text, prec] = formatOperandPrec(operand, ctx);
    return prec < minPrec ? `(${text})` : text;
}

function formatOperandPrec(operand: Operand, ctx: FormatContext): [string, number] {
    if ('col' in operand) return [formatColumn(operand.col, ctx), PREC_PRIMARY];
    if ('lit' in operand) {
        const lit = operand.lit;
        // A negative number is unary minus applied to its magnitude.
        return [lit === '$author' ? '$author' : formatLiteral(lit), typeof lit === 'number' && lit < 0 ? PREC_UNARY : PREC_PRIMARY];
    }
    if (operand.fn === 'len') return [`length(${formatOperand(operand.args[0], ctx, PREC_SUM)})`, PREC_PRIMARY];
    const prec = operand.fn === 'mul' ? PREC_PRODUCT : PREC_SUM;
    const symbol = { add: '+', sub: '-', mul: '*' }[operand.fn];
    // Left-associative: only the right side needs parens at equal precedence.
    const left = formatOperand(operand.args[0], ctx, prec);
    const right = formatOperand(operand.args[1], ctx, prec + 1);
    return [`${left} ${symbol} ${right}`, prec];
}

function formatColumn(col: string, ctx: FormatContext): string {
    return ctx.gatedTable !== undefined ? `${ctx.ident(ctx.gatedTable)}.${ctx.ident(col)}` : ctx.ident(col);
}

function formatCmp(cmp: string): string {
    return ({ eq: '=', ne: '!=', lt: '<', le: '<=', gt: '>', ge: '>=' } as Record<string, string>)[cmp] ?? '=';
}

function formatLiteral(value: json.Literal): string {
    if (typeof value === 'string') return sqlString(value);
    if (typeof value === 'object') return `JSON ${sqlString(json.toStringCanonical(value))}`;
    return json.toStringCanonical(value);
}

function sqlString(value: string): string {
    return `'${value.replace(/'/g, "''")}'`;
}
