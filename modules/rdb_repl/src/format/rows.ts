import type { HashDisplayContext } from "./display.js";
import { STRUCTURAL_HASH_COLUMNS } from "./display.js";

export type FormatRowsOptions = {
    ctx?: HashDisplayContext;
    structuralColumns?: Set<string>;
    identityColumns?: Set<string>;
};

export function formatRows(
    rows: Record<string, unknown>[],
    columns?: string[],
    options?: FormatRowsOptions,
): string {
    if (rows.length === 0) return '(0 rows)';
    const cols = columns ?? [...new Set(rows.flatMap((row) => Object.keys(row)))];
    const structural = options?.structuralColumns ?? STRUCTURAL_HASH_COLUMNS;
    const identity = options?.identityColumns ?? new Set<string>();
    const rendered = rows.map((row) => cols.map((column) =>
        renderCell(row[column], options?.ctx, cellOpts(column, structural, identity))));
    const widths = cols.map((column, i) => Math.max(column.length, ...rendered.map((row) => row[i].length)));
    const header = cols.map((column, i) => column.padEnd(widths[i])).join(' | ');
    const separator = widths.map((width) => '-'.repeat(width)).join('-+-');
    const body = rendered.map((row) => row.map((cell, i) => cell.padEnd(widths[i])).join(' | ')).join('\n');
    return `${header}\n${separator}\n${body}`;
}

export function formatRowsVertical(
    rows: Record<string, unknown>[],
    columns?: string[],
    options?: FormatRowsOptions,
): string {
    if (rows.length === 0) return '(0 rows)';
    const cols = columns ?? [...new Set(rows.flatMap((row) => Object.keys(row)))];
    const structural = options?.structuralColumns ?? STRUCTURAL_HASH_COLUMNS;
    const identity = options?.identityColumns ?? new Set<string>();
    return rows.map((row, index) => [
        `*** row ${index + 1} ***`,
        ...cols.map((column) =>
            `${column}: ${renderCell(row[column], options?.ctx, cellOpts(column, structural, identity))}`),
    ].join('\n')).join('\n\n');
}

function cellOpts(
    column: string, structural: Set<string>, identity: Set<string>,
): { role: 'hash' | 'cell'; identity?: boolean } {
    if (structural.has(column)) return { role: 'hash', identity: column === 'rowAuthor' };
    if (identity.has(column)) return { role: 'cell', identity: true };
    return { role: 'cell' };
}

function renderCell(
    value: unknown, ctx?: HashDisplayContext,
    opts: { role: 'hash' | 'cell'; identity?: boolean } = { role: 'cell' },
): string {
    if (ctx !== undefined) return ctx.formatValue(value, opts);
    if (value === undefined) return '';
    if (value === null) return 'null';
    if (typeof value === 'object') return JSON.stringify(value);
    return String(value);
}
