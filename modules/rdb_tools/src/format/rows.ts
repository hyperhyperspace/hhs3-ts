import type { HashDisplayContext } from "./display.js";
import { STRUCTURAL_HASH_COLUMNS } from "./display.js";

export type FormatRowsOptions = {
    ctx?: HashDisplayContext;
    structuralColumns?: Set<string>;
};

export function formatRows(
    rows: Record<string, unknown>[],
    columns?: string[],
    options?: FormatRowsOptions,
): string {
    if (rows.length === 0) return '(0 rows)';
    const cols = columns ?? [...new Set(rows.flatMap((row) => Object.keys(row)))];
    const structural = options?.structuralColumns ?? STRUCTURAL_HASH_COLUMNS;
    const rendered = rows.map((row) => cols.map((column) => renderCell(
        row[column],
        options?.ctx,
        structural.has(column) ? 'hash' : 'cell',
    )));
    const widths = cols.map((column, i) => Math.max(column.length, ...rendered.map((row) => row[i].length)));
    const header = cols.map((column, i) => column.padEnd(widths[i])).join(' | ');
    const sep = widths.map((width) => '-'.repeat(width)).join('-+-');
    const body = rendered.map((row) => row.map((cell, i) => cell.padEnd(widths[i])).join(' | ')).join('\n');
    return `${header}\n${sep}\n${body}`;
}

export function formatRowsVertical(
    rows: Record<string, unknown>[],
    columns?: string[],
    options?: FormatRowsOptions,
): string {
    if (rows.length === 0) return '(0 rows)';
    const cols = columns ?? [...new Set(rows.flatMap((row) => Object.keys(row)))];
    const structural = options?.structuralColumns ?? STRUCTURAL_HASH_COLUMNS;
    return rows
        .map((row, index) => {
            const header = `*** row ${index + 1} ***`;
            const lines = cols.map((column) => `${column}: ${renderCell(
                row[column],
                options?.ctx,
                structural.has(column) ? 'hash' : 'cell',
            )}`);
            return [header, ...lines].join('\n');
        })
        .join('\n\n');
}

function renderCell(
    value: unknown,
    ctx?: HashDisplayContext,
    role: 'hash' | 'cell' = 'cell',
): string {
    if (ctx !== undefined) return ctx.formatValue(value, { role });
    if (value === undefined) return '';
    if (value === null) return 'null';
    if (typeof value === 'object') return JSON.stringify(value);
    return String(value);
}
