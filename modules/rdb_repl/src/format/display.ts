import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { LangExecutionResult } from "@hyper-hyper-space/hhs3_rdb_lang";
import type { HashWidth, ReplSession } from "../session.js";
import { formatRows, formatRowsVertical } from "./rows.js";

export const STRUCTURAL_HASH_COLUMNS = new Set(['rowId', 'rowAuthor']);
export type FormatStringRole = 'hash' | 'cell';
export type FormatStringOpts = { role?: FormatStringRole; hashPrefix?: boolean };

// Heuristic for a content hash rendered as base64 (so it can be truncated to a
// unique prefix in display). Content hashes are SHORT and padded; the upper
// bound keeps longer base64 `bytes` column values (which share the alphabet)
// from being mistaken for hashes and cosmetically truncated. This is a display
// heuristic only — the formatter has no per-column type context.
const MAX_SPECULATIVE_HASH_LENGTH = 64;

export function looksLikeSpeculativeHash(s: string): boolean {
    return s.length >= 30 && s.length <= MAX_SPECULATIVE_HASH_LENGTH
        && /^[A-Za-z0-9+/]+=*$/.test(s) && s.endsWith('=');
}

export function isTruncatable(value: string, role: FormatStringRole): boolean {
    return role === 'hash' || looksLikeSpeculativeHash(value);
}

export function uniquePrefixes(hashes: B64Hash[], minLen = 8): Map<B64Hash, string> {
    const out = new Map<B64Hash, string>();
    for (const hash of hashes) {
        for (let len = minLen; len <= hash.length; len++) {
            const prefix = hash.slice(0, len);
            if (hashes.filter((candidate) => candidate.startsWith(prefix)).length === 1) {
                out.set(hash, prefix);
                break;
            }
        }
        if (!out.has(hash)) out.set(hash, hash);
    }
    return out;
}

export function buildKeyLabelMap(session: ReplSession): Map<string, string> {
    const out = new Map<string, string>();
    for (const key of session.keyVault?.list() ?? []) out.set(key.keyId, `$${key.label}`);
    for (const entry of session.aliases.list('key')) {
        if (!out.has(entry.hash)) out.set(entry.hash, `$${entry.name}`);
    }
    return out;
}

export function collectTruncatableLiterals(value: unknown): string[] {
    if (typeof value === 'string') return isTruncatable(value, 'cell') ? [value] : [];
    if (Array.isArray(value)) return value.flatMap(collectTruncatableLiterals);
    if (value !== null && typeof value === 'object') {
        return Object.values(value).flatMap(collectTruncatableLiterals);
    }
    return [];
}

export function collectTruncatableFromColumnChanges(
    changes: Array<{ before?: unknown; after?: unknown }>,
): string[] {
    return changes.flatMap((change) => [
        ...(change.before === undefined ? [] : collectTruncatableLiterals(change.before)),
        ...(change.after === undefined ? [] : collectTruncatableLiterals(change.after)),
    ]);
}

export function collectTruncatableStrings(
    rows: Record<string, unknown>[],
    structuralColumns: Set<string> = STRUCTURAL_HASH_COLUMNS,
): string[] {
    const out = new Set<string>();
    for (const row of rows) {
        for (const [column, value] of Object.entries(row)) {
            if (typeof value === 'string' && (structuralColumns.has(column) || looksLikeSpeculativeHash(value))) {
                out.add(value);
            }
        }
    }
    return [...out];
}

export function collectTruncatableFromResult(result: LangExecutionResult): string[] {
    switch (result.kind) {
        case 'select':
            return collectTruncatableStrings(result.rows.map((row) => ({
                rowId: row.rowId,
                ...(row.author === undefined ? {} : { rowAuthor: row.author }),
                ...row.values,
            })));
        case 'log':
            return result.rows.flatMap((row) => [row.fullHash, ...row.prev]);
        case 'add-member':
            return [result.memberId, result.database, result.entryHash];
        case 'insert':
        case 'update':
        case 'delete':
            return [result.rowId, result.entryHash];
        case 'bundle':
            return [result.entryHash];
        case 'alter-schema':
            return [result.schema, result.entryHash];
        case 'update-schema':
            return [result.group, result.entryHash];
        case 'update-ref':
            return [result.ref, result.group, result.entryHash];
        default:
            return [];
    }
}

function truncateHash(value: string, width: HashWidth, prefixes: Map<B64Hash, string>): string {
    if (width === 'full') return value;
    return typeof width === 'number' ? value.slice(0, width) : prefixes.get(value) ?? value;
}

export class HashDisplayContext {
    private readonly keyMap: Map<string, string>;
    private readonly prefixMap: Map<B64Hash, string>;

    constructor(private readonly session: ReplSession, values: Iterable<string>) {
        this.keyMap = buildKeyLabelMap(session);
        const hashes = [...new Set([...values].filter((value) => value.length > 0))];
        this.prefixMap = session.hashWidth === 'auto' ? uniquePrefixes(hashes) : new Map();
    }

    formatString(value: string, opts: FormatStringOpts = {}): string {
        const role = opts.role ?? 'cell';
        if (this.session.hashLabels) {
            const label = this.keyMap.get(value);
            if (label !== undefined) return label;
        }
        if (!isTruncatable(value, role)) return value;
        let rendered = truncateHash(value, this.session.hashWidth, this.prefixMap);
        if (opts.hashPrefix === true) rendered = `#${rendered}`;
        return rendered;
    }

    formatValue(value: unknown, opts: FormatStringOpts = {}): string {
        if (value === undefined) return '';
        if (value === null) return 'null';
        if (typeof value === 'string') return this.formatString(value, opts);
        if (typeof value === 'object') return JSON.stringify(value);
        return String(value);
    }
}

export function createDisplayContext(session: ReplSession, values: Iterable<string>): HashDisplayContext {
    return new HashDisplayContext(session, values);
}

export function formatSessionRows(
    session: ReplSession,
    rows: Record<string, unknown>[],
    columns?: string[],
    options?: { structuralColumns?: Set<string>; vertical?: boolean },
): string {
    const structuralColumns = options?.structuralColumns ?? STRUCTURAL_HASH_COLUMNS;
    const ctx = createDisplayContext(session, collectTruncatableStrings(rows, structuralColumns));
    const formatOptions = { ctx, structuralColumns };
    return options?.vertical === true || session.outputMode === 'vertical'
        ? formatRowsVertical(rows, columns, formatOptions)
        : formatRows(rows, columns, formatOptions);
}

export function formatDisplayString(
    session: ReplSession,
    value: string,
    opts: FormatStringOpts = {},
): string {
    const values = isTruncatable(value, opts.role ?? 'cell') ? [value] : [];
    return createDisplayContext(session, values).formatString(value, opts);
}
