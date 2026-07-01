import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { LangExecutionResult } from "@hyper-hyper-space/hhs3_rdb_lang";

import type { HashWidth, WorkspaceSession } from "../session/session.js";
import { formatRows, formatRowsVertical } from "./rows.js";

export const STRUCTURAL_HASH_COLUMNS = new Set(['rowId', 'rowAuthor']);

export type FormatStringRole = 'hash' | 'cell';

export type FormatStringOpts = {
    role?: FormatStringRole;
    hashPrefix?: boolean;
};

export function looksLikeSpeculativeHash(s: string): boolean {
    return s.length >= 30
        && /^[A-Za-z0-9+/]+=*$/.test(s)
        && s.endsWith('=');
}

export function isTruncatable(value: string, role: FormatStringRole): boolean {
    return role === 'hash' || looksLikeSpeculativeHash(value);
}

export function uniquePrefixes(hashes: B64Hash[], minLen = 8): Map<B64Hash, string> {
    const out = new Map<B64Hash, string>();
    for (const hash of hashes) {
        for (let len = minLen; len <= hash.length; len += 1) {
            const prefix = hash.slice(0, len);
            if (hashes.filter((h) => h.startsWith(prefix)).length === 1) {
                out.set(hash, prefix);
                break;
            }
        }
        if (!out.has(hash)) out.set(hash, hash);
    }
    return out;
}

export function buildKeyLabelMap(session: WorkspaceSession): Map<string, string> {
    const out = new Map<string, string>();
    if (session.keystore !== undefined) {
        for (const key of session.keystore.list()) {
            out.set(key.keyId, `$${key.label}`);
        }
    }
    for (const entry of session.aliases.list('key')) {
        if (!out.has(entry.hash)) out.set(entry.hash, `$${entry.name}`);
    }
    return out;
}

export function collectTruncatableLiterals(value: unknown): string[] {
    if (typeof value === 'string') {
        return isTruncatable(value, 'cell') ? [value] : [];
    }
    if (Array.isArray(value)) {
        return value.flatMap((item) => collectTruncatableLiterals(item));
    }
    if (value !== null && typeof value === 'object') {
        return Object.values(value).flatMap((item) => collectTruncatableLiterals(item));
    }
    return [];
}

export function collectTruncatableFromColumnChanges(
    changes: Array<{ before?: unknown; after?: unknown }>,
): string[] {
    const out: string[] = [];
    for (const change of changes) {
        if (change.before !== undefined) out.push(...collectTruncatableLiterals(change.before));
        if (change.after !== undefined) out.push(...collectTruncatableLiterals(change.after));
    }
    return out;
}

export function collectTruncatableStrings(
    rows: Record<string, unknown>[],
    structuralColumns: Set<string> = STRUCTURAL_HASH_COLUMNS,
): string[] {
    const out = new Set<string>();
    for (const row of rows) {
        for (const [column, value] of Object.entries(row)) {
            if (typeof value !== 'string') continue;
            if (structuralColumns.has(column) || looksLikeSpeculativeHash(value)) {
                out.add(value);
            }
        }
    }
    return [...out];
}

export function collectTruncatableFromResult(result: LangExecutionResult): string[] {
    switch (result.kind) {
        case 'select':
            return collectTruncatableStrings(result.rows.map((row) => {
                const record: Record<string, unknown> = { rowId: row.rowId };
                if (row.author !== undefined) record.rowAuthor = row.author;
                Object.assign(record, row.values);
                return record;
            }));
        case 'log': {
            const hashes: string[] = [];
            for (const row of result.rows) {
                hashes.push(row.fullHash);
                for (const prev of row.prev) hashes.push(prev);
            }
            return hashes;
        }
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

function truncateHash(value: string, width: HashWidth, prefixMap: Map<B64Hash, string>): string {
    if (width === 'full') return value;
    if (typeof width === 'number') return value.slice(0, width);
    return prefixMap.get(value) ?? value;
}

export class HashDisplayContext {
    private readonly keyMap: Map<string, string>;
    private readonly prefixMap: Map<B64Hash, string>;

    constructor(
        private readonly session: WorkspaceSession,
        truncatableValues: Iterable<string>,
    ) {
        this.keyMap = buildKeyLabelMap(session);
        const hashes = [...new Set([...truncatableValues].filter((v) => typeof v === 'string' && v.length > 0))];
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

export function createDisplayContext(
    session: WorkspaceSession,
    truncatableValues: Iterable<string>,
): HashDisplayContext {
    return new HashDisplayContext(session, truncatableValues);
}

export function formatSessionRows(
    session: WorkspaceSession,
    rows: Record<string, unknown>[],
    columns?: string[],
    options?: { structuralColumns?: Set<string>; vertical?: boolean },
): string {
    const structuralColumns = options?.structuralColumns ?? STRUCTURAL_HASH_COLUMNS;
    const truncatable = collectTruncatableStrings(rows, structuralColumns);
    const ctx = createDisplayContext(session, truncatable);
    const formatOptions = { ctx, structuralColumns };
    if (options?.vertical === true || session.outputMode === 'vertical') {
        return formatRowsVertical(rows, columns, formatOptions);
    }
    return formatRows(rows, columns, formatOptions);
}

export function formatDisplayString(
    session: WorkspaceSession,
    value: string,
    opts: FormatStringOpts = {},
): string {
    const truncatable = isTruncatable(value, opts.role ?? 'cell') ? [value] : [];
    return createDisplayContext(session, truncatable).formatString(value, opts);
}
