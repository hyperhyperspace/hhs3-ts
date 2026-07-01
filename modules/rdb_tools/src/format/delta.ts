import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { formatOpVoidDetail, type OpVerdictChange, type RTableChanges } from "@hyper-hyper-space/hhs3_rdb";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";

import {
    type DeltaPayload,
    schemaChangeRows,
    versionHashes,
} from "../delta/payload.js";
import {
    collectTruncatableFromColumnChanges,
    createDisplayContext,
    type HashDisplayContext,
} from "./display.js";
import { formatJson } from "./json.js";
import { formatRows, formatRowsVertical } from "./rows.js";
import type { WorkspaceSession } from "../session/session.js";

export function formatDelta(session: WorkspaceSession, payload: DeltaPayload): string {
    if (session.outputMode === 'json') {
        return formatJson(buildJsonPayload(payload));
    }
    return formatDeltaText(session, payload);
}

function buildJsonPayload(payload: DeltaPayload): unknown {
    const { delta } = payload;
    const schemaChanges = payload.kind === 'schema'
        ? { tableChanges: payload.delta.tableChanges }
        : { tableChanges: payload.delta.schemaChanges.tableChanges };

    const base = {
        kind: payload.kind,
        name: payload.name,
        objectId: payload.objectId,
        start: versionHashes(delta.start),
        end: versionHashes(delta.end),
        revisionBound: versionHashes(delta.revisionBound),
        schemaChanges,
    };

    if (payload.kind === 'group') {
        return {
            ...base,
            tables: buildGroupTablePayload(payload),
            opVerdictChanges: payload.delta.opVerdictChanges,
        };
    }
    return base;
}

function buildGroupTablePayload(payload: Extract<DeltaPayload, { kind: 'group' }>) {
    const tables: Array<{ table: string; tableId: B64Hash; rowChanges: RTableChanges['rowChanges'] }> = [];
    for (const [tableId, child] of payload.delta.nested) {
        const changes = child.changes as RTableChanges;
        if (changes.rowChanges.length === 0) continue;
        tables.push({
            table: payload.tableIdToName.get(tableId) ?? tableId,
            tableId,
            rowChanges: changes.rowChanges,
        });
    }
    tables.sort((a, b) => a.table.localeCompare(b.table));
    return tables;
}

function formatDeltaText(session: WorkspaceSession, payload: DeltaPayload): string {
    const { delta } = payload;
    const versionMembers = [
        ...versionHashes(delta.start),
        ...versionHashes(delta.end),
        ...versionHashes(delta.revisionBound),
    ];
    const ctx = createDisplayContext(session, versionMembers);

    const lines: string[] = [
        `DELTA ${payload.kind} ${payload.name}`,
        `  start: #${formatVersion(delta.start, ctx)}`,
        `  end:   #${formatVersion(delta.end, ctx)}`,
        `  bound: #${formatVersion(delta.revisionBound, ctx)}`,
    ];

    const tableChanges = payload.kind === 'schema'
        ? payload.delta.tableChanges
        : payload.delta.schemaChanges.tableChanges;
    const schemaRows = schemaChangeRows(tableChanges);

    if (schemaRows.length > 0) {
        lines.push('', 'schema changes:');
        lines.push(renderRows(session, schemaRows, ['table', 'change', 'detail']));
    }

    if (payload.kind === 'group') {
        const rowLines = formatGroupRowChanges(session, payload);
        if (rowLines !== undefined) {
            lines.push('', 'row changes:');
            lines.push(rowLines);
        }
        const opLines = formatGroupOpChanges(session, payload);
        if (opLines !== undefined) {
            lines.push('', 'op changes:');
            lines.push(opLines);
        }
    }

    if (schemaRows.length === 0 && (payload.kind !== 'group' || !hasGroupChanges(payload))) {
        lines.push('', '(no changes)');
    }

    return lines.join('\n');
}

function hasGroupChanges(payload: Extract<DeltaPayload, { kind: 'group' }>): boolean {
    if (hasRowChanges(payload)) return true;
    return payload.delta.opVerdictChanges.length > 0;
}

function hasRowChanges(payload: Extract<DeltaPayload, { kind: 'group' }>): boolean {
    for (const child of payload.delta.nested.values()) {
        if ((child.changes as RTableChanges).rowChanges.length > 0) return true;
    }
    return false;
}

function formatGroupRowChanges(
    session: WorkspaceSession,
    payload: Extract<DeltaPayload, { kind: 'group' }>,
): string | undefined {
    const rawRows: Array<{
        table: string;
        rowId: B64Hash;
        live: string;
        author: string;
        columnChanges: RTableChanges['rowChanges'][number]['columnChanges'];
        tableId?: B64Hash;
    }> = [];
    for (const [tableId, child] of payload.delta.nested) {
        const changes = child.changes as RTableChanges;
        for (const rowChange of changes.rowChanges) {
            rawRows.push({
                table: payload.tableIdToName.get(tableId) ?? tableId,
                tableId: payload.tableIdToName.get(tableId) === undefined ? tableId : undefined,
                rowId: rowChange.rowId,
                live: `${rowChange.liveBefore} -> ${rowChange.liveAfter}`,
                author: rowChange.author ?? '',
                columnChanges: rowChange.columnChanges,
            });
        }
    }
    if (rawRows.length === 0) return undefined;

    const truncatable = rawRows.flatMap((row) => [
        row.rowId,
        ...(row.author.length > 0 ? [row.author] : []),
        ...(row.tableId !== undefined ? [row.tableId] : []),
        ...collectTruncatableFromColumnChanges(row.columnChanges),
    ]);
    const ctx = createDisplayContext(session, truncatable);
    const rows = rawRows.map((row) => ({
        table: row.tableId === undefined ? row.table : ctx.formatString(row.tableId, { role: 'hash' }),
        rowId: ctx.formatString(row.rowId, { role: 'hash', hashPrefix: true }),
        live: row.live,
        author: row.author.length === 0 ? '' : ctx.formatString(row.author, { role: 'hash' }),
        columns: formatColumnChanges(row.columnChanges, ctx),
    }));
    return renderRows(session, rows, ['table', 'rowId', 'live', 'author', 'columns'], ctx);
}

function formatGroupOpChanges(
    session: WorkspaceSession,
    payload: Extract<DeltaPayload, { kind: 'group' }>,
): string | undefined {
    const changes = payload.delta.opVerdictChanges;
    if (changes.length === 0) return undefined;

    const truncatable = changes.flatMap((change) => [
        change.entry,
        ...(change.rowId !== undefined ? [change.rowId] : []),
        ...(change.author !== undefined ? [change.author] : []),
        ...(change.refVersion ?? []),
    ]);
    const ctx = createDisplayContext(session, truncatable);
    const rows = changes.map((change) => formatOpVerdictRow(change, ctx));
    return renderRows(session, rows, ['entry', 'kind', 'void', 'table', 'rowId', 'author', 'binding', 'reason'], ctx);
}

function formatOpVerdictRow(change: OpVerdictChange, ctx: HashDisplayContext): Record<string, string> {
    return {
        entry: ctx.formatString(change.entry, { role: 'hash', hashPrefix: true }),
        kind: change.kind,
        void: `${change.voidBefore} -> ${change.voidAfter}`,
        table: change.table ?? '',
        rowId: change.rowId === undefined ? '' : ctx.formatString(change.rowId, { role: 'hash', hashPrefix: true }),
        author: change.author === undefined ? '' : ctx.formatString(change.author, { role: 'hash' }),
        binding: change.binding ?? '',
        reason: change.reason === undefined ? '' : formatOpVoidDetail(change.reason),
    };
}

function formatVersion(v: Version, ctx: HashDisplayContext): string {
    return [...v]
        .sort()
        .map((h) => ctx.formatString(h, { role: 'hash' }))
        .join(',');
}

function formatColumnChanges(
    changes: RTableChanges['rowChanges'][number]['columnChanges'],
    ctx: HashDisplayContext,
): string {
    if (changes.length === 0) return '';
    return changes.map((change) => {
        const before = change.before === undefined ? '' : ctx.formatValue(change.before, { role: 'cell' });
        const after = change.after === undefined ? '' : ctx.formatValue(change.after, { role: 'cell' });
        return `${change.column}: ${before} -> ${after}`;
    }).join(', ');
}

function renderRows(
    session: WorkspaceSession,
    rows: Record<string, unknown>[],
    columns: string[],
    ctx?: HashDisplayContext,
): string {
    const structuralColumns = new Set(['rowId', 'author']);
    const options = ctx === undefined ? undefined : { ctx, structuralColumns };
    if (session.outputMode === 'vertical') {
        return formatRowsVertical(rows, columns, options);
    }
    return formatRows(rows, columns, options);
}
