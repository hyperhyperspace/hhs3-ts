import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import { formatOpVoidDetail, type OpVerdictChange, type RTableChanges } from "@hyper-hyper-space/hhs3_rdb";
import type { DeltaPayload } from "../delta/payload.js";
import { schemaChangeRows, versionHashes } from "../delta/payload.js";
import type { ReplSession } from "../session.js";
import { collectTruncatableFromColumnChanges, createDisplayContext, type HashDisplayContext } from "./display.js";
import { formatJson } from "./json.js";
import { formatRows, formatRowsVertical } from "./rows.js";

export function formatDelta(session: ReplSession, payload: DeltaPayload): string {
    if (session.outputMode === 'json') return formatJson(jsonPayload(payload));
    const delta = payload.delta;
    const ctx = createDisplayContext(session, [
        ...versionHashes(delta.start), ...versionHashes(delta.end), ...versionHashes(delta.revisionBound),
    ]);
    const lines = [
        `DELTA ${payload.kind} ${payload.name}`,
        `  start: #${formatVersion(delta.start, ctx)}`,
        `  end:   #${formatVersion(delta.end, ctx)}`,
        `  bound: #${formatVersion(delta.revisionBound, ctx)}`,
    ];
    const tableChanges = payload.kind === 'schema' ? payload.delta.tableChanges : payload.delta.schemaChanges.tableChanges;
    const schemaRows = schemaChangeRows(tableChanges);
    if (schemaRows.length > 0) lines.push('', 'schema changes:', renderRows(session, schemaRows, ['table', 'change', 'detail']));
    let hasChanges = schemaRows.length > 0;
    if (payload.kind === 'group') {
        const rowChanges = formatGroupRows(session, payload);
        if (rowChanges !== undefined) {
            hasChanges = true;
            lines.push('', 'row changes:', rowChanges);
        }
        const opChanges = formatGroupOps(session, payload);
        if (opChanges !== undefined) {
            hasChanges = true;
            lines.push('', 'op changes:', opChanges);
        }
    }
    if (!hasChanges) lines.push('', '(no changes)');
    return lines.join('\n');
}

function jsonPayload(payload: DeltaPayload): unknown {
    const base = {
        kind: payload.kind,
        name: payload.name,
        objectId: payload.objectId,
        start: versionHashes(payload.delta.start),
        end: versionHashes(payload.delta.end),
        revisionBound: versionHashes(payload.delta.revisionBound),
        schemaChanges: {
            tableChanges: payload.kind === 'schema'
                ? payload.delta.tableChanges : payload.delta.schemaChanges.tableChanges,
        },
    };
    if (payload.kind === 'schema') return base;
    const tables = [...payload.delta.nested].flatMap(([tableId, child]) => {
        const rowChanges = (child.changes as RTableChanges).rowChanges;
        return rowChanges.length === 0 ? [] : [{
            table: payload.tableIdToName.get(tableId) ?? tableId, tableId, rowChanges,
        }];
    }).sort((a, b) => a.table.localeCompare(b.table));
    return { ...base, tables, opVerdictChanges: payload.delta.opVerdictChanges };
}

function formatGroupRows(session: ReplSession, payload: Extract<DeltaPayload, { kind: 'group' }>): string | undefined {
    const raw = [...payload.delta.nested].flatMap(([tableId, child]) =>
        (child.changes as RTableChanges).rowChanges.map((change) => ({
            table: payload.tableIdToName.get(tableId) ?? tableId,
            tableId: payload.tableIdToName.has(tableId) ? undefined : tableId,
            rowId: change.rowId,
            live: `${change.liveBefore} -> ${change.liveAfter}`,
            author: change.author ?? '',
            columnChanges: change.columnChanges,
        })));
    if (raw.length === 0) return undefined;
    const ctx = createDisplayContext(session, raw.flatMap((row) => [
        row.rowId, row.author, ...(row.tableId === undefined ? [] : [row.tableId]),
        ...collectTruncatableFromColumnChanges(row.columnChanges),
    ]));
    const rows = raw.map((row) => ({
        table: row.tableId === undefined ? row.table : ctx.formatString(row.tableId, { role: 'hash' }),
        rowId: ctx.formatString(row.rowId, { role: 'hash', hashPrefix: true }),
        live: row.live,
        author: row.author === '' ? '' : ctx.formatString(row.author, { role: 'hash' }),
        columns: row.columnChanges.map((change) => {
            const before = change.before === undefined ? '' : ctx.formatValue(change.before);
            const after = change.after === undefined ? '' : ctx.formatValue(change.after);
            return `${change.column}: ${before} -> ${after}`;
        }).join(', '),
    }));
    return renderRows(session, rows, ['table', 'rowId', 'live', 'author', 'columns'], ctx);
}

function formatGroupOps(session: ReplSession, payload: Extract<DeltaPayload, { kind: 'group' }>): string | undefined {
    const changes = payload.delta.opVerdictChanges;
    if (changes.length === 0) return undefined;
    const ctx = createDisplayContext(session, changes.flatMap((change) => [
        change.entry, ...(change.rowId === undefined ? [] : [change.rowId]),
        ...(change.author === undefined ? [] : [change.author]), ...(change.refVersion ?? []),
    ]));
    return renderRows(session, changes.map((change) => formatOp(change, ctx)),
        ['entry', 'kind', 'void', 'table', 'rowId', 'author', 'binding', 'reason'], ctx);
}

function formatOp(change: OpVerdictChange, ctx: HashDisplayContext): Record<string, string> {
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

function formatVersion(version: Version, ctx: HashDisplayContext): string {
    return [...version].sort().map((hash) => ctx.formatString(hash, { role: 'hash' })).join(',');
}

function renderRows(
    session: ReplSession,
    rows: Record<string, unknown>[],
    columns: string[],
    ctx?: HashDisplayContext,
): string {
    const options = ctx === undefined ? undefined : { ctx, structuralColumns: new Set(['rowId', 'author']) };
    return session.outputMode === 'vertical'
        ? formatRowsVertical(rows, columns, options)
        : formatRows(rows, columns, options);
}
