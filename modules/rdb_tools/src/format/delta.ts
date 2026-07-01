import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { RTableChanges } from "@hyper-hyper-space/hhs3_rdb";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";

import {
    type DeltaPayload,
    schemaChangeRows,
    versionHashes,
} from "../delta/payload.js";
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
        return { ...base, tables: buildGroupTablePayload(payload) };
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
    const lines: string[] = [
        `DELTA ${payload.kind} ${payload.name}`,
        `  start: #${formatVersion(delta.start)}`,
        `  end:   #${formatVersion(delta.end)}`,
        `  bound: #${formatVersion(delta.revisionBound)}`,
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
    }

    if (schemaRows.length === 0 && (payload.kind !== 'group' || !hasRowChanges(payload))) {
        lines.push('', '(no changes)');
    }

    return lines.join('\n');
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
    const rows: Record<string, unknown>[] = [];
    for (const [tableId, child] of payload.delta.nested) {
        const changes = child.changes as RTableChanges;
        for (const rowChange of changes.rowChanges) {
            rows.push({
                table: payload.tableIdToName.get(tableId) ?? tableId,
                rowId: `#${rowChange.rowId}`,
                live: `${rowChange.liveBefore} -> ${rowChange.liveAfter}`,
                author: rowChange.author ?? '',
                columns: formatColumnChanges(rowChange.columnChanges),
            });
        }
    }
    if (rows.length === 0) return undefined;
    return renderRows(session, rows, ['table', 'rowId', 'live', 'author', 'columns']);
}

function formatVersion(v: Version): string {
    return [...v].sort().join(',');
}

function formatColumnChanges(changes: RTableChanges['rowChanges'][number]['columnChanges']): string {
    if (changes.length === 0) return '';
    return changes.map((change) => {
        const before = change.before === undefined ? '' : JSON.stringify(change.before);
        const after = change.after === undefined ? '' : JSON.stringify(change.after);
        return `${change.column}: ${before} -> ${after}`;
    }).join(', ');
}

function renderRows(
    session: WorkspaceSession,
    rows: Record<string, unknown>[],
    columns: string[],
): string {
    if (session.outputMode === 'vertical') {
        return formatRowsVertical(rows, columns);
    }
    return formatRows(rows, columns);
}
