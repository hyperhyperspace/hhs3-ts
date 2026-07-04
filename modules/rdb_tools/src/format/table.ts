import type { Row } from "@hyper-hyper-space/hhs3_rdb";
import type { LangExecutionResult } from "@hyper-hyper-space/hhs3_rdb_lang";

import type { WorkspaceSession } from "../session/session.js";
import type { StatementRunResult } from "../session/adapter.js";
import {
    collectTruncatableFromResult,
    collectTruncatableStrings,
    createDisplayContext,
} from "./display.js";
import { formatJson } from "./json.js";
import { formatLog } from "./log.js";
import { formatRows, formatRowsVertical } from "./rows.js";

export { formatRows, formatRowsVertical } from "./rows.js";

export function renderStatementMain(session: WorkspaceSession, item: StatementRunResult): string {
    if (session.outputMode === 'json') return formatJson(item.result);
    return formatTableResult(item.result, session);
}

export function renderStatementNotices(session: WorkspaceSession, item: StatementRunResult): string {
    if (session.outputMode === 'json') return '';
    return (item.notices ?? []).filter((line) => line.length > 0).join('\n');
}

export function renderStatementOutput(session: WorkspaceSession, item: StatementRunResult): string {
    const main = renderStatementMain(session, item);
    const notices = renderStatementNotices(session, item);
    return [main, notices].filter((line) => line.length > 0).join('\n');
}

export function formatTableResult(
    result: LangExecutionResult,
    session: WorkspaceSession,
): string {
    const mode = session.outputMode === 'vertical' ? 'vertical' : 'table';
    const ctx = createDisplayContext(session, collectTruncatableFromResult(result));

    switch (result.kind) {
        case 'create-plan':
            return `create ${result.plan.kind} ${result.plan.name}`;
        case 'add-member':
            return `added ${result.member} ${ctx.formatString(result.memberId, { role: 'hash' })} to ${ctx.formatString(result.database, { role: 'hash' })} (${ctx.formatString(result.entryHash, { role: 'hash' })})`;
        case 'select': {
            const schemaColumns = result.columns;
            const records = result.rows.map((row) => selectRowToRecord(row, schemaColumns));
            const displayColumns = schemaColumns === undefined
                ? undefined
                : [
                    'rowId',
                    ...(records.some((row) => row.rowAuthor !== undefined) ? ['rowAuthor'] : []),
                    ...schemaColumns,
                ];
            const truncatable = collectTruncatableStrings(records);
            const selectCtx = createDisplayContext(session, truncatable);
            const options = { ctx: selectCtx };
            return mode === 'vertical'
                ? formatRowsVertical(records, displayColumns, options)
                : formatRows(records, displayColumns, options);
        }
        case 'log':
            return formatLog(result, session, mode);
        case 'set-view':
            return 'view set';
        case 'insert':
            return `inserted ${ctx.formatString(result.rowId, { role: 'hash' })} (${ctx.formatString(result.entryHash, { role: 'hash' })})`;
        case 'update':
            return `updated ${ctx.formatString(result.rowId, { role: 'hash' })} (${ctx.formatString(result.entryHash, { role: 'hash' })})`;
        case 'delete':
            return `deleted ${ctx.formatString(result.rowId, { role: 'hash' })} (${ctx.formatString(result.entryHash, { role: 'hash' })})`;
        case 'bundle':
            return `bundle ${ctx.formatString(result.entryHash, { role: 'hash' })} (${result.writes} writes)`;
        case 'alter-schema':
            return `altered schema ${ctx.formatString(result.schema, { role: 'hash' })} (${result.rules} rules, ${ctx.formatString(result.entryHash, { role: 'hash' })})`;
        case 'update-schema':
            return `updated schema on ${ctx.formatString(result.group, { role: 'hash' })} (${ctx.formatString(result.entryHash, { role: 'hash' })})`;
        case 'update-ref':
            return `updated ref ${ctx.formatString(result.ref, { role: 'hash' })} on ${ctx.formatString(result.group, { role: 'hash' })} (${ctx.formatString(result.entryHash, { role: 'hash' })})`;
    }
}

function selectRowToRecord(row: Row, schemaColumns?: string[]): Record<string, unknown> {
    const record: Record<string, unknown> = { rowId: row.rowId };
    if (row.author !== undefined) record.rowAuthor = row.author;
    if (schemaColumns === undefined) {
        Object.assign(record, row.values);
        return record;
    }
    for (const column of schemaColumns) {
        if (row.values[column] !== undefined) record[column] = row.values[column];
    }
    return record;
}
