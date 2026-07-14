import type { Row } from "@hyper-hyper-space/hhs3_rdb";
import type { LangExecutionResult } from "@hyper-hyper-space/hhs3_rdb_lang";
import type { StatementRunResult } from "../adapter.js";
import type { ReplSession } from "../session.js";
import { collectTruncatableFromResult, collectTruncatableStrings, createDisplayContext } from "./display.js";
import { formatJson } from "./json.js";
import { formatLog } from "./log.js";
import { formatRows, formatRowsVertical } from "./rows.js";

export { formatRows, formatRowsVertical } from "./rows.js";

export function renderStatementMain(session: ReplSession, item: StatementRunResult): string {
    return session.outputMode === 'json' ? formatJson(item.result) : formatTableResult(item.result, session);
}

export function renderStatementNotices(session: ReplSession, item: StatementRunResult): string {
    return session.outputMode === 'json' ? '' : (item.notices ?? []).filter(Boolean).join('\n');
}

export function renderStatementOutput(session: ReplSession, item: StatementRunResult): string {
    return [
        item.mainStreamed ? '' : renderStatementMain(session, item),
        item.noticesStreamed ? '' : renderStatementNotices(session, item),
    ].filter(Boolean).join('\n');
}

export function formatTableResult(result: LangExecutionResult, session: ReplSession): string {
    const mode = session.outputMode === 'vertical' ? 'vertical' : 'table';
    const ctx = createDisplayContext(session, collectTruncatableFromResult(result));
    switch (result.kind) {
        case 'create-plan': return `create ${result.plan.kind} ${result.plan.name}`;
        case 'add-member':
            return `added ${result.member} ${ctx.formatString(result.memberId, { role: 'hash' })} to ${ctx.formatString(result.database, { role: 'hash' })} (${ctx.formatString(result.entryHash, { role: 'hash' })})`;
        case 'select': {
            const records = result.rows.map((row) => selectRowToRecord(row, result.columns));
            const columns = result.columns === undefined ? undefined : [
                'rowId',
                ...(records.some((row) => row.rowAuthor !== undefined) ? ['rowAuthor'] : []),
                ...result.columns,
            ];
            const selectCtx = createDisplayContext(session, collectTruncatableStrings(records));
            return mode === 'vertical'
                ? formatRowsVertical(records, columns, { ctx: selectCtx })
                : formatRows(records, columns, { ctx: selectCtx });
        }
        case 'log': return formatLog(result, session, mode);
        case 'set-view': return 'view set';
        case 'insert': return `inserted ${ctx.formatString(result.rowId, { role: 'hash' })} (${ctx.formatString(result.entryHash, { role: 'hash' })})`;
        case 'update': return `updated ${ctx.formatString(result.rowId, { role: 'hash' })} (${ctx.formatString(result.entryHash, { role: 'hash' })})`;
        case 'delete': return `deleted ${ctx.formatString(result.rowId, { role: 'hash' })} (${ctx.formatString(result.entryHash, { role: 'hash' })})`;
        case 'bundle': return `bundle ${ctx.formatString(result.entryHash, { role: 'hash' })} (${result.writes} writes)`;
        case 'alter-schema': return `altered schema ${ctx.formatString(result.schema, { role: 'hash' })} (${result.rules} rules, ${ctx.formatString(result.entryHash, { role: 'hash' })})`;
        case 'update-schema': return `updated schema on ${ctx.formatString(result.group, { role: 'hash' })} (${ctx.formatString(result.entryHash, { role: 'hash' })})`;
        case 'update-ref': return `updated ref ${ctx.formatString(result.ref, { role: 'hash' })} on ${ctx.formatString(result.group, { role: 'hash' })} (${ctx.formatString(result.entryHash, { role: 'hash' })})`;
    }
}

function selectRowToRecord(row: Row, columns?: string[]): Record<string, unknown> {
    const record: Record<string, unknown> = {
        rowId: row.rowId,
        ...(row.author === undefined ? {} : { rowAuthor: row.author }),
    };
    if (columns === undefined) Object.assign(record, row.values);
    else for (const column of columns) if (row.values[column] !== undefined) record[column] = row.values[column];
    return record;
}
