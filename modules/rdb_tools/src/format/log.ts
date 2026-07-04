import { renderLogOpLine, type LogLangResult } from "@hyper-hyper-space/hhs3_rdb_lang";

import type { OutputMode, WorkspaceSession } from "../session/session.js";
import { collectTruncatableFromResult, createDisplayContext } from "./display.js";
import { formatRows, formatRowsVertical } from "./rows.js";

const LOG_OP_MAX_LEN = 33;

export function formatLog(
    result: LogLangResult,
    session: WorkspaceSession,
    mode: Exclude<OutputMode, 'json'> = 'table',
): string {
    const ctx = createDisplayContext(session, collectTruncatableFromResult(result));
    const records = result.rows.map((row) => logRowToRecord(row, result, ctx));
    const columns = logColumns(result, records);
    return mode === 'vertical'
        ? formatRowsVertical(records, columns, { ctx })
        : formatRows(records, columns, { ctx });
}

function logColumns(result: LogLangResult, records: Record<string, unknown>[]): string[] {
    const columns = ['hash', 'prev', 'op'];
    if (records.some((record) => 'status' in record)) columns.push('status');
    if (result.explain) columns.push('reason');
    return columns;
}

function truncateOp(line: string): string {
    return line.length <= LOG_OP_MAX_LEN ? line : `${line.slice(0, LOG_OP_MAX_LEN - 3)}...`;
}

function logRowToRecord(
    row: LogLangResult['rows'][number],
    result: LogLangResult,
    ctx: ReturnType<typeof createDisplayContext>,
): Record<string, unknown> {
    const record: Record<string, unknown> = {
        hash: ctx.formatString(row.fullHash, { role: 'hash', hashPrefix: true }),
        prev: row.prev.length === 0
            ? '-'
            : row.prev.map((h) => ctx.formatString(h, { role: 'hash', hashPrefix: true })).join(','),
        op: truncateOp(renderLogOpLine(row.payload, row.prev, result.renderContext)),
    };
    if (row.void !== undefined) {
        record['status'] = row.void ? 'Cancelled' : 'OK';
    }
    if (result.explain) {
        record['reason'] = row.reason ?? '';
    }
    return record;
}
