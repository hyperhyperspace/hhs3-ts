import { renderLogOpLine, type LogLangResult } from "@hyper-hyper-space/hhs3_rdb_lang";
import type { OutputMode, ReplSession } from "../session.js";
import { collectTruncatableFromResult, createDisplayContext } from "./display.js";
import { formatRows, formatRowsVertical } from "./rows.js";

const LOG_OP_MAX_LEN = 33;

export function formatLog(
    result: LogLangResult,
    session: ReplSession,
    mode: Exclude<OutputMode, 'json'> = 'table',
): string {
    const ctx = createDisplayContext(session, collectTruncatableFromResult(result));
    const records = result.rows.map((row) => {
        const record: Record<string, unknown> = {
            hash: ctx.formatString(row.fullHash, { role: 'hash', hashPrefix: true }),
            prev: row.prev.length === 0
                ? '-'
                : row.prev.map((hash) => ctx.formatString(hash, { role: 'hash', hashPrefix: true })).join(','),
            op: truncateOp(renderLogOpLine(row.payload, row.prev, result.renderContext)),
        };
        if (row.void !== undefined) record.status = row.void ? 'Cancelled' : 'OK';
        if (result.explain) record.reason = row.reason ?? '';
        return record;
    });
    const columns = ['hash', 'prev', 'op'];
    if (records.some((record) => 'status' in record)) columns.push('status');
    if (result.explain) columns.push('reason');
    return mode === 'vertical'
        ? formatRowsVertical(records, columns, { ctx })
        : formatRows(records, columns, { ctx });
}

function truncateOp(line: string): string {
    return line.length <= LOG_OP_MAX_LEN ? line : `${line.slice(0, LOG_OP_MAX_LEN - 3)}...`;
}
