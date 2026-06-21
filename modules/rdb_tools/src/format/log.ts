import type { LogLangResult, LogRow } from "@hyper-hyper-space/hhs3_rdb_lang";

import type { OutputMode } from "../session/session.js";
import { formatRows, formatRowsVertical } from "./rows.js";

export function formatLog(result: LogLangResult, mode: Exclude<OutputMode, 'json'> = 'table'): string {
    const records = result.rows.map(logRowToRecord);
    return mode === 'vertical' ? formatRowsVertical(records) : formatRows(records);
}

export function logRowToRecord(row: LogRow): Record<string, unknown> {
    return {
        hash: `#${row.hash}`,
        prev: row.prev.length === 0 ? '-' : row.prev.map((h) => `#${h}`).join(','),
        action: row.action ?? row.type ?? 'entry',
        summary: row.summary,
    };
}
