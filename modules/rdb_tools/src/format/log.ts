import type { LogLangResult } from "@hyper-hyper-space/hhs3_rdb_lang";

import type { OutputMode, WorkspaceSession } from "../session/session.js";
import { collectTruncatableFromResult, createDisplayContext } from "./display.js";
import { formatRows, formatRowsVertical } from "./rows.js";

export function formatLog(
    result: LogLangResult,
    session: WorkspaceSession,
    mode: Exclude<OutputMode, 'json'> = 'table',
): string {
    const ctx = createDisplayContext(session, collectTruncatableFromResult(result));
    const records = result.rows.map((row) => logRowToRecord(row, ctx));
    return mode === 'vertical'
        ? formatRowsVertical(records, undefined, { ctx })
        : formatRows(records, undefined, { ctx });
}

function logRowToRecord(
    row: LogLangResult['rows'][number],
    ctx: ReturnType<typeof createDisplayContext>,
): Record<string, unknown> {
    return {
        hash: ctx.formatString(row.fullHash, { role: 'hash', hashPrefix: true }),
        prev: row.prev.length === 0
            ? '-'
            : row.prev.map((h) => ctx.formatString(h, { role: 'hash', hashPrefix: true })).join(','),
        action: row.action ?? row.type ?? 'entry',
        summary: row.summary,
    };
}
