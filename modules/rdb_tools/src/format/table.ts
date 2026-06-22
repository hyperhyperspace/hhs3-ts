import type { Row } from "@hyper-hyper-space/hhs3_rdb";
import type { LangExecutionResult } from "@hyper-hyper-space/hhs3_rdb_lang";

import type { OutputMode } from "../session/session.js";
import { formatLog } from "./log.js";
import { formatRows, formatRowsVertical } from "./rows.js";

export { formatRows, formatRowsVertical } from "./rows.js";

export function formatTableResult(result: LangExecutionResult, mode: Exclude<OutputMode, 'json'> = 'table'): string {
    switch (result.kind) {
        case 'create-plan':
            return `create ${result.plan.kind} ${result.plan.name}`;
        case 'select': {
            const records = result.rows.map(selectRowToRecord);
            return mode === 'vertical' ? formatRowsVertical(records) : formatRows(records);
        }
        case 'log':
            return formatLog(result, mode);
        case 'set-view':
            return 'view set';
        case 'insert':
            return `inserted ${result.rowId} (${result.entryHash})`;
        case 'update':
            return `updated ${result.rowId} (${result.entryHash})`;
        case 'delete':
            return `deleted ${result.rowId} (${result.entryHash})`;
        case 'bundle':
            return `bundle ${result.entryHash} (${result.writes} writes)`;
        case 'alter-schema':
            return `altered schema ${result.schema} (${result.rules} rules, ${result.entryHash})`;
        case 'deploy-schema':
            return `deployed schema on ${result.group} (${result.entryHash})`;
        case 'update-ref':
            return `updated ref ${result.ref} on ${result.group} (${result.entryHash})`;
    }
}

function selectRowToRecord(row: Row): Record<string, unknown> {
    return {
        rowId: row.rowId,
        ...(row.author !== undefined ? { rowAuthor: row.author } : {}),
        ...row.values,
    };
}
