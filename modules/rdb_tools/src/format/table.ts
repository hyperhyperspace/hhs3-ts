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
        case 'add-member':
            return `added ${result.member} ${result.memberId} to ${result.database} (${result.entryHash})`;
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
            return mode === 'vertical'
                ? formatRowsVertical(records, displayColumns)
                : formatRows(records, displayColumns);
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
        case 'update-schema':
            return `updated schema on ${result.group} (${result.entryHash})`;
        case 'update-ref':
            return `updated ref ${result.ref} on ${result.group} (${result.entryHash})`;
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
