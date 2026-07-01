import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { RSchemaDelta, RTableGroupDelta, TableChange } from "@hyper-hyper-space/hhs3_rdb";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";

export type SchemaDeltaPayload = {
    kind: 'schema';
    name: string;
    objectId: B64Hash;
    delta: RSchemaDelta;
};

export type GroupDeltaPayload = {
    kind: 'group';
    name: string;
    objectId: B64Hash;
    delta: RTableGroupDelta;
    tableIdToName: Map<B64Hash, string>;
};

export type DeltaPayload = SchemaDeltaPayload | GroupDeltaPayload;

export function versionHashes(v: Version): B64Hash[] {
    return [...v].sort();
}

export function schemaChangeRows(tableChanges: TableChange[]): Record<string, unknown>[] {
    const rows: Record<string, unknown>[] = [];
    for (const change of tableChanges) {
        if (!change.existedBefore && change.existsAfter) {
            rows.push({ table: change.table, change: 'add-table', detail: '' });
            continue;
        }
        if (change.existedBefore && !change.existsAfter) {
            rows.push({ table: change.table, change: 'drop-table', detail: '' });
            continue;
        }
        for (const columnChange of change.columnChanges) {
            const kind = columnChange.before === undefined
                ? 'add-column'
                : columnChange.after === undefined
                    ? 'drop-column'
                    : 'change-column';
            rows.push({
                table: change.table,
                change: kind,
                detail: formatColumnDetail(columnChange.column, columnChange.before, columnChange.after),
            });
        }
        if (change.concurrentDeletesChanged) {
            rows.push({ table: change.table, change: 'concurrent-deletes', detail: 'changed' });
        }
        if (change.fksChanged) {
            rows.push({ table: change.table, change: 'fks', detail: 'changed' });
        }
        if (change.restrictionsChanged) {
            rows.push({ table: change.table, change: 'restrictions', detail: 'changed' });
        }
    }
    return rows;
}

function formatColumnDetail(column: string, before: unknown, after: unknown): string {
    if (before === undefined && after !== undefined) {
        return `${column}: ${JSON.stringify(after)}`;
    }
    if (before !== undefined && after === undefined) {
        return column;
    }
    if (before !== undefined && after !== undefined) {
        return `${column}: ${JSON.stringify(before)} -> ${JSON.stringify(after)}`;
    }
    return column;
}
