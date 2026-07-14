import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import type { RSchemaDelta, RTableGroupDelta, TableChange } from "@hyper-hyper-space/hhs3_rdb";

export type SchemaDeltaPayload = {
    kind: 'schema'; name: string; objectId: B64Hash; delta: RSchemaDelta;
};
export type GroupDeltaPayload = {
    kind: 'group'; name: string; objectId: B64Hash; delta: RTableGroupDelta;
    tableIdToName: Map<B64Hash, string>;
};
export type DeltaPayload = SchemaDeltaPayload | GroupDeltaPayload;

export function versionHashes(version: Version): B64Hash[] {
    return [...version].sort();
}

export function schemaChangeRows(changes: TableChange[]): Record<string, unknown>[] {
    const rows: Record<string, unknown>[] = [];
    for (const change of changes) {
        if (!change.existedBefore && change.existsAfter) rows.push({ table: change.table, change: 'add-table', detail: '' });
        else if (change.existedBefore && !change.existsAfter) rows.push({ table: change.table, change: 'drop-table', detail: '' });
        else {
            for (const column of change.columnChanges) {
                const kind = column.before === undefined ? 'add-column'
                    : column.after === undefined ? 'drop-column' : 'change-column';
                const detail = column.before === undefined
                    ? `${column.column}: ${JSON.stringify(column.after)}`
                    : column.after === undefined ? column.column
                        : `${column.column}: ${JSON.stringify(column.before)} -> ${JSON.stringify(column.after)}`;
                rows.push({ table: change.table, change: kind, detail });
            }
            if (change.concurrentDeletesChanged) rows.push({ table: change.table, change: 'concurrent-deletes', detail: 'changed' });
            if (change.fksChanged) rows.push({ table: change.table, change: 'fks', detail: 'changed' });
            if (change.restrictionsChanged) rows.push({ table: change.table, change: 'restrictions', detail: 'changed' });
        }
    }
    return rows;
}
