// HIP-29's invariant as a test check: after every target transaction, each
// group's tables are rdb's rows at the group's checkpoint, with the outbox's
// changes applied on top in commit order. There is no mask: every row of every
// projected table is compared, by local id.
//
// The comparison is in the target's vocabulary. rdb's rows at the checkpoint
// are turned into what the target should hold for them: projected column
// names, a local foreign key as the referenced row's local id, a key-ref
// column and the author as the key's local id. The pending changes are applied
// on top as capture recorded them: an insert writes its whole row, an update
// writes its columns, a delete removes the row, and an update or a delete of a
// row that isn't there does nothing. NULL and an absent column are the same.

import { assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { json } from "@hyper-hyper-space/hhs3_json";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import type { RSchemaView, RTableGroup } from "@hyper-hyper-space/hhs3_rdb";
import {
    AdapterConfig, CapturedChange, DEFAULT_KEY_DOMAIN, KeyIndex, MaterializationTarget, MaterializedChangeSource,
    QueryableTarget, RowIdentityIndex, authorColumn, idColumn, providerColumnRole, resolveFk, targetTableName,
} from "@hyper-hyper-space/hhs3_rdb_adapter";

import { projectedNameForRdbColumn } from "./rdb_projection_oracle.js";

// What the check reads from a target: its checkpoints, its pending changes, its
// rows by local id, and its two identity indexes.
export type CheckedTarget = MaterializationTarget & MaterializedChangeSource & QueryableTarget
    & RowIdentityIndex & KeyIndex;

// One group to check, with the adapter config it is projected with (its table
// names, author column, and so on).
export type CheckedGroup = { group: RTableGroup; config?: AdapterConfig };

type LocalRow = { [column: string]: json.Literal };
type TableRows = Map<number, LocalRow>;

// A value as the comparison sees it: NULL is absent, and every other value is
// compared by its normalized json text.
function present(row: { [column: string]: json.Literal | null | undefined }): LocalRow {
    const out: LocalRow = {};
    for (const [column, value] of Object.entries(row)) {
        if (value !== null && value !== undefined) out[column] = value;
    }
    return out;
}

function rowText(row: LocalRow | undefined): string {
    if (row === undefined) return '<no row>';
    const sorted: LocalRow = {};
    for (const column of Object.keys(row).sort()) sorted[column] = row[column];
    return json.toStringNormalized(sorted);
}

// What the target should hold for one rdb row at the checkpoint, in target
// columns, with references turned into local ids. A reference to a row the
// target has no id for stays as text, so it can never match by accident.
async function expectedFromRdb(
    target: CheckedTarget, config: AdapterConfig, rdbTable: string,
    values: { [column: string]: json.Literal }, author: string | undefined, schemaView: RSchemaView,
): Promise<LocalRow> {
    const out: LocalRow = {};
    const fks = schemaView.getFKs(rdbTable);
    const provider = schemaView.getIdProvider?.(rdbTable);
    const columnDefs = schemaView.getTable(rdbTable)?.columns ?? {};
    for (const [rdbColumn, value] of Object.entries(values)) {
        if (value === null || value === undefined) continue;
        const name = projectedNameForRdbColumn(config, rdbTable, rdbColumn, schemaView);
        if (name === undefined) continue;   // a public key column is not projected
        const ref = fks[rdbColumn];
        const isKeyRef = providerColumnRole(provider, rdbColumn).role === 'keyId'
            || columnDefs[rdbColumn]?.type === 'identity';
        if (ref !== undefined) {
            const fk = resolveFk(config, ref);
            if (fk.kind === 'id') {
                const localId = await target.localIdForRowHash(fk.targetTable, value as string);
                out[name] = localId ?? `unmapped row ${String(value)}`;
                continue;
            }
        } else if (isKeyRef) {
            const keyId = await target.idForKeyHash(DEFAULT_KEY_DOMAIN, value as string);
            out[name] = keyId ?? `unknown key ${String(value)}`;
            continue;
        }
        out[name] = value;
    }
    const authorCol = authorColumn(config);
    if (authorCol !== undefined && author !== undefined && schemaView.hasTable(rdbTable)) {
        const keyId = await target.idForKeyHash(DEFAULT_KEY_DOMAIN, author);
        out[authorCol] = keyId ?? `unknown key ${author}`;
    }
    return out;
}

// Applies one captured change on top of the expected rows.
function applyChange(rows: TableRows, change: CapturedChange): void {
    if (change.kind === 'insert') {
        rows.set(change.localId, present(change.values));
        return;
    }
    const existing = rows.get(change.localId);
    if (existing === undefined) return;   // an update or a delete of a missing row does nothing
    if (change.kind === 'delete') {
        rows.delete(change.localId);
        return;
    }
    const next: LocalRow = { ...existing };
    for (const [column, value] of Object.entries(change.values)) {
        if (value === null) delete next[column];
        else next[column] = value;
    }
    rows.set(change.localId, next);
}

// The rows a target table holds now, by local id, as the app reads them.
async function actualRows(target: CheckedTarget, config: AdapterConfig, table: string): Promise<TableRows> {
    const rows: TableRows = new Map();
    const id = idColumn(config);
    for (const row of await target.executeQuery({ table })) {
        const values: { [column: string]: json.Literal | null } = { ...row };
        const localId = row[id] as number;
        delete values[id];
        rows.set(localId, present(values));
    }
    return rows;
}

// The check itself. `label` names the step in the failure message.
export async function assertAtCheckpoints(
    target: CheckedTarget, groups: CheckedGroup[], label: string,
): Promise<void> {
    const expected = new Map<string, TableRows>();
    const configs = new Map<string, AdapterConfig>();
    for (const { group, config = {} } of groups) {
        const checkpoint: Version | undefined = await target.getCheckpoint(group.getId());
        if (checkpoint === undefined) continue;
        const view = await group.getView(checkpoint, checkpoint);
        const schemaView = view.getSchemaView();
        for (const rdbTable of view.getTableNames()) {
            const table = targetTableName(config, rdbTable);
            configs.set(table, config);
            const rows: TableRows = new Map();
            for (const row of await (await view.getTableView(rdbTable)).query({})) {
                const localId = await target.localIdForRowHash(table, row.rowId);
                const key = localId ?? -1;
                rows.set(key, await expectedFromRdb(target, config, rdbTable, row.values, row.author, schemaView));
            }
            expected.set(table, rows);
        }
    }

    for (const change of (await target.drainChanges()).changes) {
        const rows = expected.get(change.table);
        if (rows !== undefined) applyChange(rows, change);
    }

    for (const [table, rows] of expected) {
        const config = configs.get(table) ?? {};
        const actual = await actualRows(target, config, table);
        const ids = [...new Set([...rows.keys(), ...actual.keys()])].sort((a, b) => a - b);
        for (const localId of ids) {
            assertEquals(rowText(actual.get(localId)), rowText(rows.get(localId)),
                `${label}: ${table}#${localId} is rdb's row at the checkpoint with the pending changes on top`);
        }
    }
}
