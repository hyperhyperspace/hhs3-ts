// Pure row planner: rdb per-table RowChange deltas -> an ordered list of
// RowActions. The data-side sibling of schema_actions.ts. No IO, no id
// allocation, no target. Callers drive it after the schema channel:
//
//   const delta = (await group.computeDelta(cp, to)) as RTableGroupDelta;
//   const endView = (await group.getView(to, to)).getSchemaView();
//   const actions = rowActionsForDelta(delta, endView, group.getId(), config);
//   await target.apply(schemaActions, actions, to);   // see project.ts

import type { json } from "@hyper-hyper-space/hhs3_json";
import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import {
    deriveTableId, RSchemaView, RTableChanges, RTableGroupDelta,
} from "@hyper-hyper-space/hhs3_rdb";

import { AdapterConfig, RowAction } from "./types.js";
import { projectedColumnName, projectedIdentityColumnName, providerColumnRole, targetTableName } from "./names.js";

// ---------------------------------------------------------------------------
// Pure per-table fold
// ---------------------------------------------------------------------------

// Translate one table's RowChanges into RowActions, preserving the delta's
// rowId order (rowChanges are pre-sorted, so output is deterministic).
//
// `liveAfter` decides the kind: a live row upserts (target inserts on first
// sight of the rowId, updates otherwise, keyed through the sync table), a
// non-live row deletes (deleted OR voided by an at-use verdict flip; the target
// removes only the app row and keeps the sync row for id stability).
//
// Column values pass through under their projected names. An FK column's key is
// FK-reshaped via `schemaView.getFKs`; the VALUE stays the rowId - the target
// translates a local FK's rowId to the referenced row's serial id at apply time.
// A provider keyIdColumn projects as `key_id` with the key-hash VALUE; its
// publicKeyColumn is DROPPED from values but captured into `keyMaterial` so the
// target can intern (key_hash, public_key) into rdb_keys. An identity-typed
// column projects as `<col>_key_id` with the key-hash VALUE.
export function tableRowActions(
    changes: RTableChanges,
    rdbTable: string,
    schemaView: RSchemaView,
    config: AdapterConfig,
): RowAction[] {
    const table = targetTableName(config, rdbTable);
    const fks = schemaView.getFKs(rdbTable);
    const provider = schemaView.getIdProvider?.(rdbTable);
    const actions: RowAction[] = [];

    for (const row of changes.rowChanges) {
        if (!row.liveAfter) {
            // Covers live->deleted and never-live rows (the latter are already
            // dropped by the delta; deleting a row the target lacks is a no-op).
            actions.push({ kind: 'delete-row', table, rowId: row.rowId });
            continue;
        }

        const values: { [column: string]: json.Literal } = {};
        const keyMaterial: { [keyHash: string]: string } = {};
        let keyHash: string | undefined;
        let publicKey: string | undefined;

        for (const change of row.columnChanges) {
            if (change.after === undefined) continue;   // unset value: nothing to write
            const role = providerColumnRole(provider, change.column);
            if (role.role === 'publicKey') {
                if (typeof change.after === 'string') publicKey = change.after;
                continue;   // not projected
            }
            if (role.role === 'keyId') {
                if (typeof change.after === 'string') keyHash = change.after;
            }
            // Identity-typed columns: value is a key hash; no public key on the
            // business column itself (interning uses hash alone until registered).
            const colDef = schemaView.getTable?.(rdbTable)?.columns?.[change.column];
            if (colDef !== undefined && colDef.type === 'identity') {
                values[projectedIdentityColumnName(config, rdbTable, change.column)] = change.after;
                continue;
            }
            values[projectedColumnName(config, rdbTable, change.column, fks, provider)] = change.after;
        }

        if (keyHash !== undefined && publicKey !== undefined) {
            keyMaterial[keyHash] = publicKey;
        }

        const action: RowAction = { kind: 'upsert-row', table, rowId: row.rowId, values };
        if (row.author !== undefined) action.author = row.author;
        if (Object.keys(keyMaterial).length > 0) action.keyMaterial = keyMaterial;
        actions.push(action);
    }

    return actions;
}

// ---------------------------------------------------------------------------
// Delta convenience
// ---------------------------------------------------------------------------

// Fold a whole group delta's row channel. `endView` is the resolved schema at
// the delta's end version; `groupId` is the RTableGroup object id used to map
// the nested delta's table ids (deriveTableId(groupId, name)) back to names.
//
// Ordering is upserts-before-deletes globally, tables in schema order, rows in
// delta (rowId) order. FK-safe cross-table ordering and cycles are the target's
// concern (deferred constraints / two-pass id allocation), not the planner's -
// as with the schema mapper, which does no topological FK ordering.
export function rowActionsForDelta(
    delta: RTableGroupDelta,
    endView: RSchemaView,
    groupId: B64Hash,
    config: AdapterConfig = {},
): RowAction[] {
    const tableChanges = delta.tableChanges;

    const upserts: RowAction[] = [];
    const deletes: RowAction[] = [];

    // Only tables that still exist at the end version are projected; a table
    // dropped in this delta has its rows removed by the schema channel's
    // drop-table, so its residual row changes are intentionally ignored.
    for (const rdbTable of endView.getTableNames()) {
        const changes = tableChanges.get(deriveTableId(groupId, rdbTable));
        if (changes === undefined) continue;
        for (const action of tableRowActions(changes, rdbTable, endView, config)) {
            (action.kind === 'delete-row' ? deletes : upserts).push(action);
        }
    }

    return [...upserts, ...deletes];
}
