// Orchestrator: drives one RTableGroup into a MaterializationTarget for a
// single checkpoint advance. It is engine-agnostic - it only produces the
// SchemaAction / RowAction lists (via the pure planners) and hands them to
// target.apply(); the target owns all IO and atomicity.
//
//   const target = new SqliteTarget(new Database(path));
//   await projectGroup(group, target);        // initial backfill, then
//   await projectGroup(group, target);        // incremental (idempotent per version)
//
// The initial-vs-delta decision is the target's checkpoint: absent -> full
// backfill (schema + a live-row scan); present -> group.computeDelta from it.

import type { json } from "@hyper-hyper-space/hhs3_json";
import type {
    RTableGroup, RTableGroupDelta, RTableGroupView,
} from "@hyper-hyper-space/hhs3_rdb";

import { AdapterConfig, MaterializationTarget, RowAction } from "./types.js";
import { targetColumnName, targetTableName } from "./names.js";
import { initialSchemaActions, schemaDeltaActions } from "./schema_actions.js";
import { rowActionsForDelta } from "./row_actions.js";

// Scan every live row at the view's horizon into upsert-row actions. Lives here
// (async, view-reading) rather than in the pure row_actions.ts. Engine-agnostic:
// any future target reuses it. All upserts (an initial backfill has no deletes);
// table + column names are the resolved target names.
export async function initialRowActions(
    view: RTableGroupView,
    config: AdapterConfig = {},
): Promise<RowAction[]> {
    const actions: RowAction[] = [];

    for (const rdbTable of view.getTableNames()) {
        const table = targetTableName(config, rdbTable);
        const tableView = await view.getTableView(rdbTable);
        const rows = await tableView.query({});

        for (const row of rows) {
            const values: { [column: string]: json.Literal } = {};
            for (const [column, value] of Object.entries(row.values)) {
                values[targetColumnName(config, rdbTable, column)] = value;
            }
            const action: RowAction = { kind: 'upsert-row', table, rowId: row.rowId, values };
            if (row.author !== undefined) action.author = row.author;
            actions.push(action);
        }
    }

    return actions;
}

// Advance `target` to the group's current frontier. Initial (no checkpoint):
// full schema + live-row backfill. Incremental: the schema + row channels of
// group.computeDelta(checkpoint, frontier). Both apply atomically in one call.
export async function projectGroup(
    group: RTableGroup,
    target: MaterializationTarget,
    config: AdapterConfig = {},
): Promise<void> {
    const to = await (await group.getScopedDag()).getFrontier();
    const view = await group.getView(to, to);
    const endView = view.getSchemaView();

    const checkpoint = await target.getCheckpoint();

    if (checkpoint === undefined) {
        const schemaActions = initialSchemaActions(endView, config);
        const rowActions = await initialRowActions(view, config);
        await target.apply(schemaActions, rowActions, to);
        return;
    }

    const delta = (await group.computeDelta(checkpoint, to)) as RTableGroupDelta;
    const schemaActions = schemaDeltaActions(delta.schemaChanges, endView, config);
    const rowActions = rowActionsForDelta(delta, endView, group.getId(), config);
    await target.apply(schemaActions, rowActions, to);
}
