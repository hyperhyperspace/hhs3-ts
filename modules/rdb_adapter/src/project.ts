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
import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import type {
    OpVerdictChange, Row, RSchemaView, RTableGroup, RTableGroupDelta, RTableGroupView,
} from "@hyper-hyper-space/hhs3_rdb";

import { AdapterConfig, MaterializationTarget, OpEvent, RowAction } from "./types.js";
import { projectedColumnName, projectedIdentityColumnName, providerColumnRole, targetTableName } from "./names.js";
import { initialSchemaActions, reprojectedTables, schemaDeltaActions } from "./schema_actions.js";
import { rowActionsForDelta } from "./row_actions.js";

// Project one live rdb Row into a full-row upsert action (all written columns).
// Shared by the initial backfill and the ingestion-failure recovery path (which
// re-materializes a genuinely-failed row from rdb truth). Provider public-key
// columns are captured into keyMaterial; identity columns project as `_key_id`.
export function rowToUpsertAction(
    row: Row, rdbTable: string, schemaView: RSchemaView, config: AdapterConfig,
): RowAction {
    const table = targetTableName(config, rdbTable);
    const fks = schemaView.getFKs(rdbTable);
    const provider = schemaView.getIdProvider?.(rdbTable);

    const values: { [column: string]: json.Literal } = {};
    const keyMaterial: { [keyHash: string]: string } = {};
    let keyHash: string | undefined;
    let publicKey: string | undefined;

    for (const [column, value] of Object.entries(row.values)) {
        const role = providerColumnRole(provider, column);
        if (role.role === 'publicKey') {
            if (typeof value === 'string') publicKey = value;
            continue;
        }
        if (role.role === 'keyId') {
            if (typeof value === 'string') keyHash = value;
        }
        const colDef = schemaView.getTable?.(rdbTable)?.columns?.[column];
        if (colDef !== undefined && colDef.type === 'identity') {
            values[projectedIdentityColumnName(config, rdbTable, column)] = value;
            continue;
        }
        values[projectedColumnName(config, rdbTable, column, fks, provider)] = value;
    }

    if (keyHash !== undefined && publicKey !== undefined) keyMaterial[keyHash] = publicKey;

    const action: RowAction = { kind: 'upsert-row', table, rowId: row.rowId, values };
    if (row.author !== undefined) action.author = row.author;
    if (Object.keys(keyMaterial).length > 0) action.keyMaterial = keyMaterial;
    return action;
}

// Map the delta's op-verdict flips into durable concurrency op-events. A flip to
// voided is a 'void'; a flip back to live is a 'reinstate'. Both are surfaced so
// the app learns of a voided (or reinstated) op exactly as it learns of an
// ingestion failure. The op itself is fetchable via loadEntry(opHash); the
// structured reason (when present) is carried through.
export function opVerdictEvents(
    changes: OpVerdictChange[], groupId: B64Hash, config: AdapterConfig,
): OpEvent[] {
    const events: OpEvent[] = [];
    for (const c of changes) {
        const direction = c.voidAfter && !c.voidBefore ? 'void'
            : !c.voidAfter && c.voidBefore ? 'reinstate' : undefined;
        if (direction === undefined) continue;
        const event: OpEvent = { origin: 'concurrency', direction, groupId, opHash: c.entry, kind: c.kind };
        if (c.table !== undefined) event.table = targetTableName(config, c.table);
        if (c.rowId !== undefined) event.rowId = c.rowId;
        if (c.author !== undefined) event.author = c.author;
        if (c.reason !== undefined) event.reason = { source: 'void', detail: c.reason };
        events.push(event);
    }
    return events;
}

// Scan every live row at the view's horizon into upsert-row actions. Lives here
// (async, view-reading) rather than in the pure row_actions.ts. Engine-agnostic:
// any future target reuses it. All upserts (an initial backfill has no deletes);
// table + column names are the resolved target names. Provider publicKeyColumns
// are dropped (captured into keyMaterial); keyIdColumns project as key_id.
export async function initialRowActions(
    view: RTableGroupView,
    config: AdapterConfig = {},
): Promise<RowAction[]> {
    const actions: RowAction[] = [];
    const schemaView = view.getSchemaView();

    for (const rdbTable of view.getTableNames()) {
        const tableView = await view.getTableView(rdbTable);
        const rows = await tableView.query({});
        for (const row of rows) actions.push(rowToUpsertAction(row, rdbTable, schemaView, config));
    }

    return actions;
}

// Advance `target` to version `to`. Initial (no checkpoint): full schema +
// live-row backfill at `to`. Incremental: the schema + row channels of
// group.computeDelta(checkpoint, to). Both apply atomically in one call.
// Callers must pass a `to` that extends the target's current checkpoint.
export async function projectGroupTo(
    group: RTableGroup,
    target: MaterializationTarget,
    to: Version,
    config: AdapterConfig = {},
): Promise<void> {
    const groupId = group.getId();
    const view = await group.getView(to, to);
    const endView = view.getSchemaView();

    const checkpoint = await target.getCheckpoint(groupId);

    if (checkpoint === undefined) {
        const schemaActions = initialSchemaActions(endView, config);
        const rowActions = await initialRowActions(view, config);
        await target.apply(groupId, schemaActions, rowActions, to);
        return;
    }

    const delta = (await group.computeDelta(checkpoint, to)) as RTableGroupDelta;
    const startView = (await group.getView(checkpoint, checkpoint)).getSchemaView();
    const schemaActions = schemaDeltaActions(delta.schemaChanges, endView, startView, config);
    const rowActions = await planIncrementalRowActions(view, delta, startView, endView, groupId, config);
    const events = opVerdictEvents(delta.opVerdictChanges, groupId, config);
    await target.apply(groupId, schemaActions, rowActions, to, events);
}

// Advance `target` to the group's current frontier.
export async function projectGroup(
    group: RTableGroup,
    target: MaterializationTarget,
    config: AdapterConfig = {},
): Promise<void> {
    const to = await (await group.getScopedDag()).getFrontier();
    return projectGroupTo(group, target, to, config);
}

// Incremental row channel matching projectGroup: delta row actions, plus a
// live-view backfill of any table that needs a full re-scan to converge:
//   - REPROJECTED tables (a set-fks flip) whose existing rows' companions moved;
//   - NEWLY-CREATED tables, whose live rows may have been inserted BELOW the
//     delta's revision bound (a concurrent create+insert), so the row walk does
//     not enumerate them — the schema channel's create-table would otherwise
//     leave an empty table where the full projection has rows.
// Shared with the planner-parity fuzzer so the sweep cannot drift from production.
export async function planIncrementalRowActions(
    view: RTableGroupView,
    delta: RTableGroupDelta,
    startView: RSchemaView,
    endView: RSchemaView,
    groupId: B64Hash,
    config: AdapterConfig = {},
): Promise<RowAction[]> {
    const deltaRows = rowActionsForDelta(delta, endView, groupId, config);
    const backfillTables = reprojectedTables(delta.schemaChanges, endView, startView, config);
    for (const change of delta.schemaChanges.tableChanges) {
        // newly-created tables (rows may sit below the delta's revision bound)
        // and same-shape reincarnations (schema_actions drops+recreates them,
        // clearing every row) both need a full live-row re-scan to converge.
        if ((!change.existedBefore && change.existsAfter) || change.reincarnated) {
            backfillTables.add(change.table);
        }
    }
    return backfillTables.size === 0
        ? deltaRows
        : await mergeReprojectionBackfill(view, deltaRows, backfillTables, config);
}

// Merge a full-row live-view backfill of the reprojected (flipped) tables with
// the delta's own row actions. The backfill scans only the flipped tables'
// LIVE rows (rare set-fks migrations) and re-projects each as a full-row upsert
// via the same rowToUpsertAction the initial backfill / gold projection use, so
// it is idempotent for unchanged columns and populates the flipped companion.
// It SUPERSEDES the delta's now-redundant upserts for those tables (a full row
// is a superset of any per-column delta upsert of the same row) but KEEPS their
// deletes (the scan sees only live rows; a non-live row is a delta delete).
// Global order stays upserts-before-deletes, applied atomically by target.apply.
async function mergeReprojectionBackfill(
    view: RTableGroupView, deltaRows: RowAction[], backfillRdbTables: Set<string>, config: AdapterConfig,
): Promise<RowAction[]> {
    const schemaView = view.getSchemaView();
    const backfillTargets = new Set([...backfillRdbTables].map((t) => targetTableName(config, t)));
    const upserts: RowAction[] = [];
    for (const rdbTable of backfillRdbTables) {
        const tableView = await view.getTableView(rdbTable);
        for (const row of await tableView.query({})) upserts.push(rowToUpsertAction(row, rdbTable, schemaView, config));
    }
    const deletes: RowAction[] = [];
    for (const a of deltaRows) {
        if (a.kind === 'upsert-row') { if (!backfillTargets.has(a.table)) upserts.push(a); }
        else deletes.push(a);
    }
    return [...upserts, ...deletes];
}
