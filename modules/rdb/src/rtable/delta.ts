// Per-table delta channel: the row-level changes between two group positions
// for one member table. This is a NESTED accumulator (the group leads the
// delta; see ../rtable_group/delta.ts) keyed under the table's id in the root
// delta's `nested` map.
//
// The row channel emits a RowChange for a row IFF its enforced liveness flipped
// OR a WRITTEN (non-default) column value moved, comparing the row within the
// END horizon's incarnations: the row counts as live at start only if the
// table had the same incarnation there, and a column's start value counts only
// within the same column incarnation. What is left are effects of row-ops
// (including the deletes a schema deploy carries for the rows stranded by an
// FK it adopts, read from its meta like a delete's identity tags) and at-use
// voiding-verdict flips, which sit above the combined revision bound
// (at-use semantics make this floor exact: a causal-past schema/target can no
// longer revise an old row), and writes under a new incarnation. Schema
// defaults and table/column drops and re-adds are UNIFORM and never enumerated
// here: liveness and written values are incarnation-scoped, so a drop or reset
// changes every old row of the table, walked or not. They live in the group's
// schema sub-delta channel, which the consumer applies table-wide.
//
// Diffing uses deltaRowState (LWW written values, no default fallback) over the
// END horizon's columns. See ../rtable/view.ts.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, KeyId } from "@hyper-hyper-space/hhs3_crypto";
import { dag } from "@hyper-hyper-space/hhs3_dag";
import { Version, DeltaChanges, DeltaAccumulator } from "@hyper-hyper-space/hhs3_mvt";

import type { RSchemaView } from "../rschema/interfaces.js";
import { killedRowIds, tableOpsFromGroupPayload } from "../rtable_group/scopes.js";

import type { DeltaRowState, RTable } from "./interfaces.js";

// What the accumulator needs from its table: the public contract plus the
// effective schema at a horizon (implemented by RTableImpl).
type DeltaTable = RTable & {
    resolveSchemaView(at: Version, from?: Version): Promise<RSchemaView>;
};

const NOT_LIVE: DeltaRowState = { live: false, author: undefined, written: {} };

export type ColumnValueChange = {
    column: string;
    before: json.Literal | undefined;
    after: json.Literal | undefined;
};

export type RowChange = {
    rowId: B64Hash;
    liveBefore: boolean;
    liveAfter: boolean;
    author: KeyId | undefined;         // from the live insert (stable across the row's life)
    columnChanges: ColumnValueChange[]; // written-value diffs only, sorted by column
};

export type RTableChanges = {
    rowChanges: RowChange[];           // sorted by rowId; empty rows dropped
};

function sameLiteral(a: json.Literal | undefined, b: json.Literal | undefined): boolean {
    if (a === undefined || b === undefined) return a === b;
    return json.toStringNormalized(a) === json.toStringNormalized(b);
}

export class RTableDeltaAccumulator implements DeltaAccumulator<RTableChanges> {

    private readonly candidates = new Set<B64Hash>();

    constructor(
        private readonly table: DeltaTable,
        private readonly start: Version,
        private readonly end: Version,
    ) {}

    // Collect every rowId this table touches in the walked entry (insert /
    // update / delete all matter: any could flip liveness or a written value),
    // including the rows a schema deploy deletes for an FK it adopts.
    async ingest(entry: dag.Entry): Promise<boolean> {
        const table = this.table.getTableName();
        let touched = false;
        for (const op of tableOpsFromGroupPayload(entry.payload, table)) {
            this.candidates.add(op.rowId);
            touched = true;
        }
        for (const rowId of killedRowIds(entry, table)) {
            this.candidates.add(rowId);
            touched = true;
        }
        return touched;
    }

    async finalize(): Promise<DeltaChanges<RTableChanges>> {
        const viewStart = await this.table.getView(this.start, this.start);
        const viewEnd = await this.table.getView(this.end, this.end);

        // compare within the end horizon's incarnations (see the header)
        const name = this.table.getTableName();
        const schemaStart = await this.table.resolveSchemaView(this.start, this.start);
        const schemaEnd = await this.table.resolveSchemaView(this.end, this.end);
        const tableIncarnation = schemaEnd.getTableIncarnation(name);
        const sameTable = tableIncarnation !== undefined && schemaStart.getTableIncarnation(name) === tableIncarnation;
        const columns = await viewEnd.getColumns();
        const carried = sameTable
            ? columns.filter((c) => schemaStart.getColumnIncarnation(name, c) === schemaEnd.getColumnIncarnation(name, c))
            : [];

        const rowChanges: RowChange[] = [];
        for (const rowId of [...this.candidates].sort()) {
            const before = sameTable ? await viewStart.deltaRowState(rowId, carried) : NOT_LIVE;
            const after = await viewEnd.deltaRowState(rowId, columns);

            const columnChanges: ColumnValueChange[] = [];
            if (after.live) {
                for (const column of columns) {
                    const b = before.live ? before.written[column] : undefined;
                    const a = after.written[column];
                    if (!sameLiteral(b, a)) columnChanges.push({ column, before: b, after: a });
                }
                columnChanges.sort((x, y) => (x.column < y.column ? -1 : x.column > y.column ? 1 : 0));
            }

            if (before.live === after.live && columnChanges.length === 0) continue;

            rowChanges.push({
                rowId,
                liveBefore: before.live,
                liveAfter: after.live,
                author: after.live ? after.author : before.author,
                columnChanges,
            });
        }

        return {
            type: this.table.getType(),
            changes: { rowChanges },
            nested: new Map(),
        };
    }
}
