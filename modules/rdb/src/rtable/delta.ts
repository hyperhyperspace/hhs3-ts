// Per-table delta channel: the row-level changes between two group positions
// for one member table. This is a NESTED accumulator (the group leads the
// delta; see ../rtable_group/delta.ts) keyed under the table's id in the root
// delta's `nested` map.
//
// The row channel emits a RowChange for a row IFF its enforced liveness flipped
// OR a WRITTEN (non-default) column value moved — i.e. only effects of row-ops
// or at-use voiding-verdict flips, all of which sit above the combined revision
// bound (at-use semantics make this floor exact: a causal-past schema/target
// can no longer revise an old row). Schema-default and table/column-drop effects
// are UNIFORM and never enumerated here; they live in the group's schema
// sub-delta channel, applied by the consumer to rows not present in the walk.
//
// Diffing uses deltaRowState (LWW written values, no default fallback) over the
// UNION of both horizons' schema columns; incarnation-scoped resolution keeps
// drop/re-add from surfacing stale per-row diffs. See ../rtable/view.ts.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, KeyId } from "@hyper-hyper-space/hhs3_crypto";
import { dag } from "@hyper-hyper-space/hhs3_dag";
import { Version, DeltaChanges, DeltaAccumulator } from "@hyper-hyper-space/hhs3_mvt";

import { tableOpsFromGroupPayload } from "../rtable_group/scopes.js";

import type { RTable } from "./interfaces.js";

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
        private readonly table: RTable,
        private readonly start: Version,
        private readonly end: Version,
    ) {}

    // Collect every rowId this table touches in the walked entry (insert /
    // update / delete all matter: any could flip liveness or a written value).
    async ingest(entry: dag.Entry): Promise<boolean> {
        const ops = tableOpsFromGroupPayload(entry.payload, this.table.getTableName());
        let touched = false;
        for (const op of ops) {
            this.candidates.add(op.rowId);
            touched = true;
        }
        return touched;
    }

    async finalize(): Promise<DeltaChanges<RTableChanges>> {
        const viewStart = await this.table.getView(this.start, this.start);
        const viewEnd = await this.table.getView(this.end, this.end);

        // diff over the union of both horizons' columns (schema-independent
        // written resolution keeps drops/defaults out of the per-row diff)
        const columns = [...new Set<string>([
            ...(await viewStart.getColumns()),
            ...(await viewEnd.getColumns()),
        ])];

        const rowChanges: RowChange[] = [];
        for (const rowId of [...this.candidates].sort()) {
            const before = await viewStart.deltaRowState(rowId, columns);
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
