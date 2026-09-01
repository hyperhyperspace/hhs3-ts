// Changed-slots delta for RSchema: which tables (and which of their slots)
// differ between two resolved schema states.
//
// The accumulator follows the RCap pattern: ingest collects the candidate
// tables touched by each walked entry, finalize resolves the views at start
// and end and diffs only those candidates. The RSchema DAG has no barriers,
// so this is the only place where `from`/`start` matters for RSchema.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { dag } from "@hyper-hyper-space/hhs3_dag";
import {
    Version, Delta, DeltaChanges, DeltaAccumulator,
    walkDelta, computeForkMeet,
} from "@hyper-hyper-space/hhs3_mvt";

import { ColumnDef } from "./payload.js";
import { CreateRSchemaPayload, SchemaUpdatePayload } from "./payload.js";
import type { RSchema, RSchemaView } from "./interfaces.js";

export type ColumnChange = {
    column: string;
    before: ColumnDef | undefined;
    after: ColumnDef | undefined;
    // True iff `before` and `after` are byte-identical defs but the column's
    // LIVE INCARNATION differs across the delta — a same-shape drop+re-add (or
    // a table reincarnation via two identical add-table forks resolving to
    // different winners). The resolved def is unchanged, but every existing
    // row's written value for the OLD incarnation is masked, so a consumer must
    // treat it like a drop+add (clear the column, re-materialize). False for an
    // ordinary def change (before !== after) and for a pure add / drop.
    reincarnated: boolean;
};

export type TableChange = {
    table: string;
    existedBefore: boolean;
    existsAfter: boolean;
    // True iff the table exists on both sides but its LIVE TABLE INCARNATION
    // differs across the delta — a same-shape table drop+re-add (or a losing
    // concurrent-create fork resolving to a different winner). The resolved def
    // may be byte-identical, but every prior-incarnation row is masked, so a
    // consumer must treat it like drop-table + create-table + backfill. False
    // for a pure add / drop and for an in-place column/slot change.
    reincarnated: boolean;
    // slot-level detail, only populated when the table exists on both sides
    columnChanges: ColumnChange[];
    concurrentDeletesChanged: boolean;
    fksChanged: boolean;
    restrictionsChanged: boolean;
    // The table's idProvider designation changed. idProvider rides on the
    // winning add-table base def (no set-idProvider rule exists), so this only
    // flips on a table reincarnation; surfaced explicitly so a consumer whose
    // projection depends on the provider (which columns are dropped / renamed)
    // is never left to infer it from column reincarnations.
    idProviderChanged: boolean;
};

export type RSchemaChanges = {
    tableChanges: TableChange[];
};

export class RSchemaDelta implements Delta<RSchemaChanges> {
    readonly type: string;
    readonly changes: RSchemaChanges;
    readonly nested: ReadonlyMap<B64Hash, DeltaChanges>;

    constructor(
        public readonly start: Version,
        public readonly end: Version,
        public readonly revisionBound: Version,
        root: DeltaChanges<RSchemaChanges>,
    ) {
        this.type = root.type;
        this.changes = root.changes;
        this.nested = root.nested;
    }

    get tableChanges(): TableChange[] { return this.changes.tableChanges; }
}

function sameLiteral(a: json.Literal | undefined, b: json.Literal | undefined): boolean {
    if (a === undefined || b === undefined) return a === b;
    return json.toStringNormalized(a) === json.toStringNormalized(b);
}

// Diff one candidate table across the delta. Takes the resolved START and END
// views (not just the TableDefs) because a same-shape reincarnation is only
// visible through getColumnIncarnation / getIdProvider — the defs are equal.
function diffTable(table: string, startView: RSchemaView, endView: RSchemaView): TableChange | undefined {
    const before = startView.getTable(table);
    const after = endView.getTable(table);
    if (before === undefined && after === undefined) return undefined;

    const change: TableChange = {
        table,
        existedBefore: before !== undefined,
        existsAfter: after !== undefined,
        reincarnated: false,
        columnChanges: [],
        concurrentDeletesChanged: false,
        fksChanged: false,
        restrictionsChanged: false,
        idProviderChanged: false,
    };

    if (before === undefined || after === undefined) return change;

    // Same-shape reincarnation: table present on both sides but the live table
    // incarnation flipped (drop+re-add, or a losing concurrent-create fork).
    // Reported so a consumer resets the table (drop + create + backfill).
    change.reincarnated =
        startView.getTableIncarnation(table) !== endView.getTableIncarnation(table);

    for (const column of new Set([...Object.keys(before.columns), ...Object.keys(after.columns)])) {
        if (!sameLiteral(before.columns[column], after.columns[column])) {
            // Ordinary def change or pure add / drop: the defs already differ.
            change.columnChanges.push({
                column, before: before.columns[column], after: after.columns[column], reincarnated: false,
            });
        } else if (before.columns[column] !== undefined
            && startView.getColumnIncarnation(table, column) !== endView.getColumnIncarnation(table, column)) {
            // Same resolved def, new live incarnation: a same-shape drop+re-add
            // that leaves old rows' written values masked. Reported so a
            // consumer can clear + re-materialize the column.
            change.columnChanges.push({
                column, before: before.columns[column], after: after.columns[column], reincarnated: true,
            });
        }
    }

    change.concurrentDeletesChanged = before.concurrentDeletes !== after.concurrentDeletes;
    change.fksChanged = !sameLiteral(before.fks, after.fks);
    change.restrictionsChanged = !sameLiteral(
        before.restrictions as json.Literal | undefined,
        after.restrictions as json.Literal | undefined);
    change.idProviderChanged = !sameLiteral(
        startView.getIdProvider(table) as json.Literal | undefined,
        endView.getIdProvider(table) as json.Literal | undefined);

    if (!change.reincarnated && change.columnChanges.length === 0 && !change.concurrentDeletesChanged
        && !change.fksChanged && !change.restrictionsChanged && !change.idProviderChanged) {
        return undefined;
    }

    return change;
}

export class RSchemaDeltaAccumulator implements DeltaAccumulator<RSchemaChanges> {

    private readonly candidateTables = new Set<string>();

    constructor(
        private readonly schema: RSchema,
        private readonly start: Version,
        private readonly end: Version,
    ) {}

    async ingest(entry: dag.Entry): Promise<boolean> {
        const payload = entry.payload as json.LiteralMap;

        if (payload['action'] === 'create') {
            for (const def of (payload as CreateRSchemaPayload).tables) {
                this.candidateTables.add(def.name);
            }
            return true;
        }

        if (payload['action'] === 'schema-update') {
            for (const rule of (payload as SchemaUpdatePayload).migration) {
                this.candidateTables.add(rule.rule === 'add-table' ? rule.def.name : rule.table);
            }
            return true;
        }

        return false;
    }

    async finalize(): Promise<DeltaChanges<RSchemaChanges>> {
        const startView = await this.schema.getView(this.start, this.start);
        const endView = await this.schema.getView(this.end, this.end);

        const tableChanges: TableChange[] = [];
        for (const table of this.candidateTables) {
            const change = diffTable(table, startView, endView);
            if (change !== undefined) tableChanges.push(change);
        }

        return {
            type: this.schema.getType(),
            changes: { tableChanges },
            nested: new Map(),
        };
    }
}

export async function computeRSchemaDelta(
    schema: RSchema, rawDag: dag.Dag,
    start: Version, end: Version,
): Promise<RSchemaDelta> {
    const fork = await rawDag.findForkPosition(start, end);
    if (fork.forkA.size > 0) {
        throw new Error("computeDelta requires END to extend START");
    }

    if (fork.forkB.size === 0) {
        const empty: DeltaChanges<RSchemaChanges> = {
            type: schema.getType(), changes: { tableChanges: [] }, nested: new Map(),
        };
        return new RSchemaDelta(start, end, fork.commonFrontier, empty);
    }

    const revisionBound = await computeForkMeet(rawDag, fork.common);
    const root = await walkDelta(rawDag, start, end, revisionBound, new RSchemaDeltaAccumulator(schema, start, end));
    return new RSchemaDelta(start, end, revisionBound, root as DeltaChanges<RSchemaChanges>);
}
