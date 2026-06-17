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

import { TableDef, ColumnDef } from "./payload.js";
import { CreateRSchemaPayload, SchemaUpdatePayload } from "./payload.js";
import type { RSchema } from "./interfaces.js";

export type ColumnChange = {
    column: string;
    before: ColumnDef | undefined;
    after: ColumnDef | undefined;
};

export type TableChange = {
    table: string;
    existedBefore: boolean;
    existsAfter: boolean;
    // slot-level detail, only populated when the table exists on both sides
    columnChanges: ColumnChange[];
    concurrentDeletesChanged: boolean;
    fksChanged: boolean;
    restrictionsChanged: boolean;
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

function diffTable(table: string, before: TableDef | undefined, after: TableDef | undefined): TableChange | undefined {
    if (before === undefined && after === undefined) return undefined;

    const change: TableChange = {
        table,
        existedBefore: before !== undefined,
        existsAfter: after !== undefined,
        columnChanges: [],
        concurrentDeletesChanged: false,
        fksChanged: false,
        restrictionsChanged: false,
    };

    if (before === undefined || after === undefined) return change;

    for (const column of new Set([...Object.keys(before.columns), ...Object.keys(after.columns)])) {
        if (!sameLiteral(before.columns[column], after.columns[column])) {
            change.columnChanges.push({ column, before: before.columns[column], after: after.columns[column] });
        }
    }

    change.concurrentDeletesChanged = before.concurrentDeletes !== after.concurrentDeletes;
    change.fksChanged = !sameLiteral(before.fks, after.fks);
    change.restrictionsChanged = !sameLiteral(
        before.restrictions as json.Literal | undefined,
        after.restrictions as json.Literal | undefined);

    if (change.columnChanges.length === 0 && !change.concurrentDeletesChanged
        && !change.fksChanged && !change.restrictionsChanged) {
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
            const change = diffTable(table, startView.getTable(table), endView.getTable(table));
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
