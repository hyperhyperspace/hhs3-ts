// RTableGroup delta: the group leads a three-channel delta between two of its
// positions, mirroring RSet's full/bounded split but over MORE than one
// observed object (its RSchema plus every bound foreign group).
//
// Three channels:
//   - schema sub-delta (uniform): `schemaChanges` carries column defaults,
//     table/column drops, FK/restriction/concurrentDeletes flips as RSchema
//     changes. The consumer applies these to rows NOT present in the row-walk;
//     they are never enumerated per row (an old untouched row affected only by
//     a default/drop produces no RowChange).
//   - per-table row-walk (positional): each touched member table's RTableChanges
//     in the root delta's `nested` map, keyed by the table id. A row appears
//     only when its liveness flipped or a written value moved (see ../rtable/delta.ts).
//   - op verdict flips: `opVerdictChanges` lists group DAG entries whose
//     at-use void verdict changed between the start and end view horizons
//     (insert/update/delete/bundle ops and gated observes that flip). Row
//     channel materializes effects; op channel explains reconciliation flips.
//
// Strategies (RObject.setDeltaStrategy switch on the group):
//   - full: bound = empty, walk all history from genesis. The reference
//     implementation (slow, obviously correct), used for parity.
//   - bounded: bound = GLB(fork meet, projected bound of the schema, projected
//     bound of each bound foreign group) via combineObserverRevisionBounds. The
//     at-use semantics make this floor exact: below it no observed
//     object can revise an old row's verdict, so the walk above it captures
//     every positional change.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { dag } from "@hyper-hyper-space/hhs3_dag";
import {
    RObject, Version, version, Delta, DeltaChanges, DeltaAccumulator,
    computeForkMeet, combineObserverRevisionBounds, walkEntriesBackwardsToBound,
} from "@hyper-hyper-space/hhs3_mvt";

import type { RSchema } from "../rschema/interfaces.js";
import type { RSchemaChanges } from "../rschema/delta.js";
import type { RTable } from "../rtable/interfaces.js";
import type { RTableChanges } from "../rtable/delta.js";

import { computeOpVerdictFlips, type OpVerdictChange, type OpVoidDetail } from "./op_delta.js";

export type { OpVerdictChange, OpVerdictKind, OpVerdictWrite, OpVerdictHost, OpVoidDetail, OpVoidHorizon } from "./op_delta.js";
export { formatOpVoidDetail } from "./op_delta.js";

export type RTableGroupDeltaStrategy = 'full' | 'bounded';

export type RTableGroupChanges = {
    schemaChanges: RSchemaChanges;
    opVerdictChanges: OpVerdictChange[];
};

export class RTableGroupDelta implements Delta<RTableGroupChanges> {
    readonly type: string;
    readonly changes: RTableGroupChanges;
    readonly nested: ReadonlyMap<B64Hash, DeltaChanges>;

    constructor(
        public readonly start: Version,
        public readonly end: Version,
        public readonly revisionBound: Version,
        root: DeltaChanges<RTableGroupChanges>,
    ) {
        this.type = root.type;
        this.changes = root.changes;
        this.nested = root.nested;
    }

    get schemaChanges(): RSchemaChanges { return this.changes.schemaChanges; }

    get opVerdictChanges(): OpVerdictChange[] { return this.changes.opVerdictChanges; }

    // Per-table row changes, keyed by table id (the nested delta subtree).
    get tableChanges(): ReadonlyMap<B64Hash, RTableChanges> {
        const map = new Map<B64Hash, RTableChanges>();
        for (const [id, child] of this.nested) map.set(id, child.changes as RTableChanges);
        return map;
    }
}

// What the accumulator / computation needs from its group (RTableGroupImpl).
export type GroupDeltaHost = RObject & {
    resolveSchemaVersion(at: Version, from?: Version): Promise<Version>;
    getSchemaObject(): Promise<RSchema>;
    makeTable(name: string): RTable;
    // The objects this group OBSERVES — its RSchema plus every bound foreign
    // group — the referenced floors of the bounded revision bound.
    getObservedObjects(): Promise<RObject[]>;
    isEntryVoided(entryHash: B64Hash, from: Version): Promise<boolean>;
    explainEntryVoided(entryHash: B64Hash, from: Version): Promise<OpVoidDetail | undefined>;
    getSchemaRef(): B64Hash;
    getBindings(): { [name: string]: B64Hash };
};

export class RTableGroupDeltaAccumulator implements DeltaAccumulator<RTableGroupChanges> {

    // one child row-accumulator per touched member table
    private readonly tableAccumulators = new Map<string, DeltaAccumulator>();

    constructor(
        private readonly group: GroupDeltaHost,
        private readonly start: Version,
        private readonly end: Version,
    ) {}

    // Route the entry to each member table it touches (the `tables` meta lists
    // them; ref-advance entries carry none and route nowhere). The schema
    // sub-delta is computed wholesale in finalize, so deploy / observe entries
    // need no per-entry routing here.
    async ingest(entry: dag.Entry): Promise<boolean> {
        const tablesMeta = entry.meta['tables'];
        if (tablesMeta === undefined) return false;

        let changed = false;
        for (const table of json.fromSet(tablesMeta)) {
            let child = this.tableAccumulators.get(table);
            if (child === undefined) {
                child = this.group.makeTable(table).createDeltaAccumulator(this.start, this.end);
                this.tableAccumulators.set(table, child);
            }
            if (await child.ingest(entry)) changed = true;
        }
        return changed;
    }

    async finalize(): Promise<DeltaChanges<RTableGroupChanges>> {
        // schema sub-delta channel: the schema's own delta between the versions
        // observed at start and end (identical under either group strategy —
        // RSchema is confluent / bounded-exact).
        const schema = await this.group.getSchemaObject();
        const vStart = await this.group.resolveSchemaVersion(this.start, this.start);
        const vEnd = await this.group.resolveSchemaVersion(this.end, this.end);
        const schemaDelta = await schema.computeDelta(vStart, vEnd);
        const schemaChanges = schemaDelta.changes as RSchemaChanges;

        // per-table row channel: drop tables with no actual row changes
        const nested = new Map<B64Hash, DeltaChanges>();
        for (const [table, child] of this.tableAccumulators) {
            const childChanges = await child.finalize();
            if ((childChanges.changes as RTableChanges).rowChanges.length > 0) {
                nested.set(this.group.makeTable(table).getId(), childChanges);
            }
        }

        return {
            type: this.group.getType(),
            changes: { schemaChanges, opVerdictChanges: [] },
            nested,
        };
    }
}

function emptyChanges(type: string): DeltaChanges<RTableGroupChanges> {
    return {
        type,
        changes: { schemaChanges: { tableChanges: [] }, opVerdictChanges: [] },
        nested: new Map(),
    };
}

export async function computeRTableGroupDelta(
    group: GroupDeltaHost, rawDag: dag.Dag, strategy: RTableGroupDeltaStrategy,
    start: Version, end: Version,
): Promise<RTableGroupDelta> {
    const fork = await rawDag.findForkPosition(start, end);
    if (fork.forkA.size > 0) {
        throw new Error("computeDelta requires END to extend START");
    }

    // end == start (no new entries): nothing differs, schema included.
    if (fork.forkB.size === 0) {
        return new RTableGroupDelta(start, end, fork.commonFrontier, emptyChanges(group.getType()));
    }

    let revisionBound: Version;
    if (strategy === 'full') {
        revisionBound = version();   // empty bound: walk the whole history of `end`
    } else if (strategy === 'bounded') {
        const meet = await computeForkMeet(rawDag, fork.common);
        const observed = await group.getObservedObjects();
        // referenced objects participate in bound projection via their own
        // bounded delta (mirrors RSet -> RCap); the schema has no strategy switch.
        for (const obj of observed) {
            (obj as unknown as { setDeltaStrategy?: (s: RTableGroupDeltaStrategy) => void })
                .setDeltaStrategy?.('bounded');
        }
        revisionBound = await combineObserverRevisionBounds(group, meet, end, observed);
    } else {
        throw new Error("Invalid delta strategy: " + strategy);
    }

    const entries = await walkEntriesBackwardsToBound(rawDag, end, revisionBound);
    const acc = new RTableGroupDeltaAccumulator(group, start, end);
    for (const entry of entries) {
        await acc.ingest(entry);
    }
    const root = await acc.finalize() as DeltaChanges<RTableGroupChanges>;
    root.changes.opVerdictChanges = await computeOpVerdictFlips(group, entries, start, end);
    return new RTableGroupDelta(start, end, revisionBound, root);
}
