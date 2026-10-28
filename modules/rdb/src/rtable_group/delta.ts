// RTableGroup delta: the group leads a three-channel delta between two of its
// positions, mirroring RSet's full/bounded split but over MORE than one
// observed object (its RSchema plus every bound foreign group).
//
// Three channels:
//   - schema sub-delta (uniform): `schemaChanges` carries column defaults,
//     table/column drops and re-adds, FK/restriction/concurrentDeletes flips
//     as RSchema changes. The consumer applies these table-wide; they are
//     never enumerated per row (the row channel compares each row within the
//     end horizon's incarnations, so a row affected only by a default, a drop
//     or a reset produces no RowChange, walked or not).
//   - per-table row-walk (positional): each touched member table's RTableChanges
//     in the root delta's `nested` map, keyed by the table id. A row appears
//     only when its liveness flipped or a written value moved (see ../rtable/delta.ts).
//     A schema deploy that deletes the rows stranded by an FK it adopts is
//     walked like a delete: its `tables` meta routes it to those tables.
//   - op verdict flips: `opVerdictChanges` lists group DAG entries whose
//     at-use void verdict changed between the start and end view horizons
//     (insert/update/delete/bundle ops and gated observes that flip). Row
//     channel materializes effects; op channel explains reconciliation flips.
//
// Strategies (RObject.setDeltaStrategy switch on the group):
//   - full: bound = empty, walk all history from genesis. The reference
//     implementation (slow, obviously correct), used for parity.
//   - bounded: one backward walk from `end` that computes the bound as it
//     goes (INCREMENTAL_DELTA_BOUND.md), through mvt's walkRevisionBound.
//     The bound starts at the fork meet, drops below every observe in its
//     history whose published version is above the observed group's own
//     bound, and then drops below every walked barrier of `start` that is
//     concurrent with it and whose effect changed between `start` and `end`:
//     its verdict flipped, it observes a version above the observed group's
//     bound, or it deletes rows of a table whose concurrentDeletes flag at its
//     own position moved. Below the final bound every verdict and every row
//     whose operations all lie there is the same at both horizons. The walk
//     stops at the exit border of the bound's history and resumes from it when
//     the bound drops, so no entry is loaded or evaluated twice.
//
// The walk owns the geometry and the observe stability check. This module
// gives it the group's references and a visitor that feeds the row channel,
// records verdict flips, and tells it whether a barrier's verdict or honor
// flag changed.
//
// The schema is declared to the walk as read at (v, v): resolveSchemaView reads
// the schema at the version a position folds, deploys are never void, and the
// fold is geometric, so an entry below the bound reads the same schema at both
// horizons. A bound foreign group is read at the version this group observes at
// the horizon, and its bound is taken between the versions observed at `start`
// and at `end`, through the foreign group's computeRevisionBound.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { dag } from "@hyper-hyper-space/hhs3_dag";
import {
    RObject, Version, version, Delta, DeltaChanges, DeltaAccumulator,
    walkEntriesBackwardsToBound, findRefAdvances,
    requireExtendingFork, walkRevisionBound,
} from "@hyper-hyper-space/hhs3_mvt";
import type {
    Reference, ReferencedObject, RevisionBoundHost, RevisionBoundVisitor,
} from "@hyper-hyper-space/hhs3_mvt";

import type { RSchema, RSchemaView } from "../rschema/interfaces.js";
import type { RSchemaChanges } from "../rschema/delta.js";
import type { RTable } from "../rtable/interfaces.js";
import type { RTableChanges } from "../rtable/delta.js";

import {
    computeOpVerdictFlips, describeOpVerdictFlip, isVoidCheckable,
    type OpVerdictChange, type OpVoidDetail,
} from "./op_delta.js";

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

// A bound foreign group, as the bounded computation needs it: its own bound
// between two of its versions, and its causal DAG to compare versions in.
export type BoundGroupHost = ReferencedObject;

// What the accumulator / computation needs from its group (RTableGroupImpl).
export type GroupDeltaHost = RObject & {
    resolveSchemaVersion(at: Version, from?: Version): Promise<Version>;
    resolveSchemaView(at: Version, from?: Version): Promise<RSchemaView>;
    // The canonical version of bound group `groupId` this group observes at
    // (at, from): the version its foreign views read.
    resolveObservedForeignVersion(groupId: B64Hash, at: Version, from: Version): Promise<Version>;
    getSchemaObject(): Promise<RSchema>;
    makeTable(name: string): RTable;
    getBoundGroup(groupId: B64Hash): Promise<BoundGroupHost>;
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
    // them). Ref-advance entries carry none and route nowhere, except a schema
    // deploy that deletes rows for an FK it adopts: it lists the tables it
    // deletes from (see deriveDeployKillMeta in scopes.ts). The schema
    // sub-delta is computed wholesale in finalize, so deploy / observe entries
    // need no other per-entry routing here.
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
    const fork = await requireExtendingFork(rawDag, start, end, "computeDelta");

    // end == start (no new entries): nothing differs, schema included.
    if (fork.forkB.size === 0) {
        return new RTableGroupDelta(start, end, fork.commonFrontier, emptyChanges(group.getType()));
    }

    const acc = new RTableGroupDeltaAccumulator(group, start, end);

    if (strategy === 'full') {
        // empty bound: walk the whole history of `end`
        const entries = await walkEntriesBackwardsToBound(rawDag, end, version());
        for (const entry of entries) {
            await acc.ingest(entry);
        }
        const root = await acc.finalize() as DeltaChanges<RTableGroupChanges>;
        root.changes.opVerdictChanges = await computeOpVerdictFlips(group, entries, start, end);
        return new RTableGroupDelta(start, end, version(), root);
    }

    if (strategy !== 'bounded') {
        throw new Error("Invalid delta strategy: " + strategy);
    }

    const flips: VerdictFlip[] = [];
    const visitor = deltaVisitor(group, acc, start, end, await hasNewDeploy(group, start, end), flips);
    const bound = await walkRevisionBound(rawDag, revisionBoundHost(group), visitor, fork, start, end);

    const opVerdictChanges: OpVerdictChange[] = [];
    for (const flip of flips) {
        opVerdictChanges.push(await describeOpVerdictFlip(group, flip.entry, flip.voidBefore, flip.voidAfter, start, end));
    }
    opVerdictChanges.sort((a, b) => a.entry.localeCompare(b.entry));

    const root = await acc.finalize() as DeltaChanges<RTableGroupChanges>;
    root.changes.opVerdictChanges = opVerdictChanges;
    return new RTableGroupDelta(start, end, bound, root);
}

// The bounded revision bound between two versions of `group`, without the
// delta: the same walk with no row channel, evaluating verdicts only on the
// barriers that can lower the bound. An observer of `group` uses it to tell a
// stable observation from an unstable one.
export async function computeRTableGroupRevisionBound(
    group: GroupDeltaHost, rawDag: dag.Dag, start: Version, end: Version,
): Promise<Version> {
    const fork = await requireExtendingFork(rawDag, start, end, "computeRevisionBound");
    if (fork.forkB.size === 0) return fork.commonFrontier;
    const visitor = boundVisitor(group, start, end, await hasNewDeploy(group, start, end));
    return walkRevisionBound(rawDag, revisionBoundHost(group), visitor, fork, start, end);
}

// The group's references, as the walk reads them: each bound group at the
// version this group observes at the horizon, and the schema at (v, v).
function revisionBoundHost(group: GroupDeltaHost): RevisionBoundHost {
    return {
        getScopedDag: () => group.getScopedDag(),
        references: (): Reference[] => [
            ...[...new Set(Object.values(group.getBindings()))].map((groupId): Reference => ({
                refId: groupId,
                from: 'horizon',
                getObject: () => group.getBoundGroup(groupId),
                resolveVersion: (horizon) => group.resolveObservedForeignVersion(groupId, horizon, horizon),
            })),
            { refId: group.getSchemaRef(), from: 'at' },
        ],
    };
}

// Whether H(end) \ H(start) holds a schema deploy. Without one, no barrier's
// concurrentDeletes flag at its own position can move.
async function hasNewDeploy(group: GroupDeltaHost, start: Version, end: Version): Promise<boolean> {
    const scopedDag = await group.getScopedDag();
    const schemaRef = group.getSchemaRef();
    return !sameSet(
        await findRefAdvances(scopedDag, schemaRef, start),
        await findRefAdvances(scopedDag, schemaRef, end),
    );
}

type VerdictFlip = { entry: dag.Entry; voidBefore: boolean; voidAfter: boolean };

// Feeds every walked entry to the row channel and records every verdict flip.
// A candidate's effect changed when its verdict flipped or its honor flag did.
function deltaVisitor(
    group: GroupDeltaHost, acc: RTableGroupDeltaAccumulator, start: Version, end: Version,
    newDeploy: boolean, flips: VerdictFlip[],
): RevisionBoundVisitor {
    return {
        async visit(entry, candidate) {
            await acc.ingest(entry);
            let flipped = false;
            if (isVoidCheckable(entry.payload)) {
                const voidBefore = await group.isEntryVoided(entry.hash, start);
                const voidAfter = await group.isEntryVoided(entry.hash, end);
                if (voidBefore !== voidAfter) {
                    flips.push({ entry, voidBefore, voidAfter });
                    flipped = true;
                }
            }
            return candidate && (flipped || (newDeploy && await didDeleteHonorFlip(group, entry, start, end)));
        },
    };
}

// Reads verdicts only on the barriers that can lower the bound.
function boundVisitor(group: GroupDeltaHost, start: Version, end: Version, newDeploy: boolean): RevisionBoundVisitor {
    return {
        async visit(entry, candidate) {
            if (!candidate) return false;
            if (isVoidCheckable(entry.payload)
                && await group.isEntryVoided(entry.hash, start) !== await group.isEntryVoided(entry.hash, end)) {
                return true;
            }
            return newDeploy && await didDeleteHonorFlip(group, entry, start, end);
        },
    };
}

// The tables a barrier deletes rows of: a row delete's table, every table a
// bundle touches (a superset), or the tables a schema deploy deletes the rows
// stranded by an FK it adopts from. Observes carry no `tables` tag.
function deletedTables(entry: dag.Entry): string[] {
    const tables = entry.meta['tables'];
    return tables === undefined ? [] : [...json.fromSet(tables)];
}

// Whether a concurrent schema deploy changed, between the horizons, the
// concurrentDeletes flag at `entry`'s own position for a table it deletes
// from: that decides whether its deletes reach concurrent branches.
async function didDeleteHonorFlip(group: GroupDeltaHost, entry: dag.Entry, start: Version, end: Version): Promise<boolean> {
    const tables = deletedTables(entry);
    if (tables.length === 0) return false;
    const at = version(entry.hash);
    const schemaStart = await group.resolveSchemaView(at, start);
    const schemaEnd = await group.resolveSchemaView(at, end);
    for (const table of tables) {
        const before = schemaStart.hasTable(table) && schemaStart.getConcurrentDeletes(table);
        const after = schemaEnd.hasTable(table) && schemaEnd.getConcurrentDeletes(table);
        if (before !== after) return true;
    }
    return false;
}

function sameSet(a: ReadonlySet<B64Hash>, b: ReadonlySet<B64Hash>): boolean {
    if (a.size !== b.size) return false;
    for (const h of a) if (!b.has(h)) return false;
    return true;
}
