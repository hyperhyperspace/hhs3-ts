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
//     goes (INCREMENTAL_DELTA_BOUND.md). The bound starts
//     at the fork meet, drops below every observe in its history whose
//     published version is above the observed group's own bound, and then
//     drops below every walked barrier of `start` that is concurrent with it
//     and whose effect changed between `start` and `end`: its verdict
//     flipped, it observes a version above the observed group's bound, or it
//     deletes rows of a table whose concurrentDeletes flag at its own
//     position moved. Below the final bound every verdict and every row whose
//     operations all lie there is the same at both horizons. The walk stops
//     at the exit border of the bound's history and resumes from it when the
//     bound drops, so no entry is loaded or evaluated twice.
//
// The schema is not an observed object of the bound: schema deploys are never
// void and are folded geometrically, so an entry below the bound reads the same
// schema at both horizons. A bound foreign group's bound is taken between the
// versions this group observes at `start` and at `end` (the horizons its views
// read), through the foreign group's computeRevisionBound.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { dag, position } from "@hyper-hyper-space/hhs3_dag";
import type { EntryMetaFilter, ForkPosition } from "@hyper-hyper-space/hhs3_dag";
import {
    RObject, Version, version, Delta, DeltaChanges, DeltaAccumulator,
    computeForkMeet, walkEntriesBackwardsToBound, findRefAdvances,
    isRefAdvancePayload, extractRefVersion, refVersionAtOrBelow,
} from "@hyper-hyper-space/hhs3_mvt";
import type { CausalDag, RefAdvancePayload, ScopedDag } from "@hyper-hyper-space/hhs3_mvt";

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
export type BoundGroupHost = {
    computeRevisionBound(start: Version, end: Version): Promise<Version>;
    getCausalDag(): Promise<CausalDag>;
};

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
    const fork = await forkOf(rawDag, start, end, "computeDelta");

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

    const { bound, opVerdictChanges } = await walkWideningBound(group, rawDag, fork, start, end, acc);
    const root = await acc.finalize() as DeltaChanges<RTableGroupChanges>;
    opVerdictChanges.sort((a, b) => a.entry.localeCompare(b.entry));
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
    const fork = await forkOf(rawDag, start, end, "computeRevisionBound");
    if (fork.forkB.size === 0) return fork.commonFrontier;
    return (await walkWideningBound(group, rawDag, fork, start, end, undefined)).bound;
}

async function forkOf(rawDag: dag.Dag, start: Version, end: Version, caller: string): Promise<ForkPosition> {
    const fork = await rawDag.findForkPosition(start, end);
    if (fork.forkA.size > 0) {
        throw new Error(caller + " requires END to extend START");
    }
    return fork;
}

type ForeignBound = { dag: CausalDag; bound: Version };

// The bounded walk. With `acc`, every walked entry also feeds the row channel
// and every void-checkable one is checked for an op verdict flip. Without it,
// only the bound is computed, and verdicts are read only where they can lower
// it.
async function walkWideningBound(
    group: GroupDeltaHost, rawDag: dag.Dag, fork: ForkPosition,
    start: Version, end: Version, acc: RTableGroupDeltaAccumulator | undefined,
): Promise<{ bound: Version; opVerdictChanges: OpVerdictChange[] }> {
    const scopedDag = await group.getScopedDag();
    const meet = await computeForkMeet(rawDag, fork.common);

    // The meet, lowered below every observe in its history whose published
    // version is above the observed group's bound between the versions this
    // group observes at `start` and at `end`.
    let bound: Version = meet;
    const foreignBounds = new Map<B64Hash, ForeignBound>();
    for (const groupId of new Set(Object.values(group.getBindings()))) {
        if (bound.size === 0) break;
        const foreign = await group.getBoundGroup(groupId);
        const foreignBound: ForeignBound = {
            dag: await foreign.getCausalDag(),
            bound: await foreign.computeRevisionBound(
                await group.resolveObservedForeignVersion(groupId, start, start),
                await group.resolveObservedForeignVersion(groupId, end, end),
            ),
        };
        foreignBounds.set(groupId, foreignBound);
        bound = await lowerBelowUnstableObserves(rawDag, scopedDag, groupId, foreignBound, meet, bound);
    }

    let stopSet = await computeStopSet(rawDag, bound, end);
    // H(start) membership, exact for every walked entry: paths from `end`
    // enter H(start) at start, its frontier or fork.common, and stay there.
    const inStart = new Set<B64Hash>([...start, ...fork.commonFrontier, ...fork.common]);
    const visited = new Set<B64Hash>();
    const stoppedAtBound = new Set<B64Hash>();
    const queue: B64Hash[] = [...end];
    const opVerdictChanges: OpVerdictChange[] = [];

    // Without a new schema deploy, no barrier's concurrentDeletes flag at its
    // own position can move.
    const schemaRef = group.getSchemaRef();
    const deployCoverMoved = !sameSet(
        await findRefAdvances(scopedDag, schemaRef, start),
        await findRefAdvances(scopedDag, schemaRef, end),
    );

    while (queue.length > 0) {
        const hash = queue.shift()!;
        if (visited.has(hash)) continue;

        // The exit border of H(bound): do not walk into it, but remember the
        // entry in case the bound drops below it.
        if (stopSet.has(hash)) {
            stoppedAtBound.add(hash);
            continue;
        }
        visited.add(hash);

        const entry = await rawDag.loadEntry(hash);
        if (entry === undefined) continue;

        const entryInStart = inStart.has(hash);
        const preds = json.fromSet(entry.header.prevEntryHashes);
        if (entryInStart) {
            for (const prev of preds) inStart.add(prev);
        }

        if (acc !== undefined) {
            await acc.ingest(entry);
        }

        // A new entry is strictly after the meet, so only a barrier of
        // `start` can be concurrent with the bound.
        const canWiden = bound.size > 0 && entryInStart && isBarrierEntry(entry);
        let voidBefore = false;
        let voidAfter = false;
        if (isVoidCheckable(entry.payload) && (acc !== undefined || canWiden)) {
            voidBefore = await group.isEntryVoided(hash, start);
            voidAfter = await group.isEntryVoided(hash, end);
            if (acc !== undefined && voidBefore !== voidAfter) {
                opVerdictChanges.push(await describeOpVerdictFlip(group, entry, voidBefore, voidAfter, start, end));
            }
        }

        const triggered = canWiden && (
            voidBefore !== voidAfter
            || await isUnstableObserve(entry, foreignBounds)
            || (deployCoverMoved && await didDeleteHonorFlip(group, entry, start, end))
        );
        if (triggered) {
            const predPos = position(...preds);
            if (!await isStrictlyAfter(rawDag, predPos, bound)) {
                bound = await computeGLB(rawDag, bound, predPos);
                stopSet = await computeStopSet(rawDag, bound, end);
                // A stopped entry still inside the new history is on its new
                // border; one that left it is walked from here.
                for (const stopped of [...stoppedAtBound]) {
                    if (stopSet.has(stopped)) continue;
                    stoppedAtBound.delete(stopped);
                    queue.push(stopped);
                }
            }
        }

        for (const prev of preds) {
            if (!visited.has(prev)) queue.push(prev);
        }
    }

    return { bound, opVerdictChanges };
}

// Lower `bound` below each lowest observe of `groupId` in H(meet) whose
// published version is not at or below the foreign bound, so that observe is
// walked. The descent stops at the first stable observe on each branch:
// ref-advances are monotone, so everything below a stable one is stable.
async function lowerBelowUnstableObserves(
    rawDag: dag.Dag, scopedDag: ScopedDag, groupId: B64Hash, foreign: ForeignBound,
    meet: Version, bound: Version,
): Promise<Version> {
    const filter: EntryMetaFilter = { containsValues: { ref: [groupId] } };
    const stability = new Map<B64Hash, boolean>();
    const isStable = async (hash: B64Hash): Promise<boolean> => {
        let stable = stability.get(hash);
        if (stable === undefined) {
            const entry = await scopedDag.loadEntry(hash);
            stable = await refVersionAtOrBelow(foreign.dag, extractRefVersion(entry!.payload as RefAdvancePayload), foreign.bound);
            stability.set(hash, stable);
        }
        return stable;
    };

    const queue: B64Hash[] = [...await scopedDag.findCoverWithFilter(meet, filter)];
    const visited = new Set<B64Hash>();
    while (queue.length > 0) {
        const observe = queue.shift()!;
        if (visited.has(observe)) continue;
        visited.add(observe);
        if (await isStable(observe)) continue;

        const entry = await scopedDag.loadEntry(observe);
        const preds = position(...json.fromSet(entry!.header.prevEntryHashes));
        const unstableBelow: B64Hash[] = [];
        for (const below of await scopedDag.findCoverWithFilter(preds, filter)) {
            if (!await isStable(below)) unstableBelow.push(below);
        }
        if (unstableBelow.length > 0) {
            queue.push(...unstableBelow);
        } else {
            bound = await computeGLB(rawDag, bound, preds);
        }
    }
    return bound;
}

// The exit border of H(bound) within H(from): the entries of H(bound) that are
// heads of `from` or have a direct successor outside H(bound). Every backward
// path from `from` into H(bound) enters it there.
async function computeStopSet(rawDag: dag.Dag, bound: Version, from: Version): Promise<Set<B64Hash>> {
    if (bound.size === 0) return new Set();
    const fork = await rawDag.findForkPosition(bound, from);
    return new Set<B64Hash>([...fork.common, ...fork.commonFrontier, ...bound]);
}

// Deletes (in rows or bundles), schema deploys and observes.
function isBarrierEntry(entry: dag.Entry): boolean {
    return entry.meta['barrier'] !== undefined;
}

// Every head of `bound` is in the history of `preds`.
async function isStrictlyAfter(rawDag: dag.Dag, preds: Version, bound: Version): Promise<boolean> {
    if (bound.size === 0) return true;
    if (preds.size === 0) return false;
    return (await rawDag.findForkPosition(bound, preds)).forkA.size === 0;
}

async function computeGLB(rawDag: dag.Dag, a: Version, b: Version): Promise<Version> {
    if (a.size === 0 || b.size === 0) return version();
    return (await rawDag.findForkPosition(a, b)).commonFrontier;
}

async function isUnstableObserve(entry: dag.Entry, foreignBounds: Map<B64Hash, ForeignBound>): Promise<boolean> {
    if (!isRefAdvancePayload(entry.payload)) return false;
    const foreign = foreignBounds.get(entry.payload.refId);
    if (foreign === undefined) return false;   // a schema deploy, or no bound computed
    return !await refVersionAtOrBelow(foreign.dag, extractRefVersion(entry.payload), foreign.bound);
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
