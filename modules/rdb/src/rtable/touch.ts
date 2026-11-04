// The touch rule: reading a bound group while some of the observer's observes
// of it are pending (their verdicts are being solved in the observer's
// component).
//
// A view of the bound group reads at D, the version its live observes give,
// and each pending observe o would add its published version V_o. A pending
// observe TOUCHES a read when adding V_o to D changes something the read
// depends on:
//
//   - a slot the read uses: a schema entry that D ∪ V_o makes visible writes
//     it (compared structurally, through the migration rules' slots, never
//     through resolved values, which do not add up over observes);
//   - the entries one of its DAG queries matches, among those live in the
//     bound group: a history query (findCoverWithFilter at the version) is
//     touched when its cover at D ∪ V_o differs from its cover at D; a
//     concurrent query (findConcurrentCoverWithFilter from `from`) when its
//     concurrent cover differs, or the history cover of the same filter does
//     (a concurrent match can enter the history below one that stays
//     concurrent and leave the concurrent cover unchanged).
//
// A read no pending observe touches has its answer at D in every completion,
// so it is decided. A touched read is undecided, supported by the touching
// observes' verdicts. Covers are compared, not answers: two imports that each
// leave an answer alone can change it together, while matched entries add up.
// Comparing covers relies on the contract of every DAG index (dag.ts): a
// cover is exactly the topmost entries of its filtered set.
//
// Shortcut: when V_o is in the history of D, D ∪ V_o has D's history and o
// touches nothing.
//
// The bound group's entries never read the observer, so their verdicts are
// complete before the observer's read returns, and the liveness predicate is
// two-valued here: an undecided verdict of the bound group is a broken
// invariant and throws.
//
// Comparisons are memoized per computation, with the evaluation (the memo
// dies with it, like the evaluation's memo of verdicts): each depends only on
// the table, the filter or slot, and the canonical (D, V_o, from).

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { EntryMetaFilter, Position } from "@hyper-hyper-space/hhs3_dag";
import { anyTruth, refVersionAtOrAbove } from "@hyper-hyper-space/hhs3_mvt";
import type { CausalDag, PendingRefAdvance, ScopedDag, Undecided, Version } from "@hyper-hyper-space/hhs3_mvt";

// The slots and DAG queries of one read. Filters are built from D's schema:
// a read checks its slots first, since untouched slots fix the incarnations
// the filters carry.
export type ReadQueries = {
    slots: string[];
    history: EntryMetaFilter[];
    concurrent: EntryMetaFilter[];
};

// What the touch test needs from the bound group's table.
export type TouchTarget = {
    getId(): B64Hash;
    getGroupId(): B64Hash;
    getScopedDag(): Promise<ScopedDag>;
    getCausalDag(): Promise<CausalDag>;
    // The slots written by the schema entries visible at (wider, from) and not
    // at (at, from).
    schemaSlotsBetween(at: Version, wider: Version, from: Version): Promise<Set<string>>;
};

type Memo = Map<string, unknown>;

const memos = new WeakMap<object, Memo>();

function memoFor(scope: object): Memo {
    let memo = memos.get(scope);
    if (memo === undefined) {
        memo = new Map();
        memos.set(scope, memo);
    }
    return memo;
}

function key(p: Position): string {
    return [...p].sort().join(',');
}

function sameSet(a: Position, b: Position): boolean {
    if (a.size !== b.size) return false;
    for (const h of a) if (!b.has(h)) return false;
    return true;
}

export class TouchTest {
    private readonly memo: Memo;

    // `scope` is the computation's evaluation. `live` is the bound group's
    // liveness at `from`, the view's own predicate.
    constructor(
        private readonly target: TouchTarget,
        private readonly at: Version,
        private readonly from: Version,
        private readonly pending: readonly PendingRefAdvance[],
        scope: object,
        private readonly live: (hash: B64Hash) => Promise<boolean>,
    ) {
        this.memo = memoFor(scope);
    }

    // The canonical version every completion's candidates are gathered at:
    // D plus every pending version.
    async gatherVersion(): Promise<Version> {
        const all = new Set<B64Hash>(this.at) as Version;
        for (const o of this.pending) for (const h of o.version) all.add(h);
        return (await this.target.getCausalDag()).findMinimalCover(all) as Promise<Version>;
    }

    // The undecided answer of a read with these slots and queries, supported
    // by the touching observes, or undefined when none touches it.
    async touched(queries: ReadQueries): Promise<Undecided | undefined> {
        const touching: Undecided[] = [];
        for (const o of this.pending) {
            if (await this.touches(o, queries)) touching.push(o.verdict);
        }
        return touching.length === 0 ? undefined : anyTruth(touching) as Undecided;
    }

    async slotsTouched(slots: string[]): Promise<Undecided | undefined> {
        return this.touched({ slots, history: [], concurrent: [] });
    }

    private async touches(o: PendingRefAdvance, queries: ReadQueries): Promise<boolean> {
        const wider = await this.widen(o);
        if (wider === undefined) return false;
        if (queries.slots.length > 0) {
            const written = await this.slotsBetween(wider);
            if (queries.slots.some((slot) => written.has(slot))) return true;
        }
        for (const filter of queries.history) {
            if (!sameSet(await this.cover('history', filter, this.at), await this.cover('history', filter, wider))) return true;
        }
        for (const filter of queries.concurrent) {
            if (!sameSet(await this.cover('concurrent', filter, this.at), await this.cover('concurrent', filter, wider))) return true;
            if (!sameSet(await this.cover('history', filter, this.at), await this.cover('history', filter, wider))) return true;
        }
        return false;
    }

    // D ∪ V_o, canonical; undefined when V_o is in the history of D.
    private async widen(o: PendingRefAdvance): Promise<Version | undefined> {
        const memoKey = 'widen|' + this.target.getGroupId() + '|' + key(this.at) + '|' + key(o.version);
        if (this.memo.has(memoKey)) return this.memo.get(memoKey) as Version | undefined;
        const dag = await this.target.getCausalDag();
        let wider: Version | undefined;
        if (!await refVersionAtOrAbove(dag, this.at, o.version)) {
            wider = await dag.findMinimalCover(new Set<B64Hash>([...this.at, ...o.version])) as Version;
        }
        this.memo.set(memoKey, wider);
        return wider;
    }

    private async slotsBetween(wider: Version): Promise<Set<string>> {
        const memoKey = 'slots|' + this.target.getGroupId() + '|' + key(this.at) + '|' + key(wider) + '|' + key(this.from);
        const cached = this.memo.get(memoKey) as Set<string> | undefined;
        if (cached !== undefined) return cached;
        const slots = await this.target.schemaSlotsBetween(this.at, wider, this.from);
        this.memo.set(memoKey, slots);
        return slots;
    }

    private async cover(kind: 'history' | 'concurrent', filter: EntryMetaFilter, at: Version): Promise<Position> {
        const memoKey = kind + '|' + this.target.getId() + '|' + JSON.stringify(filter) + '|' + key(at) + '|' + key(this.from);
        const cached = this.memo.get(memoKey) as Position | undefined;
        if (cached !== undefined) return cached;
        const dag = await this.target.getScopedDag();
        const predicate = (hash: B64Hash) => this.live(hash);
        const result = kind === 'history'
            ? await dag.findCoverWithFilter(at, filter, predicate)
            : await dag.findConcurrentCoverWithFilter(this.from, at, filter, predicate);
        this.memo.set(memoKey, result);
        return result;
    }
}
