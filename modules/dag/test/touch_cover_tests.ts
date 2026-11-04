// Comparing filtered covers at two versions, checked by brute force.
//
// Let X' be a version whose history contains that of X. Write H(X) for the
// entries that match a filter, pass a predicate and are in the history of X,
// and C(X) for those that match, pass, are in the history of `from`, and are
// concurrent with X (outside its history and not after every element of it).
// A cover query with the predicate returns the topmost entries of H(X), and a
// concurrent cover query the topmost entries of C(X). The tests check:
//
//   - that contract itself, for every index: each cover holds every topmost
//     entry of its set and nothing else, including a live match that the
//     predicate exposes below a rejected one;
//   - the covers at X and X' are equal exactly when H(X) = H(X');
//   - when they are, the concurrent covers are equal exactly when C(X) = C(X');
//   - without the first comparison, equal concurrent covers can hide a change:
//     a concurrent match can enter the history below one that does not;
//   - changes add up: if neither Y1 nor Y2 changes the covers of X on its own,
//     X + Y1 + Y2 does not change them either;
//   - a change caused by Y2 against X + Y1 is also a change against X.

import { dag, EntryMetaFilter, EntryPredicate, MetaProps, Position, position, checkFilter } from "../src/index.js";
import { B64Hash, sha256 } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { set } from "@hyper-hyper-space/hhs3_util";
import { assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

class Rng {
    private s: number;
    constructor(seed: number) { this.s = seed >>> 0; }
    next(): number {
        let t = (this.s += 0x6D2B79F5) >>> 0;
        t = Math.imul(t ^ (t >>> 15), t | 1);
        t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
        return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    }
    int(lo: number, hi: number): number { return lo + Math.floor(this.next() * (hi - lo + 1)); }
}

type Fixture = {
    d: dag.Dag;
    hashes: B64Hash[];
    // each entry's history, itself included
    anc: Map<B64Hash, Set<B64Hash>>;
    meta: Map<B64Hash, MetaProps>;
};

function flatDag(): dag.Dag {
    const store = new dag.store.MemDagStorage();
    return dag.create(store, dag.idx.flat.createFlatIndex(store, new dag.idx.flat.mem.MemFlatIndexStore()), sha256);
}

function topoDag(): dag.Dag {
    const store = new dag.store.MemDagStorage();
    return dag.create(store, dag.idx.topo.createDagTopoIndex(store, new dag.idx.topo.mem.MemTopoIndexStore()), sha256);
}

function levelDag(): dag.Dag {
    const store = new dag.store.MemDagStorage();
    return dag.create(store, dag.idx.level.createDagLevelIndex(store, new dag.idx.level.mem.MemLevelIndexStore({ levelFactor: 8 })), sha256);
}

const TAGS = ['a', 'b', 'c'];

function minimalPreds(preds: Set<B64Hash>, anc: Map<B64Hash, Set<B64Hash>>): Set<B64Hash> {
    const result = new Set<B64Hash>();
    for (const p of preds) {
        let dominated = false;
        for (const q of preds) {
            if (q !== p && anc.get(q)!.has(p)) { dominated = true; break; }
        }
        if (!dominated) result.add(p);
    }
    return result;
}

async function append(fx: Fixture, payload: json.Literal, meta: MetaProps, preds: Set<B64Hash>): Promise<B64Hash> {
    const minimal = minimalPreds(preds, fx.anc);
    const h = await fx.d.append(payload, meta, minimal.size > 0 ? minimal : undefined);
    const a = new Set<B64Hash>([h]);
    for (const p of minimal) for (const x of fx.anc.get(p)!) a.add(x);
    fx.anc.set(h, a);
    fx.meta.set(h, meta);
    fx.hashes.push(h);
    return h;
}

async function buildRandom(d: dag.Dag, seed: number, size: number): Promise<Fixture> {
    const rng = new Rng(seed);
    const fx: Fixture = { d, hashes: [], anc: new Map(), meta: new Map() };
    for (let i = 0; i < size; i++) {
        const preds = new Set<B64Hash>();
        if (fx.hashes.length > 0) {
            const fanout = rng.int(1, 3);
            for (let j = 0; j < fanout; j++) {
                const n = fx.hashes.length;
                const k = rng.next() < 0.7 ? rng.int(Math.max(0, n - 6), n - 1) : rng.int(0, n - 1);
                preds.add(fx.hashes[k]!);
            }
        }
        const meta: MetaProps = {};
        if (rng.next() < 0.6) meta['t'] = json.toSet(new Set([TAGS[rng.int(0, TAGS.length - 1)]!]));
        await append(fx, { i, n: rng.int(0, 1000000000) }, meta, preds);
    }
    return fx;
}

function historyOf(fx: Fixture, p: Position): Set<B64Hash> {
    const h = new Set<B64Hash>();
    for (const x of p) for (const y of fx.anc.get(x)!) h.add(y);
    return h;
}

// Outside the history of `p` and with every element of `p` in its own history.
function isAfter(fx: Fixture, e: B64Hash, p: Position, hp: Set<B64Hash>): boolean {
    if (hp.has(e)) return false;
    const ae = fx.anc.get(e)!;
    for (const x of p) if (!ae.has(x)) return false;
    return true;
}

type Live = (h: B64Hash) => boolean;

const liveByHash: Live = (h) => (h.charCodeAt(5) + h.charCodeAt(9)) % 3 !== 0;
const allLive: Live = () => true;

function historySet(fx: Fixture, p: Position, filter: EntryMetaFilter, live: Live): Set<B64Hash> {
    const result = new Set<B64Hash>();
    for (const e of historyOf(fx, p)) {
        if (checkFilter(fx.meta.get(e)!, filter) && live(e)) result.add(e);
    }
    return result;
}

function concurrentSet(fx: Fixture, from: Position, p: Position, filter: EntryMetaFilter, live: Live): Set<B64Hash> {
    const hp = historyOf(fx, p);
    const result = new Set<B64Hash>();
    for (const e of historyOf(fx, from)) {
        if (hp.has(e) || isAfter(fx, e, p, hp)) continue;
        if (checkFilter(fx.meta.get(e)!, filter) && live(e)) result.add(e);
    }
    return result;
}

// The members of `s` that no other member has in its history.
function topmost(fx: Fixture, s: Set<B64Hash>): Set<B64Hash> {
    const result = new Set<B64Hash>();
    for (const m of s) {
        let dominated = false;
        for (const o of s) {
            if (o !== m && fx.anc.get(o)!.has(m)) { dominated = true; break; }
        }
        if (!dominated) result.add(m);
    }
    return result;
}

// Whether some member of `exposed` lies below a member of `all` outside `kept`:
// a match the predicate rejected, which the cover had to see through.
function seesThrough(fx: Fixture, exposed: Set<B64Hash>, all: Set<B64Hash>, kept: Set<B64Hash>): boolean {
    for (const m of exposed) {
        for (const o of all) {
            if (!kept.has(o) && fx.anc.get(o)!.has(m)) return true;
        }
    }
    return false;
}

const FILTERS: EntryMetaFilter[] = [
    { containsValues: { t: ['a'] } },
    { containsValues: { t: ['b'] } },
    { containsKeys: ['t'] },
];

const LIVES: Array<[Live, EntryPredicate | undefined]> = [
    [allLive, undefined],
    [liveByHash, async (h) => liveByHash(h)],
];

function union(...ps: Position[]): Position {
    const result = position();
    for (const p of ps) for (const x of p) result.add(x);
    return result;
}

function randomPosition(fx: Fixture, rng: Rng): Position {
    const p = position();
    const k = rng.int(1, 3);
    for (let i = 0; i < k; i++) p.add(fx.hashes[rng.int(0, fx.hashes.length - 1)]!);
    return p;
}

type Covers = { history: Position; concurrent: Position };

async function covers(fx: Fixture, from: Position, p: Position, filter: EntryMetaFilter, predicate: EntryPredicate | undefined): Promise<Covers> {
    return {
        history: await fx.d.findCoverWithFilter(p, filter, predicate),
        concurrent: await fx.d.findConcurrentCoverWithFilter(from, p, filter, predicate),
    };
}

function changed(a: Covers, b: Covers): boolean {
    return !set.eq(a.history, b.history) || !set.eq(a.concurrent, b.concurrent);
}

type Counts = { trials: number; historyChanged: number; concurrentOnly: number; gapHidden: number; unchanged: number; addedUp: number };

async function checkFixture(fx: Fixture, seed: number, trials: number, counts: Counts): Promise<void> {
    const rng = new Rng(seed);
    const from = await fx.d.getFrontier();

    for (let t = 0; t < trials; t++) {
        const x = randomPosition(fx, rng);
        const y1 = randomPosition(fx, rng);
        const y2 = randomPosition(fx, rng);
        const x1 = union(x, y1);
        const x2 = union(x, y2);
        const x12 = union(x, y1, y2);

        for (const filter of FILTERS) {
            for (const [live, predicate] of LIVES) {
                counts.trials++;
                const at = {
                    x: await covers(fx, from, x, filter, predicate),
                    x1: await covers(fx, from, x1, filter, predicate),
                    x2: await covers(fx, from, x2, filter, predicate),
                    x12: await covers(fx, from, x12, filter, predicate),
                };
                const sets = (p: Position) => ({
                    history: historySet(fx, p, filter, live),
                    concurrent: concurrentSet(fx, from, p, filter, live),
                });
                const sx = sets(x), sx1 = sets(x1), sx2 = sets(x2), sx12 = sets(x12);

                for (const [cx, cy, s0, s1] of [[at.x, at.x1, sx, sx1], [at.x, at.x2, sx, sx2]] as const) {
                    const historyEqual = set.eq(s0.history, s1.history);
                    assertTrue(set.eq(cx.history, cy.history) === historyEqual,
                        'history covers are equal exactly when the filtered histories are');
                    if (historyEqual) {
                        assertTrue(set.eq(cx.concurrent, cy.concurrent) === set.eq(s0.concurrent, s1.concurrent),
                            'with equal histories, concurrent covers are equal exactly when the concurrent sets are');
                        if (set.eq(s0.concurrent, s1.concurrent)) counts.unchanged++;
                        else counts.concurrentOnly++;
                    } else {
                        counts.historyChanged++;
                        if (set.eq(cx.concurrent, cy.concurrent) && !set.eq(s0.concurrent, s1.concurrent)) counts.gapHidden++;
                    }
                }

                // Changes add up.
                if (!changed(at.x, at.x1) && !changed(at.x, at.x2)) {
                    counts.addedUp++;
                    assertTrue(set.eq(sx12.history, sx.history) && set.eq(sx12.concurrent, sx.concurrent),
                        'two additions that change nothing alone change nothing together');
                    assertTrue(!changed(at.x, at.x12), 'and the covers agree');
                }

                // A change against the larger base is a change against the smaller one.
                if (changed(at.x1, at.x12)) {
                    assertTrue(changed(at.x, at.x2), 'a change against X + Y1 is a change against X');
                }
            }
        }
    }
}

const touchCoverSuite = {
    title: '[TOUCH] Comparing filtered covers at two versions',
    tests: [
        {
            name: '[TOUCH_01] a concurrent match entering the history below one that stays concurrent leaves the concurrent cover unchanged',
            invoke: async () => {
                for (const d of [flatDag(), topoDag(), levelDag()]) {
                    const fx: Fixture = { d, hashes: [], anc: new Map(), meta: new Map() };
                    const tagged: MetaProps = { t: json.toSet(new Set(['a'])) };
                    const g = await append(fx, { g: 1 }, {}, new Set());
                    const l = await append(fx, { l: 1 }, tagged, new Set([g]));
                    const m = await append(fx, { m: 1 }, tagged, new Set([l]));
                    const x = await append(fx, { x: 1 }, {}, new Set([g]));
                    const from = position(m, x);
                    const filter: EntryMetaFilter = { containsValues: { t: ['a'] } };

                    const before = await covers(fx, from, position(x), filter, undefined);
                    const after = await covers(fx, from, position(x, l), filter, undefined);
                    assertTrue(set.eq(before.concurrent, position(m)) && set.eq(after.concurrent, position(m)),
                        'the concurrent cover is {m} at both versions');
                    assertTrue(!set.eq(concurrentSet(fx, from, position(x), filter, allLive), concurrentSet(fx, from, position(x, l), filter, allLive)),
                        'but l left the concurrent set');
                    assertTrue(!set.eq(before.history, after.history), 'and the history cover shows it');
                }
            }
        },
        {
            name: '[TOUCH_02] comparing covers detects exactly the changes to the filtered sets, and the changes add up (random DAGs, every index)',
            invoke: async () => {
                const counts: Counts = { trials: 0, historyChanged: 0, concurrentOnly: 0, gapHidden: 0, unchanged: 0, addedUp: 0 };
                for (const seed of [11, 1771, 20261103]) {
                    for (const make of [flatDag, topoDag, levelDag]) {
                        const fx = await buildRandom(make(), seed, 36);
                        await checkFixture(fx, seed * 7 + 3, 60, counts);
                    }
                }
                console.log(`      ${JSON.stringify(counts)}`);
                assertTrue(counts.historyChanged > 0 && counts.concurrentOnly > 0 && counts.unchanged > 0 && counts.addedUp > 0,
                    'every kind of case occurred');
            }
        },
        {
            name: '[TOUCH_03] with or without a predicate, each cover is exactly the topmost entries of its filtered set (random DAGs, every index)',
            invoke: async () => {
                const counts = { covers: 0, belowTopmost: 0, seenThrough: 0 };
                for (const seed of [11, 1771, 20261103]) {
                    for (const make of [flatDag, topoDag, levelDag]) {
                        const fx = await buildRandom(make(), seed, 36);
                        const rng = new Rng(seed * 13 + 5);
                        const frontier = await fx.d.getFrontier();
                        for (let t = 0; t < 120; t++) {
                            const p = randomPosition(fx, rng);
                            const from = rng.next() < 0.5 ? frontier : union(p, randomPosition(fx, rng));
                            for (const filter of FILTERS) {
                                const all = {
                                    history: historySet(fx, p, filter, allLive),
                                    concurrent: concurrentSet(fx, from, p, filter, allLive),
                                };
                                for (const [live, predicate] of LIVES) {
                                    const c = await covers(fx, from, p, filter, predicate);
                                    const s = {
                                        history: historySet(fx, p, filter, live),
                                        concurrent: concurrentSet(fx, from, p, filter, live),
                                    };
                                    for (const kind of ['history', 'concurrent'] as const) {
                                        const expected = topmost(fx, s[kind]);
                                        assertTrue(set.eq(c[kind], expected), `the ${kind} cover is the topmost entries of its set`);
                                        counts.covers++;
                                        if (expected.size < s[kind].size) counts.belowTopmost++;
                                        if (seesThrough(fx, expected, all[kind], s[kind])) counts.seenThrough++;
                                    }
                                }
                            }
                        }
                    }
                }
                console.log(`      ${JSON.stringify(counts)}`);
                assertTrue(counts.belowTopmost > 0 && counts.seenThrough > 0,
                    'covers left out matches below the topmost, and saw through matches the predicate rejected');
            }
        },
    ]
};

export { touchCoverSuite };
