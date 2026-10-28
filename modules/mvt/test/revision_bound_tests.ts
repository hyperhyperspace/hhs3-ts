// walkRevisionBound on hand-built histories (rdb/plans/MVT_INCREMENTAL_BOUND_WALK.md §5).
// The visitor is scripted: an entry's effect changed when it is in the given set,
// and every (entry, candidate) call is recorded. The objects behind 'horizon'
// references are stubs that record their calls.

import { testing } from '@hyper-hyper-space/hhs3_util';
import { B64Hash, sha256 } from '@hyper-hyper-space/hhs3_crypto';
import { dag } from '@hyper-hyper-space/hhs3_dag';
import type { MetaProps } from '@hyper-hyper-space/hhs3_dag';
import { json } from '@hyper-hyper-space/hhs3_json';

import { version, Version } from '../src/mvt.js';
import { RootScopedDag } from '../src/dag/dag_nesting.js';
import { createRefAdvancePayload, createRefAdvanceMeta } from '../src/refs.js';
import {
    requireExtendingFork, walkRevisionBound,
    type Reference, type RevisionBoundVisitor,
} from '../src/revision_bound.js';

function createTestDag(): dag.Dag {
    const store = new dag.store.MemDagStorage();
    const index = dag.idx.flat.createFlatIndex(
        store,
        new dag.idx.flat.mem.MemFlatIndexStore(),
    );
    return dag.create(store, index, sha256);
}

function isExactly(v: Version, ...hashes: B64Hash[]): boolean {
    return v.size === hashes.length && hashes.every((h) => v.has(h));
}

const BARRIER: MetaProps = { barrier: json.toSet(['t']) };

type Visit = { hash: B64Hash; candidate: boolean };
type ScriptedVisitor = RevisionBoundVisitor & { visits: Visit[] };

function scriptedVisitor(changed: Iterable<B64Hash>): ScriptedVisitor {
    const answers = new Set(changed);
    const visits: Visit[] = [];
    return {
        visits,
        visit: async (entry, candidate) => {
            visits.push({ hash: entry.hash, candidate });
            return answers.has(entry.hash);
        },
    };
}

function visitedHashes(visitor: ScriptedVisitor): B64Hash[] {
    return visitor.visits.map((v) => v.hash);
}

function candidates(visitor: ScriptedVisitor): B64Hash[] {
    return visitor.visits.filter((v) => v.candidate).map((v) => v.hash);
}

function sameMembers(a: B64Hash[], b: B64Hash[]): boolean {
    return a.length === b.length && b.every((h) => a.includes(h));
}

type StubCalls = { objects: number; horizons: Version[]; boundArgs: [Version, Version][] };

function newCalls(): StubCalls {
    return { objects: 0, horizons: [], boundArgs: [] };
}

function horizonReference(
    refId: B64Hash, causalDag: dag.Dag, bound: Version, resolve: (horizon: Version) => Version, calls: StubCalls,
): Reference {
    return {
        refId,
        from: 'horizon',
        getObject: async () => {
            calls.objects++;
            return {
                computeRevisionBound: async (s: Version, e: Version) => { calls.boundArgs.push([s, e]); return bound; },
                getCausalDag: async () => causalDag,
            };
        },
        resolveVersion: async (horizon: Version) => { calls.horizons.push(horizon); return resolve(horizon); },
    };
}

async function walk(
    raw: dag.Dag, scoped: RootScopedDag, references: Reference[], visitor: RevisionBoundVisitor,
    start: Version, end: Version,
): Promise<Version> {
    const fork = await requireExtendingFork(raw, start, end, 'walk');
    const host = { getScopedDag: async () => scoped, references: () => references };
    return walkRevisionBound(raw, host, visitor, fork, start, end);
}

async function assertThrows(f: () => Promise<unknown>, msg: string): Promise<void> {
    let threw = false;
    try { await f(); } catch { threw = true; }
    testing.assertTrue(threw, msg);
}

// The history of BOUND_SIMPLIFIED_SUMMARY.md, with a non-barrier T' after the
// barrier T on the side branch:
//
//   c - x - E - s1 - N
//        \
//         T - T'
//
// start = {s1, T'}, end = {N, T'}, and the meet is {s1}.
async function localFloorHistory() {
    const raw = createTestDag();
    const scoped = new RootScopedDag(raw);
    const c = await scoped.append({ op: 'c' }, {});
    const x = await scoped.append({ op: 'x' }, {}, version(c));
    const E = await scoped.append({ op: 'E' }, {}, version(x));
    const s1 = await scoped.append({ op: 's1' }, {}, version(E));
    const T = await scoped.append({ op: 'T' }, BARRIER, version(x));
    const T2 = await scoped.append({ op: "T'" }, {}, version(T));
    const N = await scoped.append({ op: 'N' }, {}, version(s1));
    return { raw, scoped, c, x, E, s1, T, T2, N, start: version(s1, T2), end: version(N, T2) };
}

async function testRequeue() {
    const h = await localFloorHistory();
    const visitor = scriptedVisitor([h.T]);
    const bound = await walk(h.raw, h.scoped, [], visitor, h.start, h.end);

    testing.assertTrue(isExactly(bound, h.x), "the bound drops to T's predecessor");
    const order = visitedHashes(visitor);
    testing.assertTrue(order.includes(h.s1) && order.includes(h.E), 's1 and E are walked');
    testing.assertTrue(order.indexOf(h.s1) > order.indexOf(h.T), 's1 is walked after T, through the requeue');
    testing.assertTrue(sameMembers(candidates(visitor), [h.T]), 'only T is a candidate');
}

async function testUnchanged() {
    const h = await localFloorHistory();
    const visitor = scriptedVisitor([]);
    const bound = await walk(h.raw, h.scoped, [], visitor, h.start, h.end);

    testing.assertTrue(isExactly(bound, h.s1), 'the bound stays at the meet');
    testing.assertTrue(sameMembers(visitedHashes(visitor), [h.N, h.T2, h.T]), 'only N, T\' and T are walked');
    testing.assertTrue(sameMembers(candidates(visitor), [h.T]), 'a new entry and a non-barrier are never candidates');

    // The answer for an entry that is not a candidate is ignored.
    const ignored = scriptedVisitor([h.N, h.T2]);
    const ignoredBound = await walk(h.raw, h.scoped, [], ignored, h.start, h.end);
    testing.assertTrue(isExactly(ignoredBound, h.s1), 'answers for non-candidates do not lower the bound');
}

// c - r1 - r2 - m - n, where r1 publishes {f1} and r2 publishes {f2} of a
// referenced object f0 - f1 - f2 whose bound is {f1}. start = {m}, end = {n}.
async function testPhaseA() {
    const foreign = createTestDag();
    const f0 = await foreign.append({ f: 0 }, {});
    const f1 = await foreign.append({ f: 1 }, {}, version(f0));
    const f2 = await foreign.append({ f: 2 }, {}, version(f1));

    const raw = createTestDag();
    const scoped = new RootScopedDag(raw);
    const c = await scoped.append({ op: 'c' }, {});
    const r1 = await scoped.append(createRefAdvancePayload('ref', version(f1)), createRefAdvanceMeta('ref'), version(c));
    const r2 = await scoped.append(createRefAdvancePayload('ref', version(f2)), createRefAdvanceMeta('ref'), version(r1));
    const m = await scoped.append({ op: 'm' }, {}, version(r2));
    const n = await scoped.append({ op: 'n' }, {}, version(m));

    const calls = newCalls();
    const reference = horizonReference('ref', foreign, version(f1), () => version(f2), calls);
    const visitor = scriptedVisitor([]);
    const bound = await walk(raw, scoped, [reference], visitor, version(m), version(n));

    testing.assertTrue(isExactly(bound, r1), 'the bound drops to the predecessors of the lowest unstable ref-advance');
    testing.assertTrue(candidates(visitor).includes(r2), 'the unstable ref-advance is walked as a candidate');
    testing.assertFalse(visitedHashes(visitor).includes(r1), 'the stable ref-advance stops the descent and is not walked');
}

async function testReferencedVersions() {
    const h = await localFloorHistory();
    const foreign = createTestDag();
    const f0 = await foreign.append({ f: 0 }, {});
    const fs = await foreign.append({ f: 's' }, {}, version(f0));
    const fe = await foreign.append({ f: 'e' }, {}, version(fs));

    const sameAs = (a: Version, b: Version) => a.size === b.size && [...a].every((x) => b.has(x));
    const calls = newCalls();
    const reference = horizonReference(
        'ref', foreign, version(f0),
        (horizon) => sameAs(horizon, h.start) ? version(fs) : sameAs(horizon, h.end) ? version(fe) : version(f0),
        calls,
    );
    await walk(h.raw, h.scoped, [reference], scriptedVisitor([]), h.start, h.end);

    testing.assertEquals(calls.horizons.length, 2, 'the referenced version is resolved at two horizons');
    testing.assertTrue(sameAs(calls.horizons[0], h.start) && sameAs(calls.horizons[1], h.end),
        'the horizons are start and end, not the meet');
    testing.assertEquals(calls.boundArgs.length, 1, 'the referenced bound is computed once');
    testing.assertTrue(isExactly(calls.boundArgs[0][0], fs) && isExactly(calls.boundArgs[0][1], fe),
        'the referenced bound is taken between the versions observed at start and at end');
}

// c - x - E - s1 - N, with a ref-advance R after x publishing {f2} of a
// referenced object whose bound is {f1}. start = {s1, R}, end = {N, R}.
async function testPhaseB() {
    const foreign = createTestDag();
    const f0 = await foreign.append({ f: 0 }, {});
    const f1 = await foreign.append({ f: 1 }, {}, version(f0));
    const f2 = await foreign.append({ f: 2 }, {}, version(f1));

    const raw = createTestDag();
    const scoped = new RootScopedDag(raw);
    const c = await scoped.append({ op: 'c' }, {});
    const x = await scoped.append({ op: 'x' }, {}, version(c));
    const E = await scoped.append({ op: 'E' }, {}, version(x));
    const s1 = await scoped.append({ op: 's1' }, {}, version(E));
    const R = await scoped.append(createRefAdvancePayload('ref', version(f2)), createRefAdvanceMeta('ref'), version(x));
    const N = await scoped.append({ op: 'N' }, {}, version(s1));

    const reference = horizonReference('ref', foreign, version(f1), () => version(f2), newCalls());
    const visitor = scriptedVisitor([]);
    const bound = await walk(raw, scoped, [reference], visitor, version(s1, R), version(N, R));

    testing.assertTrue(isExactly(bound, x), "an unstable side-branch ref-advance lowers the bound to its predecessor");
    testing.assertTrue(candidates(visitor).includes(R), 'the ref-advance is a candidate');
    testing.assertTrue(visitedHashes(visitor).includes(E), 'the entry below the old meet is walked');
}

// c - x - s1 - N, and a barrier T after s1. start = {T}, end = {N, T}.
async function testStrictlyAfter() {
    const raw = createTestDag();
    const scoped = new RootScopedDag(raw);
    const c = await scoped.append({ op: 'c' }, {});
    const x = await scoped.append({ op: 'x' }, {}, version(c));
    const s1 = await scoped.append({ op: 's1' }, {}, version(x));
    const T = await scoped.append({ op: 'T' }, BARRIER, version(s1));
    const N = await scoped.append({ op: 'N' }, {}, version(s1));

    const visitor = scriptedVisitor([T]);
    const bound = await walk(raw, scoped, [], visitor, version(T), version(N, T));

    testing.assertTrue(candidates(visitor).includes(T), 'T is a candidate');
    testing.assertTrue(isExactly(bound, s1), 'a changed barrier strictly after the bound does not lower it');
}

//   c - a - x - E - s1 - N
//        \   \
//         \   T - T'
//          U - U' - U''
//
// start = {s1, T', U''}, end = {N, T', U''}, with T and U changed. T drops the
// bound to {x} and requeues s1; U drops it to {a} and requeues x.
async function testTwoDrops() {
    const raw = createTestDag();
    const scoped = new RootScopedDag(raw);
    const c = await scoped.append({ op: 'c' }, {});
    const a = await scoped.append({ op: 'a' }, {}, version(c));
    const x = await scoped.append({ op: 'x' }, {}, version(a));
    const E = await scoped.append({ op: 'E' }, {}, version(x));
    const s1 = await scoped.append({ op: 's1' }, {}, version(E));
    const T = await scoped.append({ op: 'T' }, BARRIER, version(x));
    const T2 = await scoped.append({ op: "T'" }, {}, version(T));
    const U = await scoped.append({ op: 'U' }, BARRIER, version(a));
    const U2 = await scoped.append({ op: "U'" }, {}, version(U));
    const U3 = await scoped.append({ op: "U''" }, {}, version(U2));
    const N = await scoped.append({ op: 'N' }, {}, version(s1));

    const visitor = scriptedVisitor([T, U]);
    const bound = await walk(raw, scoped, [], visitor, version(s1, T2, U3), version(N, T2, U3));

    testing.assertTrue(isExactly(bound, a), "the bound drops twice, to U's predecessor");
    const order = visitedHashes(visitor);
    testing.assertEquals(new Set(order).size, order.length, 'no entry is visited twice');
    testing.assertTrue(sameMembers(order, [N, T2, U3, T, U2, s1, U, E, x]), 'every entry above the bound is visited');
    testing.assertTrue(order.indexOf(s1) > order.indexOf(T), 's1 is requeued by the first drop');
    testing.assertTrue(order.indexOf(x) > order.indexOf(U), 'x is requeued by the second drop');
    testing.assertTrue(sameMembers(candidates(visitor), [T, U]), 'only the two barriers are candidates');

    // With an empty start the meet, and so the bound, is empty: nothing is a candidate.
    const empty = scriptedVisitor([T, U]);
    const emptyBound = await walk(raw, scoped, [], empty, version(), version(N, T2, U3));
    testing.assertEquals(emptyBound.size, 0, 'the bound is empty');
    testing.assertEquals(candidates(empty).length, 0, 'no candidate while the bound is empty');
}

// c - x - E - s1 - N, with a ref-advance D after x of a reference declared
// 'at'. start = {s1, D}, end = {N, D}.
async function atReferenceHistory() {
    const raw = createTestDag();
    const scoped = new RootScopedDag(raw);
    const c = await scoped.append({ op: 'c' }, {});
    const x = await scoped.append({ op: 'x' }, {}, version(c));
    const E = await scoped.append({ op: 'E' }, {}, version(x));
    const s1 = await scoped.append({ op: 's1' }, {}, version(E));
    const D = await scoped.append(createRefAdvancePayload('schema', version('v1')), createRefAdvanceMeta('schema'), version(x));
    const N = await scoped.append({ op: 'N' }, {}, version(s1));
    return { raw, scoped, s1, D, N, start: version(s1, D), end: version(N, D) };
}

async function testAtReference() {
    const h = await atReferenceHistory();
    const visitor = scriptedVisitor([]);
    const bound = await walk(h.raw, h.scoped, [{ refId: 'schema', from: 'at' }], visitor, h.start, h.end);

    testing.assertTrue(candidates(visitor).includes(h.D), "the 'at' ref-advance is a candidate");
    testing.assertTrue(isExactly(bound, h.s1), "an 'at' ref-advance whose effect did not change keeps the bound at the meet");
}

async function testDeclarations() {
    const h = await atReferenceHistory();

    const undeclared = scriptedVisitor([]);
    await assertThrows(() => walk(h.raw, h.scoped, [], undeclared, h.start, h.end),
        'a walked ref-advance of an undeclared ref throws');
    testing.assertFalse(visitedHashes(undeclared).includes(h.D), 'the undeclared ref-advance is not visited');

    const twice = scriptedVisitor([]);
    await assertThrows(
        () => walk(h.raw, h.scoped, [{ refId: 'schema', from: 'at' }, { refId: 'schema', from: 'at' }], twice, h.start, h.end),
        'a ref declared twice throws');
    testing.assertEquals(twice.visits.length, 0, 'nothing is visited');
}

async function testNothingNew() {
    const h = await localFloorHistory();
    const calls = newCalls();
    const reference = horizonReference('ref', createTestDag(), version(), () => version(), calls);
    const visitor = scriptedVisitor([h.T]);
    const bound = await walk(h.raw, h.scoped, [reference], visitor, h.end, h.end);

    testing.assertTrue(isExactly(bound, ...h.end), 'the bound is the common frontier');
    testing.assertEquals(visitor.visits.length, 0, 'nothing is visited');
    testing.assertEquals(calls.objects + calls.horizons.length + calls.boundArgs.length, 0, 'no reference is called');
}

export const revisionBoundSuite = {
    title: '[RBOUND] walkRevisionBound',
    tests: [
        { name: '[RBOUND01] a changed side-branch barrier lowers the bound, and the requeue walks what it uncovers', invoke: testRequeue },
        { name: '[RBOUND02] with nothing changed the bound stays at the meet, and only barriers of start are candidates', invoke: testUnchanged },
        { name: '[RBOUND03] Phase A lowers the bound below the lowest unstable ref-advance in the meet\'s history', invoke: testPhaseA },
        { name: '[RBOUND04] a referenced bound is taken between the versions observed at start and at end', invoke: testReferencedVersions },
        { name: '[RBOUND05] an unstable side-branch ref-advance lowers the bound in Phase B', invoke: testPhaseB },
        { name: '[RBOUND06] a changed barrier strictly after the bound does not lower it', invoke: testStrictlyAfter },
        { name: '[RBOUND07] each walked entry is visited once across two drops, and nothing is a candidate under an empty bound', invoke: testTwoDrops },
        { name: '[RBOUND08] an \'at\' reference\'s ref-advance is never unstable', invoke: testAtReference },
        { name: '[RBOUND09] an undeclared ref, or a ref declared twice, throws', invoke: testDeclarations },
        { name: '[RBOUND10] with nothing new the bound is the common frontier, and nothing is visited or called', invoke: testNothingNew },
    ],
};
