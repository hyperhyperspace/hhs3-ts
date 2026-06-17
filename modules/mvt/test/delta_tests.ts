import { testing } from '@hyper-hyper-space/hhs3_util';
import { B64Hash, sha256 } from '@hyper-hyper-space/hhs3_crypto';
import { dag, Position } from '@hyper-hyper-space/hhs3_dag';

import { RObject, version, Version } from '../src/mvt.js';
import { RootScopedDag } from '../src/dag/dag_nesting.js';
import { createRefAdvancePayload, createRefAdvanceMeta } from '../src/refs.js';
import {
    walkEntriesBackwardsToBound,
    computeForkMeet,
    computeObserverRevisionBound,
    combineObserverRevisionBounds,
} from '../src/delta.js';

function createTestDag(): dag.Dag {
    const store = new dag.store.MemDagStorage();
    const index = dag.idx.flat.createFlatIndex(
        store,
        new dag.idx.flat.mem.MemFlatIndexStore(),
    );
    return dag.create(store, index, sha256);
}

function setEq(a: Position, b: Position): boolean {
    if (a.size !== b.size) return false;
    for (const h of a) if (!b.has(h)) return false;
    return true;
}

// Minimal RObject stub exposing only the surface combineObserverRevisionBounds reaches:
// id + causal DAG + computeDelta for referenced objects, and scoped + causal DAG for the
// observer. Everything else is irrelevant to bound projection.
function stubObject(opts: {
    id?: B64Hash;
    scopedDag?: RootScopedDag;
    causalDag?: dag.Dag;
    revisionBound?: Version;
}): RObject {
    return {
        getId: () => opts.id!,
        getScopedDag: async () => opts.scopedDag!,
        getCausalDag: async () => opts.causalDag!,
        computeDelta: async () => ({ revisionBound: opts.revisionBound ?? version() }),
    } as unknown as RObject;
}

async function testWalkEntriesBackwardsToBound() {
    const d = createTestDag();
    const h0 = await d.append({ n: 0 }, {});
    const h1 = await d.append({ n: 1 }, {}, version(h0));
    const h2 = await d.append({ n: 2 }, {}, version(h1));
    const h3 = await d.append({ n: 3 }, {}, version(h2));

    const walked = await walkEntriesBackwardsToBound(d, version(h3), version(h1));

    testing.assertTrue(walked.some((e) => e.hash === h3), 'should include entry above bound');
    testing.assertTrue(walked.some((e) => e.hash === h2), 'should include entry above bound');
    testing.assertFalse(walked.some((e) => e.hash === h1), 'should exclude bound entry');
    testing.assertFalse(walked.some((e) => e.hash === h0), 'should exclude entries below bound');
}

async function testComputeForkMeet() {
    const d = createTestDag();
    const root = await d.append({ n: 'root' }, {});
    const b1 = await d.append({ n: 'b1' }, {}, version(root));
    const b2 = await d.append({ n: 'b2' }, {}, version(root));
    const merge = await d.append({ n: 'merge' }, {}, version(b1, b2));

    const fork = await d.findForkPosition(version(b1), version(merge));
    const meet = await computeForkMeet(d, fork.common);

    testing.assertTrue(meet.has(root), 'meet should include the common ancestor');
    testing.assertFalse(meet.has(b1), 'meet should not include branch tip');
    testing.assertFalse(meet.has(b2), 'meet should not include other branch tip');
}

async function testCombineObserverRevisionBounds() {
    // Observer: create -> ref-advance(refB -> fB_v2) -> mid -> end.
    // meet sits at `mid` (above the refB ref-advance); refB will be unstable (its bound
    // says fB_v1, below the observed fB_v2), so its projection drops below the meet, while
    // refA has no ref-advance and projects to the meet.
    const observerRaw = createTestDag();
    const observer = new RootScopedDag(observerRaw);

    const foreignB = createTestDag();
    const fRoot = await foreignB.append({ n: 0 }, {});
    const fB_v1 = await foreignB.append({ n: 1 }, {}, version(fRoot));
    const fB_v2 = await foreignB.append({ n: 2 }, {}, version(fB_v1));

    const refA = 'refA';
    const refB = 'refB';

    const h0 = await observer.append({ action: 'create' }, {}, undefined);
    const hRefB = await observer.append(
        createRefAdvancePayload(refB, version(fB_v2)),
        createRefAdvanceMeta(refB),
        version(h0),
    );
    const hMid = await observer.append({ action: 'add', element: 'x' }, {}, version(hRefB));
    const hEnd = await observer.append({ action: 'add', element: 'y' }, {}, version(hMid));

    const meet = version(hMid);
    const end = version(hEnd);

    const observerObj = stubObject({ scopedDag: observer, causalDag: observerRaw });
    const refAObj = stubObject({ id: refA, causalDag: createTestDag(), revisionBound: version() });
    const refBObj = stubObject({ id: refB, causalDag: foreignB, revisionBound: version(fB_v1) });

    // refB alone projects below the meet (its observed fB_v2 is above its own bound fB_v1).
    const projB = await computeObserverRevisionBound(observerObj, meet, end, refBObj);
    testing.assertTrue(projB.has(hRefB), 'unstable refB lowers the bound to its ref-advance');
    testing.assertFalse(setEq(projB, meet), 'refB projection sits below the meet');

    // Empty referenced set leaves the meet unchanged.
    const none = await combineObserverRevisionBounds(observerObj, meet, end, []);
    testing.assertTrue(setEq(none, meet), 'no referenced objects -> bound is the meet');

    // A single stable ref (no ref-advance) keeps the meet.
    const onlyA = await combineObserverRevisionBounds(observerObj, meet, end, [refAObj]);
    testing.assertTrue(setEq(onlyA, meet), 'a stable ref does not lower the bound');

    // Combining a stable ref with an unstable one yields the GLB = the lower (refB) bound.
    const combined = await combineObserverRevisionBounds(observerObj, meet, end, [refAObj, refBObj]);
    testing.assertTrue(setEq(combined, projB), 'combined bound is the GLB of the projected bounds');
    testing.assertFalse(setEq(combined, meet), 'an unstable ref lowers the combined bound below the meet');
}

export const deltaSuite = {
    title: '[DELTA] Bounded delta helpers',
    tests: [
        { name: '[DELTA_00] walkEntriesBackwardsToBound collects entries above bound', invoke: testWalkEntriesBackwardsToBound },
        { name: '[DELTA_01] computeForkMeet returns fork GLB', invoke: testComputeForkMeet },
        { name: '[DELTA_02] combineObserverRevisionBounds GLB of projected bounds', invoke: testCombineObserverRevisionBounds },
    ],
};
