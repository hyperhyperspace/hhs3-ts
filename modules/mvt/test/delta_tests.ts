import { testing } from '@hyper-hyper-space/hhs3_util';
import { sha256 } from '@hyper-hyper-space/hhs3_crypto';
import { dag } from '@hyper-hyper-space/hhs3_dag';

import { version } from '../src/mvt.js';
import { walkEntriesBackwardsToBound, computeForkMeet } from '../src/delta.js';

function createTestDag(): dag.Dag {
    const store = new dag.store.MemDagStorage();
    const index = dag.idx.flat.createFlatIndex(
        store,
        new dag.idx.flat.mem.MemFlatIndexStore(),
    );
    return dag.create(store, index, sha256);
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

export const deltaSuite = {
    title: '[DELTA] Bounded delta helpers',
    tests: [
        { name: '[DELTA_00] walkEntriesBackwardsToBound collects entries above bound', invoke: testWalkEntriesBackwardsToBound },
        { name: '[DELTA_01] computeForkMeet returns fork GLB', invoke: testComputeForkMeet },
    ],
};
