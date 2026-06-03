import { testing } from '@hyper-hyper-space/hhs3_util';
import { B64Hash, sha256, stringToUint8Array } from '@hyper-hyper-space/hhs3_crypto';
import { dag, Position, MetaProps } from '@hyper-hyper-space/hhs3_dag';
import { json } from '@hyper-hyper-space/hhs3_json';

import { RootScopedDag } from '../src/dag/dag_nesting.js';
import { version } from '../src/mvt.js';
import {
    RefAdvancePayload,
    refAdvanceFormat,
    isRefAdvancePayload,
    createRefAdvancePayload,
    extractRefVersion,
    refAdvanceMeta,
    findRefAdvances,
    findConcurrentRefAdvanceBarriers,
    refVersionAtOrAbove,
    validateRefAdvanceMonotonicity,
} from '../src/refs.js';

function createTestDag(): dag.Dag {
    const store = new dag.store.MemDagStorage();
    const index = dag.idx.flat.createFlatIndex(
        store,
        new dag.idx.flat.mem.MemFlatIndexStore()
    );
    return dag.create(store, index, sha256);
}

async function testIsRefAdvancePayload() {
    testing.assertTrue(
        isRefAdvancePayload({ action: 'ref-advance', refId: 'abc', refVersion: {} }),
        'valid ref-advance payload should be recognized',
    );
    testing.assertFalse(
        isRefAdvancePayload({ action: 'add', element: 'x' }),
        'non-ref-advance payload should be rejected',
    );
    testing.assertFalse(
        isRefAdvancePayload('not an object'),
        'string literal should be rejected',
    );
    testing.assertFalse(
        isRefAdvancePayload([1, 2, 3]),
        'array literal should be rejected',
    );
    testing.assertTrue(
        isRefAdvancePayload({ action: 'ref-advance' }),
        'minimal ref-advance (missing fields) should still be recognized by action check',
    );
}

async function testCreateAndExtractRefAdvancePayload() {
    const refId = 'testRefId123';
    const refVersion = version('hashA', 'hashB');

    const payload = createRefAdvancePayload(refId, refVersion);

    testing.assertEquals(payload.action, 'ref-advance', 'action should be ref-advance');
    testing.assertEquals(payload.refId, refId, 'refId should match');
    testing.assertTrue(isRefAdvancePayload(payload), 'created payload should pass isRefAdvancePayload');

    const extracted = extractRefVersion(payload);
    testing.assertTrue(extracted.has('hashA'), 'extracted version should contain hashA');
    testing.assertTrue(extracted.has('hashB'), 'extracted version should contain hashB');
    testing.assertEquals(extracted.size, 2, 'extracted version should have 2 elements');
}

async function testRefAdvanceFormatValidation() {
    const valid = createRefAdvancePayload('myRef', version('h1'));

    testing.assertTrue(
        json.checkFormat(refAdvanceFormat, valid, { strict: false }),
        'valid ref-advance payload should pass format check (non-strict)',
    );

    const extended = { ...valid, author: 'some-identity', signature: 'sig123' };
    testing.assertTrue(
        json.checkFormat(refAdvanceFormat, extended, { strict: false }),
        'extended ref-advance payload should pass format check (non-strict)',
    );
    testing.assertFalse(
        json.checkFormat(refAdvanceFormat, extended, { strict: true }),
        'extended ref-advance payload should fail strict format check',
    );

    const badAction = { action: 'add', refId: 'x', refVersion: {} };
    testing.assertFalse(
        json.checkFormat(refAdvanceFormat, badAction, { strict: false }),
        'wrong action should fail format check',
    );

    const missingRefVersion = { action: 'ref-advance', refId: 'x' };
    testing.assertFalse(
        json.checkFormat(refAdvanceFormat, missingRefVersion, { strict: false }),
        'missing refVersion should fail format check',
    );
}

async function testRefAdvanceMeta() {
    const refId = 'myRefId';
    const meta = refAdvanceMeta(refId);

    testing.assertTrue(meta['ref'] !== undefined, 'meta should have ref key');
    const refValues = [...json.fromSet(meta['ref'])];
    testing.assertEquals(refValues.length, 1, 'ref meta should have 1 element');
    testing.assertEquals(refValues[0], refId, 'ref meta should contain the refId');
}

async function testFindRefAdvances() {
    const rawDag = createTestDag();
    const scopedDag = new RootScopedDag(rawDag);

    const refId = 'permissionsObj';
    const meta: MetaProps = { ref: json.toSet([refId]) };

    const h1 = await scopedDag.append(
        createRefAdvancePayload(refId, version('v1')),
        meta,
        undefined,
    );

    const h2 = await scopedDag.append(
        { action: 'add', element: 'hello' },
        { elmts: json.toSet(['elmt1']) },
        version(h1),
    );

    const h3 = await scopedDag.append(
        createRefAdvancePayload(refId, version('v1', 'v2')),
        meta,
        version(h2),
    );

    const cover = await findRefAdvances(scopedDag, refId, version(h3));
    testing.assertTrue(cover.size > 0, 'should find ref-advance entries');
    testing.assertTrue(cover.has(h3), 'cover should contain the latest ref-advance');
    testing.assertFalse(cover.has(h2), 'cover should not contain non-ref-advance entry');
}

async function testFindConcurrentRefAdvanceBarriers() {
    const rawDag = createTestDag();
    const scopedDag = new RootScopedDag(rawDag);

    const refId = 'permissionsObj';

    const h1 = await scopedDag.append(
        { action: 'add', element: 'x' },
        {},
        undefined,
    );

    const hBranchA = await scopedDag.append(
        { action: 'add', element: 'a' },
        {},
        version(h1),
    );

    const barrierMeta: MetaProps = {
        ref: json.toSet([refId]),
        barrier: json.toSet(['t']),
    };

    const hBranchB = await scopedDag.append(
        createRefAdvancePayload(refId, version('v1')),
        barrierMeta,
        version(h1),
    );

    const barriers = await findConcurrentRefAdvanceBarriers(
        scopedDag, refId, version(hBranchA), version(hBranchB),
    );

    testing.assertTrue(barriers.has(hBranchB), 'should find the concurrent barrier ref-advance');
    testing.assertFalse(barriers.has(hBranchA), 'should not include non-barrier entry');
}

async function testRefAdvanceMonotonicity() {
    const referencedDag = createTestDag();
    const root = await referencedDag.append({ n: 0 }, {});
    const v1 = await referencedDag.append({ n: 1 }, {}, version(root));
    const v2 = await referencedDag.append({ n: 2 }, {}, version(v1));
    const v2Branch = await referencedDag.append({ n: '2b' }, {}, version(root));
    const mergeTip = await referencedDag.append({ n: 3 }, {}, version(v2, v2Branch));

    testing.assertTrue(
        await refVersionAtOrAbove(referencedDag, version(v2), version(v1)),
        'v2 should be at or above v1',
    );
    testing.assertTrue(
        await refVersionAtOrAbove(referencedDag, version(v1), version(v1)),
        'equal versions should pass atOrAbove check',
    );
    testing.assertFalse(
        await refVersionAtOrAbove(referencedDag, version(v1), version(v2)),
        'v1 should not be at or above v2',
    );

    const refId = root;
    const meta: MetaProps = { ref: json.toSet([refId]) };

    const observerRaw = createTestDag();
    const observer = new RootScopedDag(observerRaw);

    const h0 = await observer.append({ action: 'create' }, {}, undefined);

    testing.assertTrue(
        await validateRefAdvanceMonotonicity(
            observer, referencedDag, refId, version(v1), version(h0),
        ),
        'first advance from implicit root should pass',
    );

    const h1 = await observer.append(
        createRefAdvancePayload(refId, version(v1)),
        meta,
        version(h0),
    );

    testing.assertTrue(
        await validateRefAdvanceMonotonicity(
            observer, referencedDag, refId, version(v2), version(h1),
        ),
        'sequential forward advance should pass',
    );

    testing.assertTrue(
        await validateRefAdvanceMonotonicity(
            observer, referencedDag, refId, version(v1), version(h1),
        ),
        'equal re-advance should pass',
    );

    const h2 = await observer.append(
        createRefAdvancePayload(refId, version(v2)),
        meta,
        version(h1),
    );

    testing.assertFalse(
        await validateRefAdvanceMonotonicity(
            observer, referencedDag, refId, version(v1), version(h2),
        ),
        'backward advance should fail',
    );

    const forkRaw = createTestDag();
    const forkObserver = new RootScopedDag(forkRaw);
    const forkRoot = await forkObserver.append({ action: 'create' }, {}, undefined);

    const hBranchA = await forkObserver.append(
        createRefAdvancePayload(refId, version(v1)),
        meta,
        version(forkRoot),
    );
    const hBranchB = await forkObserver.append(
        createRefAdvancePayload(refId, version(v2Branch)),
        meta,
        version(forkRoot),
    );

    testing.assertTrue(
        await validateRefAdvanceMonotonicity(
            forkObserver, referencedDag, refId, version(mergeTip), version(hBranchA, hBranchB),
        ),
        'merge advance above both branch refs should pass',
    );

    testing.assertFalse(
        await validateRefAdvanceMonotonicity(
            forkObserver, referencedDag, refId, version(v1), version(hBranchA, hBranchB),
        ),
        'merge advance below one branch ref should fail',
    );
}

export const refsSuite = {
    title: '[REFS] Building-block helpers',
    tests: [
        { name: '[REFS_00] isRefAdvancePayload recognition', invoke: testIsRefAdvancePayload },
        { name: '[REFS_01] create and extract ref-advance payload', invoke: testCreateAndExtractRefAdvancePayload },
        { name: '[REFS_02] refAdvanceFormat validation', invoke: testRefAdvanceFormatValidation },
        { name: '[REFS_03] refAdvanceMeta structure', invoke: testRefAdvanceMeta },
        { name: '[REFS_04] findRefAdvances DAG query', invoke: testFindRefAdvances },
        { name: '[REFS_05] findConcurrentRefAdvanceBarriers DAG query', invoke: testFindConcurrentRefAdvanceBarriers },
        { name: '[REFS_06] ref-advance monotonicity validation', invoke: testRefAdvanceMonotonicity },
    ],
};
