import { testing } from '@hyper-hyper-space/hhs3_util';
import { B64Hash, sha256, stringToUint8Array } from '@hyper-hyper-space/hhs3_crypto';
import { dag, Position } from '@hyper-hyper-space/hhs3_dag';
import { json } from '@hyper-hyper-space/hhs3_json';

import { RootScopedDag } from '../src/dag/dag_nesting.js';
import { version } from '../src/mvt.js';
import {
    RefAdvancePayload,
    refAdvanceFormat,
    isRefAdvancePayload,
    createRefAdvancePayload,
    extractRefVersion,
    createRefAdvanceMeta,
    prepareRefAdvance,
    findRefAdvances,
    findConcurrentRefAdvanceBarriers,
    refVersionAtOrAbove,
    refVersionAtOrBelow,
    resolveRefVersionAtPosition,
    resolveRefVersions,
    projectForeignBound,
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

function setEq(a: Position, b: Position): boolean {
    if (a.size !== b.size) return false;
    for (const h of a) if (!b.has(h)) return false;
    return true;
}

function refMetaValues(meta: ReturnType<typeof createRefAdvanceMeta>): B64Hash[] {
    return [...json.fromSet(meta['ref']!)];
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

async function testCreateRefAdvanceMeta() {
    const refId = 'myRefId';

    const defaultMeta = createRefAdvanceMeta(refId);
    testing.assertEquals(refMetaValues(defaultMeta).length, 1, 'default meta should index one refId');
    testing.assertEquals(refMetaValues(defaultMeta)[0], refId, 'default meta should index refId');
    testing.assertEquals([...json.fromSet(defaultMeta['barrier']!)][0], 't', 'barrier tag should be included by default');

    const indexed = createRefAdvanceMeta(refId, { barrier: false });
    testing.assertEquals(refMetaValues(indexed)[0], refId, 'non-barrier meta should still index refId');
    testing.assertTrue(indexed['barrier'] === undefined, 'barrier tag should be omitted when disabled');
}

async function testPrepareRefAdvance() {
    const refId = 'capRef';
    const refVersion = version('v1', 'v2');
    const { payload, meta } = prepareRefAdvance(refId, refVersion);

    testing.assertTrue(isRefAdvancePayload(payload), 'payload should be a ref-advance');
    testing.assertEquals(payload.refId, refId, 'refId should match');
    testing.assertTrue(setEq(extractRefVersion(payload), refVersion), 'refVersion should match');

    testing.assertEquals(refMetaValues(meta)[0], refId, 'meta ref index should match');
    testing.assertEquals([...json.fromSet(meta['barrier']!)][0], 't', 'prepareRefAdvance meta should include barrier tag');
}

async function testFindRefAdvances() {
    const rawDag = createTestDag();
    const scopedDag = new RootScopedDag(rawDag);

    const refId = 'permissionsObj';
    const meta = createRefAdvanceMeta(refId);

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

    const hBranchB = await scopedDag.append(
        createRefAdvancePayload(refId, version('v1')),
        createRefAdvanceMeta(refId),
        version(h1),
    );

    const barriers = await findConcurrentRefAdvanceBarriers(
        scopedDag, refId, version(hBranchA), version(hBranchB),
    );

    testing.assertTrue(barriers.has(hBranchB), 'should find the concurrent barrier ref-advance');
    testing.assertFalse(barriers.has(hBranchA), 'should not include non-barrier entry');
}

async function testRefVersionAtOrBelow() {
    const referencedDag = createTestDag();
    const root = await referencedDag.append({ n: 0 }, {});
    const v1 = await referencedDag.append({ n: 1 }, {}, version(root));
    const v2 = await referencedDag.append({ n: 2 }, {}, version(v1));
    const v2Branch = await referencedDag.append({ n: '2b' }, {}, version(root));

    testing.assertTrue(
        await refVersionAtOrBelow(referencedDag, version(v1), version(v2)),
        'v1 should be at or below v2',
    );
    testing.assertTrue(
        await refVersionAtOrBelow(referencedDag, version(v1), version(v1)),
        'equal versions should pass atOrBelow check',
    );
    testing.assertFalse(
        await refVersionAtOrBelow(referencedDag, version(v2), version(v1)),
        'v2 should not be at or below v1',
    );
    testing.assertFalse(
        await refVersionAtOrBelow(referencedDag, version(v1), version(v2Branch)),
        'concurrent v1 should not be at or below v2Branch',
    );
    testing.assertFalse(
        await refVersionAtOrAbove(referencedDag, version(v1), version(v2Branch)),
        'concurrent positions should fail atOrAbove as well',
    );
}

async function testResolveRefVersions() {
    const refId = 'foreignObj';
    const foreignDag = createTestDag();
    const fRoot = await foreignDag.append({ n: 0 }, {});
    const fV1 = await foreignDag.append({ n: 1 }, {}, version(fRoot));

    const observerRaw = createTestDag();
    const observer = new RootScopedDag(observerRaw);

    const h0 = await observer.append({ action: 'create' }, {}, undefined);
    const h1 = await observer.append(
        createRefAdvancePayload(refId, version(fV1)),
        createRefAdvanceMeta(refId),
        version(h0),
    );
    const h2 = await observer.append({ action: 'add', element: 'x' }, {}, version(h1));

    const observerFrom = version(h2);
    const { refAt, refFrom } = await resolveRefVersions(observer, refId, h1, observerFrom);

    testing.assertTrue(setEq(refAt, version(fV1)), 'refAt should resolve foreign version at entry');
    testing.assertTrue(setEq(refFrom, version(fV1)), 'refFrom should resolve at observer frontier without widening');
}

async function testResolveRefVersionAtPositionIsLive() {
    const refId = 'foreignObj';
    const foreignDag = createTestDag();
    const fRoot = await foreignDag.append({ n: 0 }, {});
    const fV1 = await foreignDag.append({ n: 1 }, {}, version(fRoot));
    const fV2 = await foreignDag.append({ n: '2b' }, {}, version(fRoot));   // concurrent to fV1

    const observerRaw = createTestDag();
    const observer = new RootScopedDag(observerRaw);

    const h0 = await observer.append({ action: 'create' }, {}, undefined);
    // two concurrent observe ref-advances (both causal at the merge)
    const hA = await observer.append(
        createRefAdvancePayload(refId, version(fV1)),
        createRefAdvanceMeta(refId),
        version(h0),
    );
    const hB = await observer.append(
        createRefAdvancePayload(refId, version(fV2)),
        createRefAdvanceMeta(refId),
        version(h0),
    );

    const at = version(hA, hB);

    const all = await resolveRefVersionAtPosition(observer, refId, at, at);
    testing.assertTrue(all.has(fV1) && all.has(fV2), 'without isLive both ref-advances contribute');

    const filtered = await resolveRefVersionAtPosition(observer, refId, at, at, async (h) => h !== hB);
    testing.assertTrue(filtered.has(fV1), 'a live ref-advance still contributes under isLive');
    testing.assertFalse(filtered.has(fV2), 'a ref-advance rejected by isLive is skipped');

    const none = await resolveRefVersionAtPosition(observer, refId, at, at, async () => false);
    testing.assertTrue(none.has(refId) && none.size === 1, 'filtering all ref-advances falls back to version(refId)');
}

async function testProjectForeignBound() {
    const refId = 'foreignObj';
    const foreignDag = createTestDag();
    const fRoot = await foreignDag.append({ n: 0 }, {});
    const fV1 = await foreignDag.append({ n: 1 }, {}, version(fRoot));
    const fV2 = await foreignDag.append({ n: 2 }, {}, version(fV1));

    const observerRaw = createTestDag();
    const observer = new RootScopedDag(observerRaw);

    const h0 = await observer.append({ action: 'create' }, {}, undefined);
    const prepared = prepareRefAdvance(refId, version(fV2));
    const hRef = await observer.append(prepared.payload, prepared.meta, version(h0));
    const h2 = await observer.append({ action: 'add', element: 'x' }, {}, version(hRef));

    const localAt = version(h2);

    const boundStable = await projectForeignBound(
        observer, refId, foreignDag, localAt, version(fV2),
    );
    testing.assertTrue(setEq(boundStable, localAt), 'all ref-advances stable should return localAt');

    const boundUnstable = await projectForeignBound(
        observer, refId, foreignDag, localAt, version(fV1),
    );
    testing.assertTrue(boundUnstable.has(hRef), 'unstable ref-advance should lower bound to its hash');
    testing.assertFalse(boundUnstable.has(h2), 'bound should sit below localAt');
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
    const meta = createRefAdvanceMeta(refId);

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
        { name: '[REFS_03] createRefAdvanceMeta default barrier and opt-out', invoke: testCreateRefAdvanceMeta },
        { name: '[REFS_03b] prepareRefAdvance payload and meta', invoke: testPrepareRefAdvance },
        { name: '[REFS_04] findRefAdvances DAG query', invoke: testFindRefAdvances },
        { name: '[REFS_05] findConcurrentRefAdvanceBarriers DAG query', invoke: testFindConcurrentRefAdvanceBarriers },
        { name: '[REFS_06] refVersionAtOrBelow ordering', invoke: testRefVersionAtOrBelow },
        { name: '[REFS_07] resolveRefVersions for entry', invoke: testResolveRefVersions },
        { name: '[REFS_07b] resolveRefVersionAtPosition isLive filter', invoke: testResolveRefVersionAtPositionIsLive },
        { name: '[REFS_08] projectForeignBound bound projection', invoke: testProjectForeignBound },
        { name: '[REFS_09] ref-advance monotonicity validation', invoke: testRefAdvanceMonotonicity },
    ],
};
