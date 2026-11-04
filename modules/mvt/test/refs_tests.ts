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
    validateRefAdvanceMonotonicity,
    canonicalVersion,
    validateRefVersionShape,
    widenRefAtUpward,
    resolveRefFoldAtPosition,
    widenRefAtUpwardFold,
} from '../src/refs.js';
import type { RefFold } from '../src/refs.js';
import { FALSE, TRUE, undecidedOn } from '../src/verdict_evaluation.js';
import type { Truth } from '../src/verdict_evaluation.js';

// Liveness for a fold: undecided for `undecided`, false for `voids`, true
// otherwise. An undecided entry's support is its own hash.
function truths(undecided: B64Hash[], voids: B64Hash[] = []): (h: B64Hash) => Promise<Truth> {
    return async (h) => undecided.includes(h) ? undecidedOn([h]) : voids.includes(h) ? FALSE : TRUE;
}

function pendingEntries(fold: RefFold): string {
    return fold.pending.map((p) => p.entry).sort().join();
}

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

async function testResolveRefVersionAtPositionSeesThrough() {
    const refId = 'foreignObj';
    const foreignDag = createTestDag();
    const fRoot = await foreignDag.append({ n: 0 }, {});
    const fV1 = await foreignDag.append({ n: 1 }, {}, version(fRoot));
    const fV2 = await foreignDag.append({ n: 2 }, {}, version(fV1));

    const observerRaw = createTestDag();
    const observer = new RootScopedDag(observerRaw);

    const h0 = await observer.append({ action: 'create' }, {}, undefined);
    // a live ref-advance, then a void one on top of it
    const hLive = await observer.append(
        createRefAdvancePayload(refId, version(fV1)),
        createRefAdvanceMeta(refId),
        version(h0),
    );
    const hVoid = await observer.append(
        createRefAdvancePayload(refId, version(fV2)),
        createRefAdvanceMeta(refId),
        version(hLive),
    );

    const at = version(hVoid);
    const resolved = await resolveRefVersionAtPosition(observer, refId, at, at, async (h) => h !== hVoid);
    testing.assertTrue(resolved.has(fV1) && resolved.size === 1,
        'a void ref-advance does not hide the live one below it');

    const geometric = await resolveRefVersionAtPosition(observer, refId, at, at);
    testing.assertTrue(geometric.has(fV2) && geometric.size === 1, 'without isLive the latest ref-advance counts');
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

async function testCanonicalVersionAndShape(): Promise<void> {
    const raw = createTestDag();
    const g = await raw.append({ g: 1 }, {});
    const a = await raw.append({ a: 1 }, {}, version(g));
    const b = await raw.append({ b: 1 }, {}, version(a));
    const c = await raw.append({ c: 1 }, {}, version(a));

    testing.assertTrue(setEq(await canonicalVersion(raw, version(b)), version(b)), 'a singleton is already canonical');
    testing.assertTrue(setEq(await canonicalVersion(raw, version(a, b)), version(b)), 'a dominated ancestor is dropped');
    testing.assertTrue(setEq(await canonicalVersion(raw, version(b, c)), version(b, c)), 'an antichain is unchanged');

    const dominated = await validateRefVersionShape(raw, version(a, b));
    testing.assertFalse(dominated.valid, 'a version carrying a dominated ancestor is rejected');
    testing.assertTrue(!dominated.valid && dominated.why.reason.includes('antichain'), 'the rejection names the antichain');
    testing.assertTrue((await validateRefVersionShape(raw, version(b, c))).valid, 'an antichain passes');
    const empty = await validateRefVersionShape(raw, version());
    testing.assertFalse(empty.valid, 'an empty version is rejected');
    testing.assertTrue(!empty.valid && empty.why.reason.includes('empty'), 'the rejection names the empty version');
}

// Referenced: root ── v1 ── v2 ── v3, and w concurrent with v1.
// Observer: Y publishes {v1}. Concurrent with it, L publishes {v2} and D, on
// top of L, publishes {v3}; I publishes {w} and E publishes {v1}. D and L
// strictly dominate Y's base, I is incomparable with it, and E equals it.
async function testWidenRefAtUpward(): Promise<void> {
    const refId = 'foreignObj';
    const referenced = createTestDag();
    const root = await referenced.append({ n: 0 }, {});
    const v1 = await referenced.append({ n: 1 }, {}, version(root));
    const v2 = await referenced.append({ n: 2 }, {}, version(v1));
    const v3 = await referenced.append({ n: 3 }, {}, version(v2));
    const w = await referenced.append({ n: 'w' }, {}, version(root));

    const observer = new RootScopedDag(createTestDag());
    // `tag` tells apart two advances of one version at one position.
    const advance = (v: B64Hash, at: B64Hash, tag?: string) => observer.append(
        { ...createRefAdvancePayload(refId, version(v)), ...(tag === undefined ? {} : { tag }) },
        createRefAdvanceMeta(refId), version(at));
    const h0 = await observer.append({ action: 'create' }, {}, undefined);
    const Y = await advance(v1, h0);
    const L = await advance(v2, h0);
    const D = await advance(v3, L);
    const I = await advance(w, h0);
    const E = await advance(v1, h0, 'E');
    const from = version(Y, D, I, E);

    const widen = async (voids: B64Hash[]) => {
        const asked: B64Hash[] = [];
        const refAt = await widenRefAtUpward(observer, refId, version(Y), from, referenced, async (h) => {
            asked.push(h);
            return !voids.includes(h);
        });
        return { refAt, asked };
    };

    const allLive = await widen([]);
    testing.assertTrue(setEq(allLive.refAt, version(v3)), 'a live strictly dominating advance widens the cut');
    testing.assertTrue(allLive.asked.length === 1 && allLive.asked[0] === D,
        'liveness is asked of the dominating candidate only, not of the incomparable or the equal one');

    const dVoid = await widen([D]);
    testing.assertTrue(setEq(dVoid.refAt, version(v2)), 'a void candidate is seen through to the live one below it');
    testing.assertFalse(dVoid.asked.includes(I) || dVoid.asked.includes(E), 'the incomparable and the equal advances are never asked');

    const bothVoid = await widen([D, L]);
    testing.assertTrue(setEq(bothVoid.refAt, version(v1)), 'with every dominating candidate void, the cut is the base');
}

// Referenced: root ── f1 ── f2, and f3 on another branch.
// Observer: h0 ── A {f1} ── B {f2}; C, a plain entry, on top of A; Z {f3}
// concurrent with all of them. Read at {B, C}, so A is reached on C's path too.
async function testResolveRefFoldAtPosition(): Promise<void> {
    const refId = 'foreignObj';
    const referenced = createTestDag();
    const root = await referenced.append({ n: 0 }, {});
    const f1 = await referenced.append({ n: 1 }, {}, version(root));
    const f2 = await referenced.append({ n: 2 }, {}, version(f1));
    const f3 = await referenced.append({ n: 3 }, {}, version(root));

    const observer = new RootScopedDag(createTestDag());
    const advance = (v: B64Hash, at: B64Hash) => observer.append(
        createRefAdvancePayload(refId, version(v)), createRefAdvanceMeta(refId), version(at));
    const h0 = await observer.append({ action: 'create' }, {}, undefined);
    const A = await advance(f1, h0);
    const B = await advance(f2, A);
    const C = await observer.append({ action: 'other' }, {}, version(A));
    const Z = await advance(f3, h0);
    const at = version(B, C);
    const withZ = version(B, C, Z);

    for (const voids of [[], [B], [A, B], [Z], [B, Z]]) {
        for (const from of [at, withZ]) {
            const fold = await resolveRefFoldAtPosition(observer, refId, at, from, truths([], voids));
            const bool = await resolveRefVersionAtPosition(observer, refId, at, from, async (h) => !voids.includes(h));
            testing.assertTrue(setEq(fold.live, bool), `decided liveness gives the boolean fold (voids: ${voids.length})`);
            testing.assertTrue(fold.pending.length === 0, 'decided liveness leaves nothing pending');
        }
    }

    const top = await resolveRefFoldAtPosition(observer, refId, at, at, truths([B]));
    testing.assertTrue(setEq(top.live, version(f1)), 'the walk sees through an undecided advance to the live one below');
    testing.assertTrue(pendingEntries(top) === B && setEq(top.pending[0].version, version(f2)),
        'the undecided advance is pending, with its published version');
    testing.assertTrue(top.pending[0].verdict.support.has(B), 'a pending advance carries the verdict the walk read');

    const below = await resolveRefFoldAtPosition(observer, refId, at, at, truths([A]));
    testing.assertTrue(setEq(below.live, version(f2)), 'the live advance on top contributes');
    testing.assertTrue(below.pending.length === 0, 'an undecided advance below a live one of the same walk is dropped');

    const underVoid = await resolveRefFoldAtPosition(observer, refId, at, at, truths([A], [B]));
    testing.assertTrue(setEq(underVoid.live, version(refId)), 'with no live advance the fold is the referenced root');
    testing.assertTrue(pendingEntries(underVoid) === A, 'a void advance is walked past, and the undecided one below it is kept');

    const concurrent = await resolveRefFoldAtPosition(observer, refId, at, withZ, truths([Z]));
    testing.assertTrue(setEq(concurrent.live, version(f2)), 'the causal walk is unaffected');
    testing.assertTrue(pendingEntries(concurrent) === Z && setEq(concurrent.pending[0].version, version(f3)),
        'an undecided concurrent barrier is pending');
}

// The fixture of testWidenRefAtUpward: Y publishes {v1}; concurrent with it, L
// publishes {v2}, D on top of L publishes {v3}, I publishes {w} and E {v1}.
async function testWidenRefAtUpwardFold(): Promise<void> {
    const refId = 'foreignObj';
    const referenced = createTestDag();
    const root = await referenced.append({ n: 0 }, {});
    const v1 = await referenced.append({ n: 1 }, {}, version(root));
    const v2 = await referenced.append({ n: 2 }, {}, version(v1));
    const v3 = await referenced.append({ n: 3 }, {}, version(v2));
    const w = await referenced.append({ n: 'w' }, {}, version(root));

    const observer = new RootScopedDag(createTestDag());
    const advance = (v: B64Hash, at: B64Hash, tag?: string) => observer.append(
        { ...createRefAdvancePayload(refId, version(v)), ...(tag === undefined ? {} : { tag }) },
        createRefAdvanceMeta(refId), version(at));
    const h0 = await observer.append({ action: 'create' }, {}, undefined);
    const Y = await advance(v1, h0);
    const L = await advance(v2, h0);
    const D = await advance(v3, L);
    const I = await advance(w, h0);
    const E = await advance(v1, h0, 'E');
    const from = version(Y, D, I, E);

    const fold = async (undecided: B64Hash[], voids: B64Hash[] = []) => {
        const asked: B64Hash[] = [];
        const liveness = truths(undecided, voids);
        const result = await widenRefAtUpwardFold(observer, refId, version(Y), from, referenced, async (h) => {
            asked.push(h);
            return liveness(h);
        });
        return { ...result, asked };
    };

    for (const voids of [[], [D], [D, L]]) {
        const decided = await fold([], voids);
        const bool = await widenRefAtUpward(observer, refId, version(Y), from, referenced, async (h) => !voids.includes(h));
        testing.assertTrue(setEq(decided.live, bool) && decided.pending.length === 0,
            `decided liveness gives the boolean widening, with nothing pending (voids: ${voids.length})`);
    }

    const dPending = await fold([D]);
    testing.assertTrue(setEq(dPending.live, version(v2)), 'the search sees through an undecided candidate to the live one below');
    testing.assertTrue(pendingEntries(dPending) === D && setEq(dPending.pending[0].version, version(v3)),
        'the undecided candidate is a pending widening, with its version');
    testing.assertFalse(dPending.asked.includes(I) || dPending.asked.includes(E),
        'liveness is asked only of candidates strictly above the base');

    const lUndecided = await fold([L]);
    testing.assertTrue(setEq(lUndecided.live, version(v3)) && lUndecided.pending.length === 0,
        'a candidate below a live widening is not pending');

    const both = await fold([D, L]);
    testing.assertTrue(setEq(both.live, version(v1)), 'with no live widening, refAt is the base');
    testing.assertTrue(pendingEntries(both) === [D, L].sort().join(), 'each undecided candidate is a pending widening');
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
        { name: '[REFS_07b] resolveRefVersionAtPosition isLive filter', invoke: testResolveRefVersionAtPositionIsLive },
        { name: '[REFS_07c] resolveRefVersionAtPosition sees through a void ref-advance', invoke: testResolveRefVersionAtPositionSeesThrough },
        { name: '[REFS_09] ref-advance monotonicity validation', invoke: testRefAdvanceMonotonicity },
        { name: '[REFS_10] canonical version and antichain shape', invoke: testCanonicalVersionAndShape },
        { name: '[REFS_11] widenRefAtUpward widens only by live strictly dominating concurrent advances', invoke: testWidenRefAtUpward },
        { name: '[REFS_12] resolveRefFoldAtPosition keeps undecided ref-advances as pending', invoke: testResolveRefFoldAtPosition },
        { name: '[REFS_13] widenRefAtUpwardFold keeps undecided strictly dominating candidates as pending widenings', invoke: testWidenRefAtUpwardFold },
    ],
};
