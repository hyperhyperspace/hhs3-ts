import { RContext, RObject, RObjectInit, RObjectConfig, TypeRegistryMap, version } from "../src/mvt.js";
import { RSet, rSetFactory } from "../src/types/rset.js";
import { assertTrue, assertFalse } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createMockRContext } from "./mock_rcontext.js";
import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, createBasicCrypto, HASH_SHA256, stringToUint8Array } from "@hyper-hyper-space/hhs3_crypto";

const crypto = createBasicCrypto();

function createTestCtx(): RContext {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSet.typeId, rSetFactory);
    return ctx;
}

export const nestedSetTests = {
    title: '[NESTED] Nested set tests',
    tests: [
        {
            name: '[NES00] Test adding a nested set and inserting elements into it',
            invoke: async () => {
                const ctx = createTestCtx();

                const outerSetInit = await RSet.create({
                    seed: 'outer-set',
                    contentType: RSet.typeId,
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                    supportBarrierAdd: true,
                    supportBarrierDelete: true,
                });

                const outerSet = (await ctx.createObject(outerSetInit)) as RSet;

                const nestedSetPayload = await RSet.create({
                    seed: 'nested-set-1',
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                    supportBarrierAdd: true,
                    supportBarrierDelete: true,
                });

                const nestedSetHash = await outerSet.add(nestedSetPayload.payload);
                
                const outerView = await outerSet.getView();
                const nestedSet = await outerView.loadRObjectByHash(nestedSetHash) as RSet;

                await nestedSet.add('alpha');
                await nestedSet.add('beta');
                await nestedSet.add('gamma');

                const nestedView = await nestedSet.getView();
                assertTrue(await nestedView.has('alpha'), 'nested set should contain alpha');
                assertTrue(await nestedView.has('beta'), 'nested set should contain beta');
                assertTrue(await nestedView.has('gamma'), 'nested set should contain gamma');

                assertTrue(await outerView.hasByHash(nestedSetHash), 'outer set should contain the nested set');

                await nestedSet.delete('beta');
                const updatedNestedView = await nestedSet.getView();
                assertFalse(await updatedNestedView.has('beta'), 'beta should be removed from nested set');
                assertTrue(await updatedNestedView.has('alpha'), 'alpha should still be in nested set');
                assertTrue(await updatedNestedView.has('gamma'), 'gamma should still be in nested set');
            }
        },
        {
            name: '[NES01] Test multiple nested sets with overlapping elements',
            invoke: async () => {
                const ctx = createTestCtx();

                const outerSetInit = await RSet.create({
                    seed: 'outer-set-multi',
                    contentType: RSet.typeId,
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                    supportBarrierAdd: true,
                    supportBarrierDelete: true,
                });

                const outerSet = (await ctx.createObject(outerSetInit)) as RSet;

                const addNestedSet = async (seed: string) => {
                    const nestedSetInit = await RSet.create({
                        seed,
                        initialElements: [],
                        hashAlgorithm: 'sha256',
                        supportBarrierAdd: true,
                        supportBarrierDelete: true,
                    });

                    const nestedHash = await outerSet.add(nestedSetInit.payload);
                    const outerView = await outerSet.getView();
                    const nested = await outerView.loadRObjectByHash(nestedHash) as RSet;
                    return { nestedHash, nested };
                };

                const { nested: setA } = await addNestedSet('nested-A');
                const { nested: setB } = await addNestedSet('nested-B');
                const { nested: setC } = await addNestedSet('nested-C');

                await setA.add('shared');
                await setA.add('a1');
                await setA.add('a2');
                await setA.delete('a2');

                await setB.add('shared');
                await setB.add('b1');

                await setC.add('shared');
                await setC.add('c1');
                await setC.delete('shared');

                const viewA = await setA.getView();
                const viewB = await setB.getView();
                const viewC = await setC.getView();

                assertTrue(await viewA.has('shared'), 'setA should contain shared');
                assertTrue(await viewB.has('shared'), 'setB should contain shared');
                assertFalse(await viewC.has('shared'), 'setC should not contain shared after deletion');

                assertTrue(await viewA.has('a1'), 'setA should contain a1');
                assertFalse(await viewA.has('a2'), 'setA should not contain a2 after deletion');

                assertTrue(await viewB.has('b1'), 'setB should contain b1');
                assertFalse(await viewB.has('a1'), 'setB should not see elements from setA');
                assertFalse(await viewB.has('c1'), 'setB should not see elements from setC');

                assertTrue(await viewC.has('c1'), 'setC should contain c1');
                assertFalse(await viewC.has('a1'), 'setC should not see elements from setA');
                assertFalse(await viewC.has('b1'), 'setC should not see elements from setB');
            }
        },
        {
            name: '[NES02] Test concurrent additions inside nested sets',
            invoke: async () => {
                const ctx = createTestCtx();

                const outerSetInit = await RSet.create({
                    seed: 'outer-set-concurrent',
                    contentType: RSet.typeId,
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                    supportBarrierAdd: true,
                    supportBarrierDelete: true,
                });

                const outerSet = (await ctx.createObject(outerSetInit)) as RSet;

                const addNestedSet = async (seed: string) => {
                    const nestedSetInit = await RSet.create({
                        seed,
                        initialElements: [],
                        hashAlgorithm: 'sha256',
                        supportBarrierAdd: true,
                        supportBarrierDelete: true,
                    });

                    const nestedHash = await outerSet.add(nestedSetInit.payload);
                    const outerViewForLoad = await outerSet.getView();
                    const nested = await outerViewForLoad.loadRObjectByHash(nestedHash) as RSet;
                    return { nestedHash: nestedHash, nested };
                };

                const { nestedHash: hashA, nested: setA } = await addNestedSet('nested-concurrent-A');
                const { nestedHash: hashB, nested: setB } = await addNestedSet('nested-concurrent-B');

                const runConcurrentAdds = async (target: RSet, setName: string) => {
                    const alphaLabel = 'alpha';
                    const betaLabel = 'beta';
                    const gammaLabel = 'gamma';

                    const alphaHash = await target.add(alphaLabel);
                    const alphaVersion = version(alphaHash);

                    const betaHash = await target.add(betaLabel, alphaVersion);
                    const betaVersion = version(betaHash);

                    const gammaHash = await target.add(gammaLabel, alphaVersion);
                    const gammaVersion = version(gammaHash);

                    const ancestorView = await target.getView(alphaVersion);
                    assertTrue(await ancestorView.has(alphaLabel), `${setName}: alpha should be visible at its own version`);
                    assertFalse(await ancestorView.has(betaLabel), `${setName}: beta should not exist before it was added`);
                    assertFalse(await ancestorView.has(gammaLabel), `${setName}: gamma should not exist before it was added`);

                    const betaView = await target.getView(betaVersion);
                    assertTrue(await betaView.has(alphaLabel), `${setName}: alpha should be visible when reading beta branch`);
                    assertTrue(await betaView.has(betaLabel), `${setName}: beta should be visible when reading beta branch`);
                    assertFalse(await betaView.has(gammaLabel), `${setName}: gamma should not leak into beta branch view`);

                    const gammaView = await target.getView(gammaVersion);
                    assertTrue(await gammaView.has(alphaLabel), `${setName}: alpha should be visible when reading gamma branch`);
                    assertTrue(await gammaView.has(gammaLabel), `${setName}: gamma should be visible when reading gamma branch`);
                    assertFalse(await gammaView.has(betaLabel), `${setName}: beta should not leak into gamma branch view`);

                    const frontierView = await target.getView();
                    assertTrue(await frontierView.has(betaLabel), `${setName}: latest view should include beta`);
                    assertTrue(await frontierView.has(gammaLabel), `${setName}: latest view should include gamma`);

                    return { alphaHash, betaHash, gammaHash };
                };

                const aHashes = await runConcurrentAdds(setA, 'A');
                const bHashes = await runConcurrentAdds(setB, 'B');

                const combinedVersion = version(
                    aHashes.betaHash,
                    aHashes.gammaHash,
                    bHashes.betaHash,
                    bHashes.gammaHash
                );

                const combinedViewA = await setA.getView(combinedVersion);
                const combinedViewB = await setB.getView(combinedVersion);

                assertTrue(await combinedViewA.has('alpha'), 'A: combined view should include alpha');
                assertTrue(await combinedViewA.has('beta'), 'A: combined view should include beta');
                assertTrue(await combinedViewA.has('gamma'), 'A: combined view should include gamma');

                assertTrue(await combinedViewB.has('alpha'), 'B: combined view should include alpha');
                assertTrue(await combinedViewB.has('beta'), 'B: combined view should include beta');
                assertTrue(await combinedViewB.has('gamma'), 'B: combined view should include gamma');

                const bothBetaVersion = version(aHashes.betaHash, bHashes.betaHash);
                const bothBetaViewA = await setA.getView(bothBetaVersion);
                const bothBetaViewB = await setB.getView(bothBetaVersion);

                assertTrue(await bothBetaViewA.has('alpha'), 'A @bothBeta: should see alpha');
                assertTrue(await bothBetaViewA.has('beta'), 'A @bothBeta: should see beta');
                assertFalse(await bothBetaViewA.has('gamma'), 'A @bothBeta: should not see gamma');

                assertTrue(await bothBetaViewB.has('alpha'), 'B @bothBeta: should see alpha');
                assertTrue(await bothBetaViewB.has('beta'), 'B @bothBeta: should see beta');
                assertFalse(await bothBetaViewB.has('gamma'), 'B @bothBeta: should not see gamma');

                const bothGammaVersion = version(aHashes.gammaHash, bHashes.gammaHash);
                const bothGammaViewA = await setA.getView(bothGammaVersion);
                const bothGammaViewB = await setB.getView(bothGammaVersion);

                assertTrue(await bothGammaViewA.has('alpha'), 'A @bothGamma: should see alpha');
                assertFalse(await bothGammaViewA.has('beta'), 'A @bothGamma: should not see beta');
                assertTrue(await bothGammaViewA.has('gamma'), 'A @bothGamma: should see gamma');

                assertTrue(await bothGammaViewB.has('alpha'), 'B @bothGamma: should see alpha');
                assertFalse(await bothGammaViewB.has('beta'), 'B @bothGamma: should not see beta');
                assertTrue(await bothGammaViewB.has('gamma'), 'B @bothGamma: should see gamma');

                const crossVersion = version(aHashes.betaHash, bHashes.gammaHash);
                const crossViewA = await setA.getView(crossVersion);
                const crossViewB = await setB.getView(crossVersion);

                assertTrue(await crossViewA.has('alpha'), 'A @cross: should see alpha');
                assertTrue(await crossViewA.has('beta'), 'A @cross: should see beta');
                assertFalse(await crossViewA.has('gamma'), 'A @cross: should not see gamma');

                assertTrue(await crossViewB.has('alpha'), 'B @cross: should see alpha');
                assertFalse(await crossViewB.has('beta'), 'B @cross: should not see beta');
                assertTrue(await crossViewB.has('gamma'), 'B @cross: should see gamma');

                const outerView = await outerSet.getView();
                assertTrue(await outerView.hasByHash(hashA), 'outer set should still contain nested set A');
                assertTrue(await outerView.hasByHash(hashB), 'outer set should still contain nested set B');
            }
        },
        {
            name: '[NES03] Test barrier operations with nested elements',
            invoke: async () => {
                const ctx = createTestCtx();

                const outerSetInit = await RSet.create({
                    seed: 'outer-set-barrier',
                    contentType: RSet.typeId,
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                    supportBarrierAdd: true,
                    supportBarrierDelete: true,
                });

                const outerSet = (await ctx.createObject(outerSetInit)) as RSet;

                const makeNestedInit = async (seed: string) => {
                    return RSet.create({
                        seed,
                        initialElements: [],
                        hashAlgorithm: 'sha256',
                        supportBarrierAdd: true,
                        supportBarrierDelete: true,
                    });
                };

                const rootInit = await makeNestedInit('nested-root');
                const rootHash = await outerSet.add(rootInit.payload);
                const rootVersion = version(rootHash);

                const leftInit = await makeNestedInit('nested-left');
                const rightInit = await makeNestedInit('nested-right');

                const leftHash = await outerSet.add(leftInit.payload, rootVersion);
                const rightHash = await outerSet.add(rightInit.payload, rootVersion);

                const leftVersion = version(leftHash);
                const rightVersion = version(rightHash);

                const barrierInit = await makeNestedInit('nested-barrier');
                const barrierHash = await outerSet.addWithBarrier(barrierInit.payload, leftVersion);

                const rightViewAfterBarrierAdd = await outerSet.getView(rightVersion);
                assertTrue(
                    await rightViewAfterBarrierAdd.hasByHash(barrierHash),
                    'barrier add should apply to concurrent branches even with nested elements'
                );

                await outerSet.deleteWithBarrierByHash(rootHash, leftVersion);

                const rightViewAfterBarrierDelete = await outerSet.getView(rightVersion);
                assertFalse(
                    await rightViewAfterBarrierDelete.hasByHash(rootHash),
                    'barrier delete should remove nested elements from concurrent branches'
                );

                const latestView = await outerSet.getView();
                assertTrue(
                    await latestView.hasByHash(barrierHash),
                    'latest view should still include the barrier-added nested element'
                );
                assertFalse(
                    await latestView.hasByHash(rootHash),
                    'latest view should reflect barrier delete of the nested root element'
                );
            }
        },
        {
            name: '[NES04] Test deep barrier operations inside multiple nested sets',
            invoke: async () => {
                const ctx = createTestCtx();

                const outerSetInit = await RSet.create({
                    seed: 'outer-set-deep-barrier',
                    contentType: RSet.typeId,
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                    supportBarrierAdd: true,
                    supportBarrierDelete: true,
                });

                const outerSet = (await ctx.createObject(outerSetInit)) as RSet;

                const addNestedSet = async (seed: string) => {
                    const nestedInit = await RSet.create({
                        seed,
                        initialElements: [],
                        hashAlgorithm: 'sha256',
                        supportBarrierAdd: true,
                        supportBarrierDelete: true,
                    });

                    const nestedHash = await outerSet.add(nestedInit.payload);
                    const outerViewForLoad = await outerSet.getView();
                    const nested = await outerViewForLoad.loadRObjectByHash(nestedHash) as RSet;
                    return { nestedHash, nested };
                };

                const { nestedHash: hashA, nested: setA } = await addNestedSet('nested-deep-A');
                const { nestedHash: hashB, nested: setB } = await addNestedSet('nested-deep-B');

                const runInnerBarrierDeleteShared = async (target: RSet, setName: string) => {
                    const rootAddHash = await target.add('root');
                    const sharedAddHash = await target.add('shared');
                    const baseVersion = version(sharedAddHash);

                    const leftHash = await target.add('left', baseVersion);
                    const rightHash = await target.add('right', baseVersion);

                    const leftVersion = version(leftHash);
                    const rightVersion = version(rightHash);

                    const sharedLiteralHash = crypto.hash(HASH_SHA256).hashToB64(stringToUint8Array(json.toStringNormalized('shared')));
                    const deleteBarrierHash = await target.deleteWithBarrierByHash(sharedLiteralHash, leftVersion);

                    const concurrentView = await target.getView(rightVersion);
                    assertFalse(
                        await concurrentView.has('shared'),
                        `${setName}: barrier delete should remove shared from concurrent branch`
                    );
                    assertTrue(
                        await concurrentView.has('root'),
                        `${setName}: other initial elements should remain unless deleted`
                    );

                    const ancestorView = await target.getView(baseVersion);
                    assertTrue(
                        await ancestorView.has('shared'),
                        `${setName}: ancestor view should still see shared`
                    );

                    const latestView = await target.getView();
                    assertFalse(
                        await latestView.has('shared'),
                        `${setName}: latest view should reflect barrier delete over shared`
                    );
                    assertTrue(
                        await latestView.has('root'),
                        `${setName}: root should still be present in latest view`
                    );

                    return {
                        rootAddHash,
                        sharedAddHash,
                        baseVersion,
                        leftHash,
                        rightHash,
                        leftVersion,
                        rightVersion,
                        deleteBarrierHash,
                    };
                };

                const aHashes = await runInnerBarrierDeleteShared(setA, 'A');
                const bHashes = await runInnerBarrierDeleteShared(setB, 'B');

                const viewAAfter = await setA.getView();
                assertFalse(
                    await viewAAfter.has('shared'),
                    'A after B barrier run: shared should remain deleted in A'
                );
                assertTrue(
                    await viewAAfter.has('root'),
                    'A after B barrier run: root should still be present in A'
                );

                const outerView = await outerSet.getView();
                assertTrue(
                    await outerView.hasByHash(hashA),
                    'outer set should still contain nested set A after deep barrier runs'
                );
                assertTrue(
                    await outerView.hasByHash(hashB),
                    'outer set should still contain nested set B after deep barrier runs'
                );

                const versionADelete_BShared = version(aHashes.deleteBarrierHash, bHashes.sharedAddHash);
                const viewA_Adelete_Bshared = await setA.getView(versionADelete_BShared);
                const viewB_Adelete_Bshared = await setB.getView(versionADelete_BShared);
                assertFalse(await viewA_Adelete_Bshared.has('shared'), 'A@Adelete+Bshared: shared deleted in A');
                assertTrue(await viewB_Adelete_Bshared.has('shared'), 'B@Adelete+Bshared: shared still present in B');

                const versionBDelete_AShared = version(bHashes.deleteBarrierHash, aHashes.sharedAddHash);
                const viewA_Bdelete_Ashared = await setA.getView(versionBDelete_AShared);
                const viewB_Bdelete_Ashared = await setB.getView(versionBDelete_AShared);
                assertTrue(await viewA_Bdelete_Ashared.has('shared'), 'A@Bdelete+Ashared: shared still present in A');
                assertFalse(await viewB_Bdelete_Ashared.has('shared'), 'B@Bdelete+Ashared: shared deleted in B');

                const versionBothDeletes = version(aHashes.deleteBarrierHash, bHashes.deleteBarrierHash);
                const viewA_bothDeletes = await setA.getView(versionBothDeletes);
                const viewB_bothDeletes = await setB.getView(versionBothDeletes);
                assertFalse(await viewA_bothDeletes.has('shared'), 'A@bothDeletes: shared deleted in A');
                assertFalse(await viewB_bothDeletes.has('shared'), 'B@bothDeletes: shared deleted in B');
            }
        },
        {
            name: '[NES05] Nested creation applies initialElements for nested RSet',
            invoke: async () => {
                const ctx = createTestCtx();

                const outerSetInit = await RSet.create({
                    seed: 'outer-set-init-elements',
                    contentType: RSet.typeId,
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                    supportBarrierAdd: true,
                    supportBarrierDelete: true,
                });

                const outerSet = (await ctx.createObject(outerSetInit)) as RSet;

                const nestedInit = await RSet.create({
                    seed: 'nested-with-initials',
                    initialElements: ['alpha', 'beta'],
                    hashAlgorithm: 'sha256',
                    supportBarrierAdd: true,
                    supportBarrierDelete: true,
                });

                const nestedHash = await outerSet.add(nestedInit.payload);

                const outerView = await outerSet.getView();
                const nestedSet = await outerView.loadRObjectByHash(nestedHash) as RSet;

                const nestedView = await nestedSet.getView();
                assertTrue(await nestedView.has('alpha'), 'nested initialElements should include alpha');
                assertTrue(await nestedView.has('beta'), 'nested initialElements should include beta');

                assertTrue(await outerView.hasByHash(nestedHash), 'outer set should contain the nested set with initials');
            }
        },
        {
            name: '[NES06] Initial elements survive history; barrier delete propagates to concurrent branch',
            invoke: async () => {
                const ctx = createTestCtx();

                const outerSetInit = await RSet.create({
                    seed: 'outer-set-barrier-initials',
                    contentType: RSet.typeId,
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                    supportBarrierAdd: true,
                    supportBarrierDelete: true,
                });

                const outerSet = (await ctx.createObject(outerSetInit)) as RSet;

                const nestedInit = await RSet.create({
                    seed: 'nested-initial-barrier',
                    initialElements: ['root', 'remove-me', 'keep-me'],
                    hashAlgorithm: 'sha256',
                    supportBarrierAdd: true,
                    supportBarrierDelete: true,
                });

                const nestedHash = await outerSet.add(nestedInit.payload);
                const outerView = await outerSet.getView();
                const nestedSet = await outerView.loadRObjectByHash(nestedHash) as RSet;

                const creationVersion = version(nestedSet.getId());

                const creationView = await nestedSet.getView(creationVersion);
                assertTrue(await creationView.has('root'), 'creation view should see root');
                assertTrue(await creationView.has('remove-me'), 'creation view should see remove-me');
                assertTrue(await creationView.has('keep-me'), 'creation view should see keep-me');

                await nestedSet.deleteWithBarrier('remove-me');

                const latestView = await nestedSet.getView();
                assertFalse(await latestView.has('remove-me'), 'latest view should not include remove-me after barrier delete');
                assertTrue(await latestView.has('root'), 'latest view should include root');
                assertTrue(await latestView.has('keep-me'), 'latest view should include keep-me');

                const concurrentHash = await nestedSet.add('concurrent-add', creationVersion);
                const concurrentVersion = version(concurrentHash);
                const concurrentView = await nestedSet.getView(concurrentVersion);

                assertFalse(await concurrentView.has('remove-me'), 'concurrent branch should also lose remove-me due to barrier delete');
                assertTrue(await concurrentView.has('root'), 'concurrent branch should still see root');
                assertTrue(await concurrentView.has('keep-me'), 'concurrent branch should still see keep-me');
                assertTrue(await concurrentView.has('concurrent-add'), 'concurrent branch should see its own addition');

                assertTrue(await outerView.hasByHash(nestedHash), 'outer set should contain the nested set');
            }
        },
        {
            name: '[NES07] Test three-level nested sets and deep element isolation',
            invoke: async () => {
                const ctx = createTestCtx();

                const outerInit = await RSet.create({
                    seed: 'outer-set-three-level',
                    contentType: RSet.typeId,
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                    supportBarrierAdd: true,
                    supportBarrierDelete: true,
                });

                const outerSet = (await ctx.createObject(outerInit)) as RSet;

                const midInit = await RSet.create({
                    seed: 'mid-set-three-level',
                    initialElements: [],
                    contentType: RSet.typeId,
                    hashAlgorithm: 'sha256',
                    supportBarrierAdd: true,
                    supportBarrierDelete: true,
                });

                const midHash = await outerSet.add(midInit.payload);
                const outerView = await outerSet.getView();
                const midSet = (await outerView.loadRObjectByHash(midHash)) as RSet;

                const innerInit = await RSet.create({
                    seed: 'inner-set-three-level',
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                    supportBarrierAdd: true,
                    supportBarrierDelete: true,
                });

                const innerHash = await midSet.add(innerInit.payload);
                const midView = await midSet.getView();
                const innerSet = (await midView.loadRObjectByHash(innerHash)) as RSet;

                await innerSet.add('leaf-alpha');
                await innerSet.add('leaf-beta');
                await innerSet.delete('leaf-beta');
                await innerSet.add('leaf-gamma');

                const innerView = await innerSet.getView();
                assertTrue(await innerView.has('leaf-alpha'), 'inner set should contain leaf-alpha');
                assertFalse(await innerView.has('leaf-beta'), 'inner set should not contain deleted leaf-beta');
                assertTrue(await innerView.has('leaf-gamma'), 'inner set should contain leaf-gamma');

                assertTrue(await midView.hasByHash(innerHash), 'mid set should contain the inner nested set');
                const midLatest = await midSet.getView();
                assertTrue(await midLatest.hasByHash(innerHash), 'mid set latest view should still contain inner set');

                const outerLatest = await outerSet.getView();
                assertTrue(await outerLatest.hasByHash(midHash), 'outer set should contain the mid-level nested set');

                const reloadedOuterView = await outerSet.getView();
                const reloadedMid = (await reloadedOuterView.loadRObjectByHash(midHash)) as RSet;
                const reloadedMidView = await reloadedMid.getView();
                const reloadedInner = (await reloadedMidView.loadRObjectByHash(innerHash)) as RSet;
                const reloadedInnerView = await reloadedInner.getView();

                assertTrue(await reloadedInnerView.has('leaf-alpha'), 'reloaded inner set should contain leaf-alpha');
                assertTrue(await reloadedInnerView.has('leaf-gamma'), 'reloaded inner set should contain leaf-gamma');
                assertFalse(await reloadedInnerView.has('leaf-beta'), 'reloaded inner set should still exclude leaf-beta');
            }
        },
        {
            name: '[NES08] Regular deleteByHash removes a nested set while siblings remain',
            invoke: async () => {
                const ctx = createTestCtx();

                const outerSetInit = await RSet.create({
                    seed: 'outer-set-regular-delete',
                    contentType: RSet.typeId,
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                    supportBarrierAdd: true,
                    supportBarrierDelete: true,
                });

                const outerSet = (await ctx.createObject(outerSetInit)) as RSet;

                const addNestedSet = async (seed: string) => {
                    const nestedInit = await RSet.create({
                        seed,
                        initialElements: [],
                        hashAlgorithm: 'sha256',
                        supportBarrierAdd: true,
                        supportBarrierDelete: true,
                    });

                    const nestedHash = await outerSet.add(nestedInit.payload);
                    const outerView = await outerSet.getView();
                    const nested = await outerView.loadRObjectByHash(nestedHash) as RSet;
                    return { nestedHash, nested };
                };

                const { nestedHash: hashA, nested: setA } = await addNestedSet('nested-del-A');
                const { nestedHash: hashB, nested: setB } = await addNestedSet('nested-del-B');
                const { nestedHash: hashC, nested: setC } = await addNestedSet('nested-del-C');

                await setA.add('a1');
                await setB.add('b1');
                await setC.add('c1');

                const viewBefore = await outerSet.getView();
                assertTrue(await viewBefore.hasByHash(hashA), 'outer set should contain nested set A before delete');
                assertTrue(await viewBefore.hasByHash(hashB), 'outer set should contain nested set B before delete');
                assertTrue(await viewBefore.hasByHash(hashC), 'outer set should contain nested set C before delete');

                await outerSet.deleteByHash(hashB);

                const viewAfter = await outerSet.getView();
                assertTrue(await viewAfter.hasByHash(hashA), 'nested set A should survive deletion of B');
                assertFalse(await viewAfter.hasByHash(hashB), 'nested set B should be removed after deleteByHash');
                assertTrue(await viewAfter.hasByHash(hashC), 'nested set C should survive deletion of B');

                const reloadedA = await viewAfter.loadRObjectByHash(hashA) as RSet;
                const reloadedAView = await reloadedA.getView();
                assertTrue(await reloadedAView.has('a1'), 'nested set A should still contain its elements');

                const reloadedC = await viewAfter.loadRObjectByHash(hashC) as RSet;
                const reloadedCView = await reloadedC.getView();
                assertTrue(await reloadedCView.has('c1'), 'nested set C should still contain its elements');
            }
        },
    ],
};
