import { DagResource, DagResourceProvider } from "../src/dag/dag_resource";
import { Replica, ResourcesBase, TypeRegistryMap, version } from "../src/replica";
import { RSet, rSetFactory, RSetResources } from "../src/types/rset";
import { assertTrue, assertFalse } from "@hyper-hyper-space/hhs3_util/dist/test";
import { createMemDagResourceProvider } from "dag/mem_dag_storage";
import { json } from "@hyper-hyper-space/hhs3_json";
import { sha } from "@hyper-hyper-space/hhs3_crypto";

const createReplica = (resourceProvider?: DagResourceProvider): Replica<RSetResources> => {
    const registry = new TypeRegistryMap<ResourcesBase & DagResource>();

    registry.register(
        RSet.typeId,
        rSetFactory
    );

    return new Replica(registry, resourceProvider || createMemDagResourceProvider());
};

export const nestedSetTests = {
    title: '[NESTED00] Nested set tests',
    tests: [
        {
            name: '[NES00] Test adding a nested set and inserting elements into it',
            invoke: async () => {
                const storageProvider = createMemDagResourceProvider();
                const replica = createReplica(storageProvider);

                // Create the outer set that will contain nested sets
                const outerSetInit = await RSet.create({
                    seed: 'outer-set',
                    contentType: RSet.typeId,
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                    supportBarrierAdd: true,
                    supportBarrierDelete: true,
                });

                const outerSetId = await replica.addObject(outerSetInit);
                const outerSet = (await replica.getObject(outerSetId)) as RSet;

                // Add a nested set to the outer set
                const nestedSetPayload = await RSet.create({
                    seed: 'nested-set-1',
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                    supportBarrierAdd: true,
                    supportBarrierDelete: true,
                });

                const nestedSetHash = await outerSet.add(nestedSetPayload.payload);
                
                // Get the outer set view and load the nested set
                const outerView = await outerSet.getView();
                const nestedSet = await outerView.loadRObjectByHash(nestedSetHash) as RSet;

                // Add some strings to the nested set
                await nestedSet.add('alpha');
                await nestedSet.add('beta');
                await nestedSet.add('gamma');

                // Verify the nested set contains the expected elements
                const nestedView = await nestedSet.getView();
                assertTrue(await nestedView.has('alpha'), 'nested set should contain alpha');
                assertTrue(await nestedView.has('beta'), 'nested set should contain beta');
                assertTrue(await nestedView.has('gamma'), 'nested set should contain gamma');

                // Verify the outer set contains the nested set
                assertTrue(await outerView.hasByHash(nestedSetHash), 'outer set should contain the nested set');

                // Test deletion from nested set
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
                const storageProvider = createMemDagResourceProvider();
                const replica = createReplica(storageProvider);

                // Create the outer set that will contain nested sets
                const outerSetInit = await RSet.create({
                    seed: 'outer-set-multi',
                    contentType: RSet.typeId,
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                    supportBarrierAdd: true,
                    supportBarrierDelete: true,
                });

                const outerSetId = await replica.addObject(outerSetInit);
                const outerSet = (await replica.getObject(outerSetId)) as RSet;

                // Helper to create and add a nested set, returning its hash and instance
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

                // Sequential operations on each nested set, with overlapping element names
                await setA.add('shared');
                await setA.add('a1');
                await setA.add('a2');
                await setA.delete('a2');

                await setB.add('shared');
                await setB.add('b1');

                await setC.add('shared');
                await setC.add('c1');
                await setC.delete('shared'); // remove shared only from C

                const viewA = await setA.getView();
                const viewB = await setB.getView();
                const viewC = await setC.getView();

                // Check independence of sets and overlapping elements
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
                const storageProvider = createMemDagResourceProvider();
                const replica = createReplica(storageProvider);

                // Create the outer set that will contain nested sets
                const outerSetInit = await RSet.create({
                    seed: 'outer-set-concurrent',
                    contentType: RSet.typeId,
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                    supportBarrierAdd: true,
                    supportBarrierDelete: true,
                });

                const outerSetId = await replica.addObject(outerSetInit);
                const outerSet = (await replica.getObject(outerSetId)) as RSet;

                // Helper to create and add a nested set, returning its hash and instance
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

                // Helper to generate concurrent additions inside a nested set, mirroring [SET01].
                // Uses the same element names in all nested sets to ensure they remain independent.
                // `setName` is only used to annotate assertion messages; it does not affect data.
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

                    // Historical view at alpha
                    const ancestorView = await target.getView(alphaVersion);
                    assertTrue(await ancestorView.has(alphaLabel), `${setName}: alpha should be visible at its own version`);
                    assertFalse(await ancestorView.has(betaLabel), `${setName}: beta should not exist before it was added`);
                    assertFalse(await ancestorView.has(gammaLabel), `${setName}: gamma should not exist before it was added`);

                    // Beta branch view
                    const betaView = await target.getView(betaVersion);
                    assertTrue(await betaView.has(alphaLabel), `${setName}: alpha should be visible when reading beta branch`);
                    assertTrue(await betaView.has(betaLabel), `${setName}: beta should be visible when reading beta branch`);
                    assertFalse(await betaView.has(gammaLabel), `${setName}: gamma should not leak into beta branch view`);

                    // Gamma branch view
                    const gammaView = await target.getView(gammaVersion);
                    assertTrue(await gammaView.has(alphaLabel), `${setName}: alpha should be visible when reading gamma branch`);
                    assertTrue(await gammaView.has(gammaLabel), `${setName}: gamma should be visible when reading gamma branch`);
                    assertFalse(await gammaView.has(betaLabel), `${setName}: beta should not leak into gamma branch view`);

                    // Frontier view should see both concurrent additions
                    const frontierView = await target.getView();
                    assertTrue(await frontierView.has(betaLabel), `${setName}: latest view should include beta`);
                    assertTrue(await frontierView.has(gammaLabel), `${setName}: latest view should include gamma`);

                    return { alphaHash, betaHash, gammaHash };
                };

                // Run concurrent patterns independently on several nested sets
                const aHashes = await runConcurrentAdds(setA, 'A');
                const bHashes = await runConcurrentAdds(setB, 'B');

                // Build a combined version that includes operations from both nested sets
                const combinedVersion = version(
                    aHashes.betaHash,
                    aHashes.gammaHash,
                    bHashes.betaHash,
                    bHashes.gammaHash
                );

                // At a version that includes hashes from both sets, each nested set should
                // still only see its own operations, even though element names are identical.
                const combinedViewA = await setA.getView(combinedVersion);
                const combinedViewB = await setB.getView(combinedVersion);

                assertTrue(await combinedViewA.has('alpha'), 'A: combined view should include alpha');
                assertTrue(await combinedViewA.has('beta'), 'A: combined view should include beta');
                assertTrue(await combinedViewA.has('gamma'), 'A: combined view should include gamma');

                assertTrue(await combinedViewB.has('alpha'), 'B: combined view should include alpha');
                assertTrue(await combinedViewB.has('beta'), 'B: combined view should include beta');
                assertTrue(await combinedViewB.has('gamma'), 'B: combined view should include gamma');

                // Additional mixed versions to further exercise cuts involving both sets.

                // Version where both sets are at their beta branches (no gamma yet).
                const bothBetaVersion = version(aHashes.betaHash, bHashes.betaHash);
                const bothBetaViewA = await setA.getView(bothBetaVersion);
                const bothBetaViewB = await setB.getView(bothBetaVersion);

                assertTrue(await bothBetaViewA.has('alpha'), 'A @bothBeta: should see alpha');
                assertTrue(await bothBetaViewA.has('beta'), 'A @bothBeta: should see beta');
                assertFalse(await bothBetaViewA.has('gamma'), 'A @bothBeta: should not see gamma');

                assertTrue(await bothBetaViewB.has('alpha'), 'B @bothBeta: should see alpha');
                assertTrue(await bothBetaViewB.has('beta'), 'B @bothBeta: should see beta');
                assertFalse(await bothBetaViewB.has('gamma'), 'B @bothBeta: should not see gamma');

                // Version where both sets are at their gamma branches (no beta yet).
                const bothGammaVersion = version(aHashes.gammaHash, bHashes.gammaHash);
                const bothGammaViewA = await setA.getView(bothGammaVersion);
                const bothGammaViewB = await setB.getView(bothGammaVersion);

                assertTrue(await bothGammaViewA.has('alpha'), 'A @bothGamma: should see alpha');
                assertFalse(await bothGammaViewA.has('beta'), 'A @bothGamma: should not see beta');
                assertTrue(await bothGammaViewA.has('gamma'), 'A @bothGamma: should see gamma');

                assertTrue(await bothGammaViewB.has('alpha'), 'B @bothGamma: should see alpha');
                assertFalse(await bothGammaViewB.has('beta'), 'B @bothGamma: should not see beta');
                assertTrue(await bothGammaViewB.has('gamma'), 'B @bothGamma: should see gamma');

                // Crossed version: A at beta-branch, B at gamma-branch.
                const crossVersion = version(aHashes.betaHash, bHashes.gammaHash);
                const crossViewA = await setA.getView(crossVersion);
                const crossViewB = await setB.getView(crossVersion);

                assertTrue(await crossViewA.has('alpha'), 'A @cross: should see alpha');
                assertTrue(await crossViewA.has('beta'), 'A @cross: should see beta');
                assertFalse(await crossViewA.has('gamma'), 'A @cross: should not see gamma');

                assertTrue(await crossViewB.has('alpha'), 'B @cross: should see alpha');
                assertFalse(await crossViewB.has('beta'), 'B @cross: should not see beta');
                assertTrue(await crossViewB.has('gamma'), 'B @cross: should see gamma');

                // Ensure the outer set still contains all nested sets after concurrent inner updates
                const outerView = await outerSet.getView();
                assertTrue(await outerView.hasByHash(hashA), 'outer set should still contain nested set A');
                assertTrue(await outerView.hasByHash(hashB), 'outer set should still contain nested set B');
            }
        },
        {
            name: '[NES03] Test barrier operations with nested elements',
            invoke: async () => {
                const storageProvider = createMemDagResourceProvider();
                const replica = createReplica(storageProvider);

                // Create the outer set that will contain nested sets, with barrier support enabled.
                const outerSetInit = await RSet.create({
                    seed: 'outer-set-barrier',
                    contentType: RSet.typeId,
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                    supportBarrierAdd: true,
                    supportBarrierDelete: true,
                });

                const outerSetId = await replica.addObject(outerSetInit);
                const outerSet = (await replica.getObject(outerSetId)) as RSet;

                const makeNestedInit = async (seed: string) => {
                    return RSet.create({
                        seed,
                        initialElements: [],
                        hashAlgorithm: 'sha256',
                        supportBarrierAdd: true,
                        supportBarrierDelete: true,
                    });
                };

                // Add a "root" nested set, then fork into two concurrent nested sets.
                const rootInit = await makeNestedInit('nested-root');
                const rootHash = await outerSet.add(rootInit.payload);
                const rootVersion = version(rootHash);

                const leftInit = await makeNestedInit('nested-left');
                const rightInit = await makeNestedInit('nested-right');

                const leftHash = await outerSet.add(leftInit.payload, rootVersion);
                const rightHash = await outerSet.add(rightInit.payload, rootVersion);

                const leftVersion = version(leftHash);
                const rightVersion = version(rightHash);

                // Barrier add of another nested set on the left branch.
                const barrierInit = await makeNestedInit('nested-barrier');
                const barrierHash = await outerSet.addWithBarrier(barrierInit.payload, leftVersion);

                // The right branch should see the barrier-added nested element as well.
                const rightViewAfterBarrierAdd = await outerSet.getView(rightVersion);
                assertTrue(
                    await rightViewAfterBarrierAdd.hasByHash(barrierHash),
                    'barrier add should apply to concurrent branches even with nested elements'
                );

                // Barrier delete of the root nested set from the left branch.
                await outerSet.deleteWithBarrierByHash(rootHash, leftVersion);

                // The right branch should no longer see the root nested element.
                const rightViewAfterBarrierDelete = await outerSet.getView(rightVersion);
                assertFalse(
                    await rightViewAfterBarrierDelete.hasByHash(rootHash),
                    'barrier delete should remove nested elements from concurrent branches'
                );

                // Latest frontier view: barrier element remains, root element does not.
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
                const storageProvider = createMemDagResourceProvider();
                const replica = createReplica(storageProvider);

                // Outer container set holding nested sets with barrier support.
                const outerSetInit = await RSet.create({
                    seed: 'outer-set-deep-barrier',
                    contentType: RSet.typeId,
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                    supportBarrierAdd: true,
                    supportBarrierDelete: true,
                });

                const outerSetId = await replica.addObject(outerSetInit);
                const outerSet = (await replica.getObject(outerSetId)) as RSet;

                // Helper to create an empty nested set with barrier support, add it to the outer set,
                // and load it back as an RSet instance.
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

                // Helper that runs a barrier delete pattern inside a nested set,
                // mirroring [SET04] but at the inner level. It seeds the inner set
                // with 'root' and 'shared' via adds (since initialElements are not
                // applied when nested), then performs concurrent adds and a barrier delete.
                const runInnerBarrierDeleteShared = async (target: RSet, setName: string) => {
                    // Seed the inner set with required elements.
                    const rootAddHash = await target.add('root');
                    const sharedAddHash = await target.add('shared');
                    const baseVersion = version(sharedAddHash);

                    // Concurrent branch adds from the seeded base.
                    const leftHash = await target.add('left', baseVersion);
                    const rightHash = await target.add('right', baseVersion);

                    const leftVersion = version(leftHash);
                    const rightVersion = version(rightHash);

                    const sharedLiteralHash = await sha.sha256(json.toStringNormalized('shared'));
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

                // First, run the deep barrier pattern inside setA and confirm setB is unaffected.
                const aHashes = await runInnerBarrierDeleteShared(setA, 'A');

                // Run the deep barrier pattern inside setB and capture its hashes.
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

                // Outer set should continue to contain both nested sets regardless of inner barrier ops.
                const outerView = await outerSet.getView();
                assertTrue(
                    await outerView.hasByHash(hashA),
                    'outer set should still contain nested set A after deep barrier runs'
                );
                assertTrue(
                    await outerView.hasByHash(hashB),
                    'outer set should still contain nested set B after deep barrier runs'
                );

                // Cross-set mixed versions to verify independence with barrier ops.
                // A delete applied, B still at shared-add (no delete).
                const versionADelete_BShared = version(aHashes.deleteBarrierHash, bHashes.sharedAddHash);
                const viewA_Adelete_Bshared = await setA.getView(versionADelete_BShared);
                const viewB_Adelete_Bshared = await setB.getView(versionADelete_BShared);
                assertFalse(await viewA_Adelete_Bshared.has('shared'), 'A@Adelete+Bshared: shared deleted in A');
                assertTrue(await viewB_Adelete_Bshared.has('shared'), 'B@Adelete+Bshared: shared still present in B');

                // B delete applied, A still at shared-add (no delete).
                const versionBDelete_AShared = version(bHashes.deleteBarrierHash, aHashes.sharedAddHash);
                const viewA_Bdelete_Ashared = await setA.getView(versionBDelete_AShared);
                const viewB_Bdelete_Ashared = await setB.getView(versionBDelete_AShared);
                assertTrue(await viewA_Bdelete_Ashared.has('shared'), 'A@Bdelete+Ashared: shared still present in A');
                assertFalse(await viewB_Bdelete_Ashared.has('shared'), 'B@Bdelete+Ashared: shared deleted in B');

                // Both deletes applied.
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
                const storageProvider = createMemDagResourceProvider();
                const replica = createReplica(storageProvider);

                // Outer set that can hold nested RSet elements.
                const outerSetInit = await RSet.create({
                    seed: 'outer-set-init-elements',
                    contentType: RSet.typeId,
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                    supportBarrierAdd: true,
                    supportBarrierDelete: true,
                });

                const outerSetId = await replica.addObject(outerSetInit);
                const outerSet = (await replica.getObject(outerSetId)) as RSet;

                // Create a nested RSet payload with initialElements.
                const nestedInit = await RSet.create({
                    seed: 'nested-with-initials',
                    initialElements: ['alpha', 'beta'],
                    hashAlgorithm: 'sha256',
                    supportBarrierAdd: true,
                    supportBarrierDelete: true,
                });

                // Add the nested set into the outer set.
                const nestedHash = await outerSet.add(nestedInit.payload);

                // Load the nested set via outer view.
                const outerView = await outerSet.getView();
                const nestedSet = await outerView.loadRObjectByHash(nestedHash) as RSet;

                // Verify the initial elements are present in the nested set.
                const nestedView = await nestedSet.getView();
                assertTrue(await nestedView.has('alpha'), 'nested initialElements should include alpha');
                assertTrue(await nestedView.has('beta'), 'nested initialElements should include beta');

                // Sanity: outer still contains the nested set.
                assertTrue(await outerView.hasByHash(nestedHash), 'outer set should contain the nested set with initials');
            }
        },
        {
            name: '[NES06] Initial elements survive history; barrier delete propagates to concurrent branch',
            invoke: async () => {
                const storageProvider = createMemDagResourceProvider();
                const replica = createReplica(storageProvider);

                // Outer set to hold nested RSet elements.
                const outerSetInit = await RSet.create({
                    seed: 'outer-set-barrier-initials',
                    contentType: RSet.typeId,
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                    supportBarrierAdd: true,
                    supportBarrierDelete: true,
                });

                const outerSetId = await replica.addObject(outerSetInit);
                const outerSet = (await replica.getObject(outerSetId)) as RSet;

                // Nested RSet with initial elements.
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

                // Historical view at creation: all initial elements should be present.
                const creationView = await nestedSet.getView(creationVersion);
                assertTrue(await creationView.has('root'), 'creation view should see root');
                assertTrue(await creationView.has('remove-me'), 'creation view should see remove-me');
                assertTrue(await creationView.has('keep-me'), 'creation view should see keep-me');

                // Apply a barrier delete to one of the initial elements.
                await nestedSet.deleteWithBarrier('remove-me');

                // Latest view should reflect barrier delete.
                const latestView = await nestedSet.getView();
                assertFalse(await latestView.has('remove-me'), 'latest view should not include remove-me after barrier delete');
                assertTrue(await latestView.has('root'), 'latest view should include root');
                assertTrue(await latestView.has('keep-me'), 'latest view should include keep-me');

                // Concurrent branch from creationVersion without the barrier in its history.
                const concurrentHash = await nestedSet.add('concurrent-add', creationVersion);
                const concurrentVersion = version(concurrentHash);
                const concurrentView = await nestedSet.getView(concurrentVersion);

                // Barrier delete should still remove the element in the concurrent branch.
                assertFalse(await concurrentView.has('remove-me'), 'concurrent branch should also lose remove-me due to barrier delete');
                assertTrue(await concurrentView.has('root'), 'concurrent branch should still see root');
                assertTrue(await concurrentView.has('keep-me'), 'concurrent branch should still see keep-me');
                assertTrue(await concurrentView.has('concurrent-add'), 'concurrent branch should see its own addition');

                // Sanity: outer still contains the nested set.
                assertTrue(await outerView.hasByHash(nestedHash), 'outer set should contain the nested set');
            }
        },
        {
            name: '[NES07] Test three-level nested sets and deep element isolation',
            invoke: async () => {
                const storageProvider = createMemDagResourceProvider();
                const replica = createReplica(storageProvider);

                // Outer set that will contain a mid-level nested set.
                const outerInit = await RSet.create({
                    seed: 'outer-set-three-level',
                    contentType: RSet.typeId,
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                    supportBarrierAdd: true,
                    supportBarrierDelete: true,
                });

                const outerId = await replica.addObject(outerInit);
                const outerSet = (await replica.getObject(outerId)) as RSet;

                // Mid-level nested set that will itself hold another nested set plus its own literals.
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

                // Innermost nested set under the mid-level set.
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

                // Operate on the innermost set.
                await innerSet.add('leaf-alpha');
                await innerSet.add('leaf-beta');
                await innerSet.delete('leaf-beta');
                await innerSet.add('leaf-gamma');

                const innerView = await innerSet.getView();
                assertTrue(await innerView.has('leaf-alpha'), 'inner set should contain leaf-alpha');
                assertFalse(await innerView.has('leaf-beta'), 'inner set should not contain deleted leaf-beta');
                assertTrue(await innerView.has('leaf-gamma'), 'inner set should contain leaf-gamma');

                // Mid-level set keeps reference to inner nested set and its own literals are independent.
                assertTrue(await midView.hasByHash(innerHash), 'mid set should contain the inner nested set');
                // midSet has contentType, so it should not expose inner elements as literals.
                const midLatest = await midSet.getView();
                assertTrue(await midLatest.hasByHash(innerHash), 'mid set latest view should still contain inner set');

                // Outer set keeps reference to mid-level set and remains isolated from inner literals.
                const outerLatest = await outerSet.getView();
                assertTrue(await outerLatest.hasByHash(midHash), 'outer set should contain the mid-level nested set');

                // Reload through the outer view to ensure persistence across levels.
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
    ],
};
