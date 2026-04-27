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

const createTestEnvironment = async (initialElements: Array<json.Literal> = []) => {
    const ctx = createTestCtx();

    const init = await RSet.create(
        {
            seed: 'set00',
            initialElements: initialElements,
            hashAlgorithm: 'sha256',
            supportBarrierAdd: true,
            supportBarrierDelete: true,
        }
    );

    const set = (await ctx.createObject(init)) as RSet;

    return { ctx, set, setId: set.getId() };
};

export const simpleSetTests = {
    title: '[SET] Small set tests with flat DAG indexing',
    tests: [
        {
            name: '[SET00] Test sequential additions and deletions',
            invoke: async () => {
                const { set } = await createTestEnvironment();

                await set.add('alpha');
                await set.addWithBarrier('beta');
                await set.add('gamma');
                await set.addWithBarrier('delta');

                await set.delete('beta');
                await set.deleteWithBarrier('gamma');

                await set.add('epsilon');
                await set.deleteWithBarrier('delta');

                const view = await set.getView();

                assertTrue(await view.has('alpha'), 'alpha should remain present');
                assertTrue(await view.has('epsilon'), 'epsilon should remain present');
                assertFalse(await view.has('beta'), 'beta should have been removed');
                assertFalse(await view.has('gamma'), 'gamma should have been removed');
                assertFalse(await view.has('delta'), 'delta should have been barrier-deleted');
            }
        },
        {
            name: '[SET01] Test concurrent additions and historical views',
            invoke: async () => {
                const { set } = await createTestEnvironment();

                const alphaHash = await set.add('alpha');
                const alphaVersion = version(alphaHash);

                const betaHash = await set.add('beta', alphaVersion);
                const betaVersion = version(betaHash);

                const gammaHash = await set.add('gamma', alphaVersion);
                const gammaVersion = version(gammaHash);

                const ancestorView = await set.getView(alphaVersion);
                assertTrue(await ancestorView.has('alpha'), 'alpha should be visible at its own version');
                assertFalse(await ancestorView.has('beta'), 'beta should not exist before it was added');
                assertFalse(await ancestorView.has('gamma'), 'gamma should not exist before it was added');

                const betaView = await set.getView(betaVersion);
                assertTrue(await betaView.has('alpha'), 'alpha should be visible when reading beta branch');
                assertTrue(await betaView.has('beta'), 'beta should be visible when reading beta branch');
                assertFalse(await betaView.has('gamma'), 'gamma should not leak into beta branch view');

                const gammaView = await set.getView(gammaVersion);
                assertTrue(await gammaView.has('alpha'), 'alpha should be visible when reading gamma branch');
                assertTrue(await gammaView.has('gamma'), 'gamma should be visible when reading gamma branch');
                assertFalse(await gammaView.has('beta'), 'beta should not leak into gamma branch view');

                const frontierView = await set.getView();
                assertTrue(await frontierView.has('beta'), 'latest view should include beta');
                assertTrue(await frontierView.has('gamma'), 'latest view should include gamma');
            }
        },
        {
            name: '[SET02] Test barrier operations across concurrent versions',
            invoke: async () => {
                const { set } = await createTestEnvironment();

                const rootHash = await set.add('root');
                const rootVersion = version(rootHash);

                const leftHash = await set.add('left', rootVersion);
                const rightHash = await set.add('right', rootVersion);

                const leftVersion = version(leftHash);
                const rightVersion = version(rightHash);

                await set.addWithBarrier('barrier-delta', leftVersion);

                const rightViewAfterBarrierAdd = await set.getView(rightVersion);
                assertTrue(await rightViewAfterBarrierAdd.has('barrier-delta'), 'barrier add should apply to concurrent versions');

                const rootLiteralHash = crypto.hash(HASH_SHA256).hashToB64(stringToUint8Array(json.toStringNormalized('root')));
                await set.deleteWithBarrierByHash(rootLiteralHash, leftVersion);

                const rightViewAfterBarrierDelete = await set.getView(rightVersion);
                assertFalse(await rightViewAfterBarrierDelete.has('root'), 'barrier delete should remove elements from concurrent versions');

                const latestView = await set.getView();
                assertTrue(await latestView.has('barrier-delta'), 'latest view should still include barrier add');
                assertFalse(await latestView.has('root'), 'latest view should reflect the barrier delete');
            }
        },
        {
            name: '[SET03] Test reading initial elements across history',
            invoke: async () => {
                const { set } = await createTestEnvironment(['alpha', 'beta']);

                const creationVersion = version(set.getId());
                const initialView = await set.getView(creationVersion);
                assertTrue(await initialView.has('alpha'), 'alpha should be present at creation');
                assertTrue(await initialView.has('beta'), 'beta should be present at creation');

                await set.add('gamma');
                await set.delete('alpha');

                const latestView = await set.getView();
                assertFalse(await latestView.has('alpha'), 'alpha should have been deleted from the latest view');
                assertTrue(await latestView.has('beta'), 'beta should still be present in the latest view');
                assertTrue(await latestView.has('gamma'), 'gamma should be present after being added');

                const historicalView = await set.getView(creationVersion);
                assertTrue(await historicalView.has('alpha'), 'historical view should retain alpha');
                assertTrue(await historicalView.has('beta'), 'historical view should retain beta');
                assertFalse(await historicalView.has('gamma'), 'historical view should not include future elements');
            }
        },
        {
            name: '[SET04] Test barrier deletes affecting initial elements',
            invoke: async () => {
                const { set } = await createTestEnvironment(['root', 'shared']);

                const creationVersion = version(set.getId());

                const leftHash = await set.add('left', creationVersion);
                const rightHash = await set.add('right', creationVersion);

                const leftVersion = version(leftHash);
                const rightVersion = version(rightHash);

                const sharedHash = crypto.hash(HASH_SHA256).hashToB64(stringToUint8Array(json.toStringNormalized('shared')));
                await set.deleteWithBarrierByHash(sharedHash, leftVersion);

                const concurrentView = await set.getView(rightVersion);
                assertFalse(await concurrentView.has('shared'), 'barrier delete should remove shared from concurrent branch');
                assertTrue(await concurrentView.has('root'), 'other initial elements should remain unless deleted');

                const ancestorView = await set.getView(creationVersion);
                assertTrue(await ancestorView.has('shared'), 'ancestor view should still see shared');

                const latestView = await set.getView();
                assertFalse(await latestView.has('shared'), 'latest view should reflect barrier delete over initial element');
            }
        },
        {
            name: '[SET05] Test delete then re-add of the same element',
            invoke: async () => {
                const { set } = await createTestEnvironment();

                await set.add('alpha');
                await set.add('beta');

                const viewBefore = await set.getView();
                assertTrue(await viewBefore.has('alpha'), 'alpha should be present after initial add');
                assertTrue(await viewBefore.has('beta'), 'beta should be present after initial add');

                await set.delete('alpha');

                const viewAfterDelete = await set.getView();
                assertFalse(await viewAfterDelete.has('alpha'), 'alpha should be gone after delete');
                assertTrue(await viewAfterDelete.has('beta'), 'beta should survive unrelated delete');

                await set.add('alpha');

                const viewAfterReAdd = await set.getView();
                assertTrue(await viewAfterReAdd.has('alpha'), 'alpha should reappear after re-add');
                assertTrue(await viewAfterReAdd.has('beta'), 'beta should still be present');

                await set.delete('alpha');
                await set.add('alpha');

                const viewAfterSecondCycle = await set.getView();
                assertTrue(await viewAfterSecondCycle.has('alpha'), 'alpha should survive a second delete/re-add cycle');
            }
        },
        {
            name: '[SET06] Test delete then re-add of an initial element',
            invoke: async () => {
                const { set } = await createTestEnvironment(['alpha', 'beta']);

                const viewInitial = await set.getView();
                assertTrue(await viewInitial.has('alpha'), 'alpha should be present from initial elements');

                await set.delete('alpha');

                const viewAfterDelete = await set.getView();
                assertFalse(await viewAfterDelete.has('alpha'), 'alpha should be gone after delete');
                assertTrue(await viewAfterDelete.has('beta'), 'beta should survive unrelated delete');

                await set.add('alpha');

                const viewAfterReAdd = await set.getView();
                assertTrue(await viewAfterReAdd.has('alpha'), 'alpha should reappear after re-add');
                assertTrue(await viewAfterReAdd.has('beta'), 'beta should still be present');
            }
        },
        {
            name: '[SET07] Test barrier-delete then re-add of the same element',
            invoke: async () => {
                const { set } = await createTestEnvironment();

                const addHash = await set.add('alpha');
                const addVersion = version(addHash);

                await set.deleteWithBarrier('alpha');

                const viewAfterBarrierDelete = await set.getView();
                assertFalse(await viewAfterBarrierDelete.has('alpha'), 'alpha should be gone after barrier delete');

                await set.add('alpha');

                const viewAfterReAdd = await set.getView();
                assertTrue(await viewAfterReAdd.has('alpha'), 'alpha should reappear after re-add following barrier delete');

                const historicalView = await set.getView(addVersion);
                assertTrue(await historicalView.has('alpha'), 'historical view should still see alpha at its original add');
            }
        },
        {
            name: '[SET08] Set without barrier support: basic add and delete work',
            invoke: async () => {
                const ctx = createTestCtx();

                const init = await RSet.create({
                    seed: 'no-barrier-set',
                    initialElements: ['alpha'],
                    hashAlgorithm: 'sha256',
                });

                const set = (await ctx.createObject(init)) as RSet;

                const view0 = await set.getView();
                assertTrue(await view0.has('alpha'), 'initial element alpha should be present');

                await set.add('beta');
                await set.add('gamma');

                const view1 = await set.getView();
                assertTrue(await view1.has('alpha'), 'alpha should still be present');
                assertTrue(await view1.has('beta'), 'beta should be present after add');
                assertTrue(await view1.has('gamma'), 'gamma should be present after add');

                await set.delete('beta');

                const view2 = await set.getView();
                assertFalse(await view2.has('beta'), 'beta should be gone after delete');
                assertTrue(await view2.has('alpha'), 'alpha should survive unrelated delete');
                assertTrue(await view2.has('gamma'), 'gamma should survive unrelated delete');
            }
        },
        {
            name: '[SET09] Set without barrier support: addWithBarrier throws',
            invoke: async () => {
                const ctx = createTestCtx();

                const init = await RSet.create({
                    seed: 'no-barrier-add-set',
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                });

                const set = (await ctx.createObject(init)) as RSet;

                let threw = false;
                try {
                    await set.addWithBarrier('alpha');
                } catch (e) {
                    threw = true;
                }
                assertTrue(threw, 'addWithBarrier should throw when barrier add is not supported');
            }
        },
        {
            name: '[SET10] Set without barrier support: deleteWithBarrier throws',
            invoke: async () => {
                const ctx = createTestCtx();

                const init = await RSet.create({
                    seed: 'no-barrier-delete-set',
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                });

                const set = (await ctx.createObject(init)) as RSet;

                await set.add('alpha');

                let threw = false;
                try {
                    await set.deleteWithBarrier('alpha');
                } catch (e) {
                    threw = true;
                }
                assertTrue(threw, 'deleteWithBarrier should throw when barrier delete is not supported');
            }
        },
        {
            name: '[SET11] Deleting a non-existent element throws when acceptRedundantDelete is false',
            invoke: async () => {
                const { set } = await createTestEnvironment();

                let threw = false;
                try {
                    await set.delete('nonexistent');
                } catch (e) {
                    threw = true;
                }
                assertTrue(threw, 'delete of non-existent element should throw');

                await set.add('alpha');
                const view = await set.getView();
                assertTrue(await view.has('alpha'), 'set should remain usable after a failed delete');
            }
        },
    ],
};
