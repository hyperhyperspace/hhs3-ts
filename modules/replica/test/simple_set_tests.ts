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

const createTestEnvironment = async (initialElements: Array<json.Literal> = []) => {
    const storageProvider = createMemDagResourceProvider();
    const replica = createReplica(storageProvider);

    const init = await RSet.create(
        {
            seed: 'set00',
            initialElements: initialElements,
            hashAlgorithm: 'sha256',
            supportBarrierAdd: true,
            supportBarrierDelete: true,
        }
    );

    const setId = await replica.addObject(init)
    const set = (await replica.getObject(setId)) as RSet;

    return { replica, set, setId };
};

export const simpleSetTests = {
    title: '[SET00] Small set tests with flat DAG indexing',
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

                const rootLiteralHash = await sha.sha256(json.toStringNormalized('root'));
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

                const sharedHash = await sha.sha256(json.toStringNormalized('shared'));
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
    ],
};