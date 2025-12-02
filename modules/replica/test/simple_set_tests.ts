import { DagContext, DagReplica, DagStorageProvider } from "../src/dag/dag_replica";
import { TypeRegistryMap, version } from "../src/replica";
import { RSet } from "../src/types/rset";
import { assertTrue, assertFalse } from "@hyper-hyper-space/hhs3_util/dist/test";
import { createMemDagStorageProvider } from "dag/mem_dag_storage";
import { json } from "@hyper-hyper-space/hhs3_json";
import { sha } from "@hyper-hyper-space/hhs3_crypto";

const createReplica = (storageProvider?: DagStorageProvider): DagReplica => {
    const registry = new TypeRegistryMap<DagContext>();

    registry.register(
        RSet.typeId,
        RSet.load,
        RSet.validateCreatePayload
    );

    return new DagReplica(registry, storageProvider || createMemDagStorageProvider());
};

const createTestEnvironment = async () => {
    const storageProvider = createMemDagStorageProvider();
    const replica = createReplica(storageProvider);

    const set = await RSet.create(
        {
            seed: 'set00',
            elementsTypeId: 'json/string',
            elements: [],
            hashAlgorithm: 'sha256',
            supportBarrierAdd: true,
            supportBarrierDelete: true,
        },
        replica
    );

    return { replica, set, setId: set.getId() };
};

export const simpleSetTests = {
    title: '[SET00] Small set tests wit flat DAG indexing',
    tests: [
        {
            name: '[SET00] Test sequentiall additions and deletions',
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
        }
    ],
};