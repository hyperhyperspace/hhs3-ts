import { assertTrue, assertFalse } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256 } from "@hyper-hyper-space/hhs3_crypto";
import { Replica, MemDagBackend } from "../src/index.js";
import { version } from "@hyper-hyper-space/hhs3_mvt";
import { RSet, rSetFactory } from "@hyper-hyper-space/hhs3_std_types";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

function createTestReplica() {
    const replica = new Replica({ crypto, hashSuite, config: { selfValidate: true } });
    replica.attachBackend('default', new MemDagBackend(hashSuite));
    replica.registerType(RSet.typeId, rSetFactory);
    return replica;
}

export const replicaNestedTests = {
    title: '[REP-NESTED] Nested objects through Replica',
    tests: [
        {
            name: '[RN00] Outer set with nested set via Replica',
            invoke: async () => {
                const replica = createTestReplica();

                const outerInit = await RSet.create({
                    seed: 'outer',
                    contentType: RSet.typeId,
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                    supportBarrierAdd: true,
                    supportBarrierDelete: true,
                });

                const outerSet = (await replica.createObject(outerInit)) as RSet;

                const innerInit = await RSet.create({
                    seed: 'inner-1',
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                    supportBarrierAdd: true,
                    supportBarrierDelete: true,
                });

                const innerHash = await outerSet.add(innerInit.payload);
                const outerView = await outerSet.getView();
                const innerSet = (await outerView.loadRObjectByHash(innerHash)) as RSet;

                await innerSet.add('alpha');
                await innerSet.add('beta');

                const innerView = await innerSet.getView();
                assertTrue(await innerView.has('alpha'), 'inner should contain alpha');
                assertTrue(await innerView.has('beta'), 'inner should contain beta');
                assertTrue(await outerView.hasByHash(innerHash), 'outer should contain inner');
            },
        },
        {
            name: '[RN01] Concurrent inner operations across multiple nested sets',
            invoke: async () => {
                const replica = createTestReplica();

                const outerInit = await RSet.create({
                    seed: 'outer-multi',
                    contentType: RSet.typeId,
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                    supportBarrierAdd: true,
                    supportBarrierDelete: true,
                });

                const outerSet = (await replica.createObject(outerInit)) as RSet;

                const addNested = async (seed: string) => {
                    const init = await RSet.create({
                        seed,
                        initialElements: [],
                        hashAlgorithm: 'sha256',
                        supportBarrierAdd: true,
                        supportBarrierDelete: true,
                    });
                    const hash = await outerSet.add(init.payload);
                    const view = await outerSet.getView();
                    return (await view.loadRObjectByHash(hash)) as RSet;
                };

                const setA = await addNested('nested-a');
                const setB = await addNested('nested-b');

                await setA.add('shared');
                await setA.add('a-only');
                await setB.add('shared');
                await setB.add('b-only');

                const viewA = await setA.getView();
                const viewB = await setB.getView();

                assertTrue(await viewA.has('shared'), 'A should have shared');
                assertTrue(await viewA.has('a-only'), 'A should have a-only');
                assertFalse(await viewA.has('b-only'), 'A should not see b-only');

                assertTrue(await viewB.has('shared'), 'B should have shared');
                assertTrue(await viewB.has('b-only'), 'B should have b-only');
                assertFalse(await viewB.has('a-only'), 'B should not see a-only');
            },
        },
        {
            name: '[RN02] Three-level nesting through Replica',
            invoke: async () => {
                const replica = createTestReplica();

                const outerInit = await RSet.create({
                    seed: 'outer-3level',
                    contentType: RSet.typeId,
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                    supportBarrierAdd: true,
                    supportBarrierDelete: true,
                });

                const outerSet = (await replica.createObject(outerInit)) as RSet;

                const midInit = await RSet.create({
                    seed: 'mid',
                    contentType: RSet.typeId,
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                    supportBarrierAdd: true,
                    supportBarrierDelete: true,
                });

                const midHash = await outerSet.add(midInit.payload);
                const outerView = await outerSet.getView();
                const midSet = (await outerView.loadRObjectByHash(midHash)) as RSet;

                const innerInit = await RSet.create({
                    seed: 'inner-leaf',
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                    supportBarrierAdd: true,
                    supportBarrierDelete: true,
                });

                const innerHash = await midSet.add(innerInit.payload);
                const midView = await midSet.getView();
                const innerSet = (await midView.loadRObjectByHash(innerHash)) as RSet;

                await innerSet.add('leaf-val');
                const innerView = await innerSet.getView();
                assertTrue(await innerView.has('leaf-val'), 'leaf should have leaf-val');

                assertTrue(await midView.hasByHash(innerHash), 'mid should contain inner');
                assertTrue(await outerView.hasByHash(midHash), 'outer should contain mid');
            },
        },
        {
            name: '[RN03] Nested set survives restart via shared backend',
            invoke: async () => {
                const backend = new MemDagBackend(hashSuite);

                const r1 = new Replica({ crypto, hashSuite, config: { selfValidate: true } });
                r1.attachBackend('default', backend);
                r1.registerType(RSet.typeId, rSetFactory);

                const outerInit = await RSet.create({
                    seed: 'outer-persist',
                    contentType: RSet.typeId,
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                    supportBarrierAdd: true,
                    supportBarrierDelete: true,
                });

                const outerSet = (await r1.createObject(outerInit)) as RSet;

                const innerInit = await RSet.create({
                    seed: 'inner-persist',
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                });

                const innerHash = await outerSet.add(innerInit.payload);
                const view = await outerSet.getView();
                const innerSet = (await view.loadRObjectByHash(innerHash)) as RSet;
                await innerSet.add('persisted');

                const r2 = new Replica({ crypto, hashSuite, config: { selfValidate: true } });
                r2.attachBackend('default', backend);
                r2.registerType(RSet.typeId, rSetFactory);
                const restoredOuter = (await r2.createObject(outerInit)) as RSet;
                assertTrue(restoredOuter !== undefined, 'outer should be restored');

                const restoredOuterView = await restoredOuter.getView();
                assertTrue(await restoredOuterView.hasByHash(innerHash), 'outer should still contain inner');

                const restoredInner = (await restoredOuterView.loadRObjectByHash(innerHash)) as RSet;
                const restoredInnerView = await restoredInner.getView();
                assertTrue(await restoredInnerView.has('persisted'), 'inner should still have persisted element');
            },
        },
    ],
};
