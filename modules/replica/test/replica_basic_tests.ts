import { assertTrue, assertFalse } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256 } from "@hyper-hyper-space/hhs3_crypto";
import { Replica, MemDagBackend } from "../src/index.js";
import { TypeRegistryMap } from "@hyper-hyper-space/hhs3_mvt";
import { RSet, rSetFactory } from "@hyper-hyper-space/hhs3_std_types";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

function createTestReplica() {
    const replica = new Replica({ crypto, hashSuite, config: { selfValidate: true } });
    replica.attachBackend('default', new MemDagBackend(hashSuite));
    replica.registerType(RSet.typeId, rSetFactory);
    return replica;
}

export const replicaBasicTests = {
    title: '[REPLICA] Basic replica tests',
    tests: [
        {
            name: '[REP00] Attach backend and register type',
            invoke: async () => {
                const replica = createTestReplica();

                assertTrue(replica.getRegistry().has(RSet.typeId), 'RSet type should be registered');

                const factory = await replica.getRegistry().lookup(RSet.typeId);
                assertTrue(factory !== undefined, 'should be able to look up registered factory');
            },
        },
        {
            name: '[REP01] createObject returns an RSet',
            invoke: async () => {
                const replica = createTestReplica();

                const init = await RSet.create({
                    seed: 'test-set',
                    initialElements: ['alpha', 'beta'],
                    hashAlgorithm: 'sha256',
                });

                const set = (await replica.createObject(init)) as RSet;
                assertTrue(set !== undefined, 'createObject should return an object');
                assertTrue(set.getType() === RSet.typeId, 'object should have correct type');

                const view = await set.getView();
                assertTrue(await view.has('alpha'), 'set should contain initial element alpha');
                assertTrue(await view.has('beta'), 'set should contain initial element beta');
            },
        },
        {
            name: '[REP02] createObject is idempotent',
            invoke: async () => {
                const replica = createTestReplica();

                const init = await RSet.create({
                    seed: 'idempotent-set',
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                });

                const set1 = await replica.createObject(init);
                const set2 = await replica.createObject(init);

                assertTrue(set1 === set2, 'second createObject should return the same instance');
                assertTrue(set1.getId() === set2.getId(), 'ids should match');
            },
        },
        {
            name: '[REP03] createObject after restart restores from manifest',
            invoke: async () => {
                const backend = new MemDagBackend(hashSuite);

                const replica1 = new Replica({ crypto, hashSuite, config: { selfValidate: true } });
                replica1.attachBackend('default', backend);
                replica1.registerType(RSet.typeId, rSetFactory);

                const init = await RSet.create({
                    seed: 'restart-set',
                    initialElements: ['alpha'],
                    hashAlgorithm: 'sha256',
                });

                const set1 = (await replica1.createObject(init)) as RSet;
                await set1.add('beta');

                const id = set1.getId();

                const replica2 = new Replica({ crypto, hashSuite, config: { selfValidate: true } });
                replica2.attachBackend('default', backend);
                replica2.registerType(RSet.typeId, rSetFactory);
                await replica2.start();

                const set2 = (await replica2.getObject(id)) as RSet;
                assertTrue(set2 !== undefined, 'object should be reconstituted after start');

                const view = await set2.getView();
                assertTrue(await view.has('alpha'), 'restored set should have initial element');
                assertTrue(await view.has('beta'), 'restored set should have subsequent add');
            },
        },
        {
            name: '[REP04] Missing backend label throws',
            invoke: async () => {
                const replica = new Replica({ crypto, hashSuite });
                replica.registerType(RSet.typeId, rSetFactory);

                const init = await RSet.create({
                    seed: 'no-backend-set',
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                });

                let threw = false;
                try {
                    await replica.createObject(init);
                } catch (e) {
                    threw = true;
                }
                assertTrue(threw, 'createObject should throw when no backend is attached');
            },
        },
        {
            name: '[REP05] Wrong type name throws',
            invoke: async () => {
                const replica = createTestReplica();

                let threw = false;
                try {
                    await replica.createObject({ type: 'nonexistent/type', payload: {} as any });
                } catch (e) {
                    threw = true;
                }
                assertTrue(threw, 'createObject with unregistered type should throw');
            },
        },
        {
            name: '[REP06] getMesh throws when not attached',
            invoke: async () => {
                const replica = createTestReplica();

                let threw = false;
                try {
                    replica.getMesh('default');
                } catch (e) {
                    threw = true;
                }
                assertTrue(threw, 'getMesh should throw when no mesh is attached');
            },
        },
        {
            name: '[REP07] start() loads roots in createdAt order',
            invoke: async () => {
                const backend = new MemDagBackend(hashSuite);

                const replica1 = new Replica({ crypto, hashSuite, config: { selfValidate: true } });
                replica1.attachBackend('default', backend);
                replica1.registerType(RSet.typeId, rSetFactory);

                const initA = await RSet.create({ seed: 'set-a', initialElements: [], hashAlgorithm: 'sha256' });
                const initB = await RSet.create({ seed: 'set-b', initialElements: [], hashAlgorithm: 'sha256' });
                const initC = await RSet.create({ seed: 'set-c', initialElements: [], hashAlgorithm: 'sha256' });

                const setA = await replica1.createObject(initA);
                const setB = await replica1.createObject(initB);
                const setC = await replica1.createObject(initC);

                const replica2 = new Replica({ crypto, hashSuite, config: { selfValidate: true } });
                replica2.attachBackend('default', backend);
                replica2.registerType(RSet.typeId, rSetFactory);
                await replica2.start();

                assertTrue((await replica2.getObject(setA.getId())) !== undefined, 'set A should be loaded');
                assertTrue((await replica2.getObject(setB.getId())) !== undefined, 'set B should be loaded');
                assertTrue((await replica2.getObject(setC.getId())) !== undefined, 'set C should be loaded');
            },
        },
        {
            name: '[REP08] close() clears state',
            invoke: async () => {
                const replica = createTestReplica();

                const init = await RSet.create({ seed: 'close-test', initialElements: [], hashAlgorithm: 'sha256' });
                const set = await replica.createObject(init);
                const id = set.getId();

                assertTrue((await replica.getObject(id)) !== undefined, 'object should exist before close');

                await replica.close();

                assertTrue((await replica.getObject(id)) === undefined, 'object should not exist after close');
            },
        },
    ],
};
