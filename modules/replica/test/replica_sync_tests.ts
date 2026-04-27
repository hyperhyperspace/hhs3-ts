import { assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256 } from "@hyper-hyper-space/hhs3_crypto";
import { Replica, MemDagBackend } from "../src/index.js";
import { RSet, rSetFactory, SyncableObject } from "@hyper-hyper-space/hhs3_mvt";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

function createTestReplica() {
    const replica = new Replica({ crypto, hashSuite, config: { selfValidate: true } });
    replica.attachBackend('default', new MemDagBackend(hashSuite));
    replica.registerType(RSet.typeId, rSetFactory);
    return replica;
}

export const replicaSyncTests = {
    title: '[REP-SYNC] Sync lifecycle through Replica',
    tests: [
        {
            name: '[RS00] startSync and stopSync do not throw for a root RSet',
            invoke: async () => {
                const replica = createTestReplica();

                const init = await RSet.create({
                    seed: 'sync-set',
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                });

                const set = (await replica.createObject(init)) as RSet;

                await replica.startSync(set.getId());
                await replica.stopSync(set.getId());

                assertTrue(true, 'startSync/stopSync should complete without error');
            },
        },
        {
            name: '[RS01] startSync on unknown id is a no-op',
            invoke: async () => {
                const replica = createTestReplica();

                await replica.startSync('nonexistent-id');

                assertTrue(true, 'startSync with unknown id should be a no-op');
            },
        },
        {
            name: '[RS02] close calls stopSync and destroy on syncable roots',
            invoke: async () => {
                const replica = createTestReplica();

                const init = await RSet.create({
                    seed: 'close-sync-set',
                    initialElements: ['alpha'],
                    hashAlgorithm: 'sha256',
                });

                const set = (await replica.createObject(init)) as RSet;
                await replica.startSync(set.getId());

                await replica.close();

                assertTrue((await replica.getObject(set.getId())) === undefined, 'object should be gone after close');
            },
        },
    ],
};
