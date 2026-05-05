import { assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256 } from "@hyper-hyper-space/hhs3_crypto";
import { Replica, MemDagBackend } from "../src/index.js";
import { RSet, rSetFactory } from "@hyper-hyper-space/hhs3_std_types";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

function createStubSwarm(): any {
    return {
        topic: '',
        mode: 'active',
        activate() {},
        deactivate() {},
        sleep() {},
        destroy() {},
        peers() { return []; },
        onPeerJoin(_cb: any) {},
        onPeerLeave(_cb: any) {},
        blockPeer() {},
        wouldAccept() { return Promise.resolve(false); },
        adopt() { return false; },
    };
}

function createMockMesh(): any {
    return {
        createSwarm(_topic: any, _opts?: any) {
            return createStubSwarm();
        },
    };
}

function createTestReplica() {
    const replica = new Replica({ crypto, hashSuite, config: { selfValidate: true } });
    replica.attachBackend('default', new MemDagBackend(hashSuite));
    replica.attachMesh('default', createMockMesh());
    replica.registerType(RSet.typeId, rSetFactory);
    return replica;
}

export const replicaSyncTests = {
    title: '[REP-SYNC] Sync lifecycle through objects',
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
                set.configure({ meshLabel: 'default', backendLabel: 'default' });

                await set.startSync();
                await set.stopSync();

                assertTrue(true, 'set.startSync/set.stopSync should complete without error');
            },
        },
        {
            name: '[RS01] close calls stopSync and destroy on syncable roots',
            invoke: async () => {
                const replica = createTestReplica();

                const init = await RSet.create({
                    seed: 'close-sync-set',
                    initialElements: ['alpha'],
                    hashAlgorithm: 'sha256',
                });

                const set = (await replica.createObject(init)) as RSet;
                set.configure({ meshLabel: 'default', backendLabel: 'default' });
                await set.startSync();

                await replica.close();

                assertTrue((await replica.getObject(set.getId())) === undefined, 'object should be gone after close');
            },
        },
    ],
};
