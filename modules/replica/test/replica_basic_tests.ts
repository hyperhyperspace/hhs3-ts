import * as os from "node:os";
import * as path from "node:path";
import * as fs from "node:fs";
import { assertTrue, assertFalse } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import { Replica, MemDagBackend } from "../src/index.js";
import { TypeRegistryMap, RootScopedDag } from "@hyper-hyper-space/hhs3_mvt";
import { RSet, rSetFactory, RCap, rCapFactory } from "@hyper-hyper-space/hhs3_std_types";
import { SqliteDagDb } from "@hyper-hyper-space/hhs3_dag_sqlite";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

function tmpDbPath(label: string): string {
    return path.join(os.tmpdir(), `hhs3-replica-${label}-${Date.now()}-${Math.random().toString(36).slice(2)}.db`);
}

function cleanupDb(path: string): void {
    for (const suffix of ['', '-wal', '-shm']) {
        try { fs.unlinkSync(path + suffix); } catch (_e) { /* ignore */ }
    }
}

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
            name: '[REP03] createObject after restart reuses existing DAG',
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
                const set2 = (await replica2.createObject(init)) as RSet;
                assertTrue(set2 !== undefined, 'object should be restored by idempotent createObject');
                assertTrue(set2.getId() === id, 'restored object should have same id');

                const view = await set2.getView();
                assertTrue(await view.has('alpha'), 'restored set should have initial element');
                assertTrue(await view.has('beta'), 'restored set should have subsequent add');
            },
        },
        {
            name: '[REP03B] createObject cold-reopens from SqliteDagDb',
            invoke: async () => {
                const tmpFile = tmpDbPath('cold-reopen');
                let backend1: SqliteDagDb | undefined;
                let backend2: SqliteDagDb | undefined;

                try {
                    backend1 = await SqliteDagDb.open(tmpFile, { hashSuite });

                    const replica1 = new Replica({ crypto, hashSuite, config: { selfValidate: true } });
                    replica1.attachBackend('default', backend1);
                    replica1.registerType(RSet.typeId, rSetFactory);

                    const init = await RSet.create({
                        seed: 'sqlite-restart-set',
                        initialElements: ['alpha'],
                        hashAlgorithm: 'sha256',
                    });

                    const set1 = (await replica1.createObject(init)) as RSet;
                    await set1.add('beta');
                    const id = set1.getId();

                    await replica1.destroy();
                    backend1.close();
                    backend1 = undefined;

                    backend2 = await SqliteDagDb.open(tmpFile, { hashSuite });

                    const replica2 = new Replica({ crypto, hashSuite, config: { selfValidate: true } });
                    replica2.attachBackend('default', backend2);
                    replica2.registerType(RSet.typeId, rSetFactory);

                    const set2 = (await replica2.createObject(init)) as RSet;
                    assertTrue(set2 !== undefined, 'object should be restored from sqlite');
                    assertTrue(set2.getId() === id, 'restored object should have same id');

                    const view = await set2.getView();
                    assertTrue(await view.has('alpha'), 'restored sqlite set should have initial element');
                    assertTrue(await view.has('beta'), 'restored sqlite set should have subsequent add');

                    await replica2.destroy();
                } finally {
                    if (backend1 !== undefined) backend1.close();
                    if (backend2 !== undefined) backend2.close();
                    cleanupDb(tmpFile);
                }
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
                    await replica.createObject({
                        action: 'create',
                        type: 'nonexistent/type',
                        seed: 'x',
                        initialElements: [],
                        acceptRedundantDelete: false,
                        hashAlgorithm: 'sha256',
                    } as any);
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
            name: '[REP07] createObject on fresh replica loads existing roots',
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

                const restoredA = await replica2.createObject(initA);
                const restoredB = await replica2.createObject(initB);
                const restoredC = await replica2.createObject(initC);

                assertTrue(restoredA.getId() === setA.getId(), 'set A should be restored');
                assertTrue(restoredB.getId() === setB.getId(), 'set B should be restored');
                assertTrue(restoredC.getId() === setC.getId(), 'set C should be restored');
            },
        },
        {
            name: '[REP08] destroy() clears root objects',
            invoke: async () => {
                const replica = createTestReplica();

                const init = await RSet.create({ seed: 'close-test', initialElements: [], hashAlgorithm: 'sha256' });
                const set = await replica.createObject(init);
                const id = set.getId();

                assertTrue(replica.getRootIds().has(id), 'object should be a root before destroy');
                assertTrue((await replica.getObject(id)) !== undefined, 'object should exist before destroy');

                await replica.destroy();

                assertTrue((await replica.getObject(id)) === undefined, 'object should not exist after destroy');
                assertTrue(replica.getRootIds().size === 0, 'rootIds should be empty after destroy');
            },
        },
        {
            name: '[REP09] registerObject and unregisterObject for owned objects',
            invoke: async () => {
                const backend = new MemDagBackend(hashSuite);
                const replica = new Replica({ crypto, hashSuite, config: { selfValidate: true } });
                replica.attachBackend('default', backend);
                replica.registerType(RCap.typeId, rCapFactory);

                const capInit = await RCap.create({
                    seed: 'owned-cap',
                    creators: [],
                    initialCaps: { admin: { managedBy: ['creator'] } },
                });

                const factory = await replica.getRegistry().lookup(RCap.typeId);
                const id = await factory.computeRootObjectId(capInit, replica, undefined);
                const { dag, created } = await backend.getOrCreateDag(id, { type: RCap.typeId });
                if (created) {
                    await factory.executeCreationPayload(capInit, replica, new RootScopedDag(dag));
                }
                const cap = (await factory.loadObject(id, replica, { backendLabel: 'default' })) as RCap;
                replica.registerObject(cap);

                assertTrue((await replica.getObject(id)) === cap, 'registered object should be retrievable');
                assertFalse(replica.getRootIds().has(id), 'registered object should not be a root');

                await replica.unregisterObject(id);

                assertTrue((await replica.getObject(id)) === undefined, 'object should be removed after unregister');
            },
        },
        {
            name: '[REP10] unregisterObject rejects root objects',
            invoke: async () => {
                const replica = createTestReplica();
                replica.registerType(RCap.typeId, rCapFactory);

                const admin = await createIdentity(SIGNING_ED25519, hashSuite);
                const init = await RCap.create({
                    seed: 'root-cap',
                    creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
                    initialCaps: { admin: { managedBy: ['creator'] } },
                });
                const cap = await replica.createObject(init);
                const id = cap.getId();

                let threw = false;
                try {
                    await replica.unregisterObject(id);
                } catch {
                    threw = true;
                }
                assertTrue(threw, 'unregisterObject should reject root objects');
            },
        },
        {
            name: '[REP11] createObject rejects invalid payload for registered type',
            invoke: async () => {
                const replica = createTestReplica();
                let threw = false;
                try {
                    await replica.createObject({
                        action: 'create',
                        type: RSet.typeId,
                        seed: 'x',
                        initialElements: [],
                        acceptRedundantDelete: false,
                        hashAlgorithm: 'sha256',
                        contentType: RSet.typeId,
                        acceptRedundantAdd: true,
                    } as any);
                } catch {
                    threw = true;
                }
                assertTrue(threw, 'createObject should reject invalid create payload');
            },
        },
        {
            name: '[REP12] createObject rejects create payload without type',
            invoke: async () => {
                const replica = createTestReplica();
                let threw = false;
                try {
                    await replica.createObject({
                        action: 'create',
                        seed: 'x',
                        initialElements: [],
                        acceptRedundantDelete: false,
                        hashAlgorithm: 'sha256',
                    } as any);
                } catch {
                    threw = true;
                }
                assertTrue(threw, 'createObject should reject create payload missing type');
            },
        },
        {
            name: '[REP13] genesis entry persists MVT type in create payload',
            invoke: async () => {
                const replica = createTestReplica();
                const init = await RSet.create({
                    seed: 'genesis-type',
                    initialElements: ['x'],
                    hashAlgorithm: 'sha256',
                });
                const set = await replica.createObject(init);
                const entry = await (await set.getScopedDag()).loadEntry(set.getId());
                assertTrue(entry !== undefined, 'genesis entry should exist');
                const payload = entry!.payload as { type?: string };
                assertTrue(payload.type === RSet.typeId, 'genesis payload should carry MVT type');
            },
        },
    ],
};
