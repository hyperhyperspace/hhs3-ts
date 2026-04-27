import { B64Hash, BasicCrypto, HashSuite, createBasicCrypto, HASH_SHA256 } from "@hyper-hyper-space/hhs3_crypto";
import { dag, Dag } from "@hyper-hyper-space/hhs3_dag";

import {
    RContext, RObject, RObjectInit, RObjectConfig, RObjectFactory,
    RObjectTypeRegistry, TypeRegistryMap, RootScopedDag,
} from "@hyper-hyper-space/hhs3_mvt";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

export function createMockRContext(config: RObjectConfig = { selfValidate: true }): RContext {

    const registry = new TypeRegistryMap();
    const objects = new Map<B64Hash, RObject>();
    const dags = new Map<string, Dag>();

    function getOrCreateDag(key: string): Dag {
        if (!dags.has(key)) {
            const store = new dag.store.MemDagStorage();
            const index = dag.idx.flat.createFlatIndex(store, new dag.idx.flat.mem.MemFlatIndexStore());
            dags.set(key, dag.create(store, index, hashSuite));
        }
        return dags.get(key)!;
    }

    const ctx: RContext = {
        getCrypto: () => crypto,
        getHashSuite: () => hashSuite,
        getConfig: () => config,
        getRegistry: () => registry,

        getObject: async (id: B64Hash) => objects.get(id),

        getDag: async (id: B64Hash, _backendLabel?: string) => getOrCreateDag(id),

        getMesh: (_label: string) => {
            throw new Error("MockRContext does not support getMesh");
        },

        createObject: async (init: RObjectInit) => {
            const factory = await registry.lookup(init.type);
            const id = await factory.computeRootObjectId(init.payload, ctx, undefined);

            if (objects.has(id)) {
                return objects.get(id)!;
            }

            const valid = await factory.validateCreationPayload(init.payload, ctx, undefined);
            if (!valid) throw new Error('Invalid creation payload');
            const rawDag = getOrCreateDag(id);
            const scopedDag = new RootScopedDag(rawDag);
            await factory.executeCreationPayload(init.payload, ctx, scopedDag);
            const obj = await factory.loadObject(id, ctx, undefined);
            objects.set(id, obj);
            return obj;
        },
    };

    return ctx;
}
