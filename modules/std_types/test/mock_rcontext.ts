import { B64Hash, BasicCrypto, HashSuite, createBasicCrypto, HASH_SHA256 } from "@hyper-hyper-space/hhs3_crypto";
import { dag, Dag } from "@hyper-hyper-space/hhs3_dag";

import {
    RContext, RObject, Payload, RObjectConfig, RObjectFactory,
    RObjectTypeRegistry, TypeRegistryMap, RootScopedDag,
    extractCreatePayloadType, formatValidationFailure, ValidationRejectedError,
} from "@hyper-hyper-space/hhs3_mvt";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

export function createMockRContext(config: RObjectConfig = { selfValidate: true }): RContext {

    const registry = new TypeRegistryMap();
    const objects = new Map<B64Hash, RObject>();
    const rootIds = new Set<B64Hash>();
    const backendById = new Map<B64Hash, string>();
    const dags = new Map<string, Dag>();

    function getOrCreateDag(key: string): Dag {
        if (!dags.has(key)) {
            const store = new dag.store.MemDagStorage();
            const index = dag.idx.flat.createFlatIndex(store, new dag.idx.flat.mem.MemFlatIndexStore());
            dags.set(key, dag.create(store, index, hashSuite));
        }
        return dags.get(key)!;
    }

    function recordObject(obj: RObject): void {
        const id = obj.getId();
        objects.set(id, obj);
        backendById.set(id, obj.getBackendLabel());
    }

    function releaseObject(id: B64Hash): void {
        objects.delete(id);
        backendById.delete(id);
    }

    const ctx: RContext = {
        getCrypto: () => crypto,
        getHashSuite: () => hashSuite,
        getConfig: () => config,
        getRegistry: () => registry,

        getObject: async (id: B64Hash) => objects.get(id),

        getBackendLabel: async (id: B64Hash) => backendById.get(id),

        getDag: async (id: B64Hash, backendLabel?: string) => getOrCreateDag(id),

        getMesh: (_label: string) => {
            throw new Error("MockRContext does not support getMesh");
        },

        createObject: async (createPayload: Payload, backendLabel: string = 'default') => {
            const typeId = extractCreatePayloadType(createPayload);
            if (typeId === undefined) throw new Error('create payload missing type');
            const factory = await registry.lookup(typeId);
            const id = await factory.computeRootObjectId(createPayload, ctx, undefined);

            const existing = objects.get(id);
            if (existing !== undefined) {
                return existing;
            }

            const result = await factory.validateCreationPayload(createPayload, ctx, undefined);
            if (!result.valid) throw new ValidationRejectedError(formatValidationFailure(result.why), result.why);
            const rawDag = getOrCreateDag(id);
            const scopedDag = new RootScopedDag(rawDag);
            await factory.executeCreationPayload(createPayload, ctx, scopedDag);
            const obj = await factory.loadObject(id, ctx, { backendLabel });
            recordObject(obj);
            rootIds.add(id);
            return obj;
        },

        unregisterObject: async (id: B64Hash) => {
            if (rootIds.has(id)) {
                throw new Error(`Cannot unregister root object '${id}'`);
            }
            const obj = objects.get(id);
            if (obj === undefined) return;
            await obj.destroy();
            releaseObject(id);
        },
    };

    return ctx;
}
