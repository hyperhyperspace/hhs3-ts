import { Hash } from "@hyper-hyper-space/hhs3_crypto";
import { dag } from "@hyper-hyper-space/hhs3_dag";

import { DagResource, DagResourceProvider } from "./dag_resource";
import { RootScopedDag } from "./dag_nesting";
import { ResourcesBase } from "replica";

class MemDagResourceProvider<R extends ResourcesBase = ResourcesBase> implements DagResourceProvider<R> {
    private stores: Map<Hash, R & DagResource> = new Map();

    async addForObject(objectId: Hash, resources: R): Promise<R & DagResource> {
        if (!this.stores.has(objectId)) {
            const store = new dag.store.MemDagStorage();
            const index = dag.idx.flat.createFlatIndex(store, new dag.idx.flat.mem.MemFlatIndexStore);
            const d = dag.create(store, index);
            this.stores.set(objectId, {
                ...resources,
                scopedDag: { get: async () => new RootScopedDag(d) },
                causalDag: { get: async () => d },
            });
        }
        return this.stores.get(objectId)!;
    }

    async addForObjectPreflight(resources: R): Promise<R & DagResource> {
        const store = new dag.store.MemDagStorage();
        const index = dag.idx.level.createDagLevelIndex(store, new dag.idx.level.mem.MemLevelIndexStore);
        const d = dag.create(store, index);
        return {
            ...resources,
            scopedDag: { get: async () => new RootScopedDag(d) },
            causalDag: { get: async () => d },
        };
    }

}

export const createMemDagResourceProvider = () => {
    return new MemDagResourceProvider();
}
