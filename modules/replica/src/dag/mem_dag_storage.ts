import { Hash } from "@hyper-hyper-space/hhs3_crypto";
import { dag } from "@hyper-hyper-space/hhs3_dag";

import { DagResource, DagResourceProvider, DagStorage } from "./dag_resource";
import { ResourcesBase } from "replica";

class MemDagResourceProvider<R extends ResourcesBase = ResourcesBase> implements DagResourceProvider<R> {
    private stores: Map<Hash, R & DagResource> = new Map();

    async addForObject(objectId: Hash, resources: R): Promise<R & DagResource> {
        if (!this.stores.has(objectId)) {
            const store = new dag.store.MemDagStorage();
            const index = dag.idx.flat.createFlatIndex(store, new dag.idx.flat.mem.MemFlatIndexStore);
            this.stores.set(objectId, {...resources, ...{dag: {get: async () => dag.create(store, index)}}});
        }
        return this.stores.get(objectId)!;
    }

    async addForObjectPreflight(resources: R): Promise<R & DagResource> {
        const store = new dag.store.MemDagStorage();
        const index = dag.idx.level.createDagLevelIndex(store, new dag.idx.level.mem.MemLevelIndexStore);
        return {...resources, ...{dag: {get: async () => dag.create(store, index)}}}
    }

}

export const createMemDagResourceProvider = () => {
    return new MemDagResourceProvider();
}