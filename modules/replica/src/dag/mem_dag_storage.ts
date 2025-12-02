import { Hash } from "@hyper-hyper-space/hhs3_crypto";
import { dag } from "@hyper-hyper-space/hhs3_dag";

import { DagResource, DagResourceProvider, DagStorage } from "./dag_replica";
import { ResourcesBase } from "replica";

class MemDagResourceProvider<R extends ResourcesBase = ResourcesBase> implements DagResourceProvider<R> {
    private stores: Map<Hash, R & DagResource> = new Map();

    async addResource(id: Hash, resources: R): Promise<R & DagResource> {
        if (!this.stores.has(id)) {
            const store = new dag.store.MemDagStorage();
            const index = dag.idx.flat.createFlatIndex(store, new dag.idx.flat.mem.MemFlatIndexStore);
            this.stores.set(id, {...resources, ...{dag: {get: async () => dag.create(store, index)}}});
        }
        return this.stores.get(id)!;
    }
}

export const createMemDagResourceProvider = () => {
    return new MemDagResourceProvider();
}