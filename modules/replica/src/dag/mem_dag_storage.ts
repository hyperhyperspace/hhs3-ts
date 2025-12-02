import { Hash } from "@hyper-hyper-space/hhs3_crypto";
import { dag } from "@hyper-hyper-space/hhs3_dag";

import { DagStorageProvider } from "./dag_replica";

class MemDagStorage implements DagStorageProvider {
    private dags: Map<Hash, dag.Dag> = new Map();

    async getDagForObjectId(id: Hash): Promise<dag.Dag> {
        if (!this.dags.has(id)) {
            const store = new dag.store.MemDagStorage();
            const index = dag.idx.flat.createFlatIndex(store, new dag.idx.flat.mem.MemFlatIndexStore);
            this.dags.set(id, dag.create(store, index));
        }
        return this.dags.get(id)!;
    }
}

export const createMemDagStorageProvider = () => {
    return new MemDagStorage();
}