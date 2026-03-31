import { Hash } from "@hyper-hyper-space/hhs3_crypto";
import { dag, Dag } from "@hyper-hyper-space/hhs3_dag";

import { RootScopedDag, ScopedDag, CausalDag } from "./dag_nesting";

export type MemDagBackend = {
    getScopedDag(id: Hash, tag?: string): Promise<ScopedDag>;
    getCausalDag(id: Hash, tag?: string): Promise<CausalDag>;
};

export function createMemDagBackend(): MemDagBackend {
    const dags = new Map<string, Dag>();

    // Returns existing DAG for the key, or creates a new one
    const createDag = (key: string): Dag => {
        if (!dags.has(key)) {
            const store = new dag.store.MemDagStorage();
            const index = dag.idx.flat.createFlatIndex(store, new dag.idx.flat.mem.MemFlatIndexStore());
            dags.set(key, dag.create(store, index));
        }
        return dags.get(key)!;
    };

    const dagForObject = (id: string, tag?: string) => createDag(tag ? `${id}:${tag}` : id);

    return {
        getScopedDag: async (id, tag?) => new RootScopedDag(dagForObject(id, tag)),
        getCausalDag: async (id, tag?) => dagForObject(id, tag),
    };
}
