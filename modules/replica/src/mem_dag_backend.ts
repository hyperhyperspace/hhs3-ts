import { B64Hash, HashSuite } from "@hyper-hyper-space/hhs3_crypto";
import { dag, Dag } from "@hyper-hyper-space/hhs3_dag";

import { DagBackend, DagEntry } from "./replica.js";

export class MemDagBackend implements DagBackend {

    private hash: HashSuite;
    private dags: Map<B64Hash, Dag> = new Map();
    private entries: Map<B64Hash, DagEntry> = new Map();
    private counter = 0;

    constructor(hash: HashSuite) {
        this.hash = hash;
    }

    async getOrCreateDag(id: B64Hash, meta: { type: string }): Promise<{ dag: Dag; created: boolean }> {
        const existing = this.dags.get(id);
        if (existing !== undefined) {
            return { dag: existing, created: false };
        }

        const dag = this.makeDag();
        this.dags.set(id, dag);
        this.entries.set(id, { id, type: meta.type, createdAt: ++this.counter });
        return { dag, created: true };
    }

    async openDag(id: B64Hash): Promise<Dag> {
        const d = this.dags.get(id);
        if (d === undefined) {
            throw new Error(`DAG '${id}' not found in this backend`);
        }
        return d;
    }

    async listDags(): Promise<DagEntry[]> {
        return [...this.entries.values()];
    }

    private makeDag(): Dag {
        const store = new dag.store.MemDagStorage();
        const index = dag.idx.flat.createFlatIndex(store, new dag.idx.flat.mem.MemFlatIndexStore());
        return dag.create(store, index, this.hash);
    }
}
