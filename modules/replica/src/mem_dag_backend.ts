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

    async createDag(id: B64Hash, meta: { type: string }): Promise<Dag> {
        if (this.dags.has(id)) {
            throw new Error(`DAG '${id}' already exists in this backend`);
        }

        const d = this.makeDag();
        this.dags.set(id, d);
        this.entries.set(id, { id, type: meta.type, createdAt: ++this.counter });
        return d;
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
