import { ScopedDag, CausalDag } from "./dag_nesting";

export type DagCapability = {
    getScopedDag(tag?: string): Promise<ScopedDag>;
    getCausalDag(tag?: string): Promise<CausalDag>;
};
