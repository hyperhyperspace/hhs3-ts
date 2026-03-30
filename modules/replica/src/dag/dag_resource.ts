import { ResourcesBase, Replica, RObject, RObjectRegistry } from "../replica";
import { Hash } from "@hyper-hyper-space/hhs3_crypto";
import { ScopedDag, CausalDag } from "./dag_nesting";

export type ScopedDagStorage = {
    get: () => Promise<ScopedDag>;
};

export type CausalDagStorage = {
    get: () => Promise<CausalDag>;
};

export type DagResource = {
    scopedDag: ScopedDagStorage;
    causalDag: CausalDagStorage;
};

export type DagResourceProvider<R extends ResourcesBase = ResourcesBase> = {
    addForObject: (id: Hash, resources: R) => Promise<R & DagResource>;
    addForObjectPreflight: (resources: R) => Promise<R & DagResource>;
};
