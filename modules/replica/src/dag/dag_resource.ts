import { dag } from "@hyper-hyper-space/hhs3_dag";

import { ResourcesBase, Replica, RObject, RObjectRegistry } from "../replica";
import { Hash } from "@hyper-hyper-space/hhs3_crypto";

export type DagStorage = {
    get: () => Promise<dag.Dag>;
};

export type DagResource = {
    dag: DagStorage;
};

export type DagResourceProvider<R extends ResourcesBase = ResourcesBase> = {
    addForObject: (id: Hash, resources: R) => Promise<R & DagResource>;
    addForObjectPreflight: (resources: R) => Promise<R & DagResource>;
};