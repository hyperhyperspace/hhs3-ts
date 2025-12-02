import { dag } from "@hyper-hyper-space/hhs3_dag";

import { ResourcesBase, Replica, RObject, TypeRegistry } from "../replica";
import { Hash } from "@hyper-hyper-space/hhs3_crypto";

export type DagStorage = {
    get: () => Promise<dag.Dag>;
};

export type DagResource = {
    dag: DagStorage;
};

export type DagResourceProvider<R extends ResourcesBase = ResourcesBase> = {
    addResource: (id: Hash, resources: R) => Promise<R & DagResource>;
};