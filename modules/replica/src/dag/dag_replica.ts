import { dag } from "@hyper-hyper-space/hhs3_dag";

import { RContext, Replica, RObject, TypeRegistry } from "../replica";
import { Hash } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";

export type DagContext = RContext & {
    getDag: () => Promise<dag.Dag>;
};

export type DagStorageProvider = {
    getDagForObjectId: (id: Hash) => Promise<dag.Dag>;
};

export class DagReplica implements Replica {

    private typeRegistry: TypeRegistry<DagContext>;
    private storageProvider: DagStorageProvider;
    private dagCache: Map<Hash, Promise<dag.Dag>> = new Map();
    
    private objects: Map<Hash, RObject> = new Map();

    constructor(typeRegistry: TypeRegistry<DagContext>, storageProvider: DagStorageProvider) {
        this.typeRegistry = typeRegistry;
        this.storageProvider = storageProvider;
    }

    async addObject(type: string, createOpId: Hash, creationPayload: json.Literal): Promise<boolean> {
        const validateCreateFun = this.typeRegistry.getValidateCreationFun(type);
        const context = await this.getContext(createOpId);

        const isValid = await validateCreateFun(createOpId, creationPayload, context);
        
        if (isValid) {

            const loadFun = this.typeRegistry.getLoadFun(type);
            const object = await loadFun(createOpId, context);
            this.objects.set(object.getId(), object);
            return true;
        }

        return false;
    }

    async getObject(id: Hash): Promise<RObject> {
        return this.objects.get(id)!;
    }

    async getContext(createOpId: Hash): Promise<DagContext> {
        const dagPromise = this.ensureDag(createOpId);
        return {
            replica: this,
            getDag: () => dagPromise
        };
    }

    private ensureDag(createOpId: Hash): Promise<dag.Dag> {
        if (!this.dagCache.has(createOpId)) {
            this.dagCache.set(createOpId, this.storageProvider.getDagForObjectId(createOpId));
        }
        return this.dagCache.get(createOpId)!;
    }
}