import { Hash } from "@hyper-hyper-space/hhs3_crypto";
import { dag } from "@hyper-hyper-space/hhs3_dag";

import { json } from "@hyper-hyper-space/hhs3_json";

export type Version = dag.Position;
export const emptyVersion: () => Version = dag.emptyPosition;
export const version: (...hashes: Hash[]) => Version = dag.position;
export type Payload = json.Literal;

// A replicable object, used both to write and interpret changes to a DAG-based history log.

// An r-object may contain other r-objects within it, that will share its DAG store
// and use its DAG for their change history. The applyPayload function will handle applying
// changes to any sub-objects that are being modified, and the createView function will
// use views from sub-objects when necessary.

export type RObject = {
    
    getId(): Hash; // by contention, the id of the creation change op
    getType(): string;

    // for writing
    validatePayload(payload: Payload, at: Version): Promise<boolean>;
    applyPayload(payload: Payload, at: Version): Promise<Hash>;

    // for reading: enabled only by explicitly requesting a version
    getView(at?: Version, from?: Version): Promise<View>;

    subscribe(callback: (event: Event) => void): void;
    unsubscribe(callback: (event: Event) => void): void;
}

// A static view of a replicable object's state at a given version
export type View = {
    getObject(): RObject;
    getVersion(): Version;
    getFromVersion(): Version;
}

// An event that signals a change in the replicable object's state
export type Event = {
    getObjectId(): Hash;
    getType(): string;
    getVersion(): Version;
}

// This can be enriched locally with metadata, in which case this type should be extended.
// However, only the payload is actually replicated.

export type LoadRObject<R extends ResourcesBase = ResourcesBase> = (createOpId: Hash, context: R) => Promise<RObject>;
export type ValidateCreationOp<R extends ResourcesBase = ResourcesBase> = (createOpId: Hash, createOpPayload: Payload, context: R) => Promise<boolean>;

export type TypeRegistry<R extends ResourcesBase = ResourcesBase> = {
    register: (type: string, load: LoadRObject, validate: ValidateCreationOp) => void;
    getLoadFun(type: string) : LoadRObject<R>;
    getValidateCreationFun(type: String): ValidateCreationOp<R>;
}

export class TypeRegistryMap<R extends ResourcesBase = ResourcesBase> implements TypeRegistry<R> {
    private types: Map<string, [LoadRObject<R>, ValidateCreationOp<R>]> = new Map();

    register(type: string, create: LoadRObject<R>, checkCreate: ValidateCreationOp<R>) {
        this.types.set(type, [create, checkCreate]);
    }

    getLoadFun(type: string): LoadRObject<R> {
        return this.types.get(type)![0];
    }

    getValidateCreationFun(type: string): ValidateCreationOp<R> {
        return this.types.get(type)![1];
    }
}

export type ResourcesBase = {
    replica: Replica<any>;
};

export type Resource = {[key: string]: any};

export type ResourceProvider<R extends ResourcesBase, T extends Resource> = {
    addResource: (id: Hash, resources: R) => Promise<R&T>;
};

export class Replica<R extends ResourcesBase = ResourcesBase> {

    private typeRegistry: TypeRegistry<R>;
    private objects: Map<Hash, RObject> = new Map();
    private resourceProvider: ResourceProvider<ResourcesBase, R>;

    constructor(typeRegistry: TypeRegistry<R>, resourceProvider: ResourceProvider<ResourcesBase, R>) {
        this.typeRegistry = typeRegistry;
        this.resourceProvider = resourceProvider;
    }

    async getObject(id: Hash): Promise<RObject> {
        return this.objects.get(id)!;
    }

    async addObject(type: string, createOpId: Hash, creationPayload: json.Literal): Promise<boolean> {
        const validateCreateFun = this.typeRegistry.getValidateCreationFun(type);
        const context = await this.resourceProvider.addResource(createOpId, { replica: this });

        const isValid = await validateCreateFun(createOpId, creationPayload, context);
        
        if (isValid) {

            const loadFun = this.typeRegistry.getLoadFun(type);
            const object = await loadFun(createOpId, context);
            this.objects.set(object.getId(), object);
            return true;
        }

        return false;
    }

    async getResources(id: Hash): Promise<R> {
        return this.resourceProvider.addResource(id, { replica: this });
    }
}