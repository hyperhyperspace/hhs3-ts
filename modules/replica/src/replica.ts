import { Hash } from "@hyper-hyper-space/hhs3_crypto";
import { dag, MetaProps } from "@hyper-hyper-space/hhs3_dag";

import { json } from "@hyper-hyper-space/hhs3_json";

export const MAX_TYPE_LENGTH = 128;

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

export type RObjectInit = {
    type: string;
    payload: Payload;
}

export type RObjectFactory<R extends ResourcesBase = ResourcesBase> = {
    computeObjectId: (createPayload: Payload, resources: R) => Promise<Hash>;
    
    validateCreationPayload: (createPayload: Payload, resources: R) => Promise<boolean>;
    executeCreationPayload: (createPayload: Payload, resources: R) => Promise<Hash>;
    
    loadObject: (id: Hash, resources: R) => Promise<RObject>;
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


export type RObjectRegistry<R extends ResourcesBase = ResourcesBase> = {
    lookup(typeName: string): Promise<RObjectFactory<R>>;
}

export class TypeRegistryMap<R extends ResourcesBase = ResourcesBase> implements RObjectRegistry<R> {
    private types: Map<string, RObjectFactory<R>> = new Map();

    async register(typeName: string, type: RObjectFactory<R>) {
        this.types.set(typeName, type);
    }
    
    async lookup(typeName: string): Promise<RObjectFactory<R>> {
        return this.types.get(typeName)!;
    }
}

export type ResourcesBase = {
    replica: Replica<any>;
    registry: RObjectRegistry<any>;
};

export type Resource = {[key: string]: any};

export type ResourcesProvider<R extends ResourcesBase, T extends Resource> = {
    
    // resources needed for object normal lifetime
    addForObject: (id: Hash, resources: R) => Promise<R&T>;
    
    // any resources are needed before object creation (e.g. for computing its hash id, validation, etc.)
    addForObjectPreflight: (resources: R) => Promise<R&T>;
};

export class Replica<R extends ResourcesBase = ResourcesBase> {

    private registry: RObjectRegistry<R>;
    private objects: Map<Hash, RObject> = new Map();
    private resourceProvider: ResourcesProvider<ResourcesBase, R>;

    constructor(registry: RObjectRegistry<R>, resourceProvider: ResourcesProvider<ResourcesBase, R>) {
        this.registry = registry;
        this.resourceProvider = resourceProvider;
    }

    async getObject(id: Hash): Promise<RObject> {
        return this.objects.get(id)!;
    }

    async addObject(init: RObjectInit): Promise<Hash> {

        const factory = await this.registry.lookup(init.type);

        
        const preflightResources = await this.resourceProvider.addForObjectPreflight({ replica: this, registry: this.registry });
        
        const id = await factory.computeObjectId(init.payload, preflightResources);
        const valid = await factory.validateCreationPayload(init.payload, preflightResources);

        if (valid) {
            const resources = await this.resourceProvider.addForObject(id, { replica: this, registry: this.registry });
            await factory.executeCreationPayload(init.payload, resources);
            this.objects.set(id, await factory.loadObject(id, resources));
            return id
        } else {
            throw new Error('Invalid creation payload');
        }
    }

    async getResourcesForObject(id: Hash): Promise<R> {
        return this.resourceProvider.addForObject(id, { replica: this, registry: this.registry });
    }

    async getResourcesForPreflight(): Promise<R> {
        return this.resourceProvider.addForObjectPreflight({ replica: this, registry: this.registry });
    }

    
}