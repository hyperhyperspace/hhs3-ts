import { Hash, BasicCrypto } from "@hyper-hyper-space/hhs3_crypto";
import { dag, MetaProps } from "@hyper-hyper-space/hhs3_dag";

import { json } from "@hyper-hyper-space/hhs3_json";

export const MAX_TYPE_LENGTH = 128;

export type Version = dag.Position;
export const emptyVersion: () => Version = dag.emptyPosition;
export const version: (...hashes: Hash[]) => Version = dag.position;
export type Payload = json.Literal;

// RObject (Replicable Object): used both to write and interpret changes to a DAG-based history log.

// An RObject may contain other RObjects within it, that will share its DAG store
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

export type BasicProvider = {
    getReplica(): Replica<any>;
    getRegistry(): RObjectTypeRegistry<any>;
    getCrypto(): BasicCrypto;
};

export type RObjectFactory<P extends BasicProvider = BasicProvider> = {
    computeRootObjectId: (createPayload: Payload, provider: P) => Promise<Hash>;
    
    validateCreationPayload: (createPayload: Payload, provider: P) => Promise<boolean>;
    executeCreationPayload: (createPayload: Payload, provider: P) => Promise<Hash>;
    
    loadObject: (id: Hash, provider: P) => Promise<RObject>;
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


export type RObjectTypeRegistry<P extends BasicProvider = BasicProvider> = {
    lookup(typeName: string): Promise<RObjectFactory<P>>;
}

export class TypeRegistryMap<P extends BasicProvider = BasicProvider> implements RObjectTypeRegistry<P> {
    private types: Map<string, RObjectFactory<P>> = new Map();

    async register(typeName: string, type: RObjectFactory<P>) {
        this.types.set(typeName, type);
    }
    
    async lookup(typeName: string): Promise<RObjectFactory<P>> {
        return this.types.get(typeName)!;
    }
}

export type ReplicaConfig = {
    selfValidate?: boolean;
};

export class Replica<P extends BasicProvider = BasicProvider> {

    private registry: RObjectTypeRegistry<P>;
    private objects: Map<Hash, RObject> = new Map();
    config: ReplicaConfig;

    // If id is missing, the object creation payload has not yet been validated.
    // In that case yhe provider will refuse to return any resources that consume 
    // significant resources, throwing an exception isntead. This mode is intended
    // for use in validation only.
    private createProvider: (replica: Replica<P>, id?: Hash) => P;
    

    constructor(registry: RObjectTypeRegistry<P>, createProvider: (replica: Replica<P>, id?: Hash) => P, config: ReplicaConfig = {}) {
        this.registry = registry;
        this.createProvider = createProvider;
        this.config = config;
    }

    getRegistry(): RObjectTypeRegistry<P> { return this.registry; }

    async getObject(id: Hash): Promise<RObject> {
        return this.objects.get(id)!;
    }

    async addObject(init: RObjectInit): Promise<Hash> {

        const factory = await this.registry.lookup(init.type);

        const id = await factory.computeRootObjectId(init.payload, this.createProvider(this));
        const provider = this.createProvider(this, id);
        const valid = await factory.validateCreationPayload(init.payload, provider);

        if (valid) {
            await factory.executeCreationPayload(init.payload, provider);
            this.objects.set(id, await factory.loadObject(id, provider));
            return id;
        } else {
            throw new Error('Invalid creation payload');
        }
    }
}