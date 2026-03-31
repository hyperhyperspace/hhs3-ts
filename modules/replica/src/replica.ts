import { Hash } from "@hyper-hyper-space/hhs3_crypto";
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
};

export type RObjectFactory<P extends BasicProvider = BasicProvider> = {
    computeRootObjectId: (createPayload: Payload, provider: BasicProvider) => Promise<Hash>;
    
    validateCreationPayload: (createPayload: Payload, provider: BasicProvider) => Promise<boolean>;
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
    private createProvider: (id: Hash, replica: Replica<P>) => P;
    config: ReplicaConfig;

    constructor(registry: RObjectTypeRegistry<P>, createProvider: (id: Hash, replica: Replica<P>) => P, config: ReplicaConfig = {}) {
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

        const basicProvider: BasicProvider = { getReplica: () => this, getRegistry: () => this.registry };
        const id = await factory.computeRootObjectId(init.payload, basicProvider);
        const provider = this.createProvider(id, this);
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