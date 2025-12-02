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

export type LoadRObject<Ctx extends RContext = RContext> = (createOpId: Hash, context: Ctx) => Promise<RObject>;
export type ValidateCreationOp<Ctx extends RContext = RContext> = (createOpId: Hash, createOpPayload: Payload, context: Ctx) => Promise<boolean>;

export type TypeRegistry<Ctx extends RContext = RContext> = {
    register: (type: string, load: LoadRObject, validate: ValidateCreationOp) => void;
    getLoadFun(type: string) : LoadRObject<Ctx>;
    getValidateCreationFun(type: String): ValidateCreationOp<Ctx>;
}

export class TypeRegistryMap<Ctx extends RContext = RContext> implements TypeRegistry<Ctx> {
    private types: Map<string, [LoadRObject<Ctx>, ValidateCreationOp<Ctx>]> = new Map();

    register(type: string, create: LoadRObject<Ctx>, checkCreate: ValidateCreationOp<Ctx>) {
        this.types.set(type, [create, checkCreate]);
    }

    getLoadFun(type: string): LoadRObject<Ctx> {
        return this.types.get(type)![0];
    }

    getValidateCreationFun(type: string): ValidateCreationOp<Ctx> {
        return this.types.get(type)![1];
    }
}

export type RContext = {
    replica: Replica;
};

export type Replica<Ctx extends RContext = RContext> = {
    getContext: (id: Hash) => Promise<Ctx>;
    getObject(id: Hash): Promise<RObject>;
    addObject(type: string, creationChangeId: Hash, creationPayload: json.Literal): Promise<boolean>;
}