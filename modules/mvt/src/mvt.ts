import { B64Hash, BasicCrypto, HashSuite } from "@hyper-hyper-space/hhs3_crypto";
import { dag, Dag } from "@hyper-hyper-space/hhs3_dag";

import { json } from "@hyper-hyper-space/hhs3_json";

import { CausalDag, ScopedDag } from "./dag/dag_nesting.js";

export const MAX_TYPE_LENGTH = 128;

export type Version = dag.Position;
export const emptyVersion: () => Version = dag.emptyPosition;
export const version: (...hashes: B64Hash[]) => Version = dag.position;
export type Payload = json.Literal;

export type RObject = {
    
    getId(): B64Hash;
    getType(): string;

    validatePayload(payload: Payload, at: Version): Promise<boolean>;
    applyPayload(payload: Payload, at: Version): Promise<B64Hash>;

    getView(at?: Version, from?: Version): Promise<View>;

    subscribe(callback: (event: Event) => void): void;
    unsubscribe(callback: (event: Event) => void): void;
}

export type NestingParent = {
    getId(): B64Hash;
    getScopedDagForChild(childId: B64Hash): Promise<ScopedDag>;
    getCreationDagForChild(childId: B64Hash, at: Version, addPayload: Payload): Promise<ScopedDag>;
    getCausalDag(): Promise<CausalDag>;
}

export type SyncableObject = {
    startSync(): Promise<void>;
    stopSync(): Promise<void>;
    destroy(): Promise<void>;
}

export type RObjectInit = {
    type: string;
    payload: Payload;
}

export type RObjectConfig = {
    selfValidate?: boolean;
};

export type RContext = {
    getCrypto(): BasicCrypto;
    getHashSuite(): HashSuite;
    getConfig(): RObjectConfig;
    getRegistry(): RObjectTypeRegistry;

    getObject(id: B64Hash): Promise<RObject | undefined>;
    getDag(id: B64Hash, backendLabel?: string): Promise<Dag>;
    getMesh(label: string): any;

    createObject(init: RObjectInit): Promise<RObject>;
};

export type RObjectFactory = {
    defaults?: { backendLabel?: string; meshLabel?: string };

    computeRootObjectId: (createPayload: Payload, ctx: RContext, parent?: NestingParent) => Promise<B64Hash>;
    
    validateCreationPayload: (createPayload: Payload, ctx: RContext, parent?: NestingParent) => Promise<boolean>;
    executeCreationPayload: (createPayload: Payload, ctx: RContext, scopedDag: ScopedDag) => Promise<B64Hash>;
    
    loadObject: (id: B64Hash, ctx: RContext, parent?: NestingParent) => Promise<RObject>;
}

export type View = {
    getObject(): RObject;
    getVersion(): Version;
    getFromVersion(): Version;
}

export type Event = {
    getObjectId(): B64Hash;
    getType(): string;
    getVersion(): Version;
}

export type RObjectTypeRegistry = {
    lookup(typeName: string): Promise<RObjectFactory>;
    has(typeName: string): boolean;
    register(typeName: string, factory: RObjectFactory): void;
}

export class TypeRegistryMap implements RObjectTypeRegistry {
    private types: Map<string, RObjectFactory> = new Map();

    register(typeName: string, factory: RObjectFactory): void {
        this.types.set(typeName, factory);
    }
    
    has(typeName: string): boolean {
        return this.types.has(typeName);
    }

    async lookup(typeName: string): Promise<RObjectFactory> {
        const factory = this.types.get(typeName);
        if (factory === undefined) {
            throw new Error(`Type '${typeName}' not found in registry`);
        }
        return factory;
    }
}
