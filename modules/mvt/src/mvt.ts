import { B64Hash, BasicCrypto, HashSuite } from "@hyper-hyper-space/hhs3_crypto";
import { dag, Dag } from "@hyper-hyper-space/hhs3_dag";

import { json } from "@hyper-hyper-space/hhs3_json";

import { CausalDag, ScopedDag } from "./dag/dag_nesting.js";
import { ValidationResult, validationFailure, validationOk } from "./validation.js";

export const MAX_TYPE_LENGTH = 128;

export type Version = dag.Position;
export const emptyVersion: () => Version = dag.emptyPosition;
export const version: (...hashes: B64Hash[]) => Version = dag.position;
export type Payload = json.Literal;

// Format for the MVT type id on a genesis create payload. Each type's
// create*Format should pin this to a constant for that type id.
export function createPayloadTypeFormat(typeId: string): json.Format {
    return [json.Type.Constant, typeId];
}

export function createPayloadTypeFieldFormat(): json.Format {
    return [json.Type.BoundedString, MAX_TYPE_LENGTH];
}

export function extractCreatePayloadType(payload: Payload): string | undefined {
    if (payload === null || typeof payload !== 'object' || Array.isArray(payload)) {
        return undefined;
    }
    const p = payload as json.LiteralMap;
    if (p['action'] !== 'create') return undefined;
    const type = p['type'];
    return typeof type === 'string' ? type : undefined;
}

export function validateCreatePayloadType(payload: Payload, expectedTypeId: string): ValidationResult {
    return extractCreatePayloadType(payload) === expectedTypeId
        ? validationOk()
        : validationFailure(`create payload type is not '${expectedTypeId}'`);
}

// A cross-object dependency declared by extractForeignDeps. `objectId` is an
// RObject id (getObject / subscribe / getScopedDag().loadEntry) — never pass it
// to getDag. For roots, getId() equals the DAG id by replica construction; that
// coincidence is a storage invariant, not something this type encodes.
export type ForeignDep = {
    objectId: B64Hash;
    requiredHashes: B64Hash[];
}

// A recursive, changes-only node. Nested children carry only their changes (no span):
// versions are identity across nesting, so only the root delta carries start/end/bound.
// `type` is the producing object's type id, a discriminant for narrowing `changes` when
// reading values out of the heterogeneous `nested` map.
export type DeltaChanges<C = unknown> = {
    type: string;
    changes: C;
    nested: ReadonlyMap<B64Hash, DeltaChanges>;
}

// The root delta is the root DeltaChanges plus the span fields (inlined; there is no
// separately exported span type). Only the root delta has these fields.
export type Delta<C = unknown> = DeltaChanges<C> & {
    start: Version;
    end: Version;
    revisionBound: Version;
}

// The unit of composition for delta computation, produced by every object that supports
// deltas (root or nested). `ingest` is called once per walked entry and returns whether
// the entry produced an actual change (own or nested); the boolean is plumbed for the
// Phase 2 tight bound frontier. `finalize` returns this object's changes plus the nested
// subtree, recursing through any child accumulators it spawned.
export type DeltaAccumulator<C = unknown> = {
    ingest(entry: dag.Entry): Promise<boolean>;
    finalize(): Promise<DeltaChanges<C>>;
}

export type RObject = {
    
    getId(): B64Hash;
    getType(): string;

    validatePayload(payload: Payload, at: Version): Promise<ValidationResult>;
    applyPayload(payload: Payload, at: Version): Promise<B64Hash>;

    getView(at?: Version, from?: Version): Promise<View>;
    computeDelta(start: Version, end: Version): Promise<Delta>;
    createDeltaAccumulator(start: Version, end: Version): DeltaAccumulator;

    getScopedDag(): Promise<ScopedDag>;
    getCausalDag(): Promise<CausalDag>;

    extractForeignDeps(payload: Payload, at: Version): ForeignDep[] | undefined;

    // Reactivity. The callback receives the raw DAG frontier (Version) whenever
    // this object's own sub-DAG advances -- never on sibling changes. Consumers
    // deduplicate by their own cursor and pull deltas/views as needed. Register
    // the callback first, then read the current state to establish a cursor:
    // at-least-once delivery follows any subsequent change.
    subscribe(callback: (version: Version) => void): void;
    unsubscribe(callback: (version: Version) => void): void;

    getBackendLabel(): string;
    destroy(): Promise<void>;
}

export type NestingParent = {
    getId(): B64Hash;
    getBackendLabel(): string;
    getScopedDagForChild(childId: B64Hash): Promise<ScopedDag>;
    getCreationDagForChild(childId: B64Hash, at: Version, addPayload: Payload): Promise<ScopedDag>;
    getCausalDag(): Promise<CausalDag>;
}

export type SyncableObject = {
    startSync(): Promise<void>;
    stopSync(): Promise<void>;
}

export type LoadObjectOptions = {
    parent?: NestingParent;
    backendLabel?: string;
};

export type RObjectConfig = {
    selfValidate?: boolean;
};

export type RContext = {
    getCrypto(): BasicCrypto;
    getHashSuite(): HashSuite;
    getConfig(): RObjectConfig;
    getRegistry(): RObjectTypeRegistry;

    getObject(id: B64Hash): Promise<RObject | undefined>;
    getDag(id: B64Hash, backendLabel?: string): Promise<Dag | undefined>;
    getBackendLabel(id: B64Hash): Promise<string | undefined>;
    getMesh(label: string): any;

    createObject(createPayload: Payload, backendLabel?: string): Promise<RObject>;
    registerObject(obj: RObject): void;
    unregisterObject(id: B64Hash): Promise<void>;

    // Bootstrap a not-yet-present object from the mesh (used by sync roots that
    // fan out to members / transitive references). Optional: contexts that do
    // not implement it force callers to handle a missing object as an error.
    fetchObject?(id: B64Hash, opts?: { meshLabel?: string; backendLabel?: string; timeoutMs?: number }): Promise<RObject>;

    // Fetch (hash-verify) a not-yet-present object's create payload WITHOUT
    // materializing it. Lets a caller inspect the create's genesis deps
    // (factory.extractCreationForeignDeps) and defer createObject until those
    // deps are locally satisfiable. Optional, paired with fetchObject.
    fetchCreatePayload?(id: B64Hash, opts?: { meshLabel?: string; timeoutMs?: number }): Promise<Payload>;

    // Fired on first insertion into the object map (createObject cache miss and
    // registerObject). Optional: contexts that omit it cannot wake waiters when
    // a missing foreign-dep target appears. subscribeNewRoot (Replica-only)
    // remains a roots-only bus.
    subscribeNewObject?(callback: (obj: RObject) => void): void;
    unsubscribeNewObject?(callback: (obj: RObject) => void): void;
};

export type RObjectFactory = {
    computeRootObjectId: (createPayload: Payload, ctx: RContext, parent?: NestingParent) => Promise<B64Hash>;
    
    validateCreationPayload: (createPayload: Payload, ctx: RContext, parent?: NestingParent) => Promise<ValidationResult>;
    executeCreationPayload: (createPayload: Payload, ctx: RContext, scopedDag: ScopedDag) => Promise<B64Hash>;
    
    loadObject: (id: B64Hash, ctx: RContext, opts?: LoadObjectOptions) => Promise<RObject>;

    // Cross-object dependencies that must be present (and, for any listed
    // requiredHashes, locally resolvable on the dep's own DAG) BEFORE this create
    // payload can be validated / materialized. Unlike RObject.extractForeignDeps
    // (which gates ops on an existing object), this gates the object's own genesis
    // so a create pinning a non-genesis foreign version is not validated until
    // that version has arrived. Omitted / undefined means "no genesis deps".
    extractCreationForeignDeps?: (createPayload: Payload, ctx: RContext) => Promise<ForeignDep[] | undefined>;
}

export type View = {
    getObject(): RObject;
    getVersion(): Version;
    getFromVersion(): Version;

    getReferences(): Promise<B64Hash[]>;
    resolveRefVersion(refId: B64Hash): Promise<Version>;
}

// Shared implementation of RObject.subscribe/unsubscribe. Each RObject holds
// one of these, constructed with a getter for its ScopedDag. It lazily attaches
// a single listener to the ScopedDag on the first subscriber and detaches on
// the last, translating scoped growth into a raw-frontier callback. The
// ScopedDag chain, in turn, arms its parents lazily -- so an object with no
// subscribers imposes no observation cost anywhere up to the physical store.
export class ScopedDagSubscription {

    private callbacks = new Set<(version: Version) => void>();
    private scopedDag: ScopedDag | undefined = undefined;
    private listener: ((growth: { frontier: Version }) => void) | undefined = undefined;

    constructor(private readonly scopedDagProvider: () => Promise<ScopedDag>) {}

    subscribe(callback: (version: Version) => void): void {
        const wasEmpty = this.callbacks.size === 0;
        this.callbacks.add(callback);
        if (wasEmpty) {
            void this.attach();
        }
    }

    unsubscribe(callback: (version: Version) => void): void {
        this.callbacks.delete(callback);
        if (this.callbacks.size === 0) {
            this.detach();
        }
    }

    private async attach(): Promise<void> {
        if (this.listener !== undefined) return;
        const scopedDag = await this.scopedDagProvider();
        // Guard the async gap: a fast subscribe/unsubscribe may have emptied the
        // set, or another attach may have won the race.
        if (this.callbacks.size === 0 || this.listener !== undefined) return;

        const listener = (growth: { frontier: Version }) => {
            for (const cb of [...this.callbacks]) {
                try { cb(growth.frontier); } catch (_e) { /* keep firing even if a callback throws */ }
            }
        };
        this.listener = listener;
        this.scopedDag = scopedDag;
        scopedDag.addListener(listener);
    }

    private detach(): void {
        if (this.scopedDag !== undefined && this.listener !== undefined) {
            this.scopedDag.removeListener(this.listener);
        }
        this.listener = undefined;
    }
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
