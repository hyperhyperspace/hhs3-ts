import { B64Hash, BasicCrypto, HashSuite } from "@hyper-hyper-space/hhs3_crypto";
import { Dag } from "@hyper-hyper-space/hhs3_dag";
import {
    RContext, RObject, RObjectInit, RObjectConfig, RObjectFactory,
    RObjectTypeRegistry, TypeRegistryMap, SyncableObject, RootScopedDag,
} from "@hyper-hyper-space/hhs3_mvt";

export interface DagBackend {
    getOrCreateDag(id: B64Hash, meta: { type: string }): Promise<{ dag: Dag; created: boolean }>;
    openDag(id: B64Hash): Promise<Dag>;
    listDags(): Promise<DagEntry[]>;
}

export interface DagEntry {
    id: B64Hash;
    type: string;
    createdAt: number;
}

export interface ReplicaOptions {
    crypto: BasicCrypto;
    hashSuite: HashSuite;
    config?: RObjectConfig;
}

export class Replica implements RContext {

    private crypto: BasicCrypto;
    private hashSuite: HashSuite;
    private config: RObjectConfig;

    private backends: Map<string, DagBackend> = new Map();
    private meshes: Map<string, any> = new Map();
    private registry: TypeRegistryMap = new TypeRegistryMap();
    private roots: Map<B64Hash, RObject> = new Map();
    private backendByDagId: Map<B64Hash, string> = new Map();

    private dagCache: Map<string, Dag> = new Map();

    constructor(opts: ReplicaOptions) {
        this.crypto = opts.crypto;
        this.hashSuite = opts.hashSuite;
        this.config = opts.config ?? {};
    }

    // --- Attachment and registration ---

    attachBackend(label: string, backend: DagBackend): void {
        this.backends.set(label, backend);
    }

    attachMesh(label: string, mesh: any): void {
        this.meshes.set(label, mesh);
    }

    registerType(name: string, factory: RObjectFactory): void {
        this.registry.register(name, factory);
    }

    // --- Lifecycle ---

    async close(): Promise<void> {
        for (const obj of this.roots.values()) {
            if (isSyncable(obj)) {
                try { await obj.stopSync(); } catch (_) { /* best effort */ }
                try { await obj.destroy(); } catch (_) { /* best effort */ }
            }
        }
        this.roots.clear();
        this.backendByDagId.clear();
        this.dagCache.clear();
    }

    // --- Object creation (idempotent) ---

    async createObject(init: RObjectInit, backendLabel: string = 'default'): Promise<RObject> {
        const factory = await this.registry.lookup(init.type);

        const id = await factory.computeRootObjectId(init.payload, this, undefined);

        const cached = this.roots.get(id);
        if (cached !== undefined) {
            return cached;
        }

        const backend = this.backends.get(backendLabel);
        if (backend === undefined) {
            throw new Error(`No backend attached with label '${backendLabel}'`);
        }

        const valid = await factory.validateCreationPayload(init.payload, this, undefined);
        if (!valid) {
            throw new Error('Invalid creation payload');
        }

        const { dag, created } = await backend.getOrCreateDag(id, { type: init.type });
        this.backendByDagId.set(id, backendLabel);
        this.dagCache.set(`${backendLabel}:${id}`, dag);

        if (created) {
            const scopedDag = new RootScopedDag(dag);
            await factory.executeCreationPayload(init.payload, this, scopedDag);
        }

        const obj = await factory.loadObject(id, this, undefined);
        this.roots.set(id, obj);

        return obj;
    }

    // --- RContext implementation ---

    getCrypto(): BasicCrypto {
        return this.crypto;
    }

    getHashSuite(): HashSuite {
        return this.hashSuite;
    }

    getConfig(): RObjectConfig {
        return this.config;
    }

    getRegistry(): RObjectTypeRegistry {
        return this.registry;
    }

    async getObject(id: B64Hash): Promise<RObject | undefined> {
        return this.roots.get(id);
    }

    async getDag(id: B64Hash, backendLabel?: string): Promise<Dag> {
        const cacheKey = backendLabel ? `${backendLabel}:${id}` : id;

        const cached = this.dagCache.get(cacheKey);
        if (cached !== undefined) return cached;

        const label = backendLabel ?? this.resolveBackendLabelForId(id);
        const backend = this.backends.get(label);
        if (backend === undefined) {
            throw new Error(`No backend attached with label '${label}'`);
        }

        const d = await backend.openDag(id);
        this.dagCache.set(cacheKey, d);
        this.backendByDagId.set(id, label);
        return d;
    }

    getMesh(label: string): any {
        const mesh = this.meshes.get(label);
        if (mesh === undefined) {
            throw new Error(`No mesh attached with label '${label}'`);
        }
        return mesh;
    }

    // --- Internal helpers ---

    private resolveBackendLabelForId(id: B64Hash): string {
        const label = this.backendByDagId.get(id);
        if (label !== undefined) return label;

        if (this.backends.size === 1) {
            return this.backends.keys().next().value!;
        }

        if (this.backends.has('default')) return 'default';

        throw new Error(`Cannot resolve backend for DAG '${id}': no backend mapping and no default backend`);
    }
}

function isSyncable(obj: any): obj is SyncableObject {
    return typeof obj.startSync === 'function';
}
