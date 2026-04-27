import { B64Hash, BasicCrypto, HashSuite } from "@hyper-hyper-space/hhs3_crypto";
import { Dag } from "@hyper-hyper-space/hhs3_dag";
import {
    RContext, RObject, RObjectInit, RObjectConfig, RObjectFactory,
    RObjectTypeRegistry, TypeRegistryMap, SyncableObject, RootScopedDag,
} from "@hyper-hyper-space/hhs3_mvt";

export interface DagBackend {
    createDag(id: B64Hash, meta: { type: string }): Promise<Dag>;
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

interface ManifestEntry {
    type: string;
    backendLabel: string;
    createdAt: number;
}

export class Replica implements RContext {

    private crypto: BasicCrypto;
    private hashSuite: HashSuite;
    private config: RObjectConfig;

    private backends: Map<string, DagBackend> = new Map();
    private meshes: Map<string, any> = new Map();
    private registry: TypeRegistryMap = new TypeRegistryMap();
    private roots: Map<B64Hash, RObject> = new Map();
    private manifest: Map<B64Hash, ManifestEntry> = new Map();

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

    /**
     * Reconstitute all root objects from attached backends.
     *
     * Merges `listDags()` from every backend, sorts by (createdAt, backendLabel, id),
     * and loads each root via its factory.
     *
     * Contract for `factory.loadObject`: may read its own DAG and may call
     * `ctx.getRegistry().register(name, factory)`, but **must not** eagerly call
     * `ctx.getObject()` or `ctx.createObject()`. Cross-root work is deferred to
     * method calls on the returned RObject.
     */
    async start(): Promise<void> {
        type SortableEntry = DagEntry & { backendLabel: string };
        const allEntries: SortableEntry[] = [];

        for (const [label, backend] of this.backends) {
            const entries = await backend.listDags();
            for (const e of entries) {
                allEntries.push({ ...e, backendLabel: label });
            }
        }

        allEntries.sort((a, b) => {
            if (a.createdAt !== b.createdAt) return a.createdAt - b.createdAt;
            if (a.backendLabel !== b.backendLabel) return a.backendLabel < b.backendLabel ? -1 : 1;
            return a.id < b.id ? -1 : a.id > b.id ? 1 : 0;
        });

        for (const entry of allEntries) {
            if (this.roots.has(entry.id)) continue;

            this.manifest.set(entry.id, {
                type: entry.type,
                backendLabel: entry.backendLabel,
                createdAt: entry.createdAt,
            });

            if (!this.registry.has(entry.type)) {
                continue;
            }

            const factory = await this.registry.lookup(entry.type);
            const obj = await factory.loadObject(entry.id, this, undefined);
            this.roots.set(entry.id, obj);
        }
    }

    async close(): Promise<void> {
        for (const obj of this.roots.values()) {
            if (isSyncable(obj)) {
                try { await obj.stopSync(); } catch (_) { /* best effort */ }
                try { await obj.destroy(); } catch (_) { /* best effort */ }
            }
        }
        this.roots.clear();
        this.manifest.clear();
        this.dagCache.clear();
    }

    // --- Object creation (idempotent) ---

    async createObject(init: RObjectInit): Promise<RObject> {
        const factory = await this.registry.lookup(init.type);

        const id = await factory.computeRootObjectId(init.payload, this, undefined);

        if (this.roots.has(id)) {
            return this.roots.get(id)!;
        }

        if (this.manifest.has(id)) {
            const obj = await factory.loadObject(id, this, undefined);
            this.roots.set(id, obj);
            return obj;
        }

        const backendLabel = resolveBackendLabel(init.payload, factory);
        const backend = this.backends.get(backendLabel);
        if (backend === undefined) {
            throw new Error(`No backend attached with label '${backendLabel}'`);
        }

        const valid = await factory.validateCreationPayload(init.payload, this, undefined);
        if (!valid) {
            throw new Error('Invalid creation payload');
        }

        await backend.createDag(id, { type: init.type });

        this.manifest.set(id, {
            type: init.type,
            backendLabel,
            createdAt: Date.now(),
        });

        const rawDag = await backend.openDag(id);
        const scopedDag = new RootScopedDag(rawDag);
        await factory.executeCreationPayload(init.payload, this, scopedDag);
        const obj = await factory.loadObject(id, this, undefined);
        this.roots.set(id, obj);

        return obj;
    }

    // --- Sync ---

    async startSync(id: B64Hash): Promise<void> {
        const obj = this.roots.get(id);
        if (obj !== undefined && isSyncable(obj)) {
            await obj.startSync();
        }
    }

    async stopSync(id: B64Hash): Promise<void> {
        const obj = this.roots.get(id);
        if (obj !== undefined && isSyncable(obj)) {
            await obj.stopSync();
        }
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
        const entry = this.manifest.get(id);
        if (entry !== undefined) return entry.backendLabel;

        if (this.backends.size === 1) {
            return this.backends.keys().next().value!;
        }

        if (this.backends.has('default')) return 'default';

        throw new Error(`Cannot resolve backend for DAG '${id}': no manifest entry and no default backend`);
    }
}

function resolveBackendLabel(payload: any, factory: RObjectFactory): string {
    if (payload !== null && typeof payload === 'object' && typeof payload['backendLabel'] === 'string') {
        return payload['backendLabel'];
    }
    if (factory.defaults?.backendLabel !== undefined) {
        return factory.defaults.backendLabel;
    }
    return 'default';
}

function isSyncable(obj: any): obj is SyncableObject {
    return typeof obj.startSync === 'function';
}
