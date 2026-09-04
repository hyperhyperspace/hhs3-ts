import { B64Hash, BasicCrypto, HashSuite } from "@hyper-hyper-space/hhs3_crypto";
import { Dag } from "@hyper-hyper-space/hhs3_dag";
import type { Mesh, TopicId } from "@hyper-hyper-space/hhs3_mesh";
import {
    RContext, RObject, Payload, RObjectConfig, RObjectFactory,
    RObjectTypeRegistry, TypeRegistryMap, RootScopedDag,
    extractCreatePayloadType, formatValidationFailure, ValidationRejectedError,
} from "@hyper-hyper-space/hhs3_mvt";
import { fetchInit } from "@hyper-hyper-space/hhs3_sync";

export interface DagBackend {
    getOrCreateDag(id: B64Hash, meta: { type: string }): Promise<{ dag: Dag; created: boolean }>;
    openDag(id: B64Hash): Promise<Dag | undefined>;
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
    private objects: Map<B64Hash, RObject> = new Map();
    private rootIds: Set<B64Hash> = new Set();
    private backendByDagId: Map<B64Hash, string> = new Map();
    private newRootCallbacks = new Set<(obj: RObject) => void>();
    private newObjectCallbacks = new Set<(obj: RObject) => void>();

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

    detachMesh(label: string): void {
        this.meshes.delete(label);
    }

    registerType(name: string, factory: RObjectFactory): void {
        this.registry.register(name, factory);
    }

    // --- Lifecycle ---

    async destroy(): Promise<void> {
        for (const id of [...this.rootIds]) {
            const obj = this.objects.get(id);
            if (obj === undefined) continue;
            try { await obj.destroy(); } catch (_) { /* best effort */ }
            this.releaseObject(id);
        }
        this.rootIds.clear();
        this.objects.clear();
        this.backendByDagId.clear();
        this.dagCache.clear();
        this.newRootCallbacks.clear();
        this.newObjectCallbacks.clear();
    }

    /** @deprecated Use destroy() */
    async close(): Promise<void> {
        return this.destroy();
    }

    getRootIds(): ReadonlySet<B64Hash> {
        return this.rootIds;
    }

    subscribeNewRoot(callback: (obj: RObject) => void): void {
        this.newRootCallbacks.add(callback);
    }

    unsubscribeNewRoot(callback: (obj: RObject) => void): void {
        this.newRootCallbacks.delete(callback);
    }

    subscribeNewObject(callback: (obj: RObject) => void): void {
        this.newObjectCallbacks.add(callback);
    }

    unsubscribeNewObject(callback: (obj: RObject) => void): void {
        this.newObjectCallbacks.delete(callback);
    }

    // --- Object registry ---

    registerObject(obj: RObject): void {
        this.recordObject(obj);
    }

    async unregisterObject(id: B64Hash): Promise<void> {
        if (this.rootIds.has(id)) {
            throw new Error(`Cannot unregister root object '${id}'`);
        }
        const obj = this.objects.get(id);
        if (obj === undefined) return;
        await obj.destroy();
        this.releaseObject(id);
    }

    private recordObject(obj: RObject): void {
        const id = obj.getId();
        const isNew = !this.objects.has(id);
        this.objects.set(id, obj);
        const label = obj.getBackendLabel();
        this.backendByDagId.set(id, label);
        if (isNew) this.notifyNewObject(obj);
    }

    private releaseObject(id: B64Hash): void {
        this.objects.delete(id);
        this.backendByDagId.delete(id);
        for (const key of [...this.dagCache.keys()]) {
            if (key === id || key.endsWith(`:${id}`)) {
                this.dagCache.delete(key);
            }
        }
    }

    // --- Object creation (idempotent) ---

    async createObject(createPayload: Payload, backendLabel: string = 'default'): Promise<RObject> {
        const typeId = extractCreatePayloadType(createPayload);
        if (typeId === undefined) {
            throw new Error('create payload missing type');
        }

        const factory = await this.registry.lookup(typeId);

        const id = await factory.computeRootObjectId(createPayload, this, undefined);

        const cached = this.objects.get(id);
        if (cached !== undefined) {
            return cached;
        }

        const backend = this.backends.get(backendLabel);
        if (backend === undefined) {
            throw new Error(`No backend attached with label '${backendLabel}'`);
        }

        const result = await factory.validateCreationPayload(createPayload, this, undefined);
        if (!result.valid) {
            throw new ValidationRejectedError(formatValidationFailure(result.why), result.why);
        }

        const { dag, created } = await backend.getOrCreateDag(id, { type: typeId });
        this.backendByDagId.set(id, backendLabel);
        this.dagCache.set(`${backendLabel}:${id}`, dag);

        if (created) {
            const scopedDag = new RootScopedDag(dag);
            await factory.executeCreationPayload(createPayload, this, scopedDag);
        }

        const obj = await factory.loadObject(id, this, { backendLabel });
        this.recordObject(obj);
        this.rootIds.add(id);
        this.notifyNewRoot(obj);

        return obj;
    }

    // --- Remote object fetching ---

    async fetchObject(
        id: B64Hash,
        opts?: { meshLabel?: string; backendLabel?: string; timeoutMs?: number },
    ): Promise<RObject> {
        const backendLabel = opts?.backendLabel ?? 'default';

        const existing = this.objects.get(id);
        if (existing !== undefined) return existing;

        const createPayload = await this.fetchCreatePayload(id, opts);
        return await this.createObject(createPayload, backendLabel);
    }

    // Fetch and hash-verify an object's create payload without materializing it,
    // so a caller can inspect its genesis deps before deciding to createObject.
    async fetchCreatePayload(
        id: B64Hash,
        opts?: { meshLabel?: string; timeoutMs?: number },
    ): Promise<Payload> {
        const meshLabel = opts?.meshLabel ?? 'default';
        const mesh = this.getMesh(meshLabel) as Mesh;
        const swarm = mesh.createSwarm(id as TopicId);

        try {
            swarm.activate();
            return await fetchInit(id, [swarm], this.hashSuite, opts?.timeoutMs);
        } finally {
            swarm.destroy();
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
        return this.objects.get(id);
    }

    async getBackendLabel(id: B64Hash): Promise<string | undefined> {
        return this.backendByDagId.get(id);
    }

    async getDag(id: B64Hash, backendLabel?: string): Promise<Dag | undefined> {
        const cacheKey = backendLabel ? `${backendLabel}:${id}` : id;

        const cached = this.dagCache.get(cacheKey);
        if (cached !== undefined) return cached;

        const label = backendLabel ?? this.tryResolveBackendLabelForId(id);
        if (label === undefined) return undefined;

        const backend = this.backends.get(label);
        if (backend === undefined) return undefined;

        const d = await backend.openDag(id);
        if (d === undefined) return undefined;

        this.dagCache.set(`${label}:${id}`, d);
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

    private tryResolveBackendLabelForId(id: B64Hash): string | undefined {
        const label = this.backendByDagId.get(id);
        if (label !== undefined) return label;

        if (this.backends.size === 1) {
            return this.backends.keys().next().value!;
        }

        if (this.backends.has('default')) return 'default';

        return undefined;
    }

    private notifyNewRoot(obj: RObject): void {
        for (const cb of [...this.newRootCallbacks]) {
            try { cb(obj); } catch { /* keep firing even if a callback throws */ }
        }
    }

    private notifyNewObject(obj: RObject): void {
        for (const cb of [...this.newObjectCallbacks]) {
            try { cb(obj); } catch { /* keep firing even if a callback throws */ }
        }
    }
}
