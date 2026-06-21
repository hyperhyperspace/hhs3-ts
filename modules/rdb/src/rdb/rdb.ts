// RDb: the sync root and orchestrator for a deployed database. Its DAG records
// deployment membership (advisory, monotonic, add-only); its runtime role is
// to ensure member objects and their transitive references (RSchemas, bound
// foreign groups) are present and syncing in the replica — this is where the
// loosely-specified startSync / stopSync of RObject earn their keep.
//
// ACTIONS (see payload.ts for formats):
//
//   create
//     Genesis of the sync root. Carries: seed, optional name, hash algorithm.
//
//   add-schema
//     Records an RSchema as part of the deployment (monotonic, no removal in
//     v1; optional free-form `note`).
//
//   add-group
//     Records a deployed RTableGroup as part of the deployment (monotonic;
//     optional free-form `note`).
//
// Invariants:
//   - RDb state is ADVISORY: nothing's validity ever depends on it; groups are
//     fully valid and verifiable without their RDb. extractForeignDeps returns
//     undefined for this reason (membership never gates op validation).
//   - Membership is keyed by schema / group id. The optional `note` is a
//     free-form comment: never resolved, never a key. Name resolution for
//     qualified FK / exists / idProvider targets does NOT go through the RDb —
//     each RTableGroup fixes its own `bindings` (name -> group id) at creation.
//   - A member or referenced object missing from the replica triggers a mesh
//     fetch (ctx.fetchObject); if that is unavailable or fails, it is an
//     infrastructure error (throw), never an MVT data condition.
//
// startSync is two-step:
//   Step 1 (closure): BFS the transitive closure — members, each group's own
//     RSchema and bound foreign groups — fetching any object not yet present
//     with an explicit backend label. Backend labels are fixed at create /
//     fetch time and only memoized thereafter, so the whole closure must be
//     present before sessions open (resolveRefDag relies on a memoized label).
//   Step 2 (sessions): open one swarm + sync session per DAG in the closure
//     (including the RDb's own DAG) and activate it. stopSync tears them down.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, HASH_SHA256 } from "@hyper-hyper-space/hhs3_crypto";
import { dag, Entry, position } from "@hyper-hyper-space/hhs3_dag";

import {
    Payload, RObjectFactory, RContext, LoadObjectOptions,
    Version, version, ForeignDep, Event, Delta, DeltaAccumulator, View, RObject,
    SyncableObject, formatValidationFailure, validationFailure, ValidationRejectedError, ValidationResult,
} from "@hyper-hyper-space/hhs3_mvt";
import { RootScopedDag, ScopedDag, CausalDag } from "@hyper-hyper-space/hhs3_mvt";

import type { Mesh, Swarm } from "@hyper-hyper-space/hhs3_mesh";
import { createSyncSession } from "@hyper-hyper-space/hhs3_sync";
import type { SyncSession, SyncTarget } from "@hyper-hyper-space/hhs3_sync";

import type { RDb as RDbContract } from "./interfaces.js";
import { CreateRDbPayload, AddSchemaPayload, AddGroupPayload, RDB_TYPE_ID } from "./payload.js";
import { validateRDbPayloadFormat } from "./validate.js";
import { resolveMembers } from "./resolve.js";
import { RTableGroupImpl, RTABLE_GROUP_TYPE_ID } from "../rtable_group/group.js";

export { RDB_TYPE_ID } from "./payload.js";

export type RDbRuntimeConfig = {
    meshLabel?: string;
    backendLabel?: string;
    fetchTimeoutMs?: number;
};

export const rDbFactory: RObjectFactory = {

    computeRootObjectId: async (payload: Payload, ctx: RContext) => {
        const entry = dag.createEntry(payload, {}, position(), ctx.getCrypto().hash(HASH_SHA256));
        return entry.hash;
    },

    validateCreationPayload: async (payload: Payload, _ctx: RContext) => {
        if (typeof payload !== 'object' || payload === null || Array.isArray(payload)) {
            return validationFailure("RDb create payload must be an object");
        }
        if ((payload as json.LiteralMap)['action'] !== 'create') return validationFailure("RDb creation action must be 'create'");
        return validateRDbPayloadFormat(payload);
    },

    executeCreationPayload: async (payload: Payload, _ctx: RContext, scopedDag: ScopedDag) => {
        return await scopedDag.append(payload, {}, position());
    },

    loadObject: async (id: B64Hash, ctx: RContext, opts?: LoadObjectOptions) => {
        if (opts?.parent !== undefined) {
            throw new Error("RDb is a standalone object (no nesting parent)");
        }

        const backendLabel = opts?.backendLabel ?? 'default';
        const rawDag = await ctx.getDag(id, backendLabel);
        if (rawDag === undefined) throw new Error(`DAG '${id}' not found`);

        const scopedDag = new RootScopedDag(rawDag);
        const createOp = (await scopedDag.loadEntry(id))!.payload as CreateRDbPayload;
        return new RDbImpl(id, createOp, ctx, backendLabel);
    },
};

export class RDbImpl implements RDbContract, SyncableObject {

    static create = async (options: {
        seed: string;
        name?: string;
        hashAlgorithm?: string;
    }): Promise<CreateRDbPayload> => {

        const createPayload: CreateRDbPayload = {
            action: 'create',
            type: RDB_TYPE_ID,
            seed: options.seed,
        };
        if (options.name !== undefined) createPayload.name = options.name;
        if (options.hashAlgorithm !== undefined) createPayload.hashAlgorithm = options.hashAlgorithm;

        return createPayload;
    };

    static typeId = RDB_TYPE_ID;

    createOpId: B64Hash;
    createOp: CreateRDbPayload;
    private ctx: RContext;
    private readonly backendLabel: string;

    private _scopedDag: ScopedDag | undefined;
    private _causalDag: CausalDag | undefined;

    private runtimeConfig: RDbRuntimeConfig = {};
    private syncSessions: Map<B64Hash, { swarm: Swarm; session: SyncSession }> = new Map();

    constructor(createOpId: B64Hash, createOp: CreateRDbPayload, ctx: RContext, backendLabel: string = 'default') {
        this.createOpId = createOpId;
        this.createOp = createOp;
        this.ctx = ctx;
        this.backendLabel = backendLabel;
    }

    getId(): B64Hash { return this.createOpId; }
    getType(): string { return RDbImpl.typeId; }
    getBackendLabel(): string { return this.backendLabel; }

    seed(): string { return this.createOp.seed; }
    hashAlgorithm(): string | undefined { return this.createOp.hashAlgorithm; }

    getContext(): RContext { return this.ctx; }

    setRuntimeConfig(config: RDbRuntimeConfig): void {
        this.runtimeConfig = { ...this.runtimeConfig, ...config };
    }

    private selfValidate(): boolean {
        return this.ctx.getConfig().selfValidate || false;
    }

    // --- Membership writers ---

    async addSchema(schemaId: B64Hash, note?: string, at?: Version): Promise<B64Hash> {
        const payload: AddSchemaPayload = { action: 'add-schema', schemaId };
        if (note !== undefined) payload.note = note;
        return this.applyMembership(payload, at);
    }

    async addGroup(groupId: B64Hash, note?: string, at?: Version): Promise<B64Hash> {
        const payload: AddGroupPayload = { action: 'add-group', groupId };
        if (note !== undefined) payload.note = note;
        return this.applyMembership(payload, at);
    }

    private async applyMembership(payload: Payload, at?: Version): Promise<B64Hash> {
        const scopedDag = await this.getScopedDag();
        at = at ?? await scopedDag.getFrontier();

        if (this.selfValidate()) {
            const result = await this.validatePayload(payload, at);
            if (!result.valid) {
                throw new ValidationRejectedError(formatValidationFailure(result.why), result.why);
            }
        }
        return this.applyPayload(payload, at);
    }

    // --- Membership resolution ---

    async getMemberSchemas(): Promise<B64Hash[]> {
        return (await this.resolveAt()).schemaIds;
    }

    async getMemberGroups(): Promise<B64Hash[]> {
        return (await this.resolveAt()).groupIds;
    }

    private async resolveAt(at?: Version): Promise<{ schemaIds: B64Hash[]; groupIds: B64Hash[] }> {
        const scopedDag = await this.getScopedDag();
        at = at ?? await scopedDag.getFrontier();

        const entries: Entry[] = [];
        for await (const entry of scopedDag.loadAllEntries()) entries.push(entry);

        return resolveMembers(entries, at);
    }

    // --- RObject interface ---

    async validatePayload(payload: Payload, _at: Version): Promise<ValidationResult> {
        if (typeof payload !== 'object' || payload === null || Array.isArray(payload)) {
            return validationFailure("RDb membership payload must be an object", { objectHash: this.createOpId });
        }
        const action = (payload as json.LiteralMap)['action'];
        // genesis-only action; never a valid post-creation op
        if (action !== 'add-schema' && action !== 'add-group') {
            return validationFailure(`action '${String(action)}' is not an RDb membership op`, { objectHash: this.createOpId });
        }
        return validateRDbPayloadFormat(payload);
    }

    async applyPayload(payload: Payload, at: Version): Promise<B64Hash> {
        const scopedDag = await this.getScopedDag();
        return await scopedDag.append(payload, {}, at);
    }

    async getView(at?: Version, from?: Version): Promise<View> {
        const scopedDag = await this.getScopedDag();
        at = at ?? await scopedDag.getFrontier();
        from = from ?? await scopedDag.getFrontier();
        const members = await this.resolveAt(at);
        return new RDbView(this, at, from, [...members.schemaIds, ...members.groupIds]);
    }

    // RDb membership is advisory: validity never depends on member presence, so
    // it declares no foreign deps (fetch is a runtime startSync concern).
    extractForeignDeps(_payload: Payload, _at: Version): ForeignDep[] | undefined {
        return undefined;
    }

    async computeDelta(_start: Version, _end: Version): Promise<Delta> {
        throw new Error("RDb is advisory; no delta in v1");
    }

    createDeltaAccumulator(_start: Version, _end: Version): DeltaAccumulator {
        throw new Error("RDb is advisory; no delta in v1");
    }

    subscribe(_callback: (event: Event) => void): void {
        throw new Error("Method not implemented.");
    }

    unsubscribe(_callback: (event: Event) => void): void {
        throw new Error("Method not implemented.");
    }

    async getScopedDag(): Promise<ScopedDag> {
        if (this._scopedDag === undefined) {
            const rawDag = await this.ctx.getDag(this.createOpId, this.backendLabel);
            if (rawDag === undefined) throw new Error(`DAG '${this.createOpId}' not found`);
            this._scopedDag = new RootScopedDag(rawDag);
        }
        return this._scopedDag;
    }

    async getCausalDag(): Promise<CausalDag> {
        if (this._causalDag === undefined) {
            const rawDag = await this.ctx.getDag(this.createOpId, this.backendLabel);
            if (rawDag === undefined) throw new Error(`DAG '${this.createOpId}' not found`);
            this._causalDag = rawDag;
        }
        return this._causalDag;
    }

    // --- SyncableObject: two-step fan-out ---

    async startSync(): Promise<void> {
        if (this.syncSessions.size > 0) return;   // idempotent

        // Step 1: build the transitive closure, fetching anything missing.
        const closure = await this.ensureClosurePresent();

        // Step 2: open one sync session per DAG in the closure.
        const mesh = this.ctx.getMesh(this.runtimeConfig.meshLabel ?? 'default') as Mesh;

        for (const id of closure) {
            if (this.syncSessions.has(id)) continue;

            const rObject = id === this.createOpId ? this : await this.ctx.getObject(id);
            if (rObject === undefined) throw new Error(`Object '${id}' vanished from the replica during startSync`);

            const label = id === this.createOpId
                ? this.backendLabel
                : (await this.ctx.getBackendLabel(id)) ?? this.backendLabel;

            const rawDag = await this.ctx.getDag(id, label);
            if (rawDag === undefined) throw new Error(`DAG '${id}' not found during startSync`);

            const swarm = mesh.createSwarm(id);
            const target: SyncTarget = {
                dagId: id,
                dag: rawDag,
                rObject,
                hashSuite: this.ctx.getHashSuite(),
                resolveRefDag: async (refId) =>
                    this.ctx.getDag(refId, (await this.ctx.getBackendLabel(refId)) ?? this.backendLabel),
            };

            const session = createSyncSession(target, [swarm]);
            swarm.activate();
            this.syncSessions.set(id, { swarm, session });
        }
    }

    // BFS the closure of member ids + each group's schema + bound foreign
    // groups, fetching any object not yet present in the replica. Returns the
    // set of DAG ids to sync (including the RDb's own DAG).
    private async ensureClosurePresent(): Promise<B64Hash[]> {
        const members = await this.resolveAt();

        const visited = new Set<B64Hash>();
        const order: B64Hash[] = [];
        const queue: B64Hash[] = [this.createOpId, ...members.schemaIds, ...members.groupIds];

        while (queue.length > 0) {
            const id = queue.shift()!;
            if (visited.has(id)) continue;
            visited.add(id);
            order.push(id);

            if (id === this.createOpId) continue;   // RDb itself already present

            const obj = await this.ensurePresent(id);

            // groups pull in their own schema and bound foreign groups
            if (obj.getType() === RTABLE_GROUP_TYPE_ID) {
                const group = obj as RTableGroupImpl;
                queue.push(group.getSchemaRef());
                for (const boundId of Object.values(group.getBindings())) queue.push(boundId);
            }
        }

        return order;
    }

    // Return the object for `id`, fetching it from the mesh if it is not yet
    // present. A missing object with no fetch capability is an infra error.
    private async ensurePresent(id: B64Hash): Promise<RObject> {
        const existing = await this.ctx.getObject(id);
        if (existing !== undefined) return existing;

        if (this.ctx.fetchObject === undefined) {
            throw new Error(`Object '${id}' is not present in the replica and the context cannot fetch it`);
        }

        try {
            return await this.ctx.fetchObject(id, {
                meshLabel: this.runtimeConfig.meshLabel ?? 'default',
                backendLabel: this.runtimeConfig.backendLabel ?? this.backendLabel,
                timeoutMs: this.runtimeConfig.fetchTimeoutMs,
            });
        } catch (err) {
            throw new Error(`Failed to fetch object '${id}' for RDb sync: ${(err as Error).message}`);
        }
    }

    async stopSync(): Promise<void> {
        for (const { swarm, session } of this.syncSessions.values()) {
            session.destroy();
            swarm.destroy();
        }
        this.syncSessions.clear();
    }

    async destroy(): Promise<void> {
        await this.stopSync();
        this._scopedDag = undefined;
        this._causalDag = undefined;
    }
}

// Minimal advisory membership view: members are the references; RDb pins no
// reference versions.
class RDbView implements View {
    constructor(
        private obj: RDbImpl,
        private at: Version,
        private from: Version,
        private members: B64Hash[],
    ) {}

    getObject(): RObject { return this.obj; }
    getVersion(): Version { return this.at; }
    getFromVersion(): Version { return this.from; }

    async getReferences(): Promise<B64Hash[]> { return [...this.members]; }

    async resolveRefVersion(_refId: B64Hash): Promise<Version> {
        return version();   // RDb does not pin observed versions
    }
}
