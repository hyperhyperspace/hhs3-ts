// RDb: the sync root and orchestrator for a deployed database. Its DAG records
// deployment membership (advisory, monotonic, add-only); its runtime role is
// to ensure member objects and their transitive references (RSchemas, bound
// foreign groups) are present and syncing in the replica — this is where the
// loosely-specified startSync / stopSync of RObject earn their keep.
//
// ACTIONS (see payload.ts for formats):
//
//   create
//     Genesis of the sync root. Carries: seed, optional name, optional
//     creators (keyId + publicKey; deployment authority when non-empty),
//     hash algorithm.
//
//   add-schema
//     Records an RSchema as part of the deployment (monotonic, no removal in
//     v1; optional free-form `note`). When creators are declared, requires
//     author + signature from a creator.
//
//   add-group
//     Records a deployed RTableGroup as part of the deployment (monotonic;
//     optional free-form `note`). When creators are declared, requires
//     author + signature from a creator.
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
// startSync subscribes to this RDb's DAG (register, then read) and reconciles
// the sync fan-out. Reconcile is two-step and repeatable:
//   Step 1 (closure): BFS the transitive closure — members, each group's own
//     RSchema and bound foreign groups — fetching any object not yet present
//     with an explicit backend label. Backend labels are fixed at create /
//     fetch time and only memoized thereafter, so the whole closure must be
//     present before sessions open (foreign-dep lookup uses getObject, which
//     needs the closure already in the replica map).
//   Step 2 (sessions): open one swarm + sync session per DAG not yet in the
//     session map (including the RDb's own DAG) and activate it.
// Membership is add-only in v1, so reconcile only opens missing sessions.
// stopSync unsubscribes, invalidates in-flight work via an epoch, and tears
// sessions down. An in-flight start that loses to stopSync throws.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, HASH_SHA256, KeyId, PublicKey } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { dag, Entry, position } from "@hyper-hyper-space/hhs3_dag";

import {
    Payload, RObjectFactory, RContext, LoadObjectOptions,
    Version, version, ForeignDep, Delta, DeltaAccumulator, View, RObject,
    SyncableObject, formatValidationFailure, validationFailure, ValidationRejectedError, ValidationResult,
    extractCreatePayloadType,
} from "@hyper-hyper-space/hhs3_mvt";
import { RootScopedDag, ScopedDag, CausalDag, ScopedDagSubscription, signPayload as signPayloadHelper, serializePublicKeyToBase64 } from "@hyper-hyper-space/hhs3_mvt";

import type { IssueReporter, Mesh, PeerAuthorizer, Swarm } from "@hyper-hyper-space/hhs3_mesh";
import { createSyncSession } from "@hyper-hyper-space/hhs3_sync";
import type { SyncSession, SyncTarget } from "@hyper-hyper-space/hhs3_sync";

import type { RDb as RDbContract } from "./interfaces.js";
import { CreateRDbPayload, AddSchemaPayload, AddGroupPayload, RDB_TYPE_ID, SchemaCreator } from "./payload.js";
import { validateRDbPayload } from "./validate_ops.js";
import { resolveMembers } from "./resolve.js";
import { RTableGroupImpl, RTABLE_GROUP_TYPE_ID } from "../rtable_group/group.js";

export { RDB_TYPE_ID } from "./payload.js";

export type RDbRuntimeConfig = {
    meshLabel?: string;
    backendLabel?: string;
    fetchTimeoutMs?: number;
    authorizer?: PeerAuthorizer;
    report?: IssueReporter;
};

class SyncAbortedError extends Error {
    constructor() {
        super('RDb startSync aborted');
        this.name = 'SyncAbortedError';
    }
}

export const rDbFactory: RObjectFactory = {

    computeRootObjectId: async (payload: Payload, ctx: RContext) => {
        const entry = dag.createEntry(payload, {}, position(), ctx.getCrypto().hash(HASH_SHA256));
        return entry.hash;
    },

    validateCreationPayload: async (payload: Payload, ctx: RContext) => {
        if (typeof payload !== 'object' || payload === null || Array.isArray(payload)) {
            return validationFailure("RDb create payload must be an object");
        }
        if ((payload as json.LiteralMap)['action'] !== 'create') return validationFailure("RDb creation action must be 'create'");
        return validateRDbPayload(payload, { mode: 'create', ctx });
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
        creators?: { keyId: KeyId; publicKey: PublicKey }[];
        hashAlgorithm?: string;
    }): Promise<CreateRDbPayload> => {

        const createPayload: CreateRDbPayload = {
            action: 'create',
            type: RDB_TYPE_ID,
            seed: options.seed,
        };
        if (options.name !== undefined) createPayload.name = options.name;
        if (options.creators !== undefined && options.creators.length > 0) {
            createPayload.creators = options.creators.map((c) => ({
                keyId: c.keyId,
                publicKey: serializePublicKeyToBase64(c.publicKey),
            }));
        }
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

    private syncIntent: 'stopped' | 'running' = 'stopped';
    private syncEpoch = 0;
    private startGate: Promise<void> | undefined;
    private reconcileInFlight = false;
    private rescanRequested = false;
    private reconcileIdleWaiters: Array<() => void> = [];

    // A create payload fetched (hash-verified) but not yet materialized because
    // its genesis deps (e.g. a pinned, possibly post-genesis schema version, or a
    // bound foreign group) are not yet locally satisfiable. Retried on each
    // reconcile pass; materialized once every dep object is present and every
    // pinned requiredHash is on that dep's DAG.
    private pendingCreates: Map<B64Hash, { payload: Payload; deps: ForeignDep[] }> = new Map();

    // Creates that were genuinely invalid once their deps were satisfied. Dropped
    // and reported once; never retried within a run.
    private invalidCreates: Set<B64Hash> = new Set();

    // Objects whose own DAG growth may unblock a pending create (a schema
    // advancing to a pinned version). Subscribed when their session opens,
    // unsubscribed on teardown.
    private watchedObjects: Map<B64Hash, RObject> = new Map();

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

    getCreators(): SchemaCreator[] {
        return [...(this.createOp.creators ?? [])];
    }

    isCreator(keyId: KeyId): boolean {
        return this.getCreators().some((c) => c.keyId === keyId);
    }

    getContext(): RContext { return this.ctx; }

    setRuntimeConfig(config: RDbRuntimeConfig): void {
        this.runtimeConfig = { ...this.runtimeConfig, ...config };
    }

    private selfValidate(): boolean {
        return this.ctx.getConfig().selfValidate || false;
    }

    // --- Membership writers ---

    async addSchema(schemaId: B64Hash, note?: string, author?: OwnIdentity, at?: Version): Promise<B64Hash> {
        const base: Omit<AddSchemaPayload, 'author' | 'signature'> = { action: 'add-schema', schemaId };
        if (note !== undefined) base.note = note;
        return this.applyMembership(base, author, at);
    }

    async addGroup(groupId: B64Hash, note?: string, author?: OwnIdentity, at?: Version): Promise<B64Hash> {
        const base: Omit<AddGroupPayload, 'author' | 'signature'> = { action: 'add-group', groupId };
        if (note !== undefined) base.note = note;
        return this.applyMembership(base, author, at);
    }

    private async applyMembership(
        base: Omit<AddSchemaPayload, 'author' | 'signature'> | Omit<AddGroupPayload, 'author' | 'signature'>,
        author?: OwnIdentity,
        at?: Version,
    ): Promise<B64Hash> {
        const scopedDag = await this.getScopedDag();
        at = at ?? await scopedDag.getFrontier();

        let payload: Payload;
        if (this.getCreators().length > 0) {
            if (author === undefined) {
                throw new Error(`RDb membership op '${(base as json.LiteralMap)['action']}' requires an author when the database declares creators`);
            }
            payload = await signPayloadHelper(base as unknown as json.LiteralMap, author);
        } else {
            payload = base as Payload;
        }

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
        return validateRDbPayload(payload, { mode: 'op', rdb: this });
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

    private _subscription: ScopedDagSubscription | undefined;

    private subscription(): ScopedDagSubscription {
        if (this._subscription === undefined) {
            this._subscription = new ScopedDagSubscription(() => this.getScopedDag());
        }
        return this._subscription;
    }

    subscribe(callback: (version: Version) => void): void {
        this.subscription().subscribe(callback);
    }

    unsubscribe(callback: (version: Version) => void): void {
        this.subscription().unsubscribe(callback);
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

    // --- SyncableObject: subscribe + epoch-gated reconcile ---

    async startSync(): Promise<void> {
        if (this.startGate !== undefined && this.syncIntent === 'running') return this.startGate;
        if (this.syncIntent === 'running') return;

        const gate = this.runStart();
        this.startGate = gate;
        try {
            await gate;
        } finally {
            if (this.startGate === gate) this.startGate = undefined;
        }
    }

    private async runStart(): Promise<void> {
        this.syncIntent = 'running';
        const epoch = ++this.syncEpoch;
        try {
            await this.getScopedDag();
            if (!this.isCurrent(epoch)) throw new SyncAbortedError();
            this.subscribe(this.onMembershipChange);
            this.attachNewObjectListener();
            // The first pass sets up the reachable part and registers pending
            // creates; it does NOT block on a schema advancing to a pinned
            // version (that resolves later via dep-growth / new-object wakes),
            // so \sync start does not hang on remote progress.
            await this.requestReconcile(true);
            if (!this.isCurrent(epoch)) throw new SyncAbortedError();
        } catch (err) {
            if (this.syncEpoch === epoch) {
                this.unsubscribe(this.onMembershipChange);
                this.detachNewObjectListener();
                this.syncIntent = 'stopped';
                this.syncEpoch++;
                this.destroyAllSessions();
            }
            throw err;
        }
    }

    async stopSync(): Promise<void> {
        this.syncIntent = 'stopped';
        this.syncEpoch++;
        this.unsubscribe(this.onMembershipChange);
        this.detachNewObjectListener();
        this.destroyAllSessions();
    }

    private attachNewObjectListener(): void {
        if (this.ctx.subscribeNewObject !== undefined) {
            this.ctx.subscribeNewObject(this.onNewObject);
        }
    }

    private detachNewObjectListener(): void {
        if (this.ctx.unsubscribeNewObject !== undefined) {
            this.ctx.unsubscribeNewObject(this.onNewObject);
        }
    }

    // A newly-materialized object may be a dep of a pending create, or a member
    // whose session must open: rescan.
    private readonly onNewObject = (_obj: RObject): void => {
        if (this.syncIntent !== 'running') return;
        void this.requestReconcile(false);
    };

    // A watched object's DAG grew (e.g. a schema reached a pinned version):
    // a pending create may now be materializable.
    private readonly onDepChange = (_version: Version): void => {
        if (this.syncIntent !== 'running') return;
        void this.requestReconcile(false);
    };

    async destroy(): Promise<void> {
        await this.stopSync();
        this._scopedDag = undefined;
        this._causalDag = undefined;
    }

    private readonly onMembershipChange = (_version: Version): void => {
        if (this.syncIntent !== 'running') return;
        void this.requestReconcile(false);
    };

    private isCurrent(epoch: number): boolean {
        return this.syncIntent === 'running' && this.syncEpoch === epoch;
    }

    private destroyAllSessions(): void {
        for (const obj of this.watchedObjects.values()) {
            try { obj.unsubscribe(this.onDepChange); } catch { /* best effort */ }
        }
        this.watchedObjects.clear();
        this.pendingCreates.clear();
        this.invalidCreates.clear();

        const sessions = [...this.syncSessions.values()];
        this.syncSessions.clear();
        for (const { swarm, session } of sessions) {
            session.destroy();
            swarm.destroy();
        }
    }

    private waitUntilReconcileIdle(): Promise<void> {
        if (!this.reconcileInFlight) return Promise.resolve();
        return new Promise((resolve) => { this.reconcileIdleWaiters.push(resolve); });
    }

    private async requestReconcile(throwOnError: boolean): Promise<void> {
        while (this.reconcileInFlight) {
            if (!throwOnError) {
                this.rescanRequested = true;
                return;
            }
            await this.waitUntilReconcileIdle();
            if (this.syncIntent !== 'running') throw new SyncAbortedError();
        }

        this.reconcileInFlight = true;
        const epoch = this.syncEpoch;
        try {
            let delayMs = 100;
            for (;;) {
                this.rescanRequested = false;
                if (!this.isCurrent(epoch)) {
                    if (throwOnError) throw new SyncAbortedError();
                    return;
                }
                try {
                    await this.runReconcilePass(epoch);
                } catch (err) {
                    if (err instanceof SyncAbortedError) {
                        if (throwOnError) throw err;
                        return;
                    }
                    if (!this.isCurrent(epoch)) {
                        if (throwOnError) throw new SyncAbortedError();
                        return;
                    }
                    if (throwOnError) throw err;
                    this.rescanRequested = true;
                    await this.sleep(delayMs);
                    delayMs = Math.min(delayMs * 2, 2000);
                    continue;
                }
                if (!this.isCurrent(epoch)) {
                    if (throwOnError) throw new SyncAbortedError();
                    return;
                }
                if (!this.rescanRequested) return;
            }
        } finally {
            this.reconcileInFlight = false;
            const waiters = this.reconcileIdleWaiters.splice(0);
            for (const w of waiters) w();
        }
    }

    // One incremental reconcile pass. BFS the closure (members + each present
    // group's schema and bound foreign groups). For every id:
    //   - present  -> open its session immediately (so a schema syncs while the
    //                 group that pins it is still pending) and fan out;
    //   - absent   -> fetch its create payload (once), register it as pending,
    //                 enqueue its genesis deps so they fetch / session / advance,
    //                 and materialize it as soon as those deps are satisfiable.
    // Materialized objects are fanned out from too. Objects that stay pending are
    // retried on the next pass, woken by dep-growth (onDepChange) or a new object
    // appearing (onNewObject). The genesis dep graph is acyclic by hash
    // construction, so this terminates once the mesh delivers.
    private async runReconcilePass(epoch: number): Promise<void> {
        const members = await this.resolveAt();
        if (!this.isCurrent(epoch)) throw new SyncAbortedError();

        const mesh = this.ctx.getMesh(this.runtimeConfig.meshLabel ?? 'default') as Mesh;

        const visited = new Set<B64Hash>();
        const queue: B64Hash[] = [this.createOpId, ...members.schemaIds, ...members.groupIds];

        while (queue.length > 0) {
            const id = queue.shift()!;
            if (visited.has(id)) continue;
            visited.add(id);
            if (!this.isCurrent(epoch)) throw new SyncAbortedError();

            if (id === this.createOpId) {
                await this.ensureSession(id, mesh, epoch);
                continue;
            }

            if (this.invalidCreates.has(id)) continue;

            let obj = await this.ctx.getObject(id);
            if (!this.isCurrent(epoch)) throw new SyncAbortedError();

            if (obj === undefined) {
                // Prefer the deps-aware path (fetch the create payload, hold it
                // pending until its genesis deps are satisfiable). Contexts that
                // only expose fetchObject fall back to direct materialization.
                obj = this.ctx.fetchCreatePayload !== undefined
                    ? await this.tryMaterialize(id, epoch)
                    : await this.fetchAndMaterializeDirect(id);
                if (!this.isCurrent(epoch)) throw new SyncAbortedError();
            }

            if (obj === undefined) {
                // Still pending: keep its deps in the frontier so they get
                // fetched, sessioned and advanced toward the pinned version.
                const pending = this.pendingCreates.get(id);
                if (pending !== undefined) {
                    for (const dep of pending.deps) queue.push(dep.objectId);
                }
                continue;
            }

            await this.ensureSession(id, mesh, epoch);

            if (obj.getType() === RTABLE_GROUP_TYPE_ID) {
                const group = obj as RTableGroupImpl;
                queue.push(group.getSchemaRef());
                for (const boundId of Object.values(group.getBindings())) queue.push(boundId);
            }
        }
    }

    // Fetch (once) and try to materialize a not-yet-present object. Returns the
    // object if it was (or already is) materialized, otherwise undefined (still
    // pending or dropped as invalid). Throws SyncAbortedError on epoch change and
    // rethrows a fetch failure so the reconcile loop can back off / retry.
    private async tryMaterialize(id: B64Hash, epoch: number): Promise<RObject | undefined> {
        let pending = this.pendingCreates.get(id);
        if (pending === undefined) {
            const payload = await this.fetchCreatePayloadFor(id);
            if (!this.isCurrent(epoch)) throw new SyncAbortedError();
            const deps = await this.creationDepsFor(payload);
            if (!this.isCurrent(epoch)) throw new SyncAbortedError();
            pending = { payload, deps };
            this.pendingCreates.set(id, pending);
        }

        // Deps satisfiable? Every dep object present AND every pinned requiredHash
        // resolvable on that dep's own DAG.
        for (const dep of pending.deps) {
            const depObj = await this.ctx.getObject(dep.objectId);
            if (!this.isCurrent(epoch)) throw new SyncAbortedError();
            if (depObj === undefined) return undefined;
            const scoped = await depObj.getScopedDag();
            for (const requiredHash of dep.requiredHashes) {
                if (await scoped.loadEntry(requiredHash) === undefined) return undefined;
            }
        }

        try {
            const backendLabel = this.runtimeConfig.backendLabel ?? this.backendLabel;
            const obj = await this.ctx.createObject(pending.payload, backendLabel);
            this.pendingCreates.delete(id);
            return obj;
        } catch (err) {
            if (err instanceof SyncAbortedError) throw err;
            if (err instanceof ValidationRejectedError) {
                // Genuinely invalid once deps were satisfied: drop, report once,
                // never retry within this run.
                this.pendingCreates.delete(id);
                this.invalidCreates.add(id);
                this.reportInvalidCreate(id, err);
                return undefined;
            }
            // Transient infra error (e.g. a dep raced away): keep pending, retry.
            return undefined;
        }
    }

    // Legacy path for contexts that expose fetchObject but not fetchCreatePayload
    // (they cannot inspect a create's deps without materializing it). Fetches and
    // materializes in one step, as reconcile did before deps-aware materialization.
    private async fetchAndMaterializeDirect(id: B64Hash): Promise<RObject> {
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
            if (err instanceof SyncAbortedError) throw err;
            throw new Error(`Failed to fetch object '${id}' for RDb sync: ${(err as Error).message}`);
        }
    }

    private async creationDepsFor(payload: Payload): Promise<ForeignDep[]> {
        const typeId = extractCreatePayloadType(payload);
        if (typeId === undefined) return [];
        const factory = await this.ctx.getRegistry().lookup(typeId);
        if (factory.extractCreationForeignDeps === undefined) return [];
        return (await factory.extractCreationForeignDeps(payload, this.ctx)) ?? [];
    }

    private async fetchCreatePayloadFor(id: B64Hash): Promise<Payload> {
        if (this.ctx.fetchCreatePayload === undefined) {
            throw new Error(`Object '${id}' is not present in the replica and the context cannot fetch create payloads`);
        }
        try {
            return await this.ctx.fetchCreatePayload(id, {
                meshLabel: this.runtimeConfig.meshLabel ?? 'default',
                timeoutMs: this.runtimeConfig.fetchTimeoutMs,
            });
        } catch (err) {
            if (err instanceof SyncAbortedError) throw err;
            throw new Error(`Failed to fetch create payload for '${id}' for RDb sync: ${(err as Error).message}`);
        }
    }

    private reportInvalidCreate(id: B64Hash, err: ValidationRejectedError): void {
        this.runtimeConfig.report?.({
            source: 'rdb',
            kind: 'validation-failed',
            severity: 'moderate',
            opHash: id,
            dagId: this.createOpId,
            message: `RDb member create '${id}' is invalid: ${formatValidationFailure(err.why)}`,
        });
    }

    // Open a sync session for a present object (idempotent). Also watches the
    // object's growth so a pending create that depends on it advancing retries.
    private async ensureSession(id: B64Hash, mesh: Mesh, epoch: number): Promise<void> {
        if (this.syncSessions.has(id)) return;
        if (!this.isCurrent(epoch)) return;

        const rObject = id === this.createOpId ? this : await this.ctx.getObject(id);
        if (!this.isCurrent(epoch)) return;
        if (rObject === undefined) return;   // not materialized yet; a later pass opens it

        const label = id === this.createOpId
            ? this.backendLabel
            : (await this.ctx.getBackendLabel(id)) ?? this.backendLabel;
        if (!this.isCurrent(epoch)) return;

        const rawDag = await this.ctx.getDag(id, label);
        if (!this.isCurrent(epoch)) return;
        if (rawDag === undefined) throw new Error(`DAG '${id}' not found during startSync`);

        if (this.syncSessions.has(id)) return;

        const swarm = mesh.createSwarm(id, {
            authorizer: this.runtimeConfig.authorizer,
        });
        let session: SyncSession | undefined;
        try {
            const target: SyncTarget = {
                dagId: id,
                dag: rawDag,
                rObject,
                hashSuite: this.ctx.getHashSuite(),
                ctx: this.ctx,
            };
            session = createSyncSession(target, [swarm], { report: this.runtimeConfig.report });
            if (!this.isCurrent(epoch) || this.syncSessions.has(id)) {
                session.destroy();
                swarm.destroy();
                return;
            }
            this.syncSessions.set(id, { swarm, session });
            swarm.activate();

            if (id !== this.createOpId && !this.watchedObjects.has(id)) {
                rObject.subscribe(this.onDepChange);
                this.watchedObjects.set(id, rObject);
            }
        } catch (err) {
            session?.destroy();
            swarm.destroy();
            throw err;
        }
    }

    private sleep(ms: number): Promise<void> {
        return new Promise((resolve) => { setTimeout(resolve, ms); });
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
