// RDb: the sync root and orchestrator for a database deployed from a catalog.
// Its DAG records which catalog releases the admin deployed and with which
// params; its member groups are computed from those releases by the normative
// instantiation (instantiate.ts). Its runtime role is to keep the catalog, the
// referenced schemas and the member groups present and syncing, and to adopt
// deployed releases into the members' local gates.
//
// ACTIONS (see payload.ts for formats):
//
//   create
//     Genesis of the sync root: seed, optional name and creators (deployment
//     authority when non-empty), the catalog, the genesis release, its params,
//     hash algorithm. Validated without the catalog.
//
//   update-catalog
//     Deploys a later release of the same catalog (forward only) with the
//     params it first needs. Depends on the catalog at that release. When
//     creators are declared, requires author + signature from a creator.
//
// Invariants:
//   - No group's validity ever depends on its RDb: groups never observe it.
//   - Membership is computed, never stored: every replica derives the same
//     group ids from the deployed releases, the params and the creators.
//     Membership is computed ∪ assigned; assigned is empty in v1.
//   - A deployed release is adopted by this replica only within its adoption
//     range (adoption.ts); synced deploys to versions not adopted wait.
//
// startSync subscribes to this RDb's DAG (register, then read) and reconciles
// the sync fan-out, repeatably:
//   - the catalog comes through the creation-deps path (its genesis pins
//     schemas): fetch its create payload, sync those schemas, then create it;
//   - every schema the catalog references (genesis pins plus declares) is
//     fetched and synced, so a declared schema arrives before the release
//     that needs it validates;
//   - absent member groups are created from their locally computed payloads
//     once their genesis deps are present (peers never serve group creates),
//     together with their replica-local RDeployGate, which gets no session;
//   - each present object gets one swarm + sync session, and its growth
//     triggers a rescan (onDepChange);
//   - the adoption policy runs after every pass.
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
import { CreateRDbPayload, UpdateCatalogPayload, ParamValue, RDB_TYPE_ID, SchemaCreator } from "./payload.js";
import { validateRDbPayload } from "./validate_ops.js";
import { RDbOps, RDbResolution, collectOps, resolveRDb } from "./resolve.js";
import { adoptedReleases, runAdoptionPolicy } from "./adoption.js";
import type { Membership } from "./instantiate.js";
import { RTableGroupImpl, RTABLE_GROUP_TYPE_ID } from "../rtable_group/group.js";
import type { CreateTableGroupPayload } from "../rtable_group/payload.js";
import { RCatalogImpl, RCATALOG_TYPE_ID } from "../rcatalog/rcatalog.js";
import type { CatalogIndex } from "../rcatalog/resolve.js";
import { versionKey } from "../rcatalog/resolve.js";
import { isValidSemverRange, majorRange } from "../rcatalog/semver.js";
import { ensureDeployGate } from "../rdeploy_gate/rdeploy_gate.js";

export { RDB_TYPE_ID } from "./payload.js";

export type RDbRuntimeConfig = {
    meshLabel?: string;
    backendLabel?: string;
    fetchTimeoutMs?: number;
    authorizer?: PeerAuthorizer;
    report?: IssueReporter;
    // The adoption range (a semver range, e.g. '^1' or '*'); defaults to the
    // major version of the release the RDb was created at.
    adoptionRange?: string;
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
        catalog: B64Hash;
        release: B64Hash;
        params?: { [name: string]: ParamValue };
        hashAlgorithm?: string;
    }): Promise<CreateRDbPayload> => {

        const createPayload: CreateRDbPayload = {
            action: 'create',
            type: RDB_TYPE_ID,
            seed: options.seed,
            catalog: options.catalog,
            release: options.release,
        };
        if (options.name !== undefined) createPayload.name = options.name;
        if (options.creators !== undefined && options.creators.length > 0) {
            createPayload.creators = options.creators.map((c) => ({
                keyId: c.keyId,
                publicKey: serializePublicKeyToBase64(c.publicKey),
            }));
        }
        if (options.params !== undefined && Object.keys(options.params).length > 0) createPayload.params = options.params;
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

    // A create payload (fetched and hash-verified, or computed locally for a
    // member group) not yet materialized because its genesis deps (a pinned,
    // possibly post-genesis schema version, a bound group) are not yet locally
    // satisfiable. Retried on each reconcile pass.
    private pendingCreates: Map<B64Hash, { payload: Payload; deps: ForeignDep[] }> = new Map();

    // Creates that were genuinely invalid once their deps were satisfied. Dropped
    // and reported once; never retried within a run.
    private invalidCreates: Set<B64Hash> = new Set();

    // Objects whose own DAG growth may unblock a pending create or change the
    // membership (the catalog). Subscribed when their session opens.
    private watchedObjects: Map<B64Hash, RObject> = new Map();

    private resolutionCache: { key: string; resolution: RDbResolution } | undefined;

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
    getName(): string | undefined { return this.createOp.name; }
    hashAlgorithm(): string | undefined { return this.createOp.hashAlgorithm; }
    getCatalogRef(): B64Hash { return this.createOp.catalog; }
    getCreateRelease(): B64Hash { return this.createOp.release; }

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

    // --- Catalog access ---

    async getCatalog(): Promise<RCatalogImpl | undefined> {
        const obj = await this.ctx.getObject(this.getCatalogRef());
        if (obj === undefined || obj.getType() !== RCATALOG_TYPE_ID) return undefined;
        return obj as RCatalogImpl;
    }

    async getCatalogIndex(): Promise<CatalogIndex | undefined> {
        return (await this.getCatalog())?.getIndex();
    }

    // --- Writer ---

    async updateCatalog(
        release: B64Hash, params?: { [name: string]: ParamValue }, author?: OwnIdentity, note?: string, at?: Version,
    ): Promise<B64Hash> {
        const prepared = await this.prepareUpdateCatalog(release, params, author, note, at);

        if (this.selfValidate()) {
            const result = await this.validatePayload(prepared.payload, prepared.at);
            if (!result.valid) {
                throw new ValidationRejectedError(formatValidationFailure(result.why), result.why);
            }
        }
        return this.applyPayload(prepared.payload, prepared.at);
    }

    // The (signed, when creators are declared) update-catalog payload at `at`
    // (defaults to the frontier), without appending it.
    async prepareUpdateCatalog(
        release: B64Hash, params?: { [name: string]: ParamValue }, author?: OwnIdentity, note?: string, at?: Version,
    ): Promise<{ payload: json.LiteralMap; at: Version }> {
        const scopedDag = await this.getScopedDag();
        at = at ?? await scopedDag.getFrontier();

        const base: json.LiteralMap = { action: 'update-catalog', catalog: this.getCatalogRef(), release };
        if (params !== undefined && Object.keys(params).length > 0) base['params'] = params as unknown as json.Literal;
        if (note !== undefined) base['note'] = note;

        if (this.getCreators().length === 0) return { payload: base, at };
        if (author === undefined) {
            throw new Error("update-catalog requires an author when the database declares creators");
        }
        return { payload: await signPayloadHelper(base, author, at), at };
    }

    // --- Resolution ---

    async opsAt(at?: Version): Promise<RDbOps> {
        const scopedDag = await this.getScopedDag();
        at = at ?? await scopedDag.getFrontier();
        const entries: Entry[] = [];
        for await (const entry of scopedDag.loadAllEntries()) entries.push(entry);
        return collectOps(entries, at);
    }

    async resolve(at?: Version): Promise<RDbResolution> {
        const scopedDag = await this.getScopedDag();
        const frontier = await scopedDag.getFrontier();
        at = at ?? frontier;

        const catalog = await this.getCatalog();
        const catalogKey = catalog === undefined ? '-' : versionKey(await (await catalog.getScopedDag()).getFrontier());
        const key = `${versionKey(at)}|${catalogKey}`;
        if (this.resolutionCache !== undefined && this.resolutionCache.key === key) return this.resolutionCache.resolution;

        const resolution = resolveRDb({
            rdbId: this.createOpId,
            ops: await this.opsAt(at),
            catalog: catalog === undefined ? undefined : await catalog.getIndex(),
        });
        this.resolutionCache = { key, resolution };
        return resolution;
    }

    // The resolution as if `release` were deployed now with `params` (the
    // catalog planner's view of the outcome).
    async resolvePlanned(release: B64Hash, params?: { [name: string]: ParamValue }): Promise<RDbResolution> {
        const ops = await this.opsAt();
        const planned: UpdateCatalogPayload = { action: 'update-catalog', catalog: this.getCatalogRef(), release };
        if (params !== undefined && Object.keys(params).length > 0) planned.params = params;
        return resolveRDb({
            rdbId: this.createOpId,
            ops: { ...ops, updates: [...ops.updates, { hash: '~planned', payload: planned }] },
            catalog: await this.getCatalogIndex(),
        });
    }

    async getDeployedReleases(at?: Version): Promise<B64Hash[]> {
        return (await this.resolve(at)).deployed ?? [];
    }

    async getDeployHistory(at?: Version): Promise<B64Hash[]> {
        return (await this.resolve(at)).history;
    }

    async getParams(at?: Version): Promise<{ [name: string]: ParamValue }> {
        return (await this.resolve(at)).params;
    }

    async getMembership(at?: Version): Promise<Membership | undefined> {
        return (await this.resolve(at)).membership;
    }

    async getMemberGroupNames(at?: Version): Promise<Map<string, B64Hash>> {
        return new Map((await this.getMembership(at))?.names ?? []);
    }

    async getMemberGroupPayloads(at?: Version): Promise<Map<B64Hash, CreateTableGroupPayload>> {
        const out = new Map<B64Hash, CreateTableGroupPayload>();
        const membership = await this.getMembership(at);
        for (const hash of membership?.order ?? []) {
            const member = membership!.byHash.get(hash)!;
            out.set(member.id, member.payload);
        }
        return out;
    }

    async getMemberGroups(): Promise<B64Hash[]> {
        const membership = await this.getMembership();
        return (membership?.order ?? []).map((hash) => membership!.byHash.get(hash)!.id);
    }

    async getMemberSchemas(): Promise<B64Hash[]> {
        const membership = await this.getMembership();
        const schemas: B64Hash[] = [];
        for (const hash of membership?.order ?? []) {
            const schema = membership!.byHash.get(hash)!.def.schemaRef;
            if (!schemas.includes(schema)) schemas.push(schema);
        }
        return schemas;
    }

    private async creationDepsSatisfied(deps: ForeignDep[]): Promise<boolean> {
        for (const dep of deps) {
            const depObj = await this.ctx.getObject(dep.objectId);
            if (depObj === undefined) return false;
            const scoped = await depObj.getScopedDag();
            for (const requiredHash of dep.requiredHashes) {
                if (await scoped.loadEntry(requiredHash) === undefined) return false;
            }
        }
        return true;
    }

    async materializeMembers(): Promise<B64Hash[]> {
        const membership = await this.getMembership();
        if (membership === undefined) return [];

        const backendLabel = this.runtimeConfig.backendLabel ?? this.backendLabel;
        const created: B64Hash[] = [];
        for (const hash of membership.order) {
            const member = membership.byHash.get(hash)!;
            if (await this.ctx.getObject(member.id) === undefined) {
                const deps = await this.creationDepsFor(member.payload);
                if (!await this.creationDepsSatisfied(deps)) continue;
                await this.ctx.createObject(member.payload, backendLabel);
                created.push(member.id);
            }
            await ensureDeployGate(this.ctx, member.id, member.def.schemaRef, backendLabel);
        }
        return created;
    }

    // --- Adoption ---

    async getAdoptionRange(): Promise<string> {
        if (this.runtimeConfig.adoptionRange !== undefined) return this.runtimeConfig.adoptionRange;
        const state = (await this.resolve()).releases?.get(this.getCreateRelease());
        return state === undefined ? '*' : majorRange(state.version);
    }

    async setAdoptionRange(range: string): Promise<void> {
        if (!isValidSemverRange(range)) throw new Error(`invalid adoption range '${range}'`);
        this.runtimeConfig = { ...this.runtimeConfig, adoptionRange: range };
        await this.adopt();
    }

    async getAdoptedReleases(): Promise<B64Hash[]> {
        return adoptedReleases(await this.resolve(), await this.getAdoptionRange());
    }

    async adopt(): Promise<void> {
        await runAdoptionPolicy(this);
    }

    // --- RObject interface ---

    async validatePayload(payload: Payload, at: Version): Promise<ValidationResult> {
        if (typeof payload !== 'object' || payload === null || Array.isArray(payload)) {
            return validationFailure("RDb payload must be an object", { objectHash: this.createOpId });
        }
        const action = (payload as json.LiteralMap)['action'];
        // genesis-only action; never a valid post-creation op
        if (action !== 'update-catalog') {
            return validationFailure(`action '${String(action)}' is not an RDb op`, { objectHash: this.createOpId });
        }
        return validateRDbPayload(payload, { mode: 'op', rdb: this, at });
    }

    async applyPayload(payload: Payload, at: Version): Promise<B64Hash> {
        const scopedDag = await this.getScopedDag();
        return await scopedDag.append(payload, {}, at);
    }

    async getView(at?: Version, from?: Version): Promise<View> {
        const scopedDag = await this.getScopedDag();
        at = at ?? await scopedDag.getFrontier();
        from = from ?? await scopedDag.getFrontier();
        const resolution = await this.resolve(at);
        const members = resolution.membership;
        const schemas = new Set<B64Hash>();
        const groups: B64Hash[] = [];
        for (const hash of members?.order ?? []) {
            const member = members!.byHash.get(hash)!;
            schemas.add(member.def.schemaRef);
            groups.push(member.id);
        }
        return new RDbView(this, at, from, this.getCatalogRef(), resolution.deployed ?? [], [...schemas, ...groups]);
    }

    // An update-catalog depends on the catalog at the deployed release. The
    // create has no deps (it is validated without the catalog).
    extractForeignDeps(payload: Payload, _at: Version): ForeignDep[] | undefined {
        if (typeof payload !== 'object' || payload === null || Array.isArray(payload)) return undefined;
        if ((payload as json.LiteralMap)['action'] !== 'update-catalog') return undefined;
        const update = payload as unknown as UpdateCatalogPayload;
        return [{ objectId: update.catalog, requiredHashes: [update.release] }];
    }

    async computeDelta(_start: Version, _end: Version): Promise<Delta> {
        throw new Error("RDb has no delta in v1");
    }

    createDeltaAccumulator(_start: Version, _end: Version): DeltaAccumulator {
        throw new Error("RDb has no delta in v1");
    }

    private _subscription: ScopedDagSubscription | undefined;

    private subscription(): ScopedDagSubscription {
        if (this._subscription === undefined) {
            this._subscription = new ScopedDagSubscription(() => this.getScopedDag());
        }
        return this._subscription;
    }

    subscribe(callback: (version: Version) => void): Promise<void> {
        return this.subscription().subscribe(callback);
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
            await this.subscribe(this.onMembershipChange);
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

    // A watched object's DAG grew (a schema reached a pinned version, the
    // catalog gained a release or a declare): rescan.
    private readonly onDepChange = (_version: Version): void => {
        if (this.syncIntent !== 'running') return;
        void this.requestReconcile(false);
    };

    async destroy(): Promise<void> {
        await this.stopSync();
        this._scopedDag = undefined;
        this._causalDag = undefined;
        this.resolutionCache = undefined;
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

    // One incremental reconcile pass. BFS from [rdb, catalog]: the catalog fans
    // out to the schemas it references and (once resolvable) the member
    // groups; each present group fans out to its schema and bound groups. For
    // every id:
    //   - present  -> open its session (so a schema syncs while the object that
    //                 pins it is still pending) and fan out;
    //   - absent   -> a computed member is created from its local payload, any
    //                 other object has its create payload fetched (once); it is
    //                 held pending until its genesis deps are satisfiable, and
    //                 its deps are enqueued so they fetch / session / advance.
    // Objects that stay pending are retried on the next pass, woken by
    // dep-growth (onDepChange) or a new object appearing (onNewObject). The
    // genesis dep graph is acyclic by hash construction, so this terminates
    // once the mesh delivers. The adoption policy runs after the pass.
    private async runReconcilePass(epoch: number): Promise<void> {
        const mesh = this.ctx.getMesh(this.runtimeConfig.meshLabel ?? 'default') as Mesh;

        const visited = new Set<B64Hash>();
        const catalogId = this.getCatalogRef();
        const queue: B64Hash[] = [this.createOpId, catalogId];
        let membership = (await this.resolve()).membership;
        if (!this.isCurrent(epoch)) throw new SyncAbortedError();

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
                const member = membership?.byId.get(id);
                if (member !== undefined) {
                    obj = await this.tryMaterialize(id, epoch, member.payload as unknown as Payload);
                } else {
                    // Prefer the deps-aware path (fetch the create payload, hold
                    // it pending until its genesis deps are satisfiable).
                    // Contexts that only expose fetchObject fall back to direct
                    // materialization.
                    obj = this.ctx.fetchCreatePayload !== undefined
                        ? await this.tryMaterialize(id, epoch)
                        : await this.fetchAndMaterializeDirect(id);
                }
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

            if (id === catalogId && obj.getType() === RCATALOG_TYPE_ID) {
                const catalog = obj as RCatalogImpl;
                const frontier = await (await catalog.getScopedDag()).getFrontier();
                for (const schema of [...(await catalog.getIndex()).referencedSchemasAt(frontier)].sort()) queue.push(schema);
                membership = (await this.resolve()).membership;
                if (!this.isCurrent(epoch)) throw new SyncAbortedError();
                for (const hash of membership?.order ?? []) queue.push(membership!.byHash.get(hash)!.id);
            } else if (obj.getType() === RTABLE_GROUP_TYPE_ID) {
                const group = obj as RTableGroupImpl;
                queue.push(group.getSchemaRef());
                for (const boundId of Object.values(group.getBindings())) queue.push(boundId);
                if (membership?.byId.has(id) === true) {
                    await ensureDeployGate(this.ctx, id, group.getSchemaRef(), this.runtimeConfig.backendLabel ?? this.backendLabel);
                }
            }
        }

        if (!this.isCurrent(epoch)) throw new SyncAbortedError();
        try {
            await this.adopt();
        } catch (err) {
            // transient (e.g. a schema still syncing): a later pass retries
            this.runtimeConfig.report?.({
                source: 'rdb',
                kind: 'adoption-deferred',
                severity: 'low',
                dagId: this.createOpId,
                message: `adoption deferred: ${(err as Error).message}`,
            });
        }
    }

    // Try to materialize a not-yet-present object: a computed member from its
    // local payload, anything else from its fetched (once) create payload.
    // Returns the object if it was (or already is) materialized, otherwise
    // undefined (still pending or dropped as invalid). Throws SyncAbortedError
    // on epoch change and rethrows a fetch failure so the reconcile loop can
    // back off / retry.
    private async tryMaterialize(id: B64Hash, epoch: number, localPayload?: Payload): Promise<RObject | undefined> {
        let pending = this.pendingCreates.get(id);
        if (pending === undefined) {
            const payload = localPayload ?? await this.fetchCreatePayloadFor(id);
            if (!this.isCurrent(epoch)) throw new SyncAbortedError();
            const deps = await this.creationDepsFor(payload);
            if (!this.isCurrent(epoch)) throw new SyncAbortedError();
            pending = { payload, deps };
            this.pendingCreates.set(id, pending);
        }

        const satisfied = await this.creationDepsSatisfied(pending.deps);
        if (!this.isCurrent(epoch)) throw new SyncAbortedError();
        if (!satisfied) return undefined;

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
    // materializes in one step.
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

        if (id !== this.createOpId && !this.watchedObjects.has(id)) {
            try {
                await rObject.subscribe(this.onDepChange);
            } catch (err) {
                rObject.unsubscribe(this.onDepChange);
                throw err;
            }
            if (!this.isCurrent(epoch) || this.syncSessions.has(id)) {
                rObject.unsubscribe(this.onDepChange);
                return;
            }
            this.watchedObjects.set(id, rObject);
        }

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

// The RDb view: its references are the catalog, the member schemas and the
// member groups. The catalog resolves to the deployed releases.
class RDbView implements View {
    constructor(
        private obj: RDbImpl,
        private at: Version,
        private from: Version,
        private catalog: B64Hash,
        private deployed: B64Hash[],
        private members: B64Hash[],
    ) {}

    getObject(): RObject { return this.obj; }
    getVersion(): Version { return this.at; }
    getFromVersion(): Version { return this.from; }

    async getReferences(): Promise<B64Hash[]> { return [this.catalog, ...this.members]; }

    async resolveRefVersion(refId: B64Hash): Promise<Version> {
        return refId === this.catalog ? version(...this.deployed) : version();
    }
}
