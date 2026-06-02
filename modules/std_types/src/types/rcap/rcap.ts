import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, HASH_SHA256, KeyId, PublicKey } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { dag, MetaProps, position, Position, EntryPredicate } from "@hyper-hyper-space/hhs3_dag";

import { Payload, RObject, RObjectFactory, RObjectInit, RContext, SyncableObject, version, Version, View, Delta, ForeignDep } from "@hyper-hyper-space/hhs3_mvt";
import { RootScopedDag, ScopedDag, CausalDag } from "@hyper-hyper-space/hhs3_mvt";

import type { Mesh, Swarm } from "@hyper-hyper-space/hhs3_mesh";
import { createSyncSession } from "@hyper-hyper-space/hhs3_sync";
import type { SyncSession, SyncTarget } from "@hyper-hyper-space/hhs3_sync";

import { deserializePublicKeyFromBase64, signPayload as signPayloadHelper } from "../../authorship.js";

import {
    CreateRCapPayload, CapDefinition,
    AddIdentityPayload,
    CreateCapabilityPayload,
    DeleteCapabilityPayload,
    GrantPayload, MAX_CAP_ORIGINS,
    RevokePayload,
    CapPayload,
} from "./payload.js";

import type { RCapEvent } from "./events.js";
import type { RCap as RCapContract, RCapView as RCapViewContract } from "./interfaces.js";
import { validateRCapPayload } from "./validate.js";

const DEFAULT_ENROLL_CAPABILITY = 'enroll';

export const rCapFactory: RObjectFactory = {

    computeRootObjectId: async (payload: json.Literal, ctx: RContext) => {
        const entry = dag.createEntry(payload, {}, position(), ctx.getCrypto().hash(HASH_SHA256));
        return entry.hash;
    },

    validateCreationPayload: async (payload: json.Literal, ctx: RContext) =>
        validateRCapPayload(payload, { mode: "create", ctx }),

    executeCreationPayload: async (payload: json.Literal, _ctx: RContext, scopedDag: ScopedDag) => {
        const createPayload = payload as CreateRCapPayload;
        const meta: MetaProps = {
            caps: json.toSet(Object.keys(createPayload.initialCaps)),
        };
        return await scopedDag.append(payload, meta, position());
    },

    loadObject: async (id: B64Hash, ctx: RContext) => {
        const rawDag = await ctx.getDag(id);
        if (rawDag === undefined) throw new Error(`DAG '${id}' not found`);
        const scopedDag = new RootScopedDag(rawDag);
        const createOp = (await scopedDag.loadEntry(id))!.payload as CreateRCapPayload;
        return new RCapImpl(id, createOp, ctx);
    }
};

export type RCapRuntimeConfig = {
    meshLabel?: string;
    backendLabel?: string;
};

export class RCapImpl implements RCapContract {

    static create = async (options: {
        seed: string;
        creators: { keyId: KeyId; publicKey: PublicKey }[];
        initialCaps: { [capName: string]: CapDefinition };
        enrollCapability?: string;
        hashAlgorithm?: string;
    }): Promise<RObjectInit> => {
        const { serializePublicKeyToBase64 } = await import("../../authorship.js");

        const createPayload: CreateRCapPayload = {
            action: 'create',
            seed: options.seed,
            creators: options.creators.map(c => c.keyId),
            creatorKeys: options.creators.map(c => serializePublicKeyToBase64(c.publicKey)),
            initialCaps: options.initialCaps,
            hashAlgorithm: options.hashAlgorithm || 'sha256',
        };

        if (options.enrollCapability !== undefined) {
            createPayload.enrollCapability = options.enrollCapability;
        }

        return { type: RCapImpl.typeId, payload: createPayload };
    }

    static typeId = "hhs/cap_v1";

    createOpId: B64Hash;
    createOp: CreateRCapPayload;
    private ctx: RContext;

    private _scopedDag: ScopedDag | undefined;
    private _swarm: Swarm | undefined;
    private _syncSession: SyncSession | undefined;
    private runtimeConfig: RCapRuntimeConfig = { meshLabel: 'default', backendLabel: 'default' };
    private deltaStrategy: RCapDeltaStrategy = 'full';

    constructor(createOpId: B64Hash, createOp: CreateRCapPayload, ctx: RContext) {
        this.createOpId = createOpId;
        this.createOp = createOp;
        this.ctx = ctx;
    }

    getId(): string { return this.createOpId; }
    getType(): string { return RCapImpl.typeId; }

    getEnrollCapabilityName(): string {
        return this.createOp.enrollCapability ?? DEFAULT_ENROLL_CAPABILITY;
    }

    isCreator(keyId: KeyId): boolean {
        return this.createOp.creators.includes(keyId);
    }

    lookupCreatorKey(keyId: KeyId): PublicKey | undefined {
        const idx = this.createOp.creators.indexOf(keyId);
        if (idx < 0) return undefined;
        return deserializePublicKeyFromBase64(this.createOp.creatorKeys[idx]);
    }

    async lookupKey(keyId: KeyId): Promise<PublicKey | undefined> {
        const creatorKey = this.lookupCreatorKey(keyId);
        if (creatorKey !== undefined) return creatorKey;

        const scopedDag = await this.getScopedDag();
        const frontier = await scopedDag.getFrontier();
        const cover = await scopedDag.findCoverWithFilter(frontier, { containsValues: { ids: [keyId] } });

        for (const hash of cover) {
            const entry = await scopedDag.loadEntry(hash);
            if (entry === undefined) continue;
            const p = entry.payload as CapPayload;
            if (p.action === 'add-identity' && p.keyId === keyId) {
                return deserializePublicKeyFromBase64(p.publicKey);
            }
        }

        return undefined;
    }

    getHashSuite() {
        return this.ctx.getHashSuite();
    }

    selfValidate(): boolean {
        return this.ctx.getConfig().selfValidate || false;
    }

    private async applyValidatedPayload(payload: Payload, at: Version): Promise<B64Hash> {
        if (this.selfValidate()) {
            if (!await this.validatePayload(payload, at)) {
                throw new Error("Attempted to apply an invalid payload");
            }
        }
        return this.applyPayload(payload, at);
    }

    // Convenience methods

    async addIdentity(
        keyId: KeyId, publicKey: string,
        author: OwnIdentity,
        at?: Version,
    ): Promise<B64Hash> {
        const payload: Omit<AddIdentityPayload, 'author' | 'signature'> = {
            action: 'add-identity', keyId, publicKey,
        };
        const signed = await signPayloadHelper(payload as json.LiteralMap, author);
        const dag = await this.getScopedDag();
        at = at || await dag.getFrontier();
        return this.applyValidatedPayload(signed, at);
    }

    async createCap(
        capName: string, managedBy: string[],
        author: OwnIdentity,
        at?: Version,
    ): Promise<B64Hash> {
        const payload: Omit<CreateCapabilityPayload, 'author' | 'signature'> = {
            action: 'create-cap', capName, managedBy,
        };
        const signed = await signPayloadHelper(payload as json.LiteralMap, author);
        const dag = await this.getScopedDag();
        at = at || await dag.getFrontier();
        return this.applyValidatedPayload(signed, at);
    }

    async deleteCap(
        capName: string,
        author: OwnIdentity,
        at?: Version,
    ): Promise<B64Hash> {
        const payload: Omit<DeleteCapabilityPayload, 'author' | 'signature'> = {
            action: 'delete-cap', capName,
        };
        const signed = await signPayloadHelper(payload as json.LiteralMap, author);
        const dag = await this.getScopedDag();
        at = at || await dag.getFrontier();
        return this.applyValidatedPayload(signed, at);
    }

    async grant(
        grantee: KeyId, capName: string,
        author: OwnIdentity,
        at?: Version,
    ): Promise<B64Hash> {
        const scopedDag = await this.getScopedDag();
        at = at || await scopedDag.getFrontier();
        const view = await this.getView(at, at);
        const capOrigins = Array.from(await view.currentCapCreationVersion(capName))
            .sort()
            .slice(0, MAX_CAP_ORIGINS);
        const payload: Omit<GrantPayload, 'author' | 'signature'> = {
            action: 'grant', grantee, capName, capOrigins,
        };
        const signed = await signPayloadHelper(payload as json.LiteralMap, author);
        return this.applyValidatedPayload(signed, at);
    }

    async revoke(
        grantee: KeyId, capName: string,
        author: OwnIdentity,
        at?: Version,
    ): Promise<B64Hash> {
        const payload: Omit<RevokePayload, 'author' | 'signature'> = {
            action: 'revoke', grantee, capName,
        };
        const signed = await signPayloadHelper(payload as json.LiteralMap, author);
        const dag = await this.getScopedDag();
        at = at || await dag.getFrontier();
        return this.applyValidatedPayload(signed, at);
    }

    // RObject interface

    async validatePayload(payload: json.Literal, at: Version): Promise<boolean> {
        return validateRCapPayload(payload, { mode: "op", cap: this, at });
    }

    async applyPayload(payload: Payload, at: Version): Promise<B64Hash> {
        const p = payload as CapPayload;
        const meta: MetaProps = {};
        const scopedDag = await this.getScopedDag();

        switch (p.action) {
            case 'add-identity':
                meta['ids'] = json.toSet([p.keyId]);
                break;
            case 'create-cap':
                meta['caps'] = json.toSet([p.capName]);
                break;
            case 'delete-cap':
                meta['caps'] = json.toSet([p.capName]);
                meta['barrier'] = json.toSet(['t']);
                break;
            case 'grant':
                meta['grants'] = json.toSet([p.capName + ':' + p.grantee]);
                break;
            case 'revoke':
                meta['grants'] = json.toSet([p.capName + ':' + p.grantee]);
                meta['barrier'] = json.toSet(['t']);
                break;
            default:
                throw new Error("Invalid cap action in payload: " + (p as any).action);
        }

        return await scopedDag.append(payload, meta, at);
    }

    async getView(at?: Version, from?: Version): Promise<RCapViewContract> {
        const scopedDag = await this.getScopedDag();
        at = at || await scopedDag.getFrontier();
        from = from || await scopedDag.getFrontier();
        return new RCapViewImpl(this, at, from);
    }

    extractForeignDeps(_payload: Payload, _at: Version): ForeignDep[] | undefined {
        return undefined;
    }

    subscribe(_callback: (event: RCapEvent) => void): void {
        throw new Error("Method not implemented.");
    }

    unsubscribe(_callback: (event: RCapEvent) => void): void {
        throw new Error("Method not implemented.");
    }

    setDeltaStrategy(strategy: RCapDeltaStrategy): void {
        this.deltaStrategy = strategy;
    }

    async computeDelta(start: Version, end: Version): Promise<RCapDelta> {
        if (this.deltaStrategy === 'bounded') {
            return this.computeDeltaBounded(start, end);
        } else if (this.deltaStrategy === 'full') {
            return this.computeDeltaFull(start, end);
        } else {
            throw new Error("Invalid delta strategy: " + this.deltaStrategy);
        }
    }

    private addGrantPair(grantPairs: Map<string, Set<KeyId>>, capName: string, keyId: KeyId): void {
        if (!grantPairs.has(capName)) grantPairs.set(capName, new Set());
        grantPairs.get(capName)!.add(keyId);
    }

    private collectCandidatesFromEntries(
        entries: Iterable<dag.Entry>,
    ): { keyIds: Set<KeyId>; capNames: Set<string>; grantPairs: Map<string, Set<KeyId>> } {
        const keyIds = new Set<KeyId>();
        const capNames = new Set<string>(Object.keys(this.createOp.initialCaps));
        const grantPairs = new Map<string, Set<KeyId>>();

        for (const entry of entries) {
            const p = entry.payload as CapPayload;
            switch (p.action) {
                case 'add-identity':
                    keyIds.add(p.keyId);
                    break;
                case 'create-cap':
                    capNames.add(p.capName);
                    break;
                case 'delete-cap':
                    capNames.add(p.capName);
                    break;
                case 'grant':
                case 'revoke':
                    this.addGrantPair(grantPairs, p.capName, p.grantee);
                    break;
            }
        }

        return { keyIds, capNames, grantPairs };
    }

    private async walkNewEntries(rawDag: dag.Dag, from: Version, stopAt: Position): Promise<dag.Entry[]> {
        const visited = new Set<B64Hash>();
        const queue: B64Hash[] = Array.from(from);
        const walked: dag.Entry[] = [];

        while (queue.length > 0) {
            const hash = queue.shift()!;
            if (visited.has(hash)) continue;
            visited.add(hash);

            if (stopAt.has(hash)) continue;

            const entry = await rawDag.loadEntry(hash);
            if (entry === undefined) continue;
            walked.push(entry);

            for (const prevHash of json.fromSet(entry.header.prevEntryHashes)) {
                if (!visited.has(prevHash)) {
                    queue.push(prevHash);
                }
            }
        }

        return walked;
    }

    private async computeDeltaFromCandidates(
        start: Version,
        end: Version,
        revisionBound: Version,
        keyIds: Set<KeyId>,
        capNames: Set<string>,
        grantPairs: Map<string, Set<KeyId>>,
    ): Promise<RCapDelta> {
        const startView = await this.getView(start, start);
        const endView = await this.getView(end, end);

        const identityChanges: IdentityChange[] = [];
        for (const keyId of keyIds) {
            const wasIdentity = await startView.isIdentity(keyId);
            const nowIdentity = await endView.isIdentity(keyId);
            if (wasIdentity !== nowIdentity) {
                identityChanges.push({ keyId, added: nowIdentity });
            }
        }

        const capabilityChanges: CapabilityChange[] = [];
        for (const capName of capNames) {
            const existed = await startView.capabilityExists(capName);
            const exists = await endView.capabilityExists(capName);
            if (existed !== exists) {
                capabilityChanges.push({ capName, existed, exists });
            }
        }

        const grantChanges: GrantChange[] = [];
        const endCapExists = new Map<string, boolean>();
        for (const [capName, grantKeyIds] of grantPairs) {
            let capExistsInEnd = endCapExists.get(capName);
            if (capExistsInEnd === undefined) {
                capExistsInEnd = await endView.capabilityExists(capName);
                endCapExists.set(capName, capExistsInEnd);
            }
            if (!capExistsInEnd) continue;

            for (const keyId of grantKeyIds) {
                if (this.isCreator(keyId)) continue;
                const wasGranted = await startView.hasCapability(keyId, capName);
                const nowGranted = await endView.hasCapability(keyId, capName);
                if (wasGranted !== nowGranted) {
                    grantChanges.push({ keyId, capName, wasGranted, nowGranted });
                }
            }
        }

        return new RCapDelta(start, end, revisionBound, identityChanges, capabilityChanges, grantChanges);
    }

    private async computeDeltaFull(start: Version, end: Version): Promise<RCapDelta> {
        const rawDag = await this.ctx.getDag(this.createOpId, this.runtimeConfig.backendLabel);
        if (rawDag === undefined) throw new Error("DAG not found");

        const entries: dag.Entry[] = [];
        for await (const entry of rawDag.loadAllEntries()) {
            entries.push(entry);
        }
        const { keyIds, capNames, grantPairs } = this.collectCandidatesFromEntries(entries);

        return this.computeDeltaFromCandidates(start, end, version(), keyIds, capNames, grantPairs);
    }

    private async computeDeltaBounded(start: Version, end: Version): Promise<RCapDelta> {
        const rawDag = await this.ctx.getDag(this.createOpId, this.runtimeConfig.backendLabel);
        if (rawDag === undefined) throw new Error("DAG not found");

        const fork = await rawDag.findForkPosition(start, end);
        if (fork.forkA.size > 0) {
            throw new Error("bounded computeDelta requires END to extend START");
        }
        if (fork.forkB.size === 0) {
            return new RCapDelta(start, end, fork.commonFrontier, [], [], []);
        }

        // Walk back to the meet of the fork points (fork.common). Folding over common
        // directly (not an antichain) is correct: dominated elements never lower the GLB.
        const meet = await dag.computeMeet(
            [...fork.common].map((h) => position(h)),
            (a, b) => rawDag.findForkPosition(a, b).then((f) => f.commonFrontier),
        );

        const walkedEntries = await this.walkNewEntries(rawDag, end, meet);
        const { keyIds, capNames, grantPairs } = this.collectCandidatesFromEntries(walkedEntries);

        return this.computeDeltaFromCandidates(start, end, meet, keyIds, capNames, grantPairs);
    }

    configure(config: RCapRuntimeConfig): void {
        this.runtimeConfig = { ...this.runtimeConfig, ...config };
        this._scopedDag = undefined;
    }

    async getScopedDag(): Promise<ScopedDag> {
        if (this._scopedDag === undefined) {
            const rawDag = await this.ctx.getDag(this.createOpId, this.runtimeConfig.backendLabel);
            if (rawDag === undefined) throw new Error(`DAG '${this.createOpId}' not found`);
            this._scopedDag = new RootScopedDag(rawDag);
        }
        return this._scopedDag;
    }

    // SyncableObject interface

    async startSync(): Promise<void> {
        if (this._syncSession !== undefined) return;

        const mesh = this.ctx.getMesh(this.runtimeConfig.meshLabel ?? 'default') as Mesh;
        this._swarm = mesh.createSwarm(this.createOpId);

        const rawDag = await this.ctx.getDag(this.createOpId, this.runtimeConfig.backendLabel);
        if (rawDag === undefined) throw new Error(`DAG '${this.createOpId}' not found`);

        const target: SyncTarget = {
            dagId: this.createOpId,
            dag: rawDag,
            rObject: this,
            hashSuite: this.ctx.getHashSuite(),
        };

        this._syncSession = createSyncSession(target, [this._swarm]);
        this._swarm.activate();
    }

    async stopSync(): Promise<void> {
        if (this._syncSession !== undefined) {
            this._syncSession.destroy();
            this._syncSession = undefined;
        }
        if (this._swarm !== undefined) {
            this._swarm.destroy();
            this._swarm = undefined;
        }
    }

    async destroy(): Promise<void> {
        await this.stopSync();
        this._scopedDag = undefined;
    }
}

export class RCapViewImpl implements RCapViewContract {

    private target: RCapImpl;
    private at: Version;
    private from: Version;

    constructor(target: RCapImpl, at: Version, from: Version) {
        this.target = target;
        this.at = at;
        this.from = from;
    }

    getObject(): RCapContract { return this.target; }
    getVersion(): Version { return this.at; }
    getFromVersion(): Version { return this.from; }

    async getReferences(): Promise<B64Hash[]> { return []; }
    async resolveRefVersion(_refId: B64Hash): Promise<Version> {
        throw new Error("RCap does not have outgoing references");
    }

    async isIdentity(keyId: KeyId): Promise<boolean> {
        if (this.target.isCreator(keyId)) return true;

        const scopedDag = await this.target.getScopedDag();
        const cover = await scopedDag.findCoverWithFilter(this.at, { containsValues: { ids: [keyId] } });
        return cover.size > 0;
    }

    private async getFirstSurvivingCapOrigin(capName: string): Promise<B64Hash | undefined> {
        const scopedDag = await this.target.getScopedDag();
        const cover = await scopedDag.findCoverWithFilter(this.at, { containsValues: { caps: [capName] } });

        for (const hash of cover) {
            const entry = await scopedDag.loadEntry(hash);
            if (entry === undefined) continue;
            const p = entry.payload as CapPayload;

            const isPositiveCandidate =
                (p.action === 'create-cap' && p.capName === capName) ||
                (p.action === 'create' && capName in (p as CreateRCapPayload).initialCaps);

            if (!isPositiveCandidate) continue;

            const concurrentDeleteBarriers = await scopedDag.findConcurrentCoverWithFilter(
                this.from, version(hash), { containsValues: { caps: [capName], barrier: ['t'] } },
            );

            if (concurrentDeleteBarriers.size === 0) {
                return hash;
            }
        }

        return undefined;
    }

    async capabilityExists(capName: string): Promise<boolean> {
        return (await this.getFirstSurvivingCapOrigin(capName)) !== undefined;
    }

    // Admissibility check: "would an op appended at `this.at` that requires `grantee` to
    // hold `capName` be admissible when observed from `this.from`?" The answer is a pure
    // function of (this.at, this.from, grantee, capName) -- it does not depend on whether
    // the call is top-level or recursive.
    //
    // Collapsed use point (collapse-X model): when `this.at` is a multi-hash frontier it is
    // modeled as a single imaginary node X that inherits the union of predecessors AND
    // successors of its elements. So an external op u is concurrent with X iff it is
    // concurrent with EVERY element of `this.at`; if u is after even one element it is
    // "later on that branch", where use-before-revoke applies.
    //
    // Two see-through barriers express use-before-revoke and concurrent-void:
    //   B1 (grant-anchored): a valid revoke of the pair concurrent with the grant op.
    //   B2 (use-anchored):   a valid revoke of the pair concurrent with the use point X.
    // Both observe from `this.from`. Division of labor:
    //   - B2 is coarse and grant-independent: it fires only for a revoke concurrent with the
    //     WHOLE use point (concurrent with every element of `this.at`). A revoke that is
    //     merely after some element of `this.at` is not concurrent with X, so B2 defers it.
    //   - B1 (with the cover) is grant-specific and handles that deferred case: the grant
    //     survives unless a revoke is concurrent with the authorizing grant op itself.
    // B2 is always-on: a barrier visible from `this.from` and concurrent with X would void an
    // op appended at `this.at`, so the query must return false. It is vacuous when
    // `from == at` (append/delta) since nothing is concurrent with the whole horizon, and
    // concurrent-only (a sequential revoke never fires it), so it never breaks
    // use-before-revoke.
    async hasCapability(grantee: KeyId, capName: string, visiting?: Set<string>): Promise<boolean> {
        if (this.target.isCreator(grantee)) return true;

        const visitKey = grantee + '\0' + capName;
        if (visiting !== undefined && visiting.has(visitKey)) return false;
        visiting = new Set(visiting);
        visiting.add(visitKey);

        if (!await this.capabilityExists(capName)) return false;

        const scopedDag = await this.target.getScopedDag();
        const grantKey = capName + ':' + grantee;
        const managedBy = await this.getManagedBy(capName);

        // See-through validity predicate. An op (grant or revoke) of this pair is valid
        // only if its author was authorized AS OF the op's own version -- evaluated on a
        // view pinned at the using op (version(hash)) so a later revoke of the author's
        // managing cap does not retroactively invalidate it (use-before-revoke). Hosting
        // the recursion here lets the cover/barrier walks "see through" an invalid op to
        // the last valid one beneath it, instead of being masked by a dominating invalid op.
        const valid: EntryPredicate = async (hash, entry) => {
            const p = entry.payload as CapPayload;
            if (p.action === 'grant'
                && !await this.hasAnySurvivingOriginIn(capName, new Set((p as GrantPayload).capOrigins))) {
                return false;
            }
            const author = (p as GrantPayload | RevokePayload).author as KeyId;
            if (this.target.isCreator(author)) return true;
            const useView = await this.target.getView(version(hash), this.from);
            for (const mgr of managedBy) {
                if (mgr === 'creator') continue;
                if (await useView.hasCapability(author, mgr, visiting)) return true;
            }
            return false;
        };

        // B2 (use-anchored): a valid revoke of this pair concurrent with the collapsed use
        // point X -- i.e. concurrent with EVERY element of this.at (findConcurrentCoverWithFilter
        // excludes any op that is after, or before, any element). A revoke that is after only
        // some elements of this.at is left to the grant-anchored B1 below. Observed from
        // this.from; vacuous when from == at.
        const useRevokes = await scopedDag.findConcurrentCoverWithFilter(
            this.from, this.at, { containsValues: { grants: [grantKey], barrier: ['t'] } }, valid,
        );
        if (useRevokes.size > 0) return false;

        // See-through cover: the last VALID grant/revoke of this pair in past(at).
        const cover = await scopedDag.findCoverWithFilter(
            this.at, { containsValues: { grants: [grantKey] } }, valid,
        );

        for (const hash of cover) {
            const entry = await scopedDag.loadEntry(hash);
            if (entry === undefined) continue;
            const p = entry.payload as CapPayload;
            if (p.action !== 'grant') continue;

            // B1 (grant-anchored): a valid revoke of this pair concurrent with this grant op.
            const concurrentRevokes = await scopedDag.findConcurrentCoverWithFilter(
                this.from, version(hash), { containsValues: { grants: [grantKey], barrier: ['t'] } }, valid,
            );
            if (concurrentRevokes.size > 0) continue;

            return true;
        }

        return false;
    }

    async getManagedBy(capName: string): Promise<string[]> {
        if (capName in this.target.createOp.initialCaps) {
            return this.target.createOp.initialCaps[capName].managedBy;
        }

        const scopedDag = await this.target.getScopedDag();
        const cover = await scopedDag.findCoverWithFilter(this.at, { containsValues: { caps: [capName] } });

        for (const hash of cover) {
            const entry = await scopedDag.loadEntry(hash);
            if (entry === undefined) continue;
            const p = entry.payload as CapPayload;
            if (p.action === 'create-cap') {
                return p.managedBy;
            }
        }

        return [];
    }

    async currentCapCreationVersion(capName: string): Promise<Version> {
        const scopedDag = await this.target.getScopedDag();
        const cover = await scopedDag.findCoverWithFilter(this.at, { containsValues: { caps: [capName] } });
        const surviving = new Set<B64Hash>();

        for (const hash of cover) {
            const entry = await scopedDag.loadEntry(hash);
            if (entry === undefined) continue;
            const p = entry.payload as CapPayload;

            const isPositiveCandidate =
                (p.action === 'create-cap' && p.capName === capName) ||
                (p.action === 'create' && capName in (p as CreateRCapPayload).initialCaps);

            if (!isPositiveCandidate) continue;

            const concurrentDeleteBarriers = await scopedDag.findConcurrentCoverWithFilter(
                this.from, version(hash), { containsValues: { caps: [capName], barrier: ['t'] } },
            );

            if (concurrentDeleteBarriers.size === 0) {
                surviving.add(hash);
            }
        }

        return surviving;
    }

    private async hasAnySurvivingOriginIn(capName: string, origins: Set<string>): Promise<boolean> {
        const scopedDag = await this.target.getScopedDag();
        const cover = await scopedDag.findCoverWithFilter(this.at, { containsValues: { caps: [capName] } });

        for (const hash of cover) {
            if (!origins.has(hash)) continue;

            const entry = await scopedDag.loadEntry(hash);
            if (entry === undefined) continue;
            const p = entry.payload as CapPayload;

            const isPositiveCandidate =
                (p.action === 'create-cap' && p.capName === capName) ||
                (p.action === 'create' && capName in (p as CreateRCapPayload).initialCaps);

            if (!isPositiveCandidate) continue;

            const concurrentDeleteBarriers = await scopedDag.findConcurrentCoverWithFilter(
                this.from, version(hash), { containsValues: { caps: [capName], barrier: ['t'] } },
            );

            if (concurrentDeleteBarriers.size === 0) {
                return true;
            }
        }

        return false;
    }

    async getCapabilities(): Promise<string[]> {
        const caps: string[] = [];
        const all = new Set(Object.keys(this.target.createOp.initialCaps));

        const scopedDag = await this.target.getScopedDag();
        const cover = await scopedDag.findCoverWithFilter(this.at, { containsKeys: ['caps'] });
        for (const hash of cover) {
            if (hash === this.target.createOpId) continue;
            const entry = await scopedDag.loadEntry(hash);
            if (entry === undefined) continue;
            const p = entry.payload as CapPayload;
            if (p.action === 'create-cap') all.add(p.capName);
        }

        for (const name of all) {
            if (await this.capabilityExists(name)) {
                caps.push(name);
            }
        }

        return caps;
    }
}

export type RCapDeltaStrategy = 'full' | 'bounded';

export type IdentityChange = {
    keyId: KeyId;
    added: boolean;
};

export type CapabilityChange = {
    capName: string;
    existed: boolean;
    exists: boolean;
};

export type GrantChange = {
    keyId: KeyId;
    capName: string;
    wasGranted: boolean;
    nowGranted: boolean;
};

export class RCapDelta implements Delta {
    constructor(
        private start: Version,
        private end: Version,
        private revisionBound: Version,
        public readonly identityChanges: IdentityChange[],
        public readonly capabilityChanges: CapabilityChange[],
        public readonly grantChanges: GrantChange[],
    ) {}

    getStartVersion(): Version { return this.start; }
    getEndVersion(): Version { return this.end; }
    getRevisionBound(): Version { return this.revisionBound; }
}

export interface RCap extends RCapContract {}
export interface RCapView extends RCapViewContract {}
export const RCap = RCapImpl;
export const RCapView = RCapViewImpl;
