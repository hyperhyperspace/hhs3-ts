import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, HASH_SHA256, KeyId, PublicKey } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { dag, MetaProps, position } from "@hyper-hyper-space/hhs3_dag";

import { Payload, RObject, RObjectFactory, RObjectInit, RContext, LoadObjectOptions, Version, View, ForeignDep } from "@hyper-hyper-space/hhs3_mvt";
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
import { RCapViewImpl } from "./view.js";
import { RCapDelta, RCapDeltaStrategy, RCapDeltaAccumulator, computeRCapDelta } from "./delta.js";

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

    loadObject: async (id: B64Hash, ctx: RContext, opts?: LoadObjectOptions) => {
        const backendLabel = opts?.backendLabel ?? 'default';
        const rawDag = await ctx.getDag(id, backendLabel);
        if (rawDag === undefined) throw new Error(`DAG '${id}' not found`);
        const scopedDag = new RootScopedDag(rawDag);
        const createOp = (await scopedDag.loadEntry(id))!.payload as CreateRCapPayload;
        return new RCapImpl(id, createOp, ctx, backendLabel);
    }
};

export type RCapRuntimeConfig = {
    meshLabel?: string;
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
    private _causalDag: CausalDag | undefined;
    private _swarm: Swarm | undefined;
    private _syncSession: SyncSession | undefined;
    private readonly backendLabel: string;
    private meshConfig: RCapRuntimeConfig = { meshLabel: 'default' };
    private deltaStrategy: RCapDeltaStrategy = 'bounded';

    constructor(createOpId: B64Hash, createOp: CreateRCapPayload, ctx: RContext, backendLabel: string = 'default') {
        this.createOpId = createOpId;
        this.createOp = createOp;
        this.ctx = ctx;
        this.backendLabel = backendLabel;
    }

    getBackendLabel(): string {
        return this.backendLabel;
    }

    getId(): string { return this.createOpId; }
    getType(): string { return RCapImpl.typeId; }

    getInitialCaps() { return this.createOp.initialCaps; }

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
        const rawDag = await this.ctx.getDag(this.createOpId, this.backendLabel);
        if (rawDag === undefined) throw new Error("DAG not found");
        return computeRCapDelta(this, rawDag, this.deltaStrategy, start, end);
    }

    createDeltaAccumulator(start: Version, end: Version): RCapDeltaAccumulator {
        return new RCapDeltaAccumulator(this, start, end);
    }

    configure(config: RCapRuntimeConfig): void {
        this.meshConfig = { ...this.meshConfig, ...config };
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

    // SyncableObject interface

    async startSync(): Promise<void> {
        if (this._syncSession !== undefined) return;

        const mesh = this.ctx.getMesh(this.meshConfig.meshLabel ?? 'default') as Mesh;
        this._swarm = mesh.createSwarm(this.createOpId);

        const rawDag = await this.ctx.getDag(this.createOpId, this.backendLabel);
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

export { RCapViewImpl } from "./view.js";
export * from "./delta.js";

export interface RCap extends RCapContract {}
export interface RCapView extends RCapViewContract {}
export const RCap = RCapImpl;
export const RCapView = RCapViewImpl;
