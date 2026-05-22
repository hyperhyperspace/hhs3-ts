import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, HASH_SHA256, KeyId, PublicKey } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { dag, MetaProps, position, Position } from "@hyper-hyper-space/hhs3_dag";

import { Payload, RObject, RObjectFactory, RObjectInit, RContext, SyncableObject, version, Version, View, Delta, ForeignDep } from "@hyper-hyper-space/hhs3_mvt";
import { RootScopedDag, ScopedDag, CausalDag } from "@hyper-hyper-space/hhs3_mvt";

import type { Mesh, Swarm } from "@hyper-hyper-space/hhs3_mesh";
import { createSyncSession } from "@hyper-hyper-space/hhs3_sync";
import type { SyncSession, SyncTarget } from "@hyper-hyper-space/hhs3_sync";

import { verifyPayloadSignature, deserializePublicKeyFromBase64, computeKeyId, signPayload as signPayloadHelper } from "../authorship.js";
import type { KeyLookup } from "../authorship.js";

import {
    createRCapFormat, CreateRCapPayload, CapDefinition,
    addIdentityFormat, AddIdentityPayload,
    createCapabilityFormat, CreateCapabilityPayload,
    deleteCapabilityFormat, DeleteCapabilityPayload,
    grantFormat, GrantPayload,
    revokeFormat, RevokePayload,
    CapPayload,
} from "./rcap/payload.js";

import type { RCapEvent } from "./rcap/events.js";

const DEFAULT_ENROLL_CAPABILITY = 'enroll';

export const rCapFactory: RObjectFactory = {

    computeRootObjectId: async (payload: json.Literal, ctx: RContext) => {
        const entry = dag.createEntry(payload, {}, position(), ctx.getCrypto().hash(HASH_SHA256));
        return entry.hash;
    },

    validateCreationPayload: async (payload: json.Literal, _ctx: RContext) => {
        if (!json.checkFormat(createRCapFormat, payload)) {
            return false;
        }

        const cp = payload as CreateRCapPayload;

        if (cp.creators.length === 0) return false;
        if (cp.creators.length !== cp.creatorKeys.length) return false;

        const hashSuite = _ctx.getHashSuite();
        for (let i = 0; i < cp.creators.length; i++) {
            try {
                const pk = deserializePublicKeyFromBase64(cp.creatorKeys[i]);
                if (computeKeyId(pk, hashSuite) !== cp.creators[i]) return false;
            } catch {
                return false;
            }
        }

        const capNames = new Set(Object.keys(cp.initialCaps));
        for (const def of Object.values(cp.initialCaps)) {
            for (const mgr of def.managedBy) {
                if (mgr !== 'creator' && !capNames.has(mgr)) return false;
            }
        }

        if (cp.enrollCapability !== undefined) {
            if (!capNames.has(cp.enrollCapability)) return false;
        }

        return true;
    },

    executeCreationPayload: async (payload: json.Literal, _ctx: RContext, scopedDag: ScopedDag) => {
        return await scopedDag.append(payload, {}, position());
    },

    loadObject: async (id: B64Hash, ctx: RContext) => {
        const rawDag = await ctx.getDag(id);
        if (rawDag === undefined) throw new Error(`DAG '${id}' not found`);
        const scopedDag = new RootScopedDag(rawDag);
        const createOp = (await scopedDag.loadEntry(id))!.payload as CreateRCapPayload;
        return new RCap(id, createOp, ctx);
    }
};

export type RCapRuntimeConfig = {
    meshLabel?: string;
    backendLabel?: string;
};

export class RCap implements RObject, SyncableObject {

    static create = async (options: {
        seed: string;
        creators: { keyId: KeyId; publicKey: PublicKey }[];
        initialCaps: { [capName: string]: CapDefinition };
        enrollCapability?: string;
        hashAlgorithm?: string;
    }): Promise<RObjectInit> => {
        const { serializePublicKeyToBase64 } = await import("../authorship.js");

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

        return { type: RCap.typeId, payload: createPayload };
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
    getType(): string { return RCap.typeId; }

    enrollCapability(): string {
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

    private keyLookup: KeyLookup = (keyId: KeyId) => this.lookupKey(keyId);

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
        grantee: KeyId, capName: string, capOrigin: B64Hash,
        author: OwnIdentity,
        at?: Version,
    ): Promise<B64Hash> {
        const payload: Omit<GrantPayload, 'author' | 'signature'> = {
            action: 'grant', grantee, capName, capOrigin,
        };
        const signed = await signPayloadHelper(payload as json.LiteralMap, author);
        const dag = await this.getScopedDag();
        at = at || await dag.getFrontier();
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
        if (typeof payload !== 'object' || Array.isArray(payload)) return false;
        if (typeof payload['action'] !== 'string') return false;

        const action = payload['action'];

        switch (action) {
            case 'add-identity': return this.validateAddIdentity(payload, at);
            case 'create-cap':   return this.validateCreateCap(payload, at);
            case 'delete-cap':   return this.validateDeleteCap(payload, at);
            case 'grant':        return this.validateGrant(payload, at);
            case 'revoke':       return this.validateRevoke(payload, at);
            default: return false;
        }
    }

    private async validateAddIdentity(payload: Payload, at: Version): Promise<boolean> {
        if (!json.checkFormat(addIdentityFormat, payload)) return false;
        const p = payload as AddIdentityPayload;

        try {
            const pk = deserializePublicKeyFromBase64(p.publicKey);
            const hashSuite = this.ctx.getHashSuite();
            if (computeKeyId(pk, hashSuite) !== p.keyId) return false;
        } catch {
            return false;
        }

        if (!await verifyPayloadSignature(payload as json.LiteralMap, this.keyLookup)) return false;

        const authorId = p.author as KeyId;
        if (!this.isCreator(authorId)) {
            const view = await this.getView(at, at);
            if (!await view.hasCapability(authorId, this.enrollCapability())) return false;
        }

        return true;
    }

    private async validateCreateCap(payload: Payload, at: Version): Promise<boolean> {
        if (!json.checkFormat(createCapabilityFormat, payload)) return false;
        const p = payload as CreateCapabilityPayload;

        if (!await verifyPayloadSignature(payload as json.LiteralMap, this.keyLookup)) return false;

        if (!this.isCreator(p.author as KeyId)) return false;

        const view = await this.getView(at, at);
        if (await view.capabilityExists(p.capName)) return false;

        for (const mgr of p.managedBy) {
            if (mgr !== 'creator' && !await view.capabilityExists(mgr)) return false;
        }

        return true;
    }

    private async validateDeleteCap(payload: Payload, at: Version): Promise<boolean> {
        if (!json.checkFormat(deleteCapabilityFormat, payload)) return false;
        const p = payload as DeleteCapabilityPayload;

        if (!await verifyPayloadSignature(payload as json.LiteralMap, this.keyLookup)) return false;

        if (!this.isCreator(p.author as KeyId)) return false;

        const view = await this.getView(at, at);
        if (!await view.capabilityExists(p.capName)) return false;

        return true;
    }

    private async validateGrant(payload: Payload, at: Version): Promise<boolean> {
        if (!json.checkFormat(grantFormat, payload)) return false;
        const p = payload as GrantPayload;

        if (!await verifyPayloadSignature(payload as json.LiteralMap, this.keyLookup)) return false;

        const authorId = p.author as KeyId;
        const view = await this.getView(at, at);

        if (!await view.capabilityExists(p.capName)) return false;

        if (!await view.isIdentity(p.grantee)) return false;

        if (!this.isCreator(authorId)) {
            const managedBy = await view.getManagedBy(p.capName);
            let authorized = false;
            for (const mgr of managedBy) {
                if (mgr === 'creator') continue;
                if (await view.hasCapability(authorId, mgr)) {
                    authorized = true;
                    break;
                }
            }
            if (!authorized) return false;
        }

        const activeOrigin = await view.getActiveCapOrigin(p.capName);
        if (activeOrigin !== p.capOrigin) return false;

        return true;
    }

    private async validateRevoke(payload: Payload, at: Version): Promise<boolean> {
        if (!json.checkFormat(revokeFormat, payload)) return false;
        const p = payload as RevokePayload;

        if (!await verifyPayloadSignature(payload as json.LiteralMap, this.keyLookup)) return false;

        const authorId = p.author as KeyId;
        const view = await this.getView(at, at);

        if (!await view.capabilityExists(p.capName)) return false;

        if (!this.isCreator(authorId)) {
            const managedBy = await view.getManagedBy(p.capName);
            let authorized = false;
            for (const mgr of managedBy) {
                if (mgr === 'creator') continue;
                if (await view.hasCapability(authorId, mgr)) {
                    authorized = true;
                    break;
                }
            }
            if (!authorized) return false;
        }

        return true;
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

    async getView(at?: Version, from?: Version): Promise<RCapView> {
        const scopedDag = await this.getScopedDag();
        at = at || await scopedDag.getFrontier();
        from = from || await scopedDag.getFrontier();
        return new RCapView(this, at, from);
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
        const rawDag = await this.ctx.getDag(this.createOpId, this.runtimeConfig.backendLabel);
        if (rawDag === undefined) throw new Error("DAG not found");

        const keyIds = new Set<KeyId>();
        const capNames = new Set<string>(Object.keys(this.createOp.initialCaps));
        const grantPairs = new Map<string, Set<KeyId>>();

        for await (const entry of rawDag.loadAllEntries()) {
            const p = entry.payload as CapPayload;
            switch (p.action) {
                case 'add-identity':
                    keyIds.add(p.keyId);
                    break;
                case 'create-cap':
                    capNames.add(p.capName);
                    break;
                case 'grant':
                case 'revoke':
                    if (!grantPairs.has(p.capName)) grantPairs.set(p.capName, new Set());
                    grantPairs.get(p.capName)!.add(p.grantee);
                    break;
            }
        }

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
        for (const [capName, grantKeyIds] of grantPairs) {
            for (const keyId of grantKeyIds) {
                if (this.isCreator(keyId)) continue;
                const wasGranted = await startView.hasCapability(keyId, capName);
                const nowGranted = await endView.hasCapability(keyId, capName);
                if (wasGranted !== nowGranted) {
                    grantChanges.push({ keyId, capName, wasGranted, nowGranted });
                }
            }
        }

        return new RCapDelta(start, end, version(), identityChanges, capabilityChanges, grantChanges);
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

export class RCapView implements View {

    private target: RCap;
    private at: Version;
    private from: Version;

    constructor(target: RCap, at: Version, from: Version) {
        this.target = target;
        this.at = at;
        this.from = from;
    }

    getObject(): RCap { return this.target; }
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

    async capabilityExists(capName: string): Promise<boolean> {
        if (capName in this.target.createOp.initialCaps) {
            const scopedDag = await this.target.getScopedDag();
            const deleteBarriers = await scopedDag.findConcurrentCoverWithFilter(
                this.from, this.at, { containsValues: { caps: [capName], barrier: ['t'] } },
            );
            if (deleteBarriers.size > 0) return false;

            const cover = await scopedDag.findCoverWithFilter(this.at, { containsValues: { caps: [capName] } });
            for (const hash of cover) {
                if (hash === this.target.createOpId) continue;
                const entry = await scopedDag.loadEntry(hash);
                if (entry === undefined) continue;
                const p = entry.payload as CapPayload;
                if (p.action === 'delete-cap') return false;
            }

            return true;
        }

        const scopedDag = await this.target.getScopedDag();
        const cover = await scopedDag.findCoverWithFilter(this.at, { containsValues: { caps: [capName] } });
        if (cover.size === 0) return false;

        for (const hash of cover) {
            const entry = await scopedDag.loadEntry(hash);
            if (entry === undefined) continue;
            const p = entry.payload as CapPayload;
            if (p.action === 'delete-cap') return false;
        }

        const deleteBarriers = await scopedDag.findConcurrentCoverWithFilter(
            this.from, this.at, { containsValues: { caps: [capName], barrier: ['t'] } },
        );
        if (deleteBarriers.size > 0) return false;

        return true;
    }

    async hasCapability(grantee: KeyId, capName: string, visiting?: Set<string>): Promise<boolean> {
        if (this.target.isCreator(grantee)) return true;

        // Cycle detection: a query like "does A hold X?" may recurse into
        // "does B hold Y?" if X is managedBy Y, and if Y is managedBy X
        // we'd loop. Track (grantee, capName) pairs being resolved.
        const visitKey = grantee + '\0' + capName;
        if (visiting !== undefined && visiting.has(visitKey)) return false;
        visiting = new Set(visiting);
        visiting.add(visitKey);

        if (!await this.capabilityExists(capName)) return false;

        const scopedDag = await this.target.getScopedDag();
        const grantKey = capName + ':' + grantee;

        const revokeBarriers = await scopedDag.findConcurrentCoverWithFilter(
            this.from, this.at, { containsValues: { grants: [grantKey], barrier: ['t'] } },
        );
        if (revokeBarriers.size > 0) return false;

        const cover = await scopedDag.findCoverWithFilter(this.at, { containsValues: { grants: [grantKey] } });
        if (cover.size === 0) return false;

        for (const hash of cover) {
            const entry = await scopedDag.loadEntry(hash);
            if (entry === undefined) continue;
            const p = entry.payload as CapPayload;
            if (p.action === 'revoke') return false;
        }

        const activeOrigin = await this.getActiveCapOrigin(capName);
        if (activeOrigin === undefined) return false;

        const managedBy = await this.getManagedBy(capName);

        for (const hash of cover) {
            const entry = await scopedDag.loadEntry(hash);
            if (entry === undefined) continue;
            const p = entry.payload as CapPayload;
            if (p.action !== 'grant' || p.capOrigin !== activeOrigin) continue;

            const author = (p as GrantPayload).author as KeyId;

            if (this.target.isCreator(author)) return true;

            for (const mgr of managedBy) {
                if (mgr === 'creator') continue;
                if (await this.hasCapability(author, mgr, visiting)) return true;
            }
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

    async getActiveCapOrigin(capName: string): Promise<B64Hash | undefined> {
        if (capName in this.target.createOp.initialCaps) {
            const scopedDag = await this.target.getScopedDag();
            const cover = await scopedDag.findCoverWithFilter(this.at, { containsValues: { caps: [capName] } });

            for (const hash of cover) {
                if (hash === this.target.createOpId) continue;
                const entry = await scopedDag.loadEntry(hash);
                if (entry === undefined) continue;
                const p = entry.payload as CapPayload;
                if (p.action === 'delete-cap') return undefined;
                if (p.action === 'create-cap') return hash;
            }

            return this.target.createOpId;
        }

        const scopedDag = await this.target.getScopedDag();
        const cover = await scopedDag.findCoverWithFilter(this.at, { containsValues: { caps: [capName] } });

        for (const hash of cover) {
            const entry = await scopedDag.loadEntry(hash);
            if (entry === undefined) continue;
            const p = entry.payload as CapPayload;
            if (p.action === 'delete-cap') return undefined;
            if (p.action === 'create-cap') return hash;
        }

        return undefined;
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
