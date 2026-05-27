

// A Replicable Set that implements Monotonic Views.

// This class implements the RObject interface, and can be used as a top-level object in a Replica, or as
// a nested element inside other DAG-based RObjects.

// The set supports two modes of operation: if contentType is missing, the set is a simple set of literals.
// If contentType is present, the set works as a container for nested DAG-based RObjects. In this case, each
// time an element is added, its contents are used as the payload for the creation operation of the nested RObject's
// type. A new instance of the nested RObject is created with each Add, and its nested DAG is rooted at the position
// in the DAG where the Add operation is inserted. The returned hash for the element is the hash of the Add 
// operation, that has the creation op's payload as its content. Therefore there is no aliasing of contents: if the
// same creation payload is passed to two Add operations rooted in different DAG positions, two different nested
// elements will be created (with identical initialization params, since they have the same creation payload).
// Since the position affects the hash of the Add entry in the DAG, their hashes will be different.

// This type uses a sub-dag abstraction. When an element is added in nested mode, a sub-dag is created that will
// automatically wrap any updates to the nested type into RSet update operations, and will also automatically wrap
// any metadata and filtering parameters. When an element is read, the payload is automatically unwrapped.

// Notice that this wrapping process is not 100% transparent: some operations (like fork poistion finding) will
// return DAG entries from the outer, full DAG. So types that support being inserted as nested elements must be
// aware of this caveats.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, HASH_SHA256, sha256, stringToUint8Array } from "@hyper-hyper-space/hhs3_crypto";
import type { KeyId, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { dag, MetaProps, position, EntryMetaFilter, EntryPredicate, Position, MetaContainsValues } from "@hyper-hyper-space/hhs3_dag";

import { Payload, RObject, RObjectFactory, RObjectInit, RContext, NestingParent, version, Version, Delta, ForeignDep } from "@hyper-hyper-space/hhs3_mvt";
import { DagScope, NestedScopedDag, RootScopedDag, ScopedDag, CausalDag } from "@hyper-hyper-space/hhs3_mvt";
import { isRefAdvancePayload, refAdvanceMeta, createRefAdvancePayload, resolveRefVersionAtPosition } from "@hyper-hyper-space/hhs3_mvt";
import type { RefAdvancePayload } from "@hyper-hyper-space/hhs3_mvt";
import { set } from "@hyper-hyper-space/hhs3_util";

import type { Mesh, Swarm } from "@hyper-hyper-space/hhs3_mesh";
import { createSyncSession } from "@hyper-hyper-space/hhs3_sync";
import type { SyncSession, SyncTarget } from "@hyper-hyper-space/hhs3_sync";

import { signPayload as signPayloadHelper, isAuthoredPayload, extractAuthor } from "../../authorship.js";
import { RCap } from "../rcap/rcap.js";
import type { RCapView } from "../rcap/rcap.js";
import type { RSet as RSetContract, RSetView as RSetViewContract } from "./interfaces.js";
import { validateRSetPayload } from "./validate.js";

import { RAddEvent, RDeleteEvent, RSetEvent } from "./events.js";

import { CreateSetPayload } from "./payload.js";
import { AddElmtPayload } from "./payload.js";
import { DeleteElmtPayload } from "./payload.js";
import { UpdateElmtPayload } from "./payload.js";

import { SetPayload } from "./payload.js";

export const rSetFactory: RObjectFactory = {

    computeRootObjectId: async (payload: json.Literal, ctx: RContext, _parent?: NestingParent) => {

        const entry = dag.createEntry(payload, {}, position(), ctx.getCrypto().hash(HASH_SHA256));
        return entry.hash;
    },

    validateCreationPayload: async (payload: json.Literal, _ctx: RContext, parent?: NestingParent) =>
        validateRSetPayload(payload, { mode: 'create', parent }),

    executeCreationPayload: async (payload: json.Literal, _ctx: RContext, scopedDag: ScopedDag) => {

        const meta: MetaProps = {};
        const createPayload = payload as CreateSetPayload;

        if (createPayload['contentType'] === undefined) {
            meta['elmts'] = json.toSet(await Promise.all(createPayload['initialElements'].map(async (e: json.Literal) => hashElement(e))));
        }
        
        return await scopedDag.append(createPayload, meta, position());
    },

    loadObject: async (id: B64Hash, ctx: RContext, parent?: NestingParent) => {

        let scopedDag: ScopedDag;

        if (parent !== undefined) {
            scopedDag = await parent.getScopedDagForChild(id);
        } else {
            const rawDag = await ctx.getDag(id);
            if (rawDag === undefined) throw new Error(`DAG '${id}' not found`);
            scopedDag = new RootScopedDag(rawDag);
        }

        const createOp = (await scopedDag.loadEntry(id))!.payload as CreateSetPayload;
        return new RSetImpl(id, createOp, ctx, parent);
    }

}

export type RSetOptions = {
    seed: string;
    contentType?: string;
    initialElements: Array<json.Literal>;
    acceptRedundantAdd?: boolean;
    acceptRedundantDelete?: boolean;
    acceptRedundantUpdate?: boolean;
    supportBarrierAdd?: boolean;
    supportBarrierDelete?: boolean;
    hashAlgorithm?: string;
    parent?: B64Hash;
    capabilityRef?: B64Hash;
    capRequirements?: { add?: string; delete?: string; refAdvance?: string[]; refAdvanceCreators?: boolean };
}

export type RSetRuntimeConfig = {
    meshLabel?: string;
    backendLabel?: string;
}

export class RSetImpl<T extends json.Literal = json.Literal> implements RSetContract<T> {

    static create = async (options: RSetOptions) => {

        if (options.initialElements != undefined &&
            options.initialElements.length > 0 &&
            options.contentType !== undefined) {

            throw new Error("Sets of RObjects cannot have initial elements");
        }

        if (options.contentType !== undefined &&
            options.acceptRedundantAdd !== undefined) {

            throw new Error("acceptRedundantAdd cannot be set if contentType is present");
            
        }

        const createPayload: CreateSetPayload = {
            action: 'create',
            seed: options.seed,
            initialElements: options.initialElements || [],
            hashAlgorithm: options.hashAlgorithm || 'sha256',
            acceptRedundantDelete: options.acceptRedundantDelete || false,
        };


        if (options.acceptRedundantAdd !== undefined) {
            createPayload.acceptRedundantAdd = options.acceptRedundantAdd;
        }

        if (options.supportBarrierAdd && options.supportBarrierDelete) {
            throw new Error("supportBarrierAdd and supportBarrierDelete are mutually exclusive");
        }

        if (options.supportBarrierAdd !== undefined) {
            createPayload.supportBarrierAdd = options.supportBarrierAdd;
        }

        if (options.supportBarrierDelete !== undefined) {
            createPayload.supportBarrierDelete = options.supportBarrierDelete;
        }

        if (options.contentType !== undefined) {
            createPayload['contentType'] = options.contentType;
            createPayload['acceptUpdateForDeleted'] = options.acceptRedundantUpdate || false;
        }

        if (options.parent !== undefined) {
            createPayload['parent'] = options.parent;
        }

        if (options.capabilityRef !== undefined) {
            createPayload['capabilityRef'] = options.capabilityRef;
            createPayload['capRequirements'] = options.capRequirements;
        }

        return {type: RSetImpl.typeId, payload: createPayload} as RObjectInit;
    }

    static typeId = "hhs/set_v1";

    createOpId: B64Hash;
    createOp: CreateSetPayload;
    private ctx: RContext;
    private parentObj: NestingParent | undefined;

    private _scopedDag: ScopedDag | undefined;
    private _causalDag: CausalDag | undefined;
    private _swarm: Swarm | undefined;
    private _syncSession: SyncSession | undefined;
    private runtimeConfig: RSetRuntimeConfig = { meshLabel: 'default', backendLabel: 'default' };

    constructor(createOpId: B64Hash, createOp: CreateSetPayload, ctx: RContext, parent?: NestingParent) {
        this.createOpId = createOpId;
        this.createOp = createOp;
        this.ctx = ctx;
        this.parentObj = parent;
    }

    getId(): string {
        return this.createOpId;
    }

    getType(): string {
        return RSetImpl.typeId;
    }

    // Set operations

    async add(element: T, at?: Version): Promise<B64Hash> {
        if (this.isPermissioned()) throw new Error("Use addSigned() for permissioned sets");
        const dag = await this.getScopedDag();

        at = at || await dag.getFrontier();
        const payload = await this.createAddPayload(element, false, at);

        return this.applyValidatedPayload(payload, at);
    }
    
    async addWithBarrier(element: T, at?: Version): Promise<B64Hash> {
        if (this.isPermissioned()) throw new Error("Use addWithBarrierSigned() for permissioned sets");
        const dag = await this.getScopedDag();

        at = at || await dag.getFrontier();
        const payload = await this.createAddPayload(element, true, at);
        return this.applyValidatedPayload(payload, at);
    }

    private async createAddPayload(element: T, barrier: boolean, at: Version): Promise<AddElmtPayload> {

        if (this.contentType() === undefined) {
            if (!this.acceptRedundantAdd()) {
                const view = await this.getView(at);
                const hasElmt = await view.has(element);
                if (hasElmt) {
                    throw new Error("Element already exists in set, and redundant adds are not accepted");
                }            
            }
        }

        if (!this.supportBarrierAdd() && barrier) {
            throw new Error("Barrier add is not supported by this set");
        }

        if (this.supportBarrierAdd()) {
            return {action: 'add', element, barrier};
        } else {
            return {action: 'add', element};
        }        
    }

    async delete(element: T, at?: Version): Promise<B64Hash> {
        if (this.isPermissioned()) throw new Error("Use deleteSigned() for permissioned sets");

        if (this.contentType() !== undefined) {
            throw new Error("RSet.delete(element) is not well defined when contentType is present, please use deleteByHash");
        }

        const elementHash = await hashElement(element);
        return this.deleteByHash(elementHash, at);
    }

    async deleteByHash(elementHash: B64Hash, at?: Version): Promise<B64Hash> {
        if (this.isPermissioned()) throw new Error("Use deleteByHashSigned() for permissioned sets");
        const dag = await this.getScopedDag();

        at = at || await dag.getFrontier();
        const payload: DeleteElmtPayload = await this.createDeletePayload(elementHash, false, at);
        return this.applyValidatedPayload(payload, at);
        
    }

    async deleteWithBarrier(element: T, at?: Version): Promise<B64Hash> {
        if (this.isPermissioned()) throw new Error("Use deleteWithBarrierSigned() for permissioned sets");
        const elementHash = await hashElement(element);
        return this.deleteWithBarrierByHash(elementHash, at);
    }

    async deleteWithBarrierByHash(elementHash: B64Hash, at?: Version): Promise<B64Hash> {
        if (this.isPermissioned()) throw new Error("Use deleteWithBarrierByHashSigned() for permissioned sets");
        const dag = await this.getScopedDag();

        at = at || await dag.getFrontier();
        const payload: DeleteElmtPayload = await this.createDeletePayload(elementHash, true, at);
        return this.applyValidatedPayload(payload, at);
    }

    // Signed convenience methods for permissioned sets

    async addSigned(element: T, author: OwnIdentity, at?: Version): Promise<B64Hash> {
        const d = await this.getScopedDag();
        at = at || await d.getFrontier();
        const base = await this.createAddPayload(element, false, at);
        const signed = await signPayloadHelper(base as json.LiteralMap, author);
        return this.applyValidatedPayload(signed, at);
    }

    async addWithBarrierSigned(element: T, author: OwnIdentity, at?: Version): Promise<B64Hash> {
        const d = await this.getScopedDag();
        at = at || await d.getFrontier();
        const base = await this.createAddPayload(element, true, at);
        const signed = await signPayloadHelper(base as json.LiteralMap, author);
        return this.applyValidatedPayload(signed, at);
    }

    async deleteSigned(element: T, author: OwnIdentity, at?: Version): Promise<B64Hash> {
        if (this.contentType() !== undefined) {
            throw new Error("Use deleteByHashSigned for sets with contentType");
        }
        const elementHash = await hashElement(element);
        return this.deleteByHashSigned(elementHash, author, at);
    }

    async deleteByHashSigned(elementHash: B64Hash, author: OwnIdentity, at?: Version): Promise<B64Hash> {
        const d = await this.getScopedDag();
        at = at || await d.getFrontier();
        const base = await this.createDeletePayload(elementHash, false, at);
        const signed = await signPayloadHelper(base as json.LiteralMap, author);
        return this.applyValidatedPayload(signed, at);
    }

    async deleteWithBarrierSigned(element: T, author: OwnIdentity, at?: Version): Promise<B64Hash> {
        const elementHash = await hashElement(element);
        return this.deleteWithBarrierByHashSigned(elementHash, author, at);
    }

    async deleteWithBarrierByHashSigned(elementHash: B64Hash, author: OwnIdentity, at?: Version): Promise<B64Hash> {
        const d = await this.getScopedDag();
        at = at || await d.getFrontier();
        const base = await this.createDeletePayload(elementHash, true, at);
        const signed = await signPayloadHelper(base as json.LiteralMap, author);
        return this.applyValidatedPayload(signed, at);
    }

    async refAdvance(refVersion: Version, author: OwnIdentity, at?: Version): Promise<B64Hash> {
        if (!this.isPermissioned()) throw new Error("refAdvance is only for permissioned sets");
        const d = await this.getScopedDag();
        at = at || await d.getFrontier();
        const base = createRefAdvancePayload(this.capabilityRef()!, refVersion);
        const signed = await signPayloadHelper(base as unknown as json.LiteralMap, author);
        return this.applyValidatedPayload(signed, at);
    }

    private async createDeletePayload(elementHash: B64Hash, barrier: boolean, at: Version): Promise<DeleteElmtPayload> {

        if (!this.acceptRedundantDelete()) {
            const view = await this.getView(at);
            const hasElmt = await view.hasByHash(elementHash);
            if (!hasElmt) {
                throw new Error("Element does not exist in set, and redundant deletes are not accepted");
            }            
        }

        if (!this.supportBarrierDelete() && barrier) {
            throw new Error("Barrier delete is not supported by this set");
        }

        if (this.supportBarrierDelete()) {
            return {action: 'delete', elementHash, barrier};
        } else {
            return {action: 'delete', elementHash};
        }        
    }

    private async applyValidatedPayload(payload: Payload, at: Version): Promise<B64Hash> {
        if (!this.selfValidate()) {
            return this.applyPayload(payload, at);
        }

        if (!await this.validatePayload(payload, at)) {
            throw new Error("Attempted to apply an invalid payload");
        }

        return this.applyPayload(payload, at);
    }

    // RObject interface

    async validatePayload(payload: json.Literal, at: Version): Promise<boolean> {
        return validateRSetPayload(payload, { mode: 'op', set: this, at });
    }

    async applyPayload(payload: Payload, at: Version): Promise<B64Hash> {

        if (isRefAdvancePayload(payload)) {
            const refPayload = payload as unknown as RefAdvancePayload;
            const meta: MetaProps = {
                ...refAdvanceMeta(refPayload.refId),
                barrier: json.toSet(['t']),
            };
            const scopedDag = await this.getScopedDag();
            return await scopedDag.append(payload, meta, at);
        }

        const setPayload = payload as unknown as SetPayload;
        
        if (setPayload['action'] === 'update') {
            const innerFactory = await this.ctx.getRegistry().lookup(this.contentType()!);
            const innerRObject = await this.loadChildObject(innerFactory, setPayload['elementHash']);
            return await innerRObject.applyPayload(setPayload['updatePayload'], at);
        } else {
            const meta: MetaProps = {};

            switch (setPayload['action']) {
                case 'create':
                    throw new Error("Create operation is not supposed to be applied _to_ a set");
                case 'add':
                    if (this.contentType() !== undefined) {
                        const scopedDag = await this.getScopedDag();
                        const elementHash = await scopedDag.computeEntryHash(setPayload, at);
                        const creationDag = await this.getCreationDagForChild(elementHash, at, setPayload);
                        const innerFactory = await this.ctx.getRegistry().lookup(this.contentType()!);
                        await innerFactory.executeCreationPayload(setPayload['element'], this.ctx, creationDag);
                        return elementHash;

                    } else {
                        addMetaPropsForSetOp(setPayload, meta, await hashElement(setPayload['element']));
                    }    
                
                    break;
                case 'delete':
                    addMetaPropsForSetOp(setPayload, meta, setPayload['elementHash']);
                    break;
                default:
                    throw new Error("Invalid set action in payload: " + setPayload['action']);
            }
            
            const scopedDag = await this.getScopedDag();
    
            return await scopedDag.append(payload, meta, at);
        }
        
        
    }

    async getView(at?: Version, from?: Version): Promise<RSetViewContract<T>> {
        
        const dag = await this.getScopedDag();

        at = at || await dag.getFrontier();
        from = from || await dag.getFrontier();


        return new RSetViewImpl<T>(this, at, from);
    }

    getContext(): RContext {
        return this.ctx;
    }

    configure(config: RSetRuntimeConfig): void {
        this.runtimeConfig = { ...this.runtimeConfig, ...config };
        this._scopedDag = undefined;
        this._causalDag = undefined;
    }

    async loadChildObject(innerFactory: RObjectFactory, elementHash: B64Hash): Promise<RObject> {
        return innerFactory.loadObject(elementHash, this.ctx, this);
    }

    seed(): string {
        return this.createOp['seed'];
    }

    contentType(): string | undefined {
        return this.createOp['contentType'];
    }
    
    acceptRedundantAdd(): boolean {
        return this.createOp['acceptRedundantAdd'] ?? true;
    }

    acceptRedundantDelete(): boolean {
        return this.createOp['acceptRedundantDelete'];
    }

    acceptUpdateForDeleted(): boolean {
        return this.createOp['acceptUpdateForDeleted'] || false;
    }

    supportBarrierAdd(): boolean {
        return this.createOp['supportBarrierAdd'] || false;
    }

    supportBarrierDelete(): boolean {
        return this.createOp['supportBarrierDelete'] || false;
    }

    isPermissioned(): boolean {
        return this.createOp['capabilityRef'] !== undefined;
    }

    capabilityRef(): B64Hash | undefined {
        return this.createOp['capabilityRef'];
    }

    capRequirementForAdd(): string | undefined {
        return this.createOp['capRequirements']?.['add'];
    }

    capRequirementForDelete(): string | undefined {
        return this.createOp['capRequirements']?.['delete'];
    }

    refAdvanceCaps(): string[] {
        return this.createOp['capRequirements']?.['refAdvance'] ?? [];
    }

    refAdvanceCreators(): boolean {
        return this.createOp['capRequirements']?.['refAdvanceCreators'] ?? true;
    }

    selfValidate(): boolean {
        return this.ctx.getConfig().selfValidate || false;
    }

    extractForeignDeps(_payload: Payload, _at: Version): ForeignDep[] | undefined {
        const ref = this.capabilityRef();
        if (ref === undefined) return undefined;
        return [{ dagId: ref, requiredHashes: [] }];
    }

    async loadRCap(): Promise<RCap | undefined> {
        const ref = this.capabilityRef();
        if (ref === undefined) return undefined;
        try {
            const factory = await this.ctx.getRegistry().lookup(RCap.typeId);
            return await factory.loadObject(ref, this.ctx) as RCap;
        } catch {
            return undefined;
        }
    }

    subscribe(callback: (event: RAddEvent | RDeleteEvent) => void): void {
        throw new Error("Method not implemented.");
    }

    unsubscribe(callback: (event: RAddEvent | RDeleteEvent) => void): void {
        throw new Error("Method not implemented.");
    }

    private deltaStrategy: RSetDeltaStrategy = 'full';

    setDeltaStrategy(strategy: RSetDeltaStrategy): void {
        this.deltaStrategy = strategy;
    }

    private async collectAllElementHashes(): Promise<Set<B64Hash>> {
        const rawDag = await this.ctx.getDag(this.createOpId, this.runtimeConfig.backendLabel);
        if (rawDag === undefined) throw new Error("DAG not found");
        const elmtHashes = new Set<B64Hash>();
        for await (const entry of rawDag.loadAllEntries()) {
            const elmts = entry.meta['elmts'];
            if (elmts !== undefined) {
                for (const h of json.fromSet(elmts)) elmtHashes.add(h);
            }
        }
        return elmtHashes;
    }

    async computeDelta(start: Version, end: Version): Promise<RSetDelta> {
        if (this.parentObj !== undefined) {
            throw new Error("computeDelta is not supported on nested RSet");
        }

        const elmtHashes = await this.collectAllElementHashes();

        const startView = await this.getView(start, start) as RSetViewContract<T>;
        const endView = await this.getView(end, end) as RSetViewContract<T>;

        const added: B64Hash[] = [];
        const removed: B64Hash[] = [];

        for (const h of elmtHashes) {
            const inStart = await startView.hasByHash(h);
            const inEnd = await endView.hasByHash(h);
            if (!inStart && inEnd) added.push(h);
            if (inStart && !inEnd) removed.push(h);
        }

        const validityChanges: ValidityChange[] = [];

        if (this.isPermissioned()) {
            const rawDag = await this.ctx.getDag(this.createOpId, this.runtimeConfig.backendLabel);
            if (rawDag === undefined) throw new Error("DAG not found");

            for await (const entry of rawDag.loadAllEntries()) {
                const p = entry.payload as unknown as SetPayload;
                if (p['action'] !== 'add' && p['action'] !== 'delete') continue;

                const wasValid = await startView.checkEntryAuthorization(entry.hash);
                const nowValid = await endView.checkEntryAuthorization(entry.hash);

                if (wasValid !== nowValid) {
                    const elmts = entry.meta['elmts'];
                    if (elmts === undefined) continue;

                    for (const elementHash of json.fromSet(elmts)) {
                        validityChanges.push({
                            entryHash: entry.hash,
                            elementHash,
                            action: p['action'],
                            author: isAuthoredPayload(entry.payload) ? extractAuthor(entry.payload) : undefined,
                            wasValid,
                            nowValid,
                        });
                    }
                }
            }
        }

        return new RSetDelta(start, end, version(), added, removed, validityChanges);
    }

    async getScopedDag(): Promise<ScopedDag> {
        if (this._scopedDag === undefined) {
            if (this.parentObj !== undefined) {
                this._scopedDag = await this.parentObj.getScopedDagForChild(this.createOpId);
            } else {
                const rawDag = await this.ctx.getDag(this.createOpId, this.runtimeConfig.backendLabel);
                if (rawDag === undefined) throw new Error(`DAG '${this.createOpId}' not found`);
                this._scopedDag = new RootScopedDag(rawDag);
            }
        }
        return this._scopedDag;
    }

    async getCausalDag(): Promise<CausalDag> {
        if (this._causalDag === undefined) {
            if (this.parentObj !== undefined) {
                this._causalDag = await this.parentObj.getCausalDag();
            } else {
                const rawDag = await this.ctx.getDag(this.createOpId, this.runtimeConfig.backendLabel);
                if (rawDag === undefined) throw new Error(`DAG '${this.createOpId}' not found`);
                this._causalDag = rawDag;
            }
        }
        return this._causalDag;
    }

    // NestingParent interface

    async getScopedDagForChild(childId: B64Hash): Promise<ScopedDag> {
        const parentScopedDag = await this.getScopedDag();
        const scope = new ElementUpdateScope(this, childId, position(childId));
        return new NestedScopedDag(parentScopedDag, scope);
    }

    async getCreationDagForChild(childId: B64Hash, at: Version, addPayload: Payload): Promise<ScopedDag> {
        const parentScopedDag = await this.getScopedDag();
        const scope = new ElementAddScope(this, childId, at, addPayload as AddElmtPayload);
        return new NestedScopedDag(parentScopedDag, scope);
    }

    // SyncableObject interface

    async startSync(): Promise<void> {
        if (this.parentObj !== undefined) return;
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
            resolveRefDag: (refId) => this.ctx.getDag(refId),
        };

        this._syncSession = createSyncSession(target, [this._swarm]);
        this._swarm.activate();
    }

    async stopSync(): Promise<void> {
        if (this.parentObj !== undefined) return;

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
        this._causalDag = undefined;
    }
}

const addMetaPropsForSetOp = (payload: SetPayload, meta: MetaProps, elmtHash: B64Hash): void => {
    const elmts = json.toSet([elmtHash]);
    meta['elmts'] = elmts;
    if (payload['action'] === 'add' || payload['action'] === 'delete') {
        if (payload['barrier'] || false) {
            meta['barrier'] = json.toSet(['t']);
        }
    }
}

export class RSetViewImpl<T  extends json.Literal> implements RSetViewContract<T> {

    private target: RSetImpl<T>;
    private at: Version;
    private from: Version;

    constructor(target: RSetImpl<T>, at: Version, from: Version) {
        this.target = target;
        this.at = at;
        this.from = from;
    }

    getObject(): RSetContract<T> {
        return this.target;
    }

    getVersion(): Version {
        return this.at;
    }

    getFromVersion(): Version {
        return this.from;
    }

    async has(element: T): Promise<boolean> {

        if (this.target.contentType() !== undefined) {
            throw new Error("RSetView.has(element) is not well defined when contentType is present, please use hasByHash");
        }

        return this.hasByHash(await hashElement(element));
    }

    async hasByHash(elementHash: B64Hash): Promise<boolean> {
        const dag = await this.target.getScopedDag();

        let predicate: EntryPredicate | undefined;
        if (this.target.isPermissioned()) {
            const rcap = await this.target.loadRCap();
            if (rcap === undefined) throw new Error("Cannot load referenced RCap");
            const refId = this.target.capabilityRef()!;
            predicate = async (hash, entry) => {
                const rcapView = await this.resolveRCapViewForEntry(dag, rcap, refId, hash);
                return this.isEntryAuthorized(entry.payload, rcapView);
            };
        }

        const cover = await dag.findCoverWithFilter(
            this.at,
            { containsValues: { elmts: [elementHash] } },
            predicate,
        );

        for (const hash of cover) {
            const payload = (await dag.loadEntry(hash))!.payload as unknown as SetPayload;
            if (payload['action'] !== 'add' && payload['action'] !== 'create') continue;

            if (!this.target.supportBarrierDelete()) {
                return true;
            }

            const concurrentBarriers = await dag.findConcurrentCoverWithFilter(
                this.from, version(hash),
                { containsValues: { barrier: ['t'], elmts: [elementHash] } },
                predicate,
            );
            if (concurrentBarriers.size === 0) return true;
        }

        if (this.target.supportBarrierAdd()) {
            const barrierAdds = await dag.findConcurrentCoverWithFilter(
                this.from, this.at,
                { containsValues: { barrier: ['t'], elmts: [elementHash] } },
                predicate,
            );
            for (const hash of barrierAdds) {
                const payload = (await dag.loadEntry(hash))!.payload as unknown as SetPayload;
                if (payload['action'] === 'add') return true;
            }
        }

        return false;
    }

    // Compositional: rcapAt = which RCap version E is checked against (RSet barrier
    // ref-advances may widen this); rcapFrom = observation frontier for RCap revision.
    private async resolveRCapViewForEntry(
        dag: ScopedDag, rcap: RCap, refId: B64Hash, entryHash: B64Hash,
    ): Promise<RCapView> {
        const rcapAt = await resolveRefVersionAtPosition(dag, refId, version(entryHash), this.from);
        const rcapFrom = await resolveRefVersionAtPosition(dag, refId, this.from, this.from);
        return rcap.getView(rcapAt, rcapFrom) as Promise<RCapView>;
    }

    private async isEntryAuthorized(payload: json.Literal, rcapView: RCapView): Promise<boolean> {
        const p = payload as unknown as SetPayload;

        if (p['action'] === 'create') return true;

        const capName = p['action'] === 'add'
            ? this.target.capRequirementForAdd()
            : this.target.capRequirementForDelete();

        if (capName === undefined) return true;

        if (!isAuthoredPayload(payload)) return false;

        const authorId = extractAuthor(payload)!;
        return rcapView.hasCapability(authorId, capName);
    }

    async checkEntryAuthorization(entryHash: B64Hash): Promise<boolean> {
        if (!this.target.isPermissioned()) return true;
        const dag = await this.target.getScopedDag();
        const rcap = await this.target.loadRCap();
        if (rcap === undefined) throw new Error("Cannot load referenced RCap");
        const refId = this.target.capabilityRef()!;
        const entry = await dag.loadEntry(entryHash);
        if (entry === undefined) return false;
        const rcapView = await this.resolveRCapViewForEntry(dag, rcap, refId, entryHash);
        return this.isEntryAuthorized(entry.payload, rcapView);
    }

    async getReferences(): Promise<B64Hash[]> {
        const ref = this.target.capabilityRef();
        if (ref === undefined) return [];
        return [ref];
    }

    async resolveRefVersion(refId: B64Hash): Promise<Version> {
        if (refId !== this.target.capabilityRef()) {
            throw new Error("Unknown reference: " + refId);
        }

        const dag = await this.target.getScopedDag();
        return resolveRefVersionAtPosition(dag, refId, this.at, this.from);
    }

    async loadRObjectByHash(elementHash: B64Hash): Promise<RObject | undefined> {

        if (this.target.contentType() === undefined) {
            throw new Error("RSetView.getRObjectByHash is not supported when RSet has no contentType");
        }
        
        const innerFactory = await this.target.getContext().getRegistry().lookup(this.target.contentType()!);
        return innerFactory.loadObject(elementHash, this.target.getContext(), this.target);
    }
}

class NestedElementScope implements DagScope {

    private parent: RSetImpl;
    private elementHash: B64Hash;
    private start: Position;
    private executeAddOp?: AddElmtPayload;

    constructor(parent: RSetImpl, elementHash: B64Hash, startAt: Position, executeAddOp?: AddElmtPayload) {
        this.parent = parent;
        this.start = startAt;
        this.elementHash = elementHash;
        this.executeAddOp = executeAddOp;
    }

    startAt(): Position {
        return this.start;
    }

    startEmpty(): boolean {
        return this.executeAddOp !== undefined;
    }

    baseFilter(): EntryMetaFilter {
        return {containsValues: {"inner-elmts": [this.elementHash]}};
    }

    wrapPayload(payload: json.Literal, at: Position): json.Literal {


        if (this.executeAddOp !== undefined && set.eq(at, this.start)) {

            if (json.toStringNormalized(this.executeAddOp['element']) !== json.toStringNormalized(payload)) {
                throw new Error("An inner element in an RSet has modified its creation payload - this is not supported.");
            }

            return this.executeAddOp;
        } else {
            const outerPayload: UpdateElmtPayload = {
                action: 'update',
                elementHash: this.elementHash,
                updatePayload: payload,
            }

            return outerPayload;
        }
    }

    unwrapPayload(payload: json.Literal): json.Literal {
        
        const outerPayload = payload as SetPayload;

        if (outerPayload['action'] === 'update') {
            return outerPayload['updatePayload'];
        } else if (outerPayload['action'] === 'add') {
            return outerPayload['element'];
        } else {
            throw new Error("Invalid payload type in unwrapPayload for RSet: " + outerPayload['action']);
        }
    }

    wrapMeta(innerMeta: MetaProps, wrappedPayload: json.Literal, at: Position): MetaProps {

        const setPayload = wrappedPayload as SetPayload;

        const outerMeta: MetaProps = {};

        if (setPayload['action'] === 'add') {

            addMetaPropsForSetOp(setPayload, outerMeta, this.elementHash);

        }

        outerMeta["inner-elmts"] = json.toSet([this.elementHash]);

        for (const key in innerMeta) {
            outerMeta["inner-" + this.elementHash + "-" + key] = innerMeta[key];
        }

        return outerMeta;
    }

    unwrapMeta(outerMeta: MetaProps, wrappedPayload: json.Literal, at: Position): MetaProps {
        
        const innerMeta: MetaProps = {};

        const lengthToSkip = ("inner-" + this.elementHash + "-").length;
        for (const key in outerMeta) {
            if (key.startsWith("inner-" + this.elementHash + "-")) {
                innerMeta[key.substring(lengthToSkip)] = outerMeta[key];
            }
        }

        return innerMeta;
    }

    wrapFilter(filter: EntryMetaFilter): EntryMetaFilter {
        
        const wrappedFilter: EntryMetaFilter = {};

        if (filter.containsKeys !== undefined) {
            wrappedFilter.containsKeys = filter.containsKeys.map(key => "inner-" + this.elementHash + "-" + key);
        }

        if (filter.containsValues !== undefined) {
            
            wrappedFilter.containsValues = {} as MetaContainsValues;
            
            for (const key in filter.containsValues) {
                wrappedFilter.containsValues['inner-' + this.elementHash + '-' + key] = filter.containsValues[key];    
            }
        }
        
        return wrappedFilter;
    }

    async validateWrappedPayload(wrappedPayload: json.Literal, _wrappedMeta: MetaProps, at: Position): Promise<boolean> {
        if (!this.parent.selfValidate()) {
            return true;
        }

        return this.parent.validatePayload(wrappedPayload, at);
    }
}

class ElementAddScope extends NestedElementScope implements DagScope {

    constructor(parent: RSetImpl, elementHash: B64Hash, startAt: Position, executeAddOp: AddElmtPayload) {
        super(parent, elementHash, startAt, executeAddOp);
    }
}

class ElementUpdateScope extends NestedElementScope implements DagScope {

    constructor(parent: RSetImpl, elementHash: B64Hash, startAt: Position) {
        super(parent, elementHash, startAt);
    }
}

function hashElement<T extends json.Literal>(element: T): B64Hash {
    return sha256.hashToB64(stringToUint8Array(json.toStringNormalized(element)));
}

export type RSetDeltaStrategy = 'full' | 'bounded';

export type ValidityChange = {
    entryHash: B64Hash;
    elementHash: B64Hash;
    action: 'add' | 'delete' | 'create';
    author: KeyId | undefined;
    wasValid: boolean;
    nowValid: boolean;
};

export class RSetDelta implements Delta {
    constructor(
        private start: Version,
        private end: Version,
        private revisionBound: Version,
        public readonly added: B64Hash[],
        public readonly removed: B64Hash[],
        public readonly validityChanges: ValidityChange[],
    ) {}

    getStartVersion(): Version { return this.start; }
    getEndVersion(): Version { return this.end; }
    getRevisionBound(): Version { return this.revisionBound; }
}

export interface RSet<T extends json.Literal = json.Literal> extends RSetContract<T> {}
export interface RSetView<T extends json.Literal = json.Literal> extends RSetViewContract<T> {}
export const RSet = RSetImpl;
export const RSetView = RSetViewImpl;
